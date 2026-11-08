// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package env

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

// DefaultTraceServiceName prefixes span names when
// ClientOptions.TraceServiceName is empty.
const DefaultTraceServiceName = "ate-env-go-client"

// Span attribute keys, the vocabulary shared with the Python client:
// Substrate identities use Substrate's ateattr keys (ate.*), so spans join
// Substrate's own logs and traces; client-only fields use ate_env.*.
// Values carry ids, the executable name and sizes; never a command line or
// file content.
const (
	AttrActorName         = attribute.Key("ate.actor.name")
	AttrAtespace          = attribute.Key("ate.atespace")
	AttrTemplateName      = attribute.Key("ate.template.name")
	AttrTemplateAtespace  = attribute.Key("ate.template.atespace")
	AttrRootfsImage       = attribute.Key("ate_env.rootfs_image")
	AttrCommandExecutable = attribute.Key("ate_env.command.executable")
	AttrExitCode          = attribute.Key("ate_env.exit_code")
	AttrFilePath          = attribute.Key("ate_env.file.path")
	AttrFileSize          = attribute.Key("ate_env.file.size")
)

// Operations, the part of each span name after "<TraceServiceName>.".
const (
	opCreateEnvironment    = "create_environment"
	opGetEnvironment       = "get_environment"
	opSuspendEnvironment   = "suspend_environment"
	opDeleteEnvironment    = "delete_environment"
	opExec                 = "exec"
	opReadFile             = "read_file"
	opWriteFile            = "write_file"
	opGetServerInfo        = "get_server_info"
	opGetImageStatus       = "get_image_status"
	opEnsureImageTemplate  = "ensure_image_template"
	opListPlacementDomains = "list_placement_domains"
	opWarmImage            = "warm_image"
)

// tracePropagator is the only propagator the client uses: W3C Trace
// Context, no Baggage.
var tracePropagator propagation.TextMapPropagator = propagation.TraceContext{}

// NewTracerProvider returns a TracerProvider that batches spans to an
// OTLP/gRPC exporter, for callers who want the OTLP default. The exporter
// reads OTEL_EXPORTER_OTLP_ENDPOINT (default localhost:4317) and
// OTEL_EXPORTER_OTLP_INSECURE; serviceName becomes the service.name resource
// attribute. Pass it as ClientOptions.TracerProvider. The caller owns it and
// must call Shutdown to flush.
func NewTracerProvider(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("env: creating OTLP trace exporter: %w", err)
	}
	res, err := sdkresource.Merge(sdkresource.Default(),
		sdkresource.NewSchemaless(attribute.String("service.name", serviceName)))
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, fmt.Errorf("env: building OTel resource: %w", err)
	}
	return sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res)), nil
}

// clientTracer starts the client's spans. The zero value (no provider)
// starts none and leaves ctx untouched.
type clientTracer struct {
	tracer trace.Tracer
	svc    string
	// inject writes the span's W3C context into the outgoing metadata
	// itself, for a caller-supplied GRPCConn that has no otelgrpc handler.
	inject bool
	// target is the caller-supplied GRPCConn's target, for server.address
	// and server.port on its CLIENT spans.
	target string
}

func newClientTracer(opts ClientOptions) clientTracer {
	if opts.TracerProvider == nil {
		return clientTracer{}
	}
	svc := opts.TraceServiceName
	if svc == "" {
		svc = DefaultTraceServiceName
	}
	t := clientTracer{
		tracer: opts.TracerProvider.Tracer(strings.ReplaceAll(svc, "-", "_")),
		svc:    svc,
		inject: opts.GRPCConn != nil,
	}
	if opts.GRPCConn != nil {
		t.target = opts.GRPCConn.Target()
	}
	return t
}

// noopSpan is the non-recording span every call returns without a provider.
var noopSpan = trace.SpanFromContext(context.Background())

// environmentService is the gRPC service of the spans startRPC opens.
const environmentService = "ateenv.v1alpha.EnvironmentService"

// start opens the INTERNAL span "<svc>.<op>" as a child of the span in ctx,
// for an operation that may make several RPCs (exec makes StartProcess and
// StreamProcessOutput). Empty attribute values are not recorded.
func (t clientTracer) start(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.open(ctx, op, trace.SpanKindInternal, attrs)
}

// startRPC opens the span of an operation that makes exactly one
// EnvironmentService RPC, method. On the client's own connection, the
// otelgrpc handler adds the RPC's CLIENT span (with rpc.* attributes) under
// this one, so this span is INTERNAL: two nested CLIENT spans would make
// trace backends draw the client-to-server edge twice. On a caller-supplied
// GRPCConn (t.inject) there is no otelgrpc span, so this span is the RPC's
// CLIENT span and carries the rpc.* keys of rpcAttrs, server.address and
// server.port, as the Python client's spans do.
func (t clientTracer) startRPC(ctx context.Context, op, method string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !t.inject {
		return t.open(ctx, op, trace.SpanKindInternal, attrs)
	}
	return t.open(ctx, op, trace.SpanKindClient, append(attrs, rpcAttrs(environmentService, method, t.target)...))
}

func (t clientTracer) open(ctx context.Context, op string, kind trace.SpanKind, attrs []attribute.KeyValue) (context.Context, trace.Span) {
	if t.tracer == nil {
		return ctx, noopSpan
	}
	ctx, span := t.tracer.Start(ctx, t.svc+"."+op, trace.WithSpanKind(kind), trace.WithAttributes(nonEmpty(attrs)...))
	if t.inject {
		carrier := propagation.MapCarrier{}
		tracePropagator.Inject(ctx, carrier)
		for k, v := range carrier {
			ctx = metadata.AppendToOutgoingContext(ctx, k, v)
		}
	}
	return ctx, span
}

// setAttributes sets the attributes with a non-empty value on span.
func setAttributes(span trace.Span, attrs ...attribute.KeyValue) {
	if attrs = nonEmpty(attrs); len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}

// nonEmpty drops string attributes whose value is empty, as the Python
// client drops None values: an unknown value is not recorded as "".
func nonEmpty(attrs []attribute.KeyValue) []attribute.KeyValue {
	out := attrs[:0:0]
	for _, kv := range attrs {
		if kv.Value.Type() == attribute.STRING && kv.Value.AsString() == "" {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// The rpc.* convention rpcAttrs follows, as otelgrpc selects it.
const (
	semconvNew = "new" // rpc.system.name and the full rpc.method (default)
	semconvOld = "old" // rpc.system, rpc.service and the short rpc.method
	semconvDup = "dup" // both sets; rpc.method is the full name
)

// parseRPCSemconvMode reads OTEL_SEMCONV_STABILITY_OPT_IN as otelgrpc
// v0.72.0's parseSemconvMode does, and as the Python client's
// _rpc_semconv_mode does: a comma-separated list, each item trimmed; the
// first item that is rpc/dup or rpc/old decides, and anything else means new.
func parseRPCSemconvMode(value string) string {
	for item := range strings.SplitSeq(value, ",") {
		switch strings.TrimSpace(item) {
		case "rpc/dup":
			return semconvDup
		case "rpc/old":
			return semconvOld
		}
	}
	return semconvNew
}

// rpcSemconvMode is read once, as otelgrpc reads it once per handler.
var rpcSemconvMode = parseRPCSemconvMode(os.Getenv("OTEL_SEMCONV_STABILITY_OPT_IN"))

// rpcAttrs are the OTel semantic-convention attributes of one gRPC call,
// with the same keys and values as the Python client's rpc_attrs and as
// otelgrpc v0.72.0's client spans: by default rpc.system.name and
// rpc.method with the full "<service>/<method>"; with
// OTEL_SEMCONV_STABILITY_OPT_IN=rpc/old the older rpc.system, rpc.service
// and the short rpc.method; with rpc/dup both sets, with the full
// rpc.method, the value otelgrpc keeps. target is the connection's target:
// "host:port", optionally behind gRPC's dns:/// or passthrough:/// scheme;
// another form (such as unix:///path) is recorded whole as server.address.
func rpcAttrs(service, method, target string) []attribute.KeyValue {
	fullMethod := service + "/" + method
	var attrs []attribute.KeyValue
	switch rpcSemconvMode {
	case semconvOld:
		attrs = []attribute.KeyValue{
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", service),
			attribute.String("rpc.method", method),
		}
	case semconvDup:
		attrs = []attribute.KeyValue{
			attribute.String("rpc.system", "grpc"),
			attribute.String("rpc.service", service),
			attribute.String("rpc.system.name", "grpc"),
			attribute.String("rpc.method", fullMethod),
		}
	default:
		attrs = []attribute.KeyValue{
			attribute.String("rpc.system.name", "grpc"),
			attribute.String("rpc.method", fullMethod),
		}
	}
	for _, scheme := range []string{"dns:///", "passthrough:///"} {
		target = strings.TrimPrefix(target, scheme)
	}
	if target == "" {
		return attrs
	}
	host, port := target, ""
	if i := strings.LastIndex(target, ":"); i > 0 {
		host, port = target[:i], target[i+1:]
	}
	if n, err := strconv.Atoi(port); err == nil && host != "" {
		return append(attrs, attribute.String("server.address", strings.Trim(host, "[]")), attribute.Int("server.port", n))
	}
	return append(attrs, attribute.String("server.address", target))
}

// imageRefAttr is an image reference without its tag, for span attributes,
// as the Python client's image_ref_attr: "repo@sha256:..." (the digest is an
// id) and "repo" are kept; "repo:tag" and "repo:tag@sha256:..." drop the
// tag.
func imageRefAttr(ref string) string {
	name, digest, hasDigest := strings.Cut(ref, "@")
	if colon := strings.LastIndex(name, ":"); colon > strings.LastIndex(name, "/") {
		name = name[:colon]
	}
	if hasDigest {
		return name + "@" + digest
	}
	return name
}

// endSpan records err on span, if any, and ends it.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// envAssignment matches a leading shell variable assignment, NAME=value or
// bash's NAME+=value, as in "API_KEY=s3cr3t python x.py". It is the same
// pattern as the Python client's _ENV_ASSIGNMENT.
var envAssignment = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\+?=`)

// executable returns the executable name of a shell command line, as the
// Python client's executable_attr does: the base name of its first word
// after any leading NAME=value assignments, which can carry secrets. The
// line is split like a POSIX shell splits it (quotes and backslashes), so
// "'my prog' arg" gives "my prog". A line that cannot be split (an
// unterminated quote, a trailing backslash) gives "", which records no
// attribute. The rest of the line is never recorded.
func executable(commandLine string) string {
	words, ok := shellWords(commandLine)
	if !ok {
		return ""
	}
	for _, w := range words {
		if envAssignment.MatchString(w) {
			continue
		}
		return w[strings.LastIndex(w, "/")+1:]
	}
	return ""
}

// shellWords splits s into words like Python's shlex.split in POSIX mode:
// blanks separate words; single quotes keep everything literal; inside
// double quotes a backslash escapes only '"' and '\\'; outside quotes a
// backslash escapes the next character. ok is false for an unterminated
// quote or a trailing backslash.
func shellWords(s string) (words []string, ok bool) {
	var cur strings.Builder
	inWord := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			if inWord {
				words = append(words, cur.String())
				cur.Reset()
				inWord = false
			}
		case c == '\\':
			if i+1 >= len(s) {
				return nil, false
			}
			i++
			cur.WriteByte(s[i])
			inWord = true
		case c == '\'':
			end := strings.IndexByte(s[i+1:], '\'')
			if end < 0 {
				return nil, false
			}
			cur.WriteString(s[i+1 : i+1+end])
			i += end + 1
			inWord = true
		case c == '"':
			closed := false
			for i++; i < len(s); i++ {
				if s[i] == '"' {
					closed = true
					break
				}
				if s[i] == '\\' && i+1 < len(s) && (s[i+1] == '"' || s[i+1] == '\\') {
					i++
				}
				cur.WriteByte(s[i])
			}
			if !closed {
				return nil, false
			}
			inWord = true
		default:
			cur.WriteByte(c)
			inWord = true
		}
	}
	if inWord {
		words = append(words, cur.String())
	}
	return words, true
}

// spanReader counts the bytes read through it and ends its span, with
// env.file.size, on EOF, on a read error or on Close, whichever comes first.
type spanReader struct {
	io.ReadCloser
	span trace.Span
	n    int64
	once sync.Once
}

func (r *spanReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.n += int64(n)
	if err == io.EOF {
		r.finish(nil)
	} else if err != nil {
		r.finish(err)
	}
	return n, err
}

func (r *spanReader) Close() error {
	err := r.ReadCloser.Close()
	r.finish(nil)
	return err
}

func (r *spanReader) finish(err error) {
	r.once.Do(func() {
		r.span.SetAttributes(AttrFileSize.Int64(r.n))
		endSpan(r.span, err)
	})
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

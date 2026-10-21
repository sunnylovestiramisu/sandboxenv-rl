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
	"path/filepath"
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
	AttrRootfsImage       = attribute.Key("ate_env.rootfs_image")
	AttrCommandExecutable = attribute.Key("ate_env.command.executable")
	AttrExitCode          = attribute.Key("ate_env.exit_code")
	AttrFilePath          = attribute.Key("ate_env.file.path")
	AttrFileSize          = attribute.Key("ate_env.file.size")
)

// Operations, the part of each span name after "<TraceServiceName>.".
const (
	opCreateEnvironment  = "create_environment"
	opGetEnvironment     = "get_environment"
	opSuspendEnvironment = "suspend_environment"
	opDeleteEnvironment  = "delete_environment"
	opExec               = "exec"
	opReadFile           = "read_file"
	opWriteFile          = "write_file"
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
}

func newClientTracer(opts ClientOptions) clientTracer {
	if opts.TracerProvider == nil {
		return clientTracer{}
	}
	svc := opts.TraceServiceName
	if svc == "" {
		svc = DefaultTraceServiceName
	}
	return clientTracer{
		tracer: opts.TracerProvider.Tracer(strings.ReplaceAll(svc, "-", "_")),
		svc:    svc,
		inject: opts.GRPCConn != nil,
	}
}

// noopSpan is the non-recording span every call returns without a provider.
var noopSpan = trace.SpanFromContext(context.Background())

// start opens the span "<svc>.<op>" as a child of the span in ctx.
func (t clientTracer) start(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if t.tracer == nil {
		return ctx, noopSpan
	}
	ctx, span := t.tracer.Start(ctx, t.svc+"."+op, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	if t.inject {
		carrier := propagation.MapCarrier{}
		tracePropagator.Inject(ctx, carrier)
		for k, v := range carrier {
			ctx = metadata.AppendToOutgoingContext(ctx, k, v)
		}
	}
	return ctx, span
}

// endSpan records err on span, if any, and ends it.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// executable returns the executable name of a shell command line: the base
// name of its first word. The rest of the line is never recorded.
func executable(commandLine string) string {
	fields := strings.Fields(commandLine)
	if len(fields) == 0 {
		return ""
	}
	return filepath.Base(fields[0])
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

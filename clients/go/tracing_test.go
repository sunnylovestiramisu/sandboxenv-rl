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

package env_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"pgregory.net/rapid"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/guest"
	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/internaltest/fakerouter"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// tracingFixture is the stack the SDK talks to, with the API server's gRPC
// server traced into a recorder of its own: the server side of the trace.
type tracingFixture struct {
	router  *fakerouter.Router
	control *fakecontrol.Server
	addr    string
	server  *tracetest.SpanRecorder
}

func newTracingFixture(t *testing.T) *tracingFixture {
	t.Helper()
	control := fakecontrol.New()
	controlAddr, stopControl, err := control.Serve()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stopControl)
	router := fakerouter.New()
	router.Running = func(id string) bool {
		return control.Status(id) == ateapipb.ActorState_ACTOR_STATE_RUNNING
	}
	routerAddr, stopRouter := router.Serve()
	t.Cleanup(stopRouter)
	direct, err := ate.New(ate.Options{ControlAddr: controlAddr, RouterAddr: routerAddr, SkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { direct.Close() })
	service := apiservice.New(direct, routerAddr, "")

	serverRec := tracetest.NewSpanRecorder()
	serverTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(serverRec))
	grpcServer := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler(
		otelgrpc.WithTracerProvider(serverTP), otelgrpc.WithPropagators(propagation.TraceContext{}))))
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, service)
	ateenvv1alpha.RegisterProcessServiceServer(grpcServer, service)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcServer, service)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	return &tracingFixture{router: router, control: control, addr: lis.Addr().String(), server: serverRec}
}

func (f *tracingFixture) serveGuest(t *testing.T, id string) {
	t.Helper()
	srv, cleanup, err := guest.NewServer(guest.Config{Workspace: t.TempDir(), EnableProcess: true, EnableFileSystem: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	f.router.Register(id, srv)
}

func (f *tracingFixture) client(t *testing.T, opts env.ClientOptions) *env.Client {
	t.Helper()
	opts.Endpoint = f.addr
	c, err := env.NewClient(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// waitForServerSpan polls f.server.Ended() until match returns true or the
// timeout expires. Server-side otelgrpc spans end asynchronously after the RPC
// response has already returned to the client, so reading f.server.Ended()
// immediately after a client call can race the server recorder.
func (f *tracingFixture) waitForServerSpan(t *testing.T, match func(sdktrace.ReadOnlySpan) bool) sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, s := range f.server.Ended() {
			if match(s) {
				return s
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for server span")
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// exercise runs every traced operation once on environment id.
func exercise(t *testing.T, c *env.Client, id string) {
	t.Helper()
	ctx := t.Context()
	e, err := c.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: id})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := c.Get(ctx, "", id); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp, err := e.Shell(ctx, "/bin/echo hello secret-arg"); err != nil || resp.ExitCode != 0 {
		t.Fatalf("Shell: %v %+v", err, resp)
	}
	if err := e.WriteFile(ctx, "notes.txt", strings.NewReader("hello world"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	rc, err := e.ReadFile(ctx, "notes.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if b, err := io.ReadAll(rc); err != nil || string(b) != "hello world" {
		t.Fatalf("ReadFile content %q, %v", b, err)
	}
	rc.Close()
	if err := e.Suspend(ctx); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if err := e.Delete(ctx); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func attrs(s sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	m := map[attribute.Key]attribute.Value{}
	for _, kv := range s.Attributes() {
		m[kv.Key] = kv.Value
	}
	return m
}

// spansByName returns the client's own operation spans (not the otelgrpc
// RPC spans under them) by name.
func spansByName(rec *tracetest.SpanRecorder, prefix string) map[string]sdktrace.ReadOnlySpan {
	m := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		if strings.HasPrefix(s.Name(), prefix+".") {
			m[s.Name()] = s
		}
	}
	return m
}

// AC-11: with a provider the client emits "ate-env-go-client.<op>" spans
// with ate.* / ate_env.* attributes, and ate-env-api's spans join the same trace.
func TestTracingSpansWithProvider(t *testing.T) {
	f := newTracingFixture(t)
	f.serveGuest(t, "traced")
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	c := f.client(t, env.ClientOptions{TracerProvider: tp})
	exercise(t, c, "traced")

	spans := spansByName(rec, env.DefaultTraceServiceName)
	for _, op := range []string{"create_environment", "get_environment", "exec", "write_file", "read_file", "suspend_environment", "delete_environment"} {
		s, ok := spans[env.DefaultTraceServiceName+"."+op]
		if !ok {
			t.Errorf("no span %s.%s; got %v", env.DefaultTraceServiceName, op, keys(spans))
			continue
		}
		if got := attrs(s)[env.AttrActorName].AsString(); got != "traced" {
			t.Errorf("%s env.id = %q, want traced", s.Name(), got)
		}
		if s.InstrumentationScope().Name != "ate_env_go_client" {
			t.Errorf("%s scope = %q, want ate_env_go_client", s.Name(), s.InstrumentationScope().Name)
		}
	}

	create := attrs(spans["ate-env-go-client.create_environment"])
	if create[env.AttrAtespace].AsString() != apiservice.DefaultAtespace || create[env.AttrTemplateName].AsString() != apiservice.DefaultTemplate {
		t.Errorf("create_environment attributes = %v, want the resolved atespace and template", create)
	}
	if got := create[env.AttrTemplateAtespace].AsString(); got != apiservice.DefaultAtespace {
		t.Errorf("create_environment %s = %q, want the resolved template atespace %q", env.AttrTemplateAtespace, got, apiservice.DefaultAtespace)
	}
	// No image was given: the attribute is absent, not "" (as in the Python client).
	if img, ok := create[env.AttrRootfsImage]; ok {
		t.Errorf("create_environment records ate_env.rootfs_image=%q without an image", img.AsString())
	}
	exec := attrs(spans["ate-env-go-client.exec"])
	if exec[env.AttrCommandExecutable].AsString() != "echo" || exec[env.AttrExitCode].AsInt64() != 0 {
		t.Errorf("exec attributes = %v, want executable echo and exit code 0", exec)
	}
	if !exec[env.AttrFirstCommand].AsBool() {
		t.Errorf("first exec span missing ate_env.first_command=true: %v", exec)
	}
	if timedOut, ok := exec[env.AttrTimedOut]; !ok || timedOut.AsBool() {
		t.Errorf("exec ate_env.timed_out = %v (present=%v), want false", timedOut, ok)
	}
	if _, nested := spans["ate-env-go-client.start_process"]; nested {
		t.Error("Run opened a nested start_process span under exec")
	}
	for _, kv := range spans["ate-env-go-client.exec"].Attributes() {
		if strings.Contains(kv.Value.Emit(), "secret-arg") {
			t.Errorf("exec attribute %s carries the command line: %q", kv.Key, kv.Value.Emit())
		}
	}
	for _, op := range []string{"write_file", "read_file"} {
		a := attrs(spans["ate-env-go-client."+op])
		if a[env.AttrFilePath].AsString() != "notes.txt" || a[env.AttrFileSize].AsInt64() != int64(len("hello world")) {
			t.Errorf("%s attributes = %v, want path notes.txt and size 11", op, a)
		}
	}

	// The API server's CreateEnvironment span is in the client's trace,
	// under the client's RPC span for that call.
	clientSpanIDs := map[string]bool{}
	for _, s := range rec.Ended() {
		clientSpanIDs[s.SpanContext().SpanID().String()] = true
	}
	traceID := spans["ate-env-go-client.create_environment"].SpanContext().TraceID()
	serverSpan := f.waitForServerSpan(t, func(s sdktrace.ReadOnlySpan) bool {
		return s.Name() == "ateenv.v1alpha.EnvironmentService/CreateEnvironment"
	})
	if serverSpan.SpanContext().TraceID() != traceID {
		t.Errorf("server span trace %s, want the client's %s", serverSpan.SpanContext().TraceID(), traceID)
	}
	if !clientSpanIDs[serverSpan.Parent().SpanID().String()] {
		t.Errorf("server span parent %s is not a client span", serverSpan.Parent().SpanID())
	}
}

// TraceServiceName changes the span prefix and the scope.
func TestTracingServiceNamePrefix(t *testing.T) {
	f := newTracingFixture(t)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	c := f.client(t, env.ClientOptions{TracerProvider: tp, TraceServiceName: "my-trainer"})
	if _, err := c.Create(t.Context(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "prefixed"}); err != nil {
		t.Fatal(err)
	}
	spans := spansByName(rec, "my-trainer")
	s, ok := spans["my-trainer.create_environment"]
	if !ok {
		t.Fatalf("no my-trainer.create_environment span; got %v", keys(spans))
	}
	if s.InstrumentationScope().Name != "my_trainer" {
		t.Errorf("scope = %q, want my_trainer", s.InstrumentationScope().Name)
	}
	if len(spansByName(rec, env.DefaultTraceServiceName)) != 0 {
		t.Error("spans with the default prefix were recorded")
	}
}

// Without a provider the client records no span, even when a global
// provider is installed, does not touch the global provider, and sends no
// trace context to the server.
func TestTracingOffWithoutProvider(t *testing.T) {
	globalRec := tracetest.NewSpanRecorder()
	globalTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(globalRec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(globalTP)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	f := newTracingFixture(t)
	f.serveGuest(t, "untraced")
	c := f.client(t, env.ClientOptions{})
	exercise(t, c, "untraced")

	if n := len(globalRec.Ended()); n != 0 {
		t.Errorf("recorded %d spans through the global provider, want 0", n)
	}
	if otel.GetTracerProvider() != globalTP {
		t.Error("the client replaced the global tracer provider")
	}
	f.waitForServerSpan(t, func(s sdktrace.ReadOnlySpan) bool {
		return s.Name() == "ateenv.v1alpha.EnvironmentService/DeleteEnvironment"
	})
	for _, s := range f.server.Ended() {
		if s.Parent().IsValid() {
			t.Errorf("server span %s has a parent %s; the client sent trace context", s.Name(), s.Parent().SpanID())
		}
	}
}

// With a caller-supplied GRPCConn the client cannot add the otelgrpc
// handler; it puts the trace context in the call metadata itself, so the
// server span is the child of the client's operation span.
func TestTracingWithCallerConnPropagates(t *testing.T) {
	f := newTracingFixture(t)
	conn, err := grpc.NewClient(f.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	c, err := env.NewClient(env.ClientOptions{GRPCConn: conn, TracerProvider: tp})
	if err != nil {
		t.Fatal(err)
	}
	// A pre-existing stale traceparent in outgoing metadata is replaced, not duplicated.
	seededCtx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(
		"traceparent", "00-11111111111111111111111111111111-2222222222222222-01",
	))
	if _, err := c.Create(seededCtx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "own-conn"}); err != nil {
		t.Fatal(err)
	}
	create := spansByName(rec, env.DefaultTraceServiceName)["ate-env-go-client.create_environment"]
	if create == nil {
		t.Fatal("no create_environment span")
	}
	serverSpan := f.waitForServerSpan(t, func(s sdktrace.ReadOnlySpan) bool {
		return s.Name() == "ateenv.v1alpha.EnvironmentService/CreateEnvironment"
	})
	if serverSpan.Parent().SpanID() != create.SpanContext().SpanID() {
		t.Errorf("server parent %s, want the create_environment span %s", serverSpan.Parent().SpanID(), create.SpanContext().SpanID())
	}
}

// NewTracerProvider builds the OTLP default without connecting.
func TestNewTracerProvider(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	tp, err := env.NewTracerProvider(context.Background(), "my-trainer")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tp.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func keys(m map[string]sdktrace.ReadOnlySpan) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// On the client's own connection the wrapper span is INTERNAL and the
// otelgrpc span under it is the RPC's only CLIENT span. On a caller-supplied
// connection, which has no otelgrpc span, the wrapper span is the CLIENT
// span.
func TestTracingSpanKinds(t *testing.T) {
	f := newTracingFixture(t)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	c := f.client(t, env.ClientOptions{TracerProvider: tp})
	if _, err := c.Create(t.Context(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "kinds"}); err != nil {
		t.Fatal(err)
	}
	wrapper := spansByName(rec, env.DefaultTraceServiceName)["ate-env-go-client.create_environment"]
	if wrapper == nil {
		t.Fatal("no create_environment span")
	}
	if wrapper.SpanKind() != trace.SpanKindInternal {
		t.Errorf("own connection: wrapper kind = %s, want internal", wrapper.SpanKind())
	}
	clientSpans := 0
	for _, s := range rec.Ended() {
		if s.SpanKind() != trace.SpanKindClient {
			continue
		}
		clientSpans++
		if s.Name() != "ateenv.v1alpha.EnvironmentService/CreateEnvironment" || s.Parent().SpanID() != wrapper.SpanContext().SpanID() {
			t.Errorf("client span %q (parent %s), want the otelgrpc CreateEnvironment span under the wrapper", s.Name(), s.Parent().SpanID())
		}
	}
	if clientSpans != 1 {
		t.Errorf("own connection: %d client spans, want 1 (otelgrpc only)", clientSpans)
	}

	conn, err := grpc.NewClient(f.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	rec2 := tracetest.NewSpanRecorder()
	c2, err := env.NewClient(env.ClientOptions{GRPCConn: conn, TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec2))})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c2.Create(t.Context(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "kinds-caller"}); err != nil {
		t.Fatal(err)
	}
	if w := spansByName(rec2, env.DefaultTraceServiceName)["ate-env-go-client.create_environment"]; w == nil || w.SpanKind() != trace.SpanKindClient {
		t.Errorf("caller connection: wrapper = %v, want kind client", w)
	}
}

// On a caller-supplied connection the spans match the Python client's: the
// single-RPC EnvironmentService calls are CLIENT spans with rpc.* and
// server.* attributes; exec and the file calls are INTERNAL without them;
// empty values are not recorded; an image tag is dropped; ServerInfo is
// traced as get_server_info.
func TestTracingCallerConnSpansMatchPythonClient(t *testing.T) {
	// The default rpc.* convention, whatever OTEL_SEMCONV_STABILITY_OPT_IN
	// the test process has.
	env.SetRPCSemconvModeForTest(t, "new")
	f := newTracingFixture(t)
	f.serveGuest(t, "parity")
	conn, err := grpc.NewClient(f.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	rec := tracetest.NewSpanRecorder()
	c, err := env.NewClient(env.ClientOptions{GRPCConn: conn, TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))})
	if err != nil {
		t.Fatal(err)
	}
	exercise(t, c, "parity")
	if _, err := c.ServerInfo(t.Context()); err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	digest := "sha256:" + strings.Repeat("ab", 32)
	// The image is only recorded; whether the server accepts it is not tested.
	_, _ = c.Create(t.Context(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "tagged", RootfsImage: "registry.example/repo:v1@" + digest})
	_, _ = c.ImageStatus(t.Context(), &ateenvv1alpha.GetImageStatusRequest{RootfsImage: "registry.example/repo@" + digest})
	_, _ = c.EnsureImageTemplate(t.Context(), &ateenvv1alpha.EnsureImageTemplateRequest{RootfsImage: "registry.example/repo@" + digest})
	_, _ = c.ListPlacementDomains(t.Context(), &ateenvv1alpha.ListPlacementDomainsRequest{Template: &ateenvv1alpha.Template{Name: "layered", Atespace: "t-space"}})
	_, _ = c.WarmImage(t.Context(), &ateenvv1alpha.WarmImageRequest{RootfsImage: "registry.example/repo@" + digest, Nodes: 1, Template: &ateenvv1alpha.Template{Name: "layered", Atespace: "t-space"}}, nil)

	spans := map[string][]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		spans[s.Name()] = append(spans[s.Name()], s)
	}
	prefix := env.DefaultTraceServiceName + "."
	host, port, _ := strings.Cut(f.addr, ":")
	for op, method := range map[string]string{
		"create_environment": "CreateEnvironment", "get_environment": "GetEnvironment",
		"suspend_environment": "SuspendEnvironment", "delete_environment": "DeleteEnvironment",
		"get_server_info":  "GetServerInfo",
		"get_image_status": "GetImageStatus", "ensure_image_template": "EnsureImageTemplate",
		"list_placement_domains": "ListPlacementDomains", "warm_image": "WarmImage",
	} {
		if len(spans[prefix+op]) == 0 {
			t.Errorf("no %s span", op)
			continue
		}
		s := spans[prefix+op][0]
		a := attrs(s)
		if s.SpanKind() != trace.SpanKindClient {
			t.Errorf("%s kind = %s, want client", op, s.SpanKind())
		}
		if a["rpc.system.name"].AsString() != "grpc" || a["rpc.method"].AsString() != "ateenv.v1alpha.EnvironmentService/"+method {
			t.Errorf("%s rpc.* = %v", op, a)
		}
		if _, ok := a["rpc.system"]; ok {
			t.Errorf("%s has the old rpc.system in the default mode", op)
		}
		if _, ok := a["rpc.service"]; ok {
			t.Errorf("%s has the old rpc.service in the default mode", op)
		}
		if a["server.address"].AsString() != host || fmt.Sprint(a["server.port"].AsInt64()) != port {
			t.Errorf("%s server.* = %v, want %s", op, a, f.addr)
		}
	}
	for _, op := range []string{"exec", "read_file", "write_file"} {
		s := spans[prefix+op][0]
		if s.SpanKind() != trace.SpanKindInternal {
			t.Errorf("%s kind = %s, want internal, as in the Python client", op, s.SpanKind())
		}
		if _, ok := attrs(s)["rpc.method"]; ok {
			t.Errorf("%s has rpc.* attributes", op)
		}
	}
	if _, ok := attrs(spans[prefix+"get_environment"][0])[env.AttrAtespace]; ok {
		t.Error("get_environment records an empty ate.atespace")
	}
	// The template RPCs record the template they were asked about, as the
	// Python client's image_status, list_placement_domains and warm_image do.
	for _, op := range []string{"list_placement_domains", "warm_image"} {
		a := attrs(spans[prefix+op][0])
		if a[env.AttrTemplateName].AsString() != "layered" || a[env.AttrTemplateAtespace].AsString() != "t-space" {
			t.Errorf("%s template attributes = %v, want ate.template.name=layered ate.template.atespace=t-space", op, a)
		}
	}
	for _, s := range spans[prefix+"create_environment"] {
		a := attrs(s)
		img, ok := a[env.AttrRootfsImage]
		switch a[env.AttrActorName].AsString() {
		case "parity":
			if ok {
				t.Errorf("create without an image records ate_env.rootfs_image=%q", img.AsString())
			}
		case "tagged":
			if img.AsString() != "registry.example/repo@"+digest {
				t.Errorf("ate_env.rootfs_image = %q, want the reference without its tag", img.AsString())
			}
		}
	}
}

// AC-G6 and G6: StartProcess opens a start_process span; ate_env.first_command
// appears on the first exec or start_process span of an Env only; exec records
// ate_env.timed_out; failed file operations omit ate_env.file.size, as Env in
// Python does.
func TestTracingStartProcessFirstCommandTimedOutAndFailedFileSpans(t *testing.T) {
	f := newTracingFixture(t)
	f.serveGuest(t, "sp-1")
	f.serveGuest(t, "sp-2")
	rec := tracetest.NewSpanRecorder()
	c := f.client(t, env.ClientOptions{TracerProvider: sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))})

	ctx := t.Context()
	e1, err := c.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "sp-1"})
	if err != nil {
		t.Fatal(err)
	}
	// First command on e1 is StartProcess with an executable path, secret arg and env var.
	proc, err := e1.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"/bin/echo", "secret-arg"},
		Env:     map[string]string{"API_KEY": "s3cr3t"},
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if _, err := proc.Wait(ctx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	// A leading NAME=value word in Command makes the executable empty.
	_, _ = e1.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"API_KEY=s3cr3t", "/bin/echo", "secret-arg"},
	})
	// Second command on e1 is a timed-out Run: first_command must be absent, timed_out true.
	resp, err := e1.Run(ctx, env.ShellRequest{Command: "sleep 10", Timeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("timed-out Run: %v", err)
	}
	if !resp.TimedOut {
		t.Fatalf("resp.TimedOut = false, want true (%+v)", resp)
	}

	// Second environment e2: first Run gets first_command=true, second StartProcess does not.
	e2, err := c.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "sp-2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e2.Shell(ctx, "true"); err != nil {
		t.Fatal(err)
	}
	p2, err := e2.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	// Failed WriteFile and ReadFile on an environment with no guest, and an
	// early-closed ReadFile before EOF, omit ate_env.file.size.
	if err := e2.WriteFile(ctx, "partial.txt", strings.NewReader("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	partialRC, err := e2.ReadFile(ctx, "partial.txt")
	if err != nil {
		t.Fatal(err)
	}
	var head [3]byte
	if _, err := io.ReadFull(partialRC, head[:]); err != nil {
		t.Fatal(err)
	}
	if err := partialRC.Close(); err != nil {
		t.Fatal(err)
	}

	missing := c.Env("", "missing-guest")
	if err := missing.WriteFile(ctx, "x.txt", strings.NewReader("payload"), 0o644); err == nil {
		t.Fatal("WriteFile on missing guest succeeded")
	}
	if _, err := missing.ReadFile(ctx, "x.txt"); err == nil {
		t.Fatal("ReadFile on missing guest succeeded")
	}

	prefix := env.DefaultTraceServiceName + "."
	var spSpans, execSpans, wfSpans, rfSpans []sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.Name() {
		case prefix + "start_process":
			spSpans = append(spSpans, s)
		case prefix + "exec":
			execSpans = append(execSpans, s)
		case prefix + "write_file":
			wfSpans = append(wfSpans, s)
		case prefix + "read_file":
			rfSpans = append(rfSpans, s)
		}
	}
	if len(spSpans) != 3 {
		t.Fatalf("got %d start_process spans, want 3 (Run must not nest one)", len(spSpans))
	}
	if len(execSpans) != 2 {
		t.Fatalf("got %d exec spans, want 2", len(execSpans))
	}

	sp1 := attrs(spSpans[0])
	if spSpans[0].SpanKind() != trace.SpanKindInternal {
		t.Errorf("start_process kind = %s, want internal", spSpans[0].SpanKind())
	}
	if sp1[env.AttrActorName].AsString() != "sp-1" || sp1[env.AttrCommandExecutable].AsString() != "echo" {
		t.Errorf("sp-1 start_process attrs = %v, want actor sp-1 and executable echo", sp1)
	}
	if !sp1[env.AttrFirstCommand].AsBool() {
		t.Errorf("sp-1 first start_process missing ate_env.first_command=true: %v", sp1)
	}
	if _, hasExit := sp1[env.AttrExitCode]; hasExit {
		t.Errorf("start_process has ate_env.exit_code: %v", sp1)
	}
	for i, s := range spSpans[:2] {
		execAttr := attrs(s)[env.AttrCommandExecutable].AsString()
		if i == 0 && execAttr != "echo" {
			t.Errorf("start_process 0 executable = %q, want echo", execAttr)
		}
		if i == 1 && execAttr != "" {
			t.Errorf("start_process 1 executable = %q, want none", execAttr)
		}
		for _, kv := range s.Attributes() {
			if strings.Contains(kv.Value.Emit(), "s3cr3t") || strings.Contains(kv.Value.Emit(), "secret-arg") {
				t.Errorf("start_process leaked secret in %s=%q", kv.Key, kv.Value.Emit())
			}
		}
	}
	if _, ok := attrs(spSpans[1])[env.AttrFirstCommand]; ok {
		t.Errorf("second start_process on sp-1 has ate_env.first_command: %v", attrs(spSpans[1]))
	}

	// Third command on sp-1 (the timed-out Run): no first_command, timed_out=true.
	ex1 := attrs(execSpans[0])
	if _, ok := ex1[env.AttrFirstCommand]; ok {
		t.Errorf("later command on sp-1 has ate_env.first_command: %v", ex1)
	}
	if !ex1[env.AttrTimedOut].AsBool() {
		t.Errorf("timed-out exec ate_env.timed_out = %v, want true", ex1[env.AttrTimedOut])
	}

	// On sp-2: first Run has first_command=true, second StartProcess omits it.
	ex2 := attrs(execSpans[1])
	if !ex2[env.AttrFirstCommand].AsBool() {
		t.Errorf("first exec on sp-2 missing ate_env.first_command=true: %v", ex2)
	}
	sp2 := attrs(spSpans[2])
	if _, ok := sp2[env.AttrFirstCommand]; ok {
		t.Errorf("second command (start_process) on sp-2 has ate_env.first_command: %v", sp2)
	}

	if len(wfSpans) != 2 || len(rfSpans) != 2 {
		t.Fatalf("file spans = (%d, %d), want (2, 2)", len(wfSpans), len(rfSpans))
	}
	if got := attrs(wfSpans[0])[env.AttrFileSize].AsInt64(); got != int64(len("hello world")) {
		t.Errorf("complete write_file size = %d, want 11", got)
	}
	if _, ok := attrs(wfSpans[1])[env.AttrFileSize]; ok {
		t.Errorf("failed write_file recorded ate_env.file.size: %v", attrs(wfSpans[1]))
	}
	if _, ok := attrs(rfSpans[0])[env.AttrFileSize]; ok {
		t.Errorf("early-closed read_file recorded ate_env.file.size: %v", attrs(rfSpans[0]))
	}
	if _, ok := attrs(rfSpans[1])[env.AttrFileSize]; ok {
		t.Errorf("failed read_file recorded ate_env.file.size: %v", attrs(rfSpans[1]))
	}
}

func TestNonEmptyDropsEmptyStrings(t *testing.T) {
	got := env.NonEmptyForTest([]attribute.KeyValue{
		attribute.String("a", "b"),
		attribute.String("c", ""),
		attribute.Int("d", 1),
		attribute.String("e", "   "),
	})
	if len(got) != 3 {
		t.Errorf("nonEmpty = %d items (%v), want 3", len(got), got)
	}
}

func TestExecutableNeverRecordsAssignments(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		prefixes := []string{"", " ", "\n", "(", "{ ", ">out ", "2>&1 ", "<<<input ", "<<<${B:-my %s} "}
		names := []string{"API_KEY", "TOKEN", "A", "X+"}
		cmds := []string{"", " python x.py", " 'unterminated", " cmd"}
		// Values whose expansion holds a space, so the secret lands in a
		// later word than the assignment's own (test_telemetry_tracing.py
		// draws the same shapes).
		values := []string{"%s", `"$(echo "my %s")"`, "${B:-my %s}", "`echo %s x`", `$'a\' %s'`, `"my %s"`}

		prefixPattern := rapid.SampledFrom(prefixes).Draw(t, "prefix")
		name := rapid.SampledFrom(names).Draw(t, "name")
		secret := rapid.StringMatching(`^[a-zA-Z0-9_]{6,20}$`).Draw(t, "secret")
		value := fmt.Sprintf(rapid.SampledFrom(values).Draw(t, "value"), secret)
		cmd := rapid.SampledFrom(cmds).Draw(t, "cmd")

		prefix := prefixPattern
		if strings.Contains(prefixPattern, "%s") {
			prefix = fmt.Sprintf(prefixPattern, secret)
		}

		line := prefix + name + "=" + value + cmd
		got := env.ExecutableForTest(line)
		if strings.Contains(got, secret) || strings.Contains(got, "=") {
			t.Fatalf("executable(%q) = %q leaks the assignment", line, got)
		}
	})
}

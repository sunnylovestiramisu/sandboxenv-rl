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
	"io"
	"net"
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
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
// with env.* attributes, and ate-env-api's spans join the same trace.
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
		if got := attrs(s)[env.AttrEnvID].AsString(); got != "traced" {
			t.Errorf("%s env.id = %q, want traced", s.Name(), got)
		}
		if s.InstrumentationScope().Name != "ate_env_go_client" {
			t.Errorf("%s scope = %q, want ate_env_go_client", s.Name(), s.InstrumentationScope().Name)
		}
	}

	create := attrs(spans["ate-env-go-client.create_environment"])
	if create[env.AttrAtespace].AsString() != apiservice.DefaultAtespace || create[env.AttrTemplate].AsString() != apiservice.DefaultTemplate {
		t.Errorf("create_environment attributes = %v, want the resolved atespace and template", create)
	}
	if _, ok := create[env.AttrRootfsImage]; !ok {
		t.Error("create_environment lacks env.rootfs_image")
	}
	exec := attrs(spans["ate-env-go-client.exec"])
	if exec[env.AttrCommandExecutable].AsString() != "echo" || exec[env.AttrExitCode].AsInt64() != 0 {
		t.Errorf("exec attributes = %v, want executable echo and exit code 0", exec)
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
	found := false
	for _, s := range f.server.Ended() {
		if s.Name() != "ateenv.v1alpha.EnvironmentService/CreateEnvironment" {
			continue
		}
		found = true
		if s.SpanContext().TraceID() != traceID {
			t.Errorf("server span trace %s, want the client's %s", s.SpanContext().TraceID(), traceID)
		}
		if !clientSpanIDs[s.Parent().SpanID().String()] {
			t.Errorf("server span parent %s is not a client span", s.Parent().SpanID())
		}
	}
	if !found {
		t.Error("no server CreateEnvironment span")
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
	if _, err := c.Create(t.Context(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "own-conn"}); err != nil {
		t.Fatal(err)
	}
	create := spansByName(rec, env.DefaultTraceServiceName)["ate-env-go-client.create_environment"]
	if create == nil {
		t.Fatal("no create_environment span")
	}
	for _, s := range f.server.Ended() {
		if s.Name() == "ateenv.v1alpha.EnvironmentService/CreateEnvironment" {
			if s.Parent().SpanID() != create.SpanContext().SpanID() {
				t.Errorf("server parent %s, want the create_environment span %s", s.Parent().SpanID(), create.SpanContext().SpanID())
			}
			return
		}
	}
	t.Error("no server CreateEnvironment span")
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

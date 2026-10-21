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

package telemetry

import (
	"context"
	"encoding/json"
	"net"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func TestNoOp(t *testing.T) {
	inst := NewNoOp()
	ctx := context.Background()
	got, end := inst.StartSpan(ctx, "op", map[string]string{"k": "v"})
	end()
	if got != ctx {
		t.Error("NoOp StartSpan returned a new context")
	}
	inst.AddEvent(ctx, "ev", nil)
	if inst.IsRecording(ctx) {
		t.Error("NoOp IsRecording = true")
	}
	if tc := inst.TraceContext(ctx); tc != "" {
		t.Errorf("NoOp TraceContext = %q, want empty", tc)
	}
}

func newRecorder() (*tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	rec := tracetest.NewSpanRecorder()
	return rec, sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
}

func TestOTelInstrumenter(t *testing.T) {
	rec, tp := newRecorder()
	inst := newOTelInstrumenter(tp, "ate-env-api")
	ctx, end := inst.StartSpan(context.Background(), "parent", map[string]string{"ate.atespace": "ate-env"})
	if !inst.IsRecording(ctx) {
		t.Error("IsRecording = false inside a sampled span")
	}
	inst.AddEvent(ctx, "woke", map[string]string{"outcome": "woke"})

	var carrier map[string]string
	if err := json.Unmarshal([]byte(inst.TraceContext(ctx)), &carrier); err != nil {
		t.Fatalf("TraceContext is not JSON: %v", err)
	}
	sc := trace.SpanContextFromContext(ctx)
	if !strings.Contains(carrier["traceparent"], sc.TraceID().String()) || !strings.Contains(carrier["traceparent"], sc.SpanID().String()) {
		t.Errorf("traceparent %q does not name span %s/%s", carrier["traceparent"], sc.TraceID(), sc.SpanID())
	}
	if _, ok := carrier["baggage"]; ok {
		t.Error("carrier holds baggage; the propagator must be W3C Trace Context only")
	}
	end()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Name() != "parent" || s.InstrumentationScope().Name != "ate_env_api" {
		t.Errorf("span %q scope %q, want parent in scope ate_env_api", s.Name(), s.InstrumentationScope().Name)
	}
	if len(s.Attributes()) != 1 || string(s.Attributes()[0].Key) != "ate.atespace" {
		t.Errorf("attributes = %v", s.Attributes())
	}
	if len(s.Events()) != 1 || s.Events()[0].Name != "woke" {
		t.Errorf("events = %v", s.Events())
	}
}

func TestPropagatorIsTraceContextOnly(t *testing.T) {
	fields := Propagator.Fields()
	sort.Strings(fields)
	if strings.Join(fields, ",") != "traceparent,tracestate" {
		t.Errorf("propagator fields = %v, want traceparent and tracestate only", fields)
	}
}

// SetupOTel builds the exporter without connecting, installs the global
// provider and the W3C-only propagator, and shuts down cleanly.
func TestSetupOTel(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	inst, shutdown, err := SetupOTel(context.Background(), "ate-env-api")
	if err != nil {
		t.Fatalf("SetupOTel: %v", err)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("global tracer provider is %T, want the SDK provider", otel.GetTracerProvider())
	}
	fields := otel.GetTextMapPropagator().Fields()
	sort.Strings(fields)
	if strings.Join(fields, ",") != "traceparent,tracestate" {
		t.Errorf("global propagator fields = %v, want traceparent and tracestate only", fields)
	}
	// The span is left open: an ended span would make shutdown try to export
	// it to the unreachable endpoint until its timeout.
	ctx, _ := inst.StartSpan(context.Background(), "op", nil)
	if !inst.IsRecording(ctx) {
		t.Error("span from SetupOTel's instrumenter is not recording")
	}
	shutdown()
}

// A server with the otelgrpc server handler continues the trace a client
// sends as a traceparent in gRPC metadata, the way the Python client's
// interceptors send it: the server span has the client's trace id and the
// client span as its parent.
func TestServerContinuesClientTraceparent(t *testing.T) {
	rec, tp := newRecorder()
	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler(
		otelgrpc.WithTracerProvider(tp), otelgrpc.WithPropagators(Propagator))))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	// The client's span, from a provider of its own (another process).
	_, clientTP := newRecorder()
	clientCtx, clientSpan := clientTP.Tracer("client").Start(context.Background(), "ate_env_client.create_environment")
	carrier := propagation.MapCarrier{}
	Propagator.Inject(clientCtx, carrier)
	if carrier["traceparent"] == "" {
		t.Fatal("no traceparent injected")
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "traceparent", carrier["traceparent"])
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Check: %v", err)
	}
	clientSpan.End()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("server recorded %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Name() != "grpc.health.v1.Health/Check" {
		t.Errorf("server span name = %q", s.Name())
	}
	want := clientSpan.SpanContext()
	if s.SpanContext().TraceID() != want.TraceID() {
		t.Errorf("server trace id %s, want the client's %s", s.SpanContext().TraceID(), want.TraceID())
	}
	if s.Parent().SpanID() != want.SpanID() || !s.Parent().IsRemote() {
		t.Errorf("server parent span %s (remote %v), want the client's %s", s.Parent().SpanID(), s.Parent().IsRemote(), want.SpanID())
	}
}

// Untraced gives ctx a valid, remote, unsampled parent: spans under it do
// not record, and the propagated traceparent says not sampled.
func TestUntraced(t *testing.T) {
	rec, tp := newRecorder()
	ctx := Untraced(context.Background())
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() || !sc.IsRemote() || sc.IsSampled() {
		t.Fatalf("span context %+v, want valid, remote, unsampled", sc)
	}
	_, span := tp.Tracer("t").Start(ctx, "child")
	if span.IsRecording() {
		t.Error("child of an Untraced context is recording")
	}
	span.End()
	if n := len(rec.Ended()); n != 0 {
		t.Errorf("recorded %d spans, want 0", n)
	}
	carrier := propagation.MapCarrier{}
	Propagator.Inject(ctx, carrier)
	if !strings.HasSuffix(carrier["traceparent"], "-00") {
		t.Errorf("traceparent %q, want the unsampled flag 00", carrier["traceparent"])
	}
}

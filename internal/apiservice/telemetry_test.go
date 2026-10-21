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

package apiservice_test

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/guest"
	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/internaltest/fakerouter"
	"github.com/agent-substrate/env/internal/telemetry"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// telemetryEnv is newFullTestEnv with the API server wired as ate-env-api
// wires it when --metrics-listen-addr is set and, if tracing, when
// --enable-tracing is: metrics interceptors, the otelgrpc server handler and
// a tracing ate.Client.
type telemetryEnv struct {
	*testEnv
	metrics *telemetry.Metrics
}

func newTelemetryEnv(t *testing.T, tracing bool) *telemetryEnv {
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

	client, err := ate.New(ate.Options{ControlAddr: controlAddr, RouterAddr: routerAddr, SkipVerify: true, EnableTracing: tracing})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

	metrics := telemetry.NewMetrics()
	srv := apiservice.New(client, routerAddr, "")
	srv.Metrics = metrics
	t.Cleanup(srv.Close)

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(metrics.StreamServerInterceptor()),
	}
	if tracing {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}
	grpcServer := grpc.NewServer(opts...)
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, srv)
	ateenvv1alpha.RegisterProcessServiceServer(grpcServer, srv)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcServer, srv)
	metrics.InitMethods(grpcServer.GetServiceInfo())
	metrics.InitAtespace(apiservice.DefaultAtespace)
	metrics.SetEnvironmentSource(srv.EnvironmentCounts)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return &telemetryEnv{
		testEnv: &testEnv{
			envClient:  ateenvv1alpha.NewEnvironmentServiceClient(conn),
			procClient: ateenvv1alpha.NewProcessServiceClient(conn),
			fsClient:   ateenvv1alpha.NewFileSystemServiceClient(conn),
			conn:       conn,
			router:     router,
			control:    control,
			srv:        srv,
		},
		metrics: metrics,
	}
}

// histogram returns the sample count and sum of the series of family name
// whose labels include labels, or -1 when absent.
func histogram(t *testing.T, m *telemetry.Metrics, name string, labels map[string]string) (uint64, float64) {
	t.Helper()
	mfs, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if v, ok := labels[lp.GetName()]; ok && v != lp.GetValue() {
					continue next
				}
			}
			if h := metric.GetHistogram(); h != nil {
				return h.GetSampleCount(), h.GetSampleSum()
			}
			return uint64(metric.GetCounter().GetValue()), metric.GetCounter().GetValue()
		}
	}
	return 0, -1
}

func serveGuest(t *testing.T, te *telemetryEnv, id string, wrap func(http.Handler) http.Handler) context.Context {
	t.Helper()
	srv, cleanup, err := guest.NewServer(guest.Config{Workspace: t.TempDir(), EnableProcess: true, EnableFileSystem: true})
	if err != nil {
		t.Fatalf("guest.NewServer: %v", err)
	}
	t.Cleanup(cleanup)
	var h http.Handler = srv
	if wrap != nil {
		h = wrap(h)
	}
	te.router.Register(id, h)
	return metadata.AppendToOutgoingContext(context.Background(), "x-env-id", id, "x-env-atespace", apiservice.DefaultAtespace)
}

// create_to_ready starts at the client's x-ate-env-requested-at and ends
// when the guest answers its first request; later calls do not observe it
// again. The request counter moves for CreateEnvironment.
func TestCreateToReadyUsesRequestedAtHeader(t *testing.T) {
	te := newTelemetryEnv(t, false)
	requested := time.Now().Add(-2 * time.Second)
	ctx := metadata.AppendToOutgoingContext(context.Background(), telemetry.RequestedAtHeader, requested.Format(time.RFC3339Nano))
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "ready-1"}); err != nil {
		t.Fatal(err)
	}
	ok := map[string]string{"atespace": apiservice.DefaultAtespace, "status": telemetry.ReadyStatusOK}
	if n, _ := histogram(t, te.metrics, telemetry.MetricCreateToReadySeconds, ok); n != 0 {
		t.Fatalf("create_to_ready observed %d before the guest answered", n)
	}

	envCtx := serveGuest(t, te, "ready-1", nil)
	for range 2 {
		if _, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}}); err != nil {
			t.Fatal(err)
		}
	}
	n, sum := histogram(t, te.metrics, telemetry.MetricCreateToReadySeconds, ok)
	if n != 1 {
		t.Fatalf("create_to_ready{ok} count = %d, want 1", n)
	}
	if sum < 2 || sum > 30 {
		t.Errorf("create_to_ready{ok} = %.3fs, want about 2s from the requested-at header", sum)
	}
	create := map[string]string{"method": "/ateenv.v1alpha.EnvironmentService/CreateEnvironment", "code": "OK"}
	if n, _ := histogram(t, te.metrics, telemetry.MetricRequestsTotal, create); n != 1 {
		t.Errorf("requests_total{CreateEnvironment,OK} = %d, want 1", n)
	}
	start := map[string]string{"method": "/ateenv.v1alpha.ProcessService/StartProcess", "code": "OK"}
	if n, _ := histogram(t, te.metrics, telemetry.MetricRequestDurationSeconds, start); n != 2 {
		t.Errorf("request_duration_seconds_count{StartProcess,OK} = %d, want 2", n)
	}
}

// Without the header, or with one that does not parse, the clock starts at
// receipt: the duration is short.
func TestCreateToReadyWithoutHeaderStartsAtReceipt(t *testing.T) {
	te := newTelemetryEnv(t, false)
	ctx := metadata.AppendToOutgoingContext(context.Background(), telemetry.RequestedAtHeader, "yesterday")
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "ready-2"}); err != nil {
		t.Fatal(err)
	}
	envCtx := serveGuest(t, te, "ready-2", nil)
	if _, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	n, sum := histogram(t, te.metrics, telemetry.MetricCreateToReadySeconds, map[string]string{"status": telemetry.ReadyStatusOK})
	if n != 1 || sum > 2 {
		t.Errorf("create_to_ready{ok} = %d samples, %.3fs; want 1 sample from receipt time", n, sum)
	}
}

// A failed create observes status error; a delete before the guest ever
// answered observes status deleted.
func TestCreateToReadyErrorAndDeleted(t *testing.T) {
	te := newTelemetryEnv(t, false)
	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "bad", RootfsImage: "python:3.12"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("create with an unpinned image: %v, want InvalidArgument", err)
	}
	if n, _ := histogram(t, te.metrics, telemetry.MetricCreateToReadySeconds, map[string]string{"status": telemetry.ReadyStatusError}); n != 1 {
		t.Errorf("create_to_ready{error} count = %d, want 1", n)
	}
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "gone"}); err != nil {
		t.Fatal(err)
	}
	if _, err := te.envClient.DeleteEnvironment(ctx, &ateenvv1alpha.DeleteEnvironmentRequest{Id: "gone"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := histogram(t, te.metrics, telemetry.MetricCreateToReadySeconds, map[string]string{"status": telemetry.ReadyStatusDeleted}); n != 1 {
		t.Errorf("create_to_ready{deleted} count = %d, want 1", n)
	}
}

// Each wake outcome is observed once per waiting call; a call to an awake
// guest is not a wait.
func TestWakeWaitOutcomes(t *testing.T) {
	te := newTelemetryEnv(t, false)
	ctx := context.Background()
	for _, id := range []string{"awake", "woke", "timeout", "crash"} {
		if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: id}); err != nil {
			t.Fatal(err)
		}
	}
	count := func(outcome string) uint64 {
		n, _ := histogram(t, te.metrics, telemetry.MetricWakeWaitSeconds, map[string]string{"outcome": outcome})
		return n
	}
	run := func(id string) error {
		envCtx := serveGuest(t, te, id, nil)
		_, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}})
		return err
	}

	if err := run("awake"); err != nil {
		t.Fatal(err)
	}
	if n := count(telemetry.WakeOutcomeWoke) + count(telemetry.WakeOutcomeTimeout) + count(telemetry.WakeOutcomeError); n != 0 {
		t.Errorf("a call to an awake guest observed %d wake waits", n)
	}

	te.control.SetState("woke", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	go func() {
		time.Sleep(500 * time.Millisecond)
		te.control.SetState("woke", ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}()
	if err := run("woke"); err != nil {
		t.Fatal(err)
	}
	if n, sum := histogram(t, te.metrics, telemetry.MetricWakeWaitSeconds, map[string]string{"outcome": telemetry.WakeOutcomeWoke}); n != 1 || sum < 0.4 {
		t.Errorf("wake_wait{woke} = %d samples, %.3fs; want 1 of about 0.5s", n, sum)
	}

	te.control.SetState("timeout", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	te.srv.WakeBudget = time.Second
	if err := run("timeout"); status.Code(err) != codes.Unavailable {
		t.Fatalf("suspended past the budget: %v, want Unavailable", err)
	}
	if n := count(telemetry.WakeOutcomeTimeout); n != 1 {
		t.Errorf("wake_wait{timeout} count = %d, want 1", n)
	}

	te.control.SetState("crash", ateapipb.ActorState_ACTOR_STATE_CRASHED)
	if err := run("crash"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("crashed: %v, want FailedPrecondition", err)
	}
	if n := count(telemetry.WakeOutcomeError); n != 1 {
		t.Errorf("wake_wait{error} count = %d, want 1", n)
	}
	if n, _ := histogram(t, te.metrics, telemetry.MetricCreateToReadySeconds, map[string]string{"status": telemetry.ReadyStatusCrashed}); n != 1 {
		t.Errorf("create_to_ready{crashed} count = %d, want 1", n)
	}
}

// EnvironmentCounts reads the control plane: every state of every known
// atespace is reported, zero included.
func TestEnvironmentCounts(t *testing.T) {
	te := newTelemetryEnv(t, false)
	ctx := context.Background()
	for _, id := range []string{"e1", "e2", "e3"} {
		if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "t1", Atespace: "team-a"}); err != nil {
		t.Fatal(err)
	}
	te.control.SetState("e3", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	counts, err := te.srv.EnvironmentCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, c := range counts {
		got[c.Atespace+"/"+c.State] = c.Count
	}
	want := map[string]int{"ate-env/running": 2, "ate-env/suspended": 1, "ate-env/crashed": 0, "team-a/running": 1, "team-a/suspended": 0}
	for k, v := range want {
		if n, ok := got[k]; !ok || n != v {
			t.Errorf("%s = %d (present %v), want %d", k, n, ok, v)
		}
	}
	if len(counts) != 14 {
		t.Errorf("got %d samples, want 7 states x 2 atespaces", len(counts))
	}
}

// With tracing on, the API server continues the caller's trace (server span
// parented on the client span carried in traceparent), and passes it on to
// ateapi and, through the wake probe and the proxied call, to the guest.
func TestTracingContinuesClientTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(telemetry.Propagator)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	te := newTelemetryEnv(t, true)

	// The client's span, from a provider of its own (another process).
	clientTP := sdktrace.NewTracerProvider()
	clientCtx, clientSpan := clientTP.Tracer("client").Start(context.Background(), "ate_env_client.create_environment")
	defer clientSpan.End()
	carrier := propagation.MapCarrier{}
	telemetry.Propagator.Inject(clientCtx, carrier)
	traceID := clientSpan.SpanContext().TraceID()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "traceparent", carrier["traceparent"])
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "traced"}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var guestTraceparents []string
	envCtx := serveGuest(t, te, "traced", func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			guestTraceparents = append(guestTraceparents, r.Header.Get("traceparent"))
			mu.Unlock()
			h.ServeHTTP(w, r)
		})
	})
	envCtx = metadata.AppendToOutgoingContext(envCtx, "traceparent", carrier["traceparent"])
	if _, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}

	var createSpan, startSpan sdktrace.ReadOnlySpan
	sawCreateActor := false
	for _, s := range rec.Ended() {
		switch s.Name() {
		case "ateenv.v1alpha.EnvironmentService/CreateEnvironment":
			createSpan = s
		case "ateenv.v1alpha.ProcessService/StartProcess":
			startSpan = s
		}
		if strings.HasSuffix(s.Name(), "/CreateActor") && s.SpanContext().TraceID() == traceID {
			sawCreateActor = true
		}
	}
	if createSpan == nil || startSpan == nil {
		t.Fatalf("server spans missing; recorded %d spans", len(rec.Ended()))
	}
	for _, s := range []sdktrace.ReadOnlySpan{createSpan, startSpan} {
		if s.SpanContext().TraceID() != traceID {
			t.Errorf("%s trace id %s, want the client's %s", s.Name(), s.SpanContext().TraceID(), traceID)
		}
		if s.Parent().SpanID() != clientSpan.SpanContext().SpanID() {
			t.Errorf("%s parent %s, want the client span %s", s.Name(), s.Parent().SpanID(), clientSpan.SpanContext().SpanID())
		}
	}
	if !sawCreateActor {
		t.Error("no ateapi CreateActor client span in the client's trace")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(guestTraceparents) < 2 {
		t.Fatalf("guest saw %d requests, want the wake probe and the call", len(guestTraceparents))
	}
	for i, tpHeader := range guestTraceparents {
		if !strings.Contains(tpHeader, traceID.String()) {
			t.Errorf("guest request %d traceparent %q, want trace %s", i, tpHeader, traceID)
		}
	}
}

// With tracing off (the default), nothing is propagated to the guest.
func TestTracingOffSendsNoTraceparent(t *testing.T) {
	te := newTelemetryEnv(t, false)
	if _, err := te.envClient.CreateEnvironment(context.Background(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "plain"}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var seen []string
	envCtx := serveGuest(t, te, "plain", func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen = append(seen, r.Header.Get("traceparent"))
			mu.Unlock()
			h.ServeHTTP(w, r)
		})
	})
	envCtx = metadata.AppendToOutgoingContext(envCtx, "traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if _, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, h := range seen {
		if h != "" {
			t.Errorf("guest request %d carried traceparent %q with tracing off", i, h)
		}
	}
}

// The environments gauge reads the control plane on every scrape; with
// tracing on, those reads record no span (no root trace per scrape), while
// a traced request still does.
func TestEnvironmentsScrapeIsUntraced(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(telemetry.Propagator)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	te := newTelemetryEnv(t, true)

	mfs, err := te.metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	gotEnv := false
	for _, mf := range mfs {
		gotEnv = gotEnv || mf.GetName() == telemetry.MetricEnvironments
	}
	if !gotEnv {
		t.Fatal("scrape did not read the environments gauge")
	}
	if spans := rec.Ended(); len(spans) != 0 {
		names := make([]string, len(spans))
		for i, s := range spans {
			names[i] = s.Name()
		}
		t.Errorf("a scrape recorded spans %v, want none", names)
	}

	if _, err := te.envClient.CreateEnvironment(context.Background(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "after-scrape"}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range rec.Ended() {
		found = found || s.Name() == "ateenv.v1alpha.EnvironmentService/CreateEnvironment"
	}
	if !found {
		t.Error("a traced CreateEnvironment recorded no server span")
	}
}

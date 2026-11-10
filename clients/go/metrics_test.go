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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/clients/go"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var wantClientBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}

func gatherMap(t *testing.T, g prometheus.Gatherer) map[string]*dto.MetricFamily {
	t.Helper()
	fams, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	m := make(map[string]*dto.MetricFamily, len(fams))
	for _, f := range fams {
		m[f.GetName()] = f
	}
	return m
}

func histCount(mf *dto.MetricFamily, labels map[string]string) uint64 {
	if mf == nil {
		return 0
	}
	for _, m := range mf.GetMetric() {
		if len(m.GetLabel()) != len(labels) {
			continue
		}
		match := true
		for _, lp := range m.GetLabel() {
			if v, ok := labels[lp.GetName()]; !ok || v != lp.GetValue() {
				match = false
				break
			}
		}
		if match {
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func bucketBounds(m *dto.Metric) []float64 {
	var out []float64
	for _, b := range m.GetHistogram().GetBucket() {
		out = append(out, b.GetUpperBound())
	}
	return out
}

// AC-G6: with a prometheus.NewRegistry(), Run and Suspend produce the three
// documented families with the shared buckets and labels, and
// ready_to_first_command is observed once per Env.
func TestMetricsRegistererRecordsDocumentedFamiliesAndBuckets(t *testing.T) {
	f := newTracingFixture(t)
	f.serveGuest(t, "met-env")
	reg := prometheus.NewRegistry()
	c := f.client(t, env.ClientOptions{MetricsRegisterer: reg})

	ctx := t.Context()
	e, err := c.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "met-env"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := e.Shell(ctx, "echo one"); err != nil {
		t.Fatalf("first Shell: %v", err)
	}
	if _, err := e.Shell(ctx, "echo two"); err != nil {
		t.Fatalf("second Shell: %v", err)
	}
	if err := e.Suspend(ctx); err != nil {
		t.Fatalf("Suspend: %v", err)
	}

	got := gatherMap(t, reg)
	wantHelp := map[string]string{
		"ate_env_client_exec_seconds":                   "Time to run a command in an environment",
		"ate_env_client_suspend_seconds":                "Time to suspend an environment",
		"ate_env_client_ready_to_first_command_seconds": "Time from environment acquire ready until the first command begins",
	}
	if len(got) != len(wantHelp) {
		t.Fatalf("gathered %d families (%v), want %d", len(got), got, len(wantHelp))
	}
	for name, help := range wantHelp {
		mf, ok := got[name]
		if !ok {
			t.Errorf("missing family %s", name)
			continue
		}
		if mf.GetType() != dto.MetricType_HISTOGRAM {
			t.Errorf("%s type = %v, want HISTOGRAM", name, mf.GetType())
		}
		if mf.GetHelp() != help {
			t.Errorf("%s help = %q, want %q", name, mf.GetHelp(), help)
		}
		for _, m := range mf.GetMetric() {
			if b := bucketBounds(m); !slices.Equal(b, wantClientBuckets) {
				t.Errorf("%s buckets = %v, want %v", name, b, wantClientBuckets)
			}
		}
	}

	if n := histCount(got["ate_env_client_exec_seconds"], map[string]string{"status": "ok"}); n != 2 {
		t.Errorf("exec_seconds{status=ok} count = %d, want 2", n)
	}
	if n := histCount(got["ate_env_client_suspend_seconds"], map[string]string{"status": "ok"}); n != 1 {
		t.Errorf("suspend_seconds{status=ok} count = %d, want 1", n)
	}
	if n := histCount(got["ate_env_client_ready_to_first_command_seconds"], map[string]string{"status": "ok", "source": "direct"}); n != 1 {
		t.Errorf("ready_to_first_command{status=ok,source=direct} count = %d, want 1", n)
	}
}

// AC-G6: without MetricsRegisterer nothing is registered on a private or the
// default registry.
func TestMetricsOffWithoutRegisterer(t *testing.T) {
	f := newTracingFixture(t)
	f.serveGuest(t, "no-metrics")
	c := f.client(t, env.ClientOptions{})
	exercise(t, c, "no-metrics")

	for name := range gatherMap(t, prometheus.DefaultGatherer) {
		if strings.HasPrefix(name, "ate_env_client_") {
			t.Errorf("default gatherer has %s when MetricsRegisterer is nil", name)
		}
	}
}

func TestMetricsStatusLabelsOkErrorAndCancelled(t *testing.T) {
	f := newTracingFixture(t)
	f.serveGuest(t, "status-env")
	reg := prometheus.NewRegistry()
	c := f.client(t, env.ClientOptions{MetricsRegisterer: reg})
	env.SetPollBoundsForTest(c, 5*time.Millisecond, 10*time.Millisecond)

	ctx := t.Context()
	e, err := c.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "status-env"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Non-zero command exit still completed execution, so status is "ok", as in Python.
	if resp, err := e.Shell(ctx, "exit 7"); err != nil || resp.ExitCode != 7 {
		t.Fatalf("nonzero Shell: %v %+v", err, resp)
	}
	// Cancelled Run (polled and streamed).
	cctx, cancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	if _, err := e.Run(cctx, env.ShellRequest{Command: "sleep 30", Poll: true}); err == nil {
		t.Fatal("cancelled polled Run succeeded")
	}
	srunCtx, srunCancel := context.WithCancel(ctx)
	go func() {
		time.Sleep(30 * time.Millisecond)
		srunCancel()
	}()
	if _, err := e.Run(srunCtx, env.ShellRequest{Command: "sleep 30", Poll: false}); err == nil {
		t.Fatal("cancelled streamed Run succeeded")
	}
	// Failed Run on a missing environment.
	if _, err := c.Env("", "missing-env").Shell(ctx, "echo hi"); err == nil {
		t.Fatal("missing env Shell succeeded")
	}
	// Failed Suspend on a missing environment, and cancelled Suspend.
	if err := c.Suspend(ctx, "", "missing-env"); err == nil {
		t.Fatal("missing env Suspend succeeded")
	}
	sctx, scancel := context.WithCancel(ctx)
	scancel()
	if err := e.Suspend(sctx); err == nil {
		t.Fatal("cancelled Suspend succeeded")
	}
	if err := e.SuspendLocal(ctx); err != nil {
		t.Fatalf("SuspendLocal: %v", err)
	}

	got := gatherMap(t, reg)
	if n := histCount(got["ate_env_client_exec_seconds"], map[string]string{"status": "ok"}); n != 1 {
		t.Errorf("exec ok = %d, want 1", n)
	}
	if n := histCount(got["ate_env_client_exec_seconds"], map[string]string{"status": "cancelled"}); n != 2 {
		t.Errorf("exec cancelled = %d, want 2", n)
	}
	if n := histCount(got["ate_env_client_exec_seconds"], map[string]string{"status": "error"}); n != 1 {
		t.Errorf("exec error = %d, want 1", n)
	}
	if n := histCount(got["ate_env_client_suspend_seconds"], map[string]string{"status": "ok"}); n != 1 {
		t.Errorf("suspend ok = %d, want 1", n)
	}
	if n := histCount(got["ate_env_client_suspend_seconds"], map[string]string{"status": "error"}); n != 1 {
		t.Errorf("suspend error = %d, want 1", n)
	}
	if n := histCount(got["ate_env_client_suspend_seconds"], map[string]string{"status": "cancelled"}); n != 1 {
		t.Errorf("suspend cancelled = %d, want 1", n)
	}
}

// D7 / Risk 2: two Clients sharing one registry reuse the collectors; a
// conflicting collector type or label set returns an error from NewClient.
func TestMetricsIdempotentRegistrationAndConflictErrors(t *testing.T) {
	f := newTracingFixture(t)
	f.serveGuest(t, "shared-1")
	f.serveGuest(t, "shared-2")
	reg := prometheus.NewRegistry()

	c1 := f.client(t, env.ClientOptions{MetricsRegisterer: reg})
	c2 := f.client(t, env.ClientOptions{MetricsRegisterer: reg})

	ctx := t.Context()
	e1, err := c1.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "shared-1"})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := c2.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "shared-2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e1.Shell(ctx, "true"); err != nil {
		t.Fatal(err)
	}
	if _, err := e2.Shell(ctx, "true"); err != nil {
		t.Fatal(err)
	}
	got := gatherMap(t, reg)
	if n := histCount(got["ate_env_client_exec_seconds"], map[string]string{"status": "ok"}); n != 2 {
		t.Errorf("shared exec ok = %d, want 2", n)
	}
	if n := histCount(got["ate_env_client_ready_to_first_command_seconds"], map[string]string{"status": "ok", "source": "direct"}); n != 2 {
		t.Errorf("shared ready_to_first_command = %d, want 2 (once per Env)", n)
	}

	// Wrong collector type with identical descriptor (same name and labels).
	badTypeReg := prometheus.NewRegistry()
	badTypeReg.MustRegister(prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ate_env_client_exec_seconds",
		Help: "Time to run a command in an environment",
	}, []string{"status"}))
	if _, err := env.NewClient(env.ClientOptions{Endpoint: f.addr, MetricsRegisterer: badTypeReg}); err == nil {
		t.Error("NewClient succeeded when ate_env_client_exec_seconds was registered as CounterVec")
	}

	// Conflicting variable labels on the same metric name.
	badLabelReg := prometheus.NewRegistry()
	badLabelReg.MustRegister(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ate_env_client_exec_seconds",
		Help: "other",
	}, []string{"wrong_label"}))
	if _, err := env.NewClient(env.ClientOptions{Endpoint: f.addr, MetricsRegisterer: badLabelReg}); err == nil {
		t.Error("NewClient succeeded when ate_env_client_exec_seconds had conflicting labels")
	}

	// Reversed variable label order (same sorted label set in prometheus.Desc).
	swappedOrderReg := prometheus.NewRegistry()
	swappedOrderReg.MustRegister(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ate_env_client_ready_to_first_command_seconds",
		Help:    "Time from environment acquire ready until the first command begins",
		Buckets: wantClientBuckets,
	}, []string{"source", "status"}))
	if _, err := env.NewClient(env.ClientOptions{Endpoint: f.addr, MetricsRegisterer: swappedOrderReg}); err == nil {
		t.Error("NewClient succeeded when ready_to_first_command had reversed label order")
	}

	// Different histogram buckets on an existing HistogramVec, and rollback of earlier registrations.
	badBucketsReg := prometheus.NewRegistry()
	badBucketsReg.MustRegister(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ate_env_client_suspend_seconds",
		Help:    "Time to suspend an environment",
		Buckets: []float64{1.0},
	}, []string{"status"}))
	if _, err := env.NewClient(env.ClientOptions{Endpoint: f.addr, MetricsRegisterer: badBucketsReg}); err == nil {
		t.Error("NewClient succeeded when suspend_seconds had different buckets")
	}
	// Earlier exec_seconds registration must have been rolled back on failure.
	probeExec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ate_env_client_exec_seconds",
		Help:    "Time to run a command in an environment",
		Buckets: wantClientBuckets,
	}, []string{"status"})
	if err := badBucketsReg.Register(probeExec); err != nil {
		t.Errorf("exec_seconds remained registered after failed NewClient: %v", err)
	}
}

func TestMetricsFirstCommandOnceUnderConcurrentStartProcessAndRun(t *testing.T) {
	f := newTracingFixture(t)
	f.serveGuest(t, "conc-first")
	reg := prometheus.NewRegistry()
	rec := tracetest.NewSpanRecorder()
	c := f.client(t, env.ClientOptions{
		MetricsRegisterer: reg,
		TracerProvider:    sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)),
	})

	ctx := t.Context()
	e, err := c.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "conc-first"})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				p, err := e.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}})
				if err != nil {
					t.Errorf("StartProcess: %v", err)
					return
				}
				if _, err := p.Wait(ctx); err != nil {
					t.Errorf("Wait: %v", err)
				}
			} else if _, err := e.Shell(ctx, "true"); err != nil {
				t.Errorf("Shell: %v", err)
			}
		}(i)
	}
	wg.Wait()

	got := gatherMap(t, reg)
	if n := histCount(got["ate_env_client_ready_to_first_command_seconds"], map[string]string{"status": "ok", "source": "direct"}); n != 1 {
		t.Errorf("ready_to_first_command count = %d, want 1", n)
	}

	prefix := env.DefaultTraceServiceName + "."
	var cmdSpans []sdktrace.ReadOnlySpan
	var firstSpan sdktrace.ReadOnlySpan
	firstCount := 0
	for _, s := range rec.Ended() {
		if s.Name() != prefix+"exec" && s.Name() != prefix+"start_process" {
			continue
		}
		cmdSpans = append(cmdSpans, s)
		if v, ok := attrs(s)[env.AttrFirstCommand]; ok && v.AsBool() {
			firstCount++
			firstSpan = s
		}
	}
	if len(cmdSpans) != 8 {
		t.Fatalf("got %d command spans, want 8", len(cmdSpans))
	}
	if firstCount != 1 {
		t.Fatalf("got %d spans with ate_env.first_command=true, want 1", firstCount)
	}
	for _, s := range cmdSpans {
		if s.StartTime().Before(firstSpan.StartTime()) {
			t.Errorf("span %s started at %v before first_command span at %v", s.Name(), s.StartTime(), firstSpan.StartTime())
		}
	}
}

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
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func wantFamilies() []string {
	var names []string
	for _, d := range Definitions {
		names = append(names, d.Name)
	}
	sort.Strings(names)
	return names
}

// scrapeFamilies returns the sorted ate_env_api_* family names of the
// samples /metrics serves, the way AC-6 reads them.
func scrapeFamilies(t *testing.T, h http.Handler) []string {
	t.Helper()
	ts := httptest.NewServer(h)
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	seen := map[string]bool{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "ate_env_api_") {
			continue
		}
		name := strings.FieldsFunc(line, func(r rune) bool { return r == '{' || r == ' ' })[0]
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if base, ok := strings.CutSuffix(name, suffix); ok && strings.HasSuffix(base, "_seconds") {
				name = base
			}
		}
		seen[name] = true
	}
	var names []string
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func newHealthServer(t *testing.T, m *Metrics) *grpc.ClientConn {
	t.Helper()
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(m.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(m.StreamServerInterceptor()))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	m.InitMethods(srv.GetServiceInfo())
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
	return conn
}

// /metrics serves exactly the documented families, before any request,
// once the methods, the default atespace and the environment source are
// initialized as ate-env-api does at startup.
func TestMetricsServesExactlyTheDocumentedFamilies(t *testing.T) {
	m := NewMetrics()
	newHealthServer(t, m)
	m.InitAtespace("ate-env")
	m.SetEnvironmentSource(func(context.Context) ([]EnvironmentCount, error) {
		return []EnvironmentCount{{Atespace: "ate-env", State: "running", Count: 0}}, nil
	})
	got := scrapeFamilies(t, m.Handler())
	if want := wantFamilies(); !reflect.DeepEqual(got, want) {
		t.Errorf("/metrics families = %v, want %v", got, want)
	}
	t.Logf("/metrics families: %v", got)
}

// Every family registers with exactly the labels and buckets Definitions
// documents, and a histogram uses the one seconds bucket set.
func TestMetricsLabelsMatchDefinitions(t *testing.T) {
	m := NewMetrics()
	m.InitMethods(map[string]grpc.ServiceInfo{"pkg.Svc": {Methods: []grpc.MethodInfo{{Name: "M"}}}})
	m.InitAtespace("ate-env")
	m.SetEnvironmentSource(func(context.Context) ([]EnvironmentCount, error) {
		return []EnvironmentCount{{Atespace: "ate-env", State: "running", Count: 1}}, nil
	})
	mfs, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*dto.MetricFamily{}
	for _, mf := range mfs {
		byName[mf.GetName()] = mf
	}
	for _, d := range Definitions {
		mf, ok := byName[d.Name]
		if !ok {
			t.Errorf("%s not gathered", d.Name)
			continue
		}
		if strings.ToLower(mf.GetType().String()) != d.Type {
			t.Errorf("%s type = %s, want %s", d.Name, mf.GetType(), d.Type)
		}
		var labels []string
		for _, lp := range mf.GetMetric()[0].GetLabel() {
			labels = append(labels, lp.GetName())
		}
		want := slices.Clone(d.Labels)
		sort.Strings(want)
		if !reflect.DeepEqual(labels, want) {
			t.Errorf("%s labels = %v, want %v", d.Name, labels, want)
		}
		for _, l := range labels {
			if slices.Contains([]string{"env_id", "id", "actor", "instance", "image", "run_id", "path"}, l) {
				t.Errorf("%s has unbounded label %q", d.Name, l)
			}
		}
		if d.Type == TypeHistogram {
			if !reflect.DeepEqual(d.Buckets, SecondsBuckets) {
				t.Errorf("%s buckets = %v, want SecondsBuckets", d.Name, d.Buckets)
			}
			var bounds []float64
			for _, b := range mf.GetMetric()[0].GetHistogram().GetBucket() {
				bounds = append(bounds, b.GetUpperBound())
			}
			if !reflect.DeepEqual(bounds, SecondsBuckets) {
				t.Errorf("%s registered buckets = %v, want %v", d.Name, bounds, SecondsBuckets)
			}
		}
	}
}

func counterValue(t *testing.T, m *Metrics, name string, labels map[string]string) float64 {
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
				return float64(h.GetSampleCount())
			}
			return metric.GetCounter().GetValue()
		}
	}
	return -1
}

// The interceptor counts and times each call by full method and code.
func TestRequestInterceptor(t *testing.T) {
	m := NewMetrics()
	conn := newHealthServer(t, m)
	const method = "/grpc.health.v1.Health/Check"
	ok := map[string]string{"method": method, "code": "OK"}
	if got := counterValue(t, m, MetricRequestsTotal, ok); got != 0 {
		t.Fatalf("requests_total before any call = %v, want a pre-initialized 0", got)
	}
	client := healthpb.NewHealthClient(conn)
	if _, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "missing"}); err == nil {
		t.Fatal("Check of an unknown service succeeded")
	}
	if got := counterValue(t, m, MetricRequestsTotal, ok); got != 1 {
		t.Errorf("requests_total{code=OK} = %v, want 1", got)
	}
	notFound := map[string]string{"method": method, "code": "NotFound"}
	if got := counterValue(t, m, MetricRequestsTotal, notFound); got != 1 {
		t.Errorf("requests_total{code=NotFound} = %v, want 1", got)
	}
	if got := counterValue(t, m, MetricRequestDurationSeconds, ok); got != 1 {
		t.Errorf("request_duration_seconds_count{code=OK} = %v, want 1", got)
	}

	// A streaming call goes through the stream interceptor.
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	watch := map[string]string{"method": "/grpc.health.v1.Health/Watch", "code": "Canceled"}
	deadline := time.Now().Add(5 * time.Second)
	for counterValue(t, m, MetricRequestsTotal, watch) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("requests_total{Watch,Canceled} never reached 1")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestObserveHelpers(t *testing.T) {
	m := NewMetrics()
	m.ObserveWakeWait(WakeOutcomeWoke, 2*time.Second)
	m.ObserveCreateToReady("ate-env", ReadyStatusOK, time.Second)
	if got := counterValue(t, m, MetricWakeWaitSeconds, map[string]string{"outcome": "woke"}); got != 1 {
		t.Errorf("wake_wait{woke} count = %v, want 1", got)
	}
	if got := counterValue(t, m, MetricWakeWaitSeconds, map[string]string{"outcome": "timeout"}); got != 0 {
		t.Errorf("wake_wait{timeout} count = %v, want a pre-initialized 0", got)
	}
	if got := counterValue(t, m, MetricCreateToReadySeconds, map[string]string{"atespace": "ate-env", "status": "ok"}); got != 1 {
		t.Errorf("create_to_ready{ok} count = %v, want 1", got)
	}
}

// A nil *Metrics is the disabled state: every method is safe and does nothing.
func TestNilMetrics(t *testing.T) {
	var m *Metrics
	m.InitMethods(nil)
	m.InitAtespace("a")
	m.ObserveWakeWait(WakeOutcomeError, time.Second)
	m.ObserveCreateToReady("a", ReadyStatusOK, time.Second)
	m.SetEnvironmentSource(nil)
	m.observeRequest("/x", nil, time.Second)
}

// Two Metrics have registries of their own: no global registration.
func TestMetricsArePrivate(t *testing.T) {
	a, b := NewMetrics(), NewMetrics()
	a.ObserveWakeWait(WakeOutcomeWoke, time.Second)
	if got := counterValue(t, b, MetricWakeWaitSeconds, map[string]string{"outcome": "woke"}); got != 0 {
		t.Errorf("second registry sees the first's sample: %v", got)
	}
}

func TestBuildInfo(t *testing.T) {
	prevV, prevC := Version, Commit
	t.Cleanup(func() { Version, Commit = prevV, prevC })
	Version, Commit = "v1.2.3", "abc123"
	m := NewMetrics()
	mfs, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == MetricBuildInfo {
			metric := mf.GetMetric()[0]
			if metric.GetGauge().GetValue() != 1 {
				t.Errorf("build_info = %v, want 1", metric.GetGauge().GetValue())
			}
			labels := map[string]string{}
			for _, lp := range metric.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["version"] != "v1.2.3" || labels["commit"] != "abc123" {
				t.Errorf("build_info labels = %v", labels)
			}
			return
		}
	}
	t.Error("build_info not gathered")
}

// The environments collector reads its source once per cache period and
// keeps the last good answer when a read fails.
func TestEnvironmentsCollectorCachesAndKeepsLastGood(t *testing.T) {
	m := NewMetrics()
	calls := 0
	fail := false
	m.SetEnvironmentSource(func(context.Context) ([]EnvironmentCount, error) {
		calls++
		if fail {
			return nil, errors.New("control plane down")
		}
		return []EnvironmentCount{{Atespace: "ate-env", State: "running", Count: 3}}, nil
	})
	read := func() float64 {
		mfs, err := m.Registry().Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, mf := range mfs {
			if mf.GetName() == MetricEnvironments {
				return mf.GetMetric()[0].GetGauge().GetValue()
			}
		}
		return -1
	}
	if got := read(); got != 3 {
		t.Fatalf("environments = %v, want 3", got)
	}
	read()
	if calls != 1 {
		t.Errorf("source called %d times within the cache period, want 1", calls)
	}
	fail = true
	m.environments.mu.Lock()
	m.environments.fetched = time.Time{} // expire the cache
	m.environments.mu.Unlock()
	if got := read(); got != 3 {
		t.Errorf("environments after a failed read = %v, want the last good 3", got)
	}
	if calls != 2 {
		t.Errorf("source called %d times, want 2", calls)
	}
}

func TestHandlerServesExpositionFormat(t *testing.T) {
	ts := httptest.NewServer(NewMetrics().Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "# TYPE ate_env_api_build_info gauge") {
		t.Errorf("exposition lacks build_info:\n%s", body)
	}
}

// The source is read outside the collector's lock: while one scrape waits on
// a slow read, a concurrent scrape is served the last answer at once instead
// of waiting, and only one read is in flight.
func TestEnvironmentsCollectorReadsOutsideTheLock(t *testing.T) {
	m := NewMetrics()
	m.SetEnvironmentsCacheTTL(time.Nanosecond) // every scrape is due for a read
	var calls atomic.Int32
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	m.SetEnvironmentSource(func(context.Context) ([]EnvironmentCount, error) {
		if calls.Add(1) > 1 {
			entered <- struct{}{}
			<-block
		}
		return []EnvironmentCount{{Atespace: "ate-env", State: "running", Count: int(calls.Load())}}, nil
	})
	if _, err := m.Registry().Gather(); err != nil { // first read, fast
		t.Fatal(err)
	}
	slow := make(chan error, 1)
	go func() { _, err := m.Registry().Gather(); slow <- err }()
	<-entered // the second read is in flight and blocked

	done := make(chan float64, 1)
	go func() {
		mfs, _ := m.Registry().Gather()
		for _, mf := range mfs {
			if mf.GetName() == MetricEnvironments {
				done <- mf.GetMetric()[0].GetGauge().GetValue()
				return
			}
		}
		done <- -1
	}()
	select {
	case got := <-done:
		if got != 1 {
			t.Errorf("concurrent scrape served %v, want the last answer 1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent scrape waited for the in-flight read")
	}
	close(block)
	if err := <-slow; err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("source called %d times, want 2 (no read for the concurrent scrape)", n)
	}
}

func TestEnvironmentsCacheTTL(t *testing.T) {
	m := NewMetrics()
	var calls atomic.Int32
	m.SetEnvironmentSource(func(context.Context) ([]EnvironmentCount, error) {
		calls.Add(1)
		return nil, nil
	})
	for range 3 {
		_, _ = m.Registry().Gather()
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("default TTL (%s): source called %d times in three quick scrapes, want 1", DefaultEnvironmentsCacheTTL, n)
	}
	m.SetEnvironmentsCacheTTL(time.Nanosecond)
	_, _ = m.Registry().Gather()
	if n := calls.Load(); n != 2 {
		t.Errorf("after SetEnvironmentsCacheTTL(1ns): source called %d times, want 2", n)
	}
	if DefaultEnvironmentsCacheTTL < 60*time.Second {
		t.Errorf("DefaultEnvironmentsCacheTTL = %s, want at least 60s (longer than a usual scrape interval)", DefaultEnvironmentsCacheTTL)
	}
}

// The guest registry serves its one family with every exit class, and
// records an exit in its class.
func TestGuestMetrics(t *testing.T) {
	m := NewGuestMetrics()
	m.ObserveProcess(1500*time.Millisecond, "nonzero")
	mfs, err := m.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(mfs) != 1 || mfs[0].GetName() != MetricGuestProcessDurationSeconds {
		t.Fatalf("guest families = %v, want only %s", mfs, MetricGuestProcessDurationSeconds)
	}
	classes := map[string]uint64{}
	for _, metric := range mfs[0].GetMetric() {
		classes[metric.GetLabel()[0].GetValue()] = metric.GetHistogram().GetSampleCount()
	}
	want := map[string]uint64{"zero": 0, "nonzero": 1, "signal": 0, "timeout": 0}
	if !reflect.DeepEqual(classes, want) {
		t.Errorf("exit_class counts = %v, want %v", classes, want)
	}
	var nilM *GuestMetrics
	nilM.ObserveProcess(time.Second, "zero")
}

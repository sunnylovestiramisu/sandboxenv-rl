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

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseSidecar(t *testing.T) {
	argv, err := parseSidecar("  /opt/osb/execd --port 44772  ")
	if err != nil || strings.Join(argv, " ") != "/opt/osb/execd --port 44772" {
		t.Errorf("parseSidecar = %v, %v", argv, err)
	}
	for _, bad := range []string{"", "   ", "execd --port 1", "sh -c /opt/osb/execd"} {
		if _, err := parseSidecar(bad); err == nil {
			t.Errorf("parseSidecar(%q) accepted a non-absolute or empty command", bad)
		}
	}
	var m multiFlag
	if err := m.Set(""); err == nil {
		t.Error("empty flag value accepted")
	}
	_ = m.Set("/a")
	_ = m.Set("/b x")
	if len(m) != 2 || m.String() != "/a; /b x" {
		t.Errorf("multiFlag = %v", m)
	}
}

func TestReadyGateWaitsOnceForEachURL(t *testing.T) {
	var state atomic.Int32 // 0: 503, 1: 200
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if state.Load() == 0 {
			http.Error(w, "booting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()

	g := newReadyGate([]string{srv.URL + "/ready"})
	ctx := context.Background()
	if ok, why := g.Ready(ctx); ok || !strings.Contains(why, "HTTP 503") {
		t.Errorf("not ready expected, got ok=%v why=%q", ok, why)
	}
	state.Store(1)
	// Checks of one URL start at least sidecarCheckInterval apart. Without
	// this pause the next check would wait out the spacing inside the 100 ms
	// wait, and under CPU load not finish in it.
	time.Sleep(sidecarCheckInterval)
	if ok, why := g.Ready(ctx); !ok {
		t.Errorf("ready expected, got %q", why)
	}
	// Once seen, the URL is not re-checked: readiness is a start-up gate.
	state.Store(0)
	if ok, _ := g.Ready(ctx); !ok {
		t.Error("readiness flipped back after the sidecar had answered once")
	}

	unreachable := newReadyGate([]string{closedURL + "/ready"})
	if ok, why := unreachable.Ready(ctx); ok || why == "" {
		t.Errorf("an unreachable sidecar must report not ready with a reason, got ok=%v why=%q", ok, why)
	}
	if ok, _ := newReadyGate(nil).Ready(ctx); !ok {
		t.Error("no sidecars means ready")
	}
}

// Substrate's wakeup probe drops each request after 250 ms and polls every
// 1 ms. A sidecar whose readiness URL takes 300 ms to answer must still make
// /readyz ready: its check must outlive the probe request that started it.
func TestReadyzSlowSidecarBecomesReadyUnderProbeTimeout(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(300 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer sidecar.Close()
	guestSrv := httptest.NewServer(readyzHandler(newReadyGate([]string{sidecar.URL + "/ready"})))
	defer guestSrv.Close()

	probe := &http.Client{Timeout: 250 * time.Millisecond} // wakeupprobe.RequestTimeout
	deadline := time.Now().Add(5 * time.Second)
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		resp, err := probe.Get(guestSrv.URL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("/readyz not ready after 5s (%d attempts) with a 300 ms sidecar and a 250 ms probe", attempts)
}

func TestReadyGateSendsOneRequestAtATimeForEachURL(t *testing.T) {
	var total, inFlight, maxInFlight atomic.Int32
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		total.Add(1)
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
		http.Error(w, "booting", http.StatusServiceUnavailable)
	}))
	defer sidecar.Close()
	g := newReadyGate([]string{sidecar.URL + "/ready"})

	// 50 probes inside one slow answer: all share the one check in flight.
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			if ok, why := g.Ready(ctx); ok || !strings.Contains(why, "in flight") {
				t.Errorf("Ready = %v, %q; want not ready with the check in flight", ok, why)
			}
		}()
	}
	wg.Wait()
	if got := total.Load(); got != 1 {
		t.Errorf("sidecar got %d requests from 50 concurrent probes, want 1", got)
	}

	// Probes that keep polling past several slow answers never overlap them.
	stop := time.Now().Add(time.Second)
	for time.Now().Before(stop) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Ready(context.Background())
		}()
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	if got := maxInFlight.Load(); got != 1 {
		t.Errorf("sidecar saw %d concurrent requests, want 1", got)
	}
	// The failed answer is reported to later probes.
	time.Sleep(400 * time.Millisecond)
	if ok, why := g.Ready(context.Background()); ok || !strings.Contains(why, "HTTP 503") {
		t.Errorf("Ready = %v, %q; want the sidecar's HTTP 503", ok, why)
	}
}

// slowSidecar answers 2xx after d and counts its requests.
func slowSidecar(t *testing.T, d time.Duration, count *atomic.Int32) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count != nil {
			count.Add(1)
		}
		select {
		case <-time.After(d):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL + "/ready"
}

// One /readyz call must answer inside the probe's 250 ms request limit
// however many sidecar readiness URLs the template has. Fast sidecars are
// ready inside the one wait; checked one after another, as before, 30 of
// them would take at least 900 ms.
func TestReadyGateWaitsOnceForAllURLs(t *testing.T) {
	const callLimit = 240 * time.Millisecond // the probe's 250 ms, less timer slack
	for _, tc := range []struct {
		n         int
		d         time.Duration
		wantReady bool
	}{
		// 30 ms each: still ready inside the 100 ms wait when checked
		// together, while 30 of them checked one after another need 900 ms,
		// which leaves room for scheduling delay under -race.
		{30, 30 * time.Millisecond, true},
		{3, 300 * time.Millisecond, false}, // only the bound on the call time
	} {
		// A transport for each case, closed after its servers (cleanups run
		// last first), so the servers close the connections and hold their
		// TIME_WAIT. On the shared default transport the client closed them,
		// and many runs of this test used up the local ports.
		tr := &http.Transport{}
		t.Cleanup(tr.CloseIdleConnections)
		var urls []string
		for range tc.n {
			urls = append(urls, slowSidecar(t, tc.d, nil))
		}
		g := newReadyGate(urls)
		g.client = &http.Client{Transport: tr}
		start := time.Now()
		ok, why := g.Ready(context.Background())
		if took := time.Since(start); took > callLimit {
			t.Errorf("%d sidecars of %s: Ready took %s, want at most %s (one wait for all URLs)", tc.n, tc.d, took, callLimit)
		}
		if tc.wantReady && !ok {
			// Under -race or CPU contention, 30 concurrent HTTP round trips
			// may not complete inside the first 100 ms wait. The checks
			// continue in flight. Bound total time-to-ready by the serial
			// lower bound (n*d): taking longer means URLs are no longer
			// checked concurrently.
			serialBound := time.Duration(tc.n) * tc.d
			// The loop guard alone isn't enough: a Ready call entered just
			// before the bound can wait up to 100 ms more, so the elapsed time
			// is checked again after the loop.
			for time.Since(start) < serialBound {
				time.Sleep(5 * time.Millisecond)
				callStart := time.Now()
				ok, why = g.Ready(context.Background())
				if took := time.Since(callStart); took > callLimit {
					t.Errorf("%d sidecars of %s: subsequent Ready took %s, want at most %s", tc.n, tc.d, took, callLimit)
				}
				if ok {
					break
				}
			}
			if took := time.Since(start); !ok || took >= serialBound {
				t.Fatalf("%d sidecars of %s: Ready = %v after %s, want true within the serial bound %s; URLs are no longer checked concurrently: %s", tc.n, tc.d, ok, took, serialBound, why)
			}
		}
		if ok != tc.wantReady {
			t.Errorf("%d sidecars of %s: Ready = %v, %q; want %v", tc.n, tc.d, ok, why, tc.wantReady)
		}
	}
}

// A slow first sidecar must not hold back the checks of the others: the
// time to ready is the longest boot, not the sum.
func TestReadyGateChecksAllPendingURLsTogether(t *testing.T) {
	var second atomic.Int32
	g := newReadyGate([]string{
		slowSidecar(t, time.Second, nil),
		slowSidecar(t, time.Second, &second),
	})
	start := time.Now()
	g.Ready(context.Background())
	if got := second.Load(); got != 1 {
		t.Errorf("second URL got %d requests while the first was pending, want 1", got)
	}
	for !func() bool { ok, _ := g.Ready(context.Background()); return ok }() {
		if time.Since(start) > 5*time.Second {
			t.Fatal("not ready after 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if took := time.Since(start); took > 1600*time.Millisecond {
		t.Errorf("two 1 s sidecars took %s to be ready, want about 1 s (checked together)", took)
	}
}

// A sidecar that fails at once must not get a request for each 1 ms probe
// poll while it boots.
func TestReadyGateSpacesChecksOfOneURL(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		http.Error(w, "booting", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	g := newReadyGate([]string{srv.URL + "/ready"})
	stop := time.Now().Add(time.Second)
	for time.Now().Before(stop) {
		if ok, why := g.Ready(context.Background()); ok || !strings.Contains(why, "HTTP 503") && !strings.Contains(why, "in flight") {
			t.Fatalf("Ready = %v, %q; want not ready", ok, why)
		}
		time.Sleep(time.Millisecond)
	}
	// One second at one check each 50 ms is 21 checks at most.
	if got := n.Load(); got > 25 {
		t.Errorf("sidecar got %d requests in 1s, want at most about %d", got, int(time.Second/sidecarCheckInterval)+1)
	}
}

func TestRunSidecarRestartsAndStopsWithContext(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh on this host")
	}
	var mu sync.Mutex
	var lines []string
	logf := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, fmt.Sprintf(format, args...))
	}

	// A sidecar that prints and exits non-zero is restarted with backoff.
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		runSidecar(ctx, []string{sh, "-c", "echo hello from sidecar; exit 3"}, nil, logf)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runSidecar did not return after its context ended")
	}
	mu.Lock()
	joined := strings.Join(lines, "\n")
	mu.Unlock()
	if !strings.Contains(joined, "[sh] hello from sidecar") {
		t.Errorf("sidecar output was not logged with its prefix:\n%s", joined)
	}
	if n := strings.Count(joined, "restarting in"); n < 2 {
		t.Errorf("expected at least two restarts within the window (1s then 2s backoff), got %d:\n%s", n, joined)
	}
	if !strings.Contains(joined, "restarting in 1s") || !strings.Contains(joined, "restarting in 2s") {
		t.Errorf("backoff did not double:\n%s", joined)
	}

	// A long-running sidecar is killed promptly when the guest shuts down,
	// together with what it forked. `sleep 60 & wait` forks on every
	// platform; a bare `sh -c "sleep 60"` execs on macOS and would not
	// exercise the process-group kill there. The forked sleep holds the
	// output pipe, and WaitDelay (sidecarStopGrace) would not release it
	// before 5 s, so a return within the bound below proves the child died.
	bound := sidecarStopGrace - time.Second
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan struct{})
	go func() {
		runSidecar(ctx2, []string{sh, "-c", "sleep 60 & wait"}, nil, logf)
		close(done2)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel2()
	select {
	case <-done2:
	case <-time.After(bound):
		t.Fatal("sidecar (or its child) was not stopped when the context was cancelled")
	}
	mu.Lock()
	joined = strings.Join(lines, "\n")
	mu.Unlock()
	for _, want := range []string{"SIGTERM to process group", "sidecar sh stopped"} {
		if !strings.Contains(joined, want) {
			t.Errorf("termination sequence not logged (%q missing):\n%s", want, joined)
		}
	}
	if !strings.Contains(joined, "exited after SIGTERM") && !strings.Contains(joined, "already exited") {
		t.Errorf("the group's end was not logged:\n%s", joined)
	}
}

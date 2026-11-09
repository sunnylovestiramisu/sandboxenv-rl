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

package apiservice

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// slowStateRead is a readStatus that answers state after latency, or the
// context's error if ctx ends first, as a gRPC call to the control plane
// does.
func slowStateRead(latency time.Duration, state ateapipb.ActorState) func(context.Context) (*ateapipb.ActorStatus, error) {
	return func(ctx context.Context) (*ateapipb.ActorStatus, error) {
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(latency):
			return &ateapipb.ActorStatus{State: state}, nil
		}
	}
}

// A transient control-plane error on the state read (WF-5, and WF-3 for a
// control-plane ResourceExhausted) leaves the state unknown: the wait backs
// off and probes again instead of ending at once with the read's error. The
// first such error of a wait is logged once, with its code and attempt.
func TestAwaitWakeRetriesTransientStateReadErrors(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			var logs bytes.Buffer
			log.SetOutput(&logs)
			defer log.SetOutput(io.Discard)
			synctest.Test(t, func(t *testing.T) {
				s := &Server{WakeBudget: time.Minute, WakeRunningBudget: 30 * time.Second, WakeNoCapacityGrace: 10 * time.Second}
				probes, reads := 0, 0
				probe := func(context.Context) error {
					probes++
					if probes >= 3 {
						return status.Error(codes.NotFound, "no such process") // the guest answers
					}
					return status.Error(codes.Unavailable, "503: upstream connect error")
				}
				readStatus := func(context.Context) (*ateapipb.ActorStatus, error) {
					reads++
					if reads <= 2 {
						return nil, toGRPCError(status.Error(code, "ate-api: transient"))
					}
					return &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING}, nil
				}
				if err := s.awaitWake(context.Background(), "ns", "env", probe, readStatus); err != nil {
					t.Fatalf("awaitWake = %v after %d probes, want the guest's answer (nil)", err, probes)
				}
				if probes != 3 {
					t.Fatalf("probes = %d, want 3", probes)
				}
			})
			out := logs.String()
			if n := strings.Count(out, "state read failed"); n != 1 {
				t.Fatalf("logged %d state-read failures, want 1 for 2 failed reads:\n%s", n, out)
			}
			if want := "failed with " + code.String() + " at attempt 1;"; !strings.Contains(out, want) {
				t.Fatalf("log does not contain %q:\n%s", want, out)
			}
		})
	}
}

// WF-5: a transient state-read error inside a RUNNING run leaves the state
// unknown but keeps the run, so the running budget is not restarted: the wait
// ends at the running budget counted from the first RUNNING attempt, not at
// the wake budget.
func TestAwaitWakeTransientReadKeepsRunningBudget(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	synctest.Test(t, func(t *testing.T) {
		const runningBudget = 20 * time.Second
		s := &Server{WakeBudget: 5 * time.Minute, WakeRunningBudget: runningBudget}
		probe := func(context.Context) error {
			return status.Error(codes.Unavailable, "503: upstream connect error")
		}
		reads := 0
		readStatus := func(context.Context) (*ateapipb.ActorStatus, error) {
			reads++
			if reads%3 == 2 { // reads 2, 5, 8, ... fail
				return nil, status.Error(codes.Unavailable, "ate-api: transient")
			}
			return &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}, nil
		}
		start := time.Now()
		err := s.awaitWake(context.Background(), "ns", "env", probe, readStatus)
		elapsed := time.Since(start)
		if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "running budget of 20s") {
			t.Fatalf("awaitWake = %v, want the running-budget timeout", err)
		}
		if elapsed != runningBudget {
			t.Fatalf("WF-5: the wait took %s, want the running budget %s from the first RUNNING read", elapsed, runningBudget)
		}
	})
}

// A state read that is not transient still ends the wait with its error:
// NotFound means the environment is gone.
func TestAwaitWakeEndsOnNotFoundStateRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Server{WakeBudget: time.Minute, WakeRunningBudget: 30 * time.Second}
		probes := 0
		probe := func(context.Context) error {
			probes++
			return status.Error(codes.Unavailable, "503: upstream connect error")
		}
		readStatus := func(context.Context) (*ateapipb.ActorStatus, error) {
			return nil, status.Error(codes.NotFound, "actor not found")
		}
		err := s.awaitWake(context.Background(), "ns", "env", probe, readStatus)
		if status.Code(err) != codes.NotFound || probes != 1 {
			t.Fatalf("awaitWake = %v after %d probes, want NotFound after 1", err, probes)
		}
	})
}

// A read that answers NotFound at the instant its deadline passes still ends
// the wait with NotFound: the control plane's answer wins over the deadline.
func TestAwaitWakeNotFoundAtReadDeadlineEndsTheWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const budget = 10 * time.Second
		s := &Server{WakeBudget: budget, WakeRunningBudget: time.Minute}
		probe := func(context.Context) error {
			return status.Error(codes.Unavailable, "503: upstream connect error")
		}
		readStatus := func(context.Context) (*ateapipb.ActorStatus, error) {
			time.Sleep(budget) // answers just as the read's deadline passes
			return nil, status.Error(codes.NotFound, "actor not found")
		}
		if err := s.awaitWake(context.Background(), "ns", "env", probe, readStatus); status.Code(err) != codes.NotFound {
			t.Fatalf("awaitWake = %v, want NotFound", err)
		}
	})
}

// WF-6: a slow state read after the last probe cannot push the wait past
// WakeBudget; the probes here answer at once, so the wait ends at the budget.
func TestAwaitWakeStateReadBoundedByWakeBudget(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	synctest.Test(t, func(t *testing.T) {
		const budget = 10 * time.Second
		s := &Server{WakeBudget: budget, WakeRunningBudget: time.Minute}
		probe := func(context.Context) error {
			return status.Error(codes.Unavailable, "503: parking budget expired")
		}
		start := time.Now()
		err := s.awaitWake(context.Background(), "ns", "env", probe,
			slowStateRead(2*time.Second, ateapipb.ActorState_ACTOR_STATE_RESUMING))
		elapsed := time.Since(start)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("awaitWake = %v, want the Unavailable timeout", err)
		}
		if elapsed != budget {
			t.Fatalf("WF-6: the wait took %s, want exactly the wake budget %s (probes take no time)", elapsed, budget)
		}
	})
}

// WF-4: a slow state read cannot delay the probe that ends a no-capacity run
// past WakeNoCapacityGrace + 1 ms. Probe 1 answers no-room at 9.9 s, under the
// 10 s grace; the 2 s read is cut at the grace and the next probe ends the run.
// The read after that last probe is bounded by WakeBudget and the 5 s
// per-read cap, not by the grace, so the wait returns one read (2 s) after
// it, at 12.001 s.
func TestAwaitWakeStateReadBoundedByNoCapacityGrace(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	synctest.Test(t, func(t *testing.T) {
		const grace = 10 * time.Second
		s := &Server{WakeBudget: 5 * time.Minute, WakeRunningBudget: time.Minute, WakeNoCapacityGrace: grace}
		var starts []time.Time
		probe := func(context.Context) error {
			starts = append(starts, time.Now())
			if len(starts) == 1 {
				time.Sleep(9900 * time.Millisecond)
			}
			return status.Error(codes.Unavailable, "503: "+routerNoCapacityText)
		}
		runStart := time.Now()
		err := s.awaitWake(context.Background(), "ns", "env", probe,
			slowStateRead(2*time.Second, ateapipb.ActorState_ACTOR_STATE_SUSPENDED))
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("awaitWake = %v, want ResourceExhausted", err)
		}
		last := starts[len(starts)-1]
		if late := last.Sub(runStart) - grace; late > time.Millisecond {
			t.Fatalf("WF-4: the probe that ended the run (attempt %d) started %s after the run began, %s past the grace %s",
				len(starts), last.Sub(runStart), late, grace)
		}
		if elapsed, want := time.Since(runStart), grace+time.Millisecond+2*time.Second; elapsed != want {
			t.Fatalf("the wait returned at %s, want %s: the grace, the 1 ms backoff, and the last 2 s read", elapsed, want)
		}
	})
}

// A state read that hangs without an error is cut at the per-read cap
// (DefaultWakeStateReadCap), not at WakeBudget: the cut read leaves the state
// unknown, and the wait backs off and probes again, here finding the guest
// awake. Without the cap the hung read held the wait, with no probe, for the
// whole 5 min budget.
func TestAwaitWakeStateReadCappedPerRead(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	synctest.Test(t, func(t *testing.T) {
		s := &Server{WakeBudget: 5 * time.Minute, WakeRunningBudget: time.Minute}
		start := time.Now()
		var probeStarts []time.Duration
		probe := func(context.Context) error {
			probeStarts = append(probeStarts, time.Since(start))
			if len(probeStarts) >= 3 {
				return status.Error(codes.NotFound, "no such process") // the guest answers
			}
			return status.Error(codes.Unavailable, "503: upstream connect error")
		}
		reads := 0
		var hungDeadline, hungEnd time.Duration
		readStatus := func(ctx context.Context) (*ateapipb.ActorStatus, error) {
			reads++
			if reads == 1 { // hangs until its deadline, with no error of its own
				d, _ := ctx.Deadline()
				hungDeadline = d.Sub(start)
				<-ctx.Done()
				hungEnd = time.Since(start)
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING}, nil
		}
		if err := s.awaitWake(context.Background(), "ns", "env", probe, readStatus); err != nil {
			t.Fatalf("awaitWake = %v after %d probes, want the guest's answer (nil)", err, len(probeStarts))
		}
		if hungDeadline != DefaultWakeStateReadCap || hungEnd != DefaultWakeStateReadCap {
			t.Fatalf("the hung read had deadline %s and ended at %s, want both at the per-read cap %s",
				hungDeadline, hungEnd, DefaultWakeStateReadCap)
		}
		want := []time.Duration{0, DefaultWakeStateReadCap + wakeRetryInitialBackoff, DefaultWakeStateReadCap + 3*wakeRetryInitialBackoff}
		if len(probeStarts) != len(want) {
			t.Fatalf("probes started at %v, want %v", probeStarts, want)
		}
		for i := range want {
			if probeStarts[i] != want[i] {
				t.Fatalf("probes started at %v, want %v: probe again one backoff after the cap", probeStarts, want)
			}
		}
	})
}

// The log line for a cut state read names the bound that set the read's
// deadline and that deadline: the no-capacity grace and the wake budget can
// cut a read before the per-read cap, and the line must not blame the cap
// for those.
func TestAwaitWakeLogNamesTheBoundThatCutTheRead(t *testing.T) {
	prevOut := log.Writer()
	t.Cleanup(func() { log.SetOutput(prevOut) })

	for _, tc := range []struct {
		name  string
		srv   func() *Server
		probe error
		want  string
	}{
		{"grace", func() *Server {
			return &Server{WakeBudget: 5 * time.Minute, WakeRunningBudget: time.Minute, WakeNoCapacityGrace: 3 * time.Second}
		},
			status.Error(codes.Unavailable, "actor ns/env unavailable: "+routerNoCapacityText), "cut at its read deadline of 3s, set by the no-capacity grace (per-read cap 5s)"},
		{"budget", func() *Server { return &Server{WakeBudget: 4 * time.Second, WakeRunningBudget: time.Minute} },
			status.Error(codes.Unavailable, "503: upstream connect error"), "cut at its read deadline of 4s, set by the wake budget (per-read cap 5s)"},
		{"cap", func() *Server { return &Server{WakeBudget: 5 * time.Minute, WakeRunningBudget: time.Minute} },
			status.Error(codes.Unavailable, "503: upstream connect error"), "cut at its read deadline of 5s, set by the per-read cap (per-read cap 5s)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs strings.Builder
			log.SetOutput(&logs)
			synctest.Test(t, func(t *testing.T) {
				s := tc.srv()
				probes := 0
				probe := func(context.Context) error {
					probes++
					if probes >= 3 {
						return status.Error(codes.NotFound, "no such process") // the guest answers
					}
					return tc.probe
				}
				reads := 0
				readStatus := func(ctx context.Context) (*ateapipb.ActorStatus, error) {
					reads++
					if reads == 1 { // hangs until its deadline, with no error of its own
						<-ctx.Done()
						return nil, status.FromContextError(ctx.Err()).Err()
					}
					return &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING}, nil
				}
				_ = s.awaitWake(context.Background(), "ns", "env", probe, readStatus)
			})
			if out := logs.String(); !strings.Contains(out, tc.want) {
				t.Fatalf("log does not contain %q:\n%s", tc.want, out)
			}
		})
	}
}

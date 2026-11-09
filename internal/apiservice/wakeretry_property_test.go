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
	"context"
	"flag"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/internaltest/synctestrapid"
	"github.com/agent-substrate/env/internal/telemetry"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

// wakeAttempt records one probe of awaitWake and, when the probe found the
// guest unreachable, the state the control plane reported after it.
type wakeAttempt struct {
	start, done  time.Time
	answered     bool      // the guest answered the probe
	unavailable  bool      // the probe answered Unavailable
	noCapacity   bool      // ... with the router's "no worker has room" text
	ctxEnded     bool      // caller ctx ended before or during the parked probe
	readCtxEnded bool      // caller ctx ended before readStatus completed
	readCut      bool      // readStatus ran out of the time awaitWake gave it (budget, grace or cap)
	readDone     time.Time // when readStatus returned
	// readErr is the control plane's error for the state read (OK: none).
	// With an error, or readCut, state stays UNSPECIFIED: unknown.
	readErr codes.Code
	state   ateapipb.ActorState
}

var wakeStates = []ateapipb.ActorState{
	ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
	ateapipb.ActorState_ACTOR_STATE_RESUMING,
	ateapipb.ActorState_ACTOR_STATE_RUNNING,
	ateapipb.ActorState_ACTOR_STATE_CRASHED,
	ateapipb.ActorState_ACTOR_STATE_DELETING,
}

const (
	maxProbeLatency = 3 * time.Second
	maxReadLatency  = 3 * time.Second
)

// TestAwaitWakeProperties drives awaitWake (ensureAwake's loop) with generated
// router answers, probe latencies, control-plane states, state-read latencies
// and errors, budgets and caller cancellations/deadlines on synctest's fake
// clock, checking WF-1 to WF-7
// (docs/plans/ate-env-api/acceptance-criteria.md) after every run:
//
//   - WF-1: a probe the guest answers ends the wait at once, with no error.
//   - WF-2: an unreachable guest whose actor is CRASHED ends the wait at once
//     with FailedPrecondition, naming the crash reason when there is one;
//     DELETING ends it with NotFound, and a state read that answers a
//     non-transient error (NotFound, Internal) ends it with that error.
//   - WF-3: ResourceExhausted only with a positive no-capacity grace, after a
//     run of no-room answers (actor not reported RUNNING, CRASHED or
//     DELETING) that lasted the grace, measured from the first probe's start
//     to the last probe's answer; a control-plane ResourceExhausted on the
//     state read is not one.
//   - WF-4: once such a run has lasted the grace at a probe's answer, that
//     probe is the last one: the wait never runs on past the grace, however
//     slow the state read.
//   - WF-5: a timeout (Unavailable) only after the wake budget, or after the
//     running budget counted from the first attempt of the current RUNNING
//     run; a transient state-read error leaves the state unknown and never
//     ends the wait early. An unknown state keeps a RUNNING run, and the
//     attempt whose state read finds that run past the running budget is the
//     last one.
//   - WF-6: the wait never runs past the wake budget by more than one probe,
//     however slow the state read.
//   - WF-4/WF-6: every state read ends by its deadline, the earliest of the
//     wake budget, the grace end during a no-room run, and the per-read cap
//     (drawn, so reads that hang past it are generated); a read the deadline
//     cuts ends exactly then.
//   - WF-7: caller cancellation or deadline expiry during a parked probe or
//     backoff ends the wait at once with Canceled or DeadlineExceeded and
//     never marks create_to_ready or wake_wait as ready.
func TestAwaitWakeProperties(t *testing.T) {
	if f := flag.Lookup("rapid.checks"); f != nil && f.Value.String() == f.DefValue {
		_ = f.Value.Set("2000")
		defer func() { _ = f.Value.Set(f.DefValue) }()
	}
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	synctestrapid.Check(t, checkAwaitWake)
}

// wakeStep is what one probe meets: its latency, the router's answer
// (0: the guest answers; 1-4: no room; 5-9: another Unavailable), and the
// state the control plane reports after an unreachable probe (an index into
// wakeStateWeights), after readLatency, or the error it answers instead (an
// index into wakeReadErrs).
type wakeStep struct {
	latency     time.Duration
	answer      int
	state       int
	readLatency time.Duration
	readErr     int
}

// Mostly a state; sometimes a transient error, which leaves the state
// unknown; rarely NotFound (a deleted environment) or Internal, which end
// the wait with that error.
var wakeReadErrs = func() []codes.Code {
	out := []codes.Code{
		codes.Unavailable, codes.Unavailable,
		codes.DeadlineExceeded, codes.DeadlineExceeded,
		codes.ResourceExhausted, codes.ResourceExhausted,
		codes.NotFound, codes.Internal,
	}
	for len(out) < 40 {
		out = append(out, codes.OK)
	}
	return out
}()

// Mostly not-yet-running states, so runs reach the budgets.
var wakeStateWeights = []int{0, 0, 0, 1, 1, 1, 2, 2, 2, 2, 3, 4}

// maxWakeSteps bounds a timeline; a run that outlasts it gets a guest that
// answers.
const maxWakeSteps = 60

func checkAwaitWake(_ *testing.T, rt *rapid.T) {
	ms := func(lo, hi int64, label string) time.Duration {
		return time.Duration(rapid.Int64Range(lo, hi).Draw(rt, label)) * time.Millisecond
	}
	metrics := telemetry.NewMetrics()
	s := &Server{
		WakeBudget:        ms(500, 20_000, "wake_budget_ms"),
		WakeRunningBudget: ms(100, 10_000, "running_budget_ms"),
		Metrics:           metrics,
		// Mostly below maxReadLatency, so many slow reads run into the cap.
		WakeStateReadCap: ms(50, 4_000, "read_cap_ms"),
	}
	if rapid.Bool().Draw(rt, "grace_set") {
		s.WakeNoCapacityGrace = ms(50, 10_000, "grace_ms")
	}
	callerEndKind := rapid.SampledFrom([]int{0, 0, 0, 1, 2}).Draw(rt, "caller_end_kind")
	var callerEndAt time.Duration
	if callerEndKind != 0 {
		callerEndAt = ms(0, 22_000, "caller_end_ms")
	}
	crashReason := rapid.SampledFrom([]string{"", "OOMKilled"}).Draw(rt, "crash_reason")
	steps := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) wakeStep {
		step := wakeStep{
			latency: time.Duration(rapid.Int64Range(0, maxProbeLatency.Milliseconds()).Draw(t, "latency_ms")) * time.Millisecond,
			answer:  rapid.IntRange(0, 9).Draw(t, "answer"),
			state:   rapid.IntRange(0, len(wakeStateWeights)-1).Draw(t, "state"),
			readErr: rapid.IntRange(0, len(wakeReadErrs)-1).Draw(t, "read_err"),
		}
		if rapid.Bool().Draw(t, "slow_read") {
			step.readLatency = time.Duration(rapid.Int64Range(1, maxReadLatency.Milliseconds()).Draw(t, "read_latency_ms")) * time.Millisecond
		}
		return step
	}), 0, maxWakeSteps).Draw(rt, "steps")

	callerCtx := context.Background()
	var attempts []wakeAttempt
	probe := func(ctx context.Context) error {
		step := wakeStep{}
		if n := len(attempts); n < len(steps) {
			step = steps[n]
		}
		a := wakeAttempt{start: time.Now()}
		if ctx.Err() != nil {
			a.done = time.Now()
			a.ctxEnded = true
			attempts = append(attempts, a)
			return status.FromContextError(ctx.Err()).Err()
		}
		if step.latency > 0 {
			select {
			case <-ctx.Done():
				a.done = time.Now()
				a.ctxEnded = true
				attempts = append(attempts, a)
				return status.FromContextError(ctx.Err()).Err()
			case <-time.After(step.latency):
			}
		}
		a.done = time.Now()
		var perr error
		switch {
		case step.answer == 0: // the guest answers (NotFound for the probe id, as in production)
			a.answered = true
			perr = status.Error(codes.NotFound, "no such process")
		case step.answer <= 4:
			a.unavailable, a.noCapacity = true, true
			perr = status.Error(codes.Unavailable, "503: "+routerNoCapacityText)
		default:
			a.unavailable = true
			perr = status.Error(codes.Unavailable, "503: upstream connect error")
		}
		attempts = append(attempts, a)
		return perr
	}
	readStatus := func(ctx context.Context) (*ateapipb.ActorStatus, error) {
		a := &attempts[len(attempts)-1]
		defer func() { a.readDone = time.Now() }()
		step := wakeStep{readErr: len(wakeReadErrs) - 1} // a state, at once
		if n := len(attempts) - 1; n < len(steps) {
			step = steps[n]
		}
		// The read ends when ctx does, as a gRPC call does: the caller's
		// ctx, or the deadline awaitWake gave the read.
		ended := func() (*ateapipb.ActorStatus, error) {
			if callerCtx.Err() != nil {
				a.readCtxEnded = true
			} else {
				a.readCut = true
			}
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		if ctx.Err() != nil {
			return ended()
		}
		if step.readLatency > 0 {
			select {
			case <-ctx.Done():
				return ended()
			case <-time.After(step.readLatency):
			}
		}
		if c := wakeReadErrs[step.readErr]; c != codes.OK {
			a.readErr = c
			return nil, status.Error(c, "ate-api: "+c.String())
		}
		st := wakeStates[wakeStateWeights[step.state]]
		a.state = st
		out := &ateapipb.ActorStatus{State: st}
		if st == ateapipb.ActorState_ACTOR_STATE_CRASHED && crashReason != "" {
			out.Crash = &ateapipb.ActorCrash{Message: crashReason}
		}
		return out, nil
	}

	start := time.Now()
	s.trackCreate("ns", "env", start)

	ctx := context.Background()
	var cleanup func()
	var wantCtxCode codes.Code
	switch callerEndKind {
	case 1:
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		tmr := time.AfterFunc(callerEndAt, cancel)
		cleanup = func() {
			tmr.Stop()
			cancel()
		}
		wantCtxCode = codes.Canceled
	case 2:
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), callerEndAt)
		cleanup = cancel
		wantCtxCode = codes.DeadlineExceeded
	}
	callerCtx = ctx

	err := s.awaitWake(ctx, "ns", "env", probe, readStatus)
	now := time.Now()
	elapsed := now.Sub(start)
	ctxErr := ctx.Err()
	if cleanup != nil {
		cleanup()
	}

	code := status.Code(err)
	if len(attempts) == 0 {
		rt.Fatalf("WF-1: returned %v without probing", err)
	}
	last := attempts[len(attempts)-1]

	readyOK := histogramSampleCount(rt, metrics, telemetry.MetricCreateToReadySeconds, "status", telemetry.ReadyStatusOK)
	readyCrashed := histogramSampleCount(rt, metrics, telemetry.MetricCreateToReadySeconds, "status", telemetry.ReadyStatusCrashed)
	wakeWoke := histogramSampleCount(rt, metrics, telemetry.MetricWakeWaitSeconds, "outcome", telemetry.WakeOutcomeWoke)

	// WF-1, WF-2, WF-7: what the last attempt saw decides the result.
	switch {
	case last.answered:
		if err != nil {
			rt.Fatalf("WF-1: the guest answered, but awaitWake returned %v", err)
		}
		if readyOK != 1 || readyCrashed != 0 {
			rt.Fatalf("WF-1: create_to_ready ok=%d crashed=%d after guest answer", readyOK, readyCrashed)
		}
	case last.ctxEnded || last.readCtxEnded:
		if code != wantCtxCode {
			rt.Fatalf("WF-7: caller context ended during probe/read, got %v, want %v", err, wantCtxCode)
		}
	case readEndsWait(last.readErr):
		// The caller's ctx may end at the instant the read answers.
		if code != last.readErr && (ctxErr == nil || code != wantCtxCode) {
			rt.Fatalf("WF-2: a state read that answered %v gave %v, want that code", last.readErr, err)
		}
	case last.state == ateapipb.ActorState_ACTOR_STATE_CRASHED:
		if code != codes.FailedPrecondition {
			rt.Fatalf("WF-2: crashed actor gave %v, want FailedPrecondition", err)
		}
		if crashReason != "" && !strings.Contains(err.Error(), crashReason) {
			rt.Fatalf("WF-2: %q does not name the crash reason %q", err, crashReason)
		}
		if readyCrashed != 1 || readyOK != 0 {
			rt.Fatalf("WF-2: create_to_ready ok=%d crashed=%d after crash", readyOK, readyCrashed)
		}
	case last.state == ateapipb.ActorState_ACTOR_STATE_DELETING:
		if code != codes.NotFound {
			rt.Fatalf("WF-2: deleting actor gave %v, want NotFound", err)
		}
	default:
		if ctxErr != nil && code == wantCtxCode {
			// Ended during backoff select on <-ctx.Done().
		} else if code != codes.ResourceExhausted && code != codes.Unavailable {
			rt.Fatalf("WF-5: an unreachable %s actor gave %v, want ResourceExhausted or Unavailable", last.state, err)
		}
	}
	if err != nil && (readyOK != 0 || wakeWoke != 0) {
		rt.Fatalf("WF-7: failed wake (%v) marked ready (create_to_ready_ok=%d wake_woke=%d)", err, readyOK, wakeWoke)
	}
	if callerEndKind != 0 && (code == codes.Canceled || code == codes.DeadlineExceeded) {
		if ctxErr == nil || code != wantCtxCode {
			rt.Fatalf("WF-7: returned %v with ctxErr=%v, want %v", err, ctxErr, wantCtxCode)
		}
		if elapsed != callerEndAt {
			rt.Fatalf("WF-7: ended at %s, want exact caller end %s", elapsed, callerEndAt)
		}
	}
	for i, a := range attempts[:len(attempts)-1] {
		if a.answered || a.ctxEnded || a.readCtxEnded || readEndsWait(a.readErr) ||
			a.state == ateapipb.ActorState_ACTOR_STATE_CRASHED ||
			a.state == ateapipb.ActorState_ACTOR_STATE_DELETING {
			rt.Fatalf("WF-1/WF-2/WF-7: attempt %d of %d (answered=%t ctxEnded=%t state=%s) should have ended the wait",
				i+1, len(attempts), a.answered, a.ctxEnded || a.readCtxEnded, a.state)
		}
	}

	// WF-3, WF-4: the no-capacity grace.
	var runStart time.Time
	for i, a := range attempts {
		if a.unavailable && !a.ctxEnded {
			// The state read's deadline: what is left of the wake budget
			// and, while the no-room run this answer starts or continues
			// has not yet lasted the grace, what is left of the grace, but
			// never more than the per-read cap from the probe's answer (the
			// read starts then). A read that the deadline cuts ends exactly
			// then (or at once, when the probe already ran past it), and no
			// read runs past it. That the cut read keeps the run is WF-4
			// below.
			readBy := start.Add(s.WakeBudget)
			if g := s.WakeNoCapacityGrace; g > 0 && a.noCapacity {
				rs := runStart
				if rs.IsZero() {
					rs = a.start
				}
				if graceEnd := rs.Add(g); graceEnd.After(a.done) && graceEnd.Before(readBy) {
					readBy = graceEnd
				}
			}
			if capEnd := a.done.Add(s.WakeStateReadCap); capEnd.Before(readBy) {
				readBy = capEnd
			}
			if a.done.After(readBy) {
				readBy = a.done
			}
			if a.readCut && !a.readDone.Equal(readBy) {
				rt.Fatalf("WF-4/WF-6: attempt %d's state read was cut at %s, want its deadline %s",
					i+1, a.readDone.Sub(start), readBy.Sub(start))
			}
			if !a.readCtxEnded && a.readDone.After(readBy) {
				rt.Fatalf("WF-4/WF-6: attempt %d's state read ran to %s, past its deadline %s",
					i+1, a.readDone.Sub(start), readBy.Sub(start))
			}
		}
		// An unknown state (a transient read error, or a read cut short)
		// keeps the run: the router's no-room answer is what it counts.
		inRun := a.unavailable && !a.readCtxEnded && a.noCapacity &&
			!readEndsWait(a.readErr) &&
			a.state != ateapipb.ActorState_ACTOR_STATE_RUNNING &&
			a.state != ateapipb.ActorState_ACTOR_STATE_CRASHED &&
			a.state != ateapipb.ActorState_ACTOR_STATE_DELETING
		if !inRun {
			runStart = time.Time{}
			continue
		}
		if runStart.IsZero() {
			runStart = a.start
		}
		if g := s.WakeNoCapacityGrace; g > 0 && a.done.Sub(runStart) >= g {
			if i != len(attempts)-1 || code != codes.ResourceExhausted {
				rt.Fatalf("WF-4: no room for %s (grace %s) at attempt %d, but the wait went on to attempt %d and returned %v",
					a.done.Sub(runStart), g, i+1, len(attempts), err)
			}
		}
	}
	if code == codes.ResourceExhausted {
		g := s.WakeNoCapacityGrace
		if g <= 0 || runStart.IsZero() || last.done.Sub(runStart) < g {
			rt.Fatalf("WF-3: ResourceExhausted with grace %s after a no-room run of %s", g, last.done.Sub(runStart))
		}
		// WF-4: the backoff never sleeps past the grace, so the probe that
		// ends the run starts by then (1 ms is the shortest backoff).
		if late := last.start.Sub(runStart) - g; late > time.Millisecond {
			rt.Fatalf("WF-4: the last probe started %s after the grace %s ran out", late, g)
		}
	}

	// WF-5: the RUNNING run starts at the state read of its first attempt
	// and goes on through an unknown state (a transient read error, or a
	// read cut short); the attempt whose read finds it past the running
	// budget ends the wait.
	var runningSince time.Time
	for i, a := range attempts {
		unknown := a.unavailable && !a.ctxEnded && !a.readCtxEnded && !readEndsWait(a.readErr) &&
			a.state == ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED
		switch {
		case a.unavailable && !a.readCtxEnded && a.state == ateapipb.ActorState_ACTOR_STATE_RUNNING:
			if runningSince.IsZero() {
				runningSince = a.readDone
			}
		case unknown:
		default:
			runningSince = time.Time{}
		}
		if !runningSince.IsZero() && i != len(attempts)-1 && a.readDone.Sub(runningSince) >= s.WakeRunningBudget {
			rt.Fatalf("WF-5: the RUNNING run had lasted %s (running budget %s) at attempt %d's state read, but the wait went on to attempt %d",
				a.readDone.Sub(runningSince), s.WakeRunningBudget, i+1, len(attempts))
		}
	}
	// WF-5: a timeout only once a budget is spent.
	if code == codes.Unavailable {
		runningOver := !runningSince.IsZero() && now.Sub(runningSince) >= s.WakeRunningBudget
		if elapsed < s.WakeBudget && !runningOver {
			rt.Fatalf("WF-5: timed out after %s, before the wake budget %s and the running budget %s", elapsed, s.WakeBudget, s.WakeRunningBudget)
		}
	}

	// WF-6: never past the wake budget by more than one probe.
	if elapsed > s.WakeBudget+maxProbeLatency {
		rt.Fatalf("WF-6: waited %s, past the wake budget %s plus one probe (%s)", elapsed, s.WakeBudget, maxProbeLatency)
	}
}

// readEndsWait reports whether a state read that failed with c ends the
// wait with that error, as ensureAwake's doc comment says of every read error
// but Unavailable, DeadlineExceeded and ResourceExhausted.
func readEndsWait(c codes.Code) bool {
	return c == codes.NotFound || c == codes.Internal
}

func histogramSampleCount(rt *rapid.T, m *telemetry.Metrics, family, labelKey, labelVal string) uint64 {
	mfs, err := m.Registry().Gather()
	if err != nil {
		rt.Fatalf("gather metrics: %v", err)
	}
	var total uint64
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == labelKey && lp.GetValue() == labelVal {
					total += metric.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return total
}

func TestAwaitWakePropagatesProbeCancelAndDeadlineWithLiveCallerCtx(t *testing.T) {
	s := &Server{WakeBudget: time.Minute, WakeRunningBudget: 20 * time.Second}
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			err := s.awaitWake(
				context.Background(),
				"ate-env",
				"sb-upstream-abort",
				func(context.Context) error {
					return status.Error(code, "router probe aborted")
				},
				func(context.Context) (*ateapipb.ActorStatus, error) {
					t.Fatalf("readStatus must not run when probe returns %v", code)
					return nil, nil
				},
			)
			if status.Code(err) != code {
				t.Fatalf("awaitWake err = %v, want %v", err, code)
			}
		})
	}
}

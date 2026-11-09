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
	"io"
	"log"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/internaltest/synctestrapid"
	"github.com/agent-substrate/env/internal/telemetry"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

type synctestProbeMode int

const (
	probeModeUnavailable synctestProbeMode = iota
	probeModeNoCapacity
	probeModeAwake
	probeModeCertRejected
	probeModeParked
)

const (
	synctestNoRoomMsg     = "actor ate-env/env-1 unavailable: no worker has room for the actor"
	synctestCertRejectMsg = "remote error: tls: bad certificate"
	synctestEnvID         = "env-1"
	maxConcurrentCallers  = 4
)

func newSynctestControl(bt *testing.T) *fakecontrol.Server {
	bt.Helper()
	ctrl := fakecontrol.New()
	_, err := ctrl.CreateActor(context.Background(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: DefaultAtespace,
				Name:     synctestEnvID,
			},
		},
	})
	if err != nil {
		bt.Fatalf("CreateActor: %v", err)
	}
	ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	return ctrl
}

type callerResult struct {
	err                 error
	startedAt           time.Time
	endedAt             time.Time
	canceled            bool
	canceledAt          time.Time
	hasDeadline         bool
	deadline            time.Time
	startedOnCertReject bool
}

type callerHandle struct {
	id                  int
	startedAt           time.Time
	cancel              context.CancelFunc
	canceled            bool
	canceledAt          time.Time
	hasDeadline         bool
	deadline            time.Time
	startedOnCertReject bool
	done                chan callerResult
}

type ensureAwakeSM struct {
	bt *testing.T
	rt *rapid.T

	srv           *Server
	ctrl          *fakecontrol.Server
	conn          *grpc.ClientConn
	trackedCreate bool

	// Guarded state
	mu        sync.Mutex
	probeMode synctestProbeMode

	// Callers
	nextCallerID int
	callers      []*callerHandle
	completed    []callerResult

	// State history
	everAwake          bool
	everRunning        bool
	everCrashed        bool
	everFirstContact   bool
	everCrashedContact bool
}

func newEnsureAwakeSM(bt *testing.T, rt *rapid.T) *ensureAwakeSM {
	wakeBudget := time.Duration(rapid.IntRange(5, 60).Draw(rt, "wakeBudgetSeconds")) * time.Second
	wakeRunningBudget := time.Duration(rapid.IntRange(2, 10).Draw(rt, "wakeRunningBudgetSeconds")) * time.Second
	wakeNoCapacityGrace := time.Duration(rapid.SampledFrom([]int{0, 2, 4}).Draw(rt, "wakeNoCapacityGraceSeconds")) * time.Second
	trackedCreate := rapid.Bool().Draw(rt, "trackedCreate")

	ctrl := newSynctestControl(bt)
	client := ate.NewControlClient(ctrl.Client())

	srv := New(client, "127.0.0.1:1", "")
	srv.WakeBudget = wakeBudget
	srv.WakeRunningBudget = wakeRunningBudget
	srv.WakeNoCapacityGrace = wakeNoCapacityGrace
	srv.Metrics = telemetry.NewMetrics()
	srv.RouterIdentity = "spiffe://cluster.local/ns/ate-env/sa/ate-env-api"

	if trackedCreate {
		srv.trackCreate(DefaultAtespace, synctestEnvID, time.Now())
	}

	sm := &ensureAwakeSM{
		bt:            bt,
		rt:            rt,
		srv:           srv,
		ctrl:          ctrl,
		trackedCreate: trackedCreate,
		probeMode:     probeModeUnavailable,
	}

	probeInterceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}

		sm.mu.Lock()
		mode := sm.probeMode
		sm.mu.Unlock()

		switch mode {
		case probeModeUnavailable:
			return status.Error(codes.Unavailable, "guest unreachable: connection refused")
		case probeModeNoCapacity:
			return status.Error(codes.Unavailable, synctestNoRoomMsg)
		case probeModeCertRejected:
			return status.Error(codes.Unavailable, synctestCertRejectMsg)
		case probeModeAwake:
			// Returns NotFound from the guest probe (probe process not found), which ensureAwake treats as awake!
			return status.Error(codes.NotFound, "process not found")
		case probeModeParked:
			select {
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			case <-time.After(400 * time.Millisecond):
				return status.Error(codes.Unavailable, "router parking budget expired")
			}
		default:
			return status.Error(codes.Unavailable, "unknown probe mode")
		}
	}

	conn, err := grpc.NewClient(
		"passthrough:///synctest-guest",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(probeInterceptor),
	)
	if err != nil {
		bt.Fatalf("grpc.NewClient: %v", err)
	}
	sm.conn = conn

	bt.Cleanup(func() {
		// Cancel all active callers so no goroutines linger
		for _, c := range sm.callers {
			c.cancel()
		}
		synctest.Wait()
		_ = conn.Close()
	})

	return sm
}

func (sm *ensureAwakeSM) reapCompleted() {
	var remaining []*callerHandle
	for _, c := range sm.callers {
		select {
		case res := <-c.done:
			res.canceled = c.canceled
			res.canceledAt = c.canceledAt
			res.hasDeadline = c.hasDeadline
			res.deadline = c.deadline
			res.startedOnCertReject = c.startedOnCertReject
			sm.completed = append(sm.completed, res)
		default:
			remaining = append(remaining, c)
		}
	}
	sm.callers = remaining
}

func (sm *ensureAwakeSM) StartEnsureAwake(rt *rapid.T) {
	sm.reapCompleted()
	if len(sm.callers) >= maxConcurrentCallers {
		rt.Skip("max concurrent callers already running")
	}

	now := time.Now()
	var (
		hasDeadline bool
		deadline    time.Time
	)
	ctx, cancel := context.WithCancel(context.Background())
	if rapid.IntRange(1, 5).Draw(rt, "withTimeout") == 1 {
		timeout := time.Duration(rapid.IntRange(1, 8).Draw(rt, "callerTimeoutSeconds")) * time.Second
		hasDeadline = true
		deadline = now.Add(timeout)
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeout(ctx, timeout)
		origCancel := cancel
		cancel = func() {
			cancelTimeout()
			origCancel()
		}
	}

	sm.mu.Lock()
	startedOnCertReject := sm.probeMode == probeModeCertRejected
	sm.mu.Unlock()

	h := &callerHandle{
		id:                  sm.nextCallerID,
		startedAt:           now,
		cancel:              cancel,
		hasDeadline:         hasDeadline,
		deadline:            deadline,
		startedOnCertReject: startedOnCertReject,
		done:                make(chan callerResult, 1),
	}
	sm.nextCallerID++
	sm.callers = append(sm.callers, h)

	go func() {
		err := sm.srv.ensureAwake(ctx, sm.conn, DefaultAtespace, synctestEnvID)
		h.done <- callerResult{
			err:       err,
			startedAt: h.startedAt,
			endedAt:   time.Now(),
		}
	}()

	synctest.Wait()
}

func (sm *ensureAwakeSM) CancelCaller(rt *rapid.T) {
	sm.reapCompleted()
	if len(sm.callers) == 0 {
		rt.Skip("no active callers to cancel")
	}

	idx := rapid.IntRange(0, len(sm.callers)-1).Draw(rt, "callerIdx")
	c := sm.callers[idx]
	c.canceled = true
	c.canceledAt = time.Now()
	c.cancel()

	synctest.Wait()
	sm.reapCompleted()
}

func (sm *ensureAwakeSM) SetActorState(rt *rapid.T) {
	cur := sm.ctrl.Status(synctestEnvID)
	if cur == ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED ||
		cur == ateapipb.ActorState_ACTOR_STATE_CRASHED ||
		cur == ateapipb.ActorState_ACTOR_STATE_DELETING {
		rt.Skip("actor already in terminal state")
	}

	choice := rapid.SampledFrom([]string{"SUSPENDED", "RESUMING", "RUNNING", "CRASHED", "DELETING", "Deleted"}).Draw(rt, "stateChoice")
	switch choice {
	case "SUSPENDED":
		sm.ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	case "RESUMING":
		sm.ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_RESUMING)
	case "RUNNING":
		sm.ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_RUNNING)
		sm.everRunning = true
	case "CRASHED":
		sm.ctrl.Crash(synctestEnvID, "worker crashed")
		sm.everCrashed = true
	case "DELETING":
		sm.ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_DELETING)
	case "Deleted":
		_, _ = sm.ctrl.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: DefaultAtespace, Name: synctestEnvID},
		})
	}

	synctest.Wait()
}

func (sm *ensureAwakeSM) SetProbeMode(rt *rapid.T) {
	modeChoice := rapid.SampledFrom([]synctestProbeMode{
		probeModeUnavailable,
		probeModeNoCapacity,
		probeModeAwake,
		probeModeCertRejected,
		probeModeParked,
	}).Draw(rt, "probeMode")

	sm.mu.Lock()
	sm.probeMode = modeChoice
	if modeChoice == probeModeAwake {
		sm.everAwake = true
	}
	sm.mu.Unlock()

	synctest.Wait()
}

func (sm *ensureAwakeSM) PortProxyContact(rt *rapid.T) {
	sm.mu.Lock()
	isAwake := sm.probeMode == probeModeAwake
	sm.mu.Unlock()

	cur := sm.ctrl.Status(synctestEnvID)
	isRunning := cur == ateapipb.ActorState_ACTOR_STATE_RUNNING
	isCrashed := cur == ateapipb.ActorState_ACTOR_STATE_CRASHED

	canObserveOK := isAwake || isRunning
	canObserveCrashed := isCrashed

	if !canObserveOK && !canObserveCrashed {
		rt.Skip("actor neither running/awake nor crashed")
	}

	if canObserveOK && (!canObserveCrashed || rapid.Bool().Draw(rt, "preferContactOK")) {
		sm.srv.ObserveFirstContact(DefaultAtespace, synctestEnvID)
		sm.everFirstContact = true
	} else {
		sm.srv.ObserveCrashedContact(DefaultAtespace, synctestEnvID)
		sm.everCrashedContact = true
	}

	synctest.Wait()
}

func (sm *ensureAwakeSM) AdvanceClock(rt *rapid.T) {
	d := rapid.SampledFrom([]time.Duration{
		200 * time.Millisecond,
		1 * time.Second,
		3 * time.Second,
		10 * time.Second,
		25 * time.Second,
	}).Draw(rt, "advanceDur")

	time.Sleep(d)
	synctest.Wait()
}

func (sm *ensureAwakeSM) Check(rt *rapid.T) {
	sm.reapCompleted()

	now := time.Now()

	// Invariant 1 & 2: Completed and active caller checks
	certRejectFastFails := 0
	for _, res := range sm.completed {
		code := status.Code(res.err)
		dur := res.endedAt.Sub(res.startedAt)

		if res.startedOnCertReject {
			certRejectFastFails++
			msg := status.Convert(res.err).Message()
			if code != codes.FailedPrecondition || !strings.Contains(msg, sm.srv.RouterIdentity) || dur != 0 {
				rt.Fatalf("cert-rejected probe did not fail fast: dur=%v code=%v err=%v", dur, code, res.err)
			}
		}

		if dur > sm.srv.WakeBudget+5*time.Second {
			rt.Fatalf("caller exceeded wake budget: dur=%v, budget=%v", dur, sm.srv.WakeBudget)
		}

		switch code {
		case codes.OK:
			if !sm.everAwake {
				rt.Fatalf("caller succeeded with OK but probe was never awake")
			}
		case codes.FailedPrecondition:
			// Either crashed or cert rejected
			if !sm.everCrashed && !strings.Contains(status.Convert(res.err).Message(), "rejected") {
				rt.Fatalf("unexpected FailedPrecondition without crash or cert reject: %v", res.err)
			}
		case codes.NotFound:
			// Either DELETING or deleted (control plane NotFound)
		case codes.ResourceExhausted:
			if sm.srv.WakeNoCapacityGrace <= 0 {
				rt.Fatalf("ResourceExhausted returned when WakeNoCapacityGrace was 0")
			}
		case codes.Unavailable:
			runningBudgetReached := sm.everRunning && dur >= sm.srv.WakeRunningBudget
			if dur < sm.srv.WakeBudget && !runningBudgetReached {
				rt.Fatalf("WF-5: caller timed out with Unavailable after %v before WakeBudget=%v and WakeRunningBudget=%v (everRunning=%v)",
					dur, sm.srv.WakeBudget, sm.srv.WakeRunningBudget, sm.everRunning)
			}
		case codes.Canceled:
			if !res.canceled || !res.endedAt.Equal(res.canceledAt) {
				rt.Fatalf("WF-7: caller returned Canceled at %v (canceled=%v canceledAt=%v): %v",
					res.endedAt, res.canceled, res.canceledAt, res.err)
			}
		case codes.DeadlineExceeded:
			if !res.hasDeadline || res.endedAt.Before(res.deadline) {
				rt.Fatalf("WF-7: caller returned DeadlineExceeded at %v (hasDeadline=%v deadline=%v): %v",
					res.endedAt, res.hasDeadline, res.deadline, res.err)
			}
		default:
			rt.Fatalf("unexpected caller return code %v: %v", code, res.err)
		}
	}

	// Any active caller running longer than WakeBudget + 2s must have completed
	for _, c := range sm.callers {
		if elapsed := now.Sub(c.startedAt); elapsed > sm.srv.WakeBudget+2*time.Second {
			rt.Logf("active caller still stuck past WakeBudget: elapsed=%v, budget=%v", elapsed, sm.srv.WakeBudget)
			rt.Fatalf("active caller stuck past WakeBudget")
		}
	}

	// Invariant 3: create_to_ready telemetry checks
	var totalCreateToReady uint64
	var createToReadyOK uint64

	mfs, err := sm.srv.Metrics.Registry().Gather()
	if err != nil {
		rt.Fatalf("gathering metrics failed: %v", err)
	}

	for _, mf := range mfs {
		if mf.GetName() == telemetry.MetricCreateToReadySeconds {
			for _, m := range mf.GetMetric() {
				var statusVal string
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "status" {
						statusVal = lp.GetValue()
					}
				}
				cnt := m.GetHistogram().GetSampleCount()
				totalCreateToReady += cnt
				if statusVal == telemetry.ReadyStatusOK {
					createToReadyOK += cnt
				}
			}
		}
	}

	if !sm.everAwake && !sm.everFirstContact && createToReadyOK > 0 {
		rt.Fatalf("create_to_ready ok recorded without awake or first contact")
	}

	if !sm.trackedCreate {
		if totalCreateToReady != 0 {
			rt.Logf("totalCreateToReady=%d, want 0 when not tracked", totalCreateToReady)
			rt.Fatalf("create_to_ready observed when not tracked")
		}
	} else {
		if totalCreateToReady > 1 {
			rt.Logf("totalCreateToReady=%d, want <= 1", totalCreateToReady)
			rt.Fatalf("create_to_ready observed more than once")
		}

		// Check if any event should have triggered create_to_ready completion
		anyOK := sm.everFirstContact
		anyCrashed := sm.everCrashedContact
		for _, res := range sm.completed {
			if res.err == nil {
				anyOK = true
			}
			if status.Code(res.err) == codes.FailedPrecondition && strings.Contains(status.Convert(res.err).Message(), "crashed") {
				anyCrashed = true
			}
		}

		if anyOK || anyCrashed {
			if totalCreateToReady != 1 {
				rt.Logf("expected create_to_ready == 1, got %d (anyOK=%v, anyCrashed=%v)", totalCreateToReady, anyOK, anyCrashed)
				rt.Fatalf("create_to_ready was not recorded exactly once upon completion")
			}
		}
	}

	// Invariant 4: wake_wait sample checks
	var totalWakeWait uint64
	for _, mf := range mfs {
		if mf.GetName() == telemetry.MetricWakeWaitSeconds {
			for _, m := range mf.GetMetric() {
				totalWakeWait += m.GetHistogram().GetSampleCount()
			}
		}
	}

	// Total wake_wait count can never exceed callers that actually entered a wake wait
	maxExpectedWakeWait := uint64(len(sm.completed) - certRejectFastFails)
	if totalWakeWait > maxExpectedWakeWait {
		rt.Fatalf("wake_wait count %d exceeds expected max %d (completed=%d certRejectFastFails=%d)",
			totalWakeWait, maxExpectedWakeWait, len(sm.completed), certRejectFastFails)
	}
}

// A transient GetActor error during a wake goes through the real chain
// (fakecontrol.GetActor, ate.Client.Get, toGRPCError) with its code kept, so
// ensureAwake leaves the state unknown, probes again, and wakes once the
// guest answers. The first failed read is logged once, with its code.
func TestEnsureAwakeRetriesTransientGetActorErrors(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			var logs strings.Builder
			log.SetOutput(&logs)
			defer log.SetOutput(io.Discard)
			synctest.Test(t, func(bt *testing.T) {
				ctrl := newSynctestControl(bt)
				ctrl.FailGets(2, code)
				srv := New(ate.NewControlClient(ctrl.Client()), "127.0.0.1:1", "")
				srv.WakeBudget = time.Minute
				srv.WakeNoCapacityGrace = 10 * time.Second

				var probeMu sync.Mutex
				probes := 0
				probeInterceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
					probeMu.Lock()
					defer probeMu.Unlock()
					probes++
					if probes >= 3 {
						return status.Error(codes.NotFound, "process not found") // the guest answers
					}
					return status.Error(codes.Unavailable, "guest unreachable: 503")
				}
				conn, err := grpc.NewClient(
					"passthrough:///synctest-guest",
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithUnaryInterceptor(probeInterceptor),
				)
				if err != nil {
					bt.Fatalf("grpc.NewClient: %v", err)
				}
				bt.Cleanup(func() { _ = conn.Close() })

				if err := srv.ensureAwake(context.Background(), conn, DefaultAtespace, synctestEnvID); err != nil {
					bt.Fatalf("ensureAwake = %v after %d probes, want the guest's answer (nil)", err, probes)
				}
				if probes != 3 {
					bt.Fatalf("probes = %d, want 3: two failed reads, then the guest's answer", probes)
				}
				// Both injected failures were spent on the wake's reads.
				if st, err := srv.actorState(context.Background(), DefaultAtespace, synctestEnvID); err != nil || st != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
					bt.Fatalf("state after the wake = %s, %v; want SUSPENDED, nil", st, err)
				}
			})
			out := logs.String()
			if n := strings.Count(out, "state read failed"); n != 1 {
				t.Fatalf("logged %d state-read failures, want 1 for 2 failed reads:\n%s", n, out)
			}
			if want := "failed with " + code.String() + " at attempt 1;"; !strings.Contains(out, want) {
				t.Fatalf("log does not contain %q (the code must survive ate.Client.Get and toGRPCError):\n%s", want, out)
			}
		})
	}
}

func TestEnsureAwakeSynctestStateMachine(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	synctestrapid.Repeat(t, func(bt *testing.T, rt *rapid.T) rapid.StateMachine {
		return newEnsureAwakeSM(bt, rt)
	})
}

// TestEnsureAwakeExactBudgetsInSynctest verifies that the production 5-minute
// DefaultWakeBudget and 20-second DefaultWakeRunningBudget behave deterministically
// in microseconds under testing/synctest bubbles.
func TestEnsureAwakeExactBudgetsInSynctest(t *testing.T) {
	prevOut := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(prevOut) })
	t.Run("LateRunningGetsRemainingOverallBudget", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			ctrl := newSynctestControl(bt)
			client := ate.NewControlClient(ctrl.Client())
			srv := New(client, "127.0.0.1:1", "")
			srv.Metrics = telemetry.NewMetrics()

			probeInterceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return status.Error(codes.Unavailable, "guest unreachable: 503")
			}
			conn, err := grpc.NewClient(
				"passthrough:///synctest-guest",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(probeInterceptor),
			)
			if err != nil {
				bt.Fatalf("grpc.NewClient: %v", err)
			}
			bt.Cleanup(func() { _ = conn.Close() })

			resCh := make(chan callerResult, 1)
			ctx, cancel := context.WithCancel(context.Background())
			bt.Cleanup(cancel)

			start := time.Now()
			go func() {
				err := srv.ensureAwake(ctx, conn, DefaultAtespace, synctestEnvID)
				resCh <- callerResult{err: err, startedAt: start, endedAt: time.Now()}
			}()

			synctest.Wait()

			// Sleep for 4m50s in fake time while SUSPENDED
			time.Sleep(4*time.Minute + 50*time.Second)
			synctest.Wait()

			// Flip to RUNNING at 4m50s
			ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			synctest.Wait()

			// The overall budget is 5m. Even though WakeRunningBudget is 20s,
			// remaining overall budget is only 10s (5m00s - 4m50s).
			// Sleep 12s, which crosses 5m00s.
			time.Sleep(12 * time.Second)
			synctest.Wait()

			select {
			case res := <-resCh:
				if status.Code(res.err) != codes.Unavailable {
					bt.Fatalf("want Unavailable, got %v", res.err)
				}
				dur := res.endedAt.Sub(res.startedAt)
				// Should have ended at 5m00s (or within backoff interval ~5m02s)
				if dur < 5*time.Minute || dur > 5*time.Minute+3*time.Second {
					bt.Fatalf("expected caller to spend 5m overall budget, spent %v", dur)
				}
				if !strings.Contains(status.Convert(res.err).Message(), "wake budget") {
					bt.Fatalf("expected message to cite wake budget, got %q", status.Convert(res.err).Message())
				}
			default:
				bt.Fatalf("caller did not complete after spending overall budget")
			}
		})
	})

	t.Run("RunningBudgetExpiresBeforeOverallBudget", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			ctrl := newSynctestControl(bt)
			client := ate.NewControlClient(ctrl.Client())
			srv := New(client, "127.0.0.1:1", "")
			srv.Metrics = telemetry.NewMetrics()

			probeInterceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return status.Error(codes.Unavailable, "guest unreachable: 503")
			}
			conn, err := grpc.NewClient(
				"passthrough:///synctest-guest",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(probeInterceptor),
			)
			if err != nil {
				bt.Fatalf("grpc.NewClient: %v", err)
			}
			bt.Cleanup(func() { _ = conn.Close() })

			resCh := make(chan callerResult, 1)
			ctx, cancel := context.WithCancel(context.Background())
			bt.Cleanup(cancel)

			start := time.Now()
			go func() {
				err := srv.ensureAwake(ctx, conn, DefaultAtespace, synctestEnvID)
				resCh <- callerResult{err: err, startedAt: start, endedAt: time.Now()}
			}()

			synctest.Wait()

			// Sleep for 3m00s while SUSPENDED
			time.Sleep(3 * time.Minute)
			synctest.Wait()

			// Flip to RUNNING at 3m00s
			ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			synctest.Wait()

			// Advance fake time past 20s running budget (e.g. 22s -> 3m22s)
			time.Sleep(22 * time.Second)
			synctest.Wait()

			select {
			case res := <-resCh:
				if status.Code(res.err) != codes.Unavailable {
					bt.Fatalf("want Unavailable, got %v", res.err)
				}
				dur := res.endedAt.Sub(res.startedAt)
				// Should have ended at ~3m20s (NOT 5m00s!)
				if dur < 3*time.Minute+20*time.Second || dur > 3*time.Minute+23*time.Second {
					bt.Fatalf("expected caller to spend 20s running budget at 3m20s, spent %v", dur)
				}
				if !strings.Contains(status.Convert(res.err).Message(), "running budget") {
					bt.Fatalf("expected message to cite running budget, got %q", status.Convert(res.err).Message())
				}
			default:
				bt.Fatalf("caller did not complete after spending running budget")
			}
		})
	})

	t.Run("SucceedsInsideRunningBudget", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			ctrl := newSynctestControl(bt)
			client := ate.NewControlClient(ctrl.Client())
			srv := New(client, "127.0.0.1:1", "")
			srv.Metrics = telemetry.NewMetrics()

			var probeMu sync.Mutex
			probeAwake := false

			probeInterceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				probeMu.Lock()
				awake := probeAwake
				probeMu.Unlock()
				if awake {
					return status.Error(codes.NotFound, "process not found")
				}
				return status.Error(codes.Unavailable, "guest unreachable: 503")
			}
			conn, err := grpc.NewClient(
				"passthrough:///synctest-guest",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(probeInterceptor),
			)
			if err != nil {
				bt.Fatalf("grpc.NewClient: %v", err)
			}
			bt.Cleanup(func() { _ = conn.Close() })

			resCh := make(chan callerResult, 1)
			ctx, cancel := context.WithCancel(context.Background())
			bt.Cleanup(cancel)

			start := time.Now()
			go func() {
				err := srv.ensureAwake(ctx, conn, DefaultAtespace, synctestEnvID)
				resCh <- callerResult{err: err, startedAt: start, endedAt: time.Now()}
			}()

			synctest.Wait()

			// Sleep for 3m00s while SUSPENDED
			time.Sleep(3 * time.Minute)
			synctest.Wait()

			// Flip to RUNNING at 3m00s
			ctrl.SetState(synctestEnvID, ateapipb.ActorState_ACTOR_STATE_RUNNING)
			synctest.Wait()

			// Wait 15s (at 3m15s, within 20s running budget), then guest starts listening
			time.Sleep(15 * time.Second)
			synctest.Wait()

			probeMu.Lock()
			probeAwake = true
			probeMu.Unlock()

			// Next backoff attempt will hit probeAwake and succeed
			time.Sleep(3 * time.Second)
			synctest.Wait()

			select {
			case res := <-resCh:
				if res.err != nil {
					bt.Fatalf("want success (nil), got %v", res.err)
				}
				dur := res.endedAt.Sub(res.startedAt)
				if dur < 3*time.Minute+15*time.Second || dur > 3*time.Minute+20*time.Second {
					bt.Fatalf("expected caller to succeed at ~3m15s, spent %v", dur)
				}
			default:
				bt.Fatalf("caller did not succeed inside running budget")
			}
		})
	})

	t.Run("CanceledOrExpiredDuringParkedProbeDoesNotMarkAwake", func(t *testing.T) {
		synctest.Test(t, func(bt *testing.T) {
			ctrl := newSynctestControl(bt)
			client := ate.NewControlClient(ctrl.Client())
			srv := New(client, "127.0.0.1:1", "")
			srv.Metrics = telemetry.NewMetrics()
			srv.trackCreate(DefaultAtespace, synctestEnvID, time.Now())

			probeInterceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				<-ctx.Done()
				return status.FromContextError(ctx.Err()).Err()
			}
			conn, err := grpc.NewClient(
				"passthrough:///synctest-guest",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(probeInterceptor),
			)
			if err != nil {
				bt.Fatalf("grpc.NewClient: %v", err)
			}
			bt.Cleanup(func() { _ = conn.Close() })

			// 1. First call with a 2-second timeout
			ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelTimeout()
			errTimeoutCh := make(chan error, 1)
			go func() {
				errTimeoutCh <- srv.ensureAwake(ctxTimeout, conn, DefaultAtespace, synctestEnvID)
			}()
			synctest.Wait()

			time.Sleep(2 * time.Second)
			synctest.Wait()

			select {
			case err := <-errTimeoutCh:
				if status.Code(err) != codes.DeadlineExceeded {
					bt.Fatalf("want DeadlineExceeded, got %v", err)
				}
			default:
				bt.Fatalf("caller did not return on deadline exceeded")
			}

			// 2. Second call with context.WithCancel, cancel after 100ms
			ctxCancel, cancelFunc := context.WithCancel(context.Background())
			defer cancelFunc()
			errCancelCh := make(chan error, 1)
			go func() {
				errCancelCh <- srv.ensureAwake(ctxCancel, conn, DefaultAtespace, synctestEnvID)
			}()
			synctest.Wait()

			time.Sleep(100 * time.Millisecond)
			cancelFunc()
			synctest.Wait()

			select {
			case err := <-errCancelCh:
				if status.Code(err) != codes.Canceled {
					bt.Fatalf("want Canceled, got %v", err)
				}
			default:
				bt.Fatalf("caller did not return on cancel")
			}

			// 3. Assert both MetricCreateToReadySeconds and MetricWakeWaitSeconds with outcome="woke" have 0 samples
			mfs, err := srv.Metrics.Registry().Gather()
			if err != nil {
				bt.Fatalf("Gather: %v", err)
			}
			for _, mf := range mfs {
				if mf.GetName() == telemetry.MetricCreateToReadySeconds {
					for _, m := range mf.GetMetric() {
						if cnt := m.GetHistogram().GetSampleCount(); cnt > 0 {
							bt.Fatalf("expected 0 create_to_ready samples, got %d", cnt)
						}
					}
				}
				if mf.GetName() == telemetry.MetricWakeWaitSeconds {
					for _, m := range mf.GetMetric() {
						var outcome string
						for _, lp := range m.GetLabel() {
							if lp.GetName() == "outcome" {
								outcome = lp.GetValue()
							}
						}
						if outcome == telemetry.WakeOutcomeWoke {
							if cnt := m.GetHistogram().GetSampleCount(); cnt > 0 {
								bt.Fatalf("expected 0 wake_wait woke samples, got %d", cnt)
							}
						}
					}
				}
			}
		})
	})
}

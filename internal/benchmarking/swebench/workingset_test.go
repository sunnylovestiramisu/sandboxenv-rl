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

package swebench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

type wsFakeBackend struct {
	creates         atomic.Int64
	suspends        atomic.Int64
	deletes         atomic.Int64
	forks           atomic.Int64
	suspWait        time.Duration
	failNextCreate  atomic.Bool
	failNextFork    atomic.Bool
	failNextSuspend atomic.Bool
	failNextDelete  atomic.Bool
	// suspendGate, when set, holds each Suspend until the channel is closed;
	// gateIDSubstr narrows the hold to the sandboxes whose id contains it.
	suspendGate  chan struct{}
	gateIDSubstr string
	// onCreate, when set, receives one value per successful Create.
	onCreate chan struct{}
	// deleteGate, when set, holds each sandbox Delete until the channel is closed.
	deleteGate chan struct{}
	// createGate, when set, holds each Create whose id contains
	// createGateIDSubstr until the channel is closed or ctx expires, as a
	// slow create under a cancelled run does; createHeld, when set, receives
	// one value per Create the gate holds.
	createGate         chan struct{}
	createGateIDSubstr string
	createHeld         chan struct{}

	mu         sync.Mutex
	deletedIDs []string
}

func (b *wsFakeBackend) Create(ctx context.Context, id, _ string) (Sandbox, error) {
	if b.failNextCreate.CompareAndSwap(true, false) {
		return nil, errors.New("injected create error")
	}
	if b.createGate != nil && strings.Contains(id, b.createGateIDSubstr) {
		if b.createHeld != nil {
			b.createHeld <- struct{}{}
		}
		select {
		case <-b.createGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	b.creates.Add(1)
	if b.onCreate != nil {
		b.onCreate <- struct{}{}
	}
	return &wsFakeSandbox{id: id, b: b}, nil
}

func (b *wsFakeBackend) Delete(_ context.Context, id string) error {
	b.deletes.Add(1)
	b.mu.Lock()
	b.deletedIDs = append(b.deletedIDs, id)
	b.mu.Unlock()
	return nil
}

func (b *wsFakeBackend) Fork(_ context.Context, _, childID, _ string) (Sandbox, error) {
	if b.failNextFork.CompareAndSwap(true, false) {
		return nil, errors.New("injected fork error")
	}
	b.forks.Add(1)
	return &wsFakeSandbox{id: childID, b: b}, nil
}

type wsFakeSandbox struct {
	id string
	b  *wsFakeBackend
}

func (s *wsFakeSandbox) Run(_ context.Context, _ string, _ time.Duration) (Result, error) {
	return Result{ExitCode: 0, Stdout: "ok\n"}, nil
}

func (s *wsFakeSandbox) WriteFile(_ context.Context, _ string, _ []byte, _ fs.FileMode) error {
	return nil
}

func (s *wsFakeSandbox) Suspend(_ context.Context) error {
	if s.b.suspendGate != nil && strings.Contains(s.id, s.b.gateIDSubstr) {
		<-s.b.suspendGate
	}
	if s.b.suspWait > 0 {
		time.Sleep(s.b.suspWait)
	}
	if s.b.failNextSuspend.CompareAndSwap(true, false) {
		return errors.New("injected async suspend error")
	}
	s.b.suspends.Add(1)
	return nil
}

func (s *wsFakeSandbox) Delete(_ context.Context) error {
	if s.b.deleteGate != nil {
		<-s.b.deleteGate
	}
	if s.b.failNextDelete.CompareAndSwap(true, false) {
		return errors.New("injected delete error")
	}
	s.b.deletes.Add(1)
	s.b.mu.Lock()
	s.b.deletedIDs = append(s.b.deletedIDs, s.id)
	s.b.mu.Unlock()
	return nil
}

func TestWorkingSetHitMissAndChurnAcrossPolicies(t *testing.T) {
	ctx := context.Background()

	// 1. suspend-sync: Step 1 creates 2 (misses), Step 2 reuses 1 (hit) and creates 1 (miss).
	bSync := &wsFakeBackend{}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-sync",
		Backend:    bSync,
		ParkPolicy: schema.ParkSuspendSync,
		MaxParked:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	ws.BeginStep()
	e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil || e1.Hit {
		t.Fatalf("step 1 acquire a: hit=%v err=%v", e1.Hit, err)
	}
	e2, err := ws.Acquire(ctx, 1, 1, "inst-b", "img-b")
	if err != nil || e2.Hit {
		t.Fatalf("step 1 acquire b: hit=%v err=%v", e2.Hit, err)
	}
	if err := ws.Release(ctx, e1); err != nil {
		t.Fatal(err)
	}
	if err := ws.Release(ctx, e2); err != nil {
		t.Fatal(err)
	}
	st1 := ws.StepStats(0, 0)
	if st1.Hits != 0 || st1.Misses != 2 || st1.StandingPoolSize != 2 {
		t.Fatalf("step 1 stats = %+v", st1)
	}

	ws.BeginStep()
	e3, err := ws.Acquire(ctx, 2, 0, "inst-a", "img-a")
	if err != nil || !e3.Hit || !e3.WasParked {
		t.Fatalf("step 2 acquire a: want hit, got hit=%v wasParked=%v err=%v", e3.Hit, e3.WasParked, err)
	}
	e4, err := ws.Acquire(ctx, 2, 1, "inst-c", "img-c")
	if err != nil || e4.Hit {
		t.Fatalf("step 2 acquire c: want miss, got hit=%v err=%v", e4.Hit, err)
	}
	if err := ws.Release(ctx, e3); err != nil {
		t.Fatal(err)
	}
	if err := ws.Release(ctx, e4); err != nil {
		t.Fatal(err)
	}
	st2 := ws.StepStats(0, 0)
	if st2.Hits != 1 || st2.Misses != 1 || st2.HitRate != 0.5 || st2.StandingPoolSize != 2 || st2.Retired != 1 {
		t.Fatalf("step 2 stats = %+v", st2)
	}
	if err := ws.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// 2. delete policy: 0 hits on step 2, standing pool stays 0.
	bDel := &wsFakeBackend{}
	wsDel, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-del",
		Backend:    bDel,
		ParkPolicy: schema.ParkDelete,
	})
	if err != nil {
		t.Fatal(err)
	}
	d1, _ := wsDel.Acquire(ctx, 1, 0, "inst-a", "img-a")
	_ = wsDel.Release(ctx, d1)
	if wsDel.StandingPoolSize() != 0 {
		t.Fatalf("delete standing pool = %d, want 0", wsDel.StandingPoolSize())
	}
	d2, _ := wsDel.Acquire(ctx, 2, 0, "inst-a", "img-a")
	if d2.Hit {
		t.Fatal("delete policy must not produce working-set hits")
	}
	_ = wsDel.Release(ctx, d2)

	// 3. fork policy: step 1 creates golden base + forks child; step 2 forks from golden base (hit).
	bFork := &wsFakeBackend{}
	wsFork, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-fork",
		Backend:    bFork,
		ParkPolicy: schema.ParkFork,
	})
	if err != nil {
		t.Fatal(err)
	}
	f1, err := wsFork.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil || f1.Hit || !f1.IsForkChild {
		t.Fatalf("fork step 1 = %+v, %v", f1, err)
	}
	_ = wsFork.Release(ctx, f1)
	f2, err := wsFork.Acquire(ctx, 2, 0, "inst-a", "img-a")
	if err != nil || !f2.Hit || !f2.IsForkChild {
		t.Fatalf("fork step 2 = %+v, %v", f2, err)
	}
	_ = wsFork.Release(ctx, f2)
	if bFork.creates.Load() != 1 || bFork.forks.Load() != 2 {
		t.Fatalf("fork backend creates=%d forks=%d, want 1 create and 2 forks", bFork.creates.Load(), bFork.forks.Load())
	}
	_ = wsFork.Close(ctx)
}

func TestWorkingSetPrewarmAndSuspendAsyncOverlap(t *testing.T) {
	ctx := context.Background()
	b := &wsFakeBackend{suspWait: 5 * time.Millisecond}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-async",
		Backend:    b,
		ParkPolicy: schema.ParkSuspendAsync,
	})
	if err != nil {
		t.Fatal(err)
	}

	ws.BeginStep()
	e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.Release(ctx, e1); err != nil {
		t.Fatal(err)
	}
	// Prewarm img-b for step 2 while we pause between steps.
	ws.Prewarm(ctx, 2, []string{"img-b"})
	barrierAt := time.Now()
	time.Sleep(15 * time.Millisecond)
	resumeAt := time.Now()

	overlapMS, err := ws.AwaitAsyncSuspends(ctx, barrierAt, resumeAt)
	if err != nil || overlapMS <= 0 {
		t.Fatalf("AwaitAsyncSuspends = (%v, %v), want >0 overlap", overlapMS, err)
	}
	leadMS, err := ws.WaitPrewarm(ctx, 2, resumeAt)
	if err != nil || leadMS <= 0 {
		t.Fatalf("WaitPrewarm = (%v, %v), want >0 lead margin", leadMS, err)
	}

	// Step 2 should get a hit on both img-a and prewarmed img-b!
	ws.BeginStep()
	hA, err := ws.Acquire(ctx, 2, 0, "inst-a", "img-a")
	if err != nil || !hA.Hit {
		t.Fatalf("step 2 img-a hit=%v err=%v", hA.Hit, err)
	}
	hB, err := ws.Acquire(ctx, 2, 1, "inst-b", "img-b")
	if err != nil || !hB.Hit {
		t.Fatalf("step 2 prewarmed img-b hit=%v err=%v", hB.Hit, err)
	}
	_ = ws.Release(ctx, hA)
	_ = ws.Release(ctx, hB)
	_, _ = ws.AwaitAsyncSuspends(ctx, time.Now(), time.Now())
	_ = ws.Close(ctx)
}

func TestRunScheduledStepsRecordsLabelsAndTrajectoryMix(t *testing.T) {
	ctx := context.Background()
	fb := &wsFakeBackend{}
	var buf bytes.Buffer
	rec := NewRecorder(&buf)

	tA := &Trajectory{Name: "traj-a", Steps: []Step{{Name: "a", Run: "true"}}}
	tB := &Trajectory{Name: "traj-b", Steps: []Step{{Name: "b", Run: "true"}}}
	insts := []Instance{
		{InstanceID: "inst-1", Image: "reg/img-1@" + goodDigest},
		{InstanceID: "inst-2", Image: "reg/img-2@" + goodDigest},
	}

	sum, err := Run(ctx, Config{
		RunID:        "ws-mix",
		Backend:      fb,
		Instances:    insts,
		Trajectories: []*Trajectory{tA, tB},
		Mode:         ModeSteps,
		Park:         ParkSuspendSync,
		Steps:        2,
		TasksPerStep: 2,
		GroupSize:    2,
		Width:        2,
		Seed:         42,
		Labels:       map[string]string{"arm": "candidate"},
		Recorder:     rec,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sum.Tasks != 8 || sum.Failed != 0 {
		t.Fatalf("Summary = %+v, want 8 tasks and 0 failed", sum)
	}

	evs, err := ReadEvents(&buf)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	seenTraj := map[string]int{}
	var runLabels map[string]string
	for _, ev := range evs {
		switch ev.Event {
		case EventTask:
			seenTraj[ev.Name]++
		case EventRun:
			runLabels = ev.Labels
		}
	}
	if seenTraj["traj-a"] == 0 || seenTraj["traj-b"] == 0 {
		t.Fatalf("expected both trajectories in mix to be drawn, got %v", seenTraj)
	}
	if runLabels["arm"] != "candidate" || runLabels["trajectory"] != "traj-a,traj-b" || runLabels["mode"] != "steps" {
		t.Fatalf("EventRun Labels = %v, want arm=candidate, trajectory=traj-a,traj-b, mode=steps", runLabels)
	}
}

func TestWorkingSetAcquireAsyncSuspendErrorDeletesAndRecreates(t *testing.T) {
	ctx := context.Background()
	b := &wsFakeBackend{suspWait: 2 * time.Millisecond}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-async-err",
		Backend:    b,
		ParkPolicy: schema.ParkSuspendAsync,
	})
	if err != nil {
		t.Fatal(err)
	}

	ws.BeginStep()
	e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil || e1.Hit {
		t.Fatalf("step 1 acquire: entry=%+v err=%v", e1, err)
	}
	b.failNextSuspend.Store(true)
	if err := ws.Release(ctx, e1); err != nil {
		t.Fatalf("step 1 release: %v", err)
	}

	// Step 2: Acquire hits e1 whose background suspend fails; it must delete e1,
	// increment retired counters, and fall back to creating a fresh sandbox.
	ws.BeginStep()
	e2, err := ws.Acquire(ctx, 2, 0, "inst-a", "img-a")
	if err != nil {
		t.Fatalf("step 2 acquire returned error instead of recreating: %v", err)
	}
	if e2.SandboxID == e1.SandboxID || e2.Hit || e2.WasParked {
		t.Fatalf("step 2 acquire = %+v, want fresh sandbox (Hit=false, WasParked=false, id != %s)", e2, e1.SandboxID)
	}
	if b.creates.Load() != 2 {
		t.Fatalf("backend creates = %d, want 2", b.creates.Load())
	}
	if err := ws.awaitRetires(ctx); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	deleted := slices.Clone(b.deletedIDs)
	b.mu.Unlock()
	if !slices.Equal(deleted, []string{e1.SandboxID}) {
		t.Fatalf("deleted sandboxes = %v, want [%s]", deleted, e1.SandboxID)
	}
	st2 := ws.StepStats(0, 0)
	if st2.Hits != 0 || st2.Misses != 1 || st2.Created != 1 || st2.Reused != 0 || st2.Retired != 1 || st2.StandingPoolSize != 0 {
		t.Fatalf("step 2 stats = %+v, want Hits=0 Misses=1 Created=1 Reused=0 Retired=1 StandingPoolSize=0", st2)
	}
	sum := ws.SummaryStats(0, 0)
	if sum.Hits != 0 || sum.Misses != 2 || sum.Created != 2 || sum.Reused != 0 || sum.Retired != 1 {
		t.Fatalf("summary stats = %+v, want Hits=0 Misses=2 Created=2 Reused=0 Retired=1", sum)
	}
	_, _ = ws.AwaitAsyncSuspends(ctx, time.Time{}, time.Time{})

	// Step 2b: Park two sandboxes for img-a where the first fails async suspend and
	// the second succeeds; Step 3 Acquire must delete+retire the first and reuse the second.
	e2b, err := ws.Acquire(ctx, 2, 1, "inst-a2", "img-a")
	if err != nil || e2b.Hit {
		t.Fatalf("step 2 second acquire: entry=%+v err=%v", e2b, err)
	}
	b.failNextSuspend.Store(true)
	if err := ws.Release(ctx, e2); err != nil {
		t.Fatal(err)
	}
	// Wait for e2's suspend to consume failNextSuspend before e2b's starts.
	<-e2.asyncDone
	if err := ws.Release(ctx, e2b); err != nil {
		t.Fatal(err)
	}
	_, _ = ws.AwaitAsyncSuspends(ctx, time.Time{}, time.Time{})

	ws.BeginStep()
	e3, err := ws.Acquire(ctx, 3, 0, "inst-a", "img-a")
	if err != nil {
		t.Fatalf("step 3 acquire: %v", err)
	}
	if !e3.Hit || e3.SandboxID != e2b.SandboxID {
		t.Fatalf("step 3 acquire = %+v, want Hit=true reusing %s", e3, e2b.SandboxID)
	}
	st3 := ws.StepStats(0, 0)
	if st3.Hits != 1 || st3.Misses != 0 || st3.Reused != 1 || st3.Retired != 1 {
		t.Fatalf("step 3 stats = %+v, want Hits=1 Misses=0 Reused=1 Retired=1", st3)
	}
	if err := ws.Release(ctx, e3); err != nil {
		t.Fatal(err)
	}
	_, _ = ws.AwaitAsyncSuspends(ctx, time.Time{}, time.Time{})
	if err := ws.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkingSetAcquireForkCountsBaseAndChildCreations(t *testing.T) {
	ctx := context.Background()
	b := &wsFakeBackend{}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-fork-counts",
		Backend:    b,
		ParkPolicy: schema.ParkFork,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Cold base Create failure must not increment created counters.
	ws.BeginStep()
	b.failNextCreate.Store(true)
	if _, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a"); err == nil {
		t.Fatal("expected error when cold base Create fails")
	}
	if got := ws.StepStats(0, 0); got.Created != 0 || got.Misses != 1 {
		t.Fatalf("after failed base Create: StepStats = %+v, want Created=0 Misses=1", got)
	}

	// 2. Cold base Create succeeds, child Fork fails: created must be 1 (for baseSB).
	ws.BeginStep()
	b.failNextFork.Store(true)
	if _, err := ws.Acquire(ctx, 2, 0, "inst-b", "img-b"); err == nil {
		t.Fatal("expected error when child Fork fails")
	}
	if got := ws.StepStats(0, 0); got.Created != 1 || got.Misses != 1 {
		t.Fatalf("after base Create + failed child Fork: StepStats = %+v, want Created=1 Misses=1", got)
	}

	// 3. Cold base Create + child Fork both succeed: created must be 2 (baseSB + childSB).
	ws.BeginStep()
	c1, err := ws.Acquire(ctx, 3, 0, "inst-c", "img-c")
	if err != nil || c1.Hit || !c1.IsForkChild {
		t.Fatalf("step 3 fork acquire = %+v, %v", c1, err)
	}
	if got := ws.StepStats(0, 0); got.Created != 2 || got.Misses != 1 || got.Hits != 0 {
		t.Fatalf("step 3 cold fork StepStats = %+v, want Created=2 Misses=1 Hits=0", got)
	}
	if err := ws.Release(ctx, c1); err != nil {
		t.Fatal(err)
	}

	// 4. Warm hit on img-c forks a new child: created must increment by 1 for childSB.
	ws.BeginStep()
	c2, err := ws.Acquire(ctx, 4, 0, "inst-c", "img-c")
	if err != nil || !c2.Hit || !c2.IsForkChild {
		t.Fatalf("step 4 warm fork acquire = %+v, %v", c2, err)
	}
	if got := ws.StepStats(0, 0); got.Created != 1 || got.Hits != 1 || got.Reused != 1 {
		t.Fatalf("step 4 warm fork StepStats = %+v, want Created=1 Hits=1 Reused=1", got)
	}
	if err := ws.Release(ctx, c2); err != nil {
		t.Fatal(err)
	}

	// 5. Warm hit on img-c whose child Fork fails: the acquire counts no hit,
	// no reuse and no creation, and the bases of img-b and img-c stay.
	ws.BeginStep()
	b.failNextFork.Store(true)
	if _, err := ws.Acquire(ctx, 5, 0, "inst-c", "img-c"); err == nil {
		t.Fatal("expected error when warm child Fork fails")
	}
	if got := ws.StepStats(0, 0); got.Hits != 0 || got.Reused != 0 || got.Misses != 0 || got.Created != 0 || got.StandingPoolSize != 2 {
		t.Fatalf("step 5 failed warm fork StepStats = %+v, want Hits=0 Reused=0 Misses=0 Created=0 StandingPoolSize=2", got)
	}

	if err := ws.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Total created: 1 (img-b base) + 2 (img-c base + step 3 child) + 1 (step 4 child) = 4.
	// Total retired: 2 (step 3 + step 4 children) + 2 (img-b + img-c bases in Close) = 4.
	// Hits: step 4. Misses: steps 1 to 3. Step 5 counts in neither.
	sum := ws.SummaryStats(0, 0)
	if sum.Created != 4 || sum.Retired != 4 || sum.Hits != 1 || sum.Misses != 3 || sum.Reused != 1 {
		t.Fatalf("SummaryStats after Close = %+v, want Created=4 Retired=4 Hits=1 Misses=3 Reused=1", sum)
	}
}

func TestWorkingSetAcquireForkFailedBaseSuspendRetiresBase(t *testing.T) {
	ctx := context.Background()
	b := &wsFakeBackend{}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-fork-base-suspend",
		Backend:    b,
		ParkPolicy: schema.ParkFork,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The cold base is created and its suspend fails: the acquire returns the
	// error, the base is deleted and counted out, and no child is forked.
	ws.BeginStep()
	b.failNextSuspend.Store(true)
	if _, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a"); err == nil {
		t.Fatal("expected the base suspend error")
	}
	if b.creates.Load() != 1 || b.forks.Load() != 0 {
		t.Fatalf("backend creates=%d forks=%d, want 1 create and 0 forks", b.creates.Load(), b.forks.Load())
	}
	if err := ws.awaitRetires(ctx); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	deleted := slices.Clone(b.deletedIDs)
	b.mu.Unlock()
	if len(deleted) != 1 || !strings.Contains(deleted[0], "base-inst-a") {
		t.Fatalf("deleted sandboxes = %v, want only the img-a base", deleted)
	}
	if got := ws.StepStats(0, 0); got.Hits != 0 || got.Misses != 1 || got.Created != 1 || got.Retired != 1 || got.StandingPoolSize != 0 {
		t.Fatalf("after failed base suspend: StepStats = %+v, want Hits=0 Misses=1 Created=1 Retired=1 StandingPoolSize=0", got)
	}

	// The next acquire of the image finds no base and goes cold again.
	c, err := ws.Acquire(ctx, 1, 1, "inst-a", "img-a")
	if err != nil || c.Hit || !c.IsForkChild {
		t.Fatalf("second acquire = %+v, %v; want a cold fork child", c, err)
	}
	if got := ws.StepStats(0, 0); got.Misses != 2 || got.Created != 3 || got.Retired != 1 || got.StandingPoolSize != 1 {
		t.Fatalf("after the cold retry: StepStats = %+v, want Misses=2 Created=3 Retired=1 StandingPoolSize=1", got)
	}
	if err := ws.Release(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := ws.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Created: the failed base, the second base and its child. Retired: the
	// failed base, the child at Release, the second base at Close.
	if sum := ws.SummaryStats(0, 0); sum.Created != 3 || sum.Retired != 3 {
		t.Fatalf("SummaryStats after Close = %+v, want Created=3 Retired=3", sum)
	}
}

func TestWorkingSetAcquireForkBaseRaceRetiresLoser(t *testing.T) {
	ctx := context.Background()
	gate := make(chan struct{})
	b := &wsFakeBackend{suspendGate: gate, onCreate: make(chan struct{}, 2)}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-fork-race",
		Backend:    b,
		ParkPolicy: schema.ParkFork,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Two cold acquires of one image both pass the base check before either
	// base exists. Both bases are held at their suspend until both are created,
	// thus both try to install a base and exactly one loses.
	ws.BeginStep()
	entries := make([]*WorkingSetEntry, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range entries {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			entries[i], errs[i] = ws.Acquire(ctx, 1, i, fmt.Sprintf("inst-%d", i), "img-a")
		}(i)
	}
	<-b.onCreate
	<-b.onCreate
	close(gate)
	wg.Wait()
	for i := range entries {
		if errs[i] != nil || entries[i].Hit || !entries[i].IsForkChild {
			t.Fatalf("acquire %d = %+v, %v; want a cold fork child", i, entries[i], errs[i])
		}
	}

	// Both children fork from the base that won; the other base is deleted
	// and counted out, and the pool holds one base.
	if entries[0].BaseID != entries[1].BaseID {
		t.Fatalf("children fork from different bases: %s and %s", entries[0].BaseID, entries[1].BaseID)
	}
	if err := ws.awaitRetires(ctx); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	deleted := slices.Clone(b.deletedIDs)
	b.mu.Unlock()
	if len(deleted) != 1 || deleted[0] == entries[0].BaseID || !strings.Contains(deleted[0], "base-inst-") {
		t.Fatalf("deleted sandboxes = %v, want only the base that lost to %s", deleted, entries[0].BaseID)
	}
	if b.creates.Load() != 2 || b.forks.Load() != 2 {
		t.Fatalf("backend creates=%d forks=%d, want 2 creates and 2 forks", b.creates.Load(), b.forks.Load())
	}
	if got := ws.StepStats(0, 0); got.Hits != 0 || got.Misses != 2 || got.Created != 4 || got.Retired != 1 || got.StandingPoolSize != 1 {
		t.Fatalf("after the race: StepStats = %+v, want Hits=0 Misses=2 Created=4 Retired=1 StandingPoolSize=1", got)
	}
	for _, e := range entries {
		if err := ws.Release(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if err := ws.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Created: two bases and two children. Retired: the base that lost, the
	// two children at Release, the base that won at Close.
	if sum := ws.SummaryStats(0, 0); sum.Created != 4 || sum.Retired != 4 {
		t.Fatalf("SummaryStats after Close = %+v, want Created=4 Retired=4", sum)
	}
}

func TestWorkingSetPrewarmRetiresWhatItDeletes(t *testing.T) {
	ctx := context.Background()
	newWS := func(t *testing.T, b *wsFakeBackend, policy schema.ParkPolicy) *WorkingSet {
		t.Helper()
		ws, err := NewWorkingSet(WorkingSetConfig{JobID: "job-pw", Backend: b, ParkPolicy: policy})
		if err != nil {
			t.Fatal(err)
		}
		return ws
	}
	deletedIDs := func(b *wsFakeBackend) []string {
		b.mu.Lock()
		defer b.mu.Unlock()
		return slices.Clone(b.deletedIDs)
	}

	// A prewarm whose suspend fails is deleted and counted out, under both
	// park policies that suspend: the run keeps Created == Retired.
	for _, policy := range []schema.ParkPolicy{schema.ParkSuspendAsync, schema.ParkFork} {
		t.Run("suspend fails "+string(policy), func(t *testing.T) {
			b := &wsFakeBackend{}
			ws := newWS(t, b, policy)
			b.failNextSuspend.Store(true)
			ws.Prewarm(ctx, 2, []string{"img-a"})
			if _, err := ws.WaitPrewarm(ctx, 2, time.Now()); err == nil {
				t.Fatal("WaitPrewarm returned no error, want the suspend error")
			}
			if err := ws.awaitRetires(ctx); err != nil {
				t.Fatal(err)
			}
			if deleted := deletedIDs(b); len(deleted) != 1 || !strings.Contains(deleted[0], "prewarm") {
				t.Fatalf("deleted sandboxes = %v, want only the prewarm", deleted)
			}
			if sum := ws.SummaryStats(0, 0); sum.Created != 1 || sum.Retired != 1 || sum.StandingPoolSize != 0 {
				t.Fatalf("SummaryStats = %+v, want Created=1 Retired=1 StandingPoolSize=0", sum)
			}
			if err := ws.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}

	// A fork-mode prewarm whose base loses to a cold acquire of the same image
	// deletes its base and counts it out. The prewarm's suspend is held at the
	// gate until the acquire has installed its own base.
	t.Run("fork base loses to an acquire", func(t *testing.T) {
		gate := make(chan struct{})
		b := &wsFakeBackend{suspendGate: gate, gateIDSubstr: "prewarm", onCreate: make(chan struct{}, 2)}
		ws := newWS(t, b, schema.ParkFork)
		ws.Prewarm(ctx, 2, []string{"img-a"})
		<-b.onCreate
		ws.BeginStep()
		c, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
		if err != nil || c.Hit || !c.IsForkChild {
			t.Fatalf("acquire = %+v, %v; want a cold fork child", c, err)
		}
		close(gate)
		if _, err := ws.WaitPrewarm(ctx, 2, time.Now()); err != nil {
			t.Fatalf("WaitPrewarm: %v", err)
		}
		if err := ws.awaitRetires(ctx); err != nil {
			t.Fatal(err)
		}
		if deleted := deletedIDs(b); len(deleted) != 1 || !strings.Contains(deleted[0], "prewarm") || deleted[0] == c.BaseID {
			t.Fatalf("deleted sandboxes = %v, want only the prewarm base (the acquire's base is %s)", deleted, c.BaseID)
		}
		if b.creates.Load() != 2 || b.forks.Load() != 1 {
			t.Fatalf("backend creates=%d forks=%d, want 2 creates and 1 fork", b.creates.Load(), b.forks.Load())
		}
		// Created: the prewarm base, the acquire's base and its child. Retired
		// so far: the prewarm base. The step counters see the acquire only.
		if sum := ws.SummaryStats(0, 0); sum.Created != 3 || sum.Retired != 1 || sum.StandingPoolSize != 1 {
			t.Fatalf("SummaryStats = %+v, want Created=3 Retired=1 StandingPoolSize=1", sum)
		}
		if got := ws.StepStats(0, 0); got.Created != 2 || got.Retired != 0 {
			t.Fatalf("StepStats = %+v, want Created=2 Retired=0", got)
		}
		if err := ws.Release(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := ws.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if sum := ws.SummaryStats(0, 0); sum.Created != 3 || sum.Retired != 3 {
			t.Fatalf("SummaryStats after Close = %+v, want Created=3 Retired=3", sum)
		}
	})
}

func TestWorkingSetRetireLogsFailedDelete(t *testing.T) {
	ctx := context.Background()
	var logs bytes.Buffer
	b := &wsFakeBackend{}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-retire-log",
		Backend:    b,
		ParkPolicy: schema.ParkSuspendAsync,
		Logger:     slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	ws.BeginStep()
	e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil {
		t.Fatal(err)
	}
	b.failNextSuspend.Store(true)
	if err := ws.Release(ctx, e1); err != nil {
		t.Fatal(err)
	}
	<-e1.asyncDone

	// The retire of e1 hits a failed delete: the acquire still recreates, the
	// sandbox is still counted out, and the failure is logged with its id.
	b.failNextDelete.Store(true)
	ws.BeginStep()
	e2, err := ws.Acquire(ctx, 2, 0, "inst-a", "img-a")
	if err != nil || e2.Hit {
		t.Fatalf("acquire after the failed delete = %+v, %v; want a fresh sandbox", e2, err)
	}
	if got := ws.StepStats(0, 0); got.Created != 1 || got.Retired != 1 || got.StandingPoolSize != 0 {
		t.Fatalf("StepStats = %+v, want Created=1 Retired=1 StandingPoolSize=0", got)
	}
	if err := ws.awaitRetires(ctx); err != nil {
		t.Fatal(err)
	}
	if b.deletes.Load() != 0 {
		t.Fatalf("backend deletes = %d, want 0 (the only delete failed)", b.deletes.Load())
	}
	for _, want := range []string{"delete on retire failed", "env=" + e1.SandboxID, "injected delete error"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log output %q does not contain %q", logs.String(), want)
		}
	}
	if err := ws.Release(ctx, e2); err != nil {
		t.Fatal(err)
	}
	_, _ = ws.AwaitAsyncSuspends(ctx, time.Time{}, time.Time{})
	if err := ws.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkingSetAcquireCancelledDuringAsyncSuspendReparks(t *testing.T) {
	ctx := context.Background()
	gate := make(chan struct{})
	b := &wsFakeBackend{suspendGate: gate}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-async-cancel",
		Backend:    b,
		ParkPolicy: schema.ParkSuspendAsync,
	})
	if err != nil {
		t.Fatal(err)
	}

	ws.BeginStep()
	e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil {
		t.Fatal(err)
	}
	// e1's background suspend is held at the gate.
	if err := ws.Release(ctx, e1); err != nil {
		t.Fatal(err)
	}

	// An acquire whose ctx is cancelled while e1 still suspends returns the
	// ctx error and puts e1 back in the pool: nothing is deleted or counted.
	ws.BeginStep()
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ws.Acquire(cctx, 2, 0, "inst-a", "img-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire error = %v, want context.Canceled", err)
	}
	if ws.StandingPoolSize() != 1 || b.deletes.Load() != 0 {
		t.Fatalf("after the cancelled acquire: pool=%d deletes=%d, want pool=1 deletes=0", ws.StandingPoolSize(), b.deletes.Load())
	}
	if got := ws.StepStats(0, 0); got.Hits != 0 || got.Misses != 0 || got.Created != 0 || got.Reused != 0 || got.Retired != 0 {
		t.Fatalf("after the cancelled acquire: StepStats = %+v, want all zero", got)
	}

	// Once the suspend finishes, a live acquire reuses e1.
	close(gate)
	e2, err := ws.Acquire(ctx, 2, 1, "inst-a", "img-a")
	if err != nil || !e2.Hit || e2.SandboxID != e1.SandboxID {
		t.Fatalf("acquire after the cancel = %+v, %v; want a hit on %s", e2, err, e1.SandboxID)
	}
	if got := ws.StepStats(0, 0); got.Hits != 1 || got.Misses != 0 || got.Created != 0 || got.Reused != 1 {
		t.Fatalf("after the live acquire: StepStats = %+v, want Hits=1 Misses=0 Created=0 Reused=1", got)
	}
	if b.creates.Load() != 1 {
		t.Fatalf("backend creates = %d, want 1", b.creates.Load())
	}
	if err := ws.Release(ctx, e2); err != nil {
		t.Fatal(err)
	}
	_, _ = ws.AwaitAsyncSuspends(ctx, time.Time{}, time.Time{})
	if err := ws.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkingSetAcquireCancelledIntoFullPoolRetiresHit(t *testing.T) {
	ctx := context.Background()
	type fixture struct {
		b        *wsFakeBackend
		ws       *WorkingSet
		e1       *WorkingSetEntry
		gate     chan struct{}
		cancel   context.CancelFunc
		acquired chan error
	}
	// setup parks e1 (img-a) in a pool of two slots with its suspend held at
	// the gate, then starts an acquire of img-a that takes e1 and waits on
	// its suspend. It returns once the pool shows e1 gone.
	setup := func(t *testing.T) *fixture {
		t.Helper()
		f := &fixture{gate: make(chan struct{}), acquired: make(chan error, 1)}
		f.b = &wsFakeBackend{suspendGate: f.gate, gateIDSubstr: "inst-a"}
		var err error
		f.ws, err = NewWorkingSet(WorkingSetConfig{
			JobID:      "job-cancel-full",
			Backend:    f.b,
			ParkPolicy: schema.ParkSuspendAsync,
			MaxParked:  2,
		})
		if err != nil {
			t.Fatal(err)
		}
		f.ws.BeginStep()
		f.e1, err = f.ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.ws.Release(ctx, f.e1); err != nil {
			t.Fatal(err)
		}
		f.ws.BeginStep()
		var cctx context.Context
		cctx, f.cancel = context.WithCancel(ctx)
		go func() {
			_, err := f.ws.Acquire(cctx, 2, 0, "inst-a", "img-a")
			f.acquired <- err
		}()
		for deadline := time.Now().Add(5 * time.Second); f.ws.StandingPoolSize() != 0; {
			if time.Now().After(deadline) {
				t.Fatal("the acquire did not take e1 off the pool")
			}
			time.Sleep(time.Millisecond)
		}
		return f
	}
	// park creates and releases a sandbox of image whose suspend is not held
	// at the gate, thus it settles on its own.
	park := func(t *testing.T, ws *WorkingSet, taskIdx int, inst, image string) *WorkingSetEntry {
		t.Helper()
		e, err := ws.Acquire(ctx, 2, taskIdx, inst, image)
		if err != nil || e.Hit {
			t.Fatalf("acquire %s = %+v, %v; want a cold create", image, e, err)
		}
		if err := ws.Release(ctx, e); err != nil {
			t.Fatal(err)
		}
		return e
	}
	deletedIDs := func(b *wsFakeBackend) []string {
		b.mu.Lock()
		defer b.mu.Unlock()
		return slices.Clone(b.deletedIDs)
	}

	t.Run("full pool retires the hit", func(t *testing.T) {
		f := setup(t)
		// Two releases fill both slots while the acquire waits on e1.
		e2 := park(t, f.ws, 1, "inst-b", "img-b")
		e3 := park(t, f.ws, 2, "inst-c", "img-c")
		f.cancel()
		if err := <-f.acquired; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled acquire error = %v, want context.Canceled", err)
		}
		// e1 is counted out and deleted once its suspend settles; the two
		// settled sandboxes stay.
		if f.ws.StandingPoolSize() != 2 || f.b.deletes.Load() != 0 {
			t.Fatalf("after the cancelled acquire: pool=%d deletes=%d, want pool=2 deletes=0", f.ws.StandingPoolSize(), f.b.deletes.Load())
		}
		if got := f.ws.StepStats(0, 0); got.Hits != 0 || got.Misses != 2 || got.Created != 2 || got.Retired != 1 {
			t.Fatalf("StepStats = %+v, want Hits=0 Misses=2 Created=2 Retired=1", got)
		}
		close(f.gate)
		if err := f.ws.awaitRetires(ctx); err != nil {
			t.Fatal(err)
		}
		if deleted := deletedIDs(f.b); !slices.Equal(deleted, []string{f.e1.SandboxID}) {
			t.Fatalf("deleted sandboxes = %v, want [%s]", deleted, f.e1.SandboxID)
		}
		// The settled sandboxes still hit; img-a goes cold.
		hb, err := f.ws.Acquire(ctx, 3, 0, "inst-b", "img-b")
		if err != nil || !hb.Hit || hb.SandboxID != e2.SandboxID {
			t.Fatalf("acquire img-b = %+v, %v; want a hit on %s", hb, err, e2.SandboxID)
		}
		hc, err := f.ws.Acquire(ctx, 3, 1, "inst-c", "img-c")
		if err != nil || !hc.Hit || hc.SandboxID != e3.SandboxID {
			t.Fatalf("acquire img-c = %+v, %v; want a hit on %s", hc, err, e3.SandboxID)
		}
		ha, err := f.ws.Acquire(ctx, 3, 2, "inst-a", "img-a")
		if err != nil || ha.Hit {
			t.Fatalf("acquire img-a = %+v, %v; want a cold create", ha, err)
		}
		if err := f.ws.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("free slot reparks the hit at the head", func(t *testing.T) {
		f := setup(t)
		// One release of img-a takes one slot while the acquire waits on e1;
		// the other slot stays free.
		ez := park(t, f.ws, 1, "inst-z", "img-a")
		f.cancel()
		if err := <-f.acquired; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled acquire error = %v, want context.Canceled", err)
		}
		// e1 is back in the pool ahead of ez; nothing is deleted or counted out.
		if f.ws.StandingPoolSize() != 2 || f.b.deletes.Load() != 0 {
			t.Fatalf("after the cancelled acquire: pool=%d deletes=%d, want pool=2 deletes=0", f.ws.StandingPoolSize(), f.b.deletes.Load())
		}
		if got := f.ws.StepStats(0, 0); got.Hits != 0 || got.Misses != 1 || got.Created != 1 || got.Retired != 0 {
			t.Fatalf("StepStats = %+v, want Hits=0 Misses=1 Created=1 Retired=0", got)
		}
		close(f.gate)
		h1, err := f.ws.Acquire(ctx, 3, 0, "inst-a", "img-a")
		if err != nil || !h1.Hit || h1.SandboxID != f.e1.SandboxID {
			t.Fatalf("first acquire of img-a = %+v, %v; want a hit on %s", h1, err, f.e1.SandboxID)
		}
		h2, err := f.ws.Acquire(ctx, 3, 1, "inst-a", "img-a")
		if err != nil || !h2.Hit || h2.SandboxID != ez.SandboxID {
			t.Fatalf("second acquire of img-a = %+v, %v; want a hit on %s", h2, err, ez.SandboxID)
		}
		if deleted := deletedIDs(f.b); len(deleted) != 0 {
			t.Fatalf("deleted sandboxes = %v, want none", deleted)
		}
		if err := f.ws.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWorkingSetAcquirePreCancelledContextLeavesPoolUntouched(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		suspendFail bool
	}{
		{"healthy settled entry", false},
		{"failed settled entry", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &wsFakeBackend{}
			ws, err := NewWorkingSet(WorkingSetConfig{
				JobID:      "job-precancel",
				Backend:    b,
				ParkPolicy: schema.ParkSuspendAsync,
			})
			if err != nil {
				t.Fatal(err)
			}
			ws.BeginStep()
			e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
			if err != nil {
				t.Fatal(err)
			}
			b.failNextSuspend.Store(tc.suspendFail)
			if err := ws.Release(ctx, e1); err != nil {
				t.Fatal(err)
			}
			<-e1.asyncDone

			// An acquire on a context that expired before it ran returns the
			// context error and leaves the pool as it was: e1 stays parked,
			// settled or not, nothing is deleted or counted, and no cold
			// create runs. Repeated, because the old select picked at random
			// between the expired context and the settled suspend.
			ws.BeginStep()
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			for i := 0; i < 20; i++ {
				if _, err := ws.Acquire(cctx, 2, i, "inst-a", "img-a"); !errors.Is(err, context.Canceled) {
					t.Fatalf("pre-cancelled acquire %d error = %v, want context.Canceled", i, err)
				}
			}
			if ws.StandingPoolSize() != 1 || b.deletes.Load() != 0 || b.creates.Load() != 1 {
				t.Fatalf("after the pre-cancelled acquires: pool=%d deletes=%d creates=%d, want pool=1 deletes=0 creates=1",
					ws.StandingPoolSize(), b.deletes.Load(), b.creates.Load())
			}
			if got := ws.StepStats(0, 0); got.Hits != 0 || got.Misses != 0 || got.Created != 0 || got.Reused != 0 || got.Retired != 0 {
				t.Fatalf("after the pre-cancelled acquires: StepStats = %+v, want all zero", got)
			}

			// A live acquire then reads the settled suspend: a healthy e1 is
			// reused, a failed one is retired and the acquire goes cold.
			e2, err := ws.Acquire(ctx, 2, 0, "inst-a", "img-a")
			if err != nil {
				t.Fatalf("live acquire: %v", err)
			}
			if err := ws.awaitRetires(ctx); err != nil {
				t.Fatal(err)
			}
			b.mu.Lock()
			deleted := slices.Clone(b.deletedIDs)
			b.mu.Unlock()
			got := ws.StepStats(0, 0)
			if tc.suspendFail {
				if e2.Hit || e2.SandboxID == e1.SandboxID {
					t.Fatalf("live acquire = %+v, want a cold create (e1 is %s)", e2, e1.SandboxID)
				}
				if !slices.Equal(deleted, []string{e1.SandboxID}) {
					t.Fatalf("deleted sandboxes = %v, want [%s]", deleted, e1.SandboxID)
				}
				if got.Hits != 0 || got.Misses != 1 || got.Created != 1 || got.Reused != 0 || got.Retired != 1 {
					t.Fatalf("after the live acquire: StepStats = %+v, want Hits=0 Misses=1 Created=1 Reused=0 Retired=1", got)
				}
			} else {
				if !e2.Hit || e2.SandboxID != e1.SandboxID {
					t.Fatalf("live acquire = %+v, want a hit on %s", e2, e1.SandboxID)
				}
				if len(deleted) != 0 {
					t.Fatalf("deleted sandboxes = %v, want none", deleted)
				}
				if got.Hits != 1 || got.Misses != 0 || got.Created != 0 || got.Reused != 1 || got.Retired != 0 {
					t.Fatalf("after the live acquire: StepStats = %+v, want Hits=1 Misses=0 Created=0 Reused=1 Retired=0", got)
				}
			}
			if err := ws.Release(ctx, e2); err != nil {
				t.Fatal(err)
			}
			_, _ = ws.AwaitAsyncSuspends(ctx, time.Time{}, time.Time{})
			if err := ws.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}

	// Under ParkFork the same holds: a cold acquire on an expired context
	// counts no miss and creates no base, a warm one forks no child.
	for _, tc := range []struct {
		name string
		warm bool
	}{
		{"fork cold", false},
		{"fork warm", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &wsFakeBackend{}
			ws, err := NewWorkingSet(WorkingSetConfig{
				JobID:      "job-precancel-fork",
				Backend:    b,
				ParkPolicy: schema.ParkFork,
			})
			if err != nil {
				t.Fatal(err)
			}
			var wantCreates, wantForks, wantDeletes int64
			var wantPool int
			if tc.warm {
				// One live acquire leaves the golden base behind; its child
				// is deleted at Release.
				ws.BeginStep()
				c, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
				if err != nil {
					t.Fatal(err)
				}
				if err := ws.Release(ctx, c); err != nil {
					t.Fatal(err)
				}
				wantCreates, wantForks, wantDeletes, wantPool = 1, 1, 1, 1
			}

			ws.BeginStep()
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			for i := 0; i < 20; i++ {
				if _, err := ws.Acquire(cctx, 2, i, "inst-a", "img-a"); !errors.Is(err, context.Canceled) {
					t.Fatalf("pre-cancelled acquire %d error = %v, want context.Canceled", i, err)
				}
			}
			if b.creates.Load() != wantCreates || b.forks.Load() != wantForks || b.deletes.Load() != wantDeletes || ws.StandingPoolSize() != wantPool {
				t.Fatalf("after the pre-cancelled acquires: creates=%d forks=%d deletes=%d pool=%d, want creates=%d forks=%d deletes=%d pool=%d",
					b.creates.Load(), b.forks.Load(), b.deletes.Load(), ws.StandingPoolSize(), wantCreates, wantForks, wantDeletes, wantPool)
			}
			if got := ws.StepStats(0, 0); got.Hits != 0 || got.Misses != 0 || got.Created != 0 || got.Reused != 0 || got.Retired != 0 {
				t.Fatalf("after the pre-cancelled acquires: StepStats = %+v, want all zero", got)
			}

			// A live acquire then forks the base, or creates one and forks.
			c, err := ws.Acquire(ctx, 2, 0, "inst-a", "img-a")
			if err != nil || c.Hit != tc.warm || !c.IsForkChild {
				t.Fatalf("live acquire = %+v, %v; want a fork child with Hit=%v", c, err, tc.warm)
			}
			got := ws.StepStats(0, 0)
			if tc.warm {
				if got.Hits != 1 || got.Misses != 0 || got.Created != 1 || got.Reused != 1 || got.Retired != 0 {
					t.Fatalf("after the live acquire: StepStats = %+v, want Hits=1 Misses=0 Created=1 Reused=1 Retired=0", got)
				}
			} else if got.Hits != 0 || got.Misses != 1 || got.Created != 2 || got.Reused != 0 || got.Retired != 0 {
				t.Fatalf("after the live acquire: StepStats = %+v, want Hits=0 Misses=1 Created=2 Reused=0 Retired=0", got)
			}
			if err := ws.Release(ctx, c); err != nil {
				t.Fatal(err)
			}
			if err := ws.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkingSetCloseDrainsInFlightPrewarm(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		policy      schema.ParkPolicy
		suspendFail bool
		images      int
		// closeExpires gives Close a deadline that expires while the prewarm
		// still holds at its suspend.
		closeExpires bool
	}{
		{"suspend-async, suspend succeeds", schema.ParkSuspendAsync, false, 2, false},
		{"suspend-async, suspend fails", schema.ParkSuspendAsync, true, 2, false},
		{"fork, suspend succeeds", schema.ParkFork, false, 2, false},
		{"fork, suspend fails", schema.ParkFork, true, 2, false},
		{"suspend-async, one image", schema.ParkSuspendAsync, false, 1, false},
		{"fork, one image", schema.ParkFork, false, 1, false},
		{"suspend-async, Close deadline expires", schema.ParkSuspendAsync, false, 1, true},
		{"fork, Close deadline expires", schema.ParkFork, false, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := make(chan struct{})
			b := &wsFakeBackend{suspendGate: gate, onCreate: make(chan struct{}, 2)}
			ws, err := NewWorkingSet(WorkingSetConfig{
				JobID:      "job-close-prewarm",
				Backend:    b,
				ParkPolicy: tc.policy,
			})
			if err != nil {
				t.Fatal(err)
			}
			// The prewarm has created its first sandbox and holds at its
			// suspend when Close runs, as on a run cancelled during the step
			// before; WaitPrewarm never ran for it.
			ws.Prewarm(ctx, 2, []string{"img-a", "img-b"}[:tc.images])
			<-b.onCreate
			b.failNextSuspend.Store(tc.suspendFail)

			if tc.closeExpires {
				// Close gives up on the prewarm when its deadline expires and
				// returns the context error; the prewarm then reaches a closed
				// pool and retires what it made instead of parking it.
				cctx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
				defer cancel()
				if err := ws.Close(cctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Close on an expiring deadline = %v, want context.DeadlineExceeded", err)
				}
				close(gate)
			} else {
				// Close waits for the prewarm: it returns once the gate opens.
				closeDone := make(chan error, 1)
				go func() { closeDone <- ws.Close(ctx) }()
				select {
				case err := <-closeDone:
					t.Fatalf("Close returned %v while the prewarm was still in flight", err)
				case <-time.After(20 * time.Millisecond):
				}
				close(gate)
				if err := <-closeDone; err != nil {
					t.Fatal(err)
				}
			}

			// The prewarm reports the closed pool whether it found it closed
			// at the next image or at the park of the only one.
			if _, err := ws.WaitPrewarm(ctx, 2, time.Now()); !errors.Is(err, errWorkingSetClosed) {
				t.Fatalf("WaitPrewarm after Close error = %v, want errWorkingSetClosed", err)
			}
			if err := ws.awaitRetires(ctx); err != nil {
				t.Fatal(err)
			}

			// The sandbox the prewarm made is deleted and counted out whether
			// its suspend succeeded or failed, the prewarm creates nothing for
			// img-b, and nothing is parked in the closed pool.
			b.mu.Lock()
			deleted := slices.Clone(b.deletedIDs)
			b.mu.Unlock()
			if len(deleted) != 1 || !strings.Contains(deleted[0], "prewarm") {
				t.Fatalf("deleted sandboxes = %v, want only the prewarm", deleted)
			}
			if b.creates.Load() != 1 {
				t.Fatalf("backend creates = %d, want 1", b.creates.Load())
			}
			wantSuspends := int64(1)
			if tc.suspendFail {
				wantSuspends = 0
			}
			if b.suspends.Load() != wantSuspends {
				t.Fatalf("backend suspends = %d, want %d", b.suspends.Load(), wantSuspends)
			}
			if ws.StandingPoolSize() != 0 {
				t.Fatalf("StandingPoolSize after Close = %d, want 0", ws.StandingPoolSize())
			}
			if sum := ws.SummaryStats(0, 0); sum.Created != 1 || sum.Retired != 1 {
				t.Fatalf("SummaryStats after Close = %+v, want Created=1 Retired=1", sum)
			}
		})
	}
}

func TestWorkingSetCloseHonorsContextOnUnsettledPark(t *testing.T) {
	ctx := context.Background()
	gate := make(chan struct{})
	b := &wsFakeBackend{suspendGate: gate}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-close-unsettled",
		Backend:    b,
		ParkPolicy: schema.ParkSuspendAsync,
	})
	if err != nil {
		t.Fatal(err)
	}
	ws.BeginStep()
	e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil {
		t.Fatal(err)
	}
	// e1's background suspend holds at the gate.
	if err := ws.Release(ctx, e1); err != nil {
		t.Fatal(err)
	}

	// Close on a deadline that expires while e1 still suspends returns the
	// context error, counts e1 out and leaves its delete to follow the
	// suspend in the background: nothing is deleted while the gate holds.
	cctx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	defer cancel()
	if err := ws.Close(cctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close on an expiring deadline = %v, want context.DeadlineExceeded", err)
	}
	if b.deletes.Load() != 0 {
		t.Fatalf("backend deletes = %d before the suspend settled, want 0", b.deletes.Load())
	}
	if ws.StandingPoolSize() != 0 {
		t.Fatalf("StandingPoolSize after Close = %d, want 0", ws.StandingPoolSize())
	}
	if sum := ws.SummaryStats(0, 0); sum.Created != 1 || sum.Retired != 1 {
		t.Fatalf("SummaryStats after Close = %+v, want Created=1 Retired=1", sum)
	}

	// Once the suspend settles, the retire delete runs.
	close(gate)
	if err := ws.awaitRetires(ctx); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	deleted := slices.Clone(b.deletedIDs)
	b.mu.Unlock()
	if !slices.Equal(deleted, []string{e1.SandboxID}) {
		t.Fatalf("deleted sandboxes = %v, want [%s]", deleted, e1.SandboxID)
	}
}

// TestRunScheduledStepsAcquireErrorAndCancelCleanUpWithoutPanic covers a
// task whose acquire fails: the run records it as failed and goes on to its
// cleanup. Before, the task event read the trajectory name off an
// environment that had none, and the loader crashed before the cleanup.
func TestRunScheduledStepsAcquireErrorAndCancelCleanUpWithoutPanic(t *testing.T) {
	traj := &Trajectory{Name: "traj-a", Steps: []Step{{Name: "a", Run: "true"}}}
	img1, img2 := "reg/img-1@"+goodDigest, "reg/img-2@"+goodDigest
	insts := []Instance{
		{InstanceID: "inst-1", Image: img1},
		{InstanceID: "inst-2", Image: img2},
	}
	failedTask := func(t *testing.T, buf *bytes.Buffer, wantErr string) {
		t.Helper()
		evs, err := ReadEvents(buf)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		var failedCreates, failedTasks int
		for _, ev := range evs {
			switch {
			case ev.Event == EventCreate && !ev.OK:
				failedCreates++
			case ev.Event == EventTask && !ev.OK:
				failedTasks++
				if ev.Name != "traj-a" || !strings.Contains(ev.Err, wantErr) {
					t.Fatalf("failed task event = %+v, want Name=traj-a and Err containing %q", ev, wantErr)
				}
			}
		}
		if failedCreates != 1 || failedTasks != 1 {
			t.Fatalf("failed create events = %d, failed task events = %d; want 1 and 1", failedCreates, failedTasks)
		}
	}

	t.Run("cancelled prefetch run", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		suspendGate := make(chan struct{})
		b := &wsFakeBackend{
			// The prewarm for step 2 holds at its suspend; the only task of
			// step 1 holds at its create.
			suspendGate: suspendGate, gateIDSubstr: "prewarm",
			onCreate:   make(chan struct{}, 2),
			createGate: make(chan struct{}), createGateIDSubstr: "inst-1", createHeld: make(chan struct{}, 1),
		}
		var buf bytes.Buffer
		sched := &schema.Schedule{
			SchemaVersion: schema.SchemaVersion,
			ScenarioName:  "cancel-prefetch",
			Jobs: []schema.ScheduledJob{{
				Job:        "ws-cancel",
				ParkPolicy: schema.ParkSuspendAsync,
				Steps: []schema.ScheduledStep{
					{Step: 1, Tasks: []schema.ScheduledTask{{InstanceID: "inst-1", Image: img1}}, PrefetchImages: []string{img2}},
					{Step: 2, Tasks: []schema.ScheduledTask{{InstanceID: "inst-2", Image: img2}}},
				},
			}},
		}
		// The run is cancelled once the prewarm made its sandbox and the
		// task's create is under way; the suspend gate then opens so the
		// cleanup can join the prewarm.
		go func() {
			<-b.onCreate
			<-b.createHeld
			cancel()
			close(suspendGate)
		}()
		sum, err := Run(ctx, Config{
			RunID:      "ws-cancel",
			Backend:    b,
			Instances:  insts,
			Trajectory: traj,
			Mode:       ModeSteps,
			Park:       ParkSuspendAsync,
			Schedule:   sched,
			Prefetch:   true,
			Width:      1,
			Recorder:   NewRecorder(&buf),
		})
		if !errors.Is(err, context.Canceled) || sum == nil {
			t.Fatalf("Run = %+v, %v; want a summary and context.Canceled", sum, err)
		}
		if sum.Tasks != 1 || sum.Failed != 1 {
			t.Fatalf("Summary = %+v, want 1 task and 1 failed", sum)
		}
		failedTask(t, &buf, "context canceled")

		// The prewarm sandbox is the only one made, and the cleanup deleted it.
		b.mu.Lock()
		deleted := slices.Clone(b.deletedIDs)
		b.mu.Unlock()
		if b.creates.Load() != 1 || len(deleted) != 1 || !strings.Contains(deleted[0], "prewarm") {
			t.Fatalf("backend creates = %d, deleted = %v; want 1 create and only the prewarm deleted", b.creates.Load(), deleted)
		}
	})

	t.Run("failed fork-base suspend", func(t *testing.T) {
		b := &wsFakeBackend{}
		b.failNextSuspend.Store(true)
		var buf bytes.Buffer
		sum, err := Run(context.Background(), Config{
			RunID:        "ws-fork-base",
			Backend:      b,
			Instances:    insts[:1],
			Trajectory:   traj,
			Mode:         ModeSteps,
			Park:         ParkFork,
			Steps:        2,
			TasksPerStep: 1,
			GroupSize:    1,
			Width:        1,
			Recorder:     NewRecorder(&buf),
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		// Step 1 fails at the base suspend; step 2 goes cold again and runs.
		if sum.Tasks != 2 || sum.Failed != 1 || sum.WorkingSetMisses != 2 || sum.WorkingSetHits != 0 {
			t.Fatalf("Summary = %+v, want 2 tasks, 1 failed, 2 misses, 0 hits", sum)
		}
		failedTask(t, &buf, "injected async suspend error")

		// Created: the base whose suspend failed and the second base; forked:
		// one child. All three are deleted by the end of the run.
		b.mu.Lock()
		deleted := slices.Clone(b.deletedIDs)
		b.mu.Unlock()
		if b.creates.Load() != 2 || b.forks.Load() != 1 || len(deleted) != 3 {
			t.Fatalf("backend creates = %d forks = %d deleted = %v; want 2 creates, 1 fork, 3 deletes", b.creates.Load(), b.forks.Load(), deleted)
		}
	})
}

func TestWorkingSetAcquireDoesNotWaitForTheRetireDelete(t *testing.T) {
	ctx := context.Background()
	gate := make(chan struct{})
	b := &wsFakeBackend{deleteGate: gate}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "job-retire-bg",
		Backend:    b,
		ParkPolicy: schema.ParkSuspendAsync,
	})
	if err != nil {
		t.Fatal(err)
	}

	ws.BeginStep()
	e1, err := ws.Acquire(ctx, 1, 0, "inst-a", "img-a")
	if err != nil {
		t.Fatal(err)
	}
	b.failNextSuspend.Store(true)
	if err := ws.Release(ctx, e1); err != nil {
		t.Fatal(err)
	}
	<-e1.asyncDone

	// The acquire that finds e1's failed suspend counts it out and goes on to
	// a cold create while e1's delete is still held at the gate.
	ws.BeginStep()
	actx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	e2, err := ws.Acquire(actx, 2, 0, "inst-a", "img-a")
	if err != nil || e2.Hit {
		t.Fatalf("acquire = %+v, %v; want a fresh sandbox", e2, err)
	}
	if b.deletes.Load() != 0 {
		t.Fatalf("backend deletes = %d before the gate opened, want 0", b.deletes.Load())
	}
	if got := ws.StepStats(0, 0); got.Created != 1 || got.Retired != 1 || got.StandingPoolSize != 0 {
		t.Fatalf("StepStats = %+v, want Created=1 Retired=1 StandingPoolSize=0", got)
	}

	// Close joins the delete: it returns once the gate opens, not before.
	closeDone := make(chan error, 1)
	go func() { closeDone <- ws.Close(ctx) }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned %v while the retire delete was still held", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if b.deletes.Load() != 1 {
		t.Fatalf("backend deletes = %d after Close, want 1", b.deletes.Load())
	}
	b.mu.Lock()
	deleted := slices.Clone(b.deletedIDs)
	b.mu.Unlock()
	if !slices.Equal(deleted, []string{e1.SandboxID}) {
		t.Fatalf("deleted sandboxes = %v, want [%s]", deleted, e1.SandboxID)
	}
}

func BenchmarkWorkingSetThroughput(b *testing.B) {
	ctx := context.Background()
	fb := &wsFakeBackend{}
	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      "bench",
		Backend:    fb,
		ParkPolicy: schema.ParkSuspendSync,
		MaxParked:  64,
	})
	if err != nil {
		b.Fatal(err)
	}
	images := make([]string, 32)
	for i := range images {
		images[i] = fmt.Sprintf("reg/img-%02d@%s", i, goodDigest)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		img := images[i%len(images)]
		e, err := ws.Acquire(ctx, i/len(images)+1, i%len(images), "inst", img)
		if err != nil {
			b.Fatal(err)
		}
		if err := ws.Release(ctx, e); err != nil {
			b.Fatal(err)
		}
	}
}

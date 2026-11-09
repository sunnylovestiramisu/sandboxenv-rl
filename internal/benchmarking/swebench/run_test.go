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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeBackend is an in-memory Backend that records calls and can fail on
// demand. Its sandboxes answer every command with exit code 0 unless the
// command names a nonzero exit (e.g. "exit 1").
type fakeBackend struct {
	mu             sync.Mutex
	created        map[string]int
	suspended      map[string]int
	suspendedAsync map[string]int
	forked         map[string]string
	deleted        map[string]int
	deletedByID    map[string]int
	// onCreate, when set, runs after every successful create; tests use it
	// to cancel a run at a known point.
	onCreate    func(id string)
	commands    []string
	files       map[string]string
	failCreate  map[string]bool
	failDelete  map[string]int // sandbox deletes still to refuse, by id
	unavailable int32          // first-command Unavailable errors still to return

	inflight    atomic.Int32
	maxInflight atomic.Int32
	// awake counts environments created and not parked or deleted; maxAwake
	// is its high-water mark, what the running slots would have to hold.
	awake    atomic.Int32
	maxAwake atomic.Int32
}

func (f *fakeBackend) bumpAwake(d int32) {
	n := f.awake.Add(d)
	for {
		m := f.maxAwake.Load()
		if n <= m || f.maxAwake.CompareAndSwap(m, n) {
			return
		}
	}
}

func newFake() *fakeBackend {
	return &fakeBackend{created: map[string]int{}, suspended: map[string]int{}, suspendedAsync: map[string]int{},
		forked: map[string]string{}, deleted: map[string]int{},
		deletedByID: map[string]int{}, files: map[string]string{}, failCreate: map[string]bool{},
		failDelete: map[string]int{}}
}

func (f *fakeBackend) Delete(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedByID[id]++
	if f.created[id] == 0 {
		return status.Error(codes.NotFound, "no such environment")
	}
	return nil
}

func (f *fakeBackend) Create(ctx context.Context, id, image string) (Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created[id]++
	if f.failCreate[id] {
		return nil, errors.New("create refused")
	}
	if f.onCreate != nil {
		f.onCreate(id)
	}
	f.bumpAwake(1)
	return &fakeSandbox{b: f, id: id}, nil
}

type fakeSandbox struct {
	b      *fakeBackend
	id     string
	parked atomic.Bool
}

// wake counts the environment awake again on its first call after a park.
func (s *fakeSandbox) wake() {
	if s.parked.CompareAndSwap(true, false) {
		s.b.bumpAwake(1)
	}
}

func (s *fakeSandbox) enter() func() {
	n := s.b.inflight.Add(1)
	for {
		m := s.b.maxInflight.Load()
		if n <= m || s.b.maxInflight.CompareAndSwap(m, n) {
			break
		}
	}
	return func() { s.b.inflight.Add(-1) }
}

func (s *fakeSandbox) Run(ctx context.Context, command string, timeout time.Duration) (Result, error) {
	defer s.enter()()
	s.wake()
	if atomic.AddInt32(&s.b.unavailable, -1) >= 0 {
		return Result{}, status.Error(codes.Unavailable, "waking")
	}
	atomic.AddInt32(&s.b.unavailable, 1)
	time.Sleep(time.Millisecond)
	s.b.mu.Lock()
	s.b.commands = append(s.b.commands, s.id+": "+command)
	s.b.mu.Unlock()
	if code, ok := strings.CutPrefix(command, "exit "); ok {
		n, err := strconv.Atoi(code)
		if err != nil {
			panic("fakeSandbox: " + command)
		}
		return Result{ExitCode: n, Stderr: "failed"}, nil
	}
	if strings.HasPrefix(command, "echo ") {
		return Result{Stdout: strings.TrimPrefix(command, "echo ")}, nil
	}
	return Result{Stdout: "ok"}, nil
}

func (s *fakeSandbox) WriteFile(ctx context.Context, path string, content []byte, mode fs.FileMode) error {
	defer s.enter()()
	s.wake()
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	s.b.files[s.id+":"+path] = string(content)
	return nil
}

func (s *fakeSandbox) Suspend(ctx context.Context) error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	s.b.suspended[s.id]++
	if s.parked.CompareAndSwap(false, true) {
		s.b.bumpAwake(-1)
	}
	return nil
}

func (s *fakeSandbox) SuspendWithMode(ctx context.Context, async bool) error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	if async {
		s.b.suspendedAsync[s.id]++
	} else {
		s.b.suspended[s.id]++
	}
	if s.parked.CompareAndSwap(false, true) {
		s.b.bumpAwake(-1)
	}
	return nil
}

func (s *fakeSandbox) Fork(ctx context.Context, newID string) (Sandbox, error) {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	s.b.created[newID]++
	s.b.forked[newID] = s.id
	return &fakeSandbox{b: s.b, id: newID}, nil
}

// Delete fails on a done context, as the real API would, so a cleanup that
// runs on a cancelled run context is visible as a leak.
func (s *fakeSandbox) Delete(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	if s.b.failDelete[s.id] > 0 {
		s.b.failDelete[s.id]--
		return errors.New("delete refused")
	}
	s.b.deleted[s.id]++
	if s.parked.CompareAndSwap(false, true) {
		s.b.bumpAwake(-1)
	}
	return nil
}

func instances(n int) []Instance {
	out := make([]Instance, n)
	for i := range out {
		out[i] = Instance{InstanceID: "repo__repo-" + string(rune('a'+i)), Image: "reg/img@" + goodDigest,
			Patch: "PATCH", EvalScript: "EVAL"}
	}
	return out
}

func testTrajectory(t *testing.T, rewardCmd string) *Trajectory {
	t.Helper()
	tr, err := ParseTrajectory([]byte(`
name: test
setup:
  - {name: baseline, run: "git commit baseline", expect_exit: 0}
reset:
  - {name: reset, run: "git checkout", expect_exit: 0}
steps:
  - {name: write, write: {path: /tmp/p, content: "{{.Patch}}"}}
  - {name: apply, run: "git apply", expect_exit: 0}
  - {name: tests, run: "` + rewardCmd + `", reward: true}
`))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func baseConfig(t *testing.T, b Backend, out *bytes.Buffer) Config {
	t.Helper()
	var w io.Writer
	if out != nil {
		w = out
	}
	return Config{
		RunID:       "t",
		Backend:     b,
		Instances:   instances(3),
		Trajectory:  testTrajectory(t, "true"),
		Recorder:    NewRecorder(w),
		Logger:      slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		Width:       2,
		WakeBackoff: time.Millisecond,
	}
}

func TestEvalCreatesReplaysDeletes(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 2
	cfg.Labels = map[string]string{"cluster": "a", "arm": "x"}

	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Tasks != 6 || sum.Failed != 0 || sum.Rewarded != 6 {
		t.Fatalf("summary %+v", sum)
	}
	if len(f.created) != 6 || len(f.deleted) != 6 {
		t.Fatalf("created %d deleted %d", len(f.created), len(f.deleted))
	}
	for id, n := range f.created {
		if n != 1 || f.deleted[id] != 1 {
			t.Fatalf("env %s created %d deleted %d", id, n, f.deleted[id])
		}
	}
	if got := f.maxInflight.Load(); got > 2 {
		t.Fatalf("max in flight %d > width 2", got)
	}
	if f.files["t-repo--repo-a-0:/tmp/p"] != "PATCH" {
		t.Fatalf("patch not written: %v", f.files)
	}
	resets, setups := countCommands(f, "git checkout", "git commit baseline")
	if resets != 0 || setups != 6 {
		t.Fatalf("eval mode: %d resets (want 0), %d setups (want 6, once per environment)", resets, setups)
	}
	rep := Build(parseEvents(t, out.String()))
	if rep.Tasks != 6 || rep.RewardRate != 1 {
		t.Fatalf("report %+v", rep)
	}
	// The run event carries the loader's own labels and the operator's.
	for k, want := range map[string]string{"cluster": "a", "arm": "x", "mode": "eval", "width": "2", "images": "3", "tasks_per_image": "2"} {
		if rep.Labels[k] != want {
			t.Errorf("label %s = %q, want %q (labels %v)", k, rep.Labels[k], want, rep.Labels)
		}
	}
	if rep.Labels["trajectory"] == "" {
		t.Errorf("no trajectory label: %v", rep.Labels)
	}
	for _, ev := range []string{EventCreate, EventTTFI, EventTask, EventDelete, EventRun} {
		if !hasStat(rep, ev) {
			t.Errorf("no %s events", ev)
		}
	}
}

func TestEvalCleanupSurvivesCancel(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	cfg.Instances = instances(6)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel the run while its first environments are in flight, so later
	// creates never start and the in-flight ones must still be deleted.
	var once sync.Once
	f.onCreate = func(string) { once.Do(cancel) }
	if _, err := Run(ctx, cfg); err == nil {
		t.Fatal("a cancelled run must report the cancellation")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.created) == 0 {
		t.Fatal("nothing was created before the cancel; the test proves nothing")
	}
	for id := range f.created {
		if f.deleted[id] != 1 {
			t.Fatalf("environment %s leaked after cancel: deleted %d", id, f.deleted[id])
		}
	}
}

func TestFailedCreateIsDeletedByName(t *testing.T) {
	f := newFake()
	bad := EnvID("t", "repo__repo-a", 0)
	f.failCreate[bad] = true
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if f.deletedByID[bad] != 1 {
		t.Fatalf("the failed create must be deleted by name once, got %d", f.deletedByID[bad])
	}
	for id, n := range f.deletedByID {
		if id != bad && n != 0 {
			t.Fatalf("%s deleted by name although its create succeeded", id)
		}
	}
}

func TestNoThinkBeforeFirstCommand(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	cfg.Instances = instances(1)
	tr, err := ParseTrajectory([]byte("name: t\nsteps:\n  - {name: a, run: \"true\", think: 5s, reward: true}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Trajectory = tr
	start := time.Now()
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("think time before the first command was not skipped")
	}
}

func TestEvalCountsFailures(t *testing.T) {
	f := newFake()
	f.failCreate[EnvID("t", "repo__repo-a", 0)] = true
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	cfg.Trajectory = testTrajectory(t, "exit 1")

	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// One create failure; the other two run to the reward step and get 0.
	if sum.Tasks != 3 || sum.Failed != 1 || sum.Rewarded != 0 {
		t.Fatalf("summary %+v", sum)
	}
	rep := Build(parseEvents(t, out.String()))
	if rep.RewardRate != 0 || rep.Failed != 1 {
		t.Fatalf("report %+v", rep)
	}
}

func TestExpectExitFailsTask(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	tr, err := ParseTrajectory([]byte("name: t\nsteps:\n  - {name: a, run: \"exit 2\", expect_exit: 0}\n  - {name: b, run: \"true\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Trajectory = tr
	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Failed != 3 {
		t.Fatalf("summary %+v", sum)
	}
	for _, c := range f.commands {
		if strings.HasSuffix(c, ": true") {
			t.Fatal("steps after a failed expectation must not run")
		}
	}
}

// A step the guest kills at its timeout (exit 137 at or past the deadline)
// fails the task and stops the trajectory, as run_fleet_bench.py does on its
// TimeoutError, and the report classifies it as a tier 2 command_timeout.
func TestTimedOutStepFailsTask(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	tr, err := ParseTrajectory([]byte("name: t\nsteps:\n  - {name: slow, run: \"exit 137\", timeout: 1ms}\n  - {name: b, run: \"true\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Trajectory = tr
	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Tasks != 3 || sum.Failed != 3 {
		t.Fatalf("summary %+v, want 3 tasks, all failed", sum)
	}
	for _, c := range f.commands {
		if strings.HasSuffix(c, ": true") {
			t.Fatal("steps after a timed-out step must not run")
		}
	}
	events := parseEvents(t, out.String())
	var slow int
	for _, e := range events {
		if e.Event != EventStep || e.Name != "slow" {
			continue
		}
		slow++
		if e.OK || e.Timeout == nil || !*e.Timeout || e.Exit == nil || *e.Exit != 137 || !strings.Contains(e.Err, "timed out after") {
			t.Fatalf("timed-out step event %+v, want ok=false timeout=true exit=137 and a timed out error", e)
		}
	}
	if slow != 3 {
		t.Fatalf("got %d slow step events, want 3", slow)
	}
	fc := Build(events).FaultClassification
	if fc == nil || fc.AllRollouts != 3 || fc.Tier2Policy != 3 || fc.InfraDropped != 0 {
		t.Fatalf("fault classification %+v, want 3 rollouts, 3 policy, 0 infra", fc)
	}
	var timeouts int
	for _, c := range fc.Categories {
		if c.Tier == schema.FaultTierPolicy && c.Category == schema.FaultCatCommandTimeout {
			timeouts = c.Dropped
		}
	}
	if timeouts != 3 {
		t.Fatalf("command_timeout dropped = %d, want 3; categories %+v", timeouts, fc.Categories)
	}
}

func TestStepsSuspendPark(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeSteps
	cfg.Rollouts = 2
	cfg.Steps = 3
	cfg.Park = ParkSuspend

	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Tasks != 18 || sum.Failed != 0 || sum.Rewarded != 18 {
		t.Fatalf("summary %+v", sum)
	}
	if len(f.created) != 6 {
		t.Fatalf("created %d envs, want 6 created once", len(f.created))
	}
	for id, n := range f.created {
		if n != 1 || f.suspended[id] != 2 || f.deleted[id] != 1 {
			t.Fatalf("env %s created %d suspended %d deleted %d", id, n, f.suspended[id], f.deleted[id])
		}
	}
	resets, setups := countCommands(f, "git checkout", "git commit baseline")
	if resets != 12 || setups != 6 {
		t.Fatalf("suspend park: %d resets (want 6 envs x 2 later steps), %d setups (want 6, once per environment)", resets, setups)
	}
	rep := Build(parseEvents(t, out.String()))
	if len(rep.Steps) != 3 || rep.Steps[0].Tasks != 6 || rep.Steps[0].ParkMS == 0 || rep.Steps[2].ParkMS != 0 {
		t.Fatalf("steps %+v", rep.Steps)
	}
	if s := stat(rep, EventWake); s.Count != 12 {
		t.Fatalf("wake events %+v, want 12 (6 envs x steps 2 and 3)", s)
	}
	if s := stat(rep, EventTTFI); s.Count != 6 {
		t.Fatalf("ttfi events %+v, want 6", s)
	}
}

func TestStepsParkEachBoundsAwakeEnvironments(t *testing.T) {
	for _, park := range []Park{ParkSuspend, ParkSuspendSync, ParkSuspendAsync, ParkDelete} {
		t.Run(string(park), func(t *testing.T) {
			f := newFake()
			var out bytes.Buffer
			cfg := baseConfig(t, f, &out)
			cfg.Mode = ModeSteps
			cfg.Rollouts = 3 // 9 environments on a width of 2
			cfg.Steps = 3
			cfg.Park = park
			cfg.ParkEach = true

			sum, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if sum.Tasks != 27 || sum.Failed != 0 || sum.Rewarded != 27 {
				t.Fatalf("summary %+v", sum)
			}
			if m := f.maxAwake.Load(); m > int32(cfg.Width) {
				t.Fatalf("%d environments awake at once, want at most the width %d", m, cfg.Width)
			}
			rep := Build(parseEvents(t, out.String()))
			if len(rep.Steps) != 3 || rep.Steps[0].Tasks != 9 || rep.Steps[0].BarrierMS == 0 || rep.Steps[0].ParkMS != 0 {
				t.Fatalf("steps %+v, want 3 steps of 9 tasks with a barrier and no park event", rep.Steps)
			}
			wakes, ttfis := stat(rep, EventWake).Count, stat(rep, EventTTFI).Count
			switch park {
			case ParkSuspend, ParkSuspendSync, ParkSuspendAsync:
				if len(f.created) != 9 || wakes != 18 || ttfis != 9 {
					t.Fatalf("created %d wakes %d ttfis %d, want 9 created once, 18 wakes, 9 ttfis", len(f.created), wakes, ttfis)
				}
				// suspend and suspend-sync park durably, suspend-async locally.
				want, other := f.suspended, f.suspendedAsync
				if park == ParkSuspendAsync {
					want, other = f.suspendedAsync, f.suspended
				}
				for id, n := range f.created {
					if n != 1 || want[id] != 2 || other[id] != 0 || f.deleted[id] != 1 {
						t.Fatalf("env %s created %d parked %d (other mode %d) deleted %d, want 1, 2 in the %s mode (the last step deletes instead), 0 and 1", id, n, want[id], other[id], f.deleted[id], park)
					}
				}
				// The last step's deletes happen inside the step: every delete
				// event precedes the step's barrier event.
				events := parseEvents(t, out.String())
				lastBarrier, deletesBefore := -1, 0
				for i, ev := range events {
					if ev.Event == EventBarrier && ev.Step == 3 {
						lastBarrier = i
					}
				}
				for _, ev := range events[:lastBarrier+1] {
					if ev.Event == EventDelete && ev.Step == 3 {
						deletesBefore++
					}
				}
				if lastBarrier < 0 || deletesBefore != 9 {
					t.Fatalf("%d of 9 last-step deletes recorded before the step 3 barrier (at %d); the rest ran in the cleanup", deletesBefore, lastBarrier)
				}
			case ParkDelete:
				if len(f.created) != 27 || wakes != 0 || ttfis != 27 {
					t.Fatalf("created %d wakes %d ttfis %d, want 27 created once, no wakes, 27 ttfis", len(f.created), wakes, ttfis)
				}
				for id, n := range f.created {
					if n != 1 || f.deleted[id] != 1 {
						t.Fatalf("env %s created %d deleted %d", id, n, f.deleted[id])
					}
				}
			}
		})
	}
}

// With Keep the last step leaves the fleet in place on both park modes, as
// the barrier path does: a suspend park parks it like the other steps, a
// delete park leaves it awake.
func TestStepsParkEachKeepParksTheLastStep(t *testing.T) {
	for _, park := range []Park{ParkSuspend, ParkDelete} {
		t.Run(string(park), func(t *testing.T) {
			f := newFake()
			var out bytes.Buffer
			cfg := baseConfig(t, f, &out)
			cfg.Mode = ModeSteps
			cfg.Rollouts = 2 // 6 environments
			cfg.Steps = 2
			cfg.Park = park
			cfg.ParkEach = true
			cfg.Keep = true
			if _, err := Run(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
			switch park {
			case ParkSuspend:
				if len(f.created) != 6 || len(f.deleted) != 0 {
					t.Fatalf("created %d deleted %v, want 6 created and none deleted", len(f.created), f.deleted)
				}
				for id, n := range f.suspended {
					if n != 2 {
						t.Fatalf("env %s suspended %d times, want 2 (every step parks under -keep)", id, n)
					}
				}
			case ParkDelete:
				// Step 1 retires its 6; step 2 creates 6 more and leaves them
				// awake, as the barrier path's -park delete -keep does.
				left := 0
				for id, n := range f.created {
					if n != 1 || f.deleted[id] > 1 {
						t.Fatalf("env %s created %d deleted %d", id, n, f.deleted[id])
					}
					if f.deleted[id] == 0 {
						left++
					}
				}
				if len(f.created) != 12 || left != 6 || len(f.suspended) != 0 || f.awake.Load() != 6 {
					t.Fatalf("created %d left %d suspended %d awake %d, want 12 created, 6 left awake, none suspended",
						len(f.created), left, len(f.suspended), f.awake.Load())
				}
			}
		})
	}
}

func TestStepsParkEachDeletesFailedEnvironmentsAtOnce(t *testing.T) {
	f := newFake()
	f.unavailable = 1000 // every first command fails until the wake budget runs out
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeSteps
	cfg.Rollouts = 3
	cfg.Steps = 2
	cfg.Park = ParkSuspend
	cfg.ParkEach = true
	cfg.WakeBudget = 5 * time.Millisecond
	cfg.WakeBackoff = 2 * time.Millisecond

	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Failed != 9 {
		t.Fatalf("summary %+v, want all 9 failed", sum)
	}
	if m := f.maxAwake.Load(); m > int32(cfg.Width) {
		t.Fatalf("%d environments awake at once, want at most the width %d: a failed environment kept its slot", m, cfg.Width)
	}
	for id, n := range f.created {
		if n != 1 || f.deleted[id] != 1 || f.suspended[id] != 0 {
			t.Fatalf("env %s created %d deleted %d suspended %d, want 1, 1 (deleted when its step failed) and 0", id, n, f.deleted[id], f.suspended[id])
		}
	}
}

func TestStepsDeleteParkRecreates(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeSteps
	cfg.Rollouts = 1
	cfg.Steps = 2
	cfg.Park = ParkDelete

	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	// Each step creates under a fresh name: 3 environments x 2 steps.
	if len(f.created) != 6 {
		t.Fatalf("created %d distinct names, want 6: %v", len(f.created), f.created)
	}
	fresh := 0
	for id, n := range f.created {
		if n != 1 || f.deleted[id] != 1 || f.suspended[id] != 0 {
			t.Fatalf("env %s created %d deleted %d suspended %d", id, n, f.deleted[id], f.suspended[id])
		}
		if strings.HasSuffix(id, "-s2") {
			fresh++
		}
	}
	if fresh != 3 {
		t.Fatalf("%d step-2 names, want 3", fresh)
	}
	rep := Build(parseEvents(t, out.String()))
	if s := stat(rep, EventTTFI); s.Count != 6 {
		t.Fatalf("ttfi events %+v, want 3 envs x 2 creates", s)
	}
	if hasStat(rep, EventWake) {
		t.Fatal("delete park has no wake events")
	}
	resets, setups := countCommands(f, "git checkout", "git commit baseline")
	if resets != 0 || setups != 6 {
		t.Fatalf("delete park: %d resets (want 0, every environment is new), %d setups (want 3 envs x 2 creates)", resets, setups)
	}
}

// countCommands counts the fake's recorded commands containing each needle.
func countCommands(f *fakeBackend, resetNeedle, setupNeedle string) (resets, setups int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.commands {
		if strings.Contains(c, resetNeedle) {
			resets++
		}
		if strings.Contains(c, setupNeedle) {
			setups++
		}
	}
	return resets, setups
}

func TestStepsKeepLeavesFleet(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeSteps
	cfg.Rollouts = 1
	cfg.Steps = 1
	cfg.Park = ParkNone
	cfg.Keep = true
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if len(f.deleted) != 0 || len(f.suspended) != 0 {
		t.Fatalf("keep: deleted %v suspended %v", f.deleted, f.suspended)
	}
}

func TestFirstCommandRetriesUnavailable(t *testing.T) {
	f := newFake()
	f.unavailable = 2
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	cfg.Instances = instances(1)
	cfg.WakeBudget = time.Second
	tr, err := ParseTrajectory([]byte("name: t\nsteps:\n  - {name: a, run: \"true\", reward: true}\n"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Trajectory = tr
	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Failed != 0 {
		t.Fatalf("summary %+v", sum)
	}

	rep := Build(parseEvents(t, out.String()))
	if s := stat(rep, EventTTFI); s.Count != 1 {
		t.Fatalf("ttfi %+v", s)
	}
	for _, e := range parseEvents(t, out.String()) {
		if e.Event == EventTTFI && e.Attempts != 3 {
			t.Fatalf("ttfi attempts = %d, want 3", e.Attempts)
		}
	}

	f = newFake()
	f.unavailable = 1000
	cfg.Backend = f
	cfg.WakeBudget = 5 * time.Millisecond
	cfg.WakeBackoff = 2 * time.Millisecond
	sum, err = Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Failed != 1 {
		t.Fatalf("budget exhausted must fail the task: %+v", sum)
	}
}

func TestIsTransient(t *testing.T) {
	for code, want := range map[codes.Code]bool{
		codes.Unavailable: true, codes.Unknown: true, codes.Internal: true, codes.Aborted: true, codes.DeadlineExceeded: true,
		codes.InvalidArgument: false, codes.NotFound: false, codes.FailedPrecondition: false, codes.AlreadyExists: false,
	} {
		if got := isTransient(status.Error(code, "x")); got != want {
			t.Errorf("%v: transient = %v, want %v", code, got, want)
		}
	}
	if isTransient(errors.New("plain")) {
		t.Error("a non-status error is not transient")
	}
}

func TestConfigValidation(t *testing.T) {
	f := newFake()
	good := baseConfig(t, f, nil)
	good.Mode = ModeEval
	good.TasksPerImage = 1
	bad := []func(c *Config){
		func(c *Config) { c.RunID = "" },
		func(c *Config) { c.RunID = "Swe-1" },
		func(c *Config) { c.RunID = "swe_1" },
		func(c *Config) { c.RunID = strings.Repeat("a", 21) },
		func(c *Config) { c.Backend = nil },
		func(c *Config) { c.Instances = nil },
		func(c *Config) { c.Trajectory = nil },
		func(c *Config) { c.Width = 0 },
		func(c *Config) { c.TasksPerImage = 0 },
		func(c *Config) { c.Mode = "bogus" },
		func(c *Config) { c.Mode = ModeSteps; c.Rollouts = 0; c.Steps = 1 },
		func(c *Config) { c.Mode = ModeSteps; c.Rollouts = 1; c.Steps = 1; c.Park = "bogus" },
		func(c *Config) { c.Mode = ModeSteps; c.Rollouts = 1; c.Steps = 1; c.Park = ParkNone; c.ParkEach = true },
		func(c *Config) { c.Mode = ModeSteps; c.Rollouts = 1; c.Steps = 1; c.Park = ParkFork; c.ParkEach = true },
		func(c *Config) {
			c.Mode = ModeSteps
			c.Rollouts = 1
			c.Steps = 1
			c.Park = ParkDelete
			c.ParkEach = true
			c.ImagesPerStep = 1
		},
	}
	for i, mutate := range bad {
		c := good
		mutate(&c)
		if _, err := Run(context.Background(), c); err == nil {
			t.Errorf("case %d: expected a validation error", i)
		}
	}
}

func TestEnvID(t *testing.T) {
	if got := EnvID("swe-1", "astropy__astropy-7336", 2); got != "swe-1-astropy--astropy-7336-2" {
		t.Fatalf("got %q", got)
	}
	long := EnvID("r", strings.Repeat("x", 80)+"__y", 0)
	if len(long) > 63 {
		t.Fatalf("too long: %q", long)
	}
	if long == EnvID("r", strings.Repeat("x", 80)+"__z", 0) {
		t.Fatal("truncated ids must stay distinct")
	}
}

func parseEvents(t *testing.T, jsonl string) []Event {
	t.Helper()
	var events []Event
	for _, line := range strings.Split(strings.TrimSpace(jsonl), "\n") {
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	return events
}

func stat(rep *Report, event string) Stat {
	for _, s := range rep.Stats {
		if s.Event == event {
			return s
		}
	}
	return Stat{}
}

func hasStat(rep *Report, event string) bool {
	return stat(rep, event).Count > 0
}

// A delete park that fails must not leak the environment: it leaves the run,
// keeps its handle, and the final cleanup deletes it.
func TestStepsFailedDeleteParkDoesNotLeak(t *testing.T) {
	f := newFake()
	cfg := baseConfig(t, f, nil)
	cfg.Mode = ModeSteps
	cfg.Rollouts = 1
	cfg.Steps = 3
	cfg.Park = ParkDelete
	for _, in := range cfg.Instances {
		f.failDelete[EnvID(cfg.RunID, in.InstanceID, 0)] = 1
	}

	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if len(f.created) != 3 {
		t.Fatalf("created %v; want only the 3 step-1 environments, which leave the run after the failed park", f.created)
	}
	for id := range f.created {
		if f.deleted[id] != 1 {
			t.Errorf("environment %s deleted %d times, want 1 (leaked)", id, f.deleted[id])
		}
	}
}

func TestValidateChecksTheLongestName(t *testing.T) {
	long := Instance{InstanceID: strings.Repeat("a", 40), Image: "reg/img@" + goodDigest, Patch: "P", EvalScript: "E"}
	base := func() Config {
		cfg := baseConfig(t, newFake(), nil)
		cfg.RunID = strings.Repeat("r", maxRunIDLen)
		cfg.Instances = []Instance{long}
		cfg.Mode = ModeSteps
		cfg.Rollouts = 11
		cfg.Steps = 10
		cfg.Park = ParkDelete
		return cfg
	}
	// 20 + 1 + 36 + 1 + "10" + "-s10" = 64 characters.
	cfg := base()
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "more than Substrate's 63") {
		t.Fatalf("validate() = %v, want the 64-character name refused", err)
	}
	for name, edit := range map[string]func(*Config){
		"nine steps (63 characters)": func(c *Config) { c.Steps = 9 },
		"suspend park, no suffix":    func(c *Config) { c.Park = ParkSuspend },
		"ten rollouts":               func(c *Config) { c.Rollouts = 10 },
	} {
		cfg := base()
		edit(&cfg)
		if err := cfg.validate(); err != nil {
			t.Errorf("%s: validate() = %v, want nil", name, err)
		}
		if n := cfg.longestName(); len(n) > maxNameLen {
			t.Errorf("%s: longest name %q has %d characters", name, n, len(n))
		}
	}
	// Eval mode adds no suffix: the replay index alone counts.
	cfg = base()
	cfg.Mode, cfg.TasksPerImage = ModeEval, 100001 // largest index 100000
	if err := cfg.validate(); err == nil {
		t.Fatal("eval mode with a 6-digit replay index: want the long name refused")
	}
}

type zeroRewardGrader struct{}

func (zeroRewardGrader) Parse(stdout []byte) (*int, string, error) {
	if !strings.Contains(string(stdout), "RESOLVED=0") {
		return nil, "", errors.New("no zero-reward marker")
	}
	r := 0
	return &r, "custom-zero-verdict", nil
}

type zeroRewardAdapter struct{}

func (zeroRewardAdapter) Kind() string                  { return "zero-reward-test" }
func (zeroRewardAdapter) Validate(Instance) error       { return nil }
func (zeroRewardAdapter) RequireGrading(Instance) error { return nil }
func (zeroRewardAdapter) TemplateData(in Instance) (any, error) {
	return in, nil
}
func (zeroRewardAdapter) Grades(t *Trajectory) bool {
	for _, steps := range [][]Step{t.Setup, t.Reset, t.Steps} {
		for _, s := range steps {
			if strings.Contains(s.Run, "RESOLVED=0") {
				return true
			}
		}
	}
	return false
}
func (zeroRewardAdapter) Grader() Grader { return zeroRewardGrader{} }

func TestGraderRewardOverridesZeroExitCode(t *testing.T) {
	RegisterAdapter(zeroRewardAdapter{})
	t.Cleanup(func() { unregisterAdapter("zero-reward-test") })
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeEval
	cfg.TasksPerImage = 1
	inst := Instance{InstanceID: "repo__repo-a", Image: "reg/img@" + goodDigest, Kind: "zero-reward-test", Patch: "PATCH", EvalScript: "EVAL"}
	cfg.Instances = []Instance{inst}
	// Reward step runs "echo RESOLVED=0" (exit code 0), and the adapter's Grader returns reward=0.
	cfg.Trajectory = testTrajectory(t, "echo RESOLVED=0")

	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Tasks != 1 || sum.Failed != 0 || sum.Rewarded != 0 {
		t.Fatalf("summary = %+v, want Tasks=1 Failed=0 Rewarded=0", sum)
	}
	rep := Build(parseEvents(t, out.String()))
	if rep.RewardRate != 0 || len(rep.Unresolved) != 1 || rep.Unresolved[0].Reason != "custom-zero-verdict" {
		t.Fatalf("report = %+v, want RewardRate=0 and unresolved reason custom-zero-verdict", rep)
	}
}

type nilGraderAdapter struct{}

func (nilGraderAdapter) Kind() string                          { return "nil-grader-test" }
func (nilGraderAdapter) Validate(Instance) error               { return nil }
func (nilGraderAdapter) RequireGrading(Instance) error         { return nil }
func (nilGraderAdapter) TemplateData(in Instance) (any, error) { return in, nil }
func (nilGraderAdapter) Grades(*Trajectory) bool               { return false }
func (nilGraderAdapter) Grader() Grader                        { return nil }

func TestGraderNilFallbackUsesExitCode(t *testing.T) {
	RegisterAdapter(nilGraderAdapter{})
	t.Cleanup(func() { unregisterAdapter("nil-grader-test") })

	for _, tc := range []struct {
		cmd        string
		wantReward int
	}{
		{cmd: "true", wantReward: 1},
		{cmd: "exit 1", wantReward: 0},
	} {
		f := newFake()
		var out bytes.Buffer
		cfg := baseConfig(t, f, &out)
		cfg.Mode = ModeEval
		cfg.TasksPerImage = 1
		cfg.Instances = []Instance{{InstanceID: "repo__repo-a", Image: "reg/img@" + goodDigest, Kind: "nil-grader-test"}}
		cfg.Trajectory = testTrajectory(t, tc.cmd)
		sum, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatalf("%s: %v", tc.cmd, err)
		}
		if sum.Rewarded != tc.wantReward {
			t.Fatalf("%s: Rewarded = %d, want %d", tc.cmd, sum.Rewarded, tc.wantReward)
		}
	}
}

func TestLabelsAreDeclared(t *testing.T) {
	cfg := Config{Mode: ModeSteps, Width: 3, Trajectory: &Trajectory{Name: "t"}, Rollouts: 2, Steps: 4, Park: ParkDelete,
		ImagesPerStep: 2, CreateTimeout: time.Minute, StepTimeout: time.Minute, WakeBudget: time.Minute, WakeBackoff: time.Second,
		Labels: map[string]string{"commit": "abc"}, Invocation: map[string]string{"dataset": "d.jsonl"}}
	l := cfg.labels()
	if l["commit"] != "abc" || l["dataset"] != "d.jsonl" || l["width"] != "3" || l["rollouts"] != "2" || l["wake_budget"] != "1m0s" || l["images_per_step"] != "2" {
		t.Fatalf("labels = %v", l)
	}
	for k := range l {
		if k == "commit" {
			continue
		}
		if !slices.Contains(LoaderLabelKeys, k) {
			t.Errorf("the loader records %s but LoaderLabelKeys lacks it, so report -arm would not compare it", k)
		}
	}
}

// An operator label may not take a key the loader records: report -arm
// trusts those keys to tell one workload from another.
func TestValidateRejectsLoaderLabelKeys(t *testing.T) {
	for _, k := range []string{"width", "dataset", "set.READ_FILES"} {
		cfg := baseConfig(t, newFake(), nil)
		cfg.Mode, cfg.TasksPerImage = ModeEval, 1
		cfg.Labels = map[string]string{k: "x"}
		if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "label "+k) {
			t.Errorf("validate() with -label %s = %v, want it refused", k, err)
		}
	}
	cfg := baseConfig(t, newFake(), nil)
	cfg.Mode, cfg.TasksPerImage = ModeEval, 1
	cfg.Labels = map[string]string{"commit": "abc"}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() with an operator label: %v", err)
	}
}

func TestMixedTrajectoriesDrawPerEnvironment(t *testing.T) {
	run := func(seed int64) map[string]string {
		f := newFake()
		var out bytes.Buffer
		cfg := baseConfig(t, f, &out)
		cfg.Mode = ModeEval
		cfg.TasksPerImage = 4
		a := testTrajectory(t, "true")
		a.Name = "alpha"
		b := testTrajectory(t, "true")
		b.Name = "beta"
		cfg.Trajectories = []*Trajectory{a, b}
		cfg.Seed = seed
		if _, err := Run(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
		byEnv := map[string]string{}
		for _, e := range parseEvents(t, out.String()) {
			if e.Event == EventTask {
				byEnv[e.Env] = e.Name
			}
			if e.Event == EventRun && (e.Labels["trajectory"] != "alpha,beta" || e.Labels["seed"] != strconv.FormatInt(seed, 10)) {
				t.Fatalf("run labels: %v", e.Labels)
			}
		}
		return byEnv
	}
	first := run(7)
	if len(first) != 12 {
		t.Fatalf("task events: %v", first)
	}
	seen := map[string]int{}
	for _, name := range first {
		seen[name]++
	}
	if seen["alpha"] == 0 || seen["beta"] == 0 {
		t.Fatalf("both trajectories must be drawn over 12 environments: %v", seen)
	}
	if again := run(7); fmt.Sprint(again) != fmt.Sprint(first) {
		t.Fatalf("the same seed must give the same assignment:\n%v\n%v", first, again)
	}
}

// A caller that sets only Trajectories gets the first as Trajectory; a nil
// entry in the mix is refused.
func TestValidateDefaultsTrajectoryFromTheMix(t *testing.T) {
	cfg := baseConfig(t, newFake(), nil)
	cfg.Mode, cfg.TasksPerImage = ModeEval, 1
	only := cfg.Trajectory
	cfg.Trajectory, cfg.Trajectories = nil, []*Trajectory{only}
	if err := cfg.validate(); err != nil || cfg.Trajectory != only {
		t.Fatalf("validate() = %v, Trajectory %p; want nil and the mix's first %p", err, cfg.Trajectory, only)
	}
	cfg.Trajectories = []*Trajectory{only, nil}
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "trajectory 2 of the mix is nil") {
		t.Fatalf("validate() with a nil entry = %v", err)
	}
}

func TestImagesPerStepTakesTheNextWindowEachStep(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Instances = instances(5) // a..e
	cfg.Mode = ModeSteps
	cfg.Park = ParkDelete
	cfg.Rollouts = 1
	cfg.Steps = 3
	cfg.ImagesPerStep = 2

	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 2 images x 1 rollout x 3 steps; the third step wraps around to "a".
	if sum.Tasks != 6 || sum.Failed != 0 || sum.Rewarded != 6 {
		t.Fatalf("summary %+v", sum)
	}
	want := map[string]bool{
		"t-repo--repo-a-0": true, "t-repo--repo-b-0": true,
		"t-repo--repo-c-0-s2": true, "t-repo--repo-d-0-s2": true,
		"t-repo--repo-e-0-s3": true, "t-repo--repo-a-0-s3": true,
	}
	if len(f.created) != len(want) {
		t.Fatalf("created %v, want %v", f.created, want)
	}
	for id := range want {
		if f.created[id] != 1 || f.deleted[id] != 1 {
			t.Fatalf("env %s created %d deleted %d; created %v deleted %v", id, f.created[id], f.deleted[id], f.created, f.deleted)
		}
	}
	events := parseEvents(t, out.String())
	ttfi, parks, barriers := 0, 0, 0
	for _, e := range events {
		switch e.Event {
		case EventTTFI:
			ttfi++
		case EventPark:
			parks++
			if e.Name != string(ParkDelete) || e.Count != 2 {
				t.Errorf("park event %+v", e)
			}
		case EventBarrier:
			barriers++
		case EventWake:
			t.Errorf("a window run never wakes: %+v", e)
		case EventRun:
			if e.Labels["images_per_step"] != "2" || e.Labels["park"] != "delete" {
				t.Errorf("run labels %v", e.Labels)
			}
		}
	}
	// Every environment is fresh, so every task starts with a ttfi; the last
	// window's retirement is the cleanup, not a park.
	if ttfi != 6 || barriers != 3 || parks != 2 {
		t.Fatalf("ttfi %d barriers %d parks %d, want 6 3 2", ttfi, barriers, parks)
	}
	rep := Build(events)
	if len(rep.Steps) != 3 || rep.Steps[0].Tasks != 2 || rep.Steps[2].Live != 2 {
		t.Fatalf("steps %+v", rep.Steps)
	}

	cfg.ImagesPerStep = 9
	if _, err := Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "more than the 5 instances") {
		t.Fatalf("a window larger than the dataset must be rejected: %v", err)
	}
	cfg.ImagesPerStep, cfg.Park = 2, ParkSuspend
	if _, err := Run(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "park mode delete") {
		t.Fatalf("a window with a suspend park must be rejected: %v", err)
	}
}

func TestStepsSuspendAsyncAndForkPark(t *testing.T) {
	// Test ParkSuspendAsync
	fAsync := newFake()
	var outAsync bytes.Buffer
	cfgAsync := baseConfig(t, fAsync, &outAsync)
	cfgAsync.Mode = ModeSteps
	cfgAsync.Rollouts = 1
	cfgAsync.Steps = 2
	cfgAsync.Park = ParkSuspendAsync

	sumAsync, err := Run(context.Background(), cfgAsync)
	if err != nil {
		t.Fatalf("Run(ParkSuspendAsync): %v", err)
	}
	if sumAsync.Tasks != 6 || sumAsync.Failed != 0 {
		t.Fatalf("ParkSuspendAsync summary = %+v", sumAsync)
	}
	for id := range fAsync.created {
		if fAsync.suspendedAsync[id] != 1 || fAsync.suspended[id] != 0 {
			t.Fatalf("env %s suspendedAsync=%d suspended=%d, want 1 and 0", id, fAsync.suspendedAsync[id], fAsync.suspended[id])
		}
	}

	// Test ParkFork
	fFork := newFake()
	var outFork bytes.Buffer
	cfgFork := baseConfig(t, fFork, &outFork)
	cfgFork.Mode = ModeSteps
	cfgFork.Rollouts = 1
	cfgFork.Steps = 2
	cfgFork.Park = ParkFork

	sumFork, err := Run(context.Background(), cfgFork)
	if err != nil {
		t.Fatalf("Run(ParkFork): %v", err)
	}
	if sumFork.Tasks != 6 || sumFork.Failed != 0 {
		t.Fatalf("ParkFork summary = %+v", sumFork)
	}
	if len(fFork.forked) != 3 {
		t.Fatalf("forked = %v, want 3 forked environments", fFork.forked)
	}
	for newID, fromID := range fFork.forked {
		if !strings.HasSuffix(newID, "-s2") {
			t.Errorf("forked newID = %q, want -s2 suffix", newID)
		}
		if fFork.deleted[fromID] != 1 || fFork.deleted[newID] != 1 {
			t.Errorf("deleted[%s]=%d, deleted[%s]=%d, want 1 each", fromID, fFork.deleted[fromID], newID, fFork.deleted[newID])
		}
	}
}

func TestOpenLoopModeRecordsSlotWaitMS(t *testing.T) {
	f := newFake()
	var out bytes.Buffer
	cfg := baseConfig(t, f, &out)
	cfg.Mode = ModeOpenLoop
	cfg.ArrivalRate = 500
	cfg.Duration = 40 * time.Millisecond
	cfg.Width = 1
	cfg.Seed = 42

	sum, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run open-loop: %v", err)
	}
	if sum.Tasks == 0 {
		t.Fatal("expected at least 1 task in open-loop run")
	}
	var taskEvents int
	for _, ev := range parseEvents(t, out.String()) {
		if ev.Event == EventTask {
			taskEvents++
			if ev.SlotWaitMS < 0 {
				t.Errorf("negative SlotWaitMS: %v", ev.SlotWaitMS)
			}
		}
	}
	if taskEvents == 0 {
		t.Fatal("expected EventTask events in open-loop output")
	}
}

// flakyCreateClient is an EnvClient whose Create fails on every second call.
type flakyCreateClient struct {
	calls atomic.Int64
}

func (c *flakyCreateClient) Create(_ context.Context, _, _ string) (EnvHandle, error) {
	n := c.calls.Add(1)
	if n%2 == 0 {
		return EnvHandle{}, errors.New("CreateError: no worker has room")
	}
	return EnvHandle{Name: fmt.Sprintf("env-%d", n), Source: string(schema.SourceColdStart)}, nil
}

func (c *flakyCreateClient) Exec(context.Context, string, string, time.Duration) (ExecResult, error) {
	return ExecResult{Stdout: "ok"}, nil
}

func (c *flakyCreateClient) Delete(context.Context, string) error { return nil }

func (c *flakyCreateClient) Snapshot(_ context.Context, name string) (string, error) {
	return "snap-" + name, nil
}

func (c *flakyCreateClient) Resume(_ context.Context, snap, _, _ string) (EnvHandle, error) {
	return EnvHandle{Name: "resumed-" + snap, Source: string(schema.SourceSnapshotResume)}, nil
}

// A rollout whose create fails is still a rollout. The simulated Runner must
// emit a failed task event for it on both loops, so all_rollouts counts every
// attempt and the failed creates are in the numerator and the denominator of
// the infra fault rate, not missing from both.
func TestRunnerFailedCreateIsARolloutInBothLoops(t *testing.T) {
	for _, loop := range []string{"open", "closed"} {
		t.Run(loop, func(t *testing.T) {
			var out bytes.Buffer
			rec := NewRecorder(&out)
			r := &Runner{
				Client:    &flakyCreateClient{},
				Recorder:  rec,
				SleepFunc: func(context.Context, time.Duration) error { return nil },
			}
			if loop == "open" {
				r.Config = Config{RunID: "sim", JobName: "job", Mode: ModeOpenLoop, Concurrency: 2,
					ArrivalRate: 2000, Duration: 30 * time.Millisecond, Seed: 7, Instances: SyntheticInstances(4, "")}
			} else {
				tasks := make([]schema.ScheduledTask, 6)
				for i := range tasks {
					tasks[i] = schema.ScheduledTask{InstanceID: fmt.Sprintf("inst-%d", i), Image: "img"}
				}
				r.Config = Config{RunID: "sim", JobName: "job", Mode: ModeSteps, Concurrency: 2}
				r.Scheduled = schema.ScheduledJob{Job: "job", Steps: []schema.ScheduledStep{{Step: 1, Tasks: tasks}}}
			}
			// The closed loop returns the first create error after the step
			// finishes; the events of every task are recorded either way.
			_ = r.Run(context.Background())
			if err := rec.Flush(); err != nil {
				t.Fatal(err)
			}
			events := parseEvents(t, out.String())
			var creates, failedCreates, tasks, failedTasks int
			for _, e := range events {
				switch {
				case e.Event == EventPhase && e.Name == "create":
					creates++
					if !e.OK {
						failedCreates++
					}
				case e.Event == EventTask:
					tasks++
					if !e.OK {
						failedTasks++
					}
				}
			}
			if creates == 0 || failedCreates == 0 {
				t.Fatalf("test needs attempted and failed creates, got %d/%d", failedCreates, creates)
			}
			if tasks != creates || failedTasks != failedCreates {
				t.Fatalf("task events %d (failed %d), want one per create %d (failed %d)", tasks, failedTasks, creates, failedCreates)
			}
			fc := Build(events).FaultClassification
			if fc == nil || fc.AllRollouts != creates || fc.InfraDropped != failedCreates || fc.Tier1Dropped != failedCreates {
				t.Fatalf("fault classification %+v, want all_rollouts=%d infra_dropped=tier1=%d", fc, creates, failedCreates)
			}
			if loop == "closed" && (creates != 6 || failedCreates != 3) {
				t.Fatalf("closed loop ran %d creates (%d failed), want 6 (3)", creates, failedCreates)
			}
		})
	}
}

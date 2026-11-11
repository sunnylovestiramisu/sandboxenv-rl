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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

// ForkableBackend is an optional extension to Backend for backends that support
// creating a child sandbox from a suspended golden base sandbox.
type ForkableBackend interface {
	Backend
	Fork(ctx context.Context, parentID, childID, image string) (Sandbox, error)
}

// WorkingSetEntry represents a sandbox managed by a WorkingSet.
type WorkingSetEntry struct {
	SandboxID   string
	BaseID      string
	Image       string
	InstanceID  string
	Sandbox     Sandbox
	CreateStart time.Time
	CreatedStep int
	LastStep    int
	ReuseCount  int
	Hit         bool
	WasParked   bool
	IsForkChild bool

	asyncDone chan struct{}
	asyncDur  time.Duration
	asyncEnd  time.Time
	asyncErr  error
}

// WorkingSetStats summarizes per-step or cumulative working-set behavior.
type WorkingSetStats struct {
	Hits                 int
	Misses               int
	HitRate              float64
	StandingPoolSize     int
	Created              int
	Reused               int
	Retired              int
	PrefetchLeadMarginMS float64
	ReleaseOverlapMS     float64
}

// WorkingSetConfig configures a WorkingSet manager for a job.
type WorkingSetConfig struct {
	JobID      string
	Backend    Backend
	ParkPolicy schema.ParkPolicy
	// MaxParked caps the number of idle sandboxes retained in the standing pool.
	// Zero means unbounded.
	MaxParked int
	// Logger receives the pool's warnings; nil means slog.Default().
	Logger *slog.Logger
}

type prewarmFuture struct {
	done   chan struct{}
	doneAt time.Time
	err    error
}

// errWorkingSetClosed is what enqueueParked returns once Close has run: the
// entry was not parked.
var errWorkingSetClosed = errors.New("swebench: working set closed")

// WorkingSet manages a client-side standing pool of parked sandboxes across
// training steps, matching each step's scheduled tasks by Image.
type WorkingSet struct {
	cfg     WorkingSetConfig
	backend Backend
	log     *slog.Logger

	mu          sync.Mutex
	parked      map[string][]*WorkingSetEntry // image -> FIFO queue of parked sandboxes
	parkedOrder []*WorkingSetEntry            // LRU order (oldest first) across all images
	forkBases   map[string]*WorkingSetEntry   // image -> golden base sandbox for ParkFork
	inFlight    []*WorkingSetEntry            // in-flight async suspends
	prewarms    map[int]*prewarmFuture        // step -> async prewarm job
	retiring    int                           // background retire deletes in flight
	retireIdle  chan struct{}                 // closed when retiring drops to zero
	closed      bool                          // set by Close; a late prewarm retires what it made

	seq       atomic.Uint64
	totalHits int
	totalMiss int
	created   int
	reused    int
	retired   int

	stepHits    int
	stepMisses  int
	stepCreated int
	stepReused  int
	stepRetired int
}

// NewWorkingSet constructs a WorkingSet for the given configuration.
func NewWorkingSet(cfg WorkingSetConfig) (*WorkingSet, error) {
	if cfg.JobID == "" {
		return nil, errors.New("swebench: workingset job_id is required")
	}
	if cfg.Backend == nil {
		return nil, errors.New("swebench: workingset backend is required")
	}
	norm, err := schema.NormalizeParkPolicy(cfg.ParkPolicy)
	if err != nil {
		return nil, err
	}
	cfg.ParkPolicy = norm
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &WorkingSet{
		cfg:       cfg,
		backend:   cfg.Backend,
		log:       cfg.Logger,
		parked:    make(map[string][]*WorkingSetEntry),
		forkBases: make(map[string]*WorkingSetEntry),
		prewarms:  make(map[int]*prewarmFuture),
	}, nil
}

// ParkPolicy returns the normalized park policy of this WorkingSet.
func (ws *WorkingSet) ParkPolicy() schema.ParkPolicy {
	return ws.cfg.ParkPolicy
}

func (ws *WorkingSet) nextSandboxID(instanceID string, step, taskIdx int) string {
	n := ws.seq.Add(1)
	base := EnvID(ws.cfg.JobID, instanceID, taskIdx)
	suffix := fmt.Sprintf("-s%d-%d", step, n)
	if len(base)+len(suffix) > maxNameLen {
		base = base[:maxNameLen-len(suffix)]
	}
	return base + suffix
}

// BeginStep resets the per-step working-set counters at the start of a step.
func (ws *WorkingSet) BeginStep() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.stepHits = 0
	ws.stepMisses = 0
	ws.stepCreated = 0
	ws.stepReused = 0
	ws.stepRetired = 0
}

// Acquire obtains a sandbox for (instanceID, image) at the given step and taskIndex.
// If a parked sandbox with the same image is available in the pool (or a golden
// base snapshot exists under ParkFork), it is reused (Hit = true); otherwise a
// new sandbox is created (Hit = false).
func (ws *WorkingSet) Acquire(ctx context.Context, step, taskIndex int, instanceID, image string) (*WorkingSetEntry, error) {
	// A context that expired before the acquire ran takes nothing off the
	// pool and counts nothing, under every park policy: a hit would only go
	// back, or be retired, for nothing, and a fork acquire would count a
	// miss, and create or fork, for a task that gets no sandbox.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ws.cfg.ParkPolicy == schema.ParkFork {
		return ws.acquireFork(ctx, step, taskIndex, instanceID, image)
	}

	for {
		// A retired hit sends the loop round again: check the context
		// before the next sandbox comes off the pool.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ws.mu.Lock()
		queue := ws.parked[image]
		var hit *WorkingSetEntry
		if len(queue) > 0 {
			hit = queue[0]
			if len(queue) == 1 {
				delete(ws.parked, image)
			} else {
				ws.parked[image] = queue[1:]
			}
			ws.removeFromOrderLocked(hit)
		}
		ws.mu.Unlock()

		if hit == nil {
			break
		}
		if hit.asyncDone != nil {
			if !awaitSettled(ctx, hit.asyncDone) {
				ws.repark(ctx, hit)
				return nil, ctx.Err()
			}
			if hit.asyncErr != nil {
				ws.deleteRetired(ctx, hit.SandboxID, hit.Sandbox, nil)
				ws.mu.Lock()
				ws.retired++
				ws.stepRetired++
				ws.mu.Unlock()
				continue
			}
		}
		ws.mu.Lock()
		ws.totalHits++
		ws.reused++
		ws.stepHits++
		ws.stepReused++
		ws.mu.Unlock()
		hit.InstanceID = instanceID
		hit.LastStep = step
		hit.ReuseCount++
		hit.Hit = true
		return hit, nil
	}

	ws.mu.Lock()
	ws.totalMiss++
	ws.stepMisses++
	ws.mu.Unlock()

	sbID := ws.nextSandboxID(instanceID, step, taskIndex)
	createStart := time.Now()
	sb, err := ws.backend.Create(ctx, sbID, image)
	if err != nil {
		return nil, err
	}
	ws.mu.Lock()
	ws.created++
	ws.stepCreated++
	ws.mu.Unlock()
	return &WorkingSetEntry{
		SandboxID:   sbID,
		BaseID:      sbID,
		Image:       image,
		InstanceID:  instanceID,
		Sandbox:     sb,
		CreateStart: createStart,
		CreatedStep: step,
		LastStep:    step,
		ReuseCount:  0,
		Hit:         false,
		WasParked:   false,
	}, nil
}

// awaitSettled waits, up to ctx, for the suspend of a parked sandbox and
// reports whether it settled. A settled suspend wins over an expired ctx:
// select picks at random between two ready cases, and a sandbox that
// settled, with or without an error, must not go back in the pool as one
// still suspending.
func awaitSettled(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
	case <-ctx.Done():
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// deleteRetired deletes, in the background, a sandbox the pool gives up on:
// one whose suspend failed, a base that lost the fork-base race, a prewarm
// the pool no longer needs, a parked sandbox a cancelled acquire found no
// room for. The delete starts once settled is closed (nil: at once) and
// runs under cleanupTimeout on a context that survives the caller's
// cancellation, thus a slow delete holds neither the acquire nor the
// release that gave the sandbox up. Close waits for the deletes still in
// flight. A failed delete is logged and not retried, thus the sandbox
// leaks. The caller counts the sandbox out.
func (ws *WorkingSet) deleteRetired(ctx context.Context, id string, sb Sandbox, settled <-chan struct{}) {
	if sb == nil {
		return
	}
	ws.mu.Lock()
	if ws.retiring == 0 {
		ws.retireIdle = make(chan struct{})
	}
	ws.retiring++
	ws.mu.Unlock()
	go func() {
		defer func() {
			ws.mu.Lock()
			ws.retiring--
			if ws.retiring == 0 {
				close(ws.retireIdle)
			}
			ws.mu.Unlock()
		}()
		if settled != nil {
			<-settled
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		err := sb.Delete(cleanupCtx)
		cancel()
		if err != nil && !isNotFound(err) {
			ws.log.Warn("working set: delete on retire failed; the sandbox leaks",
				slog.String("env", id), slog.String("err", err.Error()))
		}
	}()
}

// awaitRetires waits for the retire deletes in flight, up to ctx. Each delete
// runs under cleanupTimeout, thus the wait ends with or without a deadline
// on ctx.
func (ws *WorkingSet) awaitRetires(ctx context.Context) error {
	ws.mu.Lock()
	idle, pending := ws.retireIdle, ws.retiring
	ws.mu.Unlock()
	if pending == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// repark puts a parked sandbox back at the head of its queue, and of the
// eviction order, after the acquire that took it was cancelled while the
// sandbox still suspended. When other releases filled MaxParked in the
// meantime, the sandbox is counted out and deleted once its suspend
// settles: the pool keeps a settled sandbox over one a cancelled acquire
// hands back.
func (ws *WorkingSet) repark(ctx context.Context, e *WorkingSetEntry) {
	ws.mu.Lock()
	if ws.cfg.MaxParked > 0 && len(ws.parkedOrder) >= ws.cfg.MaxParked {
		ws.retired++
		ws.stepRetired++
		ws.mu.Unlock()
		ws.deleteRetired(ctx, e.SandboxID, e.Sandbox, e.asyncDone)
		return
	}
	ws.parked[e.Image] = append([]*WorkingSetEntry{e}, ws.parked[e.Image]...)
	ws.parkedOrder = append([]*WorkingSetEntry{e}, ws.parkedOrder...)
	ws.mu.Unlock()
}

func suspendSandbox(ctx context.Context, sb Sandbox, async bool) error {
	if ms, ok := sb.(ModeSuspender); ok {
		return ms.SuspendWithMode(ctx, async)
	}
	if async {
		if as, ok := sb.(AsyncSuspender); ok {
			return as.SuspendAsync(ctx)
		}
	}
	return sb.Suspend(ctx)
}

func (ws *WorkingSet) acquireFork(ctx context.Context, step, taskIndex int, instanceID, image string) (*WorkingSetEntry, error) {
	// As in Acquire, a miss counts before the create and a hit counts once
	// the sandbox is handed out: here, once the fork returns the child.
	ws.mu.Lock()
	base, hasBase := ws.forkBases[image]
	if !hasBase {
		ws.totalMiss++
		ws.stepMisses++
	}
	ws.mu.Unlock()

	if !hasBase {
		baseID := ws.nextSandboxID("base-"+instanceID, step, taskIndex)
		createStart := time.Now()
		baseSB, err := ws.backend.Create(ctx, baseID, image)
		if err != nil {
			return nil, err
		}
		ws.mu.Lock()
		ws.created++
		ws.stepCreated++
		ws.mu.Unlock()
		if err := suspendSandbox(ctx, baseSB, true); err != nil {
			ws.deleteRetired(ctx, baseID, baseSB, nil)
			ws.mu.Lock()
			ws.retired++
			ws.stepRetired++
			ws.mu.Unlock()
			return nil, err
		}
		base = &WorkingSetEntry{
			SandboxID:   baseID,
			BaseID:      baseID,
			Image:       image,
			InstanceID:  instanceID,
			Sandbox:     baseSB,
			CreateStart: createStart,
			CreatedStep: step,
			LastStep:    step,
			WasParked:   true,
		}
		ws.mu.Lock()
		if existing, ok := ws.forkBases[image]; ok {
			ws.retired++
			ws.stepRetired++
			ws.mu.Unlock()
			ws.deleteRetired(ctx, baseID, baseSB, nil)
			base = existing
		} else {
			ws.forkBases[image] = base
			ws.mu.Unlock()
		}
	}

	childID := ws.nextSandboxID(instanceID, step, taskIndex)
	createStart := time.Now()
	var (
		childSB Sandbox
		err     error
	)
	if fb, ok := ws.backend.(ForkableBackend); ok {
		childSB, err = fb.Fork(ctx, base.SandboxID, childID, image)
	} else if sf, ok := base.Sandbox.(SandboxForker); ok {
		childSB, err = sf.Fork(ctx, childID)
	} else if fb, ok := ws.backend.(ForkBackend); ok {
		childSB, err = fb.Fork(ctx, base.SandboxID, childID)
	} else {
		childSB, err = ws.backend.Create(ctx, childID, image)
	}
	if err != nil {
		return nil, err
	}
	ws.mu.Lock()
	ws.created++
	ws.stepCreated++
	if hasBase {
		ws.totalHits++
		ws.reused++
		ws.stepHits++
		ws.stepReused++
	}
	ws.mu.Unlock()
	return &WorkingSetEntry{
		SandboxID:   childID,
		BaseID:      base.SandboxID,
		Image:       image,
		InstanceID:  instanceID,
		Sandbox:     childSB,
		CreateStart: createStart,
		CreatedStep: step,
		LastStep:    step,
		ReuseCount:  base.ReuseCount + 1,
		Hit:         hasBase,
		WasParked:   hasBase,
		IsForkChild: true,
	}, nil
}

// Release parks or retires an active sandbox at the end of its task or step
// according to the WorkingSet's ParkPolicy.
func (ws *WorkingSet) Release(ctx context.Context, entry *WorkingSetEntry) error {
	if entry == nil || entry.Sandbox == nil {
		return nil
	}
	switch ws.cfg.ParkPolicy {
	case schema.ParkDelete:
		err := entry.Sandbox.Delete(ctx)
		ws.mu.Lock()
		ws.retired++
		ws.stepRetired++
		ws.mu.Unlock()
		return err

	case schema.ParkFork:
		if entry.IsForkChild {
			err := entry.Sandbox.Delete(ctx)
			ws.mu.Lock()
			ws.retired++
			ws.stepRetired++
			ws.mu.Unlock()
			return err
		}
		return nil

	case schema.ParkNone:
		entry.WasParked = false
		entry.asyncDone = nil
		return ws.enqueueParked(ctx, entry)

	case schema.ParkSuspendSync:
		start := time.Now()
		if err := suspendSandbox(ctx, entry.Sandbox, false); err != nil {
			ws.deleteRetired(ctx, entry.SandboxID, entry.Sandbox, nil)
			ws.mu.Lock()
			ws.retired++
			ws.stepRetired++
			ws.mu.Unlock()
			return err
		}
		entry.WasParked = true
		entry.asyncDur = time.Since(start)
		entry.asyncEnd = time.Now()
		entry.asyncDone = nil
		return ws.enqueueParked(ctx, entry)

	case schema.ParkSuspendAsync:
		done := make(chan struct{})
		entry.asyncDone = done
		entry.asyncErr = nil
		entry.WasParked = true
		ws.mu.Lock()
		ws.inFlight = append(ws.inFlight, entry)
		ws.mu.Unlock()
		if err := ws.enqueueParked(ctx, entry); err != nil {
			close(done)
			return err
		}
		go func(e *WorkingSetEntry) {
			start := time.Now()
			err := suspendSandbox(ctx, e.Sandbox, true)
			e.asyncDur = time.Since(start)
			e.asyncEnd = time.Now()
			e.asyncErr = err
			close(done)
		}(entry)
		return nil

	default:
		return fmt.Errorf("swebench: unsupported park policy %q", ws.cfg.ParkPolicy)
	}
}

// AwaitAsyncSuspends waits for all in-flight asynchronous suspends launched
// during the current step and returns the total overlapped suspend duration
// (in milliseconds) that completed before stepResumeAt.
func (ws *WorkingSet) AwaitAsyncSuspends(ctx context.Context, stepBarrierAt, stepResumeAt time.Time) (float64, error) {
	ws.mu.Lock()
	pending := ws.inFlight
	ws.inFlight = nil
	ws.mu.Unlock()

	var (
		overlapMS float64
		firstErr  error
	)
	for _, e := range pending {
		if e.asyncDone != nil {
			select {
			case <-ctx.Done():
				return overlapMS, ctx.Err()
			case <-e.asyncDone:
			}
			if e.asyncErr != nil && firstErr == nil {
				firstErr = e.asyncErr
			}
			dur := e.asyncDur
			if !stepResumeAt.IsZero() && !stepBarrierAt.IsZero() {
				pauseWindow := stepResumeAt.Sub(stepBarrierAt)
				if pauseWindow > 0 && dur < pauseWindow {
					overlapMS += ms(dur)
				} else if pauseWindow > 0 {
					overlapMS += ms(pauseWindow)
				} else {
					overlapMS += ms(dur)
				}
			} else {
				overlapMS += ms(dur)
			}
		}
	}
	return overlapMS, firstErr
}

// Prewarm asynchronously creates and parks sandboxes for nextStep's prefetchImages
// off the critical path when they are not already present in the standing pool.
func (ws *WorkingSet) Prewarm(ctx context.Context, nextStep int, prefetchImages []string) {
	if len(prefetchImages) == 0 {
		return
	}
	fut := &prewarmFuture{done: make(chan struct{})}
	ws.mu.Lock()
	ws.prewarms[nextStep] = fut
	ws.mu.Unlock()

	go func() {
		defer func() {
			fut.doneAt = time.Now()
			close(fut.done)
		}()
		for i, img := range prefetchImages {
			if ctx.Err() != nil {
				fut.err = ctx.Err()
				return
			}
			ws.mu.Lock()
			if ws.closed {
				ws.mu.Unlock()
				fut.err = errWorkingSetClosed
				return
			}
			alreadyParked := len(ws.parked[img]) > 0
			if ws.cfg.ParkPolicy == schema.ParkFork && ws.forkBases[img] != nil {
				alreadyParked = true
			}
			ws.mu.Unlock()
			if alreadyParked {
				continue
			}

			sbID := ws.nextSandboxID("prewarm", nextStep, i)
			createStart := time.Now()
			sb, err := ws.backend.Create(ctx, sbID, img)
			if err != nil {
				fut.err = err
				continue
			}
			// A prewarm runs between steps, thus it counts into the run totals
			// only: created here, retired when the pool gives the sandbox up.
			ws.mu.Lock()
			ws.created++
			ws.mu.Unlock()
			retire := func() {
				ws.deleteRetired(ctx, sbID, sb, nil)
				ws.mu.Lock()
				ws.retired++
				ws.mu.Unlock()
			}

			if ws.cfg.ParkPolicy == schema.ParkFork {
				if err := suspendSandbox(ctx, sb, true); err != nil {
					retire()
					fut.err = err
					continue
				}
				ws.mu.Lock()
				if ws.forkBases[img] == nil && !ws.closed {
					ws.forkBases[img] = &WorkingSetEntry{
						SandboxID:   sbID,
						BaseID:      sbID,
						Image:       img,
						Sandbox:     sb,
						CreateStart: createStart,
						CreatedStep: nextStep,
						LastStep:    nextStep - 1,
						WasParked:   true,
					}
					ws.mu.Unlock()
					continue
				}
				closed := ws.closed
				ws.mu.Unlock()
				// Another cold acquire made the base first, or the pool closed
				// meanwhile: either way the pool has no use for this one. A
				// closed pool ends the prewarm, and WaitPrewarm says so.
				retire()
				if closed {
					fut.err = errWorkingSetClosed
					return
				}
				continue
			}

			wasParked := false
			if ws.cfg.ParkPolicy == schema.ParkSuspendAsync || ws.cfg.ParkPolicy == schema.ParkSuspendSync {
				if err := suspendSandbox(ctx, sb, ws.cfg.ParkPolicy == schema.ParkSuspendAsync); err != nil {
					retire()
					fut.err = err
					continue
				}
				wasParked = true
			}
			entry := &WorkingSetEntry{
				SandboxID:   sbID,
				BaseID:      sbID,
				Image:       img,
				Sandbox:     sb,
				CreateStart: createStart,
				CreatedStep: nextStep,
				LastStep:    nextStep - 1,
				WasParked:   wasParked,
			}
			if err := ws.enqueueParked(ctx, entry); errors.Is(err, errWorkingSetClosed) {
				retire()
				fut.err = errWorkingSetClosed
				return
			}
		}
	}()
}

// WaitPrewarm waits for any async Prewarm scheduled for step to finish and
// returns the prefetch lead margin in milliseconds (positive when prewarm
// finished before stepStart).
func (ws *WorkingSet) WaitPrewarm(ctx context.Context, step int, stepStart time.Time) (float64, error) {
	ws.mu.Lock()
	fut, ok := ws.prewarms[step]
	ws.mu.Unlock()
	if !ok || fut == nil {
		return 0, nil
	}
	select {
	case <-ctx.Done():
		// The future stays registered: Close waits for the prewarm a
		// cancelled wait leaves running.
		return 0, ctx.Err()
	case <-fut.done:
	}
	ws.mu.Lock()
	if ws.prewarms[step] == fut {
		delete(ws.prewarms, step)
	}
	ws.mu.Unlock()
	lead := stepStart.Sub(fut.doneAt)
	if lead < 0 {
		lead = 0
	}
	return ms(lead), fut.err
}

func (ws *WorkingSet) enqueueParked(ctx context.Context, entry *WorkingSetEntry) error {
	ws.mu.Lock()
	if ws.closed {
		ws.mu.Unlock()
		return errWorkingSetClosed
	}
	var evict []*WorkingSetEntry
	if ws.cfg.MaxParked > 0 {
		for len(ws.parkedOrder)+1 > ws.cfg.MaxParked && len(ws.parkedOrder) > 0 {
			oldest := ws.parkedOrder[0]
			ws.parkedOrder = ws.parkedOrder[1:]
			ws.removeFromImageQueueLocked(oldest)
			ws.retired++
			ws.stepRetired++
			evict = append(evict, oldest)
		}
	}
	ws.parked[entry.Image] = append(ws.parked[entry.Image], entry)
	ws.parkedOrder = append(ws.parkedOrder, entry)
	ws.mu.Unlock()

	var firstErr error
	for _, old := range evict {
		if old.asyncDone != nil {
			<-old.asyncDone
		}
		if err := old.Sandbox.Delete(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// RetireSurplus deletes parked sandboxes whose images are not in keepImages
// until the standing pool size is at most maxKeep.
func (ws *WorkingSet) RetireSurplus(ctx context.Context, keepImages map[string]bool, maxKeep int) (int, error) {
	ws.mu.Lock()
	var toDelete []*WorkingSetEntry
	remaining := make([]*WorkingSetEntry, 0, len(ws.parkedOrder))
	for _, e := range ws.parkedOrder {
		if !keepImages[e.Image] && (maxKeep <= 0 || len(ws.parkedOrder)-len(toDelete) > maxKeep) {
			ws.removeFromImageQueueLocked(e)
			ws.retired++
			ws.stepRetired++
			toDelete = append(toDelete, e)
		} else {
			remaining = append(remaining, e)
		}
	}
	ws.parkedOrder = remaining
	ws.mu.Unlock()

	var firstErr error
	for _, e := range toDelete {
		if e.asyncDone != nil {
			<-e.asyncDone
		}
		if err := e.Sandbox.Delete(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(toDelete), firstErr
}

// awaitPrewarms waits, up to ctx, for the prewarms still in flight.
func (ws *WorkingSet) awaitPrewarms(ctx context.Context) error {
	ws.mu.Lock()
	pending := make([]*prewarmFuture, 0, len(ws.prewarms))
	for _, fut := range ws.prewarms {
		pending = append(pending, fut)
	}
	ws.mu.Unlock()
	for _, fut := range pending {
		select {
		case <-fut.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Close marks the pool closed, waits for the prewarms still in flight,
// deletes all parked and fork-base sandboxes remaining in the pool, and
// waits for the retire deletes still in flight, each wait up to ctx. A
// parked sandbox whose suspend has not settled when ctx expires is deleted
// in the background once it settles, and Close returns the context error.
// A prewarm that reaches the pool after Close started retires what it
// made, thus a run cancelled mid-step leaves no sandbox behind in a closed
// pool.
func (ws *WorkingSet) Close(ctx context.Context) error {
	ws.mu.Lock()
	ws.closed = true
	ws.mu.Unlock()
	var firstErr error
	if err := ws.awaitPrewarms(ctx); err != nil {
		firstErr = err
	}

	ws.mu.Lock()
	all := append([]*WorkingSetEntry(nil), ws.parkedOrder...)
	for _, b := range ws.forkBases {
		all = append(all, b)
	}
	ws.parked = make(map[string][]*WorkingSetEntry)
	ws.parkedOrder = nil
	ws.forkBases = make(map[string]*WorkingSetEntry)
	ws.retired += len(all)
	ws.mu.Unlock()

	for _, e := range all {
		if e.asyncDone != nil && !awaitSettled(ctx, e.asyncDone) {
			// The suspend outlives ctx: the delete follows it in the
			// background, under cleanupTimeout, as for any retired sandbox.
			ws.deleteRetired(ctx, e.SandboxID, e.Sandbox, e.asyncDone)
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			continue
		}
		if e.Sandbox != nil {
			if err := e.Sandbox.Delete(ctx); err != nil && !isNotFound(err) && firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := ws.awaitRetires(ctx); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// StandingPoolSize returns the current number of parked sandboxes in the pool.
func (ws *WorkingSet) StandingPoolSize() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.cfg.ParkPolicy == schema.ParkFork {
		return len(ws.forkBases)
	}
	return len(ws.parkedOrder)
}

// StepStats returns the working-set metrics for the current step.
func (ws *WorkingSet) StepStats(prefetchLeadMS, releaseOverlapMS float64) WorkingSetStats {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	total := ws.stepHits + ws.stepMisses
	var rate float64
	if total > 0 {
		rate = float64(ws.stepHits) / float64(total)
	}
	pool := len(ws.parkedOrder)
	if ws.cfg.ParkPolicy == schema.ParkFork {
		pool = len(ws.forkBases)
	}
	return WorkingSetStats{
		Hits:                 ws.stepHits,
		Misses:               ws.stepMisses,
		HitRate:              rate,
		StandingPoolSize:     pool,
		Created:              ws.stepCreated,
		Reused:               ws.stepReused,
		Retired:              ws.stepRetired,
		PrefetchLeadMarginMS: prefetchLeadMS,
		ReleaseOverlapMS:     releaseOverlapMS,
	}
}

// SummaryStats returns the cumulative working-set metrics across the entire run.
func (ws *WorkingSet) SummaryStats(prefetchLeadMS, releaseOverlapMS float64) WorkingSetStats {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	total := ws.totalHits + ws.totalMiss
	var rate float64
	if total > 0 {
		rate = float64(ws.totalHits) / float64(total)
	}
	pool := len(ws.parkedOrder)
	if ws.cfg.ParkPolicy == schema.ParkFork {
		pool = len(ws.forkBases)
	}
	return WorkingSetStats{
		Hits:                 ws.totalHits,
		Misses:               ws.totalMiss,
		HitRate:              rate,
		StandingPoolSize:     pool,
		Created:              ws.created,
		Reused:               ws.reused,
		Retired:              ws.retired,
		PrefetchLeadMarginMS: prefetchLeadMS,
		ReleaseOverlapMS:     releaseOverlapMS,
	}
}

func (ws *WorkingSet) removeFromOrderLocked(target *WorkingSetEntry) {
	for i, e := range ws.parkedOrder {
		if e == target {
			ws.parkedOrder = append(ws.parkedOrder[:i], ws.parkedOrder[i+1:]...)
			return
		}
	}
}

func (ws *WorkingSet) removeFromImageQueueLocked(target *WorkingSetEntry) {
	q := ws.parked[target.Image]
	for i, e := range q {
		if e == target {
			ws.parked[target.Image] = append(q[:i], q[i+1:]...)
			if len(ws.parked[target.Image]) == 0 {
				delete(ws.parked, target.Image)
			}
			return
		}
	}
}

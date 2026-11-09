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
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

var nonZeroExitRe = regexp.MustCompile(`(?i)\b(?:exit(?:ed)?(?:\s+with\s+code)?\s+-?\d+|exit\s+-?\d+,\s*expected\s+-?\d+|returncode=-?\d+)`)

type faultBucketKey struct {
	tier     schema.FaultTier
	category string
}

type faultBucketAccum struct {
	dropped    int
	recovered  int
	events     int
	exampleErr string
}

type faultCause struct {
	tier     schema.FaultTier
	category string
	err      string
	event    string
}

type rolloutKey struct {
	run         string
	job         string
	step        int
	instance    string
	task        int
	envFallback string
}

type envStepKey struct {
	run  string
	job  string
	step int
	env  string
}

type rolloutFaultState struct {
	inEpisode         bool
	unrecoveredCauses []faultCause
}

type faultTracker struct {
	// allRollouts counts task events. The contract with every producer
	// (swebench-loader, run_fleet_bench.py, the simulated Runner) is one task
	// event per attempted rollout, also when the create fails: a rollout
	// that never got an environment is in the denominator and is dropped.
	allRollouts       int
	postRolloutErrors int
	buckets           map[faultBucketKey]*faultBucketAccum
	rollouts          map[rolloutKey]*rolloutFaultState
	envToRollout      map[envStepKey]rolloutKey
}

func newFaultTracker() *faultTracker {
	return &faultTracker{
		buckets:      map[faultBucketKey]*faultBucketAccum{},
		rollouts:     map[rolloutKey]*rolloutFaultState{},
		envToRollout: map[envStepKey]rolloutKey{},
	}
}

func (ft *faultTracker) ensureBucket(tier schema.FaultTier, category string) *faultBucketAccum {
	k := faultBucketKey{tier: tier, category: category}
	b, ok := ft.buckets[k]
	if !ok {
		b = &faultBucketAccum{}
		ft.buckets[k] = b
	}
	return b
}

func (ft *faultTracker) recordExample(b *faultBucketAccum, errStr string) {
	clean := cleanExampleErr(errStr)
	if clean == "" {
		return
	}
	if b.exampleErr == "" {
		b.exampleErr = clean
	}
}

func cleanExampleErr(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", " "), "\n", " "))
	if len(s) > 200 {
		return s[:197] + "..."
	}
	return s
}

func exampleErrForEvent(e Event) string {
	if e.Err != "" {
		return e.Err
	}
	if e.Timeout != nil && *e.Timeout {
		if e.Name != "" {
			return fmt.Sprintf("step %s timed out", e.Name)
		}
		return fmt.Sprintf("%s timed out", e.Event)
	}
	if e.Exit != nil && *e.Exit != 0 {
		if e.Name != "" {
			return fmt.Sprintf("step %s exited %d", e.Name, *e.Exit)
		}
		return fmt.Sprintf("%s exited %d", e.Event, *e.Exit)
	}
	if !e.OK {
		if e.Name != "" {
			return fmt.Sprintf("%s/%s failed", e.Event, e.Name)
		}
		return fmt.Sprintf("%s failed", e.Event)
	}
	return ""
}

func normStepKey(step int) int {
	if step <= 0 {
		return 1
	}
	return step
}

func (ft *faultTracker) keyForEvent(e Event) (rolloutKey, bool) {
	st := normStepKey(e.Step)
	if e.Instance != "" || e.Task > 0 {
		rk := rolloutKey{
			run:      e.RunID,
			job:      e.Job,
			step:     st,
			instance: e.Instance,
			task:     e.Task,
		}
		if e.Env != "" && e.Env != EventRun && e.Env != EventBarrier && e.Env != EventPark && e.Env != EventReap {
			ft.envToRollout[envStepKey{run: e.RunID, job: e.Job, step: st, env: e.Env}] = rk
		}
		return rk, true
	}
	if e.Env != "" && e.Env != EventRun && e.Env != EventBarrier && e.Env != EventPark && e.Env != EventReap {
		ek := envStepKey{run: e.RunID, job: e.Job, step: st, env: e.Env}
		if mapped, ok := ft.envToRollout[ek]; ok {
			return mapped, true
		}
		return rolloutKey{
			run:         e.RunID,
			job:         e.Job,
			step:        st,
			envFallback: e.Env,
		}, true
	}
	return rolloutKey{
		run:  e.RunID,
		job:  e.Job,
		step: st,
	}, false
}

func (ft *faultTracker) ensureRollout(rk rolloutKey) *rolloutFaultState {
	rs, ok := ft.rollouts[rk]
	if !ok {
		rs = &rolloutFaultState{}
		ft.rollouts[rk] = rs
	}
	return rs
}

// Add folds one Event into the faultTracker.
func (ft *faultTracker) Add(e Event) {
	switch e.Event {
	case EventRun, EventBarrier, EventPark, EventThink:
		return
	case EventStep:
		if e.Name == "" && e.OK {
			// Trainer step metric summary row, not a rollout step.
			return
		}
	case EventPhase:
		if e.Name == "workerpool_spinup" || e.Name == "workerpool_spindown" {
			return
		}
	}

	// Teardown operations (delete / reap) happen after or outside a rollout and
	// never drop an already-scored episode.
	if e.Event == EventDelete || e.Event == EventReap || (e.Event == EventPhase && e.Name == "delete") {
		if !e.OK {
			ft.postRolloutErrors++
			tier, cat := ClassifyEventFault(e, true)
			b := ft.ensureBucket(tier, cat)
			b.events++
			ft.recordExample(b, exampleErrForEvent(e))
		}
		return
	}

	// Upfront prewarm probes run outside rollout tasks; record raw failure events
	// without counting them as dropped rollouts.
	if e.Event == EventPrewarm {
		if !e.OK {
			tier, cat := ClassifyEventFault(e, false)
			b := ft.ensureBucket(tier, cat)
			b.events++
			ft.recordExample(b, exampleErrForEvent(e))
		}
		return
	}

	if e.Event == EventTask {
		ft.allRollouts++
		rk, hasKey := ft.keyForEvent(e)
		var rs *rolloutFaultState
		if hasKey {
			rs = ft.rollouts[rk]
			delete(ft.rollouts, rk)
		}
		if e.Env != "" {
			delete(ft.envToRollout, envStepKey{run: e.RunID, job: e.Job, step: normStepKey(e.Step), env: e.Env})
		}

		if e.OK {
			// Rollout succeeded: any unrecovered infra failure recorded earlier on
			// this rollout key (e.g. a failed create/wake retried on a fresh sandbox)
			// was recovered by retry and is excluded from dropped rollouts.
			if rs != nil {
				for _, c := range rs.unrecoveredCauses {
					if c.tier != schema.FaultTierPolicy {
						b := ft.ensureBucket(c.tier, c.category)
						b.recovered++
						ft.recordExample(b, c.err)
					}
				}
			}
			return
		}

		// Rollout failed / dropped: attribute at most 1 dropped rollout.
		var primary faultCause
		if rs != nil && len(rs.unrecoveredCauses) > 0 {
			primary = selectPrimaryRolloutCause(rs.unrecoveredCauses)
		} else {
			inEp := rs != nil && rs.inEpisode
			if e.Err == "" && e.Timeout == nil && e.Exit == nil && inEp {
				primary = faultCause{
					tier:     schema.FaultTierPolicy,
					category: schema.FaultCatNonZeroExit,
					err:      "missing reward verdict",
					event:    e.Event,
				}
			} else {
				tier, cat := ClassifyEventFault(e, inEp)
				primary = faultCause{
					tier:     tier,
					category: cat,
					err:      exampleErrForEvent(e),
					event:    e.Event,
				}
			}
			b := ft.ensureBucket(primary.tier, primary.category)
			b.events++
		}
		b := ft.ensureBucket(primary.tier, primary.category)
		b.dropped++
		ft.recordExample(b, primary.err)
		return
	}

	rk, hasKey := ft.keyForEvent(e)
	if !hasKey {
		if e.OK {
			if e.Attempts > 1 {
				switch e.Event {
				case EventTTFI:
					b := ft.ensureBucket(schema.FaultTierBeforeEpisode, schema.FaultCatTransportError)
					b.recovered++
					ft.recordExample(b, fmt.Sprintf("ttfi succeeded after %d attempts", e.Attempts))
				case EventWake:
					b := ft.ensureBucket(schema.FaultTierDuringEpisode, schema.FaultCatWakeResumeFailure)
					b.recovered++
					ft.recordExample(b, fmt.Sprintf("wake succeeded after %d attempts", e.Attempts))
				}
			}
			return
		}
		tier, cat := ClassifyEventFault(e, false)
		errEx := exampleErrForEvent(e)
		b := ft.ensureBucket(tier, cat)
		b.events++
		ft.recordExample(b, errEx)
		return
	}
	rs := ft.ensureRollout(rk)

	if e.OK {
		// If an earlier create/wake attempt on this rollout failed and a retry of
		// that lifecycle operation succeeded (for example, a failed create retried
		// on a fresh sandbox), mark the earlier infra failure(s) recovered so any
		// unrelated failure later in the episode attributes to the new cause.
		if len(rs.unrecoveredCauses) > 0 && isLifecycleRetrySuccess(e, rs.unrecoveredCauses) {
			var remaining []faultCause
			for _, c := range rs.unrecoveredCauses {
				if c.tier != schema.FaultTierPolicy {
					b := ft.ensureBucket(c.tier, c.category)
					b.recovered++
					ft.recordExample(b, c.err)
				} else {
					remaining = append(remaining, c)
				}
			}
			rs.unrecoveredCauses = remaining
		}
		if e.Attempts > 1 {
			switch e.Event {
			case EventTTFI:
				b := ft.ensureBucket(schema.FaultTierBeforeEpisode, schema.FaultCatTransportError)
				b.recovered++
				ft.recordExample(b, fmt.Sprintf("ttfi succeeded after %d attempts", e.Attempts))
			case EventWake:
				b := ft.ensureBucket(schema.FaultTierDuringEpisode, schema.FaultCatWakeResumeFailure)
				b.recovered++
				ft.recordExample(b, fmt.Sprintf("wake succeeded after %d attempts", e.Attempts))
			}
		}
		if marksEpisodeActive(e) {
			rs.inEpisode = true
		}
		return
	}

	// Sub-event failed (!e.OK).
	tier, cat := ClassifyEventFault(e, rs.inEpisode)
	errEx := exampleErrForEvent(e)
	b := ft.ensureBucket(tier, cat)
	b.events++
	ft.recordExample(b, errEx)
	rs.unrecoveredCauses = append(rs.unrecoveredCauses, faultCause{
		tier:     tier,
		category: cat,
		err:      errEx,
		event:    e.Event,
	})
}

func isLifecycleRetrySuccess(e Event, prior []faultCause) bool {
	isCreate := e.Event == EventCreate || (e.Event == EventPhase && e.Name == "create")
	isWake := e.Event == EventWake || (e.Event == EventPhase && (e.Name == "wake" || e.Name == "resume"))
	if !isCreate && !isWake {
		return false
	}
	for _, c := range prior {
		if isCreate && (c.event == EventCreate || c.event == EventPhase) {
			return true
		}
		if isWake && (c.event == EventWake || c.event == EventCreate || c.event == EventPhase) {
			return true
		}
	}
	return false
}

func marksEpisodeActive(e Event) bool {
	switch e.Event {
	case EventTTFI, EventWake, EventStep:
		return true
	case EventPhase:
		switch e.Name {
		case "agent_exec", "exec", "bootstrap", "eval_harness", "grade":
			return true
		}
	}
	return false
}

// midEpisodeEvent reports whether ev is an infrastructure operation inside
// the episode: the rollout already ran a command (inEpisode), or the event is
// a wake, suspend or fork, or the phase of one, which only happen to an
// environment a rollout is using. A create, a first command (ttfi) and a
// prewarm probe are before the episode.
func midEpisodeEvent(ev Event, inEpisode bool) bool {
	if inEpisode {
		return true
	}
	switch ev.Event {
	case EventWake, EventSuspend, EventFork:
		return true
	case EventPhase:
		switch ev.Name {
		case "wake", "resume", "suspend", "snapshot", "fork":
			return true
		}
	}
	return false
}

// selectPrimaryRolloutCause chooses the primary root cause for a failed rollout
// when multiple sub-events recorded errors on the same rollout (for example,
// EventTTFI/EventWake followed immediately by EventStep with the same error).
func selectPrimaryRolloutCause(causes []faultCause) faultCause {
	// Prefer the earliest cause; if the first cause had an empty error string
	// and a paired step event carries the concrete message for the same tier,
	// borrow the non-empty message.
	primary := causes[0]
	if primary.err == "" {
		for _, c := range causes[1:] {
			if c.tier == primary.tier && c.category == primary.category && c.err != "" {
				primary.err = c.err
				break
			}
		}
	}
	return primary
}

// Finish materializes the schema.FaultClassification summary.
func (ft *faultTracker) Finish() *schema.FaultClassification {
	var tier1Dropped, tier2Policy, tier3Dropped, recoveredRetries int
	var categories []schema.FaultCategorySummary

	for k, b := range ft.buckets {
		switch k.tier {
		case schema.FaultTierBeforeEpisode:
			tier1Dropped += b.dropped
		case schema.FaultTierPolicy:
			tier2Policy += b.dropped
		case schema.FaultTierDuringEpisode:
			tier3Dropped += b.dropped
		}
		if k.tier != schema.FaultTierPolicy {
			recoveredRetries += b.recovered
		}
		if b.dropped > 0 || b.recovered > 0 || b.events > 0 {
			categories = append(categories, schema.FaultCategorySummary{
				Tier:       k.tier,
				Category:   k.category,
				Dropped:    b.dropped,
				Recovered:  b.recovered,
				Events:     b.events,
				ExampleErr: b.exampleErr,
			})
		}
	}

	sortFaultCategories(categories)

	infraDropped := tier1Dropped + tier3Dropped
	target := schema.DefaultInfraFaultRateTarget
	var rate float64
	// No rollouts is no evidence of meeting the target: an aborted run must
	// not read as a pass. The renderers print "no rollouts" for it.
	meetsTarget := false
	if ft.allRollouts > 0 {
		exactRate := float64(infraDropped) / float64(ft.allRollouts)
		meetsTarget = exactRate < target
		rate = math.Round(exactRate*1e6) / 1e6
	}

	return &schema.FaultClassification{
		AllRollouts:       ft.allRollouts,
		InfraDropped:      infraDropped,
		Tier1Dropped:      tier1Dropped,
		Tier2Policy:       tier2Policy,
		Tier3Dropped:      tier3Dropped,
		RecoveredRetries:  recoveredRetries,
		PostRolloutErrors: ft.postRolloutErrors,
		InfraFaultRate:    rate,
		TargetRate:        target,
		MeetsTarget:       meetsTarget,
		Categories:        categories,
	}
}

func tierRank(t schema.FaultTier) int {
	switch t {
	case schema.FaultTierBeforeEpisode:
		return 1
	case schema.FaultTierPolicy:
		return 2
	case schema.FaultTierDuringEpisode:
		return 3
	default:
		return 4
	}
}

func categoryRank(cat string) int {
	switch cat {
	case schema.FaultCatCreateTimeout:
		return 1
	case schema.FaultCatNoCapacity:
		return 2
	case schema.FaultCatImagePullUnpack:
		return 3
	case schema.FaultCatTransportError:
		return 4
	case schema.FaultCatNonZeroExit:
		return 5
	case schema.FaultCatCommandTimeout:
		return 6
	case schema.FaultCatGuestOOM:
		return 7
	case schema.FaultCatWakeResumeFailure:
		return 8
	case schema.FaultCatSandboxCrash:
		return 9
	case schema.FaultCatSnapshotRestoreFailure:
		return 10
	case schema.FaultCatEvictionPreemption:
		return 11
	case schema.FaultCatOther:
		return 12
	default:
		return 99
	}
}

func sortFaultCategories(cats []schema.FaultCategorySummary) {
	sort.Slice(cats, func(i, j int) bool {
		ti, tj := tierRank(cats[i].Tier), tierRank(cats[j].Tier)
		if ti != tj {
			return ti < tj
		}
		if cats[i].Dropped != cats[j].Dropped {
			return cats[i].Dropped > cats[j].Dropped
		}
		if cats[i].Recovered != cats[j].Recovered {
			return cats[i].Recovered > cats[j].Recovered
		}
		if cats[i].Events != cats[j].Events {
			return cats[i].Events > cats[j].Events
		}
		ri, rj := categoryRank(cats[i].Category), categoryRank(cats[j].Category)
		if ri != rj {
			return ri < rj
		}
		return cats[i].Category < cats[j].Category
	})
}

// ClassifyEventFault deterministically maps a failed Event (and whether the
// rollout had already entered its episode) onto the 3-tier fault taxonomy:
//   - Tier 1 (tier1_before_episode): create_timeout, no_capacity, image_pull_unpack, transport_error, other
//   - Tier 2 (tier2_policy): nonzero_exit, command_timeout, guest_oom (excluded from infra_fault_rate)
//   - Tier 3 (tier3_during_episode): wake_resume_failure, sandbox_crash, snapshot_restore_failure, eviction_preemption, other
func ClassifyEventFault(ev Event, inEpisode bool) (schema.FaultTier, string) {
	msgLower := strings.ToLower(ev.Err)
	tailLower := strings.ToLower(ev.Tail)
	// The tier 3 matchers (eviction, crash, snapshot, wake) only apply to an
	// operation inside the episode. Before it, the same words describe a
	// create or boot failure and the event takes the tier 1 path below: a
	// create that fails with "template evicted from cache" or "no golden
	// snapshot" is not a mid-episode eviction or restore failure.
	midEpisode := midEpisodeEvent(ev, inEpisode)

	// 1. Image pull, manifest fetch, OCI layer unpack, or golden template bake failures
	// always belong to Tier 1 image_pull_unpack even when surfaced during first restore.
	if isImagePullUnpackError(msgLower) {
		return schema.FaultTierBeforeEpisode, schema.FaultCatImagePullUnpack
	}

	// 2. Node preemption, kubelet eviction, or worker node loss -> Tier 3 eviction_preemption.
	if midEpisode && isEvictionOrPreemptionError(msgLower) {
		return schema.FaultTierDuringEpisode, schema.FaultCatEvictionPreemption
	}

	// 3. Capacity exhaustion / slot-wait timeout -> Tier 1 no_capacity.
	if isNoCapacityError(msgLower) {
		return schema.FaultTierBeforeEpisode, schema.FaultCatNoCapacity
	}

	// 4. Guest command execution outcomes -> Tier 2 policy (excluded from infra_fault_rate)
	// whenever the event is a guest command step/phase and not a control/data-plane RPC failure.
	if isCommandExecutionEvent(ev) && !hasInfraFailureMarker(msgLower) {
		if tier, cat, ok := classifyPolicyOutcome(ev, msgLower, tailLower); ok {
			return tier, cat
		}
	}

	// 5. Sandbox / actor crash -> Tier 3 sandbox_crash.
	if midEpisode && isSandboxCrashError(msgLower) {
		return schema.FaultTierDuringEpisode, schema.FaultCatSandboxCrash
	}

	// 6. Snapshot checkpoint, restore, fork, or GCS snapshot bucket errors -> Tier 3 snapshot_restore_failure.
	if midEpisode && isSnapshotRestoreEventOrError(ev, msgLower) {
		return schema.FaultTierDuringEpisode, schema.FaultCatSnapshotRestoreFailure
	}

	// 7. Wake / ResumeActor / mid-episode infra unreachability vs pre-episode create/boot errors.
	if isWakeOrResumeEventOrError(ev, msgLower, midEpisode) {
		return schema.FaultTierDuringEpisode, schema.FaultCatWakeResumeFailure
	}
	// 8. Nothing matched. The error is still an infrastructure fault of its
	// tier (the policy outcomes returned above), but it gets the "other"
	// category and keeps its example_err, instead of inflating a named
	// category the report would then misread.
	if inEpisode {
		return schema.FaultTierDuringEpisode, schema.FaultCatOther
	}
	if isTimeoutMessage(msgLower) || (ev.Timeout != nil && *ev.Timeout) {
		return schema.FaultTierBeforeEpisode, schema.FaultCatCreateTimeout
	}
	if isTransportError(msgLower) {
		return schema.FaultTierBeforeEpisode, schema.FaultCatTransportError
	}
	return schema.FaultTierBeforeEpisode, schema.FaultCatOther
}

// isTransportError matches a connection the client could not open or keep:
// gRPC Unavailable and the dial, TLS, reset and stream errors under it.
func isTransportError(msgLower string) bool {
	return containsAny(
		msgLower,
		"code = unavailable",
		"connection refused",
		"connection reset",
		"connection error",
		"transport is closing",
		"transport: error",
		"broken pipe",
		"stream terminated",
		"tls handshake",
		"x509:",
		"upstream connect error",
		"dial tcp",
		"no such host",
		"unexpected eof",
		"http2:",
	)
}

func isCommandExecutionEvent(ev Event) bool {
	switch ev.Event {
	case EventStep:
		return ev.Name != ""
	case EventPhase:
		switch ev.Name {
		case "agent_exec", "exec", "bootstrap", "eval_harness", "grade", "run_tests", "test":
			return true
		}
	case EventTTFI, EventWake, EventTask:
		if ev.Timeout != nil && *ev.Timeout {
			return true
		}
		if ev.Exit != nil {
			return true
		}
		msgLower := strings.ToLower(ev.Err)
		return strings.Contains(msgLower, "commandtimeouterror") ||
			strings.Contains(msgLower, "commandexecutionerror") ||
			strings.Contains(msgLower, "commandstarterror") ||
			strings.Contains(msgLower, "timeouterror") ||
			(strings.HasPrefix(msgLower, "step ") && strings.Contains(msgLower, "timed out")) ||
			(strings.HasPrefix(msgLower, "chmod ") && (strings.Contains(msgLower, "timed out") || strings.Contains(msgLower, "exited"))) ||
			nonZeroExitRe.MatchString(msgLower)
	}
	return false
}

func hasInfraFailureMarker(msgLower string) bool {
	for _, marker := range []string{
		"rpc error: code =",
		"infrastructureerror",
		"sandboxunavailableerror",
		"sandboxcrashederror",
		"sandboxnotfounderror",
		"sandboxprotocolerror",
		"waketimeouterror",
		"createerror",
		"snapshoterror",
		"wake budget",
		"running budget",
		"crashed while waking",
		"actor_state_",
		"environment_status_",
		"atelet ",
		"resumeactor",
		"createactor",
		"suspendactor",
		"detachactor",
		"createenvironment",
		"suspendenvironment",
		"forkenvironment",
		"connection refused",
		"connection reset",
		"transport is closing",
		"broken pipe",
		"stream terminated",
		"tls handshake",
		"x509:",
		"upstream connect error",
		"poll proc-",
	} {
		if strings.Contains(msgLower, marker) {
			return true
		}
	}
	return false
}

func classifyPolicyOutcome(ev Event, msgLower, tailLower string) (schema.FaultTier, string, bool) {
	timedOut := (ev.Timeout != nil && *ev.Timeout) ||
		strings.Contains(msgLower, "commandtimeouterror") ||
		strings.Contains(msgLower, "timeouterror") ||
		strings.Contains(msgLower, "command timed out") ||
		strings.Contains(msgLower, "process timed out") ||
		strings.Contains(msgLower, "execution timed out") ||
		strings.Contains(msgLower, "timed_out=true") ||
		strings.Contains(msgLower, "timed out")

	// Check guest OOM first when not explicitly marked as a command timeout
	// (because guest timeout also sends SIGKILL -> exit 137 at deadline).
	if !timedOut {
		if (ev.Exit != nil && *ev.Exit == 137) ||
			containsAny(msgLower, "out of memory", "oom-kill", "oom_kill", "oom killed", "cannot allocate memory", "memoryerror", "exit 137,", "exited 137", "exited with code 137", "returncode=137") ||
			containsAny(tailLower, "out of memory", "oom-kill", "oom_kill", "cannot allocate memory", "memoryerror") {
			return schema.FaultTierPolicy, schema.FaultCatGuestOOM, true
		}
	}

	if timedOut {
		return schema.FaultTierPolicy, schema.FaultCatCommandTimeout, true
	}

	if (ev.Exit != nil && *ev.Exit != 0) ||
		containsAny(msgLower, "commandexecutionerror", "commandstarterror", "non-zero exit", "nonzero exit", "executable file not found") ||
		nonZeroExitRe.MatchString(msgLower) {
		return schema.FaultTierPolicy, schema.FaultCatNonZeroExit, true
	}

	if ev.Event == EventPhase && (ev.Name == "eval_harness" || ev.Name == "grade" || ev.Name == "bootstrap") && !ev.OK && msgLower == "" {
		return schema.FaultTierPolicy, schema.FaultCatNonZeroExit, true
	}

	return "", "", false
}

func isImagePullUnpackError(msgLower string) bool {
	return containsAny(
		msgLower,
		"imagecache.ensureimage",
		"unpacking layer",
		"oci_unpack",
		"manifest_fetch",
		"errimagepull",
		"imagepullbackoff",
		"pull image",
		"pulling image",
		"image pull",
		`while creating "guest" oci bundle`,
		"unhandled tar entry",
		"tar stream",
		"golden_snapshot_state_failed",
		"ensure image template",
		"derive template",
		"manifest unknown",
		"image not found",
	)
}

// isEvictionOrPreemptionError matches the words of a node or pod loss. The
// anchors are whole words: "preemptible" in a node pool name and "evict" in a
// cache message are not a preemption or an eviction.
func isEvictionOrPreemptionError(msgLower string) bool {
	return containsAny(
		msgLower,
		"preempted",
		"preemption",
		"evicted",
		"eviction",
		"diskpressure",
		"memorypressure",
		"pidpressure",
		"nodenotready",
		"node not ready",
		"node disappeared",
		"worker disappeared",
		"terminated by kubelet",
		"spot termination",
		"node shutdown",
	)
}

func isNoCapacityError(msgLower string) bool {
	if containsAny(
		msgLower,
		"no worker has room",
		"no worker has had room",
		"no-capacity",
		"no capacity",
		"start_failure_no_capacity",
		"slot_wait",
		"slot wait",
		"waiting for concurrency slot",
		"acquire slot",
		"insufficient cpu",
		"insufficient memory",
		"insufficient ephemeral-storage",
		"unschedulable",
		"nodes are available",
		"capacity exceeded",
	) {
		return true
	}
	if strings.Contains(msgLower, "resourceexhausted") &&
		!containsAny(msgLower, "error 429", "ratelimitexceeded", "snapshot", "suspend", "restore", "delete") {
		return true
	}
	return false
}

func isSandboxCrashError(msgLower string) bool {
	return containsAny(
		msgLower,
		"actor_state_crashed",
		"environment_status_crashed",
		"start_failure_crashed",
		"sandboxcrashederror",
		"sandbox crashed",
		"actor crashed",
		"container crashed",
		"guest crashed",
	) || (strings.Contains(msgLower, "crashed while waking") && !strings.Contains(msgLower, "atelet restore"))
}

func isSnapshotRestoreEventOrError(ev Event, msgLower string) bool {
	if ev.Event == EventSuspend || ev.Event == EventFork {
		return true
	}
	if ev.Event == EventPhase && (ev.Name == "snapshot" || ev.Name == "suspend" || ev.Name == "fork" || ev.Name == "resume") {
		return true
	}
	// "snapshot" alone also names the golden snapshot of a template; only the
	// phrases of a checkpoint, restore or bucket failure count.
	return containsAny(
		msgLower,
		"atelet restore",
		"restore failed",
		"worker_restore",
		"worker_checkpoint",
		"snapshoterror",
		"snapshot failed",
		"snapshot error",
		"snapshot restore",
		"restore snapshot",
		"restoring snapshot",
		"snapshot not found",
		"snapshot upload",
		"snapshot download",
		"snapshot bucket",
		"snapshots bucket",
		"suspendenvironment",
		"suspendlocal",
		"suspendactor",
		"forkenvironment",
		"detachactor",
		"error 429",
		"ratelimitexceeded",
	)
}

// isWakeOrResumeEventOrError matches a wake event by kind, and the words of a
// resume failure on an operation inside the episode (midEpisode). Before the
// episode the same words describe the boot of a fresh environment, which the
// caller classifies as a tier 1 create or boot failure.
func isWakeOrResumeEventOrError(ev Event, msgLower string, midEpisode bool) bool {
	if ev.Event == EventWake || (ev.Event == EventPhase && ev.Name == "wake") {
		return true
	}
	if !midEpisode {
		return false
	}
	return containsAny(msgLower,
		"while resuming", "actor_state_resuming", "resumeactor", "waketimeouterror",
		"wake budget", "running budget", "ensureawake")
}

func isTimeoutMessage(msgLower string) bool {
	return containsAny(
		msgLower,
		"deadlineexceeded",
		"context deadline exceeded",
		"timed out",
		"timeout",
		"wake budget",
		"running budget",
	)
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// FaultTierLabel returns a human-readable label for tier in text and Markdown tables.
func FaultTierLabel(tier schema.FaultTier) string {
	switch tier {
	case schema.FaultTierBeforeEpisode:
		return "Tier 1 (before episode, infra)"
	case schema.FaultTierPolicy:
		return "Tier 2 (during episode, policy)"
	case schema.FaultTierDuringEpisode:
		return "Tier 3 (during episode, infra)"
	default:
		return string(tier)
	}
}

// faultNotClassified is what the renderers print for a report without a
// FaultClassification: one loaded from a report.json written before the field
// existed. The events it would need are gone, and nothing is estimated.
const faultNotClassified = "not classified (events predate fault classification)"

// faultNoRollouts is what the renderers print for a classification with no
// task events: there is no rate and no verdict to show.
const faultNoRollouts = "no rollouts"

// FaultTargetStatus returns "PASS" when fc meets its target rate, "FAIL" when
// it does not, "no rollouts" when nothing ran, and "not classified" for a nil fc.
func FaultTargetStatus(fc *schema.FaultClassification) string {
	switch {
	case fc == nil:
		return "not classified"
	case fc.AllRollouts == 0:
		return faultNoRollouts
	case fc.MeetsTarget:
		return "PASS"
	}
	return "FAIL"
}

// InfraFaultCell formats a compact per-arm infra_fault_rate cell for comparison tables.
func InfraFaultCell(r *Report) string {
	if r == nil || r.FaultClassification == nil {
		return "not classified"
	}
	fc := r.FaultClassification
	if fc.AllRollouts == 0 {
		return faultNoRollouts
	}
	return fmt.Sprintf("%.2f%% (%s, %d/%d)", fc.InfraFaultRate*100.0, FaultTargetStatus(fc), fc.InfraDropped, fc.AllRollouts)
}

// infraFaultHeader names the arm table's infra fault column after the target
// rate the reports share, "Infra Fault Rate (<2%)". Without a classification,
// or with different targets, the column carries no number.
func infraFaultHeader(reps []*Report) string {
	target := -1.0
	for _, r := range reps {
		if r == nil || r.FaultClassification == nil {
			continue
		}
		t := r.FaultClassification.TargetRate
		if target >= 0 && t != target {
			return "Infra Fault Rate"
		}
		target = t
	}
	if target < 0 {
		return "Infra Fault Rate"
	}
	return fmt.Sprintf("Infra Fault Rate (<%s%%)", strconv.FormatFloat(target*100, 'f', -1, 64))
}

func writeFaultClassificationHeaderText(w io.Writer, fc *schema.FaultClassification) {
	if fc == nil {
		fmt.Fprintf(w, "faults: %s\n", faultNotClassified)
		return
	}
	if fc.AllRollouts == 0 {
		fmt.Fprintf(w, "faults: %s\n", faultNoRollouts)
		return
	}
	fmt.Fprintf(w, "faults: infra_fault_rate=%.2f%% (target <%.1f%%: %s) infra_dropped=%d/%d (tier1=%d, tier3=%d) policy_tier2=%d recovered_retries=%d",
		fc.InfraFaultRate*100.0, fc.TargetRate*100.0, FaultTargetStatus(fc),
		fc.InfraDropped, fc.AllRollouts, fc.Tier1Dropped, fc.Tier3Dropped, fc.Tier2Policy, fc.RecoveredRetries)
	if fc.PostRolloutErrors > 0 {
		fmt.Fprintf(w, " post_rollout_errors=%d", fc.PostRolloutErrors)
	}
	fmt.Fprintln(w)
}

func writeFaultClassificationTableText(w io.Writer, fc *schema.FaultClassification) {
	if fc == nil || len(fc.Categories) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "fault classification:")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "tier\tcategory\tdropped\trecovered\tevents\texample_err")
	for _, c := range fc.Categories {
		ex := c.ExampleErr
		if len(ex) > 120 {
			ex = ex[:117] + "..."
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%s\n", c.Tier, c.Category, c.Dropped, c.Recovered, c.Events, ex)
	}
	tw.Flush()
}

func escapeMarkdownCell(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	s = strings.ReplaceAll(s, "`", "'")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}

func writeFaultClassificationMarkdown(w io.Writer, fc *schema.FaultClassification) {
	if fc == nil {
		return
	}
	fmt.Fprintln(w, "\n## Error & Fault Classification (3 tiers)")
	if fc.AllRollouts == 0 {
		fmt.Fprintf(w, "- **Infra Fault Rate**: %s\n", faultNoRollouts)
	} else {
		fmt.Fprintf(w, "- **Infra Fault Rate (`(tier1 + tier3 dropped) / all_rollouts`)**: **%.2f%%** (%d / %d rollouts) vs `< %.1f%%` target -> **%s**\n",
			fc.InfraFaultRate*100.0, fc.InfraDropped, fc.AllRollouts, fc.TargetRate*100.0, FaultTargetStatus(fc))
	}
	fmt.Fprintf(w, "- **Rollout Breakdown**: Tier 1 before-episode dropped=`%d`, Tier 3 during-episode dropped=`%d`, Tier 2 policy=`%d` (excluded from fault rate), recovered by retry=`%d`",
		fc.Tier1Dropped, fc.Tier3Dropped, fc.Tier2Policy, fc.RecoveredRetries)
	if fc.PostRolloutErrors > 0 {
		fmt.Fprintf(w, ", post-rollout teardown errors=`%d`", fc.PostRolloutErrors)
	}
	fmt.Fprintln(w)
	if len(fc.Categories) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| Tier | Category | Dropped | Recovered | Events | Example Error |")
	fmt.Fprintln(w, "|---|---|---:|---:|---:|---|")
	for _, c := range fc.Categories {
		ex := "-"
		if c.ExampleErr != "" {
			ex = "`" + escapeMarkdownCell(c.ExampleErr) + "`"
		}
		fmt.Fprintf(w, "| %s | `%s` | %d | %d | %d | %s |\n",
			FaultTierLabel(c.Tier), c.Category, c.Dropped, c.Recovered, c.Events, ex)
	}
}

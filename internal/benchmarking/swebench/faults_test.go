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
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

func TestClassifyEventFaultAllThreeTiers(t *testing.T) {
	trueVal := true
	exit1 := 1
	exit137 := 137

	cases := []struct {
		name      string
		ev        Event
		inEpisode bool
		wantTier  schema.FaultTier
		wantCat   string
	}{
		{
			name:     "tier1 create timeout",
			ev:       Event{Event: EventCreate, OK: false, Err: "rpc error: code = DeadlineExceeded desc = context deadline exceeded"},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatCreateTimeout,
		},
		{
			name:     "tier1 no capacity worker room",
			ev:       Event{Event: EventCreate, OK: false, Err: "CreateError: no worker has had room for actor env-1"},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatNoCapacity,
		},
		{
			name:     "tier1 slot wait timeout",
			ev:       Event{Event: EventCreate, OK: false, Err: "slot_wait timed out waiting for concurrency slot"},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatNoCapacity,
		},
		{
			name: "tier1 image pull unpack inside atelet restore on boot",
			ev: Event{
				Event: EventTTFI,
				OK:    false,
				Err:   `step baseline: wake 36k-2s-a-628: actor 36k-2s-a-628 crashed while waking: resume failed: atelet Restore: while creating "guest" OCI bundle: in imageCache.EnsureImage(img): while unpacking layer sha256:abc: unhandled tar entry`,
			},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatImagePullUnpack,
		},
		{
			name:     "tier1 transport error before episode",
			ev:       Event{Event: EventCreate, OK: false, Err: `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: connection refused"`},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatTransportError,
		},
		{
			name:      "tier2 policy non-zero exit",
			ev:        Event{Event: EventStep, Name: "apply_patch", OK: false, Exit: &exit1, Err: "exit 1, expected 0"},
			inEpisode: true,
			wantTier:  schema.FaultTierPolicy,
			wantCat:   schema.FaultCatNonZeroExit,
		},
		{
			name:      "tier2 policy command timeout via Timeout flag with exit 137",
			ev:        Event{Event: EventStep, Name: "run_tests", OK: false, Exit: &exit137, Timeout: &trueVal, Err: "exit 137, expected 0"},
			inEpisode: true,
			wantTier:  schema.FaultTierPolicy,
			wantCat:   schema.FaultCatCommandTimeout,
		},
		{
			name:      "tier2 policy command timeout via Python TimeoutError string",
			ev:        Event{Event: EventStep, Name: "run_tests", OK: false, Err: "step 'run_tests' timed out after 1200s"},
			inEpisode: true,
			wantTier:  schema.FaultTierPolicy,
			wantCat:   schema.FaultCatCommandTimeout,
		},
		{
			name:      "tier2 policy guest OOM exit 137 without timeout",
			ev:        Event{Event: EventStep, Name: "explore", OK: false, Exit: &exit137, Err: "exit 137, expected 0"},
			inEpisode: true,
			wantTier:  schema.FaultTierPolicy,
			wantCat:   schema.FaultCatGuestOOM,
		},
		{
			name:      "tier2 policy guest OOM via Tail MemoryError",
			ev:        Event{Event: EventStep, Name: "run_tests", OK: false, Exit: &exit1, Err: "exit 1, expected 0", Tail: "Traceback:\nMemoryError: Unable to allocate 8.00 GiB"},
			inEpisode: true,
			wantTier:  schema.FaultTierPolicy,
			wantCat:   schema.FaultCatGuestOOM,
		},
		{
			name:      "tier3 wake resume failure during episode",
			ev:        Event{Event: EventWake, OK: false, Err: "rpc error: code = DeadlineExceeded desc = environment dens2-v1-a-12 did not answer within the wake budget: actor is ACTOR_STATE_RESUMING"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatWakeResumeFailure,
		},
		{
			name:      "tier3 mid-episode step RPC DeadlineExceeded stays infra not policy",
			ev:        Event{Event: EventStep, Name: "apply_patch", OK: false, Err: "rpc error: code = DeadlineExceeded desc = environment env-9 did not answer within the wake budget"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatWakeResumeFailure,
		},
		{
			name:      "tier3 sandbox crash ACTOR_STATE_CRASHED",
			ev:        Event{Event: EventStep, Name: "explore", OK: false, Err: "rpc error: code = FailedPrecondition desc = actor env-4 is in ACTOR_STATE_CRASHED"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatSandboxCrash,
		},
		{
			name:      "tier3 snapshot restore failure on GCS 429",
			ev:        Event{Event: EventSuspend, OK: false, Err: "SuspendEnvironment: googleapi: Error 429: rateLimitExceeded"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatSnapshotRestoreFailure,
		},
		{
			name:      "tier3 eviction or node preemption",
			ev:        Event{Event: EventStep, Name: "run_tests", OK: false, Err: "sandbox pod evicted due to node preemption"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatEvictionPreemption,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTier, gotCat := ClassifyEventFault(tc.ev, tc.inEpisode)
			if gotTier != tc.wantTier || gotCat != tc.wantCat {
				t.Fatalf("ClassifyEventFault(%+v, inEpisode=%v) = (%q, %q), want (%q, %q)",
					tc.ev, tc.inEpisode, gotTier, gotCat, tc.wantTier, tc.wantCat)
			}
		})
	}
}

// The tier 3 categories describe an operation inside the episode. The same
// words on a create, a first command or a node name must not produce them.
func TestClassifyEventFaultTier3MatchersNeedTheEpisode(t *testing.T) {
	cases := []struct {
		name      string
		ev        Event
		inEpisode bool
		wantTier  schema.FaultTier
		wantCat   string // exact category, or "" for any category
		notCat    string // category that must not come out
	}{
		{
			name:     "create with no golden snapshot is not a snapshot restore failure",
			ev:       Event{Event: EventCreate, OK: false, Err: "CreateEnvironment: template t has no golden snapshot"},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatOther,
			notCat:   schema.FaultCatSnapshotRestoreFailure,
		},
		{
			name:     "create with a template evicted from cache is not an eviction",
			ev:       Event{Event: EventCreate, OK: false, Err: "CreateEnvironment: template t evicted from cache"},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatOther,
			notCat:   schema.FaultCatEvictionPreemption,
		},
		{
			name:      "unknown error in the episode is tier 3 other, not a wake failure",
			ev:        Event{Event: EventStep, Name: "explore", OK: false, Err: "write /tmp/x: no space left on device"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatOther,
		},
		{
			name:     "unknown error before the episode is tier 1 other, not a transport error",
			ev:       Event{Event: EventCreate, OK: false, Err: "CreateEnvironment: write /var/lib/ate/x: no space left on device"},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatOther,
		},
		{
			name:     "connection refused before the episode stays a transport error",
			ev:       Event{Event: EventCreate, OK: false, Err: `rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: connection refused"`},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatTransportError,
		},
		{
			name:     "wake on a preemptible node pool is a wake failure, not a preemption",
			ev:       Event{Event: EventWake, OK: false, Err: "rpc error: code = Unavailable desc = node gke-preemptible-pool-1 unreachable"},
			wantTier: schema.FaultTierDuringEpisode,
			wantCat:  schema.FaultCatWakeResumeFailure,
		},
		{
			name:     "first command that crashes while booting is a tier 1 boot failure",
			ev:       Event{Event: EventTTFI, OK: false, Err: "step baseline: wake env-1: actor env-1 crashed while waking: resume failed: exit status 1"},
			wantTier: schema.FaultTierBeforeEpisode,
			notCat:   schema.FaultCatSandboxCrash,
		},
		{
			name:     "first command past the wake budget is a tier 1 create timeout",
			ev:       Event{Event: EventTTFI, OK: false, Err: "rpc error: code = DeadlineExceeded desc = environment env-1 did not answer within the wake budget: actor is ACTOR_STATE_RESUMING"},
			wantTier: schema.FaultTierBeforeEpisode,
			wantCat:  schema.FaultCatCreateTimeout,
		},
		{
			name:      "step in the episode with a snapshot restore phrase is tier 3",
			ev:        Event{Event: EventStep, Name: "explore", OK: false, Err: "rpc error: code = Internal desc = snapshot restore failed for env-1"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatSnapshotRestoreFailure,
		},
		{
			name:      "step in the episode on a preempted node is tier 3",
			ev:        Event{Event: EventStep, Name: "explore", OK: false, Err: "rpc error: code = Unavailable desc = node gke-pool-1 preempted"},
			inEpisode: true,
			wantTier:  schema.FaultTierDuringEpisode,
			wantCat:   schema.FaultCatEvictionPreemption,
		},
		{
			name:     "suspend is inside the episode by kind",
			ev:       Event{Event: EventSuspend, OK: false, Err: "SuspendEnvironment: googleapi: Error 429: rateLimitExceeded"},
			wantTier: schema.FaultTierDuringEpisode,
			wantCat:  schema.FaultCatSnapshotRestoreFailure,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotTier, gotCat := ClassifyEventFault(tc.ev, tc.inEpisode)
			if gotTier != tc.wantTier {
				t.Fatalf("tier = %q (category %q), want %q", gotTier, gotCat, tc.wantTier)
			}
			if tc.wantCat != "" && gotCat != tc.wantCat {
				t.Fatalf("category = %q, want %q", gotCat, tc.wantCat)
			}
			if tc.notCat != "" && gotCat == tc.notCat {
				t.Fatalf("category = %q, which this event must not produce", gotCat)
			}
		})
	}
}

func TestRolloutFaultDeduplicationRetriesPolicyExclusionAndTarget(t *testing.T) {
	exit1 := 1
	exit137 := 137
	reward0 := 0
	reward1 := 1

	var evs []Event
	// 1. 96 clean rollouts (90 rewarded=1, 6 graded unresolved reward=0 with ok=true; 2 with recovered ttfi/wake retries).
	for i := 0; i < 96; i++ {
		inst := "inst-ok"
		envName := "env-ok"
		attempts := 1
		if i == 0 || i == 1 {
			attempts = 2 // recovered transient retry on ttfi
		}
		rew := &reward1
		if i >= 90 {
			rew = &reward0 // harness test verdict reward=0 (ok=true), NOT dropped or infra fault
		}
		evs = append(evs,
			Event{RunID: "faults", Job: "r2e", Instance: inst, Task: i, Env: envName, Step: 1, Event: EventCreate, DurMS: 50, OK: true},
			Event{RunID: "faults", Job: "r2e", Instance: inst, Task: i, Env: envName, Step: 1, Event: EventTTFI, DurMS: 180, OK: true, Attempts: attempts},
			Event{RunID: "faults", Job: "r2e", Instance: inst, Task: i, Env: envName, Step: 1, Event: EventStep, Name: "grade", DurMS: 400, OK: true, Reward: rew},
			Event{RunID: "faults", Job: "r2e", Instance: inst, Task: i, Env: envName, Step: 1, Event: EventTask, Name: "r2e-gold", DurMS: 600, OK: true, Reward: rew},
		)
		// Post-rollout delete error on task 0 must NOT drop the already-completed rollout.
		if i == 0 {
			evs = append(evs, Event{
				RunID: "faults", Job: "r2e", Instance: inst, Task: i, Env: envName, Step: 1,
				Event: EventDelete, DurMS: 120, OK: false, Err: "DeleteActor: googleapi: Error 429: rateLimitExceeded",
			})
		}
	}

	// 2. Rollout 96: failed create retried on a fresh sandbox that ultimately succeeded -> recovered_retries (+1), NOT dropped!
	evs = append(evs,
		Event{RunID: "faults", Job: "swe", Instance: "inst-retry", Task: 96, Env: "env-96-a", Step: 1, Event: EventCreate, DurMS: 300, OK: false, Err: "no worker has room"},
		Event{RunID: "faults", Job: "swe", Instance: "inst-retry", Task: 96, Env: "env-96-b", Step: 1, Event: EventCreate, DurMS: 60, OK: true},
		Event{RunID: "faults", Job: "swe", Instance: "inst-retry", Task: 96, Env: "env-96-b", Step: 1, Event: EventTTFI, DurMS: 190, OK: true, Attempts: 1},
		Event{RunID: "faults", Job: "swe", Instance: "inst-retry", Task: 96, Env: "env-96-b", Step: 1, Event: EventTask, Name: "swebench-gold", DurMS: 700, OK: true, Reward: &reward1},
	)

	// 3. Rollout 97: Tier 2 policy failure (guest OOM exit 137) -> tier2_policy=1, excluded from infra_fault_rate!
	evs = append(evs,
		Event{RunID: "faults", Job: "r2e", Instance: "inst-oom", Task: 97, Env: "env-97", Step: 1, Event: EventCreate, DurMS: 50, OK: true},
		Event{RunID: "faults", Job: "r2e", Instance: "inst-oom", Task: 97, Env: "env-97", Step: 1, Event: EventTTFI, DurMS: 150, OK: true},
		Event{RunID: "faults", Job: "r2e", Instance: "inst-oom", Task: 97, Env: "env-97", Step: 1, Event: EventStep, Name: "baseline", DurMS: 100, OK: true},
		Event{RunID: "faults", Job: "r2e", Instance: "inst-oom", Task: 97, Env: "env-97", Step: 1, Event: EventStep, Name: "explore", DurMS: 250, OK: false, Exit: &exit137, Err: "exit 137, expected 0"},
		Event{RunID: "faults", Job: "r2e", Instance: "inst-oom", Task: 97, Env: "env-97", Step: 1, Event: EventTask, Name: "r2e-gold", DurMS: 400, OK: false},
	)

	// 4. Rollout 98: Tier 2 policy failure (non-zero exit) -> tier2_policy=2, excluded from infra_fault_rate!
	evs = append(evs,
		Event{RunID: "faults", Job: "r2e", Instance: "inst-nz", Task: 98, Env: "env-98", Step: 1, Event: EventCreate, DurMS: 50, OK: true},
		Event{RunID: "faults", Job: "r2e", Instance: "inst-nz", Task: 98, Env: "env-98", Step: 1, Event: EventTTFI, DurMS: 150, OK: true},
		Event{RunID: "faults", Job: "r2e", Instance: "inst-nz", Task: 98, Env: "env-98", Step: 1, Event: EventStep, Name: "apply_patch", DurMS: 80, OK: false, Exit: &exit1, Err: "exit 1, expected 0"},
		Event{RunID: "faults", Job: "r2e", Instance: "inst-nz", Task: 98, Env: "env-98", Step: 1, Event: EventTask, Name: "r2e-gold", DurMS: 230, OK: false},
	)

	// 5. Rollout 99: Tier 3 wake/resume failure where swebench-loader records BOTH EventWake(OK:false)
	// AND EventStep("baseline", OK:false) with the same error before EventTask(OK:false, Err:"").
	// Must count as 1 dropped rollout (1/100 = 1.00% < 2.0% -> PASS), NOT 2!
	wakeErr := "rpc error: code = DeadlineExceeded desc = environment env-99 did not answer within the wake budget: actor is ACTOR_STATE_RESUMING"
	evs = append(evs,
		Event{RunID: "faults", Job: "swe", Instance: "inst-wake-fail", Task: 99, Env: "env-99", Step: 1, Event: EventWake, DurMS: 180000, OK: false, Attempts: 4, Err: wakeErr},
		Event{RunID: "faults", Job: "swe", Instance: "inst-wake-fail", Task: 99, Env: "env-99", Step: 1, Event: EventStep, Name: "baseline", DurMS: 180000, OK: false, Err: wakeErr},
		Event{RunID: "faults", Job: "swe", Instance: "inst-wake-fail", Task: 99, Env: "env-99", Step: 1, Event: EventTask, Name: "swebench-gold", DurMS: 180000, OK: false},
		Event{RunID: "faults", Event: EventRun, Name: "steps", DurMS: 200000, OK: false, Count: 100},
	)

	rep := Build(evs)
	fc := rep.FaultClassification
	if fc == nil {
		t.Fatal("expected non-nil FaultClassification")
	}
	if fc.AllRollouts != 100 {
		t.Fatalf("AllRollouts = %d, want 100", fc.AllRollouts)
	}
	if fc.Tier1Dropped != 0 || fc.Tier3Dropped != 1 || fc.InfraDropped != 1 {
		t.Fatalf("unexpected infra dropped counts: tier1=%d tier3=%d infra=%d (want 0, 1, 1)", fc.Tier1Dropped, fc.Tier3Dropped, fc.InfraDropped)
	}
	if fc.Tier2Policy != 2 {
		t.Fatalf("Tier2Policy = %d, want 2", fc.Tier2Policy)
	}
	if fc.Tier1Dropped+fc.Tier2Policy+fc.Tier3Dropped != rep.Failed {
		t.Fatalf("sum of tier drops (%d) != rep.Failed (%d)", fc.Tier1Dropped+fc.Tier2Policy+fc.Tier3Dropped, rep.Failed)
	}
	// 2 recovered ttfi retries + 1 recovered create retry = 3 recovered retries (failed wake's 4 attempts did NOT recover).
	if fc.RecoveredRetries != 3 {
		t.Fatalf("RecoveredRetries = %d, want 3", fc.RecoveredRetries)
	}
	if fc.PostRolloutErrors != 1 {
		t.Fatalf("PostRolloutErrors = %d, want 1", fc.PostRolloutErrors)
	}
	if math.Abs(fc.InfraFaultRate-0.01) > 1e-6 || !fc.MeetsTarget {
		t.Fatalf("InfraFaultRate = %v, MeetsTarget = %v; want 0.01 and true (< 0.02)", fc.InfraFaultRate, fc.MeetsTarget)
	}

	// Per-job fault classification: r2e has 0% infra fault rate (2 policy faults), swe has 1/2 = 50% infra fault rate.
	if r2eFC := rep.Jobs["r2e"].FaultClassification; r2eFC == nil || r2eFC.InfraDropped != 0 || r2eFC.Tier2Policy != 2 || !r2eFC.MeetsTarget {
		t.Fatalf("unexpected r2e FaultClassification: %+v", r2eFC)
	}
	if sweFC := rep.Jobs["swe"].FaultClassification; sweFC == nil || sweFC.InfraDropped != 1 || sweFC.RecoveredRetries != 1 || sweFC.MeetsTarget {
		t.Fatalf("unexpected swe FaultClassification: %+v", sweFC)
	}

	// Render Text, Markdown, and stats.jsonl and verify schema + content.
	var txt, md bytes.Buffer
	rep.WriteText(&txt)
	rep.WriteMarkdown(&md)
	if !strings.Contains(txt.String(), "infra_fault_rate=1.00% (target <2.0%: PASS)") || !strings.Contains(txt.String(), "wake_resume_failure") {
		t.Fatalf("WriteText missing fault classification:\n%s", txt.String())
	}
	if !strings.Contains(md.String(), "## Error & Fault Classification (3 tiers)") || !strings.Contains(md.String(), "`guest_oom`") {
		t.Fatalf("WriteMarkdown missing fault classification table:\n%s", md.String())
	}

	statsPath := filepath.Join(t.TempDir(), "stats.jsonl")
	if err := WriteStatsJSONL(statsPath, rep, "faults", "swebench-steps", "test"); err != nil {
		t.Fatalf("WriteStatsJSONL: %v", err)
	}
	rawStats, err := os.ReadFile(statsPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateJSONLAgainstJSONSchema("stats_row.schema.json", bytes.NewReader(rawStats)); err != nil {
		t.Fatalf("stats_row schema validation failed: %v", err)
	}
}

func TestTwoPercentTargetBoundaryAndCompareThreshold(t *testing.T) {
	// Exactly 2 dropped out of 100 rollouts = 2.00% -> must FAIL strict "< 2%" target!
	var evs []Event
	for i := 0; i < 100; i++ {
		ok := i >= 2
		errStr := ""
		if !ok {
			errStr = "CreateError: no worker has room"
		}
		evs = append(evs,
			Event{RunID: "cand-2pct", Instance: "inst", Task: i, Env: "e", Event: EventCreate, OK: ok, Err: errStr},
			Event{RunID: "cand-2pct", Instance: "inst", Task: i, Env: "e", Event: EventTask, OK: ok, Err: errStr},
		)
	}
	cand := Build(evs)
	if cand.FaultClassification == nil || cand.FaultClassification.MeetsTarget {
		t.Fatalf("expected 2/100 (2.00%%) to fail strict <2%% target, got %+v", cand.FaultClassification)
	}

	base := Build([]Event{
		{RunID: "base-0pct", Instance: "inst", Task: 0, Env: "e", Event: EventCreate, OK: true},
		{RunID: "base-0pct", Instance: "inst", Task: 0, Env: "e", Event: EventTask, OK: true},
	})
	maxRate := 0.02
	res := Compare(base, cand, Thresholds{MaxInfraFaultRate: &maxRate})
	if res.Passed || len(res.Regressions) == 0 || !strings.Contains(res.Regressions[0], "infra_fault_rate") {
		t.Fatalf("expected Compare regression on MaxInfraFaultRate=0.02 at 2.00%%, got %+v", res)
	}

	// A candidate without a classification has no rate to check. With the
	// threshold set, that is a failure, not a pass at an assumed 0%.
	unclassified := &Report{RunID: "legacy-cand", Tasks: 10, Failed: 5}
	res = Compare(base, unclassified, Thresholds{MaxInfraFaultRate: &maxRate})
	if res.Passed || len(res.Regressions) != 1 || !strings.Contains(res.Regressions[0], "no fault_classification") {
		t.Fatalf("expected Compare to fail a candidate without fault_classification, got %+v", res)
	}
	// Without the threshold the same pair compares and passes: the diff row
	// is informational.
	res = Compare(base, unclassified, Thresholds{})
	if !res.Passed {
		t.Fatalf("expected Compare without MaxInfraFaultRate to pass, got %+v", res)
	}
}

// A report.json written before fault_classification existed has only error
// counts, not events. Loading it must not invent a classification from those
// counts: the field stays nil and every renderer says so.
func TestLegacyReportWithoutEventsIsNotClassified(t *testing.T) {
	legacy := &Report{
		RunID:  "legacy",
		Tasks:  200,
		Failed: 3,
		Stats:  []Stat{{Event: EventTTFI, Count: 200, Failed: 1, Retried: 2}},
		Errors: []ErrorCount{
			{Err: "exit 1, expected 0", Count: 2},
			{Err: "rpc error: code = DeadlineExceeded desc = environment env-1 did not answer within the wake budget", Count: 4},
		},
	}
	NormalizeLegacyReport(legacy)
	if legacy.FaultClassification != nil {
		t.Fatalf("legacy report must stay unclassified, got %+v", legacy.FaultClassification)
	}

	var txt, md, cmp bytes.Buffer
	legacy.WriteText(&txt)
	legacy.WriteMarkdown(&md)
	WriteComparison(&cmp, []*Report{legacy}, []string{"legacy"})
	if !strings.Contains(txt.String(), "faults: not classified (events predate fault classification)") {
		t.Fatalf("WriteText must say the legacy report is not classified:\n%s", txt.String())
	}
	if !strings.Contains(md.String(), "- **Infra Fault Rate**: not classified (events predate fault classification)") ||
		strings.Contains(md.String(), "## Error & Fault Classification") {
		t.Fatalf("WriteMarkdown must say not classified and omit the table:\n%s", md.String())
	}
	if !strings.Contains(cmp.String(), "not classified") || strings.Contains(cmp.String(), "PASS") {
		t.Fatalf("WriteComparison must not show a pass for an unclassified report:\n%s", cmp.String())
	}
	if got := InfraFaultCell(legacy); got != "not classified" {
		t.Fatalf("InfraFaultCell(legacy) = %q, want %q", got, "not classified")
	}
	if got := FaultTargetStatus(nil); got != "not classified" {
		t.Fatalf("FaultTargetStatus(nil) = %q, want %q", got, "not classified")
	}
	var arms bytes.Buffer
	WriteComparisonMarkdown(&arms, []*Report{legacy}, []string{"legacy"})
	if !strings.Contains(arms.String(), "| Infra Fault Rate |") || strings.Contains(arms.String(), "(<") {
		t.Fatalf("arm table header must carry no target without a classification:\n%s", arms.String())
	}
}

// A run with no task events has no rate and no verdict. It must render as
// "no rollouts", not as a pass, and meets_target must be false.
func TestNoRolloutsIsNotAPass(t *testing.T) {
	rep := Build([]Event{
		{RunID: "aborted", Env: "probe-1", Event: EventPrewarm, OK: false, Err: "CreateError: no worker has room"},
		{RunID: "aborted", Event: EventRun, Name: "steps", OK: false},
	})
	fc := rep.FaultClassification
	if fc == nil || fc.AllRollouts != 0 || fc.MeetsTarget {
		t.Fatalf("fault classification %+v, want all_rollouts=0 meets_target=false", fc)
	}
	if got := FaultTargetStatus(fc); got != "no rollouts" {
		t.Fatalf("FaultTargetStatus = %q, want %q", got, "no rollouts")
	}
	if got := InfraFaultCell(rep); got != "no rollouts" {
		t.Fatalf("InfraFaultCell = %q, want %q", got, "no rollouts")
	}
	var txt, md, cmp, arms bytes.Buffer
	rep.WriteText(&txt)
	rep.WriteMarkdown(&md)
	WriteComparison(&cmp, []*Report{rep}, []string{"aborted"})
	WriteComparisonMarkdown(&arms, []*Report{rep}, []string{"aborted"})
	for name, out := range map[string]string{"text": txt.String(), "markdown": md.String(), "comparison": cmp.String(), "arms": arms.String()} {
		if !strings.Contains(out, "no rollouts") || strings.Contains(out, "PASS") {
			t.Fatalf("%s must say no rollouts and never PASS:\n%s", name, out)
		}
	}
	if !strings.Contains(arms.String(), "| Infra Fault Rate (<2%) |") {
		t.Fatalf("arm table header must carry the target rate:\n%s", arms.String())
	}
}

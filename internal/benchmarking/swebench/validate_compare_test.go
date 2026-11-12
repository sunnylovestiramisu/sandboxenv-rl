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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

func boolPtr(v bool) *bool        { return &v }
func floatPtr(v float64) *float64 { return &v }

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestMultiWorkerShardMergeAndExactPercentiles(t *testing.T) {
	dir := t.TempDir()

	// Create 2 worker shards with distinct raw TTFI durations:
	// worker-0: [100, 200, 300] (shard p50 = 200)
	// worker-1: [1000, 2000, 3000, 4000, 5000, 6000, 7000] (shard p50 = 4000)
	// Combined sorted 10 samples: [100, 200, 300, 1000, 2000, 3000, 4000, 5000, 6000, 7000]
	// Nearest-rank exact combined p50 (ceil(0.5*10)-1 = idx 4) = 2000.0 (whereas average of shard p50s would be 2100.0!)
	// Nearest-rank exact combined p90 (ceil(0.9*10)-1 = idx 8) = 6000.0
	// Nearest-rank exact combined p95 (ceil(0.95*10)-1 = idx 9) = 7000.0
	// Exact combined max = 7000.0
	rec0, err := NewShardRecorder(dir, "run-shard-test", "worker-0")
	if err != nil {
		t.Fatalf("NewShardRecorder worker-0: %v", err)
	}
	rec0.SetJob("job-a")
	for i, d := range []float64{100, 200, 300} {
		rec0.Record(Event{
			Time:   mustTime("2026-10-22T01:20:01Z"),
			Event:  EventTTFI,
			Step:   1,
			Task:   i,
			DurMS:  d,
			OK:     true,
			Reward: intp(1),
		})
		rec0.Record(Event{
			Time:   mustTime("2026-10-22T01:20:05Z"),
			Event:  EventTask,
			Step:   1,
			Task:   i,
			DurMS:  d * 2,
			OK:     true,
			Reward: intp(1),
			Metrics: map[string]float64{
				"llm_think_ms": d * 0.5,
				"env_wait_ms":  d * 1.5,
			},
		})
	}
	if err := rec0.Close(); err != nil {
		t.Fatalf("close rec0: %v", err)
	}

	rec1, err := NewShardRecorder(dir, "run-shard-test", "worker-1")
	if err != nil {
		t.Fatalf("NewShardRecorder worker-1: %v", err)
	}
	rec1.SetJob("job-a")
	for i, d := range []float64{1000, 2000, 3000, 4000, 5000, 6000, 7000} {
		rec1.Record(Event{
			Time:   mustTime("2026-10-22T01:20:02Z"),
			Event:  EventTTFI,
			Step:   1,
			Task:   i + 3,
			DurMS:  d,
			OK:     true,
			Reward: intp(1),
		})
		rec1.Record(Event{
			Time:   mustTime("2026-10-22T01:20:10Z"),
			Event:  EventTask,
			Step:   1,
			Task:   i + 3,
			DurMS:  d * 2,
			OK:     true,
			Reward: intp(1),
			Metrics: map[string]float64{
				"llm_think_ms": d * 0.5,
				"env_wait_ms":  d * 1.5,
			},
		})
	}
	rec1.Record(Event{
		Time:  mustTime("2026-10-22T01:20:11Z"),
		Event: EventBarrier,
		Step:  1,
		DurMS: 14500.0,
		OK:    true,
		Count: 10,
	})
	if err := rec1.Close(); err != nil {
		t.Fatalf("close rec1: %v", err)
	}

	globPattern := filepath.Join(dir, "events", "*.jsonl")
	rep, err := SummarizeGlob(globPattern)
	if err != nil {
		t.Fatalf("SummarizeGlob: %v", err)
	}
	if rep.Tasks != 10 || rep.Rewarded != 10 {
		t.Fatalf("tasks=%d rewarded=%d, want 10/10", rep.Tasks, rep.Rewarded)
	}
	ttfi, ok := rep.StatFor(EventTTFI, "")
	if !ok || ttfi.Count != 10 {
		t.Fatalf("ttfi.Count = %d (ok=%v), want 10", ttfi.Count, ok)
	}
	if ttfi.P50MS != 2000.0 {
		t.Errorf("ttfi.P50MS = %v, want exact combined 2000.0", ttfi.P50MS)
	}
	if ttfi.P90MS != 6000.0 {
		t.Errorf("ttfi.P90MS = %v, want exact combined 6000.0", ttfi.P90MS)
	}
	if ttfi.P95MS != 7000.0 {
		t.Errorf("ttfi.P95MS = %v, want exact combined 7000.0", ttfi.P95MS)
	}
	if ttfi.P99MS != 7000.0 {
		t.Errorf("ttfi.P99MS = %v, want exact combined 7000.0", ttfi.P99MS)
	}
	if ttfi.MaxMS != 7000.0 {
		t.Errorf("ttfi.MaxMS = %v, want 7000.0", ttfi.MaxMS)
	}

	// Verify StepRolloutBreakdown:
	// Task durations are 2x TTFI: p50 = 4000.0, p100 = 14000.0, skew = 14000/4000 = 3.50
	// BarrierMS = 14500.0, env_wait is 75% of rollout_time -> EnvOverheadPct = 75.0
	if len(rep.RolloutBreakdown) != 1 {
		t.Fatalf("len(RolloutBreakdown) = %d, want 1", len(rep.RolloutBreakdown))
	}
	rb := rep.RolloutBreakdown[0]
	if rb.P50MS != 4000.0 || rb.P100MS != 14000.0 {
		t.Errorf("RolloutBreakdown P50=%v P100=%v, want 4000.0 / 14000.0", rb.P50MS, rb.P100MS)
	}
	if rb.StragglerSkew != 3.5 {
		t.Errorf("RolloutBreakdown StragglerSkew = %v, want 3.5", rb.StragglerSkew)
	}
	if rb.BarrierMS != 14500.0 {
		t.Errorf("RolloutBreakdown BarrierMS = %v, want 14500.0", rb.BarrierMS)
	}
	if rb.EnvOverheadPct != 75.0 {
		t.Errorf("RolloutBreakdown EnvOverheadPct = %v, want 75.0", rb.EnvOverheadPct)
	}
	if rb.LLMThinkMS+rb.EnvWaitMS != 5720.0 {
		t.Errorf("LLMThinkMS (%v) + EnvWaitMS (%v) = %v, want 5720.0", rb.LLMThinkMS, rb.EnvWaitMS, rb.LLMThinkMS+rb.EnvWaitMS)
	}
}

func TestWriteRunWindowAndStatsJSONLValidateAgainstSchema(t *testing.T) {
	dir := t.TempDir()
	rwPath := filepath.Join(dir, "run_window.json")
	rw := schema.RunWindow{
		RunID:         "swe-run-42",
		TestName:      "swebench-steps",
		Tag:           "candidate",
		StartTS:       "2026-10-22T01:20:00Z",
		EndTS:         "2026-10-22T01:25:00Z",
		SteadyStartTS: "2026-10-22T01:21:00Z",
		SteadyEndTS:   "2026-10-22T01:24:30Z",
		Cluster:       "gke-rlbench-1",
		SUTEndpoint:   "localhost:8080",
		Metadata:      map[string]string{"overlay": "prefetch"},
	}
	if err := WriteRunWindow(rwPath, rw); err != nil {
		t.Fatalf("WriteRunWindow: %v", err)
	}
	rawRW, err := os.ReadFile(rwPath)
	if err != nil {
		t.Fatalf("read run_window.json: %v", err)
	}
	if err := schema.ValidateAgainstJSONSchema("run_window.schema.json", rawRW); err != nil {
		t.Fatalf("ValidateAgainstJSONSchema(run_window.schema.json): %v", err)
	}
	gotRW, err := ReadRunWindow(rwPath)
	if err != nil {
		t.Fatalf("ReadRunWindow: %v", err)
	}
	if gotRW.RunID != "swe-run-42" || gotRW.Cluster != "gke-rlbench-1" {
		t.Fatalf("unexpected RunWindow: %+v", gotRW)
	}

	rep, err := SummarizeFile("../schema/testdata/baseline-eval.jsonl")
	if err != nil {
		t.Fatalf("SummarizeFile: %v", err)
	}
	rep.CustomMetrics = map[string]schema.MetricSummary{
		"overlay_prefetch_queue_depth": {
			Count: 4,
			Min:   1,
			P50:   2,
			P90:   3.5,
			P95:   4,
			Max:   4,
			Unit:  "items",
		},
	}

	statsPath := filepath.Join(dir, "stats.jsonl")
	if err := WriteStatsJSONL(statsPath, rep, "swe-run-42", "swebench-eval", "candidate"); err != nil {
		t.Fatalf("WriteStatsJSONL: %v", err)
	}
	rawStats, err := os.ReadFile(statsPath)
	if err != nil {
		t.Fatalf("read stats.jsonl: %v", err)
	}
	if err := schema.ValidateJSONLAgainstJSONSchema("stats_row.schema.json", bytes.NewReader(rawStats)); err != nil {
		t.Fatalf("ValidateJSONLAgainstJSONSchema(stats_row.schema.json): %v", err)
	}
	rows, err := schema.DecodeJSONLStrict[schema.StatsRow](bytes.NewReader(rawStats))
	if err != nil {
		t.Fatalf("DecodeJSONLStrict[StatsRow]: %v", err)
	}
	seenMetrics := map[string]bool{}
	for _, row := range rows {
		seenMetrics[row.Metric] = true
	}
	for _, want := range []string{"trial_summary", "phase_breakdown_summary", "throughput", "op_ttfi", "overlay_prefetch_queue_depth", "custom_metrics"} {
		if !seenMetrics[want] {
			t.Errorf("stats.jsonl missing expected metric row %q (got %v)", want, seenMetrics)
		}
	}
}

func TestValidityRules1Through7(t *testing.T) {
	makeValidFixture := func() (*Report, ValidateOptions) {
		b := NewBuilder()
		b.Add(Event{Time: mustTime("2026-10-22T01:20:00Z"), RunID: "r1", Job: "sim-1", Event: EventPrewarm, DurMS: 100, OK: true})
		b.Add(Event{Time: mustTime("2026-10-22T01:20:01Z"), RunID: "r1", Job: "sim-1", Event: EventCreate, Step: 1, Task: 0, DurMS: 50, OK: true, CacheHit: boolPtr(true)})
		b.Add(Event{Time: mustTime("2026-10-22T01:20:02Z"), RunID: "r1", Job: "sim-1", Event: EventTTFI, Step: 1, Task: 0, DurMS: 200, OK: true})
		b.Add(Event{Time: mustTime("2026-10-22T01:20:05Z"), RunID: "r1", Job: "sim-1", Event: EventTask, Step: 1, Task: 0, DurMS: 1000, OK: true, Reward: intp(1)})
		b.Add(Event{Time: mustTime("2026-10-22T01:20:06Z"), RunID: "r1", Job: "sim-1", Event: EventCreate, Step: 1, Task: 1, DurMS: 60, OK: true, CacheHit: boolPtr(true)})
		b.Add(Event{Time: mustTime("2026-10-22T01:20:07Z"), RunID: "r1", Job: "sim-1", Event: EventTTFI, Step: 1, Task: 1, DurMS: 220, OK: true})
		b.Add(Event{Time: mustTime("2026-10-22T01:20:10Z"), RunID: "r1", Job: "sim-1", Event: EventTask, Step: 1, Task: 1, DurMS: 1100, OK: true, Reward: intp(1)})
		b.Add(Event{Time: mustTime("2026-10-22T01:20:12Z"), RunID: "r1", Job: "sim-1", Event: EventReap, DurMS: 80, OK: true, Metrics: map[string]float64{"orphan_count": 0}})
		rep := b.Finish()
		opts := ValidateOptions{
			Footing: "warm",
			CacheBefore: &NodeCacheSnapshot{
				Nodes: []NodeCacheNodeEntry{{Node: "n1", Hits: 10, Misses: 2}},
			},
			CacheAfter: &NodeCacheSnapshot{
				Nodes: []NodeCacheNodeEntry{{Node: "n1", Hits: 14, Misses: 2}},
			},
			MinRewardRate:        0.8,
			CheckRewardRate:      true,
			MinTasks:             2,
			MaxFailedCreateRatio: 0.1,
			CheckFailedCreates:   true,
			Schedule: &schema.Schedule{
				SchemaVersion: "v1",
				ScenarioName:  "scen",
				Jobs: []schema.ScheduledJob{
					{
						Job: "sim-1",
						Steps: []schema.ScheduledStep{
							{
								Step: 1,
								Tasks: []schema.ScheduledTask{
									{InstanceID: "i1", Image: "img1"},
									{InstanceID: "i2", Image: "img2"},
								},
							},
						},
					},
				},
			},
			TraceCaptured: false,
			OrphanCount:   0,
			CheckReap:     true,
		}
		return rep, opts
	}

	t.Run("all 7 rules pass and render Markdown", func(t *testing.T) {
		rep, opts := makeValidFixture()
		v := Validate(rep, opts)
		if !v.Valid || len(v.Reasons) != 0 {
			t.Fatalf("expected valid report, got %+v", v)
		}
		var md bytes.Buffer
		rep.WriteMarkdown(&md)
		if !strings.Contains(md.String(), "## Validity") || !strings.Contains(md.String(), "PASS (`valid: true`)") {
			t.Fatalf("Markdown missing PASS validity section:\n%s", md.String())
		}
	})

	t.Run("Rule 1 warm footing fails without prewarm or positive cache hits", func(t *testing.T) {
		rep, opts := makeValidFixture()
		rep.PrewarmCount = 0
		opts.CacheAfter = &NodeCacheSnapshot{
			Nodes: []NodeCacheNodeEntry{{Node: "n1", Hits: 10, Misses: 5}},
		}
		v := Validate(rep, opts)
		if v.Valid || !strings.Contains(strings.Join(v.Reasons, "; "), "rule 1 (footing=warm)") {
			t.Fatalf("expected Rule 1 warm failure, got %+v", v)
		}
	})

	t.Run("Rule 1 cold footing passes with 0 prewarm and 0 cache hits, fails with prewarm", func(t *testing.T) {
		rep, opts := makeValidFixture()
		rep.PrewarmCount = 0
		rep.CreateCacheHits = 0
		rep.CreateCacheMisses = 2
		opts.Footing = "cold"
		opts.CacheBefore = &NodeCacheSnapshot{Hits: 5, Misses: 1}
		opts.CacheAfter = &NodeCacheSnapshot{Hits: 5, Misses: 3}
		v := Validate(rep, opts)
		if !v.Valid {
			t.Fatalf("expected cold footing pass, got %+v", v)
		}

		rep.PrewarmCount = 1
		v2 := Validate(rep, opts)
		if v2.Valid || !strings.Contains(strings.Join(v2.Reasons, "; "), "rule 1 (footing=cold)") {
			t.Fatalf("expected Rule 1 cold failure when prewarm present, got %+v", v2)
		}
	})

	t.Run("Rule 2 reward rate below threshold fails", func(t *testing.T) {
		rep, opts := makeValidFixture()
		rep.RewardRate = 0.5
		v := Validate(rep, opts)
		if v.Valid || !strings.Contains(strings.Join(v.Reasons, "; "), "rule 2 (reward_rate)") {
			t.Fatalf("expected Rule 2 failure, got %+v", v)
		}
	})

	t.Run("Rule 3 task count below minimum fails", func(t *testing.T) {
		rep, opts := makeValidFixture()
		opts.MinTasks = 10
		v := Validate(rep, opts)
		if v.Valid || !strings.Contains(strings.Join(v.Reasons, "; "), "rule 3 (min_tasks)") {
			t.Fatalf("expected Rule 3 failure, got %+v", v)
		}
	})

	t.Run("Rule 4 failed create fraction above threshold fails", func(t *testing.T) {
		rep, opts := makeValidFixture()
		for i := range rep.Stats {
			if rep.Stats[i].Event == EventCreate {
				rep.Stats[i].Count = 4
				rep.Stats[i].Failed = 2 // 50% > 10%
			}
		}
		v := Validate(rep, opts)
		if v.Valid || !strings.Contains(strings.Join(v.Reasons, "; "), "rule 4 (failed_creates)") {
			t.Fatalf("expected Rule 4 failure, got %+v", v)
		}
	})

	t.Run("Rule 5 incomplete schedule fails", func(t *testing.T) {
		rep, opts := makeValidFixture()
		opts.Schedule.Jobs = append(opts.Schedule.Jobs, schema.ScheduledJob{
			Job: "missing-job",
			Steps: []schema.ScheduledStep{
				{Step: 1, Tasks: []schema.ScheduledTask{{InstanceID: "i1", Image: "img"}}},
			},
		})
		v := Validate(rep, opts)
		if v.Valid || !strings.Contains(strings.Join(v.Reasons, "; "), "rule 5 (schedule)") {
			t.Fatalf("expected Rule 5 failure, got %+v", v)
		}
	})

	t.Run("Rule 6 records trace capture without failing", func(t *testing.T) {
		rep, opts := makeValidFixture()
		if v := Validate(rep, opts); !v.Valid || rep.TraceCaptured {
			t.Fatalf("untraced: valid=%t captured=%t", v.Valid, rep.TraceCaptured)
		}
		if md := rep.TraceMetadata(); md[TraceCapturedMetadataKey] != "false" || len(md) != 1 {
			t.Fatalf("untraced metadata %v", md)
		}

		rep, opts = makeValidFixture()
		opts.TraceCaptured, opts.TraceFileCount = true, 3
		if v := Validate(rep, opts); !v.Valid || !rep.TraceCaptured || rep.TraceFileCount != 3 {
			t.Fatalf("traced: %+v captured=%t files=%d", v, rep.TraceCaptured, rep.TraceFileCount)
		}
		if md := rep.TraceMetadata(); md[TraceCapturedMetadataKey] != "true" || md[TraceFileCountMetadataKey] != "3" {
			t.Fatalf("traced metadata %v", md)
		}
		var md bytes.Buffer
		rep.WriteMarkdown(&md)
		if !strings.Contains(md.String(), "**Trace captured**: yes (3 trace files") {
			t.Fatalf("report.md lacks the trace line:\n%s", md.String())
		}

		// A trace-mode run event, or a report that already says so, is evidence too.
		rep, opts = makeValidFixture()
		opts.Events = append(opts.Events, Event{Event: EventRun, Name: "trace", OK: true})
		if v := Validate(rep, opts); !v.Valid || !rep.TraceCaptured {
			t.Fatalf("from events: valid=%t captured=%t reasons=%v", v.Valid, rep.TraceCaptured, v.Reasons)
		}
		rep, opts = makeValidFixture()
		rep.TraceCaptured = true
		if v := Validate(rep, opts); !v.Valid || !rep.TraceCaptured {
			t.Fatalf("persisted: valid=%t captured=%t", v.Valid, rep.TraceCaptured)
		}
	})

	t.Run("FindTraceFiles discovers every trace layout", func(t *testing.T) {
		dir := t.TempDir()
		for _, rel := range []string{
			"trace.jsonl",
			"verl-live-trace.jsonl",
			filepath.Join("traces", "worker-0.jsonl"),
			filepath.Join("traces", "verl-job", "worker-1.jsonl"),
			filepath.Join("results", "traces", "worker-2.jsonl"),
			filepath.Join("traces", "empty.jsonl"), // empty: not evidence
			"events.jsonl",                         // not a trace
		} {
			if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, rel)), 0o755); err != nil {
				t.Fatal(err)
			}
			content := []byte("{}\n")
			if strings.HasPrefix(filepath.Base(rel), "empty") {
				content = nil
			}
			if err := os.WriteFile(filepath.Join(dir, rel), content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if got := FindTraceFiles(dir); len(got) != 5 {
			t.Fatalf("expected 5 trace files, got %v", got)
		}
		if FindTraceFiles(t.TempDir()) != nil {
			t.Fatal("expected no trace files in an empty dir")
		}
	})

	t.Run("Rule 7 non-zero orphan count fails", func(t *testing.T) {
		rep, opts := makeValidFixture()
		opts.OrphanCount = 3
		v := Validate(rep, opts)
		if v.Valid || !strings.Contains(strings.Join(v.Reasons, "; "), "rule 7 (reap)") {
			t.Fatalf("expected Rule 7 failure, got %+v", v)
		}
		var md bytes.Buffer
		rep.WriteMarkdown(&md)
		if !strings.Contains(md.String(), "FAIL (`valid: false`)") || !strings.Contains(md.String(), "orphan_count=3") {
			t.Fatalf("expected FAIL validity section in Markdown:\n%s", md.String())
		}
	})
}

func TestCompareThresholdsPassAndFail(t *testing.T) {
	base := &Report{
		RunID:      "base",
		Tasks:      4,
		Rewarded:   4,
		RewardRate: 1.0,
		Stats: []Stat{
			{Event: EventTTFI, Count: 4, P50MS: 1000, P90MS: 1100, P95MS: 1200, MaxMS: 1300},
			{Event: EventWake, Count: 4, P50MS: 500, P90MS: 550, P95MS: 600, MaxMS: 650},
		},
		CustomMetrics: map[string]schema.MetricSummary{
			"queue_depth": {Count: 5, P50: 2, P95: 4, Max: 5},
		},
	}

	th := Thresholds{
		MaxLatencyRegressionPct: 15.0,
		MaxP50RegressionPct:     map[string]float64{"ttfi": 10.0},
		MaxP95RegressionPct:     map[string]float64{"ttfi": 12.0},
		MinRewardDelta:          floatPtr(-0.05),
		CustomMetricMaxP95:      map[string]float64{"queue_depth": 6.0},
		CustomMetrics: map[string]MetricBound{
			"queue_depth": {
				MaxP50: floatPtr(4.0),
				MaxMax: floatPtr(8.0),
			},
		},
	}

	t.Run("candidate within bounds passes", func(t *testing.T) {
		cand := &Report{
			RunID:      "cand-ok",
			Tasks:      4,
			Rewarded:   4,
			RewardRate: 0.98,
			Stats: []Stat{
				{Event: EventTTFI, Count: 4, P50MS: 1050, P90MS: 1150, P95MS: 1300, MaxMS: 1350},
				{Event: EventWake, Count: 4, P50MS: 540, P90MS: 580, P95MS: 650, MaxMS: 680},
			},
			CustomMetrics: map[string]schema.MetricSummary{
				"queue_depth": {Count: 5, P50: 3, P95: 5, Max: 6},
			},
		}
		res := Compare(base, cand, th)
		if !res.Passed || len(res.Regressions) != 0 {
			t.Fatalf("expected pass, got regressions: %v", res.Regressions)
		}
	})

	t.Run("candidate violating latency, reward, and custom_metrics fails", func(t *testing.T) {
		cand := &Report{
			RunID:      "cand-bad",
			Tasks:      4,
			Rewarded:   2,
			RewardRate: 0.50, // delta -0.50 < -0.05
			Stats: []Stat{
				{Event: EventTTFI, Count: 4, P50MS: 1300, P90MS: 1500, P95MS: 1600, MaxMS: 1700}, // +30% > +10%
				{Event: EventWake, Count: 4, P50MS: 700, P90MS: 750, P95MS: 800, MaxMS: 850},     // +40% > +15%
			},
			CustomMetrics: map[string]schema.MetricSummary{
				"queue_depth": {Count: 5, P50: 5, P95: 9, Max: 12},
			},
		}
		res := Compare(base, cand, th)
		if res.Passed {
			t.Fatal("expected comparison failure, got Passed=true")
		}
		if len(res.Regressions) < 4 {
			t.Fatalf("expected at least 4 regressions, got %v", res.Regressions)
		}
		md := FormatComparisonMarkdown(res)
		if !strings.Contains(md, "**Status**: FAIL") || !strings.Contains(md, "REGRESSED") {
			t.Fatalf("unexpected comparison markdown:\n%s", md)
		}
	})
}

func TestValidateRule1ReadsThePreflightCoverage(t *testing.T) {
	now := time.Now()
	complete := &schema.PreflightCoverage{SchemaVersion: schema.SchemaVersion, RunID: "pf", StartedAt: now, FinishedAt: now, Complete: true,
		Goldens: []schema.GoldenRecord{{Image: "img", State: "ready"}},
		Domains: []schema.DomainCoverage{{Name: "d0", Nodes: 1, NodesSeen: []string{"n0"}, Complete: true,
			Images: []schema.ImageCoverage{{Image: "img", Nodes: []string{"n0"}, Wanted: 1, Probes: 1, Complete: true}}}}}
	incomplete := &schema.PreflightCoverage{SchemaVersion: schema.SchemaVersion, RunID: "pf", StartedAt: now, FinishedAt: now, Complete: false,
		Goldens: []schema.GoldenRecord{{Image: "img", State: "pending"}},
		Domains: []schema.DomainCoverage{{Name: "d0", Nodes: 2, NodesSeen: []string{"n0"}, Complete: false,
			Images: []schema.ImageCoverage{{Image: "img", Nodes: []string{"n0"}, Wanted: 2, Probes: 6, Rounds: 3, Complete: false}}}}}
	has := func(v schema.ReportValidity, substr string) bool {
		for _, r := range v.Reasons {
			if strings.Contains(r, substr) {
				return true
			}
		}
		return false
	}
	if err := complete.Validate(); err != nil {
		t.Fatalf("complete fixture: %v", err)
	}
	// A complete pre-flight stands in for prewarm events on warm footing.
	v := Validate(&Report{DistinctImages: []string{"img"}}, ValidateOptions{Footing: "warm", PreflightCoverage: complete})
	if !v.Valid || len(v.Evidence) != 1 || !strings.Contains(v.Evidence[0], "pre-flight coverage complete") || !strings.Contains(v.Evidence[0], "run "+complete.RunID) {
		t.Errorf("warm with a complete pre-flight: valid=%v evidence=%v, want the coverage named", v.Valid, v.Evidence)
	}
	if ev := Validate(&Report{PrewarmCount: 3}, ValidateOptions{Footing: "warm"}).Evidence; len(ev) != 1 || !strings.Contains(ev[0], "3 prewarm event(s), no pre-flight coverage") {
		t.Errorf("warm on prewarm events: evidence=%v", ev)
	}
	if ev := Validate(&Report{}, ValidateOptions{Footing: "cold"}).Evidence; len(ev) != 1 || !strings.Contains(ev[0], "no prewarm events and no pre-flight coverage") {
		t.Errorf("cold: evidence=%v", ev)
	}
	if has(v, "expected prewarm events") || has(v, "pre-flight is incomplete") || has(v, "did not cover") {
		t.Errorf("warm + complete pre-flight: %v", v.Reasons)
	}
	// A pre-flight over a subset is complete for what it ran; the run's other
	// images are not covered.
	v = Validate(&Report{DistinctImages: []string{"img", "other"}}, ValidateOptions{Footing: "warm", PreflightCoverage: complete})
	if !has(v, "did not cover 1 of the run's 2 images (first: other)") {
		t.Errorf("warm + partial pre-flight: %v", v.Reasons)
	}
	// An incomplete one fails warm footing and names the shortfall, once:
	// no prewarm-event reason doubles it when there are none.
	v = Validate(&Report{PrewarmCount: 0, DistinctImages: []string{"img"}}, ValidateOptions{Footing: "warm", PreflightCoverage: incomplete})
	if !has(v, "pre-flight is incomplete") || !has(v, "1 goldens not ready") || !has(v, "1 (image, domain) pairs short") || has(v, "expected prewarm events") {
		t.Errorf("warm + incomplete pre-flight: %v", v.Reasons)
	}
	// Any pre-flight contradicts cold footing; without a path the run id names it.
	v = Validate(&Report{}, ValidateOptions{Footing: "cold", PreflightCoverage: complete, PreflightCoveragePath: "cov.json"})
	if !has(v, "a pre-flight ran before the run (cov.json)") {
		t.Errorf("cold + pre-flight: %v", v.Reasons)
	}
	v = Validate(&Report{}, ValidateOptions{Footing: "cold", PreflightCoverage: complete})
	if !has(v, "a pre-flight ran before the run (run pf)") {
		t.Errorf("cold + pre-flight without a path: %v", v.Reasons)
	}
	// A missing file is a rule 1 reason, not a crash.
	v = Validate(&Report{}, ValidateOptions{Footing: "warm", PreflightCoveragePath: filepath.Join(t.TempDir(), "none.json")})
	if !has(v, "rule 1: ") {
		t.Errorf("missing coverage file: %v", v.Reasons)
	}
}

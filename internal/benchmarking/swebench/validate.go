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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

// NodeCacheNodeEntry holds per-node cache counters from JSON cache_before.json / cache_after.json.
type NodeCacheNodeEntry struct {
	Node      string `json:"node"`
	TS        string `json:"ts,omitempty"`
	Hits      int64  `json:"hits"`
	Misses    int64  `json:"misses"`
	Errors    int64  `json:"errors,omitempty"`
	Evictions int64  `json:"evictions,omitempty"`
	BytesUsed int64  `json:"bytes_used,omitempty"`
}

// NodeCacheSnapshot represents a node image-cache sample (either multi-node
// JSON format with "nodes", a flat {"hits", "misses"} JSON object, or a
// Prometheus text sample parsed via ParseNodeCacheSample).
type NodeCacheSnapshot struct {
	SchemaVersion string               `json:"schema_version,omitempty"`
	RunID         string               `json:"run_id,omitempty"`
	CollectedAt   string               `json:"collected_at,omitempty"`
	Nodes         []NodeCacheNodeEntry `json:"nodes,omitempty"`
	Hits          int64                `json:"hits,omitempty"`
	Misses        int64                `json:"misses,omitempty"`
}

// TotalHitsAndMisses sums hits and misses across Nodes (or returns top-level Hits/Misses).
func (s *NodeCacheSnapshot) TotalHitsAndMisses() (hits int64, misses int64) {
	if s == nil {
		return 0, 0
	}
	if len(s.Nodes) > 0 {
		for _, n := range s.Nodes {
			hits += n.Hits
			misses += n.Misses
		}
		return hits, misses
	}
	return s.Hits, s.Misses
}

// Delta computes the CacheDelta between s (before) and after.
func (s *NodeCacheSnapshot) Delta(after *NodeCacheSnapshot) CacheDelta {
	if s == nil || after == nil {
		return CacheDelta{}
	}
	bHits, bMisses := s.TotalHitsAndMisses()
	aHits, aMisses := after.TotalHitsAndMisses()
	hitsDelta := aHits - bHits
	missesDelta := aMisses - bMisses
	if hitsDelta < 0 {
		hitsDelta = aHits
	}
	if missesDelta < 0 {
		missesDelta = aMisses
	}
	nodesCount := len(after.Nodes)
	if nodesCount == 0 && len(s.Nodes) > 0 {
		nodesCount = len(s.Nodes)
	}
	var nodesWithMisses int
	beforeByNode := map[string]int64{}
	for _, n := range s.Nodes {
		beforeByNode[n.Node] = n.Misses
	}
	for _, n := range after.Nodes {
		if n.Misses-beforeByNode[n.Node] > 0 {
			nodesWithMisses++
		}
	}
	if missesDelta > 0 && nodesWithMisses == 0 {
		nodesWithMisses = 1
	}
	return CacheDelta{
		Nodes:           nodesCount,
		Hits:            float64(hitsDelta),
		Misses:          float64(missesDelta),
		NodesWithMisses: nodesWithMisses,
	}
}

// LoadNodeCacheSnapshot reads a cache_before or cache_after file (JSON or Prometheus tab-prefixed text).
func LoadNodeCacheSnapshot(path string) (*NodeCacheSnapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read node cache snapshot %q: %w", path, err)
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var snap NodeCacheSnapshot
		if err := json.Unmarshal(trimmed, &snap); err != nil {
			return nil, fmt.Errorf("decode node cache snapshot %q: %w", path, err)
		}
		return &snap, nil
	}
	parsed, err := ParseNodeCacheSample(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse node cache sample %q: %w", path, err)
	}
	var hits, misses float64
	var nodes []NodeCacheNodeEntry
	for node, counts := range parsed.Requests {
		hits += counts["hit"]
		misses += counts["miss"]
		nodes = append(nodes, NodeCacheNodeEntry{
			Node:   node,
			Hits:   int64(counts["hit"]),
			Misses: int64(counts["miss"]),
		})
	}
	return &NodeCacheSnapshot{
		Nodes:  nodes,
		Hits:   int64(hits),
		Misses: int64(misses),
	}, nil
}

// ValidateOptions configures the 7 validity rules from DESIGN.md §6.
type ValidateOptions struct {
	// Rule 1: declared footing ("warm" or "cold"). Empty skips Rule 1.
	Footing         string
	CacheBefore     *NodeCacheSnapshot
	CacheAfter      *NodeCacheSnapshot
	CacheBeforePath string
	CacheAfterPath  string

	// Rule 2: minimum required reward rate [0.0, 1.0].
	MinRewardRate   float64
	CheckRewardRate bool

	// Rule 3: minimum required total tasks.
	MinTasks int

	// Rule 4: maximum allowed failed create fraction [0.0, 1.0].
	MaxFailedCreateRatio float64
	CheckFailedCreates   bool

	// Rule 5: resolved schedule that all jobs must have completed.
	Schedule *schema.Schedule

	// Rule 6: trace capture evidence (trace-mode job, trace files on disk).
	// Recorded in the report, never a validity failure.
	TraceCaptured  bool
	TraceFileCount int

	// Rule 7: remaining orphan count after reap (must be 0).
	OrphanCount int
	CheckReap   bool

	// Optional raw event slice if not already summarized into Report.
	Events []Event

	// Rule 1: the pre-flight's preflight-coverage.json, when a PreflightWarmer
	// ran before the run. A complete pre-flight is prewarm evidence on warm
	// footing; an incomplete one fails warm footing with its shortfall; any
	// pre-flight contradicts cold footing.
	PreflightCoverage     *schema.PreflightCoverage
	PreflightCoveragePath string
}

// Validate evaluates all 7 validity rules from DESIGN.md §6 against rep and opts,
// populates rep.Validity, and returns the resulting schema.ReportValidity.
func Validate(rep *Report, opts ValidateOptions) schema.ReportValidity {
	var reasons []string
	if rep == nil {
		return schema.ReportValidity{
			Valid:   false,
			Reasons: []string{"report is nil"},
		}
	}

	prewarmCount := rep.PrewarmCount
	createHits := rep.CreateCacheHits
	createMisses := rep.CreateCacheMisses
	hasTrace := false
	reapSeen := rep.ReapSeen
	reapFailed := rep.ReapFailed
	orphanCount := rep.OrphanCount

	for _, ev := range opts.Events {
		if ev.Event == EventPrewarm {
			prewarmCount++
		}
		if ev.Event == EventCreate && ev.CacheHit != nil {
			if *ev.CacheHit {
				createHits++
			} else {
				createMisses++
			}
		}
		if ev.Phase == "trace" || (ev.Tags != nil && ev.Tags["mode"] == "trace") || (ev.Event == EventRun && ev.Name == "trace") {
			hasTrace = true
		}
		if ev.Event == EventReap {
			reapSeen = true
			if !ev.OK {
				reapFailed = true
			}
			if v, ok := ev.Metrics["orphan_count"]; ok && int(v) > orphanCount {
				orphanCount = int(v)
			} else if ev.Name == "orphans" && ev.Count > orphanCount {
				orphanCount = ev.Count
			}
		}
	}

	// Resolve optional cache_before / cache_after files.
	cacheBefore := opts.CacheBefore
	if cacheBefore == nil && opts.CacheBeforePath != "" {
		if snap, err := LoadNodeCacheSnapshot(opts.CacheBeforePath); err != nil {
			reasons = append(reasons, fmt.Sprintf("rule 1: %v", err))
		} else {
			cacheBefore = snap
		}
	}
	cacheAfter := opts.CacheAfter
	if cacheAfter == nil && opts.CacheAfterPath != "" {
		if snap, err := LoadNodeCacheSnapshot(opts.CacheAfterPath); err != nil {
			reasons = append(reasons, fmt.Sprintf("rule 1: %v", err))
		} else {
			cacheAfter = snap
		}
	}

	var hasCacheDelta bool
	var hitsDelta, missesDelta int64
	if cacheBefore != nil && cacheAfter != nil {
		bHits, bMisses := cacheBefore.TotalHitsAndMisses()
		aHits, aMisses := cacheAfter.TotalHitsAndMisses()
		hitsDelta = aHits - bHits
		missesDelta = aMisses - bMisses
		hasCacheDelta = true
		if rep.Cache == nil && (hitsDelta+missesDelta > 0 || opts.Footing != "") {
			d := cacheBefore.Delta(cacheAfter)
			rep.Cache = &d
			rep.PopulateCacheMissesPerFirstCmd()
		}
	} else if rep.Cache != nil {
		hitsDelta = int64(rep.Cache.Hits)
		missesDelta = int64(rep.Cache.Misses)
		hasCacheDelta = true
	}

	coverage := opts.PreflightCoverage
	if coverage == nil && opts.PreflightCoveragePath != "" {
		if c, err := ReadPreflightCoverage(opts.PreflightCoveragePath); err != nil {
			reasons = append(reasons, fmt.Sprintf("rule 1: %v", err))
		} else {
			coverage = c
		}
	}

	// Rule 1: Warm or cold footing matches prewarm events, the pre-flight and
	// the node cache delta. evidence names what the verdict rests on.
	var evidence []string
	coverageSource := opts.PreflightCoveragePath
	if coverageSource == "" && coverage != nil {
		coverageSource = "run " + coverage.RunID
	}
	switch opts.Footing {
	case "warm":
		if coverage != nil {
			missing := preflightMissingImages(coverage, rep.DistinctImages)
			if !coverage.Complete {
				reasons = append(reasons, "rule 1 (footing=warm): the pre-flight is incomplete: "+preflightShortfall(coverage))
			}
			// A pre-flight over a subset (-images, -domains) is complete for
			// what it ran; the run's images must all be in it.
			if len(missing) > 0 {
				reasons = append(reasons, fmt.Sprintf("rule 1 (footing=warm): the pre-flight did not cover %d of the run's %d images (first: %s)", len(missing), len(rep.DistinctImages), missing[0]))
			}
			if coverage.Complete && len(missing) == 0 {
				evidence = append(evidence, fmt.Sprintf("rule 1 (footing=warm): pre-flight coverage complete, %d images over %d domains (%s)", len(coverage.Goldens), len(coverage.Domains), coverageSource))
			}
			if prewarmCount > 0 {
				evidence = append(evidence, fmt.Sprintf("rule 1 (footing=warm): %d prewarm event(s), not needed next to the pre-flight", prewarmCount))
			}
		} else if prewarmCount == 0 {
			reasons = append(reasons, "rule 1 (footing=warm): expected prewarm events, found 0")
		} else {
			evidence = append(evidence, fmt.Sprintf("rule 1 (footing=warm): %d prewarm event(s), no pre-flight coverage", prewarmCount))
		}
		if hasCacheDelta && hitsDelta <= 0 {
			reasons = append(reasons, fmt.Sprintf("rule 1 (footing=warm): expected positive node cache hit delta, got hits_delta=%d (misses_delta=%d)", hitsDelta, missesDelta))
		} else if hasCacheDelta {
			evidence = append(evidence, fmt.Sprintf("rule 1 (footing=warm): node cache hits delta +%d (misses delta %d)", hitsDelta, missesDelta))
		}
		if createMisses > 0 {
			reasons = append(reasons, fmt.Sprintf("rule 1 (footing=warm): %d create event(s) had cache_hit=false", createMisses))
		} else if createHits > 0 {
			evidence = append(evidence, fmt.Sprintf("rule 1 (footing=warm): %d create event(s) with cache_hit=true", createHits))
		}
	case "cold":
		if prewarmCount > 0 {
			reasons = append(reasons, fmt.Sprintf("rule 1 (footing=cold): expected 0 prewarm events, found %d", prewarmCount))
		}
		if coverage != nil {
			reasons = append(reasons, "rule 1 (footing=cold): a pre-flight ran before the run ("+coverageSource+")")
		}
		if prewarmCount == 0 && coverage == nil {
			evidence = append(evidence, "rule 1 (footing=cold): no prewarm events and no pre-flight coverage")
		}
		if hasCacheDelta && hitsDelta > 0 {
			reasons = append(reasons, fmt.Sprintf("rule 1 (footing=cold): expected 0 node cache hit delta on cold footing, got hits_delta=%d", hitsDelta))
		} else if hasCacheDelta {
			evidence = append(evidence, fmt.Sprintf("rule 1 (footing=cold): node cache hits delta 0 (misses delta %d)", missesDelta))
		}
		if createHits > 0 {
			reasons = append(reasons, fmt.Sprintf("rule 1 (footing=cold): %d create event(s) had cache_hit=true", createHits))
		} else if createMisses > 0 {
			evidence = append(evidence, fmt.Sprintf("rule 1 (footing=cold): %d create event(s) with cache_hit=false", createMisses))
		}
	case "":
		// Footing not declared; skip Rule 1.
	default:
		reasons = append(reasons, fmt.Sprintf("rule 1: invalid footing %q (must be warm or cold)", opts.Footing))
	}

	// Rule 2: Reward rate meets configured threshold.
	if opts.CheckRewardRate || opts.MinRewardRate > 0 {
		if rep.RewardRate < opts.MinRewardRate {
			reasons = append(reasons, fmt.Sprintf("rule 2 (reward_rate): reward_rate %.4f < min %.4f", rep.RewardRate, opts.MinRewardRate))
		}
	}

	// Rule 3: Task count meets configured minimum.
	if opts.MinTasks > 0 && rep.Tasks < opts.MinTasks {
		reasons = append(reasons, fmt.Sprintf("rule 3 (min_tasks): tasks %d < min %d", rep.Tasks, opts.MinTasks))
	}

	// Rule 4: Failed creates stay under configured fraction.
	createStat, _ := rep.StatFor(EventCreate, "")
	totalCreates := createStat.Count
	failedCreates := createStat.Failed
	if totalCreates == 0 && len(opts.Events) > 0 {
		for _, ev := range opts.Events {
			if ev.Event == EventCreate {
				totalCreates++
				if !ev.OK {
					failedCreates++
				}
			}
		}
	}
	if (opts.CheckFailedCreates || opts.MaxFailedCreateRatio > 0) && totalCreates > 0 {
		ratio := float64(failedCreates) / float64(totalCreates)
		if ratio > opts.MaxFailedCreateRatio {
			reasons = append(reasons, fmt.Sprintf("rule 4 (failed_creates): failed create ratio %.4f > max %.4f (%d/%d)", ratio, opts.MaxFailedCreateRatio, failedCreates, totalCreates))
		}
	}

	// Rule 5: Every job ran its full schedule.
	if opts.Schedule != nil {
		for _, sj := range opts.Schedule.Jobs {
			expectedSteps := len(sj.Steps)
			expectedTasks := 0
			for _, st := range sj.Steps {
				expectedTasks += len(st.Tasks)
			}

			if js, ok := rep.Jobs[sj.Job]; ok {
				if js.Tasks < expectedTasks {
					reasons = append(reasons, fmt.Sprintf("rule 5 (schedule): job %q completed %d tasks, scheduled %d", sj.Job, js.Tasks, expectedTasks))
				}
				if js.Steps < expectedSteps {
					reasons = append(reasons, fmt.Sprintf("rule 5 (schedule): job %q completed %d steps, scheduled %d", sj.Job, js.Steps, expectedSteps))
				}
				if stepMap, ok := rep.JobStepTasks[sj.Job]; ok {
					for _, st := range sj.Steps {
						if got := stepMap[st.Step]; got < len(st.Tasks) {
							reasons = append(reasons, fmt.Sprintf("rule 5 (schedule): job %q step %d completed %d tasks, scheduled %d", sj.Job, st.Step, got, len(st.Tasks)))
						}
					}
				}
			} else if len(opts.Schedule.Jobs) == 1 && len(rep.Jobs) == 0 {
				// Single-job swebench run where events omit job field.
				if rep.Tasks < expectedTasks {
					reasons = append(reasons, fmt.Sprintf("rule 5 (schedule): job %q completed %d tasks, scheduled %d", sj.Job, rep.Tasks, expectedTasks))
				}
				completedSteps := len(rep.Steps)
				if completedSteps == 0 && rep.Tasks > 0 {
					completedSteps = 1
				}
				if completedSteps < expectedSteps {
					reasons = append(reasons, fmt.Sprintf("rule 5 (schedule): job %q completed %d steps, scheduled %d", sj.Job, completedSteps, expectedSteps))
				}
			} else {
				reasons = append(reasons, fmt.Sprintf("rule 5 (schedule): job %q did not run (expected %d steps, %d tasks)", sj.Job, expectedSteps, expectedTasks))
			}
		}
	}

	// Rule 6: trace capture is recorded, not rejected. Tracing adds per-call
	// latency, so the report must say whether these numbers include it.
	rep.RecordTraceCapture(opts.TraceCaptured || hasTrace, opts.TraceFileCount)

	// Rule 7: Reap reached zero remaining sandboxes/resources (orphan_count == 0).
	effectiveOrphans := orphanCount
	if opts.OrphanCount != 0 {
		effectiveOrphans = opts.OrphanCount
	}
	if effectiveOrphans != 0 {
		reasons = append(reasons, fmt.Sprintf("rule 7 (reap): orphan_count=%d (must be 0)", effectiveOrphans))
	}
	if reapFailed {
		reasons = append(reasons, "rule 7 (reap): reap event reported ok=false")
	}
	if opts.CheckReap && !reapSeen && opts.OrphanCount == 0 && len(opts.Events) > 0 {
		reasons = append(reasons, "rule 7 (reap): no reap event recorded")
	}

	validity := schema.ReportValidity{
		Valid:    len(reasons) == 0,
		Reasons:  reasons,
		Evidence: evidence,
	}
	rep.Validity = &validity
	return validity
}

// RecordTraceCapture merges trace evidence into rep. It only widens, so
// evidence from events, flags, and the artifact directory can arrive in any order.
func (rep *Report) RecordTraceCapture(captured bool, files int) {
	rep.TraceCaptured = rep.TraceCaptured || captured || files > 0 || rep.HasTraceEvents || rep.Mode == "trace"
	rep.TraceFileCount = max(rep.TraceFileCount, files)
}

// TraceCapturedLabel is the human-readable trace capture fact.
func (rep *Report) TraceCapturedLabel() string {
	switch {
	case !rep.TraceCaptured:
		return "no"
	case rep.TraceFileCount > 0:
		return fmt.Sprintf("yes (%d trace files; latencies include trace overhead)", rep.TraceFileCount)
	default:
		return "yes (latencies include trace overhead)"
	}
}

// run_window.json metadata keys for trace capture.
const (
	TraceCapturedMetadataKey  = "trace_captured"
	TraceFileCountMetadataKey = "trace_file_count"
)

// TraceMetadata returns the run_window.json metadata for rep's trace capture.
// trace_captured is always present so "no trace" is explicit.
func (rep *Report) TraceMetadata() map[string]string {
	md := map[string]string{TraceCapturedMetadataKey: strconv.FormatBool(rep.TraceCaptured)}
	if rep.TraceFileCount > 0 {
		md[TraceFileCountMetadataKey] = strconv.Itoa(rep.TraceFileCount)
	}
	return md
}

// Trace JSONL locations: the SDK's trace.jsonl / verl-live-trace.jsonl,
// RealJobDriver's traces/<job>/, and traces gathered from Ray workers.
var traceFilePatterns = []string{
	"trace.jsonl",
	"verl-live-trace.jsonl",
	filepath.Join("traces", "*.jsonl"),
	filepath.Join("traces", "*", "*.jsonl"),
	filepath.Join("results", "traces", "*.jsonl"),
}

// FindTraceFiles returns the non-empty trace JSONL files under dir, sorted.
func FindTraceFiles(dir string) []string {
	var out []string
	for _, pat := range traceFilePatterns {
		matches, _ := filepath.Glob(filepath.Join(dir, pat))
		for _, m := range matches {
			if st, err := os.Stat(m); err == nil && !st.IsDir() && st.Size() > 0 {
				out = append(out, m)
			}
		}
	}
	slices.Sort(out)
	return out
}

// ValidateReportOrEvents loads a Report from reportPath (or summarizes eventsPath,
// or initializes an empty Report), loads schedulePath if non-empty, and evaluates
// Validate(rep, opts).
func ValidateReportOrEvents(reportPath, eventsPath, schedulePath string, opts ValidateOptions) (*Report, schema.ReportValidity, error) {
	var rep *Report
	if reportPath != "" {
		r, err := LoadOrSummarizeReport(reportPath)
		if err != nil {
			return nil, schema.ReportValidity{}, fmt.Errorf("load report %q: %w", reportPath, err)
		}
		rep = r
	} else if eventsPath != "" {
		f, err := os.Open(eventsPath)
		if err != nil {
			return nil, schema.ReportValidity{}, fmt.Errorf("open events %q: %w", eventsPath, err)
		}
		evs, err := ReadEvents(f)
		_ = f.Close()
		if err != nil {
			return nil, schema.ReportValidity{}, fmt.Errorf("read events %q: %w", eventsPath, err)
		}
		rep = Build(evs)
		opts.Events = evs
	} else {
		rep = &Report{}
	}
	if schedulePath != "" && opts.Schedule == nil {
		sched, err := ReadSchedule(schedulePath)
		if err != nil {
			return nil, schema.ReportValidity{}, err
		}
		opts.Schedule = sched
	}
	v := Validate(rep, opts)
	return rep, v, nil
}

// preflightMissingImages lists the run's images that no domain of the
// pre-flight covered completely, sorted. An image the plan left unplaced is
// not in the coverage either: it boots cold by design, and warm footing does
// not hold for it.
func preflightMissingImages(c *schema.PreflightCoverage, images []string) []string {
	covered := map[string]bool{}
	for _, d := range c.Domains {
		for _, ic := range d.Images {
			if ic.Complete {
				covered[ic.Image] = true
			}
		}
	}
	var missing []string
	for _, img := range images {
		if !covered[img] {
			missing = append(missing, img)
		}
	}
	sort.Strings(missing)
	return missing
}

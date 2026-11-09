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
	"os"
	"sort"
	"strings"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

// MetricBound defines optional absolute and relative bounds for a custom metric.
type MetricBound struct {
	MaxP50           *float64 `json:"max_p50,omitempty"`
	MaxP95           *float64 `json:"max_p95,omitempty"`
	MaxMax           *float64 `json:"max_max,omitempty"`
	MinP50           *float64 `json:"min_p50,omitempty"`
	MaxRegressionPct *float64 `json:"max_regression_pct,omitempty"`
}

// Thresholds defines pass/fail regression bounds for comparing two Reports.
type Thresholds struct {
	// MaxLatencyRegressionPct is an optional default max allowed latency increase (%)
	// applied to operations when not overridden in MaxP50RegressionPct / MaxP95RegressionPct.
	MaxLatencyRegressionPct float64                `json:"max_latency_regression_pct,omitempty"`
	MaxP50RegressionPct     map[string]float64     `json:"max_p50_regression_pct,omitempty"`
	MaxP95RegressionPct     map[string]float64     `json:"max_p95_regression_pct,omitempty"`
	MinRewardDelta          *float64               `json:"min_reward_delta,omitempty"`
	MaxInfraFaultRate       *float64               `json:"max_infra_fault_rate,omitempty"`
	CustomMetricMaxP95      map[string]float64     `json:"custom_metric_max_p95,omitempty"`
	CustomMetrics           map[string]MetricBound `json:"custom_metrics,omitempty"`
}

// LoadThresholds reads and decodes a YAML or JSON Thresholds file.
func LoadThresholds(path string) (Thresholds, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Thresholds{}, fmt.Errorf("read thresholds %q: %w", path, err)
	}
	th, err := schema.DecodeYAMLStrict[Thresholds](raw)
	if err != nil {
		return Thresholds{}, fmt.Errorf("decode thresholds %q: %w", path, err)
	}
	return th, nil
}

// DiffItem represents a single metric comparison between baseline and candidate.
type DiffItem struct {
	Scope        string  `json:"scope"`
	Metric       string  `json:"metric"`
	Baseline     float64 `json:"baseline"`
	Candidate    float64 `json:"candidate"`
	Delta        float64 `json:"delta"`
	DeltaPct     float64 `json:"delta_pct"`
	Regressed    bool    `json:"regressed"`
	ThresholdMsg string  `json:"threshold_msg,omitempty"`
}

// ComparisonResult summarizes the diff and pass/fail status between two reports.
type ComparisonResult struct {
	Passed      bool       `json:"passed"`
	Diffs       []DiffItem `json:"diffs"`
	Regressions []string   `json:"regressions,omitempty"`
}

// Compare diffs candidate against baseline and checks all configured Thresholds.
func Compare(base, cand *Report, th Thresholds) ComparisonResult {
	var diffs []DiffItem
	var regressions []string

	if base == nil || cand == nil {
		return ComparisonResult{
			Passed:      false,
			Regressions: []string{"baseline and candidate reports must be non-nil"},
		}
	}

	// 1. Reward rate comparison
	rDelta := cand.RewardRate - base.RewardRate
	var rPct float64
	if base.RewardRate != 0 {
		rPct = (rDelta / base.RewardRate) * 100.0
	}
	rItem := DiffItem{
		Scope:     "run",
		Metric:    "reward_rate",
		Baseline:  base.RewardRate,
		Candidate: cand.RewardRate,
		Delta:     rDelta,
		DeltaPct:  rPct,
	}
	if th.MinRewardDelta != nil && rDelta < *th.MinRewardDelta {
		rItem.Regressed = true
		rItem.ThresholdMsg = fmt.Sprintf("reward_rate delta %.4f < min %.4f", rDelta, *th.MinRewardDelta)
		regressions = append(regressions, rItem.ThresholdMsg)
	}
	diffs = append(diffs, rItem)

	// 1b. Infra fault rate comparison (3-tier fault classification)
	if base.FaultClassification != nil || cand.FaultClassification != nil || th.MaxInfraFaultRate != nil {
		var baseFaultRate, candFaultRate float64
		if base.FaultClassification != nil {
			baseFaultRate = base.FaultClassification.InfraFaultRate
		}
		if cand.FaultClassification != nil {
			candFaultRate = cand.FaultClassification.InfraFaultRate
		}
		fDelta := candFaultRate - baseFaultRate
		var fPct float64
		if baseFaultRate != 0 {
			fPct = (fDelta / baseFaultRate) * 100.0
		}
		fItem := DiffItem{
			Scope:     "run",
			Metric:    "infra_fault_rate",
			Baseline:  baseFaultRate,
			Candidate: candFaultRate,
			Delta:     fDelta,
			DeltaPct:  fPct,
		}
		switch {
		case th.MaxInfraFaultRate == nil:
		case cand.FaultClassification == nil:
			// A candidate without a classification (a report.json from before
			// the field existed) has no rate to hold against the threshold.
			// Treating the missing value as 0 would pass a run with failures,
			// so the threshold fails instead.
			fItem.Regressed = true
			fItem.ThresholdMsg = fmt.Sprintf("infra_fault_rate: candidate has no fault_classification (events predate fault classification); cannot check max %.4f", *th.MaxInfraFaultRate)
			regressions = append(regressions, fItem.ThresholdMsg)
		case candFaultRate >= *th.MaxInfraFaultRate:
			fItem.Regressed = true
			fItem.ThresholdMsg = fmt.Sprintf("infra_fault_rate %.4f >= max %.4f", candFaultRate, *th.MaxInfraFaultRate)
			regressions = append(regressions, fItem.ThresholdMsg)
		}
		diffs = append(diffs, fItem)
	}

	// 2. Operation and trajectory step latency comparisons from Stats
	keys := orderedStatKeys(base.Stats, cand.Stats)
	for _, k := range keys {
		bSt, bOK := base.StatFor(k.event, k.name)
		cSt, cOK := cand.StatFor(k.event, k.name)
		if !bOK || !cOK || bSt.Count == 0 || cSt.Count == 0 {
			continue
		}
		scope := "op:" + k.event
		metricPrefix := k.event
		lookupKey := k.event
		fallbackPct := th.MaxLatencyRegressionPct
		if k.event == EventStep && k.name != "" {
			scope = "step:" + k.name
			metricPrefix = k.name
			lookupKey = k.name
			fallbackPct = 0
		} else if k.name != "" {
			scope = fmt.Sprintf("op:%s/%s", k.event, k.name)
			metricPrefix = k.event + "." + k.name
			lookupKey = k.event
		}

		// p50
		p50Delta := cSt.P50MS - bSt.P50MS
		var p50Pct float64
		if bSt.P50MS != 0 {
			p50Pct = (p50Delta / bSt.P50MS) * 100.0
		}
		p50Item := DiffItem{
			Scope:     scope,
			Metric:    metricPrefix + ".p50_ms",
			Baseline:  bSt.P50MS,
			Candidate: cSt.P50MS,
			Delta:     p50Delta,
			DeltaPct:  p50Pct,
		}
		if maxPct, ok := lookupThresholdPct(th.MaxP50RegressionPct, lookupKey, fallbackPct); ok && p50Pct > maxPct {
			p50Item.Regressed = true
			p50Item.ThresholdMsg = fmt.Sprintf("%s p50 regressed by +%.2f%% > max +%.2f%% (%.1f -> %.1f ms)", metricPrefix, p50Pct, maxPct, bSt.P50MS, cSt.P50MS)
			regressions = append(regressions, p50Item.ThresholdMsg)
		}
		diffs = append(diffs, p50Item)

		// p95
		p95Delta := cSt.P95MS - bSt.P95MS
		var p95Pct float64
		if bSt.P95MS != 0 {
			p95Pct = (p95Delta / bSt.P95MS) * 100.0
		}
		p95Item := DiffItem{
			Scope:     scope,
			Metric:    metricPrefix + ".p95_ms",
			Baseline:  bSt.P95MS,
			Candidate: cSt.P95MS,
			Delta:     p95Delta,
			DeltaPct:  p95Pct,
		}
		if maxPct, ok := lookupThresholdPct(th.MaxP95RegressionPct, lookupKey, fallbackPct); ok && p95Pct > maxPct {
			p95Item.Regressed = true
			p95Item.ThresholdMsg = fmt.Sprintf("%s p95 regressed by +%.2f%% > max +%.2f%% (%.1f -> %.1f ms)", metricPrefix, p95Pct, maxPct, bSt.P95MS, cSt.P95MS)
			regressions = append(regressions, p95Item.ThresholdMsg)
		}
		diffs = append(diffs, p95Item)
	}

	// 3. Per-job summary comparisons (if jobs map is populated on both)
	for _, jobName := range sortedJobNames(base.Jobs, cand.Jobs) {
		bJob, bOK := base.Jobs[jobName]
		cJob, cOK := cand.Jobs[jobName]
		if !bOK || !cOK {
			continue
		}
		for _, mName := range sortedMetricKeys(bJob.Metrics, cJob.Metrics) {
			bM, bmOK := bJob.Metrics[mName]
			cM, cmOK := cJob.Metrics[mName]
			if !bmOK || !cmOK {
				continue
			}
			p50Delta := cM.P50 - bM.P50
			var p50Pct float64
			if bM.P50 != 0 {
				p50Pct = (p50Delta / bM.P50) * 100.0
			}
			item50 := DiffItem{
				Scope:     "job:" + jobName,
				Metric:    mName + ".p50",
				Baseline:  bM.P50,
				Candidate: cM.P50,
				Delta:     p50Delta,
				DeltaPct:  p50Pct,
			}
			if maxPct, ok := lookupThresholdPct(th.MaxP50RegressionPct, mName, th.MaxLatencyRegressionPct); ok && p50Pct > maxPct {
				item50.Regressed = true
				item50.ThresholdMsg = fmt.Sprintf("%s %s p50 regressed by +%.2f%% > max +%.2f%%", jobName, mName, p50Pct, maxPct)
				regressions = append(regressions, item50.ThresholdMsg)
			}
			diffs = append(diffs, item50)

			p95Delta := cM.P95 - bM.P95
			var p95Pct float64
			if bM.P95 != 0 {
				p95Pct = (p95Delta / bM.P95) * 100.0
			}
			item95 := DiffItem{
				Scope:     "job:" + jobName,
				Metric:    mName + ".p95",
				Baseline:  bM.P95,
				Candidate: cM.P95,
				Delta:     p95Delta,
				DeltaPct:  p95Pct,
			}
			if maxPct, ok := lookupThresholdPct(th.MaxP95RegressionPct, mName, th.MaxLatencyRegressionPct); ok && p95Pct > maxPct {
				item95.Regressed = true
				item95.ThresholdMsg = fmt.Sprintf("%s %s p95 regressed by +%.2f%% > max +%.2f%%", jobName, mName, p95Pct, maxPct)
				regressions = append(regressions, item95.ThresholdMsg)
			}
			diffs = append(diffs, item95)
		}
	}

	// 4. Custom metrics comparisons
	for _, cmName := range unionCustomMetricKeys(base.CustomMetrics, cand.CustomMetrics, th) {
		bM := base.CustomMetrics[cmName]
		cM, cOK := cand.CustomMetrics[cmName]
		if !cOK {
			continue
		}
		delta := cM.P95 - bM.P95
		var pct float64
		if bM.P95 != 0 {
			pct = (delta / bM.P95) * 100.0
		}
		item := DiffItem{
			Scope:     "custom_metrics",
			Metric:    cmName + ".p95",
			Baseline:  bM.P95,
			Candidate: cM.P95,
			Delta:     delta,
			DeltaPct:  pct,
		}
		var msgs []string
		if maxVal, ok := th.CustomMetricMaxP95[cmName]; ok && cM.P95 > maxVal {
			msgs = append(msgs, fmt.Sprintf("custom_metric %s p95 %.2f > max %.2f", cmName, cM.P95, maxVal))
		}
		if bound, ok := th.CustomMetrics[cmName]; ok {
			if bound.MaxP50 != nil && cM.P50 > *bound.MaxP50 {
				msgs = append(msgs, fmt.Sprintf("custom_metric %s p50 %.2f > max_p50 %.2f", cmName, cM.P50, *bound.MaxP50))
			}
			if bound.MinP50 != nil && cM.P50 < *bound.MinP50 {
				msgs = append(msgs, fmt.Sprintf("custom_metric %s p50 %.2f < min_p50 %.2f", cmName, cM.P50, *bound.MinP50))
			}
			if bound.MaxP95 != nil && cM.P95 > *bound.MaxP95 {
				msgs = append(msgs, fmt.Sprintf("custom_metric %s p95 %.2f > max_p95 %.2f", cmName, cM.P95, *bound.MaxP95))
			}
			if bound.MaxMax != nil && cM.Max > *bound.MaxMax {
				msgs = append(msgs, fmt.Sprintf("custom_metric %s max %.2f > max_max %.2f", cmName, cM.Max, *bound.MaxMax))
			}
			if bound.MaxRegressionPct != nil && bM.P95 > 0 && pct > *bound.MaxRegressionPct {
				msgs = append(msgs, fmt.Sprintf("custom_metric %s p95 regressed by +%.2f%% > max +%.2f%%", cmName, pct, *bound.MaxRegressionPct))
			}
		}
		if len(msgs) > 0 {
			item.Regressed = true
			item.ThresholdMsg = strings.Join(msgs, "; ")
			regressions = append(regressions, msgs...)
		}
		diffs = append(diffs, item)
	}

	return ComparisonResult{
		Passed:      len(regressions) == 0,
		Diffs:       diffs,
		Regressions: regressions,
	}
}

// CompareReports is an alias for Compare.
func CompareReports(base, cand *Report, th Thresholds) ComparisonResult {
	return Compare(base, cand, th)
}

// CompareReportFiles loads baseline and candidate reports (from JSON or JSONL) and optional
// thresholds from thresholdsPath, and runs Compare.
func CompareReportFiles(basePath, candPath, thresholdsPath string) (ComparisonResult, error) {
	baseRep, err := LoadOrSummarizeReport(basePath)
	if err != nil {
		return ComparisonResult{}, fmt.Errorf("load baseline %q: %w", basePath, err)
	}
	candRep, err := LoadOrSummarizeReport(candPath)
	if err != nil {
		return ComparisonResult{}, fmt.Errorf("load candidate %q: %w", candPath, err)
	}
	var th Thresholds
	if thresholdsPath != "" {
		th, err = LoadThresholds(thresholdsPath)
		if err != nil {
			return ComparisonResult{}, err
		}
	}
	return Compare(baseRep, candRep, th), nil
}

// FormatComparisonMarkdown renders a side-by-side Markdown diff table for res.
func FormatComparisonMarkdown(res ComparisonResult) string {
	var b strings.Builder
	b.WriteString("# Benchmark Comparison\n\n")
	if res.Passed {
		b.WriteString("**Status**: PASS\n\n")
	} else {
		b.WriteString("**Status**: FAIL\n\n")
		for _, r := range res.Regressions {
			fmt.Fprintf(&b, "- %s\n", r)
		}
		b.WriteString("\n")
	}

	b.WriteString("| Scope | Metric | Baseline | Candidate | Delta | Delta % | Status |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---|\n")
	for _, d := range res.Diffs {
		status := "OK"
		if d.Regressed {
			status = "REGRESSED"
		}
		fmt.Fprintf(&b, "| %s | %s | %.2f | %.2f | %+.2f | %+.2f%% | %s |\n",
			d.Scope, d.Metric, d.Baseline, d.Candidate, d.Delta, d.DeltaPct, status)
	}
	return b.String()
}

func lookupThresholdPct(m map[string]float64, name string, fallback float64) (float64, bool) {
	if m != nil {
		if v, ok := m[name]; ok {
			return v, true
		}
		trimmed := strings.TrimSuffix(name, "_ms")
		if v, ok := m[trimmed]; ok {
			return v, true
		}
		if v, ok := m[trimmed+"_ms"]; ok {
			return v, true
		}
	}
	if fallback > 0 {
		return fallback, true
	}
	return 0, false
}

func orderedStatKeys(a, b []Stat) []statKey {
	seen := map[statKey]bool{}
	var out []statKey
	for _, s := range a {
		k := statKey{s.Event, s.Name}
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, s := range b {
		k := statKey{s.Event, s.Name}
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

func sortedJobNames(a, b map[string]schema.JobReportSummary) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedMetricKeys(a, b map[string]schema.MetricSummary) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func unionCustomMetricKeys(a, b map[string]schema.MetricSummary, th Thresholds) []string {
	seen := map[string]bool{}
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	for k := range th.CustomMetricMaxP95 {
		seen[k] = true
	}
	for k := range th.CustomMetrics {
		seen[k] = true
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

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

package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// TelemetryScope identifies one subsystem perspective for telemetry collection and reporting.
type TelemetryScope string

const (
	// ScopeRL captures RL trainer step timings, rewards, hold-out validation scores,
	// policy stability (entropy/KL), throughput, convergence, and llm_think_ms vs env_wait_ms decomposition.
	ScopeRL TelemetryScope = "rl"
	// ScopeUserSandbox captures the sandbox system from the user/agent/SDK perspective
	// (create, ttfi and its sub-phases, wake, agent_exec, file I/O, suspend, delete, working set, acquisition rate).
	ScopeUserSandbox TelemetryScope = "user-sandbox"
	// ScopeSubstrate captures the sandbox system from the Substrate perspective
	// (ate-api active/parked actors, template footprint, atelet node image-cache deltas, restore sub-phases).
	ScopeSubstrate TelemetryScope = "substrate"
	// ScopeK8s captures Kubernetes cluster health, node/pod bin-packing, metrics.k8s.io
	// actual CPU/WSS utilization, pod startup sub-phases, and OOMKills/evictions/restarts.
	// Broken out from ScopeSubstrate so it applies to any K8s sandbox runtime.
	ScopeK8s TelemetryScope = "k8s"
	// ScopeAll enables all four canonical telemetry scopes.
	ScopeAll TelemetryScope = "all"
)

// DefaultTelemetryScopes returns all four canonical telemetry scopes in deterministic order.
func DefaultTelemetryScopes() []TelemetryScope {
	return []TelemetryScope{ScopeRL, ScopeUserSandbox, ScopeSubstrate, ScopeK8s}
}

// NormalizeTelemetryScope maps aliases to canonical TelemetryScope values.
func NormalizeTelemetryScope(raw string) (TelemetryScope, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "rl", "trainer", "rl-trainer":
		return ScopeRL, nil
	case "user-sandbox", "user", "client", "client-sandbox":
		return ScopeUserSandbox, nil
	case "substrate", "substrate-sandbox":
		return ScopeSubstrate, nil
	case "k8s", "k8s-health", "cluster", "kubernetes":
		return ScopeK8s, nil
	case "all", "*":
		return ScopeAll, nil
	default:
		return "", fmt.Errorf("unknown telemetry scope %q (valid: rl, user-sandbox, substrate, k8s, all)", raw)
	}
}

// ParseTelemetryScopes parses comma-separated or slice scope strings into deduplicated canonical scopes.
// Empty input or "all" expands to DefaultTelemetryScopes().
func ParseTelemetryScopes(inputs ...string) ([]TelemetryScope, error) {
	seen := make(map[TelemetryScope]bool, 4)
	for _, in := range inputs {
		for _, part := range strings.Split(in, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			sc, err := NormalizeTelemetryScope(part)
			if err != nil {
				return nil, err
			}
			if sc == ScopeAll {
				return DefaultTelemetryScopes(), nil
			}
			seen[sc] = true
		}
	}
	if len(seen) == 0 {
		return DefaultTelemetryScopes(), nil
	}
	var out []TelemetryScope
	for _, sc := range DefaultTelemetryScopes() {
		if seen[sc] {
			out = append(out, sc)
		}
	}
	return out, nil
}

// HasTelemetryScope reports whether target is enabled in active. An empty active slice enables all scopes.
func HasTelemetryScope(active []TelemetryScope, target TelemetryScope) bool {
	if len(active) == 0 {
		return true
	}
	normTarget, err := NormalizeTelemetryScope(string(target))
	if err != nil {
		normTarget = target
	}
	for _, a := range active {
		normA, err := NormalizeTelemetryScope(string(a))
		if err != nil {
			normA = a
		}
		if normA == ScopeAll || normA == normTarget {
			return true
		}
	}
	return false
}

// TelemetryPluginKind identifies a declarative feature-debug telemetry plugin type.
type TelemetryPluginKind string

const (
	// PluginKindPromQuery executes an instant or range Prometheus query into custom_metrics.
	PluginKindPromQuery TelemetryPluginKind = "prom_query"
	// PluginKindK8sLogs harvests pod logs matching a selector into <dest>/plugins/<name>.log.
	PluginKindK8sLogs TelemetryPluginKind = "k8s_logs"
	// PluginKindRegexMetric extracts numeric values from log/JSONL files matching a glob + regex.
	PluginKindRegexMetric TelemetryPluginKind = "regex_metric"
	// PluginKindCommand runs a diagnostic shell/CLI command and captures stdout/metrics.
	PluginKindCommand TelemetryPluginKind = "command"
	// PluginKindCustom invokes a registered Go Collector or TelemetryIngester by name.
	PluginKindCustom TelemetryPluginKind = "custom"
)

// TelemetryPluginSpec configures an ad-hoc metric or log collector for feature debugging.
type TelemetryPluginSpec struct {
	Name          string              `json:"name"`
	Kind          TelemetryPluginKind `json:"kind"`
	Scope         TelemetryScope      `json:"scope,omitempty"`
	Expr          string              `json:"expr,omitempty"`
	Namespace     string              `json:"namespace,omitempty"`
	LabelSelector string              `json:"label_selector,omitempty"`
	Container     string              `json:"container,omitempty"`
	Grep          string              `json:"grep,omitempty"`
	Glob          string              `json:"glob,omitempty"`
	Regex         string              `json:"regex,omitempty"`
	Command       []string            `json:"command,omitempty"`
	Unit          string              `json:"unit,omitempty"`
}

// Validate checks TelemetryPluginSpec invariants.
func (p TelemetryPluginSpec) Validate() error {
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("telemetry_plugin: name is required")
	}
	if p.Scope != "" {
		if _, err := NormalizeTelemetryScope(string(p.Scope)); err != nil {
			return fmt.Errorf("telemetry_plugin[%q]: %w", p.Name, err)
		}
	}
	switch p.Kind {
	case PluginKindPromQuery:
		if strings.TrimSpace(p.Expr) == "" {
			return fmt.Errorf("telemetry_plugin[%q]: expr is required for prom_query", p.Name)
		}
	case PluginKindK8sLogs:
		if strings.TrimSpace(p.LabelSelector) == "" {
			return fmt.Errorf("telemetry_plugin[%q]: label_selector is required for k8s_logs", p.Name)
		}
	case PluginKindRegexMetric:
		if strings.TrimSpace(p.Glob) == "" || strings.TrimSpace(p.Regex) == "" {
			return fmt.Errorf("telemetry_plugin[%q]: glob and regex are required for regex_metric", p.Name)
		}
	case PluginKindCommand:
		if len(p.Command) == 0 {
			return fmt.Errorf("telemetry_plugin[%q]: command is required for command plugin", p.Name)
		}
	case PluginKindCustom:
	default:
		return fmt.Errorf("telemetry_plugin[%q]: unsupported kind %q", p.Name, p.Kind)
	}
	return nil
}

// TelemetrySpec configures which telemetry scopes, built-in collectors, and feature plugins run.
type TelemetrySpec struct {
	Scopes     []TelemetryScope      `json:"scopes,omitempty"`
	Collectors []string              `json:"collectors,omitempty"`
	Plugins    []TelemetryPluginSpec `json:"plugins,omitempty"`
}

// Validate checks TelemetrySpec invariants.
func (t TelemetrySpec) Validate() error {
	for _, s := range t.Scopes {
		if _, err := NormalizeTelemetryScope(string(s)); err != nil {
			return fmt.Errorf("telemetry: %w", err)
		}
	}
	seen := make(map[string]bool, len(t.Plugins))
	for _, p := range t.Plugins {
		if err := p.Validate(); err != nil {
			return err
		}
		if seen[p.Name] {
			return fmt.Errorf("telemetry: duplicate plugin name %q", p.Name)
		}
		seen[p.Name] = true
	}
	return nil
}

// MetricSummary holds summary statistics for a metric across the run window.
type MetricSummary struct {
	Count int     `json:"count"`
	Sum   float64 `json:"sum,omitempty"`
	Min   float64 `json:"min,omitempty"`
	Max   float64 `json:"max,omitempty"`
	P50   float64 `json:"p50,omitempty"`
	P90   float64 `json:"p90,omitempty"`
	P95   float64 `json:"p95,omitempty"`
	P99   float64 `json:"p99,omitempty"`
	Unit  string  `json:"unit,omitempty"`
}

// MarshalJSON emits explicit 0 values for sum, min, max, p50, p90, p95, and p99
// whenever Count > 0 (DESIGN §7.3: Zero vs Missing), and omits them when Count <= 0.
func (m MetricSummary) MarshalJSON() ([]byte, error) {
	if m.Count <= 0 {
		type emptyMetricSummary struct {
			Count int    `json:"count"`
			Unit  string `json:"unit,omitempty"`
		}
		return json.Marshal(emptyMetricSummary{
			Count: m.Count,
			Unit:  m.Unit,
		})
	}
	type fullMetricSummary struct {
		Count int     `json:"count"`
		Sum   float64 `json:"sum"`
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
		P50   float64 `json:"p50"`
		P90   float64 `json:"p90"`
		P95   float64 `json:"p95"`
		P99   float64 `json:"p99"`
		Unit  string  `json:"unit,omitempty"`
	}
	return json.Marshal(fullMetricSummary{
		Count: m.Count,
		Sum:   m.Sum,
		Min:   m.Min,
		Max:   m.Max,
		P50:   m.P50,
		P90:   m.P90,
		P95:   m.P95,
		P99:   m.P99,
		Unit:  m.Unit,
	})
}

// Validate checks MetricSummary invariants.
func (m MetricSummary) Validate(name string) error {
	if m.Count < 0 {
		return fmt.Errorf("metric_summary[%q]: count must be >= 0", name)
	}
	for _, v := range []float64{m.Sum, m.Min, m.Max, m.P50, m.P90, m.P95, m.P99} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("metric_summary[%q]: values must be finite", name)
		}
	}
	return nil
}

// FaultTier classifies an error or retry into one of three reliability
// tiers (docs/glossary.md, "Benchmark reporting").
type FaultTier string

const (
	// FaultTierBeforeEpisode covers infrastructure faults before the episode runs
	// (create timeout, no capacity, image pull/unpack failure, transport error).
	FaultTierBeforeEpisode FaultTier = "tier1_before_episode"
	// FaultTierPolicy covers agent/workload policy outcomes during the episode
	// (non-zero exit, command timeout, in-guest OOM). Excluded from infra_fault_rate.
	FaultTierPolicy FaultTier = "tier2_policy"
	// FaultTierDuringEpisode covers infrastructure faults during the episode
	// (wake/ResumeActor failure, sandbox crash, snapshot/restore failure, eviction/preemption).
	FaultTierDuringEpisode FaultTier = "tier3_during_episode"

	// DefaultInfraFaultRateTarget is the reliability target: under 2% of the
	// rollouts dropped by an infrastructure fault (docs/glossary.md).
	DefaultInfraFaultRateTarget = 0.02
)

// Canonical fault categories within each FaultTier. The list is closed: the
// JSON schema's enum and Validate reject any other string, so a new kind of
// error shows up as FaultCatOther with its example_err instead of being
// absorbed into a named category.
const (
	// Tier 1: before the episode (infra).
	FaultCatCreateTimeout   = "create_timeout"
	FaultCatNoCapacity      = "no_capacity"
	FaultCatImagePullUnpack = "image_pull_unpack"
	FaultCatTransportError  = "transport_error"

	// Tier 2: during the episode (policy, excluded from infra_fault_rate).
	FaultCatNonZeroExit    = "nonzero_exit"
	FaultCatCommandTimeout = "command_timeout"
	FaultCatGuestOOM       = "guest_oom"

	// Tier 3: during the episode (infra).
	FaultCatWakeResumeFailure      = "wake_resume_failure"
	FaultCatSandboxCrash           = "sandbox_crash"
	FaultCatSnapshotRestoreFailure = "snapshot_restore_failure"
	FaultCatEvictionPreemption     = "eviction_preemption"

	// FaultCatOther is the infrastructure error no matcher of its tier
	// (tier 1 or tier 3) recognized. It counts in infra_dropped; its
	// example_err is what the taxonomy grows from.
	FaultCatOther = "other"
)

// knownFaultCategories is the closed set Validate accepts.
var knownFaultCategories = map[string]bool{
	FaultCatCreateTimeout:          true,
	FaultCatNoCapacity:             true,
	FaultCatImagePullUnpack:        true,
	FaultCatTransportError:         true,
	FaultCatNonZeroExit:            true,
	FaultCatCommandTimeout:         true,
	FaultCatGuestOOM:               true,
	FaultCatWakeResumeFailure:      true,
	FaultCatSandboxCrash:           true,
	FaultCatSnapshotRestoreFailure: true,
	FaultCatEvictionPreemption:     true,
	FaultCatOther:                  true,
}

// FaultCategorySummary aggregates dropped rollouts, recovered retries, raw failed
// events, and one representative error string for a (Tier, Category) bucket.
type FaultCategorySummary struct {
	Tier       FaultTier `json:"tier"`
	Category   string    `json:"category"`
	Dropped    int       `json:"dropped"`
	Recovered  int       `json:"recovered"`
	Events     int       `json:"events"`
	ExampleErr string    `json:"example_err,omitempty"`
}

// FaultClassification summarizes rollout-level reliability in the three
// fault tiers against the infra_fault_rate target (DefaultInfraFaultRateTarget).
// MeetsTarget is InfraFaultRate < TargetRate, and false when AllRollouts is 0.
type FaultClassification struct {
	AllRollouts       int                    `json:"all_rollouts"`
	InfraDropped      int                    `json:"infra_dropped"`
	Tier1Dropped      int                    `json:"tier1_dropped"`
	Tier2Policy       int                    `json:"tier2_policy"`
	Tier3Dropped      int                    `json:"tier3_dropped"`
	RecoveredRetries  int                    `json:"recovered_retries"`
	PostRolloutErrors int                    `json:"post_rollout_errors,omitempty"`
	InfraFaultRate    float64                `json:"infra_fault_rate"`
	TargetRate        float64                `json:"target_rate"`
	MeetsTarget       bool                   `json:"meets_target"`
	Categories        []FaultCategorySummary `json:"categories,omitempty"`
}

// Validate checks FaultClassification invariants.
func (fc *FaultClassification) Validate() error {
	if fc == nil {
		return nil
	}
	if fc.AllRollouts < 0 || fc.InfraDropped < 0 || fc.Tier1Dropped < 0 || fc.Tier2Policy < 0 || fc.Tier3Dropped < 0 || fc.RecoveredRetries < 0 || fc.PostRolloutErrors < 0 {
		return errors.New("fault_classification: counts must be >= 0")
	}
	if fc.InfraDropped != fc.Tier1Dropped+fc.Tier3Dropped {
		return fmt.Errorf("fault_classification: infra_dropped (%d) != tier1_dropped (%d) + tier3_dropped (%d)", fc.InfraDropped, fc.Tier1Dropped, fc.Tier3Dropped)
	}
	if drops := fc.Tier1Dropped + fc.Tier2Policy + fc.Tier3Dropped; drops > fc.AllRollouts {
		return fmt.Errorf("fault_classification: tier1_dropped + tier2_policy + tier3_dropped (%d) > all_rollouts (%d)", drops, fc.AllRollouts)
	}
	if fc.InfraFaultRate < 0 || math.IsNaN(fc.InfraFaultRate) || math.IsInf(fc.InfraFaultRate, 0) {
		return errors.New("fault_classification: infra_fault_rate must be finite and >= 0")
	}
	if fc.TargetRate <= 0 || math.IsNaN(fc.TargetRate) || math.IsInf(fc.TargetRate, 0) {
		return errors.New("fault_classification: target_rate must be finite and > 0")
	}
	for i, c := range fc.Categories {
		switch c.Tier {
		case FaultTierBeforeEpisode, FaultTierPolicy, FaultTierDuringEpisode:
		default:
			return fmt.Errorf("fault_classification: categories[%d].tier %q is invalid", i, c.Tier)
		}
		if strings.TrimSpace(c.Category) == "" {
			return fmt.Errorf("fault_classification: categories[%d].category is required", i)
		}
		if !knownFaultCategories[c.Category] {
			return fmt.Errorf("fault_classification: categories[%d].category %q is not a known category", i, c.Category)
		}
		if c.Dropped < 0 || c.Recovered < 0 || c.Events < 0 {
			return fmt.Errorf("fault_classification: categories[%d] counts must be >= 0", i)
		}
	}
	return nil
}

// Report is the top-level report.json artifact.
type Report struct {
	SchemaVersion       string                      `json:"schema_version"`
	RunID               string                      `json:"run_id"`
	ScenarioName        string                      `json:"scenario_name"`
	Tag                 string                      `json:"tag,omitempty"`
	GeneratedAt         string                      `json:"generated_at"`
	SystemDescription   *SystemDescription          `json:"system_description,omitempty"`
	MeasurementsRef     string                      `json:"measurements_ref,omitempty"`
	Validity            ReportValidity              `json:"validity"`
	FaultClassification *FaultClassification        `json:"fault_classification,omitempty"`
	Jobs                map[string]JobReportSummary `json:"jobs,omitempty"`
	CustomMetrics       map[string]MetricSummary    `json:"custom_metrics,omitempty"`
}

// ReportValidity records whether the run passed all validity gates and why.
// Evidence names the inputs a rule held on (for Rule 1: the pre-flight
// coverage, the prewarm events, the node cache delta), so a reader knows
// which footing evidence the verdict rests on.
type ReportValidity struct {
	Valid    bool     `json:"valid"`
	Reasons  []string `json:"reasons,omitempty"`
	Evidence []string `json:"evidence,omitempty"`
}

// JobReportSummary summarizes per-job results in Report.
type JobReportSummary struct {
	Tasks               int                      `json:"tasks"`
	Failed              int                      `json:"failed"`
	Rewarded            int                      `json:"rewarded"`
	RewardRate          float64                  `json:"reward_rate"`
	Steps               int                      `json:"steps,omitempty"`
	FaultClassification *FaultClassification     `json:"fault_classification,omitempty"`
	Metrics             map[string]MetricSummary `json:"metrics,omitempty"`
}

// Validate checks Report invariants.
func (r *Report) Validate() error {
	if err := checkSchemaVersion(r.SchemaVersion); err != nil {
		return fmt.Errorf("report: %w", err)
	}
	if strings.TrimSpace(r.RunID) == "" {
		return errors.New("report: run_id is required")
	}
	if strings.TrimSpace(r.ScenarioName) == "" {
		return errors.New("report: scenario_name is required")
	}
	if err := requireRFC3339("report.generated_at", r.GeneratedAt); err != nil {
		return err
	}
	if !r.Validity.Valid && len(r.Validity.Reasons) == 0 {
		return errors.New("report: validity.reasons must not be empty when valid is false")
	}
	if r.SystemDescription != nil {
		if err := r.SystemDescription.Validate(); err != nil {
			return fmt.Errorf("report.system_description: %w", err)
		}
	}
	if err := r.FaultClassification.Validate(); err != nil {
		return fmt.Errorf("report: %w", err)
	}
	for job, js := range r.Jobs {
		if js.Tasks < 0 || js.Failed < 0 || js.Rewarded < 0 || js.Steps < 0 {
			return fmt.Errorf("report: jobs[%q] counts must be >= 0", job)
		}
		if js.RewardRate < 0 {
			return fmt.Errorf("report: jobs[%q].reward_rate must be >= 0", job)
		}
		if err := js.FaultClassification.Validate(); err != nil {
			return fmt.Errorf("report: jobs[%q]: %w", job, err)
		}
		for k, ms := range js.Metrics {
			if err := ms.Validate(k); err != nil {
				return fmt.Errorf("report: jobs[%q]: %w", job, err)
			}
		}
	}
	for k, ms := range r.CustomMetrics {
		if err := ms.Validate(k); err != nil {
			return fmt.Errorf("report: custom_metrics: %w", err)
		}
	}
	return nil
}

// StatsRow is a single flat JSONL record in stats.jsonl for dashboard ingestion.
type StatsRow struct {
	SchemaVersion string             `json:"schema_version"`
	Timestamp     string             `json:"timestamp"`
	Tag           string             `json:"tag,omitempty"`
	TestName      string             `json:"test_name"`
	RunID         string             `json:"run_id,omitempty"`
	Job           string             `json:"job,omitempty"`
	Metric        string             `json:"metric"`
	Measurements  map[string]float64 `json:"measurements"`
	Labels        map[string]string  `json:"labels,omitempty"`
}

// Validate checks StatsRow invariants.
func (r *StatsRow) Validate() error {
	if err := checkSchemaVersion(r.SchemaVersion); err != nil {
		return fmt.Errorf("stats_row: %w", err)
	}
	if err := requireRFC3339("stats_row.timestamp", r.Timestamp); err != nil {
		return err
	}
	if strings.TrimSpace(r.TestName) == "" {
		return errors.New("stats_row: test_name is required")
	}
	if strings.TrimSpace(r.Metric) == "" {
		return errors.New("stats_row: metric is required")
	}
	if len(r.Measurements) == 0 {
		return errors.New("stats_row: measurements must not be empty")
	}
	for k, v := range r.Measurements {
		if strings.TrimSpace(k) == "" {
			return errors.New("stats_row: measurement key must not be empty")
		}
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("stats_row: measurements[%q] must be finite", k)
		}
	}
	return nil
}

// SystemDescription captures the cluster, SUT, and provenance metadata for a run.
type SystemDescription struct {
	SchemaVersion             string                  `json:"schema_version"`
	Cluster                   string                  `json:"cluster"`
	Backend                   string                  `json:"backend,omitempty"`
	Access                    string                  `json:"access,omitempty"`
	KubernetesVersion         string                  `json:"kubernetes_version,omitempty"`
	OSKernel                  string                  `json:"os_kernel,omitempty"`
	StorageDriverAndFS        string                  `json:"storage_driver_and_fs,omitempty"`
	SDKVersion                string                  `json:"sdk_version,omitempty"`
	SubstrateVersion          string                  `json:"substrate_version,omitempty"`
	GraderVersion             string                  `json:"grader_version,omitempty"`
	DatasetRevision           string                  `json:"dataset_revision,omitempty"`
	SWEBenchHarnessVersion    string                  `json:"swebench_harness_version,omitempty"`
	ExclusionListRevision     string                  `json:"exclusion_list_revision,omitempty"`
	NodePools                 map[string]int          `json:"node_pools,omitempty"`
	MachineTypes              map[string]NodePoolInfo `json:"machine_types,omitempty"`
	ZoneTopology              string                  `json:"zone_topology,omitempty"`
	BucketRegion              string                  `json:"bucket_region,omitempty"`
	RuntimeClass              string                  `json:"runtime_class,omitempty"`
	SandboxFootprint          *SandboxFootprint       `json:"sandbox_footprint,omitempty"`
	MaxActivePods             int                     `json:"max_active_pods,omitempty"`
	PoolSizePerImage          int                     `json:"pool_size_per_image,omitempty"`
	WorkerPodCount            int                     `json:"worker_pod_count,omitempty"`
	WorkerActorCapacity       int                     `json:"worker_actor_capacity,omitempty"`
	MaxConcurrentSandboxesCap int                     `json:"max_concurrent_sandboxes_cap,omitempty"`
	APIImage                  string                  `json:"api_image,omitempty"`
	GuestImage                string                  `json:"guest_image,omitempty"`
	GitSHA                    string                  `json:"git_sha,omitempty"`
	DatasetDigest             string                  `json:"dataset_digest,omitempty"`
	ScenarioHash              string                  `json:"scenario_hash,omitempty"`
	Overlays                  []OverlayRef            `json:"overlays,omitempty"`
	Load                      *LoadParams             `json:"load,omitempty"`
	LifecyclePolicy           *SandboxLifecyclePolicy `json:"lifecycle_policy,omitempty"`
}

// SandboxLifecyclePolicy records the configured and observed sandbox warming,
// parking, and trainer warmup policy so baseline (ephemeral) and warm-pool
// runs are self-documenting in system_description.json and report.json.
type SandboxLifecyclePolicy struct {
	Mode                 string `json:"mode,omitempty"`
	ParkMode             string `json:"park_mode,omitempty"`
	IdleAutoParkEnabled  bool   `json:"idle_auto_park_enabled"`
	IdleAutoParkTimeoutS int    `json:"idle_auto_park_timeout_s,omitempty"`
	PrewarmAtStart       bool   `json:"prewarm_at_start"`
	PrewarmCount         int    `json:"prewarm_count,omitempty"`
	PerStepWarming       string `json:"per_step_warming,omitempty"`
	TrainerWarmupBatches int    `json:"trainer_warmup_batches,omitempty"`
}

// NodePoolInfo describes a Kubernetes node pool's machine type and size.
type NodePoolInfo struct {
	MachineType string `json:"machine_type"`
	Count       int    `json:"count"`
	Zone        string `json:"zone,omitempty"`
}

// SandboxFootprint describes the requested resource footprint per sandbox.
type SandboxFootprint struct {
	CPU       float64 `json:"cpu"`
	MemoryGiB float64 `json:"memory_gib"`
	DiskGiB   float64 `json:"disk_gib,omitempty"`
}

// OverlayRef records an applied FeatureOverlay, its hash, and the location and
// hash/commit of the baseline recipe it was built to patch on.
type OverlayRef struct {
	Name             string            `json:"name"`
	Path             string            `json:"path,omitempty"`
	Hash             string            `json:"hash,omitempty"`
	BaseRecipePath   string            `json:"base_recipe_path,omitempty"`
	BaseRecipeHash   string            `json:"base_recipe_hash,omitempty"`
	BaseRecipeCommit string            `json:"base_recipe_commit,omitempty"`
	PatchFile        string            `json:"patch_file,omitempty"`
	BaseFileHashes   map[string]string `json:"base_file_hashes,omitempty"`
	SUTOverrides     map[string]string `json:"sut_overrides,omitempty"`
}

// OverlayManifest is the standalone overlay/overlay.json metadata file stored
// inside a benchmark bundle's overlay/ directory.
type OverlayManifest struct {
	SchemaVersion    string            `json:"schema_version"`
	Name             string            `json:"name"`
	Path             string            `json:"path,omitempty"`
	Hash             string            `json:"hash"`
	BaseRecipePath   string            `json:"base_recipe_path"`
	BaseRecipeHash   string            `json:"base_recipe_hash"`
	BaseRecipeCommit string            `json:"base_recipe_commit,omitempty"`
	PatchFile        string            `json:"patch_file,omitempty"`
	BaseFileHashes   map[string]string `json:"base_file_hashes,omitempty"`
	EnvOverrides     map[string]string `json:"env_overrides,omitempty"`
	Description      string            `json:"description,omitempty"`
}

// Validate checks OverlayManifest invariants.
func (m *OverlayManifest) Validate() error {
	if err := checkSchemaVersion(m.SchemaVersion); err != nil {
		return fmt.Errorf("overlay_manifest: %w", err)
	}
	if strings.TrimSpace(m.Name) == "" {
		return errors.New("overlay_manifest: name is required")
	}
	if strings.TrimSpace(m.Hash) == "" {
		return errors.New("overlay_manifest: hash is required")
	}
	if strings.TrimSpace(m.BaseRecipePath) == "" {
		return errors.New("overlay_manifest: base_recipe_path is required")
	}
	if strings.TrimSpace(m.BaseRecipeHash) == "" {
		return errors.New("overlay_manifest: base_recipe_hash is required")
	}
	return nil
}

// LoadParams records the effective load parameters for the run.
type LoadParams struct {
	Tasks               int `json:"tasks,omitempty"`
	GroupSize           int `json:"group_size,omitempty"`
	Steps               int `json:"steps,omitempty"`
	TasksPerStep        int `json:"tasks_per_step,omitempty"`
	TrajectoriesPerStep int `json:"trajectories_per_step,omitempty"`
	Repeat              int `json:"repeat,omitempty"`
	MaxConcurrent       int `json:"max_concurrent,omitempty"`
	DistinctImages      int `json:"distinct_images,omitempty"`
}

// Validate checks SystemDescription invariants.
func (d *SystemDescription) Validate() error {
	if err := checkSchemaVersion(d.SchemaVersion); err != nil {
		return fmt.Errorf("system_description: %w", err)
	}
	if strings.TrimSpace(d.Cluster) == "" {
		return errors.New("system_description: cluster is required")
	}
	switch d.Backend {
	case "", "substrate", "agent-sandbox":
	default:
		return fmt.Errorf("system_description: invalid backend %q", d.Backend)
	}
	switch d.Access {
	case "", "sdk", "ate-env-api", "direct":
	default:
		return fmt.Errorf("system_description: invalid access %q", d.Access)
	}
	for k, c := range d.NodePools {
		if c < 0 {
			return fmt.Errorf("system_description: node_pools[%q] must be >= 0", k)
		}
	}
	for k, np := range d.MachineTypes {
		if strings.TrimSpace(np.MachineType) == "" {
			return fmt.Errorf("system_description: machine_types[%q].machine_type is required", k)
		}
		if np.Count < 0 {
			return fmt.Errorf("system_description: machine_types[%q].count must be >= 0", k)
		}
	}
	if d.SandboxFootprint != nil {
		if d.SandboxFootprint.CPU < 0 || d.SandboxFootprint.MemoryGiB < 0 || d.SandboxFootprint.DiskGiB < 0 {
			return errors.New("system_description: sandbox_footprint values must be >= 0")
		}
	}
	for i, o := range d.Overlays {
		if strings.TrimSpace(o.Name) == "" {
			return fmt.Errorf("system_description: overlays[%d].name is required", i)
		}
	}
	if d.LifecyclePolicy != nil {
		if d.LifecyclePolicy.IdleAutoParkTimeoutS < 0 || d.LifecyclePolicy.PrewarmCount < 0 || d.LifecyclePolicy.TrainerWarmupBatches < 0 {
			return errors.New("system_description: lifecycle_policy counts/timeouts must be >= 0")
		}
	}
	return nil
}

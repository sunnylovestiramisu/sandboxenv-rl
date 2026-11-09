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
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

// Stat summarizes one kind of event.
type Stat struct {
	Event  string  `json:"event"`
	Name   string  `json:"name,omitempty"`
	Count  int     `json:"count"`
	Failed int     `json:"failed"`
	P50MS  float64 `json:"p50_ms"`
	P90MS  float64 `json:"p90_ms,omitempty"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms,omitempty"`
	MaxMS  float64 `json:"max_ms"`
	// Retried counts ttfi and wake events that took more than one attempt:
	// the environment was not serving when first asked.
	Retried       int     `json:"retried,omitempty"`
	SizeMBP50     float64 `json:"size_mb_p50,omitempty"`
	SizeMBP95     float64 `json:"size_mb_p95,omitempty"`
	SizeMBMax     float64 `json:"size_mb_max,omitempty"`
	CheckpointMBS float64 `json:"checkpoint_mb_s,omitempty"`
}

// Unresolved is an instance whose reward step scored 0, with the grader's
// verdict when the step's output ended in one.
type Unresolved struct {
	Instance string `json:"instance"`
	Reason   string `json:"reason"`
}

// StepStat is one training step in steps mode.
type StepStat struct {
	Step     int `json:"step"`
	Tasks    int `json:"tasks"`
	Failed   int `json:"failed"`
	Rewarded int `json:"rewarded"`
	// Live is how many environments were still in the fleet at this step;
	// steps mode drops an environment after its first failure, so a
	// shrinking Live makes later steps' counts incomparable.
	Live      int     `json:"live"`
	BarrierMS float64 `json:"barrier_ms"`
	ParkMS    float64 `json:"park_ms"`
	WakeP50MS float64 `json:"wake_p50_ms"`
	WakeP95MS float64 `json:"wake_p95_ms"`

	WorkingSetHits       int     `json:"working_set_hits"`
	WorkingSetMisses     int     `json:"working_set_misses"`
	WorkingSetHitRate    float64 `json:"working_set_hit_rate"`
	StandingPoolSize     int     `json:"standing_pool_size,omitempty"`
	ActiveSandboxes      int     `json:"active_sandboxes"`
	PausedSandboxes      int     `json:"paused_sandboxes,omitempty"`
	Created              int     `json:"created"`
	Reused               int     `json:"reused"`
	Retired              int     `json:"retired"`
	PrefetchLeadMarginMS float64 `json:"prefetch_lead_margin_ms,omitempty"`
	ReleaseOverlapMS     float64 `json:"release_overlap_ms,omitempty"`
}

// StepRolloutBreakdown captures per-step rollout latency decomposition at p50
// and p100 (the step-barrier straggler), straggler skew (p100/p50), barrier
// wall time, llm_think_ms + env_wait_ms, and env_overhead_pct = env_wait / rollout_time.
type StepRolloutBreakdown struct {
	Step           int     `json:"step"`
	Tasks          int     `json:"tasks"`
	P50MS          float64 `json:"p50_ms"`
	P90MS          float64 `json:"p90_ms"`
	P95MS          float64 `json:"p95_ms"`
	P99MS          float64 `json:"p99_ms,omitempty"`
	P100MS         float64 `json:"p100_ms"`
	StragglerSkew  float64 `json:"straggler_skew"`
	BarrierMS      float64 `json:"barrier_ms"`
	LLMThinkMS     float64 `json:"llm_think_ms"`
	EnvWaitMS      float64 `json:"env_wait_ms"`
	LLMThinkP50MS  float64 `json:"llm_think_p50_ms,omitempty"`
	LLMThinkP100MS float64 `json:"llm_think_p100_ms,omitempty"`
	EnvWaitP50MS   float64 `json:"env_wait_p50_ms,omitempty"`
	EnvWaitP100MS  float64 `json:"env_wait_p100_ms,omitempty"`
	EnvOverhead    float64 `json:"env_overhead"`
	EnvOverheadPct float64 `json:"env_overhead_pct"`
}

// StepInterference summarizes per-step cross-job contention when multiple jobs
// execute concurrently against the same SUT.
type StepInterference struct {
	Step            int                `json:"step"`
	ActiveJobs      int                `json:"active_jobs"`
	TotalTasks      int                `json:"total_tasks"`
	CreateP50MS     float64            `json:"create_p50_ms,omitempty"`
	CreateP95MS     float64            `json:"create_p95_ms,omitempty"`
	SlotWaitP50MS   float64            `json:"slot_wait_p50_ms,omitempty"`
	SlotWaitP95MS   float64            `json:"slot_wait_p95_ms,omitempty"`
	PerJobTasks     map[string]int     `json:"per_job_tasks,omitempty"`
	PerJobTTFIP50MS map[string]float64 `json:"per_job_ttfi_p50_ms,omitempty"`
}

// InterferenceSummary quantifies cross-job contention and tail-latency
// amplification across concurrent jobs in a multi-job scenario.
type InterferenceSummary struct {
	ActiveJobs         int                `json:"active_jobs"`
	OverlappingSteps   int                `json:"overlapping_steps"`
	OverlapWindowCount int                `json:"overlap_window_count,omitempty"`
	SlotWaitP50MS      float64            `json:"slot_wait_p50_ms,omitempty"`
	SlotWaitP95MS      float64            `json:"slot_wait_p95_ms,omitempty"`
	SlotWaitMaxMS      float64            `json:"slot_wait_max_ms,omitempty"`
	TTFISpreadRatio    float64            `json:"ttfi_spread_ratio,omitempty"`
	Steps              []StepInterference `json:"steps,omitempty"`
}

// HeadlineBaseline summarizes the published baseline at a scale rung (TTFI and
// its client-boundary sub-phases, peak/sustained acquisition throughput, and
// lifecycle stage latencies).
type HeadlineBaseline struct {
	ClaimsCount           int     `json:"claims_count,omitempty"`
	PeakClaimsPerSec      float64 `json:"peak_claims_per_sec,omitempty"`
	SustainedClaimsPerSec float64 `json:"sustained_claims_per_sec,omitempty"`
	TasksPerMin           float64 `json:"tasks_per_min,omitempty"`
	TTFIP50MS             float64 `json:"ttfi_p50_ms,omitempty"`
	TTFIP90MS             float64 `json:"ttfi_p90_ms,omitempty"`
	TTFIP95MS             float64 `json:"ttfi_p95_ms,omitempty"`
	TTFIP99MS             float64 `json:"ttfi_p99_ms,omitempty"`
	TTFIMaxMS             float64 `json:"ttfi_max_ms,omitempty"`
	SDKSlotWaitP50MS      float64 `json:"sdk_slot_wait_p50_ms,omitempty"`
	SDKSlotWaitP95MS      float64 `json:"sdk_slot_wait_p95_ms,omitempty"`
	SDKSlotWaitP99MS      float64 `json:"sdk_slot_wait_p99_ms,omitempty"`
	WireClaimToReadyP50MS float64 `json:"wire_claim_to_ready_p50_ms,omitempty"`
	WireClaimToReadyP95MS float64 `json:"wire_claim_to_ready_p95_ms,omitempty"`
	WireClaimToReadyP99MS float64 `json:"wire_claim_to_ready_p99_ms,omitempty"`
	ReadyToFirstCmdP50MS  float64 `json:"ready_to_first_cmd_p50_ms,omitempty"`
	ReadyToFirstCmdP95MS  float64 `json:"ready_to_first_cmd_p95_ms,omitempty"`
	ReadyToFirstCmdP99MS  float64 `json:"ready_to_first_cmd_p99_ms,omitempty"`
	CreateP50MS           float64 `json:"create_p50_ms,omitempty"`
	CreateP95MS           float64 `json:"create_p95_ms,omitempty"`
	CreateP99MS           float64 `json:"create_p99_ms,omitempty"`
	WakeP50MS             float64 `json:"wake_p50_ms,omitempty"`
	WakeP95MS             float64 `json:"wake_p95_ms,omitempty"`
	WakeP99MS             float64 `json:"wake_p99_ms,omitempty"`
	ExecP50MS             float64 `json:"exec_p50_ms,omitempty"`
	ExecP95MS             float64 `json:"exec_p95_ms,omitempty"`
	ExecP99MS             float64 `json:"exec_p99_ms,omitempty"`
	SuspendP50MS          float64 `json:"suspend_p50_ms,omitempty"`
	SuspendP95MS          float64 `json:"suspend_p95_ms,omitempty"`
	SuspendP99MS          float64 `json:"suspend_p99_ms,omitempty"`
	SuspendSizeMBP50      float64 `json:"suspend_size_mb_p50,omitempty"`
	SuspendSizeMBP95      float64 `json:"suspend_size_mb_p95,omitempty"`
	CheckpointMBS         float64 `json:"checkpoint_mb_s,omitempty"`
	DeleteP50MS           float64 `json:"delete_p50_ms,omitempty"`
	DeleteP95MS           float64 `json:"delete_p95_ms,omitempty"`
	DeleteP99MS           float64 `json:"delete_p99_ms,omitempty"`
}

// RLStepBreakdown summarizes the RL training step wall-clock breakdown and
// bottleneck attribution across rollout generation, actor update, log-prob,
// parameter sync, reward, and advantage computation.
type RLStepBreakdown struct {
	StepsRecorded        int     `json:"steps_recorded"`
	StepTotalP50MS       float64 `json:"step_total_p50_ms,omitempty"`
	StepTotalP95MS       float64 `json:"step_total_p95_ms,omitempty"`
	StepTotalMaxMS       float64 `json:"step_total_max_ms,omitempty"`
	RolloutGenP50MS      float64 `json:"rollout_gen_p50_ms,omitempty"`
	RolloutGenP95MS      float64 `json:"rollout_gen_p95_ms,omitempty"`
	RolloutGenMaxMS      float64 `json:"rollout_gen_max_ms,omitempty"`
	UpdateActorP50MS     float64 `json:"update_actor_p50_ms,omitempty"`
	UpdateActorP95MS     float64 `json:"update_actor_p95_ms,omitempty"`
	OldLogProbP50MS      float64 `json:"old_log_prob_p50_ms,omitempty"`
	OldLogProbP95MS      float64 `json:"old_log_prob_p95_ms,omitempty"`
	ParamSyncP50MS       float64 `json:"param_sync_p50_ms,omitempty"`
	ParamSyncP95MS       float64 `json:"param_sync_p95_ms,omitempty"`
	TestingP50MS         float64 `json:"testing_p50_ms,omitempty"`
	TestingP95MS         float64 `json:"testing_p95_ms,omitempty"`
	RewardP50MS          float64 `json:"reward_p50_ms,omitempty"`
	AdvP50MS             float64 `json:"adv_p50_ms,omitempty"`
	TokensPerSecP50      float64 `json:"tokens_per_sec_p50,omitempty"`
	TokensPerSecP95      float64 `json:"tokens_per_sec_p95,omitempty"`
	MFUActorP50          float64 `json:"mfu_actor_p50,omitempty"`
	EnvWaitP50MS         float64 `json:"env_wait_p50_ms,omitempty"`
	EnvWaitP100MS        float64 `json:"env_wait_p100_ms,omitempty"`
	LLMThinkP50MS        float64 `json:"llm_think_p50_ms,omitempty"`
	LLMThinkP100MS       float64 `json:"llm_think_p100_ms,omitempty"`
	EnvOverheadPct       float64 `json:"env_overhead_pct,omitempty"`
	StragglerSkewP100P50 float64 `json:"straggler_skew_p100_p50,omitempty"`
	DominantBottleneck   string  `json:"dominant_bottleneck,omitempty"`
}

// HardwareCostSummary captures sandbox density, bin-packing efficiency, actual
// vs requested CPU & memory working-set usage, node-hours / GPU-hours per task,
// server-side pod startup sub-phases, and cluster health counters.
type HardwareCostSummary struct {
	ActiveSandboxes                       int     `json:"active_sandboxes"`
	ParkedSandboxes                       int     `json:"parked_sandboxes"`
	PausedSandboxes                       int     `json:"paused_sandboxes"`
	TotalSandboxes                        int     `json:"total_sandboxes"`
	PeakActiveSandboxes                   int     `json:"peak_active_sandboxes,omitempty"`
	PeakParkedSandboxes                   int     `json:"peak_parked_sandboxes,omitempty"`
	MaxConcurrentSandboxesCap             int     `json:"max_concurrent_sandboxes_cap,omitempty"`
	MaxActivePods                         int     `json:"max_active_pods,omitempty"`
	WorkerPodCount                        int     `json:"worker_pod_count,omitempty"`
	WorkerActorCapacity                   int     `json:"worker_actor_capacity,omitempty"`
	WorkerNodeCount                       int     `json:"worker_node_count"`
	SystemNodeCount                       int     `json:"system_node_count"`
	GPUNodeCount                          int     `json:"gpu_node_count"`
	TotalGPUs                             int     `json:"total_gpus"`
	WorkerTotalVCPU                       float64 `json:"worker_total_vcpu"`
	WorkerAllocatableVCPU                 float64 `json:"worker_allocatable_vcpu"`
	WorkerTotalGiB                        float64 `json:"worker_total_gib"`
	WorkerAllocatableGiB                  float64 `json:"worker_allocatable_gib"`
	SandboxesPerNodeP50                   float64 `json:"sandboxes_per_node_p50"`
	SandboxesPerNodeP90                   float64 `json:"sandboxes_per_node_p90"`
	SandboxesPerNodeP99                   float64 `json:"sandboxes_per_node_p99"`
	SandboxesPerNodeMax                   float64 `json:"sandboxes_per_node_max"`
	SandboxesPerVCPU                      float64 `json:"sandboxes_per_vcpu"`
	SandboxesPerGiBRAM                    float64 `json:"sandboxes_per_gib_ram"`
	ClusterPackingCPUPct                  float64 `json:"cluster_packing_cpu_pct"`
	ClusterPackingMemPct                  float64 `json:"cluster_packing_mem_pct"`
	WorkerNodeCPUUtilP50Pct               float64 `json:"worker_node_cpu_util_p50_pct,omitempty"`
	WorkerNodeCPUUtilMaxPct               float64 `json:"worker_node_cpu_util_max_pct,omitempty"`
	WorkerNodeMemUtilP50Pct               float64 `json:"worker_node_mem_util_p50_pct,omitempty"`
	WorkerNodeMemUtilMaxPct               float64 `json:"worker_node_mem_util_max_pct,omitempty"`
	SandboxPodCPUP50MCores                float64 `json:"sandbox_pod_cpu_p50_mcores,omitempty"`
	SandboxPodCPUMaxMCores                float64 `json:"sandbox_pod_cpu_max_mcores,omitempty"`
	SandboxPodWSSP50MiB                   float64 `json:"sandbox_pod_wss_p50_mib,omitempty"`
	SandboxPodWSSMaxMiB                   float64 `json:"sandbox_pod_wss_max_mib,omitempty"`
	WorkerNodeHoursPerTask                float64 `json:"worker_node_hours_per_task,omitempty"`
	SystemNodeHoursPerTask                float64 `json:"system_node_hours_per_task,omitempty"`
	AcceleratorHoursPerTask               float64 `json:"accelerator_hours_per_task,omitempty"`
	SandboxStalledAcceleratorHoursPerTask float64 `json:"sandbox_stalled_accelerator_hours_per_task,omitempty"`
	PodScheduleP50MS                      float64 `json:"pod_schedule_p50_ms,omitempty"`
	PodScheduleP95MS                      float64 `json:"pod_schedule_p95_ms,omitempty"`
	PodInitP50MS                          float64 `json:"pod_init_p50_ms,omitempty"`
	PodInitP95MS                          float64 `json:"pod_init_p95_ms,omitempty"`
	PodContainerStartP50MS                float64 `json:"pod_container_start_p50_ms,omitempty"`
	PodContainerStartP95MS                float64 `json:"pod_container_start_p95_ms,omitempty"`
	PodTotalReadyP50MS                    float64 `json:"pod_total_ready_p50_ms,omitempty"`
	PodTotalReadyP95MS                    float64 `json:"pod_total_ready_p95_ms,omitempty"`
	OOMKills                              int     `json:"oom_kills"`
	PodEvictions                          int     `json:"pod_evictions"`
	ContainerRestarts                     int     `json:"container_restarts"`
}

// StepConvergencePoint captures one training step along the RL convergence curve.
type StepConvergencePoint struct {
	Step              int                `json:"step"`
	StepTimeS         float64            `json:"step_time_s"`
	WallElapsedS      float64            `json:"wall_elapsed_s"`
	RewardMean        float64            `json:"reward_mean"`
	ValScore          float64            `json:"val_score,omitempty"`
	ValCoreAccMean1   *float64           `json:"val-core/r2e_gym/acc/mean@1,omitempty"`
	ValMetrics        map[string]float64 `json:"val_metrics,omitempty"`
	KLLoss            float64            `json:"kl_loss,omitempty"`
	PGLoss            float64            `json:"pg_loss,omitempty"`
	GradNorm          float64            `json:"grad_norm,omitempty"`
	Entropy           float64            `json:"entropy,omitempty"`
	ResponseLenMean   float64            `json:"response_len_mean,omitempty"`
	ThroughputTokPerS float64            `json:"throughput_tok_per_s,omitempty"`
	MFUActor          float64            `json:"mfu_actor,omitempty"`
}

// ConvergenceSummary summarizes RL training progression and convergence status.
type ConvergenceSummary struct {
	TotalStepsRun                 int                    `json:"total_steps_run"`
	InitialReward                 float64                `json:"initial_reward"`
	FinalReward                   float64                `json:"final_reward"`
	PeakReward                    float64                `json:"peak_reward"`
	PeakStep                      int                    `json:"peak_step"`
	RewardDelta                   float64                `json:"reward_delta"`
	Converged                     bool                   `json:"converged"`
	ConvergenceStatus             string                 `json:"convergence_status"`
	StepsToConvergence            *int                   `json:"steps_to_convergence,omitempty"`
	WallTimeToConvergenceS        *float64               `json:"wall_time_to_convergence_s,omitempty"`
	AcceleratorHoursToConvergence *float64               `json:"accelerator_hours_to_convergence,omitempty"`
	Curve                         []StepConvergencePoint `json:"curve,omitempty"`
}

// Report is the summary of one run's JSONL, stable enough to diff between
// runs.
type Report struct {
	RunID string `json:"run"`
	Mode  string `json:"mode,omitempty"`
	// Reps is how many runs the report pools, the run events it saw: one for
	// a file, the reps of an arm under report -arm (see Pool). Runs lists
	// their run ids in order.
	Reps                 int                  `json:"reps,omitempty"`
	Runs                 []string             `json:"runs,omitempty"`
	RunMS                float64              `json:"run_ms,omitempty"`
	Tasks                int                  `json:"tasks"`
	Failed               int                  `json:"failed"`
	Rewarded             int                  `json:"rewarded"`
	RewardRate           float64              `json:"reward_rate"`
	Stats                []Stat               `json:"stats"`
	Steps                []StepStat           `json:"steps,omitempty"`
	Headline             *HeadlineBaseline    `json:"headline_baseline,omitempty"`
	RLBreakdown          *RLStepBreakdown     `json:"rl_step_breakdown,omitempty"`
	HardwareCost         *HardwareCostSummary `json:"hardware_cost,omitempty"`
	Convergence          *ConvergenceSummary  `json:"convergence,omitempty"`
	WorkingSetHits       int                  `json:"working_set_hits,omitempty"`
	WorkingSetMisses     int                  `json:"working_set_misses,omitempty"`
	WorkingSetHitRate    float64              `json:"working_set_hit_rate,omitempty"`
	StandingPoolSize     int                  `json:"standing_pool_size,omitempty"`
	ActiveSandboxes      int                  `json:"active_sandboxes,omitempty"`
	ParkedSandboxes      int                  `json:"parked_sandboxes,omitempty"`
	PausedSandboxes      int                  `json:"paused_sandboxes,omitempty"`
	PeakActiveSandboxes  int                  `json:"peak_active_sandboxes,omitempty"`
	PeakParkedSandboxes  int                  `json:"peak_parked_sandboxes,omitempty"`
	Created              int                  `json:"created,omitempty"`
	Reused               int                  `json:"reused,omitempty"`
	Retired              int                  `json:"retired,omitempty"`
	PrefetchLeadMarginMS float64              `json:"prefetch_lead_margin_ms,omitempty"`
	ReleaseOverlapMS     float64              `json:"release_overlap_ms,omitempty"`
	// RolloutBreakdown holds per-step rollout decomposition (p50 vs p100 straggler
	// skew, barrier time, llm_think_ms + env_wait_ms, and env_overhead_pct).
	RolloutBreakdown []StepRolloutBreakdown `json:"rollout_breakdown,omitempty"`
	// FaultClassification is the rollout-level 3-tier fault classification
	// and the infra_fault_rate target status (docs/glossary.md).
	FaultClassification *schema.FaultClassification `json:"fault_classification,omitempty"`
	// Errors counts distinct error strings, most frequent first.
	Errors []ErrorCount `json:"errors,omitempty"`
	// Unresolved lists the instances whose reward was 0, once each.
	Unresolved []Unresolved `json:"unresolved,omitempty"`
	// Cache is what the node image caches did between the samples given to
	// report -cache-before and -cache-after; nil without them.
	Cache *CacheDelta `json:"cache,omitempty"`
	// Restores is atelet's restore phase breakdown joined to the run's
	// environments (report -restore-log); nil without it.
	Restores *RestoreBreakdown `json:"restores,omitempty"`
	// Labels are the run event's labels: the loader's own and the operator's.
	Labels map[string]string `json:"labels,omitempty"`
	// Slowest lists the slowest first commands and wakes (report -top N).
	Slowest []SlowEvent `json:"slowest,omitempty"`
	// CustomMetrics summarizes per-event Metrics maps.
	CustomMetrics map[string]schema.MetricSummary `json:"custom_metrics,omitempty"`
	// Jobs summarizes per-job task/step/metric breakdowns in multi-job runs.
	Jobs map[string]schema.JobReportSummary `json:"jobs,omitempty"`
	// Interference summarizes cross-job contention when >1 job is active.
	Interference *InterferenceSummary `json:"interference,omitempty"`
	// AcquisitionSources counts how environments were acquired (warm_pool, cold_create, resume, etc.).
	AcquisitionSources map[string]int `json:"acquisition_sources,omitempty"`
	// Validity holds the result of evaluating Validity Rules 1-7.
	Validity *schema.ReportValidity `json:"validity,omitempty"`
	// RunWindow holds the optional run_window.json metadata attached to the report.
	RunWindow *schema.RunWindow `json:"run_window,omitempty"`
	// SystemDescription holds the Setup Identification & cluster provenance metadata.
	SystemDescription *schema.SystemDescription `json:"system_description,omitempty"`
	// Scopes records which telemetry scopes (rl, user-sandbox, substrate, k8s) are active.
	Scopes []schema.TelemetryScope `json:"scopes,omitempty"`
	// ParkMode records how sandboxes were managed between rollouts/steps ("ephemeral", "suspend-sync", "suspend-async", "fork").
	ParkMode string `json:"park_mode,omitempty"`
	// LifecyclePolicy records the configured and observed sandbox warming, parking, and trainer warmup policy.
	LifecyclePolicy *schema.SandboxLifecyclePolicy `json:"lifecycle_policy,omitempty"`
	// SchemaEra identifies whether the report has post-audit or pre-audit-legacy telemetry.
	SchemaEra string `json:"schema_era,omitempty"`

	// TraceCaptured (Validity Rule 6): an SDK trace was captured, so latencies
	// include trace overhead. Always emitted so "no trace" is explicit.
	TraceCaptured  bool `json:"trace_captured"`
	TraceFileCount int  `json:"trace_file_count,omitempty"`

	// Auxiliary counters populated from event streams for validity rules and setup identification.
	PrewarmCount        int  `json:"prewarm_count,omitempty"`
	HasTraceEvents      bool `json:"has_trace_events,omitempty"`
	ReapSeen            bool `json:"reap_seen,omitempty"`
	ReapFailed          bool `json:"reap_failed,omitempty"`
	DeletedCount        int  `json:"deleted_count,omitempty"`
	OrphanCount         int  `json:"orphan_count,omitempty"`
	CreateCacheHits     int  `json:"create_cache_hits,omitempty"`
	CreateCacheMisses   int  `json:"create_cache_misses,omitempty"`
	inferredCacheHits   int
	inferredCacheMisses int
	stepActiveCounts    map[int]int
	stepCreatedCounts   map[int]int
	stepReusedCounts    map[int]int
	stepRetiredCounts   map[int]int
	stepCacheHits       map[int]int
	stepCacheMisses     map[int]int
	JobStepTasks        map[string]map[int]int `json:"job_step_tasks,omitempty"`
	DistinctImages      []string               `json:"distinct_images,omitempty"`
	DistinctInstances   int                    `json:"distinct_instances,omitempty"`
	DistinctEnvs        int                    `json:"distinct_envs,omitempty"`
	RunTags             map[string]string      `json:"run_tags,omitempty"`
}

// SlowEvent is one of the slowest first commands or wakes of a run.
type SlowEvent struct {
	Event    string  `json:"event"`
	Env      string  `json:"env"`
	Instance string  `json:"instance,omitempty"`
	Step     int     `json:"step,omitempty"`
	DurMS    float64 `json:"dur_ms"`
	Attempts int     `json:"attempts,omitempty"`
}

// Slowest returns the n slowest successful ttfi events, then the n slowest
// successful wake events, longest first.
func Slowest(events []Event, n int) []SlowEvent {
	var out []SlowEvent
	for _, kind := range []string{EventTTFI, EventWake} {
		var picked []SlowEvent
		for _, e := range events {
			if e.Event == kind && e.OK {
				picked = append(picked, SlowEvent{Event: e.Event, Env: e.Env, Instance: e.Instance, Step: e.Step, DurMS: e.DurMS, Attempts: e.Attempts})
			}
		}
		sort.SliceStable(picked, func(i, j int) bool { return picked[i].DurMS > picked[j].DurMS })
		if len(picked) > n {
			picked = picked[:n]
		}
		out = append(out, picked...)
	}
	return out
}

// ScopeEnabled reports whether target telemetry scope is active on rep (empty Scopes enables all).
func (rep *Report) ScopeEnabled(target schema.TelemetryScope) bool {
	if rep == nil {
		return true
	}
	return schema.HasTelemetryScope(rep.Scopes, target)
}

// StatFor returns the Stat for (event, name) if present in rep.Stats.
func (rep *Report) StatFor(event, name string) (Stat, bool) {
	if rep == nil {
		return Stat{}, false
	}
	for _, s := range rep.Stats {
		if s.Event == event && s.Name == name {
			return s, true
		}
	}
	return Stat{}, false
}

// ThroughputTasksPerMin returns the aggregate task throughput (tasks/min) when RunMS > 0.
func (rep *Report) ThroughputTasksPerMin() float64 {
	if rep == nil || rep.RunMS <= 0 {
		return 0
	}
	return round2(float64(rep.Tasks) / (rep.RunMS / 60000.0))
}

// ErrorCount is one error string and how often it occurred.
type ErrorCount struct {
	Err   string `json:"err"`
	Count int    `json:"count"`
}

type statKey struct{ event, name string }

type taskRolloutSample struct {
	durMS      float64
	llmThinkMS float64
	envWaitMS  float64
}

type jobAccum struct {
	tasks    int
	failed   int
	rewarded int
	steps    map[int]int
	ttfi     []float64
	faults   *faultTracker
}

type wsSnapshot struct {
	hits                 int
	misses               int
	standingPoolSize     int
	created              int
	reused               int
	retired              int
	prefetchLeadMarginMS float64
	releaseOverlapMS     float64
}

func sumWSSnapshots(m map[string]wsSnapshot) (out wsSnapshot, hitRate float64) {
	for _, s := range m {
		out.hits += s.hits
		out.misses += s.misses
		out.standingPoolSize += s.standingPoolSize
		out.created += s.created
		out.reused += s.reused
		out.retired += s.retired
		out.prefetchLeadMarginMS += s.prefetchLeadMarginMS
		out.releaseOverlapMS += s.releaseOverlapMS
	}
	if total := out.hits + out.misses; total > 0 {
		hitRate = float64(out.hits) / float64(total)
	}
	return out, hitRate
}

func maxFloat64(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	mx := xs[0]
	for _, v := range xs[1:] {
		if v > mx {
			mx = v
		}
	}
	return mx
}

// Builder folds a stream of events into a Report without materializing the full
// event slice in memory.
type Builder struct {
	rep                Report
	durs               map[statKey][]float64
	sizes              map[statKey][]float64
	sizeRates          map[statKey][]float64
	failed             map[statKey]int
	retried            map[statKey]int
	unresolved         map[string]string
	order              []statKey
	steps              map[int]*StepStat
	wakes              map[int][]float64
	barriers           map[int][]float64
	parks              map[int][]float64
	lives              map[int][]float64
	runs               []float64
	labels             labelUnion
	errs               map[string]int
	stepRollouts       map[int][]taskRolloutSample
	stepThinkPool      map[int][]float64
	customMetrics      map[string][]float64
	jobs               map[string]*jobAccum
	jobRunWS           map[string]wsSnapshot
	stepJobBarrierWS   map[int]map[string]wsSnapshot
	slotWaits          []float64
	stepSlotWaits      map[int][]float64
	stepCreates        map[int][]float64
	stepJobTasks       map[int]map[string]int
	stepJobTTFI        map[int]map[string][]float64
	sources            map[string]int
	claimTimes         []time.Time
	createTimes        []time.Time
	minEventStart      time.Time
	maxEventEnd        time.Time
	trainerStepMetrics map[int]map[string]float64
	trainerStepDurMS   map[int]float64
	distinctImages     map[string]bool
	distinctInstances  map[string]bool
	distinctEnvs       map[string]bool
	runTags            map[string]string
	jobRunTags         map[string]map[string]string
	envState           map[string]string
	stepActiveEnvs     map[int]map[string]bool
	stepPausedEnvs     map[int]map[string]bool
	stepTaskIntervals  map[int][][2]time.Time
	stepCreated        map[int]int
	stepReused         map[int]int
	stepRetired        map[int]int
	stepCacheHits      map[int]int
	stepCacheMisses    map[int]int
	stepInferredHits   map[int]int
	stepInferredMisses map[int]int
	peakActiveEnvs     int
	peakPausedEnvs     int
	faults             *faultTracker
}

// NewBuilder returns a streaming report Builder.
func NewBuilder() *Builder {
	return &Builder{
		durs:               map[statKey][]float64{},
		sizes:              map[statKey][]float64{},
		sizeRates:          map[statKey][]float64{},
		failed:             map[statKey]int{},
		retried:            map[statKey]int{},
		unresolved:         map[string]string{},
		steps:              map[int]*StepStat{},
		wakes:              map[int][]float64{},
		barriers:           map[int][]float64{},
		parks:              map[int][]float64{},
		lives:              map[int][]float64{},
		labels:             labelUnion{},
		errs:               map[string]int{},
		stepRollouts:       map[int][]taskRolloutSample{},
		stepThinkPool:      map[int][]float64{},
		customMetrics:      map[string][]float64{},
		jobs:               map[string]*jobAccum{},
		jobRunWS:           map[string]wsSnapshot{},
		stepJobBarrierWS:   map[int]map[string]wsSnapshot{},
		stepSlotWaits:      map[int][]float64{},
		stepCreates:        map[int][]float64{},
		stepJobTasks:       map[int]map[string]int{},
		stepJobTTFI:        map[int]map[string][]float64{},
		sources:            map[string]int{},
		trainerStepMetrics: map[int]map[string]float64{},
		trainerStepDurMS:   map[int]float64{},
		distinctImages:     map[string]bool{},
		distinctInstances:  map[string]bool{},
		distinctEnvs:       map[string]bool{},
		runTags:            map[string]string{},
		jobRunTags:         map[string]map[string]string{},
		envState:           map[string]string{},
		stepActiveEnvs:     map[int]map[string]bool{},
		stepPausedEnvs:     map[int]map[string]bool{},
		stepTaskIntervals:  map[int][][2]time.Time{},
		stepCreated:        map[int]int{},
		stepReused:         map[int]int{},
		stepRetired:        map[int]int{},
		stepCacheHits:      map[int]int{},
		stepCacheMisses:    map[int]int{},
		stepInferredHits:   map[int]int{},
		stepInferredMisses: map[int]int{},
		faults:             newFaultTracker(),
	}
}

func (b *Builder) markEnvState(env string, step int, state string) {
	if env == "" || env == EventRun || env == EventBarrier || env == EventPark || env == EventReap {
		return
	}
	stepKey := step
	if stepKey <= 0 {
		stepKey = 1
	}
	switch state {
	case "active":
		b.envState[env] = "active"
		if b.stepActiveEnvs[stepKey] == nil {
			b.stepActiveEnvs[stepKey] = map[string]bool{}
		}
		b.stepActiveEnvs[stepKey][env] = true
	case "paused":
		b.envState[env] = "paused"
		if b.stepPausedEnvs[stepKey] == nil {
			b.stepPausedEnvs[stepKey] = map[string]bool{}
		}
		b.stepPausedEnvs[stepKey][env] = true
	case "deleted":
		delete(b.envState, env)
	}
	var curActive, curPaused int
	for _, st := range b.envState {
		switch st {
		case "active":
			curActive++
		case "paused":
			curPaused++
		}
	}
	if curActive > b.peakActiveEnvs {
		b.peakActiveEnvs = curActive
	}
	if curPaused > b.peakPausedEnvs {
		b.peakPausedEnvs = curPaused
	}
}

func (b *Builder) ensureJob(job string) *jobAccum {
	if job == "" {
		return nil
	}
	ja, ok := b.jobs[job]
	if !ok {
		ja = &jobAccum{
			steps:  map[int]int{},
			faults: newFaultTracker(),
		}
		b.jobs[job] = ja
	}
	return ja
}

// Add folds one event into the Builder.
func (b *Builder) Add(e Event) {
	if b.rep.RunID == "" {
		b.rep.RunID = e.RunID
	}
	if e.Phase == "trace" || (e.Tags != nil && e.Tags["mode"] == "trace") {
		b.rep.HasTraceEvents = true
	}
	if !e.Time.IsZero() && e.Event != EventReap {
		evStart := e.Time
		if e.DurMS > 0 {
			evStart = e.Time.Add(-time.Duration(e.DurMS * float64(time.Millisecond)))
		}
		if b.minEventStart.IsZero() || evStart.Before(b.minEventStart) {
			b.minEventStart = evStart
		}
		if b.maxEventEnd.IsZero() || e.Time.After(b.maxEventEnd) {
			b.maxEventEnd = e.Time
		}
	}
	if e.Event != EventReap {
		if e.Image != "" {
			b.distinctImages[e.Image] = true
		}
		if e.Instance != "" {
			b.distinctInstances[e.Instance] = true
		}
		if e.Env != "" && e.Event != EventRun && e.Event != EventBarrier && e.Event != EventPark {
			b.distinctEnvs[e.Env] = true
			if e.OK {
				switch e.Event {
				case EventCreate, EventTTFI, EventWake, EventFork:
					b.markEnvState(e.Env, e.Step, "active")
				case EventSuspend:
					b.markEnvState(e.Env, e.Step, "paused")
				case EventDelete:
					b.markEnvState(e.Env, e.Step, "deleted")
				case EventStep:
					b.markEnvState(e.Env, e.Step, "active")
				case EventPhase:
					switch e.Name {
					case "suspend":
						b.markEnvState(e.Env, e.Step, "paused")
					case "delete":
						b.markEnvState(e.Env, e.Step, "deleted")
					default:
						b.markEnvState(e.Env, e.Step, "active")
					}
				}
			}
		}
	}
	for mk, mv := range e.Metrics {
		if !math.IsNaN(mv) && !math.IsInf(mv, 0) {
			b.customMetrics[mk] = append(b.customMetrics[mk], mv)
		}
	}
	ja := b.ensureJob(e.Job)
	if b.faults == nil {
		b.faults = newFaultTracker()
	}
	b.faults.Add(e)
	if ja != nil {
		ja.faults.Add(e)
	}

	switch e.Event {
	case EventPrewarm:
		b.rep.PrewarmCount++
	case EventReap:
		b.rep.ReapSeen = true
		if !e.OK {
			b.rep.ReapFailed = true
		}
		if e.DeletedCount > b.rep.DeletedCount {
			b.rep.DeletedCount = e.DeletedCount
		} else if v, ok := e.Metrics["deleted_count"]; ok && int(v) > b.rep.DeletedCount {
			b.rep.DeletedCount = int(v)
		} else if e.Name != "orphans" && e.Count > 0 {
			b.rep.DeletedCount += e.Count
		}
		if e.OrphanCount > b.rep.OrphanCount {
			b.rep.OrphanCount = e.OrphanCount
		} else if v, ok := e.Metrics["orphan_count"]; ok && int(v) > b.rep.OrphanCount {
			b.rep.OrphanCount = int(v)
		} else if e.Name == "orphans" && e.Count > b.rep.OrphanCount {
			b.rep.OrphanCount = e.Count
		}
	case EventThink:
		stepKey := e.Step
		if stepKey <= 0 {
			stepKey = 1
		}
		b.stepThinkPool[stepKey] = append(b.stepThinkPool[stepKey], e.DurMS)
	case EventCreate:
		stepKey := e.Step
		if stepKey <= 0 {
			stepKey = 1
		}
		if e.CacheHit != nil {
			if *e.CacheHit {
				b.rep.CreateCacheHits++
				b.stepCacheHits[stepKey]++
			} else {
				b.rep.CreateCacheMisses++
				b.stepCacheMisses[stepKey]++
			}
		}
		if e.OK && e.DurMS > 0 {
			if e.DurMS < 5000.0 {
				b.rep.inferredCacheHits++
				b.stepInferredHits[stepKey]++
			} else {
				b.rep.inferredCacheMisses++
				b.stepInferredMisses[stepKey]++
			}
		}
		if e.Source != "" {
			b.sources[e.Source]++
		}
		if e.OK {
			b.stepCreated[stepKey]++
			if !e.Time.IsZero() {
				b.createTimes = append(b.createTimes, e.Time)
			}
			b.stepCreates[stepKey] = append(b.stepCreates[stepKey], e.DurMS)
		}
	case EventDelete:
		if e.OK {
			stepKey := e.Step
			if stepKey <= 0 {
				stepKey = 1
			}
			b.stepRetired[stepKey]++
		}
	case EventPhase:
		if (e.Name == "create" || e.Name == "resume") && e.Source != "" {
			b.sources[e.Source]++
		}
		if e.Name == "resume" && e.OK && e.Step > 0 {
			b.stepReused[e.Step]++
		}
		if e.Name == "create" && e.OK {
			if !e.Time.IsZero() {
				b.createTimes = append(b.createTimes, e.Time)
			}
			stepKey := e.Step
			if stepKey <= 0 {
				stepKey = 1
			}
			b.stepCreates[stepKey] = append(b.stepCreates[stepKey], e.DurMS)
			if ja != nil {
				ja.ttfi = append(ja.ttfi, e.DurMS)
				if b.stepJobTTFI[stepKey] == nil {
					b.stepJobTTFI[stepKey] = map[string][]float64{}
				}
				b.stepJobTTFI[stepKey][e.Job] = append(b.stepJobTTFI[stepKey][e.Job], e.DurMS)
			}
		}
	case EventTTFI:
		if e.OK && !e.Time.IsZero() {
			b.claimTimes = append(b.claimTimes, e.Time)
		}
		if ja != nil && e.OK {
			ja.ttfi = append(ja.ttfi, e.DurMS)
			stepKey := e.Step
			if stepKey <= 0 {
				stepKey = 1
			}
			if b.stepJobTTFI[stepKey] == nil {
				b.stepJobTTFI[stepKey] = map[string][]float64{}
			}
			b.stepJobTTFI[stepKey][e.Job] = append(b.stepJobTTFI[stepKey][e.Job], e.DurMS)
		}
	}

	if e.SlotWaitMS > 0 {
		b.slotWaits = append(b.slotWaits, e.SlotWaitMS)
		stepKey := e.Step
		if stepKey <= 0 {
			stepKey = 1
		}
		b.stepSlotWaits[stepKey] = append(b.stepSlotWaits[stepKey], e.SlotWaitMS)
	}

	k := statKey{e.Event, ""}
	if e.Event == EventStep || e.Event == EventPark || e.Event == EventPhase || e.Event == EventTask {
		k.name = e.Name // a task's name is its trajectory, so a mix gets a row each
	}
	if _, ok := b.durs[k]; !ok {
		b.order = append(b.order, k)
		b.durs[k] = nil
	}
	if e.Attempts > 1 {
		b.retried[k]++
	}
	if e.Event == EventStep && e.Reward != nil && *e.Reward == 0 {
		if _, seen := b.unresolved[e.Instance]; !seen {
			if e.Detail != "" {
				b.unresolved[e.Instance] = e.Detail
			} else {
				b.unresolved[e.Instance] = verdict(e.Tail)
			}
		}
	}
	if e.Event == EventStep && e.Name == "think" {
		stepKey := e.Step
		if stepKey <= 0 {
			stepKey = 1
		}
		b.stepThinkPool[stepKey] = append(b.stepThinkPool[stepKey], e.DurMS)
	}
	if e.OK {
		if !(e.Event == EventStep && e.Name == "" && e.Step == 0 && e.DurMS <= 0) {
			b.durs[k] = append(b.durs[k], e.DurMS)
		}
		sz := e.SizeMB
		if sz <= 0 && e.Bytes > 0 {
			sz = float64(e.Bytes) / (1024.0 * 1024.0)
		}
		if sz <= 0 && e.Metrics != nil {
			sz = e.Metrics["size_mb"]
		}
		if sz <= 0 && e.Tags != nil && e.Tags["size_mb"] != "" {
			if parsed, err := strconv.ParseFloat(e.Tags["size_mb"], 64); err == nil && parsed > 0 {
				sz = parsed
			}
		}
		if sz > 0 {
			b.sizes[k] = append(b.sizes[k], sz)
			if e.DurMS > 0 {
				b.sizeRates[k] = append(b.sizeRates[k], sz/(e.DurMS/1000.0))
			}
		}
	} else {
		b.failed[k]++
		if e.Err != "" {
			b.errs[e.Err]++
		}
	}
	switch e.Event {
	case EventRun:
		b.rep.Reps++
		b.rep.Runs = append(b.rep.Runs, e.RunID)
		if b.rep.RunID == "" && e.RunID != "" {
			b.rep.RunID = e.RunID
		}
		if b.rep.Mode == "" {
			b.rep.Mode = e.Name
		}
		if e.Name == "trace" {
			b.rep.HasTraceEvents = true
		}
		b.runs = append(b.runs, e.DurMS)
		b.rep.RunMS = e.DurMS
		b.labels.add(e.Labels)
		b.rep.WorkingSetHits = e.WorkingSetHits
		b.rep.WorkingSetMisses = e.WorkingSetMisses
		b.rep.WorkingSetHitRate = e.WorkingSetHitRate
		b.rep.StandingPoolSize = e.StandingPoolSize
		b.rep.Created = e.Created
		b.rep.Reused = e.Reused
		b.rep.Retired = e.Retired
		b.rep.PrefetchLeadMarginMS = e.PrefetchLeadMarginMS
		b.rep.ReleaseOverlapMS = e.ReleaseOverlapMS
		if e.Job != "" {
			b.jobRunWS[e.Job] = wsSnapshot{
				hits:                 e.WorkingSetHits,
				misses:               e.WorkingSetMisses,
				standingPoolSize:     e.StandingPoolSize,
				created:              e.Created,
				reused:               e.Reused,
				retired:              e.Retired,
				prefetchLeadMarginMS: e.PrefetchLeadMarginMS,
				releaseOverlapMS:     e.ReleaseOverlapMS,
			}
		}
		for tk, tv := range e.Tags {
			b.runTags[tk] = tv
		}
		if e.Job != "" && len(e.Tags) > 0 {
			jt := make(map[string]string, len(e.Tags))
			for tk, tv := range e.Tags {
				jt[tk] = tv
			}
			b.jobRunTags[e.Job] = jt
		}
	case EventTask:
		b.rep.Tasks++
		if !e.OK {
			b.rep.Failed++
		}
		if e.Reward != nil && *e.Reward == 1 {
			b.rep.Rewarded++
		}
		if e.Step > 0 {
			s := stepStat(b.steps, e.Step)
			s.Tasks++
			if !e.OK {
				s.Failed++
			}
			if e.Reward != nil && *e.Reward == 1 {
				s.Rewarded++
			}
			if !e.Time.IsZero() && e.DurMS > 0 {
				start := e.Time.Add(-time.Duration(e.DurMS * float64(time.Millisecond)))
				b.stepTaskIntervals[e.Step] = append(b.stepTaskIntervals[e.Step], [2]time.Time{start, e.Time})
			}
		}
		stepKey := e.Step
		if stepKey <= 0 {
			stepKey = 1
		}
		b.stepRollouts[stepKey] = append(b.stepRollouts[stepKey], extractRolloutSample(e))
		if ja != nil {
			ja.tasks++
			if !e.OK {
				ja.failed++
			}
			if e.Reward != nil && *e.Reward == 1 {
				ja.rewarded++
			}
			ja.steps[stepKey]++
			if b.stepJobTasks[stepKey] == nil {
				b.stepJobTasks[stepKey] = map[string]int{}
			}
			b.stepJobTasks[stepKey][e.Job]++
		}
	case EventStep:
		if e.Name == "" && e.Step >= 0 {
			if e.Step > 0 {
				st := stepStat(b.steps, e.Step)
				if st.BarrierMS == 0 {
					st.BarrierMS = e.DurMS
				}
				if ja != nil {
					ja.steps[e.Step]++
				}
				if e.DurMS > b.trainerStepDurMS[e.Step] {
					b.trainerStepDurMS[e.Step] = e.DurMS
				}
			}
			if len(e.Metrics) > 0 {
				if b.trainerStepMetrics[e.Step] == nil {
					b.trainerStepMetrics[e.Step] = make(map[string]float64, len(e.Metrics))
				}
				for mk, mv := range e.Metrics {
					b.trainerStepMetrics[e.Step][mk] = mv
				}
			}
		}
	case EventBarrier:
		st := stepStat(b.steps, e.Step)
		b.barriers[e.Step] = append(b.barriers[e.Step], e.DurMS)
		b.lives[e.Step] = append(b.lives[e.Step], float64(e.Count))
		st.WorkingSetHits = e.WorkingSetHits
		st.WorkingSetMisses = e.WorkingSetMisses
		st.WorkingSetHitRate = e.WorkingSetHitRate
		st.StandingPoolSize = e.StandingPoolSize
		st.Created = e.Created
		st.Reused = e.Reused
		st.Retired = e.Retired
		st.PrefetchLeadMarginMS = e.PrefetchLeadMarginMS
		st.ReleaseOverlapMS = e.ReleaseOverlapMS
		if e.Job != "" {
			if b.stepJobBarrierWS[e.Step] == nil {
				b.stepJobBarrierWS[e.Step] = map[string]wsSnapshot{}
			}
			b.stepJobBarrierWS[e.Step][e.Job] = wsSnapshot{
				hits:                 e.WorkingSetHits,
				misses:               e.WorkingSetMisses,
				standingPoolSize:     e.StandingPoolSize,
				created:              e.Created,
				reused:               e.Reused,
				retired:              e.Retired,
				prefetchLeadMarginMS: e.PrefetchLeadMarginMS,
				releaseOverlapMS:     e.ReleaseOverlapMS,
			}
		}
		if e.Step > 0 && len(e.Metrics) > 0 {
			if b.trainerStepMetrics[e.Step] == nil {
				b.trainerStepMetrics[e.Step] = make(map[string]float64, len(e.Metrics))
			}
			for mk, mv := range e.Metrics {
				b.trainerStepMetrics[e.Step][mk] = mv
			}
			if e.DurMS > b.trainerStepDurMS[e.Step] {
				b.trainerStepDurMS[e.Step] = e.DurMS
			}
		}
	case EventPark:
		st := stepStat(b.steps, e.Step)
		b.parks[e.Step] = append(b.parks[e.Step], e.DurMS)
		if e.StandingPoolSize > 0 {
			st.StandingPoolSize = e.StandingPoolSize
		}
		if e.ReleaseOverlapMS > 0 {
			st.ReleaseOverlapMS = e.ReleaseOverlapMS
		}
	case EventWake:
		if e.OK {
			b.wakes[e.Step] = append(b.wakes[e.Step], e.DurMS)
			if e.Step > 0 {
				b.stepReused[e.Step]++
			}
		}
	}
}

func peakConcurrentIntervals(intervals [][2]time.Time) int {
	if len(intervals) == 0 {
		return 0
	}
	type pt struct {
		t time.Time
		d int
	}
	pts := make([]pt, 0, len(intervals)*2)
	for _, iv := range intervals {
		pts = append(pts, pt{t: iv[0], d: 1}, pt{t: iv[1], d: -1})
	}
	sort.Slice(pts, func(i, j int) bool {
		if pts[i].t.Equal(pts[j].t) {
			return pts[i].d < pts[j].d
		}
		return pts[i].t.Before(pts[j].t)
	})
	var cur, peak int
	for _, p := range pts {
		cur += p.d
		if cur > peak {
			peak = cur
		}
	}
	return peak
}

func extractRolloutSample(ev Event) taskRolloutSample {
	dur := ev.DurMS
	var think, envWait float64
	hasThink := false
	hasEnvWait := false
	if ev.Metrics != nil {
		if v, ok := ev.Metrics["llm_think_ms"]; ok && v >= 0 {
			think = v
			hasThink = true
		}
		if v, ok := ev.Metrics["env_wait_ms"]; ok && v >= 0 {
			envWait = v
			hasEnvWait = true
		}
	}
	switch {
	case hasThink && hasEnvWait:
		if dur <= 0 {
			dur = think + envWait
		}
	case hasThink && !hasEnvWait:
		envWait = math.Max(0, dur-think)
	case !hasThink && hasEnvWait:
		think = math.Max(0, dur-envWait)
	default:
		think = 0
		envWait = dur
	}
	return taskRolloutSample{
		durMS:      dur,
		llmThinkMS: think,
		envWaitMS:  envWait,
	}
}

// Finish computes exact percentiles over the accumulated durations and returns
// the finalized Report.
func (b *Builder) Finish() *Report {
	rep := b.rep
	rep.SchemaEra = "post-audit-v1"
	if rep.Tasks > 0 {
		rep.RewardRate = float64(rep.Rewarded) / float64(rep.Tasks)
	}
	if len(b.jobs) > 1 {
		rep.RunMS = maxFloat64(b.runs)
	} else {
		rep.RunMS = median(b.runs)
	}
	if len(b.jobRunWS) > 1 {
		wsSum, hitRate := sumWSSnapshots(b.jobRunWS)
		rep.WorkingSetHits = wsSum.hits
		rep.WorkingSetMisses = wsSum.misses
		rep.WorkingSetHitRate = hitRate
		rep.StandingPoolSize = wsSum.standingPoolSize
		rep.Created = wsSum.created
		rep.Reused = wsSum.reused
		rep.Retired = wsSum.retired
		rep.PrefetchLeadMarginMS = wsSum.prefetchLeadMarginMS
		rep.ReleaseOverlapMS = wsSum.releaseOverlapMS
	}
	rep.Labels = b.labels.merged()
	for _, k := range b.order {
		d := b.durs[k]
		if k.event == EventStep && k.name == "" && len(d) == 0 && b.failed[k] == 0 {
			continue
		}
		sort.Float64s(d)
		st := Stat{
			Event:   k.event,
			Name:    k.name,
			Count:   len(d) + b.failed[k],
			Failed:  b.failed[k],
			P50MS:   percentile(d, 0.50),
			P90MS:   percentile(d, 0.90),
			P95MS:   percentile(d, 0.95),
			P99MS:   percentile(d, 0.99),
			MaxMS:   percentile(d, 1),
			Retried: b.retried[k],
		}
		if szs := b.sizes[k]; len(szs) > 0 {
			sort.Float64s(szs)
			st.SizeMBP50 = round2(percentile(szs, 0.50))
			st.SizeMBP95 = round2(percentile(szs, 0.95))
			st.SizeMBMax = round2(percentile(szs, 1.0))
		}
		if rates := b.sizeRates[k]; len(rates) > 0 {
			sort.Float64s(rates)
			st.CheckpointMBS = round2(percentile(rates, 0.50))
		}
		rep.Stats = append(rep.Stats, st)
	}
	if len(b.stepActiveEnvs) > 0 || len(b.stepTaskIntervals) > 0 {
		rep.stepActiveCounts = make(map[int]int, len(b.stepActiveEnvs)+len(b.stepTaskIntervals))
		for stNum, envSet := range b.stepActiveEnvs {
			rep.stepActiveCounts[stNum] = len(envSet)
		}
		for stNum, ivs := range b.stepTaskIntervals {
			if peak := peakConcurrentIntervals(ivs); peak > 0 {
				if envCnt := len(b.stepActiveEnvs[stNum]); envCnt == 0 || peak < envCnt {
					rep.stepActiveCounts[stNum] = peak
				}
			}
		}
	}
	if len(b.stepCreated) > 0 {
		rep.stepCreatedCounts = make(map[int]int, len(b.stepCreated))
		for k, v := range b.stepCreated {
			rep.stepCreatedCounts[k] = v
		}
	}
	if len(b.stepReused) > 0 {
		rep.stepReusedCounts = make(map[int]int, len(b.stepReused))
		for k, v := range b.stepReused {
			rep.stepReusedCounts[k] = v
		}
	}
	if len(b.stepRetired) > 0 {
		rep.stepRetiredCounts = make(map[int]int, len(b.stepRetired))
		for k, v := range b.stepRetired {
			rep.stepRetiredCounts[k] = v
		}
	}
	hitsMap := b.stepCacheHits
	missesMap := b.stepCacheMisses
	if len(hitsMap) == 0 && len(missesMap) == 0 {
		hitsMap = b.stepInferredHits
		missesMap = b.stepInferredMisses
	}
	if len(hitsMap) > 0 {
		rep.stepCacheHits = make(map[int]int, len(hitsMap))
		for k, v := range hitsMap {
			rep.stepCacheHits[k] = v
		}
	}
	if len(missesMap) > 0 {
		rep.stepCacheMisses = make(map[int]int, len(missesMap))
		for k, v := range missesMap {
			rep.stepCacheMisses[k] = v
		}
	}
	for step, s := range b.steps {
		w := b.wakes[step]
		sort.Float64s(w)
		cp := *s
		cp.WakeP50MS = percentile(w, 0.5)
		cp.WakeP95MS = percentile(w, 0.95)
		if len(b.barriers[step]) > 0 {
			if len(b.jobs) > 1 {
				cp.BarrierMS = maxFloat64(b.barriers[step])
			} else {
				cp.BarrierMS = median(b.barriers[step])
			}
			s.BarrierMS = cp.BarrierMS
		}
		if jw := b.stepJobBarrierWS[step]; len(jw) > 1 {
			wsSum, hitRate := sumWSSnapshots(jw)
			cp.WorkingSetHits = wsSum.hits
			cp.WorkingSetMisses = wsSum.misses
			cp.WorkingSetHitRate = hitRate
			cp.StandingPoolSize = wsSum.standingPoolSize
			cp.Created = wsSum.created
			cp.Reused = wsSum.reused
			cp.Retired = wsSum.retired
			cp.PrefetchLeadMarginMS = wsSum.prefetchLeadMarginMS
			cp.ReleaseOverlapMS = wsSum.releaseOverlapMS
			*s = cp
		}
		if len(b.parks[step]) > 0 {
			cp.ParkMS = median(b.parks[step])
			s.ParkMS = cp.ParkMS
		}
		if len(b.lives[step]) > 0 {
			cp.Live = 0
			for _, n := range b.lives[step] {
				cp.Live += int(n)
			}
		}
		if cp.PausedSandboxes == 0 {
			paused := len(b.stepPausedEnvs[step])
			if cp.StandingPoolSize > paused {
				paused = cp.StandingPoolSize
			}
			cp.PausedSandboxes = paused
		}
		rep.Steps = append(rep.Steps, cp)
	}
	sort.Slice(rep.Steps, func(i, j int) bool { return rep.Steps[i].Step < rep.Steps[j].Step })

	peakActive := b.peakActiveEnvs
	if len(b.stepTaskIntervals) > 0 {
		peakActive = 0
	}
	peakPaused := b.peakPausedEnvs
	if rep.StandingPoolSize > peakPaused {
		peakPaused = rep.StandingPoolSize
	}
	for _, st := range rep.Steps {
		if st.Live > peakActive {
			peakActive = st.Live
		}
		if n := rep.stepActiveCounts[st.Step]; n > peakActive {
			peakActive = n
		}
		if st.PausedSandboxes > peakPaused {
			peakPaused = st.PausedSandboxes
		}
		if st.StandingPoolSize > peakPaused {
			peakPaused = st.StandingPoolSize
		}
	}
	if peakActive == 0 {
		peakActive = b.peakActiveEnvs
	}
	rep.PeakActiveSandboxes = peakActive
	rep.PeakParkedSandboxes = peakPaused
	rep.ActiveSandboxes = peakActive
	rep.ParkedSandboxes = peakPaused
	rep.PausedSandboxes = peakPaused

	// Populate per-step rollout decomposition when either step barriers or explicit
	// think/wait decomposition metrics were recorded.
	var rolloutStepNums []int
	seenRolloutStep := map[int]bool{}
	for s := range b.stepRollouts {
		rolloutStepNums = append(rolloutStepNums, s)
		seenRolloutStep[s] = true
	}
	for s := range b.trainerStepMetrics {
		if !seenRolloutStep[s] {
			rolloutStepNums = append(rolloutStepNums, s)
			seenRolloutStep[s] = true
		}
	}
	sort.Ints(rolloutStepNums)
	for _, s := range rolloutStepNums {
		samples := b.stepRollouts[s]
		var barrierMS float64
		if st, ok := b.steps[s]; ok {
			barrierMS = st.BarrierMS
		}
		hasExplicitThink := len(b.stepThinkPool[s]) > 0
		for _, smp := range samples {
			if smp.llmThinkMS > 0 {
				hasExplicitThink = true
				break
			}
		}
		if tm := b.trainerStepMetrics[s]; len(tm) > 0 && !hasExplicitThink {
			if rb, ok := buildTrainerRolloutBreakdown(s, tm, barrierMS); ok {
				if rb.Tasks <= 1 && len(samples) > rb.Tasks && len(b.trainerStepMetrics) <= 1 {
					rb.Tasks = len(samples)
				}
				rep.RolloutBreakdown = append(rep.RolloutBreakdown, rb)
				continue
			}
		}
		if len(samples) > 0 {
			rb := buildStepRolloutBreakdown(s, samples, b.stepThinkPool[s], barrierMS)
			rep.RolloutBreakdown = append(rep.RolloutBreakdown, rb)
		}
	}

	if rep.RunMS <= 0 && !b.minEventStart.IsZero() && !b.maxEventEnd.IsZero() && b.maxEventEnd.After(b.minEventStart) {
		rep.RunMS = round1(float64(b.maxEventEnd.Sub(b.minEventStart).Milliseconds()))
	}

	if len(b.customMetrics) > 0 {
		rep.CustomMetrics = make(map[string]schema.MetricSummary, len(b.customMetrics))
		for k, xs := range b.customMetrics {
			rep.CustomMetrics[k] = makeMetricSummary(xs, "")
		}
	}

	rep.Headline = b.buildHeadlineBaseline(&rep)
	rep.RLBreakdown = b.buildRLStepBreakdown(&rep)
	rep.Convergence = b.buildConvergenceSummary(&rep)
	if b.faults != nil {
		rep.FaultClassification = b.faults.Finish()
	}

	if len(b.jobs) > 0 {
		rep.Jobs = make(map[string]schema.JobReportSummary, len(b.jobs))
		rep.JobStepTasks = make(map[string]map[int]int, len(b.jobs))
		for jobName, ja := range b.jobs {
			var rate float64
			if ja.tasks > 0 {
				rate = float64(ja.rewarded) / float64(ja.tasks)
			}
			js := schema.JobReportSummary{
				Tasks:      ja.tasks,
				Failed:     ja.failed,
				Rewarded:   ja.rewarded,
				RewardRate: rate,
				Steps:      len(ja.steps),
			}
			if ja.faults != nil {
				js.FaultClassification = ja.faults.Finish()
			}
			if len(ja.ttfi) > 0 {
				js.Metrics = map[string]schema.MetricSummary{
					"ttfi_ms": makeMetricSummary(ja.ttfi, "ms"),
				}
			}
			rep.Jobs[jobName] = js
			stepCopy := make(map[int]int, len(ja.steps))
			for stNum, cnt := range ja.steps {
				stepCopy[stNum] = cnt
			}
			rep.JobStepTasks[jobName] = stepCopy
		}
	}

	if len(b.sources) > 0 {
		rep.AcquisitionSources = make(map[string]int, len(b.sources))
		for k, v := range b.sources {
			rep.AcquisitionSources[k] = v
		}
	}

	if len(b.jobs) > 1 {
		rep.Interference = b.buildInterferenceSummary()
	}

	rep.DistinctInstances = len(b.distinctInstances)
	rep.DistinctEnvs = len(b.distinctEnvs)
	if len(b.distinctImages) > 0 {
		rep.DistinctImages = make([]string, 0, len(b.distinctImages))
		for img := range b.distinctImages {
			rep.DistinctImages = append(rep.DistinctImages, img)
		}
		sort.Strings(rep.DistinctImages)
	}
	if len(b.runTags) > 0 {
		rep.RunTags = mergeMultiJobRunTags(b.runTags, b.jobRunTags, len(rep.DistinctImages))
	}

	if rep.DeletedCount == 0 {
		if st, ok := rep.StatFor(EventDelete, ""); ok && st.Count > st.Failed {
			rep.DeletedCount = st.Count - st.Failed
		}
	}
	rep.ParkMode = inferParkMode(&rep)
	rep.LifecyclePolicy = inferLifecyclePolicy(&rep, rep.LifecyclePolicy)

	for msg, n := range b.errs {
		rep.Errors = append(rep.Errors, ErrorCount{Err: msg, Count: n})
	}
	for inst, why := range b.unresolved {
		rep.Unresolved = append(rep.Unresolved, Unresolved{Instance: inst, Reason: why})
	}
	sort.Slice(rep.Unresolved, func(i, j int) bool { return rep.Unresolved[i].Instance < rep.Unresolved[j].Instance })
	sort.Slice(rep.Errors, func(i, j int) bool {
		if rep.Errors[i].Count != rep.Errors[j].Count {
			return rep.Errors[i].Count > rep.Errors[j].Count
		}
		return rep.Errors[i].Err < rep.Errors[j].Err
	})
	return &rep
}

// normalizeDatasetRevision strips container-internal ko baked prefixes (/var/run/ko/)
// while leaving caller-supplied file paths, URLs, and multi-dataset '+' strings intact.
func normalizeDatasetRevision(raw string) string {
	d := strings.TrimSpace(raw)
	if strings.HasPrefix(d, "/var/run/ko/") {
		return filepath.Base(d)
	}
	return d
}

// datasetShortName is the dataset's base name without its .jsonl extension,
// the segment of the merged tag of a multi-shard run
// (r2e-dataset+swe-dataset).
func datasetShortName(raw string) string {
	d := strings.TrimSpace(raw)
	if d == "" {
		return ""
	}
	if qIdx := strings.IndexByte(d, '?'); qIdx >= 0 {
		d = d[:qIdx]
	}
	return strings.TrimSuffix(filepath.Base(d), ".jsonl")
}

// mergeMultiJobRunTags combines per-job EventRun tags when multiple concurrent
// shards (e.g., job="r2e" + job="swe") are summarized into one Report, so
// SystemDescription.Load reflects total fleet load instead of the last shard.
func mergeMultiJobRunTags(base map[string]string, jobTags map[string]map[string]string, distinctImageCount int) map[string]string {
	out := make(map[string]string, len(base))
	for k, v := range base {
		out[k] = v
	}
	if d, ok := out["dataset"]; ok && d != "" {
		out["dataset"] = normalizeDatasetRevision(d)
	}
	if len(jobTags) <= 1 {
		return out
	}

	jobs := make([]string, 0, len(jobTags))
	for j := range jobTags {
		jobs = append(jobs, j)
	}
	sort.Strings(jobs)

	for _, sumKey := range []string{"load_tasks", "load_tasks_per_step", "load_trajectories_per_step", "load_max_concurrent"} {
		sum := 0
		for _, j := range jobs {
			if v, err := strconv.Atoi(jobTags[j][sumKey]); err == nil && v > 0 {
				sum += v
			}
		}
		if sum > 0 {
			out[sumKey] = strconv.Itoa(sum)
		}
	}

	for _, maxKey := range []string{"load_group_size", "load_steps", "load_repeat"} {
		best := 0
		for _, j := range jobs {
			if v, err := strconv.Atoi(jobTags[j][maxKey]); err == nil && v > best {
				best = v
			}
		}
		if best > 0 {
			out[maxKey] = strconv.Itoa(best)
		}
	}

	if distinctImageCount > 0 {
		out["load_distinct_images"] = strconv.Itoa(distinctImageCount)
	} else {
		imgSum := 0
		for _, j := range jobs {
			if v, err := strconv.Atoi(jobTags[j]["load_distinct_images"]); err == nil && v > 0 {
				imgSum += v
			}
		}
		if imgSum > 0 {
			out["load_distinct_images"] = strconv.Itoa(imgSum)
		}
	}

	var datasets []string
	for _, j := range jobs {
		if ds := datasetShortName(jobTags[j]["dataset"]); ds != "" && !slices.Contains(datasets, ds) {
			datasets = append(datasets, ds)
		}
	}
	sort.Strings(datasets)
	if len(datasets) > 0 {
		out["dataset"] = strings.Join(datasets, "+")
	}

	for _, j := range jobs {
		if gi := strings.TrimSpace(jobTags[j]["guest_image"]); gi != "" {
			out["guest_image"] = gi
			break
		}
	}

	return out
}

// inferParkMode is the run's park mode: the loader's park label on the run
// event (Config.labels), else the name of the park event, else what the
// suspend and fork events imply. The label is the only record under
// -park-each, which parks inside the barrier and writes no park event.
func inferParkMode(rep *Report) string {
	if rep == nil {
		return "ephemeral"
	}
	if p := strings.TrimSpace(rep.Labels["park"]); p != "" && p != "none" {
		return p
	}
	for _, s := range rep.Stats {
		if s.Event == EventPark && s.Name != "" && s.Name != "none" && s.Count > 0 {
			return s.Name
		}
	}
	if st, ok := rep.StatFor(EventSuspend, ""); ok && st.Count > 0 {
		return "suspend"
	}
	if st, ok := rep.StatFor(EventFork, ""); ok && st.Count > 0 {
		return "fork"
	}
	return "ephemeral"
}

func inferLifecyclePolicy(rep *Report, existing *schema.SandboxLifecyclePolicy) *schema.SandboxLifecyclePolicy {
	pol := &schema.SandboxLifecyclePolicy{
		Mode:           "ephemeral",
		ParkMode:       "ephemeral",
		PerStepWarming: "none",
	}
	if existing != nil {
		*pol = *existing
	}
	if rep != nil {
		pm := strings.TrimSpace(rep.ParkMode)
		if pm == "" {
			pm = inferParkMode(rep)
		}
		pol.ParkMode = pm
		if rep.PrewarmCount > pol.PrewarmCount {
			pol.PrewarmCount = rep.PrewarmCount
		}
		if pol.PrewarmCount > 0 {
			pol.PrewarmAtStart = true
		}
		hasWarming := pm != "ephemeral" || rep.Reused > 0 || rep.StandingPoolSize > 0 || rep.PausedSandboxes > 0 || pol.IdleAutoParkEnabled
		if st, ok := rep.StatFor(EventWake, ""); ok && st.Count > 0 {
			hasWarming = true
			if pol.PerStepWarming == "" || pol.PerStepWarming == "none" {
				pol.PerStepWarming = "wake_on_demand"
			}
		}
		if st, ok := rep.StatFor(EventFork, ""); ok && st.Count > 0 {
			hasWarming = true
			if pol.PerStepWarming == "" || pol.PerStepWarming == "none" {
				pol.PerStepWarming = "fork_from_snapshot"
			}
		}
		if rep.PrefetchLeadMarginMS > 0 {
			hasWarming = true
			pol.PerStepWarming = "prefetch_batch"
		}
		if hasWarming {
			pol.Mode = "warm_pool"
		} else {
			pol.Mode = "ephemeral"
			if pol.PerStepWarming == "" {
				pol.PerStepWarming = "none"
			}
		}
	}
	if pol.Mode == "" {
		pol.Mode = "ephemeral"
	}
	if pol.ParkMode == "" {
		pol.ParkMode = "ephemeral"
	}
	if pol.PerStepWarming == "" {
		pol.PerStepWarming = "none"
	}
	return pol
}

func buildStepRolloutBreakdown(step int, samples []taskRolloutSample, extraThinks []float64, barrierWallMS float64) StepRolloutBreakdown {
	n := len(samples)
	cp := make([]taskRolloutSample, n)
	copy(cp, samples)

	if len(extraThinks) > 0 {
		var extraSum float64
		for _, v := range extraThinks {
			extraSum += v
		}
		perTaskExtra := extraSum / float64(n)
		for i := range cp {
			if cp[i].llmThinkMS == 0 && perTaskExtra > 0 {
				think := math.Min(cp[i].durMS, perTaskExtra)
				cp[i].llmThinkMS = think
				cp[i].envWaitMS = math.Max(0, cp[i].durMS-think)
			}
		}
	}

	sort.Slice(cp, func(i, j int) bool {
		return cp[i].durMS < cp[j].durMS
	})

	durs := make([]float64, n)
	var sumRollout, sumThink, sumWait float64
	for i, s := range cp {
		durs[i] = s.durMS
		sumRollout += s.durMS
		sumThink += s.llmThinkMS
		sumWait += s.envWaitMS
	}

	p50 := percentile(durs, 0.50)
	p90 := percentile(durs, 0.90)
	p95 := percentile(durs, 0.95)
	p99 := percentile(durs, 0.99)
	p100 := durs[n-1]

	var skew float64
	if p50 > 0 {
		skew = round2(p100 / p50)
	}
	barrier := barrierWallMS
	if barrier <= 0 {
		barrier = p100
	}

	medIdx := int(math.Ceil(0.50*float64(n))) - 1
	if medIdx < 0 {
		medIdx = 0
	}
	thinkP50 := cp[medIdx].llmThinkMS
	waitP50 := cp[medIdx].envWaitMS
	thinkP100 := cp[n-1].llmThinkMS
	waitP100 := cp[n-1].envWaitMS

	meanThink := sumThink / float64(n)
	meanWait := sumWait / float64(n)

	var envOverhead, envOverheadPct float64
	if sumRollout > 0 {
		envOverhead = round4(sumWait / sumRollout)
		envOverheadPct = round1(100.0 * sumWait / sumRollout)
	}

	return StepRolloutBreakdown{
		Step:           step,
		Tasks:          n,
		P50MS:          round1(p50),
		P90MS:          round1(p90),
		P95MS:          round1(p95),
		P99MS:          round1(p99),
		P100MS:         round1(p100),
		StragglerSkew:  skew,
		BarrierMS:      round1(barrier),
		LLMThinkMS:     round1(meanThink),
		EnvWaitMS:      round1(meanWait),
		LLMThinkP50MS:  round1(thinkP50),
		LLMThinkP100MS: round1(thinkP100),
		EnvWaitP50MS:   round1(waitP50),
		EnvWaitP100MS:  round1(waitP100),
		EnvOverhead:    envOverhead,
		EnvOverheadPct: envOverheadPct,
	}
}

func buildTrainerRolloutBreakdown(step int, tm map[string]float64, barrierWallMS float64) (StepRolloutBreakdown, bool) {
	p50S, hasP50 := tm["fully_async/processing_time/tp50"]
	if !hasP50 || p50S <= 0 {
		p50S, hasP50 = tm["timing_s/gen"]
	}
	if !hasP50 || p50S <= 0 {
		return StepRolloutBreakdown{}, false
	}
	p50MS := p50S * 1000.0
	p95MS := p50MS
	if v, ok := tm["fully_async/processing_time/tp95"]; ok && v > 0 {
		p95MS = v * 1000.0
	} else if v, ok := tm["fully_async/processing_time/tp99"]; ok && v > 0 {
		p95MS = v * 1000.0
	}
	p99MS := p95MS
	if v, ok := tm["fully_async/processing_time/tp99"]; ok && v > 0 {
		p99MS = v * 1000.0
	}
	p100MS := p99MS
	if v, ok := tm["fully_async/processing_time/max"]; ok && v > 0 {
		p100MS = v * 1000.0
	}
	var skew float64
	if p50MS > 0 {
		skew = round2(p100MS / p50MS)
	}
	envWaitMS := tm["timing_s/agent_loop/tool_calls/mean"] * 1000.0
	envWaitP100MS := tm["timing_s/agent_loop/tool_calls/max"] * 1000.0
	thinkMS := math.Max(0, p50MS-envWaitMS)
	if genMean, ok := tm["timing_s/agent_loop/generate_sequences/mean"]; ok && genMean > 0 && envWaitMS == 0 {
		thinkMS = genMean * 1000.0
	}
	thinkP100MS := math.Max(0, p100MS-envWaitP100MS)
	var envOverhead, envOverheadPct float64
	if p50MS > 0 {
		envOverhead = round4(envWaitMS / p50MS)
		envOverheadPct = round1(100.0 * envWaitMS / p50MS)
	}
	barrier := barrierWallMS
	if barrier <= 0 {
		if stS, ok := tm["timing_s/step"]; ok && stS > 0 {
			barrier = stS * 1000.0
		} else {
			barrier = p100MS
		}
	}
	tasks := int(tm["fully_async/count/current_step_samples"])
	if tasks <= 0 {
		tasks = int(tm["fully_async/static/required_samples"])
	}
	if tasks <= 0 {
		tasks = 1
	}
	return StepRolloutBreakdown{
		Step:           step,
		Tasks:          tasks,
		P50MS:          round1(p50MS),
		P90MS:          round1(p95MS),
		P95MS:          round1(p95MS),
		P99MS:          round1(p99MS),
		P100MS:         round1(p100MS),
		StragglerSkew:  skew,
		BarrierMS:      round1(barrier),
		LLMThinkMS:     round1(thinkMS),
		EnvWaitMS:      round1(envWaitMS),
		LLMThinkP50MS:  round1(thinkMS),
		LLMThinkP100MS: round1(thinkP100MS),
		EnvWaitP50MS:   round1(envWaitMS),
		EnvWaitP100MS:  round1(envWaitP100MS),
		EnvOverhead:    envOverhead,
		EnvOverheadPct: envOverheadPct,
	}, true
}

func (b *Builder) buildHeadlineBaseline(rep *Report) *HeadlineBaseline {
	hb := &HeadlineBaseline{
		TasksPerMin: rep.ThroughputTasksPerMin(),
	}
	if st, ok := rep.StatFor(EventTTFI, ""); ok {
		hb.TTFIP50MS = st.P50MS
		hb.TTFIP90MS = st.P90MS
		hb.TTFIP95MS = st.P95MS
		hb.TTFIP99MS = st.P99MS
		hb.TTFIMaxMS = st.MaxMS
	}
	if cm, ok := rep.CustomMetrics["sdk_slot_wait_ms"]; ok && cm.Count > 0 {
		hb.SDKSlotWaitP50MS = cm.P50
		hb.SDKSlotWaitP95MS = cm.P95
		hb.SDKSlotWaitP99MS = cm.P99
	} else if len(b.slotWaits) > 0 {
		sw := append([]float64(nil), b.slotWaits...)
		sort.Float64s(sw)
		hb.SDKSlotWaitP50MS = round1(percentile(sw, 0.50))
		hb.SDKSlotWaitP95MS = round1(percentile(sw, 0.95))
		hb.SDKSlotWaitP99MS = round1(percentile(sw, 0.99))
	}
	if cm, ok := rep.CustomMetrics["wire_claim_to_ready_ms"]; ok && cm.Count > 0 {
		hb.WireClaimToReadyP50MS = cm.P50
		hb.WireClaimToReadyP95MS = cm.P95
		hb.WireClaimToReadyP99MS = cm.P99
	}
	if cm, ok := rep.CustomMetrics["ready_to_first_cmd_ms"]; ok && cm.Count > 0 {
		hb.ReadyToFirstCmdP50MS = cm.P50
		hb.ReadyToFirstCmdP95MS = cm.P95
		hb.ReadyToFirstCmdP99MS = cm.P99
	}
	if st, ok := rep.StatFor(EventCreate, ""); ok {
		hb.CreateP50MS = st.P50MS
		hb.CreateP95MS = st.P95MS
		hb.CreateP99MS = st.P99MS
		if hb.WireClaimToReadyP50MS == 0 {
			hb.WireClaimToReadyP50MS = st.P50MS
			hb.WireClaimToReadyP95MS = st.P95MS
			hb.WireClaimToReadyP99MS = st.P99MS
		}
	}
	if st, ok := rep.StatFor(EventWake, ""); ok {
		hb.WakeP50MS = st.P50MS
		hb.WakeP95MS = st.P95MS
		hb.WakeP99MS = st.P99MS
	}
	for _, execName := range []struct {
		ev   string
		name string
	}{
		{EventStep, "exec"},
		{EventStep, "agent_exec"},
		{EventPhase, "agent_exec"},
		{EventStep, "patch"},
		{EventStep, "setup"},
	} {
		if st, ok := rep.StatFor(execName.ev, execName.name); ok && st.Count > 0 {
			hb.ExecP50MS = st.P50MS
			hb.ExecP95MS = st.P95MS
			hb.ExecP99MS = st.P99MS
			break
		}
	}
	if st, ok := rep.StatFor(EventSuspend, ""); ok {
		hb.SuspendP50MS = st.P50MS
		hb.SuspendP95MS = st.P95MS
		hb.SuspendP99MS = st.P99MS
		hb.SuspendSizeMBP50 = st.SizeMBP50
		hb.SuspendSizeMBP95 = st.SizeMBP95
		hb.CheckpointMBS = st.CheckpointMBS
	}
	if st, ok := rep.StatFor(EventDelete, ""); ok {
		hb.DeleteP50MS = st.P50MS
		hb.DeleteP95MS = st.P95MS
		hb.DeleteP99MS = st.P99MS
	}

	claims := b.createTimes
	if len(claims) == 0 {
		claims = b.claimTimes
	}
	hb.ClaimsCount = len(claims)
	if len(claims) > 0 {
		sort.Slice(claims, func(i, j int) bool { return claims[i].Before(claims[j]) })
		runSec := rep.RunMS / 1000.0
		if runSec <= 0 && len(claims) > 1 {
			runSec = claims[len(claims)-1].Sub(claims[0]).Seconds()
		}
		if runSec > 0 {
			hb.SustainedClaimsPerSec = round2(float64(len(claims)) / runSec)
		}
		var maxInWindow int
		for i := range claims {
			winEnd := claims[i].Add(time.Second)
			cnt := 0
			for j := i; j < len(claims) && !claims[j].After(winEnd); j++ {
				cnt++
			}
			if cnt > maxInWindow {
				maxInWindow = cnt
			}
		}
		hb.PeakClaimsPerSec = float64(maxInWindow)
		if hb.SustainedClaimsPerSec > hb.PeakClaimsPerSec {
			hb.PeakClaimsPerSec = hb.SustainedClaimsPerSec
		}
	}
	return hb
}

func (b *Builder) buildRLStepBreakdown(rep *Report) *RLStepBreakdown {
	rb := &RLStepBreakdown{
		StepsRecorded: len(rep.Steps),
	}
	if st, ok := rep.StatFor(EventStep, ""); ok && st.Count > 0 {
		rb.StepTotalP50MS = st.P50MS
		rb.StepTotalP95MS = st.P95MS
		rb.StepTotalMaxMS = st.MaxMS
	} else if st, ok := rep.StatFor(EventBarrier, ""); ok && st.Count > 0 {
		rb.StepTotalP50MS = st.P50MS
		rb.StepTotalP95MS = st.P95MS
		rb.StepTotalMaxMS = st.MaxMS
	}
	if cm, ok := rep.CustomMetrics["timing_s/gen"]; ok && cm.Count > 0 {
		rb.RolloutGenP50MS = round1(cm.P50 * 1000.0)
		rb.RolloutGenP95MS = round1(cm.P95 * 1000.0)
		rb.RolloutGenMaxMS = round1(cm.Max * 1000.0)
	}
	if cm, ok := rep.CustomMetrics["timing_s/update_actor"]; ok && cm.Count > 0 {
		rb.UpdateActorP50MS = round1(cm.P50 * 1000.0)
		rb.UpdateActorP95MS = round1(cm.P95 * 1000.0)
	}
	if cm, ok := rep.CustomMetrics["timing_s/old_log_prob"]; ok && cm.Count > 0 {
		rb.OldLogProbP50MS = round1(cm.P50 * 1000.0)
		rb.OldLogProbP95MS = round1(cm.P95 * 1000.0)
	}
	if cm, ok := rep.CustomMetrics["timing_s/param_sync"]; ok && cm.Count > 0 {
		rb.ParamSyncP50MS = round1(cm.P50 * 1000.0)
		rb.ParamSyncP95MS = round1(cm.P95 * 1000.0)
	} else if cm, ok := rep.CustomMetrics["timing_s/update_weights"]; ok && cm.Count > 0 {
		rb.ParamSyncP50MS = round1(cm.P50 * 1000.0)
		rb.ParamSyncP95MS = round1(cm.P95 * 1000.0)
	}
	if cm, ok := rep.CustomMetrics["timing_s/reward"]; ok && cm.Count > 0 {
		rb.RewardP50MS = round4(cm.P50 * 1000.0)
	}
	if cm, ok := rep.CustomMetrics["timing_s/adv"]; ok && cm.Count > 0 {
		rb.AdvP50MS = round4(cm.P50 * 1000.0)
	}
	if cm, ok := rep.CustomMetrics["timing_s/testing"]; ok && cm.Count > 0 {
		rb.TestingP50MS = round1(cm.P50 * 1000.0)
		rb.TestingP95MS = round1(cm.P95 * 1000.0)
	}
	if cm, ok := rep.CustomMetrics["perf/throughput"]; ok && cm.Count > 0 {
		rb.TokensPerSecP50 = round2(cm.P50)
		rb.TokensPerSecP95 = round2(cm.P95)
	}
	if cm, ok := rep.CustomMetrics["perf/mfu/actor"]; ok && cm.Count > 0 {
		rb.MFUActorP50 = round4(cm.P50)
	}
	if len(rep.RolloutBreakdown) > 0 {
		var p50s, p95s, maxs, envWaits, envWaitP100s, thinks, thinkP100s, overheads, skews []float64
		for _, r := range rep.RolloutBreakdown {
			p50s = append(p50s, r.P50MS)
			p95s = append(p95s, r.P95MS)
			maxs = append(maxs, r.P100MS)
			envWaits = append(envWaits, r.EnvWaitMS)
			envWaitP100s = append(envWaitP100s, r.EnvWaitP100MS)
			thinks = append(thinks, r.LLMThinkMS)
			thinkP100s = append(thinkP100s, r.LLMThinkP100MS)
			overheads = append(overheads, r.EnvOverheadPct)
			skews = append(skews, r.StragglerSkew)
		}
		sort.Float64s(p50s)
		sort.Float64s(p95s)
		sort.Float64s(maxs)
		sort.Float64s(envWaits)
		sort.Float64s(envWaitP100s)
		sort.Float64s(thinks)
		sort.Float64s(thinkP100s)
		sort.Float64s(overheads)
		sort.Float64s(skews)
		if rb.RolloutGenP50MS == 0 {
			rb.RolloutGenP50MS = round1(percentile(p50s, 0.50))
			rb.RolloutGenP95MS = round1(percentile(p95s, 0.95))
			rb.RolloutGenMaxMS = round1(percentile(maxs, 1.0))
		}
		rb.EnvWaitP50MS = round1(percentile(envWaits, 0.50))
		rb.EnvWaitP100MS = round1(percentile(envWaitP100s, 1.0))
		rb.LLMThinkP50MS = round1(percentile(thinks, 0.50))
		rb.LLMThinkP100MS = round1(percentile(thinkP100s, 1.0))
		rb.EnvOverheadPct = round1(percentile(overheads, 0.50))
		rb.StragglerSkewP100P50 = round2(percentile(skews, 0.50))
	}

	switch {
	case rb.LLMThinkP50MS > rb.EnvWaitP50MS && rb.RolloutGenP50MS >= rb.UpdateActorP50MS:
		rb.DominantBottleneck = fmt.Sprintf("llm_inference_and_straggler_turns (llm_think=%.1fms vs env_wait=%.1fms, skew=%.2fx)", rb.LLMThinkP50MS, rb.EnvWaitP50MS, rb.StragglerSkewP100P50)
	case rb.EnvWaitP50MS >= rb.LLMThinkP50MS && rb.EnvWaitP50MS > 0:
		rb.DominantBottleneck = fmt.Sprintf("sandbox_environment_wait (env_wait=%.1fms, env_overhead=%.1f%%)", rb.EnvWaitP50MS, rb.EnvOverheadPct)
	case rb.UpdateActorP50MS > rb.RolloutGenP50MS:
		rb.DominantBottleneck = fmt.Sprintf("trainer_actor_update (update_actor=%.1fms vs rollout_gen=%.1fms)", rb.UpdateActorP50MS, rb.RolloutGenP50MS)
	default:
		rb.DominantBottleneck = "sandbox_execution"
	}
	return rb
}

func (b *Builder) buildConvergenceSummary(rep *Report) *ConvergenceSummary {
	if len(b.trainerStepMetrics) == 0 && len(rep.Steps) == 0 {
		return nil
	}
	var stepNums []int
	seen := map[int]bool{}
	for s := range b.trainerStepMetrics {
		stepNums = append(stepNums, s)
		seen[s] = true
	}
	for _, st := range rep.Steps {
		if !seen[st.Step] {
			stepNums = append(stepNums, st.Step)
			seen[st.Step] = true
		}
	}
	sort.Ints(stepNums)

	var curve []StepConvergencePoint
	var cumWallS float64
	var peakReward float64
	peakStep := 0

	for i, s := range stepNums {
		tm := b.trainerStepMetrics[s]
		stepTimeS := b.trainerStepDurMS[s] / 1000.0
		if v, ok := tm["timing_s/step"]; ok && v > 0 {
			stepTimeS = v
		} else if st, ok := b.steps[s]; ok && st.BarrierMS > 0 && stepTimeS <= 0 {
			stepTimeS = st.BarrierMS / 1000.0
		}
		cumWallS += stepTimeS

		var rewardMean float64
		if v, ok := tm["critic/score/mean"]; ok {
			rewardMean = v
		} else if v, ok := tm["critic/rewards/mean"]; ok {
			rewardMean = v
		} else if st, ok := b.steps[s]; ok && st.Tasks > 0 {
			rewardMean = float64(st.Rewarded) / float64(st.Tasks)
		}

		if i == 0 || rewardMean > peakReward {
			peakReward = rewardMean
			peakStep = s
		}

		klLoss := tm["actor/kl_loss"]
		if klLoss == 0 {
			klLoss = tm["actor/ppo_kl"]
		}
		var valCoreAcc *float64
		if v, ok := tm["val-core/r2e_gym/acc/mean@1"]; ok {
			vc := round4(v)
			valCoreAcc = &vc
		}
		var valMetrics map[string]float64
		for k, v := range tm {
			if strings.HasPrefix(k, "val-core/") || strings.HasPrefix(k, "val-aux/") {
				if valMetrics == nil {
					valMetrics = map[string]float64{}
				}
				valMetrics[k] = round4(v)
				if valCoreAcc == nil && strings.HasPrefix(k, "val-core/") && strings.Contains(k, "acc/mean@1") {
					vc := round4(v)
					valCoreAcc = &vc
				}
			}
		}
		pt := StepConvergencePoint{
			Step:              s,
			StepTimeS:         round2(stepTimeS),
			WallElapsedS:      round2(cumWallS),
			RewardMean:        round4(rewardMean),
			ValScore:          round4(extractValScore(tm)),
			ValCoreAccMean1:   valCoreAcc,
			ValMetrics:        valMetrics,
			KLLoss:            round4(klLoss),
			PGLoss:            round4(tm["actor/pg_loss"]),
			GradNorm:          round4(tm["actor/grad_norm"]),
			Entropy:           round4(tm["actor/entropy"]),
			ResponseLenMean:   round1(tm["response_length/mean"]),
			ThroughputTokPerS: round2(tm["perf/throughput"]),
			MFUActor:          round4(tm["perf/mfu/actor"]),
		}
		curve = append(curve, pt)
	}

	if len(curve) == 0 {
		return nil
	}
	var trainCurve []StepConvergencePoint
	for _, pt := range curve {
		if pt.Step > 0 || pt.RewardMean > 0 {
			trainCurve = append(trainCurve, pt)
		}
	}
	if len(trainCurve) == 0 {
		trainCurve = curve
	}
	initR := trainCurve[0].RewardMean
	finalR := trainCurve[len(trainCurve)-1].RewardMean
	delta := round4(finalR - initR)

	// Detect whether convergence was genuinely reached within the measured steps:
	// require at least 10 training steps and a 5-step plateau with std < 0.02 and
	// positive improvement >= +0.10 (or reward >= 0.85). Never guess or extrapolate!
	converged := false
	var convStep *int
	var convWallS *float64
	if len(trainCurve) >= 10 {
		for idx := 4; idx < len(trainCurve); idx++ {
			var sum float64
			for k := idx - 4; k <= idx; k++ {
				sum += trainCurve[k].RewardMean
			}
			mean := sum / 5.0
			var sq float64
			for k := idx - 4; k <= idx; k++ {
				d := trainCurve[k].RewardMean - mean
				sq += d * d
			}
			std := math.Sqrt(sq / 5.0)
			if std < 0.02 && (mean-initR >= 0.10 || mean >= 0.85) {
				converged = true
				stVal := trainCurve[idx].Step
				wVal := trainCurve[idx].WallElapsedS
				convStep = &stVal
				convWallS = &wVal
				break
			}
		}
	}

	var status string
	if converged && convStep != nil && convWallS != nil {
		status = fmt.Sprintf("CONVERGED at step %d (wall_time=%.1fs, reward=%.4f)", *convStep, *convWallS, finalR)
	} else {
		status = fmt.Sprintf("NOT REACHED (ran %d steps / %.1fs; initial_reward=%.4f -> final_reward=%.4f, peak_reward=%.4f at step %d)",
			len(trainCurve), cumWallS, initR, finalR, peakReward, peakStep)
	}

	return &ConvergenceSummary{
		TotalStepsRun:          len(trainCurve),
		InitialReward:          initR,
		FinalReward:            finalR,
		PeakReward:             round4(peakReward),
		PeakStep:               peakStep,
		RewardDelta:            delta,
		Converged:              converged,
		ConvergenceStatus:      status,
		StepsToConvergence:     convStep,
		WallTimeToConvergenceS: convWallS,
		Curve:                  curve,
	}
}

// PopulateStepChurnAndConcurrency populates per-step ActiveSandboxes, Live,
// Created, Reused, Retired, WorkingSetHits, WorkingSetMisses, and
// WorkingSetHitRate (and rolls them up to top-level Report fields when absent).
func (rep *Report) PopulateStepChurnAndConcurrency(defaultActive int) {
	if rep == nil {
		return
	}
	var sumCreated, sumReused, sumRetired, sumHits, sumMisses int
	for i := range rep.Steps {
		stNum := rep.Steps[i].Step
		if rep.Steps[i].ActiveSandboxes == 0 {
			if n := rep.stepActiveCounts[stNum]; n > 0 {
				rep.Steps[i].ActiveSandboxes = n
			} else if rep.Steps[i].Live > 0 {
				rep.Steps[i].ActiveSandboxes = rep.Steps[i].Live
			} else if defaultActive > 0 {
				rep.Steps[i].ActiveSandboxes = defaultActive
			}
		}
		if defaultActive > 0 && rep.Steps[i].ActiveSandboxes == rep.Steps[i].Tasks && rep.Steps[i].Tasks > defaultActive*2 {
			rep.Steps[i].ActiveSandboxes = defaultActive
		}
		if rep.Steps[i].Live == 0 && rep.Steps[i].ActiveSandboxes > 0 {
			rep.Steps[i].Live = rep.Steps[i].ActiveSandboxes
		}
		if rep.Steps[i].PausedSandboxes == 0 && rep.Steps[i].StandingPoolSize > 0 {
			rep.Steps[i].PausedSandboxes = rep.Steps[i].StandingPoolSize
		}
		if rep.Steps[i].Created == 0 && rep.stepCreatedCounts[stNum] > 0 {
			rep.Steps[i].Created = rep.stepCreatedCounts[stNum]
		}
		if rep.Steps[i].Reused == 0 && rep.stepReusedCounts[stNum] > 0 {
			rep.Steps[i].Reused = rep.stepReusedCounts[stNum]
		}
		if rep.Steps[i].Created == 0 && rep.Steps[i].Reused == 0 && rep.Steps[i].Tasks > 0 {
			rep.Steps[i].Created = rep.Steps[i].Tasks
		}
		if rep.Steps[i].Retired == 0 {
			if rep.stepRetiredCounts[stNum] > 0 {
				rep.Steps[i].Retired = rep.stepRetiredCounts[stNum]
			} else if strings.HasPrefix(rep.ParkMode, "ephemeral") && rep.Steps[i].Created > 0 {
				rep.Steps[i].Retired = rep.Steps[i].Created
			}
		}
		if rep.Steps[i].WorkingSetHits == 0 && rep.Steps[i].WorkingSetMisses == 0 {
			h := rep.stepCacheHits[stNum]
			m := rep.stepCacheMisses[stNum]
			if h+m > 0 {
				rep.Steps[i].WorkingSetHits = h
				rep.Steps[i].WorkingSetMisses = m
				rep.Steps[i].WorkingSetHitRate = round4(float64(h) / float64(h+m))
			}
		}
		sumCreated += rep.Steps[i].Created
		sumReused += rep.Steps[i].Reused
		sumRetired += rep.Steps[i].Retired
		sumHits += rep.Steps[i].WorkingSetHits
		sumMisses += rep.Steps[i].WorkingSetMisses
	}
	if rep.Created == 0 && rep.Reused == 0 && (sumCreated > 0 || sumReused > 0) {
		rep.Created = sumCreated
		if st, ok := rep.StatFor(EventCreate, ""); ok && st.Count-st.Failed > rep.Created {
			rep.Created = st.Count - st.Failed
		}
		rep.Reused = sumReused
		rep.Retired = sumRetired
		if strings.HasPrefix(rep.ParkMode, "ephemeral") && rep.Retired < rep.Created {
			rep.Retired = rep.Created
		}
	}
	if rep.WorkingSetHits == 0 && rep.WorkingSetMisses == 0 {
		if rep.Cache != nil && (rep.Cache.Hits > 0 || rep.Cache.Misses > 0) {
			rep.WorkingSetHits = int(rep.Cache.Hits)
			rep.WorkingSetMisses = int(rep.Cache.Misses)
			rep.WorkingSetHitRate = round4(rep.Cache.Hits / (rep.Cache.Hits + rep.Cache.Misses))
		} else if sumHits > 0 || sumMisses > 0 {
			rep.WorkingSetHits = sumHits
			rep.WorkingSetMisses = sumMisses
			rep.WorkingSetHitRate = round4(float64(sumHits) / float64(sumHits+sumMisses))
		}
	}
}

// AttachClusterTelemetry populates rep.HardwareCost (and updates accelerator-hours
// to convergence if applicable) from measurements.json and system_description.json.
func (rep *Report) AttachClusterTelemetry(sys *schema.SystemDescription, meas *schema.Measurements) {
	if rep == nil || (sys == nil && meas == nil) {
		return
	}
	hc := &HardwareCostSummary{}
	if meas != nil && meas.ServerSummary != nil {
		ss := meas.ServerSummary
		hc.ActiveSandboxes = int(ss["active_sandboxes"].P50)
		hc.ParkedSandboxes = int(ss["parked_sandboxes"].P50)
		if hc.ParkedSandboxes == 0 {
			hc.ParkedSandboxes = int(ss["paused_sandboxes"].P50)
		}
		hc.TotalSandboxes = int(ss["total_sandboxes"].P50)
		liveParkedFromServer := hc.ParkedSandboxes > 0
		if hc.ParkedSandboxes == 0 {
			maxParked := rep.StandingPoolSize
			if rep.PeakParkedSandboxes > maxParked {
				maxParked = rep.PeakParkedSandboxes
			}
			for _, st := range rep.Steps {
				if st.StandingPoolSize > maxParked {
					maxParked = st.StandingPoolSize
				}
				if st.PausedSandboxes > maxParked {
					maxParked = st.PausedSandboxes
				}
			}
			hc.ParkedSandboxes = maxParked
		}
		hc.PausedSandboxes = hc.ParkedSandboxes
		hc.PeakActiveSandboxes = rep.PeakActiveSandboxes
		if int(ss["active_sandboxes"].Max) > hc.PeakActiveSandboxes {
			hc.PeakActiveSandboxes = int(ss["active_sandboxes"].Max)
		}
		if hc.ActiveSandboxes > hc.PeakActiveSandboxes {
			hc.PeakActiveSandboxes = hc.ActiveSandboxes
		}
		hc.PeakParkedSandboxes = rep.PeakParkedSandboxes
		if int(ss["parked_sandboxes"].Max) > hc.PeakParkedSandboxes {
			hc.PeakParkedSandboxes = int(ss["parked_sandboxes"].Max)
		}
		if hc.ParkedSandboxes > hc.PeakParkedSandboxes {
			hc.PeakParkedSandboxes = hc.ParkedSandboxes
		}
		if !liveParkedFromServer {
			fleetTotal := hc.TotalSandboxes
			if rep.DistinctEnvs > fleetTotal && !strings.HasPrefix(rep.ParkMode, "ephemeral") {
				fleetTotal = rep.DistinctEnvs
			}
			if hc.ActiveSandboxes > fleetTotal {
				fleetTotal = hc.ActiveSandboxes
			}
			if hc.ParkedSandboxes > fleetTotal {
				fleetTotal = hc.ParkedSandboxes
			}
			hc.TotalSandboxes = fleetTotal
		} else if hc.ActiveSandboxes+hc.ParkedSandboxes > hc.TotalSandboxes {
			hc.TotalSandboxes = hc.ActiveSandboxes + hc.ParkedSandboxes
		}
		rep.ActiveSandboxes = hc.ActiveSandboxes
		rep.ParkedSandboxes = hc.ParkedSandboxes
		rep.PausedSandboxes = hc.PausedSandboxes
		rep.PeakActiveSandboxes = hc.PeakActiveSandboxes
		rep.PeakParkedSandboxes = hc.PeakParkedSandboxes
		hc.MaxConcurrentSandboxesCap = int(ss["max_concurrent_sandboxes_cap"].P50)
		hc.WorkerPodCount = int(ss["worker_pod_count"].P50)
		hc.WorkerActorCapacity = int(ss["worker_actor_capacity"].P50)
		hc.WorkerNodeCount = int(ss["worker_node_count"].P50)
		hc.SystemNodeCount = int(ss["system_node_count"].P50)
		hc.GPUNodeCount = int(ss["gpu_node_count"].P50)
		hc.TotalGPUs = int(ss["total_gpus"].P50)
		hc.WorkerTotalVCPU = ss["worker_total_vcpu"].P50
		hc.WorkerAllocatableVCPU = ss["worker_allocatable_vcpu"].P50
		hc.WorkerTotalGiB = ss["worker_total_gib"].P50
		hc.WorkerAllocatableGiB = ss["worker_allocatable_gib"].P50
		if spn, ok := ss["sandboxes_per_node"]; ok {
			hc.SandboxesPerNodeP50 = spn.P50
			hc.SandboxesPerNodeP90 = spn.P90
			hc.SandboxesPerNodeP99 = spn.P99
			hc.SandboxesPerNodeMax = spn.Max
		}
		hc.SandboxesPerVCPU = ss["sandboxes_per_vcpu"].P50
		hc.SandboxesPerGiBRAM = ss["sandboxes_per_gib_ram"].P50
		hc.ClusterPackingCPUPct = ss["cluster_packing_cpu_pct"].P50
		hc.ClusterPackingMemPct = ss["cluster_packing_mem_pct"].P50
		if u, ok := ss["worker_node_cpu_util_pct"]; ok {
			hc.WorkerNodeCPUUtilP50Pct = u.P50
			hc.WorkerNodeCPUUtilMaxPct = u.Max
		}
		if u, ok := ss["worker_node_mem_util_pct"]; ok {
			hc.WorkerNodeMemUtilP50Pct = u.P50
			hc.WorkerNodeMemUtilMaxPct = u.Max
		}
		if u, ok := ss["sandbox_pod_cpu_mcores"]; ok {
			hc.SandboxPodCPUP50MCores = u.P50
			hc.SandboxPodCPUMaxMCores = u.Max
		}
		if u, ok := ss["sandbox_pod_wss_mib"]; ok {
			hc.SandboxPodWSSP50MiB = u.P50
			hc.SandboxPodWSSMaxMiB = u.Max
		}
		if ps, ok := ss["pod_schedule_ms"]; ok {
			hc.PodScheduleP50MS = ps.P50
			hc.PodScheduleP95MS = ps.P95
		}
		if pi, ok := ss["pod_init_ms"]; ok {
			hc.PodInitP50MS = pi.P50
			hc.PodInitP95MS = pi.P95
		}
		if cs, ok := ss["pod_container_start_ms"]; ok {
			hc.PodContainerStartP50MS = cs.P50
			hc.PodContainerStartP95MS = cs.P95
		}
		if tr, ok := ss["pod_total_ready_ms"]; ok {
			hc.PodTotalReadyP50MS = tr.P50
			hc.PodTotalReadyP95MS = tr.P95
		}
		hc.OOMKills = int(math.Max(ss["oom_kills"].P50, ss["oom_kills"].Max))
		hc.PodEvictions = int(math.Max(ss["pod_evictions"].P50, ss["pod_evictions"].Max))
		hc.ContainerRestarts = int(math.Max(ss["container_restarts"].P50, ss["container_restarts"].Max))

		if wss, ok := ss["sandbox_pod_wss_mib"]; ok && wss.P50 > 0 {
			for i := range rep.Stats {
				if rep.Stats[i].Event == EventSuspend && rep.Stats[i].SizeMBP50 == 0 && rep.Stats[i].Count > 0 {
					rep.Stats[i].SizeMBP50 = round2(wss.P50)
					p95 := wss.P90
					if wss.P95 > 0 {
						p95 = wss.P95
					} else if p95 == 0 {
						p95 = wss.Max
					}
					rep.Stats[i].SizeMBP95 = round2(p95)
					rep.Stats[i].SizeMBMax = round2(wss.Max)
					if rep.Stats[i].P50MS > 0 {
						rep.Stats[i].CheckpointMBS = round2(rep.Stats[i].SizeMBP50 / (rep.Stats[i].P50MS / 1000.0))
					}
					if rep.Headline != nil && rep.Stats[i].Name == "" {
						rep.Headline.SuspendSizeMBP50 = rep.Stats[i].SizeMBP50
						rep.Headline.SuspendSizeMBP95 = rep.Stats[i].SizeMBP95
						rep.Headline.CheckpointMBS = rep.Stats[i].CheckpointMBS
					}
				}
			}
		}

		if rep.Cache == nil {
			_, hasHits := ss["cache_hits"]
			_, hasMisses := ss["cache_misses"]
			hasExplicitSnapshotDelta := meas.CacheBeforeRef != "" && meas.CacheAfterRef != ""
			evHits := rep.CreateCacheHits
			evMisses := rep.CreateCacheMisses
			if evHits+evMisses == 0 {
				evHits = rep.inferredCacheHits
				evMisses = rep.inferredCacheMisses
			}
			if !hasExplicitSnapshotDelta && evHits+evMisses > 0 {
				nodesWithMisses := 0
				if evMisses > 0 {
					nodesWithMisses = hc.WorkerNodeCount
					if nodesWithMisses == 0 || evMisses < nodesWithMisses {
						nodesWithMisses = 1
					}
				}
				rep.Cache = &CacheDelta{
					Nodes:           hc.WorkerNodeCount,
					Hits:            float64(evHits),
					Misses:          float64(evMisses),
					NodesWithMisses: nodesWithMisses,
				}
				rep.PopulateCacheMissesPerFirstCmd()
			} else if hasHits || hasMisses {
				hits := ss["cache_hits"].P50
				misses := ss["cache_misses"].P50
				nodesWithMisses := int(ss["cache_nodes_with_misses"].P50)
				if misses > 0 && nodesWithMisses == 0 {
					nodesWithMisses = 1
				}
				rep.Cache = &CacheDelta{
					Nodes:           hc.WorkerNodeCount,
					Hits:            hits,
					Misses:          misses,
					NodesWithMisses: nodesWithMisses,
				}
				rep.PopulateCacheMissesPerFirstCmd()
			}
		}
		rep.PopulateStepChurnAndConcurrency(hc.ActiveSandboxes)

		if rep.Restores == nil {
			rep.Restores = buildRestoreBreakdownFromSummary(rep, ss)
		}
	}

	taskDenom := rep.Tasks
	if taskDenom <= 0 && rep.Headline != nil && rep.Headline.ClaimsCount > 0 {
		taskDenom = rep.Headline.ClaimsCount
	}
	runHours := (rep.RunMS / 1000.0) / 3600.0
	if runHours > 0 && taskDenom > 0 {
		if hc.WorkerNodeCount > 0 {
			hc.WorkerNodeHoursPerTask = math.Round((float64(hc.WorkerNodeCount)*runHours/float64(taskDenom))*1e6) / 1e6
		}
		if hc.SystemNodeCount > 0 {
			hc.SystemNodeHoursPerTask = math.Round((float64(hc.SystemNodeCount)*runHours/float64(taskDenom))*1e6) / 1e6
		}
		if hc.TotalGPUs > 0 {
			gpuHoursPerTask := float64(hc.TotalGPUs) * runHours / float64(taskDenom)
			hc.AcceleratorHoursPerTask = math.Round(gpuHoursPerTask*1e6) / 1e6
			var envFrac float64
			if rep.RLBreakdown != nil && rep.RLBreakdown.EnvOverheadPct > 0 {
				envFrac = rep.RLBreakdown.EnvOverheadPct / 100.0
			}
			hc.SandboxStalledAcceleratorHoursPerTask = math.Round(gpuHoursPerTask*envFrac*1e6) / 1e6
		}
	}
	if rep.Convergence != nil && rep.Convergence.WallTimeToConvergenceS != nil && hc.TotalGPUs > 0 {
		gh := float64(hc.TotalGPUs) * (*rep.Convergence.WallTimeToConvergenceS / 3600.0)
		ghRound := round4(gh)
		rep.Convergence.AcceleratorHoursToConvergence = &ghRound
	}
	rep.HardwareCost = hc
	if sys != nil {
		if sys.MaxActivePods > 0 {
			hc.MaxActivePods = sys.MaxActivePods
		}
		if hc.WorkerPodCount == 0 && sys.WorkerPodCount > 0 {
			hc.WorkerPodCount = sys.WorkerPodCount
		}
		if hc.WorkerActorCapacity == 0 && sys.WorkerActorCapacity > 0 {
			hc.WorkerActorCapacity = sys.WorkerActorCapacity
		}
		if hc.MaxConcurrentSandboxesCap == 0 && sys.MaxConcurrentSandboxesCap > 0 {
			hc.MaxConcurrentSandboxesCap = sys.MaxConcurrentSandboxesCap
		}
		if hc.MaxConcurrentSandboxesCap == 0 && hc.WorkerNodeCount > 0 {
			fpCPU := 2.0
			fpMem := 4.0
			if sys.SandboxFootprint != nil {
				if sys.SandboxFootprint.CPU > 0 {
					fpCPU = sys.SandboxFootprint.CPU
				}
				if sys.SandboxFootprint.MemoryGiB > 0 {
					fpMem = sys.SandboxFootprint.MemoryGiB
				}
			}
			perNodeCPU := hc.WorkerAllocatableVCPU / float64(hc.WorkerNodeCount)
			perNodeGiB := hc.WorkerAllocatableGiB / float64(hc.WorkerNodeCount)
			slotsPerNode := 0
			if perNodeCPU > 0 && fpCPU > 0 {
				slotsPerNode = int(math.Floor(perNodeCPU / fpCPU))
			}
			if perNodeGiB > 0 && fpMem > 0 {
				memSlots := int(math.Floor(perNodeGiB / fpMem))
				if slotsPerNode == 0 || memSlots < slotsPerNode {
					slotsPerNode = memSlots
				}
			}
			if slotsPerNode > 110 {
				slotsPerNode = 110
			}
			if slotsPerNode > 0 {
				hc.MaxConcurrentSandboxesCap = slotsPerNode * hc.WorkerNodeCount
			}
		}
		if hc.WorkerPodCount > 0 && hc.WorkerActorCapacity > 0 {
			wCap := hc.WorkerPodCount * hc.WorkerActorCapacity
			if hc.MaxConcurrentSandboxesCap == 0 || wCap < hc.MaxConcurrentSandboxesCap {
				hc.MaxConcurrentSandboxesCap = wCap
			}
		}
		if hc.MaxActivePods > 0 {
			hc.MaxConcurrentSandboxesCap = hc.MaxActivePods
			if hc.WorkerPodCount > 0 && hc.WorkerActorCapacity > 0 {
				wCap := hc.WorkerPodCount * hc.WorkerActorCapacity
				if wCap < hc.MaxConcurrentSandboxesCap {
					hc.MaxConcurrentSandboxesCap = wCap
				}
			}
			sys.MaxConcurrentSandboxesCap = hc.MaxConcurrentSandboxesCap
		}
		if sys.WorkerPodCount == 0 && hc.WorkerPodCount > 0 {
			sys.WorkerPodCount = hc.WorkerPodCount
		}
		if sys.WorkerActorCapacity == 0 && hc.WorkerActorCapacity > 0 {
			sys.WorkerActorCapacity = hc.WorkerActorCapacity
		}
		if sys.MaxConcurrentSandboxesCap == 0 && hc.MaxConcurrentSandboxesCap > 0 {
			sys.MaxConcurrentSandboxesCap = hc.MaxConcurrentSandboxesCap
		}
		if sys.SubstrateVersion == "" && sys.APIImage != "" {
			if idx := strings.LastIndex(sys.APIImage, "@"); idx >= 0 {
				sys.SubstrateVersion = sys.APIImage[idx+1:]
			} else if idx := strings.LastIndex(sys.APIImage, ":"); idx > strings.LastIndex(sys.APIImage, "/") {
				sys.SubstrateVersion = sys.APIImage[idx+1:]
			}
		}
		if sys.GuestImage == "" {
			if rep.RunTags != nil && rep.RunTags["guest_image"] != "" {
				sys.GuestImage = rep.RunTags["guest_image"]
			} else if len(rep.DistinctImages) > 0 {
				sys.GuestImage = rep.DistinctImages[0]
			}
		}
		if sys.DatasetRevision == "" && rep.RunTags != nil && rep.RunTags["dataset"] != "" {
			sys.DatasetRevision = normalizeDatasetRevision(rep.RunTags["dataset"])
		}
		if sys.Load == nil {
			sys.Load = rep.buildDefaultLoadParams(hc, sys)
		} else {
			if sys.Load.MaxConcurrent <= 0 {
				switch {
				case hc.ActiveSandboxes > 0:
					sys.Load.MaxConcurrent = hc.ActiveSandboxes
				case rep.StandingPoolSize > 0:
					sys.Load.MaxConcurrent = rep.StandingPoolSize
				case rep.DistinctEnvs > 0:
					sys.Load.MaxConcurrent = rep.DistinctEnvs
				}
			}
			if sys.Load.DistinctImages <= 0 {
				if len(rep.DistinctImages) > 0 {
					sys.Load.DistinctImages = len(rep.DistinctImages)
				} else if sys.GuestImage != "" || sys.APIImage != "" {
					sys.Load.DistinctImages = 1
				}
			}
			if sys.Load.TrajectoriesPerStep <= 0 && sys.Load.GroupSize > 0 && sys.Load.TasksPerStep > 0 {
				sys.Load.TrajectoriesPerStep = sys.Load.GroupSize * sys.Load.TasksPerStep
			}
			if sys.Load.Repeat <= 0 {
				sys.Load.Repeat = 1
			}
		}
		rep.LifecyclePolicy = inferLifecyclePolicy(rep, sys.LifecyclePolicy)
		sys.LifecyclePolicy = rep.LifecyclePolicy
		rep.SystemDescription = sys
	} else {
		rep.LifecyclePolicy = inferLifecyclePolicy(rep, rep.LifecyclePolicy)
	}
}

func (rep *Report) buildDefaultLoadParams(hc *HardwareCostSummary, sys *schema.SystemDescription) *schema.LoadParams {
	lp := &schema.LoadParams{Repeat: 1}
	if rep.RunTags != nil {
		if v, err := strconv.Atoi(rep.RunTags["load_tasks"]); err == nil && v > 0 {
			lp.Tasks = v
		}
		if v, err := strconv.Atoi(rep.RunTags["load_group_size"]); err == nil && v > 0 {
			lp.GroupSize = v
		}
		if v, err := strconv.Atoi(rep.RunTags["load_steps"]); err == nil && v > 0 {
			lp.Steps = v
		}
		if v, err := strconv.Atoi(rep.RunTags["load_tasks_per_step"]); err == nil && v > 0 {
			lp.TasksPerStep = v
		}
		if v, err := strconv.Atoi(rep.RunTags["load_trajectories_per_step"]); err == nil && v > 0 {
			lp.TrajectoriesPerStep = v
		}
		if v, err := strconv.Atoi(rep.RunTags["load_max_concurrent"]); err == nil && v > 0 {
			lp.MaxConcurrent = v
		}
		if v, err := strconv.Atoi(rep.RunTags["load_distinct_images"]); err == nil && v > 0 {
			lp.DistinctImages = v
		}
	}
	if lp.Steps <= 0 {
		if len(rep.Steps) > 0 {
			lp.Steps = len(rep.Steps)
		} else {
			lp.Steps = 1
		}
	}
	if lp.Tasks <= 0 {
		if rep.DistinctInstances > 0 {
			lp.Tasks = rep.DistinctInstances
		} else {
			lp.Tasks = rep.Tasks
		}
	}
	if lp.GroupSize <= 0 {
		lp.GroupSize = 1
	}
	if lp.TasksPerStep <= 0 {
		if len(rep.Steps) > 0 && rep.Steps[0].Tasks > 0 {
			lp.TasksPerStep = rep.Steps[0].Tasks / lp.GroupSize
		} else if len(rep.RolloutBreakdown) > 0 && rep.RolloutBreakdown[0].Tasks > 0 {
			lp.TasksPerStep = rep.RolloutBreakdown[0].Tasks
		} else {
			lp.TasksPerStep = lp.Tasks
		}
	}
	if lp.TrajectoriesPerStep <= 0 {
		lp.TrajectoriesPerStep = lp.GroupSize * lp.TasksPerStep
	}
	if lp.MaxConcurrent <= 0 {
		switch {
		case hc != nil && hc.ActiveSandboxes > 0:
			lp.MaxConcurrent = hc.ActiveSandboxes
		case rep.StandingPoolSize > 0:
			lp.MaxConcurrent = rep.StandingPoolSize
		case rep.DistinctEnvs > 0:
			lp.MaxConcurrent = rep.DistinctEnvs
		default:
			lp.MaxConcurrent = lp.TrajectoriesPerStep
		}
	}
	if lp.DistinctImages <= 0 {
		if len(rep.DistinctImages) > 0 {
			lp.DistinctImages = len(rep.DistinctImages)
		} else if sys != nil && (sys.GuestImage != "" || sys.APIImage != "") {
			lp.DistinctImages = 1
		}
	}
	return lp
}

func roundMetric(v float64) float64 {
	if math.Abs(v) > 0 && math.Abs(v) < 10.0 {
		return math.Round(v*1e6) / 1e6
	}
	return round1(v)
}

func makeMetricSummary(xs []float64, unit string) schema.MetricSummary {
	if len(xs) == 0 {
		return schema.MetricSummary{Unit: unit}
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	var sum float64
	for _, v := range cp {
		sum += v
	}
	return schema.MetricSummary{
		Count: len(cp),
		Sum:   roundMetric(sum),
		Min:   roundMetric(cp[0]),
		P50:   roundMetric(percentile(cp, 0.50)),
		P90:   roundMetric(percentile(cp, 0.90)),
		P95:   roundMetric(percentile(cp, 0.95)),
		P99:   roundMetric(percentile(cp, 0.99)),
		Max:   roundMetric(cp[len(cp)-1]),
		Unit:  unit,
	}
}

func feedReaderToBuilder(r io.Reader, b *Builder) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			return fmt.Errorf("parse event: %w", err)
		}
		b.Add(e)
	}
	return sc.Err()
}

// Summarize streams JSONL events from r into a Builder and returns the report.
func Summarize(r io.Reader) (*Report, error) {
	b := NewBuilder()
	if err := feedReaderToBuilder(r, b); err != nil {
		return nil, err
	}
	return b.Finish(), nil
}

// SummarizeFile reads path (or stdin if "-"/"") and summarizes it.
func SummarizeFile(path string) (*Report, error) {
	if path == "" || path == "-" {
		return Summarize(os.Stdin)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Summarize(f)
}

// BuildReport reads a JSONL events file from path and returns its Report.
func BuildReport(path string) (*Report, error) {
	return SummarizeFile(path)
}

func (b *Builder) buildInterferenceSummary() *InterferenceSummary {
	sum := &InterferenceSummary{
		ActiveJobs: len(b.jobs),
	}
	if len(b.slotWaits) > 0 {
		sw := append([]float64(nil), b.slotWaits...)
		sort.Float64s(sw)
		sum.SlotWaitP50MS = round1(percentile(sw, 0.50))
		sum.SlotWaitP95MS = round1(percentile(sw, 0.95))
		sum.SlotWaitMaxMS = round1(sw[len(sw)-1])
	}

	var minJobP50, maxJobP50 float64
	for _, ja := range b.jobs {
		if len(ja.ttfi) == 0 {
			continue
		}
		cp := append([]float64(nil), ja.ttfi...)
		sort.Float64s(cp)
		p50 := percentile(cp, 0.50)
		if p50 > 0 {
			if minJobP50 == 0 || p50 < minJobP50 {
				minJobP50 = p50
			}
			if p50 > maxJobP50 {
				maxJobP50 = p50
			}
		}
	}
	if minJobP50 > 0 && maxJobP50 >= minJobP50 {
		sum.TTFISpreadRatio = round2(maxJobP50 / minJobP50)
	}

	var stepNums []int
	for s := range b.stepJobTasks {
		stepNums = append(stepNums, s)
	}
	sort.Ints(stepNums)

	for _, s := range stepNums {
		jt := b.stepJobTasks[s]
		if len(jt) > 1 {
			sum.OverlappingSteps++
			sum.OverlapWindowCount++
		}
		var total int
		perJob := make(map[string]int, len(jt))
		for jName, cnt := range jt {
			perJob[jName] = cnt
			total += cnt
		}
		si := StepInterference{
			Step:        s,
			ActiveJobs:  len(jt),
			TotalTasks:  total,
			PerJobTasks: perJob,
		}
		if creates := b.stepCreates[s]; len(creates) > 0 {
			cp := append([]float64(nil), creates...)
			sort.Float64s(cp)
			si.CreateP50MS = round1(percentile(cp, 0.50))
			si.CreateP95MS = round1(percentile(cp, 0.95))
		}
		if waits := b.stepSlotWaits[s]; len(waits) > 0 {
			cp := append([]float64(nil), waits...)
			sort.Float64s(cp)
			si.SlotWaitP50MS = round1(percentile(cp, 0.50))
			si.SlotWaitP95MS = round1(percentile(cp, 0.95))
		}
		if jttfi := b.stepJobTTFI[s]; len(jttfi) > 0 {
			si.PerJobTTFIP50MS = make(map[string]float64, len(jttfi))
			for jName, vals := range jttfi {
				cp := append([]float64(nil), vals...)
				sort.Float64s(cp)
				si.PerJobTTFIP50MS[jName] = round1(percentile(cp, 0.50))
			}
		}
		sum.Steps = append(sum.Steps, si)
	}
	return sum
}

// SummarizeFiles merges multiple JSONL event shard files in a single streaming
// Builder pass so exact p50/p90/p95/max percentiles are computed across all raw
// event durations (never averaging per-shard percentiles).
func SummarizeFiles(paths []string) (*Report, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no event files provided")
	}
	sorted := append([]string(nil), paths...)
	sort.Strings(sorted)
	b := NewBuilder()
	for _, p := range sorted {
		f, err := os.Open(p)
		if err != nil {
			return nil, fmt.Errorf("open shard %q: %w", p, err)
		}
		err = feedReaderToBuilder(f, b)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("read shard %q: %w", p, err)
		}
	}
	return b.Finish(), nil
}

// SummarizeGlob expands pattern (for example, "events/*.jsonl") and streams all
// matching shard files into a single Builder to compute exact combined percentiles.
func SummarizeGlob(pattern string) (*Report, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob %q: %w", pattern, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no event files matched glob %q", pattern)
	}
	return SummarizeFiles(matches)
}

// ReadEvents reads a JSONL events file.
func ReadEvents(r io.Reader) ([]Event, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	var events []Event
	for sc.Scan() {
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(b, &e); err != nil {
			return nil, fmt.Errorf("parse event: %w", err)
		}
		events = append(events, e)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// Build computes the report from events in memory.
func Build(events []Event) *Report {
	b := NewBuilder()
	for _, e := range events {
		b.Add(e)
	}
	return b.Finish()
}

// verdict extracts the grader's verdict from the end of a reward step's
// output: "invalid run", "f2p A/B p2p C/D", or "reward 0" when the output
// does not end in the grader's JSON line.
func verdict(tail string) string {
	if a := AdapterFor(DefaultKind); a != nil && a.Grader() != nil {
		if _, detail, err := a.Grader().Parse([]byte(tail)); err == nil && detail != "" {
			return detail
		}
	}
	return "reward 0"
}

// WriteExclusions writes the unresolved instances in the exclude-file format:
// one id per line followed by the reason as a comment.
func (rep *Report) WriteExclusions(w io.Writer, header string) {
	fmt.Fprintf(w, "# %s\n# Generated from run %s: %d of %d tasks unresolved.\n", header, rep.RunID, len(rep.Unresolved), rep.Tasks)
	for _, u := range rep.Unresolved {
		fmt.Fprintf(w, "%s  %s\n", u.Instance, u.Reason)
	}
}

func stepStat(m map[int]*StepStat, step int) *StepStat {
	s, ok := m[step]
	if !ok {
		s = &StepStat{Step: step}
		m[step] = s
	}
	return s
}

// median sorts d and returns its middle value, or the mean of the two
// middle values for an even count, so two reps of an arm pool to the value
// between them rather than the lower one. 0 for no values.
func median(d []float64) float64 {
	n := len(d)
	if n == 0 {
		return 0
	}
	sort.Float64s(d)
	if n%2 == 1 {
		return d[n/2]
	}
	return (d[n/2-1] + d[n/2]) / 2
}

// labelUnion merges the labels of pooled runs' run events: a key keeps its
// value when every run agrees, else every value in the order first seen,
// "|"-joined, so a column that mixes commits says so.
type labelUnion map[string][]string

func (u labelUnion) add(labels map[string]string) {
	for k, v := range labels {
		if !slices.Contains(u[k], v) {
			u[k] = append(u[k], v)
		}
	}
}

func (u labelUnion) merged() map[string]string {
	if len(u) == 0 {
		return nil
	}
	out := make(map[string]string, len(u))
	for k, vs := range u {
		out[k] = strings.Join(vs, "|")
	}
	return out
}

// Pool builds one report over the reps of an arm, as Build over their merged
// events (counts add up, durations take the median, Runs lists the reps).
// The reps must be complete, distinct runs of one workload: each with a run
// event and the loader's labels, no run id twice, and poolKeys agreeing.
func Pool(runs [][]Event) (*Report, error) {
	var all []Event
	seen := map[string]int{}
	var shape map[string]string
	for i, events := range runs {
		first := i == 0
		var run *Event
		n := 0
		for j := range events {
			if events[j].Event == EventRun {
				run = &events[j]
				n++
			}
		}
		if run == nil {
			return nil, fmt.Errorf("rep %d has no run event: an interrupted run cannot be pooled", i+1)
		}
		if n > 1 {
			return nil, fmt.Errorf("rep %d holds %d run events: one file is one run, give each run its own file", i+1, n)
		}
		if run.Labels["mode"] == "" {
			return nil, fmt.Errorf("rep %d (run %q) has no loader labels: pool only runs whose loader labeled the run event", i+1, run.RunID)
		}
		if prev, dup := seen[run.RunID]; dup {
			return nil, fmt.Errorf("rep %d repeats run id %q of rep %d", i+1, run.RunID, prev)
		}
		seen[run.RunID] = i + 1
		if first {
			shape = run.Labels
		} else {
			for _, k := range poolKeys(shape, run.Labels) {
				if shape[k] != run.Labels[k] {
					return nil, fmt.Errorf("rep %d differs from rep 1 in %s (%q, %q): pool only reps of one workload", i+1, k, run.Labels[k], shape[k])
				}
			}
		}
		all = append(all, events...)
	}
	return Build(all), nil
}

// poolKeys are the labels two reps must agree on: the loader's own and every
// -set value (set.KEY) either of them carries.
func poolKeys(a, b map[string]string) []string {
	var set []string
	for _, m := range []map[string]string{a, b} {
		for k := range m {
			if strings.HasPrefix(k, "set.") && !slices.Contains(set, k) {
				set = append(set, k)
			}
		}
	}
	sort.Strings(set)
	return append(slices.Clone(LoaderLabelKeys), set...)
}

// percentile of a sorted slice by the nearest-rank method, ceil(p*n)-1; 0
// for an empty slice.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p >= 1 {
		return sorted[n-1]
	}
	i := int(math.Ceil(p*float64(n))) - 1
	if i < 0 {
		i = 0
	}
	if i >= n {
		i = n - 1
	}
	return sorted[i]
}

// WriteText prints the report as tables.
func (rep *Report) WriteText(w io.Writer) {
	fmt.Fprintf(w, "run %s mode=%s tasks=%d failed=%d rewarded=%d reward_rate=%.3f run=%.1fs\n",
		rep.RunID, rep.Mode, rep.Tasks, rep.Failed, rep.Rewarded, rep.RewardRate, rep.RunMS/1000)
	if len(rep.Labels) > 0 {
		fmt.Fprintf(w, "labels: %s\n", labelLine(rep.Labels))
	}
	writeFaultClassificationHeaderText(w, rep.FaultClassification)
	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "event\tname\tcount\tfailed\tretried\tp50_ms\tp95_ms\tmax_ms")
	for _, s := range rep.Stats {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%.0f\t%.0f\t%.0f\n", s.Event, s.Name, s.Count, s.Failed, s.Retried, s.P50MS, s.P95MS, s.MaxMS)
	}
	tw.Flush()
	if len(rep.Steps) > 0 {
		fmt.Fprintln(w)
		tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "step\tlive\ttasks\tfailed\trewarded\tbarrier_s\tpark_s\twake_p50_ms\twake_p95_ms")
		for _, s := range rep.Steps {
			fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%d\t%.1f\t%.1f\t%.0f\t%.0f\n", s.Step, s.Live, s.Tasks, s.Failed, s.Rewarded,
				s.BarrierMS/1000, s.ParkMS/1000, s.WakeP50MS, s.WakeP95MS)
		}
		tw.Flush()
	}
	if c := rep.Cache; c != nil {
		first := 0
		for _, s := range rep.Stats {
			if s.Event == EventTTFI {
				first = s.Count
			}
		}
		fmt.Fprintf(w, "\nimage cache during the run: %d nodes differenced", c.Nodes)
		for _, x := range []struct {
			n    int
			what string
		}{{c.Restarted, "restarted, counted from zero"}, {c.Added, "added, counted from zero"}, {c.Removed, "removed, left out"}, {c.Skipped, "with no before sample, left out"}} {
			if x.n > 0 {
				fmt.Fprintf(w, ", %d %s", x.n, x.what)
			}
		}
		fmt.Fprintf(w, "; misses (cold pulls) %.0f on %d nodes, hits %.0f, errors %.0f", c.Misses, c.NodesWithMisses, c.Hits, c.Errors)
		if first > 0 {
			fmt.Fprintf(w, "; first commands %d, misses per first command %.2f", first, c.Misses/float64(first))
		}
		fmt.Fprintln(w)
	}
	if b := rep.Restores; b != nil {
		fmt.Fprint(w, "\nrestore phases from atelet's log, paired with ttfi (first) and wake events by time; outside = the event's span minus atelet's total")
		for _, x := range []struct {
			n    int
			what string
		}{{b.OutsideRun, "records before or after this run"}, {b.Unmatched, "records for actors not in this run"}, {b.Unpaired, "records inside no ttfi or wake window"}} {
			if x.n > 0 {
				fmt.Fprintf(w, "; %d %s", x.n, x.what)
			}
		}
		fmt.Fprintln(w)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "restore\tphase\tcount\tp50_ms\tp90_ms\tp95_ms\tmax_ms")
		group := func(label string, g RestoreGroup) {
			for _, p := range g.Phases {
				fmt.Fprintf(tw, "%s\t%s\t%d\t%.0f\t%.0f\t%.0f\t%.0f\n", label, p.Phase, p.Count, p.P50MS, p.P90MS, p.P95MS, p.MaxMS)
			}
			if p := g.Outside; p != nil {
				fmt.Fprintf(tw, "%s\t%s\t%d\t%.0f\t%.0f\t%.0f\t%.0f\n", label, p.Phase, p.Count, p.P50MS, p.P90MS, p.P95MS, p.MaxMS)
			}
			if g.Failed > 0 {
				fmt.Fprintf(tw, "%s\tfailed\t%d\t\t\t\t\n", label, g.Failed)
			}
			if g.FailedEvents > 0 {
				fmt.Fprintf(tw, "%s\tfailed_events\t%d\t\t\t\t\n", label, g.FailedEvents)
			}
		}
		group("first", b.First)
		group("wake", b.Wakes)
		tw.Flush()
	}
	if len(rep.Slowest) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "slowest first commands and wakes:")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "event\tenv\tinstance\tstep\tms\tattempts")
		for _, e := range rep.Slowest {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%.0f\t%d\n", e.Event, e.Env, e.Instance, e.Step, e.DurMS, e.Attempts)
		}
		tw.Flush()
	}
	if len(rep.Unresolved) > 0 {
		fmt.Fprintf(w, "\nunresolved: %d instances (report -unresolved FILE writes them in exclude-file format)\n", len(rep.Unresolved))
	}
	writeFaultClassificationTableText(w, rep.FaultClassification)
	if len(rep.Errors) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "errors:")
		for i, e := range rep.Errors {
			if i == 10 {
				fmt.Fprintf(w, "  ... %d more\n", len(rep.Errors)-10)
				break
			}
			msg := strings.ReplaceAll(e.Err, "\n", " ")
			if len(msg) > 160 {
				msg = msg[:160] + "..."
			}
			fmt.Fprintf(w, "  %5d  %s\n", e.Count, msg)
		}
	}
}

// WriteMarkdown renders the report as GitHub-flavored Markdown, including
// ## Per-Step Rollout Decomposition and ## Validity when populated.
func (rep *Report) WriteMarkdown(w io.Writer) {
	fmt.Fprintf(w, "# SWE-bench Benchmark Report (`%s`)\n\n", rep.RunID)
	fmt.Fprintf(w, "- **Mode**: `%s`\n", rep.Mode)
	fmt.Fprintf(w, "- **Trace captured**: %s\n", rep.TraceCapturedLabel())
	if len(rep.Scopes) > 0 {
		var scStrs []string
		for _, sc := range rep.Scopes {
			scStrs = append(scStrs, string(sc))
		}
		fmt.Fprintf(w, "- **Telemetry Scopes**: `%s`\n", strings.Join(scStrs, ", "))
	}
	fmt.Fprintf(w, "- **Tasks**: %d total, %d rewarded (%.1f%%), %d failed\n",
		rep.Tasks, rep.Rewarded, rep.RewardRate*100, rep.Failed)
	if fc := rep.FaultClassification; fc == nil {
		fmt.Fprintf(w, "- **Infra Fault Rate**: %s\n", faultNotClassified)
	} else if fc.AllRollouts == 0 {
		fmt.Fprintf(w, "- **Infra Fault Rate**: %s\n", faultNoRollouts)
	} else {
		fmt.Fprintf(w, "- **Infra Fault Rate**: %.2f%% (%d/%d rollouts; target `<%.1f%%`: **%s**; tier1=%d, tier3=%d, tier2_policy=%d, recovered=%d)\n",
			fc.InfraFaultRate*100.0, fc.InfraDropped, fc.AllRollouts, fc.TargetRate*100.0, FaultTargetStatus(fc),
			fc.Tier1Dropped, fc.Tier3Dropped, fc.Tier2Policy, fc.RecoveredRetries)
	}
	fmt.Fprintf(w, "- **Wall Time**: %.1f ms (%.2f tasks/min)\n\n",
		rep.RunMS, rep.ThroughputTasksPerMin())

	if rep.ScopeEnabled(schema.ScopeUserSandbox) {
		fmt.Fprintln(w, "## Operation Latencies")
		fmt.Fprintln(w, "| Event | Name | Count | Fail | Retried | P50 (ms) | P90 (ms) | P95 (ms) | P99 (ms) | Max (ms) |")
		fmt.Fprintln(w, "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
		for _, st := range rep.Stats {
			fmt.Fprintf(w, "| %s | %s | %d | %d | %d | %.1f | %.1f | %.1f | %.1f | %.1f |\n",
				st.Event, st.Name, st.Count, st.Failed, st.Retried, st.P50MS, st.P90MS, st.P95MS, st.P99MS, st.MaxMS)
		}
	}

	writeFaultClassificationMarkdown(w, rep.FaultClassification)

	if rep.ScopeEnabled(schema.ScopeRL) && len(rep.RolloutBreakdown) > 0 {
		fmt.Fprintln(w, "\n## Per-Step Rollout Decomposition")
		fmt.Fprintln(w, "| Step | Tasks | P50 (ms) | P90 (ms) | P95 (ms) | P99 (ms) | P100 (ms) | Straggler Skew (p100/p50) | Barrier (ms) | LLM Think (ms) | Env Wait (ms) | Env Overhead (%) |")
		fmt.Fprintln(w, "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
		for _, rb := range rep.RolloutBreakdown {
			fmt.Fprintf(w, "| %d | %d | %.1f | %.1f | %.1f | %.1f | %.1f | %.2f | %.1f | %.1f | %.1f | %.1f%% |\n",
				rb.Step, rb.Tasks, rb.P50MS, rb.P90MS, rb.P95MS, rb.P99MS, rb.P100MS, rb.StragglerSkew, rb.BarrierMS, rb.LLMThinkMS, rb.EnvWaitMS, rb.EnvOverheadPct)
		}
	}

	if len(rep.CustomMetrics) > 0 {
		fmt.Fprintln(w, "\n## Custom Metrics")
		fmt.Fprintln(w, "| Metric | Count | Min | P50 | P90 | P95 | P99 | Max |")
		fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|---:|")
		var keys []string
		for k := range rep.CustomMetrics {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			cm := rep.CustomMetrics[k]
			fmt.Fprintf(w, "| %s | %d | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f |\n",
				k, cm.Count, cm.Min, cm.P50, cm.P90, cm.P95, cm.P99, cm.Max)
		}
	}

	if rep.Validity != nil {
		fmt.Fprintln(w, "\n## Validity")
		if rep.Validity.Valid {
			fmt.Fprintln(w, "- **Status**: PASS (`valid: true`)")
		} else {
			fmt.Fprintln(w, "- **Status**: FAIL (`valid: false`)")
			for _, reason := range rep.Validity.Reasons {
				fmt.Fprintf(w, "  - %s\n", reason)
			}
		}
	}

	if rep.SystemDescription != nil {
		sd := rep.SystemDescription
		lp := sd.Load
		if lp == nil {
			lp = &schema.LoadParams{Repeat: 1}
		}
		fmt.Fprintln(w, "\n## 0. Setup Identification")
		fmt.Fprintln(w, "| Category | Parameter | Value |")
		fmt.Fprintln(w, "|---|---|---|")
		dsVal := sd.DatasetRevision
		if dsVal == "" {
			dsVal = sd.DatasetDigest
		}
		if dsVal == "" {
			dsVal = "N/A"
		}
		fmt.Fprintf(w, "| **Configurable: Dataset** | Dataset (training data) | `%s` |\n", dsVal)
		fmt.Fprintf(w, "| **Configurable: Load** | Tasks (count of unique tasks) | %d |\n", lp.Tasks)
		fmt.Fprintf(w, "| **Configurable: Load** | Group size (rollouts/trajectories per task) | %d |\n", lp.GroupSize)
		fmt.Fprintf(w, "| **Configurable: Load** | Steps (training steps per experiment) | %d |\n", lp.Steps)
		fmt.Fprintf(w, "| **Configurable: Load** | Tasks per Step (batch size) | %d |\n", lp.TasksPerStep)
		fmt.Fprintf(w, "| **Configurable: Load** | Total experiments (independent repeats) | %d |\n", lp.Repeat)
		if sd.SandboxFootprint != nil {
			diskStr := "default (node ephemeral)"
			if sd.SandboxFootprint.DiskGiB > 0 {
				diskStr = fmt.Sprintf("%.1f GiB", sd.SandboxFootprint.DiskGiB)
			}
			fmt.Fprintf(w, "| **Configurable: Sandbox** | Sandbox footprint (CPU / Memory / Disk) | %.1f vCPU / %.1f GiB RAM / %s |\n",
				sd.SandboxFootprint.CPU, sd.SandboxFootprint.MemoryGiB, diskStr)
		}
		fmt.Fprintf(w, "| **Configurable: Sandbox** | `max_concurrent` (sandboxes in use at once) | %d |\n", lp.MaxConcurrent)
		if sd.MaxConcurrentSandboxesCap > 0 {
			fmt.Fprintf(w, "| **Configurable: Sandbox** | `max_concurrent_sandboxes_cap` (server-side concurrency cap) | %d |\n", sd.MaxConcurrentSandboxesCap)
		}
		lpol := sd.LifecyclePolicy
		if lpol == nil {
			lpol = rep.LifecyclePolicy
		}
		if lpol != nil {
			fmt.Fprintf(w, "| **Configurable: Sandbox** | Lifecycle & parking mode (`mode` / `park_mode`) | `%s` (`park_mode=%s`, idle auto-park: `%v`) |\n",
				lpol.Mode, lpol.ParkMode, lpol.IdleAutoParkEnabled)
			fmt.Fprintf(w, "| **Configurable: Sandbox** | Warming policy (RL start prewarm / per-step warming / trainer warmup batches) | prewarm=`%v` (%d), per-step=`%s`, trainer_warmup_batches=%d |\n",
				lpol.PrewarmAtStart, lpol.PrewarmCount, lpol.PerStepWarming, lpol.TrainerWarmupBatches)
		}
		fmt.Fprintf(w, "| **Calculated / Fetched** | Trajectories per step (`group_size * tasks_per_step`) | %d |\n", lp.TrajectoriesPerStep)
		imgDesc := fmt.Sprintf("%d", lp.DistinctImages)
		if sd.GuestImage != "" {
			imgDesc = fmt.Sprintf("%d (`%s`)", lp.DistinctImages, sd.GuestImage)
		}
		fmt.Fprintf(w, "| **Calculated / Fetched** | Sandbox images for dataset | %s |\n", imgDesc)
		fmt.Fprintf(w, "| **Environment: Versions** | GKE Kubernetes version | `%s` |\n", sd.KubernetesVersion)
		subVer := sd.SubstrateVersion
		if sd.APIImage != "" && subVer != "" && subVer != sd.APIImage {
			subVer = fmt.Sprintf("%s (`%s`)", subVer, sd.APIImage)
		} else if subVer == "" {
			subVer = sd.APIImage
		}
		fmt.Fprintf(w, "| **Environment: Versions** | Substrate version | `%s` |\n", subVer)
		sdkVer := sd.SDKVersion
		if sdkVer == "" {
			sdkVer = sd.GitSHA
		}
		fmt.Fprintf(w, "| **Environment: Versions** | Snapshot SDK version (commit) | `%s` |\n", sdkVer)
		if len(sd.MachineTypes) > 0 {
			var pools []string
			for p := range sd.MachineTypes {
				pools = append(pools, p)
			}
			sort.Strings(pools)
			var parts []string
			for _, p := range pools {
				info := sd.MachineTypes[p]
				if info.Zone != "" {
					parts = append(parts, fmt.Sprintf("`%s`: %dx `%s` (%s)", p, info.Count, info.MachineType, info.Zone))
				} else {
					parts = append(parts, fmt.Sprintf("`%s`: %dx `%s`", p, info.Count, info.MachineType))
				}
			}
			fmt.Fprintf(w, "| **Environment: Hardware** | Machine types used (and count) | %s |\n", strings.Join(parts, "; "))
		}
		fmt.Fprintf(w, "| **Environment: Hardware** | Zones and networking | `%s` (cluster `%s`, runtime `%s`, backend `%s/%s`) |\n",
			sd.ZoneTopology, sd.Cluster, sd.RuntimeClass, sd.Backend, sd.Access)
	}

	if rep.ScopeEnabled(schema.ScopeUserSandbox) && rep.Headline != nil {
		hb := rep.Headline
		fmt.Fprintln(w, "\n## 1. Published Baseline (Scale Ladder Rung)")
		fmt.Fprintln(w, "| Metric | Value |")
		fmt.Fprintln(w, "|---|---:|")
		fmt.Fprintf(w, "| Peak Acquisition Throughput | %.2f claims/s |\n", hb.PeakClaimsPerSec)
		fmt.Fprintf(w, "| Sustained Acquisition Throughput | %.2f claims/s |\n", hb.SustainedClaimsPerSec)
		fmt.Fprintf(w, "| Task Throughput | %.2f tasks/min |\n", hb.TasksPerMin)
		fmt.Fprintf(w, "| TTFI (p50 / p90 / p95 / p99 / max) | %.1f / %.1f / %.1f / %.1f / %.1f ms |\n", hb.TTFIP50MS, hb.TTFIP90MS, hb.TTFIP95MS, hb.TTFIP99MS, hb.TTFIMaxMS)
		fmt.Fprintf(w, "| TTFI Sub-phase: SDK Slot Wait (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.SDKSlotWaitP50MS, hb.SDKSlotWaitP95MS, hb.SDKSlotWaitP99MS)
		fmt.Fprintf(w, "| TTFI Sub-phase: Wire Claim-to-Ready (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.WireClaimToReadyP50MS, hb.WireClaimToReadyP95MS, hb.WireClaimToReadyP99MS)
		fmt.Fprintf(w, "| TTFI Sub-phase: Ready-to-First-Command (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.ReadyToFirstCmdP50MS, hb.ReadyToFirstCmdP95MS, hb.ReadyToFirstCmdP99MS)
		fmt.Fprintf(w, "| Create Latency (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.CreateP50MS, hb.CreateP95MS, hb.CreateP99MS)
		fmt.Fprintf(w, "| Wake Latency (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.WakeP50MS, hb.WakeP95MS, hb.WakeP99MS)
		fmt.Fprintf(w, "| Exec Latency (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.ExecP50MS, hb.ExecP95MS, hb.ExecP99MS)
		fmt.Fprintf(w, "| Suspend Latency (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.SuspendP50MS, hb.SuspendP95MS, hb.SuspendP99MS)
		fmt.Fprintf(w, "| Delete Latency (p50 / p95 / p99) | %.1f / %.1f / %.1f ms |\n", hb.DeleteP50MS, hb.DeleteP95MS, hb.DeleteP99MS)
	}

	if rep.ScopeEnabled(schema.ScopeRL) && rep.RLBreakdown != nil {
		rb := rep.RLBreakdown
		fmt.Fprintln(w, "\n## 2. Bottleneck Identification")
		fmt.Fprintf(w, "- **Dominant Bottleneck**: `%s`\n\n", rb.DominantBottleneck)
		fmt.Fprintln(w, "| RL Step Phase | P50 (ms) | P95 (ms) | Max (ms) |")
		fmt.Fprintln(w, "|---|---:|---:|---:|")
		fmt.Fprintf(w, "| Step Total (`timing_s/step`) | %.1f | %.1f | %.1f |\n", rb.StepTotalP50MS, rb.StepTotalP95MS, rb.StepTotalMaxMS)
		fmt.Fprintf(w, "| Rollout Generation (`timing_s/gen`) | %.1f | %.1f | %.1f |\n", rb.RolloutGenP50MS, rb.RolloutGenP95MS, rb.RolloutGenMaxMS)
		fmt.Fprintf(w, "| Actor Update (`timing_s/update_actor`) | %.1f | %.1f | - |\n", rb.UpdateActorP50MS, rb.UpdateActorP95MS)
		fmt.Fprintf(w, "| Old Log-Prob (`timing_s/old_log_prob`) | %.1f | %.1f | - |\n", rb.OldLogProbP50MS, rb.OldLogProbP95MS)
		fmt.Fprintf(w, "| Weight / Parameter Sync (`timing_s/param_sync`) | %.1f | %.1f | - |\n", rb.ParamSyncP50MS, rb.ParamSyncP95MS)
		fmt.Fprintf(w, "| Reward (`timing_s/reward`) | %.3f | - | - |\n", rb.RewardP50MS)
		fmt.Fprintf(w, "| Advantage (`timing_s/adv`) | %.3f | - | - |\n", rb.AdvP50MS)
		fmt.Fprintf(w, "| Rollout LLM Think (p50 / p100) | %.1f | - | %.1f |\n", rb.LLMThinkP50MS, rb.LLMThinkP100MS)
		fmt.Fprintf(w, "| Rollout Env Wait (p50 / p100) | %.1f | - | %.1f |\n", rb.EnvWaitP50MS, rb.EnvWaitP100MS)
		fmt.Fprintf(w, "| Environment Overhead (`env_wait / rollout_time`) | %.1f%% | - | - |\n", rb.EnvOverheadPct)
		fmt.Fprintf(w, "| Straggler Skew (`p100 / p50`) | %.2fx | - | - |\n", rb.StragglerSkewP100P50)
	}

	if (rep.ScopeEnabled(schema.ScopeK8s) || rep.ScopeEnabled(schema.ScopeSubstrate)) && rep.HardwareCost != nil {
		hc := rep.HardwareCost
		fmt.Fprintln(w, "\n## 3. Hardware Cost of Sandboxes & Cluster Health")
		fmt.Fprintln(w, "| Metric | Value |")
		fmt.Fprintln(w, "|---|---:|")
		if rep.ScopeEnabled(schema.ScopeSubstrate) {
			fmt.Fprintf(w, "| Sandboxes (Active / Paused [Parked] / Total) | %d / %d / %d (peak active: %d, peak paused: %d) |\n",
				hc.ActiveSandboxes, hc.PausedSandboxes, hc.TotalSandboxes, hc.PeakActiveSandboxes, hc.PeakParkedSandboxes)
			if hc.MaxConcurrentSandboxesCap > 0 {
				fmt.Fprintf(w, "| Server Concurrent Sandbox Cap (`max_concurrent_sandboxes_cap`) | %d |\n", hc.MaxConcurrentSandboxesCap)
			}
		}
		if rep.ScopeEnabled(schema.ScopeK8s) {
			fmt.Fprintf(w, "| Nodes (Worker / System / GPU) | %d / %d / %d (%d GPUs) |\n", hc.WorkerNodeCount, hc.SystemNodeCount, hc.GPUNodeCount, hc.TotalGPUs)
			fmt.Fprintf(w, "| Sandboxes per Worker Node (p50 / p90 / p99 / max) | %.1f / %.1f / %.1f / %.1f |\n", hc.SandboxesPerNodeP50, hc.SandboxesPerNodeP90, hc.SandboxesPerNodeP99, hc.SandboxesPerNodeMax)
			fmt.Fprintf(w, "| Sandboxes per vCPU | %.4f |\n", hc.SandboxesPerVCPU)
			fmt.Fprintf(w, "| Sandboxes per GiB RAM | %.4f |\n", hc.SandboxesPerGiBRAM)
			fmt.Fprintf(w, "| Cluster Bin-Packing (CPU / Memory Requested vs Allocatable) | %.1f%% / %.1f%% |\n", hc.ClusterPackingCPUPct, hc.ClusterPackingMemPct)
			fmt.Fprintf(w, "| Actual Worker Node CPU Utilization (p50 / max) | %.1f%% / %.1f%% |\n", hc.WorkerNodeCPUUtilP50Pct, hc.WorkerNodeCPUUtilMaxPct)
			fmt.Fprintf(w, "| Actual Worker Node Memory WSS Utilization (p50 / max) | %.1f%% / %.1f%% |\n", hc.WorkerNodeMemUtilP50Pct, hc.WorkerNodeMemUtilMaxPct)
			fmt.Fprintf(w, "| Actual Sandbox Pod CPU Usage (p50 / max) | %.1f / %.1f mcores |\n", hc.SandboxPodCPUP50MCores, hc.SandboxPodCPUMaxMCores)
			fmt.Fprintf(w, "| Actual Sandbox Pod Memory WSS (p50 / max) | %.1f / %.1f MiB |\n", hc.SandboxPodWSSP50MiB, hc.SandboxPodWSSMaxMiB)
			fmt.Fprintf(w, "| Worker Node-Hours per Task | %.6f node-h/task |\n", hc.WorkerNodeHoursPerTask)
			fmt.Fprintf(w, "| System/Control-Plane Node-Hours per Task | %.6f node-h/task |\n", hc.SystemNodeHoursPerTask)
			fmt.Fprintf(w, "| Accelerator-Hours per Task | %.6f GPU-h/task |\n", hc.AcceleratorHoursPerTask)
			fmt.Fprintf(w, "| Sandbox-Stalled Accelerator-Hours per Task | %.6f GPU-h/task |\n", hc.SandboxStalledAcceleratorHoursPerTask)
			fmt.Fprintf(w, "| Pod Startup Sub-Phases (Schedule / ContainerStart / TotalReady p50) | %.1f / %.1f / %.1f ms |\n", hc.PodScheduleP50MS, hc.PodContainerStartP50MS, hc.PodTotalReadyP50MS)
			fmt.Fprintf(w, "| Cluster Health (OOMKills / Evictions / Container Restarts) | %d / %d / %d |\n", hc.OOMKills, hc.PodEvictions, hc.ContainerRestarts)
		}
	}

	if rep.ScopeEnabled(schema.ScopeRL) && rep.Convergence != nil {
		cs := rep.Convergence
		fmt.Fprintln(w, "\n## 4. Convergence Info & Training Progression")
		fmt.Fprintf(w, "- **Total Steps Run**: %d\n", cs.TotalStepsRun)
		fmt.Fprintf(w, "- **Initial -> Final Reward**: %.4f -> %.4f (delta %+.4f; peak %.4f at step %d)\n",
			cs.InitialReward, cs.FinalReward, cs.RewardDelta, cs.PeakReward, cs.PeakStep)
		fmt.Fprintf(w, "- **Convergence Status**: %s\n\n", cs.ConvergenceStatus)
		if len(cs.Curve) > 0 {
			fmt.Fprintln(w, "| Step | Step Time (s) | Wall Elapsed (s) | Reward Mean | Val Score | KL Loss | PG Loss | Grad Norm | Entropy | Resp Len (tok) | Throughput (tok/s) | Actor MFU |")
			fmt.Fprintln(w, "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
			for _, pt := range cs.Curve {
				valStr := "-"
				if pt.ValScore > 0 {
					valStr = fmt.Sprintf("%.4f", pt.ValScore)
				}
				fmt.Fprintf(w, "| %d | %.2f | %.2f | %.4f | %s | %.4f | %.4f | %.4f | %.4f | %.1f | %.1f | %.4f |\n",
					pt.Step, pt.StepTimeS, pt.WallElapsedS, pt.RewardMean, valStr, pt.KLLoss, pt.PGLoss, pt.GradNorm, pt.Entropy, pt.ResponseLenMean, pt.ThroughputTokPerS, pt.MFUActor)
			}
		}
	}
}

func extractValScore(tm map[string]float64) float64 {
	if len(tm) == 0 {
		return 0
	}
	if v, ok := tm["val-aux/math_7500/reward/mean@1"]; ok && v > 0 {
		return v
	}
	for k, v := range tm {
		if (strings.HasPrefix(k, "val-core/") && strings.HasSuffix(k, "/acc/mean@1")) ||
			(strings.HasPrefix(k, "val-aux/") && (strings.HasSuffix(k, "/reward/mean@1") || strings.HasSuffix(k, "/score/mean@1"))) {
			if v > 0 {
				return v
			}
		}
	}
	return 0
}

// WriteStatsJSONL emits flat schema.StatsRow JSONL records for s (trial_summary,
// phase_breakdown_summary, per-op latency/throughput rows, and custom_metrics)
// and validates every row against schema.StatsRow.
func WriteStatsJSONL(path string, s *Report, runID, testName, tag string) error {
	if s == nil {
		return fmt.Errorf("nil report")
	}
	if runID == "" {
		runID = s.RunID
	}
	if testName == "" {
		if s.RunWindow != nil && s.RunWindow.TestName != "" {
			testName = s.RunWindow.TestName
		} else if s.Mode != "" {
			testName = "swebench-" + s.Mode
		} else {
			testName = "swebench"
		}
	}
	if tag == "" && s.RunWindow != nil && s.RunWindow.Tag != "" {
		tag = s.RunWindow.Tag
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	if s.RunWindow != nil && s.RunWindow.EndTS != "" {
		ts = s.RunWindow.EndTS
	}

	// trace_captured on every row keeps traced and untraced runs from being aggregated together.
	baseLabels := map[string]string{"trace_captured": strconv.FormatBool(s.TraceCaptured)}
	if s.Mode != "" {
		baseLabels["mode"] = s.Mode
	}

	var rows []schema.StatsRow

	// 1. trial_summary row
	trialMeas := map[string]float64{
		"tasks":         float64(s.Tasks),
		"failed":        float64(s.Failed),
		"rewarded":      float64(s.Rewarded),
		"reward_rate":   round4(s.RewardRate),
		"wall_ms":       round1(s.RunMS),
		"tasks_per_min": s.ThroughputTasksPerMin(),
	}
	if st, ok := s.StatFor(EventTTFI, ""); ok {
		trialMeas["ttfi_p50_ms"] = st.P50MS
		trialMeas["ttfi_p90_ms"] = st.P90MS
		trialMeas["ttfi_p95_ms"] = st.P95MS
		trialMeas["ttfi_p99_ms"] = st.P99MS
		trialMeas["ttfi_max_ms"] = st.MaxMS
	}
	if st, ok := s.StatFor(EventWake, ""); ok {
		trialMeas["wake_p50_ms"] = st.P50MS
		trialMeas["wake_p90_ms"] = st.P90MS
		trialMeas["wake_p95_ms"] = st.P95MS
		trialMeas["wake_p99_ms"] = st.P99MS
		trialMeas["wake_max_ms"] = st.MaxMS
	}
	if fc := s.FaultClassification; fc != nil {
		trialMeas["infra_fault_rate"] = fc.InfraFaultRate
		trialMeas["infra_dropped"] = float64(fc.InfraDropped)
		trialMeas["tier1_dropped"] = float64(fc.Tier1Dropped)
		trialMeas["tier2_policy"] = float64(fc.Tier2Policy)
		trialMeas["tier3_dropped"] = float64(fc.Tier3Dropped)
		trialMeas["recovered_retries"] = float64(fc.RecoveredRetries)
	}
	rows = append(rows, schema.StatsRow{
		SchemaVersion: schema.SchemaVersion,
		Timestamp:     ts,
		Tag:           tag,
		TestName:      testName,
		RunID:         runID,
		Metric:        "trial_summary",
		Measurements:  trialMeas,
		Labels:        copyLabels(baseLabels),
	})
	if fc := s.FaultClassification; fc != nil {
		meetsVal := 0.0
		if fc.MeetsTarget {
			meetsVal = 1.0
		}
		rows = append(rows, schema.StatsRow{
			SchemaVersion: schema.SchemaVersion,
			Timestamp:     ts,
			Tag:           tag,
			TestName:      testName,
			RunID:         runID,
			Metric:        "fault_classification",
			Measurements: map[string]float64{
				"all_rollouts":        float64(fc.AllRollouts),
				"infra_dropped":       float64(fc.InfraDropped),
				"tier1_dropped":       float64(fc.Tier1Dropped),
				"tier2_policy":        float64(fc.Tier2Policy),
				"tier3_dropped":       float64(fc.Tier3Dropped),
				"recovered_retries":   float64(fc.RecoveredRetries),
				"post_rollout_errors": float64(fc.PostRolloutErrors),
				"infra_fault_rate":    fc.InfraFaultRate,
				"target_rate":         fc.TargetRate,
				"meets_target":        meetsVal,
			},
			Labels: copyLabels(baseLabels),
		})
	}

	// 2. phase_breakdown_summary row
	phaseMeas := map[string]float64{
		"steps_recorded": float64(len(s.Steps)),
	}
	for _, st := range s.Stats {
		if st.Event == EventStep && st.Name != "" {
			phaseMeas[st.Name+"_p50_ms"] = st.P50MS
			phaseMeas[st.Name+"_p90_ms"] = st.P90MS
			phaseMeas[st.Name+"_p95_ms"] = st.P95MS
			phaseMeas[st.Name+"_p99_ms"] = st.P99MS
			phaseMeas[st.Name+"_max_ms"] = st.MaxMS
		}
	}
	if len(s.RolloutBreakdown) > 0 {
		n := float64(len(s.RolloutBreakdown))
		var sumP50, sumP100, sumSkew, sumBarrier, sumThink, sumWait, sumOverhead float64
		for _, rb := range s.RolloutBreakdown {
			sumP50 += rb.P50MS
			sumP100 += rb.P100MS
			sumSkew += rb.StragglerSkew
			sumBarrier += rb.BarrierMS
			sumThink += rb.LLMThinkMS
			sumWait += rb.EnvWaitMS
			sumOverhead += rb.EnvOverheadPct
		}
		phaseMeas["rollout_p50_ms"] = round1(sumP50 / n)
		phaseMeas["rollout_p100_ms"] = round1(sumP100 / n)
		phaseMeas["straggler_skew"] = round2(sumSkew / n)
		phaseMeas["barrier_ms"] = round1(sumBarrier / n)
		phaseMeas["llm_think_ms"] = round1(sumThink / n)
		phaseMeas["env_wait_ms"] = round1(sumWait / n)
		phaseMeas["env_overhead_pct"] = round2(sumOverhead / n)
	}
	if s.RLBreakdown != nil {
		if s.RLBreakdown.StragglerSkewP100P50 > 0 {
			phaseMeas["straggler_skew"] = s.RLBreakdown.StragglerSkewP100P50
		}
		if s.RLBreakdown.LLMThinkP50MS > 0 {
			phaseMeas["llm_think_ms"] = s.RLBreakdown.LLMThinkP50MS
		}
		if s.RLBreakdown.EnvWaitP50MS > 0 {
			phaseMeas["env_wait_ms"] = s.RLBreakdown.EnvWaitP50MS
		}
		if s.RLBreakdown.EnvOverheadPct > 0 {
			phaseMeas["env_overhead_pct"] = s.RLBreakdown.EnvOverheadPct
		}
	}
	rows = append(rows, schema.StatsRow{
		SchemaVersion: schema.SchemaVersion,
		Timestamp:     ts,
		Tag:           tag,
		TestName:      testName,
		RunID:         runID,
		Metric:        "phase_breakdown_summary",
		Measurements:  phaseMeas,
		Labels:        copyLabels(baseLabels),
	})

	// 3. Per-operation latency rows & throughput row
	for _, st := range s.Stats {
		lbl := copyLabels(baseLabels)
		if lbl == nil {
			lbl = map[string]string{}
		}
		lbl["op"] = st.Event
		metricName := "op_" + st.Event
		if st.Name != "" {
			lbl["name"] = st.Name
			metricName = "op_" + st.Event + "_" + st.Name
		}
		rows = append(rows, schema.StatsRow{
			SchemaVersion: schema.SchemaVersion,
			Timestamp:     ts,
			Tag:           tag,
			TestName:      testName,
			RunID:         runID,
			Metric:        metricName,
			Measurements: map[string]float64{
				"count":  float64(st.Count),
				"failed": float64(st.Failed),
				"p50_ms": st.P50MS,
				"p90_ms": st.P90MS,
				"p95_ms": st.P95MS,
				"p99_ms": st.P99MS,
				"max_ms": st.MaxMS,
			},
			Labels: lbl,
		})
	}

	rows = append(rows, schema.StatsRow{
		SchemaVersion: schema.SchemaVersion,
		Timestamp:     ts,
		Tag:           tag,
		TestName:      testName,
		RunID:         runID,
		Metric:        "throughput",
		Measurements: map[string]float64{
			"tasks_per_min": s.ThroughputTasksPerMin(),
			"wall_ms":       round1(s.RunMS),
		},
		Labels: copyLabels(baseLabels),
	})

	// 4. custom_metrics rows
	if len(s.CustomMetrics) > 0 {
		var cmNames []string
		for k := range s.CustomMetrics {
			cmNames = append(cmNames, k)
		}
		sort.Strings(cmNames)
		aggMeas := map[string]float64{}
		for _, k := range cmNames {
			cm := s.CustomMetrics[k]
			aggMeas[k+"_p50"] = cm.P50
			aggMeas[k+"_p95"] = cm.P95
			aggMeas[k+"_max"] = cm.Max
			lbl := copyLabels(baseLabels)
			if lbl == nil {
				lbl = map[string]string{}
			}
			lbl["custom_metric"] = k
			if cm.Unit != "" {
				lbl["unit"] = cm.Unit
			}
			rows = append(rows, schema.StatsRow{
				SchemaVersion: schema.SchemaVersion,
				Timestamp:     ts,
				Tag:           tag,
				TestName:      testName,
				RunID:         runID,
				Metric:        k,
				Measurements: map[string]float64{
					"count": float64(cm.Count),
					"sum":   cm.Sum,
					"min":   cm.Min,
					"p50":   cm.P50,
					"p90":   cm.P90,
					"p95":   cm.P95,
					"p99":   cm.P99,
					"max":   cm.Max,
				},
				Labels: lbl,
			})
		}
		rows = append(rows, schema.StatsRow{
			SchemaVersion: schema.SchemaVersion,
			Timestamp:     ts,
			Tag:           tag,
			TestName:      testName,
			RunID:         runID,
			Metric:        "custom_metrics",
			Measurements:  aggMeas,
			Labels:        copyLabels(baseLabels),
		})
	}

	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir for stats.jsonl: %w", err)
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create stats.jsonl %q: %w", path, err)
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	for i, row := range rows {
		if err := row.Validate(); err != nil {
			return fmt.Errorf("row %d (%s): %w", i, row.Metric, err)
		}
		b, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshal row %d: %w", i, err)
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func copyLabels(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }
func round4(v float64) float64 { return math.Round(v*10000) / 10000 }

// Round1 rounds v to 1 decimal place.
func Round1(v float64) float64 { return round1(v) }

// Round2 rounds v to 2 decimal places.
func Round2(v float64) float64 { return round2(v) }

// Round4 rounds v to 4 decimal places.
func Round4(v float64) float64 { return round4(v) }

// RoundStatVal rounds v to 4 decimal places when |v| < 10 and 2 decimal places otherwise.
func RoundStatVal(v float64) float64 {
	if math.Abs(v) < 10 {
		return round4(v)
	}
	return round2(v)
}

// NearestRankPct returns the nearest-rank percentile p in [0, 1] of sorted.
func NearestRankPct(sorted []float64, p float64) float64 {
	return percentile(sorted, p)
}

// SingleScalarSummary constructs a MetricSummary for a single scalar value.
func SingleScalarSummary(val float64, unit string) schema.MetricSummary {
	return schema.MetricSummary{
		Count: 1,
		Sum:   val,
		Min:   val,
		P50:   val,
		P90:   val,
		P95:   val,
		P99:   val,
		Max:   val,
		Unit:  unit,
	}
}

// SummarizeMetricSlice computes a nearest-rank MetricSummary rounded to 2 decimal places.
func SummarizeMetricSlice(xs []float64, unit string) schema.MetricSummary {
	if len(xs) == 0 {
		return schema.MetricSummary{Unit: unit}
	}
	cp := append([]float64(nil), xs...)
	sort.Float64s(cp)
	var sum float64
	for _, v := range cp {
		sum += v
	}
	return schema.MetricSummary{
		Count: len(cp),
		Sum:   round2(sum),
		Min:   round2(cp[0]),
		P50:   round2(percentile(cp, 0.50)),
		P90:   round2(percentile(cp, 0.90)),
		P95:   round2(percentile(cp, 0.95)),
		P99:   round2(percentile(cp, 0.99)),
		Max:   round2(cp[len(cp)-1]),
		Unit:  unit,
	}
}

// ParsePromLineValue extracts the numeric value from a Prometheus text-format sample line.
func ParsePromLineValue(line string) (float64, bool) {
	rb := strings.LastIndexByte(line, '}')
	tail := line
	if rb >= 0 {
		tail = line[rb+1:]
	} else if sp := strings.IndexAny(line, " \t"); sp >= 0 {
		tail = line[sp+1:]
	}
	v, err := lastNumber(tail)
	if err != nil {
		return 0, false
	}
	return v, true
}

// LoadOrSummarizeReport loads a pre-rendered report.json (normalizing legacy fields)
// or summarizes a JSONL event stream from path.
func LoadOrSummarizeReport(path string) (*Report, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '{' && !bytes.Contains(trimmed, []byte("\n{")) {
		var rep Report
		if err := json.Unmarshal(trimmed, &rep); err == nil && (len(rep.Stats) > 0 || rep.Tasks > 0 || len(rep.Jobs) > 0) {
			NormalizeLegacyReport(&rep)
			return &rep, nil
		}
	}
	return Summarize(bytes.NewReader(raw))
}

// WriteReportFiles writes the requested Markdown, unresolved exclusion, flat stats.jsonl,
// and JSON report files for rep.
func WriteReportFiles(rep *Report, jsonOut, mdOut, unresolvedOut, unresolvedNote, statsOut, testName, tag string) error {
	if mdOut != "" {
		var mdBuf bytes.Buffer
		rep.WriteMarkdown(&mdBuf)
		if err := os.WriteFile(mdOut, mdBuf.Bytes(), 0o644); err != nil {
			return err
		}
	}
	if unresolvedOut != "" {
		uf, err := os.Create(unresolvedOut)
		if err != nil {
			return err
		}
		rep.WriteExclusions(uf, unresolvedNote)
		if err := uf.Close(); err != nil {
			return err
		}
	}
	if statsOut != "" {
		if err := WriteStatsJSONL(statsOut, rep, rep.RunID, testName, tag); err != nil {
			return err
		}
	}
	if jsonOut != "" {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(jsonOut, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// WriteComparison prints several runs side by side, one column per run: the
// header figures, then every event in the order it first appears, then the
// per-step figures when a run has steps. It is the table for the same
// workload on different clusters or builds. names are the files the reports
// came from; a column is headed by its run id unless that is empty or shared
// with another column, in which case the file name is used.
func WriteComparison(w io.Writer, reps []*Report, names []string) {
	labels := columnLabels(reps, names)
	fmt.Fprintln(w, "cells: p50/p95 ms n=count [f=failed r=retried]; steps: live tasks [f=failed] barrier_s/park_s wake p50/p95 ms")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	row := func(label string, cell func(*Report) string) {
		fmt.Fprint(tw, label)
		for _, r := range reps {
			fmt.Fprintf(tw, "\t%s", cell(r))
		}
		fmt.Fprintln(tw)
	}
	fmt.Fprint(tw, "run")
	for _, l := range labels {
		fmt.Fprintf(tw, "\t%s", l)
	}
	fmt.Fprintln(tw)
	row("mode", func(r *Report) string { return r.Mode })
	row("trace_captured", func(r *Report) string { return strconv.FormatBool(r.TraceCaptured) })
	row("reps", func(r *Report) string { return fmt.Sprintf("%d", r.Reps) })
	row("tasks", func(r *Report) string { return fmt.Sprintf("%d", r.Tasks) })
	row("failed", func(r *Report) string { return fmt.Sprintf("%d", r.Failed) })
	row("infra_fault_rate", func(r *Report) string {
		fc := r.FaultClassification
		switch {
		case fc == nil:
			return "not classified"
		case fc.AllRollouts == 0:
			return faultNoRollouts
		}
		return fmt.Sprintf("%.2f%% (%s)", fc.InfraFaultRate*100.0, FaultTargetStatus(fc))
	})
	row("reward_rate", func(r *Report) string { return fmt.Sprintf("%.3f", r.RewardRate) })
	row("run_s", func(r *Report) string { return fmt.Sprintf("%.1f", r.RunMS/1000) })
	builtin := map[string]bool{"run": true, "mode": true, "trace_captured": true, "reps": true, "tasks": true, "failed": true, "infra_fault_rate": true, "reward_rate": true, "run_s": true}
	for _, k := range labelKeys(reps) {
		if k == "mode" && labelMatchesMode(reps) {
			continue // the loader's own label; the mode row above says it
		}
		name := k
		if builtin[k] {
			name = "label." + k // an operator label named like a built-in row
		}
		row(name, func(r *Report) string {
			if v, ok := r.Labels[k]; ok {
				return v
			}
			return "-"
		})
	}

	type key struct{ event, name string }
	var order []key
	seen := map[key]bool{}
	for _, r := range reps {
		for _, s := range r.Stats {
			k := key{s.Event, s.Name}
			if !seen[k] {
				seen[k] = true
				order = append(order, k)
			}
		}
	}
	for _, k := range order {
		label := k.event
		if k.name != "" {
			label += " " + k.name
		}
		row(label, func(r *Report) string {
			for _, s := range r.Stats {
				if s.Event != k.event || s.Name != k.name {
					continue
				}
				cell := fmt.Sprintf("%.0f/%.0f n=%d", s.P50MS, s.P95MS, s.Count)
				if s.Count == s.Failed {
					// No call succeeded: there is no latency to show.
					cell = fmt.Sprintf("-/- n=%d", s.Count)
				}
				if s.Failed > 0 {
					cell += fmt.Sprintf(" f=%d", s.Failed)
				}
				if s.Retried > 0 {
					cell += fmt.Sprintf(" r=%d", s.Retried)
				}
				return cell
			}
			return "-"
		})
	}

	last := 0
	for _, r := range reps {
		for _, s := range r.Steps {
			if s.Step > last {
				last = s.Step
			}
		}
	}
	for n := 1; n <= last; n++ {
		row(fmt.Sprintf("step %d", n), func(r *Report) string {
			for _, s := range r.Steps {
				if s.Step != n {
					continue
				}
				cell := fmt.Sprintf("live=%d tasks=%d", s.Live, s.Tasks)
				if s.Failed > 0 {
					cell += fmt.Sprintf(" f=%d", s.Failed)
				}
				wake := fmt.Sprintf("%.0f/%.0f", s.WakeP50MS, s.WakeP95MS)
				if s.WakeP50MS == 0 && s.WakeP95MS == 0 {
					wake = "-/-" // no wake in this step: the first one, or -park none
				}
				return cell + fmt.Sprintf(" %.1f/%.1f %s", s.BarrierMS/1000, s.ParkMS/1000, wake)
			}
			return "-"
		})
	}
	tw.Flush()
}

// WriteComparisonMarkdown writes the arm table of a strategy or client sweep,
// one row per run: its client and strategy labels, tasks, failures, reward
// rate, wall clock, throughput, warm-pool hit rate, peak active sandboxes,
// standing pool and the TTFC (event="ttfi") and wake (cmd complete) percentiles;
// then WriteComparison's text table in a code block. run-r2e-swebench.sh writes
// it to comparison.md.
func WriteComparisonMarkdown(w io.Writer, reps []*Report, names []string) {
	labels := columnLabels(reps, names)
	fmt.Fprintln(w, "# Multi-Arm Strategy & Client Chain Comparison")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "| Arm | Client | Strategy | Tasks | Failed | %s | Reward Rate | Wall Clock (s) | Throughput (tasks/min) | Warm Hit Rate | Peak Active | Standing Pool | TTFC (cmd complete) p50/p95/p99 | Wake (cmd complete) p50/p95/p99 |\n", infraFaultHeader(reps))
	fmt.Fprintln(w, "| :--- | :--- | :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- | :--- |")
	for i, r := range reps {
		client := r.Labels["client"]
		if client == "" {
			client = "-"
		}
		strategy := r.Labels["strategy"]
		if strategy == "" {
			strategy = r.Mode
		}
		if strategy == "" {
			strategy = "-"
		}
		hitRate, pool := "-", "-"
		if r.WorkingSetHits+r.WorkingSetMisses > 0 || r.StandingPoolSize > 0 {
			hitRate = fmt.Sprintf("%.1f%%", r.WorkingSetHitRate*100)
			pool = strconv.Itoa(r.StandingPoolSize)
		}
		tpm := r.ThroughputTasksPerMin()
		if r.Headline != nil && r.Headline.TasksPerMin > 0 {
			tpm = r.Headline.TasksPerMin
		}
		fmt.Fprintf(w, "| `%s` | `%s` | `%s` | %d | %d | %s | %.3f | %.1f | %.1f | %s | %d | %s | %s | %s |\n",
			labels[i], client, strategy, r.Tasks, r.Failed, InfraFaultCell(r), r.RewardRate, r.RunMS/1000, tpm,
			hitRate, r.PeakActiveSandboxes, pool, percentileCell(r, "ttfi"), percentileCell(r, "wake"))
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "## Detailed Event Breakdown (`swebench-loader report`)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "```text")
	var text bytes.Buffer
	WriteComparison(&text, reps, names)
	fmt.Fprintln(w, strings.TrimRight(text.String(), "\n"))
	fmt.Fprintln(w, "```")
}

// percentileCell is the p50/p95/p99 cell of the unnamed stat of event in the
// arm table; "-" when the run has none, and no latency when no call succeeded.
func percentileCell(r *Report, event string) string {
	for _, s := range r.Stats {
		if s.Event != event || s.Name != "" {
			continue
		}
		if s.Count == s.Failed {
			return fmt.Sprintf("-/-/- (n=%d, f=%d)", s.Count, s.Failed)
		}
		p99 := s.P99MS
		if p99 == 0 {
			p99 = s.P95MS
		}
		return fmt.Sprintf("%.0f/%.0f/%.0f ms (n=%d)", s.P50MS, s.P95MS, p99, s.Count)
	}
	return "-"
}

// columnLabels heads each column with its run id, or with the file name when
// the run id is empty or the same as another column's; when the file names
// also repeat, with the full path.
func columnLabels(reps []*Report, names []string) []string {
	count := map[string]int{}
	for _, r := range reps {
		count[r.RunID]++
	}
	labels := make([]string, len(reps))
	byFile := map[int]bool{}
	for i, r := range reps {
		labels[i] = r.RunID
		if r.RunID != "" && count[r.RunID] == 1 {
			continue
		}
		if i < len(names) && names[i] != "" {
			labels[i] = strings.TrimSuffix(filepath.Base(names[i]), ".jsonl")
			byFile[i] = true
		} else {
			labels[i] = fmt.Sprintf("run %d", i+1)
		}
	}
	seen := map[string]int{}
	for _, l := range labels {
		seen[l]++
	}
	for i := range labels {
		if byFile[i] && seen[labels[i]] > 1 {
			labels[i] = names[i]
		}
	}
	return labels
}

// NodeCacheSample is one reading of the image-cache counters of every node,
// as sample-node-cache.sh writes it: a node name, a tab, then a Prometheus
// text line, with one "# sampled <unix seconds>" line per node. Nodes maps
// each node the sample covered to the start time of its atelet process (zero
// when that series was absent); Requests holds ate_imagecache_requests_total
// by node and outcome.
type NodeCacheSample struct {
	// Time is when the sample was taken: the latest "# sampled" stamp.
	Time     time.Time
	Nodes    map[string]time.Time
	Requests map[string]map[string]float64
}

const (
	cacheRequestsSeries        = "ate_imagecache_requests_total{"
	kubeletImageRequestsSeries = "kubelet_image_manager_ensure_image_requests_total{"
	processStartSeries         = "process_start_time_seconds"
)

// ParseNodeCacheSample reads a sample file. Comment lines and other metric
// families only register the node; a line without the node tab, a request
// line without labels, or a value that is not a number is an error.
func ParseNodeCacheSample(r io.Reader) (*NodeCacheSample, error) {
	s := &NodeCacheSample{Nodes: map[string]time.Time{}, Requests: map[string]map[string]float64{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimRight(sc.Text(), " \r")
		if text == "" {
			continue
		}
		node, rest, ok := strings.Cut(text, "\t")
		if !ok || node == "" {
			return nil, fmt.Errorf("sample line %d: no node name and tab", line)
		}
		if _, seen := s.Nodes[node]; !seen {
			s.Nodes[node] = time.Time{}
		}
		switch {
		case strings.HasPrefix(rest, "# sampled "):
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(rest, "# sampled ")), 64)
			if err != nil {
				return nil, fmt.Errorf("sample line %d: sample time %q: %w", line, rest, err)
			}
			if t := unixTime(v); t.After(s.Time) {
				s.Time = t
			}
		case strings.HasPrefix(rest, "#"):
		case strings.HasPrefix(rest, processStartSeries+" ") || strings.HasPrefix(rest, processStartSeries+"{"):
			tail := rest[len(processStartSeries):]
			if rb := strings.IndexByte(tail, '}'); strings.HasPrefix(tail, "{") && rb >= 0 {
				tail = tail[rb+1:]
			}
			v, err := lastNumber(tail)
			if err != nil {
				return nil, fmt.Errorf("sample line %d: %w", line, err)
			}
			s.Nodes[node] = unixTime(v)
		case strings.HasPrefix(rest, cacheRequestsSeries):
			rb := strings.LastIndexByte(rest, '}')
			if rb < 0 {
				return nil, fmt.Errorf("sample line %d: no labels", line)
			}
			outcome := ""
			for _, kv := range strings.Split(rest[len(cacheRequestsSeries):rb], ",") {
				if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && (k == "ate_imagecache_outcome" || k == "outcome") {
					outcome = strings.Trim(v, `"`)
				}
			}
			if outcome == "" {
				return nil, fmt.Errorf("sample line %d: no ate_imagecache_outcome label", line)
			}
			v, err := lastNumber(rest[rb+1:])
			if err != nil {
				return nil, fmt.Errorf("sample line %d: %w", line, err)
			}
			if s.Requests[node] == nil {
				s.Requests[node] = map[string]float64{}
			}
			s.Requests[node][outcome] += v
		case strings.HasPrefix(rest, kubeletImageRequestsSeries):
			rb := strings.LastIndexByte(rest, '}')
			if rb < 0 {
				return nil, fmt.Errorf("sample line %d: no labels", line)
			}
			present := ""
			for _, kv := range strings.Split(rest[len(kubeletImageRequestsSeries):rb], ",") {
				if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && k == "present_locally" {
					present = strings.Trim(v, `"`)
				}
			}
			outcome := "miss"
			if present == "true" {
				outcome = "hit"
			}
			v, err := lastNumber(rest[rb+1:])
			if err != nil {
				return nil, fmt.Errorf("sample line %d: %w", line, err)
			}
			if s.Requests[node] == nil {
				s.Requests[node] = map[string]float64{}
			}
			s.Requests[node][outcome] += v
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return s, nil
}

// lastNumber parses the sample value of a Prometheus text line: the first
// field after the metric name and labels (a timestamp may follow it).
func lastNumber(tail string) (float64, error) {
	fields := strings.Fields(tail)
	if len(fields) == 0 {
		return 0, errors.New("no value")
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, fmt.Errorf("value %q: %w", fields[0], err)
	}
	return v, nil
}

func unixTime(seconds float64) time.Time {
	sec := math.Floor(seconds)
	return time.Unix(int64(sec), int64((seconds-sec)*1e9))
}

// CacheDelta is what the node image caches did between two samples. The
// misses are the cold pulls of the window: every image lookup that had to
// pull and unpack, golden bakes and wakes on a node without the image
// included.
type CacheDelta struct {
	// Nodes were in both samples with the same atelet process; their
	// counters are differenced. Restarted nodes were in both samples but
	// atelet started in between, and Added nodes were only in the after
	// sample with an atelet younger than the before sample (a node the
	// autoscaler brought in); both count from zero. Removed nodes were only in
	// the before sample. Skipped nodes were only in the after sample with an
	// atelet older than the before sample, so the before scrape failed; they
	// are reported and left out.
	Nodes     int     `json:"nodes"`
	Restarted int     `json:"restarted,omitempty"`
	Added     int     `json:"added,omitempty"`
	Removed   int     `json:"removed,omitempty"`
	Skipped   int     `json:"skipped,omitempty"`
	Hits      float64 `json:"hits"`
	Misses    float64 `json:"misses"`
	// Errors sums the error, cancelled and timeout outcomes.
	Errors            float64 `json:"errors"`
	NodesWithMisses   int     `json:"nodes_with_misses"`
	MissesPerFirstCmd float64 `json:"misses_per_first_cmd"`
}

// PopulateCacheMissesPerFirstCmd computes Cache.MissesPerFirstCmd normalized
// by the run's first-command (ttfi / create / task) count.
func (rep *Report) PopulateCacheMissesPerFirstCmd() {
	if rep == nil || rep.Cache == nil {
		return
	}
	denom := 0
	if st, ok := rep.StatFor(EventTTFI, ""); ok && st.Count > 0 {
		denom = st.Count
	} else if st, ok := rep.StatFor(EventCreate, ""); ok && st.Count > 0 {
		denom = st.Count
	} else if rep.Tasks > 0 {
		denom = rep.Tasks
	}
	if denom > 0 {
		rep.Cache.MissesPerFirstCmd = round4(rep.Cache.Misses / float64(denom))
	}
}

// Delta compares s, taken before a run, with after.
func (s *NodeCacheSample) Delta(after *NodeCacheSample) CacheDelta {
	var d CacheDelta
	add := func(counts map[string]float64) {
		for o, v := range counts {
			switch o {
			case "hit":
				d.Hits += v
			case "miss":
				d.Misses += v
				if v > 0 {
					d.NodesWithMisses++
				}
			default:
				d.Errors += v
			}
		}
	}
	for node, startAfter := range after.Nodes {
		startBefore, inBefore := s.Nodes[node]
		switch {
		case !inBefore:
			if !startAfter.IsZero() && !s.Time.IsZero() && startAfter.After(s.Time) {
				d.Added++
				add(after.Requests[node])
			} else {
				d.Skipped++
			}
		case !startAfter.IsZero() && !startBefore.IsZero() && !startAfter.Equal(startBefore):
			d.Restarted++
			add(after.Requests[node])
		default:
			d.Nodes++
			diff := map[string]float64{}
			for o, v := range after.Requests[node] {
				delta := v - s.Requests[node][o]
				if delta < 0 {
					// No start time to prove a restart, but the counter went down.
					delta = v
				}
				diff[o] = delta
			}
			add(diff)
		}
	}
	for node := range s.Nodes {
		if _, inAfter := after.Nodes[node]; !inAfter {
			d.Removed++
		}
	}
	return d
}

// RestoreTiming is one "Restore timing breakdown" record from atelet's log,
// as harvest-restore-timings.sh collects them: the actor restored, which is
// the environment's name, and the seconds each phase took.
type RestoreTiming struct {
	// Time is when atelet wrote the record, at the end of the restore.
	Time     time.Time
	Actor    string
	Fidelity string
	// Err is the record's error.type; empty on success.
	Err string
	// Phases maps a phase (volume_mount, manifest_fetch, sandbox_assets,
	// download, oci_unpack, worker_restore, total) to seconds.
	Phases map[string]float64
}

const restoreDurationPrefix = "ate.actor.restore.duration."

// restorePhaseOrder is the order atelet times the phases in. A phase not in
// it (an older or newer atelet) is reported after these, in name order.
var restorePhaseOrder = []string{"volume_mount", "manifest_fetch", "sandbox_assets", "download", "oci_unpack", "worker_restore", "persist", "total"}

// ParseRestoreTimings reads atelet log lines (or schema.RestoreSample JSONL
// lines), one JSON record each, keeps restore records and groups them by actor
// in time order. Other records are skipped, among them the worker's own
// restore record, which has the same message but no
// ate.actor.restore.duration.total. A line that is not JSON, or a restore
// record without an actor name or a parseable time, is an error.
func ParseRestoreTimings(r io.Reader) (map[string][]RestoreTiming, error) {
	out := map[string][]RestoreTiming{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	line := 0
	for sc.Scan() {
		line++
		text := bytes.TrimSpace(sc.Bytes())
		if len(text) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(text, &rec); err != nil {
			return nil, fmt.Errorf("restore log line %d: %w", line, err)
		}
		if rec["msg"] != "Restore timing breakdown" {
			// Also accept schema.RestoreSample JSONL records (total_ms + actor/pod).
			var sample schema.RestoreSample
			if err := json.Unmarshal(text, &sample); err == nil {
				if rt, ok := RestoreTimingFromSample(sample, rec); ok {
					out[rt.Actor] = append(out[rt.Actor], rt)
				}
			}
			continue
		}
		rt := RestoreTiming{Phases: map[string]float64{}}
		for k, v := range rec {
			if f, ok := v.(float64); ok && strings.HasPrefix(k, restoreDurationPrefix) {
				rt.Phases[strings.TrimPrefix(k, restoreDurationPrefix)] = f
			}
		}
		if _, ok := rt.Phases["total"]; !ok {
			continue
		}
		rt.Actor, _ = rec["ate.actor.name"].(string)
		if rt.Actor == "" {
			return nil, fmt.Errorf("restore log line %d: no ate.actor.name", line)
		}
		ts, ok := rec["time"].(string)
		if !ok {
			return nil, fmt.Errorf("restore log line %d: no time", line)
		}
		var err error
		if rt.Time, err = time.Parse(time.RFC3339Nano, ts); err != nil {
			return nil, fmt.Errorf("restore log line %d: time %q: %w", line, ts, err)
		}
		rt.Fidelity, _ = rec["ate.snapshot.fidelity"].(string)
		rt.Err, _ = rec["error.type"].(string)
		out[rt.Actor] = append(out[rt.Actor], rt)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for _, rs := range out {
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].Time.Before(rs[j].Time) })
	}
	return out, nil
}

// RestoreTimingFromSample converts a schema.RestoreSample (plus optional extra phase fields in rawRec)
// into a RestoreTiming record with seconds per phase.
func RestoreTimingFromSample(s schema.RestoreSample, rawRec map[string]any) (RestoreTiming, bool) {
	if s.TotalMS <= 0 {
		return RestoreTiming{}, false
	}
	actor := s.Env
	if actor == "" && rawRec != nil {
		actor, _ = rawRec["actor"].(string)
		if actor == "" {
			actor, _ = rawRec["pod"].(string)
		}
	}
	if actor == "" {
		return RestoreTiming{}, false
	}
	rt := RestoreTiming{
		Actor:    actor,
		Fidelity: s.Kind,
		Phases: map[string]float64{
			"total": s.TotalMS / 1000.0,
		},
	}
	if s.ManifestFetchMS > 0 || hasKey(rawRec, "manifest_fetch_ms") {
		rt.Phases["manifest_fetch"] = s.ManifestFetchMS / 1000.0
	}
	if s.DownloadMS > 0 || hasKey(rawRec, "download_ms") {
		rt.Phases["download"] = s.DownloadMS / 1000.0
	}
	if s.OCIUnpackMS > 0 || hasKey(rawRec, "oci_unpack_ms") {
		rt.Phases["oci_unpack"] = s.OCIUnpackMS / 1000.0
	}
	if s.WorkerRestoreMS > 0 || hasKey(rawRec, "worker_restore_ms") {
		rt.Phases["worker_restore"] = s.WorkerRestoreMS / 1000.0
	}
	if s.PersistMS > 0 || hasKey(rawRec, "persist_ms") {
		rt.Phases["persist"] = s.PersistMS / 1000.0
	}
	for _, pair := range []struct {
		jsonKey string
		phase   string
	}{
		{"volume_mount_ms", "volume_mount"},
		{"sandbox_assets_ms", "sandbox_assets"},
		{"pod_schedule_ms", "pod_schedule"},
		{"pod_init_ms", "pod_init"},
		{"pod_container_start_ms", "pod_container_start"},
	} {
		if v, ok := rawRec[pair.jsonKey].(float64); ok {
			rt.Phases[pair.phase] = v / 1000.0
		}
	}
	if s.TS != nil {
		rt.Time = *s.TS
	} else if ts, ok := rawRec["ts"].(string); ok && ts != "" {
		rt.Time, _ = time.Parse(time.RFC3339Nano, ts)
	} else if ts, ok := rawRec["time"].(string); ok && ts != "" {
		rt.Time, _ = time.Parse(time.RFC3339Nano, ts)
	}
	return rt, true
}

func hasKey(m map[string]any, k string) bool {
	if m == nil {
		return false
	}
	_, ok := m[k]
	return ok
}

// PhaseStat is one restore phase over the environments of a run.
type PhaseStat struct {
	Phase string  `json:"phase"`
	Count int     `json:"count"`
	P50MS float64 `json:"p50_ms"`
	P90MS float64 `json:"p90_ms"`
	P95MS float64 `json:"p95_ms"`
	MaxMS float64 `json:"max_ms"`
}

// RestoreGroup is the phase breakdown of the restores paired with one kind
// of event: ttfi (the environment's first start) or wake.
type RestoreGroup struct {
	Restores int `json:"restores"`
	// Failed counts records with an error.type; their phases are left out.
	Failed int `json:"failed,omitempty"`
	// FailedEvents counts restores whose event itself failed (a retried or
	// timed-out first command); their span is left out of Outside.
	FailedEvents int         `json:"failed_events,omitempty"`
	Phases       []PhaseStat `json:"phases"`
	// Outside is the event's span minus atelet's total: ateapi, the
	// scheduler, the router and the command itself. Nil when nothing paired.
	Outside *PhaseStat `json:"outside,omitempty"`
}

// RestoreBreakdown is atelet's restore records joined to a run's events.
type RestoreBreakdown struct {
	// OutsideRun counts records before the run's first event started or after
	// its last ended: an earlier run with the same run id, say.
	OutsideRun int `json:"outside_run,omitempty"`
	// Unmatched counts records for actors the run has no event for.
	Unmatched int `json:"unmatched,omitempty"`
	// Unpaired counts records inside the run whose time no ttfi or wake
	// window of their environment contains: a restore after an eviction, or
	// after a failed suspend.
	Unpaired int          `json:"unpaired,omitempty"`
	First    RestoreGroup `json:"first"`
	Wakes    RestoreGroup `json:"wakes"`
}

// pairSlack absorbs the clock difference between the loader and a node.
const pairSlack = 2 * time.Second

// JoinRestores pairs each restore record with the ttfi or wake event of its
// environment whose window, the event's end minus its duration up to its
// end, contains the record's time. A record paired with a ttfi event is a
// first start; one paired with a wake is a wake. Records outside the run's
// span, for unknown actors, or inside no window are counted, not paired.
func JoinRestores(events []Event, timings map[string][]RestoreTiming) *RestoreBreakdown {
	b := &RestoreBreakdown{}
	windows := map[string][]Event{}
	var spanStart, spanEnd time.Time
	for _, e := range events {
		if e.Event != EventTTFI && e.Event != EventWake {
			continue
		}
		windows[e.Env] = append(windows[e.Env], e)
		if start := e.Time.Add(-time.Duration(e.DurMS * float64(time.Millisecond))); spanStart.IsZero() || start.Before(spanStart) {
			spanStart = start
		}
		if spanEnd.IsZero() || e.Time.After(spanEnd) {
			spanEnd = e.Time
		}
	}
	var firstRecs, wakeRecs []RestoreTiming
	var firstOutside, wakeOutside []float64
	for actor, recs := range timings {
		for _, rt := range recs {
			if spanStart.IsZero() || rt.Time.Before(spanStart.Add(-pairSlack)) || rt.Time.After(spanEnd.Add(pairSlack)) {
				b.OutsideRun++
				continue
			}
			evs := windows[actor]
			if len(evs) == 0 {
				b.Unmatched++
				continue
			}
			var ev *Event
			for i := range evs {
				start := evs[i].Time.Add(-time.Duration(evs[i].DurMS * float64(time.Millisecond)))
				if rt.Time.Before(start.Add(-pairSlack)) || rt.Time.After(evs[i].Time.Add(pairSlack)) {
					continue
				}
				if ev == nil || evs[i].Time.Before(ev.Time) {
					ev = &evs[i]
				}
			}
			if ev == nil {
				b.Unpaired++
				continue
			}
			g, recsOut, outside := &b.Wakes, &wakeRecs, &wakeOutside
			if ev.Event == EventTTFI {
				g, recsOut, outside = &b.First, &firstRecs, &firstOutside
			}
			if rt.Err != "" {
				g.Failed++
				continue
			}
			*recsOut = append(*recsOut, rt)
			if !ev.OK {
				g.FailedEvents++
				continue
			}
			*outside = append(*outside, ev.DurMS-rt.Phases["total"]*1000)
		}
	}
	b.First.Restores, b.First.Phases, b.First.Outside = len(firstRecs), phaseStats(firstRecs), spanStat("outside", firstOutside)
	b.Wakes.Restores, b.Wakes.Phases, b.Wakes.Outside = len(wakeRecs), phaseStats(wakeRecs), spanStat("outside", wakeOutside)
	return b
}

// phaseStats gives the known phases in atelet's order, then any other phase
// the records carry in name order, so an atelet that names a phase
// differently still shows up.
func phaseStats(recs []RestoreTiming) []PhaseStat {
	known := map[string]bool{}
	for _, p := range restorePhaseOrder {
		known[p] = true
	}
	var extra []string
	seen := map[string]bool{}
	for _, rt := range recs {
		for p := range rt.Phases {
			if !known[p] && !seen[p] {
				seen[p] = true
				extra = append(extra, p)
			}
		}
	}
	sort.Strings(extra)
	var out []PhaseStat
	for _, phase := range append(append([]string{}, restorePhaseOrder...), extra...) {
		var d []float64
		for _, rt := range recs {
			if v, ok := rt.Phases[phase]; ok {
				d = append(d, v*1000)
			}
		}
		if len(d) == 0 {
			continue
		}
		sort.Float64s(d)
		out = append(out, PhaseStat{Phase: phase, Count: len(d), P50MS: percentile(d, 0.50), P90MS: percentile(d, 0.90), P95MS: percentile(d, 0.95), MaxMS: percentile(d, 1)})
	}
	return out
}

// spanStat summarizes event spans under name; nil when there are none.
func spanStat(name string, d []float64) *PhaseStat {
	if len(d) == 0 {
		return nil
	}
	sort.Float64s(d)
	return &PhaseStat{Phase: name, Count: len(d), P50MS: percentile(d, 0.50), P90MS: percentile(d, 0.90), P95MS: percentile(d, 0.95), MaxMS: percentile(d, 1)}
}

// labelMatchesMode reports whether every report's mode label, where it has
// one, is its mode: the loader's own label, which the mode row already shows.
func labelMatchesMode(reps []*Report) bool {
	for _, r := range reps {
		if v, ok := r.Labels["mode"]; ok && v != r.Mode {
			return false
		}
	}
	return true
}

// labelKeys is the sorted union of the reports' label keys.
func labelKeys(reps []*Report) []string {
	seen := map[string]bool{}
	for _, r := range reps {
		for k := range r.Labels {
			seen[k] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// labelLine renders labels as "k=v k=v", keys sorted.
func labelLine(labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for _, k := range labelKeys([]*Report{{Labels: labels}}) {
		parts = append(parts, k+"="+labels[k])
	}
	return strings.Join(parts, " ")
}

func buildRestoreBreakdownFromSummary(rep *Report, ss map[string]schema.MetricSummary) *RestoreBreakdown {
	if len(ss) == 0 {
		return nil
	}
	toPhaseStat := func(phase string, st schema.MetricSummary) PhaseStat {
		p90 := st.P90
		if p90 == 0 {
			p90 = st.P95
		}
		if p90 == 0 {
			p90 = st.Max
		}
		p95 := st.P95
		if p95 == 0 {
			p95 = st.P90
		}
		if p95 == 0 {
			p95 = st.Max
		}
		return PhaseStat{
			Phase: phase,
			Count: st.Count,
			P50MS: round2(st.P50),
			P90MS: round2(p90),
			P95MS: round2(p95),
			MaxMS: round2(st.Max),
		}
	}

	var phases []PhaseStat
	var totalStat schema.MetricSummary
	var hasTotal bool

	if tot, ok := ss["restore_total_ms"]; ok && tot.Count > 0 {
		totalStat = tot
		hasTotal = true
		for _, pair := range []struct {
			key   string
			phase string
		}{
			{"restore_volume_mount_ms", "volume_mount"},
			{"restore_manifest_fetch_ms", "manifest_fetch"},
			{"restore_sandbox_assets_ms", "sandbox_assets"},
			{"restore_download_ms", "download"},
			{"restore_oci_unpack_ms", "oci_unpack"},
			{"restore_worker_restore_ms", "worker_restore"},
			{"restore_persist_ms", "persist"},
			{"restore_total_ms", "total"},
		} {
			if st, ok := ss[pair.key]; ok && st.Count > 0 {
				phases = append(phases, toPhaseStat(pair.phase, st))
			}
		}
	} else if tot, ok := ss["pod_total_ready_ms"]; ok && tot.Count > 0 {
		totalStat = tot
		hasTotal = true
		cnt := tot.Count
		sched := ss["pod_schedule_ms"]
		if sched.Count == 0 {
			sched.Count = cnt
		}
		initSt := ss["pod_init_ms"]
		if initSt.Count == 0 {
			initSt.Count = cnt
		}
		cstart := ss["pod_container_start_ms"]
		if cstart.Count == 0 {
			cstart.Count = cnt
		}
		zeroSt := schema.MetricSummary{Count: cnt}
		phases = []PhaseStat{
			toPhaseStat("volume_mount", sched),
			toPhaseStat("manifest_fetch", initSt),
			toPhaseStat("download", zeroSt),
			toPhaseStat("oci_unpack", cstart),
			toPhaseStat("worker_restore", zeroSt),
			toPhaseStat("persist", zeroSt),
			toPhaseStat("total", tot),
			toPhaseStat("pod_schedule", sched),
			toPhaseStat("pod_init", initSt),
			toPhaseStat("pod_container_start", cstart),
		}
	}
	if !hasTotal {
		return nil
	}

	rb := &RestoreBreakdown{
		First: RestoreGroup{
			Restores: totalStat.Count,
			Phases:   phases,
		},
	}
	if evSt, ok := rep.StatFor(EventTTFI, ""); ok && evSt.Count > 0 {
		totP90 := totalStat.P90
		if totP90 == 0 {
			totP90 = totalStat.P95
		}
		totP95 := totalStat.P95
		if totP95 == 0 {
			totP95 = totalStat.P90
		}
		rb.First.Outside = &PhaseStat{
			Phase: "outside",
			Count: evSt.Count,
			P50MS: round2(math.Max(0, evSt.P50MS-totalStat.P50)),
			P90MS: round2(math.Max(0, evSt.P90MS-totP90)),
			P95MS: round2(math.Max(0, evSt.P95MS-totP95)),
			MaxMS: round2(math.Max(0, evSt.MaxMS-totalStat.Max)),
		}
	} else if evSt, ok := rep.StatFor(EventCreate, ""); ok && evSt.Count > 0 {
		totP90 := totalStat.P90
		if totP90 == 0 {
			totP90 = totalStat.P95
		}
		totP95 := totalStat.P95
		if totP95 == 0 {
			totP95 = totalStat.P90
		}
		rb.First.Outside = &PhaseStat{
			Phase: "outside",
			Count: evSt.Count,
			P50MS: round2(math.Max(0, evSt.P50MS-totalStat.P50)),
			P90MS: round2(math.Max(0, evSt.P90MS-totP90)),
			P95MS: round2(math.Max(0, evSt.P95MS-totP95)),
			MaxMS: round2(math.Max(0, evSt.MaxMS-totalStat.Max)),
		}
	}
	if wakeSt, ok := rep.StatFor(EventWake, ""); ok && wakeSt.Count > 0 {
		rb.Wakes = RestoreGroup{
			Restores: wakeSt.Count,
			Phases: []PhaseStat{
				{
					Phase: "worker_restore",
					Count: wakeSt.Count,
					P50MS: wakeSt.P50MS,
					P90MS: wakeSt.P90MS,
					P95MS: wakeSt.P95MS,
					MaxMS: wakeSt.MaxMS,
				},
				{
					Phase: "total",
					Count: wakeSt.Count,
					P50MS: wakeSt.P50MS,
					P90MS: wakeSt.P90MS,
					P95MS: wakeSt.P95MS,
					MaxMS: wakeSt.MaxMS,
				},
			},
			Outside: &PhaseStat{
				Phase: "outside",
				Count: wakeSt.Count,
			},
		}
	}
	return rb
}

// NormalizeLegacyReport populates derived baseline/park_mode/schema_era fields
// on reports produced before the scale-ladder audit (G8) so pre-audit and
// post-audit reports can be compared without nil-pointer or missing-metadata
// failures while preserving authentic measurements.
func NormalizeLegacyReport(rep *Report) {
	if rep == nil {
		return
	}
	if rep.SystemDescription == nil && rep.HardwareCost == nil {
		rep.SchemaEra = "pre-audit-legacy"
	} else {
		rep.SchemaEra = "post-audit-v2"
	}
	if rep.ParkMode == "" {
		rep.ParkMode = inferParkMode(rep)
	}
	b := &Builder{}
	if rep.Headline == nil {
		rep.Headline = b.buildHeadlineBaseline(rep)
	}
	if rep.RLBreakdown == nil {
		rep.RLBreakdown = b.buildRLStepBreakdown(rep)
	}
	// FaultClassification stays nil on a report built before the field
	// existed: the per-event data it needs is gone, and the renderers say
	// "not classified" instead of estimating from the error counts.
	rep.PopulateCacheMissesPerFirstCmd()
	rep.PopulateStepChurnAndConcurrency(rep.ActiveSandboxes)
}

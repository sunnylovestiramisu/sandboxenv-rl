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
	"errors"
	"fmt"
)

// JobMode enumerates the supported job runner modes in a Scenario.
type JobMode string

const (
	// JobModeClosedLoop runs step-barrier-synchronized batch rollouts.
	JobModeClosedLoop JobMode = "closed-loop"
	// JobModeOpenLoop runs Poisson-arriving tasks at a target arrival rate.
	JobModeOpenLoop JobMode = "open-loop"
	// JobModeReal launches a real trainer workload (in-cluster or external).
	JobModeReal JobMode = "real"
)

// Scenario-level ParkPolicy aliases (ParkPolicy is defined in schema.go).
const (
	// ParkPolicyHold retains active pods across trainer pauses.
	ParkPolicyHold ParkPolicy = "hold"
	// ParkPolicySnapshot suspends active environments to snapshot and resumes next step.
	ParkPolicySnapshot ParkPolicy = "snapshot"
	// ParkPolicyRelease deletes active environments and re-acquires next step.
	ParkPolicyRelease ParkPolicy = "release"
)

// Scenario defines one benchmark run against a System Under Test (SUT).
type Scenario struct {
	SchemaVersion string `json:"schema_version"`
	Name          string `json:"name"`
	Description   string `json:"description,omitempty"`
	Seed          int64  `json:"seed,omitempty"`
	Repeat        int    `json:"repeat,omitempty"`
	Footing       string `json:"footing,omitempty"`
	// Preflight names the image placement pre-flight a warm footing rests
	// on: the plan the loader ran with (`swebench-loader run -placement
	// -preflight`) and the coverage file it wrote, which `bench run` copies
	// into the bundle as preflight-coverage.json for Validity Rule 1.
	Preflight    *PreflightSpec            `json:"preflight,omitempty"`
	SUT          SUTSpec                   `json:"sut"`
	Jobs         []JobSpec                 `json:"jobs"`
	WindowPolicy *WindowPolicy             `json:"window_policy,omitempty"`
	Window       WindowPolicy              `json:"window,omitempty"`
	Telemetry    *TelemetrySpec            `json:"telemetry,omitempty"`
	Variants     []FeatureOverlay          `json:"variants,omitempty"`
	Overlays     map[string]FeatureOverlay `json:"overlays,omitempty"`
}

// PreflightSpec is the pre-flight a scenario's footing rests on. Plan is the
// image-placement.json; Coverage is the preflight-coverage.json a pre-flight
// over it wrote, relative to the scenario file when not absolute. Either can
// be absent; when both are given, bench run checks that the coverage names
// the plan (by file name), and a coverage is used for a warm footing only.
type PreflightSpec struct {
	Plan     string `json:"plan,omitempty"`
	Coverage string `json:"coverage,omitempty"`
}

// Validate checks Scenario invariants.
func (s Scenario) Validate() error {
	if s.SchemaVersion != "" && s.SchemaVersion != SchemaVersion {
		return fmt.Errorf("scenario: unsupported schema_version %q (want %q)", s.SchemaVersion, SchemaVersion)
	}
	if s.SchemaVersion == "" {
		return errors.New("scenario: schema_version is required")
	}
	if s.Name == "" {
		return errors.New("scenario: name is required")
	}
	if s.Repeat < 0 {
		return fmt.Errorf("scenario: repeat must be >= 0, got %d", s.Repeat)
	}
	// scenario.schema.json has the same enum; absent is allowed.
	if s.Footing != "" && s.Footing != "warm" && s.Footing != "cold" {
		return fmt.Errorf("scenario: footing must be warm or cold, got %q", s.Footing)
	}
	if s.Preflight != nil && s.Preflight.Plan == "" && s.Preflight.Coverage == "" {
		return errors.New("scenario: preflight needs a plan or a coverage path")
	}
	if err := s.SUT.Validate(); err != nil {
		return fmt.Errorf("scenario sut: %w", err)
	}
	if len(s.Jobs) == 0 {
		return errors.New("scenario: at least one job is required")
	}
	seenJobs := make(map[string]bool, len(s.Jobs))
	for i, j := range s.Jobs {
		if err := j.Validate(); err != nil {
			return fmt.Errorf("scenario job[%d]: %w", i, err)
		}
		if seenJobs[j.Name] {
			return fmt.Errorf("scenario: duplicate job name %q", j.Name)
		}
		seenJobs[j.Name] = true
	}
	if s.WindowPolicy != nil {
		if err := s.WindowPolicy.Validate(); err != nil {
			return fmt.Errorf("scenario window_policy: %w", err)
		}
	}
	if s.Telemetry != nil {
		if err := s.Telemetry.Validate(); err != nil {
			return fmt.Errorf("scenario telemetry: %w", err)
		}
	}
	for i, v := range s.Variants {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("scenario variant[%d]: %w", i, err)
		}
	}
	for _, v := range s.Overlays {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("scenario overlay %q: %w", v.Name, err)
		}
	}
	return nil
}

// SUTSpec describes the System Under Test configuration.
type SUTSpec struct {
	Backend        string            `json:"backend,omitempty"`
	Access         string            `json:"access,omitempty"`
	Endpoint       string            `json:"endpoint,omitempty"`
	Substrate      *SubstrateSUTSpec `json:"substrate,omitempty"`
	NodePool       string            `json:"node_pool,omitempty"`
	RuntimeClass   string            `json:"runtime_class,omitempty"`
	Replicas       int               `json:"replicas,omitempty"`
	ExtraArgs      map[string]string `json:"extra_args,omitempty"`
	Namespace      string            `json:"namespace,omitempty"`
	Template       string            `json:"template,omitempty"`
	ServerAddr     string            `json:"server_addr,omitempty"`
	StorageBackend string            `json:"storage_backend,omitempty"`
	PoolEnabled    *bool             `json:"pool_enabled,omitempty"`
	PoolSizePerImg int               `json:"pool_size_per_image,omitempty"`
	MaxActivePods  int               `json:"max_active_pods,omitempty"`
	ValuesOverlay  map[string]any    `json:"values_overlay,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

// SubstrateSUTSpec holds Substrate-specific SUT configuration parameters.
type SubstrateSUTSpec struct {
	Atespace     string `json:"atespace,omitempty"`
	BaseTemplate string `json:"base_template,omitempty"`
}

// Validate checks SUTSpec invariants.
func (s SUTSpec) Validate() error {
	if s.Backend != "" && s.Backend != "substrate" && s.Backend != "agent-sandbox" {
		return fmt.Errorf("invalid backend %q (want \"substrate\" or \"agent-sandbox\")", s.Backend)
	}
	if s.Access != "" && s.Access != "sdk" && s.Access != "ate-env-api" && s.Access != "direct" {
		return fmt.Errorf("invalid access %q (want \"sdk\", \"ate-env-api\", or \"direct\")", s.Access)
	}
	if s.Replicas < 0 {
		return fmt.Errorf("replicas must be >= 0, got %d", s.Replicas)
	}
	return nil
}

// JobSpec defines one simulated or real RL job loop within a Scenario.
type JobSpec struct {
	Name            string            `json:"name"`
	Kind            string            `json:"kind,omitempty"`
	Mode            JobMode           `json:"mode,omitempty"`
	LoaderMode      string            `json:"loader_mode,omitempty"`
	RealMode        string            `json:"real_mode,omitempty"`
	Priority        int               `json:"priority,omitempty"`
	Weight          float64           `json:"weight,omitempty"`
	StartOffsetMS   float64           `json:"start_offset_ms,omitempty"`
	Dataset         string            `json:"dataset,omitempty"`
	DatasetPath     string            `json:"dataset_path,omitempty"`
	Trajectory      string            `json:"trajectory,omitempty"`
	ImagePrefix     string            `json:"image_prefix,omitempty"`
	Park            ParkPolicy        `json:"park,omitempty"`
	ParkPolicy      ParkPolicy        `json:"park_policy,omitempty"`
	Images          int               `json:"images,omitempty"`
	Instances       int               `json:"instances,omitempty"`
	TasksPerStep    int               `json:"tasks_per_step,omitempty"`
	BatchSize       int               `json:"batch_size,omitempty"`
	GroupSize       int               `json:"group_size,omitempty"`
	RolloutsPerTask int               `json:"rollouts_per_task,omitempty"`
	Steps           int               `json:"steps,omitempty"`
	MaxConcurrent   int               `json:"max_concurrent,omitempty"`
	Concurrency     int               `json:"concurrency,omitempty"`
	StepPauseMS     float64           `json:"step_pause_ms,omitempty"`
	ArrivalRate     float64           `json:"arrival_rate,omitempty"`
	DurationSec     float64           `json:"duration_sec,omitempty"`
	Prefetch        bool              `json:"prefetch,omitempty"`
	LookaheadSteps  int               `json:"lookahead_steps,omitempty"`
	SnapshotLoop    bool              `json:"snapshot_loop,omitempty"`
	Seed            int64             `json:"seed,omitempty"`
	ScheduleMode    string            `json:"schedule_mode,omitempty"`
	TrainerManifest string            `json:"trainer_manifest,omitempty"`
	TrainerCommand  []string          `json:"trainer_command,omitempty"`
	TraceMode       string            `json:"trace_mode,omitempty"`
	EnvVars         map[string]string `json:"env_vars,omitempty"`
}

// Validate checks JobSpec invariants.
func (j JobSpec) Validate() error {
	if j.Name == "" {
		return errors.New("job name is required")
	}
	if j.Kind != "" {
		switch j.Kind {
		case "simulated":
			if j.Dataset == "" && j.DatasetPath == "" && j.Mode == "" {
				return fmt.Errorf("job %q: simulated job requires dataset and trajectory", j.Name)
			}
			if j.Dataset != "" && j.Trajectory == "" && j.Mode == "" {
				return fmt.Errorf("job %q: simulated job requires dataset and trajectory", j.Name)
			}
			if j.RealMode != "" {
				return fmt.Errorf("job %q: simulated job cannot set real_mode %q", j.Name, j.RealMode)
			}
			if j.LoaderMode != "" && j.LoaderMode != "eval" && j.LoaderMode != "steps" && j.LoaderMode != "open-loop" {
				return fmt.Errorf("job %q: invalid loader_mode %q (want \"eval\", \"steps\", or \"open-loop\")", j.Name, j.LoaderMode)
			}
			if j.LoaderMode == "steps" && j.Steps < 1 {
				return fmt.Errorf("job %q: steps loader_mode requires steps >= 1, got %d", j.Name, j.Steps)
			}
		case "real":
			if j.LoaderMode != "" {
				return fmt.Errorf("job %q: real job cannot set loader_mode %q", j.Name, j.LoaderMode)
			}
			if j.RealMode != "" && j.RealMode != "measure" && j.RealMode != "trace" {
				return fmt.Errorf("job %q: real job requires real_mode \"measure\" or \"trace\", got %q", j.Name, j.RealMode)
			}
			if j.RealMode == "" && j.Mode == "" {
				return fmt.Errorf("job %q: real job requires real_mode \"measure\" or \"trace\"", j.Name)
			}
		default:
			return fmt.Errorf("job %q: invalid kind %q (want \"simulated\" or \"real\")", j.Name, j.Kind)
		}
	} else if j.Mode != "" {
		switch j.Mode {
		case JobModeClosedLoop, JobModeOpenLoop, JobModeReal:
		default:
			return fmt.Errorf("job %q: invalid mode %q", j.Name, j.Mode)
		}
	} else {
		return fmt.Errorf("job %q: kind or mode is required", j.Name)
	}
	if j.ScheduleMode != "" && j.ScheduleMode != "fixed" && j.ScheduleMode != "sampled" && j.ScheduleMode != "explicit" {
		return fmt.Errorf("job %q: invalid schedule_mode %q (want \"fixed\", \"sampled\", or \"explicit\")", j.Name, j.ScheduleMode)
	}
	if j.Park != "" {
		if _, err := NormalizeParkPolicy(j.Park); err != nil {
			return fmt.Errorf("job %q: %w", j.Name, err)
		}
	}
	if j.ParkPolicy != "" {
		if _, err := NormalizeParkPolicy(j.ParkPolicy); err != nil {
			return fmt.Errorf("job %q: %w", j.Name, err)
		}
	}
	if j.Images < 0 || j.TasksPerStep < 0 || j.GroupSize < 0 || j.Steps < 0 || j.MaxConcurrent < 0 {
		return fmt.Errorf("job %q: numeric counts must be >= 0", j.Name)
	}
	if j.StartOffsetMS < 0 || j.StepPauseMS < 0 {
		return fmt.Errorf("job %q: start_offset_ms and step_pause_ms must be >= 0", j.Name)
	}
	return nil
}

// WindowPolicy configures steady-state step boundaries and run duration caps inside Scenario.
type WindowPolicy struct {
	SteadyStateStartStep int     `json:"steady_state_start_step,omitempty"`
	SteadyStateEndStep   int     `json:"steady_state_end_step,omitempty"`
	WarmupSteps          int     `json:"warmup_steps,omitempty"`
	CooldownSteps        int     `json:"cooldown_steps,omitempty"`
	WarmupSec            float64 `json:"warmup_sec,omitempty"`
	CooldownSec          float64 `json:"cooldown_sec,omitempty"`
	MaxDurationS         float64 `json:"max_duration_s,omitempty"`
}

// Validate checks WindowPolicy invariants.
func (w WindowPolicy) Validate() error {
	if w.SteadyStateStartStep < 0 || w.SteadyStateEndStep < 0 || w.WarmupSteps < 0 {
		return errors.New("step bounds must be >= 0")
	}
	if w.SteadyStateEndStep > 0 && w.SteadyStateStartStep > w.SteadyStateEndStep {
		return fmt.Errorf("steady_state_start_step (%d) > steady_state_end_step (%d)", w.SteadyStateStartStep, w.SteadyStateEndStep)
	}
	if w.MaxDurationS < 0 {
		return fmt.Errorf("max_duration_s must be >= 0, got %f", w.MaxDurationS)
	}
	return nil
}

// FeatureOverlay bundles A/B feature overrides onto a base Scenario.
type FeatureOverlay struct {
	SchemaVersion string            `json:"schema_version,omitempty"`
	Name          string            `json:"name"`
	Description   string            `json:"description,omitempty"`
	Park          ParkPolicy        `json:"park,omitempty"`
	ParkPolicy    string            `json:"park_policy,omitempty"`
	Prefetch      *bool             `json:"prefetch,omitempty"`
	Lookahead     int               `json:"lookahead_steps,omitempty"`
	WarmPoolSize  int               `json:"warm_pool_size,omitempty"`
	SnapshotMode  string            `json:"snapshot_mode,omitempty"`
	PrewarmPolicy string            `json:"prewarm_policy,omitempty"`
	SUT           SUTSpec           `json:"sut,omitempty"`
	EnvVars       map[string]string `json:"env_vars,omitempty"`
	Flags         map[string]string `json:"flags,omitempty"`
}

// Validate checks FeatureOverlay invariants.
func (o FeatureOverlay) Validate() error {
	if o.SchemaVersion != "" && o.SchemaVersion != SchemaVersion {
		return fmt.Errorf("feature overlay %q: unsupported schema_version %q", o.Name, o.SchemaVersion)
	}
	if o.Name == "" {
		return errors.New("feature overlay name is required")
	}
	if o.Park != "" {
		if _, err := NormalizeParkPolicy(o.Park); err != nil {
			return fmt.Errorf("feature overlay %q: %w", o.Name, err)
		}
	}
	if o.ParkPolicy != "" {
		if _, err := NormalizeParkPolicy(ParkPolicy(o.ParkPolicy)); err != nil {
			return fmt.Errorf("feature overlay %q: %w", o.Name, err)
		}
	}
	return nil
}

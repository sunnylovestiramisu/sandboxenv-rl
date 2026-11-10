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
	"hash/fnv"
	"log/slog"
	"math"
	"math/rand"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	env "github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mode selects the workload shape.
type Mode string

const (
	// ModeEval runs every (image, task) once: create, replay, delete.
	ModeEval Mode = "eval"
	// ModeSteps keeps a fleet across training steps and parks it in between.
	ModeSteps Mode = "steps"
	// ModeOpenLoop dispatches tasks at a Poisson arrival rate (-arrival-rate)
	// bounded by Width concurrency slots, recording SlotWaitMS per task.
	ModeOpenLoop Mode = "open-loop"
)

// Park is what happens to the fleet between training steps.
type Park string

const (
	ParkNone Park = "none" // environments stay running
	// ParkSuspend is a durable suspend: the call returns once the snapshot
	// is in object storage. The next command wakes the environment.
	ParkSuspend Park = "suspend"
	// ParkSuspendSync is the schedule's name for the same durable suspend.
	ParkSuspendSync Park = "suspend-sync"
	// ParkSuspendAsync is a local suspend (the SDK's SuspendLocal): the call
	// returns once the snapshot is on the node, and the upload goes on in
	// the background. Until it lands the environment resumes on that node
	// only.
	ParkSuspendAsync Park = "suspend-async"
	ParkFork         Park = "fork"   // Fork from golden base snapshot per step
	ParkDelete       Park = "delete" // DeleteEnvironment; the next step creates again
)

// Config is one run. Every field that shapes the load is a parameter so the
// same binary serves the smoke test and the 500-wide run.
type Config struct {
	RunID      string
	Dataset    string
	Backend    Backend
	Instances  []Instance
	Trajectory *Trajectory
	Recorder   *Recorder
	Logger     *slog.Logger

	Mode Mode
	// Strategy, when non-empty, selects the warm-pool orchestration strategy
	// (none, naive, sliding, or pipelined) matching Python SandboxFleet.
	Strategy Strategy
	// WindowSize bounds the number of distinct images warmed per batch in
	// StrategySliding and StrategyPipelined (0 uses DefaultWindowSize).
	WindowSize int
	// Width is the most tasks in flight at once (the "500 wide").
	Width int
	// TasksPerImage is the eval mode fan-out per image.
	TasksPerImage int
	// Rollouts and Steps shape steps mode: Instances x Rollouts environments
	// driven through Steps training steps.
	Rollouts int
	Steps    int
	Park     Park
	// ImagesPerStep, in steps mode, makes each step take the next
	// ImagesPerStep instances of the dataset (wrapping around), create
	// Rollouts fresh environments for each, and retire them after the step:
	// the shape of a trainer that creates and retires pools every step. A
	// new image is a new derived template and a golden bake on first use,
	// which is the cost this shape measures. Requires ParkDelete. Zero keeps
	// one fleet across the steps.
	ImagesPerStep int
	// ParkEach, in steps mode, parks each environment as soon as its step
	// ends instead of parking the fleet after the whole step, so at most
	// Width environments are awake at once and the fleet can be larger than
	// the running slots: the shape of a trainer that releases a sandbox at
	// the end of its episode. The step barrier then includes the parking and
	// there is no park event. Requires a suspend park or ParkDelete, and not
	// ImagesPerStep, which retires its environments anyway.
	ParkEach bool

	// Schedule is an optional pre-resolved step-by-step schedule. When set (or
	// when TasksPerStep, GroupSize, StepPause, Prefetch, ParkSuspendAsync,
	// ParkSuspendSync, or ParkFork are used in ModeSteps), steps mode drives a
	// client-side WorkingSet across the scheduled steps.
	Schedule     *schema.Schedule
	TasksPerStep int
	GroupSize    int
	StepPause    time.Duration
	Prefetch     bool

	// ArrivalRate is the target Poisson arrival rate (tasks/sec) in ModeOpenLoop.
	ArrivalRate float64
	// Duration bounds the open-loop arrival window when set.
	Duration     time.Duration
	Concurrency  int
	JobName      string
	SnapshotLoop bool

	// CreateTimeout bounds one create call; StepTimeout is the default for a
	// run step without its own timeout.
	CreateTimeout time.Duration
	StepTimeout   time.Duration
	// WakeBudget is how long the first command after a create or a park is
	// retried on transient errors: a wake that outlasts the router's parking
	// budget answers Unavailable, and a first create whose image is still
	// being golden-baked answers Unknown until the bake is done. WakeBackoff
	// is the initial pause between attempts; it doubles up to five times
	// that.
	WakeBudget  time.Duration
	WakeBackoff time.Duration
	// Keep leaves the environments in place when the run ends.
	Keep bool
	// Labels are the operator's labels for the run event: the Substrate
	// commit, the cluster, the arm. A key the loader records itself
	// (LoaderLabelKeys, or the set. prefix) is refused by validate, so the
	// same-workload check of report -arm cannot be overridden.
	Labels map[string]string
	// Invocation holds the loader's own labels that only its command line
	// knows: dataset, connections, base_template, exclude_file, instances.
	Invocation map[string]string
	// Trajectories, when it holds more than one, is the mix: each
	// environment gets one of them, drawn with Seed, so a run can interleave
	// workloads the way a training step mixes task shapes. Trajectory is the
	// first of them.
	Trajectories []*Trajectory
	Seed         int64
}

// trajectoryNames joins the names of the run's trajectories.
func (c *Config) trajectoryNames() string {
	if len(c.Trajectories) <= 1 {
		return c.Trajectory.Name
	}
	names := make([]string, len(c.Trajectories))
	for i, t := range c.Trajectories {
		names[i] = t.Name
	}
	return strings.Join(names, ",")
}

// LoaderLabelKeys are the labels the loader records about its own invocation
// on the run event: the run's shape and the settings that move its latencies
// and failure counts. report -arm pools only reps that agree on all of them,
// and on every -set value, recorded as set.KEY. An operator -label may not use
// one of them, nor the set. prefix.
var LoaderLabelKeys = []string{
	"mode", "strategy", "window_size", "width", "images", "trajectory", "tasks_per_image", "rollouts", "steps", "park",
	"create_timeout", "step_timeout", "wake_budget", "wake_backoff",
	"dataset", "connections", "delete_rate", "base_template", "exclude_file", "instances", "seed", "images_per_step",
	"park_each", "placement",
}

// labels describes the run for its run event.
func (c *Config) labels() map[string]string {
	l := map[string]string{
		"mode":           string(c.Mode),
		"width":          strconv.Itoa(c.Width),
		"images":         strconv.Itoa(len(c.Instances)),
		"trajectory":     c.trajectoryNames(),
		"create_timeout": c.CreateTimeout.String(),
		"step_timeout":   c.StepTimeout.String(),
		"wake_budget":    c.WakeBudget.String(),
		"wake_backoff":   c.WakeBackoff.String(),
	}
	if c.Strategy != "" {
		l["strategy"] = string(c.Strategy)
		if c.Strategy == StrategySliding || c.Strategy == StrategyPipelined {
			l["window_size"] = strconv.Itoa(c.effectiveWindowSize())
		}
	}
	if len(c.Trajectories) > 1 {
		l["seed"] = strconv.FormatInt(c.Seed, 10)
	}
	switch c.Mode {
	case ModeEval:
		l["tasks_per_image"] = strconv.Itoa(c.TasksPerImage)
	case ModeSteps:
		l["rollouts"] = strconv.Itoa(c.Rollouts)
		l["steps"] = strconv.Itoa(c.Steps)
		parkLbl := string(c.Park)
		if c.Strategy == StrategyNone {
			parkLbl = string(ParkDelete)
		} else if c.Strategy != "" {
			parkLbl = string(ParkSuspend)
		}
		l["park"] = parkLbl
		if c.ImagesPerStep > 0 {
			l["images_per_step"] = strconv.Itoa(c.ImagesPerStep)
		}
		if c.ParkEach {
			l["park_each"] = "true"
		}
	}
	for k, v := range c.Invocation {
		l[k] = v
	}
	for k, v := range c.Labels {
		l[k] = v
	}
	return l
}

// Summary is what a run produced, for the exit status and the log.
type Summary struct {
	Tasks                int
	Failed               int
	Rewarded             int
	Duration             time.Duration
	WorkingSetHits       int
	WorkingSetMisses     int
	WorkingSetHitRate    float64
	StandingPoolSize     int
	Created              int
	Reused               int
	Retired              int
	PrefetchLeadMarginMS float64
	ReleaseOverlapMS     float64
}

func (c *Config) usesWorkingSet() bool {
	if c.Strategy != "" {
		return false
	}
	return c.Mode == ModeSteps && (c.Schedule != nil || c.TasksPerStep > 0 || c.GroupSize > 0 ||
		c.StepPause > 0 || c.Prefetch)
}

func (c *Config) validate() error {
	if err := ValidateStrategy(c.Strategy); err != nil {
		return err
	}
	if c.WindowSize < 0 {
		return errors.New("window size must be >= 0")
	}
	if c.WindowSize > 0 && c.Strategy != StrategySliding && c.Strategy != StrategyPipelined {
		return errors.New("window size applies to sliding or pipelined strategy")
	}
	if c.Strategy != "" && c.Mode == ModeOpenLoop {
		return errors.New("strategy applies to eval or steps mode, not open-loop")
	}
	if c.Strategy != "" && c.Mode == ModeSteps && c.Steps > 1 {
		return errors.New("strategy is single-pass (requires -steps 1 in steps mode; omit -strategy for multi-step reset loops)")
	}
	if c.Strategy != "" {
		// A strategy owns the lifecycle: it parks by suspend (delete for
		// none) and runs one pass, so the step loop's settings have nothing
		// to act on. Refuse them rather than record a park or a pause that
		// did not happen.
		if c.Mode == ModeSteps {
			if c.Park == "" {
				c.Park = c.strategyPark()
			} else if c.Park != c.strategyPark() {
				return fmt.Errorf("strategy %s parks by %s; park mode %s cannot apply (omit -park with -strategy)", c.Strategy, c.strategyPark(), c.Park)
			}
		}
		switch {
		case c.StepPause > 0:
			return errors.New("step pause belongs to the step loop; it cannot be combined with a strategy")
		case c.ParkEach:
			return errors.New("park each belongs to the step loop; it cannot be combined with a strategy")
		case c.ImagesPerStep > 0:
			return errors.New("images per step belongs to the step loop; it cannot be combined with a strategy")
		case c.Prefetch:
			return errors.New("prefetch belongs to the schedule's step loop; a strategy prefetches by its window (pipelined)")
		case c.Schedule != nil:
			return errors.New("a schedule drives the step loop; it cannot be combined with a strategy")
		}
	}
	if c.Schedule != nil && c.Mode == ModeSteps && len(c.Schedule.Jobs) > 0 {
		if c.Rollouts < 1 {
			c.Rollouts = 1
		}
		if c.Steps < 1 {
			c.Steps = len(c.Schedule.Jobs[0].Steps)
		}
		if c.Park == "" {
			c.Park = Park(c.Schedule.Jobs[0].ParkPolicy)
		}
	}
	if c.Mode == ModeSteps && c.Rollouts < 1 && c.GroupSize > 0 {
		c.Rollouts = c.GroupSize
	}
	switch {
	case c.RunID == "":
		return errors.New("run id is required")
	case !runIDPattern.MatchString(c.RunID):
		return fmt.Errorf("run id %q must be lowercase letters, digits and '-'", c.RunID)
	case len(c.RunID) > maxRunIDLen:
		return fmt.Errorf("run id %q is longer than %d characters; it prefixes every environment name", c.RunID, maxRunIDLen)
	case c.Backend == nil:
		return errors.New("backend is required")
	case len(c.Instances) == 0:
		return errors.New("no instances")
	case c.Trajectory == nil && len(c.Trajectories) == 0:
		return errors.New("trajectory is required")
	case c.Width < 1:
		return errors.New("width must be >= 1")
	}
	for i, t := range c.Trajectories {
		if t == nil {
			return fmt.Errorf("trajectory %d of the mix is nil", i+1)
		}
	}
	if c.Trajectory == nil {
		c.Trajectory = c.Trajectories[0]
	}
	switch c.Mode {
	case ModeEval:
		if c.TasksPerImage < 1 {
			return errors.New("tasks per image must be >= 1")
		}
	case ModeSteps:
		if c.Rollouts < 1 || c.Steps < 1 {
			return errors.New("rollouts and steps must be >= 1")
		}
		switch c.Park {
		case ParkNone, ParkSuspend, ParkSuspendSync, ParkSuspendAsync, ParkFork, ParkDelete:
		default:
			return fmt.Errorf("unknown park mode %q", c.Park)
		}
		if c.ImagesPerStep < 0 {
			return errors.New("images per step must be >= 0")
		}
		if c.ImagesPerStep > 0 && c.Park != ParkDelete {
			return errors.New("images per step retires each step's environments; it needs park mode delete")
		}
		if c.ImagesPerStep > len(c.Instances) {
			return fmt.Errorf("images per step (%d) is more than the %d instances; a window must not repeat an instance within one step", c.ImagesPerStep, len(c.Instances))
		}
		if c.ParkEach && (c.Park == ParkNone || c.Park == ParkFork) {
			return errors.New("park each needs park mode suspend, suspend-sync, suspend-async or delete")
		}
		if c.ParkEach && c.ImagesPerStep > 0 {
			return errors.New("park each keeps one fleet across the steps; it cannot be combined with images per step")
		}
	case ModeOpenLoop:
		if c.TasksPerImage < 1 {
			c.TasksPerImage = 1
		}
		if c.ArrivalRate <= 0 {
			return errors.New("arrival rate must be > 0 in open-loop mode")
		}
	default:
		return fmt.Errorf("unknown mode %q", c.Mode)
	}
	if c.ImagesPerStep > 0 && c.Mode != ModeSteps {
		return errors.New("images per step applies to steps mode")
	}
	if c.ParkEach && c.Mode != ModeSteps {
		return errors.New("park each applies to steps mode")
	}
	if n := c.longestName(); len(n) > maxNameLen {
		return fmt.Errorf("environment name %q has %d characters, more than Substrate's %d; "+
			"use a shorter run id, fewer rollouts or tasks per image, or fewer steps", n, len(n), maxNameLen)
	}
	for k := range c.Labels {
		if slices.Contains(LoaderLabelKeys, k) || strings.HasPrefix(k, "set.") {
			return fmt.Errorf("label %s: the loader records that key itself, and report -arm trusts it; choose another name", k)
		}
	}
	return nil
}

// longestName is the longest environment name the run builds: EnvID for the
// largest replay index of each instance, plus the "-s<step>" suffix that a
// delete park gives the environments of later steps.
func (c *Config) longestName() string {
	k, suffix := c.TasksPerImage-1, ""
	if c.Strategy != "" {
		k = c.strategyFanout() - 1 // one pass: no step suffix
	} else if c.Mode == ModeSteps {
		k = c.Rollouts - 1
		if (c.Park == ParkDelete || c.Park == ParkFork || c.ImagesPerStep > 0) && c.Steps > 1 {
			suffix = fmt.Sprintf("-s%d", c.Steps)
		}
	}
	longest := ""
	for _, in := range c.Instances {
		if n := EnvID(c.RunID, in.InstanceID, k) + suffix; len(n) > len(longest) {
			longest = n
		}
	}
	return longest
}

func (c *Config) defaults() {
	if c.Recorder == nil {
		c.Recorder = NewRecorder(nil)
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.CreateTimeout <= 0 {
		c.CreateTimeout = 5 * time.Minute
	}
	if c.StepTimeout <= 0 {
		c.StepTimeout = 10 * time.Minute
	}
	if c.WakeBudget <= 0 {
		c.WakeBudget = 3 * time.Minute
	}
	if c.WakeBackoff <= 0 {
		c.WakeBackoff = DefaultWakeBackoff
	}
}

// DefaultWakeBackoff is the initial pause between first-command retries.
const DefaultWakeBackoff = 2 * time.Second

// runIDPattern is Substrate's name rule (a k8s short name); the run id is a
// prefix of every environment name. maxRunIDLen caps the run id; validate
// checks the longest name the run builds against maxNameLen, because the
// instance ids, the replay index and the step suffix add to it.
var runIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const (
	maxRunIDLen = 20
	maxNameLen  = 63
)

// cleanupTimeout bounds the deletes that run after a cancelled or finished
// run; they use a context that survives the cancellation so nothing leaks.
const cleanupTimeout = 10 * time.Minute

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// tail returns at most n trailing bytes of s.
func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// environment is one sandbox and its progress. One goroutine owns it at a
// time; phases are separated by barriers, so it needs no lock.
type environment struct {
	// id is the current actor name; base is the name of the first one. A
	// recreate after a delete park gets a fresh name, since Substrate
	// deletes asynchronously and the old name is not free immediately.
	id   string
	base string
	inst Instance
	task int
	traj *Trajectory

	sb          Sandbox
	createStart time.Time
	// failed environments are skipped for the rest of a steps run.
	failed bool

	hasWSOverride bool
	wsFresh       bool
	wsWake        bool
	slotWaitMS    float64
	// inWarmPool is set by a strategy run while the environment sits
	// suspended in its warm pool; see warmPoolStats.
	inWarmPool bool
}

type runner struct {
	cfg *Config
	rec *Recorder
	log *slog.Logger

	mu       sync.Mutex
	tasks    int
	failed   int
	rewarded int
	wsStats  WorkingSetStats
}

// ValidateConfig checks cfg as Run does before it touches the backend: the
// run id, the longest environment name against Substrate's 63 characters,
// the label keys, and the mode, strategy and park settings against each
// other. It is the check behind swebench-loader run -dry-run, which has no
// backend; a nil Backend passes.
func ValidateConfig(cfg Config) error {
	if cfg.Backend == nil {
		cfg.Backend = noBackend{}
	}
	return cfg.validate()
}

// noBackend stands in for the backend during ValidateConfig; nothing calls it.
type noBackend struct{}

func (noBackend) Create(context.Context, string, string) (Sandbox, error) {
	return nil, errors.New("no backend: validate only")
}

func (noBackend) Delete(context.Context, string) error {
	return errors.New("no backend: validate only")
}

// Run executes the configured workload and returns its summary. A non-nil
// error means the run could not be carried out; task failures are counted in
// the Summary and recorded as events, not returned.
func Run(ctx context.Context, cfg Config) (*Summary, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg.defaults()
	// ParkSuspendAsync, and the park after a fork, send local suspends, which
	// need an ate-env-api that serves suspend_mode. Fail once here rather
	// than on every park. One step without -park-each parks nothing (the
	// last step is never parked), so it needs no check.
	if cfg.Mode == ModeSteps && (cfg.Park == ParkSuspendAsync || cfg.Park == ParkFork) && (cfg.Steps > 1 || cfg.ParkEach) {
		if eb, ok := cfg.Backend.(*EnvBackend); ok {
			if err := eb.CheckSuspendLocal(ctx); err != nil {
				return nil, err
			}
		}
	}
	r := &runner{cfg: &cfg, rec: cfg.Recorder, log: cfg.Logger}

	if cfg.Strategy != "" || cfg.usesWorkingSet() {
		start := time.Now()
		var err error
		if cfg.Strategy != "" {
			err = r.runStrategy(ctx)
		} else {
			err = r.runScheduledSteps(ctx)
		}
		if err != nil {
			return nil, err
		}
		dur := time.Since(start)
		s := &Summary{
			Tasks:                r.tasks,
			Failed:               r.failed,
			Rewarded:             r.rewarded,
			Duration:             dur,
			WorkingSetHits:       r.wsStats.Hits,
			WorkingSetMisses:     r.wsStats.Misses,
			WorkingSetHitRate:    r.wsStats.HitRate,
			StandingPoolSize:     r.wsStats.StandingPoolSize,
			Created:              r.wsStats.Created,
			Reused:               r.wsStats.Reused,
			Retired:              r.wsStats.Retired,
			PrefetchLeadMarginMS: r.wsStats.PrefetchLeadMarginMS,
			ReleaseOverlapMS:     r.wsStats.ReleaseOverlapMS,
		}
		r.rec.Record(Event{
			RunID:                cfg.RunID,
			Event:                EventRun,
			Name:                 string(cfg.Mode),
			DurMS:                ms(dur),
			OK:                   s.Failed == 0,
			Count:                s.Tasks,
			Labels:               cfg.labels(),
			Tags:                 cfg.buildRunLoadTags(),
			WorkingSetHits:       s.WorkingSetHits,
			WorkingSetMisses:     s.WorkingSetMisses,
			WorkingSetHitRate:    s.WorkingSetHitRate,
			StandingPoolSize:     s.StandingPoolSize,
			Created:              s.Created,
			Reused:               s.Reused,
			Retired:              s.Retired,
			PrefetchLeadMarginMS: s.PrefetchLeadMarginMS,
			ReleaseOverlapMS:     s.ReleaseOverlapMS,
		})
		return s, ctx.Err()
	}

	envs, err := r.plan()
	if err != nil {
		return nil, err
	}
	start := time.Now()
	switch cfg.Mode {
	case ModeEval:
		r.runEval(ctx, envs)
	case ModeSteps:
		if cfg.ImagesPerStep > 0 {
			r.runWindows(ctx)
		} else {
			r.runSteps(ctx, envs)
		}
	case ModeOpenLoop:
		r.runOpenLoop(ctx, envs)
	}
	dur := time.Since(start)
	s := &Summary{Tasks: r.tasks, Failed: r.failed, Rewarded: r.rewarded, Duration: dur}
	r.rec.Record(Event{RunID: cfg.RunID, Event: EventRun, Name: string(cfg.Mode), DurMS: ms(dur),
		OK: s.Failed == 0, Count: s.Tasks, Labels: cfg.labels(), Tags: cfg.buildRunLoadTags()})
	return s, ctx.Err()
}

func (c *Config) buildRunLoadTags() map[string]string {
	tags := make(map[string]string)
	if c.Dataset != "" {
		tags["dataset"] = c.Dataset
	}
	imgSet := make(map[string]bool)
	var firstImg string
	for _, in := range c.Instances {
		if in.Image != "" {
			if firstImg == "" {
				firstImg = in.Image
			}
			imgSet[in.Image] = true
		}
	}
	if firstImg != "" {
		tags["guest_image"] = firstImg
	}
	steps := c.Steps
	if c.Mode != ModeSteps || steps < 1 {
		steps = 1
	}
	groupSize := c.GroupSize
	if groupSize < 1 {
		if c.Mode == ModeSteps {
			groupSize = c.Rollouts
		} else {
			groupSize = c.TasksPerImage
		}
	}
	if groupSize < 1 {
		groupSize = 1
	}
	tasksPerStep := c.TasksPerStep
	if tasksPerStep < 1 {
		if c.Strategy == StrategySliding || c.Strategy == StrategyPipelined {
			tasksPerStep = min(len(c.Instances), c.effectiveWindowSize())
		} else {
			tasksPerStep = len(c.Instances)
		}
	}
	tags["load_tasks"] = fmt.Sprintf("%d", len(c.Instances))
	tags["load_group_size"] = fmt.Sprintf("%d", groupSize)
	tags["load_steps"] = fmt.Sprintf("%d", steps)
	tags["load_tasks_per_step"] = fmt.Sprintf("%d", tasksPerStep)
	tags["load_trajectories_per_step"] = fmt.Sprintf("%d", groupSize*tasksPerStep)
	tags["load_max_concurrent"] = fmt.Sprintf("%d", c.Width)
	tags["load_distinct_images"] = fmt.Sprintf("%d", len(imgSet))
	tags["load_repeat"] = "1"
	return tags
}

// trajectoryPicker draws trajectories from r.cfg.Trajectories (seeded by
// r.cfg.Seed when there is a mix) and caches rendered templates per
// (source, instance).
type trajectoryPicker struct {
	r        *runner
	pick     *rand.Rand
	rendered map[*Trajectory]map[string]*Trajectory
}

func (r *runner) newTrajectoryPicker() *trajectoryPicker {
	var pick *rand.Rand
	if len(r.cfg.Trajectories) > 1 {
		pick = rand.New(rand.NewSource(r.cfg.Seed))
	}
	return &trajectoryPicker{
		r:        r,
		pick:     pick,
		rendered: make(map[*Trajectory]map[string]*Trajectory),
	}
}

func (tp *trajectoryPicker) next(in Instance) (*Trajectory, error) {
	source := tp.r.cfg.Trajectory
	if tp.pick != nil {
		source = tp.r.cfg.Trajectories[tp.pick.Intn(len(tp.r.cfg.Trajectories))]
	}
	byInstance := tp.rendered[source]
	if byInstance == nil {
		byInstance = make(map[string]*Trajectory)
		tp.rendered[source] = byInstance
	}
	key := in.InstanceID + "\x00" + in.Image
	if traj := byInstance[key]; traj != nil {
		return traj, nil
	}
	traj, err := source.Render(in)
	if err != nil {
		return nil, err
	}
	byInstance[key] = traj
	return traj, nil
}

// plan renders the trajectory per instance and names every environment.
func (r *runner) plan() ([]*environment, error) {
	if r.cfg.Mode == ModeSteps && r.cfg.ImagesPerStep > 0 {
		// Windows are planned step by step; see planWindow. Rendering every
		// trajectory here still validates the templates up front.
		for _, in := range r.cfg.Instances {
			for _, t := range r.trajectories() {
				if _, err := t.Render(in); err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}
	fanout := r.cfg.TasksPerImage
	if r.cfg.Mode == ModeSteps {
		fanout = r.cfg.Rollouts
	}
	envs := make([]*environment, 0, len(r.cfg.Instances)*fanout)
	tp := r.newTrajectoryPicker()
	for _, in := range r.cfg.Instances {
		for k := 0; k < fanout; k++ {
			traj, err := tp.next(in)
			if err != nil {
				return nil, err
			}
			id := EnvID(r.cfg.RunID, in.InstanceID, k)
			envs = append(envs, &environment{
				id:   id,
				base: id,
				inst: in,
				task: k,
				traj: traj,
			})
		}
	}
	return envs, nil
}

// EnvID derives a Substrate-valid name (lowercase alphanumerics and '-', at
// most 63 characters) from the run, the instance and the replay index. A
// truncated instance id gets a hash suffix so distinct instances stay
// distinct.
func EnvID(runID, instanceID string, k int) string {
	const maxInstance = 36
	b := strings.Builder{}
	for _, c := range strings.ToLower(instanceID) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteRune(c)
		default:
			b.WriteByte('-')
		}
	}
	inst := strings.Trim(b.String(), "-")
	if len(inst) > maxInstance {
		h := fnv.New32a()
		_, _ = h.Write([]byte(instanceID))
		inst = fmt.Sprintf("%s-%08x", strings.Trim(inst[:maxInstance-9], "-"), h.Sum32())
	}
	return fmt.Sprintf("%s-%s-%d", runID, inst, k)
}

// forEach runs fn over every index with at most width in flight and waits
// for all of them. It stops launching when ctx is done.
func forEach(ctx context.Context, n, width int, fn func(i int)) {
	sem := make(chan struct{}, width)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}(i)
	}
	wg.Wait()
}

// runEval is the evaluation shape: every environment is created, replayed
// once and deleted, Width at a time.
func (r *runner) runEval(ctx context.Context, envs []*environment) {
	var done int
	forEach(ctx, len(envs), r.cfg.Width, func(i int) {
		e := envs[i]
		if r.create(ctx, e, 0) {
			r.replay(ctx, e, 0)
		}
		if !r.cfg.Keep {
			// Delete with a context that survives a cancelled run, or a
			// Ctrl-C at width 500 leaves 500 environments behind.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
			r.remove(cleanup, e, 0)
			cancel()
		}
		r.mu.Lock()
		done++
		if done%100 == 0 || done == len(envs) {
			r.log.Info("eval progress", slog.Int("done", done), slog.Int("total", len(envs)),
				slog.Int("failed", r.failed), slog.Int("rewarded", r.rewarded))
		}
		r.mu.Unlock()
	})
}

// dispatchOpenLoop dispatches fn according to a seeded Poisson arrival process
// (rate tasks/sec) bounded by width concurrent slots, up to maxTasks (when > 0)
// and/or deadline (when non-zero). Each invocation receives its 0-based
// sequence index and the time (in ms) spent waiting for a concurrency slot.
func dispatchOpenLoop(
	ctx context.Context,
	seed int64,
	rate float64,
	width int,
	maxTasks int,
	deadline time.Time,
	fn func(seq int, slotWaitMS float64),
) error {
	if seed == 0 {
		seed = 42
	}
	if width <= 0 {
		width = 1
	}
	rng := rand.New(rand.NewSource(seed))
	sem := make(chan struct{}, width)
	var wg sync.WaitGroup

	for seq := 0; maxTasks <= 0 || seq < maxTasks; seq++ {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			break
		}
		if ctx.Err() != nil {
			break
		}
		if seq > 0 && rate > 0 {
			delay := time.Duration(rng.ExpFloat64() / rate * float64(time.Second))
			if !deadline.IsZero() && time.Now().Add(delay).After(deadline) {
				break
			}
			if delay > 0 {
				t := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					t.Stop()
					wg.Wait()
					return ctx.Err()
				case <-t.C:
				}
			}
		}
		arrivalAt := time.Now()
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case sem <- struct{}{}:
		}
		slotWaitMS := ms(time.Since(arrivalAt))

		wg.Add(1)
		go func(i int, waitMS float64) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i, waitMS)
		}(seq, slotWaitMS)
	}
	wg.Wait()
	return nil
}

// runOpenLoop dispatches tasks according to a Poisson arrival process with rate
// ArrivalRate (tasks/sec), bounded by Width concurrent slots, and records the
// time each arriving task waited for a concurrency slot in Event.SlotWaitMS.
func (r *runner) runOpenLoop(ctx context.Context, envs []*environment) {
	var deadline time.Time
	if r.cfg.Duration > 0 {
		deadline = time.Now().Add(r.cfg.Duration)
	}
	_ = dispatchOpenLoop(ctx, r.cfg.Seed, r.cfg.ArrivalRate, r.cfg.Width, len(envs), deadline, func(i int, slotWaitMS float64) {
		env := envs[i]
		env.slotWaitMS = slotWaitMS
		if r.create(ctx, env, 0) {
			r.replay(ctx, env, 0)
		}
		if !r.cfg.Keep {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
			r.remove(cleanup, env, 0)
			cancel()
		}
	})
}

// runSteps is the training shape: the fleet is created once, every step
// replays the trajectory on all of it behind a barrier, and the fleet is
// parked between steps.
// trajectories is the run's trajectory list: the mix when there is one,
// else the single Trajectory.
func (r *runner) trajectories() []*Trajectory {
	if len(r.cfg.Trajectories) > 1 {
		return r.cfg.Trajectories
	}
	return []*Trajectory{r.cfg.Trajectory}
}

// planWindow names the environments of one step in window mode: the next
// ImagesPerStep instances after the previous step's, wrapping around the
// dataset, with Rollouts environments each. Names carry the step from step
// 2 on, as a delete park's do, so an instance that comes around again gets
// a fresh name while the previous one may still be deleting.
func (r *runner) planWindow(step int, tp *trajectoryPicker) ([]*environment, error) {
	n := len(r.cfg.Instances)
	envs := make([]*environment, 0, r.cfg.ImagesPerStep*r.cfg.Rollouts)
	for i := 0; i < r.cfg.ImagesPerStep; i++ {
		in := r.cfg.Instances[((step-1)*r.cfg.ImagesPerStep+i)%n]
		for k := 0; k < r.cfg.Rollouts; k++ {
			traj, err := tp.next(in)
			if err != nil {
				return nil, err
			}
			id := EnvID(r.cfg.RunID, in.InstanceID, k)
			envs = append(envs, &environment{id: id, base: id, inst: in, task: k, traj: traj})
		}
	}
	return envs, nil
}

// runWindows drives steps mode with a new window of images each step: create
// the step's environments, replay the trajectory on all of them behind a
// barrier, then retire them, recorded as a delete park. Nothing is kept
// across steps, so the whole cost of a step is the cold path plus the work.
func (r *runner) runWindows(ctx context.Context) {
	tp := r.newTrajectoryPicker()
	for step := 1; step <= r.cfg.Steps; step++ {
		if ctx.Err() != nil {
			break
		}
		envs, err := r.planWindow(step, tp)
		if err != nil {
			r.log.Error("plan window", slog.Int("step", step), slog.String("err", err.Error()))
			return
		}
		forEach(ctx, len(envs), r.cfg.Width, func(i int) { r.create(ctx, envs[i], step) })
		barrier := time.Now()
		forEach(ctx, len(envs), r.cfg.Width, func(i int) {
			if !envs[i].failed {
				r.replay(ctx, envs[i], step)
			}
		})
		live := len(envs) - r.countFailed(envs)
		r.rec.Record(Event{RunID: r.cfg.RunID, Event: EventBarrier, Step: step, DurMS: ms(time.Since(barrier)),
			OK: ctx.Err() == nil, Count: live})
		r.log.Info("step done", slog.Int("step", step), slog.Int("images", r.cfg.ImagesPerStep),
			slog.Duration("barrier", time.Since(barrier)), slog.Int("live", live), slog.Int("failed", r.failed), slog.Int("rewarded", r.rewarded))

		// Retire the window. The last step's retirement is the run's cleanup
		// and runs on a context that survives a cancelled run.
		rctx, stop := ctx, func() {}
		if step == r.cfg.Steps || ctx.Err() != nil {
			rctx, stop = context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		}
		parkStart := time.Now()
		forEach(rctx, len(envs), r.cfg.Width, func(i int) {
			if envs[i].sb != nil {
				r.remove(rctx, envs[i], step)
			}
		})
		stop() // the cleanup context ends with its step, not with the run
		if step < r.cfg.Steps && ctx.Err() == nil {
			r.rec.Record(Event{RunID: r.cfg.RunID, Event: EventPark, Name: string(ParkDelete), Step: step,
				DurMS: ms(time.Since(parkStart)), OK: true, Count: live})
		}
	}
}

func (r *runner) runSteps(ctx context.Context, envs []*environment) {
	if !r.cfg.ParkEach {
		forEach(ctx, len(envs), r.cfg.Width, func(i int) { r.create(ctx, envs[i], 0) })
		r.log.Info("fleet created", slog.Int("environments", len(envs)), slog.Int("failed", r.countFailed(envs)))
	}

	for step := 1; step <= r.cfg.Steps; step++ {
		if ctx.Err() != nil {
			break
		}
		if r.cfg.ParkEach {
			r.stepParkEach(ctx, envs, step)
			continue
		}
		if step > 1 && r.cfg.Park == ParkDelete {
			forEach(ctx, len(envs), r.cfg.Width, func(i int) {
				if !envs[i].failed {
					r.create(ctx, envs[i], step)
				}
			})
		} else if step > 1 && r.cfg.Park == ParkFork {
			forEach(ctx, len(envs), r.cfg.Width, func(i int) {
				if !envs[i].failed {
					r.fork(ctx, envs[i], step)
				}
			})
		}
		barrier := time.Now()
		forEach(ctx, len(envs), r.cfg.Width, func(i int) {
			if !envs[i].failed {
				r.replay(ctx, envs[i], step)
			}
		})
		live := len(envs) - r.countFailed(envs)
		r.rec.Record(Event{RunID: r.cfg.RunID, Event: EventBarrier, Step: step, DurMS: ms(time.Since(barrier)),
			OK: ctx.Err() == nil, Count: live})
		r.log.Info("step done", slog.Int("step", step), slog.Duration("barrier", time.Since(barrier)),
			slog.Int("live", live), slog.Int("failed", r.failed), slog.Int("rewarded", r.rewarded))

		if step == r.cfg.Steps || r.cfg.Park == ParkNone || ctx.Err() != nil {
			continue
		}
		parkStart := time.Now()
		forEach(ctx, len(envs), r.cfg.Width, func(i int) {
			e := envs[i]
			if e.failed {
				return
			}
			switch r.cfg.Park {
			case ParkSuspend, ParkSuspendSync:
				r.suspendWithMode(ctx, e, step, false)
			case ParkSuspendAsync, ParkFork:
				r.suspendWithMode(ctx, e, step, true)
			case ParkDelete:
				r.remove(ctx, e, step)
			}
		})
		r.rec.Record(Event{RunID: r.cfg.RunID, Event: EventPark, Name: string(r.cfg.Park), Step: step,
			DurMS: ms(time.Since(parkStart)), OK: ctx.Err() == nil, Count: live})
	}

	if r.cfg.Keep {
		return
	}
	// Tear down with a context that survives a cancelled run so nothing
	// leaks on Ctrl-C.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	forEach(cleanup, len(envs), r.cfg.Width, func(i int) {
		if envs[i].sb != nil {
			r.remove(cleanup, envs[i], r.cfg.Steps)
		}
	})
}

// stepParkEach runs one step with each environment created on its first
// step (or every step under a delete park), replayed, and parked as soon as
// its replay ends, Width at a time. The barrier covers the parking, so no
// park event is recorded; the suspend and delete events carry the cost.
// The last step deletes each environment right after its replay instead of
// parking it: a suspend there would upload one more snapshot per
// environment for the run's cleanup to delete moments later, in one burst
// the bucket rate-limits; deleting here spreads the deletes over the step,
// and the cleanup finds nothing left. Keep leaves the last step's fleet in
// place on both park modes, as the barrier path does: suspended under a
// suspend park, awake under a delete park. An environment whose step failed
// leaves the run and is deleted at once, so it does not hold a slot until
// the cleanup.
func (r *runner) stepParkEach(ctx context.Context, envs []*environment, step int) {
	barrier := time.Now()
	forEach(ctx, len(envs), r.cfg.Width, func(i int) {
		e := envs[i]
		if e.failed {
			return
		}
		if step == 1 {
			if !r.create(ctx, e, 0) {
				return
			}
		} else if r.cfg.Park == ParkDelete {
			if !r.create(ctx, e, step) {
				return
			}
		}
		r.replay(ctx, e, step)
		if ctx.Err() != nil {
			return
		}
		if e.failed {
			r.remove(ctx, e, step)
			return
		}
		last := step == r.cfg.Steps
		switch {
		case last && r.cfg.Keep && r.cfg.Park == ParkDelete:
			// Keep leaves the fleet awake, as the barrier path does.
		case r.cfg.Park == ParkDelete, last && !r.cfg.Keep:
			r.remove(ctx, e, step)
		case r.cfg.Park == ParkSuspend, r.cfg.Park == ParkSuspendSync:
			r.suspendWithMode(ctx, e, step, false)
		case r.cfg.Park == ParkSuspendAsync:
			r.suspendWithMode(ctx, e, step, true)
		}
	})
	live := len(envs) - r.countFailed(envs)
	r.rec.Record(Event{RunID: r.cfg.RunID, Event: EventBarrier, Step: step, DurMS: ms(time.Since(barrier)),
		OK: ctx.Err() == nil, Count: live})
	r.log.Info("step done", slog.Int("step", step), slog.Duration("barrier", time.Since(barrier)),
		slog.Int("live", live), slog.Int("failed", r.failed), slog.Int("rewarded", r.rewarded))
}

func (r *runner) runScheduledSteps(ctx context.Context) error {
	sched := r.cfg.Schedule
	if sched == nil {
		tasksPerStep := r.cfg.TasksPerStep
		if tasksPerStep <= 0 {
			tasksPerStep = len(r.cfg.Instances)
		}
		groupSize := r.cfg.GroupSize
		if groupSize <= 0 {
			groupSize = r.cfg.Rollouts
		}
		var err error
		sched, err = ResolveSingleJobSchedule(
			r.cfg.RunID,
			uint64(r.cfg.Seed),
			schema.ParkPolicy(r.cfg.Park),
			r.cfg.Steps,
			tasksPerStep,
			groupSize,
			r.cfg.Instances,
		)
		if err != nil {
			return err
		}
	}
	if err := sched.Validate(); err != nil {
		return err
	}
	job := sched.Jobs[0]
	parkPolicy := job.ParkPolicy
	if r.cfg.Park != "" {
		if norm, err := schema.NormalizeParkPolicy(schema.ParkPolicy(r.cfg.Park)); err == nil {
			parkPolicy = norm
		}
	}

	for _, in := range r.cfg.Instances {
		for _, t := range r.trajectories() {
			if _, err := t.Render(in); err != nil {
				return err
			}
		}
	}
	instByID := InstancesByID(r.cfg.Instances)
	tp := r.newTrajectoryPicker()

	ws, err := NewWorkingSet(WorkingSetConfig{
		JobID:      r.cfg.RunID,
		Backend:    r.cfg.Backend,
		ParkPolicy: parkPolicy,
	})
	if err != nil {
		return err
	}

	var (
		totalPrefetchLeadMS   float64
		totalReleaseOverlapMS float64
		lastActive            []*WorkingSetEntry
	)

	for sIdx, st := range job.Steps {
		if ctx.Err() != nil {
			break
		}
		stepNum := st.Step
		stepStart := time.Now()
		leadMS, _ := ws.WaitPrewarm(ctx, stepNum, stepStart)
		totalPrefetchLeadMS += leadMS

		ws.BeginStep()
		if r.cfg.Prefetch && len(st.PrefetchImages) > 0 && sIdx+1 < len(job.Steps) {
			ws.Prewarm(ctx, stepNum+1, st.PrefetchImages)
		}

		stepInsts := make([]Instance, len(st.Tasks))
		stepTrajs := make([]*Trajectory, len(st.Tasks))
		stepRenderErrs := make([]error, len(st.Tasks))
		for i, tk := range st.Tasks {
			inst, ok := instByID[tk.InstanceID]
			if !ok {
				inst = Instance{InstanceID: tk.InstanceID, Image: tk.Image}
			}
			stepInsts[i] = inst
			stepTrajs[i], stepRenderErrs[i] = tp.next(inst)
		}

		active := make([]*WorkingSetEntry, len(st.Tasks))
		var liveCount int
		var liveMu sync.Mutex

		barrierStart := time.Now()
		forEach(ctx, len(st.Tasks), r.cfg.Width, func(i int) {
			tk := st.Tasks[i]
			inst := stepInsts[i]
			traj := stepTrajs[i]
			if rerr := stepRenderErrs[i]; rerr != nil {
				e := &environment{id: EnvID(r.cfg.RunID, tk.InstanceID, i), inst: inst, task: i}
				r.rec.Record(r.event(EventTask, e, stepNum, 0, rerr))
				r.countTask(e, stepNum, false, nil)
				return
			}

			cctx, cancel := context.WithTimeout(ctx, r.cfg.CreateTimeout)
			acqStart := time.Now()
			entry, aerr := ws.Acquire(cctx, stepNum, i, tk.InstanceID, tk.Image)
			cancel()
			if aerr != nil {
				e := &environment{id: EnvID(r.cfg.RunID, tk.InstanceID, i), inst: inst, task: i}
				r.rec.Record(r.event(EventCreate, e, stepNum, time.Since(acqStart), aerr))
				r.rec.Record(r.event(EventTask, e, stepNum, 0, aerr))
				r.countTask(e, stepNum, false, nil)
				return
			}

			e := &environment{
				id:            entry.SandboxID,
				base:          entry.BaseID,
				inst:          inst,
				task:          i,
				traj:          traj,
				sb:            entry.Sandbox,
				createStart:   entry.CreateStart,
				hasWSOverride: true,
				wsFresh:       !entry.Hit || entry.IsForkChild,
				wsWake:        entry.Hit && entry.WasParked,
			}
			if !entry.Hit {
				r.rec.Record(r.event(EventCreate, e, stepNum, time.Since(entry.CreateStart), nil))
			}

			r.replay(ctx, e, stepNum)
			if !e.failed {
				liveMu.Lock()
				liveCount++
				liveMu.Unlock()
			}
			if e.failed {
				cleanup, ccancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
				_ = entry.Sandbox.Delete(cleanup)
				ccancel()
				return
			}

			active[i] = entry
		})

		barrierDur := time.Since(barrierStart)
		barrierDoneAt := time.Now()
		var stepOverlapMS float64

		if sIdx+1 < len(job.Steps) && ctx.Err() == nil {
			if parkPolicy == schema.ParkSuspendAsync {
				for _, entry := range active {
					if entry != nil {
						_ = ws.Release(ctx, entry)
					}
				}
			} else if parkPolicy != schema.ParkNone {
				parkStart := time.Now()
				forEach(ctx, len(active), r.cfg.Width, func(i int) {
					entry := active[i]
					if entry == nil {
						return
					}
					e := &environment{id: entry.SandboxID, base: entry.BaseID, inst: Instance{InstanceID: entry.InstanceID, Image: entry.Image}, task: i}
					opStart := time.Now()
					err := ws.Release(ctx, entry)
					switch parkPolicy {
					case schema.ParkSuspendSync:
						r.rec.Record(r.event(EventSuspend, e, stepNum, time.Since(opStart), err))
					case schema.ParkDelete, schema.ParkFork:
						r.rec.Record(r.event(EventDelete, e, stepNum, time.Since(opStart), err))
					}
				})
				stStats := ws.StepStats(leadMS, 0)
				r.rec.Record(Event{
					RunID:            r.cfg.RunID,
					Event:            EventPark,
					Name:             string(parkPolicy),
					Step:             stepNum,
					DurMS:            ms(time.Since(parkStart)),
					OK:               ctx.Err() == nil,
					Count:            liveCount,
					StandingPoolSize: stStats.StandingPoolSize,
				})
			} else if parkPolicy == schema.ParkNone {
				for _, entry := range active {
					if entry != nil {
						_ = ws.Release(ctx, entry)
					}
				}
			}

			stepPause := r.cfg.StepPause
			if stepPause <= 0 && st.StepPauseMS > 0 {
				stepPause = time.Duration(st.StepPauseMS * float64(time.Millisecond))
			}
			if stepPause > 0 {
				select {
				case <-ctx.Done():
				case <-time.After(stepPause):
				}
			}

			if parkPolicy == schema.ParkSuspendAsync {
				parkStart := time.Now()
				overlap, _ := ws.AwaitAsyncSuspends(ctx, barrierDoneAt, time.Now())
				for i, entry := range active {
					if entry == nil {
						continue
					}
					e := &environment{id: entry.SandboxID, base: entry.BaseID, inst: Instance{InstanceID: entry.InstanceID, Image: entry.Image}, task: i}
					r.rec.Record(r.event(EventSuspend, e, stepNum, entry.asyncDur, entry.asyncErr))
				}
				stepOverlapMS = overlap
				totalReleaseOverlapMS += overlap
				stStats := ws.StepStats(leadMS, stepOverlapMS)
				r.rec.Record(Event{
					RunID:            r.cfg.RunID,
					Event:            EventPark,
					Name:             string(parkPolicy),
					Step:             stepNum,
					DurMS:            ms(time.Since(parkStart)),
					OK:               ctx.Err() == nil,
					Count:            liveCount,
					StandingPoolSize: stStats.StandingPoolSize,
					ReleaseOverlapMS: stepOverlapMS,
				})
			}
		} else {
			lastActive = active
		}

		stStats := ws.StepStats(leadMS, stepOverlapMS)
		r.rec.Record(Event{
			RunID:                r.cfg.RunID,
			Event:                EventBarrier,
			Step:                 stepNum,
			DurMS:                ms(barrierDur),
			OK:                   ctx.Err() == nil,
			Count:                liveCount,
			WorkingSetHits:       stStats.Hits,
			WorkingSetMisses:     stStats.Misses,
			WorkingSetHitRate:    stStats.HitRate,
			StandingPoolSize:     stStats.StandingPoolSize,
			Created:              stStats.Created,
			Reused:               stStats.Reused,
			Retired:              stStats.Retired,
			PrefetchLeadMarginMS: stStats.PrefetchLeadMarginMS,
			ReleaseOverlapMS:     stStats.ReleaseOverlapMS,
		})
	}

	r.wsStats = ws.SummaryStats(totalPrefetchLeadMS, totalReleaseOverlapMS)

	if r.cfg.Keep {
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	forEach(cleanup, len(lastActive), r.cfg.Width, func(i int) {
		entry := lastActive[i]
		if entry == nil || entry.Sandbox == nil {
			return
		}
		e := &environment{id: entry.SandboxID, base: entry.BaseID, inst: Instance{InstanceID: entry.InstanceID, Image: entry.Image}, task: i, sb: entry.Sandbox}
		r.remove(cleanup, e, len(job.Steps))
	})
	_ = ws.Close(cleanup)
	return nil
}

func (r *runner) countFailed(envs []*environment) int {
	n := 0
	for _, e := range envs {
		if e.failed {
			n++
		}
	}
	return n
}

// create makes the environment and records the call. It returns false and
// marks the environment failed when the create did not succeed.
func (r *runner) create(ctx context.Context, e *environment, step int) bool {
	if step > 1 {
		e.id = fmt.Sprintf("%s-s%d", e.base, step)
	}
	cctx, cancel := context.WithTimeout(ctx, r.cfg.CreateTimeout)
	defer cancel()
	e.createStart = time.Now()
	sb, err := r.cfg.Backend.Create(cctx, e.id, e.inst.Image)
	ev := r.event(EventCreate, e, step, time.Since(e.createStart), err)
	r.rec.Record(ev)
	if err != nil {
		e.failed = true
		// A task that never got an environment is still a failed task, in
		// the report as well as in the summary.
		task := r.event(EventTask, e, step, 0, err)
		r.rec.Record(task)
		r.countTask(e, step, false, nil)
		r.log.Warn("create failed", slog.String("env", e.id), slog.String("err", err.Error()))
		// The server may still finish a create whose call failed or timed
		// out on our side; delete by name so it cannot leak or collide.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		defer cancel()
		if derr := r.cfg.Backend.Delete(cleanup, e.id); derr != nil && !isNotFound(derr) {
			r.log.Warn("delete after failed create", slog.String("env", e.id), slog.String("err", derr.Error()))
		}
		return false
	}
	e.sb = sb
	return true
}

// replay runs the trajectory once on e and records one task event. A fresh
// environment (its first step, or any step after a delete park) runs the
// Setup steps first; one that survived a park runs the Reset steps first.
func (r *runner) replay(ctx context.Context, e *environment, step int) {
	fresh := step <= 1 || r.cfg.Park == ParkDelete
	if e.hasWSOverride {
		fresh = e.wsFresh
	}
	prefix := e.traj.Reset
	if fresh {
		prefix = e.traj.Setup
	}
	steps := append(append([]Step{}, prefix...), e.traj.Steps...)
	// The first instruction of a task either follows a create (eval, or a
	// delete park) or a park (suspend); its wall time is the TTFI or the
	// wake, and it is the one that may find the environment not yet
	// serving.
	first := true
	firstEvent := EventTTFI
	firstStart := e.createStart
	if e.hasWSOverride {
		if e.wsWake {
			firstEvent = EventWake
			firstStart = time.Time{}
		}
	} else if step > 1 && r.cfg.Park != ParkDelete {
		firstEvent = EventWake
		firstStart = time.Time{}
	}

	taskStart := time.Now()
	ok := true
	var reward *int
	var totalThinkMS float64
	for _, s := range steps {
		// No think time before the first command: it would land in the
		// ttfi or wake measurement, and the "model" has nothing to think
		// about before it has seen the environment.
		if s.Think > 0 && !first {
			thinkStart := time.Now()
			select {
			case <-ctx.Done():
				ok = false
			case <-time.After(time.Duration(s.Think)):
			}
			totalThinkMS += ms(time.Since(thinkStart))
			if !ok {
				break
			}
		}
		stepStart := time.Now()
		if firstStart.IsZero() {
			firstStart = stepStart
		}
		res, attempts, err := r.doStep(ctx, e, s, first)
		dur := time.Since(stepStart)
		if first {
			fe := r.event(firstEvent, e, step, time.Since(firstStart), err)
			fe.Attempts = attempts
			fe.SlotWaitMS = e.slotWaitMS
			if firstEvent == EventTTFI {
				wireMS := math.Max(0, ms(stepStart.Sub(firstStart)))
				firstCmdMS := ms(dur)
				fe.Metrics = map[string]float64{
					"sdk_slot_wait_ms":       round1(e.slotWaitMS),
					"wire_claim_to_ready_ms": round1(wireMS),
					"ready_to_first_cmd_ms":  round1(firstCmdMS),
				}
			}
			r.rec.Record(fe)
			first = false
		}
		ev := r.event(EventStep, e, step, dur, err)
		ev.Name = s.Name
		var (
			timedOut    bool
			stepTimeout time.Duration
		)
		if s.Run != "" && err == nil {
			ev.Exit = intp(res.ExitCode)
			stepTimeout = time.Duration(s.Timeout)
			if stepTimeout <= 0 {
				stepTimeout = r.cfg.StepTimeout
			}
			if res.TimedOut != nil {
				// The SDK drivers: env.ShellResponse.TimedOut, the guest's
				// Process.timed_out (the SDK's own exit-code heuristic for
				// a guest that predates it), for the same timeout.
				timedOut = *res.TimedOut
			} else {
				// agent-sandbox reports no timeout flag, so the loader
				// infers one from exit 137 at or past the deadline, as the
				// SDKs do for an old guest. An OOM kill is 137 too, but it
				// rarely lands exactly at the deadline.
				timedOut = stepTimeout > 0 && res.ExitCode == 137 && dur >= stepTimeout
			}
			if timedOut {
				ev.Timeout = &timedOut
			}
		}
		switch {
		case err != nil:
			ok = false
		case timedOut:
			// A step the guest killed at its timeout fails the task, reward
			// step or not, as run_fleet_bench.py does on its TimeoutError:
			// the trajectory did not run to its end, so there is no verdict.
			// The report classifies it as a tier 2 command_timeout.
			ok = false
			ev.OK = false
			ev.Err = fmt.Sprintf("step %q timed out after %s", s.Name, stepTimeout)
			ev.Tail = tail(res.Stdout+res.Stderr, 2000)
		case s.Reward:
			reward = intp(0)
			if res.ExitCode == 0 {
				*reward = 1
			}
			out := res.Stdout + res.Stderr
			if a := AdapterFor(e.inst.Kind); a != nil {
				if ig, ok := a.(InstanceGrader); ok {
					if parsedReward, detail, gerr := ig.GradeInstance(e.inst, res); gerr == nil {
						if parsedReward != nil {
							reward = parsedReward
						}
						if detail != "" {
							ev.Detail = detail
						}
					}
				} else if a.Grader() != nil {
					if parsedReward, detail, gerr := a.Grader().Parse([]byte(out)); gerr == nil {
						if parsedReward != nil {
							reward = parsedReward
						}
						if detail != "" {
							ev.Detail = detail
						}
					}
				}
			}
			ev.Reward = reward
			// The grader's verdict is the last line of a reward step's
			// output; keep enough of it for the report to read it back.
			ev.Tail = tail(out, 4000)
		case s.ExpectExit != nil && res.ExitCode != *s.ExpectExit:
			ok = false
			ev.OK = false
			ev.Err = fmt.Sprintf("exit %d, expected %d", res.ExitCode, *s.ExpectExit)
			ev.Tail = tail(res.Stdout+res.Stderr, 2000)
		}
		r.rec.Record(ev)
		if !ok {
			r.log.Warn("step failed", slog.String("env", e.id), slog.String("step", s.Name), slog.String("err", ev.Err))
			break
		}
	}
	if ok && reward == nil && e.traj.HasReward() {
		ok = false
	}
	ev := r.event(EventTask, e, step, time.Since(taskStart), nil)
	ev.SlotWaitMS = e.slotWaitMS
	ev.OK = ok
	ev.Reward = reward
	if totalThinkMS > 0 {
		taskDurMS := ms(time.Since(taskStart))
		ev.Metrics = map[string]float64{
			"llm_think_ms": round1(totalThinkMS),
			"env_wait_ms":  round1(math.Max(0, taskDurMS-totalThinkMS)),
		}
	}
	r.rec.Record(ev)
	r.countTask(e, step, ok, reward)
	if !ok && r.cfg.Mode == ModeSteps {
		e.failed = true
	}
}

// doStep performs one step and reports how many attempts it took. The
// first step of a task retries transient errors within WakeBudget: the
// environment may still be waking, or its image may still be baking.
func (r *runner) doStep(ctx context.Context, e *environment, s Step, first bool) (Result, int, error) {
	deadline := time.Now().Add(r.cfg.WakeBudget)
	wait := r.cfg.WakeBackoff
	var (
		res Result
		err error
	)
	for attempt := 1; ; attempt++ {
		if s.Write != nil {
			mode, _ := s.Write.FileMode()
			err = e.sb.WriteFile(ctx, s.Write.Path, []byte(s.Write.Content), mode)
		} else {
			timeout := time.Duration(s.Timeout)
			if timeout <= 0 {
				timeout = r.cfg.StepTimeout
			}
			sctx, cancel := context.WithTimeout(ctx, timeout+time.Minute)
			res, err = e.sb.Run(sctx, s.Run, timeout)
			cancel()
		}
		if err == nil || !first || !isTransient(err) || ctx.Err() != nil || time.Now().Add(wait).After(deadline) {
			return res, attempt, err
		}
		select {
		case <-ctx.Done():
			return res, attempt, err
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*r.cfg.WakeBackoff)
	}
}

// isNotFound reports whether a delete found nothing to delete.
func isNotFound(err error) bool {
	if errors.Is(err, env.ErrNotFound) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.NotFound
}

// isTransient reports whether a first-command error is worth another try.
// Unavailable is the router giving up on a wake; DeadlineExceeded, Unknown,
// Internal and Aborted are what a resume that races the golden bake or a
// worker restart surfaces as. Argument and precondition errors are not
// retried.
func isTransient(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Unknown, codes.Internal, codes.Aborted:
		return true
	}
	return false
}

func (r *runner) suspendWithMode(ctx context.Context, e *environment, step int, async bool) {
	start := time.Now()
	var err error
	if ms, ok := e.sb.(ModeSuspender); ok {
		err = ms.SuspendWithMode(ctx, async)
	} else if async {
		if as, ok := e.sb.(AsyncSuspender); ok {
			err = as.SuspendAsync(ctx)
		} else {
			err = e.sb.Suspend(ctx)
		}
	} else {
		err = e.sb.Suspend(ctx)
	}
	r.rec.Record(r.event(EventSuspend, e, step, time.Since(start), err))
	if err != nil {
		r.log.Warn("suspend failed", slog.String("env", e.id), slog.String("err", err.Error()))
	}
}

func (r *runner) fork(ctx context.Context, e *environment, step int) bool {
	parentID := e.id
	parentSB := e.sb
	newID := fmt.Sprintf("%s-s%d", e.base, step)
	cctx, cancel := context.WithTimeout(ctx, r.cfg.CreateTimeout)
	defer cancel()
	e.createStart = time.Now()

	var (
		child Sandbox
		err   error
	)
	if sf, ok := parentSB.(SandboxForker); ok {
		child, err = sf.Fork(cctx, newID)
	} else if fb, ok := r.cfg.Backend.(ForkBackend); ok {
		child, err = fb.Fork(cctx, parentID, newID)
	} else {
		child, err = r.cfg.Backend.Create(cctx, newID, e.inst.Image)
	}
	ev := r.event(EventFork, e, step, time.Since(e.createStart), err)
	ev.Env = newID
	r.rec.Record(ev)
	if err != nil {
		e.failed = true
		task := r.event(EventTask, e, step, 0, err)
		task.Env = newID
		r.rec.Record(task)
		r.countTask(e, step, false, nil)
		r.log.Warn("fork failed", slog.String("from", parentID), slog.String("env", newID), slog.String("err", err.Error()))
		return false
	}
	e.id = newID
	if parentSB != nil && !r.cfg.Keep {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
		_ = parentSB.Delete(cleanup)
		cancel()
	}
	e.sb = child
	return true
}

func (r *runner) remove(ctx context.Context, e *environment, step int) {
	if e.sb == nil {
		return
	}
	start := time.Now()
	err := e.sb.Delete(ctx)
	r.rec.Record(r.event(EventDelete, e, step, time.Since(start), err))
	if err != nil {
		// Keep e.sb for the final cleanup, and take the environment out of
		// later steps: a delete park would otherwise create the next name and
		// overwrite the only handle to this one, which then leaks.
		e.failed = true
		r.log.Warn("delete failed; the environment leaves the run and is deleted at the end",
			slog.String("env", e.id), slog.String("err", err.Error()))
		return
	}
	e.sb = nil
}

func (r *runner) event(kind string, e *environment, step int, d time.Duration, err error) Event {
	ev := Event{RunID: r.cfg.RunID, Event: kind, Env: e.id, Instance: e.inst.InstanceID,
		Step: step, Task: e.task, DurMS: ms(d), OK: err == nil}
	if kind == EventTask {
		// Which trajectory the task replayed; one run can mix several.
		ev.Name = e.traj.Name
	}
	if err != nil {
		ev.Err = err.Error()
	}
	return ev
}

func (r *runner) countTask(e *environment, step int, ok bool, reward *int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks++
	if !ok {
		r.failed++
	}
	if reward != nil && *reward == 1 {
		r.rewarded++
	}
}

// EnvHandle describes an acquired sandbox environment in scenario runners.
type EnvHandle struct {
	Name   string
	Source string
}

// ExecResult aliases Result for scenario runners.
type ExecResult = Result

// EnvClient is the lifecycle interface used by multi-job scenario runners.
type EnvClient interface {
	Create(ctx context.Context, image, instanceID string) (EnvHandle, error)
	Exec(ctx context.Context, envName, cmd string, timeout time.Duration) (ExecResult, error)
	Delete(ctx context.Context, envName string) error
	Snapshot(ctx context.Context, envName string) (string, error)
	Resume(ctx context.Context, snapshot, image, instanceID string) (EnvHandle, error)
}

// Runner drives an open-loop or closed-loop workload against an EnvClient and
// records events to Recorder.
type Runner struct {
	Config     Config
	Client     EnvClient
	Recorder   *Recorder
	Scheduled  schema.ScheduledJob
	ParkPolicy schema.ParkPolicy
	SleepFunc  func(ctx context.Context, d time.Duration) error
}

// Run executes the configured open-loop or closed-loop workload against r.Client.
func (r *Runner) Run(ctx context.Context) error {
	if r.Client == nil {
		return errors.New("Runner.Client is required")
	}
	rec := r.Recorder
	if rec == nil {
		rec = r.Config.Recorder
	}
	if rec == nil {
		rec = NewRecorder(nil)
	}
	if len(r.Scheduled.Steps) > 0 && r.Config.Mode != ModeOpenLoop {
		return r.runClosedLoop(ctx, rec)
	}
	return r.runOpenLoop(ctx, rec)
}

func (r *Runner) runOpenLoop(ctx context.Context, rec *Recorder) error {
	conc := r.Config.Concurrency
	if conc <= 0 {
		conc = r.Config.Width
	}
	if conc <= 0 {
		conc = 4
	}
	rate := r.Config.ArrivalRate
	if rate <= 0 {
		rate = 10.0
	}
	dur := r.Config.Duration
	if dur <= 0 {
		dur = 100 * time.Millisecond
	}
	instances := r.Config.Instances
	if len(instances) == 0 {
		instances = SyntheticInstances(4, "")
	}

	deadline := time.Now().Add(dur)
	return dispatchOpenLoop(ctx, r.Config.Seed, rate, conc, 0, deadline, func(tIdx int, slotWait float64) {
		in := instances[tIdx%len(instances)]
		tStart := time.Now()
		h, err := r.Client.Create(ctx, in.Image, in.InstanceID)
		cDur := DurMS(time.Since(tStart))
		src := h.Source
		if src == "" {
			src = string(schema.SourceWarmPool)
		}
		rec.Emit(Event{
			RunID:    r.Config.RunID,
			Job:      r.Config.JobName,
			Event:    EventPhase,
			Name:     "create",
			Phase:    "create",
			Source:   src,
			Env:      h.Name,
			Instance: in.InstanceID,
			Image:    in.Image,
			Task:     tIdx,
			DurMS:    cDur,
			OK:       err == nil,
			Err:      errString(err),
		})
		if err != nil {
			// One task event per attempted rollout, also when the create
			// fails: the fault classification counts all_rollouts from task
			// events, so a rollout that never got an environment must still
			// be in the denominator and in the dropped count.
			rec.Emit(Event{
				RunID:      r.Config.RunID,
				Job:        r.Config.JobName,
				Event:      EventTask,
				Instance:   in.InstanceID,
				Image:      in.Image,
				Task:       tIdx,
				DurMS:      DurMS(time.Since(tStart)),
				SlotWaitMS: slotWait,
				OK:         false,
				Err:        err.Error(),
			})
			return
		}
		_, execErr := r.Client.Exec(ctx, h.Name, "pytest -q", 30*time.Second)
		_ = r.Client.Delete(context.Background(), h.Name)
		reward := 1
		if execErr != nil {
			reward = 0
		}
		rec.Emit(Event{
			RunID:      r.Config.RunID,
			Job:        r.Config.JobName,
			Event:      EventTask,
			Source:     src,
			Env:        h.Name,
			Instance:   in.InstanceID,
			Image:      in.Image,
			Task:       tIdx,
			DurMS:      DurMS(time.Since(tStart)),
			SlotWaitMS: slotWait,
			OK:         execErr == nil,
			Reward:     &reward,
		})
	})
}

func (r *Runner) runClosedLoop(ctx context.Context, rec *Recorder) error {
	sleepFn := r.SleepFunc
	if sleepFn == nil {
		sleepFn = func(ctx context.Context, d time.Duration) error {
			if d <= 0 {
				return nil
			}
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		}
	}
	concurrency := r.Config.Concurrency
	if concurrency <= 0 {
		concurrency = r.Config.Width
	}
	if concurrency <= 0 {
		concurrency = 4
	}
	parkPolicy := r.Scheduled.ParkPolicy
	if parkPolicy == "" {
		parkPolicy = r.ParkPolicy
	}
	if parkPolicy == "" {
		parkPolicy = schema.ParkPolicyHold
	}
	jobName := r.Config.JobName
	if jobName == "" {
		jobName = r.Scheduled.Job
	}

	for stepIdx, step := range r.Scheduled.Steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		stepStart := time.Now()

		for _, img := range step.PrefetchImages {
			rec.Emit(Event{
				RunID:  r.Config.RunID,
				Job:    jobName,
				Event:  EventPhase,
				Name:   "prefetch_hint",
				Phase:  "prefetch_hint",
				Image:  img,
				Step:   step.Step,
				DurMS:  0,
				OK:     true,
				Detail: "lookahead image hint",
			})
		}

		sem := make(chan struct{}, concurrency)
		var wg sync.WaitGroup
		var firstErr error
		var errMu sync.Mutex

		for taskIdx, task := range step.Tasks {
			enqueueAt := time.Now()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sem <- struct{}{}:
			}
			slotWaitMS := DurMS(time.Since(enqueueAt))

			wg.Add(1)
			go func(tIdx int, st schema.ScheduledTask, waitMS float64) {
				defer wg.Done()
				defer func() { <-sem }()
				workerName := fmt.Sprintf("%s-w%d", jobName, tIdx%concurrency)
				if err := r.runScheduledTask(ctx, rec, jobName, workerName, step.Step, tIdx, st, waitMS, parkPolicy); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}(taskIdx, task, slotWaitMS)
		}
		wg.Wait()
		if firstErr != nil {
			return firstErr
		}

		if step.StepPauseMS > 0 && stepIdx < len(r.Scheduled.Steps)-1 {
			pauseStart := time.Now()
			if err := sleepFn(ctx, time.Duration(step.StepPauseMS*float64(time.Millisecond))); err != nil {
				return err
			}
			rec.Emit(Event{
				RunID:  r.Config.RunID,
				Job:    jobName,
				Event:  EventPhase,
				Name:   "step_pause",
				Phase:  "step_pause",
				Step:   step.Step,
				DurMS:  DurMS(time.Since(pauseStart)),
				OK:     true,
				Detail: string(parkPolicy),
			})
		}

		rec.Emit(Event{
			RunID: r.Config.RunID,
			Job:   jobName,
			Event: EventStep,
			Step:  step.Step,
			Count: len(step.Tasks),
			DurMS: DurMS(time.Since(stepStart)),
			OK:    true,
			Tags: map[string]string{
				"park_policy": string(parkPolicy),
			},
		})
	}
	return nil
}

func (r *Runner) runScheduledTask(
	ctx context.Context,
	rec *Recorder,
	jobName string,
	worker string,
	step int,
	taskNum int,
	st schema.ScheduledTask,
	slotWaitMS float64,
	parkPolicy schema.ParkPolicy,
) error {
	taskStart := time.Now()
	t0 := time.Now()
	handle, err := r.Client.Create(ctx, st.Image, st.InstanceID)
	createDur := DurMS(time.Since(t0))
	if err != nil {
		rec.Emit(Event{
			RunID:    r.Config.RunID,
			Job:      jobName,
			Worker:   worker,
			Event:    EventPhase,
			Name:     "create",
			Phase:    "create",
			Instance: st.InstanceID,
			Image:    st.Image,
			Step:     step,
			Task:     taskNum,
			DurMS:    createDur,
			OK:       false,
			Err:      err.Error(),
		})
		// One task event per attempted rollout, also when the create fails;
		// see Runner.runOpenLoop.
		rec.Emit(Event{
			RunID:      r.Config.RunID,
			Job:        jobName,
			Worker:     worker,
			Event:      EventTask,
			Instance:   st.InstanceID,
			Image:      st.Image,
			Step:       step,
			Task:       taskNum,
			DurMS:      DurMS(time.Since(taskStart)),
			SlotWaitMS: slotWaitMS,
			OK:         false,
			Err:        err.Error(),
		})
		return err
	}
	source := handle.Source
	if source == "" {
		source = string(schema.SourceColdStart)
	}
	rec.Emit(Event{
		RunID:    r.Config.RunID,
		Job:      jobName,
		Worker:   worker,
		Event:    EventPhase,
		Name:     "create",
		Phase:    "create",
		Source:   source,
		Env:      handle.Name,
		Instance: st.InstanceID,
		Image:    st.Image,
		Step:     step,
		Task:     taskNum,
		DurMS:    createDur,
		OK:       true,
	})

	defer func() {
		td0 := time.Now()
		delErr := r.Client.Delete(context.Background(), handle.Name)
		rec.Emit(Event{
			RunID:    r.Config.RunID,
			Job:      jobName,
			Worker:   worker,
			Event:    EventPhase,
			Name:     "delete",
			Phase:    "delete",
			Env:      handle.Name,
			Instance: st.InstanceID,
			Image:    st.Image,
			Step:     step,
			Task:     taskNum,
			DurMS:    DurMS(time.Since(td0)),
			OK:       delErr == nil,
		})
	}()

	tb0 := time.Now()
	res, err := r.Client.Exec(ctx, handle.Name, "echo bootstrap", 30*time.Second)
	rec.Emit(Event{
		RunID:    r.Config.RunID,
		Job:      jobName,
		Worker:   worker,
		Event:    EventPhase,
		Name:     "bootstrap",
		Phase:    "bootstrap",
		Env:      handle.Name,
		Instance: st.InstanceID,
		Image:    st.Image,
		Step:     step,
		Task:     taskNum,
		DurMS:    DurMS(time.Since(tb0)),
		OK:       err == nil && res.ExitCode == 0,
	})

	if r.Config.SnapshotLoop || parkPolicy == schema.ParkPolicySnapshot {
		ts0 := time.Now()
		snap, snapErr := r.Client.Snapshot(ctx, handle.Name)
		rec.Emit(Event{
			RunID:    r.Config.RunID,
			Job:      jobName,
			Worker:   worker,
			Event:    EventPhase,
			Name:     "snapshot",
			Phase:    "snapshot",
			Env:      handle.Name,
			Instance: st.InstanceID,
			Image:    st.Image,
			Step:     step,
			Task:     taskNum,
			DurMS:    DurMS(time.Since(ts0)),
			OK:       snapErr == nil,
			Snapshot: snap,
		})
		if snapErr == nil {
			tr0 := time.Now()
			resumed, resErr := r.Client.Resume(ctx, snap, st.Image, st.InstanceID)
			hit := true
			rec.Emit(Event{
				RunID:    r.Config.RunID,
				Job:      jobName,
				Worker:   worker,
				Event:    EventPhase,
				Name:     "resume",
				Phase:    "resume",
				Source:   string(schema.SourceSnapshotResume),
				Env:      resumed.Name,
				Instance: st.InstanceID,
				Image:    st.Image,
				Step:     step,
				Task:     taskNum,
				DurMS:    DurMS(time.Since(tr0)),
				OK:       resErr == nil,
				Snapshot: snap,
				CacheHit: &hit,
			})
			if resErr == nil && resumed.Name != "" && resumed.Name != handle.Name {
				_ = r.Client.Delete(ctx, handle.Name)
				handle = resumed
			}
		}
	}

	te0 := time.Now()
	evalRes, evalErr := r.Client.Exec(ctx, handle.Name, "pytest -q", 60*time.Second)
	evalOK := evalErr == nil && evalRes.ExitCode == 0
	reward := 0
	if evalOK {
		reward = 1
	}
	rec.Emit(Event{
		RunID:    r.Config.RunID,
		Job:      jobName,
		Worker:   worker,
		Event:    EventPhase,
		Name:     "eval_harness",
		Phase:    "eval_harness",
		Env:      handle.Name,
		Instance: st.InstanceID,
		Image:    st.Image,
		Step:     step,
		Task:     taskNum,
		DurMS:    DurMS(time.Since(te0)),
		OK:       evalOK,
		Reward:   &reward,
	})

	rec.Emit(Event{
		RunID:      r.Config.RunID,
		Job:        jobName,
		Worker:     worker,
		Event:      EventTask,
		Source:     source,
		Env:        handle.Name,
		Instance:   st.InstanceID,
		Image:      st.Image,
		Step:       step,
		Task:       taskNum,
		DurMS:      DurMS(time.Since(taskStart)),
		SlotWaitMS: slotWaitMS,
		OK:         evalOK,
		Reward:     &reward,
	})
	return nil
}

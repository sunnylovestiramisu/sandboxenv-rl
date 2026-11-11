# Substrate RL benchmark: system design and work spec

Status: draft. Owner: benchmarking. Scope: `internal/benchmarking/`,
`cmd/benchmarking/`, `benchmarking/`.

## Overview (for humans)

This system benchmarks sandbox backends—each a **System Under Test (SUT)**—under
RL training and evaluation workloads. Components do not share code; they
communicate strictly through versioned JSON/JSONL/YAML file artifacts in a
standard run directory (`$ARTIFACT_DIR` or `--dest`, on local disk or object
storage) so that simulated load generators, real RL trainers, and different
sandbox backends (accessed via the Python sandbox SDK [`ate_env_rl`], via the
`ate-env-api` gRPC service, or directly against the backend control plane) are
drop-in replaceable—and so that individual pieces can plug and play cleanly
inside other benchmarking, CI, or ML orchestration frameworks.

```
Scenario.yaml + Dataset.jsonl
      │
      ▼
[ Orchestrator (`bench`) — or External Benchmark / CI Harness ]
      │  (composable stages: resolve | render | prewarm | measure | reap | report)
      ├──► [ Provisioner ] (setup SUT or `--render-only`, prewarm, timed reap to zero)
      │
      ├──► [ Simulated Job Runner(s) ] (swebench-loader: eval | steps | open-loop)
      │                                         ▲
      │                                 (replays converted schedule + trajectories)
      │                                         │
      ├──► [ Real RL Job Runner(s) ] ──(trace mode)──► Trace.jsonl ──► [ Trace Converter ]
      │              │
      │       (measure mode: native trainer logs / MLLog / TensorBoard or Events.jsonl)
      ▼              ▼
  $ARTIFACT_DIR: events/*.jsonl + Measurements (Prometheus / logs / node-cache / server_summary.json)
      │
      ▼
[ Report & Validator (`--report-only`) ] ──► Report.json + stats.jsonl ──► [ Comparator ]
```

- **Composable Lifecycle & External Framework Plug-and-Play**: The CLI (`bench`)
  exposes each lifecycle phase as an independent subcommand (`resolve`,
  `render`, `prewarm`, `measure`, `reap`, `report`, plus `run` to chain them
  end-to-end). When running standalone, `bench run` drives the full loop:
  provisions the SUT, resolves deterministic per-step task schedules, pre-warms
  initial images/templates/pools, launches all jobs at their configured offsets,
  scrapes server/cluster telemetry and node cache samples, times a full cluster
  `Reap` back to zero resources, checks validity gates, and builds a `Report`.
  When plugged into an external benchmark or CI harness that already owns
  cluster setup, recipe/manifest deployment, or in-cluster Prometheus/log
  harvesting, the host framework can invoke `bench resolve` / `--render-only`
  ahead of the run, execute `swebench-loader` or a real trainer as a standard
  Kubernetes Job writing to `$ARTIFACT_DIR` / `--dest`, and run `bench measure`,
  `bench reap`, and `bench report --report-only` post-hoc.
- **Feature-vs-Baseline Extensibility (`Feature Overlays`)**: Benchmarking a new
  feature against a baseline does not require forking `Scenario.yaml` or
  modifying Go provisioner/schema code. A declarative **Feature Overlay**
  (`--overlay feature.yaml` or `variants:` in `Scenario.yaml`) layers onto any
  base scenario and bundles the feature's setup modifications (Helm `values`
  overrides, Kustomize/manifest patches, daemon/SDK flags), custom `setup` /
  `reap` / `teardown` hooks, and feature-specific debugging metrics (extra
  Prometheus queries, structured log extractors, and diagnostic scripts) into a
  single file, running both `baseline` and `feature` against the **exact same
  resolved `schedule.json`** and automatically diffing them via the comparator.
- **Simulated vs. Real RL**:
  - **Simulated jobs** drive scripted `Trajectories` and `Resolved schedules`
    through `swebench-loader`, scaling across threads or in-cluster workers to
    saturate the SUT in single-pass (`eval`), multi-step parked-fleet (`steps`
    with suspend-async/suspend-sync/fork/delete, trainer step pauses, and
    next-step image/template prefetch), or `open-loop` modes.
  - **Real jobs** run an actual RL trainer against the same SUT. In `measure`
    mode, sandbox and cluster performance data come from scraped metrics
    (server/SDK Prometheus counters and histograms, node cache counters, and
    `atelet` restore logs) plus coarse per-step trainer metrics (emitted
    directly as `Events.jsonl` or adapted from the trainer's native MLPerf
    `:::MLLOG` lines, TensorBoard event files, or structured step logs in
    `$ARTIFACT_DIR`), avoiding per-call logging overhead. In `trace` mode, a
    verbose SDK flag logs individual calls and sampler/trainer pauses, which are
    converted offline into `Trajectories` and a `Resolved schedule` that the
    simulated loader can replay repeatedly without accelerators.

---

## Agent Specification

This spec breaks the design into work items an agent can pick up alone. Each
item names its deliverable, acceptance criteria and dependencies. Items with
no shared dependency can run in parallel.

## 1. Principle

Reuse happens through file contracts and composable CLI stages, not shared code.
Every component reads or writes the artifacts in section 3 within a standard run
directory (`$ARTIFACT_DIR` or `--dest`, supporting both local paths and object
storage) and nothing else. Because each stage (schedule resolution, manifest
rendering, pool pre-warming, job execution, measurement collection, cluster
reaping, and reporting) can be invoked independently, any component or stage is
drop-in replaceable and can be plugged into external benchmarking, CI, or ML
orchestration frameworks without fighting the host framework's lifecycle.

## 2. Two sibling benchmarks

| Benchmark | Driver | Output |
|---|---|---|
| Simulated RL | load generator replaying scripted workloads | formal numbers; fast, cheap, runs often |
| Real RL | an actual trainer against the same SUT | formal numbers in *measure* mode (from scraped metrics + per-step trainer metrics), or a call trace in *trace* mode, never both in one job |

Both share the dataset manifest, report, system description and validity
rules. Each has its own setup and teardown.

## 3. Artifacts (the contracts)

All run outputs live under a single run directory (`$ARTIFACT_DIR` or `--dest`,
optionally organized in Hive-partitioned object-store paths such as
`<dest>/runs/<name>/run_date=<YYYY-MM-DD>/run_ts=<epoch>/run_tag=<tag>/` for
automated ingestion):

| Artifact | Format | Written by | Read by |
|---|---|---|---|
| Scenario | YAML | operator | orchestrator (`bench`) |
| Feature overlay (optional) | YAML (`--overlay feature.yaml` or `variants:` in `Scenario`) | feature author / operator | orchestrator, provisioner, measurements, report, comparator |
| Dataset manifest | JSONL, one instance per line | dataset tooling | load generator, real runner, prewarm |
| Trajectory | YAML | operator or trace converter | load generator |
| Resolved schedule | JSON (`schedule.json`) | `bench resolve` (simulated), trace converter (real) | load generator, report |
| Run window & metadata | JSON (`run_window.json`: `run_id`, `test_name`, `tag`, `start_ts`, `end_ts`, `steady_start_ts`, `steady_end_ts`, `cluster`, `sut_endpoint`, and free-form `--metadata`) | job runner, `bench`, or external harness | `bench measure`, `bench reap`, `bench report` |
| Trace | JSONL, one SDK call per line (`trace.jsonl`) | SDK verbose flag (*trace* mode only) | trace converter |
| Events | JSONL (`events/*.jsonl`), one operation per line (simulated/SDK) or per training step (real, emitted directly or adapted from native trainer `:::MLLOG` / TensorBoard / step logs), including optional typed `tags`/`metrics` extension maps | load generator, SDK, prewarm, reaper, real trainer or trainer telemetry adapter | report (glob-merges per-worker shards) |
| Measurements | JSON (`measurements.json` / `server_summary.json`, including optional `custom_metrics` map), before/after node image-cache samples (`cache_before.json`, `cache_after.json`), harvested `atelet` restore timing records (`restore_timings.jsonl`), + raw logs archive (`logs/`) | `bench measure` or host harness telemetry collector | report, validator |
| Report & Stats feed | `report.json` (structured report with system description, measurements, `custom_metrics` and validity) + `stats.jsonl` (flat `{timestamp, tag, test_name, metric, measurements}` rows for dashboards/tables) | `bench report` | comparator, external dashboards, humans |

Schema changes to any artifact require a version field bump and a reader that
accepts the previous version.

## 4. Concepts

- **SUT (System Under Test)**: a deployed sandbox backend accessed through one
  of three paths: (a) the Python sandbox SDK (`ate_env_rl` `SandboxFleet`), (b)
   the `ate-env-api` gRPC service (`ateenv.v1alpha`), or (c) directly against the
  underlying backend control plane (`ateapi` on Substrate or Kubernetes CRDs on
  Agent Sandbox, to isolate and price each abstraction layer).
- **Scenario**: one benchmark run against a single SUT. SUT config, repeat count
  (independent experiment repeats), optional `variants` (e.g., `baseline` vs.
  `feature` overlays), and a list of jobs. Its hash is recorded in the system
  description.
- **Feature overlay (Variant)**: a self-contained YAML bundle layered onto a
  base `Scenario` (`--overlay feature.yaml` or `variants:`) to benchmark a new
  feature against a baseline on the **exact same resolved `schedule.json`**
  without editing Go provisioner or schema code. Bundles:
  1. `sut_overrides`: modifications to normal setup (Helm `values` overrides,
     Kustomize/JSON6902 patches on rendered manifests, image tag overrides, and
     daemon/API/SDK/loader flags or env vars).
  2. `hooks`: feature-specific lifecycle steps (`pre_provision`,
     `post_provision`, `pre_reap` / `reap_resources` [extra Kubernetes resource
     kinds or label selectors that must reach zero for Validity Rule 7], and
     `post_teardown`).
  3. `diagnostics`: feature-specific debugging telemetry (named Prometheus
     queries, structured pod-log regex/JSONL extractors, and node/pod diagnostic
     commands archived under `$ARTIFACT_DIR/logs/<feature>/` and surfaced under
     `custom_metrics`).
  4. `thresholds` / `validity`: optional feature-specific validity checks and
     `compare` regression bounds.
- **Job**: one RL loop. Fields: id, kind (`simulated` | `real`), mode for real
  jobs (`measure` | `trace`), start offset, steps, tasks per step (batch size),
  group size (rollouts/trajectories per task; `trajectories_per_step =
  group_size * tasks_per_step`), concurrency (`max_concurrent`), step pause
  (simulated trainer gradient + weight-sync interval between rollout barriers),
  prefetch policy (whether next-step images/templates or pools are warmed ahead
  of the step boundary), park policy (`suspend-async` | `suspend-sync` | `fork`
  | `delete` | `none`), dataset.
- **Workload**: dataset manifest, trajectories and a schedule.
- **Dataset manifest**: instances with a digest-pinned image, prompt material,
  grader inputs and the image-specific setup and reset steps.
- **Task**: an instance plus a trajectory. Simulated jobs are given the
  trajectory; real jobs produce it.
- **Schedule**: the tasks a simulated job runs at each step. Authored as
  explicit per-step lists, as a seeded sampler over a dataset and trajectory
  pool, or as a fixed set. Always resolved to explicit form and saved before
  the run. For a traced real job it is recovered afterward.

## 5. Components

### 5.1 Dataset tooling (exists)
`benchmarking/swebench/`: mirror images by digest, build the manifest,
maintain exclusion lists.

### 5.2 Provisioner
Owns the SUT shared by all jobs. Interface: `Render(config, overlays...) ->
ManifestDir` (`--render-only`, emitting Helm `values.yaml` and K8s manifests
with any Feature Overlay `sut_overrides` and Kustomize/JSON6902 patches applied
to `$ARTIFACT_DIR/manifests/` without applying them so external deployment
harnesses can consume them directly), `Provision(cluster, config, overlays...)
-> endpoint` (executing any overlay `pre_provision` and `post_provision` hooks),
`Reap(runID, overlays...) -> ReapStats` (including any overlay `pre_reap` hooks
and extra `reap_resources` kinds/selectors), `Teardown(overlays...)` (executing
`post_teardown` hooks). One implementation per backend and access mode (via
Python `ate_env_rl` SDK, `ate-env-api`, or direct backend API), each wrapping a
Helm chart or manifest set. Writes the system description: cluster, backend and
SDK versions/commits, active feature overlay names/hashes and `sut_overrides`,
workload provenance (`grader_version`, `dataset_revision`,
`swebench_harness_version`, `exclusion_list_revision`), machine types, node
counts, zone and network topology, sandbox runtime class, sandbox resource
footprint (CPU, memory, disk), bucket region, scenario hash, and configured load
parameters (tasks, group size, steps, tasks per step, trajectories per step,
repeat count, `max_concurrent`, distinct sandbox images).

### 5.3 Job Runner
Owns one loop. Interface: `Render` (`--render-only`), `Setup`, `Start`, `Wait`,
`Collect`, `Teardown`. Rendered Kubernetes `Job` / `JobSet` manifests follow
standard benchmark runner conventions (`${JOB_NAME}`, `${IMAGE}`, `${TAG}`,
`${NAME}`, `${DEST}`, benchmark service account, pod certificate/CA mounts, and
benchmark node-pool tolerations) so external test-runner harnesses can template
and submit them directly as a plugin test type.

- **Simulated**: Setup stages the dataset manifest and resolved schedule and
  deploys the load generator in-cluster. Collect copies per-worker
  `events/*.jsonl` shards and `run_window.json` out to `$ARTIFACT_DIR` /
  `--dest`. Teardown deletes leftover environments and the runner resources.
  Generalizes `benchmarking/swebench/run-in-cluster.sh`.
- **Real**: Setup deploys the trainer (accelerators, weights, inference
  server, SDK endpoint, trace flag only in trace mode) or attaches to a trainer
  job launched by an external ML benchmark harness with `$ARTIFACT_DIR` mounted.
  Collect pulls coarse per-step trainer metrics—either emitted directly as
  `Events.jsonl` or converted by a pluggable **Trainer Telemetry Adapter** from
  native trainer step outputs (`:::MLLOG` lines, TensorBoard event files, or
  structured step logs in `$ARTIFACT_DIR`)—plus the trace if present (bulk
  sandbox and cluster performance data come from `Measurements`). Teardown stops
  the trainer and releases accelerators. Lives beside the trainer it wraps.

### 5.4 Load generator
`cmd/benchmarking/swebench-loader`. Seams: job spec in (plus `--name`, `--tag`,
`--dest` / `$ARTIFACT_DIR`, and free-form `--metadata` JSON when invoked
directly by an in-cluster runner Job), `Backend` and `Sandbox` interfaces as the
SUT driver (supporting SDK, `ate-env-api`, and direct-backend implementations,
plus `create`, `fork`, `suspend-async`, `suspend-sync`, `wake`/`resume`,
`delete`, and batch pre-warming and retirement), per-worker
`events/<worker-id>.jsonl` shards and `run_window.json` out. Note on park
policies across layers: Substrate removed `PauseActor` in favor of
`SuspendActor` with `DurabilityMode` (`ASYNCHRONOUS` by default, keeping the
snapshot on-node with background upload, vs. `SYNCHRONOUS` waiting for durable
object storage). Today `cmd/ate-env-api` (`internal/ate/client.go`) hardcodes
`SuspendEnvironment` to `DURABILITY_MODE_SYNCHRONOUS` (using `ASYNCHRONOUS` only
internally inside `DeleteEnvironment`) and does not yet expose a durability mode
or `ForkEnvironment` in `ateenv.v1alpha`, while the Python SDK (`ate_env_rl`)
exposes `release()` (`delete`) and `recycle()` (`suspend-sync` into a client-side
paused list). Supporting `suspend-async` and `fork` on the `ate-env-api` / SDK
paths therefore includes plumbing `DurabilityMode` on
`SuspendEnvironmentRequest` and `ForkEnvironment` through `ateenv.v1alpha`.
Scales across threads or worker instances as needed so client-side load
generation and pre-warming are never the throughput bottleneck. Modes:

- **eval**: closed loop; create, replay, delete at fixed concurrency. Exists.
- **steps**: maintains a **client-side parked fleet (working set)** of
  environments across training steps separated by a configurable `step_pause`.
  Each step matches tasks to parked environments in the working set by image,
  resets or forks the matched, creates the rest, and parks (`suspend-async`,
  `suspend-sync`, or `fork`) or deletes the surplus. When `prefetch` is enabled,
  uses the resolved schedule to pre-warm the next step's images ahead of the
  step boundary—which is backend-specific: on **Substrate**, ensuring each
  image's derived `ActorTemplate` and **golden snapshot tag exist** and the
  image is warmed in the cache of $N$ nodes (or pre-creating suspended actors
  when exercising the Python SDK's client-side `_warm_paused_pool`); on **Agent
  Sandbox**, sizing a Kubernetes `SandboxWarmPool`—and retires the previous
  step's surplus asynchronously, recording standing working-set size, churn,
  lead margin (next step's prewarm ready minus step boundary), release overlap,
  and operation errors/retries/timeouts. Replaces today's fixed fleet.
- **open-loop** (optional): Poisson arrivals regardless of completion, to find
  the throughput knee.

### 5.5 SDK instrumentation
In `measure` mode, the SDK exports in-memory Prometheus metrics (counters and
histograms for lifecycle and command/grading execution duration, cumulative
per-task sandbox execution time, `caller_wait_for_slot` [throttled by
`max_concurrent`] vs. `wire_claim_to_ready` vs. `Ready`-to-first-command
sub-phases, acquisition source [`warm_pool`, `cold_create`, `golden_restore`,
`fork`, `resume`], client-observed retries/attempts, errors, and readiness
timeouts with their cumulative time cost) scraped by `Measurements`, avoiding
per-call logging overhead; optionally, when `ENV_BENCH_EVENTS_PATH` (defaulting
to `$ARTIFACT_DIR/events`) is configured, workers flush coarse task/step
summaries at step boundaries. In `trace` mode, a verbose flag logs one JSONL
record per call with payloads and timing detail (`Trace.jsonl`). A converter
turns a trace into trajectories (recording intra-rollout sampler/LLM
inter-command think times as `Step.Think`) plus an explicit schedule (recording
inter-step trainer pauses as `step_pause`).

### 5.6 Measurements
Per SUT, config-driven. Can run during `bench run`, as a standalone post-run
command (`bench measure --start-ts ... --end-ts ... --telemetry-lag 70s`,
waiting `--telemetry-lag` so delayed OTel exports from node daemons land in
Prometheus and auto-detecting the `[steady_start_ts, steady_end_ts]` window), or
by ingesting pre-harvested `server_summary.json` / `trial_summary` /
`phase_breakdown` artifacts when executed inside a harness that already queries
Prometheus and pod logs in-cluster. For every run (regardless of success or
failure), scrapes server-side and cluster Prometheus metrics for the run window,
samples before/after per-node `atelet` image-cache counters (hits, misses/cold
pulls, errors), harvests per-actor `atelet` restore timing records, executes any
feature-specific `diagnostics` declared in active Feature Overlays, and always
saves controller, API, and node/worker daemon logs (including `previous`
container logs when a worker or node daemon restarted mid-run):

- **Sandbox and lifecycle internals**: template and snapshot creation rates,
  per-node and per-worker-pod readiness wall time and rates (plus Kubernetes
  `PodStartupLatency` [`schedule`, `image_pull`, `container_start`,
  `ready_to_serving`], which on **Substrate** applies only to worker-pool
  provisioning in the Prepare stage since actors do not start pods, and applies
  per-sandbox only on pod-per-sandbox SUTs such as Agent Sandbox), per-pool and
  per-object readiness latency, canonical per-actor startup/restore and
  checkpoint sub-phases parsed from structured `atelet` timing logs (Substrate
  `Restore`/`Checkpoint timing breakdown`: `manifest_fetch`, `download`,
  `oci_unpack`, `worker_restore`/`worker_checkpoint`, `persist`, split by
  `golden` vs. `latest`, joined to `ttfi` on first restore and `wake` on later
  restores with an `outside` remainder for time spent outside `atelet`, and
  cross-checked against client request counts to surface rotated or unreadable
  pod logs), Substrate router registry metrics (`atenet.router.parking.rejected`
  and `atenet.router.route.duration`, plus router CPU and active connections),
  and peak/sustained claim rates.
- **Cluster and node health (USE & K8s scalability)**: worker node and pod CPU,
  memory, and disk I/O pressure (PSI `stall_pct`), node disk space used %,
  per-node image/snapshot cache hits, misses (cold pulls), and eviction counts
  (from before/after `atelet` node cache samples and Prometheus), container
  `OOMKill` and kubelet eviction counts (diffing worker pod status before/after
  the run plus K8s `Evicted` events), node readiness transitions, network
  identity count and creation rate, API server request latency (`p99`) and
  rate-limit responses (HTTP 429s, plus object-store 429s/5xx), controller
  `workqueue` depth and queue duration, database connection saturation,
  controller restarts and leader-election losses, queue admission latency.
- **Hardware and storage utilization**: active vs. total (active + parked/warm)
  sandboxes per node, per vCPU, per GiB RAM, and per worker pod
  (`p50`/`p90`/`p99`), worker pool bin-packing ratio (`cluster_packing`),
  actual sandbox CPU and memory working-set (`WSS`) vs. requested footprint,
  worker vs. system/control-plane/router node counts, and canonical snapshot
  telemetry (`size_mb` percentiles, `checkpoints_in_window`, `checkpoint_mb_s`).
- **Feature-specific diagnostic metrics (`custom_metrics`)**: any extra named
  Prometheus queries, structured pod-log regex/JSONL extractors, or diagnostic
  command outputs defined by active Feature Overlays, stored under the typed
  `custom_metrics` map in `measurements.json` and `$ARTIFACT_DIR/logs/<feature>/`.

### 5.7 Orchestrator CLI
Exposed both as composable stage subcommands (for external benchmark/CI
frameworks) and as a single `bench run SCENARIO.yaml [--overlay FEATURE.yaml]`
command:

- `bench resolve`: resolves `Scenario` + `Dataset` into `$ARTIFACT_DIR/schedule.json`.
- `bench render` (`--render-only [--overlay FEATURE.yaml]`): outputs rendered SUT
  and Job Runner manifests (with any Feature Overlay `sut_overrides` and
  Kustomize/JSON6902 patches applied) into `$ARTIFACT_DIR/manifests/` without
  applying them.
- `bench prewarm`: prewarms configured initial images/templates across jobs—on
  Substrate, ensuring the derived `ActorTemplate`s exist and waiting for their
  golden snapshots to settle (`-golden-wait`), without creating an environment;
  the layers reach a node only when an environment boots there, which is the
  pre-flight's job per placement domain (`swebench-loader preflight`,
  `ate_env_rl.PreflightWarmer`, `ate-env warm-image`)—and recording the
  baseline node cache sample `cache_before.json`; on Agent Sandbox, warming
  `SandboxWarmPool`s—recording total prepare wall time, per-template/pool
  readiness time, and per-object readiness times as
  `$ARTIFACT_DIR/events/prewarm.jsonl`.
- `bench measure [--overlay FEATURE.yaml]`: harvests Prometheus metrics,
  before/after node image-cache samples (`cache_before.json`,
  `cache_after.json`), `atelet` restore timing records
  (`restore_timings.jsonl`), any Feature Overlay `diagnostics`, and
  controller/API/node logs over `run_window.json` (or `--start-ts`/`--end-ts`
  with `--telemetry-lag`) into `$ARTIFACT_DIR/measurements.json` and
  `$ARTIFACT_DIR/logs/`, always running regardless of pass or fail.
- `bench reap [--overlay FEATURE.yaml]`: timed server-queried clear of claims,
  pools, templates, sandboxes, pods, and any overlay `reap_resources` to zero,
  recorded as `$ARTIFACT_DIR/events/reap.jsonl`.
- `bench report` (`--report-only`): glob-merges `$ARTIFACT_DIR/events/*.jsonl`
  (adapting native trainer step logs if present) +
  `$ARTIFACT_DIR/measurements.json` (including `atelet` restore phase joins,
  node cache diffs, and `custom_metrics`), runs validity gates, and writes
  `report.json` + `stats.jsonl` to `$ARTIFACT_DIR` / `--dest` (local disk or
  object store).
- `bench run`: chains provision (or existing endpoint, applying any
  `--overlay`) $\to$ resolve $\to$ prewarm $\to$ setup & start job runners at
  offsets $\to$ wait $\to$ measure $\to$ reap $\to$ teardown $\to$ validate &
  report. When multiple `variants` (e.g., `baseline` vs. `feature`) are passed,
  resolves `schedule.json` **once**, runs each variant against that identical
  schedule, and invokes the comparator to emit a side-by-side diff.

### 5.8 Report and comparator
Events carry run, job, step, environment, instance, acquisition source and
sub-phase timings. In addition to `report.json`, the reporter emits flat
`{timestamp, tag, test_name, metric, measurements}` rows in `stats.jsonl`
(`trial_summary`, `server_summary`, `phase_breakdown_summary`, and per-operation
latency/throughput rows) so existing benchmark dashboards and time-series tables
can ingest runs without custom parsers. The report gives:

- **Prepare stage**: total prepare wall time (`setup()` to golden
  snapshots/node caches or warm pools ready), per-node and per-worker-pod
  readiness wall time and rates (including worker-pool `PodStartupLatency` on
  Substrate), templates/golden snapshots created per second, time per
  template/pool to become ready, and time per object to become ready
  percentiles.
- **Per-job percentile (`p50`/`p90`/`p95`/`max`) and per-step tables**:
  - **TTFI & claim throughput**: client-boundary TTFI (total and split by
    `caller_wait_for_slot` vs. `wire_claim_to_ready` vs.
    `Ready`-to-first-command, acquisition source [`warm_pool`, `cold_create`,
    `golden_restore`, `fork`, `resume`], and joined `atelet` first-restore
    phases + `outside` remainder), cluster-wide node image-cache hits, misses
    (cold pulls), errors, and misses per first command from the before/after
    node cache sample, readiness timeout count and cumulative time cost, and
    peak/sustained claim rate (claims/s).
  - **Active lifecycle & per-step working-set stages**: **in-sandbox execution
    times** (per-command/step execution latency by name [e.g., tool calls,
    `run_tests`, `grade`], **total per-task in-sandbox execution time**
    [cumulative command + grading time after TTFI], and **total per-task wall
    time** [claim through TTFI + all sandbox executions to completion], split by
    acquisition source to surface lazy-restore I/O penalties), lifecycle
    operations (`create`, `fork`, `suspend-async`, `suspend-sync`,
    `wake`/`resume`, `delete`, split by canonical `atelet` restore/checkpoint
    sub-phases [`manifest_fetch`, `download`/`image_pull`, `oci_unpack`,
    `worker_restore`/`worker_checkpoint`, `persist`, plus `outside`] or
    per-sandbox `PodStartupLatency` on pod-per-sandbox backends), data-plane
    router metrics (`atenet.router.route.duration`,
    `atenet.router.parking.rejected`, and client-observed first-command
    `attempts`/retries/timeouts), working-set reuse hit rate (%), standing
    parked working-set size, fleet park and release wall times, release overlap
    with the next step's create burst, prefetch lead margin, churn per step,
    leaked/orphaned resources at step release, task/group/step/distinct-image
    counts, and reward rate.
- **RL step & rollout breakdown**: step wall-clock breakdown (`rollout`,
  `weight_sync`, `train`), unified per-step rollout decomposition at `p50` and
  `p100` (the step-barrier straggler) into `llm_think` + `env_wait`
  (`sdk_slot_wait`, `ttfi`, `exec`, `grade`, `park`)—yielding per-step barrier
  time, straggler skew (`p100 / p50`), **environment overhead fraction
  (`env_wait / rollout_time`)**, and **accelerator bubble / sandbox-stalled
  fraction (`accelerator-hours/task`)**—plus sampler and trainer token
  throughput (`tokens/s/accelerator`), training model FLOP throughput
  (`FLOPs/s/accelerator`), and reward convergence.
- **Cluster health, reap and hardware/storage cost**: cluster and node health
  summary (node/pod CPU/mem/IO PSI `stall_pct`, `OOMKill`/kubelet/cache
  evictions, API server `p99` latency & 429s, controller `workqueue` depth &
  lag), reap wall time to zero, deletion rate (objects/s), and remaining orphan
  count, active vs. total sandbox counts and density (`sandboxes` per node, per
  vCPU, per GiB RAM, per worker pod, and `cluster_packing`), actual vs.
  requested sandbox CPU and memory `WSS`, **worker vs. system/control-plane
  node-hours per completed task**, and **snapshot volume and throughput
  (`size_mb` percentiles, `checkpoints_in_window`, `checkpoint_mb_s`)**.
- **Feature / diagnostic metrics (`custom_metrics`)**: any feature-specific
  metrics collected via Feature Overlay `diagnostics` or event extension maps,
  rendered in a dedicated report section and emitted as `stats.jsonl` rows.
- **Multi-job interference**: a cross-job timeline with latency split by how
  many other jobs were active, plus run validity and the system description.

The comparator diffs two reports (including `custom_metrics`) against base and
feature-overlay thresholds.

## 6. Validity rules

A run counts only if all hold. The report marks invalid runs and says why.

1. Warm or cold footing is declared, and both the `prewarm` events and the
   server-side node image-cache sample (`cache_before.json` /
   `cache_after.json`, since per-node cache state on Substrate is not visible to
   the client) match the declaration.
2. Reward rate meets the configured threshold.
3. Task count meets the configured minimum.
4. Failed creates stay under the configured fraction.
5. Every job ran its full schedule.
6. Trace capture is recorded, not rejected: `report.json` `trace_captured`,
   `run_window.json` `metadata.trace_captured`, the `stats.jsonl` label, and a
   `_traced` suffix on the archive folder name all say whether an SDK trace was
   captured, because trace instrumentation adds per-call latency and traced
   runs must not be compared with untraced ones unknowingly. Run metadata or a
   folder name that contradicts the bundle's trace evidence fails validation.
7. Reap reaches zero remaining sandboxes, pools, templates, claims, and any
   Feature Overlay `reap_resources` within the reap timeout (zero orphans).

## 7. Agent guardrails and testing invariants

Agents implementing work items must obey these invariants; tests must enforce
them so an implementation cannot pass by fulfilling the letter of a work item
while breaking the system.

1. **Strict schema enforcement across languages**: Go readers must reject
   unknown top-level JSON fields (`DisallowUnknownFields`) or validate against
   `W1`'s JSON Schema files, while `W1` explicitly includes typed extension maps
   (`tags: map[string]string` and `metrics: map[string]float64` on `Events`;
   `custom_metrics: map[string]MetricSummary` on `Measurements` and `Report`) so
   feature overlays can attach diagnostic metrics without breaking schema
   validation. Python SDK trace emitters (`W11`) and trainer step-event emitters
   (`W13`) must validate their output against the exact same JSON Schema files
   from `W1`, never a hand-written copy.
2. **File-only boundaries (no in-memory shortcuts)**: The orchestrator,
   schedule resolver, trace converter, load generator, reporter and comparator
   must communicate strictly by reading and writing the section 3 files on
   disk, never by passing in-memory Go structs across stages. Dataset-specific
   setup, reset and grading logic must live in the dataset manifest and
   trajectory files, never hardcoded in the orchestrator, pool manager or
   reporter.
3. **Server truth for reap and measurements (no silent zeros)**:
   - `Reap` (`W8`, `W10`) must query the backend/cluster API for all live
     resources matching the run label or prefix; deleting only the handles the
     client remembered in memory is forbidden.
   - `Measurements` (`W9`) must distinguish `0` (measured zero errors) from
     `null`/error (metric missing or scrape failed). A failed scrape must fail
     measurement collection, never default to `0`.
4. **Zero extra RPCs or per-call logging in `measure` mode**: In `measure`
   mode, SDK instrumentation (`W11`) exports in-memory Prometheus counters and
   histograms (with attribution labels from existing response headers/payloads)
   and never logs per-call lines or issues extra status/inspect RPCs on the hot
   path. Per-call JSONL logging is restricted to `trace` mode.
5. **Exact raw-event aggregation**: `Report` (`W4`) must compute percentiles
   (`p50`, `p90`, `p95`, `max`) over the merged raw event durations, never by
   averaging percentiles across steps, shards or jobs.
6. **Non-blocking prefetch and client self-benchmarking**: Next-step pool
   prefetch and previous-step retirement (`W3`) must run asynchronously off the
   step barrier's critical path. The load generator and event recorder must
   include a Go benchmark (`go test -bench`) against a zero-latency fake
   backend proving client-side overhead does not bottleneck high-concurrency
   bursts.
7. **End-to-end chain test without hand-crafted intermediate mocks**: A
   chain test must feed a trace emitted by `W11` into `W12` (converter) $\to$
   `W2` (resolver) $\to$ `W3` (load generator over an in-process gRPC/HTTP fake
   server) $\to$ `W5` (validator) $\to$ `W4` (report) $\to$ `W6` (comparator),
   verifying that concurrency curves, think times and step counts survive the
   full round trip.

## 8. Work items

| # | Item | Deliverable | Acceptance | Depends on |
|---|---|---|---|---|
| W1 | Artifact schemas | Go types with version fields for scenario (with repeat count and optional `variants`), job (with steps, tasks per step, group size, concurrency, and `suspend-async \| suspend-sync \| fork \| delete \| none` park policy), schedule, run window (`run_window.json` with `start_ts`/`end_ts`/`steady_start_ts`/`steady_end_ts` and `--metadata`), trace, events (extend with client queue wait, sub-phase, acquisition source, cache state, prepare/pool/object readiness, prefetch/retire, retry/timeout, reap, trainer step events, and typed `tags`/`metrics` extension maps), measurements and report (`report.json` with typed `custom_metrics` map plus flat `{timestamp, tag, test_name, metric, measurements}` `stats.jsonl` export rows), system description (with `grader_version`, `dataset_revision`, `swebench_harness_version`, `exclusion_list_revision`, and active overlay metadata); JSON schema files; golden fixtures | round-trip tests with `DisallowUnknownFields`; schema validation helper usable from Go and Python; old events files still parse | none |
| W2 | Schedule resolver | explicit, sampled (seeded) and fixed sources resolve to one explicit form; saved as resolved schedule JSON (`schedule.json`) | same seed gives identical output; fixture covers each source; validates against W1 schema | W1 |
| W3 | Steps-mode working set and prefetch | load generator matches tasks to its client-side working set (parked fleet) of parked environments by image, wakes + resets or forks, creates, parks (`suspend-async \| suspend-sync \| fork \| delete \| none`) or deletes surplus; supports configurable inter-step trainer pause (`step_pause`), async next-step prefetch (golden bake + $N$-node cache warm on Substrate; `SandboxWarmPool` resize on Agent Sandbox) and previous-step retirement; emits per-worker `events/<worker-id>.jsonl` shards (standing working-set size, churn, lead-margin, release-overlap and error/retry/timeout events) and `run_window.json` to `$ARTIFACT_DIR` / `--dest` | in-memory backend test with changing images per step, step pause and async prefetch proving positive lead margin; `go test -bench` throughput test; existing steps tests pass | W1, W2 |
| W4 | Job field in events and extended report | glob-merges per-worker `events/*.jsonl` shards; events carry job id and attribution fields; report groups per job, computes exact percentiles from raw events, reports prepare stage (including worker-pool `PodStartupLatency` on Substrate), peak/sustained claim rates, client-boundary TTFI split by source/cache state (and joined against server restore sub-phases with `outside` remainder when `-restore-log` is supplied), per-command and full per-task sandbox execution time (`task_exec` post-TTFI and total `task_wall` split by source/cache state), lifecycle metrics (`create`, `wake`, `reset`, `suspend-async`, `suspend-sync`, `fork`, `delete`) with canonical server sub-phases, before/after node image-cache delta (`-cache-before` / `-cache-after`), unified per-step rollout decomposition (`p50`/`p100` straggler skew, barrier time, `env_overhead_pct`), readiness timeout cost, hardware/storage cost (active vs. total density per node/vCPU/GiB RAM/pod, `cluster_packing`, worker vs. system node-hours/task, actual vs. requested CPU/memory `WSS`, snapshot `size_mb`/`checkpoints_in_window`/`checkpoint_mb_s`), `custom_metrics` passthrough, and cross-job interference split; emits both `report.json` and flat `stats.jsonl` rows to `$ARTIFACT_DIR` / `--dest` (local disk or object store) | report test over a multi-job, multi-step raw events and measurements fixture verifying exact percentiles, restore/cache joins, `custom_metrics`, interference buckets, and `stats.jsonl` row emission | W1 |
| W5 | Validity rules | validator over events, measurements (`cache_before.json` / `cache_after.json` node image-cache sample for Rule 1) and scenario (including zero-orphan reap check and missing-measurement check); report carries validity and reasons | one test per rule, pass and fail | W1, W4 |
| W6 | Comparator | `swebench-loader compare A.json B.json -thresholds T.yaml` (diffing both core report sections and `custom_metrics`); nonzero exit on regression | fixture pair with a planted regression | W4 |
| W7 | Simulated Job Runner | Go implementation of the runner interface supporting `--render-only` (emitting standard runner Job manifests with `${JOB_NAME}`, `${IMAGE}`, `${TAG}`, `${NAME}`, `${DEST}`, service account, pod cert mounts and benchmark node-pool tolerations) and in-cluster deployment without client-side throughput bottlenecks; replaces run-in-cluster.sh | smoke scenario with one job runs end to end against a cluster; rendered manifest plugs into external Job-template harnesses | W1, W2 |
| W8 | Provisioner, Reaper and Backend drivers | Helm/manifest provision (with `--render-only` manifest output), server-queried timed reap to zero and teardown; `Backend` drivers covering SDK (`ate-env-api` + Python/Go SDK, including plumbing `DurabilityMode` (`async` vs. `sync`) on `SuspendEnvironment` and `Fork` through `ateenv.v1alpha` / `cmd/ate-env-api`) and direct-API access across supported sandbox backends; system description written with provenance fields (`grader_version`, `dataset_revision`, `swebench_harness_version`, `exclusion_list_revision`) | hermetic fake-API unit tests plus cluster smoke run verifying server-side orphan count reaches zero | W1 |
| W9 | Measurements | config-driven Prometheus/cluster queries (supporting standalone `bench measure --start-ts ... --end-ts ... --telemetry-lag 70s`, steady-state window detection, or ingestion of pre-harvested `server_summary.json` / `trial_summary` / `phase_breakdown` artifacts; node/pod/pool/object readiness, Substrate router metrics `atenet.router.route.duration` and `atenet.router.parking.rejected`, peak/sustained claims, node/pod CPU/mem/IO PSI, `OOMKill`/kubelet/cache evictions, node/identity/API `p99`/429/workqueue/DB/controller/queue health, active vs. total sandboxes per node/vCPU/GiB RAM/pod, `cluster_packing`, actual CPU/memory `WSS`, worker vs. system nodes, snapshot `size_mb`/`checkpoints_in_window`/`checkpoint_mb_s`), before/after cluster-wide node image-cache counter sampler (`cache_before.json` / `cache_after.json`), structured `atelet` node restore/checkpoint/pod-startup timing log parser (`restore_timings.jsonl`, cross-checked against client request counts), and unconditional controller/API/node log capture including `previous` restarted container logs (regardless of run pass or fail); distinguishes `0` from missing/failed scrape | test against fake Prometheus HTTP server covering both valid series, pre-harvested `server_summary.json` / `cache_*.json` / `restore_timings.jsonl` ingestion, and scrape failure; smoke run attaches measurements and logs to report | W1, W8 |
| W10 | Orchestrator CLI | `cmd/benchmarking/bench` with composable subcommands (`resolve`, `render`, `prewarm`, `measure`, `reap`, `report --report-only`, and end-to-end `run SCENARIO.yaml`) driving phases in 5.7 strictly via `$ARTIFACT_DIR` / `--dest` file artifacts, including timed prepare, before/after node cache sampling, unconditional metric/log collection, and timed reap | two simulated jobs with offsets run concurrently via `bench run`; individual stage subcommands run standalone against an existing `$ARTIFACT_DIR`; reap converges to zero; report is valid | W2, W4, W5, W7, W8, W9 |
| W11 | SDK instrumentation | in-memory Prometheus metrics in `measure` mode (lifecycle, per-command `exec`/grade, and total per-task sandbox execution histograms, client slot-wait vs. wire sub-phases, acquisition source and client-observed cache state labels from existing responses, retry/error/timeout counters, zero extra RPCs, zero per-call logs); optional coarse step-boundary flush to `ENV_BENCH_EVENTS_PATH`; verbose `trace` flag emitting `Trace.jsonl`; overhead benchmarked | emitted traces validate against W1 JSON schema; microbenchmark proves measure-mode metric overhead stays under bound; trace fixture produced | W1 |
| W12 | Trace converter | trace JSONL to trajectories plus explicit schedule preserving concurrency profile, payload sizes, intra-trajectory sampler/LLM inter-command think times (`Step.Think`), and inter-step trainer pauses (`step_pause`, both recorded from the RL side that produced the trace rather than attributed to the sandbox) | end-to-end chain test: W11 trace $\to$ W12 $\to$ W3 load generator reproduces step counts, concurrency histogram, intra-trajectory think-time distribution, and inter-step trainer pauses | W1, W2, W3, W11 |
| W13 | Real Job Runner & Trainer Telemetry Adapters | runner interface (including `--render-only` and attach/post-hoc collection) over one trainer in `measure` and `trace` modes, plus pluggable trainer step-telemetry adapters (converting native MLPerf `:::MLLOG` lines, TensorBoard event files, or structured step logs in `$ARTIFACT_DIR` into trainer step `Events.jsonl`); exports trainer step breakdown (`rollout`, `weight_sync`, `train`), environment overhead fraction (`env_wait/rollout_time`), accelerator bubble/stalled fraction and accelerator-hours per task, sampler and trainer token/s/accelerator, FLOPs/s/accelerator, and rewards | one real job runs through the orchestrator; trainer telemetry adapter parses native `:::MLLOG`/TensorBoard fixtures into valid step events; trace mode yields a trace W12 converts and replays | W10, W11, W12 |
| W14 | Open-loop mode (optional) | Poisson arrival mode in the load generator | in-memory test shows arrivals independent of completion; report shows the knee | W3 |
| W15 | Feature overlays & A/B variant runner | Declarative `FeatureOverlay` YAML schema and engine (`--overlay FEATURE.yaml` and `variants:` in `Scenario.yaml`): applies `sut_overrides` (Helm `values`, Kustomize/JSON6902 manifest patches, daemon/API/SDK/loader flags), executes `pre_provision`/`post_provision`/`pre_reap`/`reap_resources`/`post_teardown` lifecycle hooks, harvests feature-specific `diagnostics` (extra Prometheus queries, structured log regex/JSONL extractors, diagnostic commands) into `custom_metrics` and `$ARTIFACT_DIR/logs/<feature>/`, evaluates feature-specific `validity`/`thresholds`, and runs Baseline-vs-Feature variants against a single shared `schedule.json` with automatic `compare` output | end-to-end A/B test over a base scenario + feature overlay fixture (patching manifests, running setup/teardown hooks, verifying extra `reap_resources` reach zero, and scraping custom Prometheus + log metrics into `custom_metrics`) reuses one `schedule.json` and emits a side-by-side `compare` diff without modifying Go provisioner or schema code | W6, W8, W9, W10 |

## 9. Milestones (incremental delivery and verification)

Work items are delivered and integrated in six vertical milestones. Each
milestone unlocks a general class of benchmark scenarios and ends with an
end-to-end verification gate so integration, schema and cluster lifecycle
defects surface before scaling up complexity or hardware cost.

| Milestone | Functionality / Scenario Enabled | Work Items | End-to-End Verification Gate |
|---|---|---|---|
| **M1: Single-Job Step-Fleet & Regression Gate (Core MVP)** | Deterministic multi-step simulated RL loops (with dynamic per-step image sets, working-set reuse/churn, inter-step pauses and async next-step prefetch) against an existing endpoint, with automated validity and regression diffing. | `W1`, `W2`, `W3`, `W4` (single-job & lifecycle tables), `W5` (event/schedule rules), `W6` | Resolve a seeded multi-step schedule with changing images per step $\to$ run `swebench-loader` in `steps` mode against a live endpoint $\to$ emit a valid `Report.json` with positive prefetch lead margin $\to$ pass `compare` against a baseline report. |
| **M2: Automated SUT Lifecycle, Server Telemetry & Backend Drivers** | Self-contained single-job runs with automated System Under Test (SUT) provisioning, server-side cluster health, restore-phase and node-cache telemetry, timed cluster reap to zero orphans, and A/B comparisons across backends and access layers (SDK/`ate-env-api` vs. direct API). | `W7`, `W8`, `W9`, plus `W4`/`W5` extensions for measurements, node-cache validation, hardware cost and zero-orphan reap | Provision a SUT from scratch $\to$ run an in-cluster simulated job through both the SDK driver and the direct-backend driver $\to$ attach scraped `Measurements` (`cache_before`/`cache_after`, `restore_timings.jsonl`) $\to$ verify `Reap` reaches zero server-side orphans $\to$ compare SDK vs. direct reports. |
| **M3: Multi-Job Contention & Throughput Saturation** | Concurrent multi-loop RL workloads sharing one SUT (staggered or aligned step barriers, independent working-set churn) to measure cross-job interference, plus open-loop arrival sweeps to find throughput knees. | `W10`, `W4` (cross-job timeline & interference split), `W14` | `bench run SCENARIO.yaml` executes multiple offset simulated jobs concurrently on a shared SUT, prewarms initial images/pools, produces a valid report with cross-job interference tables, and reaps the cluster to zero. |
| **M4: SDK Telemetry & Trace-to-Simulation Replay** | Always-on low-overhead SDK metrics (`measure` mode) and offline conversion of verbose SDK call traces (`trace` mode) into deterministic schedules and trajectories replayable by the simulated loader without accelerators. | `W11`, `W12` | Capture a verbose `Trace.jsonl` from an SDK workload $\to$ convert via `W12` into `Trajectory` YAMLs and a `Resolved schedule` $\to$ replay through `M3` (`swebench-loader`) and verify step counts, concurrency histogram, intra-trajectory think-time distribution, and inter-step pauses match the trace. |
| **M5: Live Trainer Benchmarking** | End-to-end real RL training loops orchestrated against the SUT in both `measure` mode (unified sandbox + trainer step/throughput reporting) and `trace` mode (feeding new real workloads into `M4`). | `W13` | Orchestrate one real trainer job in `measure` mode (yielding a valid `Report.json` with trainer step breakdown and accelerator throughput) and one in `trace` mode (whose trace converts and replays cleanly through `M4`). |
| **M6: Feature-vs-Baseline Overlays & Diagnostic Extensibility** | Zero-code A/B benchmarking of new features against baseline on an identical resolved schedule, bundling feature-specific setup overrides, custom setup/reap/teardown hooks, and feature-specific debugging metrics/logs into a declarative `FeatureOverlay` YAML. | `W15` | Run `bench run SCENARIO.yaml --overlay FEATURE.yaml` (or `variants: [baseline, feature]`) $\to$ verify both variants share one `schedule.json`, apply manifest patches and setup/teardown hooks, reap custom feature resources to zero, populate `custom_metrics` in `Report.json` and `stats.jsonl`, and output a side-by-side `compare` diff. |

**Parallel execution & fail-fast ordering**:
- `M1` is the minimum viable product: it requires no cluster provisioner,
  Prometheus scraper, or accelerators to start benchmarking an existing sandbox
  endpoint, and `W1` reserves the typed `custom_metrics` and `tags`/`metrics`
  extension maps up front so later feature diagnostics never require breaking
  schema changes.
- `W11` (SDK instrumentation) only depends on `W1` and can be developed in
  parallel with `M1`–`M3` so `M4` is ready as soon as `M1`'s loader working-set
  manager (`W3`) lands.
- `M2` intentionally precedes `M3` so orphan leaks, rate limits (HTTP 429s) and
  missing telemetry are caught on a single in-cluster job before multiplying
  load across concurrent jobs.
- `M6` (`W15`) only depends on `M3` (`W6`, `W8`, `W9`, `W10`), so it can be
  pulled forward in parallel with `M4`/`M5` if feature A/B experiments are
  needed on simulated workloads before live trainer integration (`M5`).

## 10. Out of scope for now

Egress for sandboxes at scale, multiple Systems Under Test (SUTs) in one
scenario, and scheduling runs on a cadence. Each is its own spec.

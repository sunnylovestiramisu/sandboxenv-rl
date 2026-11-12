# `bench`: scenario runs and real RL training runs

`bench` orchestrates two kinds of benchmark and writes every artifact into one
directory (`-dest "$ARTIFACT_DIR"`). Scenario runs (`bench run -scenario`,
`benchmarking/scenarios/*.yaml`) run in process against a local scenario
client: provisioning is render-only, `Create` returns `env-N`, `Exec` returns
exit 0, `Snapshot` returns `snap-<name>`; no sandbox is created and no cluster
is called, so they exercise the orchestration and the report, not Substrate.
Real RL training runs are a verl trainer launched from `examples/`, which
`bench` measures around: `spinup`, `measure`, `reap`, `report` run outside the
training Job and read what it writes.

The simulated benchmark, `swebench-loader`, is a separate tool with its own
page: [`benchmarking/swebench/README.md`](../../../benchmarking/swebench/README.md).
It does not use `bench run`. Its documented flow runs `bench measure -k8s`
beside `swebench-loader report` (`benchmarking/swebench/SKILL.md`); `bench
report` reads `events/*.jsonl` under `-dest`, which the loader writes when run
with `-events-dir`.

## Where Ray is

Only the real RL path runs Ray, by two launch paths. `examples/Qwen3-1.7B-verl`
and `examples/Qwen3.5-35B-A3B-verl` run `ray start` inside the training Job
(`run_fully_async.sh`), launched by their `deploy.sh`. `examples/verl_swebench`
is a KubeRay `RayJob` (`ray-job.async-verl.yaml`), applied with `kubectl apply`
or `run-telemetry-e2e.sh`, and KubeRay creates the cluster. In both the
trainer's rollout workers call the ate-env API from Ray tasks. `bench` itself
does not start or depend on Ray: `bench spinup`, `measure`, `reap` and
`report` run outside the Job and read what it writes (`train.log`,
`traces/*.jsonl`). Scenario runs and the simulated loader have no Ray in their
path at all.

## Subcommands

`cmd/benchmarking/bench` is the unified orchestrator for both scenario-driven
synthetic workloads and real RL training recipes (`examples/*-verl`). Every
subcommand communicates through a single artifact directory (`-dest "$ARTIFACT_DIR"`):

| Subcommand | Purpose |
|---|---|
| `bench run` | Runs a declarative `scenario.yaml` in process (resolve -> prewarm -> execute -> measure -> reap -> report) against the local scenario client: no sandbox is created and no cluster is called |
| `bench resolve` | Resolve a `scenario.yaml` and dataset into a deterministic `schedule.json` |
| `bench render` | Render Kubernetes Job manifests from a `scenario.yaml` |
| `bench prewarm` | Prewarm templates prior to the timed run window: ensure each image's derived `ActorTemplate`, which starts its golden snapshot bake, and wait for the goldens (`-golden-wait`, default on); no environment is created, and the nodes' layer caches are the pre-flight's job (`swebench-loader preflight`) |
| `bench spinup` | Scale `WorkerPool` (`0 -> N` `ateworker` pods), wait for `ate-env-api` + all worker pods `Ready`, and record `workerpool_spinup_wall_ms` and `worker_pod_*_ms` into `measurements.json` and `events/substrate_lifecycle.jsonl` |
| `bench spindown` | Scale `WorkerPool` (`N -> 0` `ateworker` pods), wait for all worker pods to terminate, and record `workerpool_spindown_wall_ms` and `worker_pod_terminate_ms` into `measurements.json` and `events/substrate_lifecycle.jsonl` |
| `bench measure` | Collect K8s (`-k8s`), Substrate (`-ate-endpoint`), Prometheus (`-prom-url`), node cache (`-sample-node-cache`), and restore timings (`-restore-log`) into `measurements.json` and `system_description.json` |
| `bench reap` | Verify zero orphaned environments remain post-run and emit `events/reap.jsonl` |
| `bench report` | Ingest `events/*.jsonl`, `traces/*.jsonl`, `train.log`, `measurements.json`, and `system_description.json` into `report.json` and `report.md` |
| `bench validate` | Validate `report.json`, `measurements.json`, and `system_description.json` against strict `v1` schemas, and with `-folder` the archive folder name |
| `bench upload` | Validate, then copy `$ARTIFACT_DIR` to `gs://<bucket>/<folder>/` |
| `bench compare` | Diff two or more `report.json` files or artifact directories side-by-side |

### 1. Synthetic / Scenario-Driven Run

```bash
ARTIFACT_DIR=/tmp/bench-run
go run ./cmd/benchmarking/bench run \
  -scenario benchmarking/scenarios/multi-job-swebench.yaml \
  -dest "$ARTIFACT_DIR"
go run ./cmd/benchmarking/bench validate -dest "$ARTIFACT_DIR"
```

A scenario whose warm footing rests on an image placement pre-flight names
it, and `bench run` puts the coverage into the bundle as
`preflight-coverage.json` instead of the synthetic `prewarm` event it writes
for a warm footing otherwise; the report's `validity.evidence` names it. The
coverage path is relative to the scenario file; a cold arm of an overlay run
gets no coverage; when `plan` is given too, the coverage must name it:

```yaml
footing: warm
preflight:
  plan: image-placement.json # what swebench-loader run -placement -preflight ran with
  coverage: runs/pf/preflight-coverage.json # what that pre-flight wrote, next to this file
```

### 2. Real RL Training Run (`examples/*-verl`)

```bash
ARTIFACT_DIR=/tmp/rl-bench-run
mkdir -p "$ARTIFACT_DIR/measurements_snapshots"

# 1. Spin up Substrate WorkerPool (0 -> N ateworker pods) & capture pre-run node image-cache snapshot
go run ./cmd/benchmarking/bench spinup -namespace ate-env -workerpool default-template-workerpool -replicas 32 -dest "$ARTIFACT_DIR"
go run ./cmd/benchmarking/bench measure -sample-node-cache "$ARTIFACT_DIR/cache_before.prom"

# 2. Launch the RL recipe (with ATE_ENV_TRACE_DIR=/workspace/results/traces enabled in the Job)
(cd examples/Qwen3-1.7B-verl && ./deploy.sh up)

# 3. Sample live K8s + Substrate state while training steps are active
kubectl -n ate-env port-forward deploy/ate-env-api 18443:8443 &
go run ./cmd/benchmarking/bench measure -k8s -ate-endpoint 127.0.0.1:18443 \
  -atespace ate-env -dest "$ARTIFACT_DIR"

# 4. After training completes, copy train.log + traces/*.jsonl into $ARTIFACT_DIR, then reap, sample post-cache, & spin down WorkerPool (N -> 0)
go run ./cmd/benchmarking/bench reap -ate-endpoint 127.0.0.1:18443 -atespace ate-env -prefix verl- -dest "$ARTIFACT_DIR"
go run ./cmd/benchmarking/bench measure -sample-node-cache "$ARTIFACT_DIR/cache_after.prom"
benchmarking/swebench/harvest-restore-timings.sh <run-id> "$ARTIFACT_DIR/restore_timings.jsonl"
go run ./cmd/benchmarking/bench spindown -namespace ate-env -workerpool default-template-workerpool -dest "$ARTIFACT_DIR"

# 5. Generate and validate report.json + report.md
go run ./cmd/benchmarking/bench report -dest "$ARTIFACT_DIR"
go run ./cmd/benchmarking/bench validate -dest "$ARTIFACT_DIR"
```

### 3. Archiving Results & Further Guides

Archive completed `$ARTIFACT_DIR` directories to object storage under a
consistent folder name, with `_traced` iff the run captured an SDK trace
(`report.json` `trace_captured: true`; traced latencies include trace overhead):

```text
gs://<YOUR_BENCHMARK_BUCKET>/<YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>/
gs://<YOUR_BENCHMARK_BUCKET>/<YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>_traced/
```

- **Telemetry scopes & feature debug plugins**: [`internal/benchmarking/TELEMETRY_GUIDE.md`](../../../internal/benchmarking/TELEMETRY_GUIDE.md)
- **Agent integration & execution playbook**: [`benchmarking/SKILL.md`](../../../benchmarking/SKILL.md)

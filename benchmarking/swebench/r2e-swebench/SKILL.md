---
name: r2e-swebench-simulated-benchmark
description: >-
  Configures and runs a simulated mixed R2E-Gym and SWE-bench Verified benchmark on Agent Substrate
  and ate-env across configurable concurrency, image counts, replica rollouts per image, warm-pool
  orchestration strategies (none, naive, sliding, pipelined, all), and API/SDK client chains (go,
  go-direct, python, all), verifies per-arm and side-by-side comparison artifacts, and populates
  BENCHMARK_REPORT.md from REPORT_TEMPLATE.md.
---

# Mixed R2E-Gym and SWE-bench Simulated Benchmark

Use this skill when asked to run a simulated RL load mixing R2E-Gym and SWE-bench Verified task
images on Agent Substrate without GPUs or model servers.
[`../run-r2e-swebench.sh`](../run-r2e-swebench.sh) drives two concurrent shards (`job=r2e` and
`job=swe`) for each selected `(client, strategy)` arm into `$ARTIFACT_DIR`, builds `bench report`
bundles, emits cross-arm comparison tables when sweeping multiple arms, and feeds the narrative
report template in [`./REPORT_TEMPLATE.md`](./REPORT_TEMPLATE.md).

For the 8 cluster checks that affect every `swebench-loader` measurement, read
[`../SKILL.md`](../SKILL.md). For real RL trainer hooks and `bench` overlays, read
[`../../SKILL.md`](../../SKILL.md). For event definitions and run shapes, read
[`../README.md`](../README.md).

## 1. Choose parameters, strategies, and client chains

Run the 8 cluster checks in [`../SKILL.md`](../SKILL.md) before starting. Ensure the Substrate
`WorkerPool` satisfies `replicas * per_worker_actor_capacity >= MAX_CONCURRENCY`, with CPU and
memory headroom when `--workload graded` runs concurrent `pytest` suites.

Pick the sizing, strategy, and client-chain knobs with CLI flags or environment variables (CLI flags
override environment variables):

| Parameter             | CLI flags                                                                                                                                                            | Environment variables                                                                                                              | Default and resolution rule                                                                                                                  |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Concurrency           | `-c N`, `--concurrency N`, `--r2e-width N`, `--swe-width N`                                                                                                          | `MAX_CONCURRENCY`, `R2E_WIDTH`, `SWE_WIDTH`                                                                                        | `200` total active sandboxes split proportionally across `r2e` and `swe` shards (`>= 2` when split).                                         |
| Images                | `-i N`, `--images N`, `--r2e-images N`, `--swe-images N`                                                                                                             | `IMAGES`, `R2E_IMAGES`, `SWE_IMAGES`                                                                                               | `20` per dataset (`40` total images). Pass `--images 10` for `20` total (`10` R2E + `10` SWE-bench).                                         |
| Rollouts per image    | `-r N`, `--rollouts-per-image N`, `--r2e-rollouts N`, `--swe-rollouts N`                                                                                             | `ROLLOUTS_PER_IMAGE`, `R2E_FANOUT`, `SWE_FANOUT`                                                                                   | Auto-computed as `ceil(shard_width / shard_images)` when omitted (`5` for `width=100, images=20`).                                           |
| Strategy              | `--strategy none\|naive\|sliding\|pipelined\|all\|CSV`, `--window-size N`                                                                                            | `STRATEGY`, `WINDOW_SIZE`                                                                                                          | Unset runs legacy standing-pool `MODE`; `all` sweeps `none,naive,sliding,pipelined`.                                                         |
| Client / API chain    | `--client go\|go-direct\|python\|all\|CSV` (alias `--chain`)                                                                                                         | `CLIENT`                                                                                                                           | `go` (`swebench-loader` -> Go SDK -> `ate-env-api`). `all` sweeps `go` and `python` (plus `go-direct` when `--control-addr` is set).         |
| Direct Substrate auth | `--control-addr ADDR`, `--router-addr ADDR`, `--atenet-client-cert P`, `--atenet-ca-file P`, `--atenet-server-name S`, `--ateapi-token-file P`, `--ateapi-ca-file P` | `CONTROL_ADDR`, `ROUTER_ADDR`, `ATENET_CLIENT_CERT`, `ATENET_CA_FILE`, `ATENET_SERVER_NAME`, `ATEAPI_TOKEN_FILE`, `ATEAPI_CA_FILE` | Required for `--client go-direct` (`--control-addr` points at `ateapi` gRPC `host:port`; `--ateapi-ca-file` verifies `ateapi` TLS).          |
| Workload and schedule | `--workload graded\|io`, `--mode steps\|eval\|open-loop`, `--steps N`, `--park POLICY`, `--step-pause DUR`                                                           | `WORKLOAD`, `MODE`, `STEPS`, `PARK`, `STEP_PAUSE`                                                                                  | `WORKLOAD=graded`, `MODE=steps`, `STEPS=4` (`1` single-pass when `--strategy` or `--client python` is set), `PARK=suspend`, `STEP_PAUSE=0s`. |
| Telemetry & traces    | `--collect-telemetry`, `--measure-interval SEC`, `--ate-endpoint ADDR`, `--prom-url URL`, `--preflight-coverage PATH`, `--footing warm\|cold`, `--trace`, `--otlp-endpoint ADDR` | `COLLECT_TELEMETRY`, `MEASURE_INTERVAL_SEC`, `ATE_ENDPOINT`, `PROM_URL`, `PREFLIGHT_COVERAGE`, `FOOTING`, `ENABLE_TRACING`, `OTEL_EXPORTER_OTLP_ENDPOINT` | `--collect-telemetry` (implied by `--gcs-bucket`) enables node-cache sampling, `atelet` restore harvesting, 60s `measurements_snapshots/`, `bench report -dest`, and `bench validate`. `--trace` records `traces/{r2e,swe}.jsonl` (`python`) or exports OTLP spans (`go`). |
| GCS archive upload    | `--validate-bundle`, `--gcs-bucket BUCKET`, `--gcs-folder FOLDER`, `--gcs-nickname NAME`, `--gcs-dry-run`                                                            | `VALIDATE_BUNDLE`, `GCS_BENCHMARK_BUCKET`, `GCS_BENCHMARK_FOLDER`, `GCS_BENCHMARK_NICKNAME`, `GCS_DRY_RUN`                         | Validates each arm bundle and uploads to `gs://<BUCKET>/<YYYY-MM-DD>_simulated-w<C>_r2e<R>-swe<S>_<nick>[_traced]/` via `bench upload` (never hardcode bucket names). |

[`../run-r2e-swebench.sh`](../run-r2e-swebench.sh) resolves sizing combinations with three rules:

1. **Concurrency set (or defaulted) without `--rollouts-per-image`**: splits `MAX_CONCURRENCY`
   across both shards and sets each shard's replica rollouts per image to
   `ceil(shard_width / shard_images)`.
2. **`--rollouts-per-image` set without `--concurrency`**: sets each shard's active width to
   `shard_images * shard_rollouts` and sets `MAX_CONCURRENCY` to `R2E_WIDTH + SWE_WIDTH`.
3. **Both `--concurrency` and `--rollouts-per-image` set**: caps concurrent active sandboxes to
   `shard_width` while keeping `shard_images * shard_rollouts` replica rollouts per shard. When
   `--strategy` is unset and `shard_images * shard_rollouts > shard_width` in `steps` mode,
   [`../run-r2e-swebench.sh`](../run-r2e-swebench.sh) adds `-park-each` (requiring `--park suspend`
   or `--park delete` and `--step-pause 0s`) so each sandbox parks right after its episode and awake
   sandboxes stay within `--concurrency`.

Select the warm-pool orchestration strategy with `--strategy`:

- `none`: creates `1` sandbox on demand per rollout, executes the trajectory, and deletes it (`0%`
  warm hit rate).
- `naive`: prewarms `1` suspended sandbox per rollout across every selected image upfront before
  claiming (`100%` warm hit rate, `images * rollouts` peak parked footprint).
- `sliding`: warms `window_size` distinct images (`DefaultWindowSize` =
  `clamp(ceil(images/2), 1, 8)` when `--window-size 0`), runs all rollouts in that window, deletes
  those sandboxes, and advances to the next window (`window_size * rollouts` bounded parked
  footprint).
- `pipelined`: double-buffers two windows, prefetching window `N+1` in the background while window
  `N` executes (`<= 2 * window_size * rollouts` parked footprint, sharing the shard `Width` active
  concurrency cap across prefetch and replay).
- `all`: runs `none`, `naive`, `sliding`, and `pipelined` sequentially and compares them side by
  side.

Select the client and API chain with `--client` (or `--chain`):

- `go`: `swebench-loader` -> Go SDK (`clients/go` `*env.Client`) -> `ate-env-api` -> Substrate.
- `go-direct`: `swebench-loader -backend=substrate -access=direct` -> `DirectSubstrateBackend`
  (`internal/ate.Client`) -> Substrate `ateapi` and `atenet-router`, bypassing `ate-env-api`.
- `python`: [`../run_fleet_bench.py`](../run_fleet_bench.py) (fed rendered trajectory plans from
  `swebench-loader render-plan`) -> Python `ate_env_rl` `SandboxFleet` (`SubstrateBackendDriver` +
  `SubstrateEnvClientRuntime`) -> `ate-env-api` -> Substrate.
- `all`: sweeps `go` and `python`, and includes `go-direct` automatically when `--control-addr` (or
  `CONTROL_ADDR`) is set.

Select the trajectory profile with `--workload`:

- `graded`: replays [`../r2e-gold.yaml`](../r2e-gold.yaml) on `job=r2e` and `swebench-gold` on
  `job=swe`. Both trajectories run `explore` and `read_files` steps, simulated think pauses (`1s`,
  `2s`, `3s`), gold patch application, `pytest` evaluation, and reward verification.
- `io`: replays `IO_TRAJECTORY` (default `workload-file-read`) on both shards to measure image pull,
  snapshot restore, and filesystem read latency without running `pytest`.

## 2. Build digest-pinned manifests outside the repository

Write manifests and run outputs outside the git checkout so `ko build` reads a clean working tree:

```bash
export ENV_REPO=/path/to/sandboxenv-rl
export PROJECT=YOUR_GCP_PROJECT
export REGION=us-central1
export DOCKERHUB_REMOTE="${REGION}-docker.pkg.dev/${PROJECT}/dockerhub-remote"
export SWEBENCH_REPO="${REGION}-docker.pkg.dev/${PROJECT}/swebench/swebench-verified"
export MANIFEST_DIR=/tmp/r2eswe-manifests
mkdir -p "${MANIFEST_DIR}"
```

Set `R2E_LIMIT` to at least your chosen `--r2e-images` count. Build the digest-pinned R2E-Gym
`CommandAdapter` manifest with [`../make-r2e-dataset.py`](../make-r2e-dataset.py) and
[`../../../integrations/uni-agent/scripts/resolve_images.sh`](../../../integrations/uni-agent/scripts/resolve_images.sh).
The first pass lists every candidate image, not the first `R2E_LIMIT`: the manifest pass skips a tag
whose digest an earlier tag already has, so it reads past the first `R2E_LIMIT` candidates, and
every tag it reads must be in `r2e-images.json`. Set `MODE=hubapi` on `resolve_images.sh` to resolve
thousands of tags in minutes; the mapping serves any later `R2E_LIMIT`.

```bash
R2E_LIMIT=20

"${ENV_REPO}/benchmarking/swebench/make-r2e-dataset.py" \
  --source r2e_gym_easy \
  --images-out "${MANIFEST_DIR}/r2e-images.txt"

"${ENV_REPO}/integrations/uni-agent/scripts/resolve_images.sh" \
  "${MANIFEST_DIR}/r2e-images.txt" \
  "${DOCKERHUB_REMOTE}" \
  "${MANIFEST_DIR}/r2e-images.json"

"${ENV_REPO}/benchmarking/swebench/make-r2e-dataset.py" \
  --source r2e_gym_easy \
  --limit "${R2E_LIMIT}" \
  --image-digests "${MANIFEST_DIR}/r2e-images.json" \
  --out "${MANIFEST_DIR}/r2e.jsonl"
```

Set `MODE=mirror` on `resolve_images.sh` when copying into a standard Artifact Registry repository
instead of a Docker Hub remote repository.

Build the SWE-bench Verified manifest with [`../make-dataset.py`](../make-dataset.py). Request extra
rows above `--swe-images` (for example `SWE_IMAGES + 10`, up to `500`) because
[`../offline-exclusions.txt`](../offline-exclusions.txt) removes instances whose tests require
internet access:

```bash
SWE_LIMIT=30

"${ENV_REPO}/benchmarking/swebench/make-dataset.py" \
  --registry "${SWEBENCH_REPO}" \
  --limit "${SWE_LIMIT}" \
  --out "${MANIFEST_DIR}/swebench.jsonl"
```

## 3. Validate the plan offline (`dry-run`)

Run `dry-run` before touching the cluster to verify manifest schemas, rendered Go and Python
trajectories, arm matrix expansion, shard widths, and the 63-character actor name limit:

```bash
"${ENV_REPO}/benchmarking/swebench/run-r2e-swebench.sh" dry-run \
  --r2e-dataset "${MANIFEST_DIR}/r2e.jsonl" \
  --swe-dataset "${MANIFEST_DIR}/swebench.jsonl" \
  --concurrency 200 \
  --images 20 \
  --rollouts-per-image 5 \
  --client all \
  --strategy all \
  --artifact-dir /tmp/r2eswe-dry
```

Confirm stderr prints the expected `resolved plan:` line (listing every `<client>-<strategy>` arm)
and two `"msg":"plan"` JSON lines per arm with exit code `0`.

## 4. Prewarm and run on the cluster

Run `prewarm` once per cluster or new image slice before the timed run. `prewarm` launches `1`
`workload-file-read-one` probe per selected image, waits for every derived `ActorTemplate` golden
snapshot bake to complete, warms node caches, and writes prewarm events under
`$ARTIFACT_DIR/prewarm/` outside the timed `events/` window:

```bash
export KO_DOCKER_REPO="${REGION}-docker.pkg.dev/${PROJECT}/bench"
export ARTIFACT_DIR=/tmp/r2eswe-run

"${ENV_REPO}/benchmarking/swebench/run-r2e-swebench.sh" prewarm \
  --r2e-dataset "${MANIFEST_DIR}/r2e.jsonl" \
  --swe-dataset "${MANIFEST_DIR}/swebench.jsonl" \
  --concurrency 200 \
  --images 20 \
  --rollouts-per-image 5 \
  --artifact-dir "${ARTIFACT_DIR}"
```

Execute a single-arm run or a multi-arm strategy and client sweep in-cluster:

```bash
# Sweep all 4 strategies across both Go API and ate-env-api + Python SandboxFleet:
SAMPLE_CACHE=1 HARVEST_RESTORES=1 \
"${ENV_REPO}/benchmarking/swebench/run-r2e-swebench.sh" run \
  --r2e-dataset "${MANIFEST_DIR}/r2e.jsonl" \
  --swe-dataset "${MANIFEST_DIR}/swebench.jsonl" \
  --concurrency 200 \
  --images 20 \
  --rollouts-per-image 5 \
  --client all \
  --strategy all \
  --artifact-dir "${ARTIFACT_DIR}"
```

Common parameter variations:

- **Sweep all 4 strategies on the Go API chain only**: pass `--client go --strategy all`.
- **Test the `ate-env-api` + Python `ate_env_rl` `SandboxFleet` chain on `pipelined`**: pass
  `--client python --strategy pipelined --window-size 6`.
- **Include the direct Go Substrate chain (`go-direct`)**: pass
  `--client go-direct --control-addr api.ate-system.svc.cluster.local:443 --atenet-client-cert /path/to/bundle.pem --atenet-ca-file /path/to/ca.pem --ateapi-token-file /path/to/token`
  (or combine `--client all --control-addr ... --strategy all` to sweep all 12 `(client, strategy)`
  arms). `in-cluster` mode mounts direct credentials via an ephemeral Kubernetes `Secret`.
- **20 total images (`10` R2E + `10` SWE-bench) at `200` concurrency (`10` rollouts per image)**:
  pass `--concurrency 200 --images 10 --rollouts-per-image 10`.
- **Rootfs I/O probe without `pytest`**: pass `--workload io --mode eval`.
- **Local port-forward execution**: omit `KO_DOCKER_REPO` (or pass `--execution local`) with
  `ENDPOINT=localhost:7777` (pass `--auth-token-file` when `ate-env-api` requires bearer auth, and
  `--python-bin /path/to/venv/bin/python3` when the host `python3` lacks `grpcio>=1.84` and
  `protobuf>=7.35`). Override `BASE_TEMPLATE` and `--api-service` when the cluster uses non-default
  template or `ate-env-api` Service names.

## 5. Verify outputs in `$ARTIFACT_DIR`

For a single `(client, strategy)` run, outputs land directly under `$ARTIFACT_DIR/`. For multi-arm
sweeps (`NUM_ARMS > 1`), per-arm outputs land under `$ARTIFACT_DIR/arms/<client>-<strategy>/` and
top-level comparison artifacts land at `$ARTIFACT_DIR/`:

- `events/r2e.jsonl` and `events/swe.jsonl` (or `arms/<arm>/events/*.jsonl`): raw operation streams
  stamped with `job`, `worker`, `client`, and `arm`.
- `r2e-summary.json` and `swe-summary.json` (or `arms/<arm>/*-summary.json`): per-shard
  `swebench-loader report` summaries.
- `report.json` and `report.md` (or `arms/<arm>/report.{json,md}`): unified `bench report` bundle.
  On `--workload graded`, verify `failed == 0` and `reward_rate == 1.0`. Check
  `working_set_hit_rate` (`0.0` on `none`, `1.0` on `naive`/`sliding`/`pipelined`),
  `standing_pool_size`, `peak_active_sandboxes`, `jobs.r2e`, `jobs.swe`, and `interference`.
- `comparison.txt`, `comparison.json`, and `comparison.md` (multi-arm sweeps): side-by-side latency,
  throughput (`tasks/min`), warm-pool hit rate, peak active sandboxes, and standing-pool footprint
  across all `(client, strategy)` arms.
- `system_description.json`, `measurements.json`, `measurements_snapshots/meas_*.json`, and
  `run_window.json` (or `arms/<arm>/...`): periodic in-flight and post-arm `bench measure -k8s`
  telemetry rolled up and folded into `report.json` by `bench report -dest`, plus `events/reap.jsonl`
  when `--ate-endpoint` is set on a `go` or `go-direct` arm (a `python` arm writes its own `reap`
  event into `events/{swe,r2e}.jsonl` instead) and `preflight-coverage.json` when
  `--preflight-coverage` is passed.
- `cache_before.prom`, `cache_after.prom`, `atelet_restores.jsonl`, and `traces/*.jsonl`: node cache
  counters and raw `atelet` restore records (`go` and `go-direct` arms) joined into both the
  `*-summary.json` files and `report.json` (`restores`), plus `v1` `TraceEvent` JSONL streams when
  `--trace` runs on `--client python` (which marks `trace_captured: true` and appends `_traced` to
  the `bench upload` GCS folder name).

## 6. Write the benchmark report

Copy [`./REPORT_TEMPLATE.md`](./REPORT_TEMPLATE.md) to `$ARTIFACT_DIR/BENCHMARK_REPORT.md` so
`report.md` and `comparison.md` stay intact, then fill every section:

1. **Capture `system_description.json` and cluster / version specs (`## 1`)**: the runner runs
   `bench measure -k8s` into each arm directory before `bench report`, so `system_description.json`
   and `measurements.json` sit next to `report.json`. Combine them with the pre-run checks in
   [`../SKILL.md`](../SKILL.md) to record the cluster spec (`1.1`), node pool spec (`1.2`: machine
   type, CPU family, boot disk, labels, and taints), and `ate` (Substrate) and `ate-env`
   (`sandboxenv-rl`) versions and settings (`1.3`). To measure again by hand, into one arm's
   directory (`$ARTIFACT_DIR` for a single arm):
   ```bash
   go -C "${ENV_REPO}" run ./cmd/benchmarking/bench measure -k8s \
     -atespace "${ATESPACE:-ate-env}" \
     -run-id "${RUN_ID:-r2eswe}" \
     -dest "${ARTIFACT_DIR}/arms/<client>-<strategy>"
   ```
2. **Populate the headline summary + diagrams and workload parameters
   (`## Headline Outcome & Time Attribution Summary` and `## 2`)**: Lead the headline summary with a
   first **`What ran`** bullet (prewarmer loader such as `swebench-loader plan-placement` +
   `preflight` or `bench prewarm`, images × nodes, golden/cache footing, plus the strategies tested
   in `swebench-loader` / `run_fleet_bench.py` via the `sandboxenv-rl` SDK, concurrency, rollouts,
   and reward rate), followed by a concise **`Key numbers`** bullet with single summed `p50 / p90`
   values (`Wall clock` across arm, `r2e`/`swe` shards, active/calendar sweep, and upfront prewarm;
   `Max concurrency observed` via `steps[].active_sandboxes`; **`TTFI` (`cmd started`)** `p50 / p90`
   from sandbox request -> container restored & 1st cmd started inside the guest; **`TTFC`
   (`cmd complete`)** `p50 / p90` including `baseline` execution; and **`Time to restore`**
   `p50 / p90` via `restores.{first,wakes}.phases[total]`), leaving the component drill-down
   (`handle_claim`, `prewarm`, demand-wake routing, `atelet` sub-phases, and per-window barrier
   idle) to Sections `3.1`–`3.4`; include the end-to-end execution-layer `flowchart LR` diagram and
   grouped `xychart-beta` multi-arm time histograms (`Y-axis = Time`,
   `X-axis = Test Strategy / Category` across startup lifecycle, `TTFC`, and shard/arm wall clock)
   directly inside `## Headline Outcome & Time Attribution Summary`, and populate the concurrency
   and workload parameters table in `## 2`.
3. **Populate results, findings, and exact commands executed (`## 3`, `## 4`, and `## 5`)**: Fill
   the strategy multi-arm table (`3.1`) from `comparison.json` / `comparison.md`, per-arm
   `report.json`, and `{r2e,swe}-summary.json` (with explicit `ms` vs `s` units, arm + `r2e`/`swe`
   shard `Wall (s)`, and per-shard `r2e`/`swe` `TTFC` [`cmd complete`]), decompose where time is
   spent by execution layer (`3.2`: first-command `TTFC`, single-rollout `task`, and arm/sweep
   `Wall (s)` across slot-busy work vs straggler/window-barrier idle), fill all raw sandbox
   lifecycle and command latencies across `p50`, `p90`, `p95`, `p99`, and `max` (`3.3`:
   `sdk_slot_wait`, `actor_create`, `sandbox_create`, `prewarm`, `handle_claim`
   [`wire_claim_to_ready_ms`], `ttfi` [`cmd started`: sandbox requested -> container restored & 1st
   cmd started], `first_cmd_exec` [`ready_to_first_cmd_ms`], `ttfc` [`cmd complete` via
   `stats[event="ttfi"]`], `suspend`, `wake` [`cmd complete` via `stats[event="wake"]`], `step/*`,
   `task/*`, `delete`, `step_barrier`, `run` wall clock), fill step barrier, shard wall clock, and
   shard split (`3.4`, plus node cache and `atelet` restore phases when `SAMPLE_CACHE=1` or
   `HARVEST_RESTORES=1` ran), summarize failures, findings, and caveats (`## 4`), and record the
   exact prewarm, benchmark, and measurement commands that actually ran at the bottom (`## 5`).

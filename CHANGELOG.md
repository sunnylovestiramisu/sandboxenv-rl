# Changelog

User-visible changes. GitHub release notes are generated from the merged pull
requests (see [docs/release.md](docs/release.md)); this file collects what a
user needs to know across them.

## Unreleased

### Added
- `run-in-cluster.sh` bakes a `-placement` plan into the loader image next to
  the dataset (under the plan's own file name, so the run's `placement` label
  tells plans apart) and rewrites the flag to the baked path; relative paths
  resolve against the caller's directory.
- Go client: `clients/go/README.md` covers the Go client API and operations in
  the Python README's section order, with compiled examples in
  `clients/go/example_test.go` (`go test` runs `ExampleNewClient`, which needs
  no server).
- guest: `Process.timed_out` (`optional bool`, set on every exited process)
  is true when the guest killed the process at `StartProcessRequest.timeout`
  or its default timeout, and false for any other exit, so an OOM kill at the
  deadline no longer reads as a timeout. The Go client (`ShellResponse.TimedOut`),
  the Python client (`ProcessInfo.timed_out`, `ShellResult.timed_out`,
  `ExecResult.timed_out`) and the swebench loader use it, and fall back to the
  exit-137 heuristic only for a guest that predates the field.
- Go client: `ClientOptions.MetricsRegisterer` registers the Prometheus
  histograms `ate_env_client_exec_seconds{status}`,
  `ate_env_client_suspend_seconds{status}` and
  `ate_env_client_ready_to_first_command_seconds{status,source}`
  (`source="direct"`, shared `0.05`–`600` s buckets), idempotently per
  registerer (importing `clients/go` now links
  `github.com/prometheus/client_golang/prometheus`). `Env.StartProcess` emits a
  `<TraceServiceName>.start_process` span (`Env.Run` keeps `exec` without
  nesting `start_process`), the first `Run` or `StartProcess` on an `Env` sets
  `ate_env.first_command=true` (`AttrFirstCommand`), `exec` sets
  `ate_env.timed_out` (`AttrTimedOut`), and `WriteFile` / `ReadFile` record
  `ate_env.file.size` only when the transfer completes without error, matching
  `ate_env_rl.Env`.
- `swebench-loader report` and `bench report`: `fault_classification` in
  `report.json` (top level and per job), in `stats.jsonl` (an
  `infra_fault_rate` measurement on the trial row and a `fault_classification`
  row) and in `comparison.md` (an `Infra Fault Rate` column). Each failed
  rollout lands in one of three tiers: infrastructure before the episode
  (`create_timeout`, `no_capacity`, `image_pull_unpack`, `transport_error`,
  `other`), policy during it (`nonzero_exit`, `command_timeout`, `guest_oom`,
  left out of the rate) and infrastructure during it (`wake_resume_failure`,
  `sandbox_crash`, `snapshot_restore_failure`, `eviction_preemption`,
  `other`), with the faults a retry recovered and one example error per
  category; an `other` row is an error no matcher names yet.
  `infra_fault_rate` is the infrastructure drops over all rollouts, held
  against a `target_rate` of 2%. A report written before the field exists
  has no classification and the renderers say so. `swebench-loader report
  -thresholds` takes `max_infra_fault_rate`, which fails the comparison at
  or above the rate, and when the candidate has no classification.
- ate-env-api: `-wake-state-read-cap` (default 5 s) bounds each control-plane
  state read during a wake; 0 or less means the default.
- Go client: end-to-end suites in `clients/go/e2e_test.go`, gated by
  `ATE_ENV_GUEST_TARGET` (`TestE2EGuestDaemon`, the 24 guest-daemon cases of
  `test_guest_daemon.py`, run in `ci-go`) and `ATE_ENV_API_TARGET`
  (`TestE2EFullStack`, the 5 client cases of `test_full_stack.py`, run in
  `e2e-kind`).
- Go client: `env.DefaultAtespace`. The client no longer imports `internal/ate`, and with it
  `internal/routertls` and Substrate's API package (`go.ate.dev/substrate/api/ateapi/v1`);
  `hack/verify/go-client-deps.sh` keeps it that way in the Go CI job.
- Go client: a guest call (a process or file call) that failed because the
  environment cannot start returns a `*env.StartError` with the `Reason`
  (`env.StartFailureCrashed`, `env.StartFailureNoCapacity`), the status `Code`
  and the `EnvID`, from the `ate-env-start-failure` trailer ate-env-api sets,
  as the Python client's `SandboxStartError`. It is read on unary and streaming
  calls (the `Recv` of `Process.Output` included), on the client's own
  connection and on a `GRPCConn`. `env.Retryable(err)` reports whether the work
  can succeed on a fresh environment, with the Python client's table. New
  sentinels `env.ErrInvalidArgument`, `env.ErrPermissionDenied` and
  `env.ErrFailedPrecondition` (an `ErrProcessExited` error also wraps
  `ErrFailedPrecondition`).
- Python client: `SweBenchSource` (a SWE-bench-style Hugging Face dataset,
  default `R2E-Gym/SWE-Bench-Verified`; reads only the `limit`/`offset` slice
  into memory, and an `offset` past the end reads no tasks; `name=` picks a
  config; `keep_row=True` keeps the row under `metadata["ds"]`; needs `pip
  install datasets`, imported on `load()`), `ImageListSource` (an image
  manifest, one task per image) and `read_bytes()`. `JsonlSource` and
  `ImageListSource` read `gs://` objects through `google-cloud-storage` when
  installed, else `gcloud storage cat` or `gsutil cat`. No new dependency.
- Python client: task sources and dataset-row parsing. `load_tasks()` and
  `add_tasks()` (and their `AsyncSandboxFleet` forms) take `image_field=` and
  `id_field=` (a dotted path, several paths tried in order, or a callable), or
  a `TaskSource`: `ListSource` and `JsonlSource` (a UTF-8 JSON-lines file,
  local or `gs://`; a bad row names its file and line). By default a row's image is read
  from `image`, then the R2E-Gym and SWE-bench keys (`docker_image`,
  `image_name`, under `metadata` and `env_config.entry` too), and its id from
  `task_id`, `id` or `instance_id`, so SWE-bench rows load without mapping. An
  id is never made up from a row's position. `extract_image`, `row_to_task`,
  `to_tasks` and `FieldSpec` are public. What changes for an existing dict
  row is under Changed.
- `ate-env manifest template --sidecar-env NAME=VALUE`: a variable that only
  the `--sidecar` processes get. The container carries it as
  `ATE_SIDECAR_ENV_NAME`; the guest takes it out of its own environment before
  it starts anything and gives `NAME` to the sidecars only, thus the commands
  that ate-env-api starts do not inherit it (`internal/sidecarenv`). Needs a
  guest image with this change: the template also carries the guest flag
  `-sidecar-env-required`, thus an older guest exits at start instead of
  running the sidecars without the values. The OpenHands base template passes
  `OH_SESSION_API_KEYS_0` this way; a base registered with the key in `--env`,
  and each template derived from it, must be deleted and registered again
  (`RECREATE=1` in `examples/openhands/demo.sh`).
- ate-env-api: `ListPlacementDomains` (the worker pools a worker selector can
  address: labels, nodes, workers, slots per node, from Substrate's workers;
  with a `template`, only the pools its worker selector matches) and
  `WarmImage` (server-streaming: puts one `rootfs_image` on every node of one
  domain with probe environments it creates, resumes, holds until `RUNNING`,
  reads the node of and deletes, round after round; a probe event each, then
  the coverage). Features `placement_domains`, `warm_image`. `ate-env
  placement-domains`, `ate-env warm-image <image> --worker-selector k=v`;
  `ate-env manifest access-policy --global` prints the global viewer binding
  listing workers takes; Go client `ListPlacementDomains`, `WarmImage`; Python
  `list_placement_domains()`, `warm_image()`, `PlacementDomain`, `WarmProbe`,
  `WarmImageCoverage`; an `ImagePlacement` plan's domains are `PlacementDomain`
  too, thus a plan can carry what `list_placement_domains()` returns. A
  `nodes` above the distinct nodes of the workers the selector matches is
  `InvalidArgument` when the server can list them, the per-node cap
  multiplies the count the workers report for every call on the selector,
  and only the missing global viewer binding leaves a request's `nodes`
  trusted. A probe's create and resume are retried within the
  probe's timeout on the transient failures of a loaded control plane
  (`Unavailable`, `DeadlineExceeded`, a derived template not visible yet),
  its delete on `Aborted`, `Unavailable` and `ResourceExhausted`; the probes
  a call could not delete (a `CRASHED` one cannot be, today) are listed in
  the coverage's `undeleted_probes` for a sweep, which `ate-env warm-image`
  prints and exits non-zero on. `ate-env-api -warm-max-probes-per-node`
  (default 2) caps the probes in flight per node of one worker selector
  across calls; a request's `probes_per_node` above it is refused, and the
  coverage's `waited` is the time its rounds waited for slots.

- `benchmarking/swebench/run-r2e-swebench.sh`: the mixed R2E-Gym + SWE-bench
  recipe, with `make-r2e-dataset.py` (R2E-Gym `CommandAdapter` manifests),
  `run_fleet_bench.py` (the Python `SandboxFleet` arm) and the `r2e-swebench`
  skill and `REPORT_TEMPLATE.md`; `swebench-loader run -strategy none|naive|sliding|pipelined` and
  `-window-size`, `swebench-loader render-plan`, `swebench-loader report -md`
  over several runs (the arm table), and the `-atenet-*` and `-ateapi-*`
  flags of the direct Substrate backend.
- `run-r2e-swebench.sh --collect-telemetry` (`COLLECT_TELEMETRY`): each arm
  scrapes the node image caches before and after, harvests the `atelet`
  restore records, takes a `bench measure -k8s` snapshot every
  `--measure-interval` seconds (`MEASURE_INTERVAL_SEC`, default 60) and ends
  in `bench report -dest`, which folds it all into one `v1` bundle;
  `--ate-endpoint`, `--prom-url`, `--preflight-coverage` and `--footing` feed
  the reap, the measurements and the validity rules. `--validate-bundle` runs
  `bench validate` on each bundle; `--gcs-bucket` (`GCS_BENCHMARK_BUCKET`,
  never in the repository), `--gcs-folder`, `--gcs-nickname` and
  `--gcs-dry-run` upload it with `bench upload`. `--trace` traces the loader
  (`-enable-tracing` on the Go arms, `traces/{r2e,swe}.jsonl` on `--client
  python`) and `--otlp-endpoint` names the collector. `run-in-cluster.sh`
  takes the same as environment variables (`COLLECT_TELEMETRY`,
  `ARTIFACT_DIR`, `MEASURE_INTERVAL_SEC`, `SAMPLE_CACHE`, `ATE_ENDPOINT`,
  `PROM_URL`, `PREFLIGHT_COVERAGE`, `FOOTING`, `ENABLE_TRACING`,
  `VALIDATE_BUNDLE`, `GCS_BENCHMARK_BUCKET`, `GCS_BENCHMARK_FOLDER`,
  `GCS_BENCHMARK_NICKNAME`, `GCS_DRY_RUN`), and `LOADER_NODEPOOL` /
  `LOADER_TOLERATION_KEY` to pin the loader Job to a node pool and tolerate
  its taint.
- `run_fleet_bench.py --enable-tracing` and `--trace-out PATH`: the Python arm
  points the client's trace recorder at `traces/<job>.jsonl` next to its
  events, one `v1` `TraceEvent` line per sandbox operation; a bundle with a
  trace uploads to a `_traced` folder.
- `bench report -dest` joins the raw `atelet_restores.jsonl` into `restores`
  and writes `restore_timings.jsonl` from it, so `bench validate` and the
  upload carry the samples. When shards share one bundle (`r2e` and `swe`),
  the run tags merge, `load_*` totals and the dataset tag
  `r2e-dataset+swe-dataset`, instead of keeping the last shard's, and a
  dataset tag loses its `/var/run/ko/` prefix. `bench reap -prefix run-` writes `run` as the run id of
  `events/reap.jsonl`, and `bench upload` copies the bundle's files into
  `gs://<bucket>/<folder>/` itself, not into a `<folder>/<dir>/` subdirectory.
- swebench-loader: `run -delete-rate` caps the environment deletes per second
  through ate-env-api (default 200; 0 removes the cap), and a delete the
  bucket rate-limits (429, `ResourceExhausted`) is retried with backoff for
  up to two minutes instead of failing. The run records the cap as the
  `delete_rate` label, so `report -arm` keeps runs with different caps apart.
- `swebench-loader run -placement image-placement.json`: each create carries
  the worker selector of one of its image's domains, round-robin per image
  (`EnvBackend` `WithPlacement`); `report -preflight-coverage` feeds the
  pre-flight's result to Validity Rule 1, which takes a complete pre-flight
  as prewarm evidence on warm footing and fails on an incomplete one.
- `swebench-loader preflight`, the PreflightWarmer: from an image placement
  plan, ensures each image's derived template, then per domain, as each
  golden becomes ready, probes every node of the domain with the image
  (creates placed by the domain's worker selector, node read back) until
  each node has run it, `-image-concurrency` images at once;
  `preflight-events.jsonl` and `preflight-coverage.json`. `EnvBackend` gains
  `CreateWith(CreateOptions{WorkerSelector})`, `Node`, `ImageStatus` and
  `EnsureImageTemplate` (the `ImageWarmer` interface); `domains.json` takes
  an optional `node_slots` per domain.
- ate-env-api: `GetImageStatus` and `EnsureImageTemplate`, the ActorTemplate
  derived for a `rootfs_image` and the state of its golden snapshot (`NONE`,
  `PENDING`, `READY` with the tag, `FAILED` with the reason); ensure creates
  the template, which starts the bake, with no environment. Feature
  `image_status`; `ate-env image-status <image> [--ensure]` (`image-status` joins
  the reserved environment ids); the Python
  client's `image_status()` and `ImageStatus`; the Go client's `ImageStatus`
  and `EnsureImageTemplate`.
- `examples/Qwen3.5-397B-A17B-verl`: the agentic GRPO recipe from 2 to 16 GPU
  nodes with profiles for Qwen3.5-35B-A3B and Qwen3.5-397B-A17B, steps of
  thousands of trajectories, and guarded verl patches for 64K responses.
- uni-agent provider `ate_env`: `start_wait_s` (wait for sandbox capacity
  instead of failing), `startup_concurrency` (a job-wide start-up gate through
  one Ray actor per job, which also reclaims orphaned environments) and
  `stop_retry_s` (retry deletes while the actor is mid-operation).
- `uni_agent_substrate.datasets --shuffle-seed` for datasets sorted by
  repository; `resolve_images.sh` `MODE=hubapi` (Docker Hub tags API, no
  pulls), `MODE=arapi` (Artifact Registry listing) and `MIRROR_NAME_SEP`.
- Python client: `FleetConfig.node_selector` is sent as every environment's
  `worker_selector`, and `FleetConfig.placement` adds labels per task image;
  `SubstrateBackendDriver(worker_selector=, placement=)` and
  `worker_selector_for(image)`.
- Python client: `ate_env_rl.PreflightWarmer` fills the node caches an image
  placement plan (`ImagePlacement`, from `image-placement.json`) describes:
  ensures each image's derived template, then per domain, as each golden
  becomes ready, probes every node with the image and deletes the probes;
  `PreflightCoverage` writes `preflight-coverage.json` in the loader's schema.
  `Observer.record_phase()` and the phases `preflight_golden`,
  `preflight_probe`.
- `swebench-loader plan-placement`: an image placement plan
  (`image-placement.json`) that assigns each task image to placement domains
  (worker pools a per-environment worker selector addresses) under each
  node's image-cache budget, largest image first, with replication and a hot
  rule from the schedule's demand; `domains.json` describes the domains.
- ate-env-api: `worker_selector` on `CreateEnvironment`, labels the worker pool
  must carry, ANDed with the template's own selector (`Actor.worker_selector`,
  so it holds for wakes too); `node` and `worker_selector` on `Environment`.
  `ate-env create --worker-selector key=value`, `ate-env get <id>` (`get` joins
  the reserved environment ids);
  `worker_selector=` on the Python client's `create()` and `node`,
  `worker_selector` on `EnvironmentInfo`.
- Observability across the stack, all off by default except the run report;
  see [docs/observability.md](docs/observability.md).
- Python client: OpenTelemetry tracing behind the `tracing` extra
  (`TracerConfig`, `FleetConfig.tracing`, `ObservabilityConfig.enable_tracing`).
  `ate_env_client.*` and `ate_env_fleet.*` spans, W3C `traceparent` on every gRPC
  call, context carried across `LoopRunner` threads and `process_parallel`.
- Python client: `ate_env_rl.observability`. `fleet.report` is a `RunReport`
  (per-phase count, total and max, claims, task outcomes, warm replicas) with
  `summary()`, `to_dict()` and `write(report_dir)`; `ate_env_fleet_*` and
  `ate_env_client_*` Prometheus series behind the `metrics` extra, served by
  `serve_metrics(port)`.
- ate-env-api: `--enable-tracing` (OTLP over `OTEL_EXPORTER_OTLP_*`, trace
  continued to ateapi and the guest) and `--metrics-listen-addr` (off by
  default, for example `:9090`) serving the `ate_env_api_*` families documented in
  [docs/metrics.md](docs/metrics.md).
- `ate-env manifest`: `--enable-tracing`, `--otlp-endpoint`, `--otlp-insecure`
  and `--metrics-port`.
- ate-env-guest: `--enable-tracing` and `--enable-metrics`
  (`ate_env_guest_process_duration_seconds`).
- Go client: `ClientOptions.TracerProvider` and `TraceServiceName`,
  `NewTracerProvider`, the `Attr*` span attribute keys, and `Client.Get`.
- Go client: `ShellRequest.Poll` runs a command without holding a stream
  through the router: `Env.Run` starts the process, polls it (50 ms doubling
  to 2 s, ten consecutive `Unavailable` or `DeadlineExceeded` polls
  tolerated), reads the output once after the exit, and kills the process if
  it returns first. `Poll` is off by default, and `Run` streams as before.
  `ShellResponse.Duration` and `TimedOut` (exit 137 at or past `Timeout`, the
  Python client's heuristic), in both modes.
- Go client: `ClientOptions.Connections` dials that many connections (0 and 1
  mean one). `Create` and `Env` hand them out round robin, and an `Env` sends
  every call on the connection it got; the other `Client` calls round robin.
  `ClientOptions.DialOptions` adds gRPC dial options to every connection the
  client dials, after its own, as the Python client's `channel_options`. Both
  are errors with `GRPCConn` (`Connections` above one). `Close` closes every
  connection the client dialed. The swebench-loader's `-connections` uses
  `Connections`.
- Go client: `Env.Atespace`, `Env.Info`, `Process.Env` and `Process.Info`, as
  the Python client's `Env.atespace`, `Env.info()`, `Process.env` and
  `Process.info()`.
- swebench-loader: `run -enable-tracing` records every event as a span under one
  `swebench-loader.run` span; the JSONL is unchanged.
- `examples/grpo` and the NeMo Gym rollout benchmark record their phases with
  the client's `Observer` and write the run report to `REPORT_DIR`.
  `examples/grpo/report.py` also reads that report; it keeps reading the old
  per-event JSONL for one more release.
- `benchmarking/telemetry_report`: builds a self-contained HTML report from run
  reports, OTLP/JSON spans and Prometheus or collector metrics.
- Release publishing for `ate-env-rl`: `clients/python` derives its version
  from the Git tag (`setuptools-scm`) and is published to PyPI via OIDC Trusted
  Publishing when a GitHub Release is published; `hack/release/create-release.py`
  drafts release notes and cuts draft releases (see [docs/release.md](docs/release.md)).

### Changed

- Go client: every error keeps the gRPC status in its chain, thus
  `status.Code(err)` now reports `codes.NotFound` for an `env.ErrNotFound`
  error, and `codes.FailedPrecondition` for an `env.ErrProcessExited` error,
  instead of `codes.Unknown`. The `Recv` of `Process.Output` returns the SDK's
  errors instead of the raw status error; `status.Code(err)` still reads it.
- Releases after v0.1.0 no longer build or publish the OpenHands agent-server
  layer (`oh-agent-server-layer`). Build it from `integrations/openhands/layer`
  and push it to your own registry (see `integrations/openhands/README.md`).
  The v0.1.0 image (`oh-agent-server-layer:1.55.0-v0.1.0`) gets no updates: it
  predates the #267 security re-lock. An org admin can delete the package.
- Built against Substrate `274605f1` (2026-11-05), from `475f49b0`. The
  control plane API is unchanged for ate-env (`MintActorCertificate`, which
  ate-env never called, is gone). Three runtime changes reach a template.
  Since `5d226a50` the container runs as the image's `USER` in its `WORKDIR`
  (before: root in `/`); `ate-env-guest` sets no user, so a `--rootfs-image`
  with a non-root `USER` runs the guest, the processes it starts and its
  sidecars as that user, and gVisor snapshots of such templates from before
  the upgrade do not restore: recreate them. Since `fd576d34` `/tmp` and
  `/dev/shm` are a `tmpfs` on both runtimes (`/dev/shm` 64 MiB on gVisor,
  `/tmp` capped at half the actor's memory) and `/run` comes from the image;
  files an image ships under `/tmp` are hidden, and gVisor snapshots from
  before it, goldens included, do not restore. Since `17fa8586`
  `securityContext.capabilities.add` accepts 12 capabilities only; ate-env
  templates set none. The Kind e2e installs Substrate with
  `ate-setup --kind deploy ate-system`, since Substrate removed
  `hack/install-ate-kind.sh`.
- `ate-env manifest template`: `--snapshots-bucket` is optional. Without it
  the template names no `snapshotConfig.storageLocation` and its snapshots go
  to the cluster's `--snapshot-location`. With it, ate-api-server needs the
  `PerTemplateSnapshotLocation` preview gate and the template must be created
  with `kubectl-ate --preview`; ate-env-api sends the same opt-in when it
  derives a template from such a base. Substrate since `d86746d8` rejects the
  field without the gate, so a base template created earlier with a bucket
  must be recreated without one on a cluster that has the gate off.
- Python client: renamed distribution package from `ate-env-client` to
  `ate-env-rl` and import module from `ate_env` to `ate_env_rl`.
- Python client: `warm_pool` pools the actors it created before it raises
  `SandboxStartError`. An overlapping `warm_pool(wait=True)` counts the actors
  another call is still creating and can return before the pool is full.
- Python client: `unwarm_pool`, and a `resize_pool` that shrinks the pool,
  cancel `warm_pool` calls in flight for that pool: those calls delete the
  actors they create instead of pooling them.
- Python client: `load_tasks()` and `add_tasks()` read a dict row as follows.
  The id changes rename a row that loaded before, so `acquire()` by id and
  stored results see the new id.
  - Only `instance_id`: loads with that id (was `ValueError`).
  - No image: `ValueError` (was `KeyError('image')`).
  - Image under `docker_image`, `image_name`, or those keys under `metadata`
    or `env_config.entry`: loads (was `KeyError('image')`).
  - Image with surrounding whitespace: stripped.
  - Blank `image`: the next key is tried (was the image `""`).
  - Non-string image, such as `123`: `ValueError` (was `Task.image == 123`).
  - One-element list id: unwrapped, `["one"]` gives `"one"` (was
    `"['one']"`).
  - Bool id: refused (was `"True"`).
  - Dict or multi-element list id: skipped for the next key (was its
    `str()`).
  - Blank or non-string `template`: no template (was the value).
  - Whitespace-only or binary id: skipped for the next key (was the value);
    a UTF-8 bytes id is decoded (was `"b'...'"`).

### Fixed
- `swebench-loader preflight` and `ate_env_rl.PreflightWarmer` no longer delete an
  environment whose name a probe collided with: a create the server refused
  (`AlreadyExists`, a bad argument, a missing template, no permission, no
  room) made nothing and is left alone; only a create that was cut off is
  deleted by id. The default run id is `pf-<6 random characters>`, unique per
  pass, as `WarmImage`'s is, instead of the fixed `pf`.

- swebench-loader: a step the guest kills at its timeout (exit 137 at or
  past the deadline) fails the task and stops the trajectory, as
  `run_fleet_bench.py` does on its `TimeoutError`. Before, the Go arm marked
  the step ok and ran on, so the two arms disagreed on `failed`.
- `integrations/nemo-gym`: `exec(user=...)` no longer raises `ValueError`.
  NeMo Gym `main` sends `user` on many commands (the Harbor environment
  server's default user, `user="root"` from the transfer helpers), and each
  one failed. The provider now probes the image once per sandbox and runs the
  command through `sudo -n -u <user>` when the image's user has a passwordless
  sudo, or through `su` when the image runs as root; a user that is the
  image's own runs as is. An image with neither, one whose sudo wants a
  password, and a uid under `su` return code 125 with
  `error_type="user_switch_unavailable"`, which Gym's transfer helpers answer
  by running as the default user.
- ate-env-api: `WarmImage` deletes a probe whose create the control plane
  commits after the probe's delete (a create attempt cut off by its deadline,
  a cancel or `Unavailable` while the control plane queues, even when a later
  attempt made the probe). Before, such a probe stayed `SUSPENDED`, untracked
  and missing from `undeleted_probes`. The delete of such a probe is now
  repeated for a short settle window, and each round lists the atespace once
  before it gives its probe slots back and deletes any of its probes that
  appeared later. When that list fails, those probes are listed in
  `undeleted_probes`. A create that the control plane commits after the
  settle window and the round's list still leaks and is not listed. A round
  with such a probe holds its slots about 2 s longer, and a cancelled call
  returns about 2 s later.
- OpenHands layer: security re-lock of the agent-server dependencies for the
  open Dependabot alerts: pillow 13.0.0, pypdf 6.21.4, pyasn1 0.6.5, certifi
  2026.10.23, fastmcp and fastmcp-slim 3.4.11, oauthlib 4.0.1, setuptools
  84.2.1. filelock stays at 3.32.7, because each stable litellm from 1.104
  requires filelock<4.0. `lock.sh` now starts from the committed lock and
  passes its arguments to `uv pip compile`.
- ate-env-api port proxy: both forms forward the path exactly as sent when it
  holds bytes such as `{ } | ^` or raw UTF-8. They were re-escaped, and an
  escaped slash next to them was decoded: `/a{b%2Fc` reached the daemon as
  `/a%7Bb/c`. The path form now matches its routing prefix on the path as sent,
  so an escaped slash in the prefix no longer routes to an environment.
- Python client: `RunReport.to_dict()` keeps every phase's
  `0 <= max_s <= total_s <= count * max_s`. It rounded `total_s` and `max_s`
  to 0.01 separately, so 3 phases of 0.004 s reported `total_s` 0.01 and
  `max_s` 0.0. `max_s` now rounds up to the next 0.01 and `total_s` stays
  between `max_s` and `count * max_s`, which can leave it one float step off
  the 0.01 grid (5 x 0.09 s gives 0.44999999999999996). `summary()` prints
  the same rounded values (#273).
- swebench-loader `-park-each`: the last step deletes each environment right
  after its replay instead of suspending it (unless `-keep`). A 37,000
  environment run suspended them all, one more snapshot upload each. The
  cleanup then deleted them in one burst at about 1,850 per second; 15,031
  deletes failed on GCS 429 and leaked.
- swebench-loader: `-park suspend-async` suspends locally through ate-env-api
  (`SuspendLocal`: the call returns once the snapshot is on the node and the
  upload goes on in the background), as it already did through direct
  Substrate access. It sent a durable suspend and waited for the upload: p50
  0.8 s, p95 9 s, max 46.5 s over 180,000 suspends in one run. `-park-each`
  parks with `suspend-sync` and `suspend-async`, which it silently skipped,
  and rejects `fork`. A run that parks with `suspend-async` or `fork` (whose
  park is a local suspend too) checks that ate-env-api serves `suspend_mode`
  before it starts. `report` takes `park_mode` from the run's `park` label,
  so a `-park-each` run, which records no `park` event, reports
  `suspend-async` or `suspend-sync` instead of `suspend`.
- ate-env-api: deriving an image template from a base that already has a
  non-image volume named `guest`, or a mount at or under `/ate`, fails with
  `InvalidArgument` naming the clash, instead of creating a template that
  Substrate rejects for a duplicate volume or nested mounts.
- ate-env-api: deriving an image template from a base that already mounts
  the guest image volume fails with `InvalidArgument` when another volume is
  mounted at or under `/ate` or the guest is not mounted exactly once at
  `/ate`, instead of creating a template whose guest is not alone at `/ate`.
  It also fails when the base's command does not start the guest from the
  mount (no command, or a first argument that is not the guest binary under
  `/ate`, such as `/ko-app/ate-env-guest`), instead of creating a template
  whose guest is not found at boot, and when its guest volume's image is not
  pinned by digest, as a base that runs the guest as its image already must
  be.
- ate-env-api: a guest call that waits for its environment to wake keeps
  waiting through a transient control-plane error on the state read
  (`Unavailable`, `DeadlineExceeded`, or `ResourceExhausted` such as a quota)
  instead of failing at once with it. A slow state read no longer holds the
  wait past `-wake-budget`, and no longer delays the probe that ends a
  no-capacity run past `-wake-no-capacity-grace`. A state read that hangs
  without an error is cut at `-wake-state-read-cap`, and the wait probes again
  instead of sitting with no probe until `-wake-budget`. When all reads are
  cut, the wait probes once per `-wake-state-read-cap` plus backoff and ends at
  `-wake-budget` with `Unavailable` "while UNKNOWN".
- ate-env-api: a guest call whose caller context cancels or expires while its
  wake probe is parked at the router returns `Canceled` or `DeadlineExceeded`
  instead of treating the unwoken environment as awake and recording
  `create_to_ready` and `wake_wait` as ready.
- ate-env-api port proxy: the path form
  (`/v1alpha/envs/<id>/ports/<port>/...`) forwards the rest of the path
  exactly as sent; it re-escaped `! ' ( ) *` (`/report(1).txt` reached the
  daemon as `/report%281%29.txt`) when the path held no other escape.
- ate-env-api: a `CreateEnvironment` whose derived image template was created
  by a concurrent call for another image (two digests sharing their first 12
  hex digits) fails with `FailedPrecondition` instead of running that image.
- ate-env-api: a base template whose guest command climbs out with `..`
  (`/../ko-app/ate-env-guest`) is re-rooted under `/ate` instead of deriving
  templates whose guest is not found at boot.
- Python client: environments no longer outlive `teardown()`. A recycle,
  `warm_pool` call or failed warm claim that overlaps a teardown deletes its
  environment instead of pooling it, and the warm-replica gauge stays equal to
  the warm pool. An environment whose delete fails or is cancelled stays
  owned, and the next `reap` of any run deletes it. An environment that an `acquire`
  creates while its own run's teardown runs is also deleted by the next `reap`
  of any run, unless the caller releases it first.
- Python client: an `acquire` cancelled while its environment's create is in
  flight, for example by its own `timeout_s`, no longer leaks the environment
  if the create reached Substrate. The driver keeps it, and the next `reap` of
  any run deletes it; if a teardown ran during the create, the driver starts
  its delete in a background task; a teardown that already runs does not wait
  for it.

# Changelog

User-visible changes. GitHub release notes are generated from the merged pull
requests (see [docs/release.md](docs/release.md)); this file collects what a
user needs to know across them.

## Unreleased

### Added
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
  probe's create and resume are retried within the
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

- Python client: renamed distribution package from `ate-env-client` to
  `ate-env-rl` and import module from `ate_env` to `ate_env_rl`.
- Python client: `warm_pool` pools the actors it created before it raises
  `SandboxStartError`. An overlapping `warm_pool(wait=True)` counts the actors
  another call is still creating and can return before the pool is full.
- Python client: `unwarm_pool`, and a `resize_pool` that shrinks the pool,
  cancel `warm_pool` calls in flight for that pool: those calls delete the
  actors they create instead of pooling them.

### Fixed

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

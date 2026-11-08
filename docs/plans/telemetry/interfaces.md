# Interface contracts: telemetry for sandboxenv-rl

Status: describes `main` at `3b03b03` (2026-10-29). The first version of this file (2026-10-21) is
the contract the developer started from; [decisions.md](./decisions.md) lists each name that changed
between it and the code. Where a planned name differs from the shipped one, the planned name is in
the Notes column.

## Python: `ate_env_rl.telemetry.tracing` (#93, #164)

| Name                                                                                       | Signature or shape                                                                                                                                                                      | Behavior                                                                                                                                                                                                                                                                                      |
| ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CLIENT_SPAN_PREFIX`                                                                       | `"ate_env_client"`                                                                                                                                                                      | Span names are `ate_env_client.<op>` whatever `trace_service_name` is; the service name sets the resource `service.name` only.                                                                                                                                                                |
| `initialize_tracer(service_name: str) -> None`                                             |                                                                                                                                                                                         | Builds a `TracerProvider` with a `BatchSpanProcessor` and the OTLP/gRPC exporter when an application has not installed one and `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (`OTLP_ENDPOINT_ENV_VARS`) is set. Logs one ERROR and returns when the extra is missing. |
| `start_span(...)`                                                                          | context manager                                                                                                                                                                         | Starts `ate_env_client.<op>`; a no-op when tracing is off. Notes: the plan had `trace_span` and `async_trace_span` decorators.                                                                                                                                                                |
| `TracerManager`                                                                            |                                                                                                                                                                                         | Resolves the tracer at span time so an application provider installed before or after the client wins.                                                                                                                                                                                        |
| `create_tracer_manager(config: TracerConfig) -> tuple[TracerManager \| None, Any \| None]` |                                                                                                                                                                                         | `(None, None)` when disabled or the extra is absent.                                                                                                                                                                                                                                          |
| `tracer_from_config(config: TracerConfig \| None)`                                         |                                                                                                                                                                                         | The tracer for a config, or `None`.                                                                                                                                                                                                                                                           |
| `inject_metadata(metadata) -> list[tuple[str, str]]`                                       |                                                                                                                                                                                         | Appends the W3C carrier (`traceparent`, `tracestate`) of the current context. Never baggage.                                                                                                                                                                                                  |
| `TraceContextInterceptor` and the four call-kind classes                                   | `UnaryUnaryTraceContextInterceptor`, `UnaryStreamTraceContextInterceptor`, `StreamUnaryTraceContextInterceptor`, `StreamStreamTraceContextInterceptor`; factory `_trace_interceptors()` | Added to the `grpc.aio` channel after the auth interceptors when tracing is on.                                                                                                                                                                                                               |
| `rpc_attrs(service, method, target) -> dict`                                               |                                                                                                                                                                                         | otelgrpc's keys: `rpc.system.name = "grpc"`, `rpc.method = "<service>/<method>"`, `server.address`, `server.port`. `OTEL_SEMCONV_STABILITY_OPT_IN=rpc/old` gives `rpc.system`, `rpc.service` and the short `rpc.method`; `rpc/dup` gives both sets.                                           |
| `set_attributes`, `image_ref_attr`, `executable_attr`, `mark_error`                        | helpers                                                                                                                                                                                 | `executable_attr` keeps the executable name only (never the command line); `image_ref_attr` drops an image tag and keeps the repository and digest.                                                                                                                                           |
| context across threads                                                                     |                                                                                                                                                                                         | `LoopRunner.run` and `submit` run the coroutine in a copy of the caller's `contextvars` context; `process_parallel` copies the context into its pool threads. Notes: the plan had a `run_with_context` helper.                                                                                |

Without the extra, nothing in `ate_env_rl.telemetry` imports OpenTelemetry at module load; the
module resolves names lazily (`_load_otel`, module `__getattr__`). Notes: the plan had
`OPENTELEMETRY_AVAILABLE` and `Mock*` classes.

### Config (`ate_env_rl.config`)

Frozen dataclasses with the existing unknown-field rejection:

```python
@dataclass(frozen=True)
class TracerConfig:
    enable_tracing: bool = False
    trace_service_name: str = "ate-env-client"

@dataclass(frozen=True)
class ObservabilityConfig:
    enable_metrics: bool | None = None   # None: on when the metrics extra is installed, else one INFO; True warns when it is missing
    enable_tracing: bool = False         # ate_env_fleet.<phase> spans and client tracing under trace_service_name
    trace_service_name: str = "ate-env-fleet"

# FleetConfig fields
tracing: TracerConfig = field(default_factory=TracerConfig)
observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
```

`FleetConfig` accepts a dataclass or a plain dict for both; `to_dict()` returns them as plain dicts.
`Client(..., tracer_config=TracerConfig(...))` traces a client used on its own. When both configs
enable tracing, `ObservabilityConfig.trace_service_name` wins. Notes: the plan had
`enable_metrics=True`.

### Span names and attributes

| Emitter                  | Span                                                                            | Attributes                                                                                                                                          |
| ------------------------ | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Client`                 | `ate_env_client.create_environment`                                             | `ate.actor.name`, `ate.atespace`, `ate.template.name`, `ate.template.atespace`, `ate_env.rootfs_image`; `rpc.*`, `server.address`, `server.port`    |
| `Client`                 | `ate_env_client.get_environment`, `.suspend_environment`, `.delete_environment` | `ate.actor.name`, `ate.atespace`; `rpc.*`, `server.address`, `server.port`                                                                          |
| `Client`                 | `ate_env_client.get_server_info`                                                | `rpc.*`, `server.address`, `server.port`                                                                                                            |
| `SandboxHandle`          | `ate_env_client.exec`                                                           | `ate.actor.name`, `ate.atespace`, `ate_env.run_id`, `ate_env.command.executable`, `ate_env.first_command`, `ate_env.exit_code`, `ate_env.timed_out` |
| `SandboxHandle`          | `ate_env_client.write_file`, `.read_file`                                       | `ate.actor.name`, `ate.atespace`, `ate_env.run_id`, `ate_env.file.path`, `ate_env.file.size`                                                        |
| `Env`                    | `ate_env_client.shell`, `.start_process`                                        | `ate.actor.name`, `ate.atespace`, `ate_env.command.executable`, `ate_env.first_command`; `ate_env.exit_code` on `shell`                             |
| `Env`                    | `ate_env_client.write_file`, `.read_file`                                       | `ate.actor.name`, `ate.atespace`, `ate_env.file.path`, `ate_env.file.size`                                                                          |
| `SubstrateBackendDriver` | `ate_env_client.ensure_template`                                                | `ate.template.name`                                                                                                                                 |
| `SubstrateBackendDriver` | `ate_env_client.acquire`                                                        | `ate.template.name`, `ate_env.run_id`; `ate.actor.name` once claimed                                                                                |
| `SubstrateBackendDriver` | `ate_env_client.warm_pool`                                                      | `ate.template.name`, `ate_env.replicas`, `ate_env.run_id`                                                                                           |
| `SubstrateBackendDriver` | `ate_env_client.release`                                                        | `ate.actor.name`; `error.type` when the delete fails                                                                                                |
| `SubstrateBackendDriver` | `ate_env_client.reap`                                                           | `ate_env.run_id`                                                                                                                                    |
| `Observer`               | `ate_env_fleet.<phase>`, `ate_env_fleet.run`                                    | `strategy`, `backend`; `family` on phase spans                                                                                                      |

Substrate identities use Substrate's `ateattr` keys so spans join Substrate's logs and traces
without a mapping; client-only fields use `ate_env.*`. `rpc.*` follows otelgrpc 0.72
(`rpc.system.name`, full `rpc.method`; #164). The mock driver records no spans. A warm claim's
`get_environment` span is the wake; the trace recorder labels it `wake`, but there is no
`ate_env_client.wake` span. The trace recorder still accepts `ate_env.env_id` as a fallback key; the
client does not set it. Notes: the plan had an `env.*` vocabulary and `atefleet.*` spans.

Rule: no command line, file content, token or image tag in any attribute value beyond the executable
name, sizes, references and ids.

## Python: `ate_env_rl.observability` (#109, #113, #115)

| Name                                                         | Contract                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `FLEET_SPAN_PREFIX`                                          | `"ate_env_fleet"`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `RunReport`                                                  | fields `strategy, total_s, phases, claims, tasks_ok, tasks_err, release_errors, warm_total, peak_warm, environment`; `add_phase(name, dur)` clamps a negative duration to 0 (so does `Observer.task_done`); `add_task(status)`, `add_claim`, `add_release_error`; `to_dict()` keys `strategy, total_s, phases{name: {count, total_s, max_s}}, claims, tasks_ok, tasks_err, release_errors, warm_replicas_total, warm_replicas_peak, environment`; `summary()` rows in `ORDER` then `TOTAL`; `write(report_dir, n_tasks=None)` writes `<strategy>_<n>tasks_<YYYYMMDD-HHMMSS>.txt` and `.json` and never overwrites |
| `RunReport.ORDER`                                            | `preflight, plan, ensure_template, warm_pool, acquire, process, recycle, release, reap, teardown`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `Observer(config=None, *, registry=None)`                    | `.phase(name, *, backend="-", family="-")` context manager (status `ok`, `error` or `cancelled`), `.run(strategy, *, backend="-")` context manager yielding the `RunReport` (one run at a time per `Observer`; an overlapping run raises `RuntimeError`), `.task_done(backend, family, status, seconds)`, `.claim(backend, status="ok")`, `.release_failed(backend)`, `.warm_add(backend, n)`, `.warm_remove(backend, n)`, `.warm_reset()`, `.flush_step_events(...)`                                                                                                                                             |
| `null_observer()`                                            | an `Observer` that records nothing, for callers that opt out                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `repo_family(task_or_image) -> str`                          | `metadata["repo"]` tail, else the SWE-bench image convention, else `unknown`; at most `MAX_FAMILIES` (64) distinct values per process, then `other`; a repo named `other` or `unknown` becomes `other_repo` or `unknown_repo`                                                                                                                                                                                                                                                                                                                                                                                     |
| `serve_metrics(port=9095, addr="0.0.0.0", *, registry=None)` | `prometheus_client.start_http_server`; raises `RuntimeError` without the extra; a busy port raises `OSError`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| hooks                                                        | `SandboxFleet` and `AsyncSandboxFleet` wrap `preflight`, `plan`, `ensure_template`, `warm_pool`, `acquire`, `release`, `recycle`, `teardown`; `process_parallel` wraps `process_fn` in `phase("process")` and calls `task_done`; the Substrate and mock drivers call `claim`, `warm_add` and `warm_remove` and record the sub-phases `slot_wait` and `wire_claim`; only the Substrate driver calls `release_failed`; after `run`, `fleet.report` holds the `RunReport`                                                                                                                                            |

### Prometheus families, trainer process

| Family                                                                                                           | Type      | Labels                                     | Buckets                                                                 |
| ---------------------------------------------------------------------------------------------------------------- | --------- | ------------------------------------------ | ----------------------------------------------------------------------- |
| `ate_env_fleet_phase_latency_seconds`                                                                            | Histogram | `phase, backend, family, strategy, status` | `SEC_BUCKETS`: 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600 |
| `ate_env_fleet_task_latency_seconds`                                                                             | Histogram | `strategy, backend, family, status`        | same                                                                    |
| `ate_env_fleet_run_latency_seconds`                                                                              | Histogram | `strategy, status`                         | same                                                                    |
| `ate_env_fleet_claims_total`                                                                                     | Counter   | `backend, status`                          |                                                                         |
| `ate_env_fleet_tasks_total`                                                                                      | Counter   | `strategy, status`                         |                                                                         |
| `ate_env_fleet_release_failures_total`                                                                           | Counter   | `backend`                                  |                                                                         |
| `ate_env_fleet_warm_replicas`                                                                                    | Gauge     | `backend`                                  |                                                                         |
| `ate_env_client_acquire_seconds`, `_exec_seconds`, `_suspend_seconds`, `_warm_claim_seconds`, `_release_seconds` | Histogram | `status`                                   | same                                                                    |
| `ate_env_client_slot_wait_seconds`, `_wire_claim_seconds`, `_ready_to_first_command_seconds`                     | Histogram | `status, source`                           | same                                                                    |

`backend` is `substrate` or `mock`. `status` is `ok`, `error` or `cancelled`. `source` is
`warm_pool`, `cold_create`, `mock` or `direct`. `family` comes from `repo_family`. Registration
happens on the first metrics-enabled `Observer` or on `enable_client_metrics()`, through an
idempotent `_metric` helper keyed by registry; never at import. Notes: the plan had `atefleet_*` and
an `ate_env_client_wake_seconds` histogram.

`ate_env_rl.telemetry.metrics` also exports `observe(op, seconds, status)`, `observe_subphase`,
`timed(op)`, `render_prometheus()`, `get_recorded_summary()`, `reset_recorded_metrics()` and
`flush_step_events(...)`; the last three serve the benchmark harness.

## Go: `internal/telemetry` (#114, #150)

| Name                                                                   | Contract                                                                                                                                                                                                                                |
| ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SetupOTel(ctx, serviceName string) (func(), error)`                   | OTLP/gRPC exporter from `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`; an `http://` endpoint also means plaintext) and `OTEL_EXPORTER_OTLP_INSECURE`; resource `service.name`; W3C propagator only; returns the shutdown func |
| `Untraced(ctx) context.Context`                                        | a context whose calls start no span; used for the scrape-time `ListActors`                                                                                                                                                              |
| `Metrics`, `NewMetrics()`                                              | the `ate_env_api_*` families on a private registry (no Go runtime or process collectors)                                                                                                                                                |
| `GuestMetrics`, `NewGuestMetrics()`                                    | `ate_env_guest_process_duration_seconds` on a private registry                                                                                                                                                                          |
| `Definition`, `Definitions`                                            | the metric definitions `hack/metrics-docs` renders into `docs/metrics.md`                                                                                                                                                               |
| `RequestedAtHeader`                                                    | `"x-ate-env-requested-at"`; a client may send it (RFC 3339) on `CreateEnvironment`; the server uses it as the start of `create_to_ready` when present, else its receipt time                                                            |
| `EnvironmentSource`, `EnvironmentCount`, `DefaultEnvironmentsCacheTTL` | the control-plane read behind `ate_env_api_environments`, cached for 60 s                                                                                                                                                               |
| server wiring                                                          | `grpc.StatsHandler(otelgrpc.NewServerHandler())` when `-enable-tracing`; `internal/ate/client.go` adds `otelgrpc.NewClientHandler()` to the `ateapi` connection and to guest connections, including the wake probe                      |

Notes: the plan had an `Instrumenter` interface with `NewNoOp()`; otelgrpc covers every boundary, so
none was added.

### `ate-env-api` flags

| Flag                        | Default                              | Meaning                                                                                                                                           |
| --------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-enable-tracing`           | `false`                              | export OpenTelemetry traces over OTLP/gRPC; incoming W3C context is continued                                                                     |
| `-metrics-listen-addr`      | empty (metrics off)                  | address of the Prometheus listener, for example `:9090`; a listener of its own, without authentication or Host check. Notes: the plan had `:9090` |
| `-metrics-environments-ttl` | `DefaultEnvironmentsCacheTTL` (60 s) | how long `ate_env_api_environments` serves one `ListActors` read                                                                                  |

### `ate-env-api` families

| Family                                 | Type      | Labels             |
| -------------------------------------- | --------- | ------------------ |
| `ate_env_api_requests_total`           | Counter   | `method, code`     |
| `ate_env_api_request_duration_seconds` | Histogram | `method, code`     |
| `ate_env_api_create_to_ready_seconds`  | Histogram | `atespace, status` |
| `ate_env_api_wake_wait_seconds`        | Histogram | `outcome`          |
| `ate_env_api_environments`             | Gauge     | `atespace, state`  |
| `ate_env_api_build_info`               | Gauge     | `version, commit`  |

[docs/metrics.md](../../metrics.md) is generated from `telemetry.Definitions` by
`go run ./hack/metrics-docs`; CI fails when it is stale.

### `ate-env manifest` flags

| Flag               | Renders                                                                                                                          |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `--enable-tracing` | `-enable-tracing` on `ate-env-api`                                                                                               |
| `--otlp-endpoint`  | `OTEL_EXPORTER_OTLP_ENDPOINT` on the `ate-env-api` container                                                                     |
| `--otlp-insecure`  | `OTEL_EXPORTER_OTLP_INSECURE=true`                                                                                               |
| `--metrics-port`   | `-metrics-listen-addr=:<port>`, a `metrics` container and Service port, `prometheus.io` scrape annotations; rejects the API port |

## Go client (#150)

| Name                                                                    | Contract                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ClientOptions.TracerProvider trace.TracerProvider`                     | nil means no spans; the client never creates an exporter                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `ClientOptions.TraceServiceName string`                                 | default `ate-env-go-client`; the span name prefix                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| spans                                                                   | `<TraceServiceName>.create_environment`, `.get_environment`, `.suspend_environment`, `.delete_environment`, `.get_server_info`, `.exec`, `.read_file`, `.write_file`; `Client.Get` backs `get_environment`. The five single-RPC `EnvironmentService` spans are CLIENT on a caller-supplied `GRPCConn`, with otelgrpc 0.72's `rpc.system.name`, the full `rpc.method` and `server.address`, `server.port` (`OTEL_SEMCONV_STABILITY_OPT_IN` as in Python), and INTERNAL on the client's own connection, where otelgrpc adds the CLIENT span. `exec` and the file spans are INTERNAL. Empty attribute values are not recorded. |
| `Attr*` constants                                                       | `AttrActorName` (`ate.actor.name`), `AttrAtespace`, `AttrTemplateName`, `AttrTemplateAtespace`, `AttrRootfsImage` (`ate_env.rootfs_image`, image tag dropped as in Python), `AttrCommandExecutable`, `AttrExitCode`, `AttrFilePath`, `AttrFileSize`: the Python keys for every field the Go client has. Not in Go: `ate_env.run_id` and `ate_env.first_command`, which describe the Python fleet's run and acquired sandbox, and `ate_env.timed_out`, because the guest API returns an exit status and no timeout field (the Python client infers `timed_out` from its own deadline).                                       |
| propagation                                                             | the dialed connection gets `otelgrpc.NewClientHandler()`; with a caller-supplied `GRPCConn`, each call carries the carrier in its metadata                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `NewTracerProvider(ctx, serviceName) (*sdktrace.TracerProvider, error)` | the OTLP default for callers who want one                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

## Guest (#150)

| Flag              | Meaning                                                                                                                                                                         |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-enable-tracing` | otelgrpc server handler after the unknown-field interceptors; exports over OTLP configured by `OTEL_EXPORTER_OTLP_*`                                                            |
| `-enable-metrics` | serves `ate_env_guest_process_duration_seconds{exit_class}` at `/metrics` on the guest listener, without authentication; `exit_class` in `zero`, `nonzero`, `signal`, `timeout` |

Notes: the plan had one flag for both. The stripped binary grew by 2.6 MiB.

## Benchmarks (#168)

- `internal/benchmarking/swebench/events.go`: `Recorder` takes a `trace.Tracer` through `WithTracer`
  or `SetTracer(tracer, parent)`. Each event becomes one span named after `Event.Event`, timed from
  `ts - dur_ms` to `ts`, with `run`, `step` and `task` always and `job`, `worker`, `env`,
  `instance`, `name`, `attempts`, `count`, `exit` and `reward` when set as attributes, and status
  from `OK`. `swebench-loader run -enable-tracing` calls `telemetry.SetupOTel` and opens one
  `swebench-loader.run` span. JSONL output is unchanged (`testdata/events.golden.txt`). Notes: the
  plan had an `Instrumenter` and a start/done pair per span.
- `examples/grpo` (`ENABLE_TRACING`, `METRICS_PORT`, `REPORT_DIR`) and
  `integrations/nemo-gym/benchmarks/rollout_bench.py` (`REPORT_DIR`, `--report-dir`) use the
  `Observer`; `report.py` reads the run report JSON and the old JSONL for one release.
- `benchmarking/telemetry_report/build_report.py` builds `report.html` and `run.json` from run
  reports, OTLP/JSON span files and Prometheus scrapes.
- `tests/telemetry/otel-collector.yaml`: the acceptance collector (OTLP 4317 and 4318, a
  `prometheus` receiver, OTLP/JSON files under `/data`, the `debug` exporter, an optional forward).

## Environment variables

| Variable                                                                                                                                                       | Read by                                                                                               |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_INSECURE`, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT` | every exporter: Python client, `ate-env-api`, guest, Go client's `NewTracerProvider`, swebench loader |
| `OTEL_SEMCONV_STABILITY_OPT_IN`                                                                                                                                | Python `Client` spans (`rpc/old`, `rpc/dup`)                                                          |
| `ENABLE_TRACING`, `METRICS_PORT`, `REPORT_DIR`                                                                                                                 | `examples/grpo`, `rollout_bench.py`, #186's pipeline                                                  |
| `ENV_BENCH_MODE`, `ENV_BENCH_TRACE_PATH`, `ARTIFACT_DIR`                                                                                                       | `ate_env_rl.telemetry.trace_recorder` (#173)                                                          |

## Warm-pool accounting (AC-15)

Shipped in #221 (`clients/python/src/ate_env_rl/backend/substrate.py`). `SubstrateBackendDriver`
coroutines share one event loop and interleave at every `await`; the bookkeeping holds under any
interleaving:

| Name or rule      | Contract                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| gauge equals pool | `ate_env_fleet_warm_replicas{backend}` equals the driver's warm-pool size after every step (P-9)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `_teardowns`      | an int that `reap` increments under the lock when it starts and when it ends; `_reaping` counts the reaps in progress. A recycle, `warm_pool` or failed warm claim reads `_teardowns` before its await and pools its actor only if it is unchanged and no reap is running (`_may_pool`); otherwise it deletes the actor. The actor stays owned until the delete succeeds. A failed delete (counted as a release failure) or a cancelled one re-tags it with `WARM_POOL_RUN_ID`, which every `reap` deletes, so the next reap of any run removes it. Actors a torn-down or drained `warm_pool` call made are owned under `WARM_POOL_RUN_ID` from the start. A `release` that popped an actor and did not delete it (its delete failed or was cancelled, or a recycle was cancelled in `suspend()`) owns it again through `_return_popped`: under `WARM_POOL_RUN_ID` if a reap ran since the pop, else under its run id, checked and updated in one lock hold (P-10, AC-15.9) |
| `_warm_inflight`  | `dict[(pool key, _teardowns, pool generation), int]`: actors `warm_pool` calls are still creating. A call computes `needed = replicas - len(pool) - inflight` and reserves `needed` under the lock; the reservation turns into pool entries under one lock. `reap` clears the dict (P-11)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `_pool_gens`      | a generation per pool key. `unwarm_pool`, and a `resize_pool` below what is pooled plus in flight, bump it (`_cancel_warm_calls`); a `warm_pool` call whose generation changed deletes the actors it created instead of pooling them (P-11)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| gauge on reap     | `reap` clears the pool and moves `ate_env_fleet_warm_replicas` by the same count, so a direct reap keeps gauge and pool equal without a fleet's reset (P-9)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| overlapping calls | a `warm_pool` call counts the actors other calls are still creating and does not wait for them, so it can return before the pool is full (`warm_pool_async` docstring)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |

A warm claim re-checks `_may_pool` after `get()` on both paths: if a reap ran since the pop, a
failed claim deletes the actor, and a successful one does not hand it out, deletes it if it is still
owned (a reap of another run, or one cancelled before its delete, left it alive and unpooled), and
claims or creates another. A successful claim checks `_may_pool` and takes ownership in one lock
hold, so a reap on another thread cannot run in between (AC-15.7). A claim abandoned that way
records one `wire_claim` sample with status `error`, as a failed claim does; the claim or create
that follows records its own. A cold create whose own run was reaped while it ran
(`_run_reaped_since`: a reap of that run is running, or ended after the acquire read `_teardowns`)
owns the actor under `WARM_POOL_RUN_ID`; the caller's handle keeps its run id (AC-15.10). A cold
create cancelled in flight (by `acquire`'s `timeout_s` or its caller) owns the actor under
`WARM_POOL_RUN_ID` too, since the create may have landed, and starts its delete in a background task
if a reap ran since the acquire read `_teardowns` (`_own_abandoned_create`, AC-15.2). It records one
`wire_claim` sample with status `error` and source `cold_create`, as a failed create does.

A `warm_pool` call cancelled during its `create()` calls releases its reservation and owns every
actor it sent a `create()` for under `WARM_POOL_RUN_ID`, so any run's reap deletes them; if a reap
ran since the call planned, it also starts their deletes in background tasks, since that reap may
have missed them. With `wait=True`, an actor whose `create()` succeeds and `suspend()` raises is
owned the same way and deleted through `_delete_unpooled` (AC-15.8).
[properties.md](./properties.md) and [verify/README.md](./verify/README.md) have the details.

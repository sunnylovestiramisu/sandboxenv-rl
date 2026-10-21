# Observability

ate-env has three telemetry layers, the same ones agent-sandbox has, so runs on
either stack can be read side by side:

1. **Run report**: always on, no dependency. Per-phase count, total and max for
   one fleet run, written as a table and as JSON.
2. **Prometheus metrics**: the trainer process (`atefleet_*`,
   `ate_env_client_*`), ate-env-api (`ate_env_api_*`) and the guest
   (`ate_env_guest_*`).
3. **OpenTelemetry traces**: one trace from the trainer through ate-env-api to
   the Substrate control plane and the guest, exported over OTLP.

Everything except the run report is off by default. With it off, nothing new is
imported, no exporter is created and no collector is registered.

## Enabling it

| Component | Traces | Metrics |
|---|---|---|
| Python client and fleet | `FleetConfig(observability=ObservabilityConfig(enable_tracing=True))`, or `FleetConfig(tracing=TracerConfig(enable_tracing=True))`, or `Client(..., tracer_config=TracerConfig(enable_tracing=True))`; needs the `tracing` extra | `ObservabilityConfig(enable_metrics=True)` (the default; a logged no-op without the `metrics` extra) and `ate_env.observability.serve_metrics(port)` |
| ate-env-api | `--enable-tracing` | `--metrics-listen-addr` (default `:9090`; empty disables) |
| `ate-env manifest` | `--enable-tracing`, `--otlp-endpoint`, `--otlp-insecure` | `--metrics-port` (adds a `metrics` container and Service port and `prometheus.io` scrape annotations) |
| ate-env-guest | `--enable-tracing` in the ActorTemplate's guest arguments | `--enable-metrics` (serves `/metrics` on the guest listener) |
| Go client (`clients/go`) | `ClientOptions.TracerProvider` (any `trace.TracerProvider`; `env.NewTracerProvider(ctx, name)` is the OTLP default) and `ClientOptions.TraceServiceName` (default `ate-env-go-client`) | none |
| swebench-loader | `swebench-loader run -enable-tracing` | none (the JSONL events are its record) |

Install the Python extras with:

```bash
pip install './clients/python[tracing,metrics]'
```

Every exporter is configured by the standard OpenTelemetry variables:

| Variable | Meaning |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC collector, default `localhost:4317`; an `http://` endpoint also means plaintext |
| `OTEL_EXPORTER_OTLP_INSECURE` | `true` for a plaintext connection |
| `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`, ... | the rest of the [OTLP exporter settings](https://opentelemetry.io/docs/specs/otel/protocol/exporter/) |

The `service.name` of each process is fixed: `ate-env-fleet` (or the
`trace_service_name` you set), `ate-env-client` for a client traced on its own,
`ate-env-api`, `ate-env-guest`, `ate-env-go-client` and `swebench-loader`. A
Python process that already installed an SDK `TracerProvider` keeps it; the
client adopts it instead of creating an OTLP exporter.

Context crosses process boundaries as W3C Trace Context (`traceparent`,
`tracestate`) in gRPC metadata. Baggage is never sent.

A deployment with everything on:

```bash
ate-env manifest --enable-tracing \
  --otlp-endpoint http://otel-collector.ate-env-telemetry:4317 --otlp-insecure \
  --metrics-port 9090 ... | kubectl apply -f -
```

## What nests under what

One `fleet.run(...)` is one trace. The fleet's phase spans (`atefleet.*`) hold
the client's spans (`ate_env_client.*`), the client's gRPC calls carry the
trace to ate-env-api, whose otelgrpc server span continues it and whose own
calls continue it to ateapi (`ateapi.Control/*`) and, for process and file
calls, to the guest:

```
atefleet.run                                          ate-env-fleet
  atefleet.acquire
    ate_env_client.acquire
      ate_env_client.create_environment
        ateenv.v1alpha.EnvironmentService/CreateEnvironment     ate-env-api (server)
          ateapi.Control/CreateActor                            ate-env-api (client)
            ateapi.Control/CreateActor                          ateapi, when Substrate exports
  atefleet.process
    ate_env_client.exec
      ateenv.v1alpha.ProcessService/StartProcess                ate-env-api (server)
        ateenv.v1alpha.ProcessService/StartProcess              ate-env-api (client, to the guest)
          ateenv.v1alpha.ProcessService/StartProcess            ate-env-guest (server)
  atefleet.release
    ate_env_client.release
      ate_env_client.delete_environment
        ateenv.v1alpha.EnvironmentService/DeleteEnvironment     ate-env-api
  atefleet.teardown
    ate_env_client.reap
```

A Go client's spans take the place of the `ate_env_client.*` ones, named
`<TraceServiceName>.<operation>` (`create_environment`, `get_environment`,
`suspend_environment`, `delete_environment`, `exec`, `read_file`,
`write_file`), as children of whatever span the caller's context carries.

The swebench loader writes each event when its operation finishes, so with
`-enable-tracing` every event becomes one span recorded after the fact, timed
from the event's `ts` minus `dur_ms` to `ts`, all under one run span:

```
swebench-loader.run          attributes run, mode
  create                     run, env, instance, step, task
  ttfi                       ... attempts
  step                       ... name (the trajectory step), exit, reward
  task
  suspend | delete | wake
  step_barrier | park        steps mode, count
  run                        the whole run, count
```

The span is named after the event (`Event.Event`); its status is Ok when the
event is `ok` and Error with the event's `err` otherwise. The JSONL is byte for
byte the same with tracing on or off.

Span attributes carry ids and sizes (`env.id`, `env.atespace`,
`env.template`, `env.rootfs_image`, `env.command.executable`, `env.exit_code`,
`env.file.path`, `env.file.size`, and the loader's `run`, `env`, `instance`),
never a command line or file content.

## Metrics

- ate-env-api and ate-env-guest families: [metrics.md](metrics.md), generated
  from `internal/telemetry` by `go run ./hack/metrics-docs`. CI fails when it is
  stale.
- Trainer families (`atefleet_phase_latency_seconds`,
  `atefleet_task_latency_seconds`, `atefleet_run_latency_seconds`,
  `atefleet_claims_total`, `atefleet_tasks_total`, `atefleet_warm_replicas`,
  `ate_env_client_{acquire,exec,suspend,wake,release}_seconds`): the
  [Python client README](../clients/python/README.md#observability).

Collectors register on the first metrics-enabled `Observer`, never at import,
and the NeMo Gym provider never creates one, so a provider process has no
Prometheus side effects.

### The cardinality rule

Labels carry a bounded vocabulary only:

| Label | Values |
|---|---|
| `phase` | the phase names in the glossary below |
| `backend` | `substrate`, `mock` |
| `family` | the task's repo family (`ate_env.observability.repo_family`), never the image tag |
| `strategy`, `status` | the fleet strategy; `ok` or `error` |
| `method`, `code` | registered gRPC full method names and status codes |
| `atespace`, `state`, `outcome`, `exit_class` | as documented in [metrics.md](metrics.md) |

An environment id, actor id, run id, command line, file path or image tag never
becomes a metric label or a span name. They go in span attributes, where a
trace backend indexes them without multiplying series.

## Run report

`fleet.report` holds a `RunReport` after `SandboxFleet.run` and
`AsyncSandboxFleet.run`. `summary()` prints the table, `to_dict()` returns the
JSON keys, and `write(report_dir)` writes both:

```python
fleet.run(process_fn)
print(fleet.report.summary())
fleet.report.write(os.environ["REPORT_DIR"])   # <strategy>_<n>tasks_<YYYYMMDD-HHMMSS>.txt and .json
```

```text
── Run report (strategy=sliding) ──
  environment:
    substrate: atespace=ate-env  endpoint=localhost:7777  strategy=sliding  concurrency=8  tasks=200  images=12
  ----------------------------------------
  ensure_template        0.01s  (n=12, max=0.00s)
  acquire               41.20s  (n=200, max=1.90s)
  process               95.33s  (n=200, max=2.41s)
  release                6.02s  (n=200, max=0.12s)
  teardown               0.31s  (n=1, max=0.31s)
  ----------------------------------------
  TOTAL                 24.87s
  claims=200  tasks=200ok/0err  warm_replicas total=0 peak=0
```

```json
{
  "strategy": "sliding",
  "total_s": 24.87,
  "phases": {"acquire": {"count": 200, "total_s": 41.2, "max_s": 1.9}, "...": {}},
  "claims": 200,
  "tasks_ok": 200,
  "tasks_err": 0,
  "warm_replicas_total": 0,
  "warm_replicas_peak": 0,
  "environment": {"substrate": {"atespace": "ate-env", "endpoint": "localhost:7777", "...": "..."}}
}
```

Phase totals are summed durations, so under concurrency they exceed the
wall-clock `TOTAL`. The GRPO example (`examples/grpo`, `REPORT_DIR`) and the
NeMo Gym rollout benchmark (`integrations/nemo-gym/benchmarks/rollout_bench.py`,
`REPORT_DIR` or `--report-dir`) write the same report, with strategy `grpo` and
`rollouts`.

### Phase glossary

| Phase | What it times |
|---|---|
| `preflight` | the backend's connectivity, permission and capacity checks |
| `plan` | sizing the run: one entry per image, resolving its template |
| `ensure_template` | resolving or creating the template for an image, once per image |
| `warm_pool` | bringing an image's warm pool to its size |
| `acquire` | getting a ready environment for a task (create, or claim from the pool) |
| `process` | the caller's work on one task (`process_fn`) |
| `recycle` | returning an environment to the warm pool |
| `release` | giving an environment back (delete) |
| `reap` | kept in the table order for agent-sandbox-rl parity; the fleet records its sweep as the `ate_env_client.reap` span inside `teardown` |
| `teardown` | the end-of-run sweep of every environment the run created |
| `suspend`, `wake` | GRPO example only: parking the fleet between steps and the first call after |
| `run` | the whole run (`atefleet.run`, `atefleet_run_latency_seconds`) |

## Collectors and Cloud Trace

### A collector in the cluster

[`tests/telemetry/otel-collector.yaml`](../tests/telemetry/otel-collector.yaml)
is the collector the telemetry acceptance runs use. It receives OTLP on 4317
and 4318, scrapes ate-env-api's `/metrics`, writes spans and metrics as OTLP/JSON
lines to `/data`, logs spans with the `debug` exporter and forwards them to
another OTLP endpoint. Set `METRICS_TARGET` and `FORWARD_ENDPOINT` in it, then:

```bash
kubectl create namespace ate-env-telemetry
kubectl -n ate-env-telemetry apply -f tests/telemetry/otel-collector.yaml
ate-env manifest --enable-tracing --otlp-endpoint http://otel-collector.ate-env-telemetry:4317 \
  --otlp-insecure --metrics-port 9090 ... | kubectl apply -f -
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.ate-env-telemetry:4317   # trainer in the cluster
```

Copy the files out for the report tool below:

```bash
kubectl -n ate-env-telemetry cp -c files deploy/otel-collector:/data ./otel-data
```

The fixture is for tests, not production: one replica, an `emptyDir`, no auth.

### Cloud Trace and GKE managed OpenTelemetry

Cloud Trace accepts OTLP through a collector, the same route agent-sandbox
documents:

- **Local development**: run `otel/opentelemetry-collector-contrib` in Docker
  with a `googlecloud` exporter and Application Default Credentials
  (`gcloud auth application-default login`), and point
  `OTEL_EXPORTER_OTLP_ENDPOINT` at `http://localhost:4317`.
- **GKE**: enable Managed OpenTelemetry for GKE (GKE 1.34.1-gke.2178000 or
  later; it runs a collector in the `gke-managed-otel` namespace) or deploy the
  [Google-Built OpenTelemetry Collector](https://docs.cloud.google.com/stackdriver/docs/instrumentation/opentelemetry-collector-gke),
  and set `--otlp-endpoint` (ate-env-api) and `OTEL_EXPORTER_OTLP_ENDPOINT`
  (trainer, guest) to its Service, for the managed collector
  `http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317`. The
  fixture above forwards there by default, so the same traces show in Cloud
  Trace and in its files; without the managed collector, remove `otlp/forward`
  from its traces pipeline.

The node or workload identity needs `roles/cloudtrace.agent` (and
`roles/monitoring.metricWriter` for metrics). When Substrate's ateapi exports to
the same backend, its spans join the trace under ate-env-api's
`ateapi.Control/*` client spans.

## HTML run report

`benchmarking/telemetry_report/build_report.py` (standard library only) turns
any subset of the three layers into one self-contained `report.html` and the
`run.json` behind it:

```bash
python benchmarking/telemetry_report/build_report.py \
  --report "$REPORT_DIR" \
  --spans ./otel-data/spans.jsonl \
  --metrics ./otel-data/metrics.jsonl,./trainer.prom \
  --title my-run --out /tmp/telemetry-report
```

- `--report`: RunReport JSON files or a directory of them (`REPORT_DIR`).
- `--spans`: OTLP/JSON spans, one export request per line, as the collector's
  `file` exporter writes them (hex ids) or as `MessageToDict` of an OTLP
  request gives them (base64 ids).
- `--metrics`: Prometheus text scrapes (`curl .../metrics > x.prom`) or the
  collector's `file` exporter metrics (only the newest point of each series is
  used).

Tabs: overview (phase table, runs, traces per service), caller timeline,
traces with waterfalls, span latency and metric quantiles.

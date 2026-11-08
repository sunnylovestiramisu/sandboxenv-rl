# Research: inventory at seconds resolution, and traces

Date: 2026-10-30. Code read: `ai-on-gke/sandboxenv-rl` at `3b03b03` (2026-10-29) and
`agent-substrate/substrate` at `28520c0d` (2026-10-27). Cloud facts from the vendor documents linked
at the end.

## Question 1: how to know what exists, within seconds

### Substrate's actor event stream

`ateapi` writes one record per committed actor state transition, after the store commit, never
before (`cmd/ateapi/internal/controlapi/workflow.go`, `logActorStateChanged`, `logActorDeleted`;
`crash.go`, `logActorCrashed`; `internal/actorevent`). Facts from `docs/observability.md` in
Substrate:

- Records: `Actor state changed` (severity 9) and `Actor crashed` (severity 17). Attributes: the
  five identity keys `ate.atespace`, `ate.actor.name`, `ate.actor.uid`, `ate.template.atespace`,
  `ate.template.name`, plus `ate.actor.operation.name` (`create`, `resume`, `suspend`, `delete`,
  `revert`) and `ate.actor.state` (the `ateapi.ActorState` values lowercased, plus `deleted`).
  `trace_id`, `span_id` and `trace_flags` join the record to the RPC that drove it.
- Creation counts: a new actor is born `suspended` with operation `create`. `deleted` is the last
  record an actor gets.
- Consumer rules the document fixes: take the last record per `ate.actor.uid`; never sample or
  filter this stream; a gap looks like an actor that sat still, so reconcile with the control plane.
- Transport: with `OTEL_LOGS_EXPORTER` unset, which is the GKE default, the records go to pod stdout
  as JSON with the keys at the top level, so Cloud Logging indexes them as
  `jsonPayload."ate.actor.state"`. With `otlp` they go out as OTLP log events named
  `ate.actor.state_changed` and `ate.actor.crashed` in scope
  `go.ate.dev/substrate/internal/actorevent`. The document recommends `otlp,console` because `otlp`
  alone can lose a record on a full queue or an ungraceful exit. The GKE managed collector forwards
  only to Cloud Logging, Monitoring and Trace, so routing by `event.name` needs a self-managed
  collector.
- A third event, `ate.actor.usage_sampled`, carries per-actor CPU and memory from the ateworkers
  once per `--usage-sample-interval` (default 1 m, at least 50 s), the only per-actor resource
  signal.

### Control-plane reads

`api/ateapi/v1/ateapi.proto` has no streaming or watch RPC. What exists:

- `ListActors` with an empty `atespace` lists across all atespaces, paged with `ListOptions`
  (`page_size` coerced to at most 1000, `pagination.go`, `maxPageSize`). `Actor` carries
  `ResourceMetadata` (`uid`, `create_time`, `update_time`, `delete_time`), `actor_template`,
  `source_tag` and `ActorStatus` (`state`, `worker_assignment`, `assigned_node`, `crash`).
- `ListWorkers` returns each worker pod with `worker_namespace`, `worker_pool`, `worker_pod`,
  `node_name` and `WorkerStatus` (`state`, `capacity`, `allocated`). `ListWorkerActorAssignments`
  maps a worker to its actors. `ListAtespaces`, `ListActorTemplates`, `ListTags` complete the
  catalog.
- No rate limiter in `cmd/ateapi` (grep for `rate.Limiter` and `ratelimit` finds none); the list is
  a paged Postgres read. `ate-env-api` already pages `ListActors` per atespace for its
  `ate_env_api_environments` gauge (`internal/ate/client.go`).
- Roles in `cmd/ateapi/internal/authz`: `viewer`, `editor`, `owner`. The `ate-env manifest` grants
  `editor` only; a reporter needs a global AccessPolicy with `viewer`. `kubectl ate get actor -A` is
  the precedent for cross-atespace listing.
- Kubernetes side: WorkerPool `status.replicas`, `status.readyReplicas`, `status.selector`
  (`api/infra/k8s/ate/v1/workerpool_types.go`); atecontroller emits Kubernetes Events on WorkerPool
  conditions; informers deliver changes in under a second.

`ate-env-api` cannot serve a fleet inventory: `ateenv.v1alpha` has only `CreateEnvironment`,
`GetEnvironment`, `SuspendEnvironment`, `DeleteEnvironment` and `GetServerInfo`, and its
`ate_env_api_environments` gauge covers only the atespaces that deployment has created environments
in (`apiservice.Server.atespaces`, at most `maxAtespaces`), cached for `DefaultEnvironmentsCacheTTL`
(60 s, flag `-metrics-environments-ttl`).

### Metrics

- Managed Service for Prometheus scrapes at a 5 s minimum (the operator clamps lower intervals;
  Cloud Monitoring rejects points closer than 2.5 s). Full granularity is kept one week, then
  1-minute points for five weeks, then 10-minute points.
- `ateapi` serves `ate.actor.lifecycle.operation.duration` (operation, template, pool, class,
  snapshot kind and fidelity, `error.type`), `ate.scheduler.assignment.duration` (outcome, pool),
  `ate.actor.crashes`, `ate.workerpool.workers` (every replica reports the whole fleet: `max` by
  pool, state and class, then `sum`). atelet serves the restore, checkpoint, upload and image-cache
  families; atecontroller serves `ate.workerpool.desired_workers` and `ready_workers`. On GKE these
  reach Cloud Monitoring by OTLP push through the managed collector; `OTEL_METRICS_EXPORTER`
  defaults to `otlp,prometheus`. The push interval is the SDK default of 60 s unless
  `OTEL_METRIC_EXPORT_INTERVAL` is set.
- No Substrate metric carries actor identity, by the registry's cardinality rules
  (`docs/metrics/registry/metrics.yaml`).
- `ate-env manifest --metrics-port` emits `prometheus.io` annotations, which managed collection
  ignores; a `PodMonitoring` is missing.

### Conclusion for inventory

Poll `ListActors` across atespaces plus `ListWorkers` every 5 s for the truth, and consume the actor
event stream for exact transition times and the live feed. Freshness of the poll is the interval
plus one object write. Freshness of events is seconds on the OTLP path and seconds typical, 99%
under 60 s, through Cloud Logging. Neither needs a change to Substrate; the OTLP path needs one
environment variable in `ate-otel-config` and a rollout restart.

## Question 2: traces, end to end

### The join exists today

- Python and Go client spans carry `ate.actor.name`, `ate.atespace`, `ate.template.name`,
  `ate.template.atespace`, `ate_env.rootfs_image` and `ate_env.run_id`
  (`clients/python/src/ate_env_rl/client.py`, `clients/go/tracing.go`).
- `ate-env-api` continues the context with otelgrpc on its server and on the `ateapi`, guest and
  wake-probe connections (#114). Its own spans carry `rpc.*` only.
- The atenet router's ingress extracts `traceparent` from the request metadata and calls
  `ResumeActor` under that context (`cmd/atenet/internal/router/ingress/ingress.go`, `resumer.go`).
  Parentless requests root at 1% there.
- `ateapi` stamps actor attributes on its server span (`controlapi/span_identity.go`), opens a
  `step.<name>` span per workflow step with `step.skipped` and `step.skip_reason` (`workflow.go`,
  `stepSpan`, `markSkipped`), traces every Postgres statement, and calls atelet `Run`, `Restore` and
  `Checkpoint` with otelgrpc (`workflow_resume.go`, `workflow_suspend.go`).
- ateworker's otelgrpc spans travel through the atelet OTLP relay on a unix socket
  (`internal/otlprelay`), stamped `ate.otlp.relay=relay`. Every component samples parent-based
  (`internal/serverboot/sampling.go`, `ControlPlaneTraceRatio = 0.1`), so a trace the trainer
  samples is complete through to the worker.
- The lifecycle log records carry the trace id, so an inventory event links to its trace.

This closes the telemetry plan's open question: no trace-context label on actors is needed. The join
needs only that Substrate's components and the ate-env collector export to the same backend.

### Edges

- `ate-env-api` spans lack identity attributes; `ateapi` shows the pattern to copy.
- `ate-env-guest` sets `service.name` only; its spans carry no actor identity on the resource.
- Telemetry an SDK emits inside an actor needs a per-actor EgressPolicy `http` rule to the collector
  on ports 4317 and 4318, and the collector sees the egress gateway as the source. A template-level
  default is Substrate #1430; actor identity injection is #853 and #761.

### Sinks and latency

- Both SDKs batch for 5 s by default (`BatchSpanProcessor`, `sdktrace.WithBatcher`).
- Cloud Trace accepts OTLP at `telemetry.googleapis.com` (GA September 2025) with higher limits than
  the older exporter, 30-day retention and SQL over spans. No freshness number is published; the
  documents say near real-time, and the first trace in a project waits for database creation.
- The collector-contrib `google_cloud_storage` exporter writes OTLP objects with time partitioning,
  gzip and routing by a resource attribute, traces included since v0.145.0. The `googlecloudpubsub`
  exporter publishes OTLP as CloudEvents to a topic.
- The repo's fixture already fans out to files, the `debug` exporter and a forward endpoint; the
  change is configuration.
- Volume: #186's end-to-end run produced 622 spans for 12 tasks, about 50 spans per task.

## Question 3: Cloud delivery paths

| Path                                                 | Documented or measured latency                                                                   |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Cloud Logging sink to Pub/Sub                        | seconds typical; 99% of entries under 60 s from arrival at Logging; occasional minute-long tails |
| Direct Cloud Storage trigger to Cloud Run (Eventarc) | near-immediate, Pub/Sub under the hood; no published figure                                      |
| Pub/Sub BigQuery subscription                        | streaming, seconds; no code                                                                      |
| Pub/Sub Cloud Storage subscription                   | batched files, window of at least one minute                                                     |
| Managed Prometheus                                   | 5 s scrape floor; the troubleshooting guide recommends `offset` for the newest window            |

## Sources

- Substrate: `docs/observability.md`, `internal/actorevent/actorevent.go`,
  `cmd/ateapi/internal/controlapi/{workflow.go,span_identity.go,metrics.go,pagination.go}`,
  `cmd/atenet/internal/router/ingress/{ingress.go,resumer.go}`, `internal/serverboot/sampling.go`,
  `api/ateapi/v1/ateapi.proto`.
- sandboxenv-rl: `internal/ate/client.go`, `internal/apiservice/server.go`,
  `internal/telemetry/{otel.go,metrics.go}`, `tests/telemetry/otel-collector.yaml`,
  `cmd/ate-env/manifest.go`, `docs/observability.md`.
- [googlecloudstorage exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/googlecloudstorageexporter),
  [googlecloudpubsub exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/googlecloudpubsubexporter),
  [Managed Service for Prometheus](https://cloud.google.com/stackdriver/docs/managed-prometheus),
  [prometheus-engine PR #2328](https://github.com/GoogleCloudPlatform/prometheus-engine/pull/2328),
  [Logs routed to Pub/Sub](https://docs.cloud.google.com/logging/docs/export/pubsub),
  [Eventarc Cloud Storage trigger](https://cloud.google.com/blog/topics/developers-practitioners/introducing-new-cloud-storage-trigger-eventarc),
  [OpenTelemetry in Google Cloud Observability](https://cloud.google.com/blog/products/management-tools/opentelemetry-now-in-google-cloud-observability),
  [Telemetry API v1.traces](https://docs.cloud.google.com/stackdriver/docs/reference/telemetry/v1.traces).

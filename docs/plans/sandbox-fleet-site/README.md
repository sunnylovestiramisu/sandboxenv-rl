# Sandbox fleet site: design

Status: accepted plan (#223); pull request 1, `internal/inventory`, merged as #226. Last checked
2026-11-09 against `ai-on-gke/sandboxenv-rl` at `f10c68c` and the Substrate `go.mod` requires,
`274605f1`; the audit is in [research.md](./research.md), which first read `3b03b03` and `28520c0d`
on 2026-10-30. The decisions taken so far are in [decisions.md](./decisions.md). Companions:
[prd.md](./prd.md), [execution-plan.md](./execution-plan.md), [interfaces.md](./interfaces.md),
[acceptance-criteria.md](./acceptance-criteria.md) and, for the order of operations with `gcloud`,
[quickstart.md](./quickstart.md).

## Goal

One website, deployed once on Cloud Run behind Identity-Aware Proxy, that every cluster user can
open and that shows, within seconds, every ate-env deployment, WorkerPool, actor template and actor
across all namespaces and all clusters, with the metrics and the traces behind them one click away.
The site is read-only: it holds no cluster credentials and its service account has no write role
anywhere.

## Shape

```
per cluster                                                     central project
───────────                                                     ───────────────
ateapi ─ actor events (stdout, later OTLP) ─► Cloud Logging sink ─► Pub/Sub topic ─┬─► BigQuery subscription ─► actor_events table
                                            (phase 2: collector ─► Pub/Sub)        └─► Cloud Storage subscription ─► gs://…/v1/events/
reporter (Deployment) ─ ListActors, ListWorkers, K8s informers ─► gs://…/v1/inventory/<project>/<cluster>/latest.json
ate-env-api :metrics ─ PodMonitoring 5 to 15 s ─► Managed Prometheus ─► Cloud Monitoring
Substrate components ─ OTLP push via the GKE managed collector ─► Cloud Monitoring, Cloud Trace
trainers, ate-env-api, guests ─ OTLP ─► telemetry collector ─┬─► Cloud Trace
                                                             └─► gs://…/v1/traces/ (OTLP/JSON, partitioned)
trainers ─ REPORT_DIR ─► gs://…/v1/runs/<run_id>/

users ─► IAP ─► Cloud Run site ─ reads ─► GCS, BigQuery, Cloud Monitoring PromQL API, Cloud Trace links
```

## Data classes

| Signal            | Produced by                                                                                                                         | Path                                                                                                                                                                                                                                                                                                                                                                   | Freshness                                                                                           | Store                                                                                            | Identity keys                                                                             |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| Current inventory | reporter, one per cluster                                                                                                           | `ListActors` across all atespaces, `ListWorkers`, `ListAtespaces`, `ListActorTemplates`, `ListTags`; Kubernetes informers for WorkerPools, worker pods, `ate-env-api` Deployments                                                                                                                                                                                      | poll interval, 5 s default                                                                          | GCS `latest.json` per cluster, hourly snapshots kept 30 days                                     | `ate.actor.uid`, atespace, name, template, `update_time`                                  |
| Transitions       | `ateapi`: `Actor state changed`, `Actor crashed`, one record per committed transition                                               | phase 1: pod stdout, Cloud Logging sink, Pub/Sub; phase 2: `OTEL_LOGS_EXPORTER=otlp,console`, collector, Pub/Sub                                                                                                                                                                                                                                                       | seconds; 99% under 60 s on the Logging path                                                         | BigQuery table via a Pub/Sub BigQuery subscription; GCS archive via a Cloud Storage subscription | the five `ate.*` identity keys, `ate.actor.operation.name`, `ate.actor.state`, `trace_id` |
| Time series       | `ate-env-api` (`ate_env_api_*`), `ate-env-guest`, Substrate (`ate.actor.*`, `ate.scheduler.*`, `ate.workerpool.*`, atelet families) | PodMonitoring for `ate-env-api`; Substrate pushes OTLP through the managed collector                                                                                                                                                                                                                                                                                   | 5 s scrape floor; Substrate's push interval is 60 s unless `OTEL_METRIC_EXPORT_INTERVAL` is lowered | Cloud Monitoring, full resolution one week                                                       | none, by the cardinality rule                                                             |
| Traces            | Python and Go clients, `ate-env-api`, guest, swebench loader, Substrate `ateapi`, router, atelet, ateworker                         | OTLP to the per-cluster collector; Substrate through the managed collector                                                                                                                                                                                                                                                                                             | SDK batch flush, 5 s default                                                                        | Cloud Trace, 30 days; raw OTLP/JSON in GCS                                                       | `ate.actor.name`, `ate.atespace`, `ate.template.name`, `ate_env.run_id`, `trace_id`       |
| Run artifacts     | trainers (`RunReport.write`, `build_report.py`) and the benchmark harness (`bench validate`, `bench upload`, #270)                  | trainers: `REPORT_DIR` on a GCS mount or copied at run end; benchmarks: one folder per run in the benchmark bucket, `<YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>[_traced]`, with the six files `bundle.go` requires: `system_description.json`, `measurements.json`, `run_window.json`, `report.md`, `report.json`, and `events.jsonl` or `events/*.jsonl` | at run end                                                                                          | GCS                                                                                              | `run_id`; the bundle folder name                                                          |

Current state comes from the poll, never from traces or events. Events give the exact transition
times and the live feed. Traces answer why one step was slow. Metrics give fleet-wide rates and
capacity. Each store is the one Google already runs for that signal; GCS is the only store the
project owns.

## Components

Per cluster, applied by `ate-env manifest` or the fleet's Config Sync repository:

- Telemetry collector: the acceptance fixture `tests/telemetry/otel-collector.yaml` hardened into a
  deployment with three exporters: Cloud Trace (`otlphttp` to `telemetry.googleapis.com`),
  `google_cloud_storage` for raw spans, and `googlecloudpubsub` for actor events once Substrate
  emits them over OTLP. One replica per cluster is enough for the ate-env side; Substrate's own
  components keep the managed collector for traces and metrics; in phase 2 only `ate-api-server`'s
  logs go to this collector, through `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`.
- Reporter: `cmd/ate-env-reporter`, a single-replica Deployment in its own namespace labeled
  `ate.dev/api-access=true`, reusing `internal/ate` to dial `ateapi`. It rewrites `latest.json`
  every interval and adds an hourly snapshot. Workload Identity to a service account with
  `roles/storage.objectUser` on the bucket, restricted by an IAM condition to that cluster's
  `v1/inventory/` and `v1/snapshots/` prefixes (an overwrite needs `storage.objects.delete`, which
  `objectCreator` lacks); its principal merged into the global AccessPolicy's `viewer` binding, the
  binding `ate-env manifest access-policy --global` already renders for `ate-env-api`'s
  `ListPlacementDomains` (#235), because the global policy is one object; a ClusterRole with `get`,
  `list`, `watch` on WorkerPools, SandboxConfigs, Deployments, Services and Pods.
- PodMonitoring for `ate-env-api`'s metrics port, rendered by `ate-env manifest --metrics-port`.
- Substrate configuration: `OTEL_LOGS_EXPORTER=otlp,console` and `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`
  on the `ate-api-server` Deployment (phase 2; not in the shared `ate-otel-config`, because the
  managed collector does not export logs to Pub/Sub and atelet refuses per-signal endpoints that
  differ for its relay) and the reporter's member in the global `viewer` binding.

Central, once:

- Bucket with uniform access, lifecycle rules, no public access.
- Pub/Sub topic `ate-actor-events` with a BigQuery subscription and a Cloud Storage subscription.
- Cloud Logging sink per cluster project (phase 1) with the filter in
  [interfaces.md](./interfaces.md).
- Cloud Run service `ate-fleet-site` behind IAP with a Google group; service account with
  `roles/storage.objectViewer`, `roles/bigquery.dataViewer` and `roles/bigquery.jobUser` on the
  events dataset, `roles/monitoring.viewer` on the metrics scope.

## Freshness budget

| Question                                                         | Source                           | Expected delay                                                                |
| ---------------------------------------------------------------- | -------------------------------- | ----------------------------------------------------------------------------- |
| Which deployments exist, what version, how many actors per state | reporter `latest.json`           | poll interval plus one object write, 5 to 7 s                                 |
| When did actor X enter state Y                                   | `actor_events`                   | seconds on the OTLP path; seconds typical, 99% under 60 s on the Logging path |
| Request rates, wake waits, create-to-ready, capacity per pool    | Cloud Monitoring                 | scrape interval plus ingestion, 10 to 30 s                                    |
| Why was this task slow                                           | Cloud Trace or the GCS span file | batch flush plus backend ingestion, tens of seconds                           |

The site shows `generated_at` for each cluster and marks a cluster stale after three missed
intervals, which doubles as reporter health.

## Rejected alternatives

- Deriving inventory from traces: tracing is opt-in, idle and suspended actors emit nothing, and
  WorkerPools and templates have no spans. See decisions.md, 2026-10-30.
- A central poller reaching into clusters from Cloud Run: concentrates credentials to every cluster
  in one internet-facing service; push keeps the site credential-free.
- `ate-env-api` reporting its own inventory: per-namespace view only, and it widens the
  security-sensitive adapter.
- Grafana as the UI: needs a database and is awkward on Cloud Run; the site is a thin custom app and
  links to Cloud Monitoring dashboards for depth.
- A long-running daemon that scans the bucket: Cloud Run is request-driven; subscriptions and
  Eventarc replace polling.

## Security

- Read-only is enforced by IAM rather than by code: the site's service account has viewer roles
  only, the reporter's has `storage.objectUser` under an IAM condition on its own two prefixes of
  one bucket and `viewer` in Substrate, the collector's has `pubsub.publisher`,
  `storage.objectCreator` and `cloudtrace.agent`.
- A compromised reporter can overwrite or delete only its own cluster's `latest.json` and hourly
  snapshots. The event archive under `v1/events/` and the BigQuery table are written by Pub/Sub,
  outside every reporter's prefixes, and keep the transition history.
- The metrics listeners of `ate-env-api` and the guest have no authentication and stay
  cluster-internal; only the in-cluster collectors scrape them.
- Snapshots and events carry atespace names, template names, image references and run ids, never
  tokens. Cross-tenant visibility is a policy choice: IAP supplies the user identity, so the site
  can filter namespaces by group later without a redesign.

## Drift check

Run this before each pull request of the plan and when `main` moves; record the result in the audit
section of [research.md](./research.md) and update the last-checked line above (FS-13).

1. RPCs and fields: `grep -E '^\s*rpc ' proto/ateenv/v1alpha/env.proto` and the `Environment`
   message. The plan assumes no RPC enumerates environments; a list or watch call changes the
   reporter.
2. Substrate, at the version `go.mod` requires: `ListActors` with an empty atespace lists across
   atespaces and pages at 1000; `Control` has no `Batch*` or `stream` RPC; the actor event names in
   `internal/actorevent` are `ate.actor.state_changed`, `ate.actor.crashed` and
   `ate.actor.usage_sampled`; the authz roles are `viewer`, `editor` and `owner`.
3. Schema: `diff` the JSON example in [interfaces.md](./interfaces.md) against
   `internal/inventory/testdata/snapshot.golden.json`, then `go test ./internal/inventory/`.
4. Metrics, in two halves. Each `ate_env_*` family in the named PromQL queries exists in
   `docs/metrics.md` or in the `ate_env_fleet_*` and `ate_env_client_*` registrations under
   `clients/python`. Each other `ate_*` family (today `ate_workerpool_workers` and
   `ate_actor_crashes_total`) exists in Substrate's `docs/metrics/registry/metrics.yaml` at the
   version `go.mod` requires, under the OTLP-to-Prometheus naming: dots become underscores and a
   counter gains `_total`, so the entries to find are `ate.workerpool.workers` and
   `ate.actor.crashes`. The automated form of this step is `TestFS13DriftCheckStep4`.
5. Manifest: `ate-env manifest` still has `access-policy --global`, `--metrics-port`,
   `--enable-tracing`, `--otlp-endpoint` and `--otlp-insecure`.
6. Benchmark bundles: `cmd/benchmarking/bench/bundle.go` still names the folder form and the
   required files the Runs page reads.
7. Conventions: criterion ids stay unique across `docs/plans/` (`FS-` here).

## Open parameters

The project and cluster list, the Google group, the bucket name, whether Cloud Trace is enabled in
each cluster project, and whether all users see all namespaces.

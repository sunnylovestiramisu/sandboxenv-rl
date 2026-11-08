# Acceptance criteria: sandbox fleet site

Status: draft, 2026-10-30. Each criterion has a command and an expected result. Evidence goes into
`evidence/` in this directory, one file per acceptance pass, with the commit of each component.

| ID    | Title                                    | Phase |
| ----- | ---------------------------------------- | ----- |
| FS-1  | Inventory freshness within one interval  | 3     |
| FS-2  | Transition visible within seconds        | 2, 7  |
| FS-3  | Read-only enforced by IAM                | 1, 5  |
| FS-4  | Two clusters, two projects, side by side | 8     |
| FS-5  | One trace from the trainer to the worker | 4     |
| FS-6  | Metrics at 5 s resolution                | 4     |
| FS-7  | IAP gate                                 | 1, 5  |
| FS-8  | No actor identity in metric labels       | 4     |
| FS-9  | Stale reporter is visible                | 3, 5  |
| FS-10 | Schema and secrets                       | 3     |
| FS-11 | Run artifacts resolve by run id          | 6     |
| FS-12 | Documentation                            | 8     |

## FS-1: Inventory freshness

- Command: resize a WorkerPool or create one environment with the Python client; sample
  `gsutil stat gs://<bucket>/v1/inventory/<p>/<c>/latest.json` every second and
  `GET /api/v1/clusters/<p>/<c>`.
- Expected: the change is in `latest.json` within `interval + 2 s` and on the site within another 30
  s cache window or immediately with `?nocache=1`.
- Pass rule: ten trials, all within budget.

## FS-2: Transition latency

- Command: suspend and resume one environment with the client while recording the wall-clock time;
  query `fleet.actor_events` for the actor uid; read `/api/v1/actors/<uid>/events`.
- Expected: Logging path: median under 10 s, 99th percentile under 60 s over 100 transitions. OTLP
  path (phase 7): median under 5 s. The record's `state` and `operation` match the client's calls,
  and `trace_id` equals the client span's trace id when tracing is on.
- Pass rule: the percentiles hold and no transition is missing against the client's log.

## FS-3: Read-only

- Command: `gcloud projects get-iam-policy` and `gsutil iam get` for the site's service account;
  `gcloud run services describe` for its identity; an attempt from the site's identity to write an
  object and to call `ateapi`.
- Expected: viewer roles only; the write returns 403; no `ateapi` endpoint or token is configured in
  the service.
- Pass rule: all three hold.

## FS-4: Multi-cluster

- Command: deploy the reporter and collector in a second cluster in a second project; open `/`.
- Expected: both clusters listed with distinct project and cluster labels, their own deployments and
  counts; `actor_events` rows carry the right project and cluster.
- Pass rule: no field from one cluster attributed to the other.

## FS-5: Trace join

- Command: run the mock-free quickstart with `ObservabilityConfig(enable_tracing=True)` against a
  cluster whose Substrate exports to Cloud Trace; open the trace by id in Cloud Trace; read the span
  file under `v1/traces/`.
- Expected: one trace id spans `ate_env_fleet.run`, `ate_env_client.create_environment`,
  `ateenv.v1alpha.EnvironmentService/CreateEnvironment`, `ateapi.Control/*`, `step.*` workflow
  spans, atelet `Restore` and ateworker spans with `ate.otlp.relay=relay`. The GCS file holds the
  client and `ate-env-api` spans of the same trace id.
- Pass rule: the chain is unbroken in Cloud Trace; the file has the ate-env half.

## FS-6: Metrics resolution

- Command: PodMonitoring with `interval: 5s`; after five minutes,
  `count_over_time(ate_env_api_requests_total[1m])` through the PromQL API.
- Expected: at least 10 samples per minute per series; the `environments` named query returns
  per-cluster, per-atespace counts.
- Pass rule: holds for every `ate-env-api` in both clusters.

## FS-7: IAP

- Command: `curl -i https://<site>/` without credentials; the same with an identity token of a group
  member and of a non-member.
- Expected: redirect or 403 without credentials; 200 for the member; 403 for the non-member; the
  Fleet page shows the member's email from the IAP assertion.
- Pass rule: all four.

## FS-8: Cardinality

- Command: list label names of every `ate_env_*`, `ate_actor_*`, `ate_scheduler_*` and
  `ate_workerpool_*` series in Cloud Monitoring.
- Expected: no label named after an actor, a run id, an environment id or an image tag.
- Pass rule: holds.

## FS-9: Stale reporter

- Command: scale the reporter to zero; wait three intervals; open `/`.
- Expected: the cluster shows a stale badge with the age of `generated_at`; the API returns
  `"stale": true`; scaling back clears it within one interval.
- Pass rule: holds.

## FS-10: Schema and secrets

- Command: `go test ./internal/inventory/...` and a grep over a day of snapshots and events for
  `token`, `password`, `authorization`, `ya29.`, `AIza`.
- Expected: the golden file matches; zero hits.
- Pass rule: holds.

## FS-11: Runs

- Command: run the GRPO example with `REPORT_DIR` on the bucket path and tracing on; open
  `/runs/<run_id>`.
- Expected: the run report table, the HTML report link, the span file and the Cloud Trace link all
  resolve; the waterfall renders from the span file.
- Pass rule: holds.

## FS-12: Documentation

- Expected: `docs/fleet-site.md` covers deployment per cluster and central, the IAM matrix, the
  bucket layout, the freshness budget and the tenancy rule; `CHANGELOG.md` lists the reporter, the
  manifests and the site.
- Pass rule: present and checked against the code in the final pull request.

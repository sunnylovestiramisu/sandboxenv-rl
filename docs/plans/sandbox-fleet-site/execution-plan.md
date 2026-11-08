# Execution plan: sandbox fleet site

Status: draft, 2026-10-30. Design: [README.md](./README.md). Contracts:
[interfaces.md](./interfaces.md). Done when every criterion in
[acceptance-criteria.md](./acceptance-criteria.md) passes on two clusters in two projects and the
pull requests below are merged.

## Where the code lives

- `ai-on-gke/sandboxenv-rl`: the reporter (`cmd/ate-env-reporter`), the snapshot schema
  (`internal/inventory`), the site (`cmd/ate-env-fleet-site`), the collector and PodMonitoring
  manifests (`manifests/telemetry/`), the Terraform for the central project (`deploy/fleet-site/`),
  and `docs/fleet-site.md`. The reporter reuses `internal/ate`; the site reuses the schema package.
  A separate repository is the alternative if the maintainers want the public repo to stay code that
  runs in the cluster only; recorded in [decisions.md](./decisions.md).
- `agent-substrate/substrate`: no code. Configuration per cluster (`ate-otel-config`) and one
  AccessPolicy.
- This directory: the plan, the decisions, the acceptance evidence.

## Phases

| Phase | Deliverable                                                                                                                                                                                                                                                                                                                                                                                                                                             | Depends on |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| 0     | Parameters from the owner: central project, bucket name, Google group, cluster list with projects, tenancy rule. Verify the Substrate `viewer` role on a test atespace.                                                                                                                                                                                                                                                                                 | none       |
| 1     | Central project by Terraform ([quickstart.md](./quickstart.md) has the `gcloud` order): bucket with lifecycle rules, Pub/Sub topic with BigQuery and Cloud Storage subscriptions, BigQuery dataset, service accounts, Cloud Run service skeleton behind IAP.                                                                                                                                                                                            | 0          |
| 2     | Events phase 1: a Cloud Logging sink per cluster project from `ateapi` stdout to the topic; verify records in BigQuery within a minute.                                                                                                                                                                                                                                                                                                                 | 1          |
| 3     | Reporter: `cmd/ate-env-reporter`, schema `internal/inventory`, `ate-env manifest reporter`, AccessPolicy `viewer`, Workload Identity. Verify `latest.json` every 5 s from one cluster.                                                                                                                                                                                                                                                                  | 1          |
| 4     | Telemetry manifests: `PodMonitoring` from `ate-env manifest --metrics-port`; collector deployment with Cloud Trace and GCS exporters. Verify 5 s samples in Cloud Monitoring and span files in the bucket.                                                                                                                                                                                                                                              | 1          |
| 5     | Site pages Fleet, Cluster, Actor, Live and the JSON API; IAP JWT verification; PromQL proxy with an allow-list of queries.                                                                                                                                                                                                                                                                                                                              | 2, 3, 4    |
| 6     | Runs page: run reports and span files by `run_id`; one-run waterfall from the span file.                                                                                                                                                                                                                                                                                                                                                                | 4, 5       |
| 7     | Events phase 2: `OTEL_LOGS_EXPORTER=otlp,console` and `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` set on the `ate-api-server` Deployment, pointing at the self-managed collector (the managed collector does not export logs to Pub/Sub; the shared `ate-otel-config` stays as is, because atelet refuses per-signal endpoints that differ for its relay), collector route by `event.name` to Pub/Sub, sink retired per cluster once both paths agree for a day. | 2, 4       |
| 8     | Second cluster in a second project; acceptance pass; `docs/fleet-site.md`; CHANGELOG.                                                                                                                                                                                                                                                                                                                                                                   | 5, 6       |
| 9     | Follow-ups: identity attributes on `ate-env-api` spans; group-to-namespace filtering; `ate.actor.usage_sampled` on the Actor page.                                                                                                                                                                                                                                                                                                                      | 8          |

Phases 2, 3 and 4 run in parallel after phase 1.

## Pull requests, sandboxenv-rl

| #   | Scope                                                                                                                                                              | Size   |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ |
| 1   | `internal/inventory`: the snapshot schema, JSON tags, a golden file                                                                                                | small  |
| 2   | `cmd/ate-env-reporter` and `ate-env manifest reporter`: poll loop, diffing on `uid` and `update_time`, atomic writes, health endpoint, tests against `fakecontrol` | medium |
| 3   | `manifests/telemetry/`: collector deployment and `PodMonitoring`; `ate-env manifest --pod-monitoring`                                                              | small  |
| 4   | `cmd/ate-env-fleet-site`: HTTP server, IAP verification, GCS and BigQuery readers, Fleet and Cluster pages, JSON API                                               | large  |
| 5   | Actor and Live pages, PromQL proxy, Runs page                                                                                                                      | medium |
| 6   | `deploy/fleet-site/` Terraform and `docs/fleet-site.md`                                                                                                            | medium |
| 7   | identity attributes on `ate-env-api` server spans                                                                                                                  | small  |

Each pull request follows the repo rules in `AGENTS.md`: one change per pull request, no AI
attribution trailers, Apache headers, tests in the same pull request.

## Roles

The method from the telemetry plan applies: a tech lead owns [interfaces.md](./interfaces.md), a
developer owns the pull requests, a tester runs [acceptance-criteria.md](./acceptance-criteria.md)
and writes evidence into this directory, a reviewer reads each pull request against the contracts.
Disputes are settled in [decisions.md](./decisions.md).

## Rules

- Nothing in this plan writes to a cluster except the reporter's and collector's own Deployments and
  the AccessPolicy for the reporter.
- No actor identity in a metric label. Identity lives in snapshots, events and span attributes.
- Every secret-bearing value stays out of snapshots and events; the schema test asserts no key named
  `token`, `password` or `authorization`.
- A deviation from the contracts is an entry in [decisions.md](./decisions.md), appended, never
  edited.

## Risks

| Risk                                               | Mitigation                                                                                              |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| The managed collector cannot route by `event.name` | the ate-env collector is self-managed from the first phase; Substrate's components keep the managed one |
| Cloud Run cold starts on the first request         | minimum instances of 1 on the service; snapshots cached in memory per instance                          |
| Bucket listing cost as snapshots accumulate        | hourly snapshots only, lifecycle 30 days; `latest.json` is one object per cluster                       |
| Terraform drift between projects                   | one module applied per cluster project with only the sink and the service accounts varying              |

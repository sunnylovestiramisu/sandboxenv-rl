# PRD: sandbox fleet site

Status: accepted (#223); last checked 2026-11-09 against `f10c68c`. Design:
[README.md](./README.md). Evidence: [research.md](./research.md).

## Problem

ate-env runs in several clusters and namespaces, each with its own `ate-env-api`, WorkerPools and
actor templates. Nobody can answer, without `kubectl` access to each cluster, which deployments
exist, at which version, how many environments each holds and in which state, whether a WorkerPool
has capacity, or where a slow step spent its time. The telemetry series gave every component metrics
and traces, and Substrate emits a per-transition actor event stream, but the signals land in
per-project backends with no fleet view, and the per-run explorer plan assumes a workstation and one
run at a time.

## Users

- Platform operators who need the fleet at a glance: clusters, deployments, versions, capacity,
  crash and wake-failure rates, and a stale reporter.
- RL engineers who need their namespace's environments and runs: counts by state, templates and
  images in use, run reports, and a trace for a slow task.
- Substrate and ate-env maintainers who need an actor's history: every transition with its time, the
  restore breakdown, and the trace that drove it.

## Scope

Read-only. One Cloud Run service behind IAP. Pages in the first release:

1. Fleet: clusters, namespaces, `ate-env-api` deployments with image and version, actors by state,
   WorkerPool desired and ready workers, reporter freshness.
2. Cluster: atespaces, templates and derived templates, tags, WorkerPools with capacity and
   allocation per worker, actors with filters by atespace, template, state and node.
3. Actor: identity, current state, transition history with timestamps and operations, crash record,
   usage samples when present, links to the trace of each transition.
4. Runs: trainer run reports from the bucket, the benchmark bundles `bench upload` writes (#270) by
   folder name, the HTML report, the span file, the Cloud Trace link.
5. Live: the most recent transitions across the fleet.

Behind the pages, a JSON API with the same data, so scripts and the benchmark tools can read it.

### Data paths

- Inventory: a reporter per cluster polling `ateapi` and the Kubernetes API, writing to GCS.
- Transitions: Substrate's actor event stream to Pub/Sub, then BigQuery and GCS.
- Metrics: Managed Prometheus for `ate-env-api`; Substrate's OTLP push; the site queries PromQL.
- Traces: the per-cluster collector exporting to Cloud Trace and to GCS.

## Non-goals

- Any write: no create, suspend, delete, scale, or template changes from the site.
- Dashboards and alerting beyond links to Cloud Monitoring; alert rules stay in Cloud Monitoring.
- A trace viewer: the site renders one run's waterfall from its span file and links to Cloud Trace
  for everything else.
- Per-actor logs: `kubectl ate logs` and Cloud Logging keep that job.
- Changes to Substrate code. The plan needs one Substrate configuration change and one AccessPolicy.

## Success

- Fleet page freshness: a new `ate-env-api` deployment or a WorkerPool resize appears within 10 s.
- Actor page: a transition appears within 10 s at the median and within 60 s at the 99th percentile
  on the Cloud Logging path, within 5 s on the OTLP path.
- Read-only: the site's service account holds viewer roles only, verified by an IAM policy test.
- Every actor identity on the site resolves to the same `ate.actor.uid` in Cloud Logging, Cloud
  Trace and the span files.
- Two clusters in two projects visible side by side in the first acceptance run.

## Risks

| Risk                                         | Effect                                  | Mitigation                                                                                               |
| -------------------------------------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Event records lost on the OTLP path          | a stale state with no sign              | `otlp,console` plus the poll as resync; the UI shows last poll time                                      |
| `ListActors` cost at large actor counts      | load on `ateapi`                        | page size 1000, interval configurable, reporter backs off on errors                                      |
| Tenancy: every user sees every namespace     | run names and images leak across teams  | IAP identity available; group-to-namespace filter is a follow-up, decided before the first external user |
| Substrate `viewer` role semantics            | the reporter may see less than expected | verify the role on a test atespace before building the reporter                                          |
| Cloud Trace not enabled in a cluster project | traces missing for that cluster         | the GCS span file is the fallback; the site shows which backend has the trace                            |

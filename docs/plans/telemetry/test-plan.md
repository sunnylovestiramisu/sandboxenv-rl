# Test plan: telemetry for sandboxenv-rl

Status: describes `main` at `3b03b03` (2026-10-29). Written for the tester role on the series'
branches; now the regression pass for a change that touches `ate_env_rl.telemetry`,
`ate_env_rl.observability`, `internal/telemetry`, the otelgrpc wiring, `clients/go/tracing.go` or
the swebench `Recorder`. Run the sections that match the change and put the results in the pull
request's Tests section; the repository keeps no result files.

## 0. Prerequisites

```bash
git fetch origin && git checkout <branch>
git log --oneline -1                                   # record the head in the PR

# two venvs: base and full
python3 -m venv /tmp/ate-base && /tmp/ate-base/bin/pip install -q -e 'clients/python[dev]'
python3 -m venv /tmp/ate-full && /tmp/ate-full/bin/pip install -q -e 'clients/python[dev,tracing,metrics]'

# cluster (sections 3 and 7 only)
alias K="kubectl -n ate-env"
alias KT="kubectl -n ate-env-telemetry"
K get deploy,svc                                        # ate-env-api Deployment and Service, a WorkerPool
kubectl -n ate-system get deploy                        # Substrate: ate-api-server, ate-controller, atenet-router
```

Snapshot what the pass must not disturb:

```bash
kubectl get ns -o name | sort > /tmp/before-namespaces.txt
kubectl -n ate-system get deploy -o name | sort > /tmp/before-ate-system.txt
```

Scripts (write them under `/tmp/ate-telemetry-tests/`; do not commit them):

- `quickstart_mock.py`:
  `FleetConfig(backend="mock", strategy="naive", max_concurrent=4, observability=ObservabilityConfig(enable_metrics=True, enable_tracing=True))`,
  20 tasks over 4 images, `process_fn` runs `handle.exec("echo hi")`, prints
  `fleet.report.summary()`.
- `quickstart_cluster.py`: same shape with `backend="substrate"`, `endpoint` = the port-forwarded
  `ate-env-api`, `auth_token` from the cluster's secret, 4 tasks, `OTEL_EXPORTER_OTLP_ENDPOINT`
  pointed at the collector port-forward.
- `overhead.py`: 200 tasks, mock backend, `max_concurrent=8`, `process_fn` a no-op; an argument
  selects off, report only, or tracing and metrics on (in-memory exporter); prints wall seconds.

## 1. Base install and no-op path (AC-1, AC-2)

```bash
/tmp/ate-base/bin/pip list | grep -iE 'opentelemetry|prometheus'          # expect nothing
/tmp/ate-base/bin/python -c "import ate_env_rl, ate_env_rl.observability, ate_env_rl.telemetry; print('ok')"
/tmp/ate-base/bin/python /tmp/ate-telemetry-tests/quickstart_mock.py 2>&1 | tee /tmp/ac2.log
grep -ciE 'tracing extra|metrics extra|opentelemetry|prometheus' /tmp/ac2.log   # expect one line per layer
```

Record the `dependencies` block of `clients/python/pyproject.toml` and the extras.

## 2. Span tree and wire propagation (AC-3, AC-4)

```bash
/tmp/ate-full/bin/python -m pytest clients/python/tests/test_telemetry_tracing.py -q -k 'nests or metadata' -s
```

Paste one printed span tree. Check the parent ids by hand for `ate_env_fleet.acquire` to
`ate_env_client.create_environment` and `ate_env_fleet.process` to `ate_env_client.exec`, in the
sync fleet, the async fleet and the `LoopRunner` path. Confirm the metadata test covers unary-unary,
unary-stream, stream-unary and stream-stream and the disabled case.

## 3. `ate-env-api` tracing and metrics on the cluster (AC-5, AC-6)

```bash
kubectl create ns ate-env-telemetry 2>/dev/null || true
KT apply -f tests/telemetry/otel-collector.yaml       # set METRICS_TARGET and FORWARD_ENDPOINT in it first
ate-env manifest --enable-tracing --otlp-endpoint http://otel-collector.ate-env-telemetry:4317 \
  --otlp-insecure --metrics-port 9090 ... | kubectl apply -f -
K get deploy ate-env-api -o jsonpath='{.spec.template.spec.containers[0].args}'   # expect -enable-tracing and -metrics-listen-addr=:9090
K rollout status deploy/ate-env-api
K port-forward svc/ate-env-api 7777:7777 >/tmp/pf-api.log 2>&1 &
K port-forward deploy/ate-env-api 9090:9090 >/tmp/pf-metrics.log 2>&1 &
KT port-forward deploy/otel-collector 4317:4317 >/tmp/pf-otel.log 2>&1 &
curl -s localhost:9090/metrics | grep -E '^ate_env_api_' | cut -d'{' -f1 | sort -u > /tmp/metrics-before.txt
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 OTEL_EXPORTER_OTLP_INSECURE=true \
  /tmp/ate-full/bin/python /tmp/ate-telemetry-tests/quickstart_cluster.py
curl -s localhost:9090/metrics | grep -E '^ate_env_api_(requests_total|create_to_ready_seconds_count|wake_wait_seconds_count)' > /tmp/metrics-after.txt
KT cp -c files deploy/otel-collector:/data ./otel-data
python benchmarking/telemetry_report/build_report.py --spans ./otel-data/spans.jsonl \
  --metrics ./otel-data/metrics.jsonl --out /tmp/report
```

In `./otel-data/spans.jsonl` find the `ate-env-fleet` span `ate_env_client.create_environment` and
the `ate-env-api` span `ateenv.v1alpha.EnvironmentService/CreateEnvironment`; record both trace ids
and the server span's parent id. If `ateapi.Control/*` spans with the same trace id are present, say
so; if not, record that Substrate does not export to this collector and mark AC-5 as passed at the
`ate-env-api` boundary.

Timings to record from the same run: `ate_env_fleet.acquire` p50 and max, `ate_env_client.exec` p50,
`ate_env_api_wake_wait_seconds` count and sum.

## 4. Report, registry, cardinality (AC-7, AC-8, AC-9)

```bash
/tmp/ate-full/bin/python -m pytest clients/python/tests/test_observability.py clients/python/tests/test_telemetry_metrics.py -q -s
/tmp/ate-full/bin/python - <<'PY'
import prometheus_client
before = set(prometheus_client.REGISTRY._names_to_collectors)
import ate_env_rl, ate_env_rl.observability
after = set(prometheus_client.REGISTRY._names_to_collectors)
print("registered at import:", sorted(after - before))        # expect []
PY
```

Paste one `summary()` print and the label names of every `ate_env_fleet_*` and `ate_env_client_*`
family (from `REGISTRY._names_to_collectors[...]._labelnames`).

## 5. Benchmarks and Go (AC-10, AC-11)

```bash
go test ./internal/benchmarking/swebench/... -run 'Event|Recorder|Golden' -v 2>&1 | tail -30
go test ./clients/go/... -run Trac -v 2>&1 | tail -30
go test ./internal/telemetry/... ./internal/apiservice/... ./cmd/ate-env-api/... 2>&1 | tail -10
```

For AC-10 the golden test is the check; to refresh the golden file on purpose run the test with
`-update` and review the diff.

## 6. Docs and CI (AC-12, AC-13)

```bash
go run ./hack/metrics-docs | diff -u docs/metrics.md - && echo "metrics docs fresh"
grep -n '^#' docs/observability.md
grep -n -iE 'tracing|metrics|extras|metrics-docs' .github/workflows/ci.yaml
gh pr checks <PR number> -R ai-on-gke/sandboxenv-rl
```

## 7. Blast radius and overhead (AC-14)

```bash
/tmp/ate-base/bin/python -m pytest clients/python/tests -q                 # all green, telemetry off
git diff --stat origin/main...HEAD | tail -5
for mode in off report full; do for i in 1 2 3; do /tmp/ate-full/bin/python /tmp/ate-telemetry-tests/overhead.py $mode; done; done
kubectl get ns -o name | sort | diff /tmp/before-namespaces.txt -           # only ate-env-telemetry added
kubectl -n ate-system get deploy -o name | sort | diff /tmp/before-ate-system.txt -
```

## 8. Property-based tests and proofs

[properties.md](./properties.md) specifies P-1 to P-20. What exists:

```bash
# AC-15 (P-9, P-10, P-11), #221, #238 to #240, #242, #276: 40 plain tests and the stateful test_warm_pool_accounting.
# hypothesis comes with the dev extra, so either venv works.
/tmp/ate-base/bin/python -m pytest clients/python/tests/test_warm_pool_accounting.py -q

# Dafny 4.11, from the pinned nixpkgs. CI's `Dafny models` job runs `make ci-dafny`, which checks
# these results against verify/README.md (hack/verify/dafny.py).
make ci-dafny

# Or by hand:
cd docs/plans/telemetry/verify
dafny verify telemetry.dfy                # 14 verified, 0 errors
dafny verify warm_pool.dfy                # 16 verified, 3 errors, all in WarmPoolBefore
dafny verify warm_pool_generations.dfy    # 32 verified, 3 errors: PlanFirstCovers, ClaimFailPutBackSafe, ClaimOkReturnsLive
dafny verify warm_pool_drain.dfy          # 65 verified, 10 errors: CommitStaleBeforeSafe, ResizeBeforeSafe, DeleteFailBeforeSafe, WarmCancelledBeforeSafe, WarmSuspendFailedBeforeSafe, WarmCommitDrainedBeforeSafe, RecycleSuspendCancelledBeforeSafe, DeleteCancelledSafe, ReleaseDeleteFailSafe, AcquireCommitBeforeSafe
```

The Dafny errors are by design: each failing lemma models code before #221 or one of its review
rounds, or an open gap ([verify/README.md](./verify/README.md)). CI fails if a result changes and
the README does not; when a change touches the modelled code, also say in the PR whether the models
still match it.

Still proposed, not in the repository:

- The other Hypothesis properties (P-1 to P-8, P-12, P-13, P-15 to P-18). Run them with Hypothesis'
  default profile (100 examples) in CI and a larger `max_examples` locally or nightly. Each shrunk
  counter-example goes into the PR's Tests section and becomes an `@example` regression.
- P-14 (`test_first_command_exactly_once`) with concurrent execs on one handle. It is expected to
  fail on `main` until `SandboxHandle.exec` claims the first-command flag inside the span.
- The Go properties (P-19, P-20), with
  `go test ./internal/benchmarking/swebench/... ./clients/go/... -run 'TracingIsTransparent|SpansFollowProvider'`
  after adding `pgregory.net/rapid` to `go.mod`.

## What to write in the pull request

Date, branch and head, venv contents (`pip freeze | grep -iE 'opentelemetry|prometheus|grpcio'`),
cluster context, then one row per acceptance criterion the change claims: pass or fail, the evidence
excerpt, the time it took. A timing table for sections 3 and 7.

## Failure triage hints

- Spans missing under `AsyncSandboxFleet` or the driver: the `LoopRunner` did not copy the caller's
  context; check that the coroutine runs under `contextvars.copy_context()`.
- `traceparent` on unary calls only: a stream interceptor kind is missing; `grpc.aio` dispatches one
  interceptor class per call kind.
- `Duplicated timeseries` on a second `Observer`: registration is not idempotent per registry;
  compare with `_metric` in `ate_env_rl.telemetry.metrics`.
- `ate-env-api` spans without a parent: the server is not reading incoming metadata
  (`otelgrpc.NewServerHandler` missing) or the client interceptor runs before the span is current.
- Collector shows client spans only: check `OTEL_EXPORTER_OTLP_INSECURE=true` on the server and the
  Service DNS name; `K logs deploy/ate-env-api | grep -i otlp`.
- A flaky server-span assertion: wait for the span, as #187 does, instead of sleeping.

## Cleanup

```bash
KT delete -f tests/telemetry/otel-collector.yaml; kubectl delete ns ate-env-telemetry
pkill -f 'port-forward (svc/ate-env-api|deploy/ate-env-api|deploy/otel-collector)'
```

Redeploy `ate-env-api` without the telemetry flags only if the cluster had them off before the pass.

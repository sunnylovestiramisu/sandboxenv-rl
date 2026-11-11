# Acceptance criteria: telemetry for sandboxenv-rl

Status: describes `main` at `c0c41ac` (2026-11-03). Each criterion names the test or evidence that
backs it on `main`. Commands are the ones to re-run; [test-plan.md](./test-plan.md) orders them.

Conventions:

- `$VENV_BASE` = a venv with `pip install -e 'clients/python[dev]'`; `$VENV_FULL` = a venv with
  `pip install -e 'clients/python[dev,tracing,metrics]'`. Run from the repository root.
- `$K` = `kubectl -n ate-env`; `$KT` = `kubectl -n ate-env-telemetry` on the acceptance cluster.
- "In-memory exporter" =
  `opentelemetry.sdk.trace.export.in_memory_span_exporter.InMemorySpanExporter` behind a
  `SimpleSpanProcessor` (the `spans` fixture in `clients/python/tests`).
- Names in backticks are the contracts in [interfaces.md](./interfaces.md); a criterion fails if the
  implementation uses another name.
- "Properties (proposed)" names the properties in [properties.md](./properties.md) that would
  validate a criterion over generated inputs. Those tests are not in the repository. "Properties"
  names the ones whose tests are.

| ID    | Title                                                                               | PR                                 | Status at `ae29368` with #242                                                                                                                         |
| ----- | ----------------------------------------------------------------------------------- | ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| AC-1  | Base install carries no telemetry dependency and imports clean                      | 1, 2                               | pass: `pyproject.toml`, `test_registry_import_does_not_load_prometheus`                                                                               |
| AC-2  | Telemetry enabled without the extras is a logged no-op                              | 1, 2                               | pass: `test_missing_extra_logs_one_error`, `test_observer_without_layers_is_report_only`                                                              |
| AC-3  | One in-process trace from `ate_env_fleet.run` to `ate_env_client.*`, sync and async | 1, 2                               | partial: `test_nests_*` in `test_telemetry_tracing.py`; 3.4 open (P-14)                                                                               |
| AC-4  | The W3C context is on the wire for all four gRPC call kinds                         | 1                                  | pass: `test_metadata_carries_traceparent_on_all_call_kinds`, `test_metadata_has_no_traceparent_when_disabled`                                         |
| AC-5  | `ate-env-api` continues the client trace and exports it                             | 3                                  | pass: `internal/apiservice/telemetry_test.go`; #114 and #186 cluster runs                                                                             |
| AC-6  | `ate-env-api` serves the documented `ate_env_api_*` families                        | 3                                  | pass: `internal/telemetry/metrics_test.go`, `cmd/ate-env-api/metrics_test.go`, `docs/metrics.md`                                                      |
| AC-7  | `RunReport` has the agent-sandbox-rl shape                                          | 2                                  | met by #279: `test_report_shape_and_summary_order`, `test_report_summary_order_has_reap`, `test_report_phase_bounds_hold_in_to_dict_for_short_phases` |
| AC-8  | Prometheus series register lazily with exactly the documented labels                | 2                                  | pass: `test_registry_*` in `test_observability.py`, `test_telemetry_metrics.py`                                                                       |
| AC-9  | Labels stay bounded under many images and environments                              | 2                                  | pass: `test_cardinality_*`                                                                                                                            |
| AC-10 | swebench loader JSONL is unchanged and every event has a span                       | 5                                  | pass: `events_test.go` with `testdata/events.golden.txt`                                                                                              |
| AC-11 | Go client: provider injection, span names, no exporter by default                   | 4                                  | pass: `clients/go/tracing_test.go`                                                                                                                    |
| AC-12 | Docs: `docs/observability.md`, generated `docs/metrics.md`, CI freshness check      | 5                                  | pass: CI step `docs/metrics.md is generated from internal/telemetry`                                                                                  |
| AC-13 | CI runs the suites with and without the extras; lint and boilerplate pass           | 1, 2, 5                            | pass: `.github/workflows/ci.yaml`                                                                                                                     |
| AC-14 | Blast radius: existing tests pass unchanged; telemetry-off overhead under 2%        | all                                | partial: suites green; overhead not measured                                                                                                          |
| AC-15 | Warm-pool accounting under concurrent acquire, recycle, `warm_pool` and teardown    | #221, #238, #239, #240, #242, #276 | pass: 15.1 to 15.10 (`test_warm_pool_accounting.py`, `verify/*.dfy`)                                                                                  |
| AC-16 | The span and attribute inventory in `interfaces.md` matches the code, checked in CI | follow-up                          | not met: the table is hand-written; last checked against `c0c41ac`                                                                                    |
| AC-17 | The Go and Python clients emit the same span names and keys for shared operations   | 7                                  | partial: 17.3, 17.4 on shared vectors (#329); 17.1, 17.2 on per-client tests                                                                          |
| AC-18 | No command line, secret or file content in any span attribute value                 | follow-up                          | pass: example tests and property tests in both clients (#341)                                                                                         |
| AC-19 | Several fleets in one process share the warm-replica gauge                          | 2                                  | pass: `test_warm_gauge_adds_up_across_observers_and_resets_only_own_share`                                                                            |
| AC-20 | CI verifies the Dafny models with the documented counts                             | follow-up                          | pass: `Dafny models` job, `hack/verify/dafny.py`                                                                                                      |

## AC-1: Base install carries no telemetry dependency

- Commands: in `$VENV_BASE`: `pip list | grep -iE 'opentelemetry|prometheus'` and
  `python -c "import ate_env_rl, ate_env_rl.observability, ate_env_rl.telemetry; print('ok')"`.
- Expected: the grep prints nothing; the import prints `ok`.
- Pass rule: both as expected; `clients/python/pyproject.toml` base `dependencies` lists only
  `grpcio` and `protobuf`; the extras `tracing` and `metrics` exist.
- On `main`: `dependencies = ["grpcio>=1.84", "protobuf>=7.35"]`; extras `tracing` (three
  `opentelemetry-*>=1.43` packages) and `metrics` (`prometheus-client>=0.20`).

## AC-2: Telemetry enabled without the extras is a no-op

- Command: in `$VENV_BASE`, run the mock-backend quickstart from the test plan with
  `FleetConfig(tracing=TracerConfig(enable_tracing=True), observability=ObservabilityConfig(enable_metrics=True))`.
- Expected: the run completes; one ERROR for the missing `tracing` extra and one WARNING for the
  missing `metrics` extra; `fleet.report.summary()` still prints.
- Pass rule: no exception, the report exists, each message appears once per process.
- Properties (proposed): P-12.

## AC-3: One in-process trace, sync and async

- Command: `pytest clients/python/tests/test_telemetry_tracing.py -k nests` in `$VENV_FULL`.
- Expected: with the in-memory exporter, `SandboxFleet.run` on the mock backend yields
  `ate_env_fleet.run` to `ate_env_fleet.acquire` to `ate_env_client.create_environment`, and
  `ate_env_fleet.process` to `ate_env_client.exec`, each child's `parent.span_id` equal to its
  parent's `span_id`; the same tree under `AsyncSandboxFleet`, through `SubstrateBackendDriver`'s
  `LoopRunner` path against the threaded fake server, and under a user span that parents the run.
- Pass rule: the assertions hold for the sync, async, runner and user-span cases.
- First command: the first exec started on an acquired sandbox, and only that one, carries
  `ate_env.first_command=true`.
  `pytest clients/python/tests/test_telemetry_tracing.py -k first_command` covers sequential execs.
  With execs started concurrently on one handle the marked span is not always the first started,
  because `SandboxHandle.exec` sets the flag before the span starts (P-14, open on `main`).
- Status: partial. 3.1 to 3.3 and 3.5 pass; 3.4 (first command under concurrent execs) is open.
- Properties (proposed): P-13, P-14.

## AC-4: The W3C context is on the wire

- Command: `pytest clients/python/tests/test_telemetry_tracing.py -k 'metadata or baggage'`.
- Expected: the recorder server sees request metadata for one unary-unary, unary-stream,
  stream-unary and stream-stream call; each carries `traceparent` matching the client's current span
  and no `baggage` key; with tracing disabled no `traceparent` is sent.
- Pass rule: four call kinds covered, both states.
- `inject_metadata` keeps the caller's metadata unchanged and in order, and does not add a carrier
  key the caller already set (compared case-insensitively). The code does this; no test on `main`
  covers a caller-set `traceparent`.
- Properties (proposed): P-4, P-5, P-6, P-7, P-15.

## AC-5: `ate-env-api` continues the client trace and exports it

- Setup: `ate-env-api` deployed with
  `ate-env manifest --enable-tracing --otlp-endpoint http://otel-collector.ate-env-telemetry:4317 --otlp-insecure --metrics-port 9090`;
  the collector from `tests/telemetry/otel-collector.yaml`.
- Command: run the quickstart from `$VENV_FULL` with the same `OTEL_*` variables and
  `ObservabilityConfig(enable_tracing=True)`; then
  `$KT cp -c files deploy/otel-collector:/data ./otel-data` and read `./otel-data/spans.jsonl`, or
  `$KT logs deploy/otel-collector | grep -A3 'Trace ID'`.
- Expected: spans from service `ate-env-fleet` (`ate_env_fleet.acquire`,
  `ate_env_client.create_environment`) and from service `ate-env-api`
  (`ateenv.v1alpha.EnvironmentService/CreateEnvironment`) share one trace id; the server span's
  parent id is the client span id. When Substrate exports to the same collector, `ateapi.Control/*`
  spans share the trace id; otherwise record that the join stops at `ate-env-api`.
- Pass rule: one trace id across client and server spans, parent id matching.
- Unit coverage on `main`: `TestTracingContinuesClientTrace` asserts the server span's parent from
  incoming metadata; `TestCreateToReadyUsesRequestedAtHeader` and `TestWakeWaitOutcomes` cover the
  two histograms.

## AC-6: `ate-env-api` serves the documented metric families

- Command: `$K port-forward deploy/ate-env-api 9090:9090 &` then
  `curl -s localhost:9090/metrics | grep -E '^ate_env_api_' | cut -d'{' -f1 | sort -u`.
- Expected: exactly `ate_env_api_requests_total`, `ate_env_api_request_duration_seconds`,
  `ate_env_api_create_to_ready_seconds`, `ate_env_api_wake_wait_seconds`, `ate_env_api_environments`
  and `ate_env_api_build_info` (with the `_bucket`, `_sum` and `_count` series of the histograms);
  after one quickstart run, `requests_total` has increased for `CreateEnvironment` and
  `DeleteEnvironment` and `create_to_ready_seconds_count` is at least 1.
- Pass rule: the family set matches [docs/metrics.md](../../metrics.md); counters moved.

## AC-7: `RunReport` has the agent-sandbox-rl shape

- Status: met by #279. `main` at `beedf88` failed the bound in `to_dict()`: it rounded `total_s` and
  `max_s` to 0.01 separately, so 3 phases of 0.004 s reported `total_s` 0.01 and `max_s` 0.0 (#273).
  `max_s` now rounds up and `total_s` is kept in `[max_s, count * max_s]`.
- Command: `pytest clients/python/tests/test_observability.py -k "report or phase_stats_consistent"`
  and `pytest clients/python/tests/test_release_errors.py -k report`.
- Expected: `to_dict()` keys are exactly
  `strategy, total_s, phases, claims, tasks_ok, tasks_err, release_errors, warm_replicas_total, warm_replicas_peak, environment`;
  `phases` holds `count, total_s, max_s` per phase; `summary()` prints one row per phase in
  `RunReport.ORDER` and a `TOTAL` row, with the rounded `to_dict()` values; negative durations clamp
  to 0, so every phase keeps `0 <= max_s <= total_s <= count * max_s`, in `phases` and in the
  rounded `to_dict()` values; each `add_release_error()` adds 1 to `release_errors`, and `summary()`
  prints a `release_errors=N` line while it is nonzero; `write()` produces the `.txt` and `.json`
  pair without overwriting.
- Pass rule: assertions hold.
  `test_release_errors.py::test_failed_delete_does_not_raise_and_is_counted` checks the
  `release_errors=N` summary line.
- Properties: P-8 (`test_phase_stats_consistent`); proposed: P-16.

## AC-8: Prometheus series register lazily with the documented labels

- Command: `pytest clients/python/tests/test_observability.py -k registry` and
  `pytest clients/python/tests/test_telemetry_metrics.py`.
- Expected: importing `ate_env_rl` and `ate_env_rl.observability` adds nothing to
  `prometheus_client.REGISTRY` and does not import `prometheus_client`; constructing an `Observer`
  with metrics on registers the `ate_env_fleet_*` families once; a second `Observer` and a new
  registry do not raise or lose samples; each family's label names are exactly those in
  [interfaces.md](./interfaces.md); `ate_env_client_*` histograms register on first use only;
  `serve_metrics(port)` answers `/metrics`.
- Pass rule: assertions hold.
- Properties (proposed): P-17.

## AC-9: Labels stay bounded

- Command: `pytest clients/python/tests/test_observability.py -k cardinality`.
- Expected: a mock run over 50 distinct images from 5 repo families and 200 tasks produces at most 5
  distinct `family` values; no label value contains an environment id, actor id, run id or image
  tag; no label named `env_id`, `instance`, `image` or `run_id` on any family; a repo named `other`
  or `unknown` does not collide with the overflow and no-family values; more than `MAX_FAMILIES`
  families collapse into `other`. The registry holds at most `MAX_FAMILIES` (64) names, and a
  process emits at most `MAX_FAMILIES + 2` distinct `family` values: the registered families,
  `other` and `unknown`, neither of which counts toward the cap.
- Pass rule: assertions hold.
- Properties (proposed): P-1, P-2, P-3, P-18.

## AC-10: swebench loader JSONL unchanged, one span per event

- Command: `go test ./internal/benchmarking/swebench/... -run 'Event|Recorder|Golden' -v`.
- Expected: the `Recorder` output for the fixed event sequence equals `testdata/events.golden.txt`
  byte for byte with tracing off and on; with a test exporter, each event yields one span named
  after `Event.Event`, start `ts - dur_ms` and end `ts`, status `Ok` when `OK`, else `Error` with
  `Err` (or `not ok`). Attributes: `run`, `step` and `task` always; `job`, `worker`, `env`,
  `instance` and `name` when non-empty; `attempts` and `count` when above 0; `exit` and `reward`
  when set.
- Pass rule: golden file identical; span count equals event count.
- Properties (proposed): P-19.

## AC-11: Go client provider injection and span names

- Command: `go test ./clients/go/... -run Trac -v`.
- Expected: with `ClientOptions{TracerProvider: tp}` and an in-memory exporter, calls produce
  `ate-env-go-client.create_environment`, `.exec`, `.read_file`, `.write_file` with the `Attr*`
  attributes; without a provider no span is recorded and no exporter is constructed;
  `TraceServiceName` changes the prefix; a caller-supplied `GRPCConn` still carries `traceparent`.
- Pass rule: assertions hold.
- Properties (proposed): P-20.

## AC-12: Docs

- Commands: `go run ./hack/metrics-docs | diff -u docs/metrics.md -`; open
  [docs/observability.md](../../observability.md).
- Expected: the diff is empty; the guide covers enabling flags and variables, what nests under what
  with a span-tree example, the metric families or a link to `docs/metrics.md`, the cardinality
  rule, the run report format and phase glossary, the collector and Cloud Trace routes, and the HTML
  report tool; `clients/python/README.md` has an Observability section; `CHANGELOG.md` lists the
  user-visible additions.
- Pass rule: all present; CI runs the diff.

## AC-13: CI

- Expected: `.github/workflows/ci.yaml` runs the Python client suite once with `[dev]` and once with
  `[dev,tracing,metrics]`, `ruff check`, the boilerplate check, `go test ./...`, and the
  metrics-docs diff; all green on each PR. The job names `Go build, vet, test and license headers`
  and `Python client and integration tests` stay as the `main` ruleset requires.
- Pass rule: the workflow shows the steps; the PR's checks are green.

## AC-14: Blast radius and overhead

- Commands: `pytest clients/python/tests -q` in `$VENV_BASE` (telemetry off); the overhead script
  from the test plan (200 tasks, mock backend, `max_concurrent=8`) three times each with telemetry
  off, with the report only, and with tracing and metrics on into the in-memory exporter.
- Expected: every pre-existing test passes unchanged; the three wall-clock medians are recorded.
- Pass rule: the telemetry-off median is within 2% of the same run on the parent of `4f3f51d` (#93,
  the series' first commit), and the report-only median within 5% of telemetry off.
- Status: the suites are green in CI on every PR of the series. No overhead measurement is recorded
  in the pull requests; the procedure stays in [test-plan.md](./test-plan.md) section 7.

## AC-15: Warm-pool accounting under concurrency

`SubstrateBackendDriver` coroutines share one event loop and interleave at every `await`.

- Status: met by #221. Added after the series from a model of the driver's warm-pool bookkeeping;
  `main` at `9b2148a` failed it.
- 15.1: after every step of any interleaving of acquire, recycle, `warm_pool` and teardown, the
  `ate_env_fleet_warm_replicas{backend}` gauge equals the number of actors in the driver's warm
  pool.
- 15.2: a recycle, `warm_pool` call or failed warm claim paused in an `await` while teardown runs
  deletes its actor when it resumes; after teardown no environment from the run is alive on
  Substrate or tracked by the driver. Met by #276 for a cold `acquire` cancelled while its
  `create()` is in flight (by its own `timeout_s` or by its caller): the driver owns the actor under
  `WARM_POOL_RUN_ID`, so any run's reap deletes it, and if a reap ran since the acquire read
  `_teardowns`, starts its delete in a background task; a teardown that already runs does not wait
  for it. Before it, an actor whose create landed stayed alive and untracked (#273).
- 15.3: overlapping `warm_pool(template, replicas=R)` calls create at most `R` warm actors in total
  for one pool key.
- 15.4: `unwarm_pool`, and a `resize_pool` below what is pooled plus in flight, cancel the
  `warm_pool` calls in flight for that pool: they delete the actors they create instead of pooling
  them, and a `warm_pool` call after the cancel is not counted against the cancelled ones.
- 15.5: an actor whose delete fails after a teardown, or after `unwarm_pool` or `resize_pool`
  drained it, stays tracked by the driver, and the next `reap` of any run deletes it. This covers
  the deletes that start after the teardown (`warm_pool`, a failed warm claim, a recycle that saw
  the teardown, a drain); 15.9 covers the rest.
- 15.6: a direct `reap` (outside a fleet, with no `warm_reset`) leaves the gauge equal to the pool.
- 15.7: a warm claim returns a live environment, and leaves no actor it popped alive and unpooled.
  Met by #239: a claim whose `get()` succeeds after an overlapping reap deletes the popped actor if
  it is still owned and claims or creates another; before it, the claim returned the deleted one
  (`ClaimOkReturnsLive`).
- 15.8: a `warm_pool` call cancelled during its `create()` calls, or one of whose actors fails to
  suspend, leaves no live actor untracked. Met by #240: a cancelled call owns the actors it sent a
  `create()` for under `WARM_POOL_RUN_ID`, so any run's reap deletes them, and deletes them itself
  if a reap ran since it planned (that reap may have missed them), and an actor whose `suspend()`
  fails is owned the same way and deleted at once; before it, those actors leaked.
- 15.9: a delete that is cancelled after the run's reap, or a `release` whose delete fails or is
  cancelled while a reap runs, also leaves the actor for the next `reap` of any run. Met by #238;
  before it both kept the reaped run id, so no later reap deleted the actor (`DeleteCancelledSafe`,
  `ReleaseDeleteFailSafe`).
- 15.10: an `acquire` whose cold create overlaps its own run's reap leaves its environment where a
  later `reap` finds it. Met by #242: the driver owns that environment under `WARM_POOL_RUN_ID` (the
  caller's handle keeps its run id, and its release deletes the environment as usual); before it,
  the environment stayed under the reaped run id, and if the caller never released it, `reap` of a
  later run skipped it (`AcquireCommitBeforeSafe`). Found in review of #239.
- Commands: in `$VENV_BASE` (the `dev` extra carries `hypothesis`):
  `python -m pytest clients/python/tests/test_warm_pool_accounting.py`; with Dafny 4.11:
  `dafny verify docs/plans/telemetry/verify/warm_pool.dfy`,
  `dafny verify docs/plans/telemetry/verify/warm_pool_generations.dfy` and
  `dafny verify docs/plans/telemetry/verify/warm_pool_drain.dfy`.
- Expected: the pytest run passes (40 plain tests and the stateful `test_warm_pool_accounting`).
  15.2: `test_cold_acquire_cancelled_in_create_leaves_the_actor_for_any_reap` (cancelled by
  `timeout_s`, and by the caller),
  `test_cold_acquire_cancelled_in_create_after_its_runs_reap_deletes_the_actor` and the machine's
  `start_acquire`, `finish_acquire` and `cancel_acquire` rules. 15.4:
  `test_unwarm_cancels_a_warm_pool_in_flight`,
  `test_shrinking_resize_cancels_a_warm_pool_in_flight`,
  `test_warm_pool_after_unwarm_ignores_the_cancelled_call` and the machine's `unwarm` rule. 15.5:
  `test_failed_delete_after_teardown_leaves_warm_actors_for_any_later_reap`,
  `test_failed_recycle_delete_after_teardown_leaves_the_actor_for_any_later_reap`,
  `test_failed_delete_of_drained_actors_keeps_them_for_the_next_reap`. 15.6:
  `test_direct_reap_moves_the_gauge_with_the_pool`. 15.7:
  `test_warm_claim_overlapping_a_reap_returns_a_live_environment` (reap finished or still running;
  popped actor matched by the reap, `run-0`, or skipped by it, `run-1`; in the still-running cases
  the gate pauses the reap's delete of the popped actor for `run-0`, and the claim's own delete of
  it for `run-1`, where the reap deletes nothing). 15.8:
  `test_cancelled_warm_pool_leaves_its_actors_for_any_reap` (which also checks the reap counts only
  the 2 actors that exist), `test_warm_pool_cancelled_in_suspend_leaves_its_actors_for_any_reap`,
  `test_warm_pool_cancelled_just_before_the_runs_reap_deletes_its_actors`,
  `test_warm_actor_whose_suspend_fails_is_deleted_or_left_for_any_reap` (delete succeeds, and
  fails). 15.9: `test_cancelled_delete_after_reap_leaves_warm_actors_for_any_later_reap`,
  `test_release_delete_lost_during_reap_leaves_the_actor_for_any_later_reap` (failed and cancelled),
  `test_recycle_cancelled_in_suspend_after_a_reap_leaves_the_actor_for_any_later_reap`,
  `test_warm_pool_cancelled_before_its_deletes_start_leaves_actors_for_any_later_reap`, and
  `test_release_delete_failure_without_a_reap_keeps_the_run_id` for the path that must not change.
  15.10: `test_cold_acquire_overlapping_its_runs_reap_leaves_the_actor_for_any_later_reap` (reap
  finished, and still running), and
  `test_cold_acquire_keeps_its_run_id_without_an_overlapping_reap_of_its_run` (another run's reap
  during the create, or the run's own reap before it) for the paths that must not change.
  `warm_pool.dfy`: 16 verified, 3 errors, all in `WarmPoolBefore` (the code before #221).
  `warm_pool_generations.dfy`: 32 verified, 3 errors, in `PlanFirstCovers` and
  `ClaimFailPutBackSafe` (#221's first version) and `ClaimOkReturnsLive` (the code before 15.7's
  fix). `warm_pool_drain.dfy`: 65 verified, 10 errors, in `CommitStaleBeforeSafe`,
  `ResizeBeforeSafe` and `DeleteFailBeforeSafe` (the code before 15.4 and 15.5),
  `WarmCancelledBeforeSafe` and `WarmSuspendFailedBeforeSafe` (before 15.8's fix) and
  `WarmCommitDrainedBeforeSafe`, `RecycleSuspendCancelledBeforeSafe`, `DeleteCancelledSafe` and
  `ReleaseDeleteFailSafe` (the code before 15.9's fix) and `AcquireCommitBeforeSafe` (before 15.10's
  fix). [verify/README.md](./verify/README.md) explains each.
- Pass rule: pytest green; Dafny output matches. CI runs both (the Dafny check is AC-20).
- Status: 15.1 to 15.10 pass. The 15.9 tests fail on the code before #238, the 15.7 test on the code
  before #239, the 15.8 test on the code before #240, the 15.10 test on the code before #242, the
  15.2 cancelled-acquire tests on the code before #276.
- Properties: P-9 (15.1, 15.6), P-10 (15.2, 15.5, 15.8, 15.9, 15.10), P-11 (15.3, 15.4), P-24
  (15.7).

## AC-16: The span inventory matches the code

- 16.1: a test drives every `Client`, `SandboxHandle`, `Env` and `SubstrateBackendDriver` operation,
  and a fleet run, into the in-memory exporter with tracing on, and collects each span name with the
  attribute keys it carried.
- 16.2: the collected set equals the span table in [interfaces.md](./interfaces.md) (or a generated
  file that the table links to), and CI fails on a difference, as it does for `docs/metrics.md`
  (AC-12).
- Status: not met. The table is hand-written; review of #220 found three entries the code never
  emitted (`ate_env_client.wake`, `ate_env.env_id`, mock-driver spans) and two spans missing
  (`ensure_template`, `warm_pool`). `ef307ed` corrected it by hand against `c0c41ac`.
- Properties: P-13.

## AC-17: The Go and Python clients agree

- 17.1: for each operation both clients have (`create_environment`, `get_environment`,
  `suspend_environment`, `delete_environment`, `get_server_info`, `get_image_status`,
  `ensure_image_template`, `list_placement_domains`, `warm_image`, `start_process`, `exec`,
  `read_file`, `write_file`), the span names are `<service>.<op>` with the same `<op>`.
- 17.2: for the same inputs, both clients record the same `ate.*` and `ate_env.*` keys and values,
  except the differences [decisions.md](./decisions.md) records (`ate_env.run_id` is Python
  fleet-only; `ate_env.timed_out` and the `exec` span name pair `Env.Run` with `SandboxHandle.exec`
  while Python's direct `Env.shell` omits `ate_env.timed_out`; Go's `Create`, `ImageStatus` and
  `EnsureImageTemplate` also backfill server-resolved `ate.template.*` from the response).
- 17.3: under each `OTEL_SEMCONV_STABILITY_OPT_IN` value (unset, `rpc/old`, `rpc/dup`), both clients
  record the same `rpc.*`, `server.address` and `server.port` keys and values for the same call, and
  the `rpc.*` keys and values are the ones otelgrpc records for that value.
- 17.4: `executable` (Go) and `executable_attr` (Python) agree on every case of one shared table.
- Command: `go test ./clients/go -run Parity` and
  `pytest clients/python/tests/test_parity_vectors.py`, which read the same files in
  `proto/testdata/client-parity/`: `semconv_mode.json` and `rpc_attrs.json` for 17.3,
  `executable.json` for 17.4.
- Status: partial. 17.3 and 17.4 pass on the shared vectors (#329): a changed value fails a test in
  each suite. 17.1 and 17.2 rest on per-client tests (`TestTracingCallerConnSpansMatchPythonClient`,
  `test_client_rpc_spans_are_client_kind_with_rpc_attributes`) that can drift apart.
- Properties: P-22.

## AC-18: No secrets or content in attribute values

- 18.1: for generated command lines (leading `NAME=value` and `NAME+=value` assignments, quoting,
  `sh -c` strings, tokens in arguments), `ate_env.command.executable` is the executable's base name
  or absent, never an argument or an assignment value, in both clients.
- 18.2: no span attribute value contains file content, an auth token, or an image tag
  (`image_ref_attr` keeps the repository and digest and drops the tag).
- Status: pass. Example tests: `test_executable_attr_skips_env_assignments` and the
  `executable.json` vector. Property tests in both clients (`TestExecutableNeverRecordsAssignments`,
  `test_executable_never_records_assignments_prop`) draw assignment values and redirection prefixes
  with spaces inside `$(...)`, `${...}`, backticks and `$'...'` (#341), which an expansion-unaware
  split leaked; an assignment or redirection target whose value holds `$` or a backtick records
  nothing.
- Properties: P-21.

## AC-19: Several fleets share the warm-replica gauge

- 19.1: with several `Observer`s in one process, `ate_env_fleet_warm_replicas{backend}` is the sum
  of their pools.
- 19.2: one `Observer`'s `warm_reset` removes only its own share.
- Command: `pytest clients/python/tests/test_observability.py -k across_observers` in `$VENV_FULL`.
- Status: pass (`test_warm_gauge_adds_up_across_observers_and_resets_only_own_share`).
- Properties: P-23.

## AC-20: The Dafny models verify in CI

- 20.1: a CI step runs `dafny verify` on each file in [verify/](./verify/) with the version the
  README names, and fails unless the verified and error counts, and the failing lemmas, are the ones
  [verify/README.md](./verify/README.md) lists.
- Command: `make ci-dafny` (the `.#dafny` devShell runs `hack/verify/dafny.py`).
- Expected: `telemetry.dfy` 14 verified, 0 errors; `warm_pool.dfy` 16 verified, 3 errors;
  `warm_pool_generations.dfy` 32 verified, 3 errors; `warm_pool_drain.dfy` 65 verified, 10 errors;
  the failing lemmas are the ones the README's table lists; the script prints
  `All 4 Dafny models match docs/plans/telemetry/verify/README.md.`
- Status: pass. CI's `Dafny models` job runs the check with the pinned nixpkgs' Dafny 4.11.0.
  Changing a README count, making a listed lemma verify, or adding an unlisted `.dfy` file each
  fails it (checked locally on copies of the directory).

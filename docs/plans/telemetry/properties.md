# Properties: telemetry for sandboxenv-rl

Status: checked against `main` at `c0c41ac` (2026-11-03), which includes #221's fix for AC-15. Most
property tests here are proposed and not in the repository: `pgregory.net/rapid` is not in `go.mod`,
and the Hypothesis tests named "Proposed test" do not exist. Three parts exist:

- `clients/python/tests/test_warm_pool_accounting.py` (#221): the P-9 to P-11 stateful test
  `test_warm_pool_accounting` and 40 plain interleaving tests. `hypothesis` is in the `dev` extra
  since #221.
- [verify/](./verify/README.md): hand-written Dafny models of P-1 to P-11 and of the two bugs review
  found in #221's first version. CI checks their results (AC-20).
- `clients/python/tests/test_observability.py` (#279): the P-8 Hypothesis test
  `test_phase_stats_consistent`.

The tests named as "example coverage on `main`" exist and run in CI.

Each property is a universal statement, _"For any …"_, over a range of generated inputs. It
validates one or more requirements, tagged **Validates: AC-n.m**. The requirement numbers are listed
in the [requirement index](#requirement-index); they refine the criteria in
[acceptance-criteria.md](./acceptance-criteria.md). A property-based test shrinks a failing input to
a minimal counter-example, which then becomes an `@example` regression.

State on `main` with #221:

| Item            | Requirement  | State                                                                                                                                                                                                         |
| --------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P-9, P-10, P-11 | AC-15        | Fixed by #221. Before it, a recycle or `warm_pool` that awaited across a reap pooled its actor after the teardown, and overlapping `warm_pool` calls each provisioned the full shortfall.                     |
| P-14            | AC-3.4       | Open. `SandboxHandle.exec` and `exec_async` (`clients/python/src/ate_env_rl/handle.py`) call `_note_first_command()` before `_exec_span` starts the span, so another thread's exec span can start first.      |
| P-24            | AC-15.7      | Fixed by #239. Before it, a warm claim whose `get()` succeeded after an overlapping reap deleted the actor returned a deleted environment.                                                                    |
| P-10            | AC-15.8      | Fixed by #240. Before it, a `warm_pool` call cancelled during its `create()` calls leaked the actors already created: they were neither pooled nor tracked.                                                   |
| P-10            | AC-15.9      | Fixed by #238. Before it, a delete cancelled after the run's reap, or a `release` whose delete failed or was cancelled while a reap ran, left the actor under the reaped run id, so no later reap deleted it. |
| P-10            | AC-15.2      | Fixed by #276. Before it, a cold `acquire` cancelled while its `create()` was in flight (by its own `timeout_s` or its caller) left an actor whose create landed alive and untracked, so no reap deleted it.  |
| P-10            | AC-15.10     | Fixed by #242. Before it, an `acquire` whose cold create overlapped its own run's reap returned an environment owned under the reaped run id; unless the caller released it, no later reap deleted it.        |
| P-21, P-22      | AC-17, AC-18 | Partial. Example tests only; the Go tests copy the Python tables by hand.                                                                                                                                     |

## Labels and cardinality (AC-9)

### P-1: Family registry stays bounded

_For any_ stream of tasks and images, the family registry holds at most `MAX_FAMILIES` (64) names,
and at most `MAX_FAMILIES + 2` distinct `family` values are emitted: the registered families,
`other` (overflow) and `unknown` (unparseable input). Neither `other` nor `unknown` counts toward
the cap.

**Validates: AC-9.1**

- Generators: `st.lists(task_or_image(), max_size=500)`, where `task_or_image()` mixes tasks with
  `metadata["repo"]` from `st.from_regex(r"[A-Za-z0-9_.-]{1,40}/[A-Za-z0-9_.-]{1,40}")`, SWE-bench
  image names (`sweb.eval.{x86_64,arm64}.<repo>__<repo>-<n>:<tag>`), the reserved names, and
  arbitrary text.
- Proposed test: Hypothesis `test_family_registry_bounded`.
- Example coverage on `main`: `test_family_label_is_capped`,
  `test_cardinality_bounded_labels_over_many_images`.

### P-2: Labels are the family or `other`

_For any_ task or image, the `family` label is the input's repo family after escaping, or `other`,
or `unknown` when no family can be parsed. Escaping appends `_repo` to a family matching `other` or
`unknown` followed by zero or more `_repo` (`other` to `other_repo`, `other_repo` to
`other_repo_repo`), so the mapping stays one-to-one.

**Validates: AC-9.2**

- Generators: as P-1, weighted toward the reserved names, their `_repo` forms and case variants.
- Proposed test: Hypothesis `test_family_label_shape`.
- Example coverage on `main`: `test_cardinality_reserved_repo_names`,
  `test_cardinality_escaped_repo_names_stay_unique`.

### P-3: `other` only on overflow, and labels are stable

_For any_ stream of tasks, `other` is returned only when the registry is full and the family is new.
The bare reserved names never enter the registry. A family seen once keeps the same label for the
rest of the process.

**Validates: AC-9.3, AC-9.4**

- Generators: as P-1, with streams longer than 64 distinct families, and repeats.
- Proposed test: Hypothesis `test_other_only_on_overflow`.
- Example coverage on `main`: `test_cardinality_full_registry_only_new_families_get_other`.

### P-18: No identifiers in label values

_For any_ task, image, environment id, actor id and run id, no emitted label value contains the
environment id, actor id, run id or image tag, and no family defines a label named `env_id`,
`instance`, `image` or `run_id`.

**Validates: AC-9.5**

- Generators: `st.uuids()`-based ids, `st.from_regex(r"sb-[0-9a-f]{16}")`, image tags from
  `st.from_regex(r"[A-Za-z0-9_.-]{1,30}")`, tasks as P-1; a mock run over them.
- Proposed test: Hypothesis `test_labels_carry_no_ids` (scrapes the registry after the run).
- Example coverage on `main`: `test_cardinality_bounded_labels_over_many_images`.

## W3C context propagation (AC-4)

P-4 to P-7 are properties of `inject_metadata` (`ate_env_rl.telemetry.tracing`).

### P-4: Caller metadata is preserved

_For any_ caller metadata (any keys, any case, any values) and any current span, the output of
`inject_metadata` starts with the caller's metadata, unchanged and in order.

**Validates: AC-4.2**

- Generators: `st.lists(st.tuples(header_key(), st.text()), max_size=10)`, where `header_key()`
  includes `traceparent`, `TraceParent`, `baggage` and random keys; tracing on or off.
- Proposed test: Hypothesis `test_inject_preserves_metadata`.

### P-5: Only trace headers are added

_For any_ caller metadata, every entry `inject_metadata` appends is a carrier entry (`traceparent`
or `tracestate`).

**Validates: AC-4.1, AC-4.3**

- Generators: as P-4.
- Proposed test: Hypothesis `test_inject_adds_only_carrier`.

### P-6: `baggage` is never added

_For any_ caller metadata and any OpenTelemetry context, including one carrying baggage, the output
contains `baggage` only if the caller's metadata did. This holds because `_current_carrier` filters
to `_CARRIER_KEYS`.

**Validates: AC-4.3**

- Generators: as P-4, plus contexts with `opentelemetry.baggage.set_baggage` entries.
- Proposed test: Hypothesis `test_inject_never_adds_baggage`.
- Example coverage on `main`: `test_carrier_never_carries_baggage`.

### P-7: Each trace header appears exactly once

_For any_ caller metadata, each carrier key appears in the output exactly once, compared
case-insensitively: the caller's value if the caller set it, otherwise the current context's.

**Validates: AC-4.1, AC-4.2**

- Generators: as P-4.
- Proposed test: Hypothesis `test_inject_no_duplicates`.

### P-15: The context is on the wire for every call kind

_For any_ call kind (unary-unary, unary-stream, stream-unary, stream-stream), caller metadata and
tracing state, the metadata a fake `grpc.aio` server receives satisfies P-4 to P-7. With tracing
enabled its `traceparent` encodes the caller's current span id. With tracing disabled it equals the
caller's metadata.

**Validates: AC-4.1, AC-4.2, AC-4.3, AC-4.4**

- Generators: `st.sampled_from(CALL_KINDS)`, metadata as P-4, `st.booleans()` for tracing.
- Proposed test: Hypothesis `test_context_on_the_wire` (one in-process fake server per module,
  reused across examples).
- Example coverage on `main`: `test_metadata_carries_traceparent_on_all_call_kinds`,
  `test_metadata_has_no_traceparent_when_disabled`.

## Trace shape (AC-3)

### P-13: One rooted trace per run, with documented names only

_For any_ workload (1 to 20 tasks, concurrency 1 to 8, sync or async fleet, any strategy, a
`process_fn` that may raise, sleep or run 0 to 5 execs and file operations), the run emits exactly
one `ate_env_fleet.run` span. Every other span of the run reaches it through its parent chain. Every
`ate_env_client.exec`, `.read_file` and `.write_file` span's parent is the `ate_env_fleet.process`
span of the same task. Every span name and attribute key is documented in
[interfaces.md](./interfaces.md).

**Validates: AC-3.1, AC-3.2, AC-3.3, AC-3.5, AC-16.1, AC-16.2**

- Generators: a `workload()` composite of `st.integers(1, 20)` tasks, `st.integers(1, 8)`
  concurrency, `st.sampled_from(["sync", "async"])`, `st.sampled_from(STRATEGIES)`, and per task a
  list of operations from `st.sampled_from(["exec", "read", "write", "raise", "sleep"])`.
- Proposed test: Hypothesis `test_one_rooted_trace_per_run` (mock backend, in-memory exporter).
- Example coverage on `main`: the `test_nests_*` tests in `test_telemetry_tracing.py`.

### P-14: Exactly one first-command span per acquisition

_For any_ acquired sandbox and any sequence of exec, read and write calls on its handle, including
execs started concurrently from several threads, exactly one `ate_env_client.exec` span carries
`ate_env.first_command=true`, and it is the first exec span started on that handle.

**Validates: AC-3.4**

- Generators: `st.lists(st.sampled_from(["exec", "read", "write"]), min_size=1, max_size=15)`,
  `st.integers(1, 4)` threads.
- Proposed test: Hypothesis `test_first_command_exactly_once`.
- Example coverage on `main`: `test_first_command_marks_only_the_first_exec_per_sandbox` (sequential
  execs only).
- Fails on `main` (open, not addressed by #221). A run of this property against the series' code,
  with execs started concurrently on one handle, marked a span that was not the first started in 9
  of 12 runs. No run marked more than one span, so "exactly one" held and "the first started" did
  not. The cause is in `clients/python/src/ate_env_rl/handle.py`: `exec` and `exec_async` call
  `_note_first_command()` before `_exec_span` starts the span. `_note_first_command` is also a
  check-and-set without a lock. The fix belongs in the code; the property stays as written.

## Run report (AC-7)

### P-8: Phase statistics stay consistent

_For any_ sequence of `add_phase(name, d)` calls, with any names and any finite durations including
negative ones, every phase keeps `0 <= max_s <= total_s <= count * max_s`, in `phases` and in the
rounded `to_dict()` values.

**Validates: AC-7.3**

- Generators: `st.lists(st.tuples(phase_name(), duration, st.integers(1, 5)))`, each duration
  repeated that many times; `duration` mixes `st.floats(0, 0.03)`, values next to the 0.01 s
  rounding step, multiples of 0.01 up to 1 s and `st.floats(-1e3, 1e4, allow_nan=False)`.
- Test: Hypothesis `test_phase_stats_consistent` (#279). A generator of only `st.floats(-1e3, 1e4)`
  seldom reaches the rounding step; repeats of a multiple of 0.01 reach `total_s == count * max_s`,
  where the float product can land under the rounded total (3 \* 0.29).
- Example coverage on `main`: `test_report_clamps_negative_phase_duration`.

### P-16: The report matches its operations

_For any_ sequence of report operations (`add_phase`, `add_task("ok" | other)`, `add_claim`,
`add_release_error`, warm add and remove), `to_dict()` has exactly the documented keys
(`strategy, total_s, phases, claims, tasks_ok, tasks_err, release_errors, warm_replicas_total, warm_replicas_peak, environment`).
Each phase's `count` equals the number of `add_phase` calls for it and its `total_s` equals the sum
of the clamped durations. `tasks_ok + tasks_err` equals the number of `add_task` calls.
`release_errors` equals the number of `add_release_error` calls, and `summary()` has a
`release_errors=` line exactly when it is nonzero. The `summary()` rows follow `RunReport.ORDER`,
then other phases in first-seen order, then `TOTAL`. `json.loads(json.dumps(to_dict()))` equals
`to_dict()`.

**Validates: AC-7.1, AC-7.2, AC-7.4, AC-7.5, AC-7.7**

- Generators: `st.lists(report_op(), max_size=100)`, where `phase_name()` mixes the documented
  phases with arbitrary names.
- Proposed test: Hypothesis `test_report_matches_operations` (a model-based check against a plain
  dict that replays the same operations).
- Example coverage on `main`: `test_report_shape_and_summary_order`,
  `test_report_summary_order_has_reap`, `test_run_report_defaults_have_no_release_errors`,
  `test_fleet_run_report_and_metric_show_failed_releases`.

## Metrics registry (AC-8)

### P-17: Lazy, single registration with documented labels

_For any_ sequence of operations from {import, reload `ate_env_rl.observability`, create
`Observer(enable_metrics=b)`, record a client timing}, nothing is registered before the first
metrics-enabled `Observer` or client timing. After it, each documented family (including
`ate_env_fleet_release_failures_total{backend}`) is registered exactly once with exactly its
documented label names. No operation raises.

**Validates: AC-8.1, AC-8.2, AC-8.3**

- Generators: a Hypothesis stateful machine over those four rules, with a fresh `CollectorRegistry`
  per example.
- Proposed test: Hypothesis `MetricsRegistryMachine`.
- Example coverage on `main`: the `test_registry_*` tests in `test_observability.py`.

## Missing extras (AC-2)

### P-12: Missing extras degrade to a logged no-op

_For any_ combination of `enable_tracing`, `enable_metrics` and installed extras (none, `tracing`,
`metrics`, both), a mock run completes and `fleet.report` exists. Each layer that is enabled but
missing its extra logs exactly one `ERROR` or `WARNING` naming that extra. No telemetry code raises.

**Validates: AC-2.1, AC-2.2, AC-2.3**

- Generators: `st.booleans()` twice for the flags, `st.sampled_from` over the four install states
  (simulated by blocking the imports in `sys.modules`), plus a small workload as in P-13.
- Proposed test: Hypothesis `test_missing_extras_noop`.
- Example coverage on `main`: `test_missing_extra_logs_one_error`,
  `test_missing_extra_logs_one_warning`, `test_observer_without_layers_is_report_only`.

## Benchmarks and Go client (AC-10, AC-11)

### P-19: Tracing does not change the JSONL, and every event has one span

_For any_ event sequence (any event names, runs, jobs, workers, environments, instances, steps,
tasks, durations, `ok` or `err`, optional `attempts`, `count`, `exit`, `reward`), the recorder's
JSONL is byte-identical with and without a tracer. With a tracer, the number of spans equals the
number of recorded events. Each span is named after its event's `event` and ends at `ts`, starting
`dur_ms` earlier. Its attribute keys are a subset of
`run, job, worker, env, instance, step, task, name, attempts, count, exit, reward`: `run`, `step`
and `task` always, the others exactly when set. Its status is `Ok` when `ok`, else `Error` with
`err` (or `"not ok"`).

**Validates: AC-10.2, AC-10.3**

- Generators: a `rapid` generator of `Event` values with random fields, recorded in a random order.
- Proposed test: Go `rapid` `TestRecorderTracingIsTransparent`.
- Example coverage on `main`: `TestRecorderGoldenJSONL`, `TestRecorderSpans`,
  `TestRecorderNoTracerRecordsNoSpans` in `internal/benchmarking/swebench/events_test.go`.

### P-20: Spans only with a provider, one per call

_For any_ sequence of Go client calls (`CreateEnvironment`, `Exec`, `ReadFile`, `WriteFile`, …) and
any `TraceServiceName`: with a nil `TracerProvider`, zero spans are recorded and no exporter is
constructed. With a provider, there is exactly one span per call, named `<TraceServiceName>.<op>`,
with only documented `ate.*` and `ate_env.*` attribute keys plus, on CLIENT spans, the `rpc.*` and
`server.*` keys in [interfaces.md](./interfaces.md).

**Validates: AC-11.1, AC-11.2**

- Generators: `rapid.SliceOf(rapid.SampledFrom(ops))`,
  `rapid.StringMatching("[a-z][a-z0-9-]{0,20}")`.
- Proposed test: Go `rapid` `TestClientSpansFollowProvider`.
- Example coverage on `main`: `TestTracingSpansWithProvider`, `TestTracingOffWithoutProvider`,
  `TestTracingServiceNamePrefix`, `TestTracingSpanKinds` in `clients/go/tracing_test.go`.

### P-21: No secret or content in an attribute value

_For any_ command line built from leading `NAME=value` or `NAME+=value` assignments, quoted words,
`sh -c` strings and arguments holding a generated token, and any file content and image reference:
no span attribute value either client records contains an assignment value, an argument, the file
content or an image tag. `ate_env.command.executable` is the executable's base name or absent.

**Validates: AC-18.1, AC-18.2**

- Generators: `st.lists(assignment | word)` with
  `assignment = st.from_regex(r"[A-Za-z_]\w*\+?=\S+")`, a random token planted in one argument,
  `st.binary()` for content; `rapid` equivalents in Go.
- Proposed test: Hypothesis `test_attribute_values_hold_no_secret`; Go `rapid`
  `TestAttributeValuesHoldNoSecret`.
- Example coverage on `main`: `test_executable_attr_skips_env_assignments`,
  `TestExecutableNeverRecordsAssignments`, `TestExecutableMatchesPythonClient`.

### P-22: The Go and Python clients agree

_For any_ operation both clients have and any inputs to it, and any `OTEL_SEMCONV_STABILITY_OPT_IN`
value: the two clients' spans have the same `<op>` and the same attribute keys and values, except
the Python-only keys in [decisions.md](./decisions.md).

**Validates: AC-17.1, AC-17.2, AC-17.3, AC-17.4**

- Generators: operations sampled from the shared list, generated ids, paths and command lines, the
  three semconv values; written to one fixture both test suites read.
- Proposed test: a shared `testdata/span_parity.json` checked by `TestSpanParity` (Go) and
  `test_span_parity` (Python).
- Example coverage on `main`: `TestRPCAttrsFollowTheSemconvMode` and
  `test_rpc_attrs_follow_the_semconv_mode`, `TestExecutableMatchesPythonClient` and
  `test_executable_attr_skips_env_assignments`, each pair with a hand-copied table.

### P-23: The warm-replica gauge adds up across fleets

_For any_ number of `Observer`s in one process and any sequence of `warm_add`, `warm_remove` and
`warm_reset` calls on them: `ate_env_fleet_warm_replicas{backend}` equals the sum of the observers'
own counts, never goes below zero, and `warm_reset` on one observer leaves the others' shares.

**Validates: AC-19.1, AC-19.2**

- Generators: `st.integers(1, 4)` observers, `st.lists` of `(observer, op, n)`.
- Proposed test: Hypothesis `test_warm_gauge_is_the_sum_of_observers`.
- Example coverage on `main`: `test_warm_gauge_adds_up_across_observers_and_resets_only_own_share`.

## Warm-pool accounting under concurrency (AC-15)

`SubstrateBackendDriver` coroutines share one event loop and interleave at every `await`. P-9 to
P-11 share the Hypothesis stateful test `test_warm_pool_accounting` in
`clients/python/tests/test_warm_pool_accounting.py` (#221). Its rules are `acquire_new`,
`acquire_warm`, `start_recycle` and `finish_recycle` (paused inside `await suspend()`),
`start_warm_pool` and `finish_warm_pools` (paused inside `await create()`), `start_teardown` and
`finish_teardown` (paused inside a reap's `await delete()`), `unwarm` and `idle`. A fake client
pauses at those awaits, so Hypothesis generates the interleavings as well as the operations. The
machine starts no `warm_pool` after a teardown and never makes `get()` fail, so bugs A and B (see
the findings) are covered by the plain tests
`test_warm_pool_after_teardown_ignores_the_old_reservation` and
`test_failed_warm_claim_after_teardown_deletes_the_environment` in the same file.

Dafny: `WarmPool` in [verify/warm_pool.dfy](./verify/warm_pool.dfy) and
[verify/warm_pool_generations.dfy](./verify/warm_pool_generations.dfy) prove the #221 design keeps
P-9 to P-11 for every interleaving within the model.
[verify/warm_pool_drain.dfy](./verify/warm_pool_drain.dfy) adds `unwarm_pool`, `resize_pool` and run
ids, from #221's third review round (AC-15.4, AC-15.5, AC-15.9).

### P-9: The warm-replica gauge tells the truth

_For any_ sequence of acquire, recycle, `warm_pool` and teardown operations, interleaved at their
awaits, after every step `ate_env_fleet_warm_replicas{backend}` equals the number of actors in the
driver's warm pool.

**Validates: AC-15.1, AC-15.6**

- Test: `test_warm_pool_accounting`, invariant `gauge_equals_pool`;
  `test_direct_reap_moves_the_gauge_with_the_pool`.
- A reap moves the gauge by the pool it clears, so P-9 holds for a direct `reap` too, not only
  through a fleet, which also resets the gauge.
- Fixed by #221. Before it, in the P-10 recycle interleaving, the pool held 1 actor and the gauge
  read 0.

### P-10: Nothing survives teardown

_For any_ such sequence, once teardown has run and every paused call has resumed, no environment
from the run is alive on Substrate or tracked by the driver.

**Validates: AC-15.2, AC-15.5, AC-15.8, AC-15.9, AC-15.10**

- Test: `test_warm_pool_accounting`, invariant `nothing_survives_teardown`;
  `test_recycle_paused_across_teardown_deletes_the_environment`,
  `test_warm_pool_paused_across_teardown_deletes_its_actors`,
  `test_failed_warm_claim_after_teardown_deletes_the_environment`,
  `test_warm_pool_finishing_during_a_reap_deletes_its_actors`, and the failed and cancelled delete
  tests (`test_failed_delete_after_teardown_*`, `test_cancelled_delete_after_teardown_*`,
  `test_failed_recycle_delete_after_teardown_*`, `test_failed_delete_of_drained_actors_*`).
- An actor that must be deleted stays owned until the delete succeeds. If the delete fails after the
  run's reap already ran, the driver re-tags it with `WARM_POOL_RUN_ID`, so whichever run reaps next
  deletes it.
- AC-15.8, fixed by #240: a `warm_pool` call cancelled while `asyncio.gather` waits on its
  `create()` calls dropped the ids of the actors already created, so they were neither pooled nor
  owned and no reap found them. It now owns every id it sent a `create()` for under
  `WARM_POOL_RUN_ID` (a reap finds one never created NotFound, which counts as gone), and if a reap
  ran since it planned, it also deletes them in background tasks: that reap may have taken its
  snapshot before the handler ran
  (`test_warm_pool_cancelled_just_before_the_runs_reap_deletes_its_actors`). Test:
  `test_cancelled_warm_pool_leaves_its_actors_for_any_reap`; Dafny: TRACKED in
  `verify/warm_pool_drain.dfy` (`WarmCancelledFixedSafe`; `WarmCancelledBeforeSafe` models the code
  before and fails). Review of #240 found the same leak without a cancel: with `wait=True`, an actor
  whose `create()` succeeded and `suspend()` raised was neither pooled, returned nor owned. It is
  now owned under `WARM_POOL_RUN_ID` and deleted at once
  (`test_warm_actor_whose_suspend_fails_is_deleted_or_left_for_any_reap`; `WarmSuspendFailedSafe`,
  with `WarmSuspendFailedBeforeSafe` failing). The stateful machine cancels only acquires and never
  fails a suspend, so it reaches neither case.
- AC-15.2, fixed by #276: a cold `acquire` cancelled while its `create()` was in flight (for example
  by its own `timeout_s`) dropped the actor id, so an actor whose create had landed was neither
  returned nor owned, and no reap found it (#273). It is now owned under `WARM_POOL_RUN_ID`, as a
  cancelled `warm_pool` call's actors are, and deleted at once in a background task if a reap ran
  since the acquire read `_teardowns`. Tests:
  `test_cold_acquire_cancelled_in_create_leaves_the_actor_for_any_reap`,
  `test_cold_acquire_cancelled_in_create_after_its_runs_reap_deletes_the_actor`. The stateful
  machine starts acquires that pause at their own gate (in `get()` for a warm claim, in `create()`
  before or after the create lands) and finishes or cancels them at any later step, and at the end
  of every example it cancels the acquires still in flight, lets paused calls resume and tears down,
  then checks `nothing_survives_teardown`. On the code before #276 it fails, for example, with:

  ```text
  state.start_acquire(lands_first=True)      # create() landed, reply pending
  state.start_teardown()
  state.cancel_acquire(data=data(...))
  state.finish_teardown()                    # -> sb-... alive after teardown
  ```

- AC-15.9, fixed by #238: a cancelled cleanup delete, and a `release` whose delete fails or is
  cancelled while a reap runs, re-tag the actor too. See "Run-id gaps" in the findings.
- Fixed by #221. Before it, the stateful test shrank it to two minimal counter-examples (recorded
  when one `fleet_teardown` rule ran the whole reap):

  ```text
  state.start_warm_pool()                    # create() paused
  state.fleet_teardown()
  state.finish_warm_pools()                  # -> 3 warm actors alive after teardown

  owned_0 = state.acquire_new()
  recycling_0 = state.start_recycle(aid=owned_0)   # suspend() paused
  state.fleet_teardown()
  state.finish_recycle(aid=recycling_0)      # -> sb-0 alive after teardown
  ```

### P-11: `warm_pool` never over-provisions

_For any_ such sequence, the warm pool never holds more than `R` actors created by `warm_pool` for
one pool key.

**Validates: AC-15.3, AC-15.4**

- Test: `test_warm_pool_accounting`, invariant `warm_pool_never_over_provisions`;
  `test_overlapping_warm_pool_calls_provision_once`, `test_unwarm_cancels_a_warm_pool_in_flight`,
  `test_warm_pool_after_unwarm_ignores_the_cancelled_call`,
  `test_shrinking_resize_cancels_a_warm_pool_in_flight`.
- `unwarm_pool`, and a `resize_pool` below what is pooled plus in flight, start a new generation of
  the pool (`_pool_gens`). A `warm_pool` call still creating actors for the old generation deletes
  them instead of pooling them, and later calls do not count its reservation.
- Fixed by #221. Before it, the shrunk counter-example was:

  ```text
  state.start_warm_pool()
  state.start_warm_pool()
  state.finish_warm_pools()                  # -> 6 warm actors pooled, asked for 3
  ```

### P-24: A warm claim returns a live environment

_For any_ such sequence that also lets a reap run while a warm claim awaits `get()`: every
environment `acquire` returns is alive on Substrate when `acquire` returns.

**Validates: AC-15.7**

- Met by #239. Test: `test_warm_claim_overlapping_a_reap_returns_a_live_environment` pauses `get()`,
  reaps another run (to the end, or with a delete of the popped actor still in flight: the reap's
  for `run-0`, the claim's own for `run-1`, where the reap deletes nothing), lets `get()` succeed,
  and checks that `acquire` returns a live, newly created environment and that the popped actor is
  gone, both when the reap matches it (`run-0`) and when it skips it (`run-1`). Dafny:
  `ClaimOkFixedReturnsLive` in `verify/warm_pool_generations.dfy`; `ClaimOkReturnsLive` models the
  code before the fix and fails.
- The stateful machine still never runs a reap during a claim; a rule for it is proposed.

## Requirement index

The requirement numbers the properties validate. Each refines the criterion of the same number in
[acceptance-criteria.md](./acceptance-criteria.md).

| ID       | Requirement                                                                                                                        |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| AC-1.1   | The base install depends only on `grpcio` and `protobuf` and offers the extras `tracing` and `metrics`.                            |
| AC-1.2   | Importing `ate_env_rl`, `ate_env_rl.observability` or `ate_env_rl.telemetry` in the base install loads neither extra.              |
| AC-2.1   | With a layer enabled and its extra missing, the run completes and `fleet.report` exists.                                           |
| AC-2.2   | Each enabled layer missing its extra logs exactly one `ERROR` or `WARNING` naming the extra.                                       |
| AC-2.3   | Telemetry code raises no exception.                                                                                                |
| AC-3.1   | One `ate_env_fleet.run` span per run, sync and async.                                                                              |
| AC-3.2   | Every `ate_env_fleet.*` and `ate_env_client.*` span of a run descends from its `ate_env_fleet.run` span, across threads and loops. |
| AC-3.3   | Each task's exec and file spans are children of that task's `ate_env_fleet.process` span.                                          |
| AC-3.4   | The first exec started on an acquired sandbox, and only that one, carries `ate_env.first_command=true`.                            |
| AC-3.5   | Only span names and attribute keys documented in [interfaces.md](./interfaces.md).                                                 |
| AC-4.1   | With tracing on, exactly one `traceparent` matching the current span on all four call kinds.                                       |
| AC-4.2   | Caller metadata passes through unchanged and in order; a key the caller set is not added again (case-insensitive).                 |
| AC-4.3   | No `baggage` header the caller did not send.                                                                                       |
| AC-4.4   | With tracing off, the caller's metadata is sent unchanged.                                                                         |
| AC-5.1   | `ate-env-api` with `-enable-tracing` records its span under the client's trace.                                                    |
| AC-5.2   | `ate-env-api` exports to the configured OTLP endpoint.                                                                             |
| AC-6.1   | `ate-env-api` serves exactly the documented `ate_env_api_*` families and labels.                                                   |
| AC-6.2   | Calls move `ate_env_api_requests_total` and `ate_env_api_create_to_ready_seconds`.                                                 |
| AC-7.1   | `to_dict()` keys and `phases` entry keys are exactly the documented ones.                                                          |
| AC-7.2   | `add_phase(name, d)` adds 1 to `count`, `max(d, 0)` to `total_s`, and raises `max_s` to at least `max(d, 0)`.                      |
| AC-7.3   | Every phase keeps `0 <= max_s <= total_s <= count * max_s`, including after negative durations.                                    |
| AC-7.4   | `add_task("ok")` counts `tasks_ok`; any other status counts `tasks_err`.                                                           |
| AC-7.5   | `summary()` rows follow `ORDER`, then other phases first-seen, then `TOTAL`; `to_dict()` survives a JSON round trip.               |
| AC-7.6   | A mock run records the documented phase set.                                                                                       |
| AC-7.7   | `add_release_error()` adds 1 to `release_errors`; `summary()` prints `release_errors=N` while it is nonzero.                       |
| AC-8.1   | Importing `ate_env_rl` or `ate_env_rl.observability` registers nothing.                                                            |
| AC-8.2   | The first metrics-enabled `Observer` or client timing registers each documented family once, with exactly its labels.              |
| AC-8.3   | A second `Observer` or a module reload neither raises nor registers a family twice.                                                |
| AC-8.4   | `serve_metrics(port)` answers `/metrics`.                                                                                          |
| AC-9.1   | At most `MAX_FAMILIES` (64) registered families and at most `MAX_FAMILIES + 2` distinct `family` values per process.               |
| AC-9.2   | Repo families matching the reserved names (and their `_repo` forms) are escaped with `_repo`; bare reserved names never register.  |
| AC-9.3   | `other` only when the registry is full and the family is new.                                                                      |
| AC-9.4   | A registered family always gets the same label.                                                                                    |
| AC-9.5   | No ids or image tags in label values; no label named `env_id`, `instance`, `image` or `run_id`.                                    |
| AC-10.1  | With tracing off, the JSONL equals the golden file byte for byte.                                                                  |
| AC-10.2  | The JSONL is the same with tracing on and off.                                                                                     |
| AC-10.3  | One span per event, named `Event.Event`, from `ts - dur_ms` to `ts`, with the attribute and status rules in P-19.                  |
| AC-11.1  | With a nil `TracerProvider`, no span and no exporter.                                                                              |
| AC-11.2  | With a provider, one span per call named `<TraceServiceName>.<op>` with documented keys only.                                      |
| AC-15.1  | The `ate_env_fleet_warm_replicas` gauge equals the driver's warm pool after every step of any interleaving.                        |
| AC-15.2  | A recycle or `warm_pool` paused across teardown deletes its actor on resume; nothing from the run survives teardown.               |
| AC-15.3  | Overlapping `warm_pool(template, replicas=R)` calls create at most `R` warm actors for one pool key.                               |
| AC-15.4  | `unwarm_pool` and a shrinking `resize_pool` cancel the `warm_pool` calls in flight for that pool.                                  |
| AC-15.5  | An actor whose delete fails after a teardown or a drain is deleted by the next `reap` of any run.                                  |
| AC-15.6  | A direct `reap` leaves the gauge equal to the pool.                                                                                |
| AC-15.7  | A warm claim returns a live environment.                                                                                           |
| AC-15.8  | A `warm_pool` cancelled during `create()` leaves no live actor untracked.                                                          |
| AC-15.9  | A delete cancelled after a reap, or a release's delete that fails during one, leaves the actor for any later reap.                 |
| AC-15.10 | An `acquire` that completes after its own run's reap leaves its environment for a later reap.                                      |
| AC-16.1  | A test collects every span name and attribute key the client and driver operations emit.                                           |
| AC-16.2  | The collected set equals the documented table; CI fails on a difference.                                                           |
| AC-17.1  | Shared operations have the same span `<op>` in both clients.                                                                       |
| AC-17.2  | Same inputs give the same `ate.*` and `ate_env.*` keys and values, except the Python-only keys.                                    |
| AC-17.3  | Same `rpc.*` and `server.*` keys and values under each semconv value.                                                              |
| AC-17.4  | `executable` and `executable_attr` agree on one shared table.                                                                      |
| AC-18.1  | `ate_env.command.executable` never holds an argument or an assignment value.                                                       |
| AC-18.2  | No attribute value holds file content, a token or an image tag.                                                                    |
| AC-19.1  | The gauge is the sum of every `Observer`'s pool.                                                                                   |
| AC-19.2  | `warm_reset` removes only that `Observer`'s share.                                                                                 |

## Requirements without a property

These are checked by examples or on the cluster (see each criterion), because they describe one
artifact or environment rather than a range of inputs:

| Requirement         | Why not a property                                                   |
| ------------------- | -------------------------------------------------------------------- |
| AC-1.1, AC-1.2      | One dependency list and one import; a static lint covers regressions |
| AC-5.1, AC-5.2      | Needs the cluster and a collector                                    |
| AC-6.1, AC-6.2      | Needs the cluster; Go unit tests check the family and label sets     |
| AC-7.6              | One mock run's phase set                                             |
| AC-8.4              | One HTTP endpoint                                                    |
| AC-10.1             | Byte-for-byte compatibility with a fixed output (golden file)        |
| AC-12, AC-13, AC-14 | Docs, the CI configuration and timings                               |
| AC-20               | A CI step                                                            |

## Findings that changed the spec

**P-3, reserved `other`.** The Dafny model of `repo_family`
([verify/telemetry.dfy](./verify/telemetry.dfy)) without a rename step failed P-3 with the
counter-example of a repo literally named `other`. That repo got the overflow label, so the two
cases were indistinguishable on dashboards, and it used one of the 64 slots although the code
comment says reserved names do not count toward the cap. Reproduced against the code at the time:

```text
repo "acme/other", registry not full -> "other", registered, size 1
registry full, new repo              -> "other"
registry full, repo "acme/other"     -> "other"
```

Amendment: a repo whose family is a reserved name is labelled `<name>_repo`. `main` now escapes
every `other` or `unknown` followed by `_repo` repeats, which keeps the mapping one-to-one (AC-9,
P-2).

**P-8, negative durations.** The model verified P-8 only with the precondition `dur >= 0`, which no
document stated. Reproduced: `add_phase("acquire", 2.0); add_phase("acquire", -5.0)` gave
`count=2, total_s=-3.0, max_s=2.0`. Latent, because every caller uses `time.monotonic()`. Amendment:
`add_phase` and `task_done` clamp negative values to 0 (AC-7). `main` has the clamp.

**P-8, rounding in `to_dict()`.** The property held for `phases` but not for the values a report
prints: `to_dict()` rounded `total_s` and `max_s` to 0.01 separately. Reproduced on `main` at
`beedf88` (#273): 3 phases of 0.004 s gave `{"count": 3, "total_s": 0.01, "max_s": 0.0}`, so
`total_s <= count * max_s` failed. The example tests all used durations of 0.5 s or more, which
round exactly. Amendment: `max_s` rounds up to 0.01 and `total_s` is kept in
`[max_s, count * max_s]` (AC-7).

**P-1, the bound.** The property first said `MAX_FAMILIES + 1`. A Hypothesis run found 65 distinct
repos plus one unparseable input give 66 values (64 families, `other`, `unknown`). The spec was
wrong.

**P-19, one span per event.** The property first described interleaved start and done pairs. The
code writes each event when its operation finishes and records one span per event, back-dated by
`dur_ms`. The spec was behind the code.

**P-10, recycle overlapping teardown leaks a VM.** `release(recycle=True)` removes the actor from
the owned actors, then awaits `suspend()`. If `reap()` runs during that await, it sees the actor in
neither the owned actors nor the pool, so it skips it. When `suspend()` returns, the actor goes back
into the pool. A scripted reproduction (not in the repository) that runs the real driver with a fake
client paused at its awaits printed, on `main` at `9b2148a` before #221:

```text
reaped=0  still live on Substrate=['sb-1']
driver pool=1  owned=['sb-1']  observer warm=0
```

The VM survives teardown, and the gauge says 0 while the pool holds 1 (P-9).

**P-11, overlapping `warm_pool` calls double-provision.** `warm_pool` computes
`needed = replicas - len(current)` before its awaits and does not count replicas other calls are
still creating. The same reproduction printed, before #221:

```text
pool size=6 (asked for 3)  live actors=6  observer warm=6
```

Fix for both, in #221: `reap` bumps a teardown counter (`_teardowns`) when it starts and ends and
counts itself in `_reaping` while it runs, and a recycle, `warm_pool` or failed warm claim that
awaited across a reap, or finishes during one, deletes its actor instead of pooling it. `warm_pool`
reserves the actors it is creating in `_warm_inflight`, per pool key, `_teardowns` value and pool
generation ([interfaces.md](./interfaces.md#warm-pool-accounting-ac-15)).

**Hypothesis finds the same bugs.** The stateful test finds both bugs on the code before #221
without being told where to look and shrinks each to the shortest operation sequence, shown under
P-10 and P-11. It also finds a third variant the model predicted: a `warm_pool` paused in `create()`
during teardown leaks its actors.

**Bugs A and B, in #221's first version.** An adversarial code review of #221's first version found
two more bugs. Neither Dafny nor Hypothesis found them first.

- A: reap left the reservation counter alone, so a `warm_pool` started after the teardown counted
  the old reservation and returned with an empty pool. Hypothesis missed it because the machine
  starts no `warm_pool` after a teardown. `warm_pool.dfy`'s invariant `closed ==> pending == 0` and
  `Reap`'s `pending := 0` already encode the fix: modelling the first version against it fails at
  that clause. But that model makes a teardown permanent, so it cannot express the symptom.
- B: a failed warm claim (pop, await `get()`, put back on error) put its actor back after a
  teardown. Hypothesis missed it because the fake `get()` never failed. `warm_pool.dfy` cannot
  express it: its `Acquire` is one step with no await or put-back.

`warm_pool_generations.dfy` was written after the review, to state both: `PlanFirstCovers` (A) and
`ClaimFailPutBackSafe` (B) fail, the fixed actions verify, and `BugAWitness` and `BugBWitness` check
concrete traces. #221 fixed both and added a plain test for each.

**Claim/reap gap (AC-15.7), fixed by #239.** Proving "claimed actors are live" in
`warm_pool_generations.dfy` failed (`ClaimOkReturnsLive`). A warm claim pops an actor and awaits
`get()`; a reap that runs meanwhile deletes it, since it is still owned. If `get()` returns before
the delete lands, `_acquire_internal` hands the caller a deleted environment. #221 re-checked
`_teardowns` only on the failure path. P-10 holds (nothing survives the teardown); what breaks is
that `acquire` returns a live environment, which P-24 now states. #239 re-checks on the success path
too: if a reap ran since the pop, the claim moves on to another pooled actor or a cold create. The
reap deleted the popped actor if it matched it (its run or `WARM_POOL_RUN_ID`); one of another run,
or a reap cancelled before its delete, leaves it alive and unpooled, so the claim deletes it if it
is still owned. The check and taking ownership share one lock hold. In the model, a claim from the
current generation is live (an invariant), the actor is handed out only then, and otherwise
`ClaimOk` deletes what is still owned, as `ClaimFailFixed` does (`ClaimOkOtherRunWitness` is the
other-run trace).

Run-id gaps (AC-15.9), fixed by #238. `warm_pool_drain.dfy` states ORPHAN: every owned actor with no
delete pending carries a run id some later reap still matches (`WARM_POOL_RUN_ID`, or a run not
reaped yet). A fleet reaps its run once, so an actor left under a reaped run id outlives every later
teardown. The re-tag #221 shipped kept ORPHAN for failed cleanup deletes; two paths still broke it,
and a scratch test against `main` (`c0c41ac`) reproduced both:

- `DeleteCancelledSafe`: a `warm_pool` call with `run_id="run-1"` paused in `create()`;
  `reap("run-1")`; the creates return and the call starts deleting its actors; the call is
  cancelled. The three actors stay owned under `run-1`, and `reap("run-2")` leaves them alive.
  `_delete_unpooled` catches `Exception`, so a cancellation skips the re-tag.
- `ReleaseDeleteFailSafe`: `release("sb-1")` pops the actor and awaits `delete()`; `reap("run-1")`
  runs and cannot see it; the delete fails. `_release_internal` puts the actor back under `run-1`
  (`missed_by_reap` was set only by a recycle whose `suspend()` returned after a teardown), and
  `reap("run-2")` leaves it alive.

#238 re-tags a cancelled `_delete_unpooled` too, and owns the actors a torn-down or drained
`warm_pool` call made under `WARM_POOL_RUN_ID` from the start, so a cancel before any delete starts
leaves nothing under the reaped run id. A release that pops an actor and does not delete it (its
delete fails or is cancelled, or a recycle is cancelled in `suspend()`) owns it again through
`_return_popped`: under `WARM_POOL_RUN_ID` if a reap ran since the pop (`not _may_pool(teardowns)`),
else under its run id, so the run's own reap retries it. The check and the update share one lock
hold, so a reap on another thread cannot start in between. `DeleteCancelledFixedSafe`,
`WarmCommitDrainedSafe` and `ReturnPoppedSafe` verify these; the scratch reproductions are now
tests.

**Cancelled cold acquire (AC-15.2), fixed by #276.** The acceptance-criteria audit (#273) found that
a cold `acquire` cancelled while its `create()` is in flight, for example by its own `timeout_s`,
dropped the actor id although the create may have landed, so the actor stayed alive and untracked.
The stateful machine missed it because it started acquires only through rules that ran them to
completion or put actors in the owned actors directly, and cancelled nothing; the Dafny model has no
cancelled-acquire action. With acquires paused at their own gates and cancelled at any step, the
machine finds it on the code before #276 in 10 of 10 seeds at the default 500 examples (6 of 10
without the end-of-example teardown). It also cancels warm claims in `get()`: the popped actor stays
owned under its run id and the run's reap deletes it, so that path holds.

**How the concurrency model works.** Each piece of code between two `await`s is one atomic action on
shared state (`Acquire`, `RecycleStart`, `RecycleFinish`, `WarmPlan`, `WarmCommit`, `Reap`). The
Dafny model proves that every action of the #221 design preserves the invariant, so it holds for
every interleaving of any number of concurrent calls and actors. `WarmPoolBefore`, the code before
#221, fails, and each failure is one of the interleavings above.

## Limits

- Dafny `real` is exact and Python `float` is not; P-8 holds for real numbers.
- The `repo_family` model is sequential. `_bounded_family` uses double-checked locking; its
  concurrency is not covered.
- The `repo_family` model covers parseable inputs only; the `unknown` label for unparseable input
  adds one value to the P-1 bound. It renames the bare reserved names only; `main` also escapes
  their `_repo` forms.
- The warm-pool models cover one pool key and one event loop (interleaving only at `await`s). Calls
  into the same driver from two threads or two loops are outside them.
- All models are hand-written and not tied to the code; see [verify/README.md](./verify/README.md).

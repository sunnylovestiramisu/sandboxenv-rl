# Plan: Go client parity with the Python client

Status: proposal, written 2026-11-04 and re-pinned 2026-11-05 against `main` at `bff714ed`, with the
open pull requests #209, #218, #230 and #235 read. The evidence is in
[parity-matrix.md](./parity-matrix.md).

## 1. Goal

`clients/go` and `clients/python` are both supported clients of `ate-env-api`
([docs/support.md](../../support.md)). Most client work since the repository started went into
Python: a fleet SDK, a typed error contract, metrics, a README and about 9,000 lines of tests. The
Go client kept RPC coverage (each new `EnvironmentService` RPC lands in both clients in the same
pull request, as #209 and #235 do) and tracing, and stopped there. Its main consumer, the SWE-bench
loader, carries client logic of its own: a connection pool and a polled command runner, with a
comment that the runner belongs in the SDK.

This plan brings the Go client to the same capabilities, the same wire behavior and the same
telemetry names as the Python client, in Go idiom, for everything a Go consumer can use. It does not
make the two APIs look alike, and it does not port the fleet SDK by default (section 8).

## 2. The gap in one screen

- Packaging: `go get` of the client pulls 404 packages, among them Substrate's API module, through
  one constant imported from `internal/ate`.
- Errors: the `ate-env-start-failure` trailer, which the server has sent since #222 and the Python
  client turns into a retryable `SandboxStartError` with a reason, is dropped. Go callers see a bare
  FAILED_PRECONDITION or RESOURCE_EXHAUSTED. `fromGRPCError` also drops the gRPC status on its
  `ErrNotFound` and `ErrProcessExited` branches, so `status.Code(err)` returns `codes.Unknown` for
  them.
- Long commands: `Env.Run` holds one stream open for the whole command, so the router's route
  timeout cuts long test runs. The loader polls instead and asks for that option in the SDK. There
  is no `TimedOut` and no `Duration` on the result.
- Connections: the loader round-robins N clients; the client has one connection.
- Observability: no metrics, no span on `Env.StartProcess`, no `ate_env.first_command`.
- Docs and tests: no README, no runnable example, no e2e against a daemon or a cluster; the
  cross-client attribute tables are mirrored by hand.
- Fleet: nothing in `clients/go`; the loader's `WorkingSet` is the only Go pool.

Where Go is ahead: PTY and resize (the Python client exposes neither), and a unit fixture that runs
the real `apiservice` handler and guest services against fake Substrate.

## 3. Scope: three tiers

| Tier | Content                                                        | Recommendation                                                      |
| ---- | -------------------------------------------------------------- | ------------------------------------------------------------------- |
| 1    | the client: packaging, errors, run modes, connections (PR 1-5) | do now                                                              |
| 2    | observability, parity vectors, docs, e2e (PR 6-9)              | do now                                                              |
| 3    | a fleet SDK in Go (section 8)                                  | decide; the recommendation is to defer until a Go consumer needs it |

Tier 1 and 2 are nine small pull requests, each one concern, in the order of section 5. Tier 3 is a
separate decision because it is five to six pull requests on its own, the Python fleet is still
moving (#230 and #235 are open, and so are items 3, 8, 10, 13, 22, 23 and 26 of issue #123), and the
only Go consumer today has a pool shaped for benchmarks (fork, step barriers, park policies) that
the fleet does not model.

## 4. Design decisions

### D1. Same wire behavior, Go idiom

Parity means the same capabilities, the same bytes on the wire, the same span, attribute, metric and
trailer names, and the same error mapping rules. It does not mean the same method names or types: Go
keeps proto request types, `context`, `io.Reader`, `errors.Is`, `errors.As` and `status.Code`.

### D2. The client owns what Python keeps in the fleet

Go has no fleet, so the start error, the retryable classification, `TimedOut`, `Duration`, polled
execution and the connection pool go into `clients/go` as options on `ClientOptions`, `ShellRequest`
and `ShellResponse`. The loader then uses them and deletes its copies (`EnvBackend.pick`,
`runPolledProcess`).

### D3. `clients/go` imports nothing under `internal/` outside tests

New architecture invariant in [AGENTS.md](../../../AGENTS.md), checked by a verify script in the Go
CI job. Outside tests, `proto/ateenv/v1alpha` is the only in-repository import. The test files keep
the fixture's imports (`internal/apiservice`, `internal/ate`, `internal/internaltest`,
`internal/mcp`, `internal/unknownfields`, `guest`); `go list -deps` ignores them.

### D4. Shared conformance vectors

The tables both clients must agree on (image reference attribute, executable attribute, `rpc.*`
attributes per semconv mode, error mapping with the start-failure trailer, metric families and
labels, RPC coverage) live once, as JSON under `proto/testdata/client-parity/`, next to the contract
they describe, and both test suites read them. This closes AC-17 and part of AC-18 of
[the telemetry plan](../telemetry/acceptance-criteria.md) and replaces the hand-mirrored tables.

### D5. `timed_out` comes from the guest

The guest knows when its timer killed the process (`ProcessState.timedOut` in
`guest/process/tracker.go` already drives
`ate_env_guest_process_duration_seconds{exit_class="timeout"}`); the Python runtime guesses from
exit 137 and elapsed time. PR 4 adds `Process.timed_out` to `guest.proto`, sets it in the guest, and
makes both clients prefer it. Until PR 4 merges, PR 3 uses the Python heuristic. Decision needed:
the field, or the heuristic alone (section 9).

### D6. Span names, and one reversal

Go keeps `exec` for `Run`, the name the Python fleet handle and
[docs/observability.md](../../observability.md) use, and adds a `start_process` span on
`Env.StartProcess`, the name the Python `Env.start_process()` operation has. Neither client nests a
`start_process` span under `exec` or `shell`: in Python, `Env.shell` and `SandboxHandle.exec` turn
the inner tracer off. The Python direct path's `shell` name is a Python follow-up.

This plan also puts `ate_env.first_command` and `ate_env.timed_out` on Go's `exec` spans. AC-17.2 of
the telemetry plan, the Go row of [its interfaces](../telemetry/interfaces.md) and
[decisions.md](../telemetry/decisions.md) record both keys as Python-only: `first_command` because
it described the Python fleet's acquired sandbox, `timed_out` because the guest API returned no
timeout field. The Go `Env` has the same first call, and PR 4 gives the guest a timeout field, so
both apply; `ate_env.run_id` stays Python-only, because Go has no fleet run. PR 6 updates AC-17.2,
the interfaces table and decisions.md with this reversal.

### D7. Metrics are opt-in through a `prometheus.Registerer`

Nothing registers on the default registry unless the caller passes it. The families, labels and
buckets are the Python ones, with `source="direct"`. Registration is idempotent per registry, as
`telemetry/metrics.py` does it.

### D8. Out of this plan

The Go module path (`github.com/agent-substrate/env` in `go.mod`, for a repository that lives at
`ai-on-gke/sandboxenv-rl`) is a separate decision; PR 1 makes the client importable from another
module under either path. Also out: TLS to `ate-env-api`, a port-proxy client, the MCP surface and
the benchmark trace-file mode.

## 5. Pull request sequence

| #   | Branch                   | Scope                                                                                                                                                                                                                                                                                                                                                          | Depends on | Size |
| --- | ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ---- |
| 1   | `goclient/deps`          | `env.DefaultAtespace`; drop the `internal/ate` import; `hack/verify/go-client-deps.sh` in the Go CI job; the invariant in `AGENTS.md` and the rule in `docs/dev/code-layout.md`                                                                                                                                                                                | none       | S    |
| 2   | `goclient/errors`        | `StartError{Reason, Code}` from the `ate-env-start-failure` trailer on unary, server-stream and client-stream calls; `Retryable(err) bool` with the Python code table; `fromGRPCError` keeps the status error in the chain on the `ErrNotFound` and `ErrProcessExited` branches; a test that `status.Code` reads through the wrap on every branch; `errors.go` | none       | M    |
| 3   | `goclient/run-poll`      | `ShellRequest.Poll` (start, poll `Env.FindProcess` with backoff, read the output once, kill on abandon) lifted from the loader with its tests; `ShellResponse.Duration` and `TimedOut` (heuristic until PR 4); the loader's `envSandbox.Run` uses it                                                                                                           | none       | M    |
| 4   | `guest/timed-out`        | `Process.timed_out` in `guest.proto`, set by `guest/process`; `buf generate`, `make python-protos`; Go `TimedOut` and Python `ProcessInfo.timed_out`, `ExecResult.timed_out` prefer the field; CHANGELOG                                                                                                                                                       | 3          | M    |
| 5   | `goclient/connections`   | `ClientOptions.Connections`: N connections, round robin on `Create` and `Env`, an `Env` keeps its connection; `Close` closes all; rejected with `GRPCConn`; the loader's `EnvBackend` keeps one client                                                                                                                                                         | 1          | S    |
| 6   | `goclient/metrics`       | `ClientOptions.MetricsRegisterer`; `ate_env_client_exec_seconds`, `_suspend_seconds`, `_ready_to_first_command_seconds`; a `start_process` span on `StartProcess`; `ate_env.first_command` and `ate_env.timed_out`; the telemetry plan's AC-17.2, interfaces and decisions for both keys; the Go row of [docs/observability.md](../../observability.md)        | 2, 3       | M    |
| 7   | `clients/parity-vectors` | `proto/testdata/client-parity/*.json`; `clients/go/parity_test.go` and `clients/python/tests/test_parity_vectors.py`; delete the mirrored tables; step 5 and 6 of [the extend-env-surface skill](../../../.agents/skills/extend-env-surface/SKILL.md) name the vectors                                                                                         | 2, 6       | M    |
| 8   | `goclient/readme`        | `clients/go/README.md` with the Python README's sections for the shared operations; `example_test.go` compiled by `make test`; `!/clients/go/README.md` in `.prettierignore`; the rows in [clients/README.md](../../../clients/README.md) and [docs/observability.md](../../observability.md); CHANGELOG                                                       | 2, 3, 5, 6 | M    |
| 9   | `goclient/e2e`           | `clients/go/e2e_test.go` gated by `ATE_ENV_GUEST_TARGET` and `ATE_ENV_API_TARGET`, the case list of `tests/e2e/test_guest_daemon.py` and `test_full_stack.py`; `hack/ci/e2e.py` runs it after the Python full-stack tests                                                                                                                                      | 3          | S    |

PR 8 lands after what it documents, because the READMEs describe merged code only. PR 1, 2 and 3 can
run in parallel. PR 4 is the one proto change and can be dropped if D5 goes the other way; PR 3 then
keeps the heuristic and PR 6 records `ate_env.timed_out` from it.

### PR 1: packaging

- `clients/go/client.go`: `const DefaultAtespace = "ate-env"`; `Env()` uses it. `internal/ate` keeps
  its own constant.
- `hack/verify/go-client-deps.sh`: fails when `go list -deps ./clients/go` names a package under
  `github.com/agent-substrate/env/internal/`, `go.ate.dev/` or `k8s.io/`.
- [AGENTS.md](../../../AGENTS.md), "Architecture invariants": outside tests, `clients/go` imports
  only `proto/ateenv/v1alpha` from this repository, because an external module that imports the
  client must not pull the Substrate control-plane module (enforced by the verify script).
  [docs/dev/code-layout.md](../../dev/code-layout.md) gets the import rule and the second-consumer
  rule of section 7.

### PR 2: errors

- `clients/go/errors.go`: `type StartError struct { Reason string; Code codes.Code; msg string }`
  with `Error`, `Unwrap` to the status error, and the constants `StartFailureCrashed`,
  `StartFailureNoCapacity`, `StartFailureKey`: the same strings as `StartFailureKey`,
  `StartFailureCrashed` and `StartFailureNoCapacity` in `internal/apiservice` and
  `START_FAILURE_KEY` in `errors.py`; the client does not import them.
- Every guest call captures trailers: `grpc.Trailer(&md)` on unary calls, `stream.Trailer()` after
  the error on streams. As built (#297), the process and file service clients run on a `trailerConn`
  that wraps the connection (the client's own or a `GRPCConn`) and reads the trailer where `RecvMsg`
  fails. A `Send` that fails with `io.EOF` carries no status: the call site must call `CloseAndRecv`
  and map that error (`WriteFile`, `Stdin`). `Process.Output` returns a stream whose `Recv` maps its
  errors too. `fromGRPCError` returns `StartError` when the key is present, whatever the code,
  before the `ErrNotFound` and `ErrProcessExited` rules, as `map_rpc_error` does. `StartError` also
  carries `EnvID`, as `SandboxStartError` carries `sandbox_id`.
- `fromGRPCError` keeps the status error in the chain on every branch. Before PR 2 the `ErrNotFound`
  and `ErrProcessExited` branches formatted `status.Convert(err).Message()` with `%s`, so
  `errors.Is` found the sentinel and `status.Code(err)` returned `codes.Unknown`. As built (#297),
  each branch returns an `rpcError` whose `Unwrap() []error` yields the sentinels and the status
  error, so `errors.Is` still finds the sentinel and `status.Code` reads `codes.NotFound` through
  `errors.As`. `Retryable` needs this for NOT_FOUND, and so does a caller who switches on the code.
- `Retryable(err) bool`: true for `StartError`, UNAVAILABLE, DEADLINE_EXCEEDED, INTERNAL, ABORTED
  and NOT_FOUND on an environment; false for INVALID_ARGUMENT, PERMISSION_DENIED and the rest. The
  table is the one in `SubstrateEnvClientRuntime.exec_async` and becomes a parity vector in PR 7.
- Tests in `client_test.go` with the fixture: a crashed actor and a no-capacity router answer, on
  `StartProcess`, `ReadFile` and `WriteFile`; `status.Code(err)` after the wrap on all three
  branches of `fromGRPCError`, `ErrNotFound` and `ErrProcessExited` included.

### PR 3: run modes

- `ShellRequest.Poll bool`: when set, `Run` starts the process, polls `Env.FindProcess` from 50 ms
  doubling to 2 s, tolerates 10 consecutive UNAVAILABLE or DEADLINE_EXCEEDED answers, reads the
  output in one non-following stream after exit, and kills the process when `ctx` ends first. The
  bounds and the retry count are the loader's (`defaultPollMin`, `defaultPollMax`, `pollRetries`);
  expose `PollInterval` only if a caller needs it.
- `ShellResponse.Duration time.Duration` (start to exit, measured by the client) and `TimedOut bool`
  (PR 3: exit 137 and `Duration >= Timeout`, as `SubstrateEnvClientRuntime.exec_async`; PR 4: the
  guest's field).
- `internal/benchmarking/swebench/backend.go`: `envSandbox.Run` calls `Run` with `Poll: true`;
  `runPolledProcess` and its test cases move into the client.

### PR 4: `timed_out` from the guest

- `proto/ateenv/v1alpha/guest.proto`: `bool timed_out = 8;` on `Process`, "true when the guest
  killed the process because `StartProcessRequest.timeout` elapsed". `guest/process` sets it from
  `ProcessState.timedOut`, which already drives `exit_class="timeout"`. `ate-env-api` forwards the
  message unchanged.
- Go: `ShellResponse.TimedOut` reads the field; `Process.Wait` and `FindProcess` return it in the
  proto. Python: `ProcessInfo.timed_out`; `ExecResult.timed_out` prefers it and keeps the heuristic
  for a guest that predates the field.
- Follow [the extend-env-surface skill](../../../.agents/skills/extend-env-surface/SKILL.md). No
  server flag; the field is zero on an older guest, and `GetServerInfo` cannot speak for the guest.
- `CHANGELOG.md`: one entry that names both clients and the guest (section 7).

### PR 5: connections

- `ClientOptions.Connections int` (0 and 1 mean one). The client dials N connections with the same
  options; `Create` and `Env` pick the next one round robin and the `Env` keeps it for every call,
  so one environment's streams stay on one connection, as the loader does today. Lifecycle calls on
  `Client` round robin. `Connections > 1` with `GRPCConn` is an error from `NewClient`.
- The loader's `EnvBackend` becomes one `env.Client` with `Connections`; `EnvBackend.pick` goes
  away.
- As built (#317): `Connections` below zero is an error too. The same pull request adds
  `ClientOptions.DialOptions` (G4 of #292), also an error with `GRPCConn`, and the handle accessors
  `Env.Atespace`, `Env.Info`, `Process.Env` and `Process.Info` (G5). Python's fleet picks a
  connection by a hash of the actor id; Go keeps the plan's round robin, so two `Env` handles for
  one environment can use two connections.

### PR 6: metrics and the span deltas

- `ClientOptions.MetricsRegisterer prometheus.Registerer`. Families and labels:
  `ate_env_client_exec_seconds{status}`, `ate_env_client_suspend_seconds{status}`,
  `ate_env_client_ready_to_first_command_seconds{status,source}` with `source="direct"`; the buckets
  of `internal/telemetry` and `SEC_BUCKETS` in `telemetry/metrics.py` (`0.05` to `600`). `status` is
  `ok`, `error` or `cancelled` (`ctx` done), as `metrics.status_of`.
- A `start_process` span on `Env.StartProcess`, as the Python `Env.start_process()` has, with the
  `exec` attributes except `ate_env.exit_code`; `ate_env.first_command=true` on the first `Run` or
  `StartProcess` of an `Env`; `ate_env.timed_out` on `exec`.
- The telemetry plan: AC-17.2 in [acceptance-criteria.md](../telemetry/acceptance-criteria.md), the
  Go row in [interfaces.md](../telemetry/interfaces.md) and an entry in
  [decisions.md](../telemetry/decisions.md) record that `ate_env.first_command` and
  `ate_env.timed_out` are in both clients from this PR on (D6).
- [docs/observability.md](../../observability.md): the Go row of the enabling table and the trainer
  families list.

### PR 7: parity vectors

- `proto/testdata/client-parity/`: `image_ref.json`, `executable.json`, `rpc_attrs.json` (one block
  per semconv mode), `error_mapping.json` (status code, message, trailer, expected class or
  sentinel, retryable; it pins the NOT_FOUND rule that reads "environment" or "actor" in the
  message, which a server message change would flip in both clients), `metric_families.json` (name,
  labels, buckets), `rpc_coverage.json` (each `EnvironmentService` RPC and the Go and Python method
  that calls it).
- `clients/go/parity_test.go` and `clients/python/tests/test_parity_vectors.py` read them; the
  Python test checks the method names by reflection. `attrs_test.go`, `executable_test.go` and the
  mirrored Python tables go.
- Step 5 and 6 of
  [the extend-env-surface skill](../../../.agents/skills/extend-env-surface/SKILL.md) name the
  vectors.

### PR 8: README and example

- `clients/go/README.md`: install (`go get`), connect, create, shell and `Run` options, processes,
  stdin and signals, PTY, files, suspend and delete, readiness (wake on first call, `StartError`,
  `Retryable`), errors (`errors.Is`, `errors.As`, `status.Code`), connections, tracing and metrics,
  the local guest daemon. Same section order as
  [the Python README](../../../clients/python/README.md) so a reader can switch.
- `clients/go/example_test.go`: `Example*` functions that compile under `make test` and show the
  README's snippets.
- `.prettierignore` is an allowlist with nothing under `clients/`; add `!/clients/go/README.md` so
  `make verify-docs` reads the file.
- The Go rows of [clients/README.md](../../../clients/README.md) and
  [docs/observability.md](../../observability.md), and a "Go client:" line in `CHANGELOG.md`.

### PR 9: e2e

- `clients/go/e2e_test.go`: skipped without the variables; against the guest daemon the 24 cases of
  `test_guest_daemon.py`, against a cluster the 5 client cases of `test_full_stack.py`, same names.
- `hack/ci/e2e.py` runs `go test ./clients/go -run TestE2E` with `ATE_ENV_API_TARGET` set, next to
  the Python full-stack tests and the loader smoke test.

## 6. Acceptance criteria

| ID    | PR  | Criterion and command                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ----- | --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AC-G1 | 1   | `go list -deps ./clients/go \| grep -E 'github\.com/agent-substrate/env/internal/\|go\.ate\.dev\|k8s\.io'` prints nothing (today it prints `internal/ate`, `internal/routertls` and `go.ate.dev/substrate/api/ateapi/v1`); `hack/verify/go-client-deps.sh` runs in the Go CI job and fails on a violation                                                                                                                                      |
| AC-G2 | 2   | With the fixture, a guest call on a CRASHED actor returns an error where `errors.As(err, &se)` gives `se.Reason == "crashed"`; with `-wake-no-capacity-grace`, `"no-capacity"`; on `StartProcess`, `ReadFile` and `WriteFile`. `status.Code(err)` returns the code on every branch of `fromGRPCError`, `codes.NotFound` on an `ErrNotFound` error included; `errors.Is(err, ErrNotFound)` and `errors.Is(err, ErrProcessExited)` are unchanged |
| AC-G3 | 3   | `Run` with `Poll` never holds a stream while the process runs (the fixture router gains a count of open guest streams); the loader's `runPolledProcess` cases pass against the client; `Duration` is set; cancelling `ctx` kills the process                                                                                                                                                                                                   |
| AC-G4 | 4   | `sh -c 'sleep 10'` with a 1 s timeout gives `TimedOut=true, ExitCode=137`; `sh -c 'kill -9 $$'` gives `TimedOut=false, ExitCode=137`; the Python e2e asserts the same two on `ExecResult.timed_out`                                                                                                                                                                                                                                            |
| AC-G5 | 5   | `Connections: 3` opens three TCP connections to the fixture; every call of one `Env` arrives on the same one; `Close` closes all; `Connections: 2` with `GRPCConn` fails in `NewClient`                                                                                                                                                                                                                                                        |
| AC-G6 | 6   | With a `prometheus.NewRegistry()`, one `Run` and one `Suspend` produce the three families with the documented labels and buckets, `ready_to_first_command` once per `Env`; without the option the registry stays empty; `StartProcess` opens a `start_process` span; `ate_env.first_command` is on the first `exec` or `start_process` span of an `Env` only                                                                                   |
| AC-G7 | 7   | Both suites read `proto/testdata/client-parity/*.json`; changing one expected value fails a Go test and a Python test; `rpc_coverage.json` lists every RPC of `env.proto` and CI fails when one has no method in either client                                                                                                                                                                                                                 |
| AC-G8 | 8   | `clients/go/README.md` has one section per operation both clients have; `go vet ./clients/go` compiles `example_test.go`; `.prettierignore` lists `!/clients/go/README.md` and `make verify-docs` passes; [clients/README.md](../../../clients/README.md) and [docs/observability.md](../../observability.md) describe the merged state                                                                                                        |
| AC-G9 | 9   | `ATE_ENV_GUEST_TARGET=127.0.0.1:8090 go test ./clients/go -run TestE2E` passes the 24 cases; `hack/ci/e2e.py` runs the cluster cases in the `e2e-kind` job                                                                                                                                                                                                                                                                                     |

## 7. Keeping parity after this plan

- [The extend-env-surface skill](../../../.agents/skills/extend-env-surface/SKILL.md), step 5 and 6:
  a new RPC or option adds its row to `rpc_coverage.json` and, when it has a mapped error or a span
  attribute, to the other vectors. CI then fails until both clients have it (AC-G7).
- `CHANGELOG.md`: a user-visible client change names its client, "Go client:" or "Python client:",
  and a change to a shared contract (a span, an attribute, a metric, a trailer) names both.
- A feature that one consumer needs in Go goes into `clients/go` when a second consumer could use
  it; PR 1 adds this rule to [docs/dev/code-layout.md](../../dev/code-layout.md) next to the import
  rule. The loader is not the place for client logic.

## 8. Tier 3: a fleet SDK in Go

Not in the pull request sequence above. If the decision is to build it, this is the shape:

- `clients/go/fleet`: `Config` (the `FleetConfig` fields that apply to Go), a `Driver` interface
  with the methods of `backend/base.py` (`Preflight`, `EnsureTemplate`, `WarmPool`, `UnwarmPool`,
  `ResizePool`, `PoolSize`, `Acquire`, `Release` with recycle, `Reap`, `BuildRuntime`), a Substrate
  driver on `env.Client` and a mock; `Fleet` with `Setup`, `WarmImages`, `SetPoolReplicas`,
  `Acquire`, `AcquireBatch`, `Release`, `Recycle`, `Teardown`, `Run` with the four strategies;
  `Handle` with `Exec` returning `ExecResult{ExitCode, Stdout, Stderr, Duration, TimedOut}`;
  `sizing` as a pure port of `sizing.py`, vector-tested; `Lookahead`; `RunReport`, the
  `ate_env_fleet_*` families and the `ate_env_fleet.*` spans.
- Source material: the loader's `WorkingSet` for the parked pool and prefetch; the Dafny models in
  [docs/plans/telemetry/verify](../telemetry/verify/README.md) and `test_warm_pool_accounting.py`
  for the accounting rules, which are language neutral and become Go property tests.
- Five to six pull requests: driver and handle; warm pool and accounting; strategies and sizing;
  report, metrics and spans; lookahead; the loader as a consumer.
- Trigger: a Go consumer other than the loader (a Go trainer or an integration), or a decision to
  delete `WorkingSet` in favor of the fleet.

## 9. Open questions

1. Tier 3 now or on the trigger above. The recommendation is the trigger.
2. D5: the `timed_out` proto field (PR 4) or the heuristic alone. The recommendation is the field;
   it also removes a documented guess from the Python client.
3. The Python direct path's `shell` span name (D6): rename to `exec`, alias, or leave.
4. Whether `Run` should default to `Poll` when `Timeout` exceeds the router's route timeout. The
   plan keeps streaming as the default and lets the caller opt in, as the loader does.

## 10. Risks

| Risk                                                                                         | Mitigation                                                                                                                                                                                                                                                                                                                                                                                                                    |
| -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A call path misses the trailer capture and drops the start reason silently                   | AC-G2 covers one call of each shape; the error-mapping vector in PR 7 is run through the fixture for every guest method                                                                                                                                                                                                                                                                                                       |
| Two `Client`s on one registry register the same family twice                                 | idempotent registration per registry (D7), tested with two clients                                                                                                                                                                                                                                                                                                                                                            |
| `Poll` changes `Run`'s output semantics (output read once after exit)                        | `Run` buffers in both modes already; the README says `Poll` is for commands longer than the route timeout and that `Output` streams live output                                                                                                                                                                                                                                                                               |
| The proto field crosses an older `ate-env-api`                                               | protobuf-go keeps unknown fields through the proxy's unmarshal and marshal; verify once in the fixture with a stale `Process` type before relying on it                                                                                                                                                                                                                                                                       |
| A change of the Go module path changes every import path after PR 8 documents one            | PR 8 writes the import path in one place per file, and the pull request that changes the path updates it                                                                                                                                                                                                                                                                                                                      |
| The nine pull requests spread over weeks while #209, #230 and #235 keep touching `client.go` | PR 1 edits the import block of `client.go`, PR 2 `errors.go`, `env.go` and `process.go`, PR 3 `types.go` and `env.go`; #209 and #235 add methods to `client.go`, constants to `tracing.go` and cases to `client_test.go` and `tracing_test.go`. The overlap is `client.go`'s import block and the `op*` block in `tracing.go`, small enough to rebase by hand; PR 1 lands right after whichever of #209 and #235 merges first |

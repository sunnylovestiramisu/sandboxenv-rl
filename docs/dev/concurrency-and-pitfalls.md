# System invariants and concurrency pitfalls

Companion to [architecture](../architecture.md) and [code layout](./code-layout.md). This guide
catalogs the concurrency invariants of `sandboxenv-rl`.

## In-guest process lifecycle and signaling

### 1. PID 1 child subreaper re-exec avoids `cmd.Wait` race

Re-exec the process as a subreaper under PID 1 before starting the gRPC daemon.

- **Location**: `cmd/ate-env-guest/init_linux.go` (`runIfPID1`, `supervise`).
- **Why**: Direct `wait4(-1, ...)` calls inside the gRPC daemon race with `os/exec.Cmd.Wait()` in
  `guest/process.Tracker`, stealing exit codes.
- **Mechanism**: `ate-env-guest` sets `prctl(PR_SET_CHILD_SUBREAPER, 1)` and re-execs via
  `os.StartProcess`. The outer PID 1 process runs `syscall.Wait4(-1, &status, syscall.WNOHANG, nil)`
  and forwards signals while the child runs `exec.Cmd`.

### 2. Process groups and negative-PID signal delivery

Start subprocesses in dedicated process groups and signal via negative PIDs (`-pid`).

- **Location**: `guest/process/tracker.go` (`Setpgid`, `Tracker.Signal`, `Tracker.Start`),
  `guest/process/pty_linux.go` (`Setsid`), `cmd/ate-env-guest/sidecar.go`.
- **Why**: Signaling only the parent PID leaves child worker trees running on cancellation.
- **Mechanism**: Non-PTY processes set `Setpgid: true` and PTYs set `Setsid: true`.
  `(*Service).SignalProcess` and process timeout timers call `Tracker.Signal`, which runs
  `syscall.Kill(-pid, sig)` to signal the entire process group.

### 3. Spool cursor notification subscription before file read

Acquire `OutputChanged()` before reading from disk to avoid missed write notifications.

- **Location**: `guest/process/service.go` (`StreamProcessOutput`), `guest/process/tracker.go`.
- **Why**: Subscribing to `ProcessState.OutputChanged()` after reading misses concurrent disk
  writes, stalling the stream until the next write or exit.
- **Mechanism**: `StreamProcessOutput` acquires `OutputChanged()` before reading and waits on it
  when zero bytes are read while running; writers close it under mutex after flushing.

## Server networking and Substrate boundaries

### 4. Router `ate-target-actor` header deduplication and SPIFFE mTLS

Append `ate-target-actor` via interceptors and do not forward incoming caller metadata.

- **Location**: `internal/apiservice/server.go` (`guestConn`, `routerCredentials`),
  `internal/apiservice/wakeretry.go` (`ensureAwake`), `internal/ate/client.go` (`DialGuest`),
  `internal/routertls/routertls.go` (`TransportCredentials`, `RejectedCertificate`).
- **Why**: Forwarding caller gRPC metadata duplicates `ate-target-actor: <atespace>/<id>`, which
  `atenet-router` rejects (`agent-substrate/substrate#2308`).
- **Mechanism**: `guestConn` and `DialGuest` append `ate-target-actor` without forwarding caller
  metadata. `internal/routertls` hot-reloads kubelet-projected `-atenet-client-cert` and
  `servicedns` `-atenet-ca-file` bundles on handshakes. `routertls.TransportCredentials`
  (`alertConn.Write`) reads pending post-handshake TLS 1.3 alerts on initial HTTP/2 write failures
  (`EPIPE`), letting `ensureAwake` match `routertls.RejectedCertificate` on the first probe, before
  any wait, while `DialGuest` callers receive the TLS alert error.

### 5. Actor wake budgets, fail-fast start errors, and actor deletion

Verify readiness within wake budgets, fail fast on unrecoverable errors, and evict on delete.

- **Location**: `internal/apiservice/wakeretry.go` (`ensureAwake`, `StartFailureKey`),
  `internal/ate/client.go` (`Delete`, `evictGuest`).
- **Wake budgets**: Traffic to a suspended actor resumes it in `atenet`. `ensureAwake` polls
  `GetProcess("ate-env-wake-probe")` under `DefaultWakeBudget` (5m) and `DefaultWakeRunningBudget`
  (20s once `RUNNING`), returning `Canceled` or `DeadlineExceeded` probe errors immediately.
- **Fail-fast start errors**: `ensureAwake` exits early on unrecoverable states:
  `routertls.RejectedCertificate` returns `codes.FailedPrecondition` with
  `routertls.RejectionAdvice` and no trailer, `CRASHED` returns `codes.FailedPrecondition` with
  `ActorStatus.Crash.Message` and trailer `ate-env-start-failure: crashed`, `DELETING` returns
  `codes.NotFound`, and `"no worker has room for the actor"` persisting across `WakeNoCapacityGrace`
  while not `RUNNING` (default `0` waits `WakeBudget`) returns `codes.ResourceExhausted`
  (`ate-env-start-failure: no-capacity`).
- **Delete sequence**: `internal/ate.Client.Delete` calls `DeleteActor` directly in any state
  including `CRASHED` (terminating workloads, releasing slots, and deleting snapshots without
  pre-suspending) and calls `evictGuest` to drop the pooled `guestPool` connection.

### 6. Idempotent derived `ActorTemplate` creation and failed golden-snapshot recovery

Serialize derived template creation and recovery with `imageTemplateLocks`, and treat
`codes.AlreadyExists` as success after the same-digest check on the winner.

- **Location**: `internal/apiservice/imagetemplate.go` (`DeriveImageTemplate`),
  `internal/apiservice/templaterecover.go` (`imageTemplateLocks`, `imageTemplateLock`,
  `recreateFailedImageTemplate`, `failedTemplateRecreateAfter`, `templateUsers`, `keptUID`),
  `internal/apiservice/server.go` (`ensureImageTemplate`, `getOrCreateImageTemplate`,
  `sameImageDigest`).
- **Why**: Concurrent workers race to create `<base>-<digest:12>` (`imageTemplateDigestLen = 12`),
  Substrate leaves terminal `golden_snapshot_status.error_message` failures unretried, and deleting
  an in-use template strips rootfs volumes upon resume.
- **Mechanism**: `ensureImageTemplate` holds a per-template read lock (`imageTemplateLocks`) through
  `CreateActor`. `getOrCreateImageTemplate` reuses an existing template when `sameImageDigest`
  matches across repository paths, returns `codes.FailedPrecondition` on mismatch, and treats
  `codes.AlreadyExists` as success after the same-digest check on the winner. If a golden snapshot
  failed `>= failedTemplateRecreateAfter` (10m) ago, `ensureImageTemplate` upgrades to a write lock
  and calls `recreateFailedImageTemplate` (checking zero `templateUsers`) to recreate by UID.

## Client connection pooling and event loops

### 7. Python `LoopRunner` thread bridging and HTTP/2 stream concurrency

Dispatch synchronous Python calls to a persistent `LoopRunner` background thread.

- **Location**: `clients/python/src/ate_env_rl/loop_runner.py`,
  `clients/python/src/ate_env_rl/fleet.py`, `clients/python/src/ate_env_rl/backend/substrate.py`,
  `cmd/ate-env-api/main.go`.
- **Why**: Calling async gRPC methods via `asyncio.run()` creates a short-lived loop that destroys
  open gRPC channels on exit and corrupts HTTP/2 channel state.
- **Mechanism**: `LoopRunner` runs a daemon thread with a persistent event loop and dispatches
  coroutines via `asyncio.run_coroutine_threadsafe`. `SandboxFleet` sizes connections via
  `FleetConfig.connections` or `ceil(max_concurrent / 200)`, and `SubstrateBackendDriver` holds the
  pooled gRPC channels against `DefaultMaxConcurrentStreams = 4096`.

### 8. Python release error tracking and teardown retries

Record release errors in `RunReport` and retry unreleased actors during `fleet.teardown()`.

- **Location**: `clients/python/src/ate_env_rl/backend/substrate.py`.
- **Why**: Network errors during `delete` must not crash workers or leak orphan actors.
- **Mechanism**: `SubstrateBackendDriver` records failed deletions in `RunReport.release_errors` and
  keeps the actor in `_owned_actors`; `fleet.teardown()` retries all unreleased actors and raises
  `InfrastructureError` if any cannot be reclaimed.

### 9. Python warm-pool bookkeeping across awaits

Re-check teardown state after each await before pooling an actor, and reserve warm slots upfront.

- **Location**: `clients/python/src/ate_env_rl/backend/substrate.py` (`_release_internal`,
  `_warm_pool_internal`, `_acquire_internal`, `_reap_internal`, `_return_popped`,
  `_delete_unpooled`, `_keep_for_any_reap`, `_may_pool`, `_cancel_warm_calls`,
  `_own_abandoned_create`), `clients/python/tests/test_warm_pool_accounting.py`.
- **Why**: Recycles pop actors from `_owned_actors` during `suspend()`; pooling after a concurrent
  `reap` leaks actors past teardown, and unreserved shortfalls over-provision.
- **Teardown & claim checks**: `reap` increments `_teardowns` on entry and exit (`_reaping` counts
  active reaps); recycles, `warm_pool`, and warm claims read `_teardowns` before awaiting and pool
  only when unchanged with `_reaping == 0` (`_may_pool`), otherwise deleting the actor. A warm claim
  whose `get()` succeeds checks `_may_pool` and takes ownership in one lock hold, or deletes any
  stale still-owned actor and retries.
- **Ownership & generations**: Actors stay in `_owned_actors` until deletion succeeds. A cold
  `acquire` whose create overlaps its own run's reap owns the actor under `WARM_POOL_RUN_ID` too;
  its handle keeps the run id. A cold create cancelled in flight (for example by `acquire`'s
  `timeout_s`) owns the actor under `WARM_POOL_RUN_ID` (`_own_abandoned_create`), since the create
  may have landed. `_reap_internal` clears `_warm_inflight` on entry and on exit, dropping warm
  reservations on reap. `_warm_inflight` tracks in-flight creations per pool key, `_teardowns`, and
  generation (`_pool_gens`), subtracting them from shortfalls; `unwarm_pool`, and a `resize_pool`
  below pooled plus in-flight, bump `_pool_gens` (`_cancel_warm_calls`), so a `warm_pool` call that
  started before deletes what it created (`drained` in `_warm_pool_internal`) and does not pool it.
  `_delete_unpooled` re-tags a failed or cancelled delete of an unpooled actor under
  `WARM_POOL_RUN_ID` unconditionally via `_keep_for_any_reap` (distinct from `release` routing
  popped actors through `_return_popped` based on `_may_pool`). `reap` updates the warm-replica
  gauge alongside the cleared pool, and `tests/test_warm_pool_accounting.py` exercises these
  interleavings with a Hypothesis state machine.

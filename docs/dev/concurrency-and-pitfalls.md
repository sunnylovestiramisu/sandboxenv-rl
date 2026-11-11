# System invariants and concurrency pitfalls

Companion to [architecture](../architecture.md) and [code layout](./code-layout.md).

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
- **Mechanism**: Non-PTYs set `Setpgid: true` and PTYs `Setsid: true`; `(*Service).SignalProcess`
  and timeouts call `Tracker.Signal` (`syscall.Kill(-pid, sig)`) on the process group.

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
  metadata. `internal/routertls` hot-reloads kubelet `-atenet-client-cert` and `servicedns`
  `-atenet-ca-file` on handshakes. `routertls.TransportCredentials` (`alertConn.Write`) reads TLS
  1.3 post-handshake alerts on initial HTTP/2 `EPIPE` writes, letting `ensureAwake` match
  `routertls.RejectedCertificate` on first probe before any wait, while `DialGuest` gets the alert.

### 5. Actor wake budgets, fail-fast start errors, and actor deletion

Verify readiness within wake budgets, fail fast on unrecoverable errors, and evict on delete.

- **Location**: `internal/apiservice/wakeretry.go` (`ensureAwake`, `StartFailureKey`),
  `internal/ate/client.go` (`Delete`, `evictGuest`).
- **Wake budgets**: Traffic to a suspended actor resumes it in `atenet`. `ensureAwake` polls
  `GetProcess("ate-env-wake-probe")` under `DefaultWakeBudget` (5m) and `DefaultWakeRunningBudget`
  (20s once `RUNNING`), returning `Canceled` or `DeadlineExceeded` probe errors immediately. The
  state read after an `Unavailable` probe gets the rest of `WakeBudget`, and of
  `WakeNoCapacityGrace` during a no-room run, but never more than `WakeStateReadCap`
  (`-wake-state-read-cap`, default `DefaultWakeStateReadCap`, 5s): the control dial sets no
  keepalive and no per-call timeout, so a read that hangs without an error is cut there.
  `Unavailable`, `DeadlineExceeded` or `ResourceExhausted` from that read, or a read cut at its
  deadline, leaves the state unknown, and the wait goes on. When all reads are cut, the wait probes
  once per `-wake-state-read-cap` plus backoff and ends at `-wake-budget` with `Unavailable` "while
  UNKNOWN".
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

Serialize derived templates (`imageTemplateLocks`); `AlreadyExists` succeeds on matching digest.

- **Location**: `internal/apiservice/imagetemplate.go` (`DeriveImageTemplate`),
  `internal/apiservice/templaterecover.go` (`imageTemplateLocks`, `imageTemplateLock`,
  `recreateFailedImageTemplate`, `failedTemplateRecreateAfter`, `templateUsers`, `keptUID`),
  `internal/apiservice/server.go` (`ensureImageTemplate`, `getOrCreateImageTemplate`,
  `sameImageDigest`).
- **Why**: Concurrent workers race to create `<base>-<digest:12>` (`imageTemplateDigestLen = 12`),
  Substrate leaves terminal `golden_snapshot_status.error_message` failures unretried, and deleting
  an in-use template strips rootfs volumes upon resume.
- **Mechanism**: `ensureImageTemplate` holds a per-template read lock, held in this process only
  (`imageTemplateLocks`; see `docs/plans/ate-env-api/acceptance-criteria.md`), through
  `CreateActor`. `getOrCreateImageTemplate` reuses on cross-path `sameImageDigest`, fails with
  `codes.FailedPrecondition` on mismatch, and treats `codes.AlreadyExists` as success after the
  winner's digest check. If a golden snapshot failed `>= failedTemplateRecreateAfter` (10m) ago,
  `ensureImageTemplate` write-locks and calls `recreateFailedImageTemplate` (recreating by UID at
  zero `templateUsers`).

## Client connection pooling and event loops

### 7. Python `LoopRunner` thread bridging and HTTP/2 stream concurrency

Dispatch synchronous Python calls to a persistent `LoopRunner` background thread.

- **Location**: `clients/python/src/ate_env_rl/loop_runner.py`,
  `clients/python/src/ate_env_rl/fleet.py`, `clients/python/src/ate_env_rl/backend/substrate.py`,
  `cmd/ate-env-api/main.go`.
- **Why**: `asyncio.run()` creates short loops that destroy HTTP/2 gRPC channels on exit.
- **Mechanism**: `LoopRunner` runs a daemon thread with a persistent event loop and dispatches
  coroutines via `asyncio.run_coroutine_threadsafe`. `SandboxFleet` sizes connections via
  `FleetConfig.connections` or `ceil(max_concurrent / 200)`, and `SubstrateBackendDriver` holds the
  pooled gRPC channels against `DefaultMaxConcurrentStreams = 4096`.

### 8. Python release error tracking and teardown retries

Record release errors in `RunReport` and retry unreleased actors during `fleet.teardown()`.

- **Location**: `clients/python/src/ate_env_rl/backend/substrate.py`.
- **Why**: Network errors during `delete` must not crash workers or leak orphan actors.
- **Mechanism**: `SubstrateBackendDriver` logs failed deletes in `RunReport.release_errors` and
  retains `_owned_actors`; `fleet.teardown()` retries, raising `InfrastructureError` if any fail.

### 9. Python warm-pool bookkeeping across awaits

Re-check teardown state after each await before pooling an actor, and reserve warm slots upfront.

- **Location**: `clients/python/src/ate_env_rl/backend/substrate.py` (`_release_internal`,
  `_warm_pool_internal`, `_acquire_internal`, `_reap_internal`, `_return_popped`,
  `_delete_unpooled`, `_keep_for_any_reap`, `_may_pool`, `_cancel_warm_calls`,
  `_own_abandoned_create`), `clients/python/tests/test_warm_pool_accounting.py`.
- **Why**: Recycles pop actors from `_owned_actors` during `suspend()`; pooling after a concurrent
  `reap` leaks actors past teardown, and unreserved shortfalls over-provision.
- **Teardown & claim checks**: `reap` bumps `_teardowns` on entry/exit (`_reaping` counts active
  reaps); recycles, `warm_pool`, and claims read `_teardowns` before awaiting and pool only via
  `_may_pool` (`_teardowns` unchanged and `_reaping == 0`), else delete; claims re-check after
  `get()` in one lock or delete stale still-owned actors and retry.
- **Ownership & generations**: Actors stay in `_owned_actors` until deleted. A cold `acquire`
  overlapping its run's reap also owns the actor under `WARM_POOL_RUN_ID` (handle keeps run id); a
  cancelled create (e.g., `acquire` `timeout_s`) takes `WARM_POOL_RUN_ID` via
  `_own_abandoned_create` in case it landed. `_reap_internal` clears `_warm_inflight` (per pool key,
  `_teardowns`, and `_pool_gens`; subtracted from shortfalls) on entry/exit, dropping warm
  reservations on reap. `unwarm_pool` and `resize_pool` below pooled + in-flight bump `_pool_gens`
  (`_cancel_warm_calls`), so earlier `warm_pool` calls (`drained` in `_warm_pool_internal`) delete
  instead of pooling. `_delete_unpooled` unconditionally re-tags failed/cancelled deletes under
  `WARM_POOL_RUN_ID` via `_keep_for_any_reap` (unlike `release`, which routes via `_return_popped`
  and `_may_pool`). `reap` updates the warm-replica gauge (tested via Hypothesis state machine).

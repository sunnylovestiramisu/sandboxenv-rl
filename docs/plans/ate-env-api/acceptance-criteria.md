# ate-env-api: acceptance criteria

Status: `AE-1` and `AE-2` met by #244, which fixed two bugs that property tests found against them.
`AE-3`, `AE-5` (`AE-5` per replica), `WF-1`, `WF-2` and `WF-8` met on `main` (`#86` wake budgets,
`#202` the five-minute default, and `#222` fail-fast on crash and no capacity). `WF-7` met by #255,
which fixed a wake-probe cancellation bug found by property and state-machine tests in
`internal/apiservice`. `AE-4` met by #278, which also holds a base that already mounts the guest to
the guest-slot rule. `WF-3` to `WF-6` met by #275, which keeps a wake going through a transient
control-plane error on the state read and bounds that read by the wake budget and the no-capacity
grace. Every criterion in this file is scoped to the Go `ate-env-api` server and names the tests
that check it on every pull request in the Go job (`go test -race ./...`).

## Derived image templates

### Scope

`CreateEnvironment` with a `rootfs_image` pinned by digest runs the environment on a template
derived from a base template: the same template, with the image as its container's root filesystem
and the guest mounted from the base's image at `/ate`
([internal/apiservice/imagetemplate.go](../../../internal/apiservice/imagetemplate.go),
`getOrCreateImageTemplate` in
[internal/apiservice/server.go](../../../internal/apiservice/server.go)). The derived template is
named `<base>-<first 12 hex digits of the digest>` and is created on first use. Within one replica,
a template whose golden snapshot failed is recreated when no actor a recreate would harm uses it.
[docs/architecture.md](../../architecture.md) describes the flow, and the threat model lists the
image check as the mitigation for "a caller runs an image of its choice".

### Criteria

| ID   | Criterion                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Status                      | Tests                                                                                                                                       |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| AE-1 | A `CreateEnvironment` that succeeds runs the image it asked for. A derived template that runs another image is refused with `FailedPrecondition`, whether it existed before the call or a concurrent create made it.                                                                                                                                                                                                                                                                                                                    | met by #244                 | `TestPropConcurrentCreatesUseRequestedImage`, `TestConcurrentCreateOnCollidingDigestFailsPrecondition`, `TestPropSameImageDigest`           |
| AE-2 | A derived template's guest command resolves under `/ate` for every base command `rerootCommand` accepts, so the guest is found at boot.                                                                                                                                                                                                                                                                                                                                                                                                 | met by #244                 | `TestPropRerootStaysUnderMount`, `TestRerootKeepsDotDotUnderMount`                                                                          |
| AE-3 | The derived name is `<base>-<first 12 hex digits>`: the same for any repository path or tag of one digest, always a valid Kubernetes name of at most 63 characters, and never a panic or an invalid name for any input.                                                                                                                                                                                                                                                                                                                 | met on `main`               | `TestPropImageTemplateNameIdentity`, `TestPropImageTemplateNameNeverInvalid`                                                                |
| AE-4 | Deriving from a derived template equals deriving from its base: `Derive(Derive(b, n1, i1), n2, i2) == Derive(b, n2, i2)`. The base is never changed, and the derived template is one Substrate accepts: unique volume names, and the guest volume alone at `/ate`. A base whose volume or mount already takes the guest's name or `/ate`, or that mounts the guest anywhere but once at `/ate`, or that mounts the guest but has no command or a command that does not start the guest binary under `/ate`, is refused with the reason. | met by #278                 | `TestPropDeriveComposes`, `TestDeriveImageTemplateRefusesClashWithGuestSlot`, `TestDeriveImageTemplateRefusesInjectedBaseWithGuestNotAlone` |
| AE-5 | Within one ate-env-api replica, concurrent creates on a derived template whose golden snapshot failed all succeed. The template is deleted and recreated at most once, only when no actor a recreate harms uses it, no actor is created while it is absent, and exactly one remains. Across replicas this is a known limitation (see below).                                                                                                                                                                                            | met on `main` (one replica) | `TestPropConcurrentRecreate`; `TestRecreateAcrossReplicasSparesActorCreatedAfterInUseCheck` (skipped: known limitation)                     |

### Known limitations

- **AE-5 across replicas.** The in-use check and the delete in `recreateFailedImageTemplate` are
  serialized by an in-process lock only. That lock does not reach other ate-env-api replicas.
  Deployments with more than one replica (`--api-replicas`) are affected: the benchmark SUT runs 2
  by default, and large RL runs use 4 to 8. In those deployments another replica can create an actor
  on the failed template between this replica's in-use check and its delete, and the template is
  then recreated under a `RUNNING` actor. The criterion is therefore stated per replica.
  `TestRecreateAcrossReplicasSparesActorCreatedAfterInUseCheck` reproduces the interleaving with two
  replicas on one control plane and is skipped until a cross-replica guard exists (#273).

### What the property tests found

- **AE-1.** `getOrCreateImageTemplate` checked the image of a template that already existed, but
  when its own create lost a race (`AlreadyExists`) it returned the winner's template unchecked. Two
  digests that share their first 12 hex digits map to one derived name, so the losing caller's
  environment ran the winner's image. Found by `TestPropConcurrentCreatesUseRequestedImage`, which
  draws digests that share a prefix together with RPC delays. It needs that input and that ordering
  at once, which no example test had.
- **AE-2.** `rerootCommand` used `path.Join("/ate", cmd)`, which resolves a leading `..`:
  `/../ko-app/ate-env-guest` became `/ko-app/ate-env-guest`, outside the guest mount. Found by
  `TestPropRerootStaysUnderMount`, which draws paths with `..`, `.` and empty segments. The example
  tests used clean paths only.

- **AE-4.** For a base that runs the guest as its container image, `DeriveImageTemplate` added the
  guest volume and its `/ate` mount without checking that the base left room for them. A base with a
  non-image volume named `guest`, or a mount at or under `/ate`, derived a template with two `guest`
  volumes or nested mounts, which Substrate rejects, so every `rootfs_image` create on that base
  failed with an error about a template the operator never wrote. Found by `TestPropDeriveComposes`
  once its generator drew such bases; it had only drawn `modeABase` with fixed volume names and
  paths. A later audit (#273) found the same gap in the other base shape: a base that already mounts
  the guest image volume skipped the check, so another mount at or under `/ate`, or the guest
  mounted somewhere else, carried over into the derived template. `TestPropDeriveComposes` missed it
  because `genBaseTemplate` drew only guest-as-image bases, and the composition step derived from a
  base it had just built itself, which never has such a mount. It now also draws already-derived
  bases with extra mounts and a moved guest mount. Review of #278 found that the command of such a
  base also carried over unchecked: with no command, or `/ko-app/ate-env-guest` outside `/ate`, the
  guest is not found at boot. The property had drawn `nocmd` for those bases and accepted it by
  construction. It now draws commands under `/ate`, outside it (also through `..`), a shell under
  it, and none, and only the first derives.

Neither `AE-1`, `AE-2` nor `AE-4` is a clock bug, so `testing/synctest` alone would not have
generated those inputs.

## Wake budgets, fail-fast, and cancellation

### Scope

A guest call to an environment that is suspended or still starting waits in `ensureAwake`
([internal/apiservice/wakeretry.go](../../../internal/apiservice/wakeretry.go)). The loop
(`awaitWake`) probes `ProcessService.GetProcess` through the `atenet` router, reads the actor's
status from the control plane when the probe answers `Unavailable`, and backs off. Four flags bound
the wait: `-wake-budget` (`DefaultWakeBudget`, 5 minutes), `-wake-running-budget`
(`DefaultWakeRunningBudget`, 20 seconds from the first attempt in the current `RUNNING` run),
`-wake-no-capacity-grace` (disabled at `0`), and `-wake-state-read-cap` (`DefaultWakeStateReadCap`,
5 seconds), which caps each state read. Terminal start failures attach `StartFailureKey`
(`ate-env-start-failure`) in unary and streaming gRPC response trailers.

### Criteria

| ID   | Criterion                                                                                                                                                                                                                                                                                                                     | Status        | Tests                                                                                                                                                                                                                                                                                             |
| ---- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| WF-1 | A probe the guest answers (any status code except `Unavailable`, `Canceled`, or `DeadlineExceeded`) ends the wait at once with no error and records `ate_env_api_create_to_ready_seconds{status="ok"}` when tracked.                                                                                                          | met on `main` | `TestAwaitWakeProperties`, `TestEnsureAwakeSynctestStateMachine`                                                                                                                                                                                                                                  |
| WF-2 | An unreachable guest whose actor is `CRASHED` ends the wait at once with `FailedPrecondition` and the control plane's crash reason when set. `DELETING` ends the wait at once with `NotFound`. No later probe runs.                                                                                                           | met on `main` | `TestAwaitWakeProperties`, `TestEnsureAwakeSynctestStateMachine`                                                                                                                                                                                                                                  |
| WF-3 | `ResourceExhausted` occurs only with a positive `WakeNoCapacityGrace`, after a contiguous run of router no-capacity answers (while the control plane does not report the actor `RUNNING`, `CRASHED`, or `DELETING`) that lasted at least the grace period.                                                                    | met by #275   | `TestAwaitWakeProperties`, `TestAwaitWakeRetriesTransientStateReadErrors`, `TestEnsureAwakeSynctestStateMachine`                                                                                                                                                                                  |
| WF-4 | Once a no-capacity run reaches `WakeNoCapacityGrace` at a probe's answer, that probe is the last one, and the probe that ends the run starts within `WakeNoCapacityGrace` plus 1 ms.                                                                                                                                          | met by #275   | `TestAwaitWakeProperties`, `TestAwaitWakeStateReadBoundedByNoCapacityGrace`                                                                                                                                                                                                                       |
| WF-5 | A timeout (`Unavailable`) occurs only once `WakeBudget` expires overall or `WakeRunningBudget` expires from the start of the current contiguous `RUNNING` run. A state read that leaves the state unknown keeps that run. A state read that hangs without an error is cut at the 5 s per-read cap, and the wait probes again. | met by #275   | `TestAwaitWakeProperties`, `TestAwaitWakeRetriesTransientStateReadErrors`, `TestAwaitWakeTransientReadKeepsRunningBudget`, `TestAwaitWakeStateReadCappedPerRead`, `TestEnsureAwakeRetriesTransientGetActorErrors`, `TestEnsureAwakeSynctestStateMachine`, `TestEnsureAwakeExactBudgetsInSynctest` |
| WF-6 | The wait never runs past `WakeBudget` by more than one probe latency.                                                                                                                                                                                                                                                         | met by #275   | `TestAwaitWakeProperties`, `TestAwaitWakeStateReadBoundedByWakeBudget`, `TestEnsureAwakeExactBudgetsInSynctest`                                                                                                                                                                                   |
| WF-7 | Caller cancellation or deadline expiry during a parked probe or backoff ends the wait at once with `Canceled` or `DeadlineExceeded` and never records `ate_env_api_create_to_ready_seconds{status="ok"}` or `wake_wait` as `woke`.                                                                                            | met by #255   | `TestAwaitWakeProperties`, `TestEnsureAwakeSynctestStateMachine`, `TestEnsureAwakeExactBudgetsInSynctest`, `TestAwaitWakePropagatesProbeCancelAndDeadlineWithLiveCallerCtx`                                                                                                                       |
| WF-8 | Unary and streaming guest calls attach `StartFailureKey` (`ate-env-start-failure`) set to `StartFailureCrashed` on `WF-2` crash and `StartFailureNoCapacity` on `WF-3` no-capacity exhaustion, and omit it on `WF-5` wake timeouts.                                                                                           | met on `main` | `TestCrashedWhileWakingCarriesTheCrashReason`, `TestCrashedWhileWakingMarksStreamingTrailers`, `TestNoCapacityFailsAfterTheGrace`, `TestNoCapacityWaitsWithoutAGrace`                                                                                                                             |

### What the property and `synctest` tests found

- **WF-7.** `ensureAwake` checked `status.Code(err) != codes.Unavailable` after
  `probe.GetProcess(ctx, ...)` and treated every other status code as a guest answer. When a caller
  cancelled `ctx` or its deadline expired while `atenet` parked the probe, `GetProcess` returned
  `codes.Canceled` or `codes.DeadlineExceeded`. `ensureAwake` returned `nil`, forwarded the caller's
  actual RPC onto an unwoken environment, and recorded `ate_env_api_create_to_ready_seconds` with
  `status="ok"` and `ate_env_api_wake_wait_seconds` with `outcome="woke"`. Caught by pairing
  `pgregory.net/rapid` with `testing/synctest` (`internal/internaltest/synctestrapid`) across parked
  probes and caller cancellations.
- **WF-3 to WF-6.** After an `Unavailable` probe, `awaitWake` read the actor's state with the
  caller's `ctx` and returned any error from that read. A control-plane blip (`Unavailable`,
  `DeadlineExceeded`) ended the wake at once, before any budget, with an error the caller could not
  tell from a timeout; a control-plane `ResourceExhausted` (a quota) ended it as if the no-capacity
  grace had run out. The read had no deadline of its own, so its latency could push the wait past
  `WakeBudget` and the probe that ends a no-capacity run past the grace. An audit (#273) found all
  three; `TestAwaitWakeProperties` missed them because its `readStatus` fake always answered at once
  with a state. It now draws a latency for each read and, sometimes, a control-plane `Unavailable`,
  `DeadlineExceeded`, `ResourceExhausted`, `NotFound` or `Internal` instead of the state. Review of
  #275 found that a read that hangs without an error still held the wait, with no probe, until
  `WakeBudget`; each read is now capped at 5 seconds, and the property draws the cap.

### How the wake tests check them

- `TestAwaitWakeProperties` (`internal/apiservice/wakeretry_property_test.go`) runs 2,000 generated
  timelines inside `synctestrapid.Check`, drawing budgets, optional `WakeNoCapacityGrace`, probe
  latencies, router answers, actor states, state-read latencies and control-plane errors, a per-read
  cap of 50 ms to 4 s against read latencies of up to 3 s, and caller cancellation or deadline
  instants on `testing/synctest`'s fake clock. It checks that every state read ends by its exact
  deadline (the earliest of the wake budget, the grace end, and the cap).
- `TestEnsureAwakeRetriesTransientGetActorErrors` (`internal/apiservice/wakeretry_synctest_test.go`)
  fails two `GetActor` calls with `fakecontrol.FailGets` and checks that the code survives
  `ate.Client.Get` and `toGRPCError`, and that the wake probes again and succeeds.
- `TestEnsureAwakeSynctestStateMachine` (`internal/apiservice/wakeretry_synctest_test.go`) drives
  concurrent `ensureAwake` callers through `ate.NewControlClient(ctrl.Client())` and an in-bubble
  gRPC connection inside `synctestrapid.Repeat`, checking `WF-5` budget bounds, `WF-7` caller
  cancellation and deadline propagation, `routertls.RejectedCertificate` fast-fail matching on
  simulated TLS probe errors, `create_to_ready`, and `wake_wait` invariants at every
  `synctest.Wait()` checkpoint.
- `TestGuestCallFailsFastOnRejectedCertificate` (`internal/apiservice/wakeretry_test.go`) exercises
  real post-handshake TLS 1.3 alert delivery through `routertls.TransportCredentials` against
  `fakerouter`.
- `internal/apiservice/startfailure_test.go` exercises end-to-end unary and streaming gRPC trailers
  for `StartFailureKey` against `fakecontrol` and `fakerouter`.

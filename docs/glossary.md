# sandboxenv-rl glossary

Companion to [docs/architecture.md](./architecture.md). See also
[Agent Substrate's glossary](https://github.com/agent-substrate/substrate/blob/main/docs/glossary.md)
for control-plane terms.

## API resources and handles

- **Environment**: The protobuf wire representation of one sandboxed execution environment
  (`proto/ateenv/v1alpha/env.proto`), corresponding 1:1 to a Substrate actor, carrying status,
  hosting `node`, and optional `worker_selector`. Distinct from client handles `Env` and
  `SandboxHandle`.
- **Env**: The low-level client handle (`clients/go.Env` and `ate_env_rl.Env`) for managing a single
  environment (`start_process`, `shell`, `read_file`, `write_file`, `suspend`, `delete`). Neither
  SDK defines a class named `Environment`.
- **Sandbox** / **SandboxHandle**: The fleet handle (`ate_env_rl.SandboxHandle`) returned by
  `SandboxFleet.acquire` and `AsyncSandboxFleet.acquire` that wraps an environment with fleet
  recycling and both sync and async execution methods.
- **Process**: A command execution running inside an environment, tracked by `guest/process.Tracker`
  with an ID, PID, exit status, and 10 MiB disk spools (`process.DefaultMaxLogBytes`). Runs in a
  dedicated process group (`Setpgid: true`, PTY `Setsid: true`) signaled via `-pid`.
- **Template**: The name of the Substrate `ActorTemplate` used to instantiate an actor, defaulting
  to `"default-template"` via `internal/apiservice.DefaultTemplate`.
- **Derived template**: An `ActorTemplate` named `<base>-<digest:12>` generated on demand by
  `internal/apiservice/imagetemplate.go` when `CreateEnvironmentRequest` specifies a custom
  `rootfs_image`. Reused when a later request carries the same digest under another repository path
  (`sameImageDigest`).
- **Rootfs image**: A digest-pinned (`@sha256:...`) OCI container image used as the actor root
  filesystem, into which `ate-env-guest` is injected at `/ate` (`GuestMountPath`).
- **Runtime layer**: An extra digest-pinned OCI image volume mounted at a user-specified path inside
  the actor, configured on base templates via `ate-env manifest template --layer` and carried over
  by derivation.
- **Atespace** / **Tenancy**: Substrate's namespace boundary for actors, templates, and
  `AccessPolicy` records. Defaults to `"ate-env"` across Go (`internal/ate.DefaultAtespace`),
  protobuf, CLI, and Python (`FleetConfig.tenancy`).

## Substrate primitives

- **Actor**: The Substrate execution instance (gVisor container) running one environment, routed
  through `atenet` via the `ate-target-actor: <atespace>/<actor-id>` header
  (`go.ate.dev/substrate/api/ateapi/v1.Actor`).
- **ActorTemplate**: The Substrate specification
  (`go.ate.dev/substrate/api/ateapi/v1.ActorTemplate`) defining container images, commands, OCI
  image volumes, millicore CPU and byte memory limits, snapshot buckets, and worker pools.
- **Snapshot**: A filesystem and memory checkpoint taken when an actor suspends: stored in object
  storage before the call returns (`SuspendOptions{Mode: DURABILITY_MODE_SYNCHRONOUS}`), or on the
  worker's node first with the upload in the background (`DURABILITY_MODE_ASYNCHRONOUS`,
  `SuspendLocal`).
- **Worker** / **WorkerPool**: The Substrate pool of pre-started worker pods running
  `ateworker-gvisor` that execute actors inside gVisor (`runsc`).
- **AccessPolicy**: The Substrate authorization record granting
  `system:serviceaccount:ate-env:ate-env-api` permissions to manage templates and actors in an
  atespace.

## Components and runtime state

- **`ate-env-api`**: The in-cluster server (`cmd/ate-env-api`, `internal/apiservice`) exposing gRPC
  services, the MCP endpoint, and the HTTP/WebSocket port proxy, with no durable disk state.
- **`ate-env-guest`**: The daemon (`cmd/ate-env-guest`, `guest/`) running inside every actor on TCP
  port 80 (plaintext HTTP/1.1 and h2c) as Linux PID 1 child subreaper, supervising sidecars and
  serving process and file RPCs.
- **`ate-env`**: The CLI tool (`cmd/ate-env`) for managing environments, executing commands,
  transferring files, and generating deployment manifests.
- **`SandboxFleet`** / **`AsyncSandboxFleet`**: The Python SDK synchronous and asyncio fleet
  managers (`clients/python/src/ate_env_rl/fleet.py`, `async_fleet.py`) for batched RL rollouts and
  evaluations.
- **`LoopRunner`**: A dedicated background thread running a permanent `asyncio` event loop
  (`clients/python/src/ate_env_rl/loop_runner.py`), enabling synchronous Python facades to share
  gRPC channels safely.
- **Suspend**: Checkpointing a running actor and releasing its worker slot: durably with
  `SuspendOptions{Mode: DURABILITY_MODE_SYNCHRONOUS}` (`Suspend`), or locally with
  `DURABILITY_MODE_ASYNCHRONOUS` (`SuspendLocal`), after which the environment reports
  `ENVIRONMENT_STATUS_PAUSED` until the upload lands.
- **Wake** / **`ensureAwake`**: Automatic restoration of a suspended actor when `ate-env-api` routes
  traffic through `atenet`. Bounded by `DefaultWakeBudget` (5 minutes bounds the entire wait) and
  `DefaultWakeRunningBudget` (20 seconds bounds the wait once `RUNNING`).
- **Park**: The between-step action in multi-step RL rollouts (`suspend`, `delete`, or `none`).
- **TTFE**: Time To First Execution, measuring total initialization and run latency
  (`ready_s + first_cmd_s`) in `integrations/nemo-gym/benchmarks/driver_sim.py`.
- **TTFI**: Time To First Interaction, emitted as `EventTTFI` by `internal/benchmarking/swebench`,
  measuring wall time from starting `CreateEnvironment` until the first command completes inside the
  guest daemon.

## Benchmark reporting

- **Trellis**: The rollout reliability taxonomy the benchmark reports follow (`fault_classification`
  in `report.json`): a failed rollout is an infrastructure fault before the episode (tier 1), a
  policy outcome during it (tier 2) or an infrastructure fault during it (tier 3); only tiers 1 and
  3 count in `infra_fault_rate`.
- **M2 (Trellis)**: The Trellis milestone whose target the reports check: fewer than 2% of rollouts
  dropped by infrastructure faults (`fault_classification.target_rate`, `0.02`, `meets_target`).
  Distinct from milestone `M2` of `internal/benchmarking/DESIGN.md`.

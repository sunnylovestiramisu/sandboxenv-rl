# sandboxenv-rl code layout

Companion to [docs/architecture.md](../architecture.md) and [AGENTS.md](../../AGENTS.md).

## Repository layout

```
cmd/
  ate-env/               # CLI: environment management and manifest generation (ate-env manifest)
  ate-env-api/           # API server daemon: gRPC services, MCP handler, port proxy
  ate-env-guest/         # Guest daemon: Linux PID 1 subreaper init, sidecar supervision
  benchmarking/
    bench/               # Composable RL benchmark CLI (scenario lifecycle subcommands)
    swebench-loader/     # Benchmark driver: SWE-bench trajectories, placement, preflight, event recording, reports
internal/
  apiservice/            # gRPC server implementation: auth, image template derivation, wake retries
  ate/                   # Substrate client adapter: dials ateapi control plane and atenet router
  benchmarking/          # Benchmark packages: job, measure, overlay, schema, sut, swebench, trace
  hostcheck/             # HTTP middleware: DNS-rebinding defense on Host headers
  internaltest/          # Test doubles and bridges: fakecontrol, fakerouter, synctestrapid, testcerts
  inventory/             # Fleet inventory snapshot: v1 JSON Schema, strict decoder, Substrate state vocabulary
  mcp/                   # Streamable HTTP MCP server: maps shell and file tools to guest gRPC
  portproxy/             # HTTP/CONNECT reverse proxy: tunnels ports inside actors via atenet
  routertls/             # TLS config: dynamic SPIFFE client certificate and CA reload
  sidecarenv/            # Sidecar-only env: ATE_SIDECAR_ENV_<NAME> on the container; Take strips it for the sidecars
  telemetry/             # OpenTelemetry tracing setup and Prometheus metric collectors
  tool/                  # Generic MCP tool registry
  unknownfields/         # gRPC interceptors rejecting requests with unknown protobuf fields
guest/
  server.go              # Guest gRPC server listener configuration
  filesystem/            # FileSystemService: workspace-confined file streaming (DefaultReadBufferSize = 64 KiB)
  process/               # ProcessService: process tracker, PTY allocator, signal delivery, log spools
proto/ateenv/v1alpha/
  env.proto, guest.proto # EnvironmentService, ProcessService, FileSystemService protobuf definitions
  *.pb.go, *_grpc.pb.go  # [generated] Go protobuf and gRPC stubs; never hand-edit
proto/testdata/client-parity/ # shared Go/Python client parity conformance JSON vectors
clients/
  go/                    # Go SDK client: package env (Client, Env, Process, StartError)
  python/                # Python SDK: distribution package ate-env-rl, import name ate_env_rl
    src/ate_env_rl/      # Core library: Client, Env, Process, SandboxFleet, AsyncSandboxFleet, sources.py
    src/ate_env_rl/_gen/ # [generated] Python protobuf and gRPC stubs; never hand-edit
    src/ate_env_rl/adapters/ # Benchmark task converters and reward evaluation adapters (SweBenchAdapter)
    src/ate_env_rl/backend/ # Fleet drivers: SubstrateBackendDriver, MockBackendDriver
    src/ate_env_rl/runtime/ # Guest hooks: SubstrateEnvClientRuntime, MockRuntimeHook
    src/ate_env_rl/telemetry/ # Telemetry: Prometheus metric collectors and OpenTelemetry tracer
integrations/            # Framework integrations: harbor (Harbor), nemo-gym (NeMo Gym), openhands, uni-agent (R2E-Gym)
benchmarking/            # Benchmark scenarios (scenarios/), SWE-bench (swebench/), telemetry report (telemetry_report/)
docs/                    # Architecture, glossary, release, rootfs images, security, dev guides
examples/                # Examples: grpo, harbor, harbor-tools-layer, openhands, verl_swebench, Qwen3-1.7B-verl, Qwen3.5-35B-A3B-verl, Qwen3.5-397B-A17B-verl, runtime-layers, guest-daemon, mcp
hack/                    # Repository scripts: CI (ci/), release (release/), verification (verify/, boilerplate/)
```

## Decision rules

### `internal/apiservice` vs. `internal/ate` vs. `guest/`

Put code in `internal/ate` for Substrate `ateapi` calls or `internal/mcp` connection pools, `guest/`
for in-actor execution, and `internal/apiservice` for API handlers. `apiservice` calls
`internal/ate` for lifecycle and dials `guest/` via `s.guestConn`; it must not run local commands or
import `guest/`.

### `clients/go` vs. `internal/ate`

Put public Go SDK code (`package env`) in `clients/go` and internal control-plane client code in
`internal/ate` (`cmd/ate-env` imports `clients/go`, `apiservice`, `hostcheck`, and `routertls`).
External clients talk to `ate-env-api`, never to Substrate directly. Outside tests, `clients/go`
imports only `proto/ateenv/v1alpha` from this repo (never `internal/`, `go.ate.dev/`, or `k8s.io/`);
shared values like `DefaultAtespace` are copied and tested equal, enforced by
`hack/verify/go-client-deps.sh` in the Go CI job. A feature that one Go consumer needs goes into
`clients/go` when a second consumer can use it.

### `internal/` vs. `guest/`

Put code in `internal/` if only this repository's binaries use it, and in-actor services in `guest/`
(`guest/` imports `internal/unknownfields`; `cmd/ate-env-guest` imports `internal/sidecarenv` and
`internal/telemetry`). Neither must import `internal/ate` or `go.ate.dev/substrate`.

### New binary vs. new package

Create a `cmd/<binary-name>` binary only for an independently deployed daemon or CLI; place library
code under `internal/<package-name>`.

### Where protos and generated stubs go

Define in-guest services (`ProcessService`, `FileSystemService`) in
`proto/ateenv/v1alpha/guest.proto` and lifecycle services (`EnvironmentService`) in
`proto/ateenv/v1alpha/env.proto`. Never hand-edit `*.pb.go`, `*_grpc.pb.go`, or Python `*_pb2*.py*`
files. Run `buf generate` for Go stubs and `make python-protos`
(`clients/python/scripts/gen-protos.sh`) for Python stubs in `clients/python/src/ate_env_rl/_gen/`.

### Python SDK: low-level vs. fleet

Put low-level gRPC wrappers in `clients/python/src/ate_env_rl/{client,env,handle}.py`. Put pool
scheduling algorithms, warm pool maintenance, and batch rollout drivers in
`clients/python/src/ate_env_rl/{fleet,async_fleet,strategies}.py`. Synchronous fleet methods must
run coroutines through `LoopRunner.run()` to preserve shared HTTP/2 channels.

### Benchmarking: `cmd/benchmarking` vs. `internal/benchmarking` vs. `benchmarking/`

Put CLI entrypoints in `cmd/benchmarking/bench` and `cmd/benchmarking/swebench-loader`. Put reusable
Go benchmark packages in `internal/benchmarking/` (`job`, `measure`, `overlay`, `schema`, `sut`,
`swebench`, `trace`). Put scenario manifests in `benchmarking/scenarios/`, report tools in
`benchmarking/telemetry_report/`, and shell scripts in `benchmarking/swebench/`.

### Test doubles and fixtures

Never write custom Substrate mocks. Reuse `internal/internaltest`: `fakecontrol` (in-memory `ateapi`
control plane), `fakerouter` (`atenet` router), `synctestrapid` (`rapid` in `testing/synctest`
bubbles), and `testcerts` (SPIFFE client certificates).

### Agent workflows in `.agents/skills/`

- Releases: [.agents/skills/create-release/SKILL.md](../../.agents/skills/create-release/SKILL.md)
  (release notes, tags, signed assets, PyPI, GHCR).
- Adding an RPC:
  [.agents/skills/extend-env-surface/SKILL.md](../../.agents/skills/extend-env-surface/SKILL.md)
  (proto, stubs, guest, `s.guestConn`/`ensureAwake`, SDKs).
- Docs: [.agents/skills/maintain-docs/SKILL.md](../../.agents/skills/maintain-docs/SKILL.md) (style
  and validation paths).

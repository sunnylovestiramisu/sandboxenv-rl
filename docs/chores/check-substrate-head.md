# Bumping the Substrate dependency

This repository builds on [Agent Substrate](https://github.com/agent-substrate/substrate) (`go.ate.dev/substrate`), using its control plane, actor lifecycle primitives, and container networking.

## Why Substrate bumps are manual

Automated tools like Dependabot are configured in [`.github/dependabot.yml`](../../.github/dependabot.yml) to ignore `go.ate.dev/substrate`. Substrate version bumps frequently update core protobuf definitions, alter CRD schemas, change actor limits or resource representations, and update networking security requirements (such as mTLS and SPIFFE ID policies). Consequently, a Substrate bump requires coordinated changes across manifest generation, CLI flags, unit tests, E2E fixtures, and documentation, followed by validation on a live cluster.

## Bump procedure

Follow these numbered steps to upgrade `go.ate.dev/substrate`:

1. Preview compatibility against Substrate HEAD or a specific ref:
   ```bash
   # Test against remote substrate@main
   make check-substrate-head ARGS=main

   # Or test against a local substrate repository checkout
   make check-substrate-head ARGS=../substrate
   ```
   [`hack/ci/check-substrate-head.sh`](../../hack/ci/check-substrate-head.sh) backs up [`go.mod`](../../go.mod), [`go.sum`](../../go.sum), and any active workspace files to a temporary directory, registers an `EXIT` trap guaranteeing they are restored even on failure or abort, initializes a temporary `go.work` for local checkout paths or updates `go.mod` via `go get` for remote refs, and runs `go build ./...`, `go vet ./...`, and `go test -race ./...`. For day-to-day multi-repo development against a local Substrate checkout, use `go work init . ../substrate` (see [Developing against a local Substrate checkout](../../README.md#developing-against-a-local-substrate-checkout); `go.work` and `go.work.sum` are gitignored).

2. Inspect Substrate release notes and git log for breaking changes:
   Review changes between the current pin and target version. Watch for:
   - Protobuf renumbering or removed wire fields (e.g. v0.6.0 renumbered fields across Actor, ActorStatus, ActorTemplate, SnapshotConfig, and Volume, breaking compatibility with older control planes).
   - CRD schema shifts (e.g. moving `sandboxClasses` solely to WorkerPool and dropping `sandboxConfig` from ActorTemplate).
   - Ingress and networking authentication (e.g. v0.6.0 dropped plaintext HTTP router listeners, requiring TLS and client certificates authorized via `ATE_INGRESS_ALLOWED_SPIFFE_IDS`).
   - Resource limit units (e.g. converting Kubernetes quantity strings to integer millicores and bytes in `ResourceRequirements`).

3. Bump the Go module dependency:
   ```bash
   go get go.ate.dev/substrate@vX.Y.Z
   go mod tidy
   ```

4. Update repository files encoding the Substrate version or API:
   As shown in upstream pull request `#110`, a Substrate upgrade typically requires updating:
   - Manifest generation: [`cmd/ate-env/manifest.go`](../../cmd/ate-env/manifest.go) and tests in [`cmd/ate-env/manifest_test.go`](../../cmd/ate-env/manifest_test.go) (CLI flags, resource limit bounds, ActorTemplate construction).
   - Unit tests: [`internal/apiservice/imagetemplate_test.go`](../../internal/apiservice/imagetemplate_test.go) and any tests constructing Substrate protobuf objects or structs.
   - Documentation: [`README.md`](../../README.md) Platform table row ("Built against Substrate vX.Y.Z"), control plane version requirements in "Get started" and "Prerequisites", and [`docs/release.md`](../release.md).

5. Regenerate stubs if protobuf files changed:
   ```bash
   make verify-protos
   ```
   If stubs drift or fail verification, regenerate them using `make protos` (or `make python-protos` and `go tool buf generate`).

6. Validate the upgrade locally and in E2E:
   ```bash
   # Run Go and Python test suites
   make ci

   # Run Kind-based end-to-end integration tests
   make ci-e2e
   ```
   [`hack/ci/ensure-substrate.sh`](../../hack/ci/ensure-substrate.sh) resolves Substrate via the Go module graph. When developing against a local checkout with `go.work` or a `replace` directive, the local tree is used directly without hash checks. Without an override, it clones or checks out the exact pin into `${XDG_CACHE_HOME}/sandboxenv-rl/substrate-<ref>` and strictly validates that HEAD matches the expected hash.

   If Substrate changes the flags of `ate-setup --kind deploy ate-system`, update [`hack/ci/targets.py`](../../hack/ci/targets.py) when bumping.

7. Optionally verify against a running cluster:
   ```bash
   make smoke
   ```

## Recommended regression checks

Verify that all automated checks in [`.github/workflows/ci.yaml`](../../.github/workflows/ci.yaml) pass:

| Job | What it runs | Key assertions |
| `go` (`ci-go`) | Build, vet, unit tests, gofmt, docs formatting, boilerplate, proto verification | Zero compile errors, `-race` passes, proto stubs clean |
| `python` (`ci-python`) | Ruff lint and pytest suites across Python packages, examples, integrations (NeMo Gym, and uni-agent on Python 3.12), and benchmarks | API client, mock runtime, telemetry, Qwen3 verl example, NeMo Gym and uni-agent integrations, and trajectories |
| `openhands` (`ci-openhands`) | Ruff lint and pytest suite for OpenHands integration via `.#openhands` devShell (Python 3.13) | OpenHands workspace adapters and agent-server tests |
| `e2e-kind` (`ci-e2e`) | Kind cluster bootstrap, Substrate deployment, smoke, full-stack SDK, warm pool, integrations, and loader | Real control plane, guest lifecycle, fleet warm pools, and integration providers |

Automated E2E coverage breakdown:
- [`hack/ci/test_smoke.py`](../../hack/ci/test_smoke.py): an environment-creation fixture plus 9 subtests covering shell execution with stdout and stdin stream piping, file write/read roundtrip, built-in MCP server (`tools/list` and `read_file`), suspend and on-demand resume, caller authentication and port-proxy atespace allowlist, Prometheus `/metrics` exposition, and deletion.
- [`clients/python/tests/e2e/test_full_stack.py`](../../clients/python/tests/e2e/test_full_stack.py): exercises async `Client` lifecycle, process streaming, signal delivery (`SIGTERM`), image validation error cases, template environment variable inheritance, port-proxy HTTP/WebSocket capability routing, transparent wake-on-request, and `SandboxFleet` with `LookaheadWarmer` warm-pool lifecycle.
- [`integrations/nemo-gym/tests/test_e2e.py`](../../integrations/nemo-gym/tests/test_e2e.py): exercises NeMo Gym provider live end-to-end lifecycle.
- [`cmd/benchmarking/swebench-loader`](../../cmd/benchmarking/swebench-loader): runs live smoke evaluation and report generation against an in-sandbox command task.
- [`integrations/harbor/tests/test_e2e.py`](../../integrations/harbor/tests/test_e2e.py): runs Harbor provider oracle trial live test against ate-env-api.

Areas not covered by standard CI that should be manually verified:
- Multi-node and `AsyncSandboxFleet` scale runs: `SandboxFleet` warm-pool lifecycle runs in `e2e-kind`, but high-concurrency multi-node stress tests and `AsyncSandboxFleet` require manual cluster verification.
- Rootfs image injection and loader smoke: `test_create_from_image` and `swebench-loader` are exercised by default on Kind using a pinned python rootfs image (`docker.io/library/python:3.13-slim@sha256:...`). Setting `ATE_ENV_ROOTFS_IMAGE=` skips both tests:
  ```bash
  ATE_ENV_ROOTFS_IMAGE="docker.io/library/python@sha256:..." make ci-e2e
  ATE_ENV_ROOTFS_IMAGE= make ci-e2e  # skips test_create_from_image and swebench-loader
  ```

## Troubleshooting

See the canonical [Troubleshooting guide](troubleshooting.md) for full remediation details:
- [Substrate module resolution and git fetches](troubleshooting.md#substrate-module-resolution-and-git-fetches) (handling module resolution, atomic caching, and strict hash assertions).
- [`GoldenActorCrashed` and WorkerPool rollout](troubleshooting.md#goldenactorcrashed-and-workerpool-rollout).
- [Linux inotify instance exhaustion](troubleshooting.md#linux-inotify-instance-exhaustion) (`sysctl fs.inotify.max_user_instances=8192`).
- [Router TLS and SPIFFE authorization](troubleshooting.md#router-tls-and-spiffe-authorization) (`ATE_INGRESS_ALLOWED_SPIFFE_IDS`).
- [Working tree restoration guarantees](troubleshooting.md#working-tree-restoration-guarantees-in-check-substrate-headsh).

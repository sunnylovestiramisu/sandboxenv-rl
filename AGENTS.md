# sandboxenv-rl

<!-- Last full audit: 2026-11-05 (commit b815e71) -->

Isolated execution environments for reinforcement learning and evaluation on Agent Substrate. Each
environment runs as a gVisor actor supporting on-demand rootfs derivation, PTYs, file streaming, MCP
tools, and Python fleet scheduling. See [README.md](./README.md) and
[docs/architecture.md](./docs/architecture.md).

## Rules for contributors

- Do not add AI co-author or assisted-by trailers to commit messages.
- Review your diff before requesting review. For substantial changes, have a fresh agent review the
  diff adversarially, then confirm and address what it identifies.
- One change per pull request. Submit refactors and follow-ups in separate pull requests. Search
  open pull requests and issues before starting.
- Be brief. Reviewer time and context windows are limited. Write short, direct sentences, subject
  first, active voice. Avoid em-dashes, chains of semicolons, and contrast pairs ("X, not Y").

## Code

- Search the repository and its dependencies before writing a helper, cache, client, flag set, or
  test fixture. Reuse or extend what exists, and handle cases the way nearby code already does.
- Build only what the change needs. Do not add options, interfaces, wrappers, flags, exports, or API
  fields for speculative future use. Prefer a sensible default to a new knob. Delete code your
  change makes unused.
- Pass dependencies through constructors. Do not add package globals or setters for testing.
- Fail loudly. Ignore only the specific errors you expect (such as `os.ErrNotExist`). Do not fall
  back silently to an insecure or no-op mode.
- Check every claim in comments, documentation, and pull request descriptions against the code. When
  you change behavior, update the documentation that describes it.
- Find the root cause before fixing a symptom. Back performance claims with a measurement.
- Planned: Do not assume kind or GKE, or a specific CPU architecture; dev scripts must support both
  clusters.
- Put shell logic in standalone `.sh` files under `hack/` or `benchmarking/`, not inline in workflow
  YAML.

## Architecture invariants

- `internal/apiservice` owns host adapter translation and proxy routing; `guest/process` and
  `guest/filesystem` own in-environment execution; callers forward through `s.guestConn` with
  `ensureAwake` and must not buffer full files or track process state in `ate-env-api`; because
  `ate-env-api` is an unbuffered horizontal adapter (not enforced).
- `apiservice.(*Server).guestConn` owns per-call guest gRPC dialing with wake verification; new
  proxy handlers call it and must not dial `atenet` directly; because `ensureAwake` handles actor
  restoration from storage (not enforced). `internal/ate.Client.DialGuest` dials `atenet` for
  `internal/mcp` without `ensureAwake` (known gap; not enforced). `internal/portproxy` owns raw
  `CONNECT` dialing and manages its own wake loop (not enforced).
- `proto/ateenv/v1alpha` owns the contract between `ate-env-api` and `ate-env-guest`; `internal/`
  and `clients/` call generated stubs and must not import `guest/` in production (`guest/` and
  `cmd/ate-env-guest` must not import `internal/ate` or `go.ate.dev/substrate`); because
  `ate-env-guest` runs inside task images without control-plane dependencies (not enforced).
- `clients/go` is the public Go SDK; outside tests it imports only `proto/ateenv/v1alpha` from this
  repository, and nothing under `internal/`, `go.ate.dev/` or `k8s.io/`; because an external module
  that imports the client must not pull the Substrate control-plane module (enforced by
  `hack/verify/go-client-deps.sh` in the Go CI job). Its tests keep the fixture's imports.
- `internal/apiservice` owns on-demand `ActorTemplate` derivation; `apiservice.Server` calls
  `DeriveImageTemplate` and `ImageTemplateName` and callers must not build derived templates outside
  them (base templates via `ate-env manifest template`, `buildActorTemplate` in
  `cmd/ate-env/manifest.go`); because derived names (`<base>-<digest:12>`) must be deterministic and
  idempotent across races (not enforced).
- `internal/mcp` and `internal/portproxy` own external protocol translation; `internal/mcp` dials
  `ate.Client.DialGuest` (bypassing `apiservice`), and `internal/portproxy` tunnels allowed
  atespaces; callers must not bypass them; because external clients need translation and origin
  filtering (not enforced).
- `internal/hostcheck` owns Host header validation, and `internal/apiservice.BearerAuth` owns token
  auth; HTTP handlers wrap with `hostcheck.(*Checker).Wrap` (and `BearerAuth` when
  `-auth-token-file` is set) and must not re-implement checks; because browsers can DNS-rebind local
  daemons (not enforced; `GET /healthz` is exempt from auth).
- `cmd/ate-env-guest` (`init_linux.go`, `sidecar.go`) owns PID 1 reaping and sidecars, and
  `guest/process.Tracker` owns subprocess lifecycle; callers invoke `Tracker` methods and must not
  manage children directly; because PID 1's `wait4(-1, WNOHANG)` loop reaps orphans, and non-PTY
  process groups (`Setpgid: true`, PTY `Setsid: true`) need `-pid` signals (enforced by `runIfPID1`,
  `supervise`, and `guest/process.Tracker`).
- `clients/python/src/ate_env_rl`'s `LoopRunner` owns sync-to-async bridging; synchronous callers
  (`SubstrateBackendDriver`, `SubstrateEnvClientRuntime`, `SandboxFleet`, `SandboxHandle`) dispatch
  through `LoopRunner.run()` and must not call `asyncio.run()`; because `asyncio.run()` destroys the
  loop and corrupts cached HTTP/2 gRPC channels (not enforced).

Docs: [docs/architecture.md](./docs/architecture.md), [docs/glossary.md](./docs/glossary.md),
[docs/dev/concurrency-and-pitfalls.md](./docs/dev/concurrency-and-pitfalls.md),
[docs/dev/code-layout.md](./docs/dev/code-layout.md).

## Repository layout

Skills in `.agents/skills/`: [create-release](./.agents/skills/create-release/SKILL.md) (cut
releases, PyPI/GHCR), [extend-env-surface](./.agents/skills/extend-env-surface/SKILL.md) (add
RPC/tool), [maintain-docs](./.agents/skills/maintain-docs/SKILL.md) (sync/validate docs).

Python SDK: dir `clients/python`, package `ate-env-rl`, module `ate_env_rl`, handles
`ate_env_rl.Env` and `ate_env_rl.SandboxHandle`.

## Build, test, verify

Run before opening a pull request:

```bash
make verify-boilerplate
make verify-gofmt
make verify-docs
make verify-protos
make vet
make test
make python-test
make nemo-gym-test
make harbor-test
make ci-dafny   # when touching docs/plans/telemetry/verify or modelled code
python -m pytest integrations/openhands/tests benchmarking/swebench benchmarking/telemetry_report internal/benchmarking/swebench/trajectories examples/Qwen3-1.7B-verl/tests hack/release/test_create_release.py
ruff check clients/python examples/verl_swebench benchmarking/telemetry_report hack/release
ruff check --config clients/python/pyproject.toml examples/Qwen3-1.7B-verl
```

Targets: `make build` (Go), `make build-cli` (`bin/ate-env`), `make images` (`ko`),
`make format-docs` (Markdown), `make clean` (`bin/`). Regenerate protos via `buf generate` (Go) and
`make python-protos` (Python); never hand-edit generated files. Use `internal/internaltest`
(`fakecontrol`, `fakerouter`, `synctestrapid`, `testcerts`) for test doubles and `rapid`/`synctest`
bridges.

## Pull requests and commits

- Fill out `.github/pull_request_template.md`, including the `Security impact` checklist. Update
  `docs/security/threat-model.md` if any security box is checked.
- Start the pull request description with why the change is needed. Write `Fixes #N` only if the
  pull request fully resolves the issue.
- Pull requests merge as a single squashed commit whose subject ends in `(#N)` and whose body lists
  the pull request's commits, with no AI co-author or attribution trailers.

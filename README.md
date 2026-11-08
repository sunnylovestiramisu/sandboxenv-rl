# sandboxenv-rl

> [!WARNING]
> This is an alpha API and is likely to change until v1.0 is released.

## Overview

`sandboxenv-rl` provides remote, stateful sandboxed execution environments for reinforcement
learning (RL) trainers, evaluation harnesses, and agent frameworks on
[Agent Substrate](https://github.com/agent-substrate/substrate). Callers create and drive fleets
of sandboxes over gRPC, HTTP/MCP, or the Python and Go SDKs with no direct Kubernetes management.

Each environment is an isolated gVisor actor running `ate-env-guest`, which executes commands,
streams output, and transfers files, and the port proxy on `ate-env-api` reaches any TCP port
inside the actor through the router. Between steps a caller suspends the environment
(`SuspendEnvironment`) to a checkpoint snapshot, which frees the worker. The next guest call
resumes it, and new environments on a derived template restore its golden snapshot from object
storage (`gs://`) to skip a cold boot.

## Feature tour

- **Fleet SDK** ([clients/python](./clients/python/README.md)): `SandboxFleet` and
  `AsyncSandboxFleet` orchestrate batches across warm pools using `LookaheadWarmer`, step
  strategies (`none`, `naive`, `sliding`, `pipelined`), and per-image worker scheduling via
  `FleetConfig.placement` and `node_selector`.
- **Any task image** ([docs/rootfs-images](./docs/rootfs-images/README.md)): Passing a
  digest-pinned `rootfs_image` causes `ate-env-api` to derive an `ActorTemplate` from the
  request's template (`default-template` by default), mount `ate-env-guest` as an image volume, and
  cache one golden snapshot per derived template while leaving the container image unmodified.
- **Suspend modes and parking** ([docs/architecture.md](./docs/architecture.md)):
  `SuspendEnvironment` supports `SUSPEND_MODE_DURABLE` (waiting for object storage via
  `DURABILITY_MODE_SYNCHRONOUS`) and `SUSPEND_MODE_LOCAL` (`SuspendLocal` in Go, returning once
  checkpointed on the node while uploading in the background) so jobs park sandboxes between
  steps.
- **Daemons inside an environment**
  ([port proxy and MCP](#reaching-a-service-inside-an-environment)): `ate-env-api` exposes an
  HTTP/WebSocket port proxy (by header or URL path) and a streamable HTTP MCP server for reaching
  services and tool servers running inside an actor.
- **Runtime layers and sidecars**
  ([docs/rootfs-images/RUNTIMES.md](./docs/rootfs-images/RUNTIMES.md)): Template authors inject
  secondary toolchains or language runtimes into any base image with `--layer`, or run companion
  daemons inside the same actor with `--sidecar` and `--sidecar-readyz`.
- **Framework integrations** ([integrations](./integrations/README.md)): Drop-in providers integrate
  with [NeMo Gym](./integrations/nemo-gym/README.md),
  [uni-agent](./integrations/uni-agent/README.md),
  [OpenHands](./integrations/openhands/README.md), and
  [Harbor](./integrations/harbor/README.md) with no changes to those frameworks.
- **Benchmarking and placement** ([benchmarking/swebench](./benchmarking/swebench/README.md),
  [bench harness](./cmd/benchmarking/bench/README.md)): `swebench-loader` runs SWE-bench Verified
  images in `eval` and `steps` modes, `plan-placement` maps task images to placement domains under
  node cache budgets, and the `bench` harness measures lifecycle phases.

## Architecture

See [docs/architecture.md](./docs/architecture.md) for the full system design and
[docs/glossary.md](./docs/glossary.md) for terminology.

System overview:

```
╭─────────────────────────╮
│ Trainer / Eval / Agent  │   ╭──────────────────────╮
│ · NeMo Gym · uni-agent  ├──▶│ Clients & Fleet SDK  │
│ · OpenHands · Harbor    │   │  SandboxFleet / Go   │
╰─────────────────────────╯   │  LookaheadWarmer     │   gRPC / MCP
                              │  ate-env CLI         ├────────┐
╭─────────────────────────╮   ╰──────────────────────╯        │
│ Benchmarks              │                                   ▼
│ · swebench-loader &     ├─────────────────────────▶ ╭─────────────────────────╮
│   plan-placement        │                           │ ate-env-api             │
│ · bench harness         │                           │ · lifecycle & guest RPC │
╰─────────────────────────╯                           │ · port proxy & MCP      │
                                                      │ · derived templates     │
╭─────────────────────────╮   ╭──────────────────╮    ╰───────┬─────────┬───────╯
│ Snapshot Store (gs://)  │◀──┤ ateapi           │◀───────────┘         │
│ · golden snapshots      │   │ control plane    │                      │ mTLS
│ · actor checkpoints     │   ╰────────┬─────────╯                      ▼
╰────────────▲────────────╯            │                      ╭──────────────────╮
             │                         ▼                      │ atenet router    │
             │                ╭──────────────────╮            ╰─────────┬────────╯
             └────────────────┤ WorkerPools      │◀─────────────────────┘
                              │  gVisor actors   │
                              │  └ ate-env-guest │
                              ╰──────────────────╯
```

Request-path zoom:

```
 ╭──────────────╮    ╭──────────────╮ lifecycle  ╭────────────╮
 │    Clients   │    │              ├───────────▶│   ateapi   │ Substrate control plane
 │  ate-env CLI ├───▶│ ate-env-api  │            ╰────────────╯
 │  Python SDK  │    │ (API server) │ guest ops  ╭────────────╮     ╭──────────────────────╮
 ╰──────────────╯    │              ├───────────▶│   atenet   ├────▶│ actor                │
                     ╰──────────────╯ (shell/mcp)│   router   │     │  └ ate-env-guest     │
                                      port proxy ╰────────────╯     │  /readyz, /v1alpha/* │
                                                                    ╰──────────────────────╯
```

| Path | What it is |
|---|---|
| [`cmd/ate-env`](./cmd/ate-env) | CLI: create, get, suspend and delete environments, run commands, move files, and print the Kubernetes and Substrate manifests. |
| [`cmd/ate-env-api`](./cmd/ate-env-api) | The API service: environment lifecycle, guest calls, MCP and the port proxy. |
| [`cmd/ate-env-guest`](./cmd/ate-env-guest), [`guest/`](./guest) | The daemon inside each environment (a PID 1 init with sidecar supervision) and its services: `guest/process` and `guest/filesystem`. |
| [`cmd/benchmarking`](./cmd/benchmarking) | The benchmark binaries: `swebench-loader` and the `bench` harness. |
| [`clients/`](./clients/README.md) | The Go client (package `env`) and the Python client `ate_env_rl` with the fleet SDK. |
| [`integrations/`](./integrations/README.md) | Framework integrations: Harbor, NeMo Gym, OpenHands, uni-agent. |
| [`examples/`](./examples/README.md) | Runnable examples, from a Go MCP client to multi-node agentic GRPO recipes. Not supported. |
| [`benchmarking/`](./benchmarking/README.md) | The SWE-bench loader scripts, the `bench` scenarios, the telemetry report tool and the benchmarking skills. |
| [`internal/`](./internal) | The packages the binaries are built from: `apiservice` (the gRPC server), `ate` (the Substrate client), `mcp`, `portproxy`, `routertls`, `hostcheck`, `telemetry`, `tool`, `unknownfields`, `benchmarking/*`, the test doubles in `internaltest` (`fakecontrol`, `fakerouter`, `testcerts`) and a lifecycle walkthrough in `examples/env`. |
| [`proto/ateenv/v1alpha`](./proto/ateenv/v1alpha) | The `ateenv.v1alpha` API: `env.proto` (`EnvironmentService`) and `guest.proto` (`ProcessService`, `FileSystemService`), with the generated Go stubs. |
| [`docs/`](./docs) | [Architecture](./docs/architecture.md), [glossary](./docs/glossary.md), [observability](./docs/observability.md), the [metrics reference](./docs/metrics.md), [rootfs images and runtime layers](./docs/rootfs-images/README.md), the [threat model](./docs/security/threat-model.md), [support](./docs/support.md), [releases](./docs/release.md), the developer guides in [`docs/dev`](./docs/dev), the maintenance chores in [`docs/chores`](./docs/chores), and the plans in [`docs/plans`](./docs/plans). |
| [`hack/`](./hack) | Repository scripts, checks and generators: the CI runners in [`hack/ci`](./hack/ci) (the Kind e2e run, the smoke test, pin and Substrate head checks), release automation in [`hack/release`](./hack/release), license boilerplate, [`hack/verify`](./hack/verify) and `metrics-docs` (renders `docs/metrics.md`). |
| [`nix/`](./nix), [`flake.nix`](./flake.nix), [`uv.lock`](./uv.lock) | The Nix flake: the `devShell`s, the Python environments built from `uv.lock` (`python.nix`), and the CI scripts `ci-go`, `ci-python`, `ci-e2e` and `ci-release` (`ci.nix`). |
| [`tests/telemetry`](./tests/telemetry) | The OpenTelemetry collector configuration of the telemetry acceptance run. |
| [`.github/`](./.github) | CI and release workflows, Dependabot, code owners, the pull request template. |
| [`AGENTS.md`](./AGENTS.md), [`.agents/skills`](./.agents/skills) | Instructions for coding agents and the repository skills. [`CHANGELOG.md`](./CHANGELOG.md) collects user-visible changes. |

Each of `clients/`, `integrations/`, `examples/` and `benchmarking/` has a README that
lists what is inside.

## Examples

[`examples/README.md`](./examples/README.md) lists each example with what it needs and how to run
it.

| Example | What it shows |
|---|---|
| [`examples/grpo`](./examples/grpo/README.md) | A real RL loop: TRL's GRPOTrainer trains Qwen2.5-0.5B on MBPP, and each completion runs in an environment that computes the reward. Covers the park/wake pattern, with the fleet suspended after each step. CPU only. |
| [`examples/runtime-layers`](./examples/runtime-layers/README.md) | An unmodified rootfs image with a second runtime injected as a layer and supervised as a sidecar. |
| [`examples/harbor`](./examples/harbor/README.md) | Terminal-Bench 2.0 on ate-env through the Harbor provider: prebuilt images, resource class templates, `run-tb2.sh` around `harbor run`, and the comparison with Docker. |
| [`examples/harbor-tools-layer`](./examples/harbor-tools-layer/README.md) | Harbor with tools as a second layer: a static tmux and asciinema layer mounted into every task image, the class templates registered with `--layer`, and `harbor run` with the layer first on `PATH`, thus terminus-2 installs nothing. |
| [`examples/mcp`](./examples/mcp/main.go) | Go: connect to an environment's MCP endpoint, list the tools and call them. |
| [`examples/guest-daemon`](./examples/guest-daemon/README.md) | Go: run the guest gRPC services standalone, without a cluster. |
| [`examples/Qwen3.5-397B-A17B-verl`](./examples/Qwen3.5-397B-A17B-verl/README.md) | The same agentic GRPO loop from 2 to 16 GPU nodes: profiles for Qwen3.5-35B-A3B and Qwen3.5-397B-A17B (Megatron trainer with CPU-offloaded params and optimizer, FP8 vLLM replicas), steps of thousands of trajectories, sandbox sizing and guarded verl patches for 64K responses; the 35B profiles run eight at a time to put one 397B run's load on the same hardware. |
| [`examples/Qwen3.5-35B-A3B-verl`](./examples/Qwen3.5-35B-A3B-verl/README.md) | Agentic GRPO on real SWE tasks: verl `separate_async` with a Megatron trainer and vLLM rollouts on separate nodes, a uni-agent ReAct agent whose tools and hidden-test grading run in environments. Two 8-GPU nodes, one loop on its own. |
| [`examples/openhands`](./examples/openhands/README.md) | OpenHands agents and tools on the agent-server that a runtime layer mounts into any task image, through `integrations/openhands`. |
| [`examples/Qwen3-1.7B-verl`](./examples/Qwen3-1.7B-verl/README.md) | Agentic GRPO on MATH with verl `fully_async_policy`: the model writes Python that runs in an environment through a verl tool. Trainer and rollouts on separate GPU nodes. |
| [`examples/verl_swebench`](./examples/verl_swebench/README.md) | A GRPO-style asynchronous rollout loop on Ray that grades SWE-bench candidate patches in environments with the `ate_env_rl` client and its SWE-bench adapter. |

## Integrations

[`integrations/README.md`](./integrations/README.md) lists what each package contains and how it is
installed and tested.

| Integration | Framework | What it does |
|---|---|---|
| [`integrations/nemo-gym`](./integrations/nemo-gym/README.md) | [NeMo Gym](https://github.com/NVIDIA-NeMo/Gym) | Sandbox provider `substrate`, registered through the `nemo_gym.sandbox_providers` entry point, with no change to NeMo Gym. `endpoint()` reaches a daemon inside the sandbox through the port proxy, and [`layer/`](./integrations/nemo-gym/layer) is that daemon for NeMo: NeMo Skills' sandbox server and NeMo Gym's exec server injected into any task image. |
| [`integrations/uni-agent`](./integrations/uni-agent/README.md) | [uni-agent](https://github.com/verl-project/uni-agent) | Sandbox provider `ate_env` plus an `r2e_gym` task that hides the tests, lets the agent work, then grades in the same environment; dataset and image tooling for R2E-Gym, SWE-reBench and SWE-bench rows. Used by `examples/Qwen3.5-397B-A17B-verl` and `examples/Qwen3.5-35B-A3B-verl`. |
| [`integrations/openhands`](./integrations/openhands/README.md) | [OpenHands](https://github.com/OpenHands/software-agent-sdk) | `AteEnvWorkspace`, an OpenHands `RemoteWorkspace` that mirrors OpenHands' `AgentSandboxWorkspace` and runs the agent-server in any unmodified task image (glibc 2.31 or newer). |
| [`integrations/harbor`](./integrations/harbor/README.md) | [Harbor](https://github.com/harbor-framework/harbor) | `AteEnvEnvironment`, a Harbor environment provider: `harbor run ... -e ate_env_harbor:AteEnvEnvironment` runs each trial in an ate-env environment from a digest-pinned task image, with no change to Harbor. |

## Benchmarks

[`swebench-loader`](./benchmarking/swebench/README.md) drives SWE-bench Verified task images through
the API the way a simulated RL job does: `eval` mode creates, replays, grades and deletes each task,
and `steps` mode creates a fleet once and parks it between training steps. Image-delivery workloads
compare image pull, image streaming and other loaders on the same images. The report breaks down
what each phase cost.
The [`bench`](./cmd/benchmarking/bench) harness runs the scenarios in
[`benchmarking/scenarios`](./benchmarking/scenarios) (`resolve`, `render`, `prewarm`, `spinup`,
`spindown`, `measure`, `reap`, `report`, `validate`, `upload`, `compare` and `run`), and
[`benchmarking/telemetry_report`](./benchmarking/telemetry_report) renders one run's
report, spans and scrapes as one HTML page.
[`internal/benchmarking/DESIGN.md`](./internal/benchmarking/DESIGN.md) specifies the benchmark
system; [`benchmarking/README.md`](./benchmarking/README.md) lists the scripts and tools.

## Support

The components, the clients, the integrations and the `ateenv.v1alpha` API are supported;
`examples/` and `internal/examples/` are not. [docs/support.md](./docs/support.md) says what
supported means for each path.

## Get started

You need a cluster with Agent Substrate at or after
[`48f5744c`](https://github.com/agent-substrate/substrate/commit/48f5744c), a snapshots bucket,
`kubectl` with `kubectl-ate`, Go, [ko](https://ko.build), and Python 3.10 or later for the SDK. No
release has been published yet; see [docs/release.md](./docs/release.md) for the release process.
Build the CLI and images from source:

```bash
git clone https://github.com/ai-on-gke/sandboxenv-rl && cd sandboxenv-rl
go install ./cmd/ate-env
make images ATE_ENV_IMAGE_REPO=<registry>   # prints --guest-image and --api-image
```

Deploy, and register a template ([deployment guide](#deployment-guide) explains each step):

```bash
kubectl ate create atespace ate-env
ate-env manifest access-policy | kubectl ate create access-policy -a ate-env -f -
ate-env manifest --api-image <api-image> --worker-image <ateworker-gvisor-image> | kubectl apply -f -
ate-env manifest auth-secret | kubectl create -f -   # the caller token; create it once
ate-env manifest template --guest-image <guest-image> | kubectl-ate create actor-template -f -
```

Try an environment. `ate-env-api` requires the token from the Secret:

```bash
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777 &
export SUBSTRATE_ENV_AUTH_TOKEN="$(kubectl -n ate-env get secret ate-env-api-token -o jsonpath='{.data.token}' | base64 -d)"
ate-env create dev1
ate-env dev1 shell 'echo hello from $(hostname)'
ate-env delete dev1
```

Drive environments from Python with the fleet SDK (`pip install ./clients/python`). Leaving the
`with` block deletes everything the fleet created:

```python
import os

from ate_env_rl import FleetConfig, SandboxFleet, Task

config = FleetConfig(endpoint="127.0.0.1:7777", auth_token=os.environ["SUBSTRATE_ENV_AUTH_TOKEN"])
with SandboxFleet(config) as fleet:
    handle = fleet.acquire(Task(id="t1", image="docker.io/library/python@sha256:<digest>"))
    print(handle.exec(["python3", "-c", "print(6 * 7)"]).stdout)
    handle.write_file("/tmp/answer.txt", "42\n")
    print(handle.read_file_bytes("/tmp/answer.txt"))
    handle.release()
```

Then pick an [example](#examples) or an [integration](#integrations).

## Deployment guide

Prerequisites: a cluster with [Agent Substrate](https://github.com/agent-substrate/substrate)
installed, with its cluster snapshot location (`ate-setup --snapshot-location`). ate-env is built
against Substrate [`274605f1`](https://github.com/agent-substrate/substrate/commit/274605f1)
(2026-11-05, Go module `go.ate.dev/substrate`), and the cluster must run Substrate at or after
[`48f5744c`](https://github.com/agent-substrate/substrate/commit/48f5744c) (2026-10-22): that commit
moved the Kubernetes CRDs (`WorkerPool`, `SandboxConfig`) from `ate.dev/v1alpha1` to `ate.dev/v1`
and the control-plane gRPC service from `/ateapi.Control` to `/substrate.ateapi.v1.Control`.
Upgrade the control plane, CRDs, and the ate-env images together; with an older
Substrate (`v0.6.0`), use ate-env from before this bump. The WorkerPool alone chooses the sandbox
(`spec.sandboxClasses`) and the ActorTemplate names none. Because the template no
longer names a class, an actor booting without a snapshot can land on any pool whose labels its
template's `workerSelector` matches, and on any pool at all without a selector. `ate-env` labels its
WorkerPool `ate-env/workerpool: <pool>` and each template selects that label, thus actors stay on
their pool and its sandbox class; `--workerpool` on `ate-env manifest template` names the pool to select:
it defaults to `default-template-workerpool`, the pool `ate-env manifest` creates by default, whatever
the template is named. Pass it when you deploy with another `--template` or `--workerpool`. An
environment can narrow its template's pools further: `worker_selector` on `CreateEnvironment`
(`ate-env create --worker-selector key=value`) is ANDed with the template's selector and applies to the
environment's wakes too, thus a job can keep an image's environments on the pools that already hold
the image; `ate-env get <id>` shows the node an environment is on. The templates also use
`snapshotConfig` and `wakeupProbe` and declare CPU and memory limits, and `--worker-image` must be the
`ateworker-gvisor` image built from the Substrate version on the cluster.

`ate-env-api` suspends durably by default: a suspend returns once the snapshot is in object storage, thus a
suspended environment survives the loss of its node. Substrate's own default became asynchronous in
[`b40d34f1`](https://github.com/agent-substrate/substrate/commit/b40d34f1). `SuspendEnvironment` takes a
`mode`: `SUSPEND_MODE_LOCAL` (`ate-env suspend --mode local`, `Client.suspend(mode=SuspendMode.LOCAL)`,
`Client.SuspendLocal` in Go) returns once the snapshot is on the node and leaves the upload in the
background; the environment resumes on that node until the upload lands and reports `PAUSED` meanwhile.
One park-and-resume cycle is cheaper this way, which is what a loop that parks a sandbox between every
command wants; a node lost before the upload lands takes the snapshot with it.

Substrate enforces authorization by default. `ate-env-api` runs as its own ServiceAccount
(`ate-env/ate-env-api`) and needs the editor role on each atespace it creates environments in; the
atespace must exist first, because creating one takes a global owner. Step 1 sets both up.

When you upgrade an existing cluster across these changes:

- Create the atespace and the access policy (step 1) before you apply the new manifest. Without them
  `ate-env-api` fails at startup with `PermissionDenied` while it checks the default atespace.
- Create the caller token Secret (`ate-env manifest auth-secret | kubectl create -f -`) if it does
  not exist yet: `ate-env manifest` mounts `ate-env-api-token` by default unless `--insecure-no-auth`
  is passed.
- Base templates registered before keep working, because Substrate keeps their old `sandboxConfig`
  as an unknown field and drops it when it reads them, but they have no `workerSelector`. On a
  cluster with more than one WorkerPool, register them again (delete the base and its
  `<base>-<12 hex digits>` derived templates, then `ate-env manifest template`), or their actors
  can boot on another pool. Apply the new manifest first, so the pool carries the label the
  templates select.
- A base registered before the CPU and memory limits (Substrate before
  [`c58f374a`](https://github.com/agent-substrate/substrate/commit/c58f374a)) still needs registering
  again: Substrate cannot update an ActorTemplate in place, thus each template `ate-env-api` derives
  from it fails with `resources: Required`. Delete the base and its derived templates (named
  `<base>-<12 hex digits>`) with `kubectl-ate delete actor-template`, then register the base again.
- A base registered before `--wakeup-timeout` keeps Substrate's 30 s wakeup-probe timeout, and so
  does each template `ate-env-api` derived from it: `ate-env-api` reuses a derived template as it
  is, thus registering the base again does not change it. To give a `--sidecar-readyz` template
  the 120 s default, delete the base and its derived templates (`<base>-<12 hex digits>`), then
  register the base again; `ate-env-api` derives each image template again from the new base.

### 1. Deploy the Kubernetes Resources

Give `ate-env-api` access to its atespace, once, as a global owner (on a stock install the default
`kubectl-ate` identity is a bootstrap owner, which is a global owner):

```bash
kubectl ate create atespace ate-env
ate-env manifest access-policy | kubectl ate create access-policy -a ate-env -f -
```

An atespace has one access policy. If `ate-env` already has one, add the binding that
`ate-env manifest access-policy` prints to it with `kubectl ate get access-policy` and
`kubectl ate update access-policy`. Repeat both commands for each other atespace you create
environments in.

`ListPlacementDomains` reads Substrate's workers, which takes the global viewer role, and so does
`WarmImage` when a request leaves `nodes` 0. `ate-env manifest access-policy --global` prints that
binding. The global policy is one object for the whole control plane and holds the owners: merge
the binding into it with `kubectl ate get access-policy` and `kubectl ate update access-policy`,
do not create it over the existing one. Without it `WarmImage` still works when the caller passes
the node count; `ListPlacementDomains` has no such field and needs the role, whether it lists every
pool or, given a template, only the pools that template's own worker selector matches.

`WarmImage` retries a probe's create and resume within the probe's timeout on the failures a
control plane under load hands out for a while (`Unavailable` from an api server restarting,
`DeadlineExceeded` on the call, `FailedPrecondition` for a derived template not visible yet), and a
probe's delete on `Aborted`, `Unavailable` and `ResourceExhausted` within a minute. A probe still
there after that is listed in the coverage's `undeleted_probes` (`ate-env warm-image` prints them
and exits non-zero): sweep them with `ate-env delete`. A probe whose resume failed ends `CRASHED`,
which Substrate cannot delete today, so it is listed too, until the control plane takes the delete.
Across calls, `ate-env-api` caps the probes in flight per node of one worker selector with
`-warm-max-probes-per-node` (default 2, the default `probes_per_node`): a round takes `nodes x
probes_per_node` slots on its selector before it creates its probes and gives them back once they
are deleted, so a pre-flight that fans out one `warm-image` per image puts at most that many first
boots on a node at once; a request's `probes_per_node` above the cap is refused, a round that waits
for slots is logged, and the coverage's `waited` sums the wait.

Since Substrate v0.6.0 the atenet router serves only TLS listeners and admits
only clients whose SPIFFE ID is on its allow-list. The manifest mounts the pod
identity certificate and the servicedns trust bundle into `ate-env-api` for
that (`--api-router-tls`, on by default), as the `podCertificate` and
`clusterTrustBundle` projected volumes. GKE 1.37 and later serve both by
default. A 1.36 cluster must be created with
`--enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles`
and its node pools created after that, or the mount fails with
`MountVolume.SetUp failed: unimplemented`; Substrate's `setup-gcp create
cluster` does both, and its
[README](https://github.com/agent-substrate/substrate/blob/v0.6.0/tools/setup-gcp/README.md)
has the details. Versions below 1.36 are not supported. The API's identity
is `spiffe://cluster.local/ns/<namespace>/sa/ate-env-api`, and `ate-env
manifest` prints the exact ID. Add it to the Substrate install's allow-list,
one comma-separated value that becomes the router's whole
`--ingress-allowed-spiffe-ids`, so keep the IDs already on it, and rerun the
deploy for a running install:

```bash
# In the Substrate checkout.
ATE_INGRESS_ALLOWED_SPIFFE_IDS=spiffe://cluster.local/ns/ate-env/sa/ate-env-api,<existing IDs> \
  go run ./cmd/ate-setup deploy ate-system
```

Editing the `atenet-router` Deployment's `--ingress-allowed-spiffe-ids`
argument does the same by hand. ate-setup requires the list for its default
Envoy ingress dataplane, which enforces it. Without the ID, or when the
router does not trust the podidentity signer, the router rejects the API's
certificate: guest calls fail at once with `FailedPrecondition` naming the ID
and what to check, the port proxy answers 502 with the same cause, and the
API logs the ID it presents at startup. `--api-router-tls=false` leaves the
API on the router's plaintext addresses, which no Substrate release serves
since v0.6.0, the first this build pairs with; it exists for the test fakes,
and the API warns at startup when it runs that way.

Deploy the namespace, worker pool, ServiceAccount and API service:

```bash
# --api-image is the ate-env-api image that `make images` printed. The worker
# image is ateworker-gvisor from the Substrate repo at your cluster's version; in
# that checkout, `go run ./cmd/ate-setup publish worker-images` pushes the worker
# images and prints their digest-pinned refs. Use the digest: a :latest tag is
# whatever was built last, and a stale worker image registers workers with no
# capacity.
ate-env manifest \
  --api-image      <registry>/ate-env-api@sha256:<digest> \
  --worker-image   <registry>/ateworker-gvisor@sha256:<digest> | kubectl apply -f -

# The bearer token ate-env-api requires from callers. Each run prints a new
# token, thus create the Secret once; ate-env-api starts when it exists.
ate-env manifest auth-secret | kubectl create -f -

# Ensure that the pods are running:
kubectl get pods -n ate-env
```

`ate-env-api` waits for a suspended environment to wake before forwarding a guest call, up to
`--wake-budget` (default 5m; cold image pulls and cross-region snapshot reads exceed the router's own
parking budget) and, once the control plane reports the environment RUNNING, up to
`--wake-running-budget` (default 20s). Both are flags of the `ate-env-api` binary; `ate-env manifest`
does not set them, thus add them to the Deployment's arguments to change them. `--wake-budget` must
be longer than the image pull plus the template's wakeup-probe timeout (`ate-env manifest template
--wakeup-timeout`), because the control plane reports RUNNING only after the probe passes. The 5m
default covers the 120 s timeout of a `--sidecar-readyz` template plus three minutes for the pull;
raise it if you set a longer `--wakeup-timeout`. Otherwise the first guest call after a slow wake
fails with `Unavailable`, and a retry succeeds once the environment is up. A deleted environment
fails a guest call at once with `NotFound`.

Two start failures also end the wait at once, because waiting cannot fix them, and mark the call's
trailing metadata with `ate-env-start-failure`, so a client can retry the work on a fresh
environment instead of scoring it:

| Failure | Status | `ate-env-start-failure` | When |
|---|---|---|---|
| The control plane moved the environment to CRASHED while it was waking | `FailedPrecondition`, with the control plane's crash reason | `crashed` | always |
| No worker has room to run the environment | `ResourceExhausted` | `no-capacity` | only with `--wake-no-capacity-grace`, after the router has said so for that long |

`--wake-no-capacity-grace` defaults to 0: a full fleet keeps waiting for the whole `--wake-budget`,
since capacity can come back as nodes scale up or other environments stop. The Python SDK turns
both into a retryable `SandboxStartError` whose `reason` is the trailer's value. Port-proxy requests
wait on the router instead (see
[Reaching a service inside an environment](#reaching-a-service-inside-an-environment)).

`ate-env-api` can export traces and serve Prometheus metrics; both are off by default:

- `--enable-tracing` exports OpenTelemetry traces over OTLP/gRPC, configured by
  `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_INSECURE`. The server continues the W3C
  `traceparent` / `tracestate` that a caller sends and passes it on to ateapi and the guests as is.
  `ate-env manifest --enable-tracing --otlp-endpoint <url> [--otlp-insecure]` sets these.
- `--metrics-listen-addr` (for example `:9090`) serves the families in
  [docs/metrics.md](docs/metrics.md) at `/metrics` on a listener of its own.
  `--metrics-environments-ttl` (default 1m) bounds how often a scrape lists the environments.
  `ate-env manifest --metrics-port <port>` sets the flag and exposes the port on the container and
  the Service, with `prometheus.io` scrape annotations; 0 (the default) leaves metrics off. The
  metrics listener has no authentication and no `Host` check: anyone who reaches it reads request
  counts, atespace names with environment counts, and the build version.

`ate-env-api` requires a bearer token from each caller: `ate-env manifest` mounts the Secret that
`--auth-token-secret` names (default `ate-env-api-token`, key `token`) and sets `-auth-token-file`.
The pod does not start until the Secret exists. Read the token back for clients with
`kubectl -n ate-env get secret ate-env-api-token -o jsonpath='{.data.token}' | base64 -d`; callers
send it with `--auth-token-file`, `SUBSTRATE_ENV_AUTH_TOKEN` or `--auth-token` on the CLI
(`--auth-token` shows the token in `ps` output and the shell history), `ClientOptions.AuthToken` in
the Go SDK, `Client(..., auth_token=...)` or `FleetConfig(auth_token=...)` in the Python SDK, or
`Authorization: Bearer <token>` on MCP HTTP requests and port-proxy requests
([Reaching a service inside an environment](#reaching-a-service-inside-an-environment)). To
rotate the token, replace the Secret's value; `ate-env-api` reads it again once the kubelet updates
the mounted file, within about a minute. One token is valid at a time: from that moment, callers
that still send the old one get 401, thus update the clients together with the Secret.
`--insecure-no-auth` deploys `ate-env-api` without the token, in which case anyone who can reach
its Service can create environments and run commands in them.
The clients connect without TLS, thus the token travels in clear text. Past the cluster boundary,
for example through an internal LoadBalancer, put TLS termination or a mesh with mTLS in front of
`ate-env-api`.

### 2. Register the ActorTemplate in Substrate

Substrate manages ActorTemplates directly in its control plane rather than Kubernetes CRDs, and the
templates live in the `ate-env` atespace that step 1 creates. Use `ate-env manifest template` to
generate the Substrate ActorTemplate manifest:

```bash
# --guest-image is the ate-env-guest image that `make images` printed.
ate-env manifest template \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> | kubectl-ate create actor-template -f -
```

The template's snapshots go to the cluster's snapshot location. `--snapshots-bucket <url>` gives
the template a location of its own instead; that is Substrate's `PerTemplateSnapshotLocation`
preview feature, so ate-api-server needs the gate on and the template must be created with
`kubectl-ate --preview` (ate-env-api opts in the same way for the templates it derives from such a
base). A Substrate since
[`d86746d8`](https://github.com/agent-substrate/substrate/commit/d86746d8) rejects the field
otherwise.

Each actor gets 2 CPUs and 4 GiB unless you pass `--cpu` and `--memory` (Kubernetes quantities such as
`500m` or `8Gi`; CPU at most `999.999`, as Substrate requires). Substrate requires both limits and books them against worker capacity; templates
derived per rootfs image inherit them.

That template's container is the guest image itself. To run another image unmodified, name
it with `--rootfs-image`: the guest is then mounted into it as a read-only OCI image volume at
`/ate` and started from there, so the rootfs image is never rebuilt.

```bash
ate-env manifest template --template py312 \
  --rootfs-image   docker.io/library/python@sha256:<digest> \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> | kubectl-ate create actor-template -f -
```

A second runtime rides along the same way: `--layer` mounts its image read-only next to the
guest, `--sidecar` has the guest start and supervise a process from it, and `--sidecar-readyz`
holds the actor's readiness until that process answers. With `--sidecar-readyz`, Substrate's
wakeup probe waits up to 120 s instead of its default 30 s, because a runtime that boots in 20 s
alone takes longer when many environments start on one node; `--wakeup-timeout` sets another
value. Keep `ate-env-api --wake-budget` longer than the image pull plus this timeout (see step 1).
Templates registered before this flag keep 30 s (see the upgrade notes above). Here a static
busybox stands in for a runtime daemon and serves HTTP on port 44772 inside the environment:

```bash
ate-env manifest template --template layered \
  --rootfs-image   docker.io/library/python@sha256:<digest> \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> \
  --layer          web=docker.io/library/busybox@sha256:<digest of the 1.37-musl tag>=/opt/web \
  --sidecar        '/opt/web/bin/busybox httpd -f -p 44772 -h /' \
  --sidecar-readyz http://127.0.0.1:44772/etc/hostname | kubectl-ate create actor-template -f -
```

Layers must be self-contained: the `1.37-musl` busybox is static, while the glibc build fails
on images with an older libc. The runnable version is
[`examples/runtime-layers/demo.sh`](examples/runtime-layers/demo.sh).

See [docs/rootfs-images/README.md](docs/rootfs-images/README.md) for the full guide, including
creating environments from an image on demand, [docs/rootfs-images/RUNTIMES.md](docs/rootfs-images/RUNTIMES.md)
for injecting a second runtime as a layer, and [docs/rootfs-images/DESIGN.md](docs/rootfs-images/DESIGN.md)
for the design.

Then create and use an environment:

```bash
# Port-forward the ate-env-api service and load the caller token.
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777 &
export SUBSTRATE_ENV_AUTH_TOKEN="$(kubectl -n ate-env get secret ate-env-api-token -o jsonpath='{.data.token}' | base64 -d)"

# Create an environment.
ate-env create dev1

# Or run any digest-pinned image unmodified. ate-env-api derives a template
# from default-template on first use (guest mounted in as an image volume)
# and reuses it for later environments on an image with the same digest.
ate-env create py1 --rootfs-image docker.io/library/python@sha256:<digest>

# Or from the layered template above: the second runtime answers inside the
# environment, and any other image on that base inherits it.
ate-env create web1 --template layered
ate-env web1 shell '/opt/web/bin/busybox wget -qO- http://127.0.0.1:44772/etc/os-release'
ate-env create node1 --template layered --rootfs-image docker.io/library/node@sha256:<digest>

# Or keep the environment on the worker pools that carry a label (for example
# the pools that already hold the image); ANDed with the template's selector.
ate-env create t1 --rootfs-image docker.io/library/python@sha256:<digest> --worker-selector ate-env/domain=d0
ate-env get t1    # template, status, the node it is on, the selector

# Execute a shell command inside the environment.
ate-env dev1 shell 'echo hello > /note.txt'

# Feed stdin to a command and bound its run time.
echo 'shout' | ate-env dev1 shell --stdin --timeout 30s 'tr a-z A-Z'

# Read and write files.
ate-env dev1 read /note.txt
echo "world" | ate-env dev1 write /note.txt

# Suspend (snapshot) and delete.
ate-env suspend dev1
ate-env delete dev1
```

Alternatively, interact with the environment over MCP:

```bash
curl -X POST localhost:7777/v1alpha/envs/dev1/mcp \
     -H "Authorization: Bearer $SUBSTRATE_ENV_AUTH_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"shell","arguments":{"command":"echo hi"}}}'
```

## CLI

File operations and remote command execution take the environment ID first, followed by the operation:

```bash
$ ate-env create dev1
$ ate-env dev1 shell 'uname -a'
$ echo "hello world" | ate-env dev1 write /app/msg.txt
$ ate-env dev1 read /app/msg.txt
$ ate-env suspend dev1
$ ate-env delete dev1
```

Subcommand names (`create`, `get`, `image-status`, `placement-domains`, `warm-image`, `delete`,
`suspend`, `manifest`, `server-info`, `help`, `completion`) are reserved: `ate-env create` rejects them as environment IDs, because `ate-env <id> ...` cannot reach
such an environment.

`ate-env` provides help text:

```bash
$ ate-env --help
Manage environments on Agent Substrate.

Common environment commands:
  ate-env <id> read <path>       Print an environment file to stdout
  ate-env <id> write <path>      Write stdin to an environment file
  ate-env <id> shell <cmdline>   Run a shell command line in the environment

Usage:
  ate-env [command]

Available Commands:
  completion        Generate the autocompletion script for the specified shell
  create            Create and start an environment
  delete            Delete an environment
  get               Show an environment: template, status, node and worker selector
  help              Help about any command
  image-status      Show the template derived for a rootfs image and whether its golden snapshot is ready
  manifest          Generate Kubernetes manifests to deploy the system
  placement-domains List the worker pools a worker selector can address: labels, nodes, slots per node
  server-info       Show the ate-env-api server's version and features
  suspend           Suspend an environment
  warm-image        Put an image's layers and golden snapshot on every node of a worker pool before a run

Flags:
  -h, --help      help for ate-env
  -v, --version   version for ate-env

Use "ate-env [command] --help" for more information about a command.

$ ate-env manifest --help
Manifest generates Kubernetes manifests for everything environments need on
a cluster that already runs the Agent Substrate system: the target
namespace, a WorkerPool of pre-warmed workers, and the ate-env-api service.
It prints YAML to stdout without touching the cluster; apply it with kubectl.

ate-env-api requires a bearer token from its callers, read from the Secret
--auth-token-secret names (key token). Create that Secret once, in the
namespace this manifest creates, with
"ate-env manifest auth-secret | kubectl create -f -"; ate-env-api starts when
the Secret exists. --insecure-no-auth deploys without the token.

To generate the Substrate ActorTemplate manifest (for kubectl-ate), use
the "template" subcommand: ate-env manifest template

Usage:
  ate-env manifest [flags]
  ate-env manifest [command]

Available Commands:
  access-policy Generate the Substrate access policy that lets ate-env-api manage environments
  auth-secret   Generate the Secret with the bearer token ate-env-api requires from callers
  template      Generate Substrate ActorTemplate manifest

Flags:
      --api-allowed-hosts strings     extra host names that HTTP clients may use to reach ate-env-api, e.g. an internal load balancer name (a leading *. allows the names below it); the Service's in-cluster names, in the cluster's own DNS domain, IP addresses and localhost are always allowed
      --api-image string              digest-pinned ate-env-api image for the API service (required: this ate-env was built from source; release binaries default to the image published with the release, see docs/release.md)
      --api-port int32                port the ate-env-api service listens on (default 7777)
      --api-replicas int32            number of API service replicas (default 1)
      --api-router-tls                mount the pod identity certificate and the servicedns trust bundle into ate-env-api and dial the atenet router's TLS listeners, the only ones Substrate serves since v0.6.0; the API's SPIFFE ID must then be in the router's --ingress-allowed-spiffe-ids. Off, the API dials the plaintext addresses, which only the test fakes serve (default true)
      --ateom-image string            alias for --worker-image, kept from before Substrate renamed ateom to ateworker
      --auth-token-secret string      Kubernetes Secret (key 'token') mounted into ate-env-api; its value is the bearer token ate-env-api requires from callers. Create it with 'ate-env manifest auth-secret' (default "ate-env-api-token")
      --enable-tracing                run ate-env-api with --enable-tracing, exporting OpenTelemetry traces over OTLP/gRPC
  -h, --help                          help for manifest
      --insecure-no-auth              deploy ate-env-api without a caller token: anyone who can reach its Service can create environments, run commands in them and reach their ports
      --metrics-port int32            port ate-env-api serves Prometheus metrics on (--metrics-listen-addr), exposed on the container and the Service as "metrics" with prometheus.io scrape annotations; 0 leaves metrics off. The metrics listener has no authentication and no Host check
      --namespace string              Kubernetes namespace to deploy into (default "ate-env")
      --otlp-endpoint string          OTLP/gRPC collector endpoint for ate-env-api's traces, set as OTEL_EXPORTER_OTLP_ENDPOINT, e.g. http://otel-collector.observability:4317
      --otlp-insecure                 set OTEL_EXPORTER_OTLP_INSECURE=true: talk to the collector in plaintext
      --port-proxy-atespaces string   comma-separated atespaces whose environments ate-env-api's port proxy forwards to, or * for any (defaults to ate-env)
      --replicas int32                number of pre-warmed worker pods (default 5)
      --template string               ActorTemplate name (default "default-template")
      --worker-image string           digest-pinned worker image for the worker pool, e.g. ateworker-gvisor built from the Substrate repo
      --workerpool string             WorkerPool name (defaults to <template>-workerpool)

Use "ate-env manifest [command] --help" for more information about a command.
```

## API

The `ate-env-api` service exposes gRPC APIs for environment lifecycle and in-actor operations, as
well as HTTP endpoints for MCP.

The gRPC services reject a request that carries a field the server does not know, at any depth,
with `InvalidArgument`: a client built from another version or fork of this API fails instead of
being served as if the field were absent. An `InvalidArgument` whose message says "carries field
numbers" thus means that the client and the server come from different builds, not that the
request is wrong: treat it as a configuration error and do not retry. The guest applies the same
check to the requests that `ate-env-api` forwards to it. Limits:

- The check covers requests only. In the other direction, an older client accepts a newer
  server's response field, or a new value of `Environment.state`, and ignores it. That is proto3's
  normal behavior.
- Build the SWE-bench loader and `ate-env-api` from one commit. A loader flag that sets a request
  field the server does not know fails every create with "carries field numbers".
- A renumbered field is not caught. When both sides know field N with different meanings, nothing
  is unknown.
- A guest baked into a golden snapshot keeps the build it had. Guests from before this check accept
  unknown fields and ignore them, and `ate-env-api` cannot detect an old guest. After an upgrade,
  register the base templates again and delete the `<base>-<12 hex digits>` templates derived from
  them, so that their golden snapshots bake with the current guest.
- In a client stream (`WriteFile`, `WriteProcessInput`), a rejected later message ends the call,
  but what the guest already did with the earlier messages stays: a file can be partly written. A
  rejected first message does not wake a suspended environment.

### EnvironmentService

Manages the lifecycle of isolated execution environments (defined in [`proto/ateenv/v1alpha/env.proto`](proto/ateenv/v1alpha/env.proto)). Requests are handled by `ate-env-api` and translated into Agent Substrate control plane operations:

| RPC | Description |
| --- | ----------- |
| `CreateEnvironment` | Creates and starts a new environment actor from an ActorTemplate, or from a digest-pinned `rootfs_image` on top of one: the template becomes the base, the image the container, and the guest is mounted in as a read-only image volume |
| `GetEnvironment` | Retrieves environment details and status, the node it is on and its worker selector |
| `GetImageStatus` | Reports the ActorTemplate derived for a digest-pinned `rootfs_image` (whether it exists) and the state of its golden snapshot: `NONE` (no template), `PENDING` (baking; environments boot from the image), `READY` (with the golden tag), `FAILED` (with the reason). Reads only. `ate-env image-status <image>` |
| `EnsureImageTemplate` | Creates the derived ActorTemplate when it is missing, which starts the golden bake, and reports it like `GetImageStatus`; no environment is created, and a template whose bake failed is recreated the way a create would. `ate-env image-status <image> --ensure`. Advertised as feature `image_status` |
| `ListPlacementDomains` | Reports the worker pools a per-environment worker selector can address: the labels all their schedulable workers carry (the selector), their distinct nodes, workers and slots per node, read from Substrate's workers; with a `template`, only the pools that template's own worker selector matches (`ate-env placement-domains --template`); needs the global viewer role (`ate-env manifest access-policy --global`) |
| `WarmImage` | Server-streaming: puts one digest-pinned `rootfs_image` on every node of one domain before a run. Creates probe environments with the worker selector, resumes them and holds them until the control plane reports them `RUNNING` (the layers and the golden snapshot are on the node by then), reads their nodes, deletes them, and repeats until every node ran the image or `max_rounds` is spent; streams a probe per environment and ends with the coverage. A probe's create, resume and delete are retried on the transient failures of a control plane under load, within the probe's timeout; the probes a call could not delete are listed in the coverage's `undeleted_probes` for a sweep, and the server caps the probes in flight per node of one selector across calls (`-warm-max-probes-per-node`). `nodes` 0 counts the matching workers' nodes, which takes the same role |
| `SuspendEnvironment` | Suspends and checkpoints the environment: to snapshot storage by default, or to its node first with `mode: SUSPEND_MODE_LOCAL` (the upload follows in the background) |
| `DeleteEnvironment` | Deletes the environment permanently |
| `GetServerInfo` | Reports the server implementation (`sandboxenv-rl`), its version, the version of the `go.ate.dev/substrate` module it was built against, and its optional features (`rootfs_image`, `pty`, `suspend_mode`, `image_status`, `placement_domains`, `warm_image`, and `port_proxy` when enabled); answered by `ate-env-api` itself. The Substrate version is a build fact, not the version of the control plane the server talks to; Substrate has no RPC that reports that. `ate-env server-info --json` prints the four fields as one JSON object |

### ProcessService

Manages process execution, I/O streaming, and signals inside the environment container (defined in [`proto/ateenv/v1alpha/guest.proto`](proto/ateenv/v1alpha/guest.proto)). `ate-env-api` forwards requests through the Substrate router to the `ate-env-guest` daemon in the environment, and wakes a suspended environment first:

| RPC | Description |
| --- | ----------- |
| `StartProcess` | Launches a process (optionally with a stdin pipe, a PTY and a timeout) and returns the `Process` resource |
| `GetProcess` | Retrieves the process state, exit code, and timestamps |
| `StreamProcessOutput` | Streams stdout and stderr chunks; with `follow`, ends with an `exit` message carrying the final `Process` |
| `WriteProcessInput` | Streams bytes to the process's stdin; a message with `close` sends EOF |
| `SignalProcess` | Delivers a POSIX signal (`TERM`, `INT`, `KILL`, `USR1`, ...) to the process group |
| `ResizeProcess` | Sets the terminal rows and columns of a process started with a PTY and delivers `SIGWINCH` to its foreground process group |

`exit_code` follows the shell convention: the process's exit code, or 128 + signal number if it was killed by a signal. To wait for a process without receiving its output, follow the output stream with offsets past the end of the spool.

### FileSystemService

Provides chunked streaming file reading and writing within the environment container without unbounded memory usage (defined in [`proto/ateenv/v1alpha/guest.proto`](proto/ateenv/v1alpha/guest.proto)). `ate-env-api` forwards requests through the Substrate router to the `ate-env-guest` daemon in the environment, and wakes a suspended environment first:

| RPC | Description |
| --- | ----------- |
| `ReadFile` | Streams raw binary or text file contents in chunks |
| `WriteFile` | Streams raw binary or text chunks directly to a target file |


## Reaching a service inside an environment

A daemon that listens on a port inside an environment, such as one a runtime layer starts next to
the guest ([docs/rootfs-images/RUNTIMES.md](docs/rootfs-images/RUNTIMES.md)), is reachable through
the API's listener. A plain HTTP request that carries `X-Env-Id` and `X-Env-Port` (and
`X-Env-Atespace` outside `ate-env`) is forwarded through the Substrate router to that port, whatever
its path; the API strips the three headers. The router accepts the tunnel and resumes a suspended
environment for the request, which it holds while the environment wakes, up to about 10 s in
Substrate v0.6.0 (its `--parked-request-budget`, 5 s by default, plus 5 s). A longer wake, usually
only the first wake of an environment on a new image (15 to 31 s measured; a resume after a suspend
takes 1 to 2 s), gets 503 with `Retry-After: 1` and `X-Env-State: RESUMING` or `SUSPENDED`, and the
wake goes on: the client must retry, or wait for a health check before other requests.
`--wake-budget` bounds guest gRPC calls, and the port proxy only when the router refuses the tunnel
itself, for example when it is overloaded. For the router's errors, the proxy reads the
environment's state: a crashed environment gets 410 Gone with `X-Env-State: CRASHED`, after the
router's 5 s hold; one that is being deleted gets 404 with `X-Env-State: DELETING`; one that is
deleted gets 404. A client stops on 410, because only a new environment helps. The daemon's own
answers, its 503, 404 and 504 included, pass unchanged.

```bash
curl -H 'X-Env-Id: dev1' -H 'X-Env-Port: 6000' \
     -H "X-Env-Authorization: Bearer $SUBSTRATE_ENV_AUTH_TOKEN" localhost:7777/health
```

Clients that cannot set headers, such as WebSocket clients and browsers, put the target in the path
instead: `/v1alpha/envs/{id}/ports/{port}/...`, or `/v1alpha/atespaces/{atespace}/envs/{id}/ports/{port}/...`
outside `ate-env`. The API strips that prefix and forwards the rest, so a client given
`http://localhost:7777/v1alpha/envs/dev1/ports/6000` as its base URL works unchanged. WebSocket upgrades pass
through either form; gRPC to the daemon is not supported yet.

```bash
curl -H "Authorization: Bearer $SUBSTRATE_ENV_AUTH_TOKEN" localhost:7777/v1alpha/envs/dev1/ports/6000/health
```

The path form reaches the daemon exactly as sent: a path without the trailing slash is the daemon's
root, and `//` or `..` after the prefix are not cleaned or redirected.

**Who can use it.** The forwarding reaches environments only in the atespaces
`--port-proxy-atespaces` names (default `ate-env`); a request for another atespace gets 403.
Substrate actors carry no label that marks the ones ate-env created, thus inside those atespaces it
reaches any TCP port of any actor, and a dedicated atespace for ate-env is the boundary. Without
`--auth-token-file` (`ate-env manifest --insecure-no-auth`), it does not authenticate callers:
whoever can reach the API's listener can reach those ports. The guest API already reaches each
actor that runs the guest; the forwarding extends that reach to every port. Keep the listener inside
the cluster (the manifest's Service is `ClusterIP`), restrict it with a NetworkPolicy where other
workloads share the cluster, and make each daemon check its own credential.

With `--auth-token-file`, each forwarded request, in either form, must carry the API's token, in
`X-Env-Authorization: Bearer <token>` or in `Authorization: Bearer <token>`; the API answers 401
otherwise. The token does not reach the daemon: the API strips `X-Env-Authorization`, and strips
`Authorization` when it carried the token. Send the token in `X-Env-Authorization` when the daemon
needs its own `Authorization`, such as an OpenAI-compatible server. A browser cannot add either
header to a page load or a WebSocket handshake, thus with the token on, the path form serves
scripted clients only.

A WebSocket client that cannot set headers sends a port capability instead of the token:
`ate-env-capability:<capability>@` in the user info of the URL, which the client sends as
`Authorization: Basic`. The capability is `v1.<expiry>.<mac>`, where
`mac` is the hex HMAC-SHA256, keyed with the API's token, of
`"ate-env port capability v1\n<atespace>\n<id>\n<port>\n<expiry>"` and `expiry` is in Unix
seconds, at most seven days ahead. It opens only WebSocket handshakes (a `GET` with
`Connection: Upgrade` and exactly one `Upgrade` field whose whole value is `websocket`), only to
that environment port, until it expires; a
URL that lands in a log never shows the token. The capability must be the request's only
`Authorization` credential: a second `Authorization` field or a comma-separated value next to it
gets 400. The API never forwards a capability, also when it does not check it. Capabilities signed
with a token stop working when the token rotates; a token rotation is also the only way to revoke a
leaked capability before it expires. The capability names an atespace, an id and a port, not one
environment instance: when the caller deletes the environment and creates a new one with the same id,
the capability opens the new environment too, until it expires. Give a capability the shortest expiry
the client needs, and do not reuse an environment id while a capability for it can still be valid.

**Browsers.** The path form refuses a state-changing request or a WebSocket handshake that a browser
sends from a page of another origin, so a web page cannot drive a daemon through a `kubectl
port-forward`. A page served by the forwarded daemon itself is same-origin and passes. All daemons
behind one listener share that origin, thus a page from one daemon can call another.
`--port-proxy-trusted-origins` adds origins, such as a UI on its own host. The header form needs no
check: a browser sends custom headers to another origin only after a CORS preflight, which the API
never approves.

**DNS rebinding.** A page on an attacker's name can make that name resolve to the listener, for
example to a `kubectl port-forward` on 127.0.0.1, and then read the answers as its own origin. The
listener refuses that: a request whose `Host` is not an IP address, `localhost`, a name under
`.localhost`, a name of the API Service or a name in `--allowed-hosts` gets 403. The check covers
each HTTP route, the MCP endpoint and both forms of the port proxy, over HTTP/1.x and HTTP/2 alike,
thus also behind a front proxy that speaks h2c to the listener. Only gRPC is not checked: HTTP/2
requests with an `application/grpc` content type, which gRPC clients send to many different
authorities. The check is a browser defense, not authentication: a client that sets its own `Host`
passes, and the bearer token is the control for such callers, gRPC included.

The API Service's names are `ate-env-api`, `ate-env-api.<namespace>`, `ate-env-api.<namespace>.svc`
and `ate-env-api.<namespace>.svc.<cluster domain>`. `ate-env-api` derives them itself from
`--service-name`, with the namespace and the cluster's own DNS domain from the pod's
`/etc/resolv.conf` (else the namespace from `POD_NAMESPACE`), thus an upgrade that changes only the
image keeps them. Add other names, such as an internal load balancer, with `--api-allowed-hosts` on
`ate-env manifest`.

| Flag | Default | Effect |
|---|---|---|
| `--allowed-hosts` | none | Comma-separated host names that HTTP callers may send in `Host`, next to IP addresses, `localhost`, names under `.localhost` and the names of `--service-name`, which are always allowed. A leading `*.` allows the names below it. Needed for a name such as an internal load balancer: `ate-env manifest --api-allowed-hosts` renders it. |
| `--service-name` | `ate-env-api` | The Kubernetes Service in front of `ate-env-api`; its in-cluster names are allowed hosts. Empty allows none of them. |
| `--atenet-connect` | `atenet-router.ate-system.svc.cluster.local:8081`, or `:8444` with `--atenet-client-cert` | The router's CONNECT listener. An empty value turns the forwarding off. |
| `--port-proxy-atespaces` | `ate-env` | Comma-separated atespaces whose environments the forwarding reaches; `*` means any atespace the router serves. An empty value is an error. A request that names no atespace targets `ate-env`, thus list it to serve such requests. |
| `--atenet-client-cert` | none | PEM credential bundle presented to the router's TLS listeners: the pod identity bundle the kubelet projects. With it, `--atenet` and `--atenet-connect` default to the TLS ports (443 and 8444), which are the only ones Substrate v0.6.0 serves. Re-read at every handshake. |
| `--atenet-ca-file` | none | PEM trust bundle that signs the router's serving certificate, the servicedns bundle. Required with `--atenet-client-cert`; re-read at every handshake. |
| `--atenet-server-name` | `atenet-router.ate-system.svc` | Name the router's serving certificate must carry. |
| `--port-proxy-trusted-origins` | none | Comma-separated browser origins allowed to use the path form. |
| `--port-proxy-response-header-timeout` | `0`, no limit | Bound on the wait for a daemon's response headers. Without it, the caller's deadline ends the wait. |

## Built-in MCP Server

The API exposes a streamable MCP endpoint at `POST /v1alpha/envs/{id}/mcp` serving built-in tools.
Outside the `ate-env` atespace, add `?atespace=<name>` to the URL. Each request carries the caller
token in `Authorization: Bearer <token>`; the examples read it from `SUBSTRATE_ENV_AUTH_TOKEN`. Leave
the header out for an `ate-env-api` deployed with `--insecure-no-auth`.

### Available Tools

| Tool | Category | Description |
| ---- | -------- | ----------- |
| `read_file` | Filesystem | Read a file |
| `write_file` | Filesystem | Create or overwrite a file |
| `shell` | Shell | Run a shell command line inside the environment |

### JSON-RPC Over HTTP

Clients communicate with the MCP server at `/v1alpha/envs/{id}/mcp` using JSON-RPC 2.0:

#### Initialize

```bash
curl -X POST localhost:7777/v1alpha/envs/dev1/mcp \
     -H "Authorization: Bearer $SUBSTRATE_ENV_AUTH_TOKEN" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json, text/event-stream" \
     -d '{
       "jsonrpc": "2.0",
       "id": 1,
       "method": "initialize",
       "params": {
         "protocolVersion": "2025-11-25",
         "capabilities": {},
         "clientInfo": {"name": "curl", "version": "1.0.0"}
       }
     }'
```

#### List Tools (`tools/list`)

```bash
curl -X POST localhost:7777/v1alpha/envs/dev1/mcp \
     -H "Authorization: Bearer $SUBSTRATE_ENV_AUTH_TOKEN" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json, text/event-stream" \
     -d '{
       "jsonrpc": "2.0",
       "id": 2,
       "method": "tools/list"
     }'
```

#### Call Tool (`tools/call`)

```bash
curl -X POST localhost:7777/v1alpha/envs/dev1/mcp \
     -H "Authorization: Bearer $SUBSTRATE_ENV_AUTH_TOKEN" \
     -H "Content-Type: application/json" \
     -H "Accept: application/json, text/event-stream" \
     -d '{
       "jsonrpc": "2.0",
       "id": 3,
       "method": "tools/call",
       "params": {
         "name": "shell",
         "arguments": {"command": "echo hello from mcp"}
       }
     }'
```
## Local CI

Maintainers run the CI checks locally using `make`, in a reproducible toolchain: tool versions come from `flake.lock`, `go.mod` (including the protobuf generators, via `tool` directives) and `uv.lock` (every Python package, for all the Python projects: one uv workspace at the repository root), so a given commit gets the same toolchain on every host. The runs aren't hermetic: Kind E2E pulls `kindest/node`, Substrate images and `python:3.13-slim` by pinned digest at run time and clones Substrate at the `go.mod` pin.

Supported hosts: CI runs on `x86_64-linux` (GitHub's `ubuntu-24.04`) with Go from `go.mod` and Python 3.13 (the previous workflow used 3.12); the uni-agent integration runs on Python 3.12, because upstream uni-agent doesn't support 3.13 yet. The flake also provides `aarch64-linux` and `aarch64-darwin` devShells. CI checks that the `aarch64-darwin` (Apple Silicon macOS) devShell builds and runs the basic tools, but runs the test suites only on Linux, so treat both as best effort. Intel macOS isn't supported, because current nixpkgs dropped `x86_64-darwin`. The E2E suite needs Docker on any host.

Execution is managed by `hack/ci-compat.sh`, which automatically selects [Nix](https://nixos.org) (running impurely, inheriting your environment) and falls back to Docker (`Dockerfile.ci`) if Nix is not installed:

```bash
# Run all local CI checks (Go and Python suites)
make ci

# Run individual suites
make ci-go
make ci-python
make ci-openhands
make ci-harbor
make ci-dafny
make ci-e2e

# Run Go and Python test suites with coverage and generate reports (see docs/chores/coverage.md)
make coverage

# Preserve the Kind cluster after ci-e2e completes
KEEP_CLUSTER=1 make ci-e2e

# Fast iteration: reuse an existing cluster without re-creating or deleting it
REUSE_CLUSTER=sbx-e2e-abc123 make ci-e2e

# Verify compatibility against substrate@HEAD (or custom git ref / local path)
make check-substrate-head ARGS=main
make check-substrate-head ARGS=../substrate

# Verify and update dependency pins
make check-pins
make update-pins

# Before pushing: lint GitHub Actions workflows locally (offline zizmor security audit + actionlint)
make lint-actions-locally

# Enter the interactive development shell
make dev-shell

# Enter the integrations' development shells (their Python envs, from uv.lock)
nix develop .#openhands
nix develop .#uni-agent
nix develop .#harbor

# Without Nix: the same locked Python packages in .venv
uv sync --all-packages --all-extras

# Override the runner (auto [default: nix -> docker], nix, docker, or host)
CI_RUNNER=docker make ci
```

### Runners and Environment

- **`auto` (default):** Runs via Nix (`nix develop -c <cmd>`) if `nix` is installed, else via Docker if `docker` is available.
- **`nix`:** Runs impurely using `nix develop -c` without whitelisting or clearing the environment. Every Python env is built by Nix from `uv.lock` ([`nix/python.nix`](nix/python.nix), with uv2nix), with this repository's projects installed editable; besides the default devShell, `nix develop .#openhands` (Python 3.13) and `nix develop .#uni-agent` (Python 3.12) carry the integrations' envs, and `ci-python` builds the base and uni-agent envs it needs (`nix build .#python-env-base .#python-env-uni-agent`) on first use, so other commands don't pay for them. After editing a `pyproject.toml`, run `uv lock` in the devShell and commit `uv.lock` (see [check-pins.md](docs/chores/check-pins.md)).
- **`docker`:** Automatically builds `sandboxenv-rl-ci:local` from a copy of the tree and runs the command in a container. The Nix store and the Go and other caches live in named volumes (`sandboxenv-rl-nix`, `sandboxenv-rl-cache`), so like on a Nix host, the first run builds the devShell and later runs add only what changed. `docker volume rm sandboxenv-rl-nix` reclaims the space. Commands that write files (`make protos`, `make update-pins`) return a `git diff` that is applied on the host as you, so file ownership stays yours. Note that passing `--docker-socket` (used only by `make ci-e2e` in the Docker runner) mounts `/var/run/docker.sock`, which grants the container root-equivalent access to the host; `make smoke` uses `--host-network` instead. Both share the host network and mount the host kubeconfig read-only (a single file; a colon-separated `KUBECONFIG` isn't mounted), so `REUSE_CLUSTER` works under Docker too.
- **`host`:** Executes the command directly on the host, with whatever is on `PATH`. Targets that only need Go and Python work without Nix or Docker: `make protos` needs Go (`GOTOOLCHAIN` fetches the version `go.mod` asks for) and `grpcio-tools` (`uv sync --package ate-env-rl --extra dev`, or `pip install -e 'clients/python[dev]'`); buf and the protoc plugins come from `go.mod` through `go tool`. The `ci-*`, `smoke`, `check-pins`, `update-pins`, `check-substrate-head` and `lint-actions-locally` commands are scripts the devShell provides, so with `host` they only work inside `nix develop`.

> [!NOTE]
> Online GitHub Actions security audits (such as ref-version-mismatch and impostor-commit) are covered by the organization's required GitHub Actions scan on pull requests. `make lint-actions-locally` runs zizmor in `--offline` mode alongside actionlint.

### Developing against a local Substrate checkout

When making cross-repository changes across [Agent Substrate](https://github.com/agent-substrate/substrate) and `sandboxenv-rl`, use Go workspaces (`go.work`) to link the checkouts:

```bash
# Initialize a Go workspace linking sandboxenv-rl and your local Substrate clone
go work init . ../substrate

# Or add Substrate to an existing workspace
go work use ../substrate
```

Both `go.work` and `go.work.sum` are gitignored and never committed, ensuring CI always builds against the strictly pinned module version.

To revert to the pinned `go.mod` dependency:
```bash
# Remove the workspace file
rm go.work go.work.sum

# Or temporarily bypass workspace mode for a command
GOWORK=off make ci-go
```

**What uses the local checkout:**
The Go module graph decides where Substrate comes from. When a workspace is active (or a `replace` directive is present in `go.mod`), the following tools automatically build against and run with your local Substrate checkout instead of the `go.mod` pin:
- **Go compilation and test suites (`make ci-go` / `go build ./...` / `go test ./...`):** The Go compiler natively loads Substrate packages from the workspace directory.
- **Substrate locator (`ensure-substrate`):** Discovers the local override from the module graph, prints the local directory, and skips commit hash checks (strict hash verification only applies when there is no override).
- **Kind E2E integration pipeline (`make ci-e2e` / `hack/ci/e2e.py`):** Relies on `ensure-substrate` to build `kubectl-ate` from your local Substrate tree, compile the `ateworker-gvisor` container image from your local source, and execute cluster setup scripts (`hack/create-kind-cluster.sh` and `go run ./cmd/ate-setup --kind deploy ate-system`) directly from the local Substrate directory.

Other tools do not consult the Go module graph: `check-pins` validates Nix and container digests against repository manifests, `nix devShell` manages CLI toolchain binaries, and `check-substrate-head` manages its own isolated target ref or temporary workspace.

> [!NOTE]
> **Parent-directory pitfall:** Go searches upward for `go.work` files. A `go.work` file placed in a parent directory (such as `~/Projects/rl-substrate/go.work`) will be discovered and applied to all child worktrees automatically. Run `go env GOWORK` to verify which `go.work` file is currently active.

For maintenance chores and troubleshooting, see [Dependency pins and how to update them](docs/chores/check-pins.md), [Bumping the Substrate dependency](docs/chores/check-substrate-head.md), and the [Troubleshooting guide](docs/chores/troubleshooting.md).

## Observability

Run reports, Prometheus metrics and OpenTelemetry traces across the Python client, ate-env-api,
the guest and the Go client, all off by default except the run report: see
[docs/observability.md](docs/observability.md) and the metric reference [docs/metrics.md](docs/metrics.md).

## Security

The trust boundaries and their known gaps are in [docs/security/threat-model.md](docs/security/threat-model.md).
To report a vulnerability, follow [SECURITY.md](SECURITY.md).

## Cleanup

```bash
# Delete each environment while ate-env-api still runs (list them with
# `kubectl ate get actor -a ate-env`):
ate-env delete <id>

# Remove the namespace: the WorkerPool, the ServiceAccount and ate-env-api.
kubectl delete ns ate-env

# Delete each template, including the <base>-<12 hex digits> templates that
# ate-env-api derived per rootfs image (list them with
# `kubectl ate get actor-template -a ate-env`). Each delete also removes the
# template's golden snapshot.
kubectl ate delete actor-template -a ate-env default-template

# Remove the access policy, then the atespace (this fails while any actor remains).
kubectl ate delete access-policy -a ate-env
kubectl ate delete atespace ate-env
```

## Origin

This repository started in October 2026 from
[agent-substrate/env](https://github.com/agent-substrate/env) with its git history, its pull
requests (recreated here with an `[env#N]` title prefix) and its open issues. The components keep
the `ate-env`, `ate-env-api`, and `ate-env-guest` names, the `ate_env_rl` Python package, the
`ateenv.v1alpha` API, and the `github.com/agent-substrate/env` Go module path.

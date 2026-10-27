# sandboxenv-rl

> [!WARNING]
> This is an alpha API and is likely to change until v1.0 is released.

Sandboxed execution environments for reinforcement learning (RL) on
[Agent Substrate](https://github.com/agent-substrate/substrate). Each rollout, eval task or agent
episode gets its own isolated, stateful environment: a Substrate actor that runs in gVisor and is
driven remotely through a gRPC API, a CLI, a Python fleet SDK or MCP.

RL jobs create many environments per step and leave most of them idle while the model generates.
Substrate fits that shape. A suspended environment lives as a snapshot and holds no worker, and the
next command resumes it, thus a job can park its environments between steps. Later environments on
the same image restore a golden snapshot instead of booting. This repo adds the environment API,
the fleet SDK, the framework integrations and the benchmarks on top.

## What changed

This repo started in October 2026 as a copy of [agent-substrate/env](https://github.com/agent-substrate/env),
with its history, its pull requests (recreated here with an `[env#N]` title prefix) and its open
issues. agent-substrate/env is being repurposed, and the RL work continues here. The components keep
their ate-env names: the `ate-env`, `ate-env-api` and `ate-env-guest` binaries and images, the
`ate_env` Python package and the `ateenv.v1alpha` API. The Go module path is still
`github.com/agent-substrate/env`.

The focus moves from a general environment service to RL training and evaluation:

| Area | What it adds |
|---|---|
| Fleet SDK | `SandboxFleet` and `AsyncSandboxFleet` in [`clients/python`](clients/python/README.md): acquire sandboxes for tasks by image, run tasks with the `naive`, `sliding` or `pipelined` strategy, pre-warm images, release and recycle. |
| Any task image | `rootfs_image` runs a digest-pinned image unmodified: `ate-env-api` derives an ActorTemplate per image and mounts the guest into it as an image volume. Runtime layers and sidecars add a second runtime to any image ([docs/rootfs-images](docs/rootfs-images/README.md)). |
| Daemons inside an environment | A port proxy on `ate-env-api` reaches any port in an environment by header or by path, WebSockets included ([details](#reaching-a-service-inside-an-environment)). |
| Framework integrations | A [NeMo Gym](integrations/nemo-gym/README.md) sandbox provider, a [uni-agent](integrations/uni-agent/README.md) sandbox provider with an R2E-Gym task graded by hidden tests in the same environment, and a [GRPO](examples/grpo/README.md) training loop on TRL. |
| Benchmarks | A [SWE-bench Verified loader](benchmarking/swebench/README.md) that drives real task images the way an RL job does, and the [design](internal/benchmarking/DESIGN.md) of the benchmark system. |
| Platform | Built against Substrate `v0.6.0` (actor limits are integer millicores and bytes): the WorkerPool chooses the sandbox, each template selects its pool by label, suspends are durable, and `ate-env-api` runs as its own identity under Substrate authorization. The guest runs as a PID 1 init that reaps orphans, and it allocates PTYs. The control plane must run v0.6.0 or later: the release renumbered proto fields env reads and writes, so an older control plane cannot serve this build. `ate-env-api` reaches actors through the router's TLS listeners with its pod identity certificate (`ate-env manifest --api-router-tls`, on by default), so the install must list `spiffe://cluster.local/ns/ate-env/sa/ate-env-api` in the router's ingress allow-list. |

## Architecture

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
| [`cmd/ate-env`](cmd/ate-env) | CLI: create, suspend and delete environments, run commands, move files, and print the Kubernetes and Substrate manifests. |
| [`cmd/ate-env-api`](cmd/ate-env-api) | The API service: environment lifecycle, guest calls, MCP and the port proxy. |
| [`cmd/ate-env-guest`](cmd/ate-env-guest) | The daemon inside each environment: processes and files. |
| [`clients/python`](clients/python/README.md) | Python client, import name `ate_env`: `Client` and `Env` for single environments, and the fleet SDK. |
| [`clients/go`](clients/go) | Go client. |
| [`integrations/`](#integrations) | Framework integrations. |
| [`examples/`](#examples) | Runnable examples. |
| [`benchmarking/`](benchmarking/swebench/README.md), [`cmd/benchmarking`](cmd/benchmarking), [`internal/benchmarking`](internal/benchmarking/DESIGN.md) | The SWE-bench loader, its scripts and the benchmark design. |
| [`docs/`](docs) | Rootfs images and runtime layers, the [metrics reference](docs/metrics.md), the [threat model](docs/security/threat-model.md), and how releases are cut. |

## Examples

| Example | What it shows |
|---|---|
| [`examples/grpo`](examples/grpo/README.md) | A real RL loop: TRL's GRPOTrainer trains Qwen2.5-0.5B on MBPP, and each completion runs in an environment that computes the reward. Covers the park/wake pattern, with the fleet suspended after each step. CPU only. |
| [`examples/runtime-layers`](examples/runtime-layers/README.md) | An unmodified rootfs image with a second runtime injected as a layer and supervised as a sidecar. |
| [`examples/mcp`](examples/mcp/main.go) | Go: connect to an environment's MCP endpoint, list the tools and call them. |
| [`examples/guest-daemon`](examples/guest-daemon/README.md) | Go: run the guest gRPC services standalone, without a cluster. |
| [`examples/Qwen3.5-35B-A3B-verl`](examples/Qwen3.5-35B-A3B-verl/README.md) | Agentic GRPO on real SWE tasks: verl `separate_async` with a Megatron trainer and vLLM rollouts on separate nodes, a uni-agent ReAct agent whose tools and hidden-test grading run in environments. Two 8-GPU nodes. |

## Integrations

| Integration | Framework | What it does |
|---|---|---|
| [`integrations/nemo-gym`](integrations/nemo-gym/README.md) | [NeMo Gym](https://github.com/NVIDIA-NeMo/Gym) | Sandbox provider `substrate`, registered through the `nemo_gym.sandbox_providers` entry point, with no change to NeMo Gym. `endpoint()` reaches a daemon inside the sandbox through the port proxy. |
| [`integrations/uni-agent`](integrations/uni-agent/README.md) | [uni-agent](https://github.com/verl-project/uni-agent) | Sandbox provider `ate_env` plus an `r2e_gym` task that hides the tests, lets the agent work, then grades in the same environment; dataset and image tooling for R2E-Gym, SWE-reBench and SWE-bench rows. Used by `examples/Qwen3.5-35B-A3B-verl`. |

## Benchmarks

[`swebench-loader`](benchmarking/swebench/README.md) drives SWE-bench Verified task images through
the API the way a simulated RL job does: `eval` mode creates, replays, grades and deletes each task,
and `steps` mode creates a fleet once and parks it between training steps. Image-delivery workloads
compare image pull, image streaming and other loaders on the same images. The report breaks down
what each phase cost.
[`internal/benchmarking/DESIGN.md`](internal/benchmarking/DESIGN.md) specifies the benchmark system.

## Support

The components, the clients, the integrations and the `ateenv.v1alpha` API are supported;
`examples/` and `internal/examples/` are not. [docs/support.md](docs/support.md) says what
supported means for each path.

## Get started

You need a cluster with Agent Substrate v0.6.0 or later, a snapshots bucket,
`kubectl` with `kubectl-ate`, Go, [ko](https://ko.build), and Python 3.10 or later for the SDK. This
repo publishes no release binaries or images yet, thus build them:

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
ate-env manifest template --guest-image <guest-image> --snapshots-bucket <object-storage-url> \
  | kubectl-ate create actor-template -f -
```

Try an environment:

```bash
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777 &
ate-env create dev1
ate-env dev1 shell 'echo hello from $(hostname)'
ate-env delete dev1
```

Drive environments from Python with the fleet SDK (`pip install ./clients/python`). Leaving the
`with` block deletes everything the fleet created:

```python
from ate_env import FleetConfig, SandboxFleet, Task

with SandboxFleet(FleetConfig(endpoint="127.0.0.1:7777")) as fleet:
    handle = fleet.acquire(Task(id="t1", image="docker.io/library/python@sha256:<digest>"))
    print(handle.exec(["python3", "-c", "print(6 * 7)"]).stdout)
    handle.write_file("/tmp/answer.txt", "42\n")
    print(handle.read_file_bytes("/tmp/answer.txt"))
    handle.release()
```

Then pick an [example](#examples) or an [integration](#integrations).

## Deployment guide

Prerequisites: a cluster with [Agent Substrate](https://github.com/agent-substrate/substrate)
installed and a snapshots bucket. ate-env is built against Substrate
[`v0.6.0`](https://github.com/agent-substrate/substrate/releases/tag/v0.6.0) (Go module
`go.ate.dev/substrate`), and the control plane must run v0.6.0 or later: the release renumbered
the wire fields of Actor, ActorStatus, ActorTemplate, SnapshotConfig and Volume, so this build
cannot create or read actors on an older control plane, and its templates carry limits as integer
millicores and bytes. Upgrade the control plane and the ate-env images together; with an older
Substrate, use ate-env from before the v0.6.0 bump. The WorkerPool alone chooses the sandbox
(`spec.sandboxClasses`) and the ActorTemplate names none. Because the template no
longer names a class, an actor booting without a snapshot can land on any pool whose labels its
template's `workerSelector` matches, and on any pool at all without a selector. `ate-env` labels its
WorkerPool `ate-env/workerpool: <pool>` and each template selects that label, thus actors stay on
their pool and its sandbox class; `--workerpool` on `ate-env manifest template` names the pool to select:
it defaults to `default-template-workerpool`, the pool `ate-env manifest` creates by default, whatever
the template is named. Pass it when you deploy with another `--template` or `--workerpool`. The templates also use
`snapshotConfig` and `wakeupProbe` and declare CPU and memory limits, and `--worker-image` must be the
`ateworker-gvisor` image built from the Substrate version on the cluster.

`ate-env-api` suspends synchronously: a suspend returns once the snapshot is in object storage, thus a
suspended environment survives the loss of its node. Substrate's own default became asynchronous in
[`b40d34f1`](https://github.com/agent-substrate/substrate/commit/b40d34f1).

Substrate enforces authorization by default. `ate-env-api` runs as its own ServiceAccount
(`ate-env/ate-env-api`) and needs the editor role on each atespace it creates environments in; the
atespace must exist first, because creating one takes a global owner. Step 1 sets both up.

When you upgrade an existing cluster across these changes:

- Create the atespace and the access policy (step 1) before you apply the new manifest. Without them
  `ate-env-api` fails at startup with `PermissionDenied` while it checks the default atespace.
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

# Ensure that the pods are running:
kubectl get pods -n ate-env
```

`ate-env-api` waits for a suspended environment to wake before forwarding a guest call, up to
`--wake-budget` (default 2m; cold image pulls and cross-region snapshot reads exceed the router's own
parking budget) and, once the control plane reports the environment RUNNING, up to
`--wake-running-budget` (default 20s). Both are flags of the `ate-env-api` binary; `ate-env manifest`
does not set them, thus add them to the Deployment's arguments to change them.

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

By default `ate-env manifest` deploys `ate-env-api` without caller authentication for use over
`kubectl port-forward`. Before exposing `ate-env-api` beyond loopback (e.g. through an internal
LoadBalancer or to multi-tenant namespaces), create a Kubernetes Secret with a `token` key and pass
`--auth-token-secret <secret-name>` to `ate-env manifest` (which mounts the secret and sets
`-auth-token-file` on `ate-env-api`); callers then authenticate with `--auth-token-file`,
`SUBSTRATE_ENV_AUTH_TOKEN` or `--auth-token` on the CLI (`--auth-token` shows the token in `ps`
output and the shell history), `ClientOptions.AuthToken` in the Go SDK, `Client(..., auth_token=...)`
in the Python SDK, or `Authorization: Bearer <token>` on MCP HTTP requests and port-proxy requests
([Reaching a service inside an environment](#reaching-a-service-inside-an-environment)).
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
  --guest-image    <registry>/ate-env-guest@sha256:<digest> \
  --snapshots-bucket <object-storage-url> | kubectl-ate create actor-template -f -
```

Each actor gets 2 CPUs and 4 GiB unless you pass `--cpu` and `--memory` (Kubernetes quantities such as
`500m` or `8Gi`; CPU at most `999.999`, as Substrate requires). Substrate requires both limits and books them against worker capacity; templates
derived per rootfs image inherit them.

That template's container is the guest image itself. To run another image unmodified, name
it with `--rootfs-image`: the guest is then mounted into it as a read-only OCI image volume at
`/ate` and started from there, so the rootfs image is never rebuilt.

```bash
ate-env manifest template --template py312 \
  --rootfs-image   docker.io/library/python@sha256:<digest> \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> \
  --snapshots-bucket <object-storage-url> | kubectl-ate create actor-template -f -
```

A second runtime rides along the same way: `--layer` mounts its image read-only next to the
guest, `--sidecar` has the guest start and supervise a process from it, and `--sidecar-readyz`
holds the actor's readiness until that process answers. Here a static busybox stands in for a
runtime daemon and serves HTTP on port 44772 inside the environment:

```bash
ate-env manifest template --template layered \
  --rootfs-image   docker.io/library/python@sha256:<digest> \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> \
  --layer          web=docker.io/library/busybox@sha256:<digest of the 1.37-musl tag>=/opt/web \
  --sidecar        '/opt/web/bin/busybox httpd -f -p 44772 -h /' \
  --sidecar-readyz http://127.0.0.1:44772/etc/hostname \
  --snapshots-bucket <object-storage-url> | kubectl-ate create actor-template -f -
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
# Port-forward the ate-env-api service.
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777 &

# Create an environment.
ate-env create dev1

# Or run any digest-pinned image unmodified. ate-env-api derives a template
# from default-template on first use (guest mounted in as an image volume)
# and reuses it for later environments on the same image.
ate-env create py1 --rootfs-image docker.io/library/python@sha256:<digest>

# Or from the layered template above: the second runtime answers inside the
# environment, and any other image on that base inherits it.
ate-env create web1 --template layered
ate-env web1 shell '/opt/web/bin/busybox wget -qO- http://127.0.0.1:44772/etc/os-release'
ate-env create node1 --template layered --rootfs-image docker.io/library/node@sha256:<digest>

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

Subcommand names (`create`, `delete`, `suspend`, `manifest`, `server-info`, `help`, `completion`) are
reserved: `ate-env create` rejects them as environment IDs, because `ate-env <id> ...` cannot reach
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
  completion  Generate the autocompletion script for the specified shell
  create      Create and start an environment
  delete      Delete an environment
  help        Help about any command
  manifest    Generate Kubernetes manifests to deploy the system
  server-info Show the ate-env-api server's version and features
  suspend     Suspend an environment

Flags:
  -h, --help      help for ate-env
  -v, --version   version for ate-env

Use "ate-env [command] --help" for more information about a command.

$ ate-env manifest --help
Manifest generates Kubernetes manifests for everything environments need on
a cluster that already runs the Agent Substrate system: the target
namespace, a WorkerPool of pre-warmed workers, and the ate-env-api service.
It prints YAML to stdout without touching the cluster; apply it with kubectl.

To generate the Substrate ActorTemplate manifest (for kubectl-ate), use
the "template" subcommand: ate-env manifest template

Usage:
  ate-env manifest [flags]
  ate-env manifest [command]

Available Commands:
  access-policy Generate the Substrate access policy that lets ate-env-api manage environments
  template      Generate Substrate ActorTemplate manifest

Flags:
      --api-allowed-hosts strings   extra host names that HTTP clients may use to reach ate-env-api, e.g. an internal load balancer name (a leading *. allows the names below it); the Service's in-cluster names, in the cluster's own DNS domain, IP addresses and localhost are always allowed
      --api-image string            digest-pinned ate-env-api image for the API service (required: this ate-env was built from source; release binaries default to the image published with the release, see docs/release.md)
      --api-port int32              port the ate-env-api service listens on (default 7777)
      --api-replicas int32          number of API service replicas (default 1)
      --api-router-tls              mount the pod identity certificate and the servicedns trust bundle into ate-env-api and dial the atenet router's TLS listeners, the only ones Substrate serves since v0.6.0; the API's SPIFFE ID must then be in the router's --ingress-allowed-spiffe-ids. Off, the API dials the plaintext addresses, which only the test fakes serve (default true)
      --ateom-image string          alias for --worker-image, kept from before Substrate renamed ateom to ateworker
      --auth-token-secret string    Kubernetes Secret name (with key 'token') to mount into ate-env-api and require as a caller bearer token
      --enable-tracing              run ate-env-api with --enable-tracing, exporting OpenTelemetry traces over OTLP/gRPC
  -h, --help                        help for manifest
      --metrics-port int32          port ate-env-api serves Prometheus metrics on (--metrics-listen-addr), exposed on the container and the Service as "metrics" with prometheus.io scrape annotations; 0 leaves metrics off. The metrics listener has no authentication and no Host check
      --namespace string            Kubernetes namespace to deploy into (default "ate-env")
      --otlp-endpoint string        OTLP/gRPC collector endpoint for ate-env-api's traces, set as OTEL_EXPORTER_OTLP_ENDPOINT, e.g. http://otel-collector.observability:4317
      --otlp-insecure               set OTEL_EXPORTER_OTLP_INSECURE=true: talk to the collector in plaintext
      --replicas int32              number of pre-warmed worker pods (default 5)
      --template string             ActorTemplate name (default "default-template")
      --worker-image string         digest-pinned worker image for the worker pool, e.g. ateworker-gvisor built from the Substrate repo
      --workerpool string           WorkerPool name (defaults to <template>-workerpool)

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
| `GetEnvironment` | Retrieves environment details and status |
| `SuspendEnvironment` | Suspends and checkpoints the environment to snapshot storage |
| `DeleteEnvironment` | Deletes the environment permanently |
| `GetServerInfo` | Reports the server implementation (`sandboxenv-rl`), its version, the version of the `go.ate.dev/substrate` module it was built against, and its optional features (`rootfs_image`, `pty`, and `port_proxy` when enabled); answered by `ate-env-api` itself. The Substrate version is a build fact, not the version of the control plane the server talks to; Substrate has no RPC that reports that. `ate-env server-info --json` prints the four fields as one JSON object |

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
its path; the API strips the three headers. The router resumes a suspended environment first,
and the proxy waits for the wake up to `--wake-budget` (the same wait as a guest call) before it
answers 503.

```bash
curl -H 'X-Env-Id: dev1' -H 'X-Env-Port: 6000' localhost:7777/health
```

Clients that cannot set headers, such as WebSocket clients and browsers, put the target in the path
instead: `/v1alpha/envs/{id}/ports/{port}/...`, or `/v1alpha/atespaces/{atespace}/envs/{id}/ports/{port}/...`
outside `ate-env`. The API strips that prefix and forwards the rest, so a client given
`http://localhost:7777/v1alpha/envs/dev1/ports/6000` as its base URL works unchanged. WebSocket upgrades pass
through either form; gRPC to the daemon is not supported yet.

```bash
curl localhost:7777/v1alpha/envs/dev1/ports/6000/health
```

The path form reaches the daemon exactly as sent: a path without the trailing slash is the daemon's
root, and `//` or `..` after the prefix are not cleaned or redirected.

**Who can use it.** Without `--auth-token-file`, the forwarding does not authenticate callers.
Whoever can reach the API's listener can reach any TCP port of any actor the Substrate router serves,
in any atespace, including actors that ate-env did not create. The guest API already reaches each actor that runs
the guest; the forwarding extends that reach to every port. Keep the listener inside the cluster
(the manifest's Service is `ClusterIP`), restrict it with a NetworkPolicy where other workloads
share the cluster, and make each daemon check its own credential.

With `--auth-token-file`, each forwarded request, in either form, must carry the API's token, in
`X-Env-Authorization: Bearer <token>` or in `Authorization: Bearer <token>`; the API answers 401
otherwise. The token does not reach the daemon: the API strips `X-Env-Authorization`, and strips
`Authorization` when it carried the token. Send the token in `X-Env-Authorization` when the daemon
needs its own `Authorization`, such as an OpenAI-compatible server. A browser cannot add either
header to a page load or a WebSocket handshake, thus with the token on, the path form serves
scripted clients only.

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
| `--atenet-client-cert` | none | PEM credential bundle presented to the router's TLS listeners: the pod identity bundle the kubelet projects. With it, `--atenet` and `--atenet-connect` default to the TLS ports (443 and 8444), which are the only ones Substrate v0.6.0 serves. Re-read at every handshake. |
| `--atenet-ca-file` | none | PEM trust bundle that signs the router's serving certificate, the servicedns bundle. Required with `--atenet-client-cert`; re-read at every handshake. |
| `--atenet-server-name` | `atenet-router.ate-system.svc` | Name the router's serving certificate must carry. |
| `--port-proxy-trusted-origins` | none | Comma-separated browser origins allowed to use the path form. |
| `--port-proxy-response-header-timeout` | `0`, no limit | Bound on the wait for a daemon's response headers. Without it, the caller's deadline ends the wait. |

## Built-in MCP Server

The API exposes a streamable MCP endpoint at `POST /v1alpha/envs/{id}/mcp` serving built-in tools.
Outside the `ate-env` atespace, add `?atespace=<name>` to the URL.

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

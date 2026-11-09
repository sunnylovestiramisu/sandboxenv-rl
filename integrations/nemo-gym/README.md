# NeMo Gym sandbox provider for Agent Substrate

Runs [NeMo Gym](https://github.com/NVIDIA-NeMo/Gym) rollout sandboxes as **Substrate actors**,
fronted by the `ate-env` API of this repo and reached through its Python client,
[`ate_env_rl`](../../clients/python).
Registers as provider `substrate` next to the built-ins (Docker, Daytona, ECS Fargate, Enroot,
OpenShell, OpenSandbox, Apptainer) via the `nemo_gym.sandbox_providers` entry point — **no changes
to NeMo Gym required**, and no changes to your resource servers beyond the `sandbox:` config block.

Why run rollouts on Substrate:

- **Idle sandboxes hold no worker.** With `create.suspend_policy` set, the provider parks a sandbox
  once it has been idle for a second after a command, and ate-env-api wakes it on the next one
  (see [Parking](#parking-an-idle-sandbox-holds-no-worker)). A rollout fleet is mostly waiting on
  the policy model, so effective density is bounded by the *active* set, not the fleet size.
- **Repeat sandboxes start from golden snapshots.** The first sandbox on an image pays the boot;
  later ones restore a memory snapshot instead of booting.
- **Prepared states fork.** A sandbox that finished expensive task setup can be snapshotted and
  branched into N independent rollouts (best-of-N from a mid-episode state).

Verified against **nemo-gym 0.5.0**: the registry resolves this provider through the entry point
(`create_provider({"substrate": {...}})`) and the full unit suite passes against the real
`nemo_gym.sandbox.providers.base` types.

## How it maps onto ate-env

One gRPC endpoint, `ate-env-api`, carries everything (see the
[client README](../../clients/python/README.md#how-it-works)):

| Provider call | `ate_env_rl` call | Where it lands |
|---|---|---|
| `create(spec)` | `Client.create(id, template_name=…, template_atespace=…, rootfs_image=…)`, then `Env.shell("true")` until it answers, then `Env.write_file` for `spec.files` | `EnvironmentService` → Substrate control plane; the probe and the files go to the guest |
| `exec(handle, cmd, cwd=, env=, timeout_s=, user=)` | `Env.start_process(["sh","-c",cmd], cwd, env, timeout=)` + `Process.output(follow=True)`; with `user=`, `cmd` goes through `sudo -n -u <user>` or `su` (see Resource mapping) | `ProcessService` in `ate-env-guest`, proxied through the atenet router |
| `upload_file` / `download_file` | `Env.write_file` / `Env.read_file_bytes` | `FileSystemService` in the guest |
| `status(handle)` | `Client.get(id)` → lifecycle status | `EnvironmentService`; does **not** wake a parked sandbox |
| `close(handle)` | `Client.delete(id)`, NOT_FOUND-safe | `EnvironmentService` |
| `endpoint(handle, port)` (nemo-gym 0.7) | `Client.get(id)` for existence, then a static `SandboxEndpoint` | ate-env-api's port proxy: each request goes through the router to that port in the sandbox, where a runtime layer's daemon listens (see below) |

Environment ids are `gym-<10 hex>`; each handle remembers the atespace it was created in.

## Prerequisites

On the cluster:

1. **Agent Substrate installed** (`ate-system` namespace healthy — api-server, controller,
   atelet, atenet-router, valkey). `go run ./cmd/ate-setup deploy ate-system` in a checkout of
   the [substrate repo](https://github.com/agent-substrate/substrate) installs it; its Quickstart
   and `cmd/ate-setup/README.md` have the flags, and `--kind` targets a Kind cluster. The guest data
   plane is gRPC through the router, which needs a substrate at or after
   [substrate#1183](https://github.com/agent-substrate/substrate/pull/1183) (h2 on the ingress,
   protocol mirrored to actors). Without it every exec and file call fails with
   `server closed the stream without sending trailers`.
2. **The `ate-env` system deployed** from this repo at or after the `ateenv.v1alpha` proto
   move: namespace, WorkerPool, ActorTemplate(s), and the `ate-env-api` service (see the
   [root README](../../README.md#deployment-guide)).

   ```bash
   ate-env manifest \
     --api-image    <ate-env-api image> \
     --worker-image <ateworker-gvisor image matching your substrate build> | kubectl apply -f -
   kubectl get pods -n ate-env   # api + warm workers Running
   # once ate-env-api is Running (it creates the ate-env atespace templates live in):
   ate-env manifest template \
     --guest-image  <ate-env-guest image> | kubectl-ate create actor-template -f -
   ```

   If your `ateapi` requires authentication (it does on any standard install), the
   `ate-env-api` Deployment needs a projected ServiceAccount token with audience
   `api.ate-system.svc` and the `-ateapi-token-file` flag pointing at it.
3. **A base ActorTemplate** in the `ate-env` atespace: `default-template`, which the root
   README's quickstart registers. That is all a task image needs: `SandboxSpec.image` goes to
   `ate-env-api` as `rootfs_image`, and the API derives a template from the base on first use
   (the guest mounted into the image as a read-only volume) and reuses it afterwards. Register
   extra templates only for images that need their own resources or workspace, and map them
   under `create.image_templates`.

   > **Every template must run `ate-env-guest` inside the actor.** Readiness, exec, file
   > transfer, even `create()` completing — all of it goes through the guest's gRPC services
   > behind the router. Derived templates get the guest automatically; a hand-written template
   > without it fails *completely*: the actor starts, but every guest call fails and `create()`
   > times out with `SandboxCreateVerificationError`. Images must be digest-pinned
   > (`repo@sha256:…`); the API rejects tags.
4. **Network path from wherever Gym's rollout workers run** to `ate-env-api`: in-cluster DNS
   (`ate-env-api.ate-env:7777`) or, for a workstation, a port-forward:

   ```bash
   kubectl port-forward -n ate-env svc/ate-env-api 7777:7777
   ```

Locally:

5. **Python ≥ 3.10** and the `ate-env-rl` package from this repo's `clients/python` (not on
   PyPI yet). `nemo-gym` itself is only needed on the machine running the Gym resource servers —
   this package's tests run without it (see Testing).

## Task images

Three ways to run a task image, from least to most work:

**Pass the image (default).** Set `SandboxSpec.image` to a digest-pinned reference. `create()`
sends it as `rootfs_image` on the base template; the API derives `<base>-<12 hex of the digest>`
once, bakes its golden snapshot, and later creates on the same image restore from it. Nothing to
register, nothing to rebuild. See [`docs/rootfs-images/README.md`](../../docs/rootfs-images/README.md).

**Register a template per image** when an image needs its own resources, workspace or a second
runtime layer, then map it under `create.image_templates`:

```bash
ate-env manifest template --template gym-py312 \
  --rootfs-image   docker.io/library/python@sha256:<digest> \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> \
  --workspace      /workspace | kubectl-ate create actor-template -f -
```

**Bake the guest into the image** (legacy, for control planes without image volumes):
`hack/bake-task-image.sh` builds the guest as a layer on top of any base image with `ko` and
prints the digest-pinned ref; the template's container then runs `/ko-app/ate-env-guest` directly.

Digests for public images: `crane digest --platform linux/amd64 python:3.12-slim`. The task
image needs a `sh` for `exec()`. `timeout_s` no longer needs a `timeout` binary: the guest
enforces the deadline itself.

## Installation

```bash
# from a checkout of this repo:
pip install ./clients/python                       # ate-env-rl (import name ate_env_rl)
pip install ./integrations/nemo-gym                # nemo-gym-substrate
# with NeMo Gym in the same environment:
pip install './integrations/nemo-gym[gym]'
```

The `gym` extra resolves only on Python ≥ 3.13.14, the floor the current `nemo-gym` release on PyPI
(0.7.0) declares. The provider, its tests and CI run on Python ≥ 3.10 without it: structural mirrors
of the `nemo_gym` base types stand in.

The entry point makes the provider discoverable immediately:

```python
from nemo_gym.sandbox.providers.registry import create_provider
provider = create_provider({"substrate": {"connection": {"api_url": "127.0.0.1:7777"}}})
```

## How to use

### From NeMo Gym config (the normal path)

Add the `sandbox:` block to your resource-server config and select the `substrate` provider —
nothing else in the Gym setup changes:

```yaml
sandbox:
  default_metadata:
    sandbox-api: substrate
  substrate:
    connection:
      api_url: ate-env-api.ate-env:7777    # host:port or http(s):// URL; port-forward for dev
      request_timeout_s: 30                # deadline per lifecycle RPC (create/get/delete)
      # auth_token_file: /var/run/secrets/ate-env/token   # ate-env-api bearer token; see below
    create:
      template: default-template           # ActorTemplate when the spec names none
      atespace: ate-env                    # atespace the templates live in and envs are created in
      ready_timeout_s: 120
      ready_poll_interval_s: 1.0
      suspend_policy: pause_after_exec     # never (default) | pause_after_exec | suspend_after_exec; see Parking
      idle_before_suspend_s: 1.0           # idle time after a call before the park
      image_templates:                     # optional: pin an image to a registered template; others are derived
        python:3.12-slim: gym-py312
```

`namespace` is still accepted as an alias for `atespace` (the name this provider used before
ate-env adopted substrate's vocabulary); setting both is an error. `api_url` is plain gRPC
(h2c) like the Go client and CLI; an `https://` URL opens a TLS channel instead.

When ate-env-api requires a bearer token (`-auth-token-file` on the API), point
`connection.auth_token_file` at a file that holds it, or set `SUBSTRATE_ENV_AUTH_TOKEN`, the
variable the `ate-env` CLI also reads; the file wins when both are set. The provider sends the
token on its gRPC calls and in the `X-Env-Authorization` header of each `endpoint()`. Without
either, it sends no token. An empty `auth_token_file` is a configuration error.

Then run your resource server / `gym env start` as usual. Sandboxes created by Gym's resource
lifecycle now appear as Substrate actors; `kubectl get pods -n ate-env` shows the warm workers
hosting them.

### Directly from Python (debugging, scripts)

```python
import asyncio
from nemo_gym_substrate import SubstrateSandboxProvider
from nemo_gym.sandbox.providers.base import SandboxSpec   # or nemo_gym_substrate._compat

async def main():
    provider = SubstrateSandboxProvider({"connection": {"api_url": "127.0.0.1:7777"}})
    handle = await provider.create(SandboxSpec(
        files={"/task/hello.sh": "echo hello from substrate"},
        workdir="/task",
        env={"EPISODE": "1"},
    ))
    result = await provider.exec(handle, "sh /task/hello.sh && echo episode=$EPISODE")
    print(result.stdout, result.return_code)
    await provider.close(handle)
    await provider.aclose()

asyncio.run(main())
```

To share one `ate_env_rl.Client` between the provider and other code, pass it in:
`SubstrateSandboxProvider(config, client=client)`. The provider then never closes it.

### Per-sandbox `provider_options` (on `SandboxSpec`)

| Option | Meaning |
|---|---|
| `template` | ActorTemplate for this sandbox (overrides `create.template` and `image_templates`) |
| `atespace` | Atespace of that template, and of the environment (`namespace` accepted as alias) |

Unknown keys are rejected at `create()` time, as are unknown keys anywhere in the config block.

### Parking: an idle sandbox holds no worker

A rollout spends most of its wall-clock waiting for the policy model, and a running sandbox holds
its worker slot the whole time. With `create.suspend_policy` set, the provider parks a sandbox once
it has been idle for `idle_before_suspend_s` after `create()` and after each `exec()`, upload and
download; a parked sandbox holds no worker, so a pool carries more sandboxes than it has slots.
Nothing resumes it explicitly: the next guest call goes through ate-env-api, which wakes the
sandbox (its `--wake-budget`, two minutes by default) and then runs the command. `create()` returns a
parked sandbox, so a new one holds no worker through the policy's first think time and its first
command is a wake like any later one.

| `suspend_policy` | Park | Snapshot | Resumes |
|---|---|---|---|
| `never` (default) | none | | |
| `pause_after_exec` | `SuspendMode.LOCAL` | on the node; uploads in the background | on that node until the upload lands; the cheapest park-and-resume cycle |
| `suspend_after_exec` | `SuspendMode.DURABLE` | in object storage before the park counts as done | on any node; survives the loss of the node |

Rules the provider keeps:

- A sandbox is never parked while a request to it is in flight.
- A park that has not reached the API yet is dropped by a new request, so back-to-back commands
  (an eval's test runs) pay no checkpoint and no restore between them. One that has reached the
  API is left to finish, and the new request waits for it, because a sandbox that is still
  `SUSPENDING` cannot be woken.
- A park runs in its own task; a cancelled caller never cancels the suspend. `aclose()` waits for
  every park in flight before it closes the client.
- A failed park costs density, not correctness: the sandbox stays running and the next command
  works. A park still mid-transition after 30 s is left to the control plane with a warning, and
  the next idle period retries.
- `pause_after_exec` needs an ate-env-api that serves `suspend_mode` (`ate-env server-info`). An
  older one refuses the local mode; the provider then logs a warning once and parks durably for
  the rest of its life.
- A sandbox that declares `SandboxSpec.ports` is skipped by automatic idle parking, because HTTP
  requests sent through `endpoint()` go straight through the port proxy without the provider's
  in-flight tracking and would race with an idle suspend. Call `pause()` and `resume()` explicitly
  between turns when using `endpoint()`.
- A sandbox shared with another process (`serialize_handle()` here, `connect()` there) is skipped
  by automatic idle parking in both processes, for the same reason: the other process's calls do
  not pass through this provider's in-flight tracking. `pause()` still parks it.

### Sharing a sandbox with another process

The provider implements nemo-gym's `ConnectableProvider`, thus `AsyncSandbox.serialize()` and
`AsyncSandbox.connect()` work. Gym's Harbor environment server uses them to hand each episode's
sandbox to the agent. The descriptor holds the environment id, its atespace, the working
directory and the declared ports. Any process with the same `connection` settings can connect.
The descriptor leaves out `SandboxSpec.env`, which can hold secrets: a borrower passes its own
`env` on each exec. `connect()` refuses an environment that does not exist, is `CRASHED` or is
being deleted. A parked one is fine, because the next guest call wakes it. Close the sandbox in
the process that created it; a borrower disconnects.

`status()` reports a parked sandbox as `PAUSED` (nemo-gym 0.7 and later; `RUNNING` before that),
without waking it. The provider also implements nemo-gym's `SupportsSandboxPauseResume`:
`pause(handle)` waits for requests in flight to finish, parks the sandbox with the policy's mode
(`LOCAL` under `never`) and returns once the control plane reports it parked; `resume(handle)`
sends one guest probe, which ate-env-api answers once the sandbox runs again. A training loop can
park a sandbox between turns that way without waiting for the idle timer.

When parking pays: the saving is the think time between commands, the cost is one checkpoint and
one restore per idle period. With as many slots as sandboxes, `never` wins; with more sandboxes
than slots and think times of seconds or more, parking lets the pool drain. A paused sandbox is
pinned to its node until its upload lands, so a drain or a pool rollout before then loses it; use
`suspend_after_exec`, or suspend the fleet durably, before either.

### Services inside the sandbox: the NeMo runtime layer

Some NeMo components talk to a service inside the sandbox instead of running commands through
`exec`: NeMo Skills' code-execution sandbox server (what `ns_tools` and the math recipes call for
`ipython` and `python` tool calls), NeMo Gym's own exec server (what the ECS Fargate provider
injects), a tool daemon. The task images do not contain them, and rebuilding hundreds of thousands
of SWE-bench images to add them is not an option. ate-env attaches such a daemon at runtime the
same way it attaches its own guest: as a read-only OCI image volume, a *runtime layer*, that the
guest starts and supervises next to itself
([docs/rootfs-images/RUNTIMES.md](../../docs/rootfs-images/RUNTIMES.md)). The provider implements
nemo-gym 0.7's `SupportsSandboxEndpoint`, so Gym reaches the daemon from outside the cluster's
actor network.

[`layer/`](layer/) is that layer for NeMo: one OCI image, built from scratch, that holds a
relocatable CPython 3.12 ([python-build-standalone](https://github.com/astral-sh/python-build-standalone))
with hash-pinned wheels and two servers, each started with the layer's own interpreter so the task
image contributes nothing but its kernel and filesystem:

| server | command (as a guest sidecar) | protocol | from |
|---|---|---|---|
| NeMo Skills local sandbox server | `/opt/nemo/python/bin/python3 -m nemo_runtime.sandbox_server --host 0.0.0.0 --port 6000` | `POST /execute` (`generated_code`, `language` = `ipython`/`python`/`shell`/`lean4`, `timeout`, `std_input`), IPython sessions by `X-Session-ID`, `GET /health` | `nemo_skills/code_execution/local_sandbox/local_sandbox_server.py`, fetched when the layer is built, pinned by commit and sha256 ([provenance](layer/third_party/nemo-skills/README.md)) |
| NeMo Gym exec server | `/opt/nemo/python/bin/python3 -m nemo_runtime.exec_server --host 0.0.0.0 --port 19542` | `POST /exec` (`cmd`, `timeout`), `POST /upload`, `GET /download?path=`, `GET /health` | the `EXEC_SERVER_SCRIPT` of `nemo_gym/sandbox/providers/ecs_fargate/engine.py`, vendored verbatim ([provenance](layer/third_party/nemo-gym/README.md)) |

Both execute whatever they are sent, with the task image's user and filesystem, and neither
authenticates its caller; see the trust note under Limits. What runs the code:

- `ipython`: the layer's interpreter and its packages (IPython, psutil, Flask and their
  dependencies, nothing else). Each session is a child process of the server.
- `python`: the first `python` on `PATH`. The server appends the layer's `bin` to `PATH`, thus
  a task image that has a `python` runs the code with its own interpreter and packages, and an
  image without one (or with only `python3`) gets the layer's CPython 3.12.
- `shell` on the sandbox server: `/bin/bash`, which the task image must contain (Debian and
  Ubuntu based images do; busybox and distroless images do not).
- `POST /exec` on the exec server: `bash` when it is on `PATH`, else `/bin/sh`.

Build and push it once per registry (linux/amd64 and linux/arm64; `layer/lock.sh` refreshes the
hash-pinned requirements):

```bash
docker buildx build --platform linux/amd64,linux/arm64 --provenance=false --sbom=false \
  --push -t <registry>/nemo-runtime-layer:<tag> integrations/nemo-gym/layer
crane digest <registry>/nemo-runtime-layer:<tag>   # templates take the digest, not the tag
```

1. **Put the daemon on the base template, once.** Every environment created from this base with a
   task image (`rootfs_image`) gets the daemon; the task images stay unmodified.
   [`hack/runtime-layer-demo.sh`](hack/runtime-layer-demo.sh) does this and the steps below.

   ```bash
   ate-env manifest template --template gym-nemo \
     --rootfs-image   docker.io/library/python@sha256:<digest> \
     --layer          nemo=<registry>/nemo-runtime-layer@sha256:<digest>=/opt/nemo \
     --sidecar        '/opt/nemo/python/bin/python3 -m nemo_runtime.sandbox_server --host 0.0.0.0 --port 6000' \
     --sidecar-readyz http://127.0.0.1:6000/health | kubectl-ate create actor-template -f -
   ```

   Add a second `--sidecar`/`--sidecar-readyz` pair for the exec server when a recipe needs it.
   The actor reports ready only once every sidecar answers its readiness URL, so `create()`
   returns a sandbox whose daemon is up. Give the base a task image of its own
   (`--rootfs-image`): the guest's image is Alpine-based and cannot run the layer's glibc
   interpreter, so a base without one never bakes its golden snapshot and shows `ERROR` in
   `kubectl-ate get actor-template`, although the templates derived from it for real task images
   work. Point `create.template` (not a per-sandbox `provider_options` template, which selects the
   base itself) at it, so each task image derives from the base and inherits the layer.

2. **Select the base and declare the port.**

   ```yaml
   sandbox:
     substrate:
       connection:
         api_url: ate-env-api.ate-env:7777
         # endpoint_url: http://ate-env-api.ate-env:7777   # default: api_url
       create:
         template: gym-nemo
   ```

   ```python
   handle = await provider.create(SandboxSpec(image=task_image, ports=(6000,)))
   ```

3. **Resolve the endpoint and call the daemon.**

   ```python
   ep = await provider.endpoint(handle, 6000)
   async with httpx.AsyncClient(base_url=ep.endpoint, headers=ep.headers) as http:
       r = await http.post("/execute", json={"generated_code": "print(1)", "language": "python"})
   ```

   `ep.endpoint` is ate-env-api, and `ep.headers` (`X-Env-Id`, `X-Env-Atespace`, `X-Env-Port`, and
   `X-Env-Authorization` when the provider has a token) route each request through the Substrate
   router to port 6000 in this sandbox. `endpoint()` does not touch the guest: the first request
   through the proxy resumes a parked sandbox, and the Substrate router holds that request up to
   about 10 s while the sandbox wakes. A longer wake, usually only the first one on a new image,
   gets 503 with `Retry-After` and `X-Env-State`, and the caller retries until the sandbox is up.
   It checks only that the sandbox exists, and raises `RuntimeError` at once for one that is
   deleted or crashed. A port that `SandboxSpec.ports` did not declare is a `ValueError`,
   as in Gym's Docker provider; `create()` rejects a spec whose ports are outside 1-65535 or name
   the guest's port 80. Set `connection.endpoint_url` when the code that calls the daemon reaches
   ate-env-api at another address than the provider does.

Verified 2026-10-21 on a GKE cluster (gVisor pool, Substrate `ddb46651`, ate-env-api with the port
proxy) with `hack/runtime-layer-demo.sh` and `tests/test_e2e.py -k runtime_layer`: environments from
unmodified `python:3.12-slim` and `debian:bookworm-slim` (no Python inside) were serving 4 to 5 s
after `create`, with readiness gated on both sidecars' `/health`; `POST /execute` answered from
inside the environment, through the header form and through the path form, an IPython session kept
its state across two calls, `language: python` ran on the layer's interpreter where the image had
none, and the exec server answered `POST /exec`. The layer image is 87 MB compressed. That cluster
ran Substrate `ddb46651`, before the v0.6.0 bump on `main` (#110, #111); a re-run on v0.6.0 is
pending, because the port proxy now dials the router over TLS.

Limits:

- **HTTP/1.1 only.** The router tunnels HTTP to the port, not raw TCP. Streamed responses are
  flushed as they arrive and WebSocket upgrades pass through; gRPC to the daemon is not supported
  yet.
- **The daemon must be self-contained.** A layer cannot rely on the task image's libraries: ship a
  static binary, or put the interpreter and its packages in the layer, as `layer/` does for the two
  NeMo servers. The layer's CPython is a glibc build and its wheels are locked for
  `manylinux_2_31` (`layer/lock.sh`), so a task image needs glibc 2.31 or newer: Ubuntu 20.04,
  Debian 11 and newer, and not Alpine or another musl image. A re-lock at `manylinux_2_17`
  would lower the floor for older images; it needs a layer rebuild.
- **The daemon must listen on `0.0.0.0`.** The router reaches the port on the actor's address; a
  daemon bound to `127.0.0.1` only answers processes inside the sandbox, and requests through the
  endpoint fail with 502.
- **The API token is the only check in front of the daemon.** Both NeMo servers run what they are
  sent and check no credential of their own: NeMo Skills' `LocalSandbox` client and Gym's exec
  client cannot send one, and the upstream servers stay unchanged. The sandbox is the trust
  boundary, as with NeMo Skills' own sandbox container and the ECS provider's exec server. With
  `-auth-token-file` on ate-env-api (`ate-env manifest --auth-token-secret`), each `endpoint()`
  request must carry the token in `X-Env-Authorization`; the provider sends it when it has a token
  (see `connection.auth_token_file` above), and the proxy removes it before the daemon sees the
  request. Without the token, anyone who can reach ate-env-api can call any port in any sandbox,
  thus a shared deployment must turn it on. Either way, keep ate-env-api inside the cluster.
- Port 80 belongs to the guest. The daemon shares the task image's user, environment and
  filesystem.
- Each distinct task image yields one derived ActorTemplate on its base, created on first use and
  reused after; the daemon adds no templates.

## Testing

```bash
cd integrations/nemo-gym
python3 -m venv .venv
.venv/bin/pip install ../../clients/python -e '.[test]'

# Unit/contract tests — hermetic: an in-process fake ate-env-api (tests/fakes.py)
# behind the real ate_env_rl client; no cluster, no nemo-gym needed (base types fall
# back to structural mirrors in _compat.py; with nemo-gym installed the same
# tests run against the real types):
.venv/bin/python -m pytest tests/test_provider.py -q      # or: make nemo-gym-test (repo root)

# End-to-end against a live cluster (full lifecycle on a real actor):
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777 &
SUBSTRATE_E2E_API_URL=127.0.0.1:7777 .venv/bin/python -m pytest tests/test_e2e.py -q
# SUBSTRATE_E2E_TEMPLATE=<name> picks the ActorTemplate (default: default-template)

# The runtime-layer test needs a base template with the NeMo runtime layer (register one
# with hack/runtime-layer-demo.sh); it creates a sandbox on it, resolves endpoint() and
# calls NeMo Skills' sandbox server through ate-env-api's port proxy:
SUBSTRATE_E2E_API_URL=127.0.0.1:7777 SUBSTRATE_E2E_RUNTIME_TEMPLATE=gym-nemo \
  SUBSTRATE_E2E_RUNTIME_IMAGE=docker.io/library/python@sha256:<digest> \
  .venv/bin/python -m pytest tests/test_e2e.py -q -k runtime_layer
```

## Benchmark

`benchmarks/rollout_bench.py` shapes load like a NeMo-RL rollout batch: N parallel rollouts,
each *create → seed task file → T agent turns (exec + mocked model think time) → close*, on real
actors:

```bash
python benchmarks/rollout_bench.py --api-url 127.0.0.1:7777 --rollouts 5 --turns 3 --think-s 2
```

`benchmarks/driver_sim.py` is the start-time / time-to-first-exec probe over a list of templates
(one task per template per rollout), the same shape as the SWE-500 campaign's eval arm.

First numbers on a small test cluster (5 warm workers, gVisor, cold creates; measured with the
earlier HTTP guest proxy, 2026-09-03 — rerun on the gRPC path before quoting):

| Phase | p50 | max | Note |
|---|---|---|---|
| create | 4.95 s | 18.1 s | cold resume; max shows 5-way contention for 5 workers |
| exec (turn) | 0.14 s | 0.15 s | router + guest round trip |
| close | 3.06 s | 4.0 s | delete path |

Turn overhead is already negligible against model think time; create/close dominate — which is
exactly what golden-snapshot starts and fork-from-golden creates are for.

## Resource mapping and isolation

- One Gym sandbox = one Substrate **actor**: a gVisor (or micro-VM) sandbox multiplexed onto a
  warm WorkerPool worker. Isolation is the sandbox class; actors share worker nodes.
- `SandboxSpec.image` maps to a registered ActorTemplate via `image_templates`, or is sent as
  `rootfs_image` and served from a derived template. The first create on an image pulls it and
  bakes a golden snapshot; later creates restore from it.
- `spec.files` are seeded through the guest filesystem service after readiness. `spec.env` /
  `spec.workdir` are applied per exec (`StartProcess` takes `env` and `cwd` natively).
- `exec()` returns a result with `return_code=125` and an `error_type` when the sandbox did not
  run the command to completion; a nonzero exit of the command itself is a plain result with no
  `error_type`. That is the contract Gym's own providers follow, and what `mini_swe_agent_2` checks
  to end a rollout whose sandbox is gone instead of stepping on against it. `error_type="timeout"`:
  the guest killed the process group with SIGKILL at `timeout_s` (the output it produced is kept,
  and `substrate provider: exec timed out after …` follows it on stderr), or the guest did not
  answer within `timeout_s` plus a few seconds and the client killed the process.
  `error_type="sandbox"`: the environment is gone (`NOT_FOUND`) or the call could not reach the
  guest (`ate_env_rl.EnvError`, named on stderr). `upload_file()` and `download_file()` still raise.
- `exec(user=…)` runs the command through `sudo -n -u <user>` when the image's user has a
  passwordless sudo (`sudo -n true` succeeds), or through `su` when the image runs as root; the
  guest itself runs commands as the image's user, which the provider probes once per sandbox. A
  non-root image therefore needs a `NOPASSWD` sudoers entry for its user, because Gym's transfer
  helpers run as `user="root"`. A user that is the image's own runs as is. An image with neither
  tool, one whose sudo wants a password, and a uid under `su` (which takes a name only) return
  return code 125 with `error_type="user_switch_unavailable"`, which Gym's transfer helpers
  answer by running as the default user.
- `status()` reads the environment's lifecycle state from `ate-env-api` without touching the
  guest, so polling it never wakes a parked sandbox. A **suspended or paused actor reports
  `PAUSED`**: it is alive and one call away, since the router resumes it on the next command (a
  nemo-gym before 0.7 has no `PAUSED`, and gets `RUNNING`). `RESUMING` → `STARTING`, `CRASHED` →
  `ERROR`, deleted or `DELETING` → `STOPPED`.

Known limits (tracked in the RL-on-Substrate proposal):

- `spec.ttl_s` is **not enforced** — substrate has no server-side TTL yet; clean up via
  `close()`. A crashed harness leaks actors until deleted.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `SandboxCreateError: … rootfs_image … pinned by digest` (`InvalidArgument`) | `SandboxSpec.image` is a tag. Use `repo@sha256:<digest>` (`crane digest --platform linux/amd64 <tag>`), or map the image to a registered template under `create.image_templates` |
| `SandboxCreateError: … UNAVAILABLE` at once | `api_url` unreachable — port-forward died, or wrong kubectl context (`kubectl config current-context`) |
| Create times out (`SandboxCreateVerificationError: … UNAVAILABLE` / `INTERNAL`) | Check warm workers exist (`kubectl get pods -n ate-env`) and the template is Ready (`kubectl get actortemplate -n ate-env`). More parallel creates than warm workers will queue. If the message says `server closed the stream without sending trailers`, the router is not carrying gRPC to actors (see Prerequisites #1) |
| Create times out and the actor is `RUNNING` | The template's image does not run `ate-env-guest` — the actor is up but nothing serves the guest API. See "Task images" |
| Every exec returns `return_code=125`, `error_type="sandbox"` and `does not exist` on stderr | The environment is gone (deleted by another party, or the `ate-env-api` restarted with a different atespace default). `status()` returns `STOPPED`; create a new sandbox |
| Every exec returns `UNAVAILABLE` with `CERTIFICATE_EXPIRED` | The atenet router's pod certificate expired and wasn't hot-reloaded (seen on routers running > ~1 day): `kubectl -n ate-system rollout restart deploy/atenet-router` |
| Actor stuck `RESUMING` forever (`status()` stays `STARTING`) | Restore landed on a CPU-incompatible node (mixed Intel/AMD pools) or raced a router restart. Delete the env and recreate; long-term fix is CPU-aware placement |

# OpenHands on ate-env: `ate-env-openhands`

Runs the [OpenHands](https://github.com/OpenHands/software-agent-sdk) agent-server inside any
unmodified task image (glibc 2.31 or newer) and connects the OpenHands SDK to it.
`AteEnvWorkspace` is an OpenHands `RemoteWorkspace` that mirrors OpenHands' own
`AgentSandboxWorkspace` (`openhands-workspace[agent-sandbox]`, kubernetes-sigs/agent-sandbox):
code written against that workspace moves to Agent Substrate by changing the constructor.
A program written against agent-sandbox-rl's fleet moves by changing its imports (section 8).

```python
from ate_env_openhands import AteEnvWorkspace

with AteEnvWorkspace(
    template="oh-agent-server",                      # base template with the agent-server layer
    image="docker.io/swebench/sweb.eval.x86_64.django_1776_django-11099@sha256:…",
    api_key=SESSION_KEY,                             # the template's OH_SESSION_API_KEYS_0
    working_dir="/testbed",
) as workspace:
    print(workspace.execute_command("git status").stdout)
    conversation = Conversation(agent=agent, workspace=workspace)   # RemoteConversation
```

How it fits together:

- The agent-server comes from a runtime layer mounted read-only at `/opt/oh`
  ([docs/rootfs-images/RUNTIMES.md](../../docs/rootfs-images/RUNTIMES.md)). The task image is not
  rebuilt; agent-sandbox, by contrast, runs the stock agent-server image.
- The guest starts the agent-server as a sidecar and holds the actor's readiness until its
  `/health` answers.
- The workspace reaches it through ate-env-api's port proxy, at
  `/v1alpha/atespaces/<atespace>/envs/<env>/ports/<port>/...`, with the agent-server's own session
  key, and with `api_token` in `X-Env-Authorization` when ate-env-api requires its bearer token.
  The SDK uses that URL as its workspace host, so commands, files and `RemoteConversation`
  (the agent loop and its event WebSocket) work unchanged. With `api_token`, the event WebSocket
  carries a port capability instead of the token, because the SDK sends no headers on it.
  No `kubectl port-forward`.

| path | what it is |
|---|---|
| [`src/ate_env_openhands/`](src/ate_env_openhands) | `AteEnvWorkspace`, `port_url`, the fleet and R2E-Gym adapters, `agent_sandbox_rl` |
| [`layer/`](layer) | the runtime layer: Dockerfile, hash-locked dependencies, `lock.sh` |
| [`../../examples/openhands`](../../examples/openhands) | the examples: OpenHands' agent-sandbox example 17, CodeAct, trajectory execution, a plain-HTTP demo, a fleet rollout |
| [`tests/`](tests) | unit and integration tests: a fake ate-env-api and port proxy in front of a real agent-server |

## 1. Install

```bash
pip install ./clients/python ./integrations/openhands            # the workspace
pip install './integrations/openhands[examples]'                 # + openhands-tools, for the agent examples
```

`ate-env-rl` (this repo's Python client) is not on PyPI yet, thus install it from the checkout.
The package pins `openhands-sdk` to the agent-server version in the layer (see
[Versions](#versions)). Python 3.12 or newer, as openhands-sdk requires.

## 2. Build the layer

Each release of this repo publishes the layer as
`ghcr.io/ai-on-gke/sandboxenv-rl/oh-agent-server-layer`, multi-arch and signed, tagged
`<agent-server version>-<release tag>`. The digest is in the release notes and in the release's
`images_<tag>.txt`; [docs/release.md](../../docs/release.md#verify-a-signature) shows how to verify it.
Use that digest in section 3. Build the layer yourself for another registry or another agent-server
version:

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t <registry>/oh-agent-server-layer:1.55.0 --push integrations/openhands/layer
crane digest --platform linux/amd64 <registry>/oh-agent-server-layer:1.55.0
```

The layer does not reuse the binary in the published
`ghcr.io/openhands/agent-server` images: that is a PyInstaller build made on Debian bookworm, it needs
glibc 2.36, and on older distributions it fails at start, for example on Ubuntu 22.04 (glibc 2.35):

```
ImportError: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.36' not found (required by /tmp/_MEI…/libstdc++.so.6)
```

Starting it through its own image's loader does not help either, because PyInstaller re-executes
itself. The layer is [python-build-standalone](https://github.com/astral-sh/python-build-standalone)
CPython (built for glibc 2.17, relocatable) with `openhands-agent-server` and its dependencies
installed by uv for the target platform `manylinux_2_31`, so every wheel runs on glibc 2.31 and newer.
From the image it is mounted into it uses the kernel, the C library and `/bin/sh`. The C library
must be glibc 2.31 or newer: Debian 11, Ubuntu 20.04, RHEL 9 and later work; Alpine (musl), Ubuntu
18.04 (glibc 2.27) and CentOS 7 (glibc 2.17) do not. The layer's bytecode is precompiled
(`unchecked-hash`), because Python cannot write a cache on the read-only mount; the agent-server
answers `/health` in about 3.5 s instead of 6.5 s (arm64, local Docker). The layer is about 250 MB
compressed and 900 MB on disk.

For benchmarks: the layer goes through atelet's image cache like any image. A node pulls it once and
shares it among all task images on the base. The cache evicts images that no live actor mounts, thus on
a run that churns many task images the layer can be evicted and pulled again on the same node. A slow
first command then comes from the layer pull, not from the task image.

The dependency set is hash-locked: [`layer/requirements.txt`](layer/requirements.txt) (the agent-server and
everything it pulls in, the same for linux/amd64 and linux/arm64) and
[`layer/build-constraints.txt`](layer/build-constraints.txt) (setuptools, to build the one source-only
dependency, func-timeout). Base images are pinned by digest in the layer's Dockerfile. To move to another
agent-server release, change the versions in [`layer/requirements.in`](layer/requirements.in) (the agent-server,
`openhands-sdk` and `openhands-tools`, which the agent-server does not pin itself), run
[`layer/lock.sh`](layer/lock.sh) (needs uv; it resolves for both architectures and fails if they differ),
and build with `--build-arg AGENT_SERVER_VERSION=<version>`; the build checks that the installed
agent-server, sdk and tools are that version. Change the client's `openhands-sdk` to the same version (see
[Versions](#versions)).
`lock.sh` starts from the committed lock, thus it keeps each pin that still meets the `.in` files and
moves only what they now require (for a security fix, a lower bound in `requirements.in`). Its
arguments go to `uv pip compile`: `./lock.sh --upgrade-package <name>` moves one package, and
`./lock.sh --upgrade` moves all of them to their latest releases.

## 3. Register the base template

This needs ate-env installed as in the [root README](../../README.md#get-started): Substrate v0.6.0
or later, and ate-env-api's SPIFFE ID (`spiffe://cluster.local/ns/<namespace>/sa/ate-env-api`, namespace
`ate-env` by default) in the router's ingress allow-list. The template below uses the default limits of
`ate-env manifest template` (2 CPUs, 4 GiB); pass `--cpu` and `--memory` to change them.

```bash
ate-env manifest template --template oh-agent-server \
  --rootfs-image   docker.io/library/debian@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8 \
  --guest-image    <registry>/ate-env-guest@sha256:<digest> \
  --layer          oh=<registry>/oh-agent-server-layer@sha256:<digest>=/opt/oh \
  --sidecar        '/opt/oh/python/bin/python3 -m openhands.agent_server --host 0.0.0.0 --port 8000' \
  --sidecar-readyz http://127.0.0.1:8000/health \
  --sidecar-env    OH_SESSION_API_KEYS_0=<key> | kubectl-ate create actor-template -f -
```

`--sidecar-env` gives the session key to the agent-server only. The template carries it as
`ATE_SIDECAR_ENV_OH_SESSION_API_KEYS_0`; the guest removes it from its own environment at start and
adds it, as `OH_SESSION_API_KEYS_0`, to the sidecar's environment only. Thus the commands that
ate-env-api starts in the environment (`ate-env exec`, the clients, the fleet's `handle.exec`, the
R2E-Gym adapter) and their children do not inherit it. A command that runs as the same user can
still read the key from the agent-server's `/proc/<pid>/environ` (see Limits).

`--sidecar-env` needs a `--guest-image` built with its support. The template also carries the guest
flag `-sidecar-env-required`. A guest older than this change does not know that flag and exits at
start, thus the actor crashes and the environment does not start. Without that guard, an older guest
would start the agent-server without `OH_SESSION_API_KEYS_0`, and the agent-server would then accept
each request without a key while `/health` still answers. A new guest also exits when the flag is set
and the container has no `ATE_SIDECAR_ENV_` variable.

This is the counterpart of agent-sandbox's SandboxTemplate and warm pool. Further agent-server
settings are more `--env` flags; each command in the environment sees those. The first environment
created from an image derives a template for it (layer, sidecar and settings included); later ones
reuse it.

The base template runs in a glibc image of its own (`--rootfs-image`), because its golden actor
starts the layer too. Without it, the container image is the guest image, which has no C library for
the layer's CPython, and the golden actor crashes. `debian:bookworm-slim` (glibc 2.36, `/bin/sh`
and `bash`, 26 MB compressed for amd64) is the smallest common image that meets the glibc 2.31
floor; any image that meets the floor works. An environment created without `image=` runs in this
base image. A template derived for a task image replaces only the image: the guest volume, the layer
and the sidecar stay. A base registered earlier without `--rootfs-image` keeps a crashed golden
actor: delete it and register it again with the same session key. If the old base already uses
`--sidecar-env`, its `<base>-<12 hex digits>` templates can stay, because a template derived from
the new base is identical to one derived from the old base. Delete them only to make Substrate bake
their golden snapshots again, or when the session key changes, because each derived template copies
the key. The cluster runs under [Verified](#verified) came before this base image and do not test
it.

A base template registered before `--sidecar-env` has the key in `--env`, thus each command in its
environments, and in the environments of each template derived from it, gets the key. Delete the
base and each `<base>-<12 hex digits>` template, then register the base again with `--sidecar-env`
as above. The derived templates must go too, because they copy the key in `--env` and creates reuse
them by name. This also holds for a base without `--rootfs-image` that has the key in `--env`.
`examples/openhands/demo.sh` prints a note for such a base, and `RECREATE=1` does both steps.

## 4. Use `AteEnvWorkspace`

`AteEnvWorkspace(template=..., image=...)` creates an environment, waits until the agent-server
answers `/health` through the proxy, checks the session key, creates `working_dir`, and deletes the
environment on exit. `AteEnvWorkspace(env_id=...)` attaches to an existing environment and leaves
it in place. Any failure after the create deletes the environment before the error reaches the
caller.

| Field | Default | Meaning | `AgentSandboxWorkspace` |
|---|---|---|---|
| `template` | required to create | base template with the agent-server layer | `warmpool` |
| `image` | the base template's image (`debian:bookworm-slim` in section 3) | task image, `repo@sha256:…` | none (fixed in the SandboxTemplate) |
| `atespace` | `ate-env` | atespace of the environment; the port proxy must list it in `--port-proxy-atespaces` | `namespace` |
| `template_atespace` | the server default | atespace of `template` | none |
| `api_url` | `127.0.0.1:7777` | ate-env-api, host:port or URL | `host` in `direct` mode |
| `proxy_url` | `api_url` | port proxy base URL, when it differs | none |
| `api_token` | none | ate-env-api's bearer token: on gRPC calls, and in `X-Env-Authorization` on proxy requests, which ate-env-api removes before it forwards them | none |
| `ws_capability_ttl` | `86400` | with `api_token`: seconds the port capability in `host` stays valid, at most `604500` (seven days less five minutes) | none |
| `env_id` | generated | attach to this environment instead of creating one | none |
| `server_port` | `8000` | agent-server port | `server_port` |
| `working_dir` | `/workspace` (created) | where commands run; `/testbed` on SWE-bench images | `working_dir` |
| `api_key` | none | the agent-server's session key | `api_key` |
| `create_timeout` | `600` | seconds for the create (a new image pulls and snapshots) | `sandbox_ready_timeout` |
| `health_check_timeout` | `600` | seconds for `/health`; the first use of a task image on a cluster bakes its golden snapshot and pulls the image (minutes for a large image), later uses take seconds | `health_check_timeout` |
| `delete_on_exit` | created: yes, attached: no | delete in `cleanup()` | always deletes the claim |

- `pause()` suspends the environment; its filesystem stays in Substrate's snapshot.
  `resume()` waits for `/health`, which also wakes it; any request wakes it as well. The address
  does not change, unlike a port-forward; with `api_token`, `resume()` puts a new port capability
  in `host`. To park an environment between agent turns, close the
  conversation first and reattach it after `resume()`; the agent-server keeps the conversation:

  ```python
  conversation_id = conversation.state.id
  conversation.close()  # stops the event WebSocket
  ws.pause()
  ...
  ws.resume()
  conversation = Conversation(agent=agent, workspace=ws, conversation_id=conversation_id)
  ```

  `pause()` and `resume()` keep the HTTP client open, thus a conversation that stays open across
  them can still send messages after `resume()`. The environment then does not stay suspended (see
  [Limits](#limits)).
- `port_url(port)` is the proxy URL of another port in the same environment. A request to it from
  outside the SDK adds `proxy_headers()` (empty without `api_token`).
- `get_secrets()` returns `LookupSecret` references that the agent-server answers itself: each names
  `http://127.0.0.1:<server_port>/api/settings/secrets/<name>` and carries the session key only.
  Thus a lookup never goes through the port proxy, and `api_token` never enters the sandbox. The
  agent-server resolves these URLs in process when it runs as `python -m openhands.agent_server`,
  as the layer starts it: that start sets `OH_INTERNAL_SERVER_URL`. Started another way, it falls
  back to an HTTP request to its own loopback address, authenticated by the session key, which can
  stall its own event loop. Secret names follow the agent-server's rule
  (`^[a-zA-Z][a-zA-Z0-9_]{0,63}$`).
- A rejected session key fails the constructor with `PermissionError`; a version difference between
  `openhands-sdk` and the agent-server is logged as a warning.
- The health wait stops at once on an answer that waiting cannot change: 401 or 403 from the proxy
  (`PermissionError`: a missing or wrong `api_token`, or a refused origin), 400 (`ValueError`: the
  proxy refused the port or the id), and 404 when the environment no longer exists. While it
  retries, it also reads the environment's state over gRPC, at most once every 5 s, and stops on
  CRASHED (`RuntimeError`: delete it and create a new one), DELETING, or a deleted environment.
  The Substrate router holds each request while the environment wakes, up to about 10 s; a longer
  wake gets the router's 504, or 503 with `X-Env-State` from an ate-env-api that has the port-proxy
  state check, and the wait retries it. Thus, after `pause()`, call `resume()` before other
  requests. A direct call while the wake takes longer than about 10 s, usually only the first wake
  of an environment on a new image, gets that error: `execute_command` returns `exit_code -1` with
  it in `stderr`, `file_upload` and `file_download` return `success=False`, and `RemoteConversation`
  calls raise `httpx.HTTPStatusError` (openhands-sdk 1.55.0).
- A 401 or 403 from the port proxy in the session-key check names `api_token`, not the session key,
  for example when the token changes after the health wait.
- Only the workspace that created or attached the environment deletes it. A failed constructor
  deletes only what it created, a `model_copy()` never deletes and opens its own HTTP client, and
  `__del__` does nothing during interpreter shutdown. A create that fails ambiguously (a timeout,
  a lost answer) deletes the environment by name and retries for 10 s, because the server can still
  be creating it. Get, suspend and delete have a 30 s deadline.
- `renew_ws_capability()` puts a new port capability, valid `ws_capability_ttl` from now, in
  `host`; without `api_token` it does nothing. Only a conversation opened or reattached after it
  uses the new capability (see [Limits](#limits)). `port_capability(token, atespace, env_id, port,
  expiry)` makes a capability for any port, for a WebSocket client outside the SDK.

From `AgentSandboxWorkspace`:

```diff
-from openhands.workspace import AgentSandboxWorkspace
+from ate_env_openhands import AteEnvWorkspace
-with AgentSandboxWorkspace(warmpool="openhands-pool", namespace="default",
-                           shutdown_after_seconds=30 * 60) as workspace:
+with AteEnvWorkspace(template="oh-agent-server", image=TASK_IMAGE,
+                     api_url="ate-env-api.ate-env:7777", api_key=SESSION_KEY) as workspace:
     agent = get_default_agent(llm=llm, cli_mode=True)
     conversation = Conversation(agent=agent, workspace=workspace)
```

Not mapped: `shutdown_after_seconds` (ate-env has no environment TTL), `labels`,
`pod_labels` and `pod_annotations` (environments have no labels). agent-sandbox-rl's fleet
adapters, its R2E-Gym adapter and its fleet API are in sections 6, 7 and 8.

## 5. Examples

The scripts that use the package are in [`examples/openhands`](../../examples/openhands), next to
the repository's other examples:

| file | what it is |
|---|---|
| [`convo_with_ate_env.py`](../../examples/openhands/convo_with_ate_env.py) | OpenHands' agent-sandbox example 17 with the workspace line changed |
| [`codeact_agent.py`](../../examples/openhands/codeact_agent.py) | OpenHands' CodeAct agent in an environment, with a model or `--scripted` |
| [`run_trajectory.py`](../../examples/openhands/run_trajectory.py) | executes a CodeAct trajectory from outside and records the observations |
| [`demo.sh`](../../examples/openhands/demo.sh) | registers the base template and drives the agent-server over plain HTTP |
| [`fleet_rollout.py`](../../examples/openhands/fleet_rollout.py) | an agent-sandbox-rl style rollout: `fleet.run` with `make_handle_workspace` |

## 6. Fleet adapters

`ate_env_openhands.fleet` has the two helpers of `agent_sandbox_rl.adapters.openhands`, with the
same names and ownership: the **fleet** (`ate_env_rl.SandboxFleet`) acquires and releases sandboxes;
the workspace only binds one and never creates or deletes an environment.

```python
from ate_env_rl import FleetConfig, SandboxFleet, Task
from ate_env_openhands import make_handle_workspace

fleet = SandboxFleet(FleetConfig(backend="substrate", endpoint="127.0.0.1:7777",
                                 template="oh-agent-server"))
fleet.load_tasks([Task(id="t1", image="docker.io/library/python@sha256:...")])
fleet.set_pool_replicas("docker.io/library/python@sha256:...", 4)   # idle sandboxes per image

def rollout(task, handle):
    workspace = make_handle_workspace(handle, api_key=SESSION_KEY, api_url="127.0.0.1:7777")
    try:
        return workspace.execute_command("python3 -m pytest -q", timeout=600).exit_code
    finally:
        workspace.cleanup()          # HTTP state only; fleet.run releases the sandbox

results = fleet.run(rollout)
```

- `make_handle_workspace(handle, **kw)`: an `AteEnvWorkspace` on `handle.sandbox_id`,
  `delete_on_exit=False`. `api_url`, `atespace` and `api_token` default from the handle (its data
  plane, its backend's atespace and the fleet's `auth_token`); `api_key`, `proxy_url`, the
  timeouts and an explicit `api_token` pass through.
- `make_fleet_workspace(fleet, task, **kw)`: acquires on construction and calls
  `fleet.release(handle)` from `cleanup()`, so `with make_fleet_workspace(...) as ws:` owns one
  episode; a failed start (health, session key) releases the handle before the error is raised.
- `template`, `template_atespace`, `image`, `env_id` and `delete_on_exit` are fleet-owned and
  raise `ValueError`, as agent-sandbox-rl's `_FLEET_OWNED_KWARGS` do.
- Pool sizing for a prewarm loop: `SandboxFleet.set_pool_replicas(image, n)` sizes one image's
  warm pool up or down without dropping it (idle, suspended environments; claimed ones are never
  touched), and `warm_image(image, replicas)` warms one image. Both exist on `AsyncSandboxFleet`
  too.

## 7. R2E-Gym adapter

`ate_env_openhands.r2egym` has the names of `agent_sandbox_rl.adapters.r2egym`, for ate-env:
R2E-Gym's `RepoEnv` on a sandbox the fleet acquired. No agent-server is involved: the runtime's
exec and file copy go through the handle.

```python
from ate_env_openhands.r2egym import fresh_eval_env, make_fleet_repo_env, r2egym_command_files

def rollout(task, handle):                        # task.metadata["ds"] is the dataset row
    env = make_fleet_repo_env(handle, command_files=r2egym_command_files())
    try:
        obs, reward, done, info = env.step(action)
        patch = env.runtime.get_patch()
    finally:
        env.close()                               # no-op on the sandbox; the fleet releases
    with fresh_eval_env(fleet, handle) as (_eval_handle, grader):   # a clean second sandbox
        grader.runtime.apply_patch(patch)
        return grader.compute_reward()
```

`AteEnvRuntime(DockerRuntime)` overrides the four `kubernetes-sandbox` hooks: bind the handle,
no-op teardown, `handle.exec` for `run` (with R2E-Gym's `(output, exit_code)` strings and
timeouts) and `handle.write_file` plus `tar` for `copy_to_container`. Commands run with
R2E-Gym's `PATH` (its testbed venv first) plus `docker_kwargs["environment"]` (a dict or a list
of `KEY=VALUE` strings), which can override `PATH`, as in R2E-Gym's kubernetes backends. Copied
files are owned by root. Everything else in the runtime (setup, patches, reward grading) runs
unchanged. R2E-Gym is not on PyPI: install it from
its checkout (`pip install -e path/to/R2E-Gym`); the module imports without it and raises a clear
`RuntimeError` on first use. R2E-Gym's runtime loads a kubeconfig for this backend; the adapter
stubs that during construction, so no Kubernetes access is needed.

## 8. agent-sandbox-rl programs: change the imports

`ate_env_openhands.agent_sandbox_rl` has agent-sandbox-rl's fleet API (`ClusterConfig`,
`FleetConfig`, `ResourceSpec`, `TemplateSpec`, `ObservabilityConfig`, `SandboxFleet`, `Task`, and
`adapters.openhands`) on top of `ate_env_rl`. A program written for agent-sandbox-rl, such as
agent-sandbox's `examples/agent-sandbox-rl/examples/run_openhands_fleet.py`, runs on ate-env when
its imports change and nothing else:

```diff
-from agent_sandbox_rl import (
+from ate_env_openhands.agent_sandbox_rl import (
     ClusterConfig, FleetConfig, ResourceSpec, SandboxFleet, Task, TemplateSpec,
 )
-from agent_sandbox_rl.adapters.openhands import make_handle_workspace
+from ate_env_openhands.agent_sandbox_rl.adapters.openhands import make_handle_workspace
```

The program's configuration has no ate-env address, token or base template. They come from the
environment:

| variable | what | default |
|---|---|---|
| `SUBSTRATE_ENV_API` | ate-env-api address (the `ate-env` CLI reads it too) | `127.0.0.1:7777` |
| `SUBSTRATE_ENV_AUTH_TOKEN` | ate-env-api's bearer token (the `ate-env` CLI reads it too) | none |
| `SUBSTRATE_ENV_TEMPLATE` | the base template, for OpenHands the one from section 3 | `default-template` |
| `SUBSTRATE_ENV_ATESPACE` | the atespace when `ClusterConfig.namespace` is `"default"` | `ate-env` |

`ClusterConfig(endpoint=..., auth_token=...)` and `TemplateSpec(name=...)` set the same values in
code; they exist only here, not in agent-sandbox-rl. `auth_token` is a `SecretStr`: `repr`,
`model_dump()`, `model_dump_json()` and validation errors do not show it. Prefer the variable for
the token, so that it is not in the program; surrounding white space is removed from it.

| agent-sandbox-rl | on ate-env |
|---|---|
| `ClusterConfig.namespace` | the atespace (`"default"`: see the table above) |
| `max_concurrent`, `window_size`, `run_id` | the same `ate_env_rl.FleetConfig` fields |
| `window_size=None` | `run()` derives the sliding and pipelined window as agent-sandbox-rl does (`recommended_window()`): the leading images whose warm replicas fit in `max_concurrent`, half that for pipelined; without `warm_per_task` and the disk caps, which are ignored |
| `max_warmpool_size` | `max_warmpool_replicas` |
| `ready_timeout` | `acquire_timeout_s` and `ready_timeout_s` |
| `observability` | `ate_env_rl.ObservabilityConfig` (metrics, tracing, service name); `enable_metrics=True`, the default, maps to `None`: metrics when `prometheus_client` is installed, as in agent-sandbox-rl |
| `run(fn, strategy, concurrency, epochs=)` | `ate_env_rl`'s strategy of the same name; an unknown name (also `""`) raises `ValueError`. With `epochs > 1`, the warm pools stay between passes, the fleet tears down once after the last pass, and `fleet.report` covers all passes |
| `handle.cluster_name` | the name of the `ClusterConfig` |
| `make_handle_workspace`, `make_fleet_workspace` | section 6; the token comes from the fleet |

- Ignored, with one INFO log that names each field set to a value other than its default: the
  pod fields of `TemplateSpec` (`resources`, `keepalive_command`, `runtime_class`,
  `node_selector`, `image_pull_*`, `colocate_replicas`, `extra_pod_spec`), because the base
  template fixes the command, resources and placement when it is registered; the Kubernetes
  fields of `ClusterConfig` (`kubeconfig`, `context`, `in_cluster`, `node_selector`,
  `runtime_class`, `image_pull_secret`, `weight`, `max_replicas`); and the Kubernetes mechanics
  of `FleetConfig` (`placement`, `warm_per_task`, `warm_create_budget`, `overcommit_factor`,
  `max_live_sandboxes`, the breaker, teardown hooks, pool and template names, run-namespace
  settings, `labels`, disk-aware sizing). A session key in `extra_pod_spec` has no effect: the
  agent-server takes the base template's key.
- Refused with `ValueError`: more than one cluster (one fleet talks to one ate-env-api; run one
  fleet for each), `adopt_existing=True` and `run_isolation` other than `"none"`. agent-sandbox-rl's
  validators apply too, also to the ignored fields: a configuration that it refuses is refused here.
- Refused with `NotImplementedError`: `run(keep_warm=True)`, `run(recycle=True)` and
  `run(reset=...)`. Each task gets a new environment, and the fleet deletes it on release.
  `max_reuses`, `reset_timeout`, `use_session` and `scale_on_hold` only tune that reuse and have
  no effect.
- Handles have no Kubernetes fields (`claim_name`, `pod_name`, `hostname`, `pod_ip`).

## Versions

The workspace talks to the agent-server's REST and WebSocket API through the OpenHands SDK, and that
API changes between releases, so the client's `openhands-sdk` (and `openhands-tools`, for the agent
examples) must be the same version as the agent-server in the layer: 1.55.0 here. To change both,
set the new versions in [`layer/requirements.in`](layer/requirements.in), run
[`layer/lock.sh`](layer/lock.sh), rebuild and push the layer, register the template with the new
layer digest, and change the pins in [`pyproject.toml`](pyproject.toml).

## Testing

```bash
pip install ./clients/python './integrations/openhands[test]'
python -m pytest integrations/openhands/tests -q
```

The tests run `AteEnvWorkspace` against an in-process fake of ate-env-api (environment lifecycle
over gRPC) and of its port proxy (path form, wake on request, upgrade relay), in front of a real
agent-server started on localhost, as the layer runs it. The fake proxy also answers as ate-env-api
does: 400 for port 80, and with a bearer token 401 for a request without it, and it removes
`X-Env-Authorization` before it forwards. The tests cover the create arguments, the health wait and
the answers that stop it at once (400, 401, 403, 404), cleanup after each failure (lost create answer,
a create that lands after its timeout, refused create, health timeout, wrong session key), ownership
(a failed attach or validation error deletes nothing, a copy never deletes), the token at the proxy,
secret lookups that the agent-server answers in process without the token, invalid secret names,
attach, pause and resume, exit ordering, files, the shared loop (`close_all`, the call timeout, a
fork), and a `RemoteConversation` whose event WebSocket goes through the proxy. With the token,
they cover the port capability: the proxy's test vector, the event WebSocket with a capability only,
expiry, renewal by `resume()` and `renew_ws_capability()`, no capability on REST requests or in
secret URLs, and masking in `repr`, errors and logs.

`test_agent_sandbox_rl.py` covers the section 8 mapping, and runs an agent-sandbox-rl program with
only its imports changed through a Substrate fleet against the same fakes, with the token required.
To run agent-sandbox's own `run_openhands_fleet.py` the same way, set `AGENT_SANDBOX_RL_EXAMPLE` to
its path; the test checks that the import lines are the only diff.

## Verified

GKE with gVisor workers, Substrate main from before v0.6.0, ate-env-api with the port proxy,
agent-server 1.55.0 from this layer, 2026-10-15:

| image | base | agent-server answering after create |
|---|---|---|
| `ubuntu:20.04` | glibc 2.31 | 16 s |
| `swebench/sweb.eval.x86_64.django_1776_django-11099` | Ubuntu 22.04, glibc 2.35 | 16 to 23 s |
| `namanjain12/pandas_final` (R2E-Gym) | Ubuntu 22.04, glibc 2.35 | 20 s |
| `node:22-slim` | Debian 12, glibc 2.36 | 16 s |
| `python:3.12-slim` | Debian 13, glibc 2.41 | 16 s |

- `demo.sh` on each: commands ran with the image's own shell and tools, file download worked, a call
  without the key got 401; also with `PORT=3000` and an extra `--env` setting.
- `codeact_agent.py --scripted` in the SWE-bench environment: the CodeAct run above, its event
  WebSocket through ate-env-api and the Substrate router.
- The SDK workspace on `node:22-slim` saw the image's own `node v22`; on the SWE-bench image a
  command saw the image's Python 3.11.5, not the layer's 3.12. After a suspend, the next SDK call woke
  the environment and returned within 9 s.
- On that cluster actors had no route to the internet (connection reset), hence `--scripted`.
- `run_trajectory.py` in the same SWE-bench environment: `django-11099.json` fixed the issue in 9
  steps and 5.2 s (file edits 0.3 s each once the tool runner was up), and `django-11099-grade.json`
  showed the instance's target tests failing without the fix and passing with it. `hello.json` ran on
  the same image.
- The arm64 build of the layer: the agent-server answered `/health` within 9 s in an arm64 Ubuntu 22.04
  container (local Docker); the CodeAct flow ran the same way locally before the cluster run.

- 2026-10-16, after the hash-locked rebuild, local Docker on arm64 only (the cluster runs above used
  the earlier layer): the layer mounted read-only at `/opt/oh` in `debian:bullseye-slim` (glibc
  2.31) and `ubuntu:24.04` answered `/health` and ran a command with the key (401 without);
  `run_trajectory.py` ran `hello.json` and recorded a failed step in `--out`; `codeact_agent.py
  --scripted` finished and its conversation was gone afterwards.
- 2026-11-06, the security re-lock (pillow 13.0.0, pypdf 6.21.4, certifi 2026.10.23), local Docker:
  the linux/amd64 build passed its version check; the layer mounted read-only at `/opt/oh` in
  `debian:bullseye-slim` (glibc 2.31) answered `/health` and ran a command with the key (401
  without). The arm64 build of this lock is not verified locally.

These runs came before the Substrate v0.6.0 requirement in section 3 and before the later changes
to this package. They do not test those changes.

## Limits

- `api_token` needs an ate-env-api whose port proxy checks `X-Env-Authorization` and removes it
  (ate-env-api with `-auth-token-file`). An older ate-env-api forwards the header to the
  agent-server: do not set `api_token` with it. Without `-auth-token-file`, whoever reaches
  ate-env-api reaches the agent-server port and needs only the session key.
- With `api_token`, the workspace host carries a port capability in its user info, because
  `RemoteConversation` opens the event WebSocket with `websockets.connect(ws_url)`: no extra headers,
  and the host's query string is dropped. The capability is an HMAC of this environment port and an
  expiry, keyed with the token, and ate-env-api accepts it on WebSocket upgrades only. Only the event
  WebSocket sends it: the workspace's REST client uses the host without user info, thus no REST
  request carries the capability. `repr`, `str` and the model dumps mask the capability and leave
  out `api_token`; error messages show the host without the capability; the workspace masks it in
  the `httpx`, `httpcore`, `websockets` and OpenHands workspace and conversation loggers, including
  the `Authorization: Basic` header that `websockets` logs at DEBUG level. The token itself is never
  in a URL. A leaked capability opens this port until it expires (`ws_capability_ttl`, default one
  day, at most seven days less five minutes for clock skew).
- A conversation keeps the capability it opened with. Its event WebSocket gets 401 when it
  reconnects after `ws_capability_ttl`, or after a token rotation. `resume()` and
  `renew_ws_capability()` put a new capability in the host; reattach the conversation after them
  (`Conversation(agent=..., workspace=..., conversation_id=...)`) so its event WebSocket uses it.
  `renew_ws_capability()` keeps the HTTP client open, thus a live conversation can still send
  messages. The capability needs an
  ate-env-api whose port proxy checks port capabilities; an older one answers the event WebSocket
  401.
- With a live `RemoteConversation`, `pause()` does not keep the environment suspended: the suspend
  breaks the event WebSocket, the SDK reconnects after about a second, and the proxy wakes the
  environment for that request. Thus a paused environment with a live conversation is a running
  actor, and it costs a worker. Close the conversation before `pause()` (see `pause()` above).

- One API key per template, and in practice one per atespace: the key is in the template's container
  environment, under `ATE_SIDECAR_ENV_`. Each viewer of the atespace can read it with `GetActorTemplate`, and each template
  derived per task image copies it. Substrate's `EnvVar` has a name and a value only: logs redact
  the value, reads do not, and a template cannot refer to a secret. Use a dedicated atespace for
  these environments, and add it to `ate-env manifest --port-proxy-atespaces` (for example
  `--port-proxy-atespaces=ate-env,oh-envs`): the port proxy forwards only to the atespaces listed
  there, default `ate-env`, and answers 403 for the others. Refer to the OpenHands agent-server in the
  [threat model](../../docs/security/threat-model.md#the-openhands-agent-server).
- The commands that ate-env-api starts do not inherit the key, and the agent-server removes it from
  the commands that it starts. But all of them run in the same container as the agent-server, and
  as the same user: the guest starts the agent-server and the commands as the container's user.
  Such a command can read the key from
  `/proc/<pid>/environ` of the agent-server or of the guest, which keeps the environment it was
  started with. Treat the key as readable by code in the environment.
- The agent-server runs as the image's user and keeps its state under that user's home. The first
  environment on a node pulls the layer.
- A daemon reached through ate-env-api must listen on all interfaces, not 127.0.0.1: the router
  delivers to the actor's address, and a loopback-only listener answers `502 bad gateway`.
- The examples have their own limits (model calls from inside the environment, the model key, the
  tool runner): see [`examples/openhands`](../../examples/openhands/README.md#limits).
- `fresh_eval_env` costs one more acquire per graded episode: a Substrate actor has one container,
  so the clean grading container of the MLPerf setup is a second environment from the warm pool.
- The R2E-Gym adapter is tested against a stand-in for R2E-Gym (it is not on PyPI); a live
  SWE-bench episode on a cluster is the acceptance step still open.

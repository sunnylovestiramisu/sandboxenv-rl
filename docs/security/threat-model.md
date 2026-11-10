# Threat model

This document describes the trust boundaries of ate-env as the code on `main` implements them:
what each boundary protects, who can attack it, what mitigates each threat today, and the known
gaps. It describes merged behavior only.

A pull request that adds network surface, privileges, wire or API fields, or dependencies must
update this document in the same pull request. The pull request template asks for it.

## System

```
 callers ──(1)──▶ ate-env-api ──(2)──▶ Substrate control plane (ateapi)
 (CLI, SDKs,       │  gRPC (h2c)
  MCP clients,     │  MCP (HTTP)                ┌───────────── actor (gVisor) ─────────────┐
  port proxy       │  port proxy (HTTP)         │ ate-env-guest (PID 1, port 80)           │
  clients)         └──────(3)──▶ atenet router ─┼─▶  processes, files, sidecars, daemons   │
                                                └──────────────────────────────────────────┘
```

- **ate-env-api** serves three things on one plaintext listener (`-listen`, default
  `0.0.0.0:7777`): the gRPC API (`EnvironmentService`, `ProcessService`, `FileSystemService`), the
  MCP endpoint `/v1alpha/envs/{id}/mcp`, and the port proxy (header form `X-Env-Id` and
  `X-Env-Port`, path form `/v1alpha/envs/{id}/ports/{port}/...`).
- **Lifecycle calls** (create, get, suspend, delete) go to the Substrate control plane over TLS
  with a bearer token.
- **Guest calls, MCP and the port proxy** go to the atenet router over mutual TLS (HTTPS for guest
  gRPC on port 443 and TLS HTTP CONNECT on port 8444) when `-atenet-client-cert` and
  `-atenet-ca-file` are set, which `ate-env manifest` enables by default using kubelet-projected pod
  identity (`podidentity.podcert.ate.dev/identity`) and `servicedns`
  (`servicedns.podcert.ate.dev/identity`) bundles reloaded at each handshake; without
  `-atenet-client-cert`, `ate-env-api` dials plaintext ports 80 and 8081 (served only by test
  fakes). The router forwards requests to the actor named in the `ate-target-actor` header.
- **Telemetry**, off by default: with `--metrics-listen-addr`, `ate-env-api` serves Prometheus
  metrics on a second plaintext listener; with `--enable-tracing`, it exports traces over OTLP and
  passes the caller's W3C trace context on to the control plane and the guests (see
  [Server telemetry](#server-telemetry)).
- **ate-env-guest** runs as PID 1 in each actor. It serves processes and files on port 80, reaps
  orphaned processes, and starts the sidecars that the template names.
- **`ate-env manifest`** deploys the namespace, the WorkerPool, and the `ate-env-api`
  ServiceAccount, Deployment and Service. `ate-env manifest access-policy` grants that
  ServiceAccount the `editor` role on an atespace.

## Assets

- The contents of each environment: code, data, and the credentials that callers put in it.
- The control-plane credential of `ate-env-api`: a projected ServiceAccount token, audience
  `api.ate-system.svc`, that expires after `tokenExpiration` in `cmd/ate-env/manifest.go`.
- The caller token in the Secret that `ate-env manifest` mounts: `ate-env-api-token`, or the one
  that `--auth-token-secret` names. There is no token only with `--insecure-no-auth`.
- The session key of the OpenHands agent-server (`OH_SESSION_API_KEYS_0`), in the container
  environment of each template that `integrations/openhands` registers, and of each template that
  `ate-env-api` derives from one. `examples/openhands/demo.sh` and the README register it with
  `--sidecar-env`, thus the container carries it as `ATE_SIDECAR_ENV_OH_SESSION_API_KEYS_0`.
- ActorTemplates, golden snapshots and suspended environments in the snapshots bucket.
- Worker capacity: the CPU, memory and actors of the WorkerPools.

## Attackers

| Attacker | Capability |
|---|---|
| A network peer | Reaches the `ate-env-api` listener: a workload in the cluster, or anyone behind a port-forward or a LoadBalancer. |
| A token holder | Has the caller token. Has all the powers of the API. |
| Code in an environment | Runs as the guest user inside an actor: agent actions, model output, task code. Treat it as untrusted. |
| A web page | Runs in the browser of a developer who port-forwards the API to localhost. |
| A template author | Registers ActorTemplates with `kubectl-ate`. Trusted: chooses the image, its command, the sidecars and the resources. |
| A dependency or image | A compromised Go module, Python package, base image or GitHub Action. |

## Boundary 1: callers to ate-env-api

| Threat | Current mitigation | Gap |
|---|---|---|
| Unauthenticated use of the API | `-auth-token-file` makes the gRPC API, MCP and the port proxy require `Authorization: Bearer <token>`. The port proxy also accepts the token in `X-Env-Authorization` and does not forward it. A WebSocket upgrade that cannot carry a header can present a port capability (`ate-env-capability` in the URL user info): an HMAC of one environment port and an expiry of at most seven days, keyed with the token. The proxy accepts it only on a WebSocket handshake (`GET` with `Connection: Upgrade` and exactly one `Upgrade` field whose whole value is `websocket`, thus the daemon gets the same handshake) and only as the request's single `Authorization` credential, and never forwards it. The check uses a constant-time compare. The server reads the file again when it changes. A token file that it cannot read gives the caller only `internal error`. Without a token, `ate-env-api` logs a warning when it listens on a non-loopback address. The CLI, the Go and Python clients, the SWE-bench loader, the MCP example and the NeMo Gym provider can send the token. | `ate-env manifest` mounts the token by default, and `--insecure-no-auth` turns authentication off. The `ate-env-api` binary itself has no token unless `-auth-token-file` is set: a deployment that is not made with `ate-env manifest` must set it. A leaked port capability stays valid until its expiry, for WebSocket handshakes to its port. It binds the atespace, the environment id and the port, not one environment instance: an environment that the caller creates again with the same id accepts the old capability until it expires. Callers choose the id, thus they limit this with a short expiry and by not reusing an id while a capability for it is valid. Only a token rotation revokes it, and the rotation also revokes all other capabilities and needs each caller to get the new token. The proxy limits a capability to a WebSocket handshake, but the daemon decides what it does with that handshake: a server that routes only by path, such as a Go `net/http` handler, serves its `GET` handler to it. Thus a capability opens each `GET` endpoint of a daemon that does not route handshakes separately. A FastAPI or Starlette application on uvicorn serves a handshake only from its WebSocket routes. |
| Token theft on the network | The manifest Service sets no `type`, thus it is `ClusterIP`. | The listener has no TLS: the token and all data travel in clear text. The manifest ships no NetworkPolicy. |
| One caller acts on the environments of another caller | None. | One shared token. No per-caller identity and no authorization: each token holder can use each environment. |
| Reach beyond the environments of ate-env | Lifecycle calls use the `ate-env-api` identity, which the access policy limits to its atespaces. The port proxy forwards only to the atespaces that `--port-proxy-atespaces` names (default `ate-env`) and answers 403 for the others. | Guest calls and MCP go to the router. The control plane is asked only while an environment wakes. Thus these paths reach any actor that the router serves, in any atespace, including actors that ate-env did not create. With `--port-proxy-atespaces='*'`, the port proxy does the same. |
| A web page drives the API through a port-forward (CSRF) | The port proxy path form refuses a state-changing request or a WebSocket handshake from another origin (`--port-proxy-trusted-origins` adds origins). The header form needs custom headers, and MCP needs a JSON content type: a browser sends either one to another origin only after a CORS preflight, which the API never approves. | — |
| DNS rebinding: a page on an attacker's name resolves to the forwarded listener and becomes same-origin | `internal/hostcheck.Checker` wraps every HTTP route on the listener (`/healthz`, MCP, and both header and path forms of the port proxy) and returns `403 Forbidden` unless `r.Host` is an IP address, `localhost` or `*.localhost`, an in-cluster name of `--service-name` (`ate-env-api` by default), or a host listed in `--allowed-hosts` (`--api-allowed-hosts` in the manifest). The MCP handler sets `DisableLocalhostProtection: true` because `hostcheck` enforces this check in front of it. | `Host` validation is a browser defense: non-browser clients can set `Host` arbitrarily (caller authentication is `--auth-token-file`). The raw gRPC handler on `/` stays unwrapped so gRPC clients can send their dial target as `:authority` (guarded by bearer auth). |
| The port proxy reaches the guest port | The port proxy refuses port 80, where the guest listens. | — |
| Resource exhaustion | The port proxy bounds its idle tunnels with `idleTunnelsPerPort`, `idleTunnelsTotal` and `idleTunnelTimeout` in `internal/portproxy/portproxy.go`. Worker capacity bounds the running actors. | A token holder can create environments without a quota or a rate limit; suspended environments keep their snapshots in the bucket. The MCP handler keeps one server for each environment, and the client keeps one guest connection for each environment. Neither is evicted while the process runs. |
| Control-plane detail in guest-call errors | A guest call to an environment that cannot start reports why: for one that the control plane moved to CRASHED while it was waking, the error carries Substrate's crash reason (`ActorStatus.crash.message`, at most 4096 characters), and with `--wake-no-capacity-grace` a call that no worker has room for carries the router's no-capacity text. Only a caller that may call that environment, thus one that has the token, gets it. The `ate-env-start-failure` trailer carries only `crashed` or `no-capacity`. | Some crash reasons include an atelet error verbatim, such as the error of a failed restore, and a message can name a node or a snapshot. A caller sees that detail for its own environments; it does not reveal other environments or credentials. |

## Boundary 2: ate-env-api to the Substrate control plane

| Threat | Current mitigation | Gap |
|---|---|---|
| Theft of the control-plane credential | A projected token with audience `api.ate-system.svc` and the lifetime that `tokenExpiration` sets. The ServiceAccount mounts no Kubernetes API token. | `-skip-verify` defaults to `true`, and the manifest does not set it. Thus `ate-env-api` does not verify the TLS certificate of the control plane. A peer that intercepts the connection can read the token. |
| Excess privilege | A dedicated ServiceAccount. The access policy grants `editor` on each atespace that an operator adds. Creating an atespace takes a global owner, which `ate-env-api` is not. | `editor` includes creating ActorTemplates. Refer to on-demand rootfs images. |

## Boundary 3: the router and the actors

| Threat | Current mitigation | Gap |
|---|---|---|
| Use of a guest without the API | The router runs in `ate-system`; its TLS listeners (the only ones Substrate v0.6.0+ serves) require a client certificate (`spiffe://cluster.local/ns/<namespace>/sa/ate-env-api`) whose SPIFFE ID the operator adds to the router's `--ingress-allowed-spiffe-ids` (see README). `ate-env manifest` mounts the certificate by default (`--api-router-tls`), and the guest listens only inside the actor on port 80. | Without `-atenet-client-cert` (`--api-router-tls=false`), `ate-env-api` dials plaintext ports that only test fakes serve (calls against a Substrate v0.6.0+ router fail). Inside the cluster between the router and the actor, traffic on port 80 is unauthenticated HTTP/h2c and the guest does not authenticate callers: any workload permitted to reach the router (or an actor's network directly) can run commands as the guest user. |
| Code in an environment attacks the node or other environments | Each actor runs in gVisor, the sandbox class of the WorkerPool. | ate-env sets no actor egress policy. Egress follows the Substrate defaults. |
| Processes escape supervision | When the guest is PID 1, it runs as an init: it reaps orphans and forwards signals. A process timeout kills the process group. The guest refuses PTY window sizes that do not fit the 16-bit kernel fields. | — |
| A sidecar gains extra privilege | Only a template author sets sidecars (`--sidecar`, `--sidecar-readyz`, `--sidecar-env`), and that author already controls the image and its command. Sidecars run inside the CPU and memory limits of the actor. | — |
| A command reads a credential of a sidecar | `ate-env manifest template --sidecar-env NAME=VALUE` sets `ATE_SIDECAR_ENV_NAME` on the container. At start, before it starts anything, the guest removes each such variable from its own environment and adds `NAME` to the environment of its sidecars only (`internal/sidecarenv`, `cmd/ate-env-guest/main.go`). The process service builds the environment of each command from the guest's environment (`guest/process/tracker.go`), thus the commands that the guest starts through `ate-env-api` and their children do not inherit the variable. A suspend checkpoints the memory of the guest, thus a resumed guest keeps the environment without the variable, and a sidecar that it restarts gets the variable again. The template also carries the guest flag `-sidecar-env-required`. A guest older than `--sidecar-env` does not know the flag and exits at flag parse, thus the actor crashes at start and does not run the sidecars without the variable. A new guest exits before it listens or starts a sidecar when the flag is set and no prefixed variable exists. | A `--env` variable reaches each command: use `--sidecar-env` for a credential. The sidecars run in the same container as the code in the environment, and as the same user, because the guest starts both as the container's user. Today, at the Substrate pin in `go.mod`, that user is root for the guest, the sidecars and each command, and root reads each `/proc/<pid>/environ`. After the Substrate bump that runs containers as the image's `USER` (Substrate `5d226a50`), the three run as that one user, and a process still reads `/proc/<pid>/environ` of another process of the same user. Thus that code can read a `--sidecar-env` variable from `/proc/<pid>/environ` of a sidecar, which gets it through its environment, or of the guest, whose `/proc/<pid>/environ` keeps the environment it was started with. When the guest is PID 1, the same holds for `/proc/1/environ`. Each caller of `ate-env-api` (a token holder, or each network peer when authentication is off) can also read `/proc/1/environ` or `/proc/<pid>/environ` with `ReadFile`, without a command; this was also true before `--sidecar-env`. The guest keeps the values in its memory to give them to a restarted sidecar, and the sidecar holds them too, thus each snapshot of the environment holds them. Follow-up options: `prctl(PR_SET_DUMPABLE, 0)` in the guest after it takes the variables, and in a small launcher for the sidecar, can block same-user reads of `environ` and `mem` (not verified on gVisor; a process with `CAP_SYS_PTRACE` still reads them). The real boundary is a separate user for the sidecar. Refer to the OpenHands agent-server. |

## The OpenHands agent-server

`integrations/openhands` runs the OpenHands agent-server as a sidecar from a runtime layer. The
base template sets its session key as a sidecar-only variable (`ate-env manifest template
--sidecar-env OH_SESSION_API_KEYS_0=<key>`). The workspace reaches the agent-server through the port
proxy. A base template registered before `--sidecar-env` has the key in `--env`, and so has each
template derived from it; the rows below hold for those only after they are registered again.

| Threat | Current mitigation | Gap |
|---|---|---|
| A caller reads the session key from a template | Reading a template needs viewer access to the atespace. Substrate logs redact the values of environment variables. Use a dedicated atespace for these environments, as `integrations/openhands/README.md` says. | Each caller with viewer access to the atespace reads the key with `GetActorTemplate`: reads do not redact it. Each template that `ate-env-api` derives from the base copies it. A template cannot refer to a secret. |
| A holder of the session key drives the agent-server | With `-auth-token-file`, the port proxy also asks for the caller token. Thus the key alone does not pass the port proxy. | The agent-server checks only the session key (`check_session_api_key` in `openhands/agent_server/dependencies.py`). Without `-auth-token-file`, each network peer of `ate-env-api` that has the key controls the agent-server of each environment whose id it knows. |
| A secret lookup carries the caller token into the sandbox | `AteEnvWorkspace.get_secrets()` gives `LookupSecret` URLs on `127.0.0.1:<server_port>` that carry the session key only, and the agent-server answers them in process. Thus a secret lookup does not go through the port proxy and does not need the caller token. | — |
| Code in one environment drives the agent-server of another environment | Each actor runs in gVisor. The guest gives the `--sidecar-env` key to the agent-server only, thus the commands that the guest starts through `ate-env-api` do not inherit it: `ate-env exec`, the Go and Python clients, the fleet's `handle.exec`, and the run and copy of the R2E-Gym adapter. The agent-server removes the key from the environment of the commands that it starts: the bash tool, the commands of the CodeAct agent, and the terminal steps of `run_trajectory.py`. `examples/openhands/demo.sh` checks with `ate-env shell` that a command does not see the key. With `-auth-token-file`, the key alone does not pass the port proxy. | All environments of one base template share one key. All these commands run in the same container as the agent-server and as the same user, thus they can read the key from `/proc/<pid>/environ` of the agent-server or of the guest. This read is not verified on a cluster. On a base template registered with the key in `--env`, each command that the guest starts gets the key with `env`. In both cases, the code needs a route to `ate-env-api` to reach the agent-server of another environment. With that route, it needs nothing more when authentication is off, and a credential that the port proxy accepts when authentication is on. |
| The workspace exposes the caller token to the agent-server or a log | With `api_token`, REST requests carry the token in `X-Env-Authorization`, which the port proxy removes before it forwards them. The event WebSocket carries only a port capability for the agent-server port of its environment, never the token; an ate-env-api with port capability support does not forward it. The client default for `ws_capability_ttl` is one day. `repr`, the model dumps, error messages and the workspace's loggers mask the capability. The agent-server is a FastAPI application on uvicorn, thus a capability opens only its WebSocket routes. | A leaked capability opens WebSocket handshakes to that port until it expires, at most seven days less five minutes after the workspace made it. |

## Snapshots and the snapshots bucket

A snapshot holds the memory and the files of an environment, and thus the credentials that callers
put in it. Golden snapshots of templates and the snapshots of suspended environments go to the
cluster's snapshot location (ate-api-server's `--snapshot-location`), or to the bucket that
`ate-env manifest template --snapshots-bucket` names on a cluster with Substrate's
`PerTemplateSnapshotLocation` preview gate.

What ate-env controls: with `--snapshots-bucket`, `ate-env manifest template` writes the bucket URL
into the template's `snapshotConfig.storageLocation`; templates that `ate-env-api` derives keep it,
and their create opts in to the preview feature. `ate-env-api` does not read or write the bucket
itself.

What ate-env does not control: Substrate writes and reads the snapshots. The operator owns the
access control, the encryption and the retention of the bucket. What stays in the bucket after an
environment or a template is deleted follows Substrate.

A local suspend (`SuspendEnvironment` with `mode: SUSPEND_MODE_LOCAL`, `ate-env suspend --mode
local`) returns before the upload: the snapshot sits on the worker's node, under Substrate's state
directory, until atelet's background upload lands. That is the same data the bucket holds, for a
shorter time, on the host that already ran the environment's memory; ate-env adds no reader to it,
and the node's own access control applies. What a local suspend does trade is availability: a node
lost before the upload lands takes the snapshot, and the environment, with it.

Operator requirements:

- Grant access to the bucket only to the Substrate identities that write and read snapshots. Block
  public access.
- Use encryption with a customer-managed key if your data requires it.
- Set a lifecycle rule that matches your retention policy.
- Use a separate bucket or prefix for each cluster.

| Threat | Current mitigation | Gap |
|---|---|---|
| A reader of the bucket gets the memory, files and credentials of environments | The operator's bucket settings. | ate-env does not check the access control, the encryption or the retention of the bucket. |
| A reader of a worker node gets the snapshot of a locally suspended environment before its upload lands | The snapshot sits where the running environment's memory sat; the node's own access control. A durable suspend uploads before it returns, but atelet keeps a local copy of the snapshot on the node until the actor detaches, thus both modes leave snapshot files on the worker node. | A node lost before the upload lands loses the environment (availability, not confidentiality). |

## On-demand rootfs images

`CreateEnvironment` with `rootfs_image` makes `ate-env-api` derive an ActorTemplate named
`<base>-<first 12 hex digits of the digest>` from the base template. The image replaces the
container, and the guest is mounted into it as a read-only image volume. `CreateEnvironment` also
accepts optional `worker_selector` labels (`Actor.worker_selector`), and `GetEnvironment` reports
both `node` and `worker_selector`.

| Threat | Current mitigation | Gap |
|---|---|---|
| A caller runs an image of its choice under the settings of the base template (worker selector, snapshots bucket, resources) | The image must be pinned by digest. If `<base>-<12 hex>` already exists, `ate-env-api` reuses it when its container image carries the same SHA-256 digest (even under another repository path, keeping the existing template's reference) and refuses a different digest with `FailedPrecondition`, including when a concurrent create made it. | Any registry is accepted. |
| A caller steers environments across worker pools (`worker_selector`) or inspects placement (`node`) | Substrate ANDs `CreateEnvironmentRequest.worker_selector` with the base template's `WorkerSelector`, so per-environment selectors narrow the allowed worker pools and persist across wakes. | Any token holder can restrict its environments to a subset of the base template's pools and read the hosting Kubernetes node name from `Environment.node`. |
| Template growth | None. | Derived templates have no quota and no garbage collection. |

Accepted risk for now: this hardening is not needed for RL at the moment. Revisit it before the
API serves callers other than trusted RL jobs.

## Client tracing

The Python client emits OpenTelemetry spans when `TracerConfig(enable_tracing=True)` is set. The
spans go to the application's `TracerProvider`. Without one, the client builds an OTLP exporter
only when `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set; otherwise
the spans go nowhere.

| Item | Current state | Gap |
|---|---|---|
| Data in spans | Span names and attributes: environment, atespace and template names, the image reference without its tag, the run id, the executable name (without arguments and without leading `NAME=value` assignments), the exit code, a first-command marker, and file paths and sizes. No command arguments, no file content and no token. | The trace backend learns environment names and file paths. |
| Data on the wire | With tracing on, each gRPC call on a channel that the client creates carries the W3C `traceparent` and `tracestate`, never baggage. Without `--enable-tracing`, `ate-env-api` ignores them; with it, see [Server telemetry](#server-telemetry). | — |
| The export connection | The `OTEL_EXPORTER_OTLP_*` environment variables configure the endpoint, its TLS and its credentials. | An endpoint without TLS sends the spans in clear text. |

## Server telemetry

`ate-env-api` serves metrics only with `--metrics-listen-addr` and traces only with
`--enable-tracing`. `ate-env manifest` sets them with `--metrics-port` and `--enable-tracing`.

| Item | Current state | Gap |
|---|---|---|
| The metrics listener | Off by default. When on, it serves `GET /metrics` on its own listener, with a 5 s header read timeout. It serves request counts and latencies by gRPC method and status code, `create_to_ready` and wake-wait latencies by atespace and outcome, atespace names with environment counts by state, and the build version and commit. Labels never carry environment ids, actor ids, run ids, paths, image references or tokens. Environment counts come from the control plane at most once per `--metrics-environments-ttl` (default 1m). | The listener has no authentication and no `Host` check. Anyone who reaches it (with the manifest, any pod that reaches the pod IP or the Service port) reads the atespace names and the counts. |
| Incoming trace context | With `--enable-tracing`, the otelgrpc server handler continues the W3C `traceparent` and `tracestate` that a caller sends, and the server passes them on to ateapi and the guests as is. Only W3C Trace Context is propagated, never baggage. | The caller is not trusted: it chooses the trace id and the sampled flag, thus it can join the server's spans to its own trace or ask every hop to sample. |
| `x-ate-env-requested-at` | A caller may send this gRPC metadata on `CreateEnvironment` to start `create_to_ready` at its own clock. A value in the future or not in RFC 3339 is ignored. | A caller can send a time in the past and lengthen its own `create_to_ready` samples. |
| The export connection | The `OTEL_EXPORTER_OTLP_*` environment variables configure the endpoint, its TLS and its credentials. Control-plane reads for the environments gauge are not traced. | An endpoint without TLS (`--otlp-insecure`) sends the spans in clear text. |

## Harbor integration

`integrations/harbor` is a Harbor environment provider: `harbor run` creates one ate-env
environment per trial, runs the agent's and the verifier's commands through the guest, and moves
files through the file API. It runs in the operator's Harbor process, next to the agents' model
keys, as a client of the existing gRPC listener; nothing new listens and `ate-env-api` gains no
privilege.

| Item | Current state | Gap |
|---|---|---|
| Caller token | Read from `SUBSTRATE_ENV_AUTH_TOKEN` or the `api_token` kwarg and sent as gRPC metadata. The provider does not log it and does not write it to `trial_dir/ate_env.json`. | Harbor writes the environment kwargs into each trial's `result.json`, thus a token passed with `--ek api_token=` lands on disk with the results. The provider warns when it sees the kwarg; the README says to set the variable. |
| Registry credentials | `build=local` uses the operator's docker login, `build=cloud-build` the operator's gcloud account; the provider reads neither. Images are pushed as `<registry>/<task>:<environment_id>`. | The worker nodes need read access to that registry; granting it is an operator step outside this repository. |
| Task images | A dataset's task images run as actors, pinned by digest, the way any rootfs image does; the provider does not inspect them, and the actor's isolation is the boundary. | Egress follows the Substrate defaults (known gap 8), and most tasks and installed agents download packages at run time. |
| Commands as root | Harbor's verifier removes its reward files and `chmod`s its script as `root`. On an image that runs as root that is the image's own user, and the command runs as is. On another image the provider uses `sudo -n`, or `su` from root, only when the image offers it; otherwise the call fails and says why. | — |
| Dependencies | `harbor`, pinned to one minor, and `dockerfile-parse`, in `integrations/harbor` only; neither is a dependency of the client, the CLI or `ate-env-api`. | Dependabot's `uv` entry proposes updates through `uv.lock` inside `harbor`'s `<0.26` pin; `dockerfile-parse` has no upper bound. A minor bump of `harbor` is a manual step with the trial test as the gate. |

## Compatibility of the API

A protobuf message with fields that the server does not know is valid proto3: the server keeps the
unknown fields and ignores them. Thus a client from another version or fork of this API, which uses
the same `ateenv.v1alpha` names, can get a behavior other than the one it asked for, without an
error. This is a compatibility risk, not a boundary of this threat model.

Likewise, a new client talking to an old server receives a response where new fields are simply absent
(e.g., a missing `timed_out` boolean reads as `false`), and must safely fall back to a heuristic.

## Supply chain and dependencies

| Item | Current state | Gap |
|---|---|---|
| Images | A template mounts the guest into a rootfs image only when the guest is pinned by digest. `.ko.yaml` pins the guest base image by digest. The README asks for the worker image by digest. | — |
| Releases | `.github/workflows/release.yaml` signs container images and CLI release blobs/checksums with Cosign keyless signing (GitHub Actions OIDC) and optionally publishes the Python client (`ate-env-rl`) to PyPI when `vars.PYPI_PUBLISH_ENABLED == 'true'` (or via manual tag `workflow_dispatch` with `publish_pypi=true`) using OIDC Trusted Publishing (`pypa/gh-action-pypi-publish`, `environment: pypi`, `refs/tags/<tag>` checkout, and PEP 440 tag/wheel version verification). Actions are pinned by commit SHA. | This repository has published no release yet; before the first release, repository settings must configure a `v*` tag ruleset, and before enabling `PYPI_PUBLISH_ENABLED=true`, restrict the `pypi` GitHub Environment to `v*` tags with required reviewers (see `docs/release.md`). |
| Dependencies | `ate-env-api` links `prometheus/client_golang`, the OpenTelemetry SDK and OTLP/gRPC trace exporter, and `otelgrpc`; they run only when metrics or tracing is on. `uv.lock` pins every Python dependency of `clients/python` and the `integrations/*` projects (one uv workspace; `integrations/nemo-gym` and the others get `ate-env-rl` from it as a workspace member) by version and SHA-256, and the Nix envs CI runs are built from it; uni-agent, a git dependency, is pinned by commit. `flake.lock` pins the toolchain (Go, the Python interpreters, uv, ko, kind, kubectl) and the Nix inputs; `go.mod`/`go.sum` pin the Go modules and the protobuf generators. Dependabot covers Go modules (`gomod`), `uv.lock` (`uv`; `openhands-*`, `uni-agent` and `grpcio-tools` are moved by hand), GitHub Actions, the OpenHands layer's base images (`docker`) and `flake.lock` (`nix`); see `.github/dependabot.yml`. `integrations/harbor` is a workspace member too; its `harbor` dependency is pinned to one minor in its `pyproject.toml` and bumped by hand (see Harbor integration). | — |
| Pre-flight probes | `WarmImage` creates probe environments from the caller's template and image in the caller's atespace, under ate-env-api's own Substrate identity as every environment is: at most 256 at once (`nodes x probes_per_node`, both capped, per round) for up to `max_rounds` (10) rounds, so 2,560 creates per call at most. It resumes them, deletes them when the round settles or the call is cancelled, and leaves a name that exists already alone; the default run id carries a random suffix so two calls never name a probe alike. A probe's create, resume and delete are retried on the transient failures of a loaded control plane, within the probe's timeout (the delete within a minute); a probe still there after that is listed in the coverage's `undeleted_probes` for the caller to sweep, and logged. A probe whose create was cut off can still be committed by the control plane after its delete, even when a retry made it first: the call repeats that delete for a short settle window and, before the round gives its probe slots back, lists the atespace once and deletes any of its probes that appeared later, so such a probe holds a slot until it is gone; when that list fails, such probes are listed in `undeleted_probes`. A probe whose resume failed ends `CRASHED`, which Substrate refuses to delete today, so it stays until the control plane takes the delete and is listed the same way. Across calls, the server caps the probes in flight per node of one worker selector (`-warm-max-probes-per-node`, default 2): a round takes its slots before it creates its probes, waits for them within the call's own deadline, and gives them back once they are deleted. `ListPlacementDomains` reads Substrate's workers, which needs the global viewer role on ate-env-api's identity: that role sees every worker and atespace name on the control plane, so it is a separate, optional binding (`ate-env manifest access-policy --global`); `WarmImage` works without it when the caller passes the node count, `ListPlacementDomains` does not. | A caller with the create permission can keep a pool busy with warm calls; the per-node cap bounds the first boots in flight on a node, not the calls, and there is no per-caller rate limit beyond it and the per-call caps. Probes a call could not delete hold a slot each until swept. A create that the control plane commits after the settle window and the round's list still leaks and is not listed. |
| Integrations and examples | The code in `integrations/` is maintained. | The code in `examples/` is not supported: it gets no security fixes and no compatibility promise. |

## Known gaps

1. Caller authentication uses one shared token, and the listener has no TLS. A deployment that
   is not made with `ate-env manifest` has no authentication unless it sets `-auth-token-file`.
2. Guest calls and MCP reach any actor that the router serves; the access policy does not limit
   them. The port proxy reaches the atespaces that `--port-proxy-atespaces` names.
3. HTTP `Host` validation (`internal/hostcheck`) blocks browser DNS rebinding across `/healthz`,
   MCP, and the port proxy, but the gRPC handler on `/` stays unwrapped and non-browser clients can
   set `Host` arbitrarily (relying on `-auth-token-file`). Between the `atenet` router and the
   actor, traffic on port 80 is unauthenticated HTTP/h2c and the guest does not authenticate
   callers.
4. `ate-env-api` does not verify the TLS certificate of the control plane by default.
5. Environment creation has no quota and no rate limit. The MCP server cache and the guest
   connection pool grow with each environment.
6. ate-env does not check the access control, the encryption or the retention of the snapshots
   bucket.
7. The manifest ships no NetworkPolicy and no `securityContext`.
8. ate-env sets no actor egress policy.
9. On-demand rootfs images accept any registry and create templates without a quota or garbage
   collection (accepted for now).
10. The metrics listener, when it is on, has no authentication and no `Host` check, and incoming
    trace context from callers is forwarded as is.
11. The OpenHands session key is readable from the template by each viewer of the atespace, all
    environments of one base template share it, and it is the only check of the agent-server. Code
    in the environment can read it from `/proc/<pid>/environ` of the agent-server or the guest,
    because it runs as the same user.

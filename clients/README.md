# Clients

Two clients of `ate-env-api`, both supported ([docs/support.md](../docs/support.md)).
Both speak the `ateenv.v1alpha` API in [`proto/ateenv/v1alpha`](../proto/ateenv/v1alpha)
over gRPC and send the caller token when one is configured.

| Client | Language | Package | What it covers |
|---|---|---|---|
| [`go/`](./go/README.md) | Go | `github.com/agent-substrate/env/clients/go`, package `env` | `Client`, `Env` and `Process`: environment lifecycle (`Create`, `Get`, `Suspend`, `SuspendLocal`, `Delete`), shell and process streams, file transfer, `ServerInfo`, and OpenTelemetry client spans. The CLI and the SWE-bench loader are built on it. Package docstrings document each exported symbol. |
| [`python/`](./python/README.md) | Python 3.10+ | distribution `ate-env-rl`, import `ate_env_rl` | The async client (`Client`, `Env`, `Process`) and the fleet SDK (`SandboxFleet`, `AsyncSandboxFleet`, `SandboxHandle`) for RL jobs: acquire sandboxes by task image, run strategies, warm pools, park and wake (`suspend` takes `SuspendMode.DURABLE` or `SuspendMode.LOCAL`), run reports, metrics and tracing. The integrations are built on it. |

## Guides and layout

- [`go/README.md`](./go/README.md): install, connect, environment lifecycle, process execution, and observability.
- [`python/README.md`](./python/README.md): install, connect, the fleet SDK, pool sizing, errors, and observability ([docs/observability.md](../docs/observability.md)).
- [`python/USER_GUIDE.md`](./python/USER_GUIDE.md): RL rollouts and benchmark adapters (`ate_env_rl.adapters`), overlapping claims with policy decoding on VeRL and Ray.
- [`python/examples/quickstart.py`](./python/examples/quickstart.py): the Python client end to end against a running `ate-env-api`.
- [`python/src/ate_env_rl/_gen/README.md`](./python/src/ate_env_rl/_gen/README.md): generated protobuf and gRPC stubs (`make python-protos`).
- [`docs/dev/code-layout.md`](../docs/dev/code-layout.md): subpackage placement rules (`backend/`, `runtime/`, `adapters/`, `telemetry/`) and test doubles.

Install the Python client with the extras you need:

```bash
pip install -e 'clients/python[dev]'                    # unit and integration tests
pip install -e 'clients/python[tracing,metrics]'        # OpenTelemetry and Prometheus
```

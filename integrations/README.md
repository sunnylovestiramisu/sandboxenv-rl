# Integrations

Adapters that let an agent or RL framework run its sandboxes as Agent Substrate
environments through `ate-env-api`. Each is a Python distribution of its own that
depends on the Python client, [`ate-env-rl`](../clients/python/README.md),
and registers with the framework the way the framework expects, with no change
to the framework. They are supported ([docs/support.md](../docs/support.md)).

| Integration | Framework | Distribution / import | What it provides | Install |
|---|---|---|---|---|
| [`harbor/`](./harbor/README.md) | [Harbor](https://github.com/harbor-framework/harbor) | `ate-env-harbor` / `ate_env_harbor` | `AteEnvEnvironment`, a Harbor environment provider (`harbor run ... -e ate_env_harbor:AteEnvEnvironment`) that runs each trial in an environment from a digest-pinned or built task image, with optional runtime tool layers on `PATH`. | `pip install -e integrations/harbor` |
| [`nemo-gym/`](./nemo-gym/README.md) | [NeMo Gym](https://github.com/NVIDIA-NeMo/Gym) | `nemo-gym-substrate` / `nemo_gym_substrate` | Sandbox provider `substrate` (`SubstrateSandboxProvider`), registered through the `nemo_gym.sandbox_providers` entry point. `endpoint()` reaches a daemon inside the sandbox through the port proxy. | `pip install -e integrations/nemo-gym` |
| [`openhands/`](./openhands/README.md) | [OpenHands](https://github.com/OpenHands/software-agent-sdk) | `ate-env-openhands` / `ate_env_openhands` | `AteEnvWorkspace`, an OpenHands `RemoteWorkspace` that mirrors `AgentSandboxWorkspace` and runs the agent-server in any unmodified task image (glibc 2.31 or newer); fleet adapters (`make_handle_workspace`, `make_fleet_workspace`); `ate_env_openhands.r2egym`, a re-export of the R2E-Gym adapter in `ate_env_rl.adapters.r2egym`; agent-sandbox-rl's fleet API on ate-env (`agent_sandbox_rl/`). | `pip install -e integrations/openhands` |
| [`uni-agent/`](./uni-agent/README.md) | [uni-agent](https://github.com/verl-project/uni-agent) | `uni-agent-substrate` / `uni_agent_substrate` | Sandbox provider `ate_env` (one environment per episode) and the `r2e_gym` task that hides the tests, lets the agent work, then grades in the same environment; dataset and image tooling for R2E-Gym, SWE-reBench and SWE-bench rows. | `pip install -e integrations/uni-agent` |

Each integration's `README.md` covers its configuration, runtime layers (`nemo-gym/layer`
and `openhands/layer`; see [docs/rootfs-images/RUNTIMES.md](../docs/rootfs-images/RUNTIMES.md)),
template helper scripts, unit fakes, and, for `harbor`, `nemo-gym` and `openhands`, live
`tests/test_e2e.py` targets. See
[docs/dev/code-layout.md](../docs/dev/code-layout.md) for repository placement rules.

## Tests in CI

Three jobs of [`.github/workflows/ci.yaml`](../.github/workflows/ci.yaml) run the integration
tests, each in a Nix `devShell` whose Python environment [`nix/python.nix`](../nix/python.nix)
builds from `uv.lock`:

- `Python client and integration tests` runs `ci-python` ([`nix/ci.nix`](../nix/ci.nix)): `pytest`
  for `nemo-gym`, and `pytest` and `ruff` for `uni-agent` on Python 3.12 in the
  `python-env-uni-agent` environment, with `uni-agent` itself from git at the commit
  [`uni-agent/pyproject.toml`](./uni-agent/pyproject.toml) pins.
- `OpenHands integration` runs `pytest` and `ruff` for `openhands` in the `.#openhands` shell.
- `Harbor integration` runs `pytest` and `ruff` for `harbor` in the `.#harbor` shell; its trial
  test starts containers through Docker.

`openhands-sdk` and `harbor` pull large dependency sets, which is why the last two are jobs of
their own.

## Used by

- `harbor`: [`examples/harbor`](../examples/harbor/README.md) and [`examples/harbor-tools-layer`](../examples/harbor-tools-layer/README.md).
- `nemo-gym`: the [NeMo Gym rollout benchmark](./nemo-gym/benchmarks).
- `openhands`: [`examples/openhands`](../examples/openhands/README.md).
- `uni-agent`: [`examples/Qwen3.5-35B-A3B-verl`](../examples/Qwen3.5-35B-A3B-verl/README.md).

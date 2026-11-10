# Examples

Runnable programs that use the environment API, the Python fleet SDK and the
integrations. They are provided as is: [docs/support.md](../docs/support.md)
explains that examples get no support, no compatibility promise and no security
fixes. Copy what you need. The Go examples are packages of the root module, thus
CI compiles and tests them; the Python examples with a `tests/` directory are
tested in CI too.

Each row names what the example needs beyond a cluster that runs Substrate and
`ate-env-api` ([deployment guide](../README.md#deployment-guide)).

| Example | What it shows | Language | Needs | Entry point |
|---|---|---|---|---|
| [`grpo/`](./grpo/README.md) | A real RL loop: TRL's `GRPOTrainer` trains Qwen2.5-0.5B on MBPP and each completion runs in an environment that computes the reward. Parks the fleet between steps. Records a run report; tracing and metrics are opt-in ([docs/observability.md](../docs/observability.md)). | Python | CPU only; `ate-env-rl`, TRL | `grpo_substrate_train.py`; `report.py` summarizes a run |
| [`guest-daemon/`](./guest-daemon/README.md) | The guest gRPC services (`ProcessService`, `FileSystemService`) from the `guest` package, standalone, without a cluster. Also the target of the Go and Python clients' guest e2e tests. | Go | Nothing | `go run ./examples/guest-daemon`; `Dockerfile` |
| [`harbor/`](./harbor/README.md) | Terminal-Bench 2.0 on ate-env through [`integrations/harbor`](../integrations/harbor/README.md): prebuilt images, resource class templates, `harbor run`, and the comparison with Docker. | Shell | Python 3.12+, `ate-env-harbor`, Harbor, Docker buildx | `run-tb2.sh` |
| [`harbor-tools-layer/`](./harbor-tools-layer/README.md) | Harbor with tools as a second runtime layer: a static `tmux` and `asciinema` layer mounted into every task image and placed first on `PATH`, so `terminus-2` skips per-trial package installs. | Shell, Dockerfile | Python 3.12+, `ate-env-harbor`, Harbor, Docker 28+, an image registry | `demo.sh`, `layer/Dockerfile` |
| [`mcp/`](./mcp/README.md) | An MCP client (the official Go SDK) that connects to an environment's MCP endpoint on `ate-env-api`, lists the tools and calls `shell`. | Go | A port-forward to `ate-env-api` on 7777 | `go run ./examples/mcp` |
| [`openhands/`](./openhands/README.md) | OpenHands' own agents and tools on the agent-server that a runtime layer mounts into any task image, through [`integrations/openhands`](../integrations/openhands/README.md): a CodeAct agent, a conversation, a fleet rollout, and a scripted trajectory runner with a scripted model. | Python, shell | The `oh-agent-server` base template (`demo.sh` registers it); an LLM key, or `scripted_llm.py` | `demo.sh`, `codeact_agent.py`, `convo_with_ate_env.py`, `fleet_rollout.py`, `run_trajectory.py` |
| [`Qwen3-1.7B-verl/`](./Qwen3-1.7B-verl/README.md) | Agentic GRPO on MATH with verl `fully_async_policy`: the model writes Python, `ate_env_tool.py` runs it in an environment, the answer is rewarded. Trainer and rollouts on separate nodes. | Python, YAML, shell | GPU nodes; verl | `deploy.sh`, `job-fully-async.yaml`, `run_fully_async.sh`, `tool_config.yaml` |
| [`Qwen3.5-35B-A3B-verl/`](./Qwen3.5-35B-A3B-verl/README.md) | Agentic GRPO on real SWE tasks: verl `separate_async`, a Megatron trainer, vLLM rollouts, and a uni-agent ReAct agent whose tools and hidden-test grading run in environments through [`integrations/uni-agent`](../integrations/uni-agent/README.md). | Python, YAML, shell | Two 8-GPU nodes; verl, uni-agent | `deploy.sh`, `job-fully-async.yaml`, `run_fully_async.sh`, `task_config.yaml` |
| [`Qwen3.5-397B-A17B-verl/`](./Qwen3.5-397B-A17B-verl/README.md) | The same agentic GRPO loop from 2 to 16 GPU nodes: profiles for Qwen3.5-35B-A3B and Qwen3.5-397B-A17B (Megatron trainer with CPU-offloaded params and optimizer, FP8 vLLM replicas), steps of thousands of trajectories, sandbox sizing and guarded verl patches for 64K responses. The 35B profiles run eight at a time to put one 397B run's load on the same hardware. | Python, YAML, shell | 2 or 16 8-GPU nodes; verl, uni-agent, `crane` | `deploy.sh`, `profiles/*.env`, `job-fully-async.yaml`, `run_fully_async.sh`, `task_config.yaml` |
| [`runtime-layers/`](./runtime-layers/README.md) | An unmodified rootfs image with a second runtime injected as a layer and supervised as a sidecar ([docs/rootfs-images/RUNTIMES.md](../docs/rootfs-images/RUNTIMES.md)). | Shell | `ate-env` and `kubectl-ate` | `demo.sh` |
| [`verl_swebench/`](./verl_swebench/README.md) | A GRPO-style asynchronous rollout loop on Ray that grades SWE-bench candidate patches in environments with the `ate_env_rl` client and its SWE-bench adapter. | Python, YAML | A Ray cluster; `ate-env-rl` | `async_verl_swebench_pipeline.py`, `ray-job.async-verl.yaml` |

Related, outside this directory:

- [`clients/python/examples/quickstart.py`](../clients/python/examples/quickstart.py): the
  Python client end to end against a running `ate-env-api`.
- [`internal/examples/env`](../internal/examples/env): a Go program that walks through the
  `EnvironmentService` lifecycle.
- [`integrations/nemo-gym/benchmarks`](../integrations/nemo-gym/benchmarks) and
  [`integrations/nemo-gym/hack`](../integrations/nemo-gym/hack): the NeMo Gym provider's rollout
  benchmark, driver simulator and runtime-layer demo.

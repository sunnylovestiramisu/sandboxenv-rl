# Reinforcement Learning & Sandbox Fleet Guide

This guide covers how to use high-throughput **Sandbox Fleet Orchestration (`SandboxFleet` & `AsyncSandboxFleet`)** in `ate_env` with distributed Reinforcement Learning frameworks like **VeRL**, **Ray**, and **NeMo-Gym** on top of **Agent Substrate** and **GKE**.

---

## 1. Quickstart

### Synchronous Batch Execution

```python
from ate_env import FleetConfig, SandboxFleet, Task

config = FleetConfig(
    backend="substrate",
    endpoint="localhost:7777",
)

tasks = [
    Task(id="task-1", image="docker.io/library/python:3.11"),
    Task(id="task-2", image="docker.io/library/python:3.11"),
]

with SandboxFleet(config) as fleet:
    sandboxes = fleet.acquire(tasks)
    try:
        for task, sb in zip(tasks, sandboxes):
            res = sb.exec("python3 -c 'print(\"hello world\")'")
            print(f"Task {task.id}: {res.stdout.strip()} (ok={res.ok}, exit_code={res.exit_code})")
    finally:
        fleet.release(sandboxes)
```

---

## 2. Asynchronous Non-Blocking Rollouts (VeRL / vLLM)

In RL post-training (e.g., GRPO), candidate generation overlaps with sandbox pre-warming to hide provisioning latency:

```python
import asyncio
from ate_env import AsyncSandboxFleet, FleetConfig, Task

async def run_rollouts():
    config = FleetConfig(
        backend="substrate",
        endpoint="ate-env-api.ate-env.svc.cluster.local:7777",
        batch_size=4,
        max_warmpool_replicas=4,
    )
    
    tasks = [Task(id=f"t-{i}", image="docker.io/library/python:3.11") for i in range(4)]
    
    fleet = AsyncSandboxFleet(config)
    await fleet.setup(tasks)

    # 1. Start acquiring pre-warmed sandboxes concurrently while LLM generates tokens
    warm_task = asyncio.create_task(fleet.acquire_batch(tasks))
    await asyncio.sleep(1.5)  # Simulate token generation
    sandboxes = await warm_task

    # 2. Asynchronously evaluate candidate rollouts
    for sb in sandboxes:
        async with sb:
            await sb.write_file_async("/tmp/calc.py", "def add(a, b): return a + b\n")
            res = await sb.exec_async(
                ["python3", "-c", "import calc; assert calc.add(2, 3) == 5"], cwd="/tmp", check=False
            )
            print(f"Sandbox {sb.sandbox_id}: ok={res.ok}")

    await fleet.teardown()

asyncio.run(run_rollouts())
```

---

## 3. Architecture & Execution Semantics

The Sandbox Fleet SDK is architected in three clean layers:
- **Layer 1 (`Client` / `Env`)**: Direct asyncio gRPC client talking to `ate-env-api` for lifecycle (create, get, suspend, delete) and guest daemon execution (streaming processes and chunked filesystem I/O).
- **Layer 2 (`SandboxFleet` / `AsyncSandboxFleet`)**: Multi-tenant fleet management, sizing, pre-warming, pooling, and automated lifecycle with task-bound `SandboxHandle`.
- **Layer 3 (`ate_env.adapters`)**: Zero-dependency benchmark task converters and in-sandbox reward evaluators (such as `SweBenchAdapter`) returning typed `EvalResult` outcomes.

### Execution Contract:
- `sb.exec(cmd)` and `sb.exec_async(cmd)` return an `ExecResult(exit_code, stdout, stderr, duration_s, timed_out)`.
- With `check=False`, agent failures never raise: they are reported as `exit_code != 0` or `timed_out=True` (`res.ok` is `False`). With the default `check=True` they raise `CommandExecutionError` / `CommandTimeoutError`.
- Infrastructure and transport errors always raise `InfrastructureError` subclasses (e.g. `SandboxUnavailableError`). Retry them on a fresh sandbox when `err.retryable`, otherwise mask the rollout; never score them as agent errors.

### Design Considerations & Warm Pool Performance

Measured on a GKE dev cluster running Agent Substrate, comparing claims from a warm (suspended) pool against fresh creation from a golden snapshot:
- **Claim from warm (suspended) pool**: ran its first command in **2.6s to 3.7s**.
- **Fresh create from golden snapshot**: took **0.4s** to create plus **2.9s** to first command (**3.3s total**).

The warm pool does not significantly decrease the time to first command because resuming a suspended actor still incurs wake-up and connection establishment latency, while keeping a pool costs suspended-actor capacity and cleanup work. In RL loops the more effective lever is **overlap**: claim the next group's sandboxes while the policy is still decoding (see the example below), so ~3s of provisioning is hidden behind generation either way.

---

## 4. Benchmark Adapters (`ate_env.adapters`)

`SweBenchAdapter` converts SWE-bench records into `Task`s and grades candidate patches the way the official harness does:

```python
from ate_env import SweBenchAdapter

record = {...}  # a Hugging Face SWE-bench row
record["test_cmd"] = SweBenchAdapter.pytest_test_cmd(record, pytest_bin="/opt/miniconda3/envs/testbed/bin/pytest")
task = SweBenchAdapter.to_task(record, image_prefix="docker.io/swebench")

async with await fleet.acquire(task) as sb:
    res = await SweBenchAdapter.evaluate_async(sb, candidate_patch)
    print(res.applied, res.passed, res.reward)
```

1. The candidate patch is applied (`git apply`, falling back to `patch --fuzz`).
2. Files touched by the instance `test_patch` are restored to `base_commit` and `test_patch` is applied, so candidates cannot earn reward by editing the tests that grade them.
3. `test_cmd` (ideally exactly `FAIL_TO_PASS` + `PASS_TO_PASS`) decides the reward: 1.0 iff it exits 0 in time.
4. The working tree is always reset afterwards.

A patch that does not apply, failing tests, and timeouts score 0.0. `InfrastructureError` (including `EvalSetupError` when `test_patch` itself cannot be applied) propagates to the caller.

---

## 5. End-to-End GRPO + Ray Demo on GKE

A runnable example with Ray actors and GRPO advantage computation (simulated policy, real sandboxes) is located in [`examples/verl_swebench`](../../examples/verl_swebench):

* **Local hermetic run**:
  ```bash
  python examples/verl_swebench/async_verl_swebench_pipeline.py --backend mock --num-iters 2 --group-size 2
  ```
* **RayJob on GKE**: see the README for creating the `sandbox-sdk-pkg` ConfigMap and rendering the manifest with `envsubst`.
  See [`examples/verl_swebench/README.md`](../../examples/verl_swebench/README.md) for full deployment instructions.

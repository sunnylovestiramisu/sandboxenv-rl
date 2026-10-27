# RL Rollouts & Benchmark Adapters Guide

This guide covers what is specific to Reinforcement Learning with `ate_env`: overlapping sandbox claims with policy decoding in **VeRL** / **Ray** rollouts on **Agent Substrate** and **GKE**, and grading candidates with the benchmark adapters (`ate_env.adapters`).

For everything else (installing and connecting, `FleetConfig`, concurrency and pool sizing, run ids, errors, and observability), see the client [README](README.md).

---

## 1. Asynchronous Non-Blocking Rollouts (VeRL / vLLM)

In RL post-training (e.g., GRPO), candidate generation overlaps with sandbox claims to hide provisioning latency:

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
    try:
        await fleet.setup(tasks)

        # 1. Claim sandboxes concurrently while the LLM generates tokens.
        claim = asyncio.create_task(fleet.acquire_batch(tasks))
        await asyncio.sleep(1.5)  # Simulate token generation
        sandboxes = await claim

        # 2. Evaluate the candidates concurrently. Each handle is released when
        #    its block exits, also when another evaluation raises.
        async def evaluate(sb):
            async with sb:
                await sb.write_file_async("/tmp/calc.py", "def add(a, b): return a + b\n")
                res = await sb.exec_async(
                    ["python3", "-c", "import calc; assert calc.add(2, 3) == 5"], cwd="/tmp", check=False
                )
                return sb.sandbox_id, res.ok

        for sandbox_id, ok in await asyncio.gather(*(evaluate(sb) for sb in sandboxes)):
            print(f"Sandbox {sandbox_id}: ok={ok}")
    finally:
        await fleet.teardown()
        fleet.close()

asyncio.run(run_rollouts())
```

`acquire_batch` is all-or-nothing: when one claim fails, it releases the sandboxes it already claimed and raises the first error.

---

## 2. Architecture & Execution Semantics

The SDK has three layers:
- **Layer 1 (`Client` / `Env`)**: Direct asyncio gRPC client talking to `ate-env-api` for lifecycle (create, get, suspend, delete) and guest daemon execution (streaming processes and chunked filesystem I/O).
- **Layer 2 (`SandboxFleet` / `AsyncSandboxFleet`)**: Fleet management, sizing, pre-warming, pooling, and automated lifecycle with task-bound `SandboxHandle`.
- **Layer 3 (`ate_env.adapters`)**: Zero-dependency benchmark task converters and in-sandbox reward evaluators (such as `SweBenchAdapter`) returning typed `EvalResult` outcomes.

### Execution Contract:
- `sb.exec(cmd)` and `sb.exec_async(cmd)` return an `ExecResult(exit_code, stdout, stderr, duration_s, timed_out)`.
- With `check=False`, agent failures never raise: they are reported as `exit_code != 0` or `timed_out=True` (`res.ok` is `False`). With the default `check=True` they raise `CommandExecutionError` / `CommandTimeoutError`.
- Infrastructure and transport errors always raise `InfrastructureError` subclasses (e.g. `SandboxUnavailableError`). Retry them on a fresh sandbox when `err.retryable`, otherwise mask the rollout; never score them as agent errors.

### Design Considerations & Warm Pool Performance

Measured on a GKE dev cluster running Agent Substrate, comparing claims from a warm (suspended) pool against fresh creation from a golden snapshot:
- **Claim from warm (suspended) pool**: ran its first command in **2.6s to 3.7s**.
- **Fresh create from golden snapshot**: took **0.4s** to create plus **2.9s** to first command (**3.3s total**).

The warm pool does not significantly decrease the time to first command because resuming a suspended actor still incurs wake-up and connection establishment latency, while keeping a pool costs suspended-actor capacity and cleanup work. In RL loops the more effective lever is **overlap**: claim the next group's sandboxes while the policy is still decoding (see the example above), so ~3s of provisioning is hidden behind generation either way. A claim returns once the environment exists; it becomes ready on its first guest call, so that first call carries the wake time.

---

## 3. Benchmark Adapters (`ate_env.adapters`)

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

1. A candidate patch that touches a test-configuration file (`SweBenchAdapter.PROTECTED_FILES`: `conftest.py` in any directory, `pytest.ini`, `tox.ini`, `setup.cfg`, `pyproject.toml`) is rejected and scores 0.0. Pass `protected_files=()` to allow them, for example for a dataset whose reference patches edit `setup.cfg`.
2. The candidate patch is applied (`git apply`, falling back to `patch --fuzz`).
3. Files touched by the instance `test_patch` are restored to `base_commit` (removed when they do not exist there), and `test_patch` is applied. A `base_commit` that is not in the repository is an `EvalSetupError`.
4. `test_cmd` (ideally exactly `FAIL_TO_PASS` + `PASS_TO_PASS`) decides the reward: 1.0 iff it exits 0 in time.
5. The index and the working tree are always reset afterwards (`git reset --hard`, then `git clean`). A reset that fails is logged; release that sandbox instead of recycling it.

Steps 1 and 3 keep a policy from editing the graded tests or the test configuration. They do not make the grade tamper-proof: the candidate still controls the code under test, and anything that test collection imports (for example a module that patches pytest when it is imported). The official SWE-bench harness has the same limit. Treat the reward as a training signal, and audit high-reward candidates before trusting them.

A patch that does not apply, a rejected patch, failing tests, and timeouts score 0.0. `InfrastructureError` (including `EvalSetupError` when the test files cannot be restored or `test_patch` cannot be applied) propagates to the caller.

---

## 4. End-to-End GRPO + Ray Demo on GKE

A runnable example with Ray actors and GRPO advantage computation (simulated policy, real sandboxes) is located in [`examples/verl_swebench`](../../examples/verl_swebench):

* **Local hermetic run**:
  ```bash
  python examples/verl_swebench/async_verl_swebench_pipeline.py --backend mock --num-iters 2 --group-size 2
  ```
* **RayJob on GKE**: see the README for creating the `sandbox-sdk-pkg` ConfigMap and rendering the manifest with `envsubst`.
  See [`examples/verl_swebench/README.md`](../../examples/verl_swebench/README.md) for full deployment instructions.

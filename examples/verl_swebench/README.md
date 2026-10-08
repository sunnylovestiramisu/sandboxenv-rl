# Async GRPO Rollouts: Ray + SWE-bench on `ate_env`

This example runs a **GRPO-style RL rollout loop** (VeRL-shaped) on **Ray**, grading
**SWE-bench** candidate patches inside **Agent Substrate** sandboxes via the
`ate_env` Python client (`clients/python`).

> [!NOTE]
> The **policy side is simulated**: the sampler returns fixed candidates instead of
> running vLLM, `DataProto` is a minimal stand-in for `verl.DataProto`, and no
> gradient step is taken. The **sandbox side is real** with `--backend substrate`.
> The point of the example is the rollout/evaluation plumbing.

---

## Architecture Overview

```
                                    Ray Cluster
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                 │
│   [ Trainer actor ]                              [ Sampler actor ]              │
│   - GRPO advantage over scored rollouts          - Simulated decoding of G      │
│                                                    candidates                   │
│            ▲                                            │  (concurrently)       │
│            │ rewards                                    ▼                       │
│   [ Evaluator actor: AsyncSandboxFleet ]  ◀── acquire G sandboxes while decoding│
│   - grades G candidates concurrently with SweBenchAdapter                       │
│   - retries InfrastructureError on a fresh sandbox, else masks the rollout      │
└─────────────────────────────────────────┬───────────────────────────────────────┘
                                          │ ate_env (gRPC)
            ┌─────────────────────────────┴─────────────────────────────┐
            ▼ --backend substrate                                       ▼ --backend mock
  Agent Substrate (ate-system)                                 In-memory mock backend
  - sandboxes from the SWE-bench eval image/template          - scripted guest verdicts
  - ate-env process & file streaming                          - no cluster needed
```

Each iteration:

1. **Decode + acquire in parallel.** The sampler decodes G candidates while the
   evaluator claims G sandboxes (`AsyncSandboxFleet.acquire_batch`), so
   provisioning latency (~3s on Substrate) is hidden behind generation.
2. **Grade concurrently.** For each candidate, `SweBenchAdapter.evaluate_async`
   applies the patch, restores the files touched by the instance `test_patch`,
   applies `test_patch`, and runs `FAIL_TO_PASS` + `PASS_TO_PASS`. Reward is 1.0
   iff they all pass.
3. **Mask infrastructure failures.** `InfrastructureError` is retried once on a
   fresh sandbox. If it still fails, the rollout is masked (excluded from the
   advantage) instead of being scored 0.
4. **GRPO advantage.** Rewards are normalized within the group.

The default instance is
[`pytest-dev__pytest-5221`](pytest-dev__pytest-5221.json) (SWE-bench Lite). The
simulated sampler cycles through the **gold patch** (passes), a **plausible but
wrong** variant of it that still applies (fails tests), and a **malformed patch**
(does not apply).

---

## Requirements

- `ray` and `torch` (plus the `ate_env` client: `pip install -e clients/python`,
  or run from a checkout and the script adds `clients/python/src` to `sys.path`).
- For `--backend substrate`: a reachable `ate-env-api`
  (`SUBSTRATE_API_ENDPOINT`, default
  `ate-env-api.ate-env.svc.cluster.local:7777`) and a SWE-bench eval image or
  ActorTemplate for the instance.

| Env var | Default | Meaning |
|---|---|---|
| `SUBSTRATE_API_ENDPOINT` | `ate-env-api.ate-env.svc.cluster.local:7777` | ate-env API |
| `SUBSTRATE_ATESPACE` | `ate-env` | Tenancy / atespace |
| `SWEBENCH_IMAGE` | `docker.io/swebench/sweb.eval.x86_64.<instance>:latest` | Sandbox image |
| `SWEBENCH_TEMPLATE` | _(derived from image)_ | Pre-built ActorTemplate for the image |
| `SWEBENCH_PYTEST` | `/opt/miniconda3/envs/testbed/bin/pytest` | pytest inside the image |
| `SMOKE_IMAGE` / `SMOKE_TEMPLATE` | `python:3.10-slim` / `default-template` | Smoke task sandbox |

---

## Running the Demo

### 1. Local hermetic run (no cluster)

```bash
cd examples/verl_swebench
python3 async_verl_swebench_pipeline.py --backend mock --task-type swebench --num-iters 2 --group-size 4
```

The mock backend cannot run git or pytest. The script programs the mock guest so
that only the reference solution passes, which keeps the GRPO signal meaningful.

### 2. Live run against Agent Substrate

```bash
export SWEBENCH_IMAGE=<registry>/sweb.eval.x86_64.pytest-dev_1776_pytest-5221:latest
export SWEBENCH_TEMPLATE=<actor-template-for-that-image>   # optional
python3 async_verl_swebench_pipeline.py --backend substrate --task-type swebench --num-iters 3 --group-size 4
```

Use `--task-type smoke` for a quick end-to-end check with a plain Python image.
Use `--instance-file <row.json>` to grade a different SWE-bench instance.

### 3. RayJob on GKE

The manifest mounts this repository from a ConfigMap and is parameterized with
`envsubst`:

```bash
# From the repository root: package the client + example (ConfigMaps are limited to 1 MiB).
tar czf - clients/python/src examples/verl_swebench | base64 -w0 > /tmp/pkg.tar.gz.b64
kubectl create configmap sandbox-sdk-pkg --from-file=pkg.tar.gz.b64=/tmp/pkg.tar.gz.b64 \
  --dry-run=client -o yaml | kubectl apply -f -

export VERL_WORKER_IMAGE=<registry>/verl-worker:<tag>   # python3 + ray + torch
export SWEBENCH_IMAGE=<registry>/sweb.eval.x86_64.pytest-dev_1776_pytest-5221:latest
export SWEBENCH_TEMPLATE=<actor-template-for-that-image>
envsubst < examples/verl_swebench/ray-job.async-verl.yaml | kubectl apply -f -
```

The job runs on a CPU head node plus one `cpu-evaluators` worker. The evaluator
actor is pinned to that worker through the `sandbox_evaluator` custom resource.
No GPUs are requested because the policy is simulated. To plug in a real policy,
add a GPU worker group and pass `--sampler-gpus` / `--trainer-gpus`. The
RayCluster is deleted 5 minutes after the job finishes
(`shutdownAfterJobFinishes`).

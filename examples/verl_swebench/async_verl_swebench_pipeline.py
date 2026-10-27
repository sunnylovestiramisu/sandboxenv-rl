#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Asynchronous GRPO rollout loop (VeRL-style) with Ray + SWE-bench on ate_env.

Demonstrates:
1. Overlapping LLM token generation (sampler) with sandbox acquisition: the G
   sandboxes for an iteration are claimed via ``AsyncSandboxFleet.acquire_batch``
   *while* the sampler is decoding.
2. Concurrent, harness-faithful grading of the G candidates with
   ``SweBenchAdapter`` (candidate patch -> restore tests -> test_patch ->
   FAIL_TO_PASS + PASS_TO_PASS).
3. Infrastructure vs. agent failures: ``InfrastructureError`` is retried on a
   fresh sandbox and, if still failing, the rollout is *masked* (excluded from
   the advantage), never scored as reward 0.
4. Group-relative (GRPO) advantage computation over the scored rollouts.
5. Observability (``docs/observability.md``): the evaluator's fleet records a
   ``RunReport`` (phases, claims, tasks) and Prometheus series; with
   ``ENABLE_TRACING=1`` and ``OTEL_EXPORTER_OTLP_ENDPOINT`` set, every iteration
   is one trace: ``verl.iteration`` (driver) -> ``ate_env_fleet.acquire`` /
   ``ate_env_fleet.process`` (evaluator) -> ``ate_env_client.*`` -> ate-env-api ->
   Substrate. ``REPORT_DIR`` receives the report as ``.txt`` and ``.json``.

The policy side is simulated: the sampler returns fixed candidates (the gold
patch, a plausible-but-wrong patch, and a malformed patch) instead of running
vLLM, and ``DataProto`` is a minimal stand-in for ``verl.DataProto``. The
sandbox side is real when ``--backend substrate`` is used.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json
import os
import re
import sys
import time
import uuid
from collections.abc import Iterator
from typing import Any

import ray
import torch

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON_SRC = os.path.abspath(os.path.join(SCRIPT_DIR, "../../clients/python/src"))
if os.path.isdir(PYTHON_SRC) and PYTHON_SRC not in sys.path:
    sys.path.insert(0, PYTHON_SRC)

from ate_env import AsyncSandboxFleet, FleetConfig, ObservabilityConfig, SandboxHandle  # noqa: E402
from ate_env.adapters import SweBenchAdapter  # noqa: E402
from ate_env.errors import InfrastructureError  # noqa: E402
from ate_env.observability import repo_family  # noqa: E402
from ate_env.runtime.mock import MockRuntimeHook  # noqa: E402
from ate_env.types import ExecResult, Task  # noqa: E402

DEFAULT_INSTANCE_FILE = os.path.join(SCRIPT_DIR, "pytest-dev__pytest-5221.json")
# Official SWE-bench eval images install the repo's env as conda env "testbed".
DEFAULT_PYTEST = "/opt/miniconda3/envs/testbed/bin/pytest"

SMOKE_CHECK = "from calc import add; assert add(2, 3) == 5; print('SMOKE_OK')"
SMOKE_GOOD = "def add(a, b):\n    return a + b\n"
SMOKE_BAD = "def add(a, b):\n    return a - b\n"
MALFORMED_PATCH = "this is not a unified diff\n"

# Attempts per candidate before an InfrastructureError masks the rollout.
MAX_INFRA_ATTEMPTS = 2

# Markers around the machine-readable run outputs in the driver log, so a
# RayJob's logs can be turned into report inputs (see README, "Observability").
REPORT_MARKER = "=== ATE_ENV_RUN_REPORT_JSON ==="
METRICS_BEGIN = "=== ATE_ENV_METRICS_BEGIN ==="
METRICS_END = "=== ATE_ENV_METRICS_END ==="


# ==============================================================================
# Observability helpers (no-ops unless ENABLE_TRACING=1 and the tracing extra)
# ==============================================================================
def tracing_enabled() -> bool:
    return os.environ.get("ENABLE_TRACING", "").lower() in ("1", "true", "yes")


def _otel() -> Any:
    """The OpenTelemetry API modules, or None (tracing off or extra missing)."""
    if not tracing_enabled():
        return None
    try:
        from opentelemetry import context, trace
        from opentelemetry.trace.propagation.tracecontext import (
            TraceContextTextMapPropagator,
        )
    except ImportError:
        return None
    return context, trace, TraceContextTextMapPropagator()


def setup_tracing(service_name: str) -> None:
    """Install this process's OpenTelemetry SDK provider (OTLP/gRPC exporter).

    The ate_env client never installs the global provider: it sends its spans
    to the application's provider once one is installed. The pipeline is the
    application here, so each process (the driver and the evaluator actor)
    installs one. The driver's spans are then real, their context reaches the
    actors, and the fleet and client spans join the same trace. The exporter
    reads OTEL_EXPORTER_OTLP_ENDPOINT and the other standard variables.
    """
    otel = _otel()
    if otel is None:
        return
    trace = otel[1]
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    if isinstance(trace.get_tracer_provider(), TracerProvider):
        return  # already installed in this process
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


@contextlib.contextmanager
def driver_span(name: str, **attrs: Any) -> Iterator[dict[str, str]]:
    """Open a driver-side span; yields the W3C carrier to hand to Ray actors."""
    otel = _otel()
    if otel is None:
        yield {}
        return
    _, trace, prop = otel
    with trace.get_tracer("verl_swebench").start_as_current_span(name, attributes=attrs):
        carrier: dict[str, str] = {}
        prop.inject(carrier)
        yield carrier


@contextlib.contextmanager
def remote_parent(carrier: dict[str, str] | None) -> Iterator[None]:
    """Continue the driver's trace inside a Ray actor method."""
    otel = _otel()
    if otel is None or not carrier:
        yield
        return
    context, _, prop = otel
    token = context.attach(prop.extract(carrier))
    try:
        yield
    finally:
        context.detach(token)


def flush_spans() -> None:
    """Flush buffered spans; Ray may not run atexit hooks in actors."""
    otel = _otel()
    if otel is None:
        return
    provider = otel[1].get_tracer_provider()
    flush = getattr(provider, "force_flush", None)
    if flush is not None:
        flush(10_000)


def metrics_text() -> str:
    """This process's Prometheus exposition (empty without the metrics extra)."""
    try:
        import prometheus_client
    except ImportError:
        return ""
    return prometheus_client.generate_latest().decode()


class DataProto:
    """Minimal stand-in for ``verl.DataProto`` (tensor batch + meta_info)."""

    def __init__(self, batch: dict[str, Any], meta_info: dict[str, Any] | None = None):
        self.batch = batch
        self.meta_info = meta_info or {}

    @classmethod
    def from_dict(cls, tensors: dict[str, torch.Tensor]) -> DataProto:
        return cls(batch=tensors)


# ==============================================================================
# Task definitions
# ==============================================================================
def load_task_dict(task_type: str, instance_file: str) -> dict[str, Any]:
    """Return a task record for ``task_type`` ('swebench' or 'smoke')."""
    if task_type == "smoke":
        return {
            "instance_id": "calc-smoke",
            "image": os.environ.get("SMOKE_IMAGE", "python:3.10-slim"),
            "template": os.environ.get("SMOKE_TEMPLATE", "default-template"),
            "reference": SMOKE_GOOD,
        }

    with open(instance_file) as f:
        record = json.load(f)
    record.setdefault(
        "image",
        os.environ.get("SWEBENCH_IMAGE") or SweBenchAdapter.image_for(record["instance_id"]),
    )
    if os.environ.get("SWEBENCH_TEMPLATE"):
        record.setdefault("template", os.environ["SWEBENCH_TEMPLATE"])
    record.setdefault(
        "test_cmd",
        SweBenchAdapter.pytest_test_cmd(
            record, pytest_bin=os.environ.get("SWEBENCH_PYTEST", DEFAULT_PYTEST)
        ),
    )
    record["reference"] = record["patch"]
    return record


def to_task(task_dict: dict[str, Any]) -> Task:
    return SweBenchAdapter.to_task(task_dict)


def drop_first_added_line(diff: str) -> str | None:
    """Make a plausible-but-wrong variant of ``diff`` that still applies.

    Removes the first added line of the first hunk and fixes the hunk header.
    """
    lines = diff.splitlines(keepends=True)
    hunk = None
    for i, line in enumerate(lines):
        if line.startswith("@@"):
            hunk = i
        elif hunk is not None and line.startswith("+") and not line.startswith("+++"):
            m = re.match(r"@@ -(\d+(?:,\d+)?) \+(\d+),(\d+) @@", lines[hunk])
            if not m:
                return None
            lines[hunk] = (
                f"@@ -{m.group(1)} +{m.group(2)},{int(m.group(3)) - 1} @@"
                + lines[hunk][m.end():]
            )
            del lines[i]
            return "".join(lines)
    return None


# ==============================================================================
# Candidate scoring (runs inside the evaluator actor)
# ==============================================================================
def emulate_guest_for_mock(
    handle: SandboxHandle, task_dict: dict[str, Any], candidate: str, task_type: str
) -> None:
    """``--backend mock`` has no real guest, so script the guest's verdict.

    The mock runtime cannot run git or pytest; we program it so that only the
    reference solution passes, a malformed patch fails to apply, and anything
    else fails its tests. This keeps the GRPO signal meaningful in local runs.
    """
    if not isinstance(handle.runtime, MockRuntimeHook):
        return
    correct = candidate == task_dict["reference"]
    verdict = ExecResult(
        exit_code=0 if correct else 1,
        stdout="SMOKE_OK\n=== passed ===\n" if correct else "=== 1 failed ===\n",
        stderr="",
    )
    if task_type == "smoke":
        handle.runtime.set_response(SMOKE_CHECK, verdict)
        return
    applies = candidate.startswith("diff --git")
    handle.runtime.set_response(
        f"git apply --verbose {SweBenchAdapter.PATCH_PATH}",
        ExecResult(exit_code=0 if applies else 1, stdout="", stderr="" if applies else "corrupt patch"),
    )
    handle.runtime.set_response(task_dict["test_cmd"], verdict)


async def score_candidate(
    handle: SandboxHandle, task_dict: dict[str, Any], candidate: str, task_type: str
) -> dict[str, Any]:
    """Grade one candidate in ``handle``. Raises ``InfrastructureError`` on infra failures."""
    emulate_guest_for_mock(handle, task_dict, candidate, task_type)

    if task_type == "swebench":
        res = await SweBenchAdapter.evaluate_async(handle, candidate)
        return {
            "applied": res.applied,
            "tests_passed": res.passed,
            "timed_out": res.timed_out,
            "reward": res.reward,
            "test_output": res.logs[-500:],
        }

    # Smoke: write the candidate module and run a check against it.
    await handle.write_file_async("/tmp/calc.py", candidate)
    res = await handle.exec_async(["python3", "-c", SMOKE_CHECK], cwd="/tmp", check=False)
    passed = res.ok and "SMOKE_OK" in res.stdout
    return {
        "applied": True,
        "tests_passed": passed,
        "timed_out": res.timed_out,
        "reward": 1.0 if passed else 0.0,
        "test_output": (res.stdout + res.stderr)[-500:],
    }


# ==============================================================================
# Ray Sampler Worker (simulated vLLM policy decoding)
# ==============================================================================
@ray.remote
class AsyncVeRLSamplerWorker:
    def __init__(self, model_name: str = "deepseek-coder-7b"):
        self.model_name = model_name
        ctx = ray.get_runtime_context()
        print(f"[Sampler Worker] Node: {ctx.get_node_id()} | Model: {model_name} (simulated)")

    async def generate_rollouts_async(
        self, task: dict[str, Any], group_size: int, latency_s: float
    ) -> tuple[list[str], DataProto]:
        print(f"[Sampler Worker] Decoding {group_size} rollouts (simulated, ~{latency_s:g}s)...")
        await asyncio.sleep(latency_s)

        reference = task["reference"]
        if task["instance_id"] == "calc-smoke":
            pool = [SMOKE_GOOD, SMOKE_BAD]
        else:
            pool = [reference, drop_first_added_line(reference) or MALFORMED_PATCH, MALFORMED_PATCH]
        patches = list(itertools.islice(itertools.cycle(pool), group_size))

        prompt_len, response_len = 32, 64
        data_proto = DataProto.from_dict(
            {
                "prompts": torch.randint(100, 1000, (group_size, prompt_len)),
                "responses": torch.randint(100, 1000, (group_size, response_len)),
                "attention_mask": torch.ones((group_size, prompt_len + response_len), dtype=torch.int64),
            }
        )
        data_proto.meta_info.update(task_id=task["instance_id"], group_size=group_size)
        return patches, data_proto


# ==============================================================================
# Ray Trainer Worker (GRPO advantage)
# ==============================================================================
@ray.remote
class AsyncVeRLTrainerWorker:
    def __init__(self, lr: float = 1e-5):
        self.lr = lr
        self.step = 0
        ctx = ray.get_runtime_context()
        print(f"[Trainer Worker] Node: {ctx.get_node_id()} | LR: {lr}")

    def compute_grpo_update(
        self, data_proto: DataProto, eval_results: list[dict[str, Any]]
    ) -> dict[str, Any]:
        self.step += 1
        # Masked rollouts (infrastructure failures) carry no reward signal: they
        # get advantage 0 and mask False, so the outputs stay aligned with the
        # DataProto rows (one entry per candidate, in order).
        mask = torch.tensor([r["status"] == "scored" for r in eval_results], dtype=torch.bool)
        rewards = torch.tensor([float(r["reward"] or 0.0) for r in eval_results], dtype=torch.float32)
        scored = rewards[mask]
        adv = torch.zeros_like(rewards)
        if len(scored) >= 2:
            mean = scored.mean()
            adv[mask] = (scored - mean) / (scored.std(unbiased=False) + 1e-6)
        else:
            mean = scored.mean() if len(scored) else torch.tensor(0.0)

        print(
            f"[Trainer Worker] Step {self.step}: scored={int(mask.sum())}/{len(eval_results)} "
            f"mean_reward={mean.item():.2f} adv={[round(a, 3) for a in adv.tolist()]} "
            f"mask={mask.tolist()}"
        )
        return {
            "step": self.step,
            "mean_reward": float(mean.item()),
            "advantages": adv.tolist(),
            "mask": mask.tolist(),
            "num_masked": len(eval_results) - int(mask.sum()),
        }


# ==============================================================================
# Async Evaluator Actor (owns the AsyncSandboxFleet)
# ==============================================================================
@ray.remote
class AsyncEvaluationPoolActor:
    """Owns an ``AsyncSandboxFleet``; leases sandboxes and grades candidates."""

    def __init__(self, fleet_cfg_dict: dict[str, Any], task_type: str):
        cfg = FleetConfig.from_dict(fleet_cfg_dict)
        setup_tracing(cfg.observability.trace_service_name)
        self.fleet = AsyncSandboxFleet(cfg)
        self.task_type = task_type
        self._leases: dict[str, list[SandboxHandle]] = {}
        self._run_done: asyncio.Event | None = None
        self._run_task: asyncio.Task | None = None
        self._report: Any = None

    async def _run_scope(self, started: asyncio.Event) -> None:
        # The whole job is one fleet run: the RunReport collects every phase,
        # claim and task until teardown. The run lives in its own task so its
        # span never becomes the parent of the per-iteration traces.
        assert self._run_done is not None
        with self.fleet.observer.run("verl-grpo", backend=self.fleet._backend_label) as report:
            self._report = report
            started.set()
            await self._run_done.wait()

    async def setup(self, task_dict: dict[str, Any], carrier: dict[str, str] | None = None) -> None:
        """Size and pre-warm the pool for the task's template."""
        self._run_done = asyncio.Event()
        started = asyncio.Event()
        self._run_task = asyncio.create_task(self._run_scope(started))
        await started.wait()
        with remote_parent(carrier):
            await self.fleet.setup([to_task(task_dict)])
        self._report.environment = self.fleet._fleet.describe_environment(
            concurrency=self.fleet.config.batch_size
        )

    async def acquire(
        self, task_dict: dict[str, Any], count: int, carrier: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """Claim ``count`` sandboxes now and hold them under a lease id.

        ``acquire_s`` is the claim (the create or warm-pool RPCs) only: an
        environment becomes ready on its first guest call, so its wake time
        lands in the first evaluation command, not here.
        """
        t0 = time.monotonic()
        with remote_parent(carrier):
            handles = await self.fleet.acquire_batch([to_task(task_dict)] * count)
        lease_id = uuid.uuid4().hex
        self._leases[lease_id] = handles
        return {"lease_id": lease_id, "acquire_s": time.monotonic() - t0}

    async def _evaluate_one(
        self, idx: int, handle: SandboxHandle | None, task_dict: dict[str, Any], candidate: str
    ) -> dict[str, Any]:
        t0 = time.monotonic()
        last_err: InfrastructureError | None = None
        attempt = 0
        for attempt in range(1, MAX_INFRA_ATTEMPTS + 1):
            try:
                if handle is None:  # retry on a fresh sandbox
                    handle = await self.fleet.acquire(to_task(task_dict))
                obs = self.fleet.observer
                backend = self.fleet._backend_label
                family = repo_family(to_task(task_dict))
                t_task = time.monotonic()
                status = "error"
                try:
                    with obs.phase("process", backend=backend, family=family):
                        outcome = await score_candidate(
                            handle, task_dict, candidate, self.task_type
                        )
                    status = "ok"
                finally:
                    obs.task_done(backend, family, status, time.monotonic() - t_task)
                    # Through the fleet, so the release is a recorded phase.
                    await self.fleet.release(handle)
                return {
                    "candidate_id": idx,
                    "sandbox_id": handle.sandbox_id,
                    "status": "scored",
                    "attempts": attempt,
                    "eval_s": round(time.monotonic() - t0, 3),
                    **outcome,
                }
            except InfrastructureError as err:
                last_err = err
                print(f"[Evaluator] candidate {idx} attempt {attempt}: infra error: {err!r}")
                handle = None
                if not err.retryable:
                    break
        return {
            "candidate_id": idx,
            "sandbox_id": None,
            "status": "masked",
            "attempts": attempt,
            "eval_s": round(time.monotonic() - t0, 3),
            "error": repr(last_err),
            "applied": False,
            "tests_passed": False,
            "timed_out": False,
            "reward": None,
        }

    async def evaluate(
        self,
        lease_id: str,
        task_dict: dict[str, Any],
        candidates: list[str],
        carrier: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Grade candidates concurrently, one leased sandbox each."""
        with remote_parent(carrier):
            return await self._evaluate(lease_id, task_dict, candidates)

    async def _evaluate(
        self, lease_id: str, task_dict: dict[str, Any], candidates: list[str]
    ) -> list[dict[str, Any]]:
        handles = self._leases.pop(lease_id)
        if len(handles) != len(candidates):
            await asyncio.gather(*(self.fleet.release(h) for h in handles), return_exceptions=True)
            raise ValueError(f"lease has {len(handles)} sandboxes for {len(candidates)} candidates")
        # Each _evaluate_one releases its own sandbox (``async with``). Wait for
        # all of them before surfacing an unexpected error, so none are cut off
        # mid-evaluation.
        results = await asyncio.gather(
            *(
                self._evaluate_one(i + 1, h, task_dict, c)
                for i, (h, c) in enumerate(zip(handles, candidates))
            ),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, BaseException):
                raise r
        return list(results)  # type: ignore[arg-type]

    async def teardown(self, carrier: dict[str, str] | None = None) -> dict[str, Any]:
        """Release everything, end the run and return its report and metrics."""
        try:
            with remote_parent(carrier):
                for handles in self._leases.values():
                    await asyncio.gather(
                        *(self.fleet.release(h) for h in handles), return_exceptions=True
                    )
                self._leases.clear()
                await self.fleet.teardown()
        finally:
            if self._run_done is not None and self._run_task is not None:
                self._run_done.set()
                await self._run_task
            self.fleet.close()
            flush_spans()
        report = self._report
        out: dict[str, Any] = {"metrics": metrics_text(), "report": None, "summary": ""}
        if report is not None:
            out["report"] = report.to_dict()
            out["summary"] = report.summary()
            if os.environ.get("REPORT_DIR"):
                report.write(os.environ["REPORT_DIR"])
        return out


# ==============================================================================
# Main Orchestrator Loop
# ==============================================================================
def build_fleet_config(args: argparse.Namespace, task_dict: dict[str, Any]) -> FleetConfig:
    image_templates = {}
    if task_dict.get("template"):
        image_templates[task_dict["image"]] = task_dict["template"]
    return FleetConfig(
        observability=ObservabilityConfig(enable_tracing=tracing_enabled()),
        backend=args.backend,
        endpoint=os.environ.get("SUBSTRATE_API_ENDPOINT", "ate-env-api.ate-env.svc.cluster.local:7777"),
        data_plane=args.data_plane,
        tenancy=os.environ.get("SUBSTRATE_ATESPACE", "ate-env"),
        template=task_dict.get("template"),
        image_templates=image_templates,
        batch_size=args.group_size,
        max_warmpool_replicas=args.group_size,
        worker_family=os.environ.get("SANDBOX_WORKER_FAMILY", "c2"),
    )


async def main_async(args: argparse.Namespace) -> None:
    print("=" * 75)
    print("  Async GRPO rollouts: Ray + SWE-bench on ate_env")
    print(f"  Backend: {args.backend} | Task: {args.task_type} | Group Size: {args.group_size}")
    print("=" * 75)

    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    setup_tracing("verl-trainer")

    task_dict = load_task_dict(args.task_type, args.instance_file)
    fleet_cfg = build_fleet_config(args, task_dict)

    sampler = AsyncVeRLSamplerWorker.options(num_gpus=args.sampler_gpus).remote()
    trainer = AsyncVeRLTrainerWorker.options(num_gpus=args.trainer_gpus).remote()
    eval_opts: dict[str, Any] = {}
    if args.evaluator_resource:
        # Pin the evaluator to nodes advertising this custom resource
        # (the cpu-evaluators group in ray-job.async-verl.yaml).
        eval_opts["resources"] = {args.evaluator_resource: 0.01}
    eval_pool = AsyncEvaluationPoolActor.options(**eval_opts).remote(
        fleet_cfg.model_dump(), args.task_type
    )

    result: dict[str, Any] | None = None
    try:
        print("\n[Orchestrator] Initializing fleet & sizing warm pool...")
        with driver_span("verl.setup") as carrier:
            await eval_pool.setup.remote(task_dict, carrier)

        for iteration in range(1, args.num_iters + 1):
            print(f"\n────────────────── Iteration {iteration}/{args.num_iters} ──────────────────")
            t_iter = time.monotonic()
            with driver_span("verl.iteration", iteration=iteration,
                             group_size=args.group_size) as carrier:
                eval_results, data_proto = await run_iteration(
                    sampler, eval_pool, task_dict, args, carrier, t_iter
                )
                with driver_span("verl.train_step"):
                    train_res = await trainer.compute_grpo_update.remote(data_proto, eval_results)
            print(
                f"[Iter {iteration} Summary] Total: {time.monotonic() - t_iter:.2f}s | "
                f"Mean Reward: {train_res['mean_reward']:.2f} | Masked: {train_res['num_masked']}"
            )
    finally:
        print("\n[Orchestrator] Tearing down fleet...")
        with driver_span("verl.teardown") as carrier:
            result = await eval_pool.teardown.remote(carrier)
        flush_spans()
    if result is not None:
        emit_run_outputs(result)
    print("✨ Async pipeline finished.")


async def run_iteration(
    sampler: Any,
    eval_pool: Any,
    task_dict: dict[str, Any],
    args: argparse.Namespace,
    carrier: dict[str, str],
    t_iter: float,
) -> tuple[list[dict[str, Any]], Any]:
    # 1+2. Decode and claim sandboxes at the same time.
    sample_ref = sampler.generate_rollouts_async.remote(
        task_dict, args.group_size, args.sample_latency_s
    )
    lease_ref = eval_pool.acquire.remote(task_dict, args.group_size, carrier)
    (candidates, data_proto), lease = await asyncio.gather(sample_ref, lease_ref)
    t_ready = time.monotonic() - t_iter
    print(
        f"[Orchestrator] {len(candidates)} candidates + sandboxes ready in {t_ready:.2f}s "
        f"(sandbox claim took {lease['acquire_s']:.2f}s, overlapped with decoding; "
        "readiness happens on the first command)"
    )

    # 3. Grade all candidates concurrently.
    eval_results = await eval_pool.evaluate.remote(
        lease["lease_id"], task_dict, candidates, carrier
    )
    for r in eval_results:
        if r["status"] == "masked":
            icon = "⚠️ MASKED"
        elif r["tests_passed"]:
            icon = "✅ PASS"
        elif not r["applied"]:
            icon = "❌ NO-APPLY"
        else:
            icon = "❌ FAIL"
        print(
            f"  Cand #{r['candidate_id']} | {icon} | Eval: {r['eval_s']}s | "
            f"Attempts: {r['attempts']} | Sandbox: {r['sandbox_id']}"
        )
    return eval_results, data_proto


def emit_run_outputs(result: dict[str, Any]) -> None:
    """Print the run report and the evaluator's metrics; copy them to REPORT_DIR."""
    if result.get("summary"):
        print("\n" + result["summary"])
    if result.get("report") is not None:
        print(REPORT_MARKER + " " + json.dumps(result["report"], separators=(",", ":")))
    if result.get("metrics"):
        print(METRICS_BEGIN)
        print(result["metrics"].rstrip())
        print(METRICS_END)
    report_dir = os.environ.get("REPORT_DIR")
    if report_dir and result.get("report") is not None:
        os.makedirs(report_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        base = os.path.join(report_dir, f"verl-grpo_driver_{stamp}")
        with open(base + ".json", "w", encoding="utf-8") as fh:
            json.dump(result["report"], fh, indent=1)
        if result.get("metrics"):
            with open(base + ".prom", "w", encoding="utf-8") as fh:
                fh.write(result["metrics"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Async GRPO rollouts with Ray + SWE-bench on ate_env")
    parser.add_argument("--backend", choices=["mock", "substrate"], default="substrate")
    parser.add_argument("--data-plane", choices=["ate_env"], default="ate_env")
    parser.add_argument("--task-type", choices=["smoke", "swebench"], default="smoke")
    parser.add_argument("--instance-file", default=DEFAULT_INSTANCE_FILE,
                        help="SWE-bench instance record (Hugging Face row as JSON)")
    parser.add_argument("--num-iters", type=int, default=2)
    parser.add_argument("--group-size", type=int, default=4)
    parser.add_argument("--sample-latency-s", type=float, default=1.5,
                        help="Simulated decoding time per iteration")
    parser.add_argument("--sampler-gpus", type=float, default=0.0)
    parser.add_argument("--trainer-gpus", type=float, default=0.0)
    parser.add_argument("--evaluator-resource", default="",
                        help="Custom Ray resource to pin the evaluator actor to")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()

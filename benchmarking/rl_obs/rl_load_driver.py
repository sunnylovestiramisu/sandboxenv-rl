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

"""Synthetic RL rollout load driver for sandbox observability experiments.

Mimics the sandbox pattern of a GRPO-style RL job without GPUs: B batches of G
prompt groups x K generations; each trajectory acquires a sandbox, runs T turns
of (simulated LLM time -> one command in the sandbox), a reward command, and
releases. Harness phases (engine, trainer step, weight sync) are recorded as
telemetry phases so the run timeline has its training-pipeline lanes.

Backends:
  * ``substrate`` - real sandboxes through ate-env-api (e.g. on barkland-brust).
  * ``sim`` - an in-process simulated backend with warm / snapshot / cold
    acquire latencies and a per-node image cache. It also writes
    Substrate-shaped "Restore timing breakdown" records, so the report builder
    can be exercised end to end on a laptop.

Example (local):

    python benchmarking/rl_obs/rl_load_driver.py --backend sim --run-id demo \\
        --batches 6 --groups 8 --generations 4 --workers 16 --new-images-at-batch 3 \\
        --out /tmp/rl-obs/demo

Example (cluster):

    python benchmarking/rl_obs/rl_load_driver.py --backend substrate \\
        --endpoint ate-env-api.ate-env.svc:7777 --atespace rl-obs-poc \\
        --images-file images.txt --run-id e1-baseline --out gs://BUCKET/rl-obs/e1-baseline
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass

from ate_env import AsyncSandboxFleet, FleetConfig, Task, telemetry
from ate_env.backend.mock import MockBackendDriver
from ate_env.runtime.mock import MockRuntimeHook
from ate_env.types import ExecResult, RawSandboxInstance

log = logging.getLogger("rl_load_driver")


# ----------------------------------------------------------------------- simulation


class _SimRuntime(MockRuntimeHook):
    """Mock guest whose commands take a configurable (scaled) time."""

    def __init__(self, exec_s: float, first_exec_extra_s: float) -> None:
        super().__init__()
        self._exec_s = exec_s
        self._first_extra = first_exec_extra_s
        self._calls = 0

    async def exec_async(self, command, cwd="", env=None, timeout_s=120.0) -> ExecResult:
        self._calls += 1
        extra = self._first_extra if self._calls == 1 else 0.0
        await asyncio.sleep(max(0.0, random.lognormvariate(math.log(self._exec_s), 0.5)) + extra)
        return ExecResult(exit_code=0, stdout="ok\n", stderr="")


class SimBackend(MockBackendDriver):
    """Simulated Substrate-like backend.

    * warm pool hit (a pre-created suspended actor): fast resume on first exec
    * image cached on the chosen node: snapshot restore (~0.5 s)
    * image not cached on the node: cold pull + unpack (tens of seconds)

    Times are multiplied by ``time_scale`` so runs can be compressed. Each
    acquire writes a Substrate-shaped restore record to ``records_path``.
    """

    telemetry_name = "substrate"

    def __init__(self, *, atespace: str, nodes: int, time_scale: float,
                 records_path: str, bad_node: int | None = None) -> None:
        super().__init__()
        self.atespace = atespace
        self.nodes = [f"gke-sim-node-{i}" for i in range(nodes)]
        self.scale = time_scale
        self.bad_node = self.nodes[bad_node] if bad_node is not None else None
        self.cache: dict[str, set[str]] = {n: set() for n in self.nodes}
        self.warm: dict[str, int] = {}
        self._records = open(records_path, "a", encoding="utf-8")  # noqa: SIM115
        self._pending_first: dict[str, float] = {}

    def warm_pool(self, template_id, replicas, wait=True, run_id=None, rootfs_image=None):
        key = rootfs_image or template_id
        self.warm[key] = max(self.warm.get(key, 0), replicas)
        # Pre-warming pulls the image onto a couple of nodes.
        for n in random.sample(self.nodes, k=min(2, len(self.nodes))):
            self.cache[n].add(key)

    def unwarm_pool(self, template_id, rootfs_image=None):
        self.warm.pop(rootfs_image or template_id, None)

    async def acquire_async(self, template_id, run_id, timeout_s=180.0, rootfs_image=None):
        key = rootfs_image or template_id
        node = random.choice(self.nodes)
        actor = f"sb-{uuid.uuid4().hex[:12]}"
        if self.warm.get(key, 0) > 0:
            self.warm[key] -= 1
            path, kind = "warm_pool", "latest"
            phases = {"download": 0.05, "oci_unpack": 0.004, "ateom_restore": 0.06}
            await asyncio.sleep(0.02 * self.scale)
        else:
            path = "create"
            cached = key in self.cache[node] and node != self.bad_node
            if cached:
                kind = "golden"
                phases = {"download": random.uniform(0.2, 0.4), "oci_unpack": random.uniform(0.03, 0.08),
                          "ateom_restore": random.uniform(0.05, 0.08)}
            else:
                kind = "boot"
                phases = {"download": random.uniform(4, 9), "oci_unpack": random.uniform(10, 18),
                          "ateom_restore": random.uniform(0.05, 0.08)}
                self.cache[node].add(key)
            await asyncio.sleep(0.03 * self.scale)
        phases = {k: v * self.scale for k, v in phases.items()}  # keep records consistent with wall time
        phases["total"] = phases["download"] + phases["oci_unpack"] + phases["ateom_restore"]
        # Substrate resumes on first guest traffic: the restore cost lands on the first exec.
        self._pending_first[actor] = phases["total"]
        uid = uuid.uuid4().hex
        self._write_restore_record(actor, uid, node, kind, phases)
        return RawSandboxInstance(
            instance_id=actor, endpoint="sim", template_id=template_id, run_id=run_id,
            metadata={"acquire_path": path,
                      "telemetry_attrs": {"ate.atespace": self.atespace, "ate.actor.name": actor,
                                          "ate.actor.uid": uid}},
            rootfs_image=rootfs_image,
        )

    def acquire(self, *a, **kw):  # the async fleet uses acquire_async
        raise NotImplementedError

    async def release_async(self, instance_id, recycle=False):
        await asyncio.sleep(0.005 * self.scale)

    def build_runtime(self, instance, data_plane="sim"):
        extra = self._pending_first.pop(instance.instance_id, 0.0)
        return _SimRuntime(exec_s=0.4 * self.scale, first_exec_extra_s=extra), None

    def _write_restore_record(self, actor, uid, node, kind, phases) -> None:
        rec = {
            "time": time.time(), "msg": "Restore timing breakdown",
            "ate.atespace": self.atespace, "ate.actor.name": actor, "ate.actor.uid": uid,
            "ate.snapshot.kind": kind, "ate.snapshot.fidelity": "memory", "ate.sandbox.class": "gvisor",
            "node": node,
        }
        for k, v in phases.items():
            rec[f"ate.actor.restore.duration.{k}"] = round(v, 4)
        self._records.write(json.dumps(rec) + "\n")
        self._records.flush()

    def close(self) -> None:
        self._records.close()


# --------------------------------------------------------------------------- driver


@dataclass
class Plan:
    batches: list[list[tuple[str, str]]]  # per batch: (prompt_id, image)


def _read_images(path: str) -> list[str]:
    """Digest-pinned image refs, one per line; blank lines and # comments skipped."""
    with open(path, encoding="utf-8") as fh:
        return [ln.strip() for ln in fh if ln.strip() and not ln.lstrip().startswith("#")]


def build_plan(images: list[str], *, batches: int, groups: int, new_at: int | None,
               new_images: list[str]) -> Plan:
    out = []
    for b in range(batches):
        pool = new_images if (new_at is not None and b == new_at and new_images) else images
        out.append([(f"p{b:02d}_{g:03d}", pool[(b * groups + g) % len(pool)]) for g in range(groups)])
    return Plan(out)


async def run(args: argparse.Namespace) -> None:
    run_id = args.run_id or f"run-{uuid.uuid4().hex[:8]}"
    out = args.out
    local_out = out if not out.startswith("gs://") else os.path.join("/tmp/rl-obs", run_id)
    os.makedirs(local_out, exist_ok=True)
    if args.telemetry == "on":
        telemetry.configure(f"jsonl:{out}", run_id=run_id, worker_id=f"driver-{os.getpid()}")
    else:
        telemetry.configure(None)
    # Driver-side timings, measured the same way with telemetry on or off (E3).
    timings: dict[str, list[float]] = {"acquire": [], "first_exec": [], "exec": [], "release": [],
                                       "trajectory": []}
    _maybe_setup_otel(args)

    if args.backend == "sim":
        images = [f"sim.registry/swe/repo-{i:03d}@sha256:{i:064x}" for i in range(args.num_images)]
        new_images = [f"sim.registry/swe/new-{i:03d}@sha256:{i + 7777:064x}" for i in range(args.groups)]
        driver = SimBackend(atespace=args.atespace, nodes=args.nodes, time_scale=args.time_scale,
                            records_path=os.path.join(local_out, "substrate-records.jsonl"),
                            bad_node=args.bad_node)
        cfg = FleetConfig(backend="mock", tenancy=args.atespace, max_warmpool_replicas=args.warm_replicas,
                          acquire_timeout_s=args.acquire_timeout_s)
        fleet = AsyncSandboxFleet(cfg, driver=driver)
    else:
        images = _read_images(args.images_file)
        new_images = _read_images(args.new_images_file) if args.new_images_file else []
        cfg = FleetConfig(backend="substrate", endpoint=args.endpoint, tenancy=args.atespace,
                          template=args.template, max_warmpool_replicas=args.warm_replicas,
                          acquire_timeout_s=args.acquire_timeout_s)
        fleet = AsyncSandboxFleet(cfg)

    plan = build_plan(images, batches=args.batches, groups=args.groups,
                      new_at=args.new_images_at_batch, new_images=new_images)
    workers: asyncio.Queue[str] = asyncio.Queue()
    for i in range(args.workers):
        workers.put_nowait(f"roll-{i:03d}")
    scale = args.time_scale

    async def trajectory(b: int, prompt: str, image: str, k: int) -> None:
        worker = await workers.get()
        try:
            with telemetry.episode(run_id=run_id, worker_id=worker, batch_id=b, group_id=prompt,
                                   trajectory_id=f"{prompt}_g{k}", attempt=1, trace=args.trace):
                t_traj = time.perf_counter()
                h = await fleet.acquire(Task(id=f"{prompt}_g{k}", image=image))
                timings["acquire"].append(time.perf_counter() - t_traj)
                try:
                    for i in range(args.turns + 1):
                        if i < args.turns:
                            await asyncio.sleep(random.lognormvariate(math.log(args.model_time_s), 0.4) * scale)
                        cmd = args.exec_cmd if i < args.turns else args.reward_cmd
                        t = time.perf_counter()
                        await h.exec_async(cmd, check=False, timeout_s=args.step_timeout_s)
                        # The first call includes Substrate's lazy resume; keep it separate.
                        timings["exec" if i > 0 else "first_exec"].append(time.perf_counter() - t)
                finally:
                    t = time.perf_counter()
                    await h.release_async()
                    timings["release"].append(time.perf_counter() - t)
                    timings["trajectory"].append(time.perf_counter() - t_traj)
        except Exception as e:  # keep the run going; failures are in the event stream
            log.warning("trajectory %s_g%d failed: %s", prompt, k, e)
        finally:
            workers.put_nowait(worker)

    async def warm(batch: list[tuple[str, str]]) -> None:
        imgs = sorted({img for _p, img in batch})
        with telemetry.phase("prefetch"):
            await fleet.warm_images(imgs, replicas=args.warm_replicas)

    try:
        if plan.batches and args.warm_replicas > 0:
            await warm(plan.batches[0])
        for b, batch in enumerate(plan.batches):
            nxt = b + 1
            # By default the injected batch is not prefetched: it models new images
            # arriving without a warm pool (the cache-miss scenario).
            skip = nxt == args.new_images_at_batch and not args.prefetch_new
            prefetch = (asyncio.create_task(warm(plan.batches[nxt]))
                        if args.warm_replicas > 0 and nxt < len(plan.batches) and not skip else None)
            with telemetry.phase("engine", batch_id=b):
                await asyncio.gather(*(trajectory(b, p, img, k)
                                       for p, img in batch for k in range(args.generations)))
            with telemetry.phase("trainer_step", batch_id=b):
                await asyncio.sleep(args.train_time_s * scale)
            with telemetry.phase("weight_sync", batch_id=b):
                await asyncio.sleep(args.sync_time_s * scale)
            if prefetch is not None:
                await prefetch
            for img in sorted({img for _p, img in batch}):
                await fleet.unwarm_image(img)
            log.info("batch %d/%d done", b + 1, len(plan.batches))
    finally:
        try:
            await fleet.teardown()
        finally:
            fleet.close()
            telemetry.shutdown()
    summary = {k: {"n": len(v), "p50": _pct(v, .5), "p90": _pct(v, .9), "p99": _pct(v, .99),
                   "mean": round(sum(v) / len(v), 6) if v else None} for k, v in timings.items()}
    with open(os.path.join(local_out, "driver-timings.json"), "w", encoding="utf-8") as fh:
        json.dump({"run_id": run_id, "telemetry": args.telemetry, "summary": summary, "raw": timings}, fh)
    print(json.dumps({"run_id": run_id, "events": out, "local_dir": local_out,
                      "telemetry": args.telemetry, "driver_timings": summary}))


def _pct(v: list[float], p: float) -> float | None:
    if not v:
        return None
    s = sorted(v)
    return round(s[min(len(s) - 1, int(p * (len(s) - 1)))], 6)


def _maybe_setup_otel(args: argparse.Namespace) -> None:
    """As the host application, optionally install an OTel SDK exporting over OTLP."""
    if not args.otlp_endpoint:
        return
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    except ImportError:
        log.warning("--otlp-endpoint needs opentelemetry-sdk and opentelemetry-exporter-otlp")
        return
    tp = TracerProvider(resource=Resource.create({"service.name": "rl-load-driver"}),
                        sampler=ParentBased(TraceIdRatioBased(args.trace_ratio)))
    tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=args.otlp_endpoint,
                                                              insecure=True)))
    trace.set_tracer_provider(tp)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--backend", choices=["sim", "substrate"], default="sim")
    p.add_argument("--run-id")
    p.add_argument("--out", required=True, help="event sink directory or gs:// prefix")
    p.add_argument("--atespace", default="ate-env")
    p.add_argument("--endpoint", default=os.environ.get("ATE_ENV_API_TARGET", "127.0.0.1:7777"))
    p.add_argument("--template", default="default-template")
    p.add_argument("--images-file", help="substrate: digest-pinned task images, one per line")
    p.add_argument("--new-images-file", help="substrate: images injected at --new-images-at-batch")
    p.add_argument("--batches", type=int, default=6)
    p.add_argument("--groups", type=int, default=8, help="prompt groups per batch")
    p.add_argument("--generations", type=int, default=4)
    p.add_argument("--turns", type=int, default=5)
    p.add_argument("--workers", type=int, default=16, help="concurrent rollout workers")
    p.add_argument("--warm-replicas", type=int, default=4)
    p.add_argument("--new-images-at-batch", type=int)
    p.add_argument("--prefetch-new", action="store_true",
                   help="also prefetch the batch with injected new images (default: cold)")
    p.add_argument("--model-time-s", type=float, default=2.0)
    p.add_argument("--train-time-s", type=float, default=5.0)
    p.add_argument("--sync-time-s", type=float, default=2.0)
    p.add_argument("--exec-cmd", default="sleep 0.2 && echo ok")
    p.add_argument("--reward-cmd", default="sleep 0.5 && echo reward")
    p.add_argument("--step-timeout-s", type=float, default=300)
    p.add_argument("--acquire-timeout-s", type=float, default=600)
    p.add_argument("--time-scale", type=float, default=1.0, help="multiply simulated times")
    p.add_argument("--telemetry", choices=["on", "off"], default="on",
                   help="off: no SDK event sink (driver-side timings are still recorded)")
    p.add_argument("--trace", action="store_true", help="make each episode an OTel root span")
    p.add_argument("--otlp-endpoint", help="install an OTel SDK exporting spans here")
    p.add_argument("--trace-ratio", type=float, default=0.05)
    # sim-only
    p.add_argument("--num-images", type=int, default=24)
    p.add_argument("--nodes", type=int, default=8)
    p.add_argument("--bad-node", type=int, help="sim: node index whose image cache never hits")
    args = p.parse_args(argv)
    if args.backend == "substrate" and not args.images_file:
        p.error("--images-file is required with --backend substrate")
    return args


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(parse_args(argv)))
    return 0


if __name__ == "__main__":
    sys.exit(main())

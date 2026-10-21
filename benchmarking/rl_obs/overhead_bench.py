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

"""In-process CPU cost of SDK telemetry (experiment E3, part 1).

Runs N trajectories (acquire -> T execs -> release) against the in-memory mock
backend, so the only work is the SDK itself, under four modes:

  off        telemetry not configured (the default for SDK users)
  jsonl      rl_sandbox.* events to a local JSON-lines file
  otel       OpenTelemetry spans + stage metrics only (in-memory SDK, no export)
  jsonl+otel both

and reports the added cost per trajectory and per SDK call versus ``off``.

    python benchmarking/rl_obs/overhead_bench.py --trajectories 20000 --turns 5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import tempfile
import time

from ate_env import AsyncSandboxFleet, FleetConfig, Task, telemetry
from ate_env.telemetry import otel


def _otel_providers():
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    tp = TracerProvider()
    exporter = InMemorySpanExporter()
    tp.add_span_processor(SimpleSpanProcessor(exporter))
    return tp, MeterProvider(metric_readers=[InMemoryMetricReader()]), exporter


async def _run(n: int, turns: int, trace: bool) -> float:
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock"))
    t0 = time.perf_counter()
    for i in range(n):
        with telemetry.episode(batch_id=i // 64, trajectory_id=f"t{i}", worker_id="w0", trace=trace):
            h = await fleet.acquire(Task(id=f"t{i}", image="img"))
            for _ in range(turns):
                await h.exec_async("echo ok", check=False)
            await h.release_async()
    return time.perf_counter() - t0


def bench(mode: str, n: int, turns: int, repeats: int) -> dict:
    times = []
    for _ in range(repeats):
        exporter = None
        with tempfile.TemporaryDirectory() as tmp:
            telemetry.configure(f"jsonl:{tmp}" if "jsonl" in mode else None, run_id="bench")
            if "otel" in mode:
                tp, mp, exporter = _otel_providers()
                otel.use_providers(tp, mp)
            else:
                otel.use_providers(None, None)
            times.append(asyncio.run(_run(n, turns, trace="otel" in mode)))
            telemetry.shutdown()
            otel.use_providers(None, None)
            if exporter is not None:
                exporter.clear()
    best = min(times)
    return {"mode": mode, "seconds_best": round(best, 4),
            "seconds_median": round(statistics.median(times), 4),
            "us_per_trajectory": round(best / n * 1e6, 2)}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--trajectories", type=int, default=20000)
    p.add_argument("--turns", type=int, default=5)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--modes", default="off,jsonl,otel,jsonl+otel")
    args = p.parse_args()
    results = [bench(m, args.trajectories, args.turns, args.repeats) for m in args.modes.split(",")]
    base = next(r for r in results if r["mode"] == "off")["us_per_trajectory"]
    calls = args.turns + 2  # acquire + execs + release
    for r in results:
        r["added_us_per_trajectory"] = round(r["us_per_trajectory"] - base, 2)
        r["added_us_per_sdk_call"] = round((r["us_per_trajectory"] - base) / calls, 2)
    print(json.dumps({"trajectories": args.trajectories, "turns": args.turns,
                      "events_per_trajectory": args.turns + 3, "results": results}, indent=1))


if __name__ == "__main__":
    main()

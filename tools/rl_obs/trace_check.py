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

"""Trace completeness check (experiment E4), verified from logs.

For every traced trajectory (SDK events carrying ``trace_id``), finds the
Substrate log records of its actor and reports which components logged under
the *same* trace id - i.e. how far the SDK's W3C trace context propagated:
ate-env-api -> ateapi (state changes) -> atelet (restore/checkpoint) -> ateom.

    python tools/rl_obs/trace_check.py --events gs://BUCKET/rl-obs/e4-traces \\
        --project PROJECT --cluster CLUSTER --atespace telemetry-poc
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sources import _rfc3339, load_events, normalize_record  # noqa: E402


def fetch_traced_records(project: str, cluster: str | None, atespace: str,
                         start: float, end: float) -> list[dict]:
    clauses = ['resource.type="k8s_container"', f'jsonPayload."ate.atespace"="{atespace}"',
               "jsonPayload.trace_id:*", f'timestamp>="{_rfc3339(start - 60)}"',
               f'timestamp<="{_rfc3339(end + 120)}"']
    if cluster:
        clauses.append(f'resource.labels.cluster_name="{cluster}"')
    res = subprocess.run(["gcloud", "logging", "read", " AND ".join(clauses), f"--project={project}",
                          "--format=json", "--limit=200000"], check=True, capture_output=True,
                         text=True, timeout=900)
    out = []
    for e in json.loads(res.stdout or "[]"):
        r = normalize_record(e)
        r["_container"] = (e.get("resource", {}).get("labels", {}) or {}).get("container_name")
        out.append(r)
    return out


def check(events: list[dict], records: list[dict]) -> dict:
    traj = {}
    for e in events:
        if e.get("event") == "rl_sandbox.acquire.ready" and e.get("trace_id"):
            traj[e["ate.actor.name"]] = {"trace_id": e["trace_id"],
                                         "trajectory": e.get("rl.trajectory_id")}
    by_actor: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_actor[r.get("ate.actor.name")].append(r)
    components = Counter()
    sampled_flags = Counter()
    per_traj = []
    for actor, t in traj.items():
        seen = sorted({r["_container"] for r in by_actor.get(actor, [])
                       if r.get("trace_id") == t["trace_id"]})
        for c in seen:
            components[c] += 1
        for r in by_actor.get(actor, []):
            if r.get("trace_id") == t["trace_id"]:
                sampled_flags[r.get("trace_flags")] += 1
        per_traj.append({"actor": actor, **t, "components": seen})
    n = len(traj) or 1
    return {
        "traced_trajectories": len(traj),
        "share_with_component": {c: round(k / n, 4) for c, k in components.most_common()},
        "trace_flags_on_matching_records": dict(sampled_flags),
        "examples": [
            {**p, "cloud_trace_url": "https://console.cloud.google.com/traces/list?tid="
             f"{p['trace_id']}"} for p in per_traj[:5]],
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", required=True)
    p.add_argument("--project", required=True)
    p.add_argument("--cluster")
    p.add_argument("--atespace", required=True)
    args = p.parse_args(argv)
    events = load_events(args.events)
    start, end = min(e["ts"] for e in events), max(e["ts"] for e in events)
    records = fetch_traced_records(args.project, args.cluster, args.atespace, start, end)
    result = check(events, records)
    for ex in result["examples"]:
        ex["cloud_trace_url"] += f"&project={args.project}"
    print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())

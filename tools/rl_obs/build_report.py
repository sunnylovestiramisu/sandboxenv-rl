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

"""Build an RL sandbox observability report for one run.

Reads the SDK's rl_sandbox.* events, joins them with Substrate's restore
records (from Cloud Logging or a local JSONL file), and writes:

  * run.json   - aggregated data (the report's single source of truth)
  * report.html - a self-contained dashboard rendering run.json

Examples:

  # Local simulated run (records written by the sim backend)
  python tools/rl_obs/build_report.py --events /tmp/rl-obs/demo \\
      --substrate-records /tmp/rl-obs/demo/substrate-records.jsonl --out /tmp/rl-obs/demo/report

  # Cluster run: events in GCS, Substrate records from Cloud Logging
  python tools/rl_obs/build_report.py --events gs://BUCKET/rl-obs/e1 \\
      --project PROJECT --atespace rl-obs-poc --out ./reports/e1
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aggregate import aggregate  # noqa: E402
from sources import load_events, load_substrate_records  # noqa: E402

TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template.html")


def render_html(data: dict, template_path: str = TEMPLATE) -> str:
    with open(template_path, encoding="utf-8") as fh:
        tpl = fh.read()
    blob = json.dumps(data, separators=(",", ":"), default=str).replace("</", "<\\/")
    run_id = str(data["meta"].get("run_id") or "run")
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return (tpl.replace("__RUN_JSON__", blob)
               .replace("__RUN_ID__", run_id.replace("<", "").replace(">", ""))
               .replace("__GENERATED__", generated))


def build(events_src: str, *, records_path: str | None, project: str | None,
          atespace: str | None, cluster: str | None = None) -> dict:
    events = load_events(events_src)
    if not events:
        raise SystemExit(f"no events found under {events_src}")
    start = min(e["ts"] for e in events)
    end = max(e["ts"] for e in events)
    if atespace is None:
        atespace = next((e.get("ate.atespace") for e in events if e.get("ate.atespace")), None)
    records = load_substrate_records(path=records_path, project=project, atespace=atespace,
                                     start=start, end=end, cluster=cluster)
    return aggregate(events, records)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", required=True, help="directory or gs:// prefix with events-*.jsonl")
    p.add_argument("--substrate-records", help="local JSONL of Substrate records (skips Cloud Logging)")
    p.add_argument("--project", help="GCP project to read Substrate records from Cloud Logging")
    p.add_argument("--atespace", help="filter Substrate records (default: from the events)")
    p.add_argument("--cluster", help="GKE cluster name, narrows the Cloud Logging query")
    p.add_argument("--out", required=True, help="output directory for run.json and report.html")
    args = p.parse_args(argv)

    data = build(args.events, records_path=args.substrate_records, project=args.project,
                 atespace=args.atespace, cluster=args.cluster)
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "run.json"), "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1, default=str)
    with open(os.path.join(args.out, "report.html"), "w", encoding="utf-8") as fh:
        fh.write(render_html(data))
    m = data["meta"]
    print(json.dumps({"run_id": m["run_id"], "trajectories": m["n_trajectories"],
                      "join": m["join"], "out": args.out}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

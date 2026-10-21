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

"""Loaders: SDK event files and Substrate records (local files or Cloud Logging)."""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Any

RESTORE_MSG = "Restore timing breakdown"
STATE_MSG = "Actor state changed"


def load_events(source: str) -> list[dict[str, Any]]:
    """Read every ``events-*.jsonl`` under a directory or ``gs://`` prefix."""
    tmp = None
    try:
        if source.startswith("gs://"):
            tmp = tempfile.mkdtemp(prefix="rl-obs-events-")
            subprocess.run(["gcloud", "storage", "cp", source.rstrip("/") + "/events-*.jsonl", tmp],
                           check=True, capture_output=True, timeout=600)
            directory = tmp
        else:
            directory = source
        events: list[dict[str, Any]] = []
        for path in sorted(glob.glob(os.path.join(directory, "events-*.jsonl"))):
            with open(path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
        events.sort(key=lambda e: e.get("ts", 0))
        return events
    finally:
        if tmp:
            shutil.rmtree(tmp, ignore_errors=True)


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).replace("Z", "+00:00")
    # Cloud Logging timestamps may carry nanoseconds; trim to microseconds.
    if "." in s:
        head, _, rest = s.partition(".")
        frac = "".join(c for c in rest if c.isdigit())
        tz = rest[len(frac):]
        s = f"{head}.{frac[:6]}{tz}"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def normalize_record(entry: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Cloud Logging entry (or an already-flat record) into one dict.

    Adds ``_ts`` (epoch seconds) and ``_node`` (GKE node, when known).
    """
    payload = dict(entry.get("jsonPayload") or entry)
    labels = entry.get("labels") or {}
    payload["_ts"] = (_parse_ts(payload.get("time")) or _parse_ts(entry.get("timestamp"))
                      or _parse_ts(payload.get("ts")))
    payload["_node"] = (payload.get("node") or payload.get("k8s.node.name")
                        or labels.get("compute.googleapis.com/resource_name"))
    return payload


def load_substrate_records(*, path: str | None = None, project: str | None = None,
                           atespace: str | None = None, start: float | None = None,
                           end: float | None = None, cluster: str | None = None,
                           limit: int = 200000) -> list[dict[str, Any]]:
    """Load Substrate restore / state records from a JSONL file or Cloud Logging."""
    if path:
        out = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    out.append(normalize_record(json.loads(line)))
        return out
    if not project:
        return []
    if not shutil.which("gcloud"):
        raise RuntimeError("gcloud is required to read Substrate records from Cloud Logging")
    # resource.type (and the cluster) keep the scan to GKE container logs; without
    # them a project-wide query on jsonPayload fields can time out.
    clauses = ['resource.type="k8s_container"',
               f'(jsonPayload.msg="{RESTORE_MSG}" OR jsonPayload.msg="{STATE_MSG}")']
    if cluster:
        clauses.append(f'resource.labels.cluster_name="{cluster}"')
    if atespace:
        clauses.append(f'jsonPayload."ate.atespace"="{atespace}"')
    if start:
        clauses.append(f'timestamp>="{_rfc3339(start - 300)}"')
    if end:
        clauses.append(f'timestamp<="{_rfc3339(end + 300)}"')
    cmd = ["gcloud", "logging", "read", " AND ".join(clauses), f"--project={project}",
           "--format=json", f"--limit={limit}", "--order=asc"]
    res = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=900)
    return [normalize_record(e) for e in json.loads(res.stdout or "[]")]


def _rfc3339(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

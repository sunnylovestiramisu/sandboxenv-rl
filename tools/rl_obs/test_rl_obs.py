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

"""Tests for the rl_obs join and aggregation (run: pytest tools/rl_obs)."""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aggregate import aggregate, build_trajectories, join_restores  # noqa: E402
from build_report import render_html  # noqa: E402
from sources import normalize_record  # noqa: E402

T0 = 1_800_000_000.0


def _traj_events(sb, *, worker, batch, start, wait, path, first_exec, steps=2, uid=None):
    base = {"rl.run_id": "r", "rl.worker_id": worker, "rl.batch_id": batch,
            "rl.trajectory_id": f"t-{sb}", "rl_sandbox.backend": "substrate",
            "rl_sandbox.sandbox.id": sb, "ate.atespace": "ns", "ate.actor.name": sb, "image": "img"}
    if uid:
        base["ate.actor.uid"] = uid
    ev = [{"ts": start, "event": "rl_sandbox.acquire.started", **base},
          {"ts": start + wait, "event": "rl_sandbox.acquire.ready", "wait_s": wait,
           "acquire_path": path, **base}]
    t = start + wait
    for i in range(steps):
        dt = first_exec if i == 0 else 1.0
        t += dt
        ev.append({"ts": t, "event": "rl_sandbox.step.completed", "exec_s": dt,
                   "step.first": i == 0, "step.kind": "exec", "exit_code": 0, **base})
    ev.append({"ts": t + 2.0, "event": "rl_sandbox.released", "hold_s": t + 2 - start - wait, **base})
    return ev


def _restore(sb, *, ts, kind, unpack, node, uid=None):
    rec = {"msg": "Restore timing breakdown", "time": ts, "ate.atespace": "ns", "ate.actor.name": sb,
           "ate.snapshot.kind": kind, "ate.actor.restore.duration.download": 0.3,
           "ate.actor.restore.duration.oci_unpack": unpack,
           "ate.actor.restore.duration.ateom_restore": 0.06,
           "ate.actor.restore.duration.total": 0.36 + unpack, "node": node}
    if uid:
        rec["ate.actor.uid"] = uid
    return normalize_record(rec)


def _fixture():
    events = (_traj_events("a", worker="w0", batch=0, start=T0, wait=0.01, path="warm_pool", first_exec=0.5)
              + _traj_events("b", worker="w1", batch=0, start=T0 + 1, wait=0.02, path="create",
                             first_exec=0.9, uid="u-b")
              + _traj_events("c", worker="w0", batch=1, start=T0 + 20, wait=0.02, path="create",
                             first_exec=15.0)
              + [{"ts": T0 + 30, "event": "rl_sandbox.acquire.timeout", "wait_s": 600.0,
                  "rl.worker_id": "w1", "rl.batch_id": 1, "rl.trajectory_id": "t-x",
                  "error.type": "SandboxStartError", "image": "img"},
                 {"ts": T0, "event": "rl_sandbox.phase", "phase": "engine", "start_ts": T0,
                  "duration_s": 25.0, "rl.batch_id": 0}])
    events.sort(key=lambda e: e["ts"])
    records = [_restore("a", ts=T0 + 0.2, kind="latest", unpack=0.004, node="n1"),
               _restore("b", ts=T0 + 1.5, kind="golden", unpack=0.05, node="n1", uid="u-b"),
               _restore("c", ts=T0 + 25, kind="boot", unpack=14.0, node="n2"),
               _restore("unrelated", ts=T0 + 2, kind="boot", unpack=9.0, node="n3")]
    return events, records


def test_join_matches_by_uid_or_name_and_infers_source():
    events, records = _fixture()
    trajs = build_trajectories(events)
    stats = join_restores(trajs, records)
    assert stats == {"eligible": 3, "matched": 3, "rate": 1.0}
    by_id = {t.sandbox_id: t for t in trajs if t.sandbox_id}
    assert by_id["a"].source == "warm"
    assert by_id["b"].source == "snapshot"
    assert by_id["c"].source == "cold"
    timeout = next(t for t in trajs if t.outcome == "timeout")
    assert timeout.source == "timeout"


def test_effective_acquire_includes_lazy_resume_capped_by_first_exec():
    events, records = _fixture()
    trajs = build_trajectories(events)
    join_restores(trajs, records)
    c = next(t for t in trajs if t.sandbox_id == "c")
    # resume = restore total (14.36 s) capped by the first exec (15 s)
    assert abs(c.eff_acquire_s - (0.02 + 14.36)) < 1e-6
    assert abs(c.exec_s - (15.0 + 1.0 - 14.36)) < 1e-6


def test_aggregate_kpis_placement_and_render():
    events, records = _fixture()
    data = aggregate(events, records)
    k = data["kpis"]
    assert k["counts"] == {"warm": 1, "snapshot": 1, "cold": 1, "timeout": 1}
    assert k["timeout_share"] == 0.25
    assert k["cache_hit"] == round(2 / 3, 4)
    p = data["placement"]
    assert p["nodes"] == ["n1", "n2"] and p["pulls_total"] == 1 and p["worst_node"] == "n2"
    assert data["meta"]["join"]["rate"] == 1.0
    assert ("SandboxStartError", 1) in [tuple(e) for e in data["errors"]]
    assert data["sandboxes"]["workers"] == ["w0", "w1"]
    html = render_html(json.loads(json.dumps(data)))
    assert "__RUN_JSON__" not in html and '"run_id":"r"' in html


def test_normalize_cloud_logging_entry():
    entry = {"timestamp": "2026-10-20T23:29:11.123456789Z",
             "labels": {"compute.googleapis.com/resource_name": "gke-node-7"},
             "jsonPayload": {"msg": "Restore timing breakdown", "ate.actor.name": "sb-1"}}
    rec = normalize_record(entry)
    assert rec["_node"] == "gke-node-7" and rec["ate.actor.name"] == "sb-1"
    assert abs(rec["_ts"] - 1792538951.123456) < 1e-3


def test_boot_resume_without_restore_record_is_joined_as_cold():
    events = _traj_events("d", worker="w0", batch=0, start=T0, wait=0.02, path="create", first_exec=0.4)
    states = [normalize_record({"msg": "Actor state changed", "time": T0 + 0.05, "ate.atespace": "ns",
                                "ate.actor.name": "d", "ate.actor.state": s0})
              for s0 in ("suspended",)]
    states += [normalize_record({"msg": "Actor state changed", "time": T0 + t, "ate.atespace": "ns",
                                 "ate.actor.name": "d", "ate.actor.state": st})
               for t, st in ((0.10, "resuming"), (0.235, "running"))]
    trajs = build_trajectories(sorted(events, key=lambda e: e["ts"]))
    stats = join_restores(trajs, states)
    (t,) = trajs
    assert stats["matched"] == 1 and t.source == "cold"
    assert abs(t.restore["ate.actor.restore.duration.total"] - 0.135) < 1e-6


def test_ateom_restore_records_are_ignored():
    events = _traj_events("e", worker="w0", batch=0, start=T0, wait=0.02, path="create", first_exec=0.8)
    ateom = normalize_record({"msg": "Restore timing breakdown", "time": T0 + 0.3, "ate.atespace": "ns",
                              "ate.actor.name": "e", "ateom.actor.restore.duration.runsc": 0.1})
    atelet = _restore("e", ts=T0 + 0.31, kind="golden", unpack=0.05, node="n9")
    trajs = build_trajectories(sorted(events, key=lambda x: x["ts"]))
    join_restores(trajs, [ateom, atelet])
    assert trajs[0].source == "snapshot" and trajs[0].restore["_node"] == "n9"

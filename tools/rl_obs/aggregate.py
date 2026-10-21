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

"""Join SDK events with Substrate records and aggregate them for the run report.

Provenance of every value: M = measured (an SDK event or a Substrate record),
D = derived here from measured values.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from sources import RESTORE_MSG, STATE_MSG

# Heatmap / outcome codes.
NONE, WARM, SNAP, COLD, TIMEOUT, FAILED, UNKNOWN = range(7)
SOURCE_CODES = {"warm": WARM, "snapshot": SNAP, "cold": COLD, "timeout": TIMEOUT,
                "failed": FAILED, "unknown": UNKNOWN}
# Substrate ate.snapshot.kind -> acquire source. ate-env's warm pool holds
# suspended actors, which resume from their own latest (or node-local) snapshot.
KIND_TO_SOURCE = {"local": "warm", "latest": "warm", "golden": "snapshot", "boot": "cold"}
# OCI unpack above this means the layers were not in the node's image cache (D).
UNPACK_MISS_S = 1.0
HIST_EDGES = [round(-2 + 0.1 * i, 1) for i in range(51)]  # log10 seconds, 10 ms .. 1000 s


@dataclass
class Traj:
    sandbox_id: str | None
    worker: str | None
    batch: Any
    group: Any
    traj_id: str | None
    image: str | None
    start_ts: float
    wait_s: float
    path: str | None = None
    actor: str | None = None
    uid: str | None = None
    atespace: str | None = None
    ready_ts: float | None = None
    released_ts: float | None = None
    steps: list[dict[str, Any]] = field(default_factory=list)
    outcome: str = "ok"  # ok | timeout | failed
    error: str | None = None
    restore: dict[str, Any] | None = None
    source: str = "unknown"

    @property
    def resume_s(self) -> float:
        r = self.restore or {}
        total = r.get("ate.actor.restore.duration.total")
        if total is None:
            return 0.0
        first = next((s["exec_s"] for s in self.steps if s.get("step.first")), None)
        return min(float(total), float(first)) if first is not None else float(total)

    @property
    def eff_acquire_s(self) -> float:
        return self.wait_s + self.resume_s

    @property
    def exec_s(self) -> float:
        return max(0.0, sum(s.get("exec_s", 0.0) for s in self.steps) - self.resume_s)

    @property
    def total_s(self) -> float | None:
        end = self.released_ts or (self.steps[-1]["ts"] if self.steps else None)
        return (end - self.start_ts) if end else None


def _q(values: list[float], p: float) -> float | None:
    if not values:
        return None
    v = sorted(values)
    k = (len(v) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return v[lo] if lo == hi else v[lo] + (v[hi] - v[lo]) * (k - lo)


def _r(x: float | None, nd: int = 3) -> float | None:
    return None if x is None else round(x, nd)


def build_trajectories(events: list[dict[str, Any]]) -> list[Traj]:
    trajs: list[Traj] = []
    by_sandbox: dict[str, Traj] = {}
    for e in events:
        name = e.get("event")
        if name == "rl_sandbox.acquire.ready":
            wait = float(e.get("wait_s", 0.0))
            t = Traj(
                sandbox_id=e.get("rl_sandbox.sandbox.id"), worker=e.get("rl.worker_id"),
                batch=e.get("rl.batch_id"), group=e.get("rl.group_id"),
                traj_id=e.get("rl.trajectory_id") or e.get("rl_sandbox.task.id"),
                image=e.get("image"), start_ts=e["ts"] - wait, wait_s=wait,
                path=e.get("acquire_path"), actor=e.get("ate.actor.name"),
                uid=e.get("ate.actor.uid"), atespace=e.get("ate.atespace"), ready_ts=e["ts"],
            )
            trajs.append(t)
            if t.sandbox_id:
                by_sandbox[t.sandbox_id] = t
        elif name in ("rl_sandbox.acquire.timeout", "rl_sandbox.acquire.failed"):
            wait = float(e.get("wait_s", 0.0))
            trajs.append(Traj(
                sandbox_id=e.get("rl_sandbox.sandbox.id"), worker=e.get("rl.worker_id"),
                batch=e.get("rl.batch_id"), group=e.get("rl.group_id"),
                traj_id=e.get("rl.trajectory_id") or e.get("rl_sandbox.task.id"),
                image=e.get("image"), start_ts=e["ts"] - wait, wait_s=wait,
                outcome="timeout" if name.endswith("timeout") else "failed",
                error=e.get("error.type"),
                source="timeout" if name.endswith("timeout") else "failed",
            ))
        elif name == "rl_sandbox.step.completed":
            t = by_sandbox.get(e.get("rl_sandbox.sandbox.id"))
            if t is not None:
                t.steps.append(e)
        elif name == "rl_sandbox.released":
            t = by_sandbox.get(e.get("rl_sandbox.sandbox.id"))
            if t is not None:
                t.released_ts = e["ts"]
    return trajs


def _is_atelet_restore(r: dict[str, Any]) -> bool:
    """atelet's record (ate.actor.restore.duration.*); ateom writes the same
    message with its own ateom.actor.restore.duration.* phases, which we skip."""
    return r.get("msg") == RESTORE_MSG and "ate.actor.restore.duration.total" in r


def _boot_resumes(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Synthesize restore-like records for resumes that left no restore record.

    atelet writes "Restore timing breakdown" only when it restores a snapshot;
    a resume that boots from the image (no golden, first start) leaves just
    ateapi's "Actor state changed" resuming -> running pair. Its duration is
    the resume time and the source is a cold boot (D).
    """
    restored = {(r.get("ate.atespace"), r.get("ate.actor.name"))
                for r in records if _is_atelet_restore(r)}
    pending: dict[tuple[Any, Any], dict[str, Any]] = {}
    out: list[dict[str, Any]] = []
    for r in sorted((r for r in records if r.get("msg") == STATE_MSG and r.get("_ts") is not None),
                    key=lambda r: r["_ts"]):
        key = (r.get("ate.atespace"), r.get("ate.actor.name"))
        state = r.get("ate.actor.state")
        if state == "resuming":
            pending[key] = r
        elif state == "running" and key in pending:
            start = pending.pop(key)
            if key in restored:
                continue
            out.append({
                # The state record comes from ateapi, so its node is not the actor's.
                "msg": RESTORE_MSG, "_ts": r["_ts"], "_node": None, "_synthetic": True,
                "ate.atespace": key[0], "ate.actor.name": key[1], "ate.actor.uid": r.get("ate.actor.uid"),
                "ate.snapshot.kind": "boot",
                "ate.actor.restore.duration.total": round(r["_ts"] - start["_ts"], 6),
            })
    return out


def join_restores(trajs: list[Traj], records: list[dict[str, Any]]) -> dict[str, Any]:
    """Attach each acquire's Substrate restore record; infer the acquire source."""
    by_uid: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_name: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in list(records) + _boot_resumes(records):
        if not _is_atelet_restore(r) or r.get("_ts") is None:
            continue
        if r.get("ate.actor.uid"):
            by_uid[r["ate.actor.uid"]].append(r)
        by_name[(r.get("ate.atespace") or "", r.get("ate.actor.name") or "")].append(r)
    eligible = matched = 0
    for t in trajs:
        if t.outcome != "ok":
            continue
        eligible += 1
        cands = by_uid.get(t.uid or "") or by_name.get((t.atespace or "", t.actor or ""), [])
        lo = t.start_ts - 5
        hi = (t.released_ts or (t.ready_ts or t.start_ts) + 900) + 5
        rec = next((r for r in cands if lo <= r["_ts"] <= hi), None)
        if rec is not None:
            matched += 1
            t.restore = rec
            t.source = KIND_TO_SOURCE.get(rec.get("ate.snapshot.kind"), "unknown")
        elif t.path == "warm_pool":
            t.source = "warm"
    return {"eligible": eligible, "matched": matched,
            "rate": _r(matched / eligible, 4) if eligible else None}


def aggregate(events: list[dict[str, Any]], records: list[dict[str, Any]]) -> dict[str, Any]:
    trajs = build_trajectories(events)
    join = join_restores(trajs, records)
    phases = [e for e in events if e.get("event") == "rl_sandbox.phase"]
    warm_targets = [e for e in events if e.get("event") == "rl_sandbox.warm.target"]
    all_ts = [e["ts"] for e in events] + [p.get("start_ts", p["ts"]) for p in phases]
    start, end = (min(all_ts), max(all_ts)) if all_ts else (0.0, 0.0)
    duration = max(1e-6, end - start)
    bin_s = max(1, math.ceil(duration / 60))
    n_bins = max(1, math.ceil(duration / bin_s))

    def b_of(ts: float) -> int:
        return min(n_bins - 1, max(0, int((ts - start) // bin_s)))

    ok = [t for t in trajs if t.outcome == "ok"]
    first_e = next((e for e in events if e.get("rl.run_id")), {})
    meta = {
        "run_id": first_e.get("rl.run_id"),
        "backend": next((e.get("rl_sandbox.backend") for e in events if e.get("rl_sandbox.backend")), None),
        "atespace": next((t.atespace for t in trajs if t.atespace), None),
        "start": start, "end": end, "duration_s": _r(duration, 1), "bin_s": bin_s, "n_bins": n_bins,
        "n_workers": len({t.worker for t in trajs if t.worker}),
        "n_trajectories": len(trajs),
        "n_batches": len({t.batch for t in trajs if t.batch is not None}),
        "n_events": len(events), "n_substrate_records": len(records), "join": join,
    }

    # ---- KPIs
    eff = [t.eff_acquire_s for t in ok]
    totals = [t.total_s for t in ok if t.total_s]
    src = Counter(t.source for t in trajs)
    n = max(1, len(trajs))
    step_errors = [s for t in ok for s in t.steps if s.get("error.type")]
    infra_failed = {id(t) for t in trajs if t.outcome != "ok"} | {
        id(t) for t in ok if any(s.get("error.type") for s in t.steps)}
    # Phase-level stats need atelet's restore record; boot resumes inferred from
    # state changes carry only a total, so they count for timing but not here.
    joined = [t for t in ok if t.restore and not t.restore.get("_synthetic")]
    misses = [t for t in joined if (t.restore.get("ate.actor.restore.duration.oci_unpack") or 0) > UNPACK_MISS_S]
    batch_steps = defaultdict(float)
    for p in phases:
        if p.get("phase") in ("engine", "trainer_step", "weight_sync") and p.get("batch_id") is not None:
            batch_steps[p["batch_id"]] += float(p.get("duration_s", 0.0))
    kpis = {
        "wait_share": _r(sum(eff) / sum(totals), 4) if totals else None,
        "acq_p50": _r(_q(eff, .5)), "acq_p90": _r(_q(eff, .9)), "acq_p99": _r(_q(eff, .99)),
        "sdk_wait_p50": _r(_q([t.wait_s for t in ok], .5)),
        "mix": {k: _r(src.get(k, 0) / n, 4) for k in ("warm", "snapshot", "cold", "unknown",
                                                    "timeout", "failed")},
        "counts": dict(src),
        "timeout_share": _r(src.get("timeout", 0) / n, 4),
        "infra_failed_share": _r(len(infra_failed) / n, 4),
        "cache_hit": _r(1 - len(misses) / len(joined), 4) if joined else None,
        "step_p50": _r(statistics.median(batch_steps.values()), 1) if batch_steps else None,
        "hold_p50": _r(_q([t.released_ts - t.ready_ts for t in ok if t.released_ts and t.ready_ts], .5), 1),
        "acq_share_of_hold": _r(sum(eff) / sum(t.released_ts - t.ready_ts for t in ok
                                               if t.released_ts and t.ready_ts), 4)
        if any(t.released_ts for t in ok) else None,
    }
    errors = Counter((t.error or t.outcome) for t in trajs if t.outcome != "ok")
    errors.update(f"step: {s['error.type']}" for s in step_errors)

    # ---- Sandboxes: worker x bin heat, outcome per bin, histogram per source
    workers = sorted({t.worker for t in trajs if t.worker})
    widx = {w: i for i, w in enumerate(workers)}
    heat = [[NONE] * n_bins for _ in workers]
    heat_rank = {NONE: 0, WARM: 1, UNKNOWN: 2, SNAP: 3, COLD: 4, FAILED: 5, TIMEOUT: 6}
    outcome = [Counter() for _ in range(n_bins)]
    for t in trajs:
        b = b_of(t.start_ts)
        code = SOURCE_CODES.get(t.source, UNKNOWN)
        outcome[b][t.source] += 1
        if t.worker in widx:
            cur = heat[widx[t.worker]][b]
            if heat_rank[code] > heat_rank[cur]:
                heat[widx[t.worker]][b] = code
    hist = {k: [0] * (len(HIST_EDGES) - 1) for k in SOURCE_CODES}
    for t in trajs:
        v = t.eff_acquire_s if t.outcome == "ok" else t.wait_s
        if v <= 0:
            continue
        i = int((math.log10(v) + 2) / 0.1)
        if 0 <= i < len(HIST_EDGES) - 1:
            hist[t.source][i] += 1

    # ---- Acquire breakdown per bin (non-warm, joined)
    def phase(t: Traj, k: str) -> float:
        return float((t.restore or {}).get(f"ate.actor.restore.duration.{k}") or 0.0)

    brk_acc: list[list[Traj]] = [[] for _ in range(n_bins)]
    for t in ok:
        if t.restore and t.source != "warm":
            brk_acc[b_of(t.start_ts)].append(t)
    breakdown = []
    for lst in brk_acc:
        if not lst:
            breakdown.append(None)
            continue
        # Boot resumes (no atelet record) only have a total: show it as "boot".
        breakdown.append({
            "sdk": _r(statistics.mean(t.wait_s for t in lst), 4),
            "download": _r(statistics.mean(phase(t, "download") for t in lst), 4),
            "unpack": _r(statistics.mean(phase(t, "oci_unpack") for t in lst), 4),
            "restore": _r(statistics.mean(phase(t, "ateom_restore") for t in lst), 4),
            "boot": _r(statistics.mean(phase(t, "total") if t.restore.get("_synthetic") else 0.0
                                       for t in lst), 4),
            "n": len(lst),
            "n_boot": sum(1 for t in lst if t.restore.get("_synthetic")),
        })
    steady = [t for t in joined if t not in misses]
    breakdown_steady = {
        "sdk": _r(_q([t.wait_s for t in steady], .5), 4),
        "download": _r(_q([phase(t, "download") for t in steady], .5), 4),
        "unpack": _r(_q([phase(t, "oci_unpack") for t in steady], .5), 4),
        "restore": _r(_q([phase(t, "ateom_restore") for t in steady], .5), 4),
        "total": _r(_q([phase(t, "total") for t in steady], .5), 4),
    }

    # ---- Timeline lanes
    timeline = {
        "phases": [{"phase": p.get("phase"), "batch": p.get("batch_id"),
                    "start": _r(p.get("start_ts", p["ts"]) - start, 2),
                    "dur": _r(float(p.get("duration_s", 0.0)), 2)} for p in phases],
        "warm_requested": [0] * n_bins, "acquires": [0] * n_bins, "warm_hits": [0] * n_bins,
        "misses": [0] * n_bins,
    }
    for w in warm_targets:
        timeline["warm_requested"][b_of(w["ts"])] += int(w.get("requested") or 0)
    for t in trajs:
        timeline["acquires"][b_of(t.start_ts)] += 1
        if t.source == "warm":
            timeline["warm_hits"][b_of(t.start_ts)] += 1
    for t in misses:
        timeline["misses"][b_of(t.start_ts)] += 1

    # ---- Placement / image cache by node
    nodes = sorted({t.restore.get("_node") for t in joined if t.restore.get("_node")})
    nidx = {nd: i for i, nd in enumerate(nodes)}
    hits = [[0] * n_bins for _ in nodes]
    tot = [[0] * n_bins for _ in nodes]
    miss_by_image: Counter = Counter()
    pulls = set()
    for t in joined:
        nd = t.restore.get("_node")
        if nd not in nidx:
            continue
        b = b_of(t.start_ts)
        tot[nidx[nd]][b] += 1
        if t in misses:
            miss_by_image[t.image] += 1
            pulls.add((nd, t.image))
        else:
            hits[nidx[nd]][b] += 1
    cache_rows = [[_r(h / c, 3) if c else None for h, c in zip(hr, tr)] for hr, tr in zip(hits, tot)]
    node_ratio = {nd: (sum(hits[i]) / sum(tot[i]) if sum(tot[i]) else None) for nd, i in nidx.items()}
    worst = min((nd for nd in node_ratio if node_ratio[nd] is not None),
                key=lambda nd: node_ratio[nd], default=None)
    placement = {
        "nodes": nodes, "rows": cache_rows,
        "pulls_total": len(pulls),
        "pulls_per_new_image": _r(len(pulls) / len(miss_by_image), 2) if miss_by_image else None,
        "worst_node": worst, "worst_ratio": _r(node_ratio.get(worst), 3) if worst else None,
        "unpack_p50_miss": _r(_q([phase(t, "oci_unpack") for t in misses], .5), 3),
        "unpack_p50_hit": _r(_q([phase(t, "oci_unpack") for t in steady], .5), 4),
    }

    # ---- Trajectories
    per_batch = defaultdict(list)
    for t in ok:
        if t.total_s:
            per_batch[t.batch].append(t)
    batches = sorted(per_batch, key=lambda b: (b is None, b))
    traj_stats = []
    for b in batches:
        lst = per_batch[b]
        tt = [t.total_s for t in lst]
        traj_stats.append({
            "batch": b, "n": len(lst), "p50": _r(_q(tt, .5), 2), "p90": _r(_q(tt, .9), 2),
            "p99": _r(_q(tt, .99), 2), "max": _r(max(tt), 2),
            "acquire": _r(statistics.median(t.eff_acquire_s for t in lst), 3),
            "exec": _r(statistics.median(t.exec_s for t in lst), 3),
            "other": _r(statistics.median(max(0.0, t.total_s - t.eff_acquire_s - t.exec_s) for t in lst), 3),
        })
    slowest = sorted((t for t in ok if t.total_s), key=lambda t: -t.total_s)[:10]

    return {
        "meta": meta,
        "kpis": kpis,
        "errors": errors.most_common(),
        "sandboxes": {"workers": workers, "heat": heat,
                      "outcome": [dict(c) for c in outcome],
                      "hist": {"edges": HIST_EDGES, "counts": hist}},
        "breakdown": {"bins": breakdown, "steady": breakdown_steady},
        "timeline": timeline,
        "placement": placement,
        "trajectories": {
            "per_batch": traj_stats,
            "slowest": [{"id": t.traj_id, "batch": t.batch, "worker": t.worker,
                         "total": _r(t.total_s, 2), "acquire": _r(t.eff_acquire_s, 3),
                         "exec": _r(t.exec_s, 2), "source": t.source,
                         "node": (t.restore or {}).get("_node"), "actor": t.actor} for t in slowest],
        },
    }

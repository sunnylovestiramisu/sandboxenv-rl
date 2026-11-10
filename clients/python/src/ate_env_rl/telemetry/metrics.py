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

"""Prometheus histograms for the ate_env_rl client, behind the ``metrics`` extra.

``prometheus_client`` is imported and the ``ate_env_client_*`` collectors are
registered lazily: nothing touches the default registry at import. Client
timings are recorded once :func:`enable_client_metrics` has been called (a
metrics-enabled ``ate_env_rl.observability.Observer`` calls it); each histogram
registers on its first observation.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import threading
import time
import weakref
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .trace_recorder import get_bench_context, is_measure_mode

logger = logging.getLogger("ate_env_rl.telemetry.metrics")

# One bucket set, in seconds, for every ate_env histogram.
SEC_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600)

_MISSING_EXTRA_MSG = (
    "prometheus_client not installed; metrics disabled. "
    "Install the metrics extra: pip install 'ate-env-rl[metrics]'"
)

_LOCK = threading.Lock()
_COLLECTOR_LOCK = threading.RLock()
_PROM: Any = None  # the prometheus_client module once imported, or False
_MISSING_LOGGED = False


def _load_prom() -> Any:
    """Import prometheus_client on first use; returns the module or None."""
    global _PROM  # noqa: PLW0603
    if _PROM is None:
        with _LOCK:
            if _PROM is None:
                try:
                    import prometheus_client  # pylint: disable=import-outside-toplevel
                except ImportError:
                    _PROM = False
                else:
                    _PROM = prometheus_client
    return _PROM or None


def prometheus_available() -> bool:
    """Return True when prometheus_client is importable."""
    return _load_prom() is not None


def log_missing_extra() -> None:
    """Log the missing-extra warning once per process."""
    global _MISSING_LOGGED  # noqa: PLW0603
    with _LOCK:
        if _MISSING_LOGGED:
            return
        _MISSING_LOGGED = True
    logger.warning(_MISSING_EXTRA_MSG)


def status_of(exc: BaseException) -> str:
    """The ``status`` label for a block that raised ``exc``.

    ``cancelled`` for a cancellation (``asyncio.CancelledError``, for example a
    trainer that cancels stragglers) or an interrupt (``KeyboardInterrupt``),
    so these do not count as errors; ``error`` otherwise.
    """
    if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
        return "cancelled"
    return "error"


# name -> (collector, type, label names), one map per registry. The global
# registry uses _COLLECTORS; a custom registry gets its own map, held weakly so
# that it goes away with the registry. Keying by id(registry) is not safe: a
# new registry can reuse a dead one's id and would get the dead one's collector.
_Entry = tuple[Any, type, tuple[str, ...]]
_COLLECTORS: dict[str, _Entry] = {}
_REGISTRY_COLLECTORS: weakref.WeakKeyDictionary[Any, dict[str, _Entry]] = (
    weakref.WeakKeyDictionary())


def _metric(
    cls: Any,
    name: str,
    documentation: str,
    labelnames: Any = (),
    *,
    registry: Any = None,
    **kwargs: Any,
) -> Any:
    """Create a collector, or reuse the one this module created under ``name``.

    Idempotent across a second caller. A reuse must ask for the same type and
    label names, else ValueError. ``registry`` is a
    ``prometheus_client.CollectorRegistry`` (default: the global registry).
    After a module reload the cache is empty; the collector the earlier
    module registered is then looked up in the registry and checked the same
    way.
    """
    labels = tuple(labelnames)
    with _COLLECTOR_LOCK:
        cache = (_COLLECTORS if registry is None
                 else _REGISTRY_COLLECTORS.setdefault(registry, {}))
        hit = cache.get(name)
        if hit is None:
            reg_kw = {} if registry is None else {"registry": registry}
            try:
                collector = cls(name, documentation, labels, **reg_kw, **kwargs)
            except ValueError:
                collector = _registered(name, registry)
                if collector is None:
                    raise
            hit = cache[name] = (
                collector,
                type(collector),
                tuple(getattr(collector, "_labelnames", labels)),
            )
    collector, kind, have = hit
    if not issubclass(kind, cls) or have != labels:
        raise ValueError(
            f"metric {name!r} already exists as {kind.__name__}{list(have)}, "
            f"not {cls.__name__}{list(labels)}"
        )
    return collector


def _registered(name: str, registry: Any) -> Any:
    """The collector already registered under ``name`` (module reload), or None."""
    prom = _load_prom()
    reg = registry if registry is not None else (prom.REGISTRY if prom else None)
    # _names_to_collectors is a private prometheus_client attribute (there is no
    # public lookup by name). It is read only on this reload path.
    names = getattr(reg, "_names_to_collectors", {})
    for cand in (name, name + "_total", name + "_created"):
        if cand in names:
            return names[cand]
    return None


# op -> (family, help)
CLIENT_FAMILIES: dict[str, tuple[str, str]] = {
    "acquire": (
        "ate_env_client_acquire_seconds",
        "Time for the client to acquire an environment (claim or create)",
    ),
    "exec": ("ate_env_client_exec_seconds", "Time to run a command in an environment"),
    "suspend": ("ate_env_client_suspend_seconds", "Time to suspend an environment"),
    "warm_claim": (
        "ate_env_client_warm_claim_seconds",
        "Time to validate a warm claim",
    ),
    "release": (
        "ate_env_client_release_seconds",
        "Time to release (delete or recycle) an environment",
    ),
}

# Sub-phase attribution histograms separating client-side slot queuing, wire
# claim/creation RPCs, and ready-to-first-command idle latency.
SUBPHASE_FAMILIES: dict[str, tuple[str, str]] = {
    "slot_wait": (
        "ate_env_client_slot_wait_seconds",
        "Time waiting for a concurrency slot before acquiring an environment",
    ),
    "wire_claim": (
        "ate_env_client_wire_claim_seconds",
        "Time on the wire to claim or create an environment during acquire",
    ),
    "ready_to_first_command": (
        "ate_env_client_ready_to_first_command_seconds",
        "Time from environment acquire ready until the first command begins",
    ),
}

_ALLOWED_SOURCES = frozenset({"warm_pool", "cold_create", "mock", "direct"})

_ENABLED = False
_HISTOGRAMS: dict[str, Any] = {}
_SUBPHASE_HISTOGRAMS: dict[str, Any] = {}

_SUMMARY_LOCK = threading.Lock()
_RECORDED_OPS: dict[str, dict[str, Any]] = {}
_RECORDED_SUBPHASES: dict[tuple[str, str], dict[str, Any]] = {}


def enable_client_metrics() -> bool:
    """Start recording ``ate_env_client_*`` timings in this process.

    The switch is process-global and one-way: there is no way to turn it off,
    so a later fleet with ``enable_metrics=False`` still records them.
    Returns False (after one warning) when prometheus_client is not installed.
    """
    global _ENABLED  # noqa: PLW0603
    if _load_prom() is None:
        log_missing_extra()
        return False
    _ENABLED = True
    return True


def client_metrics_enabled() -> bool:
    """Return True when Prometheus client metrics are enabled."""
    return _ENABLED


def _histogram(op: str) -> Any:
    hist = _HISTOGRAMS.get(op)
    if hist is None:
        # Before _LOCK: _load_prom takes _LOCK itself on the first import.
        prom = _load_prom()
        with _LOCK:
            hist = _HISTOGRAMS.get(op)
            if hist is None:
                name, doc = CLIENT_FAMILIES[op]
                hist = _metric(prom.Histogram, name, doc, ["status"], buckets=SEC_BUCKETS)
                _HISTOGRAMS[op] = hist
    return hist


def _subphase_histogram(phase: str) -> Any:
    hist = _SUBPHASE_HISTOGRAMS.get(phase)
    if hist is None:
        prom = _load_prom()  # before _LOCK, as in _histogram
        with _LOCK:
            hist = _SUBPHASE_HISTOGRAMS.get(phase)
            if hist is None:
                name, doc = SUBPHASE_FAMILIES[phase]
                hist = _metric(
                    prom.Histogram,
                    name,
                    doc,
                    ["status", "source"],
                    buckets=SEC_BUCKETS,
                )
                _SUBPHASE_HISTOGRAMS[phase] = hist
    return hist


def _record_inmem_op(op: str, seconds: float, status: str) -> None:
    dur_s = max(0.0, float(seconds))
    with _SUMMARY_LOCK:
        entry = _RECORDED_OPS.get(op)
        if entry is None:
            entry = {
                "count": 0,
                "ok": 0,
                "error": 0,
                "cancelled": 0,
                "sum_s": 0.0,
                "max_s": 0.0,
            }
            _RECORDED_OPS[op] = entry
        entry["count"] += 1
        entry[status if status in ("ok", "error", "cancelled") else "error"] += 1
        entry["sum_s"] += dur_s
        entry["max_s"] = max(entry["max_s"], dur_s)


def _record_inmem_subphase(
    phase: str, seconds: float, status: str, source: str
) -> None:
    dur_s = max(0.0, float(seconds))
    bounded_source = source if source in _ALLOWED_SOURCES else "direct"
    key = (phase, bounded_source)
    with _SUMMARY_LOCK:
        entry = _RECORDED_SUBPHASES.get(key)
        if entry is None:
            entry = {
                "phase": phase,
                "source": bounded_source,
                "count": 0,
                "ok": 0,
                "error": 0,
                "sum_s": 0.0,
                "max_s": 0.0,
            }
            _RECORDED_SUBPHASES[key] = entry
        entry["count"] += 1
        if status == "ok":
            entry["ok"] += 1
        else:
            entry["error"] += 1
        entry["sum_s"] += dur_s
        entry["max_s"] = max(entry["max_s"], dur_s)


def observe(op: str, seconds: float, status: str = "ok") -> None:
    """Record one client timing; a no-op until client metrics or measure mode are enabled."""
    measure = is_measure_mode()
    if measure and not _ENABLED and _load_prom() is not None:
        enable_client_metrics()
    if not _ENABLED and not measure:
        return
    _record_inmem_op(op, seconds, status)
    if _ENABLED and op in CLIENT_FAMILIES:
        _histogram(op).labels(status=status).observe(seconds)


def observe_subphase(
    phase: str,
    seconds: float,
    status: str = "ok",
    *,
    source: str = "direct",
) -> None:
    """Record one sub-phase timing (slot_wait, wire_claim, or ready_to_first_command)."""
    measure = is_measure_mode()
    if measure and not _ENABLED and _load_prom() is not None:
        enable_client_metrics()
    if not _ENABLED and not measure:
        return
    bounded_source = source if source in _ALLOWED_SOURCES else "direct"
    _record_inmem_subphase(phase, seconds, status, bounded_source)
    if _ENABLED and phase in SUBPHASE_FAMILIES:
        _subphase_histogram(phase).labels(
            status=status, source=bounded_source
        ).observe(seconds)


@contextlib.contextmanager
def timed(op: str) -> Iterator[None]:
    """Time the block into ``ate_env_client_<op>_seconds`` (status ok, error or cancelled)."""
    if not _ENABLED and not is_measure_mode():
        yield
        return
    start = time.monotonic()
    status = "ok"
    try:
        yield
    except BaseException as e:
        status = status_of(e)
        raise
    finally:
        observe(op, time.monotonic() - start, status)


def get_recorded_summary() -> dict[str, Any]:
    """Return a snapshot of recorded operation and sub-phase timings."""
    with _SUMMARY_LOCK:
        ops = {k: dict(v) for k, v in _RECORDED_OPS.items()}
        subphases = {
            f"{phase}:{src}": dict(v)
            for (phase, src), v in _RECORDED_SUBPHASES.items()
        }
    return {"ops": ops, "subphases": subphases}


def reset_recorded_metrics() -> None:
    """Clear in-memory recorded operation and sub-phase summaries."""
    with _SUMMARY_LOCK:
        _RECORDED_OPS.clear()
        _RECORDED_SUBPHASES.clear()


def render_prometheus() -> str:
    """Render Prometheus text format from prometheus_client or the in-memory summary."""
    prom = _load_prom()
    if prom is not None and _ENABLED:
        raw: bytes = prom.generate_latest(prom.REGISTRY)
        text = raw.decode("utf-8", errors="replace")
        if text.strip():
            return text
    lines: list[str] = []
    snap = get_recorded_summary()
    for op, stats in sorted(snap["ops"].items()):
        fam = CLIENT_FAMILIES.get(op, (f"ate_env_client_{op}_seconds", f"Client {op} latency"))[0]
        lines.append(f"# TYPE {fam} histogram")
        for st in ("ok", "error", "cancelled"):
            cnt = stats.get(st, 0)
            if cnt > 0 or st == "ok":
                lines.append(f'{fam}_count{{status="{st}"}} {cnt}')
        lines.append(f'{fam}_sum {stats["sum_s"]:.6f}')
    for (_key, stats) in sorted(snap["subphases"].items()):
        phase = stats["phase"]
        src = stats["source"]
        fam = SUBPHASE_FAMILIES.get(
            phase, (f"ate_env_client_{phase}_seconds", f"Subphase {phase} latency")
        )[0]
        lines.append(f"# TYPE {fam} histogram")
        lines.append(
            f'{fam}_count{{source="{src}",status="ok"}} {stats["ok"]}'
        )
        lines.append(
            f'{fam}_sum{{source="{src}",status="ok"}} {stats["sum_s"]:.6f}'
        )
    return "\n".join(lines) + ("\n" if lines else "")


def flush_step_events(
    step: int | None = None,
    *,
    events_path: str | Path | None = None,
    run_id: str | None = None,
    job: str | None = None,
) -> list[dict[str, Any]]:
    """Flush aggregated step-boundary summary events to ENV_BENCH_EVENTS_PATH if configured."""
    snap = get_recorded_summary()
    ctx = get_bench_context()
    resolved_run = run_id or ctx.get("run") or "bench-run"
    resolved_job = job or ctx.get("job")
    resolved_step = step if step is not None else ctx.get("step", 0)
    ts_now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    events: list[dict[str, Any]] = []
    for op, stats in sorted(snap["ops"].items()):
        count = int(stats["count"])
        if count <= 0:
            continue
        sum_ms = round(stats["sum_s"] * 1000.0, 3)
        max_ms = round(stats["max_s"] * 1000.0, 3)
        mean_ms = round(sum_ms / count, 3)
        ev: dict[str, Any] = {
            "schema_version": "v1",
            "ts": ts_now,
            "run": str(resolved_run),
            "event": op,
            "dur_ms": sum_ms,
            "ok": stats["error"] == 0,
            "count": count,
            "metrics": {
                "count": float(count),
                "dur_ms_sum": sum_ms,
                "dur_ms_mean": mean_ms,
                "dur_ms_max": max_ms,
                "errors": float(stats["error"]),
            },
        }
        if resolved_job:
            ev["job"] = str(resolved_job)
        if resolved_step is not None:
            ev["step"] = int(resolved_step)
        events.append(ev)

    for _key, stats in sorted(snap["subphases"].items()):
        count = int(stats["count"])
        if count <= 0:
            continue
        sum_ms = round(stats["sum_s"] * 1000.0, 3)
        max_ms = round(stats["max_s"] * 1000.0, 3)
        mean_ms = round(sum_ms / count, 3)
        ev = {
            "schema_version": "v1",
            "ts": ts_now,
            "run": str(resolved_run),
            "event": stats["phase"],
            "source": stats["source"],
            "dur_ms": sum_ms,
            "ok": stats["error"] == 0,
            "count": count,
            "metrics": {
                "count": float(count),
                "dur_ms_sum": sum_ms,
                "dur_ms_mean": mean_ms,
                "dur_ms_max": max_ms,
                "errors": float(stats["error"]),
            },
        }
        if resolved_job:
            ev["job"] = str(resolved_job)
        if resolved_step is not None:
            ev["step"] = int(resolved_step)
        events.append(ev)

    target_raw = (
        str(events_path).strip()
        if events_path is not None
        else os.environ.get("ENV_BENCH_EVENTS_PATH", "").strip()
    )
    if target_raw and events:
        target = Path(target_raw)
        with _SUMMARY_LOCK:
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("a", encoding="utf-8") as f:
                for ev in events:
                    f.write(json.dumps(ev, separators=(",", ":")) + "\n")
    return events

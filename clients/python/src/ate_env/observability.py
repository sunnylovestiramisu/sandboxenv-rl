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

"""Fleet observability: per-phase timings, Prometheus series and spans.

Three layers, mirroring agent-sandbox-rl's ``observability.py`` so reports from
both stacks compare side by side:

1. ``RunReport``: always on and dependency free; per-phase count, total and
   max, claims, task outcomes and warm replicas. ``fleet.report`` after
   ``fleet.run(...)``.
2. Prometheus ``atefleet_*`` series (``metrics`` extra; on by default through
   ``ObservabilityConfig.enable_metrics``). Collectors register on the first
   metrics-enabled ``Observer``, never at import.
3. OpenTelemetry ``atefleet.<phase>`` spans (``tracing`` extra; opt in with
   ``enable_tracing``) through the client's provider, so ``ate_env_client.*``
   spans nest under them.

Labels are bounded: ``phase``, ``backend`` (``substrate`` or ``mock``),
``family`` (from :func:`repo_family`), ``strategy`` and ``status`` (``ok`` or
``error``). Environment ids, run ids, commands, paths and image tags never
become labels.
"""

from __future__ import annotations

import contextlib
import datetime
import json
import logging
import pathlib
import re
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

from .config import ObservabilityConfig, TracerConfig
from .telemetry import metrics as _client_metrics
from .telemetry.metrics import SEC_BUCKETS, _load_prom, _metric, log_missing_extra
from .telemetry.tracing import set_attributes, start_span, tracer_from_config

__all__ = ["Observer", "RunReport", "repo_family", "serve_metrics"]

logger = logging.getLogger("ate_env.observability")

FLEET_SPAN_PREFIX = "atefleet"

# The atefleet_* collectors, created by _ensure_metrics on the first
# metrics-enabled Observer, never at import.
PHASE_LATENCY = TASK_LATENCY = RUN_LATENCY = CLAIMS = TASKS = WARM_REPLICAS = None
_METRICS_READY = False
_METRICS_LOCK = threading.Lock()


def _ensure_metrics() -> bool:
    """Register the ``atefleet_*`` collectors once; False without prometheus_client."""
    global _METRICS_READY, PHASE_LATENCY, TASK_LATENCY, RUN_LATENCY
    global CLAIMS, TASKS, WARM_REPLICAS
    if _METRICS_READY:
        return True
    prom = _load_prom()
    if prom is None:
        log_missing_extra()
        return False
    with _METRICS_LOCK:
        if _METRICS_READY:
            return True
        PHASE_LATENCY = _metric(
            prom.Histogram, "atefleet_phase_latency_seconds", "Fleet phase latency",
            ["phase", "backend", "family", "strategy", "status"], buckets=SEC_BUCKETS)
        TASK_LATENCY = _metric(
            prom.Histogram, "atefleet_task_latency_seconds",
            "Per-task acquire+process+release latency",
            ["strategy", "backend", "family", "status"], buckets=SEC_BUCKETS)
        RUN_LATENCY = _metric(
            prom.Histogram, "atefleet_run_latency_seconds", "Whole-run latency",
            ["strategy", "status"], buckets=SEC_BUCKETS)
        CLAIMS = _metric(prom.Counter, "atefleet_claims_total", "Environment claims started",
                         ["backend", "status"])
        TASKS = _metric(prom.Counter, "atefleet_tasks_total", "Tasks processed",
                        ["strategy", "status"])
        WARM_REPLICAS = _metric(prom.Gauge, "atefleet_warm_replicas", "Current warm replicas",
                                ["backend"])
        _METRICS_READY = True
    return True


_FAMILY_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")
_ARCH_MARKERS = ("x86_64.", "arm64.")


def repo_family(task_or_image: Any) -> str:
    """A bounded-cardinality label for a task or image: its repo family.

    Prefers ``task.metadata["repo"]`` (``django/django`` -> ``django``); else
    parses a SWE-bench image name
    (``sweb.eval.x86_64.astropy__astropy-12907:latest`` -> ``astropy``); else
    ``"unknown"``.
    """
    repo = None
    metadata = getattr(task_or_image, "metadata", None)
    if isinstance(metadata, dict):
        repo = metadata.get("repo")
    image = getattr(task_or_image, "image", task_or_image)
    candidate = None
    if repo:
        candidate = str(repo).rstrip("/").split("/")[-1]
    else:
        s = str(image or "")
        for arch in _ARCH_MARKERS:
            if arch in s:
                # ...<arch><repo>__<instance>:<tag> (or _1776_ in Docker Hub names)
                tail = s.split(arch)[-1]
                candidate = tail.split("__")[0].split("_1776_")[0].split(":")[0].split("@")[0]
                break
    if candidate:
        candidate = candidate.lower()
        if _FAMILY_RE.match(candidate):
            return candidate
    return "unknown"


@dataclass
class RunReport:
    """Always-on per-run performance summary (agent-sandbox-rl's shape)."""

    strategy: str
    total_s: float = 0.0
    phases: dict[str, list[float]] = field(default_factory=dict)  # name -> [count, total_s, max_s]
    claims: int = 0
    tasks_ok: int = 0
    tasks_err: int = 0
    warm_total: int = 0
    peak_warm: int = 0
    environment: dict[str, dict[str, Any]] = field(default_factory=dict)

    ORDER = ("preflight", "plan", "ensure_template", "warm_pool", "acquire", "process",
             "recycle", "release", "reap", "teardown")

    def add_phase(self, name: str, dur: float) -> None:
        c = self.phases.setdefault(name, [0, 0.0, 0.0])
        c[0] += 1
        c[1] += dur
        c[2] = max(c[2], dur)

    def add_task(self, status: str) -> None:
        if status == "ok":
            self.tasks_ok += 1
        else:
            self.tasks_err += 1

    def add_claim(self) -> None:
        self.claims += 1

    def _ordered(self) -> list[tuple[str, list[float]]]:
        names = list(self.ORDER) + [p for p in self.phases if p not in self.ORDER]
        return [(p, self.phases[p]) for p in names if p in self.phases]

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "total_s": round(self.total_s, 2),
            "phases": {p: {"count": int(c[0]), "total_s": round(c[1], 2), "max_s": round(c[2], 2)}
                       for p, c in self._ordered()},
            "claims": self.claims,
            "tasks_ok": self.tasks_ok,
            "tasks_err": self.tasks_err,
            "warm_replicas_total": self.warm_total,
            "warm_replicas_peak": self.peak_warm,
            "environment": self.environment,
        }

    @staticmethod
    def _fmt_env(info: dict[str, Any]) -> str:
        parts = []
        for k, v in info.items():
            if v in (None, [], ""):
                continue
            if isinstance(v, list):
                v = "[" + ",".join(str(x) for x in v) + "]"
            parts.append(f"{k}={v}")
        return "  ".join(parts)

    def summary(self) -> str:
        lines = [f"── Run report (strategy={self.strategy}) ──"]
        if self.environment:
            lines.append("  environment:")
            for name, info in self.environment.items():
                lines.append(f"    {name}: {self._fmt_env(info)}")
            lines.append("  " + "-" * 40)
        for name, (count, total, mx) in self._ordered():
            lines.append(f"  {name:<18} {total:8.2f}s  (n={int(count)}, max={mx:.2f}s)")
        lines.append("  " + "-" * 40)
        lines.append(f"  {'TOTAL':<18} {self.total_s:8.2f}s")
        lines.append(f"  claims={self.claims}  tasks={self.tasks_ok}ok/{self.tasks_err}err"
                     f"  warm_replicas total={self.warm_total} peak={self.peak_warm}")
        return "\n".join(lines)

    def write(self, report_dir: str | pathlib.Path, n_tasks: int | None = None) -> pathlib.Path:
        """Write ``<strategy>_<n>tasks_<YYYYMMDD-HHMMSS>.txt`` (summary) and ``.json``.

        n_tasks defaults to the tasks this report counted. Returns the common
        path without suffix.
        """
        out = pathlib.Path(report_dir)
        out.mkdir(parents=True, exist_ok=True)
        n = self.tasks_ok + self.tasks_err if n_tasks is None else n_tasks
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        base = out / f"{self.strategy}_{n}tasks_{stamp}"
        base.with_name(base.name + ".txt").write_text(self.summary() + "\n")
        base.with_name(base.name + ".json").write_text(json.dumps(self.to_dict(), indent=2) + "\n")
        return base


class Observer:
    """Records phase, task and run timings to the RunReport, Prometheus and spans."""

    def __init__(self, config: ObservabilityConfig | None = None):
        self.config = config or ObservabilityConfig()
        # Registering collectors is deferred to here, so importing the package
        # never touches the global registry.
        self.metrics = bool(self.config.enable_metrics and _ensure_metrics())
        if self.metrics:
            _client_metrics.enable_client_metrics()
            # Held per instance so a module reload never leaves this Observer
            # pointing at cleared globals.
            self._phase_latency, self._task_latency, self._run_latency = (
                PHASE_LATENCY, TASK_LATENCY, RUN_LATENCY)
            self._claims, self._tasks, self._warm_replicas = CLAIMS, TASKS, WARM_REPLICAS
        self.tracer_manager, self.tracer = tracer_from_config(TracerConfig(
            enable_tracing=self.config.enable_tracing,
            trace_service_name=self.config.trace_service_name,
        ))
        self.report: RunReport | None = None
        self._in_run = False
        self._strategy = "-"
        self._lock = threading.Lock()
        self._warm: dict[str, int] = {}

    @contextlib.contextmanager
    def phase(self, name: str, *, backend: str = "-", family: str = "-") -> Iterator[Any]:
        """Time a phase; status is ``error`` when the block raises."""
        start = time.monotonic()
        status = "ok"
        with start_span(self.tracer, f"{FLEET_SPAN_PREFIX}.{name}") as span:
            set_attributes(span, {"strategy": self._strategy, "backend": backend, "family": family})
            try:
                yield span
            except BaseException:
                status = "error"
                raise
            finally:
                dur = time.monotonic() - start
                if self.metrics:
                    self._phase_latency.labels(phase=name, backend=backend, family=family,
                                         strategy=self._strategy, status=status).observe(dur)
                if self._in_run and self.report is not None:
                    with self._lock:
                        self.report.add_phase(name, dur)

    @contextlib.contextmanager
    def run(self, strategy: str, *, backend: str = "-") -> Iterator[RunReport]:
        """Time a whole run; yields the fresh RunReport."""
        self._strategy = strategy
        self.report = RunReport(strategy)
        self._in_run = True
        start = time.monotonic()
        status = "ok"
        with start_span(self.tracer, f"{FLEET_SPAN_PREFIX}.run") as span:
            set_attributes(span, {"strategy": strategy, "backend": backend})
            try:
                yield self.report
            except BaseException:
                status = "error"
                raise
            finally:
                self._in_run = False
                self.report.total_s = time.monotonic() - start
                if self.metrics:
                    self._run_latency.labels(strategy=strategy, status=status).observe(self.report.total_s)

    def task_done(self, backend: str, family: str, status: str, seconds: float) -> None:
        if self.metrics:
            self._task_latency.labels(strategy=self._strategy, backend=backend,
                                family=family, status=status).observe(seconds)
            self._tasks.labels(strategy=self._strategy, status=status).inc()
        if self._in_run and self.report is not None:
            with self._lock:
                self.report.add_task(status)

    def claim(self, backend: str, status: str = "ok") -> None:
        if self.metrics:
            self._claims.labels(backend=backend, status=status).inc()
        if self._in_run and self.report is not None and status == "ok":
            with self._lock:
                self.report.add_claim()

    def warm_add(self, backend: str, n: int) -> None:
        if n <= 0:
            return
        # The gauge is set under the lock so concurrent add/remove never lose an update.
        with self._lock:
            self._warm[backend] = self._warm.get(backend, 0) + n
            current = sum(self._warm.values())
            if self._in_run and self.report is not None:
                self.report.warm_total += n
                self.report.peak_warm = max(self.report.peak_warm, current)
            if self.metrics:
                self._warm_replicas.labels(backend=backend).set(self._warm[backend])

    def warm_remove(self, backend: str, n: int) -> None:
        if n <= 0:
            return
        with self._lock:
            self._warm[backend] = max(0, self._warm.get(backend, 0) - n)
            if self.metrics:
                self._warm_replicas.labels(backend=backend).set(self._warm[backend])

    def warm_reset(self) -> None:
        with self._lock:
            backends = list(self._warm.keys())
            self._warm.clear()
            if self.metrics:
                for b in backends:
                    self._warm_replicas.labels(backend=b).set(0)


class _NullObserver(Observer):
    """An Observer that records nothing; used when a fleet has none."""

    def __init__(self) -> None:
        super().__init__(ObservabilityConfig(enable_metrics=False, enable_tracing=False))


_NULL_OBSERVER: Observer | None = None


def null_observer() -> Observer:
    global _NULL_OBSERVER
    if _NULL_OBSERVER is None:
        _NULL_OBSERVER = _NullObserver()
    return _NULL_OBSERVER


def serve_metrics(port: int = 9095, addr: str = "0.0.0.0") -> Any:
    """Expose the default Prometheus registry over HTTP (``/metrics``).

    Returns what ``prometheus_client.start_http_server`` returns (the server
    and its thread); the caller owns their lifetime. Raises RuntimeError
    without the ``metrics`` extra.
    """
    prom = _load_prom()
    if prom is None:
        raise RuntimeError(
            "prometheus_client not installed; install the metrics extra: "
            "pip install 'ate-env-client[metrics]'"
        )
    return prom.start_http_server(port, addr)

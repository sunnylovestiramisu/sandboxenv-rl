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
2. Prometheus ``ate_env_fleet_*`` series (``metrics`` extra; on by default whenever
   prometheus_client is importable, see ``ObservabilityConfig.enable_metrics``).
   Collectors register on the first metrics-enabled ``Observer``, never at
   import.
3. OpenTelemetry ``ate_env_fleet.<phase>`` spans (``tracing`` extra; opt in with
   ``enable_tracing``) through the client's provider, so ``ate_env_client.*``
   spans nest under them.

Labels are bounded: ``phase``, ``backend`` (``substrate`` or ``mock``),
``family`` (from :func:`repo_family`, at most :data:`MAX_FAMILIES` distinct
values per process), ``strategy`` and ``status`` (``ok``, ``error`` or
``cancelled``). Environment ids, run ids, commands, paths and image tags never
become labels.
"""

from __future__ import annotations

import contextlib
import datetime
import json
import logging
import math
import pathlib
import re
import threading
import time
import weakref
from collections.abc import Iterator
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

from .config import ObservabilityConfig, TracerConfig
from .telemetry import metrics as _client_metrics
from .telemetry.metrics import (
    SEC_BUCKETS,
    _load_prom,
    _metric,
    log_missing_extra,
    prometheus_available,
    status_of,
)
from .telemetry.trace_recorder import is_measure_mode, should_auto_enable_tracing
from .telemetry.tracing import set_attributes, start_span, tracer_from_config

__all__ = ["MAX_FAMILIES", "Observer", "RunReport", "repo_family", "serve_metrics"]

logger = logging.getLogger("ate_env_rl.observability")

FLEET_SPAN_PREFIX = "ate_env_fleet"

# The ate_env_fleet_* collectors on the default registry, created by _ensure_metrics
# on the first metrics-enabled Observer, never at import.
PHASE_LATENCY = TASK_LATENCY = RUN_LATENCY = CLAIMS = TASKS = WARM_REPLICAS = None
RELEASE_FAILURES = None
_METRICS_READY = False
_METRICS_LOCK = threading.Lock()
# Collector sets for caller-supplied registries, held weakly so that each set
# goes away with its registry. Keying by id(registry) is not safe: a new registry
# can reuse a dead one's id and would get the dead one's collectors.
_CUSTOM_COLLECTORS: weakref.WeakKeyDictionary[Any, SimpleNamespace] = (
    weakref.WeakKeyDictionary())


def _build_collectors(prom: Any, registry: Any) -> SimpleNamespace:
    reg = {} if registry is None else {"registry": registry}
    return SimpleNamespace(
        phase_latency=_metric(
            prom.Histogram,
            "ate_env_fleet_phase_latency_seconds",
            "Fleet phase latency",
            ["phase", "backend", "family", "strategy", "status"],
            buckets=SEC_BUCKETS,
            **reg,
        ),
        task_latency=_metric(
            prom.Histogram,
            "ate_env_fleet_task_latency_seconds",
            "Per-task acquire+process+release latency",
            ["strategy", "backend", "family", "status"],
            buckets=SEC_BUCKETS,
            **reg,
        ),
        run_latency=_metric(
            prom.Histogram,
            "ate_env_fleet_run_latency_seconds",
            "Whole-run latency",
            ["strategy", "status"],
            buckets=SEC_BUCKETS,
            **reg,
        ),
        claims=_metric(
            prom.Counter,
            "ate_env_fleet_claims_total",
            "Environment claims started",
            ["backend", "status"],
            **reg,
        ),
        tasks=_metric(
            prom.Counter,
            "ate_env_fleet_tasks_total",
            "Tasks processed",
            ["strategy", "status"],
            **reg,
        ),
        release_failures=_metric(
            prom.Counter,
            "ate_env_fleet_release_failures_total",
            "Environment releases (deletes) that failed; the environment may have leaked",
            ["backend"],
            **reg,
        ),
        warm_replicas=_metric(
            prom.Gauge,
            "ate_env_fleet_warm_replicas",
            "Current warm replicas",
            ["backend"],
            **reg,
        ),
    )


def _ensure_metrics(registry: Any = None) -> SimpleNamespace | None:
    """The ``ate_env_fleet_*`` collectors for ``registry`` (default: the global one).

    Registers them once per registry; None (after one warning) without
    prometheus_client.
    """
    global _METRICS_READY, PHASE_LATENCY, TASK_LATENCY, RUN_LATENCY
    global CLAIMS, TASKS, WARM_REPLICAS, RELEASE_FAILURES
    prom = _load_prom()
    if prom is None:
        log_missing_extra()
        return None
    with _METRICS_LOCK:
        if registry is not None and registry is not prom.REGISTRY:
            if registry not in _CUSTOM_COLLECTORS:
                _CUSTOM_COLLECTORS[registry] = _build_collectors(prom, registry)
            return _CUSTOM_COLLECTORS[registry]
        if not _METRICS_READY:
            c = _build_collectors(prom, None)
            PHASE_LATENCY, TASK_LATENCY, RUN_LATENCY = (
                c.phase_latency,
                c.task_latency,
                c.run_latency,
            )
            CLAIMS, TASKS, WARM_REPLICAS = c.claims, c.tasks, c.warm_replicas
            RELEASE_FAILURES = c.release_failures
            _METRICS_READY = True
        return SimpleNamespace(
            phase_latency=PHASE_LATENCY,
            task_latency=TASK_LATENCY,
            run_latency=RUN_LATENCY,
            claims=CLAIMS,
            tasks=TASKS,
            warm_replicas=WARM_REPLICAS,
            release_failures=RELEASE_FAILURES,
        )


_FAMILY_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")
_ARCH_MARKERS = ("x86_64.", "arm64.")

#: The most distinct ``family`` label values per process. The first
#: MAX_FAMILIES repo families seen keep their name; later ones are reported as
#: ``"other"``, so a dataset with thousands of repos cannot grow the series
#: without limit. ``"unknown"`` and ``"other"`` do not count toward the cap.
MAX_FAMILIES = 64
_FAMILIES: set[str] = set()
_FAMILIES_LOCK = threading.Lock()
#: Label values with a fixed meaning (no repo family, overflow).
_RESERVED_FAMILIES = ("other", "unknown")
#: Repo families that are escaped by appending ``_repo``: a reserved name
#: followed by zero or more ``_repo``. Escaping all of them, not only the
#: reserved names, keeps the mapping one-to-one: ``other`` -> ``other_repo``,
#: ``other_repo`` -> ``other_repo_repo``, and so on, so no two repos share a
#: label value and no repo shares one with a reserved value.
_ESCAPED_FAMILY_RE = re.compile(r"^(?:other|unknown)(?:_repo)*$")


def _bounded_family(name: str) -> str:
    if _ESCAPED_FAMILY_RE.match(name):
        name += "_repo"
    if name in _FAMILIES:
        return name
    with _FAMILIES_LOCK:
        if name in _FAMILIES:
            return name
        if len(_FAMILIES) >= MAX_FAMILIES:
            return "other"
        _FAMILIES.add(name)
        return name


def repo_family(task_or_image: Any) -> str:
    """A bounded-cardinality label for a task or image: its repo family.

    Prefers ``task.metadata["repo"]`` (``django/django`` -> ``django``); else
    parses a SWE-bench image name
    (``sweb.eval.x86_64.astropy__astropy-12907:latest`` -> ``astropy``); else
    ``"unknown"``. A repo whose family is ``other`` or ``unknown`` is
    labelled ``other_repo`` / ``unknown_repo``, so it cannot be mistaken for
    the overflow or no-family value; a repo already named like an escaped
    value (``other_repo``) gets one more ``_repo``, so labels stay unique. After :data:`MAX_FAMILIES` distinct
    families in this process, any new family is ``"other"``; families seen
    before keep their name. Only SWE-bench image names are
    parsed: set ``metadata["repo"]`` for non-SWE-bench datasets (e.g.
    R2E-Gym); otherwise their family is ``"unknown"``.
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
            return _bounded_family(candidate)
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
    release_errors: int = 0
    warm_total: int = 0
    peak_warm: int = 0
    environment: dict[str, dict[str, Any]] = field(default_factory=dict)

    ORDER = (
        "preflight",
        "plan",
        "ensure_template",
        "preflight_golden",
        "preflight_probe",
        "warm_pool",
        "acquire",
        "process",
        "recycle",
        "release",
        "reap",
        "teardown",
    )

    def add_phase(self, name: str, dur: float) -> None:
        """Add one ``name`` phase of ``dur`` seconds.

        Durations come from a monotonic clock; a negative value is clamped
        to 0, so every phase keeps ``0 <= max_s <= total_s``.
        """
        dur = max(0.0, dur)
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

    def add_release_error(self) -> None:
        self.release_errors += 1

    def _ordered(self) -> list[tuple[str, list[float]]]:
        names = list(self.ORDER) + [p for p in self.phases if p not in self.ORDER]
        return [(p, self.phases[p]) for p in names if p in self.phases]

    @staticmethod
    def _phase_dict(c: list[float]) -> dict[str, Any]:
        """One ``phases`` entry, in seconds to 0.01, keeping ``0 <= max_s <= total_s <= count * max_s``.

        Rounding ``total_s`` and ``max_s`` to the nearest 0.01 separately breaks
        the bound for short phases (3 of 0.004 s give 0.01 and 0.0). So
        ``max_s`` rounds up to the next 0.01, ignoring float noise below 5e-12 s,
        and ``total_s`` rounds to the nearest 0.01, then is moved into
        ``[max_s, count * max_s]``. The move is less than 0.01 s up, or one
        float step down where ``count * max_s`` is just under a multiple of 0.01.
        A non-finite ``max_s`` is kept as is (``math.ceil`` would raise).
        """
        count = int(c[0])
        scaled = round(c[2] * 100, 9)
        max_s = math.ceil(scaled) / 100 if math.isfinite(scaled) else c[2]
        total_s = min(max(round(c[1], 2), max_s), count * max_s)
        return {"count": count, "total_s": total_s, "max_s": max_s}

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "total_s": round(self.total_s, 2),
            "phases": {p: self._phase_dict(c) for p, c in self._ordered()},
            "claims": self.claims,
            "tasks_ok": self.tasks_ok,
            "tasks_err": self.tasks_err,
            "release_errors": self.release_errors,
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
        # Rows use the to_dict() values, so the .txt and .json of one write() agree.
        for name, c in self._ordered():
            p = self._phase_dict(c)
            lines.append(
                f"  {name:<18} {p['total_s']:8.2f}s  (n={p['count']}, max={p['max_s']:.2f}s)"
            )
        lines.append("  " + "-" * 40)
        lines.append(f"  {'TOTAL':<18} {self.total_s:8.2f}s")
        lines.append(
            f"  claims={self.claims}  tasks={self.tasks_ok}ok/{self.tasks_err}err"
            f"  warm_replicas total={self.warm_total} peak={self.peak_warm}"
        )
        if self.release_errors:
            lines.append(
                f"  release_errors={self.release_errors}  (failed deletes; "
                "environments may have leaked)"
            )
        return "\n".join(lines)

    def write(self, report_dir: str | pathlib.Path, n_tasks: int | None = None) -> pathlib.Path:
        """Write ``<strategy>_<n>tasks_<YYYYMMDD-HHMMSS>.txt`` (summary) and ``.json``.

        n_tasks defaults to the tasks this report counted. Returns the common
        path without suffix. An existing report is never overwritten: when the
        name is taken (another report in the same second, for example from a
        Ray worker that shares the directory), ``-1``, ``-2``, ... is appended.
        """
        out = pathlib.Path(report_dir)
        out.mkdir(parents=True, exist_ok=True)
        n = self.tasks_ok + self.tasks_err if n_tasks is None else n_tasks
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        stem = f"{self.strategy}_{n}tasks_{stamp}"
        for i in range(10_000):
            base = out / (stem if i == 0 else f"{stem}-{i}")
            try:
                # Mode "x" claims the name atomically, also across processes.
                with open(base.with_name(base.name + ".txt"), "x") as f:
                    f.write(self.summary() + "\n")
            except FileExistsError:
                continue
            base.with_name(base.name + ".json").write_text(
                json.dumps(self.to_dict(), indent=2) + "\n"
            )
            return base
        raise FileExistsError(f"no free report name for {out / stem}")


class Observer:
    """Records phase, task and run timings to the RunReport, Prometheus and spans.

    One Observer belongs to one fleet, and it runs one ``run()`` at a time.
    ``registry`` selects a ``prometheus_client.CollectorRegistry`` for the
    ``ate_env_fleet_*`` series (default: the global registry). The client's
    ``ate_env_client_*`` histograms always use the global registry.
    """

    def __init__(self, config: ObservabilityConfig | None = None, *, registry: Any = None):
        self.config = config or ObservabilityConfig()
        # Registering collectors is deferred to here, so importing the package
        # never touches the global registry.
        self.metrics = False
        want = self.config.enable_metrics
        if want is None and not prometheus_available():
            # Default (not set by the caller): metrics are on only when the
            # extra is installed, and its absence is not a problem.
            _log_metrics_off_once()
        elif want is not False:
            # Held per instance so a module reload never leaves this Observer
            # pointing at cleared globals.
            self._collectors = _ensure_metrics(registry)
            self.metrics = self._collectors is not None
        if self.metrics:
            _client_metrics.enable_client_metrics()
            c = self._collectors
            self._phase_latency, self._task_latency, self._run_latency = (
                c.phase_latency,
                c.task_latency,
                c.run_latency,
            )
            self._claims, self._tasks, self._warm_replicas = c.claims, c.tasks, c.warm_replicas
            self._release_failures = c.release_failures
        if isinstance(self, _NullObserver):
            self.tracer = None
        else:
            tracing_on = bool(self.config.enable_tracing) or should_auto_enable_tracing(
                self.config
            )
            self.tracer = tracer_from_config(
                TracerConfig(
                    enable_tracing=tracing_on,
                    trace_service_name=self.config.trace_service_name,
                )
            )
        self.report: RunReport | None = None
        self._in_run = False
        self._strategy = "-"
        self._lock = threading.Lock()
        self._warm: dict[str, int] = {}

    @contextlib.contextmanager
    def phase(self, name: str, *, backend: str = "-", family: str = "-") -> Iterator[Any]:
        """Time a phase.

        The status is ``error`` when the block raises, and ``cancelled`` when
        it is cancelled (``asyncio.CancelledError``) or interrupted
        (``KeyboardInterrupt``).
        """
        start = time.monotonic()
        status = "ok"
        with start_span(self.tracer, f"{FLEET_SPAN_PREFIX}.{name}") as span:
            set_attributes(
                span, {"strategy": self._strategy, "backend": backend, "family": family}
            )
            try:
                yield span
            except BaseException as e:
                status = status_of(e)
                raise
            finally:
                self.record_phase(
                    name, time.monotonic() - start, backend=backend, family=family, status=status
                )

    def record_phase(
        self, name: str, dur: float, *, backend: str = "-", family: str = "-", status: str = "ok"
    ) -> None:
        """Record a phase of ``dur`` seconds that was timed elsewhere.

        ``phase()`` is the usual way; this is for a duration that spans
        callbacks, such as the wait for a golden snapshot the PreflightWarmer
        settles from a poller.
        """
        if self.metrics:
            self._phase_latency.labels(
                phase=name,
                backend=backend,
                family=family,
                strategy=self._strategy,
                status=status,
            ).observe(dur)
        if self._in_run and self.report is not None:
            with self._lock:
                self.report.add_phase(name, dur)

    @contextlib.contextmanager
    def run(self, strategy: str, *, backend: str = "-") -> Iterator[RunReport]:
        """Time a whole run; yields the fresh RunReport.

        Runs do not overlap: a second ``run()`` while one is in progress on
        this Observer (for example two concurrent ``fleet.run`` calls on one
        fleet) raises RuntimeError, so the reports never mix.
        """
        with self._lock:
            if self._in_run:
                raise RuntimeError(
                    "a run is already in progress on this fleet; runs on one fleet "
                    "must not overlap (use one fleet per concurrent run)"
                )
            self._in_run = True
            self._strategy = strategy
            report = self.report = RunReport(strategy)
        start = time.monotonic()
        status = "ok"
        try:
            with start_span(self.tracer, f"{FLEET_SPAN_PREFIX}.run") as span:
                set_attributes(span, {"strategy": strategy, "backend": backend})
                try:
                    yield report
                except BaseException as e:
                    status = status_of(e)
                    raise
        finally:
            report.total_s = time.monotonic() - start
            with self._lock:
                self._in_run = False
            if self.metrics:
                self._run_latency.labels(strategy=strategy, status=status).observe(report.total_s)
            if is_measure_mode():
                _client_metrics.flush_step_events()

    def flush_step_events(
        self,
        step: int | None = None,
        *,
        events_path: str | pathlib.Path | None = None,
    ) -> list[dict[str, Any]]:
        """Flush aggregated step-boundary summary events for measure mode."""
        return _client_metrics.flush_step_events(step=step, events_path=events_path)

    def task_done(self, backend: str, family: str, status: str, seconds: float) -> None:
        """Record one finished task that took ``seconds``.

        Durations come from a monotonic clock; a negative value is clamped
        to 0.
        """
        seconds = max(0.0, seconds)
        if self.metrics:
            self._task_latency.labels(
                strategy=self._strategy, backend=backend, family=family, status=status
            ).observe(seconds)
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

    def release_failed(self, backend: str) -> None:
        """Record a release (delete) that failed; the driver logged and did not raise."""
        if self.metrics:
            self._release_failures.labels(backend=backend).inc()
        if self._in_run and self.report is not None:
            with self._lock:
                self.report.add_release_error()

    # The warm-replica gauge is shared by every Observer in the process, so
    # each one moves it by its own deltas (inc/dec) instead of setting it to
    # its private count; several fleets then add up instead of overwriting
    # each other.

    def warm_add(self, backend: str, n: int) -> None:
        if n <= 0:
            return
        with self._lock:
            self._warm[backend] = self._warm.get(backend, 0) + n
            current = sum(self._warm.values())
            if self._in_run and self.report is not None:
                self.report.warm_total += n
                self.report.peak_warm = max(self.report.peak_warm, current)
            if self.metrics:
                self._warm_replicas.labels(backend=backend).inc(n)

    def warm_remove(self, backend: str, n: int) -> None:
        if n <= 0:
            return
        with self._lock:
            have = self._warm.get(backend, 0)
            n = min(n, have)  # never move the shared gauge below this observer's share
            self._warm[backend] = have - n
            if self.metrics and n:
                self._warm_replicas.labels(backend=backend).dec(n)

    def warm_reset(self) -> None:
        """Forget this observer's warm replicas (and only its share of the gauge)."""
        with self._lock:
            mine = dict(self._warm)
            self._warm.clear()
            if self.metrics:
                for b, n in mine.items():
                    if n:
                        self._warm_replicas.labels(backend=b).dec(n)


_METRICS_OFF_LOGGED = False


def _log_metrics_off_once() -> None:
    global _METRICS_OFF_LOGGED
    if not _METRICS_OFF_LOGGED:
        _METRICS_OFF_LOGGED = True
        logger.info(
            "prometheus_client not installed; fleet metrics off. Install the metrics "
            "extra (pip install 'ate-env-rl[metrics]') to record them."
        )


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


def serve_metrics(port: int = 9095, addr: str = "0.0.0.0", *, registry: Any = None) -> Any:
    """Expose a Prometheus registry over HTTP (``/metrics``).

    ``registry`` defaults to the global registry. The default ``addr``
    listens on all interfaces, which a scraper in the cluster needs; pass
    ``addr="127.0.0.1"`` to keep the endpoint local to the host. The port
    must be free: a busy port (for example two Ray workers on one node that
    both call ``serve_metrics(9095)``) raises ``OSError`` (``EADDRINUSE``).

    Returns what ``prometheus_client.start_http_server`` returns (the server
    and its thread); the caller owns their lifetime. Raises RuntimeError
    without the ``metrics`` extra.
    """
    prom = _load_prom()
    if prom is None:
        raise RuntimeError(
            "prometheus_client not installed; install the metrics extra: "
            "pip install 'ate-env-rl[metrics]'"
        )
    if registry is None:
        return prom.start_http_server(port, addr)
    return prom.start_http_server(port, addr, registry=registry)

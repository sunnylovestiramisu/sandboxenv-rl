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

"""Instrumentation helpers used by the fleet, handle and backends.

These keep the instrumented call sites to a few lines and cost almost nothing
when telemetry is disabled (no event is built and handles carry no telemetry).
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from . import otel
from .context import EpisodeContext, current_episode
from .events import (
    ACQUIRE_FAILED,
    ACQUIRE_READY,
    ACQUIRE_STARTED,
    ACQUIRE_TIMEOUT,
    RELEASED,
    STEP_COMPLETED,
    WARM_READY,
    WARM_TARGET,
    Recorder,
    get_recorder,
)

if TYPE_CHECKING:
    from ..types import RawSandboxInstance, Task

# Metadata keys a backend may set on RawSandboxInstance.metadata.
META_ACQUIRE_PATH = "acquire_path"      # how the backend produced it, e.g. "warm_pool" / "create"
META_TELEMETRY_ATTRS = "telemetry_attrs"  # backend join keys, e.g. {"ate.actor.name": ...}


def backend_name(backend: Any) -> str:
    """Short backend label for ``rl_sandbox.backend``."""
    name = getattr(backend, "telemetry_name", None)
    if name:
        return str(name)
    cls = type(backend).__name__.lower()
    for suffix in ("backenddriver", "driver", "backend"):
        if cls.endswith(suffix) and len(cls) > len(suffix):
            return cls[: -len(suffix)]
    return cls


def is_timeout(exc: BaseException) -> bool:
    """True if an acquire failure was a deadline rather than another error."""
    seen: set[int] = set()
    e: BaseException | None = exc
    while e is not None and id(e) not in seen:
        seen.add(id(e))
        if isinstance(e, TimeoutError):  # includes asyncio.TimeoutError on 3.11+
            return True
        if type(e).__name__ == "TimeoutError" or "timed out" in str(e).lower():
            return True
        e = e.__cause__ or e.__context__
    return False


def _round(x: float) -> float:
    return round(x, 6)


def _active(recorder: Recorder) -> bool:
    return recorder.enabled or otel.tracing_active() or otel.metrics_active()


@dataclass
class HandleTelemetry:
    """Per-sandbox telemetry state carried by a SandboxHandle."""

    recorder: Recorder
    ctx: EpisodeContext | None
    attrs: dict[str, Any]
    acquired_at: float = field(default_factory=time.monotonic)
    steps: int = 0
    released: bool = False

    def span(self, kind: str) -> AbstractContextManager[Any]:
        """OpenTelemetry span around one step (no-op when tracing is inactive)."""
        if not otel.tracing_active():
            return nullcontext()
        return otel.span(f"rl_sandbox.{kind}", {**(self.ctx.attrs() if self.ctx else {}),
                                                  **self.attrs, "step.kind": kind})

    def step(self, kind: str, t0: float, *, result: Any = None,
             exc: BaseException | None = None, **extra: Any) -> None:
        """Record one exec / file operation that started at monotonic ``t0``."""
        first = self.steps == 0
        self.steps += 1
        attrs: dict[str, Any] = {
            "step.kind": kind,
            "step.index": self.steps,
            # Backends that resume lazily (Substrate) pay the resume on the first
            # guest call, so the first step's latency includes the restore.
            "step.first": first,
            "exec_s": _round(time.monotonic() - t0),
        }
        if result is not None:
            attrs["exit_code"] = getattr(result, "exit_code", None)
            timed_out = getattr(result, "timed_out", None)
            if timed_out:
                attrs["timed_out"] = True
        if exc is not None:
            attrs["error.type"] = type(exc).__name__
        attrs.update(extra)
        self.recorder.emit(STEP_COMPLETED, ctx=self.ctx, **self.attrs, **attrs)
        otel.record_stage("step", attrs["exec_s"], {
            **self.attrs, "step.kind": kind,
            "outcome": "error" if exc is not None else ("timeout" if attrs.get("timed_out") else "ok"),
        })

    def release(self, *, recycled: bool, t0: float,
                exc: BaseException | None = None) -> None:
        if self.released:
            return
        self.released = True
        otel.record_stage("release", time.monotonic() - t0,
                          {**self.attrs, "outcome": "error" if exc else "ok"})
        self.recorder.emit(
            RELEASED, ctx=self.ctx, **self.attrs,
            hold_s=_round(t0 - self.acquired_at),
            release_s=_round(time.monotonic() - t0),
            recycled=recycled,
            steps=self.steps,
            **{"error.type": type(exc).__name__ if exc else None},
        )


class AcquireTracker:
    """Emits acquire.started on creation and ready/timeout/failed on completion."""

    def __init__(self, *, backend: Any, task: Task, template_id: str,
                 fleet_run_id: str, timeout_s: float | None) -> None:
        self.recorder = get_recorder()
        self.enabled = _active(self.recorder)
        self._span: Any = None
        if not self.enabled:
            return
        self.ctx = current_episode()
        self.t0 = time.monotonic()
        self.base: dict[str, Any] = {
            "rl_sandbox.backend": backend_name(backend),
            "rl_sandbox.fleet_run_id": fleet_run_id,
            "rl_sandbox.task.id": task.id,
            "rl_sandbox.template.id": template_id,
            "image": task.image,
        }
        self.recorder.emit(ACQUIRE_STARTED, ctx=self.ctx, timeout_s=timeout_s, **self.base)

    @contextmanager
    def scope(self) -> Iterator[None]:
        """Make the acquire span current around the backend call.

        Backend RPCs made inside (including on the SDK's background loop) become
        children of this span and carry its trace context to the server.
        """
        if not self.enabled or not otel.tracing_active():
            yield
            return
        attrs = {**(self.ctx.attrs() if self.ctx else {}), **self.base}
        with otel.span("rl_sandbox.acquire", attrs) as s:
            self._span = s
            yield

    def annotate(self, raw: RawSandboxInstance) -> None:
        """Add the backend's result to the acquire span (call inside ``scope()``)."""
        if self._span is None:
            return
        meta = raw.metadata or {}
        otel.set_span_attributes(self._span, {
            "rl_sandbox.sandbox.id": raw.instance_id,
            "acquire_path": meta.get(META_ACQUIRE_PATH),
            **(meta.get(META_TELEMETRY_ATTRS) or {}),
        })

    def ready(self, raw: RawSandboxInstance) -> HandleTelemetry | None:
        if not self.enabled:
            return None
        now = time.monotonic()
        meta = raw.metadata or {}
        attrs = dict(self.base)
        attrs["rl_sandbox.sandbox.id"] = raw.instance_id
        attrs.update(meta.get(META_TELEMETRY_ATTRS) or {})
        wait_s = _round(now - self.t0)
        path = meta.get(META_ACQUIRE_PATH)
        self.recorder.emit(ACQUIRE_READY, ctx=self.ctx, **attrs, wait_s=wait_s, acquire_path=path)
        otel.record_stage("acquire", wait_s, {**attrs, "acquire_path": path, "outcome": "ok"})
        return HandleTelemetry(recorder=self.recorder, ctx=self.ctx, attrs=attrs, acquired_at=now)

    def failed(self, exc: BaseException, raw: RawSandboxInstance | None = None) -> None:
        if not self.enabled:
            return
        attrs = dict(self.base)
        if raw is not None:
            attrs["rl_sandbox.sandbox.id"] = raw.instance_id
            attrs.update((raw.metadata or {}).get(META_TELEMETRY_ATTRS) or {})
        timeout = is_timeout(exc)
        event = ACQUIRE_TIMEOUT if timeout else ACQUIRE_FAILED
        wait_s = _round(time.monotonic() - self.t0)
        self.recorder.emit(event, ctx=self.ctx, **attrs, wait_s=wait_s,
                           **{"error.type": type(exc).__name__})
        otel.record_stage("acquire", wait_s,
                          {**attrs, "outcome": "timeout" if timeout else "error"})


@contextmanager
def track_warm(*, backend: Any, template_id: str, image: str | None,
               replicas: int, wait: bool) -> Iterator[None]:
    """Emit warm.target before warming a pool and warm.ready after it."""
    rec = get_recorder()
    if not _active(rec):
        yield
        return
    attrs = {
        "rl_sandbox.backend": backend_name(backend),
        "rl_sandbox.template.id": template_id,
        "image": image,
        "requested": replicas,
        "wait": wait,
    }
    rec.emit(WARM_TARGET, **attrs)
    t0 = time.monotonic()
    error: str | None = None
    try:
        with otel.span("rl_sandbox.warm", attrs):
            yield
    except BaseException as e:
        error = type(e).__name__
        raise
    finally:
        duration = _round(time.monotonic() - t0)
        rec.emit(WARM_READY, **attrs, duration_s=duration, **{"error.type": error})
        otel.record_stage("warm", duration, {**attrs, "outcome": "error" if error else "ok"})

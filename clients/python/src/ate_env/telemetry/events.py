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

"""The ``rl_sandbox.*`` event stream: event names, sinks and the recorder.

Events are flat JSON objects, one per line when written to a file::

    {"ts": 1792518692.123, "event": "rl_sandbox.acquire.ready",
     "rl.run_id": "r1", "rl.batch_id": 3, "rl.trajectory_id": "deepswe_30_g9",
     "rl_sandbox.backend": "substrate", "rl_sandbox.sandbox.id": "sb-1a2b",
     "ate.atespace": "ate-env", "ate.actor.name": "sb-1a2b", "wait_s": 0.142}

``ts`` is wall-clock time (seconds since the epoch). Durations (``wait_s``,
``hold_s``, ``exec_s``, ``duration_s``) are measured in-process with a monotonic
clock, so they never mix clocks from different hosts.

The stream is meant to rebuild per-sandbox state, so it must never be sampled.
No command text or file contents are ever recorded.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from typing import Any, Protocol

from .context import EpisodeContext, current_episode

logger = logging.getLogger("ate_env.telemetry")

__all__ = [
    "ACQUIRE_STARTED",
    "ACQUIRE_READY",
    "ACQUIRE_RETRY",
    "ACQUIRE_TIMEOUT",
    "ACQUIRE_FAILED",
    "STEP_COMPLETED",
    "RELEASED",
    "WARM_TARGET",
    "WARM_READY",
    "PHASE",
    "EVENT_NAMES",
    "EventSink",
    "NullSink",
    "MemorySink",
    "JsonlSink",
    "Recorder",
    "configure",
    "get_recorder",
    "shutdown",
]

ACQUIRE_STARTED = "rl_sandbox.acquire.started"
ACQUIRE_READY = "rl_sandbox.acquire.ready"
ACQUIRE_RETRY = "rl_sandbox.acquire.retry"
ACQUIRE_TIMEOUT = "rl_sandbox.acquire.timeout"
ACQUIRE_FAILED = "rl_sandbox.acquire.failed"
STEP_COMPLETED = "rl_sandbox.step.completed"
RELEASED = "rl_sandbox.released"
WARM_TARGET = "rl_sandbox.warm.target"
WARM_READY = "rl_sandbox.warm.ready"
# Harness-side pipeline phases (engine, trainer step, weight sync...) recorded
# by a harness or load driver so the run timeline has its non-sandbox lanes.
PHASE = "rl_sandbox.phase"

EVENT_NAMES = frozenset({
    ACQUIRE_STARTED, ACQUIRE_READY, ACQUIRE_RETRY, ACQUIRE_TIMEOUT, ACQUIRE_FAILED,
    STEP_COMPLETED, RELEASED, WARM_TARGET, WARM_READY, PHASE,
})


# --------------------------------------------------------------------------- sinks


class EventSink(Protocol):
    """Destination for event records. Implementations must be thread-safe."""

    def write(self, record: dict[str, Any]) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class NullSink:
    """Discards every record (the default when telemetry is not configured)."""

    def write(self, record: dict[str, Any]) -> None:
        pass

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class MemorySink:
    """Keeps records in a list; for tests and in-process analysis."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records: list[dict[str, Any]] = []

    def write(self, record: dict[str, Any]) -> None:
        with self._lock:
            self.records.append(dict(record))

    def events(self, name: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            return [r for r in self.records if name is None or r["event"] == name]

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class JsonlSink:
    """Appends records as JSON lines to a per-process file.

    ``target`` is a directory or a ``gs://bucket/prefix`` URI. The file is named
    ``events-<worker>-<pid>.jsonl`` so many processes can share one target.
    For ``gs://`` targets, records are written to a local temporary file and
    uploaded on ``close()`` (google-cloud-storage if installed, otherwise the
    ``gcloud storage`` CLI).
    """

    def __init__(self, target: str, *, worker_id: str | None = None,
                 flush_every: int = 64) -> None:
        self.target = target
        self._remote = target.startswith("gs://")
        name = f"events-{_safe(worker_id or socket.gethostname())}-{os.getpid()}.jsonl"
        if self._remote:
            self._local_dir = tempfile.mkdtemp(prefix="rl-obs-")
        else:
            self._local_dir = target
            os.makedirs(target, exist_ok=True)
        self.path = os.path.join(self._local_dir, name)
        self._name = name
        self._fh = open(self.path, "a", encoding="utf-8")  # noqa: SIM115 (closed in close())
        self._lock = threading.Lock()
        self._pending = 0
        self._flush_every = max(1, flush_every)
        self._closed = False

    def write(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, separators=(",", ":"), default=str)
        with self._lock:
            if self._closed:
                return
            self._fh.write(line + "\n")
            self._pending += 1
            if self._pending >= self._flush_every:
                self._fh.flush()
                self._pending = 0

    def flush(self) -> None:
        with self._lock:
            if not self._closed:
                self._fh.flush()
                self._pending = 0

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._fh.close()
        if self._remote:
            dest = self.target.rstrip("/") + "/" + self._name
            try:
                _upload(self.path, dest)
                shutil.rmtree(self._local_dir, ignore_errors=True)
            except Exception:
                logger.warning("Failed to upload telemetry events to %s; kept at %s",
                               dest, self.path, exc_info=True)


def _safe(s: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in s)[:64]


def _upload(local_path: str, gs_uri: str) -> None:
    try:
        from google.cloud import storage  # type: ignore[import-not-found]
    except ImportError:
        storage = None
    if storage is not None:
        bucket, _, blob = gs_uri[len("gs://"):].partition("/")
        storage.Client().bucket(bucket).blob(blob).upload_from_filename(local_path)
        return
    subprocess.run(["gcloud", "storage", "cp", local_path, gs_uri],
                   check=True, capture_output=True, timeout=300)


# ------------------------------------------------------------------------ recorder


class Recorder:
    """Builds event records and hands them to a sink.

    A record is: ``ts`` + ``event`` + static attributes (run, worker, backend) +
    the episode context (the one passed in, else the caller's current one) +
    the event's own attributes. ``None`` values are dropped.
    """

    def __init__(self, sink: EventSink | None = None, *, run_id: str | None = None,
                 worker_id: str | None = None,
                 static_attrs: dict[str, Any] | None = None) -> None:
        self.sink: EventSink = sink or NullSink()
        self._static: dict[str, Any] = {}
        if run_id:
            self._static["rl.run_id"] = run_id
        if worker_id:
            self._static["rl.worker_id"] = worker_id
        self._static.update(static_attrs or {})

    @property
    def enabled(self) -> bool:
        return not isinstance(self.sink, NullSink)

    def emit(self, event: str, *, ctx: EpisodeContext | None = None,
             **attrs: Any) -> None:
        if not self.enabled:
            return
        record: dict[str, Any] = {"ts": time.time(), "event": event}
        record.update(self._static)
        episode_ctx = ctx if ctx is not None else current_episode()
        if episode_ctx is not None:
            record.update(episode_ctx.attrs())
        for k, v in attrs.items():
            if v is not None:
                record[k] = v
        ids = _trace_ids()
        if ids is not None:
            record.setdefault("trace_id", ids[0])
            record.setdefault("span_id", ids[1])
        try:
            self.sink.write(record)
        except Exception:
            logger.debug("Dropping telemetry event %s", event, exc_info=True)

    def flush(self) -> None:
        self.sink.flush()

    def close(self) -> None:
        self.sink.close()


def _trace_ids() -> tuple[str, str] | None:
    from . import otel  # local import keeps the optional dependency optional

    return otel.current_trace_ids()


_recorder: Recorder | None = None
_recorder_lock = threading.Lock()
_atexit_registered = False


def configure(sink: EventSink | str | None = None, *, run_id: str | None = None,
              worker_id: str | None = None,
              static_attrs: dict[str, Any] | None = None) -> Recorder:
    """Install the process-wide recorder and return it.

    ``sink`` is an ``EventSink`` or a spec string:

    * ``"jsonl:<dir or gs://bucket/prefix>"`` – JSON lines per process
    * ``"memory"`` – in-memory (tests)
    * ``"none"`` / ``None`` – disabled

    Replaces (and closes) any previously configured recorder.
    """
    global _recorder, _atexit_registered
    resolved = _resolve_sink(sink, worker_id=worker_id)
    rec = Recorder(resolved, run_id=run_id, worker_id=worker_id, static_attrs=static_attrs)
    with _recorder_lock:
        old, _recorder = _recorder, rec
        if not _atexit_registered:
            atexit.register(shutdown)
            _atexit_registered = True
    if old is not None:
        old.close()
    return rec


def get_recorder() -> Recorder:
    """Return the process-wide recorder, configuring it from the environment on first use.

    Environment variables: ``RL_OBS_SINK`` (sink spec, see ``configure``),
    ``RL_OBS_RUN_ID``, ``RL_OBS_WORKER_ID``. Unset ``RL_OBS_SINK`` disables telemetry.
    """
    rec = _recorder
    if rec is not None:
        return rec
    return configure(
        os.environ.get("RL_OBS_SINK") or None,
        run_id=os.environ.get("RL_OBS_RUN_ID") or None,
        worker_id=os.environ.get("RL_OBS_WORKER_ID") or None,
    )


def shutdown() -> None:
    """Flush and close the process-wide recorder (registered with ``atexit``)."""
    global _recorder
    with _recorder_lock:
        rec, _recorder = _recorder, None
    if rec is not None:
        rec.close()


def _resolve_sink(sink: EventSink | str | None, *, worker_id: str | None) -> EventSink:
    if sink is None:
        return NullSink()
    if not isinstance(sink, str):
        return sink
    spec = sink.strip()
    if spec in ("", "none", "off"):
        return NullSink()
    if spec == "memory":
        return MemorySink()
    if spec.startswith("jsonl:"):
        return JsonlSink(spec[len("jsonl:"):], worker_id=worker_id)
    raise ValueError(f"Unknown telemetry sink spec {sink!r}; expected 'jsonl:<path>', "
                     "'memory' or 'none'")

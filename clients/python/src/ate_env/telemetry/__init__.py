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

"""Sandbox telemetry for RL runs: the ``rl_sandbox.*`` event stream.

Backend-neutral: nothing in this package knows about Substrate. Backends add
their own join keys through ``RawSandboxInstance`` metadata.

Typical use from a harness::

    from ate_env import telemetry

    telemetry.configure("jsonl:/tmp/rl-obs", run_id="run-42", worker_id="roll-7")
    with telemetry.episode(batch_id=b, group_id=prompt_id, trajectory_id=traj_id):
        handle = await fleet.acquire(task)
        ...
    telemetry.shutdown()

Or set ``RL_OBS_SINK=jsonl:gs://bucket/rl-obs/run-42`` (plus ``RL_OBS_RUN_ID``
and ``RL_OBS_WORKER_ID``) and the SDK configures itself on first use.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .context import EpisodeContext, current_episode, episode
from .events import (
    ACQUIRE_FAILED,
    ACQUIRE_READY,
    ACQUIRE_RETRY,
    ACQUIRE_STARTED,
    ACQUIRE_TIMEOUT,
    EVENT_NAMES,
    PHASE,
    RELEASED,
    STEP_COMPLETED,
    WARM_READY,
    WARM_TARGET,
    EventSink,
    JsonlSink,
    MemorySink,
    NullSink,
    Recorder,
    configure,
    get_recorder,
    shutdown,
)

__all__ = [
    "EpisodeContext",
    "current_episode",
    "episode",
    "phase",
    "configure",
    "get_recorder",
    "shutdown",
    "Recorder",
    "EventSink",
    "NullSink",
    "MemorySink",
    "JsonlSink",
    "EVENT_NAMES",
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
]


@contextmanager
def phase(name: str, **attrs: Any) -> Iterator[None]:
    """Record a harness pipeline phase (e.g. ``"engine"``, ``"trainer_step"``).

    Emits one ``rl_sandbox.phase`` event when the block exits, with ``phase``,
    ``start_ts`` and ``duration_s``, so a run timeline can show the training
    pipeline next to the sandbox lanes.
    """
    rec = get_recorder()
    start_ts = time.time()
    t0 = time.monotonic()
    error: str | None = None
    try:
        yield
    except BaseException as e:
        error = type(e).__name__
        raise
    finally:
        rec.emit(PHASE, phase=name, start_ts=start_ts,
                 duration_s=round(time.monotonic() - t0, 6), **{"error.type": error}, **attrs)

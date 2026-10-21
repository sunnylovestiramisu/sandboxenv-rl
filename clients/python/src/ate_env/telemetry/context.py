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

"""Episode context: the RL identifiers a harness attaches to sandbox telemetry.

The SDK does not know which batch, prompt group or trajectory an acquire belongs
to; the harness does. A harness sets them once per trajectory::

    from ate_env import telemetry

    with telemetry.episode(batch_id=3, group_id="deepswe_30", trajectory_id="deepswe_30_g9"):
        handle = await fleet.acquire(task)
        ...

Every telemetry event emitted inside the block carries these identifiers. The
context lives in a ``contextvars.ContextVar``, so it follows asyncio tasks and
(with the SDK's own propagation) the SDK's background event loop and thread pools.
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, fields, replace
from typing import Any

__all__ = ["EpisodeContext", "current_episode", "episode"]


@dataclass(frozen=True)
class EpisodeContext:
    """RL identifiers attached to every telemetry event emitted in its scope.

    All fields are optional; unset fields are omitted from events. Values are
    exported under the ``rl.*`` attribute names (``rl.run_id``, ``rl.batch_id``...).
    """

    run_id: str | None = None
    worker_id: str | None = None
    batch_id: int | str | None = None
    group_id: str | None = None
    trajectory_id: str | None = None
    attempt: int | None = None

    def merged(self, other: EpisodeContext | None) -> EpisodeContext:
        """Return a copy where every field set on ``other`` overrides this one."""
        if other is None:
            return self
        updates = {f.name: getattr(other, f.name) for f in fields(other)
                   if getattr(other, f.name) is not None}
        return replace(self, **updates)

    def attrs(self) -> dict[str, Any]:
        """Return the set fields as ``rl.*`` event attributes."""
        return {f"rl.{f.name}": getattr(self, f.name) for f in fields(self)
                if getattr(self, f.name) is not None}


_current: contextvars.ContextVar[EpisodeContext | None] = contextvars.ContextVar(
    "ate_env_episode", default=None
)


_FIELD_NAMES = frozenset(f.name for f in fields(EpisodeContext))


def current_episode() -> EpisodeContext | None:
    """Return the episode context active in the calling context, if any.

    Falls back to ``rl.*`` entries in OpenTelemetry baggage (e.g. set by a
    harness process upstream) when no context was set in this process.
    """
    ctx = _current.get()
    if ctx is not None:
        return ctx
    from . import otel  # local import: keeps this module free of optional deps

    found = {k: v for k, v in otel.baggage_fields().items() if k in _FIELD_NAMES}
    if not found:
        return None
    if "attempt" in found:
        try:
            found["attempt"] = int(found["attempt"])
        except (TypeError, ValueError):
            found.pop("attempt")
    return EpisodeContext(**found)


@contextmanager
def episode(ctx: EpisodeContext | None = None, *, trace: bool = False,
            **fields_: Any) -> Iterator[EpisodeContext]:
    """Scope an episode context; nested scopes override only the fields they set.

    Accepts an ``EpisodeContext`` and/or keyword fields (keywords win). With
    ``trace=True`` and OpenTelemetry tracing active, the scope is also an
    ``rl_sandbox.episode`` span, so the episode's acquire, steps and the
    backend RPCs form one trace.
    """
    new = EpisodeContext(**fields_) if fields_ else None
    base = current_episode() or EpisodeContext()
    effective = base.merged(ctx).merged(new)
    token = _current.set(effective)
    try:
        if trace:
            from . import otel

            with otel.span("rl_sandbox.episode", effective.attrs()):
                yield effective
        else:
            yield effective
    finally:
        _current.reset(token)

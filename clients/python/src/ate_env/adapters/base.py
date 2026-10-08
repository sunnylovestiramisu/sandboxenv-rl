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

"""Base protocol and result types for benchmark and RL task adapters."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Protocol, runtime_checkable

from ..handle import SandboxHandle
from ..types import Task

# Rollout results travel through Ray object refs for every candidate; keep the
# tail of the logs (where test summaries live) and drop the rest.
MAX_LOG_CHARS = 16_000


def truncate_logs(logs: str, limit: int = MAX_LOG_CHARS) -> str:
    """Keep the last ``limit`` characters of ``logs``, marking any truncation."""
    if len(logs) <= limit:
        return logs
    return f"...[truncated {len(logs) - limit} chars]...\n" + logs[-limit:]


@dataclass(frozen=True)
class EvalResult:
    """Standardized evaluation outcome for a candidate rollout inside a sandbox."""

    task_id: str
    applied: bool
    passed: bool
    timed_out: bool
    reward: float
    logs: str
    duration_s: float

    def to_dict(self) -> dict[str, Any]:
        """Serialize evaluation result to a plain dictionary."""
        return asdict(self)


@runtime_checkable
class BenchmarkAdapter(Protocol):
    """Protocol implemented by dataset / benchmark evaluation adapters.

    Adapters may accept extra optional keyword arguments, but must be callable
    with just a sandbox handle and the candidate (e.g. a patch); any grading
    inputs (test command, test patch, ...) come from ``handle.task.metadata``
    populated by ``to_task``.
    """

    @staticmethod
    def to_task(record: dict[str, Any]) -> Task:
        """Convert a benchmark record dictionary into a fleet ``Task``."""
        ...

    @staticmethod
    def evaluate(handle: SandboxHandle, candidate: str) -> EvalResult:
        """Synchronously evaluate ``candidate`` inside ``handle``."""
        ...

    @staticmethod
    async def evaluate_async(handle: SandboxHandle, candidate: str) -> EvalResult:
        """Asynchronously evaluate ``candidate`` inside ``handle``."""
        ...

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

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any

from .backend.base import BackendDriver
from .errors import CommandExecutionError, CommandTimeoutError
from .runtime.base import RuntimeGuestHook
from .telemetry import metrics
from .telemetry.tracing import (
    CLIENT_SPAN_PREFIX,
    executable_attr,
    mark_error,
    set_attributes,
    start_span,
)
from .types import DataPlaneEndpoint, ExecResult, Task


@dataclass
class SandboxHandle:
    """Unified handle to a claimed sandbox bound to a specific task.

    Provides high-level command execution, file operations,
    and release/recycling through the fleet.

    ``data_plane`` holds this sandbox's own connection coordinates (address
    plus routing headers) for harnesses that talk to the sandbox
    directly; it is None for the mock backend.

    ``tracer`` (set by the fleet when tracing is on) wraps exec and file
    operations in ``ate_env_client.exec`` / ``.write_file`` / ``.read_file``
    spans. Each carries ``ate.actor.name`` and ``ate_env.run_id``, so one
    run's step-level spans are selectable without walking parents. The first
    exec on a handle also carries ``ate_env.first_command=true``: it holds the
    wake and readiness wait, and ends the time-to-first-command window that
    starts with ``ate_env_client.acquire``.
    """
    sandbox_id: str
    endpoint: str
    task: Task
    run_id: str
    backend: BackendDriver = field(repr=False)
    runtime: RuntimeGuestHook = field(repr=False)
    data_plane: DataPlaneEndpoint | None = None
    default_timeout_s: float = 120.0
    tracer: Any = field(default=None, repr=False, compare=False)
    _first_command_started: bool = field(default=False, init=False, repr=False, compare=False)

    def _span_ids(self) -> dict[str, Any]:
        return {"ate.actor.name": self.sandbox_id, "ate_env.run_id": self.run_id or None}

    def _exec_span(self, command: str | list[str]) -> Any:
        if not self.tracer:
            return nullcontext()
        first = not self._first_command_started
        self._first_command_started = True
        return start_span(self.tracer, f"{CLIENT_SPAN_PREFIX}.exec", {
            **self._span_ids(),
            "ate_env.command.executable": executable_attr(command),
            "ate_env.first_command": True if first else None,
        })

    def _file_span(self, op: str, path: str, content: bytes | str | None = None) -> Any:
        if not self.tracer:
            return nullcontext()
        return start_span(self.tracer, f"{CLIENT_SPAN_PREFIX}.{op}", {
            **self._span_ids(),
            "ate_env.file.path": path,
            "ate_env.file.size": None if content is None else _size(content),
        })

    @staticmethod
    def _exec_result_attrs(span: Any, res: ExecResult, check: bool) -> None:
        set_attributes(span, {"ate_env.exit_code": res.exit_code, "ate_env.timed_out": res.timed_out})
        # check=True raises after the span closes; mark it failed here so the
        # trace agrees with the caller. Only the exit state is recorded.
        if check and res.timed_out:
            mark_error(span, "command timed out")
        elif check and res.exit_code != 0:
            mark_error(span, f"command exited with code {res.exit_code}")

    def exec(
        self,
        command: str | list[str],
        cwd: str = "",
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        check: bool = True,
    ) -> ExecResult:
        """Execute command inside the sandbox.

        If check=True, raises CommandExecutionError on non-zero exit code
        (CommandTimeoutError if the command hit its deadline).
        Infrastructure failures always raise InfrastructureError, regardless
        of ``check``.
        Returns ExecResult.
        """
        to_s = self.default_timeout_s if timeout_s is None else timeout_s
        with self._exec_span(command) as span, metrics.timed("exec"):
            res = self.runtime.exec(command, cwd=cwd, env=env, timeout_s=to_s)
            self._exec_result_attrs(span, res, check)
        if check and res.timed_out:
            raise CommandTimeoutError(
                f"Command '{command}' timed out after {to_s:g}s:\n"
                f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}",
                result=res,
            )
        if check and res.exit_code != 0:
            raise CommandExecutionError(
                f"Command '{command}' failed with exit code {res.exit_code}:\n"
                f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}",
                result=res,
            )
        return res

    async def exec_async(
        self,
        command: str | list[str],
        cwd: str = "",
        env: dict[str, str] | None = None,
        timeout_s: float | None = None,
        check: bool = True,
    ) -> ExecResult:
        """Asynchronously execute command inside the sandbox."""
        to_s = self.default_timeout_s if timeout_s is None else timeout_s
        with self._exec_span(command) as span, metrics.timed("exec"):
            res = await self.runtime.exec_async(command, cwd=cwd, env=env, timeout_s=to_s)
            self._exec_result_attrs(span, res, check)
        if check and res.timed_out:
            raise CommandTimeoutError(
                f"Command '{command}' timed out after {to_s:g}s:\n"
                f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}",
                result=res,
            )
        if check and res.exit_code != 0:
            raise CommandExecutionError(
                f"Command '{command}' failed with exit code {res.exit_code}:\n"
                f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}",
                result=res,
            )
        return res

    def write_file(self, path: str, content: bytes | str) -> None:
        """Write file content directly into sandbox filesystem."""
        with self._file_span("write_file", path, content):
            self.runtime.write_file(path, content)

    async def write_file_async(self, path: str, content: bytes | str) -> None:
        """Asynchronously write file content directly into sandbox filesystem."""
        with self._file_span("write_file", path, content):
            await self.runtime.write_file_async(path, content)

    def read_file_bytes(self, path: str) -> bytes:
        """Read binary file content from sandbox filesystem."""
        with self._file_span("read_file", path) as span:
            data = self.runtime.read_file_bytes(path)
            set_attributes(span, {"ate_env.file.size": len(data)})
            return data

    async def read_file_bytes_async(self, path: str) -> bytes:
        """Asynchronously read binary file content from sandbox filesystem."""
        with self._file_span("read_file", path) as span:
            data = await self.runtime.read_file_bytes_async(path)
            set_attributes(span, {"ate_env.file.size": len(data)})
            return data

    def initialize(
        self,
        init_script: str | None = None,
        cwd: str = "",
        timeout_s: float = 120.0,
    ) -> ExecResult:
        """Run post-boot initialization script inside the guest container."""
        return self.runtime.initialize(init_script=init_script, cwd=cwd, timeout_s=timeout_s)

    def release(self) -> None:
        """Release this sandbox back to the backend or terminate it."""
        try:
            self.backend.release(self.sandbox_id, recycle=False)
        finally:
            self.runtime.close()

    async def release_async(self) -> None:
        """Asynchronously release this sandbox back to the backend or terminate it."""
        try:
            if hasattr(self.backend, "release_async"):
                await self.backend.release_async(self.sandbox_id, recycle=False)
            else:
                self.backend.release(self.sandbox_id, recycle=False)
        finally:
            if hasattr(self.runtime, "close_async"):
                await self.runtime.close_async()
            else:
                self.runtime.close()

    def recycle(self) -> None:
        """Return sandbox to warm pool.

        Note: This suspends the actor and places it back in the pool without
        resetting filesystem or process state. The next claim will receive
        the actor in the state left by the previous workload.
        """
        try:
            self.backend.release(self.sandbox_id, recycle=True)
        finally:
            self.runtime.close()

    async def recycle_async(self) -> None:
        """Asynchronously return sandbox to warm pool.

        Note: This suspends the actor and places it back in the pool without
        resetting filesystem or process state. The next claim will receive
        the actor in the state left by the previous workload.
        """
        try:
            if hasattr(self.backend, "release_async"):
                await self.backend.release_async(self.sandbox_id, recycle=True)
            else:
                self.backend.release(self.sandbox_id, recycle=True)
        finally:
            if hasattr(self.runtime, "close_async"):
                await self.runtime.close_async()
            else:
                self.runtime.close()

    def __enter__(self) -> SandboxHandle:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def __aenter__(self) -> SandboxHandle:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.release_async()


def _size(content: bytes | str) -> int:
    return len(content.encode("utf-8")) if isinstance(content, str) else len(content)

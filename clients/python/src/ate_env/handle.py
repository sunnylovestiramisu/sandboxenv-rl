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

import time
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import Any

from .backend.base import BackendDriver
from .errors import CommandExecutionError, CommandTimeoutError
from .runtime.base import RuntimeGuestHook
from .telemetry.instrument import HandleTelemetry
from .types import DataPlaneEndpoint, ExecResult, Task


def _nbytes(content: bytes | str) -> int:
    return len(content) if isinstance(content, (bytes, bytearray)) else len(content.encode())


@dataclass
class SandboxHandle:
    """Unified handle to a claimed sandbox bound to a specific task.

    Provides high-level command execution, file operations,
    and release/recycling through the fleet.

    ``data_plane`` holds this sandbox's own connection coordinates (address
    plus routing headers) for harnesses that talk to the sandbox
    directly; it is None for the mock backend.

    ``telemetry`` is set by the fleet when sandbox telemetry is enabled
    (see ``ate_env.telemetry``); it records each step and the release.
    """
    sandbox_id: str
    endpoint: str
    task: Task
    run_id: str
    backend: BackendDriver = field(repr=False)
    runtime: RuntimeGuestHook = field(repr=False)
    data_plane: DataPlaneEndpoint | None = None
    default_timeout_s: float = 120.0
    telemetry: HandleTelemetry | None = field(default=None, repr=False, compare=False)

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
        t0 = time.monotonic()
        try:
            with self._span("exec"):
                res = self.runtime.exec(command, cwd=cwd, env=env, timeout_s=to_s)
        except BaseException as e:
            self._record_step("exec", t0, exc=e)
            raise
        self._record_step("exec", t0, result=res)
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
        t0 = time.monotonic()
        try:
            with self._span("exec"):
                res = await self.runtime.exec_async(command, cwd=cwd, env=env, timeout_s=to_s)
        except BaseException as e:
            self._record_step("exec", t0, exc=e)
            raise
        self._record_step("exec", t0, result=res)
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
        t0 = time.monotonic()
        try:
            with self._span("write_file"):
                self.runtime.write_file(path, content)
        except BaseException as e:
            self._record_step("write_file", t0, exc=e)
            raise
        self._record_step("write_file", t0, bytes=_nbytes(content))

    async def write_file_async(self, path: str, content: bytes | str) -> None:
        """Asynchronously write file content directly into sandbox filesystem."""
        t0 = time.monotonic()
        try:
            with self._span("write_file"):
                await self.runtime.write_file_async(path, content)
        except BaseException as e:
            self._record_step("write_file", t0, exc=e)
            raise
        self._record_step("write_file", t0, bytes=_nbytes(content))

    def read_file_bytes(self, path: str) -> bytes:
        """Read binary file content from sandbox filesystem."""
        t0 = time.monotonic()
        try:
            with self._span("read_file"):
                data = self.runtime.read_file_bytes(path)
        except BaseException as e:
            self._record_step("read_file", t0, exc=e)
            raise
        self._record_step("read_file", t0, bytes=len(data))
        return data

    async def read_file_bytes_async(self, path: str) -> bytes:
        """Asynchronously read binary file content from sandbox filesystem."""
        t0 = time.monotonic()
        try:
            with self._span("read_file"):
                data = await self.runtime.read_file_bytes_async(path)
        except BaseException as e:
            self._record_step("read_file", t0, exc=e)
            raise
        self._record_step("read_file", t0, bytes=len(data))
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
        t0, err = time.monotonic(), None
        try:
            self.backend.release(self.sandbox_id, recycle=False)
        except BaseException as e:
            err = e
            raise
        finally:
            self.runtime.close()
            self._record_release(recycled=False, t0=t0, exc=err)

    async def release_async(self) -> None:
        """Asynchronously release this sandbox back to the backend or terminate it."""
        t0, err = time.monotonic(), None
        try:
            if hasattr(self.backend, "release_async"):
                await self.backend.release_async(self.sandbox_id, recycle=False)
            else:
                self.backend.release(self.sandbox_id, recycle=False)
        except BaseException as e:
            err = e
            raise
        finally:
            if hasattr(self.runtime, "close_async"):
                await self.runtime.close_async()
            else:
                self.runtime.close()
            self._record_release(recycled=False, t0=t0, exc=err)

    def recycle(self) -> None:
        """Return sandbox to warm pool.

        Note: This suspends the actor and places it back in the pool without
        resetting filesystem or process state. The next claim will receive
        the actor in the state left by the previous workload.
        """
        t0, err = time.monotonic(), None
        try:
            self.backend.release(self.sandbox_id, recycle=True)
        except BaseException as e:
            err = e
            raise
        finally:
            self.runtime.close()
            self._record_release(recycled=True, t0=t0, exc=err)

    async def recycle_async(self) -> None:
        """Asynchronously return sandbox to warm pool.

        Note: This suspends the actor and places it back in the pool without
        resetting filesystem or process state. The next claim will receive
        the actor in the state left by the previous workload.
        """
        t0, err = time.monotonic(), None
        try:
            if hasattr(self.backend, "release_async"):
                await self.backend.release_async(self.sandbox_id, recycle=True)
            else:
                self.backend.release(self.sandbox_id, recycle=True)
        except BaseException as e:
            err = e
            raise
        finally:
            if hasattr(self.runtime, "close_async"):
                await self.runtime.close_async()
            else:
                self.runtime.close()
            self._record_release(recycled=True, t0=t0, exc=err)

    def _span(self, kind: str) -> AbstractContextManager[Any]:
        return self.telemetry.span(kind) if self.telemetry is not None else nullcontext()

    def _record_step(self, kind: str, t0: float, **kw) -> None:
        if self.telemetry is not None:
            self.telemetry.step(kind, t0, **kw)

    def _record_release(self, *, recycled: bool, t0: float,
                        exc: BaseException | None) -> None:
        if self.telemetry is not None:
            self.telemetry.release(recycled=recycled, t0=t0, exc=exc)

    def __enter__(self) -> SandboxHandle:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def __aenter__(self) -> SandboxHandle:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.release_async()

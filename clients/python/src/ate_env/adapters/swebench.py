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

"""SWE-bench task converter and in-sandbox patch evaluation adapter.

Evaluation mirrors the official SWE-bench harness:

1. Reject a candidate patch that touches a test-configuration file
   (``PROTECTED_FILES``: ``conftest.py`` anywhere, ``pytest.ini``,
   ``tox.ini``, ``setup.cfg``, ``pyproject.toml``); it scores 0.0.
2. Apply the candidate (model) patch to the repository at ``base_commit``.
3. Restore every file touched by the instance ``test_patch`` to ``base_commit``
   (discarding any candidate edits to them; a file that does not exist at
   ``base_commit`` is removed) and apply ``test_patch``.
4. Run the test command, which should select exactly the instance's
   ``FAIL_TO_PASS`` + ``PASS_TO_PASS`` tests (see ``pytest_test_cmd``).
   ``reward`` is 1.0 iff it exits 0 within its deadline.
5. Always reset the index and the working tree (``git reset --hard``, then
   ``git clean``), so a recycled sandbox starts clean.

Steps 1 and 3 keep a policy from editing the graded tests or the test
configuration. They do not make the grade tamper-proof: the candidate still
controls the code under test, and anything else that test collection imports
(for example a module that patches pytest when it is imported). The official
SWE-bench harness has the same limit. Treat the reward as a training signal,
and audit high-reward candidates before trusting them.
"""

from __future__ import annotations

import json
import logging
import re
import shlex
import time
from collections.abc import Generator, Iterable
from dataclasses import dataclass
from typing import Any

from ..errors import InfrastructureError
from ..handle import SandboxHandle
from ..types import ExecResult, Task
from .base import EvalResult, truncate_logs

logger = logging.getLogger("sandbox_sdk.adapters.swebench")

_DIFF_PATH_RE = re.compile(r"^diff --git a/(\S+) b/(\S+)$", re.MULTILINE)


class EvalSetupError(InfrastructureError):
    """The grading harness itself could not be prepared inside the sandbox.

    Example: the instance ``test_patch`` does not apply after test files were
    restored to ``base_commit``. This indicates a dataset/image mismatch, not an
    agent failure, so the rollout must be masked rather than scored.
    """

    default_retryable = False


@dataclass(frozen=True)
class _Write:
    path: str
    content: str


@dataclass(frozen=True)
class _Exec:
    command: str
    timeout_s: float | None = None


_Step = _Write | _Exec


def diff_paths(diff: str) -> list[str]:
    """Return the repository paths touched by a unified ``git diff``, in order."""
    seen: dict[str, None] = {}
    for a_path, b_path in _DIFF_PATH_RE.findall(diff or ""):
        seen.setdefault(a_path, None)
        seen.setdefault(b_path, None)
    return list(seen)


def _as_list(value: Any) -> list[str]:
    # HF SWE-bench stores FAIL_TO_PASS / PASS_TO_PASS as JSON-encoded strings.
    if value is None:
        return []
    if isinstance(value, str):
        return list(json.loads(value)) if value.strip() else []
    return list(value)


class SweBenchAdapter:
    """SWE-bench task converter and evaluator adapter."""

    PATCH_PATH = "/tmp/ate_env_solution.patch"
    TEST_PATCH_PATH = "/tmp/ate_env_test.patch"

    # Same fallback chain as the SWE-bench harness. ``patch --fuzz`` may apply
    # hunks partially before failing; the unconditional reset cleans that up.
    APPLY_CMD = (
        f"git apply --verbose {PATCH_PATH} || "
        f"patch --batch --fuzz=5 -p1 -i {PATCH_PATH}"
    )
    APPLY_TEST_PATCH_CMD = f"git apply --verbose {TEST_PATCH_PATH}"
    # ``git reset --hard`` resets the index too (``git checkout -- .`` leaves
    # staged files behind), and ``git clean`` runs even when the reset fails;
    # the exit status reports either failure.
    RESET_CMD = (
        f"rm -f {PATCH_PATH} {TEST_PATCH_PATH}; rc=0; "
        "git reset --hard -q HEAD || rc=1; git clean -fdq || rc=1; exit $rc"
    )

    #: Test-configuration files a candidate patch must not touch: pytest reads
    #: them (hooks in ``conftest.py``; ``addopts`` and plugins in the ini
    #: files), so an edit can change how the graded tests run or report. A
    #: name without "/" matches that file name in any directory. Pass
    #: ``protected_files=()`` to ``evaluate`` to allow them (for example for a
    #: dataset whose reference patches edit ``setup.cfg``).
    PROTECTED_FILES: tuple[str, ...] = (
        "conftest.py", "pytest.ini", "tox.ini", "setup.cfg", "pyproject.toml",
    )

    # --- Task conversion -------------------------------------------------

    @staticmethod
    def image_for(instance_id: str, image_prefix: str = "docker.io/swebench") -> str:
        """Official SWE-bench eval image name for an instance (x86_64)."""
        name = instance_id.replace("__", "_1776_").lower()
        return f"{image_prefix.rstrip('/')}/sweb.eval.x86_64.{name}:latest"

    @staticmethod
    def to_task(
        record: dict[str, Any],
        *,
        image_prefix: str | None = None,
        template: str | None = None,
    ) -> Task:
        """Convert a SWE-bench dataset record into an ``ate_env.Task``.

        Accepts raw Hugging Face rows (``instance_id``, ``base_commit``,
        ``test_patch``, ``FAIL_TO_PASS``, ...). The image is taken from
        ``record["image"]`` if present, otherwise derived from
        ``image_prefix`` via :meth:`image_for`.
        """
        instance_id = record.get("instance_id") or record.get("task_id")
        if not instance_id:
            raise ValueError("SWE-bench record has no 'instance_id' (or 'task_id')")
        image = record.get("image")
        if not image:
            if image_prefix is None:
                raise ValueError(
                    f"SWE-bench record {instance_id!r} has no 'image'; pass image_prefix="
                )
            image = SweBenchAdapter.image_for(instance_id, image_prefix)
        return Task(
            id=instance_id,
            image=image,
            template=template or record.get("template"),
            metadata=dict(record),
        )

    @staticmethod
    def pytest_test_cmd(record: dict[str, Any], pytest_bin: str = "pytest") -> str:
        """Build a pytest command selecting the instance's F2P + P2P tests.

        Only valid for repos whose SWE-bench test ids are pytest node ids
        (e.g. pytest, sympy, astropy). Django/sphinx use their own runners and
        need an explicit ``test_cmd``.
        """
        tests = _as_list(record.get("FAIL_TO_PASS")) + _as_list(record.get("PASS_TO_PASS"))
        if not tests:
            raise ValueError("record has no FAIL_TO_PASS / PASS_TO_PASS tests")
        # Upstream SWE-bench splits ``PASSED <nodeid>`` on spaces, leaving
        # parametrized IDs with spaces truncated with an unclosed '[' (e.g.
        # ``t.py::test_x['fixt,`` in pytest-5221); select the base test node.
        normalized = (
            t.split("[", 1)[0] if "[" in t and not t.endswith("]") else t for t in tests
        )
        quoted = " ".join(shlex.quote(t) for t in dict.fromkeys(normalized))
        return f"{pytest_bin} -rA -p no:cacheprovider {quoted}"

    # --- Evaluation ------------------------------------------------------

    @staticmethod
    def protected_paths(patch_content: str, protected_files: Iterable[str]) -> list[str]:
        """The paths in ``patch_content`` that match ``protected_files``."""
        names = tuple(protected_files)
        hits = []
        for path in diff_paths(patch_content):
            if path == "/dev/null":
                continue
            base = path.rsplit("/", 1)[-1]
            if any(path == n if "/" in n else base == n for n in names):
                hits.append(path)
        return hits

    @staticmethod
    def _restore_cmd(base_ref: str, paths: list[str]) -> str:
        """Restore ``paths`` to ``base_ref``; remove those that do not exist there.

        Fails (non-zero) when ``base_ref`` is not a commit in the repository or
        a checkout fails, so a bad ref is a setup error instead of silently
        deleting tracked test files.
        """
        ref = shlex.quote(base_ref)
        missing = shlex.quote(f"base ref {base_ref} is not a commit in this repository")
        lines = [
            f"git rev-parse --verify --quiet {shlex.quote(base_ref + '^{commit}')} >/dev/null"
            f" || {{ echo {missing} >&2; exit 1; }}"
        ]
        for p in paths:
            q = shlex.quote(p)
            lines.append(
                f"if git cat-file -e {shlex.quote(base_ref + ':' + p)} 2>/dev/null; "
                f"then git checkout {ref} -- {q} || exit 1; else rm -f -- {q}; fi"
            )
        return "\n".join(lines)

    @staticmethod
    def _plan(
        handle: SandboxHandle,
        patch_content: str,
        test_cmd: str | None,
        timeout_s: float | None,
        protected_files: Iterable[str] | None = None,
    ) -> Generator[_Step, ExecResult | None, EvalResult]:
        """Evaluation steps as a generator, driven by the sync/async runners.

        Each yielded ``_Write``/``_Exec`` is performed by the driver, which sends
        back the ``ExecResult`` (or ``None`` for writes).
        """
        task = handle.task
        meta = task.metadata or {}
        test_cmd = test_cmd or meta.get("test_cmd")
        if not test_cmd:
            raise ValueError(f"no test_cmd given and none in task {task.id!r} metadata")
        test_patch: str = meta.get("test_patch") or ""
        base_ref: str = meta.get("base_commit") or "HEAD"
        t0 = time.monotonic()

        def result(applied: bool, res: ExecResult, logs: str) -> EvalResult:
            passed = applied and res.ok
            return EvalResult(
                task_id=task.id,
                applied=applied,
                passed=passed,
                timed_out=res.timed_out,
                reward=1.0 if passed else 0.0,
                logs=truncate_logs(logs),
                duration_s=time.monotonic() - t0,
            )

        protected = SweBenchAdapter.protected_paths(
            patch_content,
            SweBenchAdapter.PROTECTED_FILES if protected_files is None else protected_files,
        )
        if protected:
            msg = (f"candidate patch touches test configuration ({', '.join(protected)}); "
                   "rejected without running the tests")
            return result(False, ExecResult(exit_code=1, stdout="", stderr=msg, duration_s=0.0), msg)

        yield _Write(SweBenchAdapter.PATCH_PATH, patch_content)
        apply_res = yield _Exec(SweBenchAdapter.APPLY_CMD)
        assert apply_res is not None
        if not apply_res.ok:
            return result(False, apply_res, apply_res.stdout + "\n" + apply_res.stderr)

        if test_patch:
            # Undo any candidate edits to graded test files, then add the tests.
            paths = [p for p in diff_paths(test_patch) if p != "/dev/null"]
            if paths:
                res = yield _Exec(SweBenchAdapter._restore_cmd(base_ref, paths))
                SweBenchAdapter._check_setup(handle, res, "restore test files")
            yield _Write(SweBenchAdapter.TEST_PATCH_PATH, test_patch)
            res = yield _Exec(SweBenchAdapter.APPLY_TEST_PATCH_CMD)
            SweBenchAdapter._check_setup(handle, res, "apply test_patch")

        test_res = yield _Exec(test_cmd, timeout_s)
        assert test_res is not None
        return result(True, test_res, test_res.stdout + "\n" + test_res.stderr)

    @staticmethod
    def evaluate(
        handle: SandboxHandle,
        patch_content: str,
        test_cmd: str | None = None,
        *,
        workdir: str = "/testbed",
        timeout_s: float | None = None,
        protected_files: Iterable[str] | None = None,
    ) -> EvalResult:
        """Apply ``patch_content`` inside the sandbox and grade it.

        ``test_cmd`` defaults to ``handle.task.metadata["test_cmd"]``. If the
        task metadata has a ``test_patch``, it is applied after restoring the
        files it touches (see module docstring).

        Scoring: ``reward`` is 1.0 only if the patch applied and the test
        command exited 0 within ``timeout_s``. A patch that touches a
        protected test-configuration file (``protected_files``, default
        ``PROTECTED_FILES``), a patch that does not apply, failing tests and
        test timeouts are agent outcomes and score 0.0.

        ``InfrastructureError`` (sandbox unreachable, command could not start,
        ``EvalSetupError``) is deliberately not caught: the caller must retry
        or mask the rollout, never score it.
        """
        plan = SweBenchAdapter._plan(handle, patch_content, test_cmd, timeout_s, protected_files)
        try:
            step = next(plan)
            while True:
                try:
                    if isinstance(step, _Write):
                        out: ExecResult | None = None
                        handle.write_file(step.path, step.content)
                    else:
                        out = handle.exec(
                            step.command, cwd=workdir, timeout_s=step.timeout_s, check=False
                        )
                except BaseException as exc:
                    step = plan.throw(exc)
                    continue
                step = plan.send(out)
        except StopIteration as stop:
            return stop.value
        finally:
            SweBenchAdapter._reset(handle, workdir)

    @staticmethod
    async def evaluate_async(
        handle: SandboxHandle,
        patch_content: str,
        test_cmd: str | None = None,
        *,
        workdir: str = "/testbed",
        timeout_s: float | None = None,
        protected_files: Iterable[str] | None = None,
    ) -> EvalResult:
        """Async variant of :meth:`evaluate` with identical semantics."""
        plan = SweBenchAdapter._plan(handle, patch_content, test_cmd, timeout_s, protected_files)
        try:
            step = next(plan)
            while True:
                try:
                    if isinstance(step, _Write):
                        out: ExecResult | None = None
                        await handle.write_file_async(step.path, step.content)
                    else:
                        out = await handle.exec_async(
                            step.command, cwd=workdir, timeout_s=step.timeout_s, check=False
                        )
                except BaseException as exc:
                    step = plan.throw(exc)
                    continue
                step = plan.send(out)
        except StopIteration as stop:
            return stop.value
        finally:
            await SweBenchAdapter._reset_async(handle, workdir)

    @staticmethod
    def _check_setup(handle: SandboxHandle, res: ExecResult | None, what: str) -> None:
        assert res is not None
        if not res.ok:
            raise EvalSetupError(
                f"SWE-bench harness failed to {what} for {handle.task.id}: "
                f"{res.stdout}\n{res.stderr}",
                sandbox_id=handle.sandbox_id,
            )

    # --- Reset -----------------------------------------------------------

    @staticmethod
    def _reset(handle: SandboxHandle, workdir: str) -> bool:
        # Best effort: never let a cleanup failure mask the verdict or the
        # original exception. A sandbox that cannot be reset should be released
        # (not recycled) by the caller. Returns whether the reset succeeded.
        try:
            res = handle.exec(SweBenchAdapter.RESET_CMD, cwd=workdir, check=False)
        except Exception:
            logger.warning("Failed to reset working tree in %s", handle.sandbox_id, exc_info=True)
            return False
        return SweBenchAdapter._reset_ok(handle, res)

    @staticmethod
    async def _reset_async(handle: SandboxHandle, workdir: str) -> bool:
        try:
            res = await handle.exec_async(SweBenchAdapter.RESET_CMD, cwd=workdir, check=False)
        except Exception:
            logger.warning("Failed to reset working tree in %s", handle.sandbox_id, exc_info=True)
            return False
        return SweBenchAdapter._reset_ok(handle, res)

    @staticmethod
    def _reset_ok(handle: SandboxHandle, res: ExecResult) -> bool:
        if res.ok:
            return True
        logger.warning(
            "Failed to reset working tree in %s (exit %s%s); release the sandbox instead "
            "of recycling it: %s",
            handle.sandbox_id, res.exit_code, ", timed out" if res.timed_out else "",
            (res.stderr or res.stdout).strip()[-500:],
        )
        return False

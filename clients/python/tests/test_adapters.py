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

"""Unit tests for benchmark task converters and evaluation adapters."""

from __future__ import annotations

import json
from typing import Any

import pytest

from ate_env.adapters import BenchmarkAdapter, EvalResult, SweBenchAdapter
from ate_env.adapters.base import MAX_LOG_CHARS, truncate_logs
from ate_env.adapters.swebench import EvalSetupError, diff_paths
from ate_env.backend.mock import MockBackendDriver
from ate_env.errors import InfrastructureError, SandboxUnavailableError
from ate_env.handle import SandboxHandle
from ate_env.runtime.mock import MockRuntimeHook
from ate_env.types import ExecResult, Task

OK = ExecResult(exit_code=0, stdout="", stderr="", duration_s=0.01)
FAIL = ExecResult(exit_code=1, stdout="", stderr="failed", duration_s=0.01)

TEST_PATCH = (
    "diff --git a/testing/test_foo.py b/testing/test_foo.py\n"
    "--- a/testing/test_foo.py\n"
    "+++ b/testing/test_foo.py\n"
    "@@ -1 +1,2 @@\n"
    " def test_foo(): pass\n"
    "+def test_bar(): pass\n"
    "diff --git a/testing/test_new.py b/testing/test_new.py\n"
    "new file mode 100644\n"
    "--- /dev/null\n"
    "+++ b/testing/test_new.py\n"
    "@@ -0,0 +1 @@\n"
    "+def test_new(): pass\n"
)


class ScriptedRuntime(MockRuntimeHook):
    """Deterministic fake runtime recording file writes and scripted command results."""

    def __init__(self, responses: list[ExecResult | Exception]):
        super().__init__()
        self.responses = list(responses)
        self.written_files: dict[str, bytes | str] = {}
        self.commands: list[tuple[str | list[str], str]] = []
        self.timeouts: list[float] = []

    def exec(
        self,
        command: str | list[str],
        cwd: str = "",
        env: dict[str, str] | None = None,
        timeout_s: float = 120.0,
    ) -> ExecResult:
        self.commands.append((command, cwd))
        self.timeouts.append(timeout_s)
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt

    def write_file(self, path: str, content: bytes | str) -> None:
        self.written_files[path] = content

    def read_file_bytes(self, path: str) -> bytes:
        val = self.written_files[path]
        return val if isinstance(val, bytes) else val.encode("utf-8")


def _record(**extra: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "instance_id": "pytest-dev__pytest-5221",
        "image": "us-docker.pkg.dev/example/swebench@sha256:" + "a" * 64,
        "template": "default-template",
        "base_commit": "4a2fdce62b73944030cff9b3e52862868ca9584d",
        "test_cmd": "pytest testing/test_foo.py",
    }
    record.update(extra)
    return record


def _make_handle(runtime: ScriptedRuntime, **extra: Any) -> SandboxHandle:
    return SandboxHandle(
        sandbox_id="sb-test-1",
        endpoint="mock://sb-test-1",
        task=SweBenchAdapter.to_task(_record(**extra)),
        run_id="run-1",
        backend=MockBackendDriver(),
        runtime=runtime,
    )


# --- Task conversion ---------------------------------------------------------


def test_swebench_adapter_conforms_to_protocol() -> None:
    assert isinstance(SweBenchAdapter, BenchmarkAdapter)
    task = SweBenchAdapter.to_task(
        {
            "instance_id": "django__django-11099",
            "image": "us-docker.pkg.dev/example/django@sha256:" + "b" * 64,
            "template": "default-template",
            "test_cmd": "pytest",
        }
    )
    assert isinstance(task, Task)
    assert task.id == "django__django-11099"
    assert task.template == "default-template"
    assert task.metadata["test_cmd"] == "pytest"


def test_to_task_accepts_raw_hf_record_and_derives_image() -> None:
    raw = {"instance_id": "pytest-dev__pytest-5221", "base_commit": "abc"}
    task = SweBenchAdapter.to_task(raw, image_prefix="docker.io/swebench")
    assert task.id == "pytest-dev__pytest-5221"
    assert task.image == "docker.io/swebench/sweb.eval.x86_64.pytest-dev_1776_pytest-5221:latest"
    assert task.metadata["base_commit"] == "abc"


def test_to_task_requires_instance_id_and_image() -> None:
    with pytest.raises(ValueError, match="instance_id"):
        SweBenchAdapter.to_task({"image": "img"})
    with pytest.raises(ValueError, match="image_prefix"):
        SweBenchAdapter.to_task({"instance_id": "x__y-1"})


def test_pytest_test_cmd_selects_f2p_and_p2p() -> None:
    record = {
        "FAIL_TO_PASS": json.dumps(["t.py::test_a", "t.py::test_b[x y]"]),
        "PASS_TO_PASS": ["t.py::test_c", "t.py::test_a", "t.py::test_d['fixt,", "t.py::test_d[['fixt',"],
    }
    cmd = SweBenchAdapter.pytest_test_cmd(record, pytest_bin="/env/bin/pytest")
    assert cmd == (
        "/env/bin/pytest -rA -p no:cacheprovider "
        "t.py::test_a 't.py::test_b[x y]' t.py::test_c t.py::test_d"
    )
    with pytest.raises(ValueError):
        SweBenchAdapter.pytest_test_cmd({})


def test_diff_paths() -> None:
    assert diff_paths(TEST_PATCH) == ["testing/test_foo.py", "testing/test_new.py"]
    assert diff_paths("") == []


def test_truncate_logs_keeps_tail() -> None:
    logs = "x" * MAX_LOG_CHARS + "SUMMARY"
    out = truncate_logs(logs)
    assert out.endswith("SUMMARY")
    assert out.startswith("...[truncated 7 chars]...")
    assert truncate_logs("short") == "short"


# --- Evaluation --------------------------------------------------------------


def test_swebench_evaluate_sync_pass_and_reset() -> None:
    rt = ScriptedRuntime(
        [
            ExecResult(exit_code=0, stdout="Applied cleanly", stderr="", duration_s=0.05),
            ExecResult(exit_code=0, stdout="1 passed", stderr="", duration_s=0.40),
            OK,  # reset
        ]
    )
    handle = _make_handle(rt)
    res = SweBenchAdapter.evaluate(handle, "diff --git ...", "pytest -k test_version")

    assert isinstance(res, EvalResult)
    assert res.applied is True
    assert res.passed is True
    assert res.reward == 1.0
    assert res.to_dict()["task_id"] == "pytest-dev__pytest-5221"
    assert rt.written_files[SweBenchAdapter.PATCH_PATH] == "diff --git ..."
    assert rt.commands[1] == ("pytest -k test_version", "/testbed")
    assert rt.commands[-1] == (SweBenchAdapter.RESET_CMD, "/testbed")
    assert len(rt.commands) == 3


def test_test_cmd_defaults_to_task_metadata_and_honours_timeout() -> None:
    rt = ScriptedRuntime([OK, OK, OK])
    res = SweBenchAdapter.evaluate(_make_handle(rt), "diff", timeout_s=7.0)
    assert res.passed
    assert rt.commands[1][0] == "pytest testing/test_foo.py"
    assert rt.timeouts[1] == 7.0


def test_swebench_evaluate_sync_patch_reject_skips_tests_but_resets() -> None:
    rt = ScriptedRuntime(
        [
            ExecResult(exit_code=1, stdout="", stderr="patch failed", duration_s=0.03),
            OK,  # reset: `patch --fuzz` may have left partial hunks behind
        ]
    )
    res = SweBenchAdapter.evaluate(_make_handle(rt), "bad patch", "pytest")

    assert res.applied is False
    assert res.passed is False
    assert res.reward == 0.0
    assert "patch failed" in res.logs
    assert [c for c, _ in rt.commands] == [SweBenchAdapter.APPLY_CMD, SweBenchAdapter.RESET_CMD]


def test_test_patch_is_applied_after_restoring_graded_test_files() -> None:
    rt = ScriptedRuntime([OK, OK, OK, OK, OK])
    handle = _make_handle(rt, test_patch=TEST_PATCH)
    res = SweBenchAdapter.evaluate(handle, "model patch that edits tests")

    assert res.passed
    cmds = [c for c, _ in rt.commands]
    assert cmds[0] == SweBenchAdapter.APPLY_CMD
    restore = cmds[1]
    assert "git checkout 4a2fdce62b73944030cff9b3e52862868ca9584d -- testing/test_foo.py" in restore
    assert "rm -f -- testing/test_new.py" in restore
    assert cmds[2] == SweBenchAdapter.APPLY_TEST_PATCH_CMD
    assert cmds[3] == "pytest testing/test_foo.py"
    assert cmds[4] == SweBenchAdapter.RESET_CMD
    assert rt.written_files[SweBenchAdapter.TEST_PATCH_PATH] == TEST_PATCH


def test_test_patch_failure_is_infrastructure_not_score() -> None:
    rt = ScriptedRuntime([OK, OK, FAIL, OK])  # apply, restore, test_patch fails, reset
    handle = _make_handle(rt, test_patch=TEST_PATCH)
    with pytest.raises(EvalSetupError) as ei:
        SweBenchAdapter.evaluate(handle, "diff")
    assert isinstance(ei.value, InfrastructureError)
    assert ei.value.retryable is False
    assert rt.commands[-1][0] == SweBenchAdapter.RESET_CMD


def test_reset_failure_does_not_mask_verdict() -> None:
    rt = ScriptedRuntime([OK, OK, SandboxUnavailableError("gone")])
    res = SweBenchAdapter.evaluate(_make_handle(rt), "diff")
    assert res.passed is True


@pytest.mark.asyncio
async def test_swebench_evaluate_async_fail_and_timeout_score_zero() -> None:
    rt = ScriptedRuntime(
        [
            ExecResult(exit_code=0, stdout="Applied", stderr="", duration_s=0.02),
            ExecResult(
                exit_code=None, stdout="", stderr="timed out", duration_s=30.0, timed_out=True
            ),
            OK,
        ]
    )
    res = await SweBenchAdapter.evaluate_async(_make_handle(rt), "diff ...", "pytest")

    assert res.applied is True
    assert res.passed is False
    assert res.timed_out is True
    assert res.reward == 0.0
    assert rt.commands[-1] == (SweBenchAdapter.RESET_CMD, "/testbed")


@pytest.mark.asyncio
async def test_async_test_patch_flow_matches_sync() -> None:
    rt = ScriptedRuntime([OK, OK, OK, FAIL, OK])
    handle = _make_handle(rt, test_patch=TEST_PATCH)
    res = await SweBenchAdapter.evaluate_async(handle, "diff")
    assert res.applied is True
    assert res.passed is False
    assert [c for c, _ in rt.commands][2] == SweBenchAdapter.APPLY_TEST_PATCH_CMD


@pytest.mark.asyncio
async def test_swebench_evaluate_async_propagates_infrastructure_error() -> None:
    rt = ScriptedRuntime(
        [
            ExecResult(exit_code=0, stdout="Applied", stderr="", duration_s=0.02),
            SandboxUnavailableError("router 503"),
            OK,
        ]
    )
    with pytest.raises(SandboxUnavailableError):
        await SweBenchAdapter.evaluate_async(_make_handle(rt), "diff ...", "pytest")
    # The working tree is still reset after the infrastructure error.
    assert rt.commands[-1] == (SweBenchAdapter.RESET_CMD, "/testbed")

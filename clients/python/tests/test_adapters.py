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
import logging
import subprocess
import sys
import time
from pathlib import Path
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


# --- Real git repository (PR #83 review: B1, B2, B3, B4) ----------------------


class SubprocessRuntime(MockRuntimeHook):
    """Runs commands with bash in a real directory; the adapter's /tmp patch files go to files_dir."""

    def __init__(self, files_dir: Path):
        super().__init__()
        self.files_dir = files_dir

    def _local(self, text: str) -> str:
        return text.replace("/tmp/ate_env_", f"{self.files_dir}/ate_env_")

    def exec(
        self,
        command: str | list[str],
        cwd: str = "",
        env: dict[str, str] | None = None,
        timeout_s: float = 120.0,
    ) -> ExecResult:
        assert isinstance(command, str)
        start = time.monotonic()
        try:
            proc = subprocess.run(["bash", "-c", self._local(command)], cwd=cwd or None,
                                  capture_output=True, text=True, timeout=timeout_s)
        except subprocess.TimeoutExpired:
            return ExecResult(exit_code=-1, stdout="", stderr="timeout",
                              duration_s=time.monotonic() - start, timed_out=True)
        return ExecResult(exit_code=proc.returncode, stdout=proc.stdout, stderr=proc.stderr,
                          duration_s=time.monotonic() - start)

    def write_file(self, path: str, content: bytes | str) -> None:
        target = Path(self._local(path))
        target.write_bytes(content if isinstance(content, bytes) else content.encode())


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=repo, check=True, capture_output=True, text=True,
    ).stdout


def _diff_of(repo: Path, edits: dict[str, str]) -> str:
    """The unified diff that applies ``edits`` (path -> new content) to HEAD."""
    for path, content in edits.items():
        (repo / path).parent.mkdir(parents=True, exist_ok=True)
        (repo / path).write_text(content)
    _git(repo, "add", "-A")
    diff = _git(repo, "diff", "--cached")
    _git(repo, "reset", "-q", "--hard", "HEAD")
    return diff


GRADED_TEST = "from calc import add\n\n\ndef test_add():\n    assert add(2, 3) == 5\n"


@pytest.fixture
def git_repo(tmp_path: Path) -> dict[str, Any]:
    """A repo with a bug in calc.add, and a test_patch that adds the graded test."""
    pytest.importorskip("pytest")
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "calc.py").write_text("def add(a, b):\n    return a - b\n")
    (repo / "tests").mkdir()
    (repo / "tests" / "test_calc.py").write_text("def test_placeholder():\n    pass\n")
    _git(repo, "init", "-q")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")
    files = tmp_path / "files"
    files.mkdir()
    return {
        "repo": repo,
        "base": _git(repo, "rev-parse", "HEAD").strip(),
        "test_patch": _diff_of(repo, {"tests/test_calc.py": GRADED_TEST}),
        "fix": _diff_of(repo, {"calc.py": "def add(a, b):\n    return a + b\n"}),
        "runtime": SubprocessRuntime(files),
        "test_cmd": f"{sys.executable} -m pytest -q -p no:cacheprovider tests",
    }


def _real_handle(g: dict[str, Any], **extra: Any) -> SandboxHandle:
    record = _record(base_commit=g["base"], test_patch=g["test_patch"], test_cmd=g["test_cmd"])
    record.update(extra)
    return SandboxHandle(sandbox_id="sb-git", endpoint="mock://sb-git",
                         task=SweBenchAdapter.to_task(record), run_id="run-1",
                         backend=MockBackendDriver(), runtime=g["runtime"])


def _assert_clean(g: dict[str, Any]) -> None:
    assert _git(g["repo"], "status", "--porcelain") == ""
    assert _git(g["repo"], "rev-parse", "HEAD").strip() == g["base"]


def test_real_repo_correct_patch_passes_and_resets(git_repo) -> None:
    res = SweBenchAdapter.evaluate(_real_handle(git_repo), git_repo["fix"],
                                   workdir=str(git_repo["repo"]))
    assert (res.applied, res.passed, res.reward) == (True, True, 1.0), res.logs
    _assert_clean(git_repo)


def test_real_repo_unfixed_code_fails(git_repo) -> None:
    noop = _diff_of(git_repo["repo"], {"README": "x\n"})
    res = SweBenchAdapter.evaluate(_real_handle(git_repo), noop, workdir=str(git_repo["repo"]))
    assert (res.applied, res.passed, res.reward) == (True, False, 0.0)
    _assert_clean(git_repo)


def test_real_repo_candidate_edits_to_graded_tests_are_undone(git_repo) -> None:
    weaken = _diff_of(git_repo["repo"], {"tests/test_calc.py": "def test_add():\n    pass\n"})
    res = SweBenchAdapter.evaluate(_real_handle(git_repo), weaken, workdir=str(git_repo["repo"]))
    assert (res.applied, res.reward) == (True, 0.0), res.logs
    _assert_clean(git_repo)


CONFTEST_HOOK = (
    "import pytest\n\n\n"
    "@pytest.hookimpl(hookwrapper=True)\n"
    "def pytest_runtest_makereport(item, call):\n"
    "    outcome = yield\n"
    "    report = outcome.get_result()\n"
    "    if report.failed:\n"
    "        report.outcome = 'passed'\n"
)


def test_real_repo_conftest_hook_is_rejected(git_repo) -> None:
    """The reviewer's exploit: a conftest.py hook that turns failures into passes."""
    exploit = _diff_of(git_repo["repo"], {"conftest.py": CONFTEST_HOOK})
    workdir = str(git_repo["repo"])
    # Without the protection the hook really earns reward on unfixed code...
    allowed = SweBenchAdapter.evaluate(_real_handle(git_repo), exploit, workdir=workdir,
                                       protected_files=())
    assert allowed.reward == 1.0, allowed.logs
    # ...and by default the patch is rejected before anything runs.
    res = SweBenchAdapter.evaluate(_real_handle(git_repo), exploit, workdir=workdir)
    assert (res.applied, res.reward) == (False, 0.0)
    assert "conftest.py" in res.logs
    _assert_clean(git_repo)


@pytest.mark.parametrize("path", ["pkg/conftest.py", "pytest.ini", "tox.ini", "setup.cfg",
                                  "pyproject.toml"])
def test_protected_paths(path: str) -> None:
    diff = f"diff --git a/{path} b/{path}\n"
    assert SweBenchAdapter.protected_paths(diff, SweBenchAdapter.PROTECTED_FILES) == [path]
    assert SweBenchAdapter.protected_paths("diff --git a/src/conftest_util.py b/src/conftest_util.py\n",
                                           SweBenchAdapter.PROTECTED_FILES) == []


def test_real_repo_invalid_base_ref_is_setup_error(git_repo) -> None:
    handle = _real_handle(git_repo, base_commit="0" * 40)
    with pytest.raises(EvalSetupError, match="restore test files") as err:
        SweBenchAdapter.evaluate(handle, git_repo["fix"], workdir=str(git_repo["repo"]))
    assert "is not a commit in this repository" in str(err.value)
    _assert_clean(git_repo)


def test_real_repo_reset_clears_the_index(git_repo) -> None:
    """A file the test command stages must not survive into the next use of the sandbox."""
    stage = f"echo x > staged.txt && git add staged.txt && {git_repo['test_cmd']}"
    res = SweBenchAdapter.evaluate(_real_handle(git_repo), git_repo["fix"], test_cmd=stage,
                                   workdir=str(git_repo["repo"]))
    assert res.passed, res.logs
    assert _git(git_repo["repo"], "diff", "--cached", "--name-only") == ""
    _assert_clean(git_repo)


async def test_real_repo_async_matches_sync(git_repo) -> None:
    res = await SweBenchAdapter.evaluate_async(_real_handle(git_repo), git_repo["fix"],
                                               workdir=str(git_repo["repo"]))
    assert (res.applied, res.passed, res.reward) == (True, True, 1.0), res.logs
    _assert_clean(git_repo)


def test_reset_failure_is_logged(caplog) -> None:
    rt = ScriptedRuntime([OK, OK, FAIL])
    handle = _make_handle(rt)
    caplog.set_level(logging.WARNING, logger="sandbox_sdk.adapters.swebench")
    res = SweBenchAdapter.evaluate(handle, "patch")
    assert res.passed
    assert any("Failed to reset working tree in sb-test-1" in r.getMessage() for r in caplog.records)

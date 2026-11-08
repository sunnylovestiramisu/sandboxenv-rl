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

"""Unit and cross-language integration tests for run_fleet_bench.py."""

from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys
import threading
from typing import Any

import pytest

_DIR = pathlib.Path(__file__).resolve().parent
_REPO_ROOT = _DIR.parents[1]
if str(_DIR) not in sys.path:
    sys.path.insert(0, str(_DIR))

import run_fleet_bench as rfb  # noqa: E402  # after the sys.path insert above

_SDK, _INFRA_ERR, _ = rfb.load_sdk()
_PKG_NAME = _SDK.__name__
_mock_backend_mod = __import__(f"{_PKG_NAME}.backend.mock", fromlist=["MockBackendDriver"])
_mock_runtime_mod = __import__(f"{_PKG_NAME}.runtime.mock", fromlist=["MockRuntimeHook"])
_types_mod = __import__(f"{_PKG_NAME}.types", fromlist=["ExecResult", "RawSandboxInstance"])
_errors_mod = __import__(f"{_PKG_NAME}.errors", fromlist=["SandboxUnavailableError"])

MockBackendDriver = _mock_backend_mod.MockBackendDriver
MockRuntimeHook = _mock_runtime_mod.MockRuntimeHook
ExecResult = _types_mod.ExecResult
SandboxUnavailableError = _errors_mod.SandboxUnavailableError


def _digest(n: int) -> str:
    return f"sha256:{n:064x}"


class _GradedMockRuntime(MockRuntimeHook):
    """Mock guest runtime that emits deterministic SWE-bench and R2E verdicts."""

    def __init__(self, *, fail_first_exec: bool = False) -> None:
        super().__init__()
        self._fail_first_exec = fail_first_exec
        self._lock = threading.Lock()
        self._calls = 0

    def exec(
        self,
        command: str | list[str],
        cwd: str = "",
        env: dict[str, str] | None = None,
        timeout_s: float = 120.0,
    ) -> Any:
        cmd_str = command if isinstance(command, str) else " ".join(command)
        with self._lock:
            self._calls += 1
            call_num = self._calls
        if self._fail_first_exec and call_num == 1:
            raise SandboxUnavailableError("transient wake unavailable")
        if "/tmp/ate-grade.py" in cmd_str or "swebench-grade.py" in cmd_str:
            verdict = json.dumps({"valid": True, "resolved": True, "f2p": "1/1", "p2p": "2/2"})
            return ExecResult(exit_code=0, stdout=f"pytest ok\n{verdict}\n", stderr="")
        if "R2E_GRADE_MARKER" in cmd_str:
            verdict = json.dumps({"valid": True, "reward": 1, "detail": "matched 1/1 tests"})
            return ExecResult(exit_code=0, stdout=f"{verdict}\n", stderr="")
        return super().exec(command, cwd=cwd, env=env, timeout_s=timeout_s)


class _GradedMockDriver(MockBackendDriver):
    """MockBackendDriver returning _GradedMockRuntime instances."""

    def __init__(self, *, fail_first_exec: bool = False) -> None:
        super().__init__()
        self._fail_first_exec = fail_first_exec

    def build_runtime(self, instance: Any, data_plane: str = "mock") -> tuple[Any, Any | None]:
        return _GradedMockRuntime(fail_first_exec=self._fail_first_exec), None


def _write_r2e_manifest(path: pathlib.Path, count: int = 6) -> pathlib.Path:
    rows = [
        json.dumps({
            "instance_id": f"r2e-inst-{i:02d}",
            "repo": "demo/repo",
            "image": f"us-docker.pkg.dev/p/r2e/img-{i:02d}@{_digest(10 + i)}",
            "kind": "command",
            "material": {
                "setup_command": "echo setup",
                "reset_command": "echo reset",
                "command": "echo R2E_GRADE_MARKER",
                "expected_exit_code": 0,
            },
        })
        for i in range(count)
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _write_swe_manifest(path: pathlib.Path, count: int = 4) -> pathlib.Path:
    rows = [
        json.dumps({
            "instance_id": f"django__django-{10000 + i}",
            "repo": "django/django",
            "image": f"us-docker.pkg.dev/p/swe/img-{i:02d}@{_digest(100 + i)}",
            "patch": "diff --git a/a.py b/a.py\n",
            "test_patch": "diff --git a/t.py b/t.py\n",
            "eval_script": "#!/bin/bash\npytest\n",
            "fail_to_pass": ["test_x"],
            "pass_to_pass": ["test_y"],
            "log_parser": "parse_log_pytest",
            "eval_type": "pass_and_fail",
        })
        for i in range(count)
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def test_unit_helpers_duration_graders_and_labels(tmp_path, monkeypatch):
    assert rfb.parse_duration_s("500ms") == pytest.approx(0.5)
    assert rfb.parse_duration_s("3m0s") == pytest.approx(180.0)
    assert rfb.parse_duration_s("1h2m3s") == pytest.approx(3723.0)
    assert rfb.parse_duration_s("2.5") == pytest.approx(2.5)
    with pytest.raises(ValueError):
        rfb.parse_duration_s("")
    with pytest.raises(ValueError):
        rfb.parse_duration_s("-5")
    with pytest.raises(ValueError):
        rfb.parse_duration_s("5days")

    assert rfb.format_go_duration(0) == "0s"
    assert rfb.format_go_duration(0.25) == "250ms"
    assert rfb.format_go_duration(180.0) == "3m0s"
    assert rfb.default_window_size(1) == 1
    assert rfb.default_window_size(6) == 3
    assert rfb.default_window_size(20) == 8

    assert rfb.dataset_label("https://storage.googleapis.com/bucket/swe-30.jsonl?sig=secret") == "swe-30.jsonl"
    assert rfb.shard_event_path("/tmp/art", "swe") == pathlib.Path("/tmp/art/events/swe.jsonl")
    assert rfb.shard_event_path("/tmp/art/events", "r2e.jsonl") == pathlib.Path("/tmp/art/events/r2e.jsonl")

    rew, detail = rfb.grade_swebench_output(
        'logs\n{"valid": true, "resolved": true, "f2p": "1/1", "p2p": "3/3"}\n', 0
    )
    assert (rew, detail) == (1, "f2p 1/1 p2p 3/3")
    assert rfb.grade_swebench_output('{"valid": false}\n', 0) == (0, "invalid run")

    assert rfb.grade_command_output('{"valid": true, "reward": 1, "detail": "ok"}\n', "", 0, 0, "") == (1, "ok")
    assert rfb.grade_command_output("wrong\n", "", 0, 0, "right") == (
        0,
        "stdout mismatch: got 'wrong', want 'right'",
    )
    assert rfb.grade_command_output("", "", 2, 0, "") == (0, "exit 2, want 0")

    monkeypatch.setenv(rfb.AUTH_TOKEN_ENV, "  env-secret  ")
    assert rfb.resolve_auth_token("") == "env-secret"
    empty_tok = tmp_path / "empty.tok"
    empty_tok.write_text("   \n", encoding="utf-8")
    with pytest.raises(ValueError, match="is empty"):
        rfb.resolve_auth_token(str(empty_tok))


def test_all_four_strategies_and_fleet_report_outputs(tmp_path):
    r2e_manifest = _write_r2e_manifest(tmp_path / "r2e-6.jsonl", count=6)

    for idx, strategy in enumerate(rfb.VALID_STRATEGIES, start=1):
        out_jsonl = tmp_path / f"events-{strategy}.jsonl"
        report_prefix = tmp_path / f"fleet-{strategy}"
        argv = [
            "--backend=mock",
            f"--dataset={r2e_manifest}",
            "--trajectory=command-default",
            f"--run-id=py-strat-{idx}",
            "--mode=steps",
            f"--strategy={strategy}",
            "--rollouts=2",
            "--steps=1",
            "--width=4",
            f"--out={out_jsonl}",
            f"--fleet-report-out={report_prefix}",
            "--label=client=python",
            f"--label=arm=python-{strategy}",
        ]
        if strategy in ("sliding", "pipelined"):
            argv.append("--window-size=2")

        rc = rfb.run_benchmark(argv, custom_driver=_GradedMockDriver())
        assert rc == 0
        assert (tmp_path / f"fleet-{strategy}.json").is_file()
        assert (tmp_path / f"fleet-{strategy}.txt").is_file()

        events = [json.loads(line) for line in out_jsonl.read_text(encoding="utf-8").splitlines()]
        run_ev = [e for e in events if e["event"] == "run"][-1]
        task_evs = [e for e in events if e["event"] == "task"]
        barrier_evs = [e for e in events if e["event"] == "step_barrier"]

        assert len(task_evs) == 12
        assert all(e["ok"] and e["reward"] == 1 for e in task_evs)
        assert len(barrier_evs) == 1
        assert run_ev["ok"] is True
        assert run_ev["count"] == 12
        assert run_ev["labels"]["strategy"] == strategy
        assert run_ev["labels"]["client"] == "python"
        assert run_ev["labels"]["arm"] == f"python-{strategy}"

        if strategy == "none":
            assert run_ev["working_set_hits"] == 0
            assert run_ev["working_set_misses"] == 12
            assert run_ev["working_set_hit_rate"] == pytest.approx(0.0)
            assert run_ev["standing_pool_size"] == 0
            assert sum(1 for e in events if e["event"] == "ttfi") == 12
            assert sum(1 for e in events if e["event"] == "create") == 12
        elif strategy == "naive":
            assert run_ev["working_set_hits"] == 12
            assert run_ev["working_set_hit_rate"] == pytest.approx(1.0)
            assert run_ev["standing_pool_size"] == 12
            assert sum(1 for e in events if e["event"] == "wake") == 12
            assert any(e["event"] == "prewarm" for e in events)
        elif strategy == "sliding":
            assert run_ev["working_set_hits"] == 12
            assert run_ev["working_set_hit_rate"] == pytest.approx(1.0)
            assert run_ev["standing_pool_size"] == 4
            assert run_ev["labels"]["window_size"] == "2"
            assert sum(1 for e in events if e["event"] == "wake") == 12
        elif strategy == "pipelined":
            assert run_ev["working_set_hits"] == 12
            assert run_ev["working_set_hit_rate"] == pytest.approx(1.0)
            assert 4 <= run_ev["standing_pool_size"] <= 8
            assert run_ev["labels"]["window_size"] == "2"
            assert sum(1 for e in events if e["event"] == "wake") == 12


def test_swebench_gold_smoke_wake_retry_and_validation(tmp_path):
    swe_manifest = _write_swe_manifest(tmp_path / "swe-4.jsonl", count=2)

    # 1. Standalone swebench-gold + transient first-step SandboxUnavailableError retry.
    gold_events_dir = tmp_path / "gold-run"
    rc = rfb.run_benchmark(
        [
            "--backend=mock",
            f"--dataset={swe_manifest}",
            "--trajectory=swebench-gold",
            "--run-id=py-gold",
            "--mode=eval",
            "--strategy=naive",
            "--tasks-per-image=1",
            "--width=2",
            "--wake-budget=5s",
            "--wake-backoff=5ms",
            f"--events-dir={gold_events_dir}",
            "--worker=swe",
        ],
        custom_driver=_GradedMockDriver(fail_first_exec=True),
    )
    assert rc == 0
    swe_jsonl = gold_events_dir / "events" / "swe.jsonl"
    assert swe_jsonl.is_file()
    events = [json.loads(line) for line in swe_jsonl.read_text(encoding="utf-8").splitlines()]
    task_evs = [e for e in events if e["event"] == "task"]
    assert len(task_evs) == 2
    assert all(e["ok"] and e["reward"] == 1 for e in task_evs)

    # 2. Standalone swebench-smoke with --set SMOKE_CMD and first-step exec retry -> attempts == 2.
    smoke_out = tmp_path / "smoke.jsonl"
    rc_smoke = rfb.run_benchmark(
        [
            "--backend=mock",
            f"--dataset={swe_manifest}",
            "--trajectory=swebench-smoke",
            "--set=SMOKE_CMD=echo smoke-ok",
            "--run-id=py-smoke",
            "--mode=eval",
            "--strategy=naive",
            "--width=1",
            "--wake-budget=5s",
            "--wake-backoff=5ms",
            f"--out={smoke_out}",
        ],
        custom_driver=_GradedMockDriver(fail_first_exec=True),
    )
    assert rc_smoke == 0
    smoke_events = [json.loads(line) for line in smoke_out.read_text(encoding="utf-8").splitlines()]
    wake_evs = [e for e in smoke_events if e["event"] == "wake"]
    assert len(wake_evs) == 2
    assert all(e["attempts"] == 2 and e["ok"] is True for e in wake_evs)

    # 3. Fail-loud CLI validation checks.
    with pytest.raises(ValueError, match="--window-size applies to"):
        rfb.run_benchmark(["--dataset", str(swe_manifest), "--strategy=naive", "--window-size=2", "--dry-run"])
    with pytest.raises(ValueError, match="single-pass"):
        rfb.run_benchmark(["--dataset", str(swe_manifest), "--mode=steps", "--steps=2", "--dry-run"])
    with pytest.raises(ValueError, match="loader records that key itself"):
        rfb.run_benchmark(["--dataset", str(swe_manifest), "--label=width=10", "--dry-run"])
    with pytest.raises(ValueError, match="does not read it"):
        rfb.run_benchmark(["--dataset", str(swe_manifest), "--trajectory=swebench-gold", "--set=UNUSED=1", "--dry-run"])

    # 4. Timed-out ExecResult (exit_code=None, timed_out=True) returns clean TimeoutError without TypeError.
    class _TimeoutHandle:
        def exec(self, command, timeout_s=10.0, check=False):
            return ExecResult(exit_code=None, stdout="partial", stderr="", timed_out=True)

    code_t, out_t, _err_t, att_t, exc_t = rfb._execute_step_with_retry(
        handle=_TimeoutHandle(),
        step_spec={"name": "slow", "run": "sleep 99", "timeout_s": 0.1},
        default_step_timeout_s=1.0,
        first=True,
        wake_budget_s=1.0,
        wake_backoff_s=0.01,
        infra_error_cls=_INFRA_ERR,
    )
    assert code_t == -1
    assert out_t == "partial"
    assert att_t == 1
    assert isinstance(exc_t, TimeoutError)

    # 5. Post-acquire runtime construction failure still emits a failed task event.
    class _BrokenRuntimeDriver(_GradedMockDriver):
        def build_runtime(self, instance: Any, data_plane: str = "mock") -> tuple[Any, Any | None]:
            raise RuntimeError("runtime attach boom")

    broken_out = tmp_path / "broken-rt.jsonl"
    rc_broken = rfb.run_benchmark(
        [
            "--backend=mock",
            f"--dataset={swe_manifest}",
            "--trajectory=swebench-smoke",
            "--run-id=py-broken",
            "--mode=eval",
            "--strategy=naive",
            "--width=2",
            f"--out={broken_out}",
        ],
        custom_driver=_BrokenRuntimeDriver(),
    )
    assert rc_broken == 1
    broken_evs = [json.loads(line) for line in broken_out.read_text(encoding="utf-8").splitlines()]
    broken_tasks = [e for e in broken_evs if e["event"] == "task"]
    assert len(broken_tasks) == 2
    assert all(e["ok"] is False and "runtime attach boom" in e.get("err", "") for e in broken_tasks)
    broken_run = next(e for e in broken_evs if e["event"] == "run")
    assert broken_run["labels"]["set.SMOKE_CMD"] == "git status --short && python3 --version || python --version"


@pytest.mark.skipif(shutil.which("go") is None, reason="go toolchain required for cross-language report check")
def test_cross_language_render_plan_and_go_report_compatibility(tmp_path):
    r2e_manifest = _write_r2e_manifest(tmp_path / "r2e-6.jsonl", count=6)
    plan_json = tmp_path / "r2e-plan.json"

    subprocess.run(
        [
            "go",
            "run",
            "./cmd/benchmarking/swebench-loader",
            "render-plan",
            f"-dataset={r2e_manifest}",
            "-images=6",
            "-trajectory=command-default",
            "-fanout=2",
            f"-out={plan_json}",
        ],
        cwd=_REPO_ROOT,
        check=True,
    )
    assert plan_json.is_file()

    arm_reports: list[str] = []
    for idx, strategy in enumerate(rfb.VALID_STRATEGIES, start=1):
        out_jsonl = tmp_path / f"py-{strategy}.jsonl"
        argv = [
            "--backend=mock",
            f"--rendered-plan={plan_json}",
            f"--run-id=py-a{idx}",
            "--mode=steps",
            f"--strategy={strategy}",
            "--rollouts=2",
            "--steps=1",
            "--width=4",
            f"--out={out_jsonl}",
            "--label=client=python",
            f"--label=arm=python-{strategy}",
        ]
        if strategy in ("sliding", "pipelined"):
            argv.append("--window-size=2")
        assert rfb.run_benchmark(argv, custom_driver=_GradedMockDriver()) == 0

        summary_json = tmp_path / f"py-{strategy}-summary.json"
        subprocess.run(
            [
                "go",
                "run",
                "./cmd/benchmarking/swebench-loader",
                "report",
                str(out_jsonl),
                "-json",
                str(summary_json),
            ],
            cwd=_REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        summary = json.loads(summary_json.read_text(encoding="utf-8"))
        assert summary["tasks"] == 12
        assert summary["rewarded"] == 12
        assert summary["reward_rate"] == pytest.approx(1.0)
        if strategy == "none":
            assert summary["labels"]["park"] == "delete"
            assert summary.get("paused_sandboxes", 0) == 0
        else:
            assert summary["labels"]["park"] == "suspend"
            assert summary.get("paused_sandboxes", 0) > 0
        summary["run"] = f"python-{strategy}"
        summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        arm_reports.append(str(summary_json))

    comp_json = tmp_path / "comparison.json"
    comp_proc = subprocess.run(
        [
            "go",
            "run",
            "./cmd/benchmarking/swebench-loader",
            "report",
            *arm_reports,
            "-json",
            str(comp_json),
        ],
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    for col in ("python-none", "python-naive", "python-sliding", "python-pipelined"):
        assert col in comp_proc.stdout
    comp_data = json.loads(comp_json.read_text(encoding="utf-8"))
    assert len(comp_data) == 4


class _RealWarmPoolStubDriver(_GradedMockDriver):
    """Stub driver mimicking SubstrateBackendDriver._warm_paused_pool[key] -> list[str]."""

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._warm_paused_pool: dict[tuple[str, str | None], list[str]] = {}
        self._warm_counter = 0

    def warm_pool(
        self,
        template_id: str,
        replicas: int,
        wait: bool = True,
        run_id: str | None = None,
        rootfs_image: str | None = None,
    ) -> None:
        super().warm_pool(template_id, replicas, wait=wait, run_id=run_id, rootfs_image=rootfs_image)
        key = (template_id, rootfs_image)
        with self._lock:
            cur = self._warm_paused_pool.setdefault(key, [])
            while len(cur) < replicas:
                self._warm_counter += 1
                cur.append(f"warm-{self._warm_counter:010x}")

    def acquire(
        self,
        template_id: str,
        run_id: str,
        timeout_s: float = 180.0,
        rootfs_image: str | None = None,
    ) -> Any:
        raw = super().acquire(template_id, run_id, timeout_s=timeout_s, rootfs_image=rootfs_image)
        if raw.metadata.get("claim_source") == "warm_pool":
            key = (template_id, rootfs_image)
            with self._lock:
                cur = self._warm_paused_pool.get(key, [])
                if cur:
                    raw.instance_id = cur.pop(0)
        return raw

    def unwarm_pool(self, template_id: str, rootfs_image: str | None = None) -> None:
        super().unwarm_pool(template_id, rootfs_image=rootfs_image)
        with self._lock:
            self._warm_paused_pool.pop((template_id, rootfs_image), None)

    def reap(self, run_id: str) -> int:
        with self._lock:
            self._warm_paused_pool.clear()
        return super().reap(run_id)


def test_real_warm_paused_pool_string_actor_ids(tmp_path: pathlib.Path) -> None:
    """Ensure _warm_paused_pool string actor IDs are never unpacked into single characters."""
    manifest = _write_r2e_manifest(tmp_path / "r2e.jsonl", count=2)
    out_jsonl = tmp_path / "events.jsonl"
    argv = [
        "--backend=mock",
        f"--dataset={manifest}",
        "--trajectory=command-default",
        "--mode=steps",
        "--strategy=naive",
        "--rollouts=1",
        "--steps=1",
        "--width=2",
        "--run-id=realwarm-t1",
        f"--out={out_jsonl}",
    ]
    assert rfb.run_benchmark(argv, custom_driver=_RealWarmPoolStubDriver()) == 0
    events = [json.loads(ln) for ln in out_jsonl.read_text(encoding="utf-8").splitlines() if ln.strip()]
    creates = [e for e in events if e["event"] == "create"]
    suspends = [e for e in events if e["event"] == "suspend"]
    prewarms = [e for e in events if e["event"] == "prewarm"]
    deletes = [e for e in events if e["event"] == "delete"]
    assert len(creates) == 2
    assert len(suspends) == 2
    assert len(prewarms) == 2
    assert len(deletes) == 2
    assert all(e["env"].startswith("warm-") and len(e["env"]) > 5 for e in creates)
    assert all(e["env"].startswith("warm-") and len(e["env"]) > 5 for e in deletes)


def test_standalone_plan_stages_large_payloads_to_files(tmp_path: pathlib.Path) -> None:
    """Ensure standalone swebench-gold and oversize command-default never pass >64 KiB on argv."""
    swe_manifest = tmp_path / "swe-large.jsonl"
    swe_row = {
        "instance_id": "numpy__numpy-99999",
        "repo": "numpy/numpy",
        "image": f"us-docker.pkg.dev/p/swe/img@{_digest(901)}",
        "patch": "diff --git a/a.py b/a.py\n" + ("+x = 1\n" * 5000),
        "test_patch": "diff --git a/t.py b/t.py\n",
        "eval_script": "#!/bin/bash\npytest\n",
        "fail_to_pass": ["test_target"],
        "pass_to_pass": [f"numpy/tests/test_big.py::test_case_{i:04d}" for i in range(4000)],
        "log_parser": "parse_log_pytest",
        "eval_type": "pass_and_fail",
    }
    swe_manifest.write_text(json.dumps(swe_row) + "\n", encoding="utf-8")

    swe_plan = rfb._load_standalone_plan(
        dataset_path=str(swe_manifest),
        images=0,
        instances_csv="",
        exclude_file="",
        trajectory_spec="swebench-gold",
        set_vars={},
    )
    swe_steps = swe_plan["instances"][0]["steps"]
    step_by_name = {s["name"]: s for s in swe_steps}
    assert "write-instance" in step_by_name
    assert step_by_name["write-instance"]["write"]["path"] == "/tmp/ate-instance.json"
    assert len(step_by_name["write-instance"]["write"]["content"]) > 200_000
    assert "run-tests" in step_by_name
    assert "/tmp/ate-eval.log" in step_by_name["run-tests"]["run"]
    assert "/tmp/ate-instance.json" in step_by_name["grade"]["run"]
    assert "/tmp/ate-eval.log" in step_by_name["grade"]["run"]
    assert all(len(s.get("run", "")) < 256 for s in swe_steps)

    cmd_manifest = tmp_path / "r2e-large.jsonl"
    big_cmd = "# padding\n" * 8000 + "echo R2E_GRADE_MARKER"
    assert len(big_cmd.encode("utf-8")) > rfb._MAX_INLINE_CMD_BYTES
    cmd_rows = [
        {
            "instance_id": "r2e-small",
            "repo": "demo/r2e",
            "image": f"us-docker.pkg.dev/p/r2e/s@{_digest(902)}",
            "kind": "command",
            "command": "echo R2E_GRADE_MARKER",
            "expected_exit_code": 0,
        },
        {
            "instance_id": "r2e-large",
            "repo": "demo/r2e",
            "image": f"us-docker.pkg.dev/p/r2e/l@{_digest(903)}",
            "kind": "command",
            "command": big_cmd,
            "expected_exit_code": 0,
        },
    ]
    cmd_manifest.write_text("\n".join(json.dumps(r) for r in cmd_rows) + "\n", encoding="utf-8")

    cmd_plan = rfb._load_standalone_plan(
        dataset_path=str(cmd_manifest),
        images=0,
        instances_csv="",
        exclude_file="",
        trajectory_spec="",
        set_vars={},
    )
    small_steps = cmd_plan["instances"][0]["steps"]
    assert len(small_steps) == 1
    assert small_steps[0]["run"] == "echo R2E_GRADE_MARKER"

    large_steps = cmd_plan["instances"][1]["steps"]
    assert [s["name"] for s in large_steps] == ["write-grade", "grade"]
    assert large_steps[0]["write"]["path"] == "/tmp/ate-cmd-grade.sh"
    assert large_steps[0]["write"]["content"] == big_cmd
    assert large_steps[1]["run"] == "/bin/bash /tmp/ate-cmd-grade.sh"
    assert large_steps[1]["reward"] is True


def test_task_whose_callback_raises_counts_once(tmp_path: pathlib.Path) -> None:
    """A rollout whose plan step is malformed raises inside the fleet callback after its
    acquire; it is one failed task and one working-set claim, not a claim counted twice."""
    plan_json = tmp_path / "boom-plan.json"
    plan_json.write_text(
        json.dumps({
            "instances": [
                {
                    "instance_id": f"r2e-boom-{i}",
                    "repo": "demo/repo",
                    "image": f"us-docker.pkg.dev/p/r2e/boom-{i}@{_digest(300 + i)}",
                    "kind": "command",
                    "has_reward": True,
                    "setup": [],
                    "steps": [
                        {
                            "name": "grade",
                            "run": "echo R2E_GRADE_MARKER",
                            "think_ms": "not-a-number",
                            "reward": True,
                        }
                    ],
                    "expected_exit_code": 0,
                }
                for i in range(2)
            ]
        }),
        encoding="utf-8",
    )
    out_jsonl = tmp_path / "events.jsonl"
    argv = [
        "--backend=mock",
        f"--rendered-plan={plan_json}",
        "--run-id=py-boom",
        "--mode=steps",
        "--strategy=naive",
        "--rollouts=1",
        "--steps=1",
        "--width=2",
        f"--out={out_jsonl}",
    ]
    assert rfb.run_benchmark(argv, custom_driver=_GradedMockDriver()) == 1
    events = [json.loads(ln) for ln in out_jsonl.read_text(encoding="utf-8").splitlines() if ln.strip()]
    tasks = [e for e in events if e["event"] == "task"]
    assert len(tasks) == 2
    assert all(not t["ok"] and "not-a-number" in t["err"] for t in tasks)
    run_ev = next(e for e in events if e["event"] == "run")
    barrier = next(e for e in events if e["event"] == "step_barrier")
    for ev in (run_ev, barrier):
        assert ev["working_set_hits"] == 2
        assert ev["working_set_misses"] == 0
    assert run_ev["count"] == 2
    assert run_ev["reused"] == 2


def _serve_fake_ate_env_api() -> tuple[Any, Any, Any, Any, int]:
    """Start the SDK's fake ate-env-api on a background asyncio loop; returns
    (environment service, server, loop, thread, port)."""
    import asyncio
    import importlib.util

    import grpc.aio

    spec = importlib.util.spec_from_file_location(
        "ate_env_rl_test_fakes", _REPO_ROOT / "clients" / "python" / "tests" / "fakes.py"
    )
    assert spec is not None and spec.loader is not None
    fakes = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = fakes  # its dataclasses resolve string annotations through here
    spec.loader.exec_module(fakes)
    gen = __import__(f"{_PKG_NAME}._gen.ateenv.v1alpha", fromlist=["env_pb2_grpc", "guest_pb2_grpc"])

    class _Recording(fakes.FakeEnvironmentService):  # type: ignore[name-defined,misc]
        def __init__(self) -> None:
            super().__init__()
            self.created: list[str] = []
            self.deleted: list[str] = []

        async def CreateEnvironment(self, request, context):  # noqa: N802
            resp = await super().CreateEnvironment(request, context)
            self.created.append(request.id)
            return resp

        async def DeleteEnvironment(self, request, context):  # noqa: N802
            self.deleted.append(request.id)
            return await super().DeleteEnvironment(request, context)

    env_svc = _Recording()
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="fake-ate-env-api", daemon=True)
    thread.start()

    async def _start() -> tuple[Any, int]:
        server = grpc.aio.server()
        gen.env_pb2_grpc.add_EnvironmentServiceServicer_to_server(env_svc, server)
        gen.guest_pb2_grpc.add_ProcessServiceServicer_to_server(fakes.FakeProcessService(), server)
        gen.guest_pb2_grpc.add_FileSystemServiceServicer_to_server(fakes.FakeFileSystemService(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        return server, port

    server, port = asyncio.run_coroutine_threadsafe(_start(), loop).result(timeout=30)
    return env_svc, server, loop, thread, port


def test_real_substrate_driver_warm_pool_over_the_fake_api(tmp_path: pathlib.Path) -> None:
    """naive over the real SubstrateBackendDriver and the SDK's fake ate-env-api: the warm-pool
    events the instrumented driver derives from the driver's own pool name the actors the API
    created, and every one of them is deleted."""
    import asyncio

    manifest = _write_r2e_manifest(tmp_path / "r2e.jsonl", count=2)
    out_jsonl = tmp_path / "events.jsonl"
    env_svc, server, loop, thread, port = _serve_fake_ate_env_api()
    try:
        argv = [
            "--backend=substrate",
            f"--endpoint=127.0.0.1:{port}",
            "--atespace=ate-env",
            "--base-template=default-template",
            "--connections=1",
            f"--dataset={manifest}",
            "--trajectory=command-default",
            "--mode=steps",
            "--strategy=naive",
            "--rollouts=1",
            "--steps=1",
            "--width=2",
            "--create-timeout=30s",
            "--step-timeout=30s",
            "--run-id=realapi-t1",
            f"--out={out_jsonl}",
        ]
        rc = rfb.run_benchmark(argv)
    finally:
        asyncio.run_coroutine_threadsafe(server.stop(None), loop).result(timeout=30)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=10)
    assert rc == 0

    events = [json.loads(ln) for ln in out_jsonl.read_text(encoding="utf-8").splitlines() if ln.strip()]
    creates = [e for e in events if e["event"] == "create"]
    deletes = [e for e in events if e["event"] == "delete"]
    assert len(env_svc.created) == 2
    assert {e["env"] for e in creates} == set(env_svc.created)
    assert {e["env"] for e in deletes} == set(env_svc.deleted) == set(env_svc.created)
    assert all(e["env"].startswith("warm-") and len(e["env"]) > 5 for e in creates)
    run_ev = next(e for e in events if e["event"] == "run")
    assert run_ev["count"] == 2
    assert run_ev["working_set_hits"] == 2
    assert run_ev["working_set_misses"] == 0


def test_enable_tracing_emits_v1_trace_events(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENV_BENCH_MODE", raising=False)
    monkeypatch.delenv("ENV_BENCH_TRACE_PATH", raising=False)

    r2e_manifest = _write_r2e_manifest(tmp_path / "r2e-2.jsonl", count=2)
    bundle_dir = tmp_path / "bundle"
    out_jsonl = bundle_dir / "events" / "r2e.jsonl"
    trace_path = bundle_dir / "traces" / "r2e.jsonl"
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    trace_path.write_text('{"stale":true}\n', encoding="utf-8")

    rc = rfb.run_benchmark(
        [
            "--backend=mock",
            f"--dataset={r2e_manifest}",
            "--trajectory=command-default",
            "--run-id=py-trace-1",
            "--worker=r2e",
            "--job=r2e",
            "--mode=steps",
            "--strategy=none",
            "--rollouts=2",
            "--steps=1",
            "--width=4",
            "--enable-tracing",
            f"--out={out_jsonl}",
        ],
        custom_driver=_GradedMockDriver(),
    )
    assert rc == 0

    # Verify ENV_BENCH_* env vars were cleaned up in finally.
    import os

    assert "ENV_BENCH_MODE" not in os.environ
    assert "ENV_BENCH_TRACE_PATH" not in os.environ

    assert trace_path.is_file()
    trace_events = [
        json.loads(ln)
        for ln in trace_path.read_text(encoding="utf-8").splitlines()
        if ln.strip()
    ]
    assert len(trace_events) >= 4
    assert all("stale" not in ev for ev in trace_events)
    assert all(ev["schema_version"] == "v1" for ev in trace_events)
    assert all(ev["run"] == "py-trace-1" and ev["job"] == "r2e" for ev in trace_events)
    assert all(ev.get("instance") in {"r2e-inst-00", "r2e-inst-01"} for ev in trace_events)
    exec_traces = [ev for ev in trace_events if ev["op"] in {"exec", "shell"}]
    assert len(exec_traces) >= 4
    assert {ev["instance"] for ev in exec_traces} == {"r2e-inst-00", "r2e-inst-01"}
    assert all(ev["image"].startswith("us-docker.pkg.dev/p/r2e/") for ev in exec_traces)

    events = [json.loads(ln) for ln in out_jsonl.read_text(encoding="utf-8").splitlines() if ln.strip()]
    reap_evs = [e for e in events if e["event"] == "reap"]
    assert len(reap_evs) == 1, reap_evs
    reap_ev = reap_evs[0]
    assert reap_ev["ok"] is True
    assert reap_ev["metrics"]["orphan_count"] == 0.0


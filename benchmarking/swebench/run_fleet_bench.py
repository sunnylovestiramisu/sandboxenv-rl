#!/usr/bin/env python3
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
"""Drive R2E-Gym and SWE-bench workloads via the Python SandboxFleet SDK.

Executes any of the four SandboxFleet orchestration strategies
(``none``, ``naive``, ``sliding``, ``pipelined``) through ``ate-env-api`` (or
the hermetic ``mock`` backend) and emits canonical ``swebench.Event`` JSONL
records so ``swebench-loader report`` and ``bench report`` summarize and
compare Go and Python arms identically.
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import dataclasses
import datetime
import hashlib
import json
import os
import pathlib
import re
import signal
import sys
import threading
import time
import types
import urllib.parse
from typing import Any, Self

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_PY_SDK_SRC = _REPO_ROOT / "clients" / "python" / "src"
if _PY_SDK_SRC.is_dir() and str(_PY_SDK_SRC) not in sys.path:
    sys.path.insert(0, str(_PY_SDK_SRC))


def load_sdk() -> tuple[types.ModuleType, type[BaseException], Any]:
    """Import the repository's Python SDK (ate_env_rl) on demand."""
    import ate_env_rl as sdk_mod
    from ate_env_rl.errors import InfrastructureError as infra_err
    from ate_env_rl.telemetry.trace_recorder import (
        current_slot_wait_s as slot_wait_var,
    )

    return sdk_mod, infra_err, slot_wait_var


def _load_trace_recorder() -> Any:
    """Import ate_env_rl.telemetry.trace_recorder on demand."""
    from ate_env_rl.telemetry import trace_recorder

    return trace_recorder


AUTH_TOKEN_ENV = "SUBSTRATE_ENV_AUTH_TOKEN"
VALID_STRATEGIES = ("none", "naive", "sliding", "pipelined")
VALID_MODES = ("eval", "steps")
VALID_BACKENDS = ("substrate", "mock")

LOADER_LABEL_KEYS = frozenset({
    "mode",
    "strategy",
    "window_size",
    "width",
    "images",
    "trajectory",
    "tasks_per_image",
    "rollouts",
    "steps",
    "park",
    "create_timeout",
    "step_timeout",
    "wake_budget",
    "wake_backoff",
    "dataset",
    "connections",
    "delete_rate",
    "base_template",
    "exclude_file",
    "instances",
    "seed",
    "images_per_step",
    "park_each",
})

_DIGEST_RE = re.compile(r"@sha256:[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DURATION_TOKEN_RE = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")
# Keep inline sh -c arguments well under Linux MAX_ARG_STRLEN (131072 bytes).
_MAX_INLINE_CMD_BYTES = 65536


def parse_duration_s(raw: str) -> float:
    """Parse a Go-style duration string (e.g. '5m', '3m0s', '500ms') or seconds float."""
    s = raw.strip()
    if not s:
        raise ValueError("empty duration")
    try:
        val = float(s)
        if val < 0:
            raise ValueError(f"negative duration: {raw!r}")
        return val
    except ValueError:
        pass

    pos = 0
    total_s = 0.0
    units = {
        "ns": 1e-9,
        "us": 1e-6,
        "µs": 1e-6,
        "ms": 1e-3,
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
    }
    for m in _DURATION_TOKEN_RE.finditer(s):
        if m.start() != pos:
            raise ValueError(f"invalid duration {raw!r}")
        pos = m.end()
        total_s += float(m.group(1)) * units[m.group(2)]
    if pos != len(s):
        raise ValueError(f"invalid duration {raw!r}")
    return total_s


def format_go_duration(seconds: float) -> str:
    """Format seconds using Go time.Duration.String() conventions."""
    if seconds <= 0:
        return "0s"
    ns = round(seconds * 1_000_000_000)
    if ns < 1_000:
        return f"{ns}ns"
    if ns < 1_000_000:
        us = ns / 1_000
        return f"{int(us)}µs" if us.is_integer() else f"{us:g}µs"
    if ns < 1_000_000_000:
        ms_val = ns / 1_000_000
        return f"{int(ms_val)}ms" if ms_val.is_integer() else f"{ms_val:g}ms"

    rem_s = ns / 1_000_000_000
    hours = int(rem_s // 3600)
    rem_s -= hours * 3600
    minutes = int(rem_s // 60)
    rem_s -= minutes * 60
    sec_str = f"{int(rem_s)}s" if float(rem_s).is_integer() else f"{rem_s:g}s"
    out = ""
    if hours:
        out += f"{hours}h"
    if minutes or hours:
        out += f"{minutes}m"
    out += sec_str
    return out


def default_window_size(num_images: int) -> int:
    """Match Go swebench.DefaultWindowSize."""
    if num_images <= 1:
        return 1
    return min(8, max(1, (num_images + 1) // 2))


def dataset_label(dataset: str) -> str:
    parsed = urllib.parse.urlparse(dataset)
    if parsed.scheme and parsed.netloc:
        return pathlib.PurePosixPath(parsed.path).name
    return pathlib.Path(dataset).name


def ids_digest(ids: list[str]) -> str:
    joined = "\n".join(sorted(ids)).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()[:8]


def read_id_list(path: str) -> list[str]:
    out: list[str] = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            out.append(line.split()[0])
    return out


def resolve_auth_token(token_file: str) -> str | None:
    if not token_file:
        env_tok = os.environ.get(AUTH_TOKEN_ENV, "").strip()
        return env_tok or None
    content = pathlib.Path(token_file).read_text(encoding="utf-8").strip()
    if not content:
        raise ValueError(f"--auth-token-file {token_file} is empty")
    return content


def parse_kv_flags(items: list[str], flag_name: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in items:
        if "=" not in raw:
            raise ValueError(f"{flag_name}: want KEY=VALUE, got {raw!r}")
        if "\t" in raw or "\n" in raw:
            raise ValueError(f"{flag_name}: {raw!r} must not contain tabs or newlines")
        k, v = raw.split("=", 1)
        if not k:
            raise ValueError(f"{flag_name}: empty key in {raw!r}")
        if k in out:
            raise ValueError(f"{flag_name}: {k} is given twice")
        out[k] = v
    return out


def shard_event_path(base_dir: str, worker: str) -> pathlib.Path:
    clean_worker = (worker or "").strip() or "worker-0"
    clean_worker = clean_worker.removesuffix(".jsonl")
    base = pathlib.Path(base_dir)
    if base.name == "events":
        return base / f"{clean_worker}.jsonl"
    return base / "events" / f"{clean_worker}.jsonl"


def grade_swebench_output(output: str, exit_code: int) -> tuple[int, str]:
    """Port of Go swebench.SWEBenchGrader.Parse with exit-code fallback."""
    idx = output.rfind('{"valid"')
    if idx >= 0:
        tail_json = output[idx:].strip()
        nl = tail_json.find("\n")
        if nl >= 0:
            tail_json = tail_json[:nl].strip()
        try:
            verdict = json.loads(tail_json)
        except json.JSONDecodeError:
            verdict = None
        if isinstance(verdict, dict):
            if not verdict.get("valid", False):
                return 0, "invalid run"
            resolved = verdict.get("resolved")
            reward = 1 if resolved is True else (0 if resolved is False else (1 if exit_code == 0 else 0))
            f2p = verdict.get("f2p", "")
            p2p = verdict.get("p2p", "")
            return reward, f"f2p {f2p} p2p {p2p}"
    return (1 if exit_code == 0 else 0), ""


def grade_command_output(
    stdout: str,
    stderr: str,
    exit_code: int,
    expected_exit_code: int,
    expected_stdout: str,
) -> tuple[int, str]:
    """Port of Go swebench.CommandGrader.GradeResult."""
    combined = stdout + stderr
    for marker in ('{"valid"', '{"reward"'):
        idx = combined.rfind(marker)
        if idx < 0:
            continue
        line = combined[idx:]
        nl = line.find("\n")
        if nl >= 0:
            line = line[:nl]
        try:
            verdict = json.loads(line.strip())
        except json.JSONDecodeError:
            continue
        if not isinstance(verdict, dict):
            continue
        valid = verdict.get("valid")
        rew = verdict.get("reward")
        resolved = verdict.get("resolved")
        if valid is None and rew is None and resolved is None:
            continue
        detail = str(verdict.get("detail") or "")
        if valid is False:
            return 0, detail or "invalid run"
        if isinstance(rew, (int, float)):
            r = 1 if rew > 0 else 0
        elif isinstance(resolved, bool):
            r = 1 if resolved else 0
        elif valid is True:
            r = 1
        else:
            r = 0
        return r, detail or f"reward {r}"

    if exit_code != expected_exit_code:
        return 0, f"exit {exit_code}, want {expected_exit_code}"
    if expected_stdout:
        actual = stdout.strip()
        want = expected_stdout.strip()
        if actual != want:
            return 0, f"stdout mismatch: got {actual!r}, want {want!r}"
        return 1, "stdout matched"
    actual = stdout.strip()
    return 1, actual if actual else f"exit {exit_code}"


def _load_standalone_plan(
    dataset_path: str,
    images: int,
    instances_csv: str,
    exclude_file: str,
    trajectory_spec: str,
    set_vars: dict[str, str],
) -> dict[str, Any]:
    """Build a RenderedPlan directly in Python when --rendered-plan is not passed."""
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    with open(dataset_path, encoding="utf-8") as f:
        for line_no, raw in enumerate(f, 1):
            s = raw.strip()
            if not s:
                continue
            row = json.loads(s)
            iid = str(row.get("instance_id") or "")
            img = str(row.get("image") or "")
            if not iid:
                raise ValueError(f"dataset line {line_no}: instance_id is required")
            if not img or not _DIGEST_RE.search(img):
                raise ValueError(f"dataset line {line_no}: {iid}: image {img!r} is not digest-pinned")
            if iid in seen_ids:
                raise ValueError(f"dataset line {line_no}: duplicate instance {iid}")
            seen_ids.add(iid)
            rows.append(row)
    if not rows:
        raise ValueError("dataset is empty")

    exclude = read_id_list(exclude_file) if exclude_file else []
    drop = set(exclude)
    filtered = [r for r in rows if r["instance_id"] not in drop]

    ids = [x.strip() for x in instances_csv.split(",") if x.strip()] if instances_csv else []
    if ids:
        for req_id in ids:
            if req_id in drop:
                raise ValueError(f"instance {req_id} is on the exclude list")
        by_id = {r["instance_id"]: r for r in filtered}
        missing = [req_id for req_id in ids if req_id not in by_id]
        if missing:
            raise ValueError(f"instances not in dataset: {', '.join(missing)}")
        selected = [by_id[req_id] for req_id in ids if req_id in by_id]
    else:
        selected = filtered[:images] if images > 0 else filtered

    if not selected:
        raise ValueError("no instances selected")

    first_kind = str(selected[0].get("kind") or "swebench")
    traj_name = trajectory_spec or "swebench-gold"
    if traj_name == "swebench-gold" and first_kind == "command":
        traj_name = "command-default"

    grader_script = ""
    if traj_name == "swebench-gold":
        grader_path = (
            _REPO_ROOT
            / "internal"
            / "benchmarking"
            / "swebench"
            / "trajectories"
            / "swebench-grade.py"
        )
        grader_script = grader_path.read_text(encoding="utf-8")

    rendered_instances: list[dict[str, Any]] = []
    used_vars: dict[str, str] = {}

    for row in selected:
        iid = str(row["instance_id"])
        img = str(row["image"])
        kind = str(row.get("kind") or "swebench")
        material = row.get("material") if isinstance(row.get("material"), dict) else row

        if traj_name == "command-default":
            cmd = str(material.get("command") or row.get("command") or "").strip()
            if not cmd:
                raise ValueError(f"{iid}: command is required")
            setup_cmd = str(material.get("setup_command") or row.get("setup_command") or "true")
            reset_cmd = str(material.get("reset_command") or row.get("reset_command") or "true")
            if len(cmd.encode("utf-8")) >= _MAX_INLINE_CMD_BYTES:
                cmd_steps: list[dict[str, Any]] = [
                    {
                        "name": "write-grade",
                        "write": {"path": "/tmp/ate-cmd-grade.sh", "content": cmd},
                    },
                    {
                        "name": "grade",
                        "run": "/bin/bash /tmp/ate-cmd-grade.sh",
                        "reward": True,
                    },
                ]
            else:
                cmd_steps = [{"name": "grade", "run": cmd, "reward": True}]
            rendered_instances.append({
                "instance_id": iid,
                "repo": str(row.get("repo") or ""),
                "image": img,
                "kind": kind,
                "trajectory_name": "command-default",
                "has_reward": True,
                "setup": [{"name": "setup", "run": setup_cmd}],
                "reset": [{"name": "reset", "run": reset_cmd}],
                "steps": cmd_steps,
                "expected_stdout": str(material.get("expected_stdout") or row.get("expected_stdout") or ""),
                "expected_exit_code": int(material.get("expected_exit_code", row.get("expected_exit_code", 0)) or 0),
            })
        elif traj_name == "swebench-smoke":
            smoke_cmd = set_vars.get("SMOKE_CMD", "git status --short && python3 --version || python --version")
            used_vars["SMOKE_CMD"] = smoke_cmd
            rendered_instances.append({
                "instance_id": iid,
                "repo": str(row.get("repo") or ""),
                "image": img,
                "kind": kind,
                "trajectory_name": "swebench-smoke",
                "has_reward": True,
                "setup": [],
                "reset": [],
                "steps": [
                    {"name": "inspect", "run": "pwd"},
                    {"name": "grade", "run": smoke_cmd, "reward": True},
                ],
                "expected_exit_code": 0,
            })
        elif traj_name == "swebench-gold":
            eval_script = str(row.get("eval_script") or "")
            log_parser = str(row.get("log_parser") or "")
            if not eval_script or not log_parser:
                raise ValueError(f"{iid}: eval_script and log_parser are required by swebench-gold")
            patch_str = str(row.get("patch") or "")
            spec_json = json.dumps(row)
            rendered_instances.append({
                "instance_id": iid,
                "repo": str(row.get("repo") or ""),
                "image": img,
                "kind": kind,
                "trajectory_name": "swebench-gold",
                "has_reward": True,
                "setup": [{"name": "baseline", "run": "cd /testbed && git rev-parse HEAD > /tmp/ate-base"}],
                "reset": [{
                    "name": "reset",
                    "run": "cd /testbed && git reset --hard $(cat /tmp/ate-base) && git clean -fd",
                }],
                "steps": [
                    {
                        "name": "write-patch",
                        "write": {"path": "/tmp/ate-gold.patch", "content": patch_str, "mode": "0644"},
                    },
                    {
                        "name": "apply-patch",
                        "run": "cd /testbed && git apply -v /tmp/ate-gold.patch",
                        "expect_exit": 0,
                    },
                    {
                        "name": "write-eval",
                        "write": {"path": "/tmp/ate-eval.sh", "content": eval_script, "mode": "0755"},
                    },
                    {
                        "name": "run-tests",
                        "run": "/bin/bash /tmp/ate-eval.sh > /tmp/ate-eval.log 2>&1 || true",
                        "timeout_s": 1200.0,
                    },
                    {
                        "name": "write-grader",
                        "write": {"path": "/tmp/ate-grade.py", "content": grader_script, "mode": "0755"},
                    },
                    {
                        "name": "write-instance",
                        "write": {"path": "/tmp/ate-instance.json", "content": spec_json, "mode": "0644"},
                    },
                    {
                        "name": "grade",
                        "run": (
                            'PY=/opt/miniconda3/bin/python; [ -x "$PY" ] || PY=python3; '
                            '"$PY" /tmp/ate-grade.py /tmp/ate-instance.json /tmp/ate-eval.log'
                        ),
                        "reward": True,
                    },
                ],
                "expected_exit_code": 0,
            })
        else:
            raise ValueError(
                f"unsupported standalone trajectory {traj_name!r}; "
                "pass --rendered-plan from 'swebench-loader render-plan' for custom YAML trajectories"
            )

    unused = sorted(set(set_vars) - set(used_vars))
    if unused:
        raise ValueError(f"--set {unused[0]}: trajectory {traj_name} does not read it")

    return {
        "dataset": dataset_path,
        "dataset_label": dataset_label(dataset_path),
        "exclude_label": f"{pathlib.Path(exclude_file).name}@{ids_digest(exclude)}" if exclude_file else "",
        "instances_label": f"{len(ids)}@{ids_digest(ids)}" if ids else "",
        "trajectory_names": traj_name,
        "set_vars": used_vars,
        "instances": rendered_instances,
    }


class EventRecorder:
    """Thread-safe JSONL writer emitting canonical Go swebench.Event lines."""

    def __init__(self, path: pathlib.Path, run_id: str, worker: str = "", job: str = "") -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(path, "w", encoding="utf-8")  # noqa: SIM115
        self._lock = threading.Lock()
        self._closed = False
        self._run_id = run_id
        self._worker = worker
        self._job = job

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def _now_rfc3339() -> str:
        return datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")

    def record(self, ev: dict[str, Any]) -> None:
        payload = dict(ev)
        payload.setdefault("ts", self._now_rfc3339())
        payload.setdefault("run", self._run_id)
        if self._job and not payload.get("job"):
            payload["job"] = self._job
        if self._worker and not payload.get("worker"):
            payload["worker"] = self._worker
        line = json.dumps(payload, separators=(",", ":")) + "\n"
        with self._lock:
            if self._closed:
                return
            self._file.write(line)
            if payload.get("event") in ("run", "step_barrier", "park", "prewarm", "reap"):
                self._file.flush()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._file.flush()
            self._file.close()


class _InstrumentedDriver:
    """Wraps a SandboxFleet BackendDriver to record create/suspend/delete timings per sandbox."""

    def __init__(
        self, inner: Any, recorder: EventRecorder, run_id: str, step_num: int = 0
    ) -> None:
        self._inner = inner
        self._rec = recorder
        self._run_id = run_id
        self._step_num = step_num
        self.backend_name = getattr(inner, "backend_name", "substrate")
        self._meta_lock = threading.Lock()
        self.acquire_starts: dict[str, float] = {}
        self.acquire_durs_ms: dict[str, float] = {}
        self.sandbox_meta: dict[str, dict[str, Any]] = {}
        self.recorded_task_ids: set[str] = set()
        self.claimed_sources: dict[str, str] = {}
        self.acquire_failures = 0
        self.cold_creates = 0
        self.deleted_count = 0
        self._emitted_warm_envs: set[str] = set()
        self._live_warm_envs: dict[str, str] = {}
        self._mock_warm_pool: dict[tuple[str, str | None], list[str]] = collections.defaultdict(list)
        self._synth_to_raw_id: dict[str, str] = {}
        self._warm_seq = 0
        self._reap_recorded: set[str] = set()
        self._local = threading.local()

    @property
    def observer(self) -> Any:
        return getattr(self._inner, "observer", None)

    @observer.setter
    def observer(self, value: Any) -> None:
        with contextlib.suppress(AttributeError):
            self._inner.observer = value

    def bind_thread_task(
        self,
        task_id: str,
        instance_id: str,
        image: str,
        step: int,
        task_idx: int,
        traj_name: str,
    ) -> None:
        self._local.task_info = {
            "task_id": task_id,
            "instance": instance_id,
            "image": image,
            "step": step,
            "task": task_idx,
            "traj_name": traj_name,
        }

    def clear_thread_task(self) -> None:
        self._local.task_info = None

    def mark_task_recorded(self, task_id: str) -> None:
        with self._meta_lock:
            self.recorded_task_ids.add(task_id)

    def _has_real_warm_pool(self) -> bool:
        return hasattr(self._inner, "_warm_paused_pool")

    @staticmethod
    def _warm_entry_id(item: Any) -> str:
        if isinstance(item, (tuple, list)):
            return str(item[0]) if item else ""
        return str(item)

    def _snapshot_real_warm_ids(
        self, template_id: str, rootfs_image: str | None
    ) -> list[str]:
        pool_map = getattr(self._inner, "_warm_paused_pool", None)
        if pool_map is None:
            return []
        inner_lock = getattr(self._inner, "_lock", None)
        ctx = inner_lock if inner_lock is not None else contextlib.nullcontext()
        with ctx:
            entries = list(pool_map.get((template_id, rootfs_image), []))
        return [aid for item in entries if (aid := self._warm_entry_id(item))]

    def _snapshot_all_real_warm_ids(self) -> set[str]:
        pool_map = getattr(self._inner, "_warm_paused_pool", None)
        if pool_map is None:
            return set()
        inner_lock = getattr(self._inner, "_lock", None)
        ctx = inner_lock if inner_lock is not None else contextlib.nullcontext()
        out: set[str] = set()
        with ctx:
            for entries in pool_map.values():
                for item in entries:
                    aid = self._warm_entry_id(item)
                    if aid:
                        out.add(aid)
        return out

    def _emit_warm_create_suspend(self, env_id: str, img_label: str, dur_ms: float) -> None:
        create_ev: dict[str, Any] = {
            "event": "create",
            "env": env_id,
            "image": img_label,
            "source": "warm_pool",
            "dur_ms": dur_ms,
            "ok": True,
        }
        suspend_ev: dict[str, Any] = {
            "event": "suspend",
            "env": env_id,
            "image": img_label,
            "dur_ms": 0.0,
            "ok": True,
        }
        if self._step_num:
            create_ev["step"] = self._step_num
            suspend_ev["step"] = self._step_num
        self._rec.record(create_ev)
        self._rec.record(suspend_ev)

    def _emit_warm_delete(self, env_id: str, img_label: str, dur_ms: float = 0.0) -> None:
        del_ev: dict[str, Any] = {
            "event": "delete",
            "env": env_id,
            "image": img_label,
            "dur_ms": dur_ms,
            "ok": True,
        }
        if self._step_num:
            del_ev["step"] = self._step_num
        self._rec.record(del_ev)

    def _sync_warm_pool_events(
        self, template_id: str, rootfs_image: str | None, dur_ms: float
    ) -> int:
        img_label = rootfs_image or template_id
        to_create: list[str] = []
        to_delete: list[tuple[str, str]] = []

        if self._has_real_warm_pool():
            cur_ids = self._snapshot_real_warm_ids(template_id, rootfs_image)
            cur_set = set(cur_ids)
            with self._meta_lock:
                for aid in cur_ids:
                    if aid not in self._emitted_warm_envs:
                        self._emitted_warm_envs.add(aid)
                        self._live_warm_envs[aid] = img_label
                        to_create.append(aid)
                for aid in list(self._live_warm_envs.keys()):
                    if self._live_warm_envs[aid] == img_label and aid not in cur_set:
                        to_delete.append((aid, self._live_warm_envs.pop(aid)))
                        self.deleted_count += 1
        else:
            target = self._inner.pool_size(template_id, rootfs_image=rootfs_image) or 0
            key = (template_id, rootfs_image)
            with self._meta_lock:
                cur_list = self._mock_warm_pool[key]
                while len(cur_list) < target:
                    self._warm_seq += 1
                    wid = f"{self._run_id}-warm-{self._warm_seq}"
                    cur_list.append(wid)
                    self._emitted_warm_envs.add(wid)
                    self._live_warm_envs[wid] = img_label
                    to_create.append(wid)
                while len(cur_list) > target:
                    wid = cur_list.pop()
                    prev_img = self._live_warm_envs.pop(wid, img_label)
                    to_delete.append((wid, prev_img))
                    self.deleted_count += 1

        per_create_ms = dur_ms / max(1, len(to_create))
        for wid in to_create:
            self._emit_warm_create_suspend(wid, img_label, per_create_ms)
        for wid, prev_img in to_delete:
            self._emit_warm_delete(wid, prev_img)
        return len(to_create)

    def preflight(self) -> None:
        self._inner.preflight()

    def ensure_template(self, env: Any) -> str:
        return self._inner.ensure_template(env)

    def warm_pool(
        self,
        template_id: str,
        replicas: int,
        wait: bool = True,
        run_id: str | None = None,
        rootfs_image: str | None = None,
    ) -> None:
        t0 = time.monotonic()
        ok = True
        err_str = ""
        try:
            self._inner.warm_pool(
                template_id, replicas, wait=wait, run_id=run_id, rootfs_image=rootfs_image
            )
        except Exception as exc:
            ok = False
            err_str = str(exc)
            raise
        finally:
            dur_ms = (time.monotonic() - t0) * 1000.0
            added = self._sync_warm_pool_events(template_id, rootfs_image, dur_ms)
            if added > 0 or not ok:
                ev: dict[str, Any] = {
                    "event": "prewarm",
                    "image": rootfs_image or template_id,
                    "count": added,
                    "dur_ms": dur_ms,
                    "ok": ok,
                }
                if err_str:
                    ev["err"] = err_str
                self._rec.record(ev)

    def resize_pool(
        self,
        template_id: str,
        replicas: int,
        wait: bool = True,
        rootfs_image: str | None = None,
    ) -> None:
        t0 = time.monotonic()
        try:
            self._inner.resize_pool(template_id, replicas, wait=wait, rootfs_image=rootfs_image)
        finally:
            dur_ms = (time.monotonic() - t0) * 1000.0
            self._sync_warm_pool_events(template_id, rootfs_image, dur_ms)

    def unwarm_pool(self, template_id: str, rootfs_image: str | None = None) -> None:
        t0 = time.monotonic()
        try:
            self._inner.unwarm_pool(template_id, rootfs_image=rootfs_image)
        finally:
            dur_ms = (time.monotonic() - t0) * 1000.0
            self._sync_warm_pool_events(template_id, rootfs_image, dur_ms)

    def pool_size(self, template_id: str, rootfs_image: str | None = None) -> int | None:
        return self._inner.pool_size(template_id, rootfs_image=rootfs_image)

    def acquire(
        self,
        template_id: str,
        run_id: str,
        timeout_s: float = 180.0,
        rootfs_image: str | None = None,
    ) -> Any:
        t0 = time.monotonic()
        info = getattr(self._local, "task_info", None) or {}
        task_id = str(info.get("task_id") or "")
        try:
            raw = self._inner.acquire(
                template_id, run_id, timeout_s=timeout_s, rootfs_image=rootfs_image
            )
        except Exception as exc:
            dur_ms = (time.monotonic() - t0) * 1000.0
            with self._meta_lock:
                self.acquire_failures += 1
                if task_id:
                    self.recorded_task_ids.add(task_id)
            ev_base: dict[str, Any] = {
                "instance": info.get("instance", ""),
                "image": info.get("image") or (rootfs_image or ""),
                "task": info.get("task", 0),
                "ok": False,
                "err": str(exc),
            }
            if info.get("step"):
                ev_base["step"] = info["step"]
            self._rec.record({**ev_base, "event": "create", "dur_ms": dur_ms})
            self._rec.record({
                **ev_base,
                "event": "task",
                "name": info.get("traj_name", ""),
                "dur_ms": dur_ms,
            })
            raise

        dur_ms = (time.monotonic() - t0) * 1000.0
        claim_source = str(raw.metadata.get("claim_source", "direct"))
        meta = {
            "instance": info.get("instance", ""),
            "image": info.get("image") or (rootfs_image or ""),
            "step": info.get("step", 0),
            "task": info.get("task", 0),
        }
        minted_warm_id: str | None = None
        with self._meta_lock:
            if task_id:
                self.claimed_sources[task_id] = claim_source
            if claim_source == "warm_pool":
                if self._has_real_warm_pool():
                    self._live_warm_envs.pop(raw.instance_id, None)
                    if raw.instance_id not in self._emitted_warm_envs:
                        self._emitted_warm_envs.add(raw.instance_id)
                        minted_warm_id = raw.instance_id
                else:
                    key = (template_id, rootfs_image)
                    cur_list = self._mock_warm_pool[key]
                    if cur_list:
                        wid = cur_list.pop(0)
                        self._live_warm_envs.pop(wid, None)
                    else:
                        self._warm_seq += 1
                        wid = f"{self._run_id}-warm-{self._warm_seq}"
                        self._emitted_warm_envs.add(wid)
                        minted_warm_id = wid
                    self._synth_to_raw_id[wid] = raw.instance_id
                    raw.instance_id = wid
            else:
                self.cold_creates += 1

            self.acquire_starts[raw.instance_id] = t0
            self.acquire_durs_ms[raw.instance_id] = dur_ms
            self.sandbox_meta[raw.instance_id] = meta

        if minted_warm_id is not None:
            self._emit_warm_create_suspend(minted_warm_id, meta["image"], 0.0)
        elif claim_source != "warm_pool":
            create_ev: dict[str, Any] = {
                "event": "create",
                "env": raw.instance_id,
                "instance": meta["instance"],
                "image": meta["image"],
                "task": meta["task"],
                "source": claim_source,
                "dur_ms": dur_ms,
                "ok": True,
            }
            if meta["step"]:
                create_ev["step"] = meta["step"]
            self._rec.record(create_ev)
        return raw

    def release(self, instance_id: str, recycle: bool = False) -> None:
        t0 = time.monotonic()
        err_str = ""
        ok = True
        with self._meta_lock:
            inner_id = (
                self._synth_to_raw_id.get(instance_id, instance_id)
                if recycle
                else self._synth_to_raw_id.pop(instance_id, instance_id)
            )
        try:
            self._inner.release(inner_id, recycle=recycle)
        except Exception as exc:
            ok = False
            err_str = str(exc)
            raise
        finally:
            dur_ms = (time.monotonic() - t0) * 1000.0
            with self._meta_lock:
                meta = self.sandbox_meta.pop(instance_id, {})
                self.acquire_starts.pop(instance_id, None)
                self.acquire_durs_ms.pop(instance_id, None)
                if not recycle and ok:
                    self.deleted_count += 1
            if not recycle:
                ev: dict[str, Any] = {
                    "event": "delete",
                    "env": instance_id,
                    "instance": meta.get("instance", ""),
                    "image": meta.get("image", ""),
                    "task": meta.get("task", 0),
                    "dur_ms": dur_ms,
                    "ok": ok,
                }
                if meta.get("step"):
                    ev["step"] = meta["step"]
                if err_str:
                    ev["err"] = err_str
                self._rec.record(ev)

    def reap(self, run_id: str) -> int:
        t0 = time.monotonic()
        reaped = self._inner.reap(run_id)
        dur_ms = (time.monotonic() - t0) * 1000.0
        to_delete: list[tuple[str, str]] = []
        if self._has_real_warm_pool():
            remaining = self._snapshot_all_real_warm_ids()
            with self._meta_lock:
                for aid in list(self._live_warm_envs.keys()):
                    if aid not in remaining:
                        to_delete.append((aid, self._live_warm_envs.pop(aid)))
                        self.deleted_count += 1
        else:
            with self._meta_lock:
                self._mock_warm_pool.clear()
                for wid, img_label in list(self._live_warm_envs.items()):
                    to_delete.append((wid, img_label))
                    self.deleted_count += 1
                self._live_warm_envs.clear()
        for wid, img_label in to_delete:
            self._emit_warm_delete(wid, img_label, dur_ms / max(1, len(to_delete)))
        with self._meta_lock:
            remaining_count = len(self._live_warm_envs) + len(self.sandbox_meta)
            # The fleet reaps twice per `with` block: the strategy tears down at
            # the end of run() and __exit__ tears down again. One reap event per
            # run id, like bench reap's events/reap.jsonl; a repeat that deletes
            # nothing and finds nothing is not an event.
            repeat = run_id in self._reap_recorded
            self._reap_recorded.add(run_id)
        if repeat and reaped == 0 and not to_delete and remaining_count == 0:
            return reaped
        self._rec.record({
            "event": "reap",
            "env": run_id,
            "task": 0,
            "dur_ms": dur_ms,
            "ok": remaining_count == 0,
            "metrics": {
                "orphan_count": float(remaining_count),
                "deleted_count": float(max(reaped, len(to_delete))),
            },
        })
        return reaped

    def close(self) -> None:
        if hasattr(self._inner, "close"):
            self._inner.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Run R2E-Gym / SWE-bench workloads through Python SandboxFleet and emit swebench JSONL events."
    )
    p.add_argument("--rendered-plan", default="", help="Path to rendered plan JSON from 'swebench-loader render-plan'.")
    p.add_argument("--dataset", default="", help="JSONL dataset manifest (used when --rendered-plan is omitted).")
    p.add_argument("--images", type=int, default=0, help="First N instances from dataset (0 = all).")
    p.add_argument("--instances", default="", help="Comma-separated instance IDs.")
    p.add_argument("--exclude-file", default="", help="Path to exclude-file.")
    p.add_argument("--trajectory", default="swebench-gold", help="Trajectory name (when --rendered-plan is omitted).")
    p.add_argument("--seed", type=int, default=1, help="Deterministic seed.")
    p.add_argument("--mode", default="eval", choices=VALID_MODES, help="Benchmark mode label (eval or steps).")
    p.add_argument("--strategy", default="pipelined", choices=VALID_STRATEGIES, help="SandboxFleet strategy.")
    p.add_argument("--window-size", type=int, default=0, help="Sliding/pipelined window size in distinct images (0 = auto).")
    p.add_argument("--width", type=int, default=50, help="Maximum concurrent tasks in flight.")
    p.add_argument("--tasks-per-image", type=int, default=None, help="eval: replays per image.")
    p.add_argument("--rollouts", type=int, default=None, help="Replicas / rollouts per image.")
    p.add_argument("--steps", type=int, default=1, help="Number of passes over the task set.")
    p.add_argument("--endpoint", default="127.0.0.1:7777", help="ate-env-api gRPC host:port.")
    p.add_argument("--atespace", default="ate-env", help="Target Substrate atespace.")
    p.add_argument("--base-template", default="default-template", help="Base ActorTemplate name.")
    p.add_argument("--connections", type=int, default=4, help="gRPC channel pool size.")
    p.add_argument("--auth-token-file", default="", help="Bearer token file for ate-env-api.")
    p.add_argument("--create-timeout", default="5m", help="Sandbox acquire timeout (e.g. 5m, 180s).")
    p.add_argument("--step-timeout", default="10m", help="Default step command timeout (e.g. 10m, 300s).")
    p.add_argument("--wake-budget", default="3m", help="First-step retry budget on transient errors.")
    p.add_argument("--wake-backoff", default="2s", help="Initial retry backoff on transient first-step errors.")
    p.add_argument("--run-id", default="", help="Run ID (lowercase alphanumeric and '-', <= 20 chars).")
    p.add_argument("--out", default="", help="Output JSONL events path.")
    p.add_argument("--events-dir", default="", help="Optional directory for per-worker shard events/<worker>.jsonl.")
    p.add_argument("--worker", default="", help="Worker shard identifier stamped onto events.")
    p.add_argument("--job", default="", help="Job identifier stamped onto events.")
    p.add_argument("--fleet-report-out", default="", help="Optional path prefix (without suffix) to write SandboxFleet RunReport .json and .txt.")
    p.add_argument("--label", action="append", default=[], help="Operator KEY=VALUE label recorded on run event (repeatable).")
    p.add_argument("--set", dest="set_vars", action="append", default=[], help="Trajectory KEY=VALUE variable (repeatable).")
    p.add_argument("--backend", default="substrate", choices=VALID_BACKENDS, help="SandboxFleet backend ('substrate' or 'mock').")
    p.add_argument(
        "--warm-per-task",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Size warm pools to 1 paused replica per rollout in each window (default: true).",
    )
    p.add_argument(
        "--enable-tracing",
        action="store_true",
        help="Enable SDK span tracing and v1 TraceEvent JSONL capture.",
    )
    p.add_argument(
        "--trace-out",
        default="",
        help="Path to write v1 TraceEvent JSONL lines (implies --enable-tracing).",
    )
    p.add_argument("--dry-run", action="store_true", help="Validate plan and flags, print JSON summary, and exit.")
    return p


def _execute_step_with_retry(
    handle: Any,
    step_spec: dict[str, Any],
    default_step_timeout_s: float,
    first: bool,
    wake_budget_s: float,
    wake_backoff_s: float,
    infra_error_cls: type[BaseException],
) -> tuple[int, str, str, int, Exception | None]:
    """Execute one write or run step on SandboxHandle, retrying transient first-step errors."""
    deadline = time.monotonic() + max(0.0, wake_budget_s)
    wait_s = max(0.001, wake_backoff_s)
    attempt = 0

    while True:
        attempt += 1
        try:
            write_spec = step_spec.get("write")
            if write_spec is not None:
                path = str(write_spec["path"])
                content = str(write_spec.get("content", ""))
                handle.write_file(path, content)
                mode = str(write_spec.get("mode") or "")
                if mode and mode not in ("0644", "644"):
                    chmod_res = handle.exec(
                        ["chmod", mode, path],
                        timeout_s=min(30.0, default_step_timeout_s),
                        check=False,
                    )
                    if getattr(chmod_res, "timed_out", False) or chmod_res.exit_code is None:
                        return (
                            -1,
                            str(chmod_res.stdout or ""),
                            str(chmod_res.stderr or ""),
                            attempt,
                            TimeoutError(f"chmod {mode} {path} timed out"),
                        )
                    if chmod_res.exit_code != 0:
                        return (
                            int(chmod_res.exit_code),
                            str(chmod_res.stdout or ""),
                            str(chmod_res.stderr or ""),
                            attempt,
                            RuntimeError(f"chmod {mode} {path} exited {chmod_res.exit_code}"),
                        )
                return 0, "", "", attempt, None

            timeout_s = float(step_spec.get("timeout_s") or 0.0)
            if timeout_s <= 0:
                timeout_s = default_step_timeout_s
            res = handle.exec(
                ["sh", "-c", str(step_spec.get("run") or "")],
                timeout_s=timeout_s,
                check=False,
            )
            if getattr(res, "timed_out", False) or res.exit_code is None:
                return (
                    -1,
                    str(res.stdout or ""),
                    str(res.stderr or ""),
                    attempt,
                    TimeoutError(
                        f"step {step_spec.get('name', '')!r} timed out after {timeout_s:g}s"
                    ),
                )
            return int(res.exit_code), str(res.stdout or ""), str(res.stderr or ""), attempt, None
        except Exception as exc:  # noqa: BLE001
            retryable = isinstance(exc, infra_error_cls) and bool(getattr(exc, "retryable", False))
            if not first or not retryable or (time.monotonic() + wait_s) > deadline:
                return -1, "", "", attempt, exc
            time.sleep(wait_s)
            wait_s = min(wait_s * 2.0, 5.0 * max(0.001, wake_backoff_s))


def run_benchmark(argv: list[str] | None = None, *, custom_driver: Any | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.window_size < 0:
        raise ValueError(f"--window-size must be >= 0, got {args.window_size}")
    if args.window_size > 0 and args.strategy not in ("sliding", "pipelined"):
        raise ValueError("--window-size applies to --strategy sliding or pipelined")
    if args.width < 1:
        raise ValueError(f"--width must be >= 1, got {args.width}")
    if args.steps < 1:
        raise ValueError(f"--steps must be >= 1, got {args.steps}")
    if args.steps > 1:
        raise ValueError(
            "--strategy is single-pass (requires --steps 1; omit --strategy on Go swebench-loader for multi-step reset loops)"
        )
    if args.connections < 1:
        raise ValueError(f"--connections must be >= 1, got {args.connections}")

    op_labels = parse_kv_flags(args.label, "--label")
    for k in op_labels:
        if k in LOADER_LABEL_KEYS or k.startswith("set."):
            raise ValueError(
                f"--label {k}: the loader records that key itself, and report -arm trusts it; choose another name"
            )
    set_vars = parse_kv_flags(args.set_vars, "--set")

    create_timeout_s = parse_duration_s(args.create_timeout)
    step_timeout_s = parse_duration_s(args.step_timeout)
    wake_budget_s = parse_duration_s(args.wake_budget)
    wake_backoff_s = parse_duration_s(args.wake_backoff)
    auth_token = resolve_auth_token(args.auth_token_file)

    run_id = args.run_id or f"py-{int(time.time())}"
    if not _RUN_ID_RE.match(run_id) or len(run_id) > 20:
        raise ValueError(
            f"--run-id {run_id!r} must match ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ and be <= 20 chars"
        )

    if args.mode == "steps":
        fanout = args.rollouts if args.rollouts is not None else 2
    else:
        if args.tasks_per_image is not None:
            fanout = args.tasks_per_image
        elif args.rollouts is not None:
            fanout = args.rollouts
        else:
            fanout = 1
    if fanout < 1:
        raise ValueError(f"fanout (rollouts / tasks-per-image) must be >= 1, got {fanout}")

    if args.rendered_plan:
        plan = json.loads(pathlib.Path(args.rendered_plan).read_text(encoding="utf-8"))
    elif args.dataset:
        plan = _load_standalone_plan(
            dataset_path=args.dataset,
            images=args.images,
            instances_csv=args.instances,
            exclude_file=args.exclude_file,
            trajectory_spec=args.trajectory,
            set_vars=set_vars,
        )
    else:
        raise ValueError("one of --rendered-plan or --dataset is required")

    instances: list[dict[str, Any]] = list(plan.get("instances") or [])
    if not instances:
        raise ValueError("plan contains no instances")

    distinct_images = list(dict.fromkeys(str(inst["image"]) for inst in instances if inst.get("image")))
    eff_window = args.window_size if args.window_size > 0 else default_window_size(len(distinct_images))

    if args.events_dir:
        out_path = shard_event_path(args.events_dir, args.worker or "worker-0")
    elif args.out:
        out_path = pathlib.Path(args.out)
    else:
        out_path = pathlib.Path(f"{run_id}.jsonl")

    tracing_enabled = bool(
        args.enable_tracing
        or args.trace_out
        or os.environ.get("ENV_BENCH_MODE", "").strip().lower() == "trace"
    )
    trace_out_path: pathlib.Path | None = None
    if args.trace_out:
        trace_out_path = pathlib.Path(args.trace_out)
    elif tracing_enabled:
        env_trace = os.environ.get("ENV_BENCH_TRACE_PATH", "").strip()
        if env_trace:
            trace_out_path = pathlib.Path(env_trace)
        else:
            shard_stem = (args.worker or out_path.stem or "trace").removesuffix(".jsonl")
            if args.events_dir:
                base_root = pathlib.Path(args.events_dir)
                if base_root.name == "events":
                    base_root = base_root.parent
                trace_out_path = base_root / "traces" / f"{shard_stem}.jsonl"
            elif out_path.parent.name == "events":
                trace_out_path = out_path.parent.parent / "traces" / f"{shard_stem}.jsonl"
            else:
                trace_out_path = out_path.parent / "traces" / f"{shard_stem}.jsonl"

    plan_summary: dict[str, Any] = {
        "msg": "plan",
        "client": "python",
        "run": run_id,
        "mode": args.mode,
        "strategy": args.strategy,
        "window_size": eff_window if args.strategy in ("sliding", "pipelined") else 0,
        "images": len(instances),
        "distinct_images": len(distinct_images),
        "fanout": fanout,
        "steps": args.steps,
        "environments": len(instances) * fanout,
        "width": args.width,
        "trajectory": plan.get("trajectory_names", args.trajectory),
        "out": str(out_path),
        "tracing": tracing_enabled,
    }
    if trace_out_path is not None:
        plan_summary["trace_out"] = str(trace_out_path)
    print(json.dumps(plan_summary), file=sys.stderr)
    if args.dry_run:
        return 0

    sdk, infra_error_cls, current_slot_wait_s = load_sdk()
    trace_rec_mod = _load_trace_recorder() if tracing_enabled else None
    prev_bench_mode = os.environ.get("ENV_BENCH_MODE")
    prev_trace_path = os.environ.get("ENV_BENCH_TRACE_PATH")
    recorder: EventRecorder | None = None

    stats: dict[str, Any] = {
        "total_tasks": 0,
        "failed_tasks": 0,
        "rewarded_tasks": 0,
        "ws_hits": 0,
        "ws_misses": 0,
        "ws_reused": 0,
        "warm_created_total": 0,
        "cold_created_total": 0,
        "retired_total": 0,
        "peak_warm_max": 0,
        "combined_reports": [],
    }
    stats_lock = threading.Lock()

    def _execute_run() -> None:
        assert recorder is not None
        # One pass: --steps is 1 (checked above), as the Go loader's -strategy.
        step_num = 1 if args.mode == "steps" else 0

        task_specs: dict[str, dict[str, Any]] = {}
        sdk_tasks: list[Any] = []
        image_task_counts: dict[str, int] = {}

        for inst in instances:
            iid = str(inst["instance_id"])
            img = str(inst["image"])
            image_task_counts[img] = image_task_counts.get(img, 0) + fanout
            rollout_trajs = inst.get("rollouts") or []
            for k in range(fanout):
                tid = iid if fanout == 1 else f"{iid}#r{k}"
                r_spec = rollout_trajs[k] if k < len(rollout_trajs) else inst
                spec = {
                    "instance_id": iid,
                    "repo": str(inst.get("repo") or ""),
                    "image": img,
                    "kind": str(inst.get("kind") or "swebench"),
                    "rollout": k,
                    "step_num": step_num,
                    "trajectory_name": str(r_spec.get("trajectory_name") or inst.get("trajectory_name") or ""),
                    "has_reward": bool(r_spec.get("has_reward", inst.get("has_reward", False))),
                    "setup": list(r_spec.get("setup") or []),
                    "steps": list(r_spec.get("steps") or []),
                    "expected_stdout": str(inst.get("expected_stdout") or ""),
                    "expected_exit_code": int(inst.get("expected_exit_code", 0) or 0),
                }
                task_specs[tid] = spec
                sdk_tasks.append(
                    sdk.Task(
                        id=tid,
                        image=img,
                        metadata={"instance_id": iid, "repo": spec["repo"], "rollout": k},
                    )
                )

        max_pool = max(1, max(image_task_counts.values(), default=fanout))
        fleet_cfg = sdk.FleetConfig(
            backend=args.backend,
            endpoint=args.endpoint if args.backend == "substrate" else None,
            tenancy=args.atespace,
            template=args.base_template,
            auth_token=auth_token,
            strategy=args.strategy,
            max_concurrent=args.width,
            batch_size=eff_window,
            window_size=eff_window if args.strategy in ("sliding", "pipelined") else None,
            max_warmpool_replicas=max_pool,
            warm_per_task=args.warm_per_task,
            warm_parallelism=min(args.width, max(1, eff_window)),
            connections=args.connections,
            acquire_timeout_s=create_timeout_s,
            ready_timeout_s=create_timeout_s,
            step_timeout_s=step_timeout_s,
            run_id=run_id,
            observability=sdk.ObservabilityConfig(
                enable_metrics=False, enable_tracing=tracing_enabled
            ),
        )

        pass_start = time.monotonic()
        pass_live = 0
        pass_warm_hits = 0
        pass_warm_created = 0

        with sdk.SandboxFleet(config=fleet_cfg, driver=custom_driver) as fleet:
            inst_driver = _InstrumentedDriver(fleet.backend, recorder, run_id, step_num=step_num)
            inst_driver.observer = fleet.observer
            fleet.backend = inst_driver
            fleet.load_tasks(sdk_tasks)

            # Wrap fleet._acquire_task so _InstrumentedDriver knows which task is acquiring on this thread.
            orig_acquire_task = fleet._acquire_task

            def _bound_acquire_task(task_obj: Any, timeout_s: float | None) -> Any:
                sp = task_specs.get(task_obj.id, {})
                clean_iid = str(sp.get("instance_id", task_obj.id))
                inst_driver.bind_thread_task(
                    task_id=task_obj.id,
                    instance_id=clean_iid,
                    image=task_obj.image,
                    step=step_num,
                    task_idx=int(sp.get("rollout", 0)),
                    traj_name=sp.get("trajectory_name", ""),
                )
                trace_task_obj = (
                    dataclasses.replace(task_obj, id=clean_iid)
                    if task_obj.id != clean_iid
                    else task_obj
                )
                try:
                    return orig_acquire_task(trace_task_obj, timeout_s)
                finally:
                    inst_driver.clear_thread_task()

            fleet._acquire_task = _bound_acquire_task  # type: ignore[method-assign]

            def _process_one(task_obj: Any, handle: Any) -> bool:
                nonlocal pass_live, pass_warm_hits
                sp = task_specs[task_obj.id]
                iid = sp["instance_id"]
                img = sp["image"]
                rollout_idx = int(sp["rollout"])
                traj_name = sp["trajectory_name"]
                slot_wait_ms = max(0.0, float(current_slot_wait_s.get() or 0.0) * 1000.0)

                is_warm = handle.claim_source == "warm_pool"

                with inst_driver._meta_lock:
                    acq_start = inst_driver.acquire_starts.get(handle.sandbox_id)

                all_steps = list(sp["setup"]) + list(sp["steps"])
                first = True
                task_t0 = time.monotonic()
                ok = True
                reward: int | None = None
                total_think_ms = 0.0

                for s_spec in all_steps:
                    think_ms = float(s_spec.get("think_ms") or 0.0)
                    if think_ms > 0 and not first:
                        t_sleep0 = time.monotonic()
                        time.sleep(think_ms / 1000.0)
                        total_think_ms += (time.monotonic() - t_sleep0) * 1000.0

                    step_t0 = time.monotonic()
                    t_ctx = (
                        trace_rec_mod.task_context(
                            run_id=run_id,
                            job=args.job or None,
                            step=step_num or None,
                            instance=iid,
                            image=img,
                        )
                        if trace_rec_mod is not None
                        else contextlib.nullcontext()
                    )
                    with t_ctx:
                        exit_code, stdout, stderr, attempts, s_err = _execute_step_with_retry(
                            handle=handle,
                            step_spec=s_spec,
                            default_step_timeout_s=step_timeout_s,
                            first=first,
                            wake_budget_s=wake_budget_s,
                            wake_backoff_s=wake_backoff_s,
                            infra_error_cls=infra_error_cls,
                        )
                    step_dur_ms = (time.monotonic() - step_t0) * 1000.0

                    if first:
                        first_kind = "wake" if is_warm else "ttfi"
                        if first_kind == "ttfi" and acq_start is not None:
                            first_dur_ms = (time.monotonic() - acq_start) * 1000.0
                            wire_ms = max(0.0, (step_t0 - acq_start) * 1000.0)
                        else:
                            first_dur_ms = step_dur_ms
                            wire_ms = 0.0
                        first_ev: dict[str, Any] = {
                            "event": first_kind,
                            "env": handle.sandbox_id,
                            "instance": iid,
                            "image": img,
                            "task": rollout_idx,
                            "dur_ms": first_dur_ms,
                            "slot_wait_ms": slot_wait_ms,
                            "attempts": attempts,
                            "ok": s_err is None,
                        }
                        if step_num:
                            first_ev["step"] = step_num
                        if s_err is not None:
                            first_ev["err"] = str(s_err)
                        if first_kind == "ttfi":
                            first_ev["metrics"] = {
                                "sdk_slot_wait_ms": round(slot_wait_ms, 1),
                                "wire_claim_to_ready_ms": round(wire_ms, 1),
                                "ready_to_first_cmd_ms": round(step_dur_ms, 1),
                            }
                        recorder.record(first_ev)
                        first = False

                    step_ev: dict[str, Any] = {
                        "event": "step",
                        "name": str(s_spec["name"]),
                        "env": handle.sandbox_id,
                        "instance": iid,
                        "image": img,
                        "task": rollout_idx,
                        "dur_ms": step_dur_ms,
                        "ok": s_err is None,
                    }
                    if step_num:
                        step_ev["step"] = step_num
                    if s_spec.get("write") is None and s_err is None:
                        step_ev["exit"] = exit_code

                    if s_err is not None:
                        ok = False
                        step_ev["err"] = str(s_err)
                    elif s_spec.get("reward"):
                        if sp["kind"] == "command":
                            rew_val, detail = grade_command_output(
                                stdout=stdout,
                                stderr=stderr,
                                exit_code=exit_code,
                                expected_exit_code=sp["expected_exit_code"],
                                expected_stdout=sp["expected_stdout"],
                            )
                        else:
                            rew_val, detail = grade_swebench_output(stdout + stderr, exit_code)
                        reward = rew_val
                        step_ev["reward"] = reward
                        if detail:
                            step_ev["detail"] = detail
                        combined_out = stdout + stderr
                        if combined_out:
                            step_ev["tail"] = combined_out[-4000:]
                    elif s_spec.get("expect_exit") is not None and exit_code != int(s_spec["expect_exit"]):
                        ok = False
                        step_ev["ok"] = False
                        step_ev["err"] = f"exit {exit_code}, expected {int(s_spec['expect_exit'])}"
                        combined_out = stdout + stderr
                        if combined_out:
                            step_ev["tail"] = combined_out[-2000:]

                    recorder.record(step_ev)
                    if not ok:
                        break

                if ok and reward is None and sp["has_reward"]:
                    ok = False

                task_dur_ms = (time.monotonic() - task_t0) * 1000.0
                task_ev: dict[str, Any] = {
                    "event": "task",
                    "name": traj_name,
                    "env": handle.sandbox_id,
                    "instance": iid,
                    "image": img,
                    "task": rollout_idx,
                    "dur_ms": task_dur_ms,
                    "slot_wait_ms": slot_wait_ms,
                    "ok": ok,
                }
                if step_num:
                    task_ev["step"] = step_num
                if reward is not None:
                    task_ev["reward"] = reward
                if total_think_ms > 0:
                    task_ev["metrics"] = {
                        "llm_think_ms": round(total_think_ms, 1),
                        "env_wait_ms": round(max(0.0, task_dur_ms - total_think_ms), 1),
                    }
                recorder.record(task_ev)
                inst_driver.mark_task_recorded(task_obj.id)

                # The task and its claim count once, with its task event: here,
                # or below for a task whose callback raised before this point.
                with stats_lock:
                    stats["total_tasks"] += 1
                    if not ok:
                        stats["failed_tasks"] += 1
                    else:
                        pass_live += 1
                    if reward == 1:
                        stats["rewarded_tasks"] += 1
                    if is_warm:
                        stats["ws_hits"] += 1
                        stats["ws_reused"] += 1
                        pass_warm_hits += 1
                    else:
                        stats["ws_misses"] += 1
                return ok

            results = fleet.run(_process_one, concurrency=args.width)
            # Tasks whose acquire, runtime setup or callback raised before the
            # task event was recorded: one failed task and one claim each.
            for task_obj, r in zip(sdk_tasks, results, strict=False):
                if isinstance(r, BaseException):
                    sp = task_specs.get(task_obj.id, {})
                    with inst_driver._meta_lock:
                        already_recorded = task_obj.id in inst_driver.recorded_task_ids
                        claim_src = inst_driver.claimed_sources.get(task_obj.id, "")
                    if not already_recorded:
                        fail_ev: dict[str, Any] = {
                            "event": "task",
                            "name": sp.get("trajectory_name", ""),
                            "instance": sp.get("instance_id", task_obj.id),
                            "image": task_obj.image,
                            "task": int(sp.get("rollout", 0)),
                            "dur_ms": 0.0,
                            "ok": False,
                            "err": str(r),
                        }
                        if step_num:
                            fail_ev["step"] = step_num
                        recorder.record(fail_ev)
                        inst_driver.mark_task_recorded(task_obj.id)
                        with stats_lock:
                            stats["total_tasks"] += 1
                            stats["failed_tasks"] += 1
                            if claim_src == "warm_pool":
                                stats["ws_hits"] += 1
                                stats["ws_reused"] += 1
                                pass_warm_hits += 1
                            else:
                                stats["ws_misses"] += 1

            rep = fleet.report
            if rep is not None:
                stats["combined_reports"].append(rep.to_dict())
                with stats_lock:
                    pass_warm_created = rep.warm_total
                    stats["warm_created_total"] += rep.warm_total
                    stats["peak_warm_max"] = max(stats["peak_warm_max"], rep.peak_warm)
                    stats["cold_created_total"] += inst_driver.cold_creates

        # Include deletes emitted during release + any unclaimed warm replicas drained on teardown.
        with stats_lock:
            stats["retired_total"] += max(
                inst_driver.deleted_count,
                pass_warm_hits + max(0, pass_warm_created - pass_warm_hits),
            )

        if args.mode == "steps":
            tot_claims = stats["ws_hits"] + stats["ws_misses"]
            hit_rate = (stats["ws_hits"] / tot_claims) if tot_claims > 0 else 0.0
            recorder.record({
                "event": "step_barrier",
                "step": step_num,
                "dur_ms": (time.monotonic() - pass_start) * 1000.0,
                "ok": stats["failed_tasks"] == 0,
                "count": pass_live,
                "working_set_hits": stats["ws_hits"],
                "working_set_misses": stats["ws_misses"],
                "working_set_hit_rate": hit_rate,
                "standing_pool_size": stats["peak_warm_max"],
                "created": stats["warm_created_total"] + stats["cold_created_total"],
                "reused": stats["ws_reused"],
                "retired": stats["retired_total"],
            })

    run_start = time.monotonic()
    try:
        if tracing_enabled:
            if trace_out_path is not None:
                trace_out_path.parent.mkdir(parents=True, exist_ok=True)
                trace_out_path.unlink(missing_ok=True)
                os.environ["ENV_BENCH_MODE"] = "trace"
                os.environ["ENV_BENCH_TRACE_PATH"] = str(trace_out_path)
            if trace_rec_mod is not None:
                trace_rec_mod.set_bench_context(run_id=run_id, job=args.job or None)

        recorder = EventRecorder(
            out_path,
            run_id=run_id,
            worker=(args.worker or ("worker-0" if args.events_dir else "")),
            job=args.job,
        )
        _execute_run()

        total_tasks = int(stats["total_tasks"])
        failed_tasks = int(stats["failed_tasks"])
        rewarded_tasks = int(stats["rewarded_tasks"])
        ws_hits = int(stats["ws_hits"])
        ws_misses = int(stats["ws_misses"])
        ws_reused = int(stats["ws_reused"])
        warm_created_total = int(stats["warm_created_total"])
        cold_created_total = int(stats["cold_created_total"])
        retired_total = int(stats["retired_total"])
        peak_warm_max = int(stats["peak_warm_max"])
        combined_reports = list(stats["combined_reports"])

        run_dur_ms = (time.monotonic() - run_start) * 1000.0
        labels: dict[str, str] = {
            "mode": args.mode,
            "strategy": args.strategy,
            "width": str(args.width),
            "images": str(len(instances)),
            "trajectory": str(plan.get("trajectory_names") or args.trajectory),
            "create_timeout": format_go_duration(create_timeout_s),
            "step_timeout": format_go_duration(step_timeout_s),
            "wake_budget": format_go_duration(wake_budget_s),
            "wake_backoff": format_go_duration(wake_backoff_s),
            "dataset": str(plan.get("dataset_label") or dataset_label(str(plan.get("dataset") or ""))),
            "connections": str(args.connections),
            "base_template": args.base_template,
        }
        if args.strategy in ("sliding", "pipelined"):
            labels["window_size"] = str(eff_window)
        if plan.get("has_trajectory_mix"):
            labels["seed"] = str(plan.get("seed", args.seed))
        if args.mode == "eval":
            labels["tasks_per_image"] = str(fanout)
        else:
            labels["rollouts"] = str(fanout)
            labels["steps"] = str(args.steps)
            labels["park"] = "delete" if args.strategy == "none" else "suspend"
        if plan.get("exclude_label"):
            labels["exclude_file"] = str(plan["exclude_label"])
        if plan.get("instances_label"):
            labels["instances"] = str(plan["instances_label"])
        for k, v in (plan.get("set_vars") or {}).items():
            labels[f"set.{k}"] = str(v)
        labels.update(op_labels)

        tasks_per_step = (
            min(len(instances), eff_window)
            if args.strategy in ("sliding", "pipelined")
            else len(instances)
        )
        tags: dict[str, str] = {
            "load_tasks": str(len(instances)),
            "load_group_size": str(fanout),
            "load_steps": str(args.steps),
            "load_tasks_per_step": str(tasks_per_step),
            "load_trajectories_per_step": str(fanout * tasks_per_step),
            "load_max_concurrent": str(args.width),
            "load_distinct_images": str(len(distinct_images)),
            "load_repeat": "1",
        }
        if plan.get("dataset"):
            tags["dataset"] = str(plan["dataset"])
        if distinct_images:
            tags["guest_image"] = distinct_images[0]

        tot_claims = ws_hits + ws_misses
        hit_rate = (ws_hits / tot_claims) if tot_claims > 0 else 0.0
        recorder.record({
            "event": "run",
            "name": args.mode,
            "dur_ms": run_dur_ms,
            "ok": failed_tasks == 0,
            "count": total_tasks,
            "labels": labels,
            "tags": tags,
            "working_set_hits": ws_hits,
            "working_set_misses": ws_misses,
            "working_set_hit_rate": hit_rate,
            "standing_pool_size": peak_warm_max,
            "created": warm_created_total + cold_created_total,
            "reused": ws_reused,
            "retired": retired_total,
        })
    finally:
        if recorder is not None:
            recorder.close()
        if tracing_enabled:
            if trace_rec_mod is not None:
                trace_rec_mod.clear_bench_context()
            if prev_bench_mode is None:
                os.environ.pop("ENV_BENCH_MODE", None)
            else:
                os.environ["ENV_BENCH_MODE"] = prev_bench_mode
            if prev_trace_path is None:
                os.environ.pop("ENV_BENCH_TRACE_PATH", None)
            else:
                os.environ["ENV_BENCH_TRACE_PATH"] = prev_trace_path

    if args.fleet_report_out and combined_reports:
        prefix = pathlib.Path(args.fleet_report_out)
        prefix.parent.mkdir(parents=True, exist_ok=True)
        payload = combined_reports[0]
        prefix.with_name(prefix.name + ".json").write_text(
            json.dumps(payload, indent=2) + "\n", encoding="utf-8"
        )
        lines = [
            (
                f"strategy={args.strategy} tasks={total_tasks} rewarded={rewarded_tasks} failed={failed_tasks} "
                f"warm_hits={ws_hits} warm_misses={ws_misses} peak_warm={peak_warm_max}"
            )
        ]
        prefix.with_name(prefix.name + ".txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(
        json.dumps({
            "msg": "run finished",
            "client": "python",
            "strategy": args.strategy,
            "tasks": total_tasks,
            "failed": failed_tasks,
            "rewarded": rewarded_tasks,
            "warm_hits": ws_hits,
            "warm_misses": ws_misses,
        }),
        file=sys.stderr,
    )
    if failed_tasks > 0:
        print(f"error: {failed_tasks} of {total_tasks} tasks failed", file=sys.stderr)
        return 1
    return 0


def _sigterm_handler(_signum: int, _frame: Any) -> None:
    raise KeyboardInterrupt("received SIGTERM")


def main() -> None:
    signal.signal(signal.SIGTERM, _sigterm_handler)
    try:
        rc = run_benchmark()
    except KeyboardInterrupt as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
    sys.exit(rc)


if __name__ == "__main__":  # pragma: no cover
    main()

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

"""Backwards-compatibility checks for the ate_env_openhands.r2egym re-export.

Full adapter unit tests live in clients/python/tests/test_r2egym.py.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import ate_env_rl.adapters.r2egym as canonical
import pytest

import ate_env_openhands.r2egym as compat


class _ExecResult:
    def __init__(self, exit_code=0, stdout="", stderr="", timed_out=False):
        self.exit_code, self.stdout, self.stderr, self.timed_out = exit_code, stdout, stderr, timed_out


class _Handle:
    def __init__(self, sandbox_id="sb-1", image="example.com/task@sha256:" + "c" * 64):
        self.sandbox_id = sandbox_id
        self.task = SimpleNamespace(
            id="x",
            image=image,
            metadata={"ds": {"instance_id": "x", "docker_image": image, "repo": "django/django"}},
        )
        self.execs: list[str] = []

    def exec(self, command, cwd="", env=None, timeout_s=None, check=True):
        self.execs.append(command)
        return _ExecResult(0, stdout=f"ran: {command}\n")

    def write_file(self, path, content):
        pass


class _FakeBaseRuntime:
    def __init__(self, ds, docker_image=None, command="/bin/bash", logger=None, backend="docker", **kw):
        self.ds, self.docker_image, self.command = ds, docker_image, command
        self.logger = logger or MagicMock()
        self.backend, self.repo_name, self.docker_kwargs = backend, "repo", kw
        self.container = None
        self.container_name = None
        self.start_container(docker_image, command, None)

    def start_container(self, image, command, name, **kw):
        if self.backend == "kubernetes-sandbox":
            self._start_kubernetes_sandbox()

    def stop_container(self):
        if self.backend == "kubernetes-sandbox":
            self._stop_kubernetes_sandbox()

    def close(self):
        self.stop_container()


class _FakeBaseRepoEnv:
    def close(self):
        self.runtime.close()


@pytest.fixture
def fake_r2egym(monkeypatch):
    canonical._CLASSES = None

    def mod(name):
        m = types.ModuleType(name)
        monkeypatch.setitem(sys.modules, name, m)
        return m

    mod("r2egym")
    mod("r2egym.agenthub")
    mod("r2egym.agenthub.environment")
    env_mod = mod("r2egym.agenthub.environment.env")
    env_mod.EnvArgs = lambda ds=None, repo_path=None, docker_image=None: SimpleNamespace(ds=ds)
    env_mod.RepoEnv = _FakeBaseRepoEnv
    mod("r2egym.agenthub.runtime")
    docker_mod = mod("r2egym.agenthub.runtime.docker")
    docker_mod.DockerRuntime = _FakeBaseRuntime
    docker_mod.CMD_TIMEOUT = 900
    docker_mod.config = SimpleNamespace()
    docker_mod.client = SimpleNamespace()
    mod("r2egym.agenthub.utils")
    mod("r2egym.agenthub.utils.log").get_logger = lambda *a, **k: MagicMock()
    mod("r2egym.agenthub.agent")
    mod("r2egym.agenthub.agent.commands").ParseCommandBash = lambda: None
    yield
    canonical._CLASSES = None


def test_public_exports_are_identical_to_ate_env_rl_adapter():
    assert compat.__all__ == canonical.__all__
    assert compat.make_fleet_repo_env is canonical.make_fleet_repo_env
    assert compat.fresh_eval_env is canonical.fresh_eval_env
    assert compat.r2egym_command_files is canonical.r2egym_command_files
    with pytest.raises(AttributeError):
        _ = compat.no_such_symbol


def test_guard_without_r2egym(monkeypatch):
    canonical._CLASSES = None
    monkeypatch.setitem(sys.modules, "r2egym", None)
    with pytest.raises(RuntimeError, match="not on PyPI"):
        compat.make_fleet_repo_env(_Handle())
    with pytest.raises(RuntimeError, match="not on PyPI"):
        compat.r2egym_command_files()


def test_star_import_via_reexport_works_without_r2egym(monkeypatch):
    canonical._CLASSES = None
    monkeypatch.setitem(sys.modules, "r2egym", None)
    ns: dict[str, object] = {}
    exec("from ate_env_openhands.r2egym import *", ns)
    assert set(compat.__all__) <= set(ns)
    assert "AteEnvRuntime" not in ns and "AteEnvRepoEnv" not in ns
    with pytest.raises(RuntimeError, match="not on PyPI"):
        _ = compat.AteEnvRuntime


def test_lazy_classes_and_entrypoints_work_via_reexport(fake_r2egym):
    assert compat.AteEnvRuntime is canonical.AteEnvRuntime
    assert compat.AteEnvRepoEnv is canonical.AteEnvRepoEnv

    first = _Handle(sandbox_id="sb-agent")
    second = _Handle(sandbox_id="sb-eval")
    fleet = SimpleNamespace(acquire=MagicMock(return_value=second), release=MagicMock())
    with compat.fresh_eval_env(fleet, first) as (eval_handle, env):
        assert eval_handle is second
        assert env.runtime.container_name == "sb-eval"
    fleet.release.assert_called_once_with(second)

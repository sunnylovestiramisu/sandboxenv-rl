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

"""R2E-Gym adapter: R2E-Gym's ``RepoEnv`` on a sandbox a fleet acquired.

The same names as ``agent_sandbox_rl.adapters.r2egym``. R2E-Gym's ``DockerRuntime`` has a
``kubernetes-sandbox`` backend with four hooks; ``AteEnvRuntime`` overrides
them: bind the fleet handle instead of claiming a pod, no-op teardown (the
fleet releases), ``handle.exec`` for commands and ``handle.write_file`` plus
``tar`` for file copies. Everything else in the runtime (setup, patches, reward
grading) runs unchanged against the environment.

    from ate_env_rl import SandboxFleet, FleetConfig
    from ate_env_openhands.r2egym import make_fleet_repo_env, r2egym_command_files

    def rollout(task, handle):                 # task.metadata["ds"] is the dataset row
        env = make_fleet_repo_env(handle, command_files=r2egym_command_files())
        try:
            obs, reward, done, info = env.step(action)
            return env.compute_reward()
        finally:
            env.close()                        # no-op on the sandbox; the fleet releases

    fleet.run(rollout)

A Substrate actor has one container, thus the clean grading container of the
MLPerf setup is a second environment from the same image: ``fresh_eval_env``
acquires it from the warm pool and releases it after the grade.

R2E-Gym is not on PyPI; install it from its checkout
(``pip install -e path/to/R2E-Gym``). Importing this module is cheap and the
R2E-Gym subclasses are built on first use, so the package works without it.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
import re
import shlex
import tarfile
import threading
import uuid
from collections.abc import Iterator
from typing import Any

logger = logging.getLogger("ate_env_openhands.r2egym")

_HINT = "requires R2E-Gym: pip install -e path/to/R2E-Gym (it is not on PyPI)"

_CLASSES: tuple[type, type] | None = None
_BUILD_LOCK = threading.Lock()
_ANSI = re.compile(r"\x1b\[[0-9;]*m|\r")


# R2E-Gym's PATH for commands in a task image (r2egym.agenthub.runtime.docker
# DOCKER_PATH): the testbed venv first. Its docker backend passes it on each exec
# and its kubernetes backend sets it in the pod env; used when the installed
# R2E-Gym does not export the constant.
_R2E_DOCKER_PATH = (
    "/root/.venv/bin:/root/.local/bin:/root/.cargo/bin:"
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)


def _as_root(info: tarfile.TarInfo) -> tarfile.TarInfo:
    """Archive members owned by root, as a copy into a container expects."""
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    return info


def _import_r2egym():
    try:
        from r2egym.agenthub.environment.env import EnvArgs, RepoEnv  # noqa: F401
        from r2egym.agenthub.runtime import docker as docker_mod
    except ImportError as e:
        raise RuntimeError(f"ate_env_openhands.r2egym {_HINT}") from e
    return docker_mod, RepoEnv


def _dataset_row(handle: Any) -> dict:
    ds = (getattr(getattr(handle, "task", None), "metadata", None) or {}).get("ds")
    if ds is None:
        raise RuntimeError(
            "Task.metadata['ds'] is required for the R2E-Gym adapter: load tasks with the "
            "dataset row kept: ate_env_rl.SweBenchSource(keep_row=True)."
        )
    return ds


_bypass = threading.local()


class _Bypassable:
    """Stands in for the kubernetes ``config`` and ``client`` names of R2E-Gym's
    docker module. It delegates to the real object, except on a thread that is
    building an ``AteEnvRuntime``: there the names in ``stubs`` answer instead.
    Installed once; other threads and other runtimes see the real objects."""

    def __init__(self, wrapped: Any, stubs: dict[str, Any]) -> None:
        self._wrapped = wrapped
        self._stubs = stubs

    def __getattr__(self, name: str) -> Any:
        if name in self._stubs and getattr(_bypass, "active", False):
            return self._stubs[name]
        return getattr(self._wrapped, name)


def _install_bypass(docker_mod: Any) -> None:
    """R2E-Gym's runtime loads a kubeconfig and builds a CoreV1Api for the
    kubernetes-sandbox backend. Neither exists where ate-env runs, and the
    hooks below never use them. Wrap both names once (idempotent); the stubs
    answer only on a thread inside ``_bypassing_kubernetes``."""
    if not isinstance(getattr(docker_mod, "config", None), _Bypassable):
        docker_mod.config = _Bypassable(getattr(docker_mod, "config", None), {
            "load_incluster_config": lambda *a, **k: None,
            "load_kube_config": lambda *a, **k: None,
        })
    if not isinstance(getattr(docker_mod, "client", None), _Bypassable):
        docker_mod.client = _Bypassable(getattr(docker_mod, "client", None), {
            "CoreV1Api": lambda *a, **k: None,
        })


# Bound on one copy into the sandbox (write the tar, then extract it).
_COPY_TIMEOUT_S = 120


def _environment_dict(environment: Any) -> dict[str, str]:
    """docker_kwargs["environment"] as a dict, like the Docker SDK accepts it.

    The SDK takes a mapping or a list of ``"KEY=VALUE"`` strings. Anything
    else, or a list item without ``=``, raises ``TypeError``.
    """
    if isinstance(environment, dict):
        return {str(k): str(v) for k, v in environment.items()}
    if isinstance(environment, (list, tuple)):
        out: dict[str, str] = {}
        for item in environment:
            if not isinstance(item, str) or "=" not in item:
                raise TypeError(f'docker_kwargs["environment"] items must be "KEY=VALUE" strings, got {item!r}')
            key, value = item.split("=", 1)
            out[key] = value
        return out
    raise TypeError(
        f'docker_kwargs["environment"] must be a dict or a list of "KEY=VALUE" strings, '
        f"got {type(environment).__name__}"
    )


@contextlib.contextmanager
def _bypassing_kubernetes() -> Iterator[None]:
    """On this thread only, the kubernetes names answer with no-ops."""
    previous = getattr(_bypass, "active", False)
    _bypass.active = True
    try:
        yield
    finally:
        _bypass.active = previous


def _build_classes() -> tuple[type, type]:
    """Build (and memoise) the R2E-Gym subclasses; RuntimeError without R2E-Gym."""
    global _CLASSES
    if _CLASSES is not None:
        return _CLASSES
    docker_mod, RepoEnv = _import_r2egym()
    with _BUILD_LOCK:
        _install_bypass(docker_mod)
    DockerRuntime = docker_mod.DockerRuntime
    cmd_timeout = getattr(docker_mod, "CMD_TIMEOUT", 90)
    docker_path = getattr(docker_mod, "DOCKER_PATH", _R2E_DOCKER_PATH)

    class AteEnvRuntime(DockerRuntime):
        """A ``DockerRuntime`` bound to an ate-env environment the fleet acquired."""

        def __init__(self, handle: Any, *, command=("/bin/bash", "-l"), logger=None, **docker_kwargs):
            self._handle = handle
            ds = _dataset_row(handle)
            with _bypassing_kubernetes():
                super().__init__(
                    ds=ds, docker_image=handle.task.image, command=list(command),
                    logger=logger, backend="kubernetes-sandbox", **docker_kwargs,
                )
            # The base start_container swallows errors from the bind hook; a
            # half-built runtime is worse than an exception.
            if getattr(self, "container", None) is None or self.container_name != handle.sandbox_id:
                raise RuntimeError(f"failed to bind environment {handle.sandbox_id!r} into the R2E-Gym runtime")
            # Check docker_kwargs["environment"] now: inside run() a bad value
            # would come back as a command error, not as an exception.
            self._exec_env()

        # --- the four kubernetes-sandbox hooks ------------------------------
        def _start_kubernetes_sandbox(self) -> None:
            if not getattr(self._handle, "sandbox_id", None):
                raise RuntimeError("the fleet handle names no sandbox_id")
            self.sb_client = None
            self.custom_api = None
            self.container_name = self._handle.sandbox_id
            self.container = self._handle  # any truthy object: the base only tests it
            self.logger.info("Bound environment %s from the fleet handle", self.container_name)

        def _stop_kubernetes_sandbox(self) -> None:
            self.logger.debug("teardown is a no-op: the fleet releases environment %s", self.container_name)

        def _exec_env(self) -> dict[str, str]:
            # R2E-Gym's PATH, overridden by docker_kwargs["environment"]. This
            # follows DockerRuntime's kubernetes backends, because these hooks
            # replace exactly that backend: _start_kubernetes_pod (R2E-Gym
            # master) and _ensure_sandbox_template_exists (the
            # kubernetes-sandbox backend, tomergee/R2E-Gym
            # agentic-sandbox-integration at 93021bb) put the same mapping in
            # the pod env. Its docker backend instead passes PATH=DOCKER_PATH
            # on each exec_run call (run and demux_run), thus there DOCKER_PATH
            # always wins.
            extra = (getattr(self, "docker_kwargs", None) or {}).get("environment") or {}
            return {"PATH": docker_path, **_environment_dict(extra)}

        def _run_kubernetes(self, code, timeout=cmd_timeout, args="", workdir=""):
            command = f"{code} {args}".strip() if args else code
            try:
                # check=False: a non-zero exit or a timeout is a result here,
                # mapped below as R2E-Gym's backends map it; only transport
                # failures raise.
                res = self._handle.exec(
                    command, cwd=workdir or "", env=self._exec_env(), timeout_s=timeout, check=False
                )
            except Exception as e:  # transport: unreachable sandbox, gRPC error
                self.logger.error("exec in environment failed: %r", e)
                return f"Error executing command in environment: {e!r}", "-1"
            output = (res.stdout or "") + (res.stderr or "")
            if res.timed_out or res.exit_code is None or res.exit_code == 124:
                self.logger.error("command timed out after %ss", timeout)
                return f"The command took too long to execute (>{timeout}s)", "-1"
            if res.exit_code != 0:
                self.logger.error("exec error: exit code %s\n%s", res.exit_code, output)
                return output, f"Error: Exit code {res.exit_code}"
            return _ANSI.sub("", output), str(res.exit_code)

        def _copy_to_container_kubernetes(self, src_path, dest_path) -> None:
            dest_dir = os.path.dirname(dest_path) or "/"
            buf = io.BytesIO()
            # Members are owned by root, and the extract does not restore owners:
            # a sandbox root cannot chown to the host uid (tar exits 2).
            with tarfile.open(fileobj=buf, mode="w") as tar:
                tar.add(src_path, arcname=os.path.basename(dest_path), filter=_as_root)
            staged = f"/tmp/ate-env-copy-{uuid.uuid4().hex[:12]}.tar"
            self._handle.write_file(staged, buf.getvalue())
            cmd = (
                f"mkdir -p {shlex.quote(dest_dir)} && tar --no-same-owner -xmf {shlex.quote(staged)} "
                f"-C {shlex.quote(dest_dir)}; rc=$?; rm -f {shlex.quote(staged)}; exit $rc"
            )
            res = self._handle.exec(cmd, timeout_s=_COPY_TIMEOUT_S, check=False)
            if res.timed_out:
                # The guest killed the shell before its rm -f ran.
                with contextlib.suppress(Exception):
                    self._handle.exec(f"rm -f {shlex.quote(staged)}", timeout_s=30, check=False)
                raise RuntimeError(
                    f"copy {src_path} -> {dest_path} timed out after {_COPY_TIMEOUT_S} s"
                )
            if res.exit_code != 0:
                detail = (res.stderr or "").strip() or (res.stdout or "").strip()
                raise RuntimeError(
                    f"copy {src_path} -> {dest_path} failed (exit {res.exit_code}): {detail}"
                )

    class AteEnvRepoEnv(RepoEnv):
        """``RepoEnv`` whose runtime is an ``AteEnvRuntime``: no cold start."""

        def __init__(self, handle: Any, args: Any, *, logger=None, verbose=True,
                     step_timeout: int = 90, reward_timeout: int = 300):
            if logger is None:
                from r2egym.agenthub.utils.log import get_logger

                self.logger = get_logger("AteEnvRepoEnv")
            else:
                self.logger = logger
            if not verbose:
                self.logger.setLevel(logging.CRITICAL)
            self.runtime = AteEnvRuntime(handle, logger=self.logger)
            self.args = args
            self.done = False
            self.observation = None
            self.state = None
            self.commands = []
            from r2egym.agenthub.agent.commands import ParseCommandBash

            self.cmd_parser = ParseCommandBash()
            self.backend = "kubernetes-sandbox"
            self.step_timeout = step_timeout
            self.reward_timeout = reward_timeout
            self.logger.info("Initialized AteEnvRepoEnv: %s image: %s", self.runtime.repo_name, self.runtime.docker_image)

        def reset(self):
            """Soft reset: the binding is checked again, nothing is recreated. For
            a clean episode, release the handle and acquire a new one."""
            self.runtime.start_container(self.runtime.docker_image, self.runtime.command, self.runtime.container_name)
            if getattr(self.runtime, "container", None) is None:
                raise RuntimeError("AteEnvRepoEnv.reset: rebinding the environment failed")
            self.observation = "Environment reset"
            self.state = None
            self.done = False
            return self.observation

    with _BUILD_LOCK:
        if _CLASSES is None:
            _CLASSES = (AteEnvRuntime, AteEnvRepoEnv)
    return _CLASSES


def make_fleet_repo_env(handle: Any, *, command_files=None, verbose: bool = False,
                        step_timeout: int = 90, reward_timeout: int = 300):
    """An R2E-Gym ``RepoEnv`` on the environment ``handle`` holds.

    ``handle.task.metadata["ds"]`` must be the dataset row (the setup and the
    reward grading read it). ``command_files`` are R2E-Gym tool files copied
    into the environment (see ``r2egym_command_files``). ``env.close()`` is a
    no-op on the environment; the fleet releases it.
    """
    _, AteEnvRepoEnv = _build_classes()
    from r2egym.agenthub.environment.env import EnvArgs

    env = AteEnvRepoEnv(handle, EnvArgs(ds=_dataset_row(handle)), verbose=verbose,
                        step_timeout=step_timeout, reward_timeout=reward_timeout)
    if command_files:
        env.add_commands(list(command_files))
    return env


@contextlib.contextmanager
def fresh_eval_env(fleet: Any, handle: Any, **repo_env_kwargs: Any) -> Iterator[tuple[Any, Any]]:
    """A second environment from the same task image, for grading in a clean
    filesystem (the ``eval`` container of the MLPerf setup). Yields
    ``(eval_handle, repo_env)``; the handle is released on exit. Apply the
    agent's patch with ``repo_env.runtime.apply_patch(...)`` and grade with
    ``repo_env.compute_reward()``."""
    eval_handle = fleet.acquire(handle.task)
    try:
        env = make_fleet_repo_env(eval_handle, **repo_env_kwargs)
        try:
            yield eval_handle, env
        finally:
            env.close()
    finally:
        fleet.release(eval_handle)


def r2egym_command_files() -> list[str]:
    """R2E-Gym's default tool files for the ``r2egym`` scaffold, from the installed
    package."""
    try:
        import r2egym
    except ImportError as e:
        raise RuntimeError(f"r2egym_command_files {_HINT}") from e
    base = os.path.join(os.path.dirname(r2egym.__file__), "agenthub", "tools")
    return [
        os.path.join(base, "r2egym", "file_editor.py"),
        os.path.join(base, "search.py"),
        os.path.join(base, "r2egym", "execute_bash.py"),
        os.path.join(base, "finish.py"),
    ]


def __getattr__(name: str):
    """``AteEnvRuntime`` and ``AteEnvRepoEnv`` are built on first use."""
    if name in ("AteEnvRuntime", "AteEnvRepoEnv"):
        runtime_cls, env_cls = _build_classes()
        return runtime_cls if name == "AteEnvRuntime" else env_cls
    raise AttributeError(name)

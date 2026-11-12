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

"""An in-process fake ate-env-api for the Harbor provider tests.

The lifecycle, process and file services speak the real gRPC API over
grpc.aio, as the client's own fakes do. The guest side is pluggable:
``LocalExecutor`` runs commands on this machine (unit tests, with paths the
tests choose), ``DockerExecutor`` runs them in a container started from the
environment's image (the trial test), which is what the real guest sees: the
task image's own filesystem, ``/logs`` and ``/app`` included, as root.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import signal
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Protocol

import grpc
import grpc.aio
from ate_env_rl._gen.ateenv.v1alpha import (
    env_pb2,
    env_pb2_grpc,
    guest_pb2,
    guest_pb2_grpc,
)

from ate_env_harbor.environment import WHOAMI_COMMAND

CHUNK = 64 * 1024
DEFAULT_ATESPACE = "ate-env"
DEFAULT_TEMPLATE = "default-template"
# WarmImage's limits and default in internal/apiservice/placement.go.
WARM_DEFAULT_PROBES_PER_NODE = 2
WARM_MAX_PROBES_PER_NODE = 8
WARM_MAX_ROUNDS = 10
WARM_MAX_PROBES = 256
RUNNING = env_pb2.ENVIRONMENT_STATUS_RUNNING
SUSPENDED = env_pb2.ENVIRONMENT_STATUS_SUSPENDED


class Executor(Protocol):
    """Where a fake environment's commands and files go."""

    async def run(
        self, argv: list[str], cwd: str, env: dict[str, str], timeout: float | None
    ) -> tuple[int, bytes, bytes]: ...

    async def write(self, path: str, data: bytes, mode: int) -> None: ...

    async def read(self, path: str) -> bytes | None: ...

    async def pause(self) -> None: ...

    async def unpause(self) -> None: ...

    async def close(self) -> None: ...


async def _subprocess(
    argv: list[str],
    *,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    stdin: bytes | None = None,
    timeout: float | None = None,
) -> tuple[int, bytes, bytes]:
    # Its own session, so the deadline kills the whole process group the way
    # the guest does; killing only `sh` leaves a grandchild holding the pipes
    # (dash forks the command instead of exec'ing it) and wait() blocks on them.
    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=cwd,
        env=env,
        stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(stdin), timeout)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        await proc.wait()
        return 137, b"", b""  # what the guest reports after SIGKILL at the deadline
    return proc.returncode or 0, out, err


class LocalExecutor:
    """Runs commands on this machine. ``path`` is the PATH the commands see."""

    def __init__(self, root: Path, path: str | None = None) -> None:
        self.root = root
        self.path = path
        root.mkdir(parents=True, exist_ok=True)

    async def run(self, argv, cwd, env, timeout):
        workdir = cwd or str(self.root)
        if not os.path.isdir(workdir):
            return 1, b"", f"cwd {workdir}: no such directory\n".encode()
        child_env = {
            "PATH": self.path if self.path is not None else os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(self.root),
            **env,
        }
        # Popen resolves argv[0] against the child's PATH; resolve it here so a
        # restricted PATH (a test without sudo and su) still finds sh itself.
        argv = [shutil.which(argv[0]) or argv[0], *argv[1:]]
        return await _subprocess(argv, cwd=workdir, env=child_env, timeout=timeout)

    async def write(self, path, data, mode):
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        target.chmod(mode or 0o644)

    async def read(self, path):
        target = Path(path)
        return target.read_bytes() if target.is_file() else None

    async def pause(self):
        pass

    async def unpause(self):
        pass

    async def close(self):
        pass


class DockerExecutor:
    """Runs commands in a container started from the environment's image."""

    def __init__(self, image: str, docker: str = "docker") -> None:
        self.image = image
        self.docker = docker
        self.container: str | None = None

    async def start(self) -> None:
        code, out, err = await _subprocess(
            [
                self.docker, "run", "-d", "--label", "ate-env-harbor-fake=1",
                self.image, "sh", "-c", "while :; do sleep 3600; done",
            ],
            timeout=300,
        )
        if code != 0:
            raise RuntimeError(f"docker run {self.image} failed: {err.decode()}")
        self.container = out.decode().strip()

    async def run(self, argv, cwd, env, timeout):
        assert self.container
        cmd = [self.docker, "exec"]
        if cwd:
            cmd += ["-w", cwd]
        for key, value in env.items():
            cmd += ["-e", f"{key}={value}"]
        cmd.append(self.container)
        if timeout:
            cmd += ["timeout", "-s", "KILL", str(max(1, int(timeout)))]
        return await _subprocess(cmd + list(argv), timeout=(timeout or 0) + 60 or None)

    async def write(self, path, data, mode):
        assert self.container
        script = 'mkdir -p "$(dirname "$1")" && cat > "$1" && chmod "$2" "$1"'
        code, _, err = await _subprocess(
            [self.docker, "exec", "-i", self.container, "sh", "-c", script, "sh", path, f"{mode or 0o644:o}"],
            stdin=data,
            timeout=120,
        )
        if code != 0:
            raise RuntimeError(f"write {path}: {err.decode()}")

    async def read(self, path):
        assert self.container
        code, out, _ = await _subprocess(
            [self.docker, "exec", self.container, "cat", path], timeout=120
        )
        return out if code == 0 else None

    async def pause(self):
        await _subprocess([self.docker, "pause", self.container or ""], timeout=60)

    async def unpause(self):
        await _subprocess([self.docker, "unpause", self.container or ""], timeout=60)

    async def close(self):
        if self.container:
            await _subprocess([self.docker, "rm", "-f", self.container], timeout=120)
            self.container = None


@dataclass
class FakeState:
    """What the fake records and the knobs tests turn."""

    executor_factory: Callable[[str, str], Any]  # (image, env id) -> Executor
    whoami: str = "root"  # the user the fake guest reports for WHOAMI_COMMAND
    envs: dict[str, dict[str, Any]] = field(default_factory=dict)
    creates: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[str] = field(default_factory=list)
    suspends: list[str] = field(default_factory=list)
    execs: list[dict[str, Any]] = field(default_factory=list)
    writes: list[tuple[str, int, int]] = field(default_factory=list)
    procs: dict[str, dict[str, Any]] = field(default_factory=dict)
    missing_templates: set[str] = field(default_factory=set)
    create_code: grpc.StatusCode | None = None
    create_delay: float = 0.0  # answer the create this late, after the server registered it
    unavailable_calls: int = 0  # guest calls answered UNAVAILABLE first, as a cold actor
    fail_exec_containing: str | None = None  # the next exec with this text answers UNAVAILABLE
    required_token: str | None = None  # every call needs "authorization: Bearer <token>"
    wakes: int = 0
    counter: int = 0
    # Image templates, goldens and placement (the warm step). A derived
    # template is keyed by (base template, image). The golden of an ensured
    # template is PENDING for golden_ready_after status calls, then READY. An
    # image in golden_failures models a bake that fails after the ensure: the
    # ensure answers PENDING (a recreate, as the server does for a FAILED
    # template), the later status calls FAILED with the error.
    ensures: list[tuple[str, str]] = field(default_factory=list)
    status_polls: dict[tuple[str, str], int] = field(default_factory=dict)
    # GetImageStatus fails with these codes, one per call, before it counts a poll.
    status_transient: list[grpc.StatusCode] = field(default_factory=list)
    golden_ready_after: int = 1
    golden_failures: dict[str, str] = field(default_factory=dict)  # image -> golden_error
    goldens_disabled: bool = False  # the templates bake no golden: NONE with the template
    domains: list[dict[str, Any]] = field(
        default_factory=lambda: [
            {"name": "pool-a", "selector": {"pool": "a"}, "nodes": 2, "workers": 2},
            {"name": "pool-b", "selector": {"pool": "b"}, "nodes": 3, "workers": 3},
        ]
    )
    deny_list_workers: bool = False  # no global viewer role: no domain list, no node count
    warm_calls: list[dict[str, Any]] = field(default_factory=list)
    warm_runs: int = 0  # a run id per WarmImage call without one, as the server's random suffix
    warm_short: set[str] = field(default_factory=set)  # images whose warm ends one node short
    warm_undeleted: set[str] = field(default_factory=set)  # images whose last probe stays behind

    def env_pb(self, env_id: str) -> env_pb2.Environment:
        rec = self.envs[env_id]
        return env_pb2.Environment(
            id=env_id,
            atespace=rec["atespace"],
            template=env_pb2.Template(name=rec["template"], atespace=rec["atespace"]),
            status=rec["status"],
        )


async def _check_token(state: FakeState, context: grpc.aio.ServicerContext) -> None:
    if state.required_token is None:
        return
    md = {k: v for k, v in (context.invocation_metadata() or ())}
    if md.get("authorization") != f"Bearer {state.required_token}":
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, "missing or wrong bearer token")


async def _require_env(state: FakeState, context: grpc.aio.ServicerContext) -> dict[str, Any]:
    await _check_token(state, context)
    md = {k: v for k, v in (context.invocation_metadata() or ())}
    env_id = md.get("x-env-id", "")
    if not env_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "x-env-id header is required")
    rec = state.envs.get(env_id)
    if rec is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f'environment "{env_id}" not found')
    if state.unavailable_calls > 0:
        state.unavailable_calls -= 1
        await context.abort(grpc.StatusCode.UNAVAILABLE, f"actor {env_id} is not running")
    if rec["status"] == SUSPENDED:
        rec["status"] = RUNNING
        state.wakes += 1
        await rec["executor"].unpause()
    return rec


class FakeEnvironmentService(env_pb2_grpc.EnvironmentServiceServicer):
    def __init__(self, state: FakeState) -> None:
        self.state = state

    async def CreateEnvironment(self, request, context):
        s = self.state
        await _check_token(s, context)
        atespace = request.atespace or DEFAULT_ATESPACE
        template = DEFAULT_TEMPLATE
        if request.HasField("template") and request.template.name:
            template = request.template.name
        s.creates.append(
            {
                "id": request.id,
                "atespace": atespace,
                "template": template,
                "rootfs_image": request.rootfs_image,
                "worker_selector": dict(request.worker_selector),
            }
        )
        if s.create_code is not None:
            await context.abort(s.create_code, "create refused")
        if not request.id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
        if request.id in s.envs:
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, f'environment "{request.id}" already exists')
        image = request.rootfs_image
        if image and "@sha256:" not in image:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"image {image!r} is not pinned by digest"
            )
        if template in s.missing_templates:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f'base actor template "{template}" not found in atespace "{atespace}"; '
                "register it first (ate-env manifest template)",
            )
        derived = f"{template}-{image.split('@sha256:', 1)[1][:12]}" if image else template
        executor = s.executor_factory(image, request.id)
        if hasattr(executor, "start"):
            await executor.start()
        s.envs[request.id] = {
            "atespace": atespace,
            "template": derived,
            "status": RUNNING,
            "executor": executor,
            "image": image,
        }
        if s.create_delay:
            await asyncio.sleep(s.create_delay)
        return env_pb2.CreateEnvironmentResponse(environment=s.env_pb(request.id))

    async def GetEnvironment(self, request, context):
        await _check_token(self.state, context)
        if request.id not in self.state.envs:
            await context.abort(grpc.StatusCode.NOT_FOUND, f'environment "{request.id}" not found')
        return env_pb2.GetEnvironmentResponse(environment=self.state.env_pb(request.id))

    async def SuspendEnvironment(self, request, context):
        await _check_token(self.state, context)
        rec = self.state.envs.get(request.id)
        if rec is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f'environment "{request.id}" not found')
        self.state.suspends.append(request.id)
        rec["status"] = SUSPENDED
        await rec["executor"].pause()
        return env_pb2.SuspendEnvironmentResponse()

    async def DeleteEnvironment(self, request, context):
        await _check_token(self.state, context)
        self.state.deletes.append(request.id)
        rec = self.state.envs.pop(request.id, None)
        if rec is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f'environment "{request.id}" not found')
        await rec["executor"].close()
        return env_pb2.DeleteEnvironmentResponse()

    @staticmethod
    def _template_key(request) -> tuple[str, str, str]:
        """(base template, atespace, image) as apiservice's templateDefaults resolves them:
        the request's atespace first, then the template's, then the default."""
        base = DEFAULT_TEMPLATE
        template_atespace = ""
        if request.HasField("template"):
            base = request.template.name or base
            template_atespace = request.template.atespace
        return base, request.atespace or template_atespace or DEFAULT_ATESPACE, request.rootfs_image

    def _image_status(self, request) -> env_pb2.ImageStatus:
        s = self.state
        base, atespace, image = self._template_key(request)
        derived = f"{base}-{image.split('@sha256:', 1)[1][:12]}"
        polls = s.status_polls.get((base, image))
        if polls is None:
            golden = env_pb2.GOLDEN_SNAPSHOT_STATE_NONE
        elif s.goldens_disabled:
            golden = env_pb2.GOLDEN_SNAPSHOT_STATE_NONE
        elif polls == 0:
            golden = env_pb2.GOLDEN_SNAPSHOT_STATE_PENDING
        elif image in s.golden_failures:
            golden = env_pb2.GOLDEN_SNAPSHOT_STATE_FAILED
        elif polls >= s.golden_ready_after:
            golden = env_pb2.GOLDEN_SNAPSHOT_STATE_READY
        else:
            golden = env_pb2.GOLDEN_SNAPSHOT_STATE_PENDING
        ready = golden == env_pb2.GOLDEN_SNAPSHOT_STATE_READY
        failed = golden == env_pb2.GOLDEN_SNAPSHOT_STATE_FAILED
        return env_pb2.ImageStatus(
            rootfs_image=image,
            template=env_pb2.Template(name=derived, atespace=atespace),
            template_exists=polls is not None,
            golden=golden,
            golden_tag="golden-1" if ready else "",
            golden_error=s.golden_failures[image] if failed else "",
        )

    async def _require_digest(self, request, context) -> None:
        if "@sha256:" not in request.rootfs_image:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"image {request.rootfs_image!r} is not pinned by digest",
            )

    async def GetImageStatus(self, request, context):
        await _check_token(self.state, context)
        await self._require_digest(request, context)
        if self.state.status_transient:
            await context.abort(self.state.status_transient.pop(0), "the fake's transient failure")
        base, _, image = self._template_key(request)
        if (base, image) in self.state.status_polls:
            self.state.status_polls[(base, image)] += 1
        return self._image_status(request)

    async def EnsureImageTemplate(self, request, context):
        s = self.state
        await _check_token(s, context)
        await self._require_digest(request, context)
        base, atespace, image = self._template_key(request)
        if base in s.missing_templates:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f'base actor template "{base}" not found in atespace "{atespace}"; '
                "register it first (ate-env manifest template)",
            )
        s.ensures.append((base, image))
        s.status_polls[(base, image)] = 0  # a FAILED template is recreated: PENDING again
        return self._image_status(request)

    async def ListPlacementDomains(self, request, context):
        await _check_token(self.state, context)
        if self.state.deny_list_workers:
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                "listing workers takes the global viewer role (ate-env manifest access-policy --global)",
            )
        if request.HasField("template") and request.template.name in self.state.missing_templates:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f'template "{request.template.name}" not found'
            )
        return env_pb2.ListPlacementDomainsResponse(
            domains=[
                env_pb2.PlacementDomain(
                    name=d["name"],
                    worker_namespace=d.get("worker_namespace", "ate-system"),
                    selector=d["selector"],
                    nodes=d["nodes"],
                    workers=d["workers"],
                    node_slots=8,
                )
                for d in self.state.domains
            ]
        )

    async def WarmImage(self, request, context):
        s = self.state
        await _check_token(s, context)
        await self._require_digest(request, context)
        base, atespace, image = self._template_key(request)
        selector = dict(request.worker_selector)
        s.warm_calls.append(
            {
                "image": image,
                "selector": selector,
                "nodes": request.nodes,
                "probes_per_node": request.probes_per_node,
                "max_rounds": request.max_rounds,
                "probe_timeout": request.probe_timeout.ToTimedelta().total_seconds()
                if request.HasField("probe_timeout")
                else None,
                "run_id": request.run_id,
                "atespace": atespace,
                "template": base,
            }
        )
        # The checks in ate-env-api's order (internal/apiservice/placement.go):
        # parseWarmOptions, the template, the node count, the round's width.
        invalid = _warm_options_error(request)
        if invalid:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, invalid)
        if base in s.missing_templates:  # the server derives the template first
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f'base actor template "{base}" not found in atespace "{atespace}"; '
                "register it first (ate-env manifest template)",
            )
        if request.nodes == 0 and s.deny_list_workers:
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                "counting the nodes takes the global viewer role; pass nodes",
            )
        # The server ANDs the selector with the workers' labels: a pool
        # matches when its workers carry every label of the selector.
        pools = [
            d for d in s.domains if all(d["selector"].get(k) == v for k, v in selector.items())
        ]
        # With nodes given the server does not look for workers first: the
        # probes of a selector nothing carries fail, and the coverage is short.
        node_names = [f"{d['name']}-node{i}" for d in pools for i in range(d["nodes"])]
        if request.nodes == 0 and not node_names:  # domainNodes: nothing to count
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"no active worker matches the selector {selector}",
            )
        wanted = request.nodes or len(node_names)
        per_node = request.probes_per_node or WARM_DEFAULT_PROBES_PER_NODE
        if wanted * per_node > WARM_MAX_PROBES:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"{wanted} nodes x {per_node} probes per node exceed the {WARM_MAX_PROBES} "
                "probes a round may start",
            )
        covered = node_names[:wanted]
        short = image in s.warm_short
        if short:
            covered = covered[:-1]
        s.warm_runs += 1
        run = request.run_id or f"warm-fake{s.warm_runs}"
        for i, node in enumerate(covered):
            event = env_pb2.WarmImageEvent(
                probe=env_pb2.WarmProbe(id=f"{run}-{i}", node=node, round=1)
            )
            event.probe.duration.FromTimedelta(timedelta(seconds=0.5))
            yield event
        undeleted = [f"{run}-{len(covered) - 1}"] if image in s.warm_undeleted and covered else []
        yield env_pb2.WarmImageEvent(
            coverage=env_pb2.WarmImageCoverage(
                nodes=sorted(covered),
                wanted=wanted,
                probes=len(covered) + (1 if short else 0),
                failed=1 if short else 0,
                rounds=1,
                complete=len(covered) >= wanted,
                error="probe did not start: no slot on the last node" if short else "",
                undeleted_probes=undeleted,
            )
        )

    async def GetServerInfo(self, request, context):
        await _check_token(self.state, context)
        return env_pb2.GetServerInfoResponse(
            server="fake-ate-env-api",
            version="v0.0.0-fake",
            substrate_version="unknown",
            features=["rootfs_image", "pty"],
        )


def _warm_options_error(request) -> str:
    """ate-env-api's ``parseWarmOptions``: the message of the first limit ``request`` breaks."""
    if request.nodes < 0:
        return "nodes must be >= 0"
    if not 0 <= request.probes_per_node <= WARM_MAX_PROBES_PER_NODE:
        return f"probes_per_node must be between 1 and {WARM_MAX_PROBES_PER_NODE}"
    if not 0 <= request.max_rounds <= WARM_MAX_ROUNDS:
        return f"max_rounds must be between 1 and {WARM_MAX_ROUNDS}"
    if request.HasField("probe_timeout") and request.probe_timeout.ToTimedelta() <= timedelta(0):
        return "probe_timeout must be positive"
    return ""


class FakeProcessService(guest_pb2_grpc.ProcessServiceServicer):
    def __init__(self, state: FakeState) -> None:
        self.state = state

    def _pb(self, process_id: str, *, exited: bool) -> guest_pb2.Process:
        proc = self.state.procs[process_id]
        pb = guest_pb2.Process(
            process_id=process_id,
            command=proc["command"],
            pid=1000 + int(process_id.rsplit("-", 1)[1]),
            state=guest_pb2.PROCESS_STATE_EXITED if exited else guest_pb2.PROCESS_STATE_RUNNING,
        )
        pb.started_at.GetCurrentTime()
        if exited:
            pb.finished_at.GetCurrentTime()
            pb.exit_code = proc["code"]
        return pb

    async def StartProcess(self, request, context):
        s = self.state
        rec = await _require_env(s, context)
        command = list(request.command)
        if not command:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "command cannot be empty")
        timeout = request.timeout.ToTimedelta().total_seconds() if request.HasField("timeout") else None
        if s.fail_exec_containing and s.fail_exec_containing in command[-1]:
            s.fail_exec_containing = None
            await context.abort(grpc.StatusCode.UNAVAILABLE, "transport failed mid-command")
        if command == ["sh", "-c", WHOAMI_COMMAND]:
            code, out, err = 0, (s.whoami + "\n").encode(), b""
        else:
            code, out, err = await rec["executor"].run(command, request.cwd, dict(request.env), timeout)
        s.counter += 1
        process_id = f"proc-{s.counter}"
        s.procs[process_id] = {
            "command": command,
            "cwd": request.cwd,
            "env": dict(request.env),
            "timeout": timeout,
            "code": code,
            "out": out,
            "err": err,
        }
        s.execs.append({"env_id": context.invocation_metadata() and dict(context.invocation_metadata()).get("x-env-id"), **s.procs[process_id]})
        return self._pb(process_id, exited=False)

    async def GetProcess(self, request, context):
        await _require_env(self.state, context)
        if request.process_id not in self.state.procs:
            await context.abort(grpc.StatusCode.NOT_FOUND, f'process "{request.process_id}" not found')
        return self._pb(request.process_id, exited=True)

    async def StreamProcessOutput(self, request, context):
        await _require_env(self.state, context)
        proc = self.state.procs.get(request.process_id)
        if proc is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f'process "{request.process_id}" not found')
        for i in range(0, len(proc["out"]), CHUNK):
            yield guest_pb2.ProcessOutput(stdout=proc["out"][i : i + CHUNK])
        for i in range(0, len(proc["err"]), CHUNK):
            yield guest_pb2.ProcessOutput(stderr=proc["err"][i : i + CHUNK])
        yield guest_pb2.ProcessOutput(exit=self._pb(request.process_id, exited=True))

    async def WriteProcessInput(self, request_iterator, context):
        await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "the fake does not take stdin")

    async def SignalProcess(self, request, context):
        await _require_env(self.state, context)
        return self._pb(request.process_id, exited=True)


class FakeFileSystemService(guest_pb2_grpc.FileSystemServiceServicer):
    def __init__(self, state: FakeState) -> None:
        self.state = state

    async def ReadFile(self, request, context):
        rec = await _require_env(self.state, context)
        data = await rec["executor"].read(request.path)
        if data is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"open {request.path}: no such file or directory"
            )
        for i in range(0, len(data), CHUNK):
            yield guest_pb2.ReadFileResponse(chunk=data[i : i + CHUNK])

    async def WriteFile(self, request_iterator, context):
        rec = await _require_env(self.state, context)
        path, mode, chunks = None, 0, []
        async for request in request_iterator:
            if path is None:
                if not request.path:
                    await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "path is required")
                if request.seek_offset:
                    await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "the fake writes whole files")
                path, mode = request.path, request.mode
            chunks.append(request.chunk)
        if path is None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "path is required")
        data = b"".join(chunks)
        await rec["executor"].write(path, data, mode)
        self.state.writes.append((path, len(data), mode))
        return guest_pb2.WriteFileResponse(bytes_written=len(data))


@contextlib.asynccontextmanager
async def serve(state: FakeState) -> AsyncIterator[str]:
    """Serve the fake on 127.0.0.1; yields ``host:port``; closes what is left."""
    server = grpc.aio.server()
    env_pb2_grpc.add_EnvironmentServiceServicer_to_server(FakeEnvironmentService(state), server)
    guest_pb2_grpc.add_ProcessServiceServicer_to_server(FakeProcessService(state), server)
    guest_pb2_grpc.add_FileSystemServiceServicer_to_server(FakeFileSystemService(state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        yield f"127.0.0.1:{port}"
    finally:
        await server.stop(None)
        for rec in list(state.envs.values()):
            with contextlib.suppress(Exception):
                await rec["executor"].close()

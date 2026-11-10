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

"""In-process grpc.aio fake servicers mirroring the real server semantics.

The metadata contract and error codes/messages replicate
internal/apiservice/server.go and the guest services, so the client is
exercised against realistic behavior without a cluster.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import timedelta

import grpc

from ate_env_rl._gen.ateenv.v1alpha import (
    env_pb2,
    env_pb2_grpc,
    guest_pb2,
    guest_pb2_grpc,
)

CHUNK_SIZE = 64 * 1024


async def _require_env(context: grpc.aio.ServicerContext) -> tuple[str, str]:
    """Extract (env_id, atespace) metadata like the API server's proxy does."""
    md = {key: value for key, value in (context.invocation_metadata() or ())}
    env_id = md.get("x-env-id", "")
    if not env_id:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "x-env-id header is required")
    return env_id, md.get("x-env-atespace", "ate-env")


class FakeEnvironmentService(env_pb2_grpc.EnvironmentServiceServicer):
    def __init__(self):
        self.image_templates: dict = {}
        # Base templates ListPlacementDomains can filter by: (atespace, name)
        # to the template's own worker selector. The default template has
        # none, so it addresses every pool with a selector.
        self.template_selectors: dict[tuple[str, str], dict[str, str]] = {
            ("ate-env", "default-template"): {},
        }
        self.environments: dict[tuple[str, str], env_pb2.Environment] = {}
        self.last_create_request: env_pb2.CreateEnvironmentRequest | None = None
        self.last_suspend_request: env_pb2.SuspendEnvironmentRequest | None = None
        # Placement, for creates, ListPlacementDomains and WarmImage: the nodes
        # behind each value of the ate-env/domain selector label, the slots per node
        # per domain, whether the global role is missing, whether every probe
        # lands on the first node, and the WarmImage requests seen.
        self.domain_nodes: dict[str, list[str]] = {}
        self._domain_counter: dict[str, int] = {}
        self.node_slots: dict[str, int] = {}
        self.deny_list_workers = False
        self.warm_same_node = False
        self.warm_requests: list[env_pb2.WarmImageRequest] = []
        # Probe ids the next WarmImage reports undeleted, and the seconds it
        # reports waited for probe slots.
        self.warm_undeleted: list[str] = []
        self.warm_waited = 0.0
        # An environment created without a known domain reports no node, as
        # before its placement; creates hand the domain's nodes out round-robin.
        self.first_node_creates = 0

    async def CreateEnvironment(self, request, context):
        self.last_create_request = request
        atespace = request.atespace or "ate-env"
        key = (atespace, request.id)
        if not request.id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "id is required")
        if key in self.environments:
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                f'environment "{request.id}" already exists',
            )
        template = env_pb2.Template(name="default-template", atespace="ate-env")
        if request.HasField("template"):
            if request.template.name:
                template.name = request.template.name
            if request.template.atespace:
                template.atespace = request.template.atespace
        if request.rootfs_image:
            # Mirror ate-env-api: the template becomes the base of one derived
            # per image, named after the digest.
            if "@sha256:" not in request.rootfs_image:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "image is not pinned by digest")
            digest = request.rootfs_image.split("@sha256:", 1)[1]
            template.name = f"{template.name}-{digest[:12]}"
        environment = env_pb2.Environment(
            id=request.id,
            atespace=atespace,
            template=template,
            status=env_pb2.ENVIRONMENT_STATUS_RUNNING,
        )
        environment.worker_selector.update(request.worker_selector)
        domain = request.worker_selector.get("ate-env/domain", "")
        if nodes := self.domain_nodes.get(domain):
            n = self._domain_counter.get(domain, 0)
            # The first first_node_creates creates of a domain all land on its
            # first node, as a scheduler that favours a warm node does; the
            # rest go round-robin.
            environment.node = nodes[0 if n < self.first_node_creates else n % len(nodes)]
            self._domain_counter[domain] = n + 1
        self.environments[key] = environment
        return env_pb2.CreateEnvironmentResponse(environment=environment)

    async def GetEnvironment(self, request, context):
        environment = await self._lookup(request, context)
        return env_pb2.GetEnvironmentResponse(environment=environment)

    async def SuspendEnvironment(self, request, context):
        environment = await self._lookup(request, context)
        self.last_suspend_request = request
        # Mirror ate-env-api: an unknown mode is INVALID_ARGUMENT; a suspended
        # or paused environment is left as it is, because Substrate's
        # SuspendActor returns at once on it; a local suspend of a running one
        # is PAUSED until its upload lands, a durable one SUSPENDED.
        if request.mode not in (
            env_pb2.SUSPEND_MODE_UNSPECIFIED,
            env_pb2.SUSPEND_MODE_DURABLE,
            env_pb2.SUSPEND_MODE_LOCAL,
        ):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"mode {request.mode} is not a SuspendMode"
            )
        if environment.status in (
            env_pb2.ENVIRONMENT_STATUS_SUSPENDED,
            env_pb2.ENVIRONMENT_STATUS_PAUSED,
        ):
            return env_pb2.SuspendEnvironmentResponse()
        if request.mode == env_pb2.SUSPEND_MODE_LOCAL:
            environment.status = env_pb2.ENVIRONMENT_STATUS_PAUSED
        else:
            environment.status = env_pb2.ENVIRONMENT_STATUS_SUSPENDED
        return env_pb2.SuspendEnvironmentResponse()

    async def DeleteEnvironment(self, request, context):
        await self._lookup(request, context)
        del self.environments[(request.atespace or "ate-env", request.id)]
        return env_pb2.DeleteEnvironmentResponse()

    def _image_status(self, request):
        template = env_pb2.Template(name="default-template", atespace="ate-env")
        if request.HasField("template"):
            if request.template.name:
                template.name = request.template.name
            if request.template.atespace:
                template.atespace = request.template.atespace
        atespace = request.atespace or template.atespace
        if not re.search(r"@sha256:[0-9a-f]{64}$", request.rootfs_image):
            return None, atespace, template
        digest = request.rootfs_image.split("@sha256:", 1)[1]
        derived = env_pb2.Template(name=f"{template.name}-{digest[:12]}", atespace=atespace)
        return (atespace, derived.name), atespace, derived

    async def GetImageStatus(self, request, context):
        key, atespace, derived = self._image_status(request)
        if key is None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "image is not pinned by digest")
        golden = self.image_templates.get(key)
        if golden is None:
            return env_pb2.ImageStatus(
                rootfs_image=request.rootfs_image, template=derived, template_exists=False,
                golden=env_pb2.GOLDEN_SNAPSHOT_STATE_NONE,
            )
        return env_pb2.ImageStatus(
            rootfs_image=request.rootfs_image, template=derived, template_exists=True,
            golden=golden.golden, golden_tag=golden.golden_tag, golden_error=golden.golden_error,
        )

    async def EnsureImageTemplate(self, request, context):
        key, _, _ = self._image_status(request)
        if key is None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "image is not pinned by digest")
        # Mirror ate-env-api: a missing template is created and bakes; a
        # template whose bake failed is recreated and bakes again.
        existing = self.image_templates.get(key)
        if existing is None or existing.golden == env_pb2.GOLDEN_SNAPSHOT_STATE_FAILED:
            self.image_templates[key] = env_pb2.ImageStatus(golden=env_pb2.GOLDEN_SNAPSHOT_STATE_PENDING)
        return await self.GetImageStatus(request, context)

    async def ListPlacementDomains(self, request, context):
        # Mirror ate-env-api: one domain per value of the ate-env/domain label
        # with the nodes behind it; PermissionDenied without the global role.
        selector = None
        if request.HasField("template") or request.atespace:
            name = request.template.name or "default-template"
            atespace = request.atespace or request.template.atespace or "ate-env"
            if (atespace, name) not in self.template_selectors:
                await context.abort(grpc.StatusCode.NOT_FOUND, f"actor template {name!r} not found")
            selector = self.template_selectors[(atespace, name)]
        if self.deny_list_workers:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "not allowed to list workers")
        resp = env_pb2.ListPlacementDomainsResponse()
        for name in sorted(self.domain_nodes):
            labels = {"ate-env/domain": name}
            if selector is not None and any(labels.get(k) != v for k, v in selector.items()):
                continue
            d = resp.domains.add(
                name=f"pool-{name}", worker_namespace="ate-system",
                nodes=len(set(self.domain_nodes[name])), workers=len(self.domain_nodes[name]),
                node_slots=self.node_slots.get(name, 0),
            )
            d.selector["ate-env/domain"] = name
        return resp

    async def WarmImage(self, request, context):
        # Mirror ate-env-api: rounds of nodes x probes_per_node probes placed
        # round-robin on the domain's nodes (always the first with
        # warm_same_node), deleted after each round; a probe event each, then
        # the coverage.
        if not re.search(r"@sha256:[0-9a-f]{64}$", request.rootfs_image):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "image is not pinned by digest")
        domain = request.worker_selector.get("ate-env/domain", "")
        pool = self.domain_nodes.get(domain)
        wanted = request.nodes
        if wanted == 0:
            if self.deny_list_workers:
                await context.abort(
                    grpc.StatusCode.PERMISSION_DENIED,
                    "nodes is 0 and ate-env-api may not list workers: pass nodes",
                )
            if not pool:
                await context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"no active worker matches the selector {dict(request.worker_selector)}",
                )
            wanted = len(set(pool))
        per_node = request.probes_per_node or 2
        rounds = request.max_rounds or 3
        run_id = request.run_id or "warm"
        covered: set[str] = set()
        probes = failed = 0
        error = ""
        rnd = 0
        self.warm_requests.append(request)
        while rnd < rounds and len(covered) < wanted:
            rnd += 1
            for k in range(wanted * per_node):
                probes += 1
                probe_id = f"{run_id}-{rnd}-{k}"
                node = ""
                if pool:
                    idx = 0 if self.warm_same_node else self._domain_counter.get(domain, 0)
                    self._domain_counter[domain] = idx + 1
                    node = pool[idx % len(pool)]
                    covered.add(node)
                else:
                    failed += 1
                    error = "running but the control plane reports no node"
                probe = env_pb2.WarmProbe(id=probe_id, node=node, round=rnd, error=error if not node else "")
                probe.duration.FromMilliseconds(5)
                yield env_pb2.WarmImageEvent(probe=probe)
            if failed == probes:
                break
        coverage = env_pb2.WarmImageCoverage(
            nodes=sorted(covered), wanted=wanted, probes=probes, failed=failed, rounds=rnd,
            complete=len(covered) >= wanted, error=error, undeleted_probes=list(self.warm_undeleted),
        )
        if self.warm_waited:
            coverage.waited.FromTimedelta(timedelta(seconds=self.warm_waited))
        yield env_pb2.WarmImageEvent(coverage=coverage)

    async def GetServerInfo(self, request, context):
        return env_pb2.GetServerInfoResponse(
            server="sandboxenv-rl",
            version="v0.0.0-fake",
            substrate_version="unknown",
            features=["rootfs_image", "pty", "suspend_mode", "image_status", "placement_domains", "warm_image"],
        )

    async def _lookup(self, request, context):
        key = (request.atespace or "ate-env", request.id)
        environment = self.environments.get(key)
        if environment is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f'environment "{request.id}" not found'
            )
        return environment


@dataclass
class FakeProc:
    """Scripted behavior for one process started against the fake.

    chunks are (field, data) pairs where field is "stdout" or "stderr";
    they are replayed by StreamProcessOutput. exit_code/signal describe the
    final state. running_polls is the number of GetProcess calls that still
    report RUNNING before the process is considered exited; a follow stream
    exits it immediately once the chunks are replayed. echo_stdin appends
    written input as stdout, like cat.
    """

    chunks: list[tuple[str, bytes]] = field(default_factory=list)
    exit_code: int = 0
    signal: int = 0
    running_polls: int = 0
    echo_stdin: bool = False
    hang_follow: bool = False  # a follow stream never ends (process never exits)
    timed_out: bool = False
    # request capture, filled by StartProcess / WriteProcessInput / SignalProcess:
    command: list[str] = field(default_factory=list)
    cwd: str = ""
    env: dict[str, str] = field(default_factory=dict)
    stdin: bool = False
    timeout_seconds: float | None = None
    stdin_data: bytearray = field(default_factory=bytearray)
    stdin_closed: bool = False
    signals: list[int] = field(default_factory=list)
    exited: bool = False


async def _start_failure(context, failure):
    """Answer like ate-env-api for an environment that cannot start: the
    ate-env-start-failure trailer, then the status. failure is
    (grpc.StatusCode, message, reason)."""
    code, message, reason = failure
    context.set_trailing_metadata((("ate-env-start-failure", reason),))
    await context.abort(code, message)


class FakeProcessService(guest_pb2_grpc.ProcessServiceServicer):
    def __init__(self):
        # (code, message, reason): fail every call as an environment that
        # cannot start (see _start_failure).
        self.start_failure: tuple[grpc.StatusCode, str, str] | None = None
        self.next_procs: list[FakeProc] = []  # scripts for upcoming StartProcess calls
        self.procs: dict[str, FakeProc] = {}
        self.last_env: tuple[str, str] | None = None
        self._counter = 0

    def _to_pb(self, process_id: str, proc: FakeProc, *, exited: bool) -> guest_pb2.Process:
        pb = guest_pb2.Process(
            process_id=process_id,
            command=proc.command,
            pid=1000 + int(process_id.rsplit("-", 1)[1]),
            state=guest_pb2.PROCESS_STATE_EXITED if exited else guest_pb2.PROCESS_STATE_RUNNING,
        )
        pb.started_at.GetCurrentTime()
        if exited:
            pb.finished_at.GetCurrentTime()
            pb.exit_code = 128 + proc.signal if proc.signal else proc.exit_code
            pb.timed_out = proc.timed_out
        return pb

    async def StartProcess(self, request, context):
        self.last_env = await _require_env(context)
        if self.start_failure:
            await _start_failure(context, self.start_failure)
        if not request.command:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "command cannot be empty")
        proc = self.next_procs.pop(0) if self.next_procs else FakeProc()
        proc.command = list(request.command)
        proc.cwd = request.cwd
        proc.env = dict(request.env)
        proc.stdin = request.stdin
        if request.HasField("timeout"):
            proc.timeout_seconds = request.timeout.ToTimedelta().total_seconds()
        self._counter += 1
        process_id = f"proc-{self._counter}"
        self.procs[process_id] = proc
        return self._to_pb(process_id, proc, exited=False)

    async def GetProcess(self, request, context):
        self.last_env = await _require_env(context)
        proc = await self._lookup(request.process_id, context)
        if not proc.exited and proc.running_polls > 0:
            proc.running_polls -= 1
            return self._to_pb(request.process_id, proc, exited=False)
        proc.exited = True
        return self._to_pb(request.process_id, proc, exited=True)

    async def StreamProcessOutput(self, request, context):
        self.last_env = await _require_env(context)
        proc = await self._lookup(request.process_id, context)
        stdout_skip = request.stdout_offset
        stderr_skip = request.stderr_offset
        for source, data in proc.chunks:
            if source == "stdout":
                data, stdout_skip = data[stdout_skip:], max(0, stdout_skip - len(data))
                if data:
                    yield guest_pb2.ProcessOutput(stdout=data)
            else:
                data, stderr_skip = data[stderr_skip:], max(0, stderr_skip - len(data))
                if data:
                    yield guest_pb2.ProcessOutput(stderr=data)
        if request.follow and proc.hang_follow:
            await asyncio.Event().wait()
        if request.follow:
            # Simulate a live process: yield to the loop between chunks and exit.
            for _ in range(proc.running_polls):
                await asyncio.sleep(0)
            proc.running_polls = 0
            proc.exited = True
        if proc.exited or proc.running_polls == 0:
            proc.exited = True
            yield guest_pb2.ProcessOutput(exit=self._to_pb(request.process_id, proc, exited=True))

    async def WriteProcessInput(self, request_iterator, context):
        self.last_env = await _require_env(context)
        proc = None
        process_id = ""
        written = 0
        async for request in request_iterator:
            if proc is None:
                process_id = request.process_id
                if not process_id:
                    await context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "process_id is required on the first message",
                    )
                proc = await self._lookup(process_id, context)
                if not proc.stdin:
                    await context.abort(
                        grpc.StatusCode.FAILED_PRECONDITION,
                        f'process "{process_id}" was started without stdin',
                    )
            if proc.exited:
                await context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION, f'process "{process_id}" has exited'
                )
            if request.data and proc.stdin_closed:
                await context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f'stdin of process "{process_id}" is closed',
                )
            proc.stdin_data.extend(request.data)
            written += len(request.data)
            if request.data and proc.echo_stdin:
                proc.chunks.append(("stdout", bytes(request.data)))
            if request.close:
                proc.stdin_closed = True
        if proc is None:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "process_id is required on the first message"
            )
        return guest_pb2.WriteProcessInputResponse(bytes_written=written)

    async def SignalProcess(self, request, context):
        self.last_env = await _require_env(context)
        proc = await self._lookup(request.process_id, context)
        if request.signal == guest_pb2.SIGNAL_UNSPECIFIED:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unsupported signal")
        if proc.exited:
            await context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f'process "{request.process_id}" has exited',
            )
        proc.signals.append(request.signal)
        if request.signal in (guest_pb2.SIGNAL_KILL, guest_pb2.SIGNAL_TERM):
            proc.signal = request.signal
            proc.running_polls = 0
        return self._to_pb(request.process_id, proc, exited=False)

    async def _lookup(self, process_id, context):
        proc = self.procs.get(process_id)
        if proc is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f'process "{process_id}" not found'
            )
        return proc


class FakeFileSystemService(guest_pb2_grpc.FileSystemServiceServicer):
    def __init__(self):
        self.start_failure: tuple[grpc.StatusCode, str, str] | None = None
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
        self.last_env: tuple[str, str] | None = None
        self.last_write_chunk_sizes: list[int] = []

    async def _validate_path(self, path, context):
        # Mirrors guest/filesystem resolveAndValidatePath's sandbox check.
        if path.startswith(".."):
            await context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f'access denied: path "{path}" is outside sandbox root directory "/workspace"',
            )

    async def ReadFile(self, request, context):
        self.last_env = await _require_env(context)
        if self.start_failure:
            await _start_failure(context, self.start_failure)
        await self._validate_path(request.path, context)
        data = self.files.get(request.path)
        if data is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"open {request.path}: no such file or directory",
            )
        for i in range(0, len(data), CHUNK_SIZE):
            yield guest_pb2.ReadFileResponse(chunk=data[i : i + CHUNK_SIZE])

    async def WriteFile(self, request_iterator, context):
        self.last_env = await _require_env(context)
        path = None
        mode = 0
        buf = bytearray()
        chunk_sizes = []
        async for request in request_iterator:
            if path is None:
                # ate-env-api wakes the environment once the first message
                # arrives, so a start failure answers after it.
                if self.start_failure:
                    await _start_failure(context, self.start_failure)
                if not request.path:
                    await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "path is required")
                await self._validate_path(request.path, context)
                path = request.path
                mode = request.mode
                if request.seek_offset < 0:
                    await context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "seek_offset cannot be negative"
                    )
                if request.seek_offset > 0:
                    # Keep existing content, zero-fill up to the offset.
                    buf = bytearray(self.files.get(path, b""))
                    buf.extend(b"\0" * max(0, request.seek_offset - len(buf)))
                    pos = request.seek_offset
                else:
                    pos = 0
            buf[pos : pos + len(request.chunk)] = request.chunk
            pos += len(request.chunk)
            chunk_sizes.append(len(request.chunk))
        self.last_write_chunk_sizes = chunk_sizes
        if path is None:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "path is required")
        self.files[path] = bytes(buf)
        self.modes.setdefault(path, mode)
        return guest_pb2.WriteFileResponse(bytes_written=sum(chunk_sizes))

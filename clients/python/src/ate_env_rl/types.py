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

"""Public dataclasses and enums mirroring the ateenv.v1alpha proto types.

Generated protobuf classes stay out of the public API; the raw stubs remain
reachable under ate_env_rl._gen for callers that need them.
"""

from __future__ import annotations

import enum
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from ._gen.ateenv.v1alpha import env_pb2, guest_pb2

__all__ = [
    "EnvironmentStatus",
    "SuspendMode",
    "ProcessState",
    "Signal",
    "Template",
    "EnvironmentInfo",
    "ProcessInfo",
    "ProcessOutput",
    "ShellResult",
    "ResourceLimits",
    "PlacementSpec",
    "EnvironmentSpec",
    "Task",
    "ExecResult",
    "DataPlaneEndpoint",
    "RawSandboxInstance",
    "PlanEntry",
    "FleetPlan",
]



class EnvironmentStatus(enum.IntEnum):
    """Lifecycle status of an environment (ateenv.v1alpha.EnvironmentStatus)."""

    UNSPECIFIED = 0
    RESUMING = 1
    RUNNING = 2
    SUSPENDING = 3
    SUSPENDED = 4
    PAUSING = 5
    PAUSED = 6
    CRASHED = 7
    DELETING = 8


class SuspendMode(enum.IntEnum):
    """How ``Client.suspend()`` treats the snapshot (ateenv.v1alpha.SuspendMode).

    ``DURABLE`` returns once the snapshot is in object storage: the
    environment survives the loss of its node and resumes on any node.
    ``LOCAL`` returns once the snapshot is on the worker's node and leaves the
    upload in the background: until it lands the environment resumes on that
    node only, its status is ``PAUSED``, and a node lost before then takes the
    snapshot with it. One park-and-resume cycle is cheaper with ``LOCAL``,
    which is what a loop that parks a sandbox between every command wants.
    """

    UNSPECIFIED = 0
    DURABLE = 1
    LOCAL = 2


class ProcessState(enum.IntEnum):
    """Lifecycle state of a process (ateenv.v1alpha.ProcessState)."""

    UNSPECIFIED = 0
    RUNNING = 1
    EXITED = 2


class Signal(enum.IntEnum):
    """POSIX signal deliverable with Process.signal(); values match Linux signal numbers."""

    HUP = 1
    INT = 2
    QUIT = 3
    ILL = 4
    TRAP = 5
    ABRT = 6
    BUS = 7
    FPE = 8
    KILL = 9
    USR1 = 10
    SEGV = 11
    USR2 = 12
    PIPE = 13
    ALRM = 14
    TERM = 15
    CHLD = 17
    CONT = 18
    STOP = 19
    TSTP = 20
    TTIN = 21
    TTOU = 22
    URG = 23
    XCPU = 24
    XFSZ = 25
    VTALRM = 26
    PROF = 27
    WINCH = 28
    IO = 29
    SYS = 31


@dataclass(frozen=True)
class Template:
    """ActorTemplate an environment is instantiated from."""

    name: str
    atespace: str


@dataclass(frozen=True)
class EnvironmentInfo:
    """Details and status of an environment."""

    id: str
    atespace: str
    template: Template | None
    status: EnvironmentStatus
    # Kubernetes node the environment runs on, or last ran on; "" before its
    # first placement.
    node: str = ""
    # The per-environment worker selector it was created with; {} when none.
    # Left out of the hash so EnvironmentInfo stays hashable, as before.
    worker_selector: Mapping[str, str] = field(default_factory=dict, hash=False)


class GoldenSnapshotState(enum.IntEnum):
    """State of a derived template's golden snapshot."""

    UNSPECIFIED = env_pb2.GOLDEN_SNAPSHOT_STATE_UNSPECIFIED
    NONE = env_pb2.GOLDEN_SNAPSHOT_STATE_NONE
    PENDING = env_pb2.GOLDEN_SNAPSHOT_STATE_PENDING
    READY = env_pb2.GOLDEN_SNAPSHOT_STATE_READY
    FAILED = env_pb2.GOLDEN_SNAPSHOT_STATE_FAILED


@dataclass(frozen=True)
class ImageStatus:
    """The template derived for a rootfs image and its golden snapshot's state.

    ``golden`` is NONE while no template exists, PENDING while the golden
    bakes (environments created now boot from the image), READY once it is
    tagged, FAILED when the bake gave up (``golden_error`` says why).
    """

    rootfs_image: str
    template: Template
    template_exists: bool
    golden: GoldenSnapshotState
    golden_tag: str = ""
    golden_error: str = ""


@dataclass(frozen=True)
class PlacementDomain:
    """A worker pool as a per-environment worker selector addresses it.

    ``selector`` is the labels the pool's workers carry: a ``worker_selector``
    equal to them keeps an environment in the pool. ``nodes`` and ``workers``
    count the active ones; ``node_slots`` is the environments one node runs
    at once, 0 when unknown.

    An ``ImagePlacement`` plan's domains are this type too: the plan file
    carries ``name``, ``selector``, ``nodes`` and ``node_slots``, thus
    ``worker_namespace`` and ``workers`` keep their defaults there.
    """

    name: str
    worker_namespace: str = ""
    selector: Mapping[str, str] = field(default_factory=dict, hash=False)
    nodes: int = 0
    workers: int = 0
    node_slots: int = 0


@dataclass(frozen=True)
class WarmProbe:
    """One probe environment of a ``warm_image`` call: created, run, node read, deleted."""

    id: str
    node: str
    round: int
    duration: float  # seconds
    error: str = ""


@dataclass(frozen=True)
class WarmImageCoverage:
    """What a ``warm_image`` call covered.

    ``complete`` is True when ``nodes`` reached ``wanted``; ``error`` is the
    last probe error when the call stopped short. ``undeleted_probes`` are the
    probe environments the server could not delete (the delete failed after
    its retries, or the probe ended CRASHED, which Substrate cannot delete
    today): ids in the call's atespace, for the caller to sweep with
    ``delete()``. ``waited`` is the time, in seconds, the rounds waited for
    probe slots on the worker selector held by other ``warm_image`` calls.
    """

    nodes: tuple[str, ...]
    wanted: int
    probes: int
    failed: int
    rounds: int
    complete: bool
    error: str = ""
    undeleted_probes: tuple[str, ...] = ()
    waited: float = 0.0


@dataclass(frozen=True)
class ServerInfo:
    """Identity, version and optional features of the ate-env-api server.

    Check a feature with ``"pty" in info.features`` before depending on it;
    names this client does not know can appear and are to be ignored.
    """

    server: str  # "sandboxenv-rl" for this repository's ate-env-api
    version: str
    substrate_version: str  # the go.ate.dev/substrate module version, or "unknown"
    features: tuple[str, ...]


@dataclass(frozen=True)
class ProcessInfo:
    """Identity, lifecycle state, and exit status of a process."""

    process_id: str
    command: tuple[str, ...]
    pid: int
    state: ProcessState
    exit_code: int  # valid once EXITED; 128 + signal number if killed by a signal
    started_at: datetime | None
    finished_at: datetime | None
    timed_out: bool = False

    @property
    def running(self) -> bool:
        """True until the process has exited."""
        return self.state == ProcessState.RUNNING


@dataclass(frozen=True)
class ProcessOutput:
    """One message from a process output stream; exactly one field is set.

    exit is the final message: the process has exited and all output was
    delivered. It is absent if the stream ends while the process runs.
    """

    stdout: bytes | None = None
    stderr: bytes | None = None
    exit: ProcessInfo | None = None


@dataclass(frozen=True)
class ShellResult:
    """Captured output and exit status of a shell command."""

    stdout: str
    stderr: str
    exit_code: int  # 128 + signal number if killed by a signal
    timed_out: bool = False


def _template_from_pb(pb: env_pb2.Template) -> Template:
    return Template(name=pb.name, atespace=pb.atespace)


def _environment_info_from_pb(pb: env_pb2.Environment) -> EnvironmentInfo:
    template = _template_from_pb(pb.template) if pb.HasField("template") else None
    return EnvironmentInfo(
        id=pb.id,
        atespace=pb.atespace,
        template=template,
        status=EnvironmentStatus(pb.status),
        node=pb.node,
        worker_selector=dict(pb.worker_selector),
    )


def _image_status_from_pb(pb: env_pb2.ImageStatus) -> ImageStatus:
    return ImageStatus(
        rootfs_image=pb.rootfs_image,
        template=_template_from_pb(pb.template),
        template_exists=pb.template_exists,
        golden=GoldenSnapshotState(pb.golden),
        golden_tag=pb.golden_tag,
        golden_error=pb.golden_error,
    )


def _server_info_from_pb(pb: env_pb2.GetServerInfoResponse) -> ServerInfo:
    return ServerInfo(
        server=pb.server,
        version=pb.version,
        substrate_version=pb.substrate_version,
        features=tuple(pb.features),
    )


def _process_info_from_pb(pb: guest_pb2.Process) -> ProcessInfo:
    started_at = (
        pb.started_at.ToDatetime(tzinfo=timezone.utc) if pb.HasField("started_at") else None
    )
    finished_at = (
        pb.finished_at.ToDatetime(tzinfo=timezone.utc) if pb.HasField("finished_at") else None
    )
    return ProcessInfo(
        process_id=pb.process_id,
        command=tuple(pb.command),
        pid=pb.pid,
        state=ProcessState(pb.state),
        exit_code=pb.exit_code,
        started_at=started_at,
        finished_at=finished_at,
        timed_out=getattr(pb, "timed_out", False),
    )


def _process_output_from_pb(pb: guest_pb2.ProcessOutput) -> ProcessOutput:
    which = pb.WhichOneof("output")
    if which == "stdout":
        return ProcessOutput(stdout=pb.stdout)
    if which == "stderr":
        return ProcessOutput(stderr=pb.stderr)
    if which == "exit":
        return ProcessOutput(exit=_process_info_from_pb(pb.exit))
    raise ValueError(f"ate_env_rl: unexpected process output {pb!r}")


@dataclass(frozen=True)
class ResourceLimits:
    """CPU and memory limits for a sandbox environment."""
    cpu: str = "2"
    memory: str = "4Gi"


@dataclass(frozen=True)
class PlacementSpec:
    """Placement constraints and worker hardware selectors."""
    node_selector: dict[str, str] = field(default_factory=dict)
    tolerations: list[dict[str, Any]] = field(default_factory=list)
    worker_family: str | None = None  # e.g., "c2", "n2", "c3"


@dataclass(frozen=True)
class EnvironmentSpec:
    """Immutable environment definition representing image, bundle, and constraints."""
    image: str
    runtime_bundle: str = "default-guest"
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    placement: PlacementSpec = field(default_factory=PlacementSpec)
    snapshot_storage_uri: str | None = None

    def template_key(self) -> str:
        """Derive canonical template ID including CPU family and bundle digest."""
        raw = f"{self.image}::{self.runtime_bundle}::{self.placement.worker_family or 'ambient'}"
        img_hash = hashlib.sha256(raw.encode()).hexdigest()[:12]
        # Clean alphanumeric template name
        clean_img = self.image.split("/")[-1].split(":")[0].replace(".", "-").replace("_", "-")
        return f"tmpl-{clean_img[:20]}-{img_hash}"


@dataclass
class Task:
    """Single workload/task definition (e.g. one SWE-bench instance).

    Attributes:
        id: Unique identifier for this task.
        image: OCI container image reference. For Substrate backends, if mapped in
            image_templates the mapped ActorTemplate is used; otherwise it is passed
            as rootfs_image to derive a template on the fly.
        metadata: Optional dictionary of arbitrary task metadata.
        template: Optional explicit ActorTemplate name override.
    """
    id: str
    image: str
    metadata: dict[str, Any] = field(default_factory=dict)
    template: str | None = None



@dataclass
class ExecResult:
    """Outcome of a command that the guest actually ran.

    A runtime returns an ExecResult only when the guest started the command
    and either observed it exit (``exit_code`` set) or killed it at its
    deadline (``timed_out=True`` and ``exit_code=None``; output may be
    partial). Anything else (unreachable sandbox, HTTP/gRPC error, malformed
    response) raises ``InfrastructureError`` instead, so transport failures
    can never be mistaken for agent failures.

    Exit codes follow standard POSIX/Linux conventions: a command killed by
    a signal reports 128 plus the signal number (for example 137 for SIGKILL).
    Note: checking ``exit_code == 137 and duration >= timeout_s`` to detect
    timeouts is a heuristic and may produce false positives if a process was
    terminated by an external signal (such as an out-of-memory killer) near
    its deadline.
    """
    exit_code: int | None
    stdout: str
    stderr: str
    duration_s: float = 0.0
    timed_out: bool = False

    def __post_init__(self) -> None:
        if self.timed_out and self.exit_code is not None:
            raise ValueError("ExecResult: exit_code must be None when timed_out=True")
        if not self.timed_out and self.exit_code is None:
            raise ValueError("ExecResult: exit_code is required unless timed_out=True")

    @property
    def ok(self) -> bool:
        """True iff the command finished within its deadline with exit code 0."""
        return not self.timed_out and self.exit_code == 0


_SENSITIVE_HEADERS = frozenset({"authorization", "proxy-authorization", "cookie", "x-api-key"})


@dataclass(repr=False)
class DataPlaneEndpoint:
    """Per-sandbox data-plane coordinates produced by a BackendDriver.

    Attributes:
        protocol: ``"grpc"`` (in-guest gRPC server multiplexed via ate-env-api).
        address: Host:port of the ate-env-api endpoint.
        headers: gRPC metadata routing headers to this specific sandbox,
            e.g. ``{"x-env-id": "<actor>", "x-env-atespace": "<atespace>"}``.
    """
    protocol: Literal["grpc"] = "grpc"
    address: str = ""
    headers: dict[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:
        shown = {k: ("<redacted>" if k.lower() in _SENSITIVE_HEADERS else v)
                 for k, v in self.headers.items()}
        return (f"DataPlaneEndpoint(protocol={self.protocol!r}, "
                f"address={self.address!r}, headers={shown!r})")


@dataclass
class RawSandboxInstance:
    """Low-level sandbox handle returned by BackendDriver."""
    instance_id: str
    endpoint: str
    template_id: str
    run_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    # Keyed by data-plane name matching FleetConfig.data_plane ("router", "grpc").
    data_planes: dict[str, DataPlaneEndpoint] = field(default_factory=dict)
    rootfs_image: str | None = None


@dataclass
class PlanEntry:
    """One environment's calculated provisioning plan."""
    image: str
    template_id: str
    replicas: int
    tasks: int
    rootfs_image: str | None = None


class FleetPlan:
    """The complete provisioning plan computed by SandboxFleet.plan()."""
    def __init__(self, entries: list[PlanEntry]):
        self.entries = entries
        self._by_image = {e.image: e for e in entries}

    def for_image(self, image: str) -> PlanEntry | None:
        return self._by_image.get(image)

    @property
    def total_replicas(self) -> int:
        return sum(e.replicas for e in self.entries)


def _placement_domain_from_pb(pb: env_pb2.PlacementDomain) -> PlacementDomain:
    return PlacementDomain(
        name=pb.name,
        worker_namespace=pb.worker_namespace,
        selector=dict(pb.selector),
        nodes=pb.nodes,
        workers=pb.workers,
        node_slots=pb.node_slots,
    )


def _warm_probe_from_pb(pb: env_pb2.WarmProbe) -> WarmProbe:
    return WarmProbe(
        id=pb.id,
        node=pb.node,
        round=pb.round,
        duration=pb.duration.ToTimedelta().total_seconds() if pb.HasField("duration") else 0.0,
        error=pb.error,
    )


def _warm_coverage_from_pb(pb: env_pb2.WarmImageCoverage) -> WarmImageCoverage:
    return WarmImageCoverage(
        nodes=tuple(pb.nodes),
        wanted=pb.wanted,
        probes=pb.probes,
        failed=pb.failed,
        rounds=pb.rounds,
        complete=pb.complete,
        error=pb.error,
        undeleted_probes=tuple(pb.undeleted_probes),
        waited=pb.waited.ToTimedelta().total_seconds() if pb.HasField("waited") else 0.0,
    )

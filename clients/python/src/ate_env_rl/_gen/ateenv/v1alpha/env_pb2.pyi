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

import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EnvironmentStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENVIRONMENT_STATUS_UNSPECIFIED: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_RESUMING: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_RUNNING: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_SUSPENDING: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_SUSPENDED: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_PAUSING: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_PAUSED: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_CRASHED: _ClassVar[EnvironmentStatus]
    ENVIRONMENT_STATUS_DELETING: _ClassVar[EnvironmentStatus]

class SuspendMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SUSPEND_MODE_UNSPECIFIED: _ClassVar[SuspendMode]
    SUSPEND_MODE_DURABLE: _ClassVar[SuspendMode]
    SUSPEND_MODE_LOCAL: _ClassVar[SuspendMode]

class GoldenSnapshotState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GOLDEN_SNAPSHOT_STATE_UNSPECIFIED: _ClassVar[GoldenSnapshotState]
    GOLDEN_SNAPSHOT_STATE_NONE: _ClassVar[GoldenSnapshotState]
    GOLDEN_SNAPSHOT_STATE_PENDING: _ClassVar[GoldenSnapshotState]
    GOLDEN_SNAPSHOT_STATE_READY: _ClassVar[GoldenSnapshotState]
    GOLDEN_SNAPSHOT_STATE_FAILED: _ClassVar[GoldenSnapshotState]
ENVIRONMENT_STATUS_UNSPECIFIED: EnvironmentStatus
ENVIRONMENT_STATUS_RESUMING: EnvironmentStatus
ENVIRONMENT_STATUS_RUNNING: EnvironmentStatus
ENVIRONMENT_STATUS_SUSPENDING: EnvironmentStatus
ENVIRONMENT_STATUS_SUSPENDED: EnvironmentStatus
ENVIRONMENT_STATUS_PAUSING: EnvironmentStatus
ENVIRONMENT_STATUS_PAUSED: EnvironmentStatus
ENVIRONMENT_STATUS_CRASHED: EnvironmentStatus
ENVIRONMENT_STATUS_DELETING: EnvironmentStatus
SUSPEND_MODE_UNSPECIFIED: SuspendMode
SUSPEND_MODE_DURABLE: SuspendMode
SUSPEND_MODE_LOCAL: SuspendMode
GOLDEN_SNAPSHOT_STATE_UNSPECIFIED: GoldenSnapshotState
GOLDEN_SNAPSHOT_STATE_NONE: GoldenSnapshotState
GOLDEN_SNAPSHOT_STATE_PENDING: GoldenSnapshotState
GOLDEN_SNAPSHOT_STATE_READY: GoldenSnapshotState
GOLDEN_SNAPSHOT_STATE_FAILED: GoldenSnapshotState

class Template(_message.Message):
    __slots__ = ("name", "atespace")
    NAME_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    name: str
    atespace: str
    def __init__(self, name: _Optional[str] = ..., atespace: _Optional[str] = ...) -> None: ...

class Environment(_message.Message):
    __slots__ = ("id", "atespace", "template", "status", "node", "worker_selector")
    class WorkerSelectorEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    WORKER_SELECTOR_FIELD_NUMBER: _ClassVar[int]
    id: str
    atespace: str
    template: Template
    status: EnvironmentStatus
    node: str
    worker_selector: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., atespace: _Optional[str] = ..., template: _Optional[_Union[Template, _Mapping]] = ..., status: _Optional[_Union[EnvironmentStatus, str]] = ..., node: _Optional[str] = ..., worker_selector: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateEnvironmentRequest(_message.Message):
    __slots__ = ("id", "atespace", "template", "rootfs_image", "worker_selector")
    class WorkerSelectorEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    ROOTFS_IMAGE_FIELD_NUMBER: _ClassVar[int]
    WORKER_SELECTOR_FIELD_NUMBER: _ClassVar[int]
    id: str
    atespace: str
    template: Template
    rootfs_image: str
    worker_selector: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., atespace: _Optional[str] = ..., template: _Optional[_Union[Template, _Mapping]] = ..., rootfs_image: _Optional[str] = ..., worker_selector: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateEnvironmentResponse(_message.Message):
    __slots__ = ("environment",)
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    environment: Environment
    def __init__(self, environment: _Optional[_Union[Environment, _Mapping]] = ...) -> None: ...

class GetImageStatusRequest(_message.Message):
    __slots__ = ("rootfs_image", "template", "atespace")
    ROOTFS_IMAGE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    rootfs_image: str
    template: Template
    atespace: str
    def __init__(self, rootfs_image: _Optional[str] = ..., template: _Optional[_Union[Template, _Mapping]] = ..., atespace: _Optional[str] = ...) -> None: ...

class EnsureImageTemplateRequest(_message.Message):
    __slots__ = ("rootfs_image", "template", "atespace")
    ROOTFS_IMAGE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    rootfs_image: str
    template: Template
    atespace: str
    def __init__(self, rootfs_image: _Optional[str] = ..., template: _Optional[_Union[Template, _Mapping]] = ..., atespace: _Optional[str] = ...) -> None: ...

class ImageStatus(_message.Message):
    __slots__ = ("rootfs_image", "template", "template_exists", "golden", "golden_tag", "golden_error")
    ROOTFS_IMAGE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_EXISTS_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_TAG_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_ERROR_FIELD_NUMBER: _ClassVar[int]
    rootfs_image: str
    template: Template
    template_exists: bool
    golden: GoldenSnapshotState
    golden_tag: str
    golden_error: str
    def __init__(self, rootfs_image: _Optional[str] = ..., template: _Optional[_Union[Template, _Mapping]] = ..., template_exists: _Optional[bool] = ..., golden: _Optional[_Union[GoldenSnapshotState, str]] = ..., golden_tag: _Optional[str] = ..., golden_error: _Optional[str] = ...) -> None: ...

class GetEnvironmentRequest(_message.Message):
    __slots__ = ("id", "atespace")
    ID_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    id: str
    atespace: str
    def __init__(self, id: _Optional[str] = ..., atespace: _Optional[str] = ...) -> None: ...

class GetEnvironmentResponse(_message.Message):
    __slots__ = ("environment",)
    ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    environment: Environment
    def __init__(self, environment: _Optional[_Union[Environment, _Mapping]] = ...) -> None: ...

class SuspendEnvironmentRequest(_message.Message):
    __slots__ = ("id", "atespace", "mode")
    ID_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    id: str
    atespace: str
    mode: SuspendMode
    def __init__(self, id: _Optional[str] = ..., atespace: _Optional[str] = ..., mode: _Optional[_Union[SuspendMode, str]] = ...) -> None: ...

class SuspendEnvironmentResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeleteEnvironmentRequest(_message.Message):
    __slots__ = ("id", "atespace")
    ID_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    id: str
    atespace: str
    def __init__(self, id: _Optional[str] = ..., atespace: _Optional[str] = ...) -> None: ...

class DeleteEnvironmentResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetServerInfoRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetServerInfoResponse(_message.Message):
    __slots__ = ("server", "version", "substrate_version", "features")
    SERVER_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SUBSTRATE_VERSION_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    server: str
    version: str
    substrate_version: str
    features: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, server: _Optional[str] = ..., version: _Optional[str] = ..., substrate_version: _Optional[str] = ..., features: _Optional[_Iterable[str]] = ...) -> None: ...

class ListPlacementDomainsRequest(_message.Message):
    __slots__ = ("template", "atespace")
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    template: Template
    atespace: str
    def __init__(self, template: _Optional[_Union[Template, _Mapping]] = ..., atespace: _Optional[str] = ...) -> None: ...

class PlacementDomain(_message.Message):
    __slots__ = ("name", "worker_namespace", "selector", "nodes", "workers", "node_slots")
    class SelectorEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    WORKER_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    NODE_SLOTS_FIELD_NUMBER: _ClassVar[int]
    name: str
    worker_namespace: str
    selector: _containers.ScalarMap[str, str]
    nodes: int
    workers: int
    node_slots: int
    def __init__(self, name: _Optional[str] = ..., worker_namespace: _Optional[str] = ..., selector: _Optional[_Mapping[str, str]] = ..., nodes: _Optional[int] = ..., workers: _Optional[int] = ..., node_slots: _Optional[int] = ...) -> None: ...

class ListPlacementDomainsResponse(_message.Message):
    __slots__ = ("domains",)
    DOMAINS_FIELD_NUMBER: _ClassVar[int]
    domains: _containers.RepeatedCompositeFieldContainer[PlacementDomain]
    def __init__(self, domains: _Optional[_Iterable[_Union[PlacementDomain, _Mapping]]] = ...) -> None: ...

class WarmImageRequest(_message.Message):
    __slots__ = ("rootfs_image", "template", "atespace", "worker_selector", "nodes", "probes_per_node", "max_rounds", "probe_timeout", "run_id")
    class WorkerSelectorEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ROOTFS_IMAGE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    ATESPACE_FIELD_NUMBER: _ClassVar[int]
    WORKER_SELECTOR_FIELD_NUMBER: _ClassVar[int]
    NODES_FIELD_NUMBER: _ClassVar[int]
    PROBES_PER_NODE_FIELD_NUMBER: _ClassVar[int]
    MAX_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    PROBE_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    rootfs_image: str
    template: Template
    atespace: str
    worker_selector: _containers.ScalarMap[str, str]
    nodes: int
    probes_per_node: int
    max_rounds: int
    probe_timeout: _duration_pb2.Duration
    run_id: str
    def __init__(self, rootfs_image: _Optional[str] = ..., template: _Optional[_Union[Template, _Mapping]] = ..., atespace: _Optional[str] = ..., worker_selector: _Optional[_Mapping[str, str]] = ..., nodes: _Optional[int] = ..., probes_per_node: _Optional[int] = ..., max_rounds: _Optional[int] = ..., probe_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., run_id: _Optional[str] = ...) -> None: ...

class WarmImageEvent(_message.Message):
    __slots__ = ("probe", "coverage")
    PROBE_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_FIELD_NUMBER: _ClassVar[int]
    probe: WarmProbe
    coverage: WarmImageCoverage
    def __init__(self, probe: _Optional[_Union[WarmProbe, _Mapping]] = ..., coverage: _Optional[_Union[WarmImageCoverage, _Mapping]] = ...) -> None: ...

class WarmProbe(_message.Message):
    __slots__ = ("id", "node", "round", "duration", "error")
    ID_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    ROUND_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    id: str
    node: str
    round: int
    duration: _duration_pb2.Duration
    error: str
    def __init__(self, id: _Optional[str] = ..., node: _Optional[str] = ..., round: _Optional[int] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., error: _Optional[str] = ...) -> None: ...

class WarmImageCoverage(_message.Message):
    __slots__ = ("nodes", "wanted", "probes", "failed", "rounds", "complete", "error", "undeleted_probes", "waited")
    NODES_FIELD_NUMBER: _ClassVar[int]
    WANTED_FIELD_NUMBER: _ClassVar[int]
    PROBES_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    ROUNDS_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    UNDELETED_PROBES_FIELD_NUMBER: _ClassVar[int]
    WAITED_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedScalarFieldContainer[str]
    wanted: int
    probes: int
    failed: int
    rounds: int
    complete: bool
    error: str
    undeleted_probes: _containers.RepeatedScalarFieldContainer[str]
    waited: _duration_pb2.Duration
    def __init__(self, nodes: _Optional[_Iterable[str]] = ..., wanted: _Optional[int] = ..., probes: _Optional[int] = ..., failed: _Optional[int] = ..., rounds: _Optional[int] = ..., complete: _Optional[bool] = ..., error: _Optional[str] = ..., undeleted_probes: _Optional[_Iterable[str]] = ..., waited: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

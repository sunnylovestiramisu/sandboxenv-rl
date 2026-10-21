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

from __future__ import annotations

import re
from dataclasses import MISSING, dataclass, field, fields
from typing import Any, Literal

BackendType = Literal["substrate", "mock"]
StrategyType = Literal["none", "naive", "sliding", "pipelined"]

# Atespaces (Substrate) are DNS-1123 labels.
_DNS1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")

ALLOWED_BACKENDS = {"substrate", "mock"}
ALLOWED_STRATEGIES = {"none", "naive", "sliding", "pipelined"}
ALLOWED_DATA_PLANES = {"ate_env"}


class ValidationError(ValueError):
    """Configuration validation error."""


class SecretStr:
    """Wraps sensitive strings to prevent accidental leakage in repr or str."""

    def __init__(self, secret_value: str):
        if not isinstance(secret_value, str):
            raise TypeError("SecretStr value must be a str")
        self._secret_value = secret_value

    def get_secret_value(self) -> str:
        return self._secret_value

    def __repr__(self) -> str:
        return "SecretStr('**********')"

    def __str__(self) -> str:
        return "**********"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SecretStr):
            return self._secret_value == other._secret_value
        return False

    def __hash__(self) -> int:
        return hash(self._secret_value)


def _init_fields(obj: Any, kwargs: dict[str, Any]) -> None:
    """Set dataclass fields from kwargs, rejecting unknown names."""
    valid_fields = {f.name: f for f in fields(obj)}
    unknown = set(kwargs.keys()) - set(valid_fields.keys())
    if unknown:
        raise ValidationError(f"Unknown fields for {type(obj).__name__}: {sorted(unknown)}")
    for name, f in valid_fields.items():
        if name in kwargs:
            val = kwargs[name]
        elif f.default is not MISSING:
            val = f.default
        elif f.default_factory is not MISSING:
            val = f.default_factory()
        else:
            val = None
        object.__setattr__(obj, name, val)


@dataclass(frozen=True)
class TracerConfig:
    """OpenTelemetry tracing for the ate_env client (needs the ``tracing`` extra).

    trace_service_name is the OTel resource ``service.name`` of the process's
    TracerProvider; client spans are always named ``ate_env_client.<op>``.
    Unknown fields are rejected.
    """

    enable_tracing: bool = False
    trace_service_name: str = "ate-env-client"

    def __init__(self, **kwargs: Any):
        _init_fields(self, kwargs)
        self.__post_init__()

    def __post_init__(self) -> None:
        if not isinstance(self.enable_tracing, bool):
            raise ValidationError(f"enable_tracing must be a bool, got {self.enable_tracing!r}")
        if not isinstance(self.trace_service_name, str) or not self.trace_service_name:
            raise ValidationError(
                f"trace_service_name must be a non-empty str, got {self.trace_service_name!r}"
            )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TracerConfig:
        return cls(**data)


@dataclass(frozen=True)
class ObservabilityConfig:
    """Fleet observability: the always-on RunReport plus optional layers.

    enable_metrics registers the ``atefleet_*`` and ``ate_env_client_*``
    Prometheus series (a logged no-op without the ``metrics`` extra);
    enable_tracing emits ``atefleet.<phase>`` spans and turns on client
    tracing under trace_service_name (needs the ``tracing`` extra).
    Unknown fields are rejected.
    """

    enable_metrics: bool = True
    enable_tracing: bool = False
    trace_service_name: str = "ate-env-fleet"

    def __init__(self, **kwargs: Any):
        _init_fields(self, kwargs)
        self.__post_init__()

    def __post_init__(self) -> None:
        for name in ("enable_metrics", "enable_tracing"):
            if not isinstance(getattr(self, name), bool):
                raise ValidationError(f"{name} must be a bool, got {getattr(self, name)!r}")
        if not isinstance(self.trace_service_name, str) or not self.trace_service_name:
            raise ValidationError(
                f"trace_service_name must be a non-empty str, got {self.trace_service_name!r}"
            )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ObservabilityConfig:
        return cls(**data)


def _nested(cls: Any, name: str, value: Any) -> Any:
    """Accept a config dataclass or a plain dict for a nested FleetConfig field."""
    if value is None:
        return cls()
    if isinstance(value, cls):
        return value
    if isinstance(value, dict):
        return cls(**value)
    raise ValidationError(f"{name} must be a {cls.__name__} or a dict, got {type(value).__name__}")


@dataclass(frozen=True)
class FleetConfig:
    """Configuration for SandboxFleet and associated BackendDriver.

    Unknown fields are rejected so misspelled or unsupported options
    fail loudly instead of being silently ignored.
    """

    backend: BackendType = "substrate"
    endpoint: str = "127.0.0.1:7777"
    data_plane: Literal["ate_env"] = "ate_env"
    tenancy: str = "ate-env"
    strategy: StrategyType = "sliding"
    template: str | None = None
    image_templates: dict[str, str] = field(default_factory=dict)

    # Sizing & Rollout parameters
    batch_size: int = 8
    max_concurrent: int = 64
    max_warmpool_replicas: int = 4
    window_size: int | None = None

    # Hardware & placement (reserved for template placement and future scheduler extensions)
    worker_family: str | None = "c2"
    node_selector: dict[str, str] = field(default_factory=dict)
    tolerations: list[dict[str, Any]] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)

    # Timeouts & Breakers
    acquire_timeout_s: float = 180.0
    ready_timeout_s: float = 180.0
    step_timeout_s: float = 120.0
    auth_token: SecretStr | None = None

    # Telemetry
    tracing: TracerConfig = field(default_factory=TracerConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

    def __init__(self, **kwargs: Any):
        _init_fields(self, kwargs)
        self.__post_init__()

    def __post_init__(self) -> None:
        if self.backend not in ALLOWED_BACKENDS:
            raise ValidationError(f"Invalid backend {self.backend!r}, must be one of {ALLOWED_BACKENDS}")

        if self.data_plane not in ALLOWED_DATA_PLANES:
            raise ValidationError(f"Invalid data_plane {self.data_plane!r}, must be one of {ALLOWED_DATA_PLANES}")

        if not isinstance(self.tenancy, str) or not _DNS1123_LABEL.match(self.tenancy):
            raise ValidationError(f"Invalid tenancy {self.tenancy!r}: must match DNS-1123 label regex")

        if self.strategy not in ALLOWED_STRATEGIES:
            raise ValidationError(f"Invalid strategy {self.strategy!r}, must be one of {ALLOWED_STRATEGIES}")

        object.__setattr__(self, "image_templates", dict(self.image_templates))

        if not isinstance(self.batch_size, int) or self.batch_size < 1:
            raise ValidationError(f"batch_size must be >= 1, got {self.batch_size}")

        if not isinstance(self.max_concurrent, int) or self.max_concurrent < 1:
            raise ValidationError(f"max_concurrent must be >= 1, got {self.max_concurrent}")

        if not isinstance(self.max_warmpool_replicas, int) or self.max_warmpool_replicas < 0:
            raise ValidationError(f"max_warmpool_replicas must be >= 0, got {self.max_warmpool_replicas}")

        if self.window_size is not None and (not isinstance(self.window_size, int) or self.window_size < 1):
            raise ValidationError(f"window_size must be >= 1, got {self.window_size}")

        object.__setattr__(self, "node_selector", dict(self.node_selector))
        object.__setattr__(self, "tolerations", list(self.tolerations))
        object.__setattr__(self, "labels", dict(self.labels))

        if self.acquire_timeout_s <= 0:
            raise ValidationError(f"acquire_timeout_s must be > 0, got {self.acquire_timeout_s}")
        object.__setattr__(self, "acquire_timeout_s", float(self.acquire_timeout_s))

        if self.ready_timeout_s <= 0:
            raise ValidationError(f"ready_timeout_s must be > 0, got {self.ready_timeout_s}")
        object.__setattr__(self, "ready_timeout_s", float(self.ready_timeout_s))

        if self.step_timeout_s <= 0:
            raise ValidationError(f"step_timeout_s must be > 0, got {self.step_timeout_s}")
        object.__setattr__(self, "step_timeout_s", float(self.step_timeout_s))

        if self.auth_token is not None and not isinstance(self.auth_token, SecretStr):
            object.__setattr__(self, "auth_token", SecretStr(str(self.auth_token)))

        object.__setattr__(self, "tracing", _nested(TracerConfig, "tracing", self.tracing))
        object.__setattr__(
            self, "observability", _nested(ObservabilityConfig, "observability", self.observability)
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to dict, keeping auth_token as SecretStr for safe round-tripping."""
        res: dict[str, Any] = {}
        for f in fields(self):
            val = getattr(self, f.name)
            res[f.name] = val
        return res

    model_dump = to_dict

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FleetConfig:
        return cls(**data)

    model_validate = from_dict

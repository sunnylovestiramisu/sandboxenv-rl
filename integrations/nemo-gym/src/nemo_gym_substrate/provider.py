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

"""NeMo Gym sandbox provider backed by Agent Substrate.

Each sandbox is a Substrate actor fronted by the ``ate-env`` API
(https://github.com/agent-substrate/env), reached through this repo's Python
client, ``ate_env_rl`` (``clients/python``). Environment lifecycle (create, get, delete) goes to
``ateenv.v1alpha.EnvironmentService`` on ``ate-env-api``; command execution and
file transfer go to the ``ProcessService`` / ``FileSystemService`` served by
the ``ate-env-guest`` daemon inside every actor, which ``ate-env-api`` proxies
through the atenet router. One gRPC endpoint carries both. Idle sandboxes can
be suspended by Substrate and are resumed transparently on the next guest
call, so a fleet of mostly-waiting rollout sandboxes holds no workers.

Contract: https://docs.nvidia.com/nemo/gym/main/infrastructure/sandbox/adding-a-provider/
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shlex
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import grpc
import grpc.aio
from ate_env_rl import (
    Client,
    Env,
    EnvError,
    EnvironmentStatus,
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
    Process,
    SuspendMode,
)

from ._compat import (
    SandboxCreateError,
    SandboxCreateVerificationError,
    SandboxEndpoint,
    SandboxExecResult,
    SandboxHandle,
    SandboxSpec,
    SandboxStatus,
)

logger = logging.getLogger(__name__)

_PROVIDER_NAME = "substrate"

# Client-side slack on top of exec(timeout_s=...): the guest enforces the
# deadline itself (see exec), so the client only has to outlive it.
_EXEC_GRACE_S = 5.0
# Deadline for one readiness or user probe. A guest that is still booting may
# hold a call open; bounding it keeps the poll loop honest.
_PROBE_TIMEOUT_S = 10.0
# Smallest guest deadline exec() asks for. The guest takes any positive
# duration, but zero selects its default (an hour) and a sub-second kill
# window is noise next to the RPC round trip.
_MIN_GUEST_TIMEOUT_S = 1.0
# Exit code the guest reports for a process group it killed with SIGKILL.
_SIGKILL_EXIT = 137
# Return code of an exec result that carries no process exit status: the
# command timed out, or the sandbox could not run it at all. The value is the
# sentinel NeMo Gym's own providers use; what makes it a runtime failure is
# the `error_type` next to it ("timeout" or "sandbox"), which Gym's agents
# check to end a rollout instead of treating the result as an observation.
_SANDBOX_RUNTIME_RETURN_CODE = 125

# Routing headers of ate-env-api's port proxy (internal/portproxy): a plain
# HTTP request carrying them is forwarded to that port inside the environment.
_ENV_ID_HEADER = "X-Env-Id"
_ENV_ATESPACE_HEADER = "X-Env-Atespace"
_ENV_PORT_HEADER = "X-Env-Port"
# The caller's bearer token for ate-env-api itself on a port-proxy request.
# The proxy checks it and removes it, so the daemon never sees it and keeps
# Authorization for its own credential.
_ENV_AUTHORIZATION_HEADER = "X-Env-Authorization"
# Environment variable with the bearer token, the one the ate-env CLI reads.
_AUTH_TOKEN_ENV = "SUBSTRATE_ENV_AUTH_TOKEN"
# The ate-env guest's own port in every environment: not a daemon port, and
# the port proxy refuses it too.
_GUEST_PORT = 80

# Parking. How long a park may stay mid-transition (SUSPENDING) before the
# provider stops waiting and leaves it to the control plane, and how often the
# state is read meanwhile; how often pause() checks for requests in flight.
_PARK_SETTLE_S = 30.0
_PARK_POLL_S = 0.25
_PAUSE_DRAIN_POLL_S = 0.1
# Env lifecycle statuses of a parked sandbox, and of one on its way there.
_PARKED = frozenset({EnvironmentStatus.SUSPENDED, EnvironmentStatus.PAUSED})
_PARKING = frozenset({EnvironmentStatus.SUSPENDING, EnvironmentStatus.PAUSING})
# create.suspend_policy values, and the suspend mode each one parks with.
_POLICY_NEVER = "never"
_POLICY_PAUSE = "pause_after_exec"
_POLICY_SUSPEND = "suspend_after_exec"
_POLICY_MODES = {_POLICY_PAUSE: SuspendMode.LOCAL, _POLICY_SUSPEND: SuspendMode.DURABLE}
# The guest runs commands as the image's user. An exec that names another user
# is wrapped in sudo, or in su when the image runs as root; this probe learns
# the image user's name and uid and which of the two the image has. A sudo
# that wants a password (no NOPASSWD entry, `requiretty`) counts as absent:
# `sudo -n true` must succeed, a passwordless switch to root, the user Gym's
# transfer helpers send. A root image whose sudo refuses falls through to su.
# When neither can switch, the result is Gym's `user_switch_unavailable`,
# which Gym's transfer helpers answer by running as the default user.
_USER_PROBE = (
    "id -un 2>/dev/null; id -u 2>/dev/null; "
    "if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then echo tool=sudo; "
    "elif command -v su >/dev/null 2>&1; then echo tool=su; fi"
)
_USER_SWITCH_UNAVAILABLE = "user_switch_unavailable"


def _validate_ports(ports: object) -> list[int]:
    """``SandboxSpec.ports`` as a list of TCP ports a daemon can listen on.

    Checked before the actor starts, so a bad spec fails at once rather than
    at ``endpoint()`` time. ``bool`` is excluded explicitly because it is an
    ``int`` subclass in Python (``True == 1``)."""
    out: list[int] = []
    for p in ports or ():
        if isinstance(p, bool) or not isinstance(p, int):
            raise TypeError(f"SandboxSpec.ports entries must be integers, got {p!r}")
        if not 1 <= p <= 65535:
            raise ValueError(f"SandboxSpec.ports entry {p} is not a TCP port (1-65535)")
        if p == _GUEST_PORT:
            raise ValueError(
                f"SandboxSpec.ports entry {p} is the ate-env guest's port; a daemon listens on another port"
            )
        out.append(p)
    return out

# Gym's status for a parked sandbox. PAUSED arrived with nemo-gym 0.7: the
# sandbox is alive and one call away, since the router resumes it on the next
# guest call. An older nemo-gym has no such member, and RUNNING is the nearest
# truth there.
_PAUSED = getattr(SandboxStatus, "PAUSED", SandboxStatus.RUNNING)

# Env lifecycle status -> Gym status (see README, "Resource mapping").
_STATUS_MAP = {
    EnvironmentStatus.RESUMING: SandboxStatus.STARTING,
    EnvironmentStatus.RUNNING: SandboxStatus.RUNNING,
    EnvironmentStatus.SUSPENDING: _PAUSED,
    EnvironmentStatus.SUSPENDED: _PAUSED,
    EnvironmentStatus.PAUSING: _PAUSED,
    EnvironmentStatus.PAUSED: _PAUSED,
    EnvironmentStatus.CRASHED: SandboxStatus.ERROR,
    EnvironmentStatus.DELETING: SandboxStatus.STOPPED,
}


def _runtime_failure(
    error_type: str, message: str, *, stdout: str | None = None, stderr: str | None = None
) -> SandboxExecResult:
    """An exec result for a command the sandbox did not run to completion.

    ``error_type`` is "timeout", "sandbox" or "user_switch_unavailable"; the
    return code is the runtime sentinel. Output the command produced before
    it was cut is kept, and ``message`` goes on stderr after it."""
    return SandboxExecResult(
        stdout=stdout,
        stderr=f"{stderr}\n{message}" if stderr else message,
        return_code=_SANDBOX_RUNTIME_RETURN_CODE,
        error_type=error_type,
    )


class _UserSwitchUnavailable(Exception):
    """The image cannot run a command as the requested user; the message says why."""


def _as_user_command(
    command: str, user: str | int, env: Mapping[str, str], switch: _UserSwitch
) -> str:
    """``command`` wrapped to run as ``user``; ``_UserSwitchUnavailable`` when
    the image cannot switch.

    The environment is inlined with ``env -- K=V ...``: sudo's ``env_reset``
    drops the caller's variables, and the guest applies them to the outer
    process only. The ``--`` keeps a key that starts with ``-`` from being
    read as an option; su takes its options before the user for the same
    reason. Both forms run on BusyBox and util-linux."""
    name = str(user)
    if name in switch.identities:
        return command
    inner = "sh -c " + shlex.quote(command)
    if env:
        inner = "env -- " + " ".join(shlex.quote(f"{k}={v}") for k, v in env.items()) + " " + inner
    if switch.tool == "sudo":
        target = f"#{name}" if name.isdigit() else name
        return f"sudo -n -u {shlex.quote(target)} {inner}"
    if switch.tool == "su" and "0" in switch.identities:
        if name.isdigit():
            raise _UserSwitchUnavailable(
                f"the image switches users with su, which takes a name, not the uid {name}"
            )
        return f"su -s /bin/sh -c {shlex.quote(inner)} -- {shlex.quote(name)}"
    raise _UserSwitchUnavailable(
        "the image runs commands as its own user and has no passwordless sudo "
        "(or su, when it runs as root)"
    )


def _parse_user_probe(stdout: str | None) -> _UserSwitch:
    """The switch a run of ``_USER_PROBE`` printed: its ``tool=`` line, and the
    other lines as the image user's identities."""
    lines = [line.strip() for line in (stdout or "").splitlines() if line.strip()]
    tool = next((line.removeprefix("tool=") for line in lines if line.startswith("tool=")), "")
    return _UserSwitch(frozenset(line for line in lines if not line.startswith("tool=")), tool)


def _require_keys(options: Mapping[str, Any], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(options) - allowed)
    if unknown:
        raise ValueError(f"{where}: unknown option(s) {unknown}; allowed: {sorted(allowed)}")


def _describe(exc: BaseException) -> str:
    """``CODE: details`` for ate_env_rl errors, so messages name the gRPC status."""
    code = getattr(exc, "code", None)
    if isinstance(code, grpc.StatusCode):
        return f"{code.name}: {exc}"
    if isinstance(exc, EnvError):
        return f"{type(exc).__name__}: {exc}"
    return str(exc) or type(exc).__name__


def _status_name(status: object) -> str:
    try:
        return EnvironmentStatus(status).name  # type: ignore[call-overload]
    except ValueError:
        return str(status)


def _atespace_option(options: Mapping[str, Any], where: str) -> str | None:
    """``atespace`` is the ate-env name; ``namespace`` is accepted as the older alias."""
    if "atespace" in options and "namespace" in options:
        raise ValueError(f"{where}: set either atespace or namespace, not both")
    value = options.get("atespace", options.get("namespace"))
    return None if value is None else str(value)


@dataclass(frozen=True)
class ConnectionConfig:
    """`connection:` block of the provider config."""

    # host:port or an http(s):// URL of ate-env-api. Plain gRPC (h2c) unless https.
    api_url: str = "http://127.0.0.1:7777"
    # Deadline for each lifecycle RPC (create / get / delete).
    request_timeout_s: float = 30.0
    # Base URL that endpoint() hands out for services inside a sandbox. Unset
    # means api_url as an http(s) URL: ate-env-api serves its port proxy on the
    # same listener. Set it when the code that calls the service reaches
    # ate-env-api at another address than the provider does.
    endpoint_url: str | None = None
    # File with ate-env-api's bearer token (-auth-token-file on the API). Unset
    # means SUBSTRATE_ENV_AUTH_TOKEN when it is set, else no token. The config
    # names a file, not the token, so the token stays out of config files and
    # of this object's repr.
    auth_token_file: str | None = None

    _ALLOWED = frozenset({"api_url", "request_timeout_s", "endpoint_url", "auth_token_file"})

    @classmethod
    def from_mapping(cls, options: Mapping[str, Any]) -> "ConnectionConfig":
        _require_keys(options, cls._ALLOWED, "sandbox.substrate.connection")
        endpoint_url = options.get("endpoint_url")
        auth_token_file = options.get("auth_token_file")
        return cls(
            api_url=str(options.get("api_url", cls.api_url)).rstrip("/"),
            request_timeout_s=float(options.get("request_timeout_s", cls.request_timeout_s)),
            endpoint_url=None if endpoint_url is None else str(endpoint_url).rstrip("/"),
            auth_token_file=None if auth_token_file is None else str(auth_token_file),
        )

    def resolve_auth_token(self) -> str | None:
        """The bearer token: from ``auth_token_file`` when set, else from
        ``SUBSTRATE_ENV_AUTH_TOKEN``, else None. Surrounding whitespace is
        ignored; a file with no token is an error, not a request to run
        without one."""
        if self.auth_token_file is None:
            return os.environ.get(_AUTH_TOKEN_ENV, "").strip() or None
        try:
            token = Path(self.auth_token_file).read_text().strip()
        except OSError as exc:
            raise ValueError(
                f"sandbox.substrate.connection.auth_token_file {self.auth_token_file!r}: {exc}"
            ) from exc
        if not token:
            raise ValueError(
                f"sandbox.substrate.connection.auth_token_file {self.auth_token_file!r} is empty"
            )
        return token

    def endpoint_base(self) -> str:
        """The absolute URL of ate-env-api's port proxy: ``endpoint_url`` when
        set, else ``api_url``; either gets ``http://`` when it names no scheme,
        because ``SandboxEndpoint`` rejects a bare ``host:port``."""
        base = self.endpoint_url or self.api_url
        return base if "://" in base else f"http://{base}"


@dataclass(frozen=True)
class CreateConfig:
    """`create:` block of the provider config."""

    # ActorTemplate when the spec names none; ate-env's own default.
    template: str = "default-template"
    # Atespace the templates live in and the environments are created in.
    atespace: str = "ate-env"
    ready_timeout_s: float = 120.0
    ready_poll_interval_s: float = 1.0
    # Optional mapping from SandboxSpec.image to an ActorTemplate name, for
    # workloads that select sandboxes by image reference. Templates must be
    # pre-provisioned on the cluster (see README).
    image_templates: Mapping[str, str] = None  # type: ignore[assignment]
    # Whether, and how, a sandbox is parked once it has been idle for
    # idle_before_suspend_s after create() and after each exec, upload and
    # download (README, "Parking"). never: leave it running between calls.
    # pause_after_exec: SuspendMode.LOCAL, the snapshot stays on the node and
    # uploads in the background, the cheapest park-and-resume cycle.
    # suspend_after_exec: SuspendMode.DURABLE, the snapshot is in object
    # storage before the park counts as done, so the sandbox survives its
    # node. Either way the next guest call wakes the sandbox through
    # ate-env-api; nothing has to resume it.
    suspend_policy: str = _POLICY_NEVER
    # Idle time a create or a call must be followed by before the park, so
    # back-to-back commands (an eval's test runs, say) pay no checkpoint and
    # no restore between them. 0 parks as the call returns.
    idle_before_suspend_s: float = 1.0

    _ALLOWED = frozenset(
        {
            "template",
            "atespace",
            "namespace",
            "ready_timeout_s",
            "ready_poll_interval_s",
            "image_templates",
            "suspend_policy",
            "idle_before_suspend_s",
        }
    )

    @classmethod
    def from_mapping(cls, options: Mapping[str, Any]) -> "CreateConfig":
        _require_keys(options, cls._ALLOWED, "sandbox.substrate.create")
        policy = str(options.get("suspend_policy", cls.suspend_policy))
        if policy != _POLICY_NEVER and policy not in _POLICY_MODES:
            raise ValueError(
                "sandbox.substrate.create.suspend_policy must be one of "
                f"{[_POLICY_NEVER, *_POLICY_MODES]}, got {policy!r}"
            )
        idle = float(options.get("idle_before_suspend_s", cls.idle_before_suspend_s))
        if idle < 0:
            raise ValueError("sandbox.substrate.create.idle_before_suspend_s must be >= 0")
        return cls(
            template=str(options.get("template", cls.template)),
            atespace=_atespace_option(options, "sandbox.substrate.create") or cls.atespace,
            ready_timeout_s=float(options.get("ready_timeout_s", cls.ready_timeout_s)),
            ready_poll_interval_s=float(
                options.get("ready_poll_interval_s", cls.ready_poll_interval_s)
            ),
            image_templates=dict(options.get("image_templates") or {}),
            suspend_policy=policy,
            idle_before_suspend_s=idle,
        )


@dataclass(frozen=True)
class SubstrateProviderOptions:
    """Per-sandbox options carried in ``SandboxSpec.provider_options``."""

    template: str | None = None
    atespace: str | None = None

    _ALLOWED = frozenset({"template", "atespace", "namespace"})

    @classmethod
    def from_mapping(cls, options: Mapping[str, Any]) -> "SubstrateProviderOptions":
        _require_keys(options, cls._ALLOWED, "SandboxSpec.provider_options")
        return cls(
            template=options.get("template"),
            atespace=_atespace_option(options, "SandboxSpec.provider_options"),
        )


@dataclass
class _Parking:
    """Provider-side parking state of one sandbox."""

    # Requests to the guest in flight; the sandbox is never parked while nonzero.
    inflight: int = 0
    # Held while a park is under way. Requests wait on it, because ate-env-api
    # cannot wake a sandbox that is still SUSPENDING.
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    # The park waiting out its idle delay, if any.
    task: asyncio.Task | None = None
    # Parks past their idle wait; those are left to finish.
    parking: int = 0
    # True once a park settled and no request went out since, so two idle
    # parks that both found the sandbox idle do not both call the API.
    parked: bool = False
    # True once the sandbox is shared with another process (serialize_handle
    # here, or connect in the borrower). That process's calls do not pass
    # through this state, thus an idle park here could cut them.
    shared: bool = False


@dataclass(frozen=True)
class _UserSwitch:
    """What a sandbox's image runs commands as, and how it runs them as another user."""

    # Name and uid of the image's user; empty when the image has no `id`, and
    # then every user gets the wrapper, or the refusal.
    identities: frozenset[str]
    # "sudo" (passwordless, for root), "su", or "" when the image has neither
    # or its sudo wants a password.
    tool: str


class SubstrateSandboxProvider:
    """NeMo Gym sandbox provider running sandboxes as Substrate actors.

    With ``create.suspend_policy`` set, a sandbox is parked once it has been
    idle after ``create()`` and after each call, and the next guest call wakes
    it; ``pause()`` and ``resume()`` (``SupportsSandboxPauseResume``) do the
    same on demand. See README, "Parking".
    """

    name = _PROVIDER_NAME

    def __init__(
        self,
        config: Mapping[str, Any] | None = None,
        *,
        client: Client | None = None,
        **kwargs: Any,
    ) -> None:
        """``config`` is the ``sandbox.substrate`` block. Pass ``client`` to reuse a
        caller-owned ``ate_env_rl.Client`` (the provider then never closes it)."""
        config = dict(config or {})
        config.update(kwargs)
        _require_keys(config, frozenset({"connection", "create"}), "sandbox.substrate")
        self._connection = ConnectionConfig.from_mapping(config.get("connection") or {})
        self._create = CreateConfig.from_mapping(config.get("create") or {})
        # Sent on the gRPC calls of a client the provider opens and on each
        # endpoint() request; a caller-owned client keeps its own token.
        self._auth_token = self._connection.resolve_auth_token()
        # Parking state per sandbox id, made on first use and dropped on close;
        # the park tasks, so aclose() can wait for them; the mode idle parks
        # use, None under the `never` policy. An ate-env-api that predates
        # SuspendEnvironment's mode turns LOCAL into DURABLE (see _park).
        self._parking: dict[str, _Parking] = {}
        self._park_tasks: set[asyncio.Task] = set()
        self._park_mode: SuspendMode | None = _POLICY_MODES.get(self._create.suspend_policy)
        self._local_refused = False
        # The user switch per sandbox id, probed on the first exec that names a
        # user and dropped on close.
        self._user_switch: dict[str, _UserSwitch] = {}
        # The channel is opened lazily so constructing a provider never dials.
        self._client: Client | None = client
        self._owns_client = client is None
        self._channel: grpc.aio.Channel | None = None

    # -- provider contract -------------------------------------------------

    async def create(self, spec: SandboxSpec) -> SandboxHandle:
        opts = SubstrateProviderOptions.from_mapping(spec.provider_options or {})
        # Ports a service inside the sandbox listens on (nemo-gym >= 0.7);
        # endpoint() resolves only these.
        ports = _validate_ports(getattr(spec, "ports", None))
        template, rootfs_image = self._resolve_template(spec, opts)
        atespace = opts.atespace or self._create.atespace
        env_id = f"gym-{uuid.uuid4().hex[:10]}"

        try:
            env = await asyncio.wait_for(
                self._ate().create(
                    env_id,
                    atespace=atespace,
                    template_name=template,
                    template_atespace=atespace,
                    rootfs_image=rootfs_image,
                ),
                timeout=self._connection.request_timeout_s,
            )
        except EnvError as exc:
            raise SandboxCreateError(
                f"creating substrate env {env_id!r} (template {atespace}/{template}): "
                f"{_describe(exc)}"
            ) from exc
        except asyncio.TimeoutError as exc:
            raise SandboxCreateError(
                f"creating substrate env {env_id!r}: no answer from "
                f"{self._connection.api_url} within {self._connection.request_timeout_s}s"
            ) from exc

        try:
            await self._wait_ready(env, spec.ready_timeout_s or self._create.ready_timeout_s)
            for target_path, content in (spec.files or {}).items():
                await env.write_file(target_path, content.encode(), mode=0o644)
        except Exception:
            await self._best_effort_delete(env_id, atespace)
            raise

        handle = SandboxHandle(
            sandbox_id=env_id,
            provider_name=self.name,
            raw={
                "env_id": env_id,
                "atespace": atespace,
                "workdir": spec.workdir,
                "env": dict(spec.env or {}),
                "ports": ports,
            },
        )
        # The first command comes after the policy's first think time. Until
        # then a sandbox without service ports would hold a worker for nothing,
        # so it is parked like after a command; the first exec wakes it.
        await self._park_when_idle(handle)
        return handle

    async def exec(
        self,
        handle: SandboxHandle,
        command: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: int | float | None = None,
        user: str | int | None = None,
    ) -> SandboxExecResult:
        raw = handle.raw or {}
        merged_env = {**(raw.get("env") or {}), **(env or {})}
        effective_cwd = cwd or raw.get("workdir") or ""

        argv = ["sh", "-c", command]
        # The guest enforces the deadline itself and kills the process group
        # with SIGKILL when it elapses, so a runaway process never outlives the
        # call that started it and the image needs no `timeout` binary. `is not
        # None` so an explicit 0 means "shortest deadline", not "unlimited".
        guest_timeout = (
            None if timeout_s is None else max(_MIN_GUEST_TIMEOUT_S, float(timeout_s))
        )

        cancelled = False
        try:
            async with self._in_flight(handle):
                result: SandboxExecResult | None = None
                if user is not None:
                    switch = await self._user_switch_for(handle)
                    if isinstance(switch, SandboxExecResult):
                        result = switch
                    else:
                        try:
                            argv = ["sh", "-c", _as_user_command(command, user, merged_env, switch)]
                        except _UserSwitchUnavailable as exc:
                            # No process started: 125 is the runtime sentinel, as
                            # Gym's OpenSandbox provider reports the same refusal.
                            result = _runtime_failure(
                                _USER_SWITCH_UNAVAILABLE,
                                f"substrate provider: cannot run as user {user!r}: {exc}",
                            )
                if result is None:
                    result = await self._run(
                        handle, argv, cwd=effective_cwd, env=merged_env, guest_timeout=guest_timeout,
                        timeout_s=timeout_s,
                    )
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            await self._park_when_idle(handle, wait=not cancelled)
        return result

    async def _run(
        self,
        handle: SandboxHandle,
        argv: list[str],
        *,
        cwd: str,
        env: dict[str, str],
        guest_timeout: float | None,
        timeout_s: int | float | None,
        what: str = "exec",
    ) -> SandboxExecResult:
        # `what` names the command in a timeout message: the caller's exec, or
        # a probe the provider ran on its own with its own deadline.
        # A failure of the sandbox itself is a result with an `error_type`, not
        # an exception: that is the shape Gym's agents check to end a rollout
        # whose sandbox is gone, and it is how Gym's own providers report it.
        # A nonzero exit of the command stays a plain result.
        sandbox = self._env(handle)
        started = time.monotonic()
        try:
            process = await sandbox.start_process(argv, cwd=cwd, env=env, timeout=guest_timeout)
        except EnvError as exc:
            return _runtime_failure("sandbox", self._sandbox_failure(handle, exc))
        try:
            result = await asyncio.wait_for(
                self._collect(process),
                timeout=None if timeout_s is None else timeout_s + _EXEC_GRACE_S,
            )
        except asyncio.TimeoutError:
            with contextlib.suppress(EnvError):
                await process.kill()
            return _runtime_failure(
                "timeout", f"substrate provider: {what} timed out after {timeout_s}s"
            )
        except EnvError as exc:
            return _runtime_failure("sandbox", self._sandbox_failure(handle, exc))
        if (
            guest_timeout is not None
            and result.return_code == _SIGKILL_EXIT
            and time.monotonic() - started >= guest_timeout
        ):
            # The guest reports a deadline kill as a plain SIGKILL and nothing
            # in the exit state says why; the elapsed time does. Report it as
            # the timeout it is, with the output the command got out first.
            return _runtime_failure(
                "timeout",
                f"substrate provider: {what} timed out after {timeout_s}s (killed by the guest)",
                stdout=result.stdout,
                stderr=result.stderr,
            )
        return result

    async def _user_switch_for(self, handle: SandboxHandle) -> _UserSwitch | SandboxExecResult:
        """The sandbox's user switch, probed once; a probe the sandbox could not
        run is returned as its failure result and not cached. The cached switch
        holds for the sandbox's life: a setup command that installs or unlocks
        sudo later does not change it."""
        switch = self._user_switch.get(handle.sandbox_id)
        if switch is not None:
            return switch
        probe = await self._run(
            handle, ["sh", "-c", _USER_PROBE], cwd="", env={},
            guest_timeout=_PROBE_TIMEOUT_S, timeout_s=_PROBE_TIMEOUT_S, what="user probe",
        )
        if probe.error_type:
            return probe
        switch = self._user_switch[handle.sandbox_id] = _parse_user_probe(probe.stdout)
        return switch

    @staticmethod
    def _sandbox_failure(handle: SandboxHandle, exc: EnvError) -> str:
        if isinstance(exc, NotFoundError):
            return f"substrate env {handle.sandbox_id!r} does not exist: {_describe(exc)}"
        return f"substrate env {handle.sandbox_id!r}: {_describe(exc)}"

    async def upload_file(self, handle: SandboxHandle, source_path: Path, target_path: str) -> None:
        cancelled = False
        try:
            async with self._in_flight(handle):
                await self._env(handle).write_file(
                    target_path, Path(source_path).read_bytes(), mode=0o644
                )
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            await self._park_when_idle(handle, wait=not cancelled)

    async def download_file(self, handle: SandboxHandle, source_path: str, target_path: Path) -> None:
        cancelled = False
        try:
            async with self._in_flight(handle):
                content = await self._env(handle).read_file_bytes(source_path)
            target = Path(target_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            await self._park_when_idle(handle, wait=not cancelled)

    async def status(self, handle: SandboxHandle) -> SandboxStatus:
        # GetEnvironment reads the actor's state without touching the guest,
        # so asking does not wake a parked sandbox.
        raw = handle.raw or {}
        try:
            info = await asyncio.wait_for(
                self._ate().get(handle.sandbox_id, atespace=self._atespace_of(raw)),
                timeout=self._connection.request_timeout_s,
            )
        except NotFoundError:
            return SandboxStatus.STOPPED
        except (EnvError, asyncio.TimeoutError):
            return SandboxStatus.UNKNOWN
        return _STATUS_MAP.get(info.status, SandboxStatus.UNKNOWN)

    async def pause(self, handle: SandboxHandle) -> None:
        """Park the sandbox now (nemo-gym's ``SupportsSandboxPauseResume``).

        Requests in flight finish first: a checkpoint under a running command
        would cut its reply. The park uses the policy's mode, or
        ``SuspendMode.LOCAL`` under the ``never`` policy (falling back to
        ``SuspendMode.DURABLE`` once an older API refuses local mode), and
        returns once the control plane reports the sandbox parked. Raises
        ``RuntimeError`` when requests are still in flight after
        ``connection.request_timeout_s``, when the sandbox is gone, when the
        park RPC fails, or when it did not settle parked.
        """
        state = self._parking_state(handle)
        self._cancel_pending_park(state)
        mode = self._park_mode or (
            SuspendMode.DURABLE if self._local_refused else SuspendMode.LOCAL
        )
        deadline = time.monotonic() + self._connection.request_timeout_s
        while True:
            async with state.lock:
                if not state.inflight:
                    # The flag alone is not enough: traffic through endpoint()
                    # wakes the sandbox on the server without a guest call
                    # through this provider, thus confirm with the control plane.
                    if state.parked and await self._parked_on_server(handle):
                        return
                    state.parked = False
                    try:
                        status = await self._park(handle, mode)
                    except EnvError as exc:
                        raise RuntimeError(
                            f"substrate env {handle.sandbox_id!r}: {_describe(exc)}"
                        ) from exc
                    except asyncio.TimeoutError as exc:
                        raise RuntimeError(
                            f"substrate env {handle.sandbox_id!r}: no answer from "
                            f"{self._connection.api_url} within {self._connection.request_timeout_s}s"
                        ) from exc
                    state.parked = status in _PARKED
                    break
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"substrate env {handle.sandbox_id!r} still has requests in flight; not pausing"
                )
            await asyncio.sleep(_PAUSE_DRAIN_POLL_S)
        if status is None:
            raise RuntimeError(f"substrate env {handle.sandbox_id!r} does not exist")
        if not state.parked:
            raise RuntimeError(
                f"substrate env {handle.sandbox_id!r} is {_status_name(status)} after pause()"
            )

    async def _parked_on_server(self, handle: SandboxHandle) -> bool:
        """Whether the control plane still reports the sandbox parked.

        ``GetEnvironment`` reads the actor's state without touching the guest.
        A sandbox that is gone or that does not answer counts as not parked,
        thus ``pause()`` goes on to ``_park``, which raises for it.
        """
        raw = handle.raw or {}
        try:
            info = await asyncio.wait_for(
                self._ate().get(handle.sandbox_id, atespace=self._atespace_of(raw)),
                timeout=self._connection.request_timeout_s,
            )
        except (EnvError, asyncio.TimeoutError):
            return False
        return info.status in _PARKED

    async def resume(self, handle: SandboxHandle) -> None:
        """Wake a parked sandbox (``SupportsSandboxPauseResume``): one guest
        probe, which ate-env-api answers once the sandbox runs again. No call
        needs this first, since every guest call wakes the sandbox the same
        way; it is for a caller that wants the wake paid before its next step.
        A pending idle park is dropped; the sandbox parks again once a later
        call leaves it idle. Raises ``RuntimeError`` for a sandbox that is
        gone or does not answer within ``create.ready_timeout_s``.
        """
        async with self._in_flight(handle):
            await self._wait_ready(
                self._env(handle),
                self._create.ready_timeout_s,
                error=RuntimeError,
                stop_on_not_found=True,
            )

    async def serialize_handle(
        self, handle: SandboxHandle, *, scope: str | None = None
    ) -> dict[str, Any]:
        """A descriptor ``connect`` rebuilds the handle from, in any process with
        this provider's connection config (nemo-gym's ``ConnectableProvider``).

        An environment is reachable by id and atespace, thus no sandbox server
        is needed to share one. ``scope`` is ignored: ate-env has no leases.
        The descriptor leaves out ``SandboxSpec.env``, which can hold secrets;
        a borrower passes its own ``env`` to each exec, as Gym's Harbor server
        does with ``SandboxAccess.env``.

        From here on this process does not park the sandbox when it is idle,
        because the borrower's calls do not pass through this provider; an
        explicit ``pause()`` still parks it."""
        state = self._parking_state(handle)
        state.shared = True
        self._cancel_pending_park(state)
        raw = handle.raw or {}
        return {
            "sandbox_id": handle.sandbox_id,
            "atespace": self._atespace_of(raw),
            "workdir": raw.get("workdir"),
            "ports": list(raw.get("ports") or ()),
        }

    async def connect(self, descriptor: Mapping[str, Any]) -> SandboxHandle:
        """Rebuild a handle from a ``serialize_handle`` descriptor (nemo-gym's
        ``ConnectableProvider``).

        Checks with the control plane that the environment exists and is not
        crashed or being deleted; a parked one is fine, as the next guest call
        wakes it. This process never parks it when idle. Raises ``ValueError``
        for a descriptor without ``sandbox_id`` and ``RuntimeError`` for an
        environment that cannot be used."""
        sandbox_id = descriptor.get("sandbox_id")
        if not isinstance(sandbox_id, str) or not sandbox_id:
            raise ValueError(f"substrate descriptor needs a sandbox_id, got {descriptor!r}")
        atespace = str(descriptor.get("atespace") or self._create.atespace)
        ports = _validate_ports(descriptor.get("ports"))
        try:
            info = await asyncio.wait_for(
                self._ate().get(sandbox_id, atespace=atespace),
                timeout=self._connection.request_timeout_s,
            )
        except NotFoundError as exc:
            raise RuntimeError(
                f"substrate env {atespace}/{sandbox_id} does not exist: {_describe(exc)}"
            ) from exc
        except EnvError as exc:
            raise RuntimeError(
                f"connecting to substrate env {atespace}/{sandbox_id}: {_describe(exc)}"
            ) from exc
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                f"connecting to substrate env {atespace}/{sandbox_id}: no answer from "
                f"{self._connection.api_url} within {self._connection.request_timeout_s}s"
            ) from exc
        if info.status in (EnvironmentStatus.CRASHED, EnvironmentStatus.DELETING):
            raise RuntimeError(
                f"substrate env {atespace}/{sandbox_id} is {_status_name(info.status)}"
            )
        handle = SandboxHandle(
            sandbox_id=sandbox_id,
            provider_name=self.name,
            raw={
                "env_id": sandbox_id,
                "atespace": atespace,
                "workdir": descriptor.get("workdir"),
                "env": {},
                "ports": ports,
            },
        )
        self._parking_state(handle).shared = True
        return handle

    async def endpoint(self, handle: SandboxHandle, port: int) -> SandboxEndpoint:
        """Resolve a declared service port to a caller-reachable endpoint
        (nemo-gym's ``SupportsSandboxEndpoint``).

        The service is a daemon inside the sandbox, typically one a runtime
        layer on the base template mounts into the task image and the guest
        starts next to itself (see README). The endpoint is ate-env-api's port
        proxy; its headers route each plain HTTP request through the Substrate
        router to ``port`` in this sandbox. Nothing here touches the guest: a
        parked sandbox is resumed by the first request through the proxy. The
        router holds that request up to about 10 s while the sandbox wakes; a
        longer wake, usually only the first one on a new image, gets 503 with
        ``Retry-After`` and ``X-Env-State``, and the caller retries. Only the
        sandbox's existence is checked, from the control plane, so a deleted
        or crashed sandbox raises ``RuntimeError`` here rather than on the
        first request.
        """
        raw = handle.raw or {}
        declared = list(raw.get("ports") or ())
        if isinstance(port, bool) or port not in declared:
            raise ValueError(
                f"Sandbox port {port!r} was not declared in SandboxSpec.ports; "
                f"declared ports: {declared!r}"
            )
        atespace = self._atespace_of(raw)
        try:
            info = await asyncio.wait_for(
                self._ate().get(handle.sandbox_id, atespace=atespace),
                timeout=self._connection.request_timeout_s,
            )
        except NotFoundError as exc:
            raise RuntimeError(f"substrate env {handle.sandbox_id!r} does not exist") from exc
        except EnvError as exc:
            raise RuntimeError(
                f"substrate env {handle.sandbox_id!r}: {_describe(exc)}"
            ) from exc
        except TimeoutError as exc:
            raise RuntimeError(
                f"substrate env {handle.sandbox_id!r}: no answer from "
                f"{self._connection.api_url} within {self._connection.request_timeout_s}s"
            ) from exc
        if info.status == EnvironmentStatus.CRASHED:
            raise RuntimeError(f"substrate env {handle.sandbox_id!r} crashed")
        headers = {
            _ENV_ID_HEADER: handle.sandbox_id,
            _ENV_ATESPACE_HEADER: atespace,
            _ENV_PORT_HEADER: str(port),
        }
        if self._auth_token:
            headers[_ENV_AUTHORIZATION_HEADER] = f"Bearer {self._auth_token}"
        return SandboxEndpoint(endpoint=self._connection.endpoint_base(), headers=headers)

    async def close(self, handle: SandboxHandle) -> None:
        atespace = self._atespace_of(handle.raw or {})
        self._user_switch.pop(handle.sandbox_id, None)
        state = self._parking.pop(handle.sandbox_id, None)
        if state is None:
            await self._best_effort_delete(handle.sandbox_id, atespace)
            return
        # A park still waiting out its delay is dropped; one that reached the
        # API holds the lock until the sandbox settles, and deleting under it
        # would only collide with it.
        self._cancel_pending_park(state)
        async with state.lock:
            await self._best_effort_delete(handle.sandbox_id, atespace)

    async def aclose(self) -> None:
        # A park in flight must finish before its client goes away, or the
        # sandbox is left mid-transition with nothing waiting for it. Loop until
        # any RPC tasks spawned while draining have finished too.
        while pending := tuple(t for t in self._park_tasks if not t.done()):
            await asyncio.gather(*pending, return_exceptions=True)
        if self._client is not None and self._owns_client:
            await self._client.close()
        if self._channel is not None:
            await self._channel.close()
        self._client = None if self._owns_client else self._client
        self._channel = None

    # -- internals ----------------------------------------------------------

    def _ate(self) -> Client:
        if self._client is None:
            self._client = self._open_client(self._connection.api_url)
        return self._client

    def _open_client(self, api_url: str) -> Client:
        url = urlparse(api_url if "://" in api_url else f"//{api_url}")
        if url.scheme == "https":
            # ate_env_rl.Client only dials h2c itself; hand it a TLS channel.
            target = f"{url.hostname}:{url.port or 443}"
            self._channel = grpc.aio.secure_channel(target, grpc.ssl_channel_credentials())
            return Client(channel=self._channel, auth_token=self._auth_token)
        return Client(api_url, auth_token=self._auth_token)

    def _atespace_of(self, raw: Mapping[str, Any]) -> str:
        return str(raw.get("atespace") or self._create.atespace)

    def _env(self, handle: SandboxHandle) -> Env:
        return self._ate().env(handle.sandbox_id, atespace=self._atespace_of(handle.raw or {}))

    def _resolve_template(
        self, spec: SandboxSpec, opts: SubstrateProviderOptions
    ) -> tuple[str, str | None]:
        """Return ``(template, rootfs_image)`` for a spec.

        An explicit provider option or an ``image_templates`` entry names a
        registered template. Any other image goes to ate-env-api as
        ``rootfs_image``: the API derives a template from the base on first
        use, with the guest mounted into the image as a read-only volume, and
        reuses it afterwards. The image must be digest-pinned; the API rejects
        tags.
        """
        if opts.template:
            return opts.template, None
        if spec.image:
            mapped = (self._create.image_templates or {}).get(spec.image)
            if mapped:
                return mapped, None
            return self._create.template, spec.image
        return self._create.template, None

    async def _wait_ready(
        self,
        env: Env,
        timeout_s: float,
        *,
        error: type[Exception] = SandboxCreateVerificationError,
        stop_on_not_found: bool = False,
    ) -> None:
        """Poll a trivial command until the guest answers it. A fresh actor may
        still be booting, and a guest call made before it serves fails with a
        transport-level error; both are retried until the deadline, then
        ``error`` is raised. The first call also resumes a parked actor. With
        ``stop_on_not_found``, a sandbox that no longer exists raises ``error``
        at once instead of after the deadline."""
        deadline = time.monotonic() + timeout_s
        last_error = "no probe attempted"
        while True:
            try:
                result = await asyncio.wait_for(env.shell("true"), timeout=_PROBE_TIMEOUT_S)
                if result.exit_code == 0:
                    return
                last_error = f"probe exited {result.exit_code}: {result.stderr.strip()[:200]}"
            except NotFoundError as exc:
                if stop_on_not_found:
                    raise error(f"substrate env {env.id!r} does not exist") from exc
                last_error = _describe(exc)
            except EnvError as exc:
                last_error = _describe(exc)
            except asyncio.TimeoutError:
                last_error = f"probe did not answer within {_PROBE_TIMEOUT_S}s"
            if time.monotonic() >= deadline:
                break
            await asyncio.sleep(self._create.ready_poll_interval_s)
        raise error(f"substrate env {env.id!r} not ready after {timeout_s}s: {last_error}")

    # -- parking ------------------------------------------------------------

    def _parking_state(self, handle: SandboxHandle) -> _Parking:
        state = self._parking.get(handle.sandbox_id)
        if state is None:
            state = self._parking[handle.sandbox_id] = _Parking()
        return state

    @contextlib.asynccontextmanager
    async def _in_flight(self, handle: SandboxHandle):
        """Hold a request slot on the sandbox.

        A park still waiting out its idle delay is dropped; one under way
        finishes first, because ate-env-api cannot wake a sandbox that is
        mid-transition. While the slot is held the sandbox is never parked.
        """
        state = self._parking_state(handle)
        self._cancel_pending_park(state)
        async with state.lock:
            state.inflight += 1
            state.parked = False  # the guest call wakes it
        try:
            yield
        finally:
            state.inflight -= 1

    @staticmethod
    def _cancel_pending_park(state: _Parking) -> None:
        """Drop a park that has not passed its idle wait. One past it is left
        to finish; a caller then waits on the state's lock instead."""
        if state.task is not None and not state.task.done() and state.parking == 0:
            state.task.cancel()
            state.task = None

    async def _park_when_idle(self, handle: SandboxHandle, *, wait: bool = True) -> None:
        """Free the sandbox's worker once it has been idle for the configured
        delay. Nothing resumes it explicitly: the next guest call goes through
        ate-env-api, which wakes it. Sandboxes declaring ``SandboxSpec.ports``
        are skipped because HTTP traffic through ``endpoint()`` bypasses the
        provider's in-flight tracking and would race with an idle park. Best
        effort: a park that fails leaves the sandbox running, which costs
        density, not correctness."""
        if self._park_mode is None or (handle.raw or {}).get("ports"):
            return
        state = self._parking_state(handle)
        if state.shared:
            return
        if state.inflight:
            # Another request holds the sandbox; the last one to finish
            # schedules the park.
            return
        self._cancel_pending_park(state)
        # Always a task of its own, shielded from the caller: a suspend
        # cancelled client-side would leave the sandbox mid-transition on the
        # server with nothing waiting for it.
        task = asyncio.create_task(self._park_after(handle, state, self._create.idle_before_suspend_s))
        state.task = task
        self._park_tasks.add(task)
        task.add_done_callback(self._park_tasks.discard)
        if wait and self._create.idle_before_suspend_s <= 0:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                if task.cancelled():
                    return  # a newer request superseded the park; the result stands
                raise

    async def _park_after(self, handle: SandboxHandle, state: _Parking, delay: float) -> None:
        try:
            if delay > 0:
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        state.parking += 1
        try:
            async with state.lock:
                if state.inflight or state.parked or self._park_mode is None:
                    return
                status = await self._park(handle, self._park_mode)
                state.parked = status in _PARKED
        except Exception as exc:  # noqa: BLE001 - a background task must not die silently
            logger.warning("parking substrate env %s failed: %s", handle.sandbox_id, _describe(exc))
        finally:
            state.parking -= 1

    async def _park(self, handle: SandboxHandle, mode: SuspendMode) -> EnvironmentStatus | None:
        """Suspend the sandbox with ``mode`` and wait out the transition.

        Returns the lifecycle status it settled in, or None when it no longer
        exists. The caller holds the sandbox's parking lock. The suspend RPC
        itself is never cancelled from here: a deadline overrun stops the
        wait, not the checkpoint, and the state is read instead.
        """
        env_id = handle.sandbox_id
        atespace = self._atespace_of(handle.raw or {})
        rpc = asyncio.create_task(self._ate().suspend(env_id, atespace=atespace, mode=mode))
        self._park_tasks.add(rpc)
        rpc.add_done_callback(self._park_tasks.discard)
        rpc.add_done_callback(lambda t: t.cancelled() or t.exception())  # never "never retrieved"
        try:
            await asyncio.wait_for(asyncio.shield(rpc), timeout=self._connection.request_timeout_s)
        except InvalidArgumentError as exc:
            if mode is not SuspendMode.LOCAL:
                raise
            # An ate-env-api that predates SuspendEnvironment's mode refuses
            # the field. Park durably from now on, and say so once.
            if not self._local_refused:
                self._local_refused = True
                logger.warning(
                    "ate-env-api at %s does not serve suspend_mode (%s); parking durably instead "
                    "of locally from now on",
                    self._connection.api_url,
                    _describe(exc),
                )
            if self._park_mode is SuspendMode.LOCAL:
                self._park_mode = SuspendMode.DURABLE
            return await self._park(handle, SuspendMode.DURABLE)
        except NotFoundError:
            return None
        except FailedPreconditionError:
            pass  # not RUNNING: parked already, by an earlier call or by the platform
        except asyncio.TimeoutError:
            logger.warning(
                "suspending substrate env %s outran %.0fs; waiting for its state instead",
                env_id,
                self._connection.request_timeout_s,
            )
        deadline = time.monotonic() + _PARK_SETTLE_S
        while True:
            call_timeout = min(
                self._connection.request_timeout_s,
                max(_PARK_POLL_S, deadline - time.monotonic()),
            )
            try:
                info = await asyncio.wait_for(
                    self._ate().get(env_id, atespace=atespace),
                    timeout=call_timeout,
                )
            except NotFoundError:
                return None
            except (EnvError, asyncio.TimeoutError):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(_PARK_POLL_S)
                continue
            status = info.status
            if status not in _PARKING:
                if status not in _PARKED:
                    logger.warning(
                        "parking substrate env %s (%s) left it %s", env_id, mode.name, _status_name(status)
                    )
                return status
            if time.monotonic() >= deadline:
                logger.warning(
                    "substrate env %s is still %s after %.0fs; leaving it to the control plane, "
                    "the next park retries",
                    env_id,
                    _status_name(status),
                    _PARK_SETTLE_S,
                )
                return status
            await asyncio.sleep(_PARK_POLL_S)

    @staticmethod
    async def _collect(process: Process) -> SandboxExecResult:
        stdout = bytearray()
        stderr = bytearray()
        final = None
        async for chunk in process.output(follow=True):
            if chunk.stdout is not None:
                stdout.extend(chunk.stdout)
            elif chunk.stderr is not None:
                stderr.extend(chunk.stderr)
            elif chunk.exit is not None:
                final = chunk.exit
        if final is None:
            # The stream can end before the exit message; wait() polls for it.
            final = await process.wait()
        return SandboxExecResult(
            stdout=stdout.decode("utf-8", errors="replace"),
            stderr=stderr.decode("utf-8", errors="replace"),
            return_code=final.exit_code,
        )

    async def _best_effort_delete(self, env_id: str, atespace: str) -> None:
        try:
            await asyncio.wait_for(
                self._ate().delete(env_id, atespace=atespace),
                timeout=self._connection.request_timeout_s,
            )
        except NotFoundError:
            pass  # already gone: close() is idempotent
        except (EnvError, asyncio.TimeoutError) as exc:
            logger.warning("deleting substrate env %s: %s", env_id, _describe(exc))

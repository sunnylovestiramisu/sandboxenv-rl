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

"""Async client for the ate-env-api service."""

from __future__ import annotations

import collections
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, nullcontext
from datetime import timedelta
from typing import Any

import grpc
import grpc.aio

from ._gen.ateenv.v1alpha import env_pb2, env_pb2_grpc, guest_pb2_grpc
from .config import TracerConfig
from .env import Env
from .errors import SandboxProtocolError, map_rpc_error
from .telemetry import metrics
from .telemetry.trace_recorder import (
    annotate_span,
    current_warm_claim,
    in_driver_span,
    is_measure_mode,
    register_env_meta,
)
from .telemetry.tracing import (
    CLIENT_SPAN_PREFIX,
    _trace_interceptors,
    image_ref_attr,
    rpc_attrs,
    start_span,
    tracer_from_config,
)
from .types import (
    EnvironmentInfo,
    ImageStatus,
    PlacementDomain,
    ServerInfo,
    SuspendMode,
    WarmImageCoverage,
    WarmProbe,
    _environment_info_from_pb,
    _image_status_from_pb,
    _placement_domain_from_pb,
    _server_info_from_pb,
    _warm_coverage_from_pb,
    _warm_probe_from_pb,
)

__all__ = ["Client", "DEFAULT_ATESPACE"]

DEFAULT_ATESPACE = "ate-env"


def _normalize_target(endpoint: str) -> str:
    target = endpoint.rstrip("/")
    for prefix in ("http://", "https://"):
        if target.startswith(prefix):
            target = target[len(prefix) :]
    return target


_ENVIRONMENT_SERVICE = "ateenv.v1alpha.EnvironmentService"


def _env_rpc_attrs(method: str, target: str | None) -> dict[str, object]:
    return rpc_attrs(_ENVIRONMENT_SERVICE, method, target)


class _ClientCallDetails(
    collections.namedtuple(
        "_ClientCallDetails",
        ["method", "timeout", "metadata", "credentials", "wait_for_ready"],
    ),
    grpc.aio.ClientCallDetails,
):
    pass


class _BaseAuthInterceptor:
    def __init__(self, token: str):
        self._token = token

    def _inject(self, details: grpc.aio.ClientCallDetails) -> _ClientCallDetails:
        header = ("authorization", f"Bearer {self._token}")
        md = list(details.metadata or [])
        if not any(k.lower() == "authorization" for k, _ in md):
            md.append(header)
        return _ClientCallDetails(
            details.method,
            details.timeout,
            md,
            details.credentials,
            details.wait_for_ready,
        )


class _UnaryUnaryAuthInterceptor(_BaseAuthInterceptor, grpc.aio.UnaryUnaryClientInterceptor):
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        return await continuation(self._inject(client_call_details), request)


class _UnaryStreamAuthInterceptor(_BaseAuthInterceptor, grpc.aio.UnaryStreamClientInterceptor):
    async def intercept_unary_stream(self, continuation, client_call_details, request):
        return await continuation(self._inject(client_call_details), request)


class _StreamUnaryAuthInterceptor(_BaseAuthInterceptor, grpc.aio.StreamUnaryClientInterceptor):
    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return await continuation(self._inject(client_call_details), request_iterator)


class _StreamStreamAuthInterceptor(_BaseAuthInterceptor, grpc.aio.StreamStreamClientInterceptor):
    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return await continuation(self._inject(client_call_details), request_iterator)


def _auth_interceptors(token: str) -> list[grpc.aio.ClientInterceptor]:
    # grpc.aio.Channel.__init__ dispatches each interceptor with if/elif across
    # the four base classes, so a single multi-inheritance instance only
    # registers for UnaryUnary. Separate instances cover all four call shapes.
    return [
        _UnaryUnaryAuthInterceptor(token),
        _UnaryStreamAuthInterceptor(token),
        _StreamUnaryAuthInterceptor(token),
        _StreamStreamAuthInterceptor(token),
    ]


class Client:
    """Manages environments against an ate-env-api endpoint over gRPC.

    A single Client multiplexes concurrent operations over one HTTP/2
    channel and is safe to share across tasks; create one, share it, and
    call close() on shutdown:

        client = Client("localhost:7777")
        try:
            env = await client.create("dev1")
            result = await env.shell("echo hello")
        finally:
            await client.close()
    """

    def __init__(
        self,
        endpoint: str = "localhost:7777",
        *,
        channel: grpc.aio.Channel | None = None,
        auth_token: str | None = None,
        tracer_config: TracerConfig | None = None,
        channel_options: Sequence[tuple[str, Any]] | None = None,
        credentials: grpc.ChannelCredentials | None = None,
    ):
        """Create a client for the ate-env-api service at endpoint.

        endpoint accepts a host:port or an http(s):// URL, matching the Go
        client. Pass channel to supply a caller-owned grpc.aio.Channel
        instead; the client then never closes it.

        channel_options are gRPC channel arguments for a channel the client
        creates (for example ``("grpc.use_local_subchannel_pool", 1)`` to
        give the channel its own TCP connection). They are ignored when
        channel is given.

        credentials makes the client's own channel use TLS, for example
        ``grpc.ssl_channel_credentials()``. Without it the channel is
        plaintext, even for an https:// endpoint, matching the Go client.
        Passing both channel and credentials raises ValueError: a
        caller-owned channel already has its credentials.

        tracer_config enables OpenTelemetry spans (``ate_env_client.<op>``)
        and, on a channel the client owns, the W3C trace context in the
        metadata of every call. It needs the ``tracing`` extra and is a
        logged no-op without it.
        """
        self.endpoint = endpoint
        self._auth_token = auth_token or None
        self.tracer_config = tracer_config or TracerConfig()
        self.tracer = tracer_from_config(self.tracer_config)
        if channel is not None and credentials is not None:
            raise ValueError("ate_env_rl: pass either channel or credentials, not both")
        # host:port of a channel the client creates; None for a caller-owned one.
        self._target = None if channel is not None else _normalize_target(endpoint or "")
        if channel is not None:
            self._channel = channel
            self._owns_channel = False
        else:
            if not endpoint:
                raise ValueError("ate_env_rl: endpoint is required when channel is not given")
            interceptors = _auth_interceptors(auth_token) if auth_token else []
            if self.tracer:
                # After auth, so the trace context rides on the final metadata.
                interceptors += _trace_interceptors()
            target = _normalize_target(endpoint)
            options = list(channel_options) if channel_options else None
            if credentials is not None:
                self._channel = grpc.aio.secure_channel(
                    target, credentials, options=options, interceptors=interceptors or None
                )
            else:
                self._channel = grpc.aio.insecure_channel(
                    target, options=options, interceptors=interceptors or None
                )
            self._owns_channel = True
        self._environments = env_pb2_grpc.EnvironmentServiceStub(self._channel)
        self._processes = guest_pb2_grpc.ProcessServiceStub(self._channel)
        self._filesystem = guest_pb2_grpc.FileSystemServiceStub(self._channel)

    def _auth_metadata(self) -> tuple[tuple[str, str], ...]:
        # Sent on each call as well as by the interceptors: a caller-owned
        # channel has no interceptors, and the interceptors skip a call that
        # already carries authorization, so the header is never doubled.
        if not self._auth_token:
            return ()
        return (("authorization", f"Bearer {self._auth_token}"),)

    def _rpc_span(
        self, op: str, method: str, attributes: Callable[[], dict[str, Any]]
    ) -> AbstractContextManager[Any]:
        """The ``ate_env_client.<op>`` CLIENT span of one EnvironmentService call.

        attributes is called only when tracing is on, so with tracing off no
        attribute dict, image reference or rpc.* value is built.
        """
        if not self.tracer:
            return nullcontext()
        return start_span(
            self.tracer,
            f"{CLIENT_SPAN_PREFIX}.{op}",
            {**attributes(), **_env_rpc_attrs(method, self._target)},
            client_kind=True,
        )

    async def close(self, grace: float | None = None) -> None:
        """Release the client's resources (closes the channel if owned).

        With ``grace``, calls in progress get up to that many seconds to
        finish; without it they are cancelled. New calls fail at once.
        """
        if self._owns_channel:
            await self._channel.close(grace)

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def create(
        self,
        id: str,
        *,
        atespace: str = DEFAULT_ATESPACE,
        template_name: str | None = None,
        template_atespace: str | None = None,
        rootfs_image: str | None = None,
        worker_selector: Mapping[str, str] | None = None,
    ) -> Env:
        """Register and start a new environment; returns a handle to it.

        The server fills defaults for the template (name "default-template" in
        atespace "ate-env") when none is given.

        With rootfs_image (a digest-pinned OCI reference, repo@sha256:...), the
        environment runs that image unmodified: the server derives an
        ActorTemplate from the template, which then acts as the base, with the
        image as the container and the ate-env-guest mounted into it as an
        image volume. The derived template is created on first use and reused
        for later environments on the same image; info() reports its name.

        worker_selector names labels the worker pool must carry (exact
        equality). Substrate ANDs it with the template's own selector, so it
        narrows the pools the environment can run on and never widens them;
        it also applies to the environment's later wakes. get() reports it,
        with the node the environment runs on.
        """
        req = env_pb2.CreateEnvironmentRequest(
            id=id, atespace=atespace, rootfs_image=rootfs_image or ""
        )
        if worker_selector:
            req.worker_selector.update(worker_selector)
        if template_name or template_atespace:
            req.template.name = template_name or ""
            req.template.atespace = template_atespace or ""
        with self._rpc_span(
            "create_environment",
            "CreateEnvironment",
            lambda: {
                "ate.actor.name": id,
                "ate.atespace": atespace,
                "ate.template.name": template_name,
                "ate.template.atespace": template_atespace,
                "ate_env.rootfs_image": image_ref_attr(rootfs_image),
            },
        ) as span:
            if span is not None:
                annotate_span(
                    span,
                    op="create",
                    env=id,
                    image=rootfs_image or template_name,
                )
                register_env_meta(id, image=rootfs_image or template_name)
            try:
                if is_measure_mode() and not in_driver_span.get():
                    with metrics.timed("acquire"):
                        resp = await self._environments.CreateEnvironment(
                            req, metadata=self._auth_metadata()
                        )
                else:
                    resp = await self._environments.CreateEnvironment(
                        req, metadata=self._auth_metadata()
                    )
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
        created = resp.environment
        return self.env(created.id, atespace=created.atespace)

    async def get(self, id: str, *, atespace: str = DEFAULT_ATESPACE) -> EnvironmentInfo:
        """Retrieve the status and configuration of an environment."""
        req = env_pb2.GetEnvironmentRequest(id=id, atespace=atespace)
        with self._rpc_span(
            "get_environment",
            "GetEnvironment",
            lambda: {"ate.actor.name": id, "ate.atespace": atespace},
        ) as span:
            if span is not None:
                annotate_span(
                    span,
                    op="wake" if current_warm_claim.get() else "get",
                    env=id,
                )
            try:
                if is_measure_mode() and not in_driver_span.get():
                    with metrics.timed("warm_claim"):
                        resp = await self._environments.GetEnvironment(
                            req, metadata=self._auth_metadata()
                        )
                else:
                    resp = await self._environments.GetEnvironment(
                        req, metadata=self._auth_metadata()
                    )
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
        return _environment_info_from_pb(resp.environment)

    async def suspend(
        self,
        id: str,
        *,
        atespace: str = DEFAULT_ATESPACE,
        mode: SuspendMode = SuspendMode.DURABLE,
    ) -> None:
        """Checkpoint and stop the environment.

        ``mode`` says when the call returns: ``SuspendMode.DURABLE`` (the
        default) once the snapshot is in object storage, ``SuspendMode.LOCAL``
        once it is on the node, with the upload in the background (see
        ``SuspendMode``). The request carries the mode only for ``LOCAL``, so
        a durable suspend works against an ate-env-api that predates the
        field; such a server refuses ``LOCAL`` with ``InvalidArgumentError``,
        because it rejects fields it does not know. ``server_info().features``
        lists ``suspend_mode`` once the server serves it.

        The mode applies when the call checkpoints a running environment. An
        environment that is already SUSPENDED, or PAUSED with its upload still
        pending, is left as it is: a DURABLE suspend of a PAUSED environment
        returns at once and does not wait for the upload.
        """
        mode = SuspendMode(mode)
        req = env_pb2.SuspendEnvironmentRequest(id=id, atespace=atespace)
        if mode is SuspendMode.LOCAL:
            req.mode = env_pb2.SUSPEND_MODE_LOCAL
        with (
            self._rpc_span(
                "suspend_environment",
                "SuspendEnvironment",
                lambda: {"ate.actor.name": id, "ate.atespace": atespace},
            ) as span,
            metrics.timed("suspend"),
        ):
            if span is not None:
                annotate_span(span, op="suspend", env=id)
            try:
                await self._environments.SuspendEnvironment(req, metadata=self._auth_metadata())
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e

    async def delete(self, id: str, *, atespace: str = DEFAULT_ATESPACE) -> None:
        """Remove the environment permanently."""
        req = env_pb2.DeleteEnvironmentRequest(id=id, atespace=atespace)
        with self._rpc_span(
            "delete_environment",
            "DeleteEnvironment",
            lambda: {"ate.actor.name": id, "ate.atespace": atespace},
        ) as span:
            if span is not None:
                annotate_span(span, op="delete", env=id)
            try:
                if is_measure_mode() and not in_driver_span.get():
                    with metrics.timed("release"):
                        await self._environments.DeleteEnvironment(
                            req, metadata=self._auth_metadata()
                        )
                else:
                    await self._environments.DeleteEnvironment(
                        req, metadata=self._auth_metadata()
                    )
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e

    async def server_info(self) -> ServerInfo:
        """Return the server's implementation, version and optional features.

        A server that predates GetServerInfo raises RpcError with code
        UNIMPLEMENTED.
        """
        with self._rpc_span("get_server_info", "GetServerInfo", dict):
            try:
                resp = await self._environments.GetServerInfo(
                    env_pb2.GetServerInfoRequest(), metadata=self._auth_metadata()
                )
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
        return _server_info_from_pb(resp)

    async def image_status(
        self,
        rootfs_image: str,
        *,
        atespace: str | None = None,
        template_name: str | None = None,
        template_atespace: str | None = None,
        ensure: bool = False,
    ) -> ImageStatus:
        """Report the template derived for a rootfs image and its golden snapshot.

        The template defaults as in create(); the atespace defaults to the
        template's, else to "ate-env", when left None. With ensure=True the
        derived template is created when it is missing, which starts the
        golden snapshot bake; no environment is created. Poll until
        ``status.golden`` is READY before measuring first boots: environments
        created while it is PENDING boot from the image instead; NONE with an
        existing template means the template disables golden snapshots.
        """
        template = None
        if template_name or template_atespace:
            template = env_pb2.Template(name=template_name or "", atespace=template_atespace or "")
        op, method = ("ensure_image_template", "EnsureImageTemplate") if ensure else ("get_image_status", "GetImageStatus")
        with self._rpc_span(
            op,
            method,
            lambda: {
                "ate_env.rootfs_image": image_ref_attr(rootfs_image),
                "ate.template.name": template_name,
                "ate.template.atespace": template_atespace,
                "ate.atespace": atespace,
            },
        ):
            try:
                if ensure:
                    resp = await self._environments.EnsureImageTemplate(
                        env_pb2.EnsureImageTemplateRequest(
                            rootfs_image=rootfs_image, template=template, atespace=atespace or ""
                        ),
                        metadata=self._auth_metadata(),
                    )
                else:
                    resp = await self._environments.GetImageStatus(
                        env_pb2.GetImageStatusRequest(
                            rootfs_image=rootfs_image, template=template, atespace=atespace or ""
                        ),
                        metadata=self._auth_metadata(),
                    )
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
        return _image_status_from_pb(resp)

    async def list_placement_domains(
        self,
        *,
        template_name: str | None = None,
        template_atespace: str | None = None,
        atespace: str | None = None,
    ) -> list[PlacementDomain]:
        """The worker pools a per-environment worker selector can address.

        With ``template_name`` (or an atespace alone, which names the
        server's default template there), only the pools that template's own
        worker selector matches: the ones a ``worker_selector`` for it can
        address, as Substrate ANDs the two. NotFoundError for an unknown
        template. ate-env-api reads the pools from Substrate's workers, which
        takes the global viewer role (``ate-env manifest access-policy
        --global``); PermissionDeniedError otherwise, template or not.
        """
        template = None
        if template_name or template_atespace:
            template = env_pb2.Template(name=template_name or "", atespace=template_atespace or "")
        req = env_pb2.ListPlacementDomainsRequest(template=template, atespace=atespace or "")
        with self._rpc_span(
            "list_placement_domains",
            "ListPlacementDomains",
            lambda: {
                "ate.template.name": template_name,
                "ate.template.atespace": template_atespace,
                "ate.atespace": atespace,
            },
        ):
            try:
                resp = await self._environments.ListPlacementDomains(
                    req, metadata=self._auth_metadata()
                )
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
        return [_placement_domain_from_pb(d) for d in resp.domains]

    async def warm_image(
        self,
        rootfs_image: str,
        *,
        worker_selector: Mapping[str, str] | None = None,
        nodes: int = 0,
        probes_per_node: int = 0,
        max_rounds: int = 0,
        probe_timeout: float | timedelta | None = None,
        run_id: str = "",
        atespace: str | None = None,
        template_name: str | None = None,
        template_atespace: str | None = None,
        on_probe: Callable[[WarmProbe], None] | None = None,
    ) -> WarmImageCoverage:
        """Put a rootfs image on every node of one placement domain.

        ate-env-api creates probe environments with ``worker_selector`` (the
        template's own pools when None), holds each until it runs, so the
        image layers and the golden snapshot are on its node, reads the node,
        deletes the probes, and repeats until every node ran the image or the
        rounds run out; no command runs in the guest. ``on_probe`` sees each
        probe as it settles. ``nodes`` 0 counts the nodes of the workers the
        selector matches, which takes the global viewer role; pass it
        otherwise. The server's defaults apply to the zero knobs: 2 probes
        per node, 3 rounds, a 10 minute probe timeout, a run id of ``warm``
        and a random suffix (unique per call, so two calls never name a probe
        alike). Wait for the golden (``image_status``) first when the run
        relies on it.
        The call lasts for the rounds: minutes for a cold image.
        """
        template = None
        if template_name or template_atespace:
            template = env_pb2.Template(name=template_name or "", atespace=template_atespace or "")
        req = env_pb2.WarmImageRequest(
            rootfs_image=rootfs_image,
            template=template,
            atespace=atespace or "",
            nodes=nodes,
            probes_per_node=probes_per_node,
            max_rounds=max_rounds,
            run_id=run_id,
        )
        if worker_selector:
            req.worker_selector.update(worker_selector)
        if probe_timeout is not None:
            if isinstance(probe_timeout, timedelta):
                req.probe_timeout.FromTimedelta(probe_timeout)
            else:
                req.probe_timeout.FromTimedelta(timedelta(seconds=probe_timeout))
        coverage: WarmImageCoverage | None = None
        with self._rpc_span(
            "warm_image",
            "WarmImage",
            lambda: {
                "ate_env.rootfs_image": image_ref_attr(rootfs_image),
                "ate.template.name": template_name,
                "ate.template.atespace": template_atespace,
                "ate.atespace": atespace,
            },
        ):
            call = self._environments.WarmImage(req, metadata=self._auth_metadata())
            try:
                async for ev in call:
                    if ev.HasField("probe"):
                        if on_probe is not None:
                            on_probe(_warm_probe_from_pb(ev.probe))
                    elif ev.HasField("coverage"):
                        coverage = _warm_coverage_from_pb(ev.coverage)
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
            finally:
                call.cancel()
        if coverage is None:
            raise SandboxProtocolError("WarmImage ended without a coverage")
        return coverage

    def env(self, id: str, *, atespace: str = DEFAULT_ATESPACE) -> Env:
        """Return a handle to an environment without checking that it exists."""
        return Env(self, id, atespace or DEFAULT_ATESPACE)

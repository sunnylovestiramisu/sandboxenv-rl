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
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

import grpc
import grpc.aio

from ._gen.ateenv.v1alpha import env_pb2, env_pb2_grpc, guest_pb2_grpc
from .config import TracerConfig
from .env import Env
from .errors import map_rpc_error
from .telemetry import metrics
from .telemetry.tracing import (
    CLIENT_SPAN_PREFIX,
    _trace_interceptors,
    image_ref_attr,
    rpc_attrs,
    start_span,
    tracer_from_config,
)
from .types import EnvironmentInfo, _environment_info_from_pb

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
    ):
        """Create a client for the ate-env-api service at endpoint.

        endpoint accepts a host:port or an http(s):// URL, matching the Go
        client. Pass channel to supply a caller-owned grpc.aio.Channel
        instead; the client then never closes it.

        tracer_config enables OpenTelemetry spans (``ate_env_client.<op>``)
        and, on a channel the client owns, the W3C trace context in the
        metadata of every call. It needs the ``tracing`` extra and is a
        logged no-op without it.
        """
        self.endpoint = endpoint
        self._auth_token = auth_token or None
        self.tracer_config = tracer_config or TracerConfig()
        self.tracer = tracer_from_config(self.tracer_config)
        # host:port of a channel the client creates; None for a caller-owned one.
        self._target = None if channel is not None else _normalize_target(endpoint or "")
        if channel is not None:
            self._channel = channel
            self._owns_channel = False
        else:
            if not endpoint:
                raise ValueError("ate_env: endpoint is required when channel is not given")
            interceptors = _auth_interceptors(auth_token) if auth_token else []
            if self.tracer:
                # After auth, so the trace context rides on the final metadata.
                interceptors += _trace_interceptors()
            self._channel = grpc.aio.insecure_channel(
                _normalize_target(endpoint), interceptors=interceptors or None
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

    async def close(self) -> None:
        """Release the client's resources (closes the channel if owned)."""
        if self._owns_channel:
            await self._channel.close()

    async def create(
        self,
        id: str,
        *,
        atespace: str = DEFAULT_ATESPACE,
        template_name: str | None = None,
        template_atespace: str | None = None,
        rootfs_image: str | None = None,
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
        """
        req = env_pb2.CreateEnvironmentRequest(id=id, atespace=atespace, rootfs_image=rootfs_image or "")
        if template_name or template_atespace:
            req.template.name = template_name or ""
            req.template.atespace = template_atespace or ""
        with self._rpc_span("create_environment", "CreateEnvironment", lambda: {
            "ate.actor.name": id,
            "ate.atespace": atespace,
            "ate.template.name": template_name,
            "ate.template.atespace": template_atespace,
            "ate_env.rootfs_image": image_ref_attr(rootfs_image),
        }):
            try:
                resp = await self._environments.CreateEnvironment(req, metadata=self._auth_metadata())
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
        created = resp.environment
        return self.env(created.id, atespace=created.atespace)

    async def get(self, id: str, *, atespace: str = DEFAULT_ATESPACE) -> EnvironmentInfo:
        """Retrieve the status and configuration of an environment."""
        req = env_pb2.GetEnvironmentRequest(id=id, atespace=atespace)
        with self._rpc_span("get_environment", "GetEnvironment", lambda: {"ate.actor.name": id, "ate.atespace": atespace}):
            try:
                resp = await self._environments.GetEnvironment(req, metadata=self._auth_metadata())
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e
        return _environment_info_from_pb(resp.environment)

    async def suspend(self, id: str, *, atespace: str = DEFAULT_ATESPACE) -> None:
        """Checkpoint and stop the environment."""
        req = env_pb2.SuspendEnvironmentRequest(id=id, atespace=atespace)
        with (
            self._rpc_span("suspend_environment", "SuspendEnvironment",
                           lambda: {"ate.actor.name": id, "ate.atespace": atespace}),
            metrics.timed("suspend"),
        ):
            try:
                await self._environments.SuspendEnvironment(req, metadata=self._auth_metadata())
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e

    async def delete(self, id: str, *, atespace: str = DEFAULT_ATESPACE) -> None:
        """Remove the environment permanently."""
        req = env_pb2.DeleteEnvironmentRequest(id=id, atespace=atespace)
        with self._rpc_span("delete_environment", "DeleteEnvironment", lambda: {"ate.actor.name": id, "ate.atespace": atespace}):
            try:
                await self._environments.DeleteEnvironment(req, metadata=self._auth_metadata())
            except grpc.RpcError as e:
                raise map_rpc_error(e) from e

    def env(self, id: str, *, atespace: str = DEFAULT_ATESPACE) -> Env:
        """Return a handle to an environment without checking that it exists."""
        return Env(self, id, atespace or DEFAULT_ATESPACE)

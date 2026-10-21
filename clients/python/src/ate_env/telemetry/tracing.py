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

"""OpenTelemetry tracing for the ate_env client, behind the ``tracing`` extra.

A port of agent-sandbox's ``k8s_agent_sandbox/trace_manager.py``: one global
``TracerProvider`` exporting over OTLP (configured by the standard
``OTEL_EXPORTER_OTLP_*`` environment variables), ``{prefix}.{op}`` span names, a
per-client ``TracerManager`` and W3C ``traceparent``/``tracestate`` propagation
(never baggage).

Unlike the original, OpenTelemetry is imported lazily, the first time tracing
is enabled, so ``import ate_env`` never imports it even when it is installed.
Without the extra every entry point is a no-op and enabling tracing logs one
error naming the missing extra.
"""

from __future__ import annotations

import asyncio
import atexit
import collections
import concurrent.futures
import contextlib
import functools
import json
import logging
import threading
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import grpc
import grpc.aio

if TYPE_CHECKING:
    from ..config import TracerConfig
    from ..loop_runner import LoopRunner

logger = logging.getLogger("ate_env.telemetry.tracing")

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

# Span name prefix of every span the ate_env client library emits. The
# TracerConfig service name is the OTel resource ``service.name``; it does not
# rename spans, so fleet and client spans keep one vocabulary.
CLIENT_SPAN_PREFIX = "ate_env_client"

_MISSING_EXTRA_MSG = (
    "OpenTelemetry not installed; tracing disabled. "
    "Install the tracing extra: pip install 'ate-env-client[tracing]'"
)

# W3C trace-context carrier keys. Baggage is deliberately never propagated.
_CARRIER_KEYS = ("traceparent", "tracestate")


class MockSpan:
    """No-op stand-in for an OpenTelemetry Span."""

    def is_recording(self) -> bool:
        return False

    def set_attribute(self, key: str, value: Any) -> None:
        """No-op."""

    def set_attributes(self, attributes: Mapping[str, Any]) -> None:
        """No-op."""

    def record_exception(self, exception: BaseException, *args: Any, **kwargs: Any) -> None:
        """No-op."""

    def end(self) -> None:
        """No-op."""


class MockTracer:
    """No-op stand-in for an OpenTelemetry Tracer."""

    def start_as_current_span(self, *args: Any, **kwargs: Any) -> nullcontext[None]:
        return nullcontext()

    def start_span(self, *args: Any, **kwargs: Any) -> MockSpan:
        return MockSpan()


class TraceStub:
    """No-op stand-in for the ``opentelemetry.trace`` module."""

    @staticmethod
    def get_current_span() -> MockSpan:
        return MockSpan()

    @staticmethod
    def set_tracer_provider(_: Any) -> None:
        """No-op."""

    @staticmethod
    def get_tracer_provider() -> None:
        return None

    @staticmethod
    def get_tracer(name: str, version: str | None = None) -> MockTracer:
        return MockTracer()

    @staticmethod
    def set_span_in_context(span: Any, context: Any = None) -> None:
        """No-op."""


class ContextStub:
    """No-op stand-in for the ``opentelemetry.context`` module."""

    @staticmethod
    def get_current() -> None:
        return None

    @staticmethod
    def attach(*args: Any, **kwargs: Any) -> None:
        """No-op."""

    @staticmethod
    def detach(*args: Any, **kwargs: Any) -> None:
        """No-op."""


class _NoOpTraceContextTextMapPropagator:
    """No-op stand-in for ``TraceContextTextMapPropagator``."""

    def inject(self, carrier: dict[str, str], context: Any = None, setter: Any = None) -> None:
        """No-op."""


# --- lazy OpenTelemetry import ------------------------------------------- #

_OTEL_LOCK = threading.Lock()
_OTEL: Any = None  # a namespace of the OTel modules once imported, or False
_MISSING_LOGGED = False


def _load_otel() -> Any:
    """Import OpenTelemetry on first use; returns a namespace or None."""
    global _OTEL
    if _OTEL is None:
        with _OTEL_LOCK:
            if _OTEL is None:
                try:
                    from opentelemetry import context as otel_context
                    from opentelemetry import trace as otel_trace
                    from opentelemetry.sdk.resources import Resource
                    from opentelemetry.sdk.trace import TracerProvider
                    from opentelemetry.sdk.trace.export import BatchSpanProcessor
                    from opentelemetry.trace.propagation.tracecontext import (
                        TraceContextTextMapPropagator,
                    )
                except ImportError:
                    _OTEL = False
                else:
                    ns = collections.namedtuple(
                        "_Otel",
                        "trace context Resource TracerProvider BatchSpanProcessor Propagator",
                    )
                    _OTEL = ns(
                        otel_trace,
                        otel_context,
                        Resource,
                        TracerProvider,
                        BatchSpanProcessor,
                        TraceContextTextMapPropagator,
                    )
    return _OTEL or None


def _log_missing_extra() -> None:
    """Log the missing-extra error once per process."""
    global _MISSING_LOGGED
    with _OTEL_LOCK:
        if _MISSING_LOGGED:
            return
        _MISSING_LOGGED = True
    logger.error(_MISSING_EXTRA_MSG)


def __getattr__(name: str) -> Any:
    # OPENTELEMETRY_AVAILABLE, trace, context and TraceContextTextMapPropagator
    # resolve lazily (PEP 562) so importing this module never imports OTel.
    if name == "OPENTELEMETRY_AVAILABLE":
        return _load_otel() is not None
    if name in ("trace", "context", "TraceContextTextMapPropagator"):
        otel = _load_otel()
        if otel is None:
            return {
                "trace": TraceStub,
                "context": ContextStub,
                "TraceContextTextMapPropagator": _NoOpTraceContextTextMapPropagator,
            }[name]
        return {
            "trace": otel.trace,
            "context": otel.context,
            "TraceContextTextMapPropagator": otel.Propagator,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# --- global TracerProvider ----------------------------------------------- #

_TRACER_PROVIDER: Any | None = None
_OWNS_PROVIDER = False  # True when initialize_tracer installed the provider itself
_TRACER_PROVIDER_LOCK = threading.Lock()
# Set once any tracer is handed out; LoopRunner checks it before capturing the
# caller's OTel context, so a process without tracing pays nothing.
_TRACING_ACTIVE = False


def tracing_active() -> bool:
    """True once tracing has been enabled anywhere in this process."""
    return _TRACING_ACTIVE


def _service_name_of(provider: Any) -> str | None:
    try:
        return provider.resource.attributes.get("service.name")
    except Exception:
        return None


def initialize_tracer(service_name: str) -> None:
    """Initialize the global OpenTelemetry TracerProvider once per process.

    - Without OpenTelemetry: logs an error naming the extra and returns.
    - Already initialized with another service name: logs a warning and keeps
      the existing provider.
    - An SDK TracerProvider the application installed itself is adopted as is
      (no warning: its service name is the application's choice), so callers
      that own their exporters keep them.
    - Otherwise installs a TracerProvider with a BatchSpanProcessor over an
      OTLPSpanExporter (configured by OTEL_EXPORTER_OTLP_*) and registers its
      shutdown with atexit.
    """
    global _TRACER_PROVIDER, _OWNS_PROVIDER

    otel = _load_otel()
    if otel is None:
        _log_missing_extra()
        return

    if _TRACER_PROVIDER is None:
        with _TRACER_PROVIDER_LOCK:
            if _TRACER_PROVIDER is None:
                current = otel.trace.get_tracer_provider()
                if isinstance(current, otel.TracerProvider):
                    _TRACER_PROVIDER = current
                else:
                    try:
                        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                            OTLPSpanExporter,
                        )
                    except ImportError:
                        _log_missing_extra()
                        return
                    provider = otel.TracerProvider(
                        resource=otel.Resource(attributes={"service.name": service_name})
                    )
                    provider.add_span_processor(otel.BatchSpanProcessor(OTLPSpanExporter()))
                    otel.trace.set_tracer_provider(provider)
                    atexit.register(provider.shutdown)
                    _TRACER_PROVIDER = provider
                    _OWNS_PROVIDER = True
                    logger.info(
                        "Global OpenTelemetry TracerProvider configured for service %r", service_name
                    )
                    return

    existing = _service_name_of(_TRACER_PROVIDER)
    if _OWNS_PROVIDER and existing and existing != service_name:
        logger.warning(
            "Global TracerProvider already initialized with service name %r; "
            "ignoring request to initialize with %r.",
            existing,
            service_name,
        )


# --- decorators ---------------------------------------------------------- #


def trace_span(span_suffix: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Wrap a method in a span named ``f"{self.trace_service_name}.{span_suffix}"``.

    The instance needs ``self.tracer`` and ``self.trace_service_name``; when
    ``self.tracer`` is falsy (tracing disabled) the method runs undecorated.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            instance = args[0] if args else None
            tracer = getattr(instance, "tracer", None)
            if not tracer:
                return func(*args, **kwargs)
            prefix = getattr(instance, "trace_service_name", CLIENT_SPAN_PREFIX)
            with tracer.start_as_current_span(f"{prefix}.{span_suffix}"):
                return func(*args, **kwargs)

        return wrapper

    return decorator


def async_trace_span(
    span_suffix: str,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Async counterpart of :func:`trace_span`."""

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            instance = args[0] if args else None
            tracer = getattr(instance, "tracer", None)
            if not tracer:
                return await func(*args, **kwargs)
            prefix = getattr(instance, "trace_service_name", CLIENT_SPAN_PREFIX)
            with tracer.start_as_current_span(f"{prefix}.{span_suffix}"):
                return await func(*args, **kwargs)

        return wrapper

    return decorator


@contextlib.contextmanager
def start_span(
    tracer: Any, name: str, attributes: Mapping[str, Any] | None = None
) -> Iterator[Any]:
    """Start ``name`` as the current span on ``tracer``; yields None when disabled.

    Attributes whose value is None are dropped. Exceptions are recorded on the
    span and mark it as an error (the SDK default).
    """
    if not tracer:
        yield None
        return
    with tracer.start_as_current_span(name) as span:
        if attributes:
            set_attributes(span, attributes)
        yield span


def set_attributes(span: Any, attributes: Mapping[str, Any]) -> None:
    """Set the non-None attributes on span; a None span is ignored."""
    if span is None:
        return
    for key, value in attributes.items():
        if value is None:
            continue
        try:
            span.set_attribute(key, value)
        except Exception:  # pragma: no cover - attribute type errors never fail a call
            pass


def image_ref_attr(ref: str | None) -> str | None:
    """An image reference without its tag, for span attributes.

    ``repo@sha256:...`` (the digest is an id) and ``repo`` are kept;
    ``repo:tag`` and ``repo:tag@sha256:...`` drop the tag.
    """
    if not ref:
        return None
    name, at, digest = ref.partition("@")
    slash = name.rfind("/")
    colon = name.rfind(":")
    if colon > slash:
        name = name[:colon]
    return f"{name}{at}{digest}"


def executable_attr(command: str | list[str] | tuple[str, ...]) -> str | None:
    """The executable name of a command (never its arguments), for span attributes."""
    if isinstance(command, (list, tuple)):
        first = str(command[0]) if command else ""
    else:
        parts = command.split(None, 1)
        first = parts[0] if parts else ""
    return first.rsplit("/", 1)[-1] or None


# --- TracerManager ------------------------------------------------------- #


class TracerManager:
    """Tracing lifecycle of one client instance.

    The instrumentation scope is the service name with ``-`` replaced by
    ``_``; the optional lifecycle span (``{service_name}.lifecycle``) parents
    every span started while it is attached.
    """

    def __init__(self, service_name: str) -> None:
        otel = _load_otel()
        self.service_name = service_name
        self.instrumentation_scope_name = service_name.replace("-", "_")
        trace_mod = otel.trace if otel else TraceStub
        self.tracer = trace_mod.get_tracer(self.instrumentation_scope_name)
        self.lifecycle_span_name = f"{service_name}.lifecycle"
        self.parent_span: Any | None = None
        self.context_token: Any | None = None
        self.propagator = otel.Propagator() if otel else _NoOpTraceContextTextMapPropagator()

    def start_lifecycle_span(self) -> None:
        """Start the client's lifecycle span and attach it to the current context."""
        if not self.tracer:
            return
        otel = _load_otel()
        trace_mod = otel.trace if otel else TraceStub
        context_mod = otel.context if otel else ContextStub
        self.parent_span = self.tracer.start_span(self.lifecycle_span_name)
        ctx = trace_mod.set_span_in_context(self.parent_span)
        self.context_token = context_mod.attach(ctx)

    def end_lifecycle_span(self) -> None:
        """End the lifecycle span and detach its context."""
        otel = _load_otel()
        context_mod = otel.context if otel else ContextStub
        if self.context_token is not None:
            try:
                context_mod.detach(self.context_token)
            except Exception:  # detaching from another context logs, never raises
                pass
            self.context_token = None
        if self.parent_span is not None:
            self.parent_span.end()
            self.parent_span = None

    def get_trace_context_json(self) -> str:
        """The current W3C carrier (traceparent and tracestate only) as JSON."""
        carrier = _current_carrier(self.propagator)
        return json.dumps(carrier) if carrier else ""


def create_tracer_manager(config: TracerConfig) -> tuple[TracerManager | None, Any | None]:
    """Build a TracerManager from config; ``(None, None)`` when disabled or OTel absent."""
    global _TRACING_ACTIVE
    if config is None or not config.enable_tracing:
        return None, None
    if _load_otel() is None:
        _log_missing_extra()
        return None, None
    manager = TracerManager(service_name=config.trace_service_name)
    _TRACING_ACTIVE = True
    return manager, manager.tracer


def tracer_from_config(config: TracerConfig | None) -> tuple[TracerManager | None, Any | None]:
    """Initialize the global provider for config's service and build its manager."""
    if config is None or not config.enable_tracing:
        return None, None
    initialize_tracer(config.trace_service_name)
    return create_tracer_manager(config)


# --- propagation --------------------------------------------------------- #


def _current_carrier(propagator: Any = None) -> dict[str, str]:
    otel = _load_otel()
    if otel is None:
        return {}
    carrier: dict[str, str] = {}
    (propagator or otel.Propagator()).inject(carrier)
    return {k: v for k, v in carrier.items() if k in _CARRIER_KEYS}


def inject_metadata(metadata: list[tuple[str, str]] | None) -> list[tuple[str, str]]:
    """Return metadata plus the W3C carrier of the current context.

    Keys already present (case-insensitive) are kept, never doubled.
    """
    md = list(metadata or [])
    carrier = _current_carrier()
    if not carrier:
        return md
    present = {k.lower() for k, _ in md}
    for key, value in carrier.items():
        if key not in present:
            md.append((key, value))
    return md


class _TraceCallDetails(
    collections.namedtuple(
        "_TraceCallDetails",
        ["method", "timeout", "metadata", "credentials", "wait_for_ready"],
    ),
    grpc.aio.ClientCallDetails,
):
    pass


class TraceContextInterceptor:
    """Base of the interceptor family that injects the W3C trace context.

    grpc.aio dispatches one interceptor class per call kind, so the family has
    one subclass per kind, mirroring the auth interceptors in client.py.
    """

    def _inject(self, details: grpc.aio.ClientCallDetails) -> _TraceCallDetails:
        return _TraceCallDetails(
            details.method,
            details.timeout,
            inject_metadata(details.metadata),
            details.credentials,
            details.wait_for_ready,
        )


class UnaryUnaryTraceContextInterceptor(
    TraceContextInterceptor, grpc.aio.UnaryUnaryClientInterceptor
):
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        return await continuation(self._inject(client_call_details), request)


class UnaryStreamTraceContextInterceptor(
    TraceContextInterceptor, grpc.aio.UnaryStreamClientInterceptor
):
    async def intercept_unary_stream(self, continuation, client_call_details, request):
        return await continuation(self._inject(client_call_details), request)


class StreamUnaryTraceContextInterceptor(
    TraceContextInterceptor, grpc.aio.StreamUnaryClientInterceptor
):
    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return await continuation(self._inject(client_call_details), request_iterator)


class StreamStreamTraceContextInterceptor(
    TraceContextInterceptor, grpc.aio.StreamStreamClientInterceptor
):
    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return await continuation(self._inject(client_call_details), request_iterator)


def _trace_interceptors() -> list[grpc.aio.ClientInterceptor]:
    """One trace-context interceptor per grpc.aio call kind."""
    return [
        UnaryUnaryTraceContextInterceptor(),
        UnaryStreamTraceContextInterceptor(),
        StreamUnaryTraceContextInterceptor(),
        StreamStreamTraceContextInterceptor(),
    ]


# --- LoopRunner context capture ------------------------------------------ #


async def _attached(ctx: Any, coro: Coroutine[Any, Any, T]) -> T:
    otel = _load_otel()
    token = otel.context.attach(ctx) if (otel is not None and ctx is not None) else None
    try:
        return await coro
    finally:
        if token is not None:
            try:
                otel.context.detach(token)
            except Exception:  # pragma: no cover
                pass


def run_with_context(
    loop_runner: LoopRunner | asyncio.AbstractEventLoop, coro: Coroutine[Any, Any, T]
) -> concurrent.futures.Future[T]:
    """Submit coro to the runner's loop under the caller's OTel context.

    Captures ``opentelemetry.context.get_current()`` on the calling thread and
    attaches it inside the coroutine, so spans started on the background loop
    parent to the caller's current span. Returns the concurrent future.
    """
    loop = loop_runner if isinstance(loop_runner, asyncio.AbstractEventLoop) else loop_runner.loop
    otel = _load_otel() if _TRACING_ACTIVE else None
    ctx = otel.context.get_current() if otel is not None else None
    return asyncio.run_coroutine_threadsafe(_attached(ctx, coro), loop)

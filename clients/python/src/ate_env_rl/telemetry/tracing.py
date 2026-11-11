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

"""OpenTelemetry tracing for the ate_env_rl client, behind the ``tracing`` extra.

Derived from agent-sandbox's ``k8s_agent_sandbox/trace_manager.py``:
``ate_env_client.<op>`` span names, a per-client ``TracerManager`` and W3C
``traceparent``/``tracestate`` propagation (never baggage).

As a library, the client never installs the process-global
``TracerProvider``. Its tracers choose a provider each time a span starts, so
spans go to whatever provider the application installs, before or after the
client starts tracing. While the application has none and
``OTEL_EXPORTER_OTLP_ENDPOINT`` or ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`` is
set, spans go to the client's own SDK provider with an OTLP/gRPC exporter;
that provider is never made global, so OpenTelemetry never refuses the
application's provider.

OpenTelemetry is imported lazily, the first time tracing is enabled, so
``import ate_env_rl`` never imports it even when it is installed. Without the
extra every entry point is a no-op and enabling tracing logs one error naming
the missing extra.
"""

from __future__ import annotations

import asyncio
import atexit
import collections
import contextlib
import logging
import os
import re
import threading
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any

import grpc
import grpc.aio

from .trace_recorder import (
    TraceTapTracer,
    get_default_trace_processor,
    is_trace_mode,
    should_auto_enable_tracing,
)
from .trace_recorder import (
    current_slot_wait_s as current_slot_wait_s,
)

if TYPE_CHECKING:
    from ..config import TracerConfig

logger = logging.getLogger("ate_env_rl.telemetry.tracing")

# Span name prefix of every span the ate_env_rl client library emits. The
# TracerConfig service name is the OTel resource ``service.name``; it does not
# rename spans, so fleet and client spans keep one vocabulary.
CLIENT_SPAN_PREFIX = "ate_env_client"

_MISSING_EXTRA_MSG = (
    "OpenTelemetry not installed; tracing disabled. "
    "Install the tracing extra: pip install 'ate-env-rl[tracing]'"
)

# W3C trace-context carrier keys. Baggage is deliberately never propagated.
_CARRIER_KEYS = ("traceparent", "tracestate")


# --- lazy OpenTelemetry import ------------------------------------------- #

_OTEL_LOCK = threading.Lock()
_OTEL: Any = None  # a namespace of the OTel API modules once imported, or False
_MISSING_LOGGED = False


def _load_otel() -> Any:
    """Import the OpenTelemetry API on first use; returns a namespace or None.

    Only the API is imported here. The SDK and the OTLP exporter are imported
    by _private_provider, and only when it builds the client's own provider.
    """
    global _OTEL
    if _OTEL is None:
        with _OTEL_LOCK:
            if _OTEL is None:
                try:
                    from opentelemetry import context as otel_context
                    from opentelemetry import trace as otel_trace
                    from opentelemetry.trace import (
                        ProxyTracerProvider,
                        SpanKind,
                        Status,
                        StatusCode,
                    )
                    from opentelemetry.trace.propagation.tracecontext import (
                        TraceContextTextMapPropagator,
                    )
                except ImportError:
                    _OTEL = False
                else:
                    ns = collections.namedtuple(
                        "_Otel",
                        "trace context ProxyTracerProvider SpanKind Status StatusCode Propagator",
                    )
                    _OTEL = ns(
                        otel_trace,
                        otel_context,
                        ProxyTracerProvider,
                        SpanKind,
                        Status,
                        StatusCode,
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
    # OPENTELEMETRY_AVAILABLE resolves lazily (PEP 562) so importing this
    # module never imports OTel.
    if name == "OPENTELEMETRY_AVAILABLE":
        return _load_otel() is not None
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# --- TracerProvider ------------------------------------------------------ #

# The environment variables that opt in to the client's own OTLP exporter.
OTLP_ENDPOINT_ENV_VARS = ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")

# The client's own SDK TracerProvider (OTLP exporter), built on first use when
# the application has no provider and an OTLP endpoint variable is set. It is
# never installed as the process-global provider.
_PRIVATE_PROVIDER: Any | None = None
_PRIVATE_PROVIDER_LOCK = threading.Lock()
_WARNED_SERVICE_NAMES: set[str] = set()


def _otlp_endpoint_configured() -> bool:
    return any(os.environ.get(name) for name in OTLP_ENDPOINT_ENV_VARS)


def _service_name_of(provider: Any) -> str | None:
    try:
        return provider.resource.attributes.get("service.name")
    except Exception:
        return None


def _private_provider(service_name: str) -> Any | None:
    """The client's own OTLP provider, or None when no OTLP endpoint is set.

    Built once per process, on the first span that needs it, with
    ``service.name`` = service_name; its shutdown (flush) is registered with
    atexit. A later caller with another service name gets one warning and
    shares the existing provider.
    """
    global _PRIVATE_PROVIDER
    if not _otlp_endpoint_configured():
        return None
    with _PRIVATE_PROVIDER_LOCK:
        if _PRIVATE_PROVIDER is None:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                    OTLPSpanExporter,
                )
                from opentelemetry.sdk.resources import Resource
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor
            except ImportError:
                _log_missing_extra()
                return None
            provider = TracerProvider(resource=Resource(attributes={"service.name": service_name}))
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
            atexit.register(provider.shutdown)
            _PRIVATE_PROVIDER = provider
            logger.info("OpenTelemetry OTLP exporter configured for service %r", service_name)
            return provider
        existing = _service_name_of(_PRIVATE_PROVIDER)
        if existing and existing != service_name and service_name not in _WARNED_SERVICE_NAMES:
            _WARNED_SERVICE_NAMES.add(service_name)
            logger.warning(
                "OTLP TracerProvider already initialized with service name %r; "
                "ignoring request to initialize with %r.",
                existing,
                service_name,
            )
        return _PRIVATE_PROVIDER


def initialize_tracer(service_name: str) -> None:
    """Check that tracing can run; never installs a process-global provider.

    - Without OpenTelemetry: logs an error naming the extra and returns.
    - Otherwise nothing happens here. Client tracers choose their provider
      each time a span starts (see ``TracerManager``): the application's
      provider once it is installed, before or after the client; else, when
      ``OTEL_EXPORTER_OTLP_ENDPOINT`` or ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT``
      is set, the client's own OTLP provider; else the no-op API proxy.
    """
    if _load_otel() is None:
        _log_missing_extra()


class _ResolvingTracer:
    """A tracer that chooses its provider each time a span starts.

    The application's provider wins as soon as it is installed, so the setup
    order never matters and the client never has to install a global
    provider (which OpenTelemetry would then refuse to replace). Until the
    application installs one, spans go to the client's own OTLP provider when
    an OTLP endpoint variable is set, and to the no-op API proxy otherwise.
    """

    def __init__(self, scope_name: str, service_name: str) -> None:
        self._scope_name = scope_name
        self._service_name = service_name
        self._cached: tuple[Any, Any] = (None, None)  # (provider, tracer)

    def _tracer(self) -> Any:
        otel = _load_otel()
        provider = otel.trace.get_tracer_provider()
        if isinstance(provider, otel.ProxyTracerProvider):
            provider = _private_provider(self._service_name) or provider
        cached_provider, tracer = self._cached
        if cached_provider is not provider:
            tracer = provider.get_tracer(self._scope_name)
            self._cached = (provider, tracer)
        return tracer

    def start_as_current_span(self, name: str, **kwargs: Any) -> Any:
        return self._tracer().start_as_current_span(name, **kwargs)

    def start_span(self, name: str, **kwargs: Any) -> Any:
        return self._tracer().start_span(name, **kwargs)


# --- span helpers -------------------------------------------------------- #


@contextlib.contextmanager
def start_span(
    tracer: Any,
    name: str,
    attributes: Mapping[str, Any] | None = None,
    *,
    client_kind: bool = False,
) -> Iterator[Any]:
    """Start ``name`` as the current span on ``tracer``; yields None when disabled.

    Attributes whose value is None are dropped. Exceptions are recorded on the
    span and mark it as an error (the SDK default). Cancellation
    (``asyncio.CancelledError``, which the SDK does not record) marks the span
    as an error with the description ``cancelled``. ``client_kind`` gives the
    span kind ``CLIENT`` (for a span that wraps exactly one outgoing RPC);
    otherwise the kind is ``INTERNAL``.
    """
    if not tracer:
        yield None
        return
    kwargs: dict[str, Any] = {}
    if client_kind:
        otel = _load_otel()
        if otel is not None:
            kwargs["kind"] = otel.SpanKind.CLIENT
    with tracer.start_as_current_span(name, **kwargs) as span:
        if attributes:
            set_attributes(span, attributes)
        try:
            yield span
        except asyncio.CancelledError:
            mark_error(span, "cancelled")
            raise


# Which rpc.* keys rpc_attrs emits: "new" (the default), "old" or "dup" (both).
_SEMCONV_NEW, _SEMCONV_OLD, _SEMCONV_DUP = "new", "old", "dup"


def _rpc_semconv_mode(value: str | None) -> str:
    """The rpc.* convention that OTEL_SEMCONV_STABILITY_OPT_IN selects.

    Parsed as otelgrpc 0.72 parses it (``parseSemconvMode``), so the Python
    client and the otelgrpc spans of ate-env-api and the Go client use the
    same keys: a comma-separated list, each item trimmed; the first item that
    is ``rpc/dup`` or ``rpc/old`` decides, and anything else means new.
    """
    for item in (value or "").split(","):
        item = item.strip()
        if item == "rpc/dup":
            return _SEMCONV_DUP
        if item == "rpc/old":
            return _SEMCONV_OLD
    return _SEMCONV_NEW


# Read once at import, as otelgrpc reads it once when its handler is built.
_RPC_SEMCONV_MODE = _rpc_semconv_mode(os.environ.get("OTEL_SEMCONV_STABILITY_OPT_IN"))


def rpc_attrs(service: str, method: str, target: str | None) -> dict[str, Any]:
    """OTel semantic-convention attributes of one gRPC call.

    The rpc.* keys follow otelgrpc (0.72 and later), and so the ate-env-api
    server spans and the Go client's spans, including its
    OTEL_SEMCONV_STABILITY_OPT_IN switch:

    - unset (new): ``rpc.system.name`` and ``rpc.method`` with the full method
      name (``ateenv.v1alpha.EnvironmentService/CreateEnvironment``); no
      ``rpc.service``.
    - ``rpc/old``: ``rpc.system``, ``rpc.service`` and ``rpc.method`` with the
      method name alone (``CreateEnvironment``).
    - ``rpc/dup``: the keys of both; ``rpc.method`` is the full name, as in
      otelgrpc, where the new value wins.

    service is the fully qualified gRPC service (``ateenv.v1alpha.X``), method
    the method name, and target the ``host:port`` of the channel (None for a
    caller-owned channel).
    """
    full_method = f"{service}/{method}"
    attrs: dict[str, Any]
    if _RPC_SEMCONV_MODE == _SEMCONV_OLD:
        attrs = {"rpc.system": "grpc", "rpc.service": service, "rpc.method": method}
    elif _RPC_SEMCONV_MODE == _SEMCONV_DUP:
        attrs = {"rpc.system": "grpc", "rpc.service": service, "rpc.system.name": "grpc", "rpc.method": full_method}
    else:
        attrs = {"rpc.system.name": "grpc", "rpc.method": full_method}
    if target:
        if target.startswith("dns:///"):
            target = target[7:]
        elif target.startswith("passthrough:///"):
            target = target[15:]
        host, sep, port = target.rpartition(":")
        if sep and port.isdigit() and host:
            attrs["server.address"] = host.strip("[]")
            attrs["server.port"] = int(port)
        else:
            attrs["server.address"] = target
    return attrs


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


# A leading shell variable assignment (``NAME=value`` or bash's ``NAME+=value``),
# e.g. ``API_KEY=x cmd``. The Go client's envAssignment is the same pattern.
_ENV_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\+?=")


_OPS = frozenset(["\n", "(", ")", ";", ";;", "&", "&&", "|", "||",
       "<", ">", ">>", "<<", "<<<", ">&", "<&", "&>", "&>>", "<>", ">|", "<<-"])
_REDIR_BASES = frozenset(["<", ">", ">>", "<<", "<<<", ">&", "<&", "<>", ">|", "<<-"])

def _is_ascii_digit(c: str) -> bool:
    # An fd number is ASCII digits, as in the Go client; str.isdigit also
    # takes "²" or "٣".
    return "0" <= c <= "9"


def _is_op_prefix(op: str) -> bool:
    digits = ""
    base = op
    for i, c in enumerate(op):
        if not _is_ascii_digit(c):
            digits = op[:i]
            base = op[i:]
            break
    else:
        digits = op
        base = ""
    if digits:
        if not base:
            return True
        return any(r.startswith(base) for r in _REDIR_BASES)
    else:
        return any(o.startswith(base) for o in _OPS)

def _is_redirection_op(op: str) -> bool:
    base = op
    for i, c in enumerate(op):
        if not _is_ascii_digit(c):
            base = op[i:]
            break
    else:
        base = ""
    return base in _REDIR_BASES or op in ("&>", "&>>")

def _tokenize_sh(s: str) -> list[str] | None:
    words = []
    cur = []
    in_word = False
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == '\\':
            if i + 1 >= n:
                return None
            if s[i+1] == '\n':
                i += 2
                continue
            if not in_word:
                in_word = True
            cur.append(s[i+1])
            i += 2
        elif c == "'":
            end = s.find("'", i + 1)
            if end < 0:
                return None
            if not in_word:
                in_word = True
            cur.append(s[i+1:end])
            i = end + 1
        elif c == '"':
            if not in_word:
                in_word = True
            i += 1
            closed = False
            while i < n:
                if s[i] == '"':
                    closed = True
                    i += 1
                    break
                if s[i] == '\\' and i + 1 < n and s[i+1] in ('"', '\\', '$', '\n', '`'):
                    if s[i+1] == '\n':
                        pass
                    else:
                        cur.append(s[i+1])
                    i += 2
                else:
                    cur.append(s[i])
                    i += 1
            if not closed:
                return None
        elif c in ' \t\r':
            if in_word:
                words.append(''.join(cur))
                cur.clear()
                in_word = False
            i += 1
        elif c in '\n();&|<>':
            cur_s = ''.join(cur)
            if c in '<>' and cur_s and all(_is_ascii_digit(x) for x in cur_s):
                op_str = cur_s + c
                cur.clear()
                in_word = False
            else:
                if in_word:
                    words.append(''.join(cur))
                    cur.clear()
                    in_word = False
                op_str = c
            
            while i + 1 < n:
                next_c = s[i+1]
                if _is_op_prefix(op_str + next_c):
                    op_str += next_c
                    i += 1
                else:
                    break
            words.append(op_str)
            i += 1
        elif c == '#' and not in_word:
            end = s.find('\n', i)
            if end < 0:
                break
            i = end
        else:
            if not in_word:
                in_word = True
            cur.append(c)
            i += 1
    if in_word:
        words.append(''.join(cur))
    return words

def executable_attr(command: str | list[str] | tuple[str, ...]) -> str | None:
    """The executable name of a command (never its arguments), for span attributes.

    A string is split like a POSIX shell splits it (quotes, backslashes,
    comments, operators), and the result is the base name of the first
    command word of its first simple command. Before that word, leading
    ``NAME=value`` and ``NAME+=value`` assignments (``API_KEY=s3cr3t python
    x.py`` gives ``python``), redirections with their targets, and opening
    ``(``, ``{`` and newlines are skipped. A control operator before the
    command word (``; A=s cmd``) gives None, and ``cmd1 && A=s cmd2`` gives
    ``cmd1``. None also when an assignment's value holds ``$`` or a backtick
    (an expansion such as ``"$(echo my secret)"`` can span words), when the
    command word holds ``=``, ``$`` or a backtick, or when the line cannot be
    split. An argv list is not parsed: its first item is the command word,
    so a leading assignment item gives None. The Go client's ``executable``
    and ``executableWords`` give the same answers
    (proto/testdata/client-parity/executable.json).
    """
    if isinstance(command, (list, tuple)):
        if not command:
            return None
        w_str = str(command[0])
        if '=' in w_str or '$' in w_str or '`' in w_str:
            return None
        return w_str.rsplit("/", 1)[-1] or None

    words = _tokenize_sh(command)
    if words is None:
        return None

    i = 0
    in_command = False
    while i < len(words):
        w = words[i]
        if w in ('\n', '(', '{'):
            if in_command:
                return None
            i += 1
            continue
        if _is_redirection_op(w):
            in_command = True
            i += 2
            continue
        if w in (';', '&', '|', '&&', '||', ';;', ')'):
            return None
        if _ENV_ASSIGNMENT.match(w):
            # An expansion in the value can span words, so the next word may
            # be the value's rest, not the command.
            if '$' in w or '`' in w:
                return None
            in_command = True
            i += 1
            continue
        if '=' in w or '$' in w or '`' in w:
            return None
        return w.rsplit("/", 1)[-1] or None
    return None


def mark_error(span: Any, description: str) -> None:
    """Set span's status to ERROR with description; a None span is ignored.

    For failures reported as results rather than exceptions raised inside the
    span (e.g. a non-zero exit with ``check=True``). Nothing else is recorded,
    so command lines and output never reach the trace backend.
    """
    if span is None:
        return
    if hasattr(span, "record_error"):
        with contextlib.suppress(Exception):
            span.record_error(description)
    otel = _load_otel()
    if otel is None:
        return
    try:
        span.set_status(otel.Status(otel.StatusCode.ERROR, description))
    except Exception:  # pragma: no cover - status errors never fail a call
        pass


# --- TracerManager ------------------------------------------------------- #


class TracerManager:
    """The tracer of one client instance.

    ``service_name`` is the OTel resource ``service.name`` (from
    ``TracerConfig.trace_service_name``); the instrumentation scope is that
    name with ``-`` replaced by ``_``. It never renames spans: client spans are
    always ``ate_env_client.<op>``.
    """

    def __init__(self, service_name: str) -> None:
        otel = _load_otel()
        if otel is None:
            raise RuntimeError(_MISSING_EXTRA_MSG)
        self.service_name = service_name
        self.instrumentation_scope_name = service_name.replace("-", "_")
        # Chooses the provider at each span start: the application's once it
        # is installed, else the client's own OTLP provider (endpoint set),
        # else the no-op API proxy.
        self.tracer = _ResolvingTracer(self.instrumentation_scope_name, service_name)


def create_tracer_manager(config: TracerConfig) -> tuple[TracerManager | None, Any | None]:
    """Build a TracerManager from config; ``(None, None)`` when disabled or OTel absent."""
    auto_trace = should_auto_enable_tracing(config)
    if (config is None or not config.enable_tracing) and not auto_trace:
        return None, None
    svc_name = config.trace_service_name if config is not None else "ate-env-client"
    if _load_otel() is None:
        if is_trace_mode() and _OTEL is not False:
            return None, TraceTapTracer(None, get_default_trace_processor())
        if config is not None and config.enable_tracing:
            _log_missing_extra()
        if is_trace_mode():
            return None, TraceTapTracer(None, get_default_trace_processor())
        return None, None
    manager = TracerManager(service_name=svc_name)
    if auto_trace:
        return manager, TraceTapTracer(manager.tracer, get_default_trace_processor())
    return manager, manager.tracer


def tracer_from_config(config: TracerConfig | None) -> Any | None:
    """The tracer for config (None when disabled or OTel absent).

    The tracer never installs a global provider; see initialize_tracer for
    where its spans go.
    """
    auto_trace = should_auto_enable_tracing(config)
    if (config is None or not config.enable_tracing) and not auto_trace:
        return None
    svc_name = config.trace_service_name if config is not None else "ate-env-client"
    if _load_otel() is not None:
        initialize_tracer(svc_name)
    return create_tracer_manager(config)[1]


# --- propagation --------------------------------------------------------- #


def _current_carrier() -> dict[str, str]:
    otel = _load_otel()
    if otel is None:
        return {}
    carrier: dict[str, str] = {}
    otel.Propagator().inject(carrier)
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

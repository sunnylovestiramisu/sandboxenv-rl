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

"""Optional OpenTelemetry layer: spans, stage metrics and trace propagation.

Uses only the OpenTelemetry *API* (``pip install ate-env-client[otel]``). The
SDK never configures exporters or providers: the host application (or the
``OTEL_*`` environment through its own SDK setup) does. Without the package,
or with no provider installed, every function here is a cheap no-op.

Metric labels are bounded (stage, outcome, backend, acquire path, step kind);
run, task, trajectory and sandbox identifiers stay on spans and events only.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

try:  # pragma: no cover - exercised implicitly by the import below
    from opentelemetry import baggage as _baggage
    from opentelemetry import metrics as _metrics
    from opentelemetry import propagate as _propagate
    from opentelemetry import trace as _trace

    AVAILABLE = True
except ImportError:  # pragma: no cover - depends on the environment
    AVAILABLE = False

__all__ = [
    "AVAILABLE",
    "tracing_active",
    "metrics_active",
    "span",
    "set_span_attributes",
    "inject",
    "baggage_fields",
    "record_stage",
    "use_providers",
    "current_trace_ids",
]

_SCOPE = "ate_env.telemetry"
_tracer_provider: Any = None   # overrides for tests / embedding apps
_meter_provider: Any = None
_stage_hist: Any = None
_stage_hist_owner: Any = None

# Attribute keys allowed as metric labels (bounded cardinality).
_METRIC_LABELS = ("stage", "outcome", "rl_sandbox.backend", "acquire_path", "step.kind")


def use_providers(tracer_provider: Any = None, meter_provider: Any = None) -> None:
    """Use these providers instead of the global ones (``None`` resets to global)."""
    global _tracer_provider, _meter_provider, _stage_hist, _stage_hist_owner
    _tracer_provider = tracer_provider
    _meter_provider = meter_provider
    _stage_hist = None
    _stage_hist_owner = None


def _get_tracer_provider() -> Any:
    return _tracer_provider if _tracer_provider is not None else _trace.get_tracer_provider()


def _get_meter_provider() -> Any:
    return _meter_provider if _meter_provider is not None else _metrics.get_meter_provider()


def tracing_active() -> bool:
    """True if spans would be recorded (API installed and a real provider set)."""
    if not AVAILABLE:
        return False
    tp = _get_tracer_provider()
    return not isinstance(tp, (_trace.ProxyTracerProvider, _trace.NoOpTracerProvider))


def metrics_active() -> bool:
    """True if metric points would be recorded (API installed and a real provider set)."""
    if not AVAILABLE:
        return False
    mp = _get_meter_provider()
    inactive = tuple(c for c in (
        getattr(_metrics, "NoOpMeterProvider", None),
        getattr(getattr(_metrics, "_internal", None), "_ProxyMeterProvider", None),
    ) if isinstance(c, type))
    return not (inactive and isinstance(mp, inactive))


def _clean(attrs: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (attrs or {}).items():
        if v is None:
            continue
        out[k] = v if isinstance(v, (str, bool, int, float)) else str(v)
    return out


@contextmanager
def span(name: str, attrs: dict[str, Any] | None = None, *,
         kind: str = "internal") -> Iterator[Any]:
    """Start a span as current; yields it (or ``None`` when tracing is inactive).

    Exceptions are recorded on the span and set its status to error.
    """
    if not tracing_active():
        yield None
        return
    tracer = _get_tracer_provider().get_tracer(_SCOPE)
    span_kind = _trace.SpanKind.CLIENT if kind == "client" else _trace.SpanKind.INTERNAL
    with tracer.start_as_current_span(name, kind=span_kind, attributes=_clean(attrs),
                                      record_exception=True,
                                      set_status_on_exception=True) as s:
        yield s


def set_span_attributes(s: Any, attrs: dict[str, Any]) -> None:
    if s is not None:
        s.set_attributes(_clean(attrs))


def inject(metadata: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Append W3C trace context (traceparent, tracestate, baggage) to gRPC metadata."""
    if not AVAILABLE:
        return metadata
    carrier: dict[str, str] = {}
    _propagate.inject(carrier)
    metadata.extend(carrier.items())
    return metadata


def baggage_fields() -> dict[str, Any]:
    """Return ``rl.*`` episode fields carried in OpenTelemetry baggage, if any."""
    if not AVAILABLE:
        return {}
    out: dict[str, Any] = {}
    for key, value in _baggage.get_all().items():
        if key.startswith("rl.") and value is not None:
            out[key[len("rl."):]] = value
    return out


def record_stage(stage: str, seconds: float, attrs: dict[str, Any] | None = None) -> None:
    """Record one ``rl_sandbox.stage.duration`` observation (seconds)."""
    global _stage_hist, _stage_hist_owner
    if not metrics_active():
        return
    mp = _get_meter_provider()
    if _stage_hist is None or _stage_hist_owner is not mp:
        meter = mp.get_meter(_SCOPE)
        _stage_hist = meter.create_histogram(
            "rl_sandbox.stage.duration", unit="s",
            description="Duration of sandbox lifecycle stages seen by the SDK",
        )
        _stage_hist_owner = mp
    labels = {"stage": stage}
    for k in _METRIC_LABELS:
        if attrs and attrs.get(k) is not None:
            labels[k] = attrs[k]
    _stage_hist.record(seconds, attributes=_clean(labels))


def current_trace_ids() -> tuple[str, str] | None:
    """(trace_id, span_id) of the current span in lowercase hex, if it is valid.

    The same field names and encoding Substrate puts on its log records, so an
    SDK event joins the server-side records of the same trace.
    """
    if not AVAILABLE:
        return None
    ctx = _trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None
    return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")

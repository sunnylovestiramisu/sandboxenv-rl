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

"""Optional OpenTelemetry layer: spans, stage metrics, propagation, baggage."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry import baggage, context  # noqa: E402
from opentelemetry.sdk.metrics import MeterProvider  # noqa: E402
from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)

from ate_env import FleetConfig, SandboxFleet, Task, telemetry  # noqa: E402
from ate_env.client import _trace_interceptors, _UnaryUnaryTraceInterceptor  # noqa: E402
from ate_env.errors import NotFoundError  # noqa: E402
from ate_env.telemetry import MemorySink, otel  # noqa: E402


@pytest.fixture
def otel_providers():
    exporter = InMemorySpanExporter()
    tp = TracerProvider()
    tp.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    mp = MeterProvider(metric_readers=[reader])
    otel.use_providers(tp, mp)
    yield exporter, reader
    otel.use_providers(None, None)


@pytest.fixture
def sink():
    s = MemorySink()
    telemetry.configure(s, run_id="run-otel")
    yield s
    telemetry.shutdown()


def _by_name(exporter):
    return {s.name: s for s in exporter.get_finished_spans()}


def _stage_points(reader):
    data = reader.get_metrics_data()
    points = []
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == "rl_sandbox.stage.duration":
                    points.extend(m.data.data_points)
    return points


def test_inactive_without_provider():
    otel.use_providers(None, None)
    assert not otel.tracing_active()
    with otel.span("x") as s:
        assert s is None


def test_episode_acquire_and_steps_form_one_trace(otel_providers, sink):
    exporter, reader = otel_providers
    fleet = SandboxFleet(FleetConfig(backend="mock"))
    with telemetry.episode(trajectory_id="t9", batch_id=1, trace=True):
        h = fleet.acquire(Task(id="t", image="img"))
        h.exec("echo hi")
        h.release()

    spans = _by_name(exporter)
    ep, acq, ex = spans["rl_sandbox.episode"], spans["rl_sandbox.acquire"], spans["rl_sandbox.exec"]
    assert acq.parent.span_id == ep.context.span_id
    assert ex.parent.span_id == ep.context.span_id
    assert {acq.context.trace_id, ex.context.trace_id} == {ep.context.trace_id}
    assert ep.attributes["rl.trajectory_id"] == "t9"
    assert acq.attributes["rl_sandbox.sandbox.id"] == h.sandbox_id
    assert acq.attributes["acquire_path"] == "mock"
    assert ex.attributes["step.kind"] == "exec"

    stages = {(p.attributes["stage"], p.attributes.get("outcome")) for p in _stage_points(reader)}
    assert {("acquire", "ok"), ("step", "ok"), ("release", "ok")} <= stages
    for p in _stage_points(reader):
        assert not any(k.startswith("rl.") or k.endswith(".id") for k in p.attributes)


def test_otel_works_without_event_sink(otel_providers):
    exporter, _ = otel_providers
    telemetry.configure(None)
    try:
        h = SandboxFleet(FleetConfig(backend="mock")).acquire(Task(id="t", image="i"))
        assert h.telemetry is not None  # spans/metrics still need per-handle state
        h.exec("echo")
        h.release()
    finally:
        telemetry.shutdown()
    assert "rl_sandbox.acquire" in _by_name(exporter)


def test_inject_adds_traceparent(otel_providers):
    with otel.span("parent"):
        md = otel.inject([("x-env-id", "sb-1")])
    keys = dict(md)
    assert keys["x-env-id"] == "sb-1"
    assert keys["traceparent"].startswith("00-")


def test_baggage_supplies_episode_fields():
    ctx = baggage.set_baggage("rl.trajectory_id", "from-baggage")
    ctx = baggage.set_baggage("rl.attempt", "2", context=ctx)
    token = context.attach(ctx)
    try:
        ep = telemetry.current_episode()
        assert ep.trajectory_id == "from-baggage" and ep.attempt == 2
    finally:
        context.detach(token)
    assert telemetry.current_episode() is None


async def test_interceptor_injects_and_spans_unary(otel_providers):
    exporter, _ = otel_providers
    seen = {}

    class _Call:
        def __await__(self):
            if False:
                yield
            return "resp"

    async def continuation(details, request):
        seen["md"] = dict(details.metadata)
        return _Call()

    class _Details:
        method = "/ateenv.v1alpha.EnvironmentService/CreateEnvironment"
        timeout = None
        metadata = [("x-env-id", "sb-1")]
        credentials = None
        wait_for_ready = None

    with otel.span("parent"):
        await _UnaryUnaryTraceInterceptor().intercept_unary_unary(continuation, _Details(), None)
    assert seen["md"]["x-env-id"] == "sb-1" and "traceparent" in seen["md"]
    rpc = _by_name(exporter)["ateenv.v1alpha.EnvironmentService/CreateEnvironment"]
    assert rpc.attributes["rpc.method"] == "CreateEnvironment"
    assert rpc.parent.span_id == _by_name(exporter)["parent"].context.span_id


async def test_backend_rpcs_are_children_of_acquire_span(fake_stack, otel_providers, sink):
    """The sync fleet's background loop keeps the acquire span as parent."""
    exporter, _ = otel_providers
    client, _fakes = fake_stack
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, tenancy="ate-env",
                      template="tmpl-a")

    def _run():
        fleet = SandboxFleet(cfg)
        try:
            with telemetry.episode(trajectory_id="s1", trace=True):
                h = fleet.acquire(Task(id="t", image="img", template="tmpl-a"))
                h.release()
        finally:
            fleet.close()

    await asyncio.to_thread(_run)
    spans = exporter.get_finished_spans()
    acq = next(s for s in spans if s.name == "rl_sandbox.acquire")
    create = next(s for s in spans if s.name.endswith("/CreateEnvironment"))
    assert create.parent.span_id == acq.context.span_id
    assert create.context.trace_id == acq.context.trace_id


async def test_rpc_errors_still_map_with_interceptor(fake_stack, otel_providers):
    client, _fakes = fake_stack
    with pytest.raises(NotFoundError):
        await client.get("does-not-exist", atespace="ate-env")


def test_events_carry_trace_ids_of_current_span(otel_providers, sink):
    with telemetry.episode(trajectory_id="tr", trace=True):
        h = SandboxFleet(FleetConfig(backend="mock")).acquire(Task(id="t", image="i"))
        h.release()
    ready = sink.events(telemetry.ACQUIRE_READY)[0]
    exporter, _ = otel_providers
    ep = _by_name(exporter)["rl_sandbox.episode"]
    assert ready["trace_id"] == format(ep.context.trace_id, "032x")
    assert len(ready["span_id"]) == 16


def test_trace_interceptors_cover_every_call_shape():
    # grpc.aio registers a multi-base interceptor for only one shape, so each
    # shape needs its own instance.
    import grpc

    shapes = [grpc.aio.UnaryUnaryClientInterceptor, grpc.aio.UnaryStreamClientInterceptor,
              grpc.aio.StreamUnaryClientInterceptor, grpc.aio.StreamStreamClientInterceptor]
    got = _trace_interceptors()
    assert len(got) == 4
    for shape, icpt in zip(shapes, got):
        assert isinstance(icpt, shape)
        assert sum(isinstance(icpt, s2) for s2 in shapes) == 1


async def test_streaming_interceptor_injects_traceparent(otel_providers):
    seen = {}

    async def continuation(details, request):
        seen["md"] = dict(details.metadata)
        return "stream"

    class _Details:
        method = "/ateenv.v1alpha.ProcessService/StreamOutput"
        timeout = None
        metadata = [("x-env-id", "sb-1")]
        credentials = None
        wait_for_ready = None

    unary_stream = _trace_interceptors()[1]
    with otel.span("parent"):
        await unary_stream.intercept_unary_stream(continuation, _Details(), None)
    assert "traceparent" in seen["md"] and seen["md"]["x-env-id"] == "sb-1"

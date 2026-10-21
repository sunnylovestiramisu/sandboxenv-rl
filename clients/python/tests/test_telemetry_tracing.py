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

"""Tracing: the no-op path, span names, nesting, and W3C context on the wire.

Tests that need OpenTelemetry skip cleanly in a venv without the tracing extra.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import textwrap

import grpc
import grpc.aio
import pytest

from ate_env import AsyncSandboxFleet, Client, FleetConfig, SandboxFleet, Task
from ate_env._gen.ateenv.v1alpha import env_pb2_grpc, guest_pb2_grpc
from ate_env.config import TracerConfig, ValidationError
from ate_env.loop_runner import LoopRunner
from ate_env.telemetry import tracing

from .fakes import FakeEnvironmentService, FakeFileSystemService, FakeProcessService

DIGEST = "sha256:" + "ab" * 32
IMAGE = f"registry.example/swe/repo-a@{DIGEST}"


# --- no-op path (runs with and without the extra) ------------------------ #


def test_import_does_not_load_opentelemetry():
    code = textwrap.dedent(
        """
        import sys
        import ate_env, ate_env.telemetry, ate_env.telemetry.tracing
        loaded = sorted(m for m in sys.modules if m.split(".")[0] == "opentelemetry")
        assert not loaded, loaded
        print("ok")
        """
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "ok"


def test_tracer_config_defaults_and_validation():
    cfg = TracerConfig()
    assert cfg.enable_tracing is False
    assert cfg.trace_service_name == "ate-env-client"
    with pytest.raises(ValidationError, match="enable_traces"):
        TracerConfig(enable_traces=True)
    with pytest.raises(ValidationError):
        TracerConfig(trace_service_name="")
    fleet_cfg = FleetConfig.from_dict({"backend": "mock", "tracing": {"enable_tracing": True}})
    assert fleet_cfg.tracing == TracerConfig(enable_tracing=True)
    assert FleetConfig.from_dict(fleet_cfg.to_dict()) == fleet_cfg
    with pytest.raises(ValidationError, match="bogus"):
        FleetConfig(tracing={"bogus": 1})


async def test_disabled_tracing_is_a_noop():
    assert tracing.create_tracer_manager(TracerConfig()) == (None, None)
    client = Client("127.0.0.1:1")
    assert client.tracer is None and client.tracer_manager is None
    await client.close()

    class Thing:
        tracer = None
        trace_service_name = "svc"

        @tracing.trace_span("op")
        def op(self):
            return 42

        @tracing.async_trace_span("aop")
        async def aop(self):
            return 43

    assert Thing().op() == 42
    assert tracing.inject_metadata([("a", "b")]) == [("a", "b")] or tracing.tracing_active()


async def test_missing_extra_logs_one_error(monkeypatch, caplog):
    monkeypatch.setattr(tracing, "_OTEL", False)
    monkeypatch.setattr(tracing, "_MISSING_LOGGED", False)
    caplog.set_level(logging.WARNING)
    cfg = TracerConfig(enable_tracing=True)
    for _ in range(3):
        client = Client("127.0.0.1:1", tracer_config=cfg)
        assert client.tracer is None
        await client.close()
    fleet = SandboxFleet(FleetConfig(backend="mock", tracing=cfg))
    fleet.load_tasks([Task(id="t", image="img")])
    assert fleet.run(lambda task, handle: handle.exec("echo hi").stdout)
    errors = [r for r in caplog.records if "tracing" in r.getMessage().lower()]
    assert len(errors) == 1, [r.getMessage() for r in errors]
    assert errors[0].levelno == logging.ERROR
    assert "ate-env-client[tracing]" in errors[0].getMessage()
    assert tracing.OPENTELEMETRY_AVAILABLE is False


# --- OpenTelemetry fixtures ---------------------------------------------- #


_PROVIDER = None


@pytest.fixture
def spans():
    """An in-memory exporter on the process-wide SDK provider; returns the exporter."""
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    global _PROVIDER
    if _PROVIDER is None:
        provider = TracerProvider(resource=Resource(attributes={"service.name": "ate-env-client"}))
        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _PROVIDER = (provider, exporter)
    provider, exporter = _PROVIDER
    # initialize_tracer adopts the application's SDK provider instead of
    # installing an OTLP exporter.
    tracing._TRACER_PROVIDER = None
    exporter.clear()
    yield exporter
    exporter.clear()


def _by_name(finished):
    out: dict[str, list] = {}
    for s in finished:
        out.setdefault(s.name, []).append(s)
    return out


def _parent_name(span, finished):
    if span.parent is None:
        return None
    for s in finished:
        if s.context.span_id == span.parent.span_id:
            return s.name
    return "<missing>"


def _ancestors(span, finished):
    names = []
    by_id = {s.context.span_id: s for s in finished}
    cur = span
    while cur.parent is not None and cur.parent.span_id in by_id:
        cur = by_id[cur.parent.span_id]
        names.append(cur.name)
    return names


def format_span_tree(finished) -> str:
    """Render finished spans as an indented tree (one line per distinct path)."""
    by_id = {s.context.span_id: s for s in finished}
    children: dict[int | None, list] = {}
    for s in finished:
        pid = s.parent.span_id if s.parent is not None and s.parent.span_id in by_id else None
        children.setdefault(pid, []).append(s)
    lines: list[str] = []

    def walk(pid, depth):
        kids = sorted(children.get(pid, []), key=lambda s: s.start_time)
        # Collapse siblings with the same name into one line with a count.
        seen: dict[str, list] = {}
        for k in kids:
            seen.setdefault(k.name, []).append(k)
        for name, group in seen.items():
            count = f" x{len(group)}" if len(group) > 1 else ""
            lines.append(f"{'  ' * depth}{name}{count}")
            walk(group[0].context.span_id, depth + 1)

    walk(None, 0)
    return "\n".join(lines)


# --- tracer manager and decorators --------------------------------------- #


def test_initialize_tracer_warns_on_second_service_name(spans, caplog, monkeypatch):
    caplog.set_level(logging.WARNING, logger="ate_env.telemetry.tracing")
    # The application's own SDK provider is adopted silently, whatever its name.
    tracing.initialize_tracer("another-service")
    assert tracing._TRACER_PROVIDER is spans_provider()
    assert not caplog.records
    # A provider ate_env installed itself warns when asked for another name.
    monkeypatch.setattr(tracing, "_OWNS_PROVIDER", True)
    tracing.initialize_tracer("ate-env-client")
    assert not caplog.records
    tracing.initialize_tracer("another-service")
    assert len(caplog.records) == 1
    assert "another-service" in caplog.records[0].getMessage()


def spans_provider():
    return _PROVIDER[0]


def test_trace_span_decorators_and_lifecycle(spans):
    manager, tracer = tracing.create_tracer_manager(
        TracerConfig(enable_tracing=True, trace_service_name="my-svc")
    )
    assert manager.instrumentation_scope_name == "my_svc"

    class Thing:
        trace_service_name = "my_svc"

        def __init__(self, tracer):
            self.tracer = tracer

        @tracing.trace_span("op")
        def op(self):
            return 1

        @tracing.async_trace_span("aop")
        async def aop(self):
            return 2

    import asyncio

    manager.start_lifecycle_span()
    try:
        thing = Thing(tracer)
        assert thing.op() == 1
        assert asyncio.run(thing.aop()) == 2
        carrier = manager.get_trace_context_json()
    finally:
        manager.end_lifecycle_span()
    import json

    assert set(json.loads(carrier)) <= {"traceparent", "tracestate"}
    finished = spans.get_finished_spans()
    names = _by_name(finished)
    assert set(names) == {"my-svc.lifecycle", "my_svc.op", "my_svc.aop"}
    assert _parent_name(names["my_svc.op"][0], finished) == "my-svc.lifecycle"
    assert _parent_name(names["my_svc.aop"][0], finished) == "my-svc.lifecycle"


def test_carrier_never_carries_baggage(spans):
    from opentelemetry import baggage, context, trace

    tracer = trace.get_tracer("t")
    token = context.attach(baggage.set_baggage("user", "secret"))
    try:
        with tracer.start_as_current_span("root"):
            md = tracing.inject_metadata([])
    finally:
        context.detach(token)
    keys = {k for k, _ in md}
    assert "traceparent" in keys
    assert "baggage" not in keys


def test_attribute_helpers_drop_tags_and_arguments():
    assert tracing.image_ref_attr("repo/img:1.2") == "repo/img"
    assert tracing.image_ref_attr(f"repo/img:1.2@{DIGEST}") == f"repo/img@{DIGEST}"
    assert tracing.image_ref_attr("host:5000/img") == "host:5000/img"
    assert tracing.image_ref_attr(None) is None
    assert tracing.executable_attr("pytest -k secret /x") == "pytest"
    assert tracing.executable_attr(["/usr/bin/python3", "-c", "x"]) == "python3"


# --- W3C context on the wire (AC-4) -------------------------------------- #


def _identity(b):
    return b


class _MetadataRecorder:
    def __init__(self):
        self.calls: dict[str, dict[str, str]] = {}

    def _record(self, kind, context):
        self.calls[kind] = {k: v for k, v in context.invocation_metadata()}

    def handler(self):
        async def uu(request, context):
            self._record("unary_unary", context)
            return request

        async def us(request, context):
            self._record("unary_stream", context)
            yield request

        async def su(request_iterator, context):
            self._record("stream_unary", context)
            async for _ in request_iterator:
                pass
            return b"ok"

        async def ss(request_iterator, context):
            self._record("stream_stream", context)
            async for item in request_iterator:
                yield item

        kw = {"request_deserializer": _identity, "response_serializer": _identity}
        return grpc.method_handlers_generic_handler(
            "test.Echo",
            {
                "UU": grpc.unary_unary_rpc_method_handler(uu, **kw),
                "US": grpc.unary_stream_rpc_method_handler(us, **kw),
                "SU": grpc.stream_unary_rpc_method_handler(su, **kw),
                "SS": grpc.stream_stream_rpc_method_handler(ss, **kw),
            },
        )


async def _call_all_kinds(channel):
    kw = {"request_serializer": _identity, "response_deserializer": _identity}

    async def reqs():
        yield b"x"

    await channel.unary_unary("/test.Echo/UU", **kw)(b"x")
    async for _ in channel.unary_stream("/test.Echo/US", **kw)(b"x"):
        pass
    await channel.stream_unary("/test.Echo/SU", **kw)(reqs())
    async for _ in channel.stream_stream("/test.Echo/SS", **kw)(reqs()):
        pass


@pytest.fixture
async def recorder_server():
    recorder = _MetadataRecorder()
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((recorder.handler(),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        yield recorder, f"127.0.0.1:{port}"
    finally:
        await server.stop(None)


KINDS = {"unary_unary", "unary_stream", "stream_unary", "stream_stream"}


async def test_metadata_carries_traceparent_on_all_call_kinds(spans, recorder_server):
    from opentelemetry import trace

    recorder, endpoint = recorder_server
    client = Client(endpoint, auth_token="tok", tracer_config=TracerConfig(enable_tracing=True))
    try:
        with trace.get_tracer("test").start_as_current_span("caller") as span:
            await _call_all_kinds(client._channel)
            ctx = span.get_span_context()
    finally:
        await client.close()
    assert set(recorder.calls) == KINDS
    want_trace = format(ctx.trace_id, "032x")
    want_span = format(ctx.span_id, "016x")
    for kind, md in recorder.calls.items():
        assert "traceparent" in md, kind
        _version, trace_id, span_id, _flags = md["traceparent"].split("-")
        assert (trace_id, span_id) == (want_trace, want_span), kind
        assert "baggage" not in md, kind
        assert md["authorization"] == "Bearer tok", kind


async def test_metadata_has_no_traceparent_when_disabled(spans, recorder_server):
    from opentelemetry import trace

    recorder, endpoint = recorder_server
    client = Client(endpoint)
    try:
        with trace.get_tracer("test").start_as_current_span("caller"):
            await _call_all_kinds(client._channel)
    finally:
        await client.close()
    assert set(recorder.calls) == KINDS
    for kind, md in recorder.calls.items():
        assert "traceparent" not in md, kind


# --- nesting (AC-3) ------------------------------------------------------ #


async def _start_fake_server():
    server = grpc.aio.server()
    fakes = (FakeEnvironmentService(), FakeProcessService(), FakeFileSystemService())
    env_pb2_grpc.add_EnvironmentServiceServicer_to_server(fakes[0], server)
    guest_pb2_grpc.add_ProcessServiceServicer_to_server(fakes[1], server)
    guest_pb2_grpc.add_FileSystemServiceServicer_to_server(fakes[2], server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


@pytest.fixture
def threaded_fake_server():
    """The fake ate-env-api on its own loop thread, for synchronous fleets."""
    runner = LoopRunner(name="fake-ate-env-api")
    server, port = runner.run(_start_fake_server())
    try:
        yield f"127.0.0.1:{port}"
    finally:
        runner.run(server.stop(None))
        runner.stop()


def _substrate_cfg(endpoint, **kw):
    return FleetConfig(
        backend="substrate",
        endpoint=endpoint,
        strategy="none",
        tracing=TracerConfig(enable_tracing=True),
        **kw,
    )


def _process(task, handle):
    handle.write_file("/workspace/a.txt", "hello")
    return handle.exec("echo hi").exit_code


def _assert_one_trace(finished):
    roots = [sp for sp in finished if sp.parent is None]
    assert [r.name for r in roots] == ["atefleet.run"], [r.name for r in roots]
    assert {sp.context.trace_id for sp in finished} == {roots[0].context.trace_id}


def _assert_substrate_tree(finished, n_tasks):
    names = _by_name(finished)
    _assert_one_trace(finished)
    creates = names["ate_env_client.create_environment"]
    assert len(creates) == n_tasks
    for create in creates:
        assert _ancestors(create, finished) == [
            "ate_env_client.acquire", "atefleet.acquire", "atefleet.run"
        ]
    assert len(names["ate_env_client.exec"]) == n_tasks
    for ex in names["ate_env_client.exec"]:
        assert _ancestors(ex, finished) == ["atefleet.process", "atefleet.run"]
        attrs = dict(ex.attributes)
        assert attrs["env.command.executable"] == "echo"
        assert attrs["env.exit_code"] == 0
        assert "echo hi" not in str(attrs)
    for delete in names["ate_env_client.delete_environment"]:
        assert _ancestors(delete, finished)[:3] == [
            "ate_env_client.release", "atefleet.release", "atefleet.run"
        ] or _ancestors(delete, finished)[:3] == [
            "ate_env_client.reap", "atefleet.teardown", "atefleet.run"
        ]
    # Each child's parent span id is its parent's span id (checked by _ancestors);
    # spot-check one pair explicitly.
    create = creates[0]
    acquire = next(sp for sp in finished if sp.context.span_id == create.parent.span_id)
    assert acquire.name == "ate_env_client.acquire"
    create_attrs = dict(create.attributes)
    assert create_attrs["env.rootfs_image"] == f"registry.example/swe/repo-a@{DIGEST}"
    assert create_attrs["env.atespace"] == "ate-env"
    run_attrs = dict(names["atefleet.run"][0].attributes)
    assert run_attrs == {"strategy": "none", "backend": "substrate"}
    proc_attrs = dict(names["atefleet.process"][0].attributes)
    assert proc_attrs["backend"] == "substrate" and proc_attrs["strategy"] == "none"


def test_nests_sync_fleet_through_loop_runner(spans, threaded_fake_server):
    """SandboxFleet + SubstrateBackendDriver: client RPCs run on the LoopRunner."""
    tasks = [Task(id=f"t{i}", image=IMAGE) for i in range(3)]
    fleet = SandboxFleet(_substrate_cfg(threaded_fake_server, max_concurrent=3))
    try:
        fleet.load_tasks(tasks)
        results = fleet.run(_process)
        finished = spans.get_finished_spans()
    finally:
        fleet.close()
    assert results == [0, 0, 0]
    print("\n" + format_span_tree(finished))
    _assert_substrate_tree(finished, 3)
    names = _by_name(finished)
    for wf in names["ate_env_client.write_file"]:
        assert _ancestors(wf, finished) == ["atefleet.process", "atefleet.run"]
        assert dict(wf.attributes)["env.file.size"] == 5


async def test_nests_async_fleet(spans):
    server, port = await _start_fake_server()
    try:
        fleet = AsyncSandboxFleet(_substrate_cfg(f"127.0.0.1:{port}"))
        fleet.load_tasks([Task(id=f"t{i}", image=IMAGE) for i in range(2)])

        async def process(task, handle):
            res = await handle.exec_async("echo hi")
            return res.exit_code

        results = await fleet.run(process)
        finished = spans.get_finished_spans()
        await fleet.backend.close_async()
    finally:
        await server.stop(None)
    assert results == [0, 0]
    _assert_substrate_tree(finished, 2)
    assert fleet.report is not None and fleet.report.tasks_ok == 2


def test_nests_mock_fleet_phases(spans):
    """The mock backend makes no RPCs; fleet phases still parent handle spans."""
    cfg = FleetConfig(backend="mock", strategy="naive", tracing=TracerConfig(enable_tracing=True))
    fleet = SandboxFleet(cfg)
    fleet.load_tasks([Task(id=f"t{i}", image=f"img-{i % 2}") for i in range(4)])
    fleet.run(lambda task, handle: handle.exec(["python3", "-c", "print(1)"]).exit_code,
              concurrency=4)
    finished = spans.get_finished_spans()
    _assert_one_trace(finished)
    names = _by_name(finished)
    execs = names["ate_env_client.exec"]
    assert len(execs) == 4
    for ex in execs:
        assert _ancestors(ex, finished) == ["atefleet.process", "atefleet.run"]
    assert dict(execs[0].attributes)["env.command.executable"] == "python3"
    for phase in ("preflight", "plan", "warm_pool", "acquire", "release", "teardown"):
        assert _ancestors(names[f"atefleet.{phase}"][0], finished)[-1] == "atefleet.run", phase


def test_nests_observability_switch_enables_client_spans(spans, threaded_fake_server):
    """ObservabilityConfig.enable_tracing alone traces the client too, under one service."""
    from ate_env.config import ObservabilityConfig

    cfg = FleetConfig(
        backend="substrate",
        endpoint=threaded_fake_server,
        strategy="none",
        observability=ObservabilityConfig(enable_tracing=True),
    )
    fleet = SandboxFleet(cfg)
    try:
        fleet.load_tasks([Task(id="t0", image=IMAGE)])
        fleet.run(_process)
        finished = spans.get_finished_spans()
    finally:
        fleet.close()
    _assert_substrate_tree(finished, 1)


def test_nests_user_span_parents_the_run(spans):
    from opentelemetry import trace

    fleet = SandboxFleet(FleetConfig(backend="mock", strategy="none",
                                     tracing=TracerConfig(enable_tracing=True)))
    fleet.load_tasks([Task(id="t0", image="img")])
    with trace.get_tracer("trainer").start_as_current_span("trainer.step"):
        fleet.run(lambda task, handle: handle.exec("echo hi").exit_code)
    finished = spans.get_finished_spans()
    run = _by_name(finished)["atefleet.run"][0]
    assert _parent_name(run, finished) == "trainer.step"

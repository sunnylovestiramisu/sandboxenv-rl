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

"""Tests for the telemetry report builder (synthetic inputs, no cluster)."""

from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import build_report  # noqa: E402

T0 = 1_792_000_000 * 10**9


def _span(tid, sid, parent, name, start_ms, dur_ms, error=False, **attrs):
    return {
        "traceId": tid, "spanId": sid, "parentSpanId": parent, "name": name,
        "startTimeUnixNano": str(T0 + start_ms * 10**6),
        "endTimeUnixNano": str(T0 + (start_ms + dur_ms) * 10**6),
        "status": {"code": 2 if error else 0},
        "attributes": [{"key": k, "value": {"stringValue": str(v)}} for k, v in attrs.items()],
    }


def _req(service, spans):
    return {"resourceSpans": [{
        "resource": {"attributes": [{"key": "service.name",
                                     "value": {"stringValue": service}}]},
        "scopeSpans": [{"spans": spans}]}]}


def _write_inputs(tmp_path):
    tid = "0af7651916cd43dd8448eb211c80319c"
    client = _req("ate-env-client", [
        _span(tid, "a" * 16, "", "ate_env_fleet.run", 0, 1000, strategy="naive"),
        _span(tid, "b" * 16, "a" * 16, "ate_env_fleet.acquire", 10, 300),
        _span(tid, "c" * 16, "b" * 16, "ate_env_client.create_environment", 20, 250,
              **{"ate.actor.name": "sb-1"}),
        _span(tid, "d" * 16, "a" * 16, "ate_env_fleet.process", 320, 600),
        _span(tid, "e" * 16, "d" * 16, "ate_env_client.exec", 330, 500, error=True),
    ])
    server = _req("ate-env-api", [
        _span(tid, "f" * 16, "c" * 16,
              "ateenv.v1alpha.EnvironmentService/CreateEnvironment", 25, 240),
    ])
    other = _req("ate-env-client", [_span("1" * 32, "2" * 16, "9" * 16, "orphan", 0, 5)])
    spans = tmp_path / "spans.jsonl"
    spans.write_text("\n".join(json.dumps(x) for x in (client, server, other)) + "\n")

    rdir = tmp_path / "reports"
    rdir.mkdir()
    (rdir / "naive_2tasks_20261021-120000.json").write_text(json.dumps({
        "strategy": "naive", "total_s": 1.0, "claims": 2, "tasks_ok": 1, "tasks_err": 1,
        "warm_replicas_total": 2, "warm_replicas_peak": 2, "environment": {"backend": "mock"},
        "phases": {"process": {"count": 2, "total_s": 0.6, "max_s": 0.5},
                   "acquire": {"count": 2, "total_s": 0.3, "max_s": 0.2},
                   "custom": {"count": 1, "total_s": 0.1, "max_s": 0.1}}}))
    (rdir / "naive_2tasks_20261021-120000.txt").write_text("not json")

    mdir = tmp_path / "scrapes"
    mdir.mkdir()
    (mdir / "ate-env-api.prom").write_text("\n".join([
        "# HELP ate_env_api_requests_total x",
        'ate_env_api_requests_total{method="/ateenv.v1alpha.EnvironmentService/CreateEnvironment",code="OK"} 3',
        'ate_env_api_create_to_ready_seconds_bucket{atespace="ate-env",status="ok",le="0.5"} 1',
        'ate_env_api_create_to_ready_seconds_bucket{atespace="ate-env",status="ok",le="1"} 3',
        'ate_env_api_create_to_ready_seconds_bucket{atespace="ate-env",status="ok",le="+Inf"} 4',
        'ate_env_api_create_to_ready_seconds_sum{atespace="ate-env",status="ok"} 3.5',
        'ate_env_api_create_to_ready_seconds_count{atespace="ate-env",status="ok"} 4',
        'ate_env_api_build_info{version="v1",commit="abc"} 1',
        'go_goroutines 12',
    ]) + "\n")
    return spans, rdir, mdir


def test_build_aggregates_all_layers(tmp_path):
    spans, rdir, mdir = _write_inputs(tmp_path)
    data = build_report.build(str(rdir), str(spans), str(mdir), "t")

    assert data["meta"]["inputs"] == {"reports": 1, "spans": 7, "metric_files": 1}
    # Phase order follows RunReport.summary(); unknown phases go last.
    assert [p["name"] for p in data["report"]["phases"]] == ["acquire", "process", "custom"]

    s = data["spans"]
    assert s["n_traces"] == 2
    assert s["multi_service_traces"] == 1
    assert s["orphan_roots"] == 1
    assert s["traces_by_service"] == {"ate-env-client": 2, "ate-env-api": 1}
    wf = s["waterfalls"]["0af7651916cd43dd8448eb211c80319c"]
    names = [r["name"] for r in wf]
    # Tree order: the server span sits under the client span it continues.
    assert names.index("ateenv.v1alpha.EnvironmentService/CreateEnvironment") == \
        names.index("ate_env_client.create_environment") + 1
    depth = {r["name"]: r["depth"] for r in wf}
    assert depth["ate_env_fleet.run"] == 0
    assert depth["ateenv.v1alpha.EnvironmentService/CreateEnvironment"] == 3
    exec_row = next(r for r in s["latency"] if r["name"] == "ate_env_client.exec")
    assert exec_row["errors"] == 1 and abs(exec_row["p50"] - 0.5) < 1e-9

    m = data["metrics"]
    assert "go_goroutines" not in m["gauges"]
    h = m["histograms"]["ate_env_api_create_to_ready_seconds"][0]
    assert h["count"] == 4 and abs(h["mean"] - 0.875) < 1e-9
    assert 0.5 < h["p50"] <= 1.0
    assert sum(m["counters"]["ate_env_api_requests_total"].values()) == 3


def test_main_writes_html(tmp_path):
    spans, rdir, mdir = _write_inputs(tmp_path)
    out = tmp_path / "out"
    assert build_report.main(["--report", str(rdir), "--spans", str(spans), "--metrics",
                              str(mdir), "--title", "<x>", "--out", str(out)]) == 0
    html = (out / "report.html").read_text()
    assert "__RUN_JSON__" not in html
    assert "<title>ate_env telemetry – x</title>" in html
    assert json.loads((out / "run.json").read_text())["spans"]["n"] == 7


def test_base64_ids_and_single_layer(tmp_path):
    import base64

    tid = base64.b64encode(bytes.fromhex("0af7651916cd43dd8448eb211c80319c")).decode()
    sid = base64.b64encode(bytes.fromhex("a" * 16)).decode()
    p = tmp_path / "s.json"
    p.write_text(json.dumps(_req("svc", [_span(tid, sid, "", "root", 0, 1)])))
    data = build_report.build(None, str(p), None)
    assert data["spans"]["traces"][0]["trace_id"] == "0af7651916cd43dd8448eb211c80319c"
    assert data["report"] == {"runs": [], "phases": []}


def test_timeline_keeps_caller_spans_and_html_escapes_data(tmp_path):
    tid = "3" * 32
    p = tmp_path / "s.jsonl"
    loader = _req("swebench-loader", [
        _span(tid, "4" * 16, "", "swebench-loader.run", 0, 100),
        _span(tid, "5" * 16, "4" * 16, "step", 10, 50, instance="</script><!--__TITLE__"),
    ])
    go_client = _req("ate-env-go-client", [
        dict(_span(tid, "6" * 16, "5" * 16, "ate-env-go-client.exec", 12, 40), kind=3)])
    server = _req("ate-env-api", [dict(_span(tid, "7" * 16, "6" * 16,
                                             "ateenv.v1alpha.GuestService/Exec", 13, 38),
                                       kind="SPAN_KIND_SERVER")])
    p.write_text("\n".join(json.dumps(x) for x in (loader, go_client, server)) + "\n")
    data = build_report.build(None, str(p), None, "t")
    names = [s["name"] for s in data["spans"]["timeline"]]
    assert names == ["swebench-loader.run", "step", "ate-env-go-client.exec"]
    html = build_report.render_html(data)
    script = html.split("<script>", 1)[1].rsplit("</script>", 1)[0]
    assert "</script>" not in script and "<!--" not in script
    assert "__TITLE__" in script  # data is inserted verbatim, never substituted


def test_collector_file_exporter_metrics(tmp_path):
    def export(ts, count, buckets, total):
        return {"resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "ate-env-api"}},
                {"key": "service.instance.id", "value": {"stringValue": "10.0.0.7:9090"}}]},
            "scopeMetrics": [{"metrics": [
                {"name": "ate_env_api_create_to_ready_seconds", "histogram": {
                    "aggregationTemporality": 2, "dataPoints": [{
                        "attributes": [{"key": "status", "value": {"stringValue": "ok"}}],
                        "timeUnixNano": str(ts), "count": str(count), "sum": 0.875 * count,
                        "bucketCounts": [str(b) for b in buckets],
                        "explicitBounds": [0.5, 1.0]}]}},
                {"name": "ate_env_api_requests_total", "sum": {
                    "isMonotonic": True, "aggregationTemporality": 2,
                    "dataPoints": [{"timeUnixNano": str(ts), "asDouble": total}]}},
                {"name": "ate_env_api_environments", "gauge": {
                    "dataPoints": [{"timeUnixNano": str(ts), "asInt": "5"}]}},
                {"name": "go_goroutines", "gauge": {"dataPoints": [{"asInt": "9"}]}},
            ]}]}]}

    p = tmp_path / "metrics.jsonl"
    # Two scrapes of the same process: only the newest cumulative point counts.
    p.write_text(json.dumps(export(1, 2, [1, 1, 0], 2)) + "\n"
                 + json.dumps(export(2, 4, [1, 2, 1], 3)) + "\n")
    m = build_report.build(None, None, str(p))["metrics"]
    h = m["histograms"]["ate_env_api_create_to_ready_seconds"][0]
    assert h["count"] == 4 and abs(h["mean"] - 0.875) < 1e-9 and 0.5 < h["p50"] <= 1.0
    assert m["counters"]["ate_env_api_requests_total"] == {"-": 3.0}
    assert m["gauges"]["ate_env_api_environments"] == {"-": 5.0}
    assert "go_goroutines" not in m["gauges"]
    assert m["files"] == ["metrics.jsonl:10.0.0.7:9090"]


# --- real outputs -------------------------------------------------------------

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _otlp_json_hex(req: dict) -> dict:
    """Rewrite proto-JSON ids (base64) as OTLP/JSON ids (hex), as the collector writes them."""
    import base64

    for rs in req.get("resourceSpans", []):
        for ss in rs.get("scopeSpans", []):
            for s in ss.get("spans", []):
                for k in ("traceId", "spanId", "parentSpanId"):
                    if s.get(k):
                        s[k] = base64.b64decode(s[k]).hex()
    return req


def _generate_real_run(out: str) -> None:
    """A substrate fleet with tracing and metrics on, against the client's fake
    ate-env-api; writes the RunReport, the spans as OTLP/JSON and a scrape.

    Runs in a subprocess (see the test) so the global tracer provider and the
    Prometheus registry of the test process stay untouched.
    """
    import importlib.util
    import pathlib

    import grpc.aio
    import prometheus_client
    from ate_env import FleetConfig, SandboxFleet, Task
    from ate_env._gen.ateenv.v1alpha import env_pb2_grpc, guest_pb2_grpc
    from ate_env.config import ObservabilityConfig
    from ate_env.loop_runner import LoopRunner
    from google.protobuf.json_format import MessageToDict
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    provider = TracerProvider(resource=Resource(attributes={"service.name": "ate-env-fleet"}))
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    path = os.path.join(REPO, "clients", "python", "tests", "fakes.py")
    spec = importlib.util.spec_from_file_location("ate_env_test_fakes", path)
    fakes = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = fakes
    spec.loader.exec_module(fakes)

    async def start():
        server = grpc.aio.server()
        env_pb2_grpc.add_EnvironmentServiceServicer_to_server(fakes.FakeEnvironmentService(), server)
        guest_pb2_grpc.add_ProcessServiceServicer_to_server(fakes.FakeProcessService(), server)
        guest_pb2_grpc.add_FileSystemServiceServicer_to_server(fakes.FakeFileSystemService(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        return server, port

    def process(task, handle):
        handle.write_file("/workspace/a.txt", "hello")
        return handle.exec("echo hi").exit_code

    runner = LoopRunner(name="fake-ate-env-api")
    server, port = runner.run(start())
    try:
        fleet = SandboxFleet(FleetConfig(
            backend="substrate", endpoint=f"127.0.0.1:{port}", strategy="none", max_concurrent=2,
            observability=ObservabilityConfig(enable_tracing=True, enable_metrics=True)))
        fleet.load_tasks([Task(id=f"t{i}", image=f"registry.example/swe/repo-{i % 2}@sha256:"
                               + "ab" * 32) for i in range(4)])
        assert fleet.run(process) == [0, 0, 0, 0]
        fleet.close()
    finally:
        runner.run(server.stop(None))
        runner.stop()

    o = pathlib.Path(out)
    fleet.report.write(o / "reports", n_tasks=4)
    req = MessageToDict(encode_spans(exporter.get_finished_spans()))
    (o / "spans-proto.jsonl").write_text(json.dumps(req) + "\n")
    (o / "spans-otlp.jsonl").write_text(json.dumps(_otlp_json_hex(json.loads(json.dumps(req))))
                                        + "\n")
    (o / "scrapes").mkdir()
    (o / "scrapes" / "trainer.prom").write_bytes(prometheus_client.generate_latest())


def test_real_fleet_outputs(tmp_path):
    """The builder over what PR 1/2 actually write: RunReport.write, spans from
    a traced fleet (proto JSON and collector OTLP/JSON ids) and a scrape."""
    import subprocess

    for mod in ("ate_env", "opentelemetry.sdk", "opentelemetry.exporter.otlp.proto.common",
                "prometheus_client"):
        pytest.importorskip(mod)
    here = os.path.dirname(os.path.abspath(__file__))
    subprocess.run([sys.executable, "-c",
                    f"import test_build_report as t; t._generate_real_run({str(tmp_path)!r})"],
                   cwd=here, check=True, timeout=120)
    reports = tmp_path / "reports"
    (json_file,) = reports.glob("none_4tasks_*.json")
    assert (reports / (json_file.stem + ".txt")).exists()

    for spans in ("spans-proto.jsonl", "spans-otlp.jsonl"):
        data = build_report.build(str(reports), str(tmp_path / spans), str(tmp_path / "scrapes"))
        assert data["meta"]["inputs"]["reports"] == 1
        run = data["report"]["runs"][0]
        assert (run["strategy"], run["tasks_ok"], run["tasks_err"], run["claims"]) == (
            "none", 4, 0, 4)
        phases = [p["name"] for p in data["report"]["phases"]]
        assert phases[:3] == ["ensure_template", "acquire", "process"] and "release" in phases

        s = data["spans"]
        assert s["services"] == ["ate-env-fleet"] and s["orphan_roots"] == 0
        run_trace = next(t for t in s["traces"] if t["root"] == "ate_env_fleet.run")
        rows = s["waterfalls"][run_trace["trace_id"]]
        depth = {}
        for r in rows:
            depth.setdefault(r["name"], r["depth"])
        assert depth["ate_env_fleet.run"] == 0
        assert depth["ate_env_fleet.acquire"] == 1
        assert depth["ate_env_client.acquire"] == 2
        assert depth["ate_env_client.create_environment"] == 3
        assert depth["ate_env_client.exec"] == 2
        latency = {r["name"] for r in s["latency"]}
        assert {"ate_env_fleet.process", "ate_env_client.exec", "ate_env_client.write_file"} <= latency
        assert {t["name"] for t in s["timeline"]} >= {"ate_env_fleet.run", "ate_env_client.exec"}

        m = data["metrics"]
        assert {"ate_env_fleet_phase_latency_seconds", "ate_env_fleet_task_latency_seconds",
                "ate_env_fleet_run_latency_seconds", "ate_env_client_exec_seconds"} <= set(m["histograms"])
        assert sum(m["counters"]["ate_env_fleet_tasks_total"].values()) == 4
        phase_rows = {r["labels"] for r in m["histograms"]["ate_env_fleet_phase_latency_seconds"]}
        assert any("phase=process" in k for k in phase_rows)

    out = tmp_path / "out"
    assert build_report.main(["--report", str(reports), "--spans",
                              str(tmp_path / "spans-otlp.jsonl"), "--metrics",
                              str(tmp_path / "scrapes"), "--out", str(out)]) == 0
    assert "ate_env_fleet.run" in (out / "report.html").read_text()

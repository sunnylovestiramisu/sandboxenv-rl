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

"""Property-based tests for the telemetry spec (properties P-1..P-18).

Each test is one property from the spec's properties.md: a "for any ..."
statement checked over Hypothesis-generated inputs, tagged with the EARS
requirements (AC-n.m) it validates. On failure Hypothesis shrinks the input to
a minimal counter-example
add it as an @example so it stays a regression case.
P-9..P-11 (warm-pool concurrency) are in test_warm_pool_properties.py.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
import threading
from types import SimpleNamespace

import grpc
from hypothesis import HealthCheck, assume, given, note, settings
from hypothesis import strategies as st
from opentelemetry import baggage, trace
from opentelemetry import context as otel_context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from ate_env import AsyncSandboxFleet, FleetConfig, SandboxFleet, Task, TracerConfig
from ate_env import observability as obs_mod
from ate_env.client import Client
from ate_env.observability import MAX_FAMILIES, RunReport, repo_family
from ate_env.telemetry import tracing as tr

from . import test_telemetry_tracing as _tracing_tests

# The global TracerProvider can be set once per process: share the one
# test_telemetry_tracing.py's spans fixture installs, creating it the same
# way if this module runs first.
if _tracing_tests._PROVIDER is None:
    _provider = TracerProvider(resource=Resource(attributes={"service.name": "ate-env-client"}))
    _exporter = InMemorySpanExporter()
    _provider.add_span_processor(SimpleSpanProcessor(_exporter))
    trace.set_tracer_provider(_provider)
    _tracing_tests._PROVIDER = (_provider, _exporter)
_EXPORTER = _tracing_tests._PROVIDER[1]
_TEST_TRACER = trace.get_tracer("pbt")

FAST = settings(max_examples=100, deadline=None,
                suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
SLOW = settings(max_examples=30, deadline=None,
                suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture])

# --- the contract, as written in interfaces.md ---------------------------- #
DOCUMENTED_REPORT_KEYS = {"strategy", "total_s", "phases", "claims", "tasks_ok", "tasks_err",
                          "release_errors", "warm_replicas_total", "warm_replicas_peak",
                          "environment"}
REPORT_ORDER = ("preflight", "plan", "ensure_template", "warm_pool", "acquire", "process",
                "recycle", "release", "reap", "teardown")
FLEET_PHASES = REPORT_ORDER + ("run",)
DOCUMENTED_SPANS = ({f"ate_env_fleet.{p}" for p in FLEET_PHASES}
                    | {f"ate_env_client.{op}" for op in (
                        "create_environment", "get_environment", "suspend_environment",
                        "delete_environment", "exec", "write_file", "read_file",
                        "ensure_template", "warm_pool", "acquire", "release", "reap")})
DOCUMENTED_ATTRS = {"ate.actor.name", "ate.atespace", "ate.template.name", "ate.template.atespace",
                    "ate_env.rootfs_image", "ate_env.run_id", "ate_env.command.executable",
                    "ate_env.exit_code", "ate_env.timed_out", "ate_env.first_command",
                    "ate_env.file.path", "ate_env.file.size", "ate_env.replicas",
                    "strategy", "backend", "family"}
CARRIER = ("traceparent", "tracestate")
FAMILY_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,62}$")
ESCAPED_RE = re.compile(r"^(?:other|unknown)(?:_repo)*$")


# =========================================================================== #
# AC-9: labels and cardinality (P-1, P-2, P-3, P-18)
# =========================================================================== #
_name = st.one_of(
    st.from_regex(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,15}", fullmatch=True),
    st.sampled_from(["other", "unknown", "Other", "UNKNOWN", "other_repo", "unknown_repo_repo"]),
)


@st.composite
def labelled_input(draw):
    """(task_or_image, expected_family or None). The generator knows the truth."""
    kind = draw(st.sampled_from(["metadata", "image", "junk"]))
    if kind == "metadata":
        org, name = draw(_name), draw(_name)
        x = SimpleNamespace(metadata={"repo": f"{org}/{name}"}, image="")
        fam = name.lower()
    elif kind == "image":
        org = draw(st.from_regex(r"[a-z0-9][a-z0-9.-]{0,15}", fullmatch=True)
                   .filter(lambda s: "__" not in s and "_1776_" not in s))
        arch = draw(st.sampled_from(["x86_64.", "arm64."]))
        tag = draw(st.from_regex(r"[a-f0-9]{8,12}", fullmatch=True))
        x = f"sweb.eval.{arch}{org}__{org}-{draw(st.integers(1, 99999))}:{tag}"
        fam = org
    else:
        x = draw(st.text(max_size=20).filter(lambda s: "x86_64." not in s and "arm64." not in s))
        fam = None
    if fam is not None and not FAMILY_RE.match(fam):
        fam = None
    if fam is not None and ESCAPED_RE.match(fam):
        fam += "_repo"
    return x, fam


@FAST
@given(st.lists(labelled_input(), max_size=200))
def test_p1_p2_p3_family_labels_match_the_model(stream):
    """P-1, P-2, P-3. Validates: AC-9.1, AC-9.2, AC-9.3, AC-9.4."""
    obs_mod._FAMILIES.clear()
    model: set[str] = set()
    for x, fam in stream:
        label = repo_family(x)
        if fam is None:
            want = "unknown"
        elif fam in model:
            want = fam
        elif len(model) < MAX_FAMILIES:
            model.add(fam)
            want = fam
        else:
            want = "other"
        assert label == want, (x, fam)
        assert len(obs_mod._FAMILIES) <= MAX_FAMILIES
        assert "other" not in obs_mod._FAMILIES and "unknown" not in obs_mod._FAMILIES


@FAST
@given(n=st.integers(0, 80),
       junk=st.lists(st.sampled_from(["", "not-an-image", "???"]), max_size=3))
def test_p1_distinct_family_values_bounded(n, junk):
    """P-1: at most MAX_FAMILIES + 2 distinct values (+ other, + unknown). Validates: AC-9.1."""
    obs_mod._FAMILIES.clear()
    stream = [SimpleNamespace(metadata={"repo": f"org/repo{i}"}, image="") for i in range(n)]
    stream += junk
    labels = {repo_family(x) for x in stream}
    assert len(labels) <= MAX_FAMILIES + 2, f"{len(labels)} distinct family values"


@FAST
@given(org=st.from_regex(r"[a-z][a-z0-9-]{2,12}", fullmatch=True),
       tag=st.from_regex(r"[a-f0-9]{8,12}", fullmatch=True),
       n=st.integers(1, 99999))
def test_p18_no_image_tag_or_instance_in_label(org, tag, n):
    """P-18. Validates: AC-9.5."""
    assume(tag not in org)
    obs_mod._FAMILIES.clear()
    label = repo_family(f"sweb.eval.x86_64.{org}__{org}-{n}:{tag}")
    assert tag not in label and str(n) not in label.replace(org, "")


# =========================================================================== #
# AC-4: W3C context propagation (P-4..P-7, P-15)
# =========================================================================== #
_key = st.one_of(st.sampled_from(["traceparent", "TraceParent", "tracestate", "baggage",
                                  "Baggage", "authorization", "x-request-id"]),
                 st.from_regex(r"[a-z][a-z0-9-]{0,10}", fullmatch=True))
_md = st.lists(st.tuples(_key, st.text(st.characters(min_codepoint=32, max_codepoint=126),
                                       max_size=20)), max_size=8)


def _count(md, key):
    return sum(1 for k, _ in md if k.lower() == key)


@FAST
@given(md=_md, active=st.booleans(), with_baggage=st.booleans())
def test_p4_to_p7_inject_metadata(md, active, with_baggage):
    """P-4..P-7. Validates: AC-4.1, AC-4.2, AC-4.3."""
    ctx = baggage.set_baggage("user", "secret") if with_baggage else otel_context.get_current()
    token = otel_context.attach(ctx)
    try:
        if active:
            with _TEST_TRACER.start_as_current_span("caller") as span:
                out = tr.inject_metadata(md)
                sc = span.get_span_context()
        else:
            out = tr.inject_metadata(md)
    finally:
        otel_context.detach(token)
    assert out[:len(md)] == md                                         # P-4
    assert all(k in CARRIER for k, _ in out[len(md):])                 # P-5
    assert _count(out, "baggage") == _count(md, "baggage")             # P-6
    for k in CARRIER:                                                  # P-7
        assert _count(out, k) == max(_count(md, k), _count(out[len(md):], k))
        assert _count(out[len(md):], k) <= (0 if _count(md, k) else 1)
    if active and not _count(md, "traceparent"):
        tp = dict(out[len(md):])["traceparent"]
        assert tp.split("-")[2] == format(sc.span_id, "016x")


def _identity(b):
    return b


_wire_key = st.from_regex(r"x-[a-z0-9]{1,8}", fullmatch=True)
_wire_md = st.lists(st.tuples(_wire_key, st.from_regex(r"[A-Za-z0-9]{0,10}", fullmatch=True)),
                    max_size=5, unique_by=lambda kv: kv[0])
KINDS = ("UU", "US", "SU", "SS")


async def _call_with_metadata(kind, md, tracing_on):
    seen = {}

    async def record(context):
        seen.update({k: v for k, v in context.invocation_metadata()})

    async def uu(req, ctx):
        await record(ctx)
        return req

    async def us(req, ctx):
        await record(ctx)
        yield req

    async def su(it, ctx):
        await record(ctx)
        async for _ in it:
            pass
        return b"ok"

    async def ss(it, ctx):
        await record(ctx)
        async for x in it:
            yield x

    kw = {"request_deserializer": _identity, "response_serializer": _identity}
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("t.E", {
        "UU": grpc.unary_unary_rpc_method_handler(uu, **kw),
        "US": grpc.unary_stream_rpc_method_handler(us, **kw),
        "SU": grpc.stream_unary_rpc_method_handler(su, **kw),
        "SS": grpc.stream_stream_rpc_method_handler(ss, **kw)}),))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    client = Client(f"127.0.0.1:{port}",
                    tracer_config=TracerConfig(enable_tracing=tracing_on))
    ck = {"request_serializer": _identity, "response_deserializer": _identity}

    async def reqs():
        yield b"x"
    try:
        with _TEST_TRACER.start_as_current_span("caller") as span:
            ch = client._channel
            if kind == "UU":
                await ch.unary_unary("/t.E/UU", **ck)(b"x", metadata=md)
            elif kind == "US":
                async for _ in ch.unary_stream("/t.E/US", **ck)(b"x", metadata=md):
                    pass
            elif kind == "SU":
                await ch.stream_unary("/t.E/SU", **ck)(reqs(), metadata=md)
            else:
                async for _ in ch.stream_stream("/t.E/SS", **ck)(reqs(), metadata=md):
                    pass
            sc = span.get_span_context()
    finally:
        await client.close()
        await server.stop(None)
    return seen, sc


@SLOW
@given(kind=st.sampled_from(KINDS), md=_wire_md, tracing_on=st.booleans())
def test_p15_context_on_the_wire(kind, md, tracing_on):
    """P-15. Validates: AC-4.1, AC-4.2, AC-4.3, AC-4.4."""
    seen, sc = asyncio.run(_call_with_metadata(kind, md, tracing_on))
    for k, v in md:
        assert seen.get(k) == v
    assert "baggage" not in seen
    if tracing_on:
        assert seen["traceparent"].split("-")[2] == format(sc.span_id, "016x")
    else:
        assert "traceparent" not in seen


# =========================================================================== #
# AC-7: run report (P-8, P-16)
# =========================================================================== #
_phase = st.one_of(st.sampled_from(REPORT_ORDER), st.from_regex(r"[a-z]{1,8}", fullmatch=True))
_report_op = st.one_of(
    st.tuples(st.just("phase"), _phase, st.floats(-1e3, 1e4, allow_nan=False)),
    st.tuples(st.just("task"), st.sampled_from(["ok", "error", "cancelled"])),
    st.tuples(st.just("claim")),
    st.tuples(st.just("release_error")),
)


def _replay(ops):
    r = RunReport("s")
    model = {"phases": {}, "seen": [], "ok": 0, "err": 0, "claims": 0, "release_errors": 0}
    for op in ops:
        if op[0] == "phase":
            _, name, d = op
            r.add_phase(name, d)
            c = model["phases"].setdefault(name, [0, 0.0, 0.0])
            if name not in model["seen"]:
                model["seen"].append(name)
            c[0] += 1
            c[1] += max(d, 0.0)
            c[2] = max(c[2], max(d, 0.0))
        elif op[0] == "task":
            r.add_task(op[1])
            model["ok" if op[1] == "ok" else "err"] += 1
        elif op[0] == "claim":
            r.add_claim()
            model["claims"] += 1
        else:
            r.add_release_error()
            model["release_errors"] += 1
    return r, model


@FAST
@given(st.lists(st.tuples(_phase, st.floats(-1e3, 1e4, allow_nan=False)), max_size=60))
def test_p8_phase_stats_consistent(calls):
    """P-8. Validates: AC-7.3."""
    r = RunReport("s")
    for name, d in calls:
        r.add_phase(name, d)
    for name, (count, total, mx) in r.phases.items():
        assert 0.0 <= mx <= total + 1e-9 and total <= count * mx + 1e-6, (name, count, total, mx)


@FAST
@given(st.lists(_report_op, max_size=80))
def test_p16_report_keys_match_spec(ops):
    """P-16 (key set). Validates: AC-7.1."""
    r, _ = _replay(ops)
    d = r.to_dict()
    assert set(d) == DOCUMENTED_REPORT_KEYS, f"extra={set(d) - DOCUMENTED_REPORT_KEYS} " \
                                             f"missing={DOCUMENTED_REPORT_KEYS - set(d)}"


@FAST
@given(st.lists(_report_op, max_size=80))
def test_p16_report_matches_operations(ops):
    """P-16 (counts, order, round trip). Validates: AC-7.2, AC-7.4, AC-7.5."""
    r, m = _replay(ops)
    d = r.to_dict()
    assert d["tasks_ok"] == m["ok"] and d["tasks_err"] == m["err"] and d["claims"] == m["claims"]
    assert d["release_errors"] == m["release_errors"]
    want_order = [p for p in REPORT_ORDER if p in m["phases"]] + \
                 [p for p in m["seen"] if p not in REPORT_ORDER]
    assert list(d["phases"]) == want_order
    for p, (count, total, mx) in m["phases"].items():
        assert d["phases"][p] == {"count": count, "total_s": round(total, 2), "max_s": round(mx, 2)}
    assert json.loads(json.dumps(d)) == d
    rows = [ln.split()[0] for ln in r.summary().splitlines()[1:]
            if ln.strip() and not ln.strip().startswith(("-", "claims", "environment",
                                                         "release_errors="))]
    assert rows == want_order + ["TOTAL"]


# =========================================================================== #
# AC-3: trace shape (P-13, P-14)
# =========================================================================== #
_task_ops = st.lists(st.sampled_from(["exec", "write", "read", "raise"]), max_size=5)


@st.composite
def workload(draw):
    n = draw(st.integers(1, 6))
    return {
        "mode": draw(st.sampled_from(["sync", "async"])),
        "strategy": draw(st.sampled_from(["naive", "sliding", "pipelined"])),
        "concurrency": draw(st.integers(1, 4)),
        "ops": [draw(_task_ops) for _ in range(n)],
    }


def _process_fn(ops_by_task):
    def process(task, handle):
        for op in ops_by_task[task.id]:
            if op == "exec":
                handle.exec("echo hi", check=False)
            elif op == "write":
                handle.write_file("/tmp/f", b"x")
            elif op == "read":
                handle.write_file("/tmp/g", b"y")
                handle.read_file_bytes("/tmp/g")
            else:
                raise RuntimeError("task failed")
        return task.id
    return process


def _run_workload(w):
    tasks = [Task(id=f"t{i}", image="img") for i in range(len(w["ops"]))]
    ops = {t.id: o for t, o in zip(tasks, w["ops"])}
    cfg = FleetConfig(backend="mock", strategy=w["strategy"], max_concurrent=w["concurrency"],
                      tracing=TracerConfig(enable_tracing=True))
    if w["mode"] == "sync":
        fleet = SandboxFleet(cfg)
        fleet.load_tasks(tasks)
        try:
            fleet.run(_process_fn(ops), concurrency=w["concurrency"])
        except Exception as e:  # a raising task may fail the run; spans must still be right
            note(f"run raised {e!r}")
    else:
        fleet = AsyncSandboxFleet(cfg)
        fleet.load_tasks(tasks)
        proc = _process_fn(ops)

        async def aproc(task, handle):
            return await asyncio.to_thread(proc, task, handle)
        try:
            asyncio.run(fleet.run(aproc, concurrency=w["concurrency"]))
        except Exception as e:
            note(f"run raised {e!r}")


@SLOW
@given(workload())
def test_p13_one_rooted_trace_per_run(w):
    """P-13. Validates: AC-3.1, AC-3.2, AC-3.3, AC-3.5."""
    _EXPORTER.clear()
    _run_workload(w)
    spans = _EXPORTER.get_finished_spans()
    by_id = {s.context.span_id: s for s in spans}
    roots = [s for s in spans if s.name == "ate_env_fleet.run"]
    assert len(roots) == 1, [s.name for s in spans]
    root = roots[0]
    for s in spans:
        if s is root:
            continue
        cur, hops = s, 0
        while cur.parent is not None and cur.parent.span_id in by_id and hops < 50:
            cur = by_id[cur.parent.span_id]
            hops += 1
        assert cur is root, f"{s.name} does not reach ate_env_fleet.run (stops at {cur.name})"
        if s.name in ("ate_env_client.exec", "ate_env_client.write_file",
                      "ate_env_client.read_file"):
            assert by_id[s.parent.span_id].name == "ate_env_fleet.process", s.name
    assert {s.name for s in spans} <= DOCUMENTED_SPANS, {s.name for s in spans} - DOCUMENTED_SPANS
    keys = {k for s in spans for k in s.attributes}
    assert keys <= DOCUMENTED_ATTRS, f"undocumented attribute keys: {keys - DOCUMENTED_ATTRS}"


@SLOW
@given(ops=st.lists(st.sampled_from(["exec", "write", "read"]), min_size=1, max_size=12),
       threads=st.integers(1, 4))
def test_p14_exactly_one_first_command(ops, threads):
    """P-14. Validates: AC-3.4."""
    assume("exec" in ops)
    _EXPORTER.clear()
    fleet = SandboxFleet(FleetConfig(backend="mock", tracing=TracerConfig(enable_tracing=True)))
    fleet.load_tasks([Task(id="t", image="img")])
    handle = fleet.acquire(fleet.tasks[0])
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)  # make thread interleavings likely
    try:
        def worker(chunk):
            for op in chunk:
                if op == "exec":
                    handle.exec("echo hi", check=False)
                elif op == "write":
                    handle.write_file("/tmp/f", b"x")
                else:
                    handle.write_file("/tmp/g", b"y")
                    handle.read_file_bytes("/tmp/g")
        chunks = [ops[i::threads] for i in range(threads)]
        ts = [threading.Thread(target=worker, args=(c,)) for c in chunks]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
    finally:
        sys.setswitchinterval(old)
        fleet.release(handle)
    execs = sorted([s for s in _EXPORTER.get_finished_spans() if s.name == "ate_env_client.exec"],
                   key=lambda s: s.start_time)
    firsts = [s for s in execs if s.attributes.get("ate_env.first_command")]
    assert len(firsts) == 1, f"{len(firsts)} exec spans marked first_command"
    assert firsts[0] is execs[0]


# =========================================================================== #
# AC-2: missing extras (P-12)
# =========================================================================== #
@SLOW
@given(tracing_on=st.booleans(), metrics_on=st.booleans(),
       installed=st.sampled_from(["none", "tracing", "metrics", "both"]),
       runs=st.integers(1, 3))
def test_p12_missing_extras_noop(tracing_on, metrics_on, installed, runs):
    """P-12. Validates: AC-2.1, AC-2.2, AC-2.3."""
    from ate_env.telemetry import metrics as mt
    saved = (tr._OTEL, tr._MISSING_LOGGED, mt._PROM, mt._MISSING_LOGGED)
    tr._OTEL = None if installed in ("tracing", "both") else False
    mt._PROM = None if installed in ("metrics", "both") else False
    tr._MISSING_LOGGED = mt._MISSING_LOGGED = False
    records: list[logging.LogRecord] = []
    h = logging.Handler()
    h.emit = records.append
    root = logging.getLogger("ate_env")
    root.addHandler(h)
    from ate_env import ObservabilityConfig
    try:
        for _ in range(runs):
            fleet = SandboxFleet(FleetConfig(
                backend="mock", strategy="naive",
                tracing=TracerConfig(enable_tracing=tracing_on),
                observability=ObservabilityConfig(enable_metrics=metrics_on)))
            fleet.load_tasks([Task(id="t", image="img")])
            assert fleet.run(lambda t, h: t.id) == ["t"]
            assert fleet.report is not None
    finally:
        root.removeHandler(h)
        tr._OTEL, tr._MISSING_LOGGED, mt._PROM, mt._MISSING_LOGGED = saved
    warn = [r for r in records if r.levelno >= logging.WARNING]
    otel_lines = [r for r in warn if "tracing extra" in r.getMessage()]
    prom_lines = [r for r in warn if "metrics extra" in r.getMessage()]
    assert len(otel_lines) == (1 if tracing_on and installed in ("none", "metrics") else 0)
    assert len(prom_lines) == (1 if metrics_on and installed in ("none", "tracing") else 0)


# =========================================================================== #
# AC-8: metrics registry (P-17), on a fresh CollectorRegistry per example
# =========================================================================== #
DOCUMENTED_FLEET_FAMILIES = {
    "ate_env_fleet_phase_latency_seconds": ("phase", "backend", "family", "strategy", "status"),
    "ate_env_fleet_task_latency_seconds": ("strategy", "backend", "family", "status"),
    "ate_env_fleet_run_latency_seconds": ("strategy", "status"),
    "ate_env_fleet_claims_total": ("backend", "status"),
    "ate_env_fleet_tasks_total": ("strategy", "status"),
    "ate_env_fleet_warm_replicas": ("backend",),
    "ate_env_fleet_release_failures_total": ("backend",),
}


def _families(reg):
    out = {}
    for name, collector in reg._names_to_collectors.items():
        base = getattr(collector, "_name", name)
        if getattr(collector, "_type", "") == "counter":
            base += "_total"  # prometheus_client stores counters without the suffix
        out[base] = tuple(getattr(collector, "_labelnames", ()))
    return out


@FAST
@given(st.lists(st.sampled_from([None, True, False]), min_size=1, max_size=6))
def test_p17_lazy_single_registration(enables):
    """P-17. Validates: AC-8.2, AC-8.3."""
    from prometheus_client import CollectorRegistry

    from ate_env import ObservabilityConfig
    reg = CollectorRegistry()
    any_on = False
    for e in enables:
        obs_mod.Observer(ObservabilityConfig(enable_metrics=e), registry=reg)
        any_on = any_on or e is not False
        fams = _families(reg)
        if not any_on:
            assert fams == {}, fams
        else:
            assert fams == DOCUMENTED_FLEET_FAMILIES, (
                f"extra={set(fams) - set(DOCUMENTED_FLEET_FAMILIES)} "
                f"missing={set(DOCUMENTED_FLEET_FAMILIES) - set(fams)} "
                f"labels={ {k: v for k, v in fams.items() if DOCUMENTED_FLEET_FAMILIES.get(k) != v} }")

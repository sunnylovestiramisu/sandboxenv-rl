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

"""RunReport, Observer, Prometheus registration and label cardinality.

Prometheus tests skip cleanly in a venv without the metrics extra.
"""

from __future__ import annotations

import importlib
import json
import logging
import subprocess
import sys
import textwrap
import urllib.request

import pytest

from ate_env import AsyncSandboxFleet, FleetConfig, SandboxFleet, Task, observability
from ate_env.config import ObservabilityConfig, ValidationError
from ate_env.observability import Observer, RunReport, repo_family, serve_metrics
from ate_env.telemetry import metrics as client_metrics

REPORT_KEYS = {
    "strategy", "total_s", "phases", "claims", "tasks_ok", "tasks_err",
    "warm_replicas_total", "warm_replicas_peak", "environment",
}
MOCK_RUN_PHASES = {
    "preflight", "plan", "ensure_template", "warm_pool", "acquire", "process", "release",
    "teardown",
}


def _mock_fleet(strategy="sliding", n_tasks=8, images=2, **kw):
    fleet = SandboxFleet(FleetConfig(backend="mock", strategy=strategy, batch_size=2,
                                     max_warmpool_replicas=2, **kw))
    fleet.load_tasks([
        Task(id=f"t{i}", image=f"sweb.eval.x86_64.repo{i % images}__repo{i % images}-1:latest")
        for i in range(n_tasks)
    ])
    return fleet


# --- config ---------------------------------------------------------------- #


def test_observability_config_defaults_and_validation():
    cfg = ObservabilityConfig()
    assert (cfg.enable_metrics, cfg.enable_tracing, cfg.trace_service_name) == (
        True, False, "ate-env-fleet")
    with pytest.raises(ValidationError, match="metrics_port"):
        ObservabilityConfig(metrics_port=1)
    with pytest.raises(ValidationError):
        ObservabilityConfig(enable_metrics="yes")
    fc = FleetConfig.from_dict({"observability": {"enable_metrics": False}})
    assert fc.observability == ObservabilityConfig(enable_metrics=False)
    assert isinstance(fc.to_dict()["observability"], ObservabilityConfig)
    assert FleetConfig.from_dict(fc.to_dict()) == fc


# --- RunReport (AC-7) ------------------------------------------------------ #


def test_report_shape_and_summary_order():
    rep = RunReport("naive")
    for name in ("teardown", "custom", "process", "preflight", "release", "acquire",
                 "warm_pool", "plan", "ensure_template", "recycle", "reap"):
        rep.add_phase(name, 0.5)
    rep.add_phase("process", 1.5)
    rep.add_task("ok")
    rep.add_task("error")
    rep.add_claim()
    rep.total_s = 3.21
    d = rep.to_dict()
    assert set(d) == REPORT_KEYS
    assert d["phases"]["process"] == {"count": 2, "total_s": 2.0, "max_s": 1.5}
    assert (d["claims"], d["tasks_ok"], d["tasks_err"]) == (1, 1, 1)
    rows = [line.split()[0] for line in rep.summary().splitlines()[1:]
            if line.strip() and not line.strip().startswith(("-", "claims="))]
    assert rows == ["preflight", "plan", "ensure_template", "warm_pool", "acquire", "process",
                    "recycle", "release", "reap", "teardown", "custom", "TOTAL"]


@pytest.mark.parametrize("strategy", ["naive", "sliding", "pipelined"])
def test_report_after_mock_run(strategy):
    fleet = _mock_fleet(strategy, observability=ObservabilityConfig(enable_metrics=False))
    results = fleet.run(lambda task, handle: handle.exec("echo hi").exit_code, concurrency=4)
    assert results == [0] * 8
    rep = fleet.report
    assert rep is not None
    print("\n" + rep.summary())
    d = rep.to_dict()
    assert set(d) == REPORT_KEYS
    assert set(d["phases"]) == MOCK_RUN_PHASES
    for stats in d["phases"].values():
        assert set(stats) == {"count", "total_s", "max_s"}
    assert d["phases"]["process"]["count"] == 8
    assert d["phases"]["acquire"]["count"] == 8
    assert d["phases"]["ensure_template"]["count"] == 2
    assert (d["claims"], d["tasks_ok"], d["tasks_err"]) == (8, 8, 0)
    assert d["warm_replicas_total"] == 4 and d["warm_replicas_peak"] >= 2
    assert d["strategy"] == strategy
    assert d["environment"]["mock"]["tasks"] == 8
    assert "TOTAL" in rep.summary()


def test_report_counts_errors_and_phase_status():
    fleet = _mock_fleet("none", n_tasks=4, observability=ObservabilityConfig(enable_metrics=False))

    def process(task, handle):
        if task.id in ("t1", "t3"):
            raise RuntimeError("boom")
        return "ok"

    results = fleet.run(process, concurrency=2)
    assert [isinstance(r, RuntimeError) for r in results] == [False, True, False, True]
    assert (fleet.report.tasks_ok, fleet.report.tasks_err) == (2, 2)
    assert fleet.report.phases["process"][0] == 4


def test_report_write(tmp_path):
    fleet = _mock_fleet("naive", n_tasks=3, observability=ObservabilityConfig(enable_metrics=False))
    fleet.run(lambda task, handle: None)
    base = fleet.report.write(tmp_path / "reports")
    txt = base.with_name(base.name + ".txt")
    js = base.with_name(base.name + ".json")
    assert base.name.startswith("naive_3tasks_") and len(base.name.split("_")[-1]) == 15
    assert txt.read_text().startswith("── Run report (strategy=naive) ──")
    assert set(json.loads(js.read_text())) == REPORT_KEYS


async def test_report_async_fleet_parity():
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock", strategy="naive",
                                          observability=ObservabilityConfig(enable_metrics=False)))
    fleet.load_tasks([Task(id=f"t{i}", image="img") for i in range(5)])

    async def process(task, handle):
        return (await handle.exec_async("echo hi")).exit_code

    assert await fleet.run(process) == [0] * 5
    d = fleet.report.to_dict()
    assert {"acquire", "process", "release", "ensure_template"} <= set(d["phases"])
    assert (d["claims"], d["tasks_ok"]) == (5, 5)


def test_report_phases_outside_run_are_not_counted():
    fleet = _mock_fleet("naive", n_tasks=2, observability=ObservabilityConfig(enable_metrics=False))
    fleet.run(lambda task, handle: None)
    before = fleet.report.to_dict()
    fleet.close()  # a teardown after the run does not change the finished report
    assert fleet.report.to_dict() == before


def test_repo_family():
    assert repo_family("sweb.eval.x86_64.astropy__astropy-12907:latest") == "astropy"
    assert repo_family("docker.io/swebench/sweb.eval.x86_64.django_1776_django-11099:v1") == "django"
    assert repo_family(Task(id="1", image="x", metadata={"repo": "django/django"})) == "django"
    assert repo_family(Task(id="1", image="sweb.eval.arm64.sympy__sympy-1:latest")) == "sympy"
    assert repo_family("python:3.11") == "unknown"
    assert repo_family(None) == "unknown"


def test_observer_without_layers_is_report_only():
    obs = Observer(ObservabilityConfig(enable_metrics=False))
    assert obs.metrics is False and obs.tracer is None
    with obs.run("naive") as rep:
        with pytest.raises(ValueError):
            with obs.phase("acquire"):
                raise ValueError("x")
        obs.claim("mock")
        obs.claim("mock", "error")
        obs.warm_add("mock", 3)
        obs.warm_remove("mock", 2)
        obs.warm_add("mock", 1)
        obs.task_done("mock", "fam", "ok", 0.1)
    assert rep.phases["acquire"][0] == 1
    assert (rep.claims, rep.warm_total, rep.peak_warm, rep.tasks_ok) == (1, 4, 3, 1)
    obs.warm_reset()


def test_missing_metrics_extra_logs_one_warning(monkeypatch, caplog):
    monkeypatch.setattr(client_metrics, "_PROM", False)
    monkeypatch.setattr(client_metrics, "_MISSING_LOGGED", False)
    monkeypatch.setattr(observability, "_METRICS_READY", False)
    caplog.set_level(logging.WARNING)
    for _ in range(3):
        assert Observer(ObservabilityConfig(enable_metrics=True)).metrics is False
    fleet = _mock_fleet("naive", n_tasks=2)
    fleet.run(lambda task, handle: None)
    assert fleet.report.tasks_ok == 2
    warnings = [r for r in caplog.records if "metrics" in r.getMessage().lower()]
    assert len(warnings) == 1
    assert "ate-env-client[metrics]" in warnings[0].getMessage()
    with pytest.raises(RuntimeError, match="metrics"):
        serve_metrics(0)


# --- Prometheus registry (AC-8) -------------------------------------------- #

FLEET_LABELS = {
    "atefleet_phase_latency_seconds": ("phase", "backend", "family", "strategy", "status"),
    "atefleet_task_latency_seconds": ("strategy", "backend", "family", "status"),
    "atefleet_run_latency_seconds": ("strategy", "status"),
    "atefleet_claims_total": ("backend", "status"),
    "atefleet_tasks_total": ("strategy", "status"),
    "atefleet_warm_replicas": ("backend",),
}
CLIENT_FAMILIES = (
    "ate_env_client_acquire_seconds", "ate_env_client_exec_seconds",
    "ate_env_client_suspend_seconds", "ate_env_client_wake_seconds",
    "ate_env_client_release_seconds",
)


def _collector(registry, name):
    reg = registry._names_to_collectors
    return reg.get(name) or reg.get(name.removesuffix("_total"))


def test_registry_untouched_at_import():
    pytest.importorskip("prometheus_client")
    code = textwrap.dedent(
        """
        import sys
        import prometheus_client
        before = set(prometheus_client.REGISTRY._names_to_collectors)
        import ate_env, ate_env.observability, ate_env.telemetry, ate_env.telemetry.metrics
        after = set(prometheus_client.REGISTRY._names_to_collectors)
        assert after == before, sorted(after - before)
        print("registered at import:", sorted(after - before))
        """
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert "registered at import: []" in out.stdout


def test_registry_import_does_not_load_prometheus():
    code = "import sys, ate_env, ate_env.observability; assert 'prometheus_client' not in sys.modules"
    subprocess.run([sys.executable, "-c", code], check=True)


def test_registry_observer_registers_documented_labels_once():
    prom = pytest.importorskip("prometheus_client")
    first = Observer(ObservabilityConfig(enable_metrics=True))
    assert first.metrics
    second = Observer(ObservabilityConfig(enable_metrics=True))
    assert second.metrics
    importlib.reload(observability)  # module reload re-registers through _metric
    third = observability.Observer(ObservabilityConfig(enable_metrics=True))
    assert third.metrics
    assert observability.PHASE_LATENCY is _collector(prom.REGISTRY, "atefleet_phase_latency_seconds")
    for name, labels in FLEET_LABELS.items():
        collector = _collector(prom.REGISTRY, name)
        assert collector is not None, name
        assert tuple(collector._labelnames) == labels, name


def test_registry_serve_metrics_answers():
    pytest.importorskip("prometheus_client")
    Observer(ObservabilityConfig(enable_metrics=True))
    server, thread = serve_metrics(port=0, addr="127.0.0.1")
    try:
        port = server.server_address[1]
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    finally:
        server.shutdown()
        server.server_close()
    assert "atefleet_phase_latency_seconds" in body


def test_registry_metrics_after_mock_run():
    prom = pytest.importorskip("prometheus_client")
    fleet = _mock_fleet("naive", n_tasks=4)
    fleet.run(lambda task, handle: handle.exec("echo hi").exit_code)

    def value(name, **labels):
        return prom.REGISTRY.get_sample_value(name, labels) or 0.0

    assert value("atefleet_phase_latency_seconds_count", phase="process", backend="mock",
                 family="repo0", strategy="naive", status="ok") >= 2
    assert value("atefleet_run_latency_seconds_count", strategy="naive", status="ok") >= 1
    assert value("atefleet_claims_total", backend="mock", status="ok") >= 4
    assert value("atefleet_tasks_total", strategy="naive", status="ok") >= 4
    assert value("ate_env_client_exec_seconds_count", status="ok") >= 4


# --- cardinality (AC-9) ---------------------------------------------------- #

FAMILIES = ("django", "astropy", "sympy", "requests", "flask")
FORBIDDEN_LABELS = {"env_id", "instance", "image", "run_id", "id", "env", "actor"}


_CARDINALITY_SCRIPT = textwrap.dedent(
    """
    import json
    import prometheus_client
    from ate_env import FleetConfig, SandboxFleet, Task

    FAMILIES = ("django", "astropy", "sympy", "requests", "flask")
    images = [f"sweb.eval.x86_64.{FAMILIES[i % 5]}__{FAMILIES[i % 5]}-{1000 + i}:latest"
              for i in range(50)]
    tasks = [Task(id=f"instance-{i}", image=images[i % 50]) for i in range(200)]
    fleet = SandboxFleet(FleetConfig(backend="mock", strategy="sliding", batch_size=10,
                                     max_warmpool_replicas=2))
    fleet.load_tasks(tasks)
    env_ids = set()

    def process(task, handle):
        env_ids.add(handle.sandbox_id)
        return handle.exec("echo hi").exit_code

    assert fleet.run(process, concurrency=8) == [0] * 200
    samples = []
    for metric in prometheus_client.REGISTRY.collect():
        if metric.name.startswith(("atefleet_", "ate_env_client_")):
            samples += [[metric.name, s.labels] for s in metric.samples]
    print(json.dumps({
        "samples": samples,
        "env_ids": sorted(env_ids),
        "run_id": fleet.run_id,
        "task_ids": [t.id for t in tasks],
        "images": images,
    }))
    """
)


def test_cardinality_bounded_labels_over_many_images():
    pytest.importorskip("prometheus_client")
    out = subprocess.run([sys.executable, "-c", _CARDINALITY_SCRIPT],
                         capture_output=True, text=True, check=True)
    data = json.loads(out.stdout.strip().splitlines()[-1])
    assert data["samples"]
    families: set[str] = set()
    values: set[str] = set()
    for name, labels in data["samples"]:
        assert not (set(labels) & FORBIDDEN_LABELS), (name, labels)
        values.update(labels.values())
        if "family" in labels:
            families.add(labels["family"])
    # "-" is the placeholder for phases without a task (preflight, plan, teardown).
    assert families - {"-"} == set(FAMILIES)
    assert len(families - {"-"}) <= 5
    forbidden = (set(data["env_ids"]) | {data["run_id"]} | set(data["task_ids"])
                 | set(data["images"]))
    assert not (values & forbidden)
    assert not any(":" in v or "@" in v or "/" in v for v in values), values

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
import re
import subprocess
import sys
import textwrap
import urllib.request

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from ate_env_rl import AsyncSandboxFleet, FleetConfig, SandboxFleet, Task, observability
from ate_env_rl.config import ObservabilityConfig, ValidationError
from ate_env_rl.observability import Observer, RunReport, repo_family, serve_metrics
from ate_env_rl.telemetry import metrics as client_metrics

REPORT_KEYS = {
    "strategy", "total_s", "phases", "claims", "tasks_ok", "tasks_err", "release_errors",
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
        None, False, "ate-env-fleet")
    with pytest.raises(ValidationError, match="metrics_port"):
        ObservabilityConfig(metrics_port=1)
    with pytest.raises(ValidationError):
        ObservabilityConfig(enable_metrics="yes")
    fc = FleetConfig.from_dict({"observability": {"enable_metrics": False}})
    assert fc.observability == ObservabilityConfig(enable_metrics=False)
    assert fc.to_dict()["observability"] == {
        "enable_metrics": False, "enable_tracing": False, "trace_service_name": "ate-env-fleet"}
    json.dumps(fc.to_dict())
    assert FleetConfig.from_dict(fc.to_dict()) == fc


# --- RunReport (AC-7) ------------------------------------------------------ #


def test_report_shape_and_summary_order():
    rep = RunReport("naive")
    for name in ("teardown", "custom", "process", "preflight", "release", "acquire",
                 "warm_pool", "plan", "ensure_template", "recycle"):
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
                    "recycle", "release", "teardown", "custom", "TOTAL"]


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


def test_report_summary_order_has_reap():
    rep = RunReport("naive")
    for name in ("teardown", "reap", "release"):
        rep.add_phase(name, 0.5)
    rows = [line.split()[0] for line in rep.summary().splitlines()[1:]
            if line.strip() and not line.strip().startswith(("-", "claims="))]
    assert rows == ["release", "reap", "teardown", "TOTAL"]


def test_report_clamps_negative_phase_duration():
    """P-8: a negative duration counts as 0, so 0 <= max_s <= total_s <= count * max_s."""
    rep = RunReport("naive")
    rep.add_phase("acquire", 2.0)
    rep.add_phase("acquire", -5.0)
    count, total, mx = rep.phases["acquire"]
    assert (count, total, mx) == (2, 2.0, 2.0)
    assert 0 <= mx <= total <= count * mx
    assert rep.to_dict()["phases"]["acquire"] == {"count": 2, "total_s": 2.0, "max_s": 2.0}
    rep.add_phase("plan", -1.0)
    assert rep.phases["plan"] == [1, 0.0, 0.0]


def test_report_phase_bounds_hold_in_to_dict_for_short_phases():
    """AC-7: the rounded phase stats keep 0 <= max_s <= total_s <= count * max_s.

    Rounded separately, 3 phases of 0.004 s gave total_s 0.01 and max_s 0.0.
    """
    rep = RunReport("naive")
    for _ in range(3):
        rep.add_phase("acquire", 0.004)
    rep.add_phase("plan", 0.004)
    phases = rep.to_dict()["phases"]
    for p in phases.values():
        assert 0 <= p["max_s"] <= p["total_s"] <= p["count"] * p["max_s"], p
    assert phases["acquire"] == {"count": 3, "total_s": 0.01, "max_s": 0.01}
    assert phases["plan"] == {"count": 1, "total_s": 0.01, "max_s": 0.01}


def _summary_rows(text: str) -> dict[str, tuple[str, int, str]]:
    """Phase rows of summary(): name -> (total, n, max) as printed."""
    rows = {}
    for m in re.finditer(r"^  (\S+)\s+(\S+)s  \(n=(\d+), max=(\S+)s\)$", text, re.M):
        rows[m[1]] = (m[2], int(m[3]), m[4])
    return rows


def test_report_summary_rows_match_to_dict(tmp_path):
    """The .txt and .json from one write() show the same phase values.

    summary() formatted the raw stats, so 3 phases of 0.004 s printed
    max=0.00s next to "max_s": 0.01 in the JSON.
    """
    rep = RunReport("naive")
    for _ in range(3):
        rep.add_phase("acquire", 0.004)
    base = rep.write(tmp_path)
    txt = base.with_name(base.name + ".txt").read_text()
    js = json.loads(base.with_name(base.name + ".json").read_text())
    assert js["phases"]["acquire"] == {"count": 3, "total_s": 0.01, "max_s": 0.01}
    assert _summary_rows(txt) == {"acquire": ("0.01", 3, "0.01")}


def test_report_infinite_phase_duration_writes(tmp_path):
    """An infinite duration does not make to_dict() or write() raise (math.ceil(inf))."""
    rep = RunReport("naive")
    rep.add_phase("acquire", float("inf"))
    rep.add_phase("plan", 0.004)
    phases = rep.to_dict()["phases"]
    assert phases["acquire"] == {"count": 1, "total_s": float("inf"), "max_s": float("inf")}
    base = rep.write(tmp_path)
    js = json.loads(base.with_name(base.name + ".json").read_text())
    assert js["phases"]["acquire"]["max_s"] == float("inf")
    assert js["phases"]["plan"] == {"count": 1, "total_s": 0.01, "max_s": 0.01}
    assert _summary_rows(base.with_name(base.name + ".txt").read_text())["acquire"] == (
        "inf", 1, "inf"
    )


# Durations around the 0.01 s rounding step (where rounding total_s and max_s
# apart breaks the bound), negative ones (clamped) and the wide range of P-8.
# Runs of one duration on the 0.01 s grid reach total_s == count * max_s,
# where the float product can land just under the rounded total (3 * 0.29).
_durations = st.one_of(
    st.floats(0, 0.03),
    st.sampled_from([0.004, 0.005, 0.0049999, 0.0050001]),
    st.integers(0, 100).map(lambda k: k / 100),
    st.floats(-1e3, 1e4, allow_nan=False),
)
_phase_runs = st.tuples(
    st.sampled_from(["acquire", "process", "release", "reap", "teardown", "custom"]),
    _durations,
    st.integers(1, 5),
)


@settings(max_examples=300, deadline=None)
@given(st.lists(_phase_runs, max_size=20))
@example([("acquire", 0.004, 3)])  # #273: total_s 0.01, max_s 0.0
@example([("acquire", 0.29, 3)])  # 3 * 0.29 == 0.8699999999999999 < round(total_s) 0.87
def test_phase_stats_consistent(runs):
    """P-8: every phase keeps 0 <= max_s <= total_s <= count * max_s, raw and in to_dict()."""
    ops = [(name, dur) for name, dur, n in runs for _ in range(n)]
    rep = RunReport("naive")
    for name, dur in ops:
        rep.add_phase(name, dur)
    phases = rep.to_dict()["phases"]
    assert set(phases) == {name for name, _ in ops}
    for name, (count, total, mx) in rep.phases.items():
        assert count == sum(1 for n, _ in ops if n == name)
        assert 0 <= mx <= total <= count * mx * (1 + 1e-12)
        p = phases[name]
        assert p["count"] == count
        assert 0 <= p["max_s"] <= p["total_s"] <= p["count"] * p["max_s"], p
        # Rounding moves each value by less than 0.01 s.
        assert abs(p["max_s"] - mx) < 0.01 and abs(p["total_s"] - total) < 0.01 + 1e-9, (p, total, mx)
    # summary() prints the to_dict() values, so the .txt and .json agree.
    rows = _summary_rows(rep.summary())
    assert rows == {
        name: (f"{p['total_s']:.2f}", p["count"], f"{p['max_s']:.2f}") for name, p in phases.items()
    }


def test_report_task_done_clamps_negative_duration():
    obs = Observer(ObservabilityConfig(enable_metrics=False))
    with obs.run("naive") as rep:
        obs.task_done("mock", "fam", "ok", -5.0)
    assert (rep.tasks_ok, rep.tasks_err) == (1, 0)
    assert all(0 <= c[2] <= c[1] for c in rep.phases.values())


def test_report_task_done_negative_duration_observes_zero():
    _, reg = _custom_registry()
    obs = Observer(ObservabilityConfig(enable_metrics=True), registry=reg)
    with obs.run("naive") as rep:
        obs.task_done("mock", "fam", "ok", -5.0)
    assert rep.tasks_ok == 1
    labels = {"strategy": "naive", "backend": "mock", "family": "fam", "status": "ok"}
    assert reg.get_sample_value("ate_env_fleet_task_latency_seconds_count", labels) == 1
    assert reg.get_sample_value("ate_env_fleet_task_latency_seconds_sum", labels) == 0.0


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
    assert "ate-env-rl[metrics]" in warnings[0].getMessage()
    with pytest.raises(RuntimeError, match="metrics"):
        serve_metrics(0)


# --- Prometheus registry (AC-8) -------------------------------------------- #

FLEET_LABELS = {
    "ate_env_fleet_phase_latency_seconds": ("phase", "backend", "family", "strategy", "status"),
    "ate_env_fleet_task_latency_seconds": ("strategy", "backend", "family", "status"),
    "ate_env_fleet_run_latency_seconds": ("strategy", "status"),
    "ate_env_fleet_claims_total": ("backend", "status"),
    "ate_env_fleet_tasks_total": ("strategy", "status"),
    "ate_env_fleet_warm_replicas": ("backend",),
    "ate_env_fleet_release_failures_total": ("backend",),
}
CLIENT_FAMILIES = (
    "ate_env_client_acquire_seconds", "ate_env_client_exec_seconds",
    "ate_env_client_suspend_seconds", "ate_env_client_warm_claim_seconds",
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
        import ate_env_rl, ate_env_rl.observability, ate_env_rl.telemetry, ate_env_rl.telemetry.metrics
        after = set(prometheus_client.REGISTRY._names_to_collectors)
        assert after == before, sorted(after - before)
        print("registered at import:", sorted(after - before))
        """
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert "registered at import: []" in out.stdout


def test_registry_import_does_not_load_prometheus():
    code = "import sys, ate_env_rl, ate_env_rl.observability; assert 'prometheus_client' not in sys.modules"
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
    assert observability.PHASE_LATENCY is _collector(prom.REGISTRY, "ate_env_fleet_phase_latency_seconds")
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
    assert "ate_env_fleet_phase_latency_seconds" in body


def test_registry_metrics_after_mock_run():
    prom = pytest.importorskip("prometheus_client")
    fleet = _mock_fleet("naive", n_tasks=4)
    fleet.run(lambda task, handle: handle.exec("echo hi").exit_code)

    def value(name, **labels):
        return prom.REGISTRY.get_sample_value(name, labels) or 0.0

    assert value("ate_env_fleet_phase_latency_seconds_count", phase="process", backend="mock",
                 family="repo0", strategy="naive", status="ok") >= 2
    assert value("ate_env_fleet_run_latency_seconds_count", strategy="naive", status="ok") >= 1
    assert value("ate_env_fleet_claims_total", backend="mock", status="ok") >= 4
    assert value("ate_env_fleet_tasks_total", strategy="naive", status="ok") >= 4
    assert value("ate_env_client_exec_seconds_count", status="ok") >= 4


# --- cardinality (AC-9) ---------------------------------------------------- #

FAMILIES = ("django", "astropy", "sympy", "requests", "flask")
FORBIDDEN_LABELS = {"env_id", "instance", "image", "run_id", "id", "env", "actor"}


_CARDINALITY_SCRIPT = textwrap.dedent(
    """
    import json
    import prometheus_client
    from ate_env_rl import FleetConfig, SandboxFleet, Task

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
        if metric.name.startswith(("ate_env_fleet_", "ate_env_client_")):
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


def _family_of(repo):
    return repo_family(Task(id="1", image="x", metadata={"repo": repo}))


def test_cardinality_reserved_repo_names(monkeypatch):
    """P-3: a repo named other or unknown is not confused with the reserved values."""
    monkeypatch.setattr(observability, "_FAMILIES", set())
    assert _family_of("acme/other") == "other_repo"
    assert _family_of("acme/unknown") == "unknown_repo"
    assert _family_of("ACME/Other") == "other_repo"
    assert repo_family("sweb.eval.x86_64.unknown__unknown-1:latest") == "unknown_repo"
    assert repo_family("python:3.11") == "unknown"
    assert not ({"other", "unknown"} & observability._FAMILIES)
    assert observability._FAMILIES == {"other_repo", "unknown_repo"}


def test_cardinality_escaped_repo_names_stay_unique(monkeypatch):
    """P-3: a repo literally named other_repo does not share a label with repo other."""
    monkeypatch.setattr(observability, "_FAMILIES", set())
    repos = ["other", "other_repo", "other_repo_repo", "unknown", "unknown_repo"]
    labels = [_family_of(f"acme/{r}") for r in repos]
    assert labels == [
        "other_repo",
        "other_repo_repo",
        "other_repo_repo_repo",
        "unknown_repo",
        "unknown_repo_repo",
    ]
    assert len(set(labels)) == len(repos)
    # Names that only look similar are not escaped.
    assert _family_of("acme/other_repos") == "other_repos"
    assert _family_of("acme/my_other_repo") == "my_other_repo"


def test_cardinality_full_registry_only_new_families_get_other(monkeypatch):
    """P-1..P-3: past MAX_FAMILIES only new families are "other"; known ones keep their name."""
    monkeypatch.setattr(observability, "_FAMILIES", set())
    names = [f"fam{i}" for i in range(observability.MAX_FAMILIES)]
    assert [_family_of(f"org/{n}") for n in names] == names
    assert _family_of("org/newfam") == "other"
    assert _family_of("acme/other") == "other"  # other_repo is new, and the registry is full
    assert [_family_of(f"org/{n}") for n in names] == names
    assert observability._FAMILIES == set(names)

    # A reserved-name repo registered before the registry fills keeps its label.
    monkeypatch.setattr(observability, "_FAMILIES", set())
    assert _family_of("acme/other") == "other_repo"
    for n in names[1:]:
        _family_of(f"org/{n}")
    assert len(observability._FAMILIES) == observability.MAX_FAMILIES
    assert _family_of("acme/other") == "other_repo"
    assert _family_of("org/newfam") == "other"
    assert not ({"other", "unknown"} & observability._FAMILIES)

# --- PR #109 review findings ----------------------------------------------- #


def _custom_registry():
    prom = pytest.importorskip("prometheus_client")
    return prom, prom.CollectorRegistry()


def test_warm_gauge_adds_up_across_observers_and_resets_only_own_share():
    """Finding 1: two fleets with one backend label add up; a reset removes only its share."""
    _, reg = _custom_registry()
    a = Observer(ObservabilityConfig(enable_metrics=True), registry=reg)
    b = Observer(ObservabilityConfig(enable_metrics=True), registry=reg)

    def gauge():
        return reg.get_sample_value("ate_env_fleet_warm_replicas", {"backend": "substrate"})

    a.warm_add("substrate", 4)
    b.warm_add("substrate", 1)
    assert gauge() == 5
    a.warm_remove("substrate", 1)
    assert gauge() == 4
    b.warm_remove("substrate", 7)  # more than b holds: b only removes its own 1
    assert gauge() == 3
    b.warm_add("substrate", 2)
    a.warm_reset()  # a's 3 go; b's 2 stay
    assert gauge() == 2
    b.warm_reset()
    assert gauge() == 0


def test_family_label_is_capped(monkeypatch):
    """Finding 3: distinct families past MAX_FAMILIES are reported as "other"."""
    monkeypatch.setattr(observability, "MAX_FAMILIES", 3)
    monkeypatch.setattr(observability, "_FAMILIES", set())
    got = [repo_family(Task(id=str(i), image="x", metadata={"repo": f"org/repo{i}"}))
           for i in range(3000)]
    assert got[:3] == ["repo0", "repo1", "repo2"]
    assert set(got[3:]) == {"other"}
    assert repo_family(Task(id="a", image="x", metadata={"repo": "org/repo1"})) == "repo1"
    assert repo_family("python:3.11") == "unknown"  # does not use up the cap


def test_report_write_never_overwrites(tmp_path, monkeypatch):
    """Finding 4: two reports in the same second get distinct names."""
    import datetime as real_datetime

    class FrozenDatetime(real_datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 23, 12, 0, 0)

    monkeypatch.setattr(observability.datetime, "datetime", FrozenDatetime)
    first, second = RunReport("naive"), RunReport("naive")
    first.add_task("ok")
    second.add_task("error")
    a = first.write(tmp_path)
    b = second.write(tmp_path)
    assert a.name == "naive_1tasks_20261023-120000"
    assert b.name == "naive_1tasks_20261023-120000-1"
    assert json.loads(a.with_name(a.name + ".json").read_text())["tasks_ok"] == 1
    assert json.loads(b.with_name(b.name + ".json").read_text())["tasks_err"] == 1


async def test_async_report_holds_only_the_phases_inside_run():
    """Finding 5: under ``async with``, setup and teardown phases are not in the report."""
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock", strategy="naive",
                                          observability=ObservabilityConfig(enable_metrics=False)))
    fleet.load_tasks([Task(id=f"t{i}", image="img") for i in range(2)])
    async with fleet:
        await fleet.run(lambda task, handle: None)
    phases = set(fleet.report.to_dict()["phases"])
    assert {"acquire", "process", "release"} <= phases
    assert not phases & {"preflight", "plan", "warm_pool", "teardown"}


def test_overlapping_runs_on_one_observer_raise():
    """Finding 6: a second run() while one is in progress raises; the first report is intact."""
    obs = Observer(ObservabilityConfig(enable_metrics=False))
    with obs.run("slow") as slow:
        with pytest.raises(RuntimeError, match="already in progress"):
            with obs.run("fast"):
                pass
        with obs.phase("process"):
            pass
    assert slow.phases["process"][0] == 1
    with obs.run("next") as nxt:  # the observer is free again
        pass
    assert nxt.strategy == "next"


async def test_overlapping_async_fleet_runs_raise():
    import asyncio

    fleet = AsyncSandboxFleet(FleetConfig(backend="mock", strategy="naive",
                                          observability=ObservabilityConfig(enable_metrics=False)))
    fleet.load_tasks([Task(id="t0", image="img")])
    gate = asyncio.Event()

    async def slow(task, handle):
        await gate.wait()

    first = asyncio.ensure_future(fleet.run(slow))
    await asyncio.sleep(0.05)
    with pytest.raises(RuntimeError, match="already in progress"):
        await fleet.run(slow)
    gate.set()
    await first
    assert fleet.report.tasks_ok == 1


def test_serve_metrics_custom_registry():
    """Findings 7 and 8: Observer and serve_metrics accept a registry."""
    prom, reg = _custom_registry()
    obs = Observer(ObservabilityConfig(enable_metrics=True), registry=reg)
    with obs.run("custom-reg-only"):
        with obs.phase("process", backend="mock", family="fam"):
            pass
    server, _thread = serve_metrics(port=0, addr="127.0.0.1", registry=reg)
    try:
        port = server.server_address[1]
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    finally:
        server.shutdown()
        server.server_close()
    assert 'ate_env_fleet_phase_latency_seconds_count{backend="mock",family="fam"' in body
    assert "ate_env_client_" not in body  # client histograms stay on the global registry
    assert prom.REGISTRY.get_sample_value(
        "ate_env_fleet_run_latency_seconds_count", {"strategy": "custom-reg-only", "status": "ok"}
    ) is None
    assert reg.get_sample_value(
        "ate_env_fleet_run_latency_seconds_count", {"strategy": "custom-reg-only", "status": "ok"}
    ) == 1


def test_metric_helper_checks_type_and_labels():
    """Finding 8: a reuse with another type or other labels is an error, not a silent reuse."""
    prom, reg = _custom_registry()
    m = client_metrics._metric(prom.Counter, "ate_env_test_typed_total", "t", ["a"], registry=reg)
    assert client_metrics._metric(prom.Counter, "ate_env_test_typed_total", "t", ["a"],
                                  registry=reg) is m
    with pytest.raises(ValueError, match="already exists"):
        client_metrics._metric(prom.Gauge, "ate_env_test_typed_total", "t", ["a"], registry=reg)
    with pytest.raises(ValueError, match="already exists"):
        client_metrics._metric(prom.Counter, "ate_env_test_typed_total", "t", ["b"], registry=reg)
    # The same name on another registry is another collector.
    other = prom.CollectorRegistry()
    assert client_metrics._metric(prom.Counter, "ate_env_test_typed_total", "t", ["a"],
                                  registry=other) is not m


def test_service_name_precedence_is_logged(caplog):
    """Finding 9: both tracing switches on logs which service name wins."""
    from ate_env_rl import TracerConfig

    caplog.set_level(logging.DEBUG, logger="ate_env_rl.fleet")
    SandboxFleet(FleetConfig(
        backend="mock",
        tracing=TracerConfig(enable_tracing=True, trace_service_name="trainer"),
        observability=ObservabilityConfig(enable_metrics=False, enable_tracing=True),
    ))
    msgs = [r.getMessage() for r in caplog.records if r.name == "ate_env_rl.fleet"]
    assert any("'ate-env-fleet'" in m and "'trainer'" in m for m in msgs), msgs


def test_default_metrics_without_extra_logs_info_not_warning(monkeypatch, caplog):
    """Finding 10: the default setting does not warn about an extra nobody asked for."""
    monkeypatch.setattr(client_metrics, "_PROM", False)
    monkeypatch.setattr(client_metrics, "_MISSING_LOGGED", False)
    monkeypatch.setattr(observability, "_METRICS_OFF_LOGGED", False)
    caplog.set_level(logging.INFO)
    for _ in range(2):
        assert Observer(ObservabilityConfig()).metrics is False
    records = [r for r in caplog.records if "metrics" in r.getMessage().lower()]
    assert [r.levelno for r in records] == [logging.INFO]


def test_shared_driver_logs_a_warning(caplog):
    """Finding 12: a second fleet on one driver warns that it takes over the events."""
    from ate_env_rl.backend.mock import MockBackendDriver

    driver = MockBackendDriver()
    cfg = FleetConfig(backend="mock", observability=ObservabilityConfig(enable_metrics=False))
    caplog.set_level(logging.WARNING, logger="ate_env_rl.fleet")
    first = SandboxFleet(cfg, driver=driver)
    assert not caplog.records
    second = SandboxFleet(cfg, driver=driver)
    assert driver.observer is second.observer and driver.observer is not first.observer
    assert any("already belongs to another fleet" in r.getMessage() for r in caplog.records)


async def test_async_run_releases_without_blocking_the_loop(monkeypatch):
    """Finding 13: AsyncSandboxFleet.run releases with release_async, never the sync release."""
    from ate_env_rl.handle import SandboxHandle

    def no_sync_release(self):
        raise AssertionError("sync release() called on the event loop")

    monkeypatch.setattr(SandboxHandle, "release", no_sync_release)
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock", strategy="naive",
                                          observability=ObservabilityConfig(enable_metrics=False)))
    fleet.load_tasks([Task(id=f"t{i}", image="img") for i in range(3)])
    assert await fleet.run(lambda task, handle: 1) == [1, 1, 1]
    assert fleet.report.phases["release"][0] == 3


async def test_cancelled_phase_has_status_cancelled():
    """Finding 14: a cancelled phase is labeled cancelled, not error."""
    import asyncio

    _, reg = _custom_registry()
    obs = Observer(ObservabilityConfig(enable_metrics=True), registry=reg)

    async def straggler():
        with obs.phase("process", backend="mock", family="fam"):
            await asyncio.sleep(10)

    task = asyncio.ensure_future(straggler())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    labels = {"phase": "process", "backend": "mock", "family": "fam", "strategy": "-"}
    assert reg.get_sample_value("ate_env_fleet_phase_latency_seconds_count",
                                {**labels, "status": "cancelled"}) == 1
    assert reg.get_sample_value("ate_env_fleet_phase_latency_seconds_count",
                                {**labels, "status": "error"}) is None


def test_cancelled_task_has_status_cancelled():
    """An interrupted task in process_parallel is labeled cancelled, not error."""
    from types import SimpleNamespace

    from ate_env_rl.strategies import process_parallel

    _, reg = _custom_registry()
    obs = Observer(ObservabilityConfig(enable_metrics=True), registry=reg)
    fleet = SimpleNamespace(observer=obs, backend_label="mock",
                            acquire=lambda task: object(), release=lambda handle: None)

    def interrupted(task, handle):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        process_parallel(fleet, [Task(id="t0", image="img", metadata={"repo": "o/fam"})],
                         interrupted, concurrency=1)
    labels = {"strategy": "-", "backend": "mock", "family": "fam"}
    assert reg.get_sample_value("ate_env_fleet_tasks_total",
                                {"strategy": "-", "status": "cancelled"}) == 1
    assert reg.get_sample_value("ate_env_fleet_tasks_total",
                                {"strategy": "-", "status": "error"}) is None
    assert reg.get_sample_value("ate_env_fleet_task_latency_seconds_count",
                                {**labels, "status": "cancelled"}) == 1


async def test_cancelled_async_task_has_status_cancelled():
    """A straggler cancelled in AsyncSandboxFleet.run is labeled cancelled, not error."""
    import asyncio

    prom = pytest.importorskip("prometheus_client")
    get = prom.REGISTRY.get_sample_value

    def tasks_total(status):
        return get("ate_env_fleet_tasks_total", {"strategy": "naive", "status": status}) or 0.0

    before = {s: tasks_total(s) for s in ("cancelled", "error")}
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock", strategy="naive",
                                          observability=ObservabilityConfig(enable_metrics=True)))
    fleet.load_tasks([Task(id="t0", image="img")])
    started = asyncio.Event()

    async def straggler(task, handle):
        started.set()
        await asyncio.sleep(10)

    run = asyncio.ensure_future(fleet.run(straggler))
    await asyncio.wait_for(started.wait(), timeout=5)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert tasks_total("cancelled") == before["cancelled"] + 1
    assert tasks_total("error") == before["error"]


def test_registry_new_custom_registry_gets_its_own_collectors():
    """An Observer on a new registry records there, even when the id is reused."""
    import gc

    seen_ids = set()
    for _ in range(50):
        _, reg = _custom_registry()
        seen_ids.add(id(reg))
        obs = Observer(ObservabilityConfig(enable_metrics=True), registry=reg)
        obs.task_done("mock", "fam", "ok", 0.1)
        assert reg.get_sample_value("ate_env_fleet_tasks_total",
                                    {"strategy": "-", "status": "ok"}) == 1
        del obs, reg
        gc.collect()
    # CPython reuses the freed id almost every time, so the loop hit the reuse case.
    assert len(seen_ids) < 50

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

"""ate_env_openhands.agent_sandbox_rl: agent-sandbox-rl's names on ate-env.

The mapping onto ate_env_rl.FleetConfig, the run() signature, the handles'
cluster_name, and an agent-sandbox-rl program that runs against the fakes with
only its imports changed. With AGENT_SANDBOX_RL_EXAMPLE set to agent-sandbox's
run_openhands_fleet.py, the upstream file itself runs the same way.
"""

from __future__ import annotations

import difflib
import functools
import io
import json
import logging
import runpy
from contextlib import redirect_stdout
from pathlib import Path

import ate_env_rl
import pydantic
import pytest
from ate_env_rl import DEFAULT_ATESPACE
from ate_env_rl.backend.mock import MockBackendDriver
from ate_env_rl.errors import InfrastructureError, PreflightError

import ate_env_openhands.agent_sandbox_rl.adapters.openhands as compat_openhands
from ate_env_openhands import fleet as oh_fleet
from ate_env_openhands.agent_sandbox_rl import (
    ClusterConfig,
    FleetConfig,
    ObservabilityConfig,
    ResourceSpec,
    SandboxFleet,
    SandboxHandle,
    Task,
    TemplateSpec,
)

from .conftest import IMAGE, SESSION_KEY

ENV_VARS = ("SUBSTRATE_ENV_API", "SUBSTRATE_ENV_AUTH_TOKEN", "SUBSTRATE_ENV_TEMPLATE",
            "SUBSTRATE_ENV_ATESPACE")
PROGRAM = Path(__file__).with_name("agent_sandbox_rl_program.py")
TOKEN = "caller-token"


@pytest.fixture(autouse=True)
def _no_ambient_env(monkeypatch):
    for name in ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _mock_fleet(config: FleetConfig | None = None) -> SandboxFleet:
    return SandboxFleet(config, driver=MockBackendDriver())


# --- configuration mapping ---------------------------------------------------


def test_fleet_config_maps_onto_ate_env():
    config = FleetConfig(
        clusters=[ClusterConfig(name="c1", namespace="team-a", endpoint="api.example:7777",
                                auth_token=TOKEN)],
        max_concurrent=4, max_warmpool_size=6, window_size=3, ready_timeout=120, run_id="run-1",
        template=TemplateSpec(name="oh-agent-server"),
        observability=ObservabilityConfig(enable_metrics=False, enable_tracing=True,
                                          trace_service_name="rollouts"),
    )
    ate = config.to_ate_env()
    assert isinstance(ate, ate_env_rl.FleetConfig)
    assert (ate.backend, ate.endpoint, ate.tenancy, ate.template) == (
        "substrate", "api.example:7777", "team-a", "oh-agent-server")
    assert (ate.max_concurrent, ate.max_warmpool_replicas, ate.window_size) == (4, 6, 3)
    assert (ate.acquire_timeout_s, ate.ready_timeout_s, ate.run_id) == (120.0, 120.0, "run-1")
    assert ate.auth_token is not None and ate.auth_token.get_secret_value() == TOKEN
    obs = ate.observability
    assert (obs.enable_metrics, obs.enable_tracing, obs.trace_service_name) == (
        False, True, "rollouts")


def test_ate_env_settings_default_from_the_environment(monkeypatch):
    ate = FleetConfig().to_ate_env()
    assert (ate.endpoint, ate.tenancy, ate.template, ate.auth_token) == (
        "127.0.0.1:7777", DEFAULT_ATESPACE, None, None)
    monkeypatch.setenv("SUBSTRATE_ENV_API", "ate-env-api.ate-env:7777")
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", TOKEN)
    monkeypatch.setenv("SUBSTRATE_ENV_TEMPLATE", "oh-agent-server")
    monkeypatch.setenv("SUBSTRATE_ENV_ATESPACE", "team-b")
    ate = FleetConfig(clusters=[ClusterConfig(namespace="default")]).to_ate_env()
    assert (ate.endpoint, ate.tenancy, ate.template) == (
        "ate-env-api.ate-env:7777", "team-b", "oh-agent-server")
    assert ate.auth_token.get_secret_value() == TOKEN
    # A namespace other than the default, and the ate-env-only fields, win.
    ate = FleetConfig(
        clusters=[ClusterConfig(namespace="team-a", endpoint="other:1", auth_token="t2")],
        template=TemplateSpec(name="base"),
    ).to_ate_env()
    assert (ate.endpoint, ate.tenancy, ate.template) == ("other:1", "team-a", "base")
    assert ate.auth_token.get_secret_value() == "t2"


def test_the_token_from_the_environment_is_stripped(monkeypatch):
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", f"  {TOKEN}\n")
    assert FleetConfig().to_ate_env().auth_token.get_secret_value() == TOKEN
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", " \n")
    assert FleetConfig().to_ate_env().auth_token is None


def test_metrics_default_to_on_when_prometheus_client_is_installed():
    # agent-sandbox-rl's enable_metrics=True only turns metrics on when
    # prometheus_client is installed; ate-env's None means the same.
    assert FleetConfig().to_ate_env().observability.enable_metrics is None
    off = FleetConfig(observability=ObservabilityConfig(enable_metrics=False))
    assert off.to_ate_env().observability.enable_metrics is False


@pytest.mark.parametrize(("kwargs", "match"), [
    ({"clusters": [ClusterConfig(name="a"), ClusterConfig(name="b")]}, "one ate-env-api endpoint"),
    ({"adopt_existing": True}, "adopt_existing"),
    ({"run_isolation": "names"}, "run_isolation"),
])
def test_settings_ate_env_cannot_honour_raise(kwargs, match):
    with pytest.raises(ValueError, match=match):
        FleetConfig(**kwargs)


@pytest.mark.parametrize("build", [
    lambda: FleetConfig(max_concurrent=0),
    lambda: FleetConfig(placement="random"),
    lambda: FleetConfig(run_id="Not_A_Label"),
    lambda: TemplateSpec(image_pull_policy="Sometimes"),
    lambda: ClusterConfig(weight=0),
    lambda: FleetConfig(avg_image_gb=0),
    lambda: FleetConfig(node_ephemeral_gb=-1.0),
    lambda: FleetConfig(disk_headroom=1.0),
    lambda: FleetConfig(template_name_prefix="R2E_"),
    lambda: FleetConfig(template_name_prefix="r2e..img-"),
    lambda: FleetConfig(pool_name_format="pool-{run_id}"),
    lambda: FleetConfig(pool_name_format="pool-{template"),
    lambda: FleetConfig(pool_name_format="pool-{other}-{template}"),
    lambda: FleetConfig(pool_name_format="Pool-{template}"),
])
def test_agent_sandbox_rl_validators_still_apply(build):
    with pytest.raises(ValueError):
        build()


def test_derived_names_that_agent_sandbox_rl_accepts_pass():
    config = FleetConfig(template_name_prefix="r2e-{run_id}-",
                         pool_name_format="pool-{run_id}-{image_hash}", disk_headroom=0.0,
                         avg_image_gb=4.0, node_ephemeral_gb=100.0)
    assert config.pool_name_format == "pool-{run_id}-{image_hash}"


def test_adopting_into_a_run_namespace_gets_agent_sandbox_rl_error():
    with pytest.raises(ValueError, match="per-run namespace holds no pools"):
        FleetConfig(adopt_existing=True, run_isolation="namespace")


def test_ignored_settings_are_named_in_one_info_log(caplog):
    config = FleetConfig(
        clusters=[ClusterConfig(name="c1", context="gke_x", namespace="team-a")],
        placement="round-robin",
        template=TemplateSpec(resources=ResourceSpec(cpu="2"), extra_pod_spec={"containers": [{}]}),
    )
    assert config.ignored_fields() == [
        "placement", "template.resources", "template.extra_pod_spec", "clusters[0].context"]
    with caplog.at_level(logging.INFO, logger="ate_env_openhands.agent_sandbox_rl"):
        config.to_ate_env()
    [record] = caplog.records
    assert "template.resources" in record.getMessage()
    assert FleetConfig().ignored_fields() == []


def test_the_token_stays_out_of_repr():
    cluster = ClusterConfig(auth_token=TOKEN)
    config = FleetConfig(clusters=[cluster])
    for model in (cluster, config):
        assert TOKEN not in repr(model) and TOKEN not in str(model)
        assert TOKEN not in str(model.model_dump())
        assert TOKEN not in json.dumps(model.model_dump(mode="json"))
        assert TOKEN not in model.model_dump_json()
    assert TOKEN not in repr(config.to_ate_env())
    assert config.cluster().ate_auth_token() == TOKEN


SECRET = "s3cr3t-QZXW"


@pytest.mark.parametrize("build", [
    lambda: FleetConfig(clusters=[{"name": "a", "auth_token": SECRET},
                                  {"name": "b", "auth_token": SECRET}]),
    lambda: FleetConfig(clusters=[{"weight": 0, "auth_token": SECRET}]),
    lambda: ClusterConfig(auth_token=SECRET, weight=0),
    lambda: ClusterConfig(auth_token=SECRET, max_replicas="many"),
])
def test_the_token_stays_out_of_validation_errors(build):
    with pytest.raises(pydantic.ValidationError) as caught:
        build()
    assert "QZXW" not in str(caught.value)  # not even the tail pydantic keeps


# --- fleet -------------------------------------------------------------------


def test_run_takes_strategy_as_its_second_argument():
    fleet = _mock_fleet(FleetConfig(max_concurrent=2))
    fleet.load_tasks([Task(id="t1", image=IMAGE), Task(id="t2", image=IMAGE)])
    assert fleet.run(lambda task, handle: task.id, "none", 2) == ["t1", "t2"]
    assert fleet.report.strategy == "none"
    fleet.run(lambda task, handle: task.id, concurrency=2)
    assert fleet.report.strategy == "naive"  # agent-sandbox-rl's default


@pytest.mark.parametrize("strategy", ["", "warp", None])
def test_an_unknown_strategy_raises_as_in_agent_sandbox_rl(strategy):
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])
    with pytest.raises(ValueError, match=r"unknown strategy '.*'; choose from \["):
        fleet.run(lambda task, handle: None, strategy)


def test_epochs_give_one_list_for_each_pass():
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])
    assert fleet.run(lambda task, handle: task.id, epochs=2) == [["t1"], ["t1"]]
    with pytest.raises(ValueError, match="epochs"):
        fleet.run(lambda task, handle: None, epochs=0)


def test_epochs_keep_the_pools_and_share_one_report(monkeypatch):
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE), Task(id="t2", image=IMAGE)])
    teardowns = []
    teardown = fleet.teardown
    monkeypatch.setattr(fleet, "teardown", lambda: (teardowns.append(1), teardown()))
    fleet.run(lambda task, handle: task.id, epochs=3)
    assert teardowns == [1]  # once, after the last pass
    assert fleet.report.tasks_ok == 6


def test_a_failed_pass_before_the_last_tears_down(monkeypatch):
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])
    teardowns, setups = [], []
    teardown, setup = fleet.teardown, fleet.setup

    def failing_second_setup(tasks=None):
        setups.append(1)
        if len(setups) == 2:
            raise RuntimeError("warm failed")
        setup(tasks)

    monkeypatch.setattr(fleet, "teardown", lambda: (teardowns.append(1), teardown()))
    monkeypatch.setattr(fleet, "setup", failing_second_setup)
    with pytest.raises(RuntimeError, match="warm failed"):
        fleet.run(lambda task, handle: task.id, epochs=3)
    assert teardowns == [1]


@pytest.mark.parametrize("strategy", ["sliding", "pipelined"])
@pytest.mark.parametrize("epochs", [1, 2])
def test_a_failed_preflight_on_the_last_pass_tears_down(monkeypatch, strategy, epochs):
    # run_sliding and run_pipelined call preflight() before their try, thus
    # their own teardown does not cover it.
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])
    teardowns, preflights = [], []
    teardown, preflight = fleet.teardown, fleet.preflight

    def failing_last_preflight():
        preflights.append(1)
        if len(preflights) == epochs:
            raise RuntimeError("preflight failed")
        preflight()

    monkeypatch.setattr(fleet, "teardown", lambda: (teardowns.append(1), teardown()))
    monkeypatch.setattr(fleet, "preflight", failing_last_preflight)
    with pytest.raises(RuntimeError, match="preflight failed"):
        fleet.run(lambda task, handle: task.id, strategy=strategy, epochs=epochs)
    assert teardowns == [1]


def _failing_reap(run_id):
    raise InfrastructureError("Failed to delete 1 environments during reap: ['env-1']")


def test_a_failed_teardown_keeps_the_error_of_a_failed_preflight(monkeypatch, caplog):
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])

    def failing_preflight():
        raise PreflightError("ate-env-api is down")

    monkeypatch.setattr(fleet, "preflight", failing_preflight)
    monkeypatch.setattr(fleet.backend, "reap", _failing_reap)
    with caplog.at_level(logging.WARNING, logger="ate_env_openhands.agent_sandbox_rl"):
        with pytest.raises(PreflightError, match="ate-env-api is down"):
            fleet.run(lambda task, handle: task.id, strategy="sliding")
    # The teardown error is logged with its traceback, not raised.
    record = next(r for r in caplog.records if "teardown" in r.getMessage())
    assert record.exc_info is not None and record.exc_info[0] is InfrastructureError


@pytest.mark.parametrize("strategy", ["none", "naive", "sliding", "pipelined"])
def test_a_failed_teardown_keeps_an_interrupt_of_a_task(monkeypatch, strategy):
    # An InfrastructureError in place of the KeyboardInterrupt would turn a
    # cancelled run into a failed one.
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])

    def interrupted(task, handle):
        raise KeyboardInterrupt

    monkeypatch.setattr(fleet.backend, "reap", _failing_reap)
    with pytest.raises(KeyboardInterrupt):
        fleet.run(interrupted, strategy=strategy)


@pytest.mark.parametrize("epochs", [1, 2])
def test_a_failed_teardown_after_clean_passes_raises(monkeypatch, epochs):
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])
    monkeypatch.setattr(fleet.backend, "reap", _failing_reap)
    with pytest.raises(InfrastructureError, match="during reap"):
        fleet.run(lambda task, handle: task.id, epochs=epochs)


def _windows(fleet: SandboxFleet, monkeypatch, strategy: str) -> list[int]:
    batches: list[int] = []
    warm_images = fleet.warm_images
    monkeypatch.setattr(fleet, "warm_images", lambda images, **kw: (
        batches.append(len(images)), warm_images(images, **kw)))
    fleet.run(lambda task, handle: task.id, strategy)
    return batches


def test_the_window_comes_from_max_concurrent_as_in_agent_sandbox_rl(monkeypatch):
    tasks = [Task(id=f"t{i}", image=f"img-{i}") for i in range(6)]
    fleet = _mock_fleet(FleetConfig(max_concurrent=2))
    fleet.load_tasks(tasks)
    # One replica for each image, two in max_concurrent: two images at a time.
    assert fleet.recommended_window() == 2
    assert fleet.recommended_window(pipelined=True) == 1
    assert _windows(fleet, monkeypatch, "sliding") == [2, 2, 2]
    assert fleet.report.environment["mock"]["strategy"] == "sliding"
    assert (fleet.config.strategy, fleet.config.window_size) == ("sliding", None)
    assert _windows(fleet, monkeypatch, "pipelined") == [1] * 6
    # Larger images take more of the budget: 4 tasks of a, 2 of b, 2 of c, max 4.
    fleet = _mock_fleet(FleetConfig(max_concurrent=4))
    fleet.load_tasks([Task(id=f"a{i}", image="a") for i in range(4)]
                     + [Task(id=f"b{i}", image="b") for i in range(2)]
                     + [Task(id=f"c{i}", image="c") for i in range(2)])
    assert fleet.recommended_window() == 3
    fleet = _mock_fleet(FleetConfig(max_concurrent=2, window_size=4))
    fleet.load_tasks(tasks)
    assert fleet.recommended_window(pipelined=True) == 4
    assert _windows(fleet, monkeypatch, "sliding") == [4, 2]


@pytest.mark.parametrize("kwargs", [{"keep_warm": True}, {"recycle": True}, {"reset": "cd /"}])
def test_session_reuse_options_raise(kwargs):
    fleet = _mock_fleet()
    fleet.load_tasks([Task(id="t1", image=IMAGE)])
    with pytest.raises(NotImplementedError, match=next(iter(kwargs))):
        fleet.run(lambda task, handle: None, **kwargs)


def test_handles_carry_the_cluster_name():
    fleet = _mock_fleet(FleetConfig(clusters=[ClusterConfig(name="c1")]))
    handle = fleet.acquire(Task(id="t1", image=IMAGE))
    assert isinstance(handle, SandboxHandle) and isinstance(handle, ate_env_rl.SandboxHandle)
    assert handle.cluster_name == "c1" and handle.sandbox_id
    [a, b] = fleet.acquire([Task(id="t2", image=IMAGE), Task(id="t3", image=IMAGE)])
    assert a.cluster_name == b.cluster_name == "c1"
    fleet.release([handle, a, b])
    seen = []
    fleet.load_tasks([Task(id="t4", image=IMAGE)])
    fleet.run(lambda task, handle: seen.append(handle.cluster_name))
    assert seen == ["c1"]


def test_the_adapter_names_are_the_integration_ones():
    assert compat_openhands.make_handle_workspace is oh_fleet.make_handle_workspace
    assert compat_openhands.make_fleet_workspace is oh_fleet.make_fleet_workspace


# --- a program with only its imports changed, against the fakes ----------------


@pytest.fixture
def ate_env_from_environment(stack, monkeypatch, tmp_path):
    """The fakes with the caller token required, reached the way an
    agent-sandbox-rl program reaches ate-env: through the environment only.

    The fake splits gRPC and the port proxy over two ports, and its
    agent-server runs on this host; a real ate-env-api serves both on one
    listener, and the layer's working directory exists. Thus the workspace
    adapter gets those two values here, and nothing else: the token comes
    from the fleet.
    """
    api_url, state = stack
    state.api_token = TOKEN
    monkeypatch.setenv("SUBSTRATE_ENV_API", api_url)
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", TOKEN)
    monkeypatch.setenv("SUBSTRATE_ENV_TEMPLATE", "oh-agent-server")
    monkeypatch.setenv("AGENT_SERVER_IMAGE", IMAGE)
    monkeypatch.setenv("SANDBOX_SESSION_KEY", SESSION_KEY)
    monkeypatch.setenv("N_CONVERSATIONS", "4")
    monkeypatch.setenv("MAX_CONCURRENT", "2")
    monkeypatch.delenv("LLM_API_KEY", raising=False)

    def fake_topology(handle, **kwargs):
        assert "api_token" not in kwargs
        return oh_fleet.make_handle_workspace(
            handle, proxy_url=state.proxy_url, working_dir=str(tmp_path / handle.task.id),
            health_check_timeout=15, **kwargs)

    monkeypatch.setattr(compat_openhands, "make_handle_workspace",
                        functools.wraps(oh_fleet.make_handle_workspace)(fake_topology))
    return state


def _check_smoke_run(results, state):
    assert sorted(r["id"] for r in results) == [f"conv-{i}" for i in range(4)]
    for r in results:
        assert (r["mode"], r["cluster"], r["exit_code"]) == ("smoke", "default", 0)
        assert r["output"].startswith("ready")
    assert state.creates and {c["template"] for c in state.creates} == {"oh-agent-server"}
    assert {c["rootfs_image"] for c in state.creates} == {IMAGE}
    assert {c["atespace"] for c in state.creates} == {DEFAULT_ATESPACE}
    assert state.envs == {}  # each handle released, the warm pool torn down


def test_an_agent_sandbox_rl_program_runs_with_only_its_imports_changed(ate_env_from_environment):
    results = runpy.run_path(str(PROGRAM))["main"]()
    _check_smoke_run(results, ate_env_from_environment)


def test_the_upstream_example_runs_with_only_its_imports_changed(ate_env_from_environment,
                                                                  monkeypatch, tmp_path):
    """Runs agent-sandbox's run_openhands_fleet.py, import-swapped. Set
    AGENT_SANDBOX_RL_EXAMPLE to its path; skipped otherwise."""
    import os

    path = os.environ.get("AGENT_SANDBOX_RL_EXAMPLE")
    if not path:
        pytest.skip("set AGENT_SANDBOX_RL_EXAMPLE to agent-sandbox's run_openhands_fleet.py")
    upstream = Path(path).read_text()
    ported = upstream.replace("from agent_sandbox_rl", "from ate_env_openhands.agent_sandbox_rl")
    changed = [line for line in difflib.ndiff(upstream.splitlines(), ported.splitlines())
               if line.startswith(("- ", "+ "))]
    assert changed and all("import" in line for line in changed), changed
    script = tmp_path / "run_openhands_fleet.py"
    script.write_text(ported)
    module = runpy.run_path(str(script))
    out = io.StringIO()
    with redirect_stdout(out):
        module["main"]()
    report = json.loads(out.getvalue())
    assert report["conversations"] == 4
    _check_smoke_run(report["results"], ate_env_from_environment)

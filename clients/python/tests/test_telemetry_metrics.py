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

"""The lazy ``ate_env_client_*`` histograms in ate_env.telemetry.metrics."""

from __future__ import annotations

import logging
import subprocess
import sys
import textwrap

import pytest

from ate_env.telemetry import metrics

FAMILIES = {
    "acquire": "ate_env_client_acquire_seconds",
    "exec": "ate_env_client_exec_seconds",
    "suspend": "ate_env_client_suspend_seconds",
    "wake": "ate_env_client_wake_seconds",
    "release": "ate_env_client_release_seconds",
}
BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600)


def test_client_families_match_the_contract():
    assert {op: name for op, (name, _doc) in metrics.CLIENT_FAMILIES.items()} == FAMILIES
    assert metrics.SEC_BUCKETS == BUCKETS


def test_disabled_client_metrics_are_a_noop(monkeypatch):
    monkeypatch.setattr(metrics, "_ENABLED", False)
    monkeypatch.setattr(metrics, "_HISTOGRAMS", {})
    metrics.observe("exec", 1.0)
    with metrics.timed("acquire"):
        pass
    assert metrics._HISTOGRAMS == {}


def test_missing_extra_logs_one_warning(monkeypatch, caplog):
    monkeypatch.setattr(metrics, "_PROM", False)
    monkeypatch.setattr(metrics, "_MISSING_LOGGED", False)
    monkeypatch.setattr(metrics, "_ENABLED", False)
    caplog.set_level(logging.WARNING, logger="ate_env.telemetry.metrics")
    assert metrics.enable_client_metrics() is False
    assert metrics.enable_client_metrics() is False
    assert len(caplog.records) == 1
    assert "ate-env-client[metrics]" in caplog.records[0].getMessage()
    metrics.observe("exec", 1.0)  # still a no-op


def test_histograms_register_on_first_use_only():
    pytest.importorskip("prometheus_client")
    code = textwrap.dedent(
        """
        import prometheus_client
        from ate_env.telemetry import metrics

        def names():
            return {n for n in prometheus_client.REGISTRY._names_to_collectors
                    if n.startswith("ate_env_client_")}

        assert metrics.enable_client_metrics()
        assert names() == set(), names()
        metrics.observe("exec", 0.2)
        assert {n.removesuffix("_bucket").removesuffix("_count").removesuffix("_sum")
                .removesuffix("_created") for n in names()} == {"ate_env_client_exec_seconds"}
        with metrics.timed("acquire"):
            pass
        try:
            with metrics.timed("acquire"):
                raise ValueError("boom")
        except ValueError:
            pass
        get = prometheus_client.REGISTRY.get_sample_value
        assert get("ate_env_client_acquire_seconds_count", {"status": "ok"}) == 1
        assert get("ate_env_client_acquire_seconds_count", {"status": "error"}) == 1
        assert get("ate_env_client_exec_seconds_bucket", {"status": "ok", "le": "0.25"}) == 1
        hist = prometheus_client.REGISTRY._names_to_collectors["ate_env_client_exec_seconds"]
        assert tuple(hist._labelnames) == ("status",)
        assert tuple(hist._upper_bounds[:-1]) == metrics.SEC_BUCKETS
        print("ok")
        """
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"


def test_metric_helper_is_idempotent():
    prom = pytest.importorskip("prometheus_client")
    a = metrics._metric(prom.Histogram, "ate_env_test_idempotent_seconds", "test", ["status"])
    b = metrics._metric(prom.Histogram, "ate_env_test_idempotent_seconds", "test", ["status"])
    assert a is b
    c = metrics._metric(prom.Counter, "ate_env_test_idempotent_total", "test", ["status"])
    d = metrics._metric(prom.Counter, "ate_env_test_idempotent_total", "test", ["status"])
    assert c is d


async def test_substrate_driver_records_client_timings(fake_stack):
    prom = pytest.importorskip("prometheus_client")
    from ate_env.backend.substrate import SubstrateBackendDriver
    from ate_env.config import ObservabilityConfig
    from ate_env.observability import Observer

    client, _fakes = fake_stack
    Observer(ObservabilityConfig(enable_metrics=True))  # enables client metrics
    get = prom.REGISTRY.get_sample_value

    def count(op, status="ok"):
        return get(f"ate_env_client_{op}_seconds_count", {"status": status}) or 0.0

    before = {op: count(op) for op in FAMILIES}
    driver = SubstrateBackendDriver(client=client)
    await driver.warm_pool_async("tmpl", replicas=1)  # create + suspend
    inst = await driver.acquire_async("tmpl", run_id="r")  # claims the warm actor
    await driver.release_async(inst.instance_id)
    assert count("suspend") == before["suspend"] + 1
    assert count("acquire") == before["acquire"] + 1
    assert count("wake") == before["wake"] + 1
    assert count("release") == before["release"] + 1

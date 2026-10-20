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

"""Fleet, handle and backend emit the rl_sandbox.* event stream."""

from __future__ import annotations

import asyncio
import threading

import pytest

from ate_env import AsyncSandboxFleet, FleetConfig, SandboxFleet, Task, telemetry
from ate_env.backend.mock import MockBackendDriver
from ate_env.backend.substrate import SubstrateBackendDriver
from ate_env.errors import SandboxStartError, SandboxUnavailableError
from ate_env.loop_runner import LoopRunner
from ate_env.strategies import process_parallel
from ate_env.telemetry import MemorySink
from ate_env.telemetry.instrument import is_timeout


@pytest.fixture
def sink():
    s = MemorySink()
    telemetry.configure(s, run_id="run-x", worker_id="roll-1")
    yield s
    telemetry.shutdown()


def _names(sink):
    return [r["event"] for r in sink.records]


def test_sync_fleet_acquire_steps_release(sink):
    fleet = SandboxFleet(FleetConfig(backend="mock"))
    task = Task(id="t1", image="img-a")
    with telemetry.episode(batch_id=3, group_id="p1", trajectory_id="p1_g0"):
        h = fleet.acquire(task)
    # Steps after the episode block still carry the acquire-time context.
    h.exec("echo hi")
    h.write_file("/tmp/x", "héllo")
    h.read_file_bytes("/tmp/x")
    h.release()

    assert _names(sink) == [
        telemetry.ACQUIRE_STARTED,
        telemetry.ACQUIRE_READY,
        telemetry.STEP_COMPLETED,
        telemetry.STEP_COMPLETED,
        telemetry.STEP_COMPLETED,
        telemetry.RELEASED,
    ]
    started, ready, ex, wr, rd, rel = sink.records
    for r in sink.records:
        assert r["rl.run_id"] == "run-x"
        assert r["rl.trajectory_id"] == "p1_g0"
        assert r["rl.batch_id"] == 3
        assert r["rl_sandbox.backend"] == "mock"
        assert r["rl_sandbox.task.id"] == "t1"
        assert r["image"] == "img-a"
    assert "rl_sandbox.sandbox.id" not in started
    assert ready["rl_sandbox.sandbox.id"] == h.sandbox_id
    assert ready["acquire_path"] == "mock" and ready["wait_s"] >= 0
    assert ex["step.kind"] == "exec" and ex["step.first"] is True and ex["exit_code"] == 0
    assert wr["step.kind"] == "write_file" and wr["bytes"] == len("héllo".encode())
    assert wr["step.first"] is False
    assert rd["step.kind"] == "read_file" and rd["bytes"] == len("héllo".encode())
    assert rel["recycled"] is False and rel["steps"] == 3 and rel["hold_s"] >= 0
    # No command text anywhere.
    assert not any("echo hi" in str(v) for r in sink.records for v in r.values())


def test_step_error_is_recorded_and_reraised(sink):
    fleet = SandboxFleet(FleetConfig(backend="mock"))
    h = fleet.acquire(Task(id="t", image="i"))
    h.runtime.set_response("boom", SandboxUnavailableError("gone"))
    with pytest.raises(SandboxUnavailableError):
        h.exec("boom")
    step = sink.events(telemetry.STEP_COMPLETED)[0]
    assert step["error.type"] == "SandboxUnavailableError"
    assert "exit_code" not in step


def test_release_emits_once(sink):
    fleet = SandboxFleet(FleetConfig(backend="mock"))
    h = fleet.acquire(Task(id="t", image="i"))
    h.release()
    h.release()
    assert len(sink.events(telemetry.RELEASED)) == 1


def test_acquire_failure_is_classified(sink):
    fleet = SandboxFleet(FleetConfig(backend="mock"), driver=MockBackendDriver(fail_acquire=True))
    with pytest.raises(SandboxStartError):
        fleet.acquire(Task(id="t", image="i"))
    (failed,) = sink.events(telemetry.ACQUIRE_FAILED)
    assert failed["error.type"] == "SandboxStartError"
    assert not sink.events(telemetry.ACQUIRE_TIMEOUT)


class _TimeoutBackend(MockBackendDriver):
    def acquire(self, *a, **kw):
        try:
            raise TimeoutError()
        except TimeoutError as e:
            raise SandboxStartError("Timed out acquiring sandbox after 1s") from e


def test_acquire_timeout_is_classified(sink):
    fleet = SandboxFleet(FleetConfig(backend="mock"), driver=_TimeoutBackend())
    with pytest.raises(SandboxStartError):
        fleet.acquire(Task(id="t", image="i"))
    (to,) = sink.events(telemetry.ACQUIRE_TIMEOUT)
    assert to["error.type"] == "SandboxStartError" and to["wait_s"] >= 0


def test_is_timeout_walks_cause_chain():
    assert is_timeout(TimeoutError())
    err = SandboxStartError("x")
    err.__cause__ = TimeoutError()
    assert is_timeout(err)
    assert not is_timeout(SandboxStartError("capacity exhausted"))


def test_warm_images_emits_target_and_ready(sink):
    fleet = SandboxFleet(FleetConfig(backend="mock", max_warmpool_replicas=4))
    fleet.warm_images(["img-a"])
    target, ready = sink.events(telemetry.WARM_TARGET), sink.events(telemetry.WARM_READY)
    assert target[0]["requested"] == 4 and target[0]["image"] == "img-a"
    assert ready[0]["duration_s"] >= 0 and "error.type" not in ready[0]


def test_disabled_telemetry_leaves_handle_untouched():
    telemetry.configure(None)
    try:
        h = SandboxFleet(FleetConfig(backend="mock")).acquire(Task(id="t", image="i"))
        assert h.telemetry is None
        h.exec("echo ok")
        h.release()
    finally:
        telemetry.shutdown()


@pytest.mark.asyncio
async def test_async_fleet_events(sink):
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock"))
    with telemetry.episode(trajectory_id="a1"):
        h = await fleet.acquire(Task(id="t", image="i"))
        await h.exec_async("echo hi")
        await h.release_async()
    assert _names(sink) == [
        telemetry.ACQUIRE_STARTED,
        telemetry.ACQUIRE_READY,
        telemetry.STEP_COMPLETED,
        telemetry.RELEASED,
    ]
    assert all(r["rl.trajectory_id"] == "a1" for r in sink.records)


@pytest.mark.asyncio
async def test_async_fleet_setup_warms_with_image(sink):
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock", max_warmpool_replicas=2))
    await fleet.setup([Task(id="t", image="img-b")])
    (target,) = sink.events(telemetry.WARM_TARGET)
    assert target["image"] == "img-b" and target["requested"] == 1


def test_loop_runner_carries_episode_context():
    runner = LoopRunner()
    try:
        async def read():
            return telemetry.current_episode()

        assert runner.run(read()) is None
        with telemetry.episode(trajectory_id="via-loop"):
            got = runner.run(read())
            fut = runner.submit(read())
        assert got.trajectory_id == "via-loop"
        assert fut.result(timeout=5).trajectory_id == "via-loop"
        # The loop thread's own context is not polluted.
        assert runner.run(read()) is None
    finally:
        runner.stop()


def test_process_parallel_carries_episode_context(sink):
    fleet = SandboxFleet(FleetConfig(backend="mock"))
    tasks = [Task(id=f"t{i}", image="i") for i in range(4)]
    threads = set()

    def fn(task, handle):
        threads.add(threading.get_ident())
        return telemetry.current_episode().batch_id

    with telemetry.episode(batch_id=7):
        results = process_parallel(fleet, tasks, fn, concurrency=4)
    assert results == [7, 7, 7, 7]
    assert all(r["rl.batch_id"] == 7 for r in sink.events(telemetry.ACQUIRE_READY))


async def test_substrate_driver_sets_acquire_path_and_join_keys(fake_stack, sink):
    client, _fakes = fake_stack
    driver = SubstrateBackendDriver(client=client, atespace="ate-env", worker_family="c2")
    await driver.warm_pool_async("tmpl-a", replicas=1)
    warm = await driver.acquire_async("tmpl-a", run_id="r")
    fresh = await driver.acquire_async("tmpl-a", run_id="r")
    assert warm.metadata["acquire_path"] == "warm_pool"
    assert fresh.metadata["acquire_path"] == "create"
    assert fresh.metadata["telemetry_attrs"] == {
        "ate.atespace": "ate-env", "ate.actor.name": fresh.instance_id,
    }


async def test_substrate_fleet_ready_event_has_join_keys(fake_stack, sink):
    client, _fakes = fake_stack
    cfg = FleetConfig(backend="substrate", endpoint=client.endpoint, tenancy="ate-env",
                      template="tmpl-a")

    def _run_sync_fleet():
        # The sync fleet drives its own background loop; the episode context
        # must reach the backend's retry events and the fleet's acquire events.
        fleet = SandboxFleet(cfg)
        try:
            with telemetry.episode(trajectory_id="s1"):
                h = fleet.acquire(Task(id="t", image="img", template="tmpl-a"))
            h.release()
            return h.sandbox_id
        finally:
            fleet.close()

    sandbox_id = await asyncio.to_thread(_run_sync_fleet)
    (ready,) = sink.events(telemetry.ACQUIRE_READY)
    assert ready["rl.trajectory_id"] == "s1"
    assert ready["rl_sandbox.backend"] == "substrate"
    assert ready["ate.atespace"] == "ate-env"
    assert ready["ate.actor.name"] == sandbox_id
    assert ready["acquire_path"] == "create"
    assert sink.events(telemetry.RELEASED)[0]["ate.actor.name"] == sandbox_id

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

"""Warm-pool accounting under concurrency.

For any sequence of acquire, recycle, warm_pool and teardown operations,
interleaved at their awaits: the warm-replica gauge equals the warm pool,
teardown leaves no environment alive, and warm_pool never provisions more than
it was asked for. The fake client pauses ``suspend()`` per actor, ``create()``
per warm_pool call, an acquire's ``get()`` and ``create()`` on its own gate,
``delete()`` during a teardown and, when told to, fails ``get()``, ``create()``
or ``delete()``, so the test chooses where the coroutines interleave.
"""

from __future__ import annotations

import asyncio
import contextvars
import dataclasses
import itertools
import sys

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    initialize,
    invariant,
    multiple,
    precondition,
    rule,
)

from ate_env_rl.backend.substrate import SubstrateBackendDriver
from ate_env_rl.config import WARM_POOL_RUN_ID, ObservabilityConfig
from ate_env_rl.errors import NotFoundError, SandboxStartError
from ate_env_rl.observability import Observer
from ate_env_rl.telemetry import metrics

R = 3  # replicas asked for by every warm_pool call


@dataclasses.dataclass
class AcquireGate:
    """Pauses one acquire's ``get()`` and ``create()`` calls until ``event`` is set."""

    event: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    # The create() reaches the server before it waits: the actor is live,
    # and only the reply is pending.
    lands_first: bool = False


# Set in the task that runs an acquire; the tasks it starts inherit it.
acquire_gate: contextvars.ContextVar[AcquireGate | None] = contextvars.ContextVar(
    "acquire_gate", default=None
)


class GatedClient:
    """A Substrate client whose calls wait on, or fail by, test-held settings."""

    def __init__(self) -> None:
        self.live: set[str] = set()
        self.suspend_gates: dict[str, asyncio.Event] = {}
        self.create_gate: asyncio.Event | None = None
        self.create_errors: dict[int, Exception] = {}  # by call number, from 0
        self.creates = 0
        self.delete_gate: asyncio.Event | None = None
        self.delete_error: Exception | None = None
        self.delete_failures = 0  # fail this many delete() calls, then succeed
        self.get_gate: asyncio.Event | None = None
        self.get_error: Exception | None = None
        self.suspend_error: Exception | None = None

    async def create(self, actor_id: str, **kw: object) -> None:
        n, self.creates = self.creates, self.creates + 1
        gate = acquire_gate.get()
        if gate is not None:
            if gate.lands_first and n not in self.create_errors:
                self.live.add(actor_id)
            await gate.event.wait()
        if self.create_gate is not None:
            await self.create_gate.wait()
        if n in self.create_errors:
            raise self.create_errors[n]
        self.live.add(actor_id)

    async def suspend(self, actor_id: str, **kw: object) -> None:
        gate = self.suspend_gates.get(actor_id)
        if gate is not None:
            await gate.wait()
        if self.suspend_error is not None:
            raise self.suspend_error

    async def delete(self, actor_id: str, **kw: object) -> None:
        if self.delete_gate is not None:
            await self.delete_gate.wait()
        if self.delete_error is not None:
            raise self.delete_error
        if self.delete_failures > 0:
            self.delete_failures -= 1
            raise RuntimeError("unavailable")
        self.live.discard(actor_id)

    async def get(self, actor_id: str, **kw: object) -> None:
        gate = acquire_gate.get()
        if gate is not None:
            await gate.event.wait()
        if self.get_gate is not None:
            await self.get_gate.wait()
        if self.get_error is not None:
            raise self.get_error


def _driver(client: GatedClient) -> tuple[SubstrateBackendDriver, Observer]:
    driver = SubstrateBackendDriver(client=client)
    obs = Observer(ObservabilityConfig(enable_metrics=False))
    driver.observer = obs
    return driver, obs


def _pool(driver: SubstrateBackendDriver) -> list[str]:
    return [a for v in driver._warm_paused_pool.values() for a in v]


async def _spin() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


async def test_recycle_paused_across_teardown_deletes_the_environment() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.suspend_gates["sb-1"] = asyncio.Event()

    recycle = asyncio.create_task(driver.release_async("sb-1", recycle=True))
    await _spin()  # paused in suspend()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.suspend_gates["sb-1"].set()
    await recycle

    assert client.live == set()
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_overlapping_warm_pool_calls_provision_once() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    first = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    second = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(first, second)

    assert len(_pool(driver)) == R
    assert len(client.live) == R
    assert sum(obs._warm.values()) == R


async def test_warm_pool_paused_across_teardown_deletes_its_actors() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.create_gate.set()
    await warm

    assert client.live == set()
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_after_teardown_ignores_the_old_reservation() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    old = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    new = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-2"))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(old, new)

    assert len(_pool(driver)) == R
    assert client.live == set(_pool(driver))
    assert sum(obs._warm.values()) == R


async def test_failed_warm_claim_after_teardown_deletes_the_environment() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    # A recycled actor from another run, waiting in the pool.
    client.live.add("sb-x")
    driver._owned_actors["sb-x"] = driver._make_instance("sb-x", "tmpl", run_id="run-0")
    driver._warm_paused_pool.setdefault(("tmpl", None), []).append("sb-x")
    obs.warm_add("substrate", 1)
    client.get_gate = asyncio.Event()
    client.get_error = RuntimeError("unavailable")

    claim = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # paused in get()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.get_gate.set()
    with pytest.raises(SandboxStartError):
        await claim

    assert client.live == set()
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_finishing_during_a_reap_deletes_its_actors() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.delete_gate = asyncio.Event()

    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # paused in the reap's delete()
    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # created during the reap
    assert _pool(driver) == []
    client.delete_gate.set()
    await reap
    obs.warm_reset()
    await warm

    assert client.live == set()
    assert _pool(driver) == []
    assert driver._owned_actors == {}
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_pools_what_it_created_before_raising() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_errors[1] = NotFoundError("no such template")

    with pytest.raises(SandboxStartError):
        await driver.warm_pool_async("tmpl", R)

    assert len(_pool(driver)) == R - 1
    assert set(_pool(driver)) == client.live
    assert sum(obs._warm.values()) == R - 1


async def test_failed_delete_after_teardown_keeps_the_actors_for_the_next_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    failures: list[str] = []
    obs.release_failed = failures.append  # type: ignore[method-assign]
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_error = RuntimeError("unavailable")
    client.create_gate.set()
    await warm

    assert failures == ["substrate"] * R
    assert len(client.live) == R
    assert set(driver._owned_actors) == client.live
    assert _pool(driver) == []

    client.delete_error = None
    await driver.reap_async("run-1")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_cancelled_delete_after_teardown_keeps_the_actor_owned() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-x")
    driver._owned_actors["sb-x"] = driver._make_instance("sb-x", "tmpl", run_id="run-0")
    driver._warm_paused_pool.setdefault(("tmpl", None), []).append("sb-x")
    obs.warm_add("substrate", 1)
    client.get_gate = asyncio.Event()
    client.get_error = RuntimeError("unavailable")

    claim = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # paused in get()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_gate = asyncio.Event()
    client.get_gate.set()
    await _spin()  # paused in the delete after teardown
    claim.cancel()
    with pytest.raises(asyncio.CancelledError):
        await claim

    assert "sb-x" in driver._owned_actors
    client.delete_gate.set()
    await driver.reap_async("run-0")
    assert client.live == set()


async def test_recycle_cancelled_in_suspend_keeps_the_actor_owned() -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.suspend_gates["sb-1"] = asyncio.Event()

    recycle = asyncio.create_task(driver.release_async("sb-1", recycle=True))
    await _spin()  # paused in suspend()
    recycle.cancel()
    with pytest.raises(asyncio.CancelledError):
        await recycle

    assert "sb-1" in driver._owned_actors
    await driver.reap_async("run-1")
    assert client.live == set()


async def test_failed_delete_after_teardown_leaves_warm_actors_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_failures = R
    client.create_gate.set()
    await warm
    assert len(client.live) == R

    # The next run's reap, not run-1's, finds them.
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_failed_recycle_delete_after_teardown_leaves_the_actor_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-1")
    handle = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    driver._owned_actors["sb-1"] = handle
    client.suspend_gates["sb-1"] = asyncio.Event()

    recycle = asyncio.create_task(driver.release_async("sb-1", recycle=True))
    await _spin()  # paused in suspend()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_failures = 1
    client.suspend_gates["sb-1"].set()
    await recycle
    assert client.live == {"sb-1"}
    assert handle.run_id == "run-1"  # the caller's handle is not changed

    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


# AC-15.9: a cancelled cleanup delete, and a release whose delete fails while a
# reap runs, leave the actor where a later run's reap finds it.
async def test_cancelled_delete_after_reap_leaves_warm_actors_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.delete_gate = asyncio.Event()
    client.create_gate.set()
    await _spin()  # paused in the deletes after the teardown
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm
    assert len(client.live) == R

    client.delete_gate = None
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("cancel", [False, True])
async def test_release_delete_lost_during_reap_leaves_the_actor_for_any_later_reap(
    cancel: bool,
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-1")
    handle = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    driver._owned_actors["sb-1"] = handle
    client.delete_gate = asyncio.Event()

    release = asyncio.create_task(driver.release_async("sb-1"))
    await _spin()  # paused in delete(); the actor is not owned now
    await driver.reap_async("run-1")  # cannot see it
    if cancel:
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release
    else:
        client.delete_error = RuntimeError("unavailable")
        client.delete_gate.set()
        await release  # release does not raise on a failed delete
    assert client.live == {"sb-1"}
    assert handle.run_id == "run-1"  # the caller's handle is not changed

    client.delete_gate = None
    client.delete_error = None
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_recycle_cancelled_in_suspend_after_a_reap_leaves_the_actor_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-1")
    handle = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    driver._owned_actors["sb-1"] = handle
    client.suspend_gates["sb-1"] = asyncio.Event()

    recycle = asyncio.create_task(driver.release_async("sb-1", recycle=True))
    await _spin()  # paused in suspend(); the actor is not owned now
    await driver.reap_async("run-1")  # cannot see it
    obs.warm_reset()
    recycle.cancel()
    with pytest.raises(asyncio.CancelledError):
        await recycle
    assert driver._owned_actors["sb-1"].run_id == WARM_POOL_RUN_ID
    assert handle.run_id == "run-1"  # the caller's handle is not changed

    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_warm_pool_cancelled_before_its_deletes_start_leaves_actors_for_any_later_reap() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    async def never_runs(actor_id: str, why: str) -> None:
        # Stands for a cancel that lands before any _delete_unpooled starts.
        await asyncio.Event().wait()

    driver._delete_unpooled = never_runs  # type: ignore[method-assign]
    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # paused in create()
    await driver.reap_async("run-1")
    obs.warm_reset()
    client.create_gate.set()
    await _spin()  # owned the actors, awaiting the deletes
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm
    assert len(client.live) == R
    assert {i.run_id for i in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}

    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_release_delete_failure_without_a_reap_keeps_the_run_id() -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    client.live.add("sb-1")
    driver._owned_actors["sb-1"] = driver._make_instance("sb-1", "tmpl", run_id="run-1")
    client.delete_failures = 1

    await driver.release_async("sb-1")
    assert driver._owned_actors["sb-1"].run_id == "run-1"  # the run's own reap retries it
    await driver.reap_async("run-1")
    assert client.live == set()


# AC-15.7: a warm claim whose get() returns after an overlapping reap deleted
# the actor does not hand that actor out.
@pytest.mark.parametrize("reap_done", [True, False])
@pytest.mark.parametrize("owner", ["run-0", "run-1"])  # run-1: the reap skips it
async def test_warm_claim_overlapping_a_reap_returns_a_live_environment(
    reap_done: bool, owner: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    subphases: list[tuple[str, str, str]] = []

    def record(phase: str, seconds: float, status: str = "ok", *, source: str = "direct") -> None:
        subphases.append((phase, status, source))

    monkeypatch.setattr(metrics, "observe_subphase", record)
    client = GatedClient()
    driver, obs = _driver(client)
    client.live.add("sb-x")
    driver._owned_actors["sb-x"] = driver._make_instance("sb-x", "tmpl", run_id=owner)
    driver._warm_paused_pool.setdefault(("tmpl", None), []).append("sb-x")
    obs.warm_add("substrate", 1)
    client.get_gate = asyncio.Event()

    claim = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # popped sb-x, paused in get()
    if not reap_done:
        # run-0: pauses the reap's delete of sb-x. run-1: the reap skips sb-x
        # and deletes nothing; this pauses the claim's own delete of it.
        client.delete_gate = asyncio.Event()
    reap = asyncio.create_task(driver.reap_async("run-0"))
    await _spin()  # run-0: the reap took sb-x (still owned); run-1: it skips sb-x
    client.get_gate.set()  # get() still succeeds
    await _spin()
    if not reap_done:
        assert client.delete_gate is not None
        client.delete_gate.set()
    instance = await claim
    await reap

    assert instance.instance_id != "sb-x"
    assert instance.instance_id in client.live
    assert instance.metadata["claim_source"] == "cold_create"
    assert "sb-x" not in client.live
    assert set(driver._owned_actors) == {instance.instance_id}
    # One sample for the abandoned warm claim, then the cold create's own.
    assert subphases == [
        ("wire_claim", "error", "warm_pool"),
        ("wire_claim", "ok", "cold_create"),
    ]


class SlowCreatesClient(GatedClient):
    """The first ``fast`` create() calls return at once; the rest wait on create_gate."""

    def __init__(self, fast: int) -> None:
        super().__init__()
        self.fast = fast

    async def create(self, actor_id: str, **kw: object) -> None:
        if self.creates < self.fast:
            self.creates += 1
            self.live.add(actor_id)
            return
        await super().create(actor_id, **kw)

    async def delete(self, actor_id: str, **kw: object) -> None:
        if actor_id not in self.live:
            raise NotFoundError(actor_id)
        await super().delete(actor_id, **kw)


class SlowSuspendsClient(SlowCreatesClient):
    """Every create() lands; the first ``fast`` suspend() calls return, the rest wait."""

    def __init__(self, fast: int) -> None:
        super().__init__(fast=1_000)
        self.fast_suspends = fast
        self.suspends = 0
        self.suspend_gate = asyncio.Event()

    async def suspend(self, actor_id: str, **kw: object) -> None:
        n, self.suspends = self.suspends, self.suspends + 1
        if n >= self.fast_suspends:
            await self.suspend_gate.wait()


# AC-15.8: a warm_pool call cancelled during its create() calls leaves no live
# actor untracked; any run's reap deletes what it made.
async def test_cancelled_warm_pool_leaves_its_actors_for_any_reap() -> None:
    client = SlowCreatesClient(fast=2)
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # two actors created, the third create() paused
    assert len(client.live) == 2
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm

    assert client.live <= set(driver._owned_actors)  # nothing alive is untracked
    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0
    assert driver._warm_inflight == {}
    # The third id's create never landed: NotFound for the reap, not counted.
    assert await driver.reap_async("run-2") == 2
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_warm_pool_cancelled_in_suspend_leaves_its_actors_for_any_reap() -> None:
    client = SlowSuspendsClient(fast=2)
    driver, obs = _driver(client)

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))  # wait=True
    await _spin()  # three actors created, the third suspend() paused
    assert len(client.live) == R
    warm.cancel()
    with pytest.raises(asyncio.CancelledError):
        await warm

    assert client.live <= set(driver._owned_actors)
    assert _pool(driver) == []
    assert await driver.reap_async("run-2") == R
    assert client.live == set()


async def test_warm_pool_cancelled_just_before_the_runs_reap_deletes_its_actors() -> None:
    # The reap snapshots what it deletes before the cancel handler runs, and
    # the run is not reaped again: the handler deletes the actors itself.
    client = SlowCreatesClient(fast=2)
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R, run_id="run-1"))
    await _spin()  # two actors created, the third create() paused
    warm.cancel()
    await driver.reap_async("run-1")  # no await in between: the handler has not run
    with pytest.raises(asyncio.CancelledError):
        await warm
    await asyncio.gather(*list(driver._background_tasks))

    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("delete_fails", [False, True])
async def test_warm_actor_whose_suspend_fails_is_deleted_or_left_for_any_reap(
    delete_fails: bool,
) -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.suspend_error = RuntimeError("suspend failed")
    if delete_fails:
        client.delete_failures = R

    await driver.warm_pool_async("tmpl", R, run_id="run-1")  # wait=True

    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0
    assert client.live <= set(driver._owned_actors)  # nothing alive is untracked
    if delete_fails:
        assert len(client.live) == R
        assert {i.run_id for i in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}
        await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


# AC-15.10: a cold acquire whose create overlaps its own run's reap leaves the
# environment where a later reap finds it.
@pytest.mark.parametrize("reap_running", [False, True])
async def test_cold_acquire_overlapping_its_runs_reap_leaves_the_actor_for_any_later_reap(
    reap_running: bool,
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    if reap_running:
        # Another actor of run-1 keeps the reap in its delete() until the gate opens.
        client.live.add("sb-old")
        driver._owned_actors["sb-old"] = driver._make_instance("sb-old", "tmpl", run_id="run-1")
        client.delete_gate = asyncio.Event()
    client.create_gate = asyncio.Event()

    acquire = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()  # cold create paused
    reap = asyncio.create_task(driver.reap_async("run-1"))
    await _spin()  # the reap's snapshot does not hold the new actor
    client.create_gate.set()
    instance = await acquire
    if reap_running:
        assert client.delete_gate is not None
        client.delete_gate.set()
    await reap

    assert instance.run_id == "run-1"  # the caller's handle is not changed
    assert driver._owned_actors[instance.instance_id].run_id == WARM_POOL_RUN_ID
    assert client.live == {instance.instance_id}
    client.delete_gate = None
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


@pytest.mark.parametrize("reaped", ["another-run-during", "own-run-before"])
async def test_cold_acquire_keeps_its_run_id_without_an_overlapping_reap_of_its_run(
    reaped: str,
) -> None:
    client = GatedClient()
    driver, _ = _driver(client)
    if reaped == "own-run-before":
        await driver.reap_async("run-1")  # a fleet tears down between runs and runs again
    client.create_gate = asyncio.Event()

    acquire = asyncio.create_task(driver._acquire_internal("tmpl", "run-1"))
    await _spin()
    if reaped == "another-run-during":
        await driver.reap_async("run-0")
    client.create_gate.set()
    instance = await acquire

    assert driver._owned_actors[instance.instance_id].run_id == "run-1"
    await driver.reap_async("run-1")
    assert client.live == set()


# AC-15.2: a cold acquire cancelled while its create() is in flight, by its own
# timeout_s or by its caller, leaves the actor, if the create landed, for any
# run's reap.
@pytest.mark.parametrize("cancel", ["timeout", "caller"])
async def test_cold_acquire_cancelled_in_create_leaves_the_actor_for_any_reap(
    cancel: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    subphases: list[tuple[str, str, str]] = []

    def record(phase: str, seconds: float, status: str = "ok", *, source: str = "direct") -> None:
        subphases.append((phase, status, source))

    monkeypatch.setattr(metrics, "observe_subphase", record)
    client = GatedClient()
    driver, _ = _driver(client)
    acquire_gate.set(AcquireGate(lands_first=True))  # the reply never arrives

    if cancel == "timeout":
        with pytest.raises(SandboxStartError):
            await driver.acquire_async("tmpl", "run-1", timeout_s=0.05)
    else:
        acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
        await _spin()  # paused in create()
        acquire.cancel()
        with pytest.raises(asyncio.CancelledError):
            await acquire
    assert client.creates == 1 and len(client.live) == 1
    assert {i.run_id for i in driver._owned_actors.values()} == {WARM_POOL_RUN_ID}
    # One wire_claim sample for the abandoned create, as for a failed one.
    wire = [s for s in subphases if s[0] == "wire_claim"]
    assert wire == [("wire_claim", "error", "cold_create")]

    await driver.reap_async("run-2")  # any later reap
    assert client.live == set()
    assert driver._owned_actors == {}


async def test_cold_acquire_cancelled_in_create_after_its_runs_reap_deletes_the_actor() -> None:
    # The run's reap ran during the create and could not see the actor, and a
    # fleet reaps its run once: the cancel deletes it.
    client = GatedClient()
    driver, _ = _driver(client)
    acquire_gate.set(AcquireGate(lands_first=True))

    acquire = asyncio.create_task(driver.acquire_async("tmpl", "run-1"))
    await _spin()  # paused in create(), the actor live
    await driver.reap_async("run-1")
    assert len(client.live) == 1
    acquire.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acquire
    await asyncio.gather(*list(driver._background_tasks))

    assert client.live == set()
    assert driver._owned_actors == {}


async def test_direct_reap_moves_the_gauge_with_the_pool() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    await driver.warm_pool_async("tmpl", R)
    assert sum(obs._warm.values()) == R

    await driver.reap_async("run-1")  # no fleet, so no warm_reset()

    assert _pool(driver) == []
    assert sum(obs._warm.values()) == 0


async def test_unwarm_cancels_a_warm_pool_in_flight() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.unwarm_pool_async("tmpl")
    client.create_gate.set()
    await warm

    assert _pool(driver) == []
    assert client.live == set()
    assert driver._owned_actors == {}
    assert sum(obs._warm.values()) == 0


async def test_warm_pool_after_unwarm_ignores_the_cancelled_call() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    old = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    await driver.unwarm_pool_async("tmpl", rootfs_image=None)
    new = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(old, new)

    assert len(_pool(driver)) == R
    assert client.live == set(_pool(driver))
    assert sum(obs._warm.values()) == R


async def test_shrinking_resize_cancels_a_warm_pool_in_flight() -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    client.create_gate = asyncio.Event()

    warm = asyncio.create_task(driver.warm_pool_async("tmpl", R))
    await _spin()  # paused in create()
    resize = asyncio.create_task(driver.resize_pool_async("tmpl", 1))
    await _spin()
    client.create_gate.set()
    await asyncio.gather(warm, resize)

    assert len(_pool(driver)) == 1
    assert client.live == set(_pool(driver))
    assert set(driver._owned_actors) == client.live
    assert sum(obs._warm.values()) == 1


@pytest.mark.parametrize("shrink", ["unwarm", "resize"])
async def test_failed_delete_of_drained_actors_keeps_them_for_the_next_reap(shrink: str) -> None:
    client = GatedClient()
    driver, obs = _driver(client)
    failures: list[str] = []
    obs.release_failed = failures.append  # type: ignore[method-assign]
    await driver.warm_pool_async("tmpl", R, run_id="run-1")
    client.delete_failures = R

    if shrink == "unwarm":
        await driver.unwarm_pool_async("tmpl")
    else:
        await driver.resize_pool_async("tmpl", 0)

    assert failures == ["substrate"] * R
    assert len(client.live) == R
    assert set(driver._owned_actors) == client.live
    await driver.reap_async("run-2")
    assert client.live == set()
    assert driver._owned_actors == {}


class WarmPoolMachine(RuleBasedStateMachine):
    owned = Bundle("owned")
    recycling = Bundle("recycling")

    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.client = GatedClient()
        self.driver, self.obs = _driver(self.client)
        self.ids = itertools.count()
        self.inflight: dict[str, asyncio.Task[None]] = {}
        self.warm_tasks: list[asyncio.Task[None]] = []
        self.reap: asyncio.Task[None] | None = None
        self.claimed: set[str] = set()
        self.torn_down = False
        self.teardowns = 0  # start_teardown calls
        # Acquires in flight: task, gate, and self.teardowns when each started.
        self.acquires: dict[int, tuple[asyncio.Task[object], AcquireGate, int]] = {}
        self.acquired: list[str] = []  # returned, not yet in the owned bundle
        # Returned by an acquire that overlapped a teardown: the caller holds
        # it, and a later reap deletes it (AC-15.10), so it may outlive this one.
        self.late: set[str] = set()

    def teardown(self) -> None:
        try:
            # Not after a failed step (or Hypothesis ending the example early):
            # the step's own error must be the one reported.
            if sys.exc_info()[0] is None:
                self._end_run()
        finally:
            self._cancel_all()

    def _end_run(self) -> None:
        # The end of a run: callers give up on the acquires still in flight,
        # paused calls resume, and the fleet tears down if it has not. Then
        # nothing is left but what acquires handed out across the teardown.
        for task, _, _ in self.acquires.values():
            task.cancel()
        if self.client.create_gate is not None:
            self.client.create_gate.set()
        for gate in self.client.suspend_gates.values():
            gate.set()
        self._spin()
        if self._quiet():
            self.start_teardown()
        if self.reap is not None:
            self.finish_teardown()
        self._spin()
        assert not (self.inflight or self.warm_tasks or self.acquires), "work still paused"
        assert not self.driver._background_tasks, "deletes still running"
        self.nothing_survives_teardown()

    def _cancel_all(self) -> None:
        tasks = [*self.inflight.values(), *self.warm_tasks]
        tasks += [t for t, _, _ in self.acquires.values()]
        if self.reap is not None:
            tasks.append(self.reap)
        while tasks:
            for t in tasks:
                t.cancel()
            self.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            # Deletes a cancel started, which may start more when cancelled.
            tasks = list(self.driver._background_tasks)
        self.loop.close()

    def _spin(self) -> None:
        self.loop.run_until_complete(_spin())
        # Collect finished work; a task still waiting on a gate stays.
        for aid, task in list(self.inflight.items()):
            if task.done():
                task.result()
                del self.inflight[aid]
        for task in [t for t in self.warm_tasks if t.done()]:
            task.result()
            self.warm_tasks.remove(task)
        for n, (task, _, started_td) in list(self.acquires.items()):
            if not task.done():
                continue
            del self.acquires[n]
            if task.cancelled():
                continue
            inst = task.result()
            aid = inst.instance_id  # type: ignore[attr-defined]
            if inst.metadata["claim_source"] == "warm_pool":  # type: ignore[attr-defined]
                self.claimed.add(aid)
            if self.teardowns != started_td:
                self.late.add(aid)
            self.acquired.append(aid)

    def _quiet(self) -> bool:
        return not self.torn_down and self.reap is None

    async def _fleet_teardown(self) -> None:
        # What SandboxFleet.close and AsyncSandboxFleet.teardown do: reset the
        # gauge as soon as the reap returns, with no await in between.
        await self.driver.reap_async("run-1")
        self.obs.warm_reset()
        self.torn_down = True

    # One actor up front, so a teardown always waits on a delete and other work
    # can run inside it.
    @initialize(target=owned)
    def first_actor(self) -> str:
        return self.acquire_new()

    # Sometimes one pooled actor up front, so acquires also claim warm ones.
    @initialize(warm=st.booleans())
    def pool_first(self, warm: bool) -> None:
        if warm:
            self.loop.run_until_complete(self.driver.warm_pool_async("tmpl", 1))

    @precondition(lambda self: self._quiet())
    @rule(target=owned)
    def acquire_new(self) -> str:
        aid = f"sb-{next(self.ids)}"
        self.client.live.add(aid)
        self.driver._owned_actors[aid] = self.driver._make_instance(aid, "tmpl", run_id="run-1")
        return aid

    @precondition(lambda self: self._quiet() and _pool(self.driver))
    @rule(target=owned)
    def acquire_warm(self) -> str:
        inst = self.loop.run_until_complete(self.driver._acquire_internal("tmpl", "run-1"))
        self.claimed.add(inst.instance_id)
        return inst.instance_id

    # An acquire through acquire_async, paused at its own gate: in get() for a
    # warm claim, in create() for a cold one, whose create may already have
    # landed on the server. It may finish or be cancelled at any later step,
    # including while a teardown runs.
    @precondition(lambda self: self._quiet())
    @rule(lands_first=st.booleans())
    def start_acquire(self, lands_first: bool) -> None:
        gate = AcquireGate(lands_first=lands_first)

        async def acquire() -> object:
            acquire_gate.set(gate)
            return await self.driver.acquire_async("tmpl", "run-1")

        task = self.loop.create_task(acquire())
        self.acquires[next(self.ids)] = (task, gate, self.teardowns)
        self._spin()  # paused in get() or create()

    @precondition(lambda self: self.acquires)
    @rule(target=owned, data=st.data())
    def finish_acquire(self, data: st.DataObject) -> object:
        n = data.draw(st.sampled_from(sorted(self.acquires)))
        self.acquires[n][1].event.set()
        self._spin()
        done, self.acquired = self.acquired, []
        return multiple(*done)

    @precondition(lambda self: self.acquires)
    @rule(data=st.data())
    def cancel_acquire(self, data: st.DataObject) -> None:
        n = data.draw(st.sampled_from(sorted(self.acquires)))
        self.acquires[n][0].cancel()
        self._spin()

    # Recycles and warm_pool calls may start and finish while a teardown runs.
    @precondition(lambda self: not self.torn_down)
    @rule(target=recycling, aid=consumes(owned))
    def start_recycle(self, aid: str) -> str:
        if (
            aid not in self.driver._owned_actors
            or aid in self.inflight
            or aid in _pool(self.driver)
        ):
            return aid  # already reaped, released, recycling or back in the pool
        self.client.suspend_gates[aid] = asyncio.Event()
        self.inflight[aid] = self.loop.create_task(self.driver.release_async(aid, recycle=True))
        self._spin()  # paused in suspend()
        return aid

    @rule(target=owned, aid=consumes(recycling))
    def finish_recycle(self, aid: str) -> str:
        if aid in self.client.suspend_gates:
            self.client.suspend_gates[aid].set()
        self._spin()
        return aid

    @precondition(lambda self: not self.torn_down)
    @rule()
    def start_warm_pool(self) -> None:
        if self.client.create_gate is None or self.client.create_gate.is_set():
            self.client.create_gate = asyncio.Event()
        self.warm_tasks.append(self.loop.create_task(self.driver.warm_pool_async("tmpl", R)))
        self._spin()  # paused in create()

    @precondition(lambda self: self.warm_tasks)
    @rule()
    def finish_warm_pools(self) -> None:
        assert self.client.create_gate is not None
        self.client.create_gate.set()
        self._spin()

    # The teardown in two steps, so other work can run while the reap waits on
    # its deletes.
    @precondition(lambda self: self._quiet())
    @rule()
    def start_teardown(self) -> None:
        self.client.delete_gate = asyncio.Event()
        self.teardowns += 1
        self.reap = self.loop.create_task(self._fleet_teardown())
        self._spin()  # paused in the reap's delete() calls

    @precondition(lambda self: self.reap is not None)
    @rule()
    def finish_teardown(self) -> None:
        assert self.client.delete_gate is not None and self.reap is not None
        self.client.delete_gate.set()
        self.client.delete_gate = None
        self.loop.run_until_complete(self.reap)
        self.reap = None
        self._spin()

    # unwarm_pool while warm_pool calls may be paused in create().
    @precondition(lambda self: self._quiet())
    @rule()
    def unwarm(self) -> None:
        self.loop.run_until_complete(self.driver.unwarm_pool_async("tmpl"))
        self._spin()

    @rule()
    def idle(self) -> None:
        self._spin()

    @invariant()
    def gauge_equals_pool(self) -> None:  # P-9
        if self.reap is not None and not self.reap.done():
            return  # the fleet resets the gauge after the reap returns
        gauge = sum(self.obs._warm.values())
        assert gauge == len(_pool(self.driver)), f"gauge={gauge} pool={len(_pool(self.driver))}"

    @invariant()
    def nothing_survives_teardown(self) -> None:  # P-10
        if (
            self.torn_down
            and not self.inflight
            and not self.warm_tasks
            and not self.acquires
            and not self.driver._background_tasks
        ):
            left = self.client.live - self.late
            assert not left, f"alive after teardown: {sorted(left)}"

    @invariant()
    def warm_pool_never_over_provisions(self) -> None:  # P-11
        # Actors warm_pool created and nobody claimed yet. Recycled actors
        # (cold-created, or claimed from the pool) may take the pool above R.
        fresh = [
            a for a in _pool(self.driver) if a.startswith("warm-") and a not in self.claimed
        ]
        assert len(fresh) <= R, f"{len(fresh)} warm_pool actors pooled, asked for {R}"


WarmPoolMachine.TestCase.settings = settings(
    max_examples=500, stateful_step_count=12, deadline=None
)
test_warm_pool_accounting = WarmPoolMachine.TestCase

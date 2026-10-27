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

"""Warm-pool accounting under concurrency (AC-15) as a stateful property test.

Property P-9..P-11: for any sequence of acquire / recycle / warm_pool / teardown
operations, interleaved at their awaits, the warm-replica gauge equals the warm
pool, teardown leaves nothing alive, and warm_pool never over-provisions.

The fake client pauses `suspend()` per actor and `create()` per warm_pool call,
so Hypothesis chooses where coroutines interleave; on failure it shrinks to the
shortest interleaving that breaks a property.
"""
import asyncio
import itertools

from hypothesis import settings
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    invariant,
    precondition,
    rule,
)

from ate_env.backend.substrate import SubstrateBackendDriver
from ate_env.config import ObservabilityConfig
from ate_env.observability import Observer

R = 3  # replicas requested by every warm_pool call


class GatedClient:
    def __init__(self):
        self.live: set[str] = set()
        self.suspend_gates: dict[str, asyncio.Event] = {}
        self.create_gate: asyncio.Event | None = None

    async def create(self, actor_id, **kw):
        if self.create_gate is not None:
            await self.create_gate.wait()
        self.live.add(actor_id)

    async def suspend(self, actor_id, **kw):
        gate = self.suspend_gates.get(actor_id)
        if gate is not None:
            await gate.wait()

    async def delete(self, actor_id, **kw):
        self.live.discard(actor_id)

    async def get(self, actor_id, **kw):
        return None


class WarmPoolMachine(RuleBasedStateMachine):
    owned = Bundle("owned")
    recycling = Bundle("recycling")

    def __init__(self):
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.client = GatedClient()
        self.driver = SubstrateBackendDriver(client=self.client)
        self.obs = Observer(ObservabilityConfig(enable_metrics=False))
        self.driver.observer = self.obs
        self.ids = itertools.count()
        self.inflight: dict[str, asyncio.Task] = {}
        self.warm_tasks: list[asyncio.Task] = []
        self.torn_down = False

    def teardown(self):
        for t in [*self.inflight.values(), *self.warm_tasks]:
            t.cancel()
        self.loop.run_until_complete(asyncio.sleep(0))
        self.loop.close()

    def _spin(self):
        for _ in range(20):
            self.loop.run_until_complete(asyncio.sleep(0))

    def _pool(self) -> list[str]:
        return [a for v in self.driver._warm_paused_pool.values() for a in v]

    # ------------------------------------------------------------------ rules
    @precondition(lambda self: not self.torn_down)
    @rule(target=owned)
    def acquire_new(self):
        aid = f"sb-{next(self.ids)}"
        self.client.live.add(aid)
        self.driver._owned_actors[aid] = self.driver._make_instance(aid, "tmpl", run_id="run-1")
        return aid

    @precondition(lambda self: not self.torn_down and self._pool())
    @rule(target=owned)
    def acquire_warm(self):
        inst = self.loop.run_until_complete(self.driver._acquire_internal("tmpl", "run-1"))
        return inst.instance_id

    @precondition(lambda self: not self.torn_down)
    @rule(target=recycling, aid=consumes(owned))
    def start_recycle(self, aid):
        if aid not in self.driver._owned_actors:
            return aid  # already reaped or released
        self.client.suspend_gates[aid] = asyncio.Event()
        self.inflight[aid] = self.loop.create_task(self.driver.release_async(aid, recycle=True))
        self._spin()  # now paused inside await suspend()
        return aid

    @rule(target=owned, aid=consumes(recycling))
    def finish_recycle(self, aid):
        task = self.inflight.pop(aid, None)
        if task is not None:
            self.client.suspend_gates[aid].set()
            self.loop.run_until_complete(task)
        return aid

    @precondition(lambda self: not self.torn_down)
    @rule()
    def start_warm_pool(self):
        if self.client.create_gate is None or self.client.create_gate.is_set():
            self.client.create_gate = asyncio.Event()
        self.warm_tasks.append(self.loop.create_task(self.driver.warm_pool_async("tmpl", R)))
        self._spin()  # now paused inside await create()

    @precondition(lambda self: self.warm_tasks)
    @rule()
    def finish_warm_pools(self):
        self.client.create_gate.set()
        self.loop.run_until_complete(asyncio.gather(*self.warm_tasks))
        self.warm_tasks.clear()

    @precondition(lambda self: not self.torn_down)
    @rule()
    def fleet_teardown(self):
        self.loop.run_until_complete(self.driver.reap_async("run-1"))
        self.obs.warm_reset()
        self.torn_down = True

    @rule()
    def idle(self):
        self._spin()

    # ------------------------------------------------------------- properties
    @invariant()
    def p9_gauge_equals_pool(self):
        assert sum(self.obs._warm.values()) == len(self._pool()), (
            f"gauge={sum(self.obs._warm.values())} pool={len(self._pool())}")

    @invariant()
    def p10_nothing_survives_teardown(self):
        if self.torn_down and not self.inflight and not self.warm_tasks:
            assert not self.client.live, f"alive after teardown: {sorted(self.client.live)}"

    @invariant()
    def p11_warm_pool_never_over_provisions(self):
        warm = [a for a in self._pool() if a.startswith("warm-")]
        assert len(warm) <= R, f"{len(warm)} warm-created actors pooled, asked for {R}"


WarmPoolMachine.TestCase.settings = settings(max_examples=300, stateful_step_count=12,
                                             deadline=None)
test_warm_pool_accounting = WarmPoolMachine.TestCase

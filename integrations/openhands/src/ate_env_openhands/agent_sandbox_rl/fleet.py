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

"""agent-sandbox-rl's ``SandboxFleet`` and ``SandboxHandle`` on top of ``ate_env_rl``.

``SandboxFleet`` takes agent-sandbox-rl's ``FleetConfig`` and runs the
``ate_env_rl`` fleet with ``FleetConfig.to_ate_env()``. Its ``run()`` has
agent-sandbox-rl's signature (``strategy`` is the second positional
argument, default ``"naive"``) and behavior: an unknown strategy raises, the
sliding and pipelined window comes from ``recommended_window()``, and the
passes of ``epochs > 1`` share one RunReport and the warm pools. The
session-reuse options (``keep_warm``, ``recycle``, ``reset``) are not
supported on ate-env and raise when set.
Handles carry agent-sandbox-rl's ``cluster_name``; the Kubernetes fields
(``claim_name``, ``pod_name``, ``hostname``, ``pod_ip``) do not exist on
ate-env.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable, Sequence
from typing import Any

from ate_env_rl import SandboxFleet as AteSandboxFleet
from ate_env_rl import SandboxHandle as AteSandboxHandle
from ate_env_rl import Task
from ate_env_rl.strategies import STRATEGIES

from .config import FleetConfig

logger = logging.getLogger("ate_env_openhands.agent_sandbox_rl")


def _replicas(tasks_image: int, tasks_total: int, max_concurrent: int, max_pool: int) -> int:
    """agent-sandbox-rl's ``sizing.compute_replicas`` (no buffer, no
    ``per_task``): the image's share of ``max_concurrent``, at least one,
    capped by its task count and ``max_pool``."""
    if tasks_image <= 0:
        return 0
    share = max_concurrent * tasks_image / (tasks_total or tasks_image)
    return int(min(max(1, round(share)), tasks_image, max_pool))


def _window(counts: dict[str, int], max_concurrent: int, max_pool: int) -> int:
    """agent-sandbox-rl's ``sizing.recommend_window``: the leading images whose
    warm replicas fit in ``max_concurrent``, at least one."""
    total = sum(counts.values()) or 1
    budget = max(1, max_concurrent)
    used = window = 0
    for count in counts.values():
        replicas = _replicas(count, total, max_concurrent, max_pool)
        if window >= 1 and used + replicas > budget:
            break
        used += replicas
        window += 1
    return max(1, window)


@dataclasses.dataclass
class SandboxHandle(AteSandboxHandle):
    """An ``ate_env_rl.SandboxHandle`` with agent-sandbox-rl's ``cluster_name``:
    the name of the ``ClusterConfig`` the fleet runs on."""

    cluster_name: str = "default"

    @classmethod
    def wrap(cls, handle: AteSandboxHandle, cluster_name: str) -> SandboxHandle:
        """Copy an ``ate_env_rl`` handle into one that carries ``cluster_name``."""
        if isinstance(handle, cls):
            return handle
        fields = dataclasses.fields(handle)
        wrapped = cls(**{f.name: getattr(handle, f.name) for f in fields if f.init},
                      cluster_name=cluster_name)
        for f in fields:
            if not f.init:
                setattr(wrapped, f.name, getattr(handle, f.name))
        return wrapped


class SandboxFleet(AteSandboxFleet):
    """agent-sandbox-rl's fleet API, run by the ``ate_env_rl`` fleet."""

    def __init__(self, config: FleetConfig | None = None, driver: Any = None):
        self.agent_sandbox_config = config or FleetConfig()
        self.cluster_name = self.agent_sandbox_config.cluster().name
        super().__init__(self.agent_sandbox_config.to_ate_env(), driver=driver)

    def acquire(self, task: Task | str | Sequence[Task | str],  # type: ignore[override]
                timeout_s: float | None = None) -> SandboxHandle | list[SandboxHandle]:
        acquired = super().acquire(task, timeout_s=timeout_s)
        if isinstance(acquired, list):
            return [SandboxHandle.wrap(h, self.cluster_name) for h in acquired]
        return SandboxHandle.wrap(acquired, self.cluster_name)

    def recommended_window(self, *, pipelined: bool = False) -> int:
        """Images to keep warm at a time for sliding and pipelined, as
        agent-sandbox-rl computes it: an explicit ``window_size`` wins; else the
        leading images whose warm replicas fit in ``max_concurrent``, halved for
        pipelined (which keeps two windows warm). ``warm_per_task`` and the
        disk caps do not apply, because ate-env ignores those settings."""
        cfg = self.agent_sandbox_config
        if cfg.window_size is not None:
            return max(1, cfg.window_size)
        window = _window(self.image_counts(), cfg.max_concurrent, cfg.max_warmpool_size)
        return max(1, window // 2) if pipelined else window

    def run(self, process_fn: Callable[[Task, SandboxHandle], Any],  # type: ignore[override]
            strategy: str = "naive", concurrency: int | None = None,
            *, epochs: int = 1, keep_warm: bool = False, recycle: bool = False,
            reset: Any = None, max_reuses: int = 32, reset_timeout: float = 5.0,
            use_session: bool = True, scale_on_hold: bool = True) -> list:
        """Run all loaded tasks under ``strategy`` (none|naive|sliding|pipelined)
        with up to ``concurrency`` parallel acquire and process.

        ``epochs > 1`` runs that many passes and returns one task-ordered list
        for each pass, as agent-sandbox-rl does: the warm pools stay between
        passes, the fleet tears down once after the last pass, and
        ``fleet.report`` covers all passes. ``max_reuses``, ``reset_timeout``,
        ``use_session`` and ``scale_on_hold`` only tune session reuse, thus they
        have no effect here.
        """
        unsupported = [name for name, value, default in (
            ("keep_warm", keep_warm, False), ("recycle", recycle, False), ("reset", reset, None),
        ) if value != default]
        if unsupported:
            raise NotImplementedError(
                f"ate-env: run({', '.join(f'{n}=' for n in unsupported)}...) is not "
                "supported; each task gets a fresh environment and the fleet deletes it "
                "on release")
        if strategy not in STRATEGIES:
            raise ValueError(f"unknown strategy '{strategy}'; choose from {sorted(STRATEGIES)}")
        if epochs < 1:
            raise ValueError("epochs must be >= 1")
        conc = max(1, concurrency or min(self.config.max_concurrent, len(self.tasks)))
        window = self.config.window_size
        if strategy in ("sliding", "pipelined"):
            window = self.recommended_window(pipelined=strategy == "pipelined")
        # The ate_env_rl strategies and the RunReport read the strategy and the
        # window from the fleet's configuration; set both for this run only.
        configured = self.config
        self.config = dataclasses.replace(configured, strategy=strategy, window_size=window)
        passes: list[Any] = []
        try:
            with self.observer.run(strategy, backend=self.backend_label) as report:
                self.report = report
                report.environment = self.describe_environment(concurrency=conc)
                # The fleet tears down here, once, and not in a strategy:
                # run_sliding and run_pipelined call preflight() and plan()
                # before their own try, thus a failure there skips the
                # strategy's teardown, also on the last pass.
                try:
                    for e in range(epochs):
                        if epochs > 1:
                            logger.info("epoch %d/%d", e + 1, epochs)
                        passes.append(STRATEGIES[strategy](self, process_fn, conc, teardown=False))
                except BaseException:
                    # The error of the pass is the one to report: a teardown
                    # error in its place would also turn an interrupt (a
                    # BaseException) into a failed run.
                    try:
                        self.teardown()
                    except Exception:
                        logger.warning("teardown after a failed pass", exc_info=True)
                    raise
                self.teardown()
        finally:
            self.config = configured
        logger.debug("\n%s", report.summary())
        return passes[0] if epochs == 1 else passes

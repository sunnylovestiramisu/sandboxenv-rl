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

from __future__ import annotations

import collections
import contextvars
import dataclasses
import logging
import threading
import uuid
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .backend.base import BackendDriver
from .backend.mock import MockBackendDriver
from .backend.substrate import SubstrateBackendDriver
from .config import ALLOWED_BACKENDS, FleetConfig, TracerConfig
from .handle import SandboxHandle
from .observability import Observer, RunReport, repo_family
from .runtime.base import RuntimeGuestHook
from .sizing import plan_replicas
from .sources import FieldSpec, TaskSource, to_tasks
from .strategies import STRATEGIES
from .telemetry.trace_recorder import (
    is_trace_mode,
    record_trace_event,
    register_env_meta,
    should_auto_enable_tracing,
    task_context,
)
from .types import (
    DataPlaneEndpoint,
    FleetPlan,
    PlanEntry,
    RawSandboxInstance,
    Task,
)

logger = logging.getLogger("ate_env_rl.fleet")


class SandboxFleet:
    """Unified Sandbox Fleet Orchestrator.

    Coordinates preflight, planning, warm-pool sizing, lifecycle management,
    task acquisition, and execution strategies across any backend driver.
    """

    def __init__(self, config: FleetConfig | None = None, driver: BackendDriver | None = None):
        self.config = config or FleetConfig()
        self.run_id = self.config.run_id or uuid.uuid4().hex[:12]
        self._tasks: list[Task] = []
        self._task_ids: set[str] = set()
        self._image_to_template: dict[str, tuple[str, str | None]] = {}
        self._plan: FleetPlan | None = None
        self._lock = threading.Lock()
        # Sandboxes acquired per task image, warm or cold (acquired_count).
        self._acquired: collections.Counter[str] = collections.Counter()
        # Serializes plan recomputation, so concurrent add_tasks calls leave
        # the plan of the last call, which saw every task added before it.
        self._plan_lock = threading.Lock()

        # Telemetry. Either FleetConfig.tracing or observability.enable_tracing
        # turns on tracing for the whole fleet under one service name, so
        # ate_env_fleet.* spans parent ate_env_client.* spans in one trace.
        obs_cfg = self.config.observability
        tracing_on = (
            self.config.tracing.enable_tracing
            or obs_cfg.enable_tracing
            or should_auto_enable_tracing(self.config.tracing)
        )
        service = (obs_cfg.trace_service_name if obs_cfg.enable_tracing
                   else self.config.tracing.trace_service_name)
        if (obs_cfg.enable_tracing and self.config.tracing.enable_tracing
                and self.config.tracing.trace_service_name != service):
            logger.debug(
                "Both tracing switches are on: service name %r from "
                "ObservabilityConfig.trace_service_name replaces %r from "
                "TracerConfig.trace_service_name", service,
                self.config.tracing.trace_service_name)
        self._tracer_config = TracerConfig(enable_tracing=tracing_on, trace_service_name=service)
        self._obs = Observer(dataclasses.replace(
            obs_cfg, enable_tracing=tracing_on, trace_service_name=service
        ))
        # The Observer was built from the same switch and service name, so its
        # tracer (None when tracing is off) is the fleet's; no second tracer.
        self._tracer = self._obs.tracer
        self.report: RunReport | None = None  # set by run()

        # Initialize Backend Driver
        if driver is not None:
            self.backend = driver
        elif self.config.backend == "mock":
            self.backend = MockBackendDriver()
        elif self.config.backend == "substrate":
            token = self.config.auth_token
            self.backend = SubstrateBackendDriver(
                api_endpoint=self.config.endpoint,
                atespace=self.config.tenancy,
                worker_family=self.config.worker_family or "c2",
                auth_token=token.get_secret_value() if token else None,
                image_templates=self.config.image_templates,
                default_template=self.config.template if self.config.template is not None else "default-template",
                labels=self.config.labels,
                acquire_timeout_s=self.config.acquire_timeout_s,
                ready_timeout_s=self.config.ready_timeout_s,
                tracer_config=self._tracer_config,
                connections=self.config.connections or -(-self.config.max_concurrent // 200),
                worker_selector=self.config.node_selector,
                placement=self.config.placement,
            )
        else:
            raise NotImplementedError(f"Backend '{self.config.backend}' is not yet supported or requires extras")

        name = getattr(self.backend, "backend_name", None)
        self.backend_label = name if name in ALLOWED_BACKENDS else self.config.backend
        # A driver belongs to one fleet: its claim and warm events go to the
        # observer of the fleet that set it last.
        previous = getattr(self.backend, "observer", None)
        if previous is not None and previous is not self._obs:
            logger.warning(
                "Backend driver %r already belongs to another fleet; its claim and "
                "warm-replica events now go to this fleet's report and series. "
                "Use one driver per fleet.", type(self.backend).__name__)
        try:
            self.backend.observer = self._obs
        except AttributeError:  # a driver that refuses new attributes
            pass

    @property
    def observer(self) -> Observer:
        """The fleet's Observer (phases, tasks, claims, warm replicas)."""
        return self._obs

    def __enter__(self) -> SandboxFleet:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            self.close()
        except Exception:
            if exc_type is None:
                raise

    def close(self) -> None:
        teardown_err: Exception | None = None
        try:
            self.teardown()
        except Exception as e:
            teardown_err = e
        finally:
            if hasattr(self.backend, "close"):
                try:
                    self.backend.close()
                except Exception as be:
                    if teardown_err is None:
                        teardown_err = be
        if teardown_err is not None:
            raise teardown_err

    @property
    def tasks(self) -> list[Task]:
        """A snapshot of the fleet's tasks.

        A copy, so a ``run()`` (or any caller) that iterates it is not
        affected by ``add_tasks`` calls made meanwhile: tasks added during a
        ``run()`` are not run by that run.
        """
        with self._lock:
            return list(self._tasks)

    def load_tasks(self, tasks: Iterable[Task | dict] | TaskSource, *, image_field: FieldSpec = None,
                   id_field: FieldSpec = None) -> None:
        """Load tasks into the fleet: ``Task`` objects, dataset rows, or a
        ``TaskSource`` such as ``JsonlSource``.

        Replaces the tasks loaded before; ``add_tasks`` appends instead. If
        ``plan()`` has run, the plan is recomputed for the new tasks.

        A row (a dict) gives its image and id from ``image_field`` and
        ``id_field``: one dotted path (``"metadata.docker_image"``), several
        tried in order, or a callable ``row -> value``. By default the image
        is read from ``image``, then the R2E-Gym and SWE-bench keys
        (``sources.DEFAULT_IMAGE_FIELDS``), and the id from ``task_id``,
        ``id`` or ``instance_id``. The whole row becomes ``Task.metadata``.

        Raises ValueError, and keeps the tasks loaded before, if a task id
        repeats within ``tasks``, a row has no image or no id, or a callable
        field raises (the error names the row): like
        ``add_tasks``, ``acquire`` by id must resolve to one task, and an id
        is never made up from a row's position.
        """
        normalized = to_tasks(tasks, image_field=image_field, id_field=id_field)
        repeated = self._repeated_ids(normalized)
        if repeated:
            raise ValueError(f"Task ids repeated: {repeated}")
        with self._lock:
            self._tasks = normalized
            self._task_ids = {t.id for t in normalized}
        self._replan_if_planned()
        logger.info("Fleet loaded %d tasks (%d unique images)",
                    len(normalized), len(self.image_counts()))

    def add_tasks(self, tasks: Iterable[Task | dict] | TaskSource, *, image_field: FieldSpec = None,
                  id_field: FieldSpec = None) -> list[Task]:
        """Append tasks to the fleet and extend the plan with their images.

        For a job whose task list grows while it runs: a worker that is handed
        an episode the orchestrator did not plan, or a trainer that streams
        batches from a prompt source. Takes the same items, sources and field
        options as ``load_tasks`` and returns them as ``Task`` objects. If ``plan()`` has run, the plan
        is recomputed so ``plan.for_image()`` covers the new images; nothing
        is warmed, call ``warm_images`` for that. A ``run()`` in progress
        does not run the added tasks. Safe to call from several threads.

        Raises ValueError, and adds nothing, if a task id is already in the
        fleet or repeats within ``tasks``: ``acquire`` by id must resolve to
        one task.
        """
        new = to_tasks(tasks, image_field=image_field, id_field=id_field)
        if not new:
            return new
        ids = [t.id for t in new]
        with self._lock:
            dupes = sorted(set(self._repeated_ids(new)) | (set(ids) & self._task_ids))
            if dupes:
                raise ValueError(f"Task ids already in the fleet or repeated: {dupes}")
            self._tasks.extend(new)
            self._task_ids.update(ids)
            total = len(self._tasks)
        self._replan_if_planned()
        logger.debug("Fleet added %d tasks (%d total)", len(new), total)
        return new

    @staticmethod
    def _repeated_ids(tasks: list[Task]) -> list[str]:
        counts = collections.Counter(t.id for t in tasks)
        return sorted(i for i, n in counts.items() if n > 1)

    def _replan_if_planned(self) -> None:
        """Recompute the plan if ``plan()`` has run, after a task change.

        The decision reads ``_plan`` under ``_plan_lock``, which is the lock
        that writes it: a ``plan()`` that took its snapshot of the tasks
        before the change finishes first, and then this recomputes it with
        the change. ``_lock`` is not held here, so the lock order is the one
        of ``plan()`` (``_plan_lock``, then ``_lock`` inside
        ``image_counts``).
        """
        with self._plan_lock:
            if self._plan is not None:
                self._plan_entries_locked()

    def image_counts(self) -> dict[str, int]:
        """Return task count per unique image."""
        counts: dict[str, int] = collections.defaultdict(int)
        with self._lock:
            tasks = list(self._tasks)
        for t in tasks:
            counts[t.image] += 1
        return dict(counts)

    def preflight(self) -> None:
        """Run backend connectivity, permissions, and capacity checks."""
        with self._obs.phase("preflight", backend=self.backend_label):
            self.backend.preflight()

    def plan(self) -> FleetPlan:
        """Compute provisioning sizing for all tasks."""
        with self._obs.phase("plan", backend=self.backend_label):
            return self._plan_entries()

    def _plan_entries(self) -> FleetPlan:
        with self._plan_lock:
            return self._plan_entries_locked()

    def _plan_entries_locked(self) -> FleetPlan:
        entries: list[PlanEntry] = []
        counts = self.image_counts()
        sized = self._size_pools(counts)
        for img, task_count in counts.items():
            template_id, rootfs_image = self._ensure_template_for_image(img)
            entries.append(PlanEntry(
                image=img,
                template_id=template_id,
                replicas=sized[img],
                tasks=task_count,
                rootfs_image=rootfs_image,
            ))
        self._plan = FleetPlan(entries)
        return self._plan

    def _size_pools(
        self, counts: dict[str, int], images: Sequence[str] | None = None
    ) -> dict[str, int]:
        """Warm replicas per image: its share of max_concurrent (see ate_env_rl.sizing).

        ``images`` limits the share to those images (a strategy's window). An
        image with no loaded task has nothing to size by and gets
        ``max_warmpool_replicas``.
        """
        cfg = self.config
        selected = list(counts) if images is None else list(dict.fromkeys(images))
        sized = plan_replicas(
            counts, cfg.max_concurrent, cfg.max_warmpool_replicas,
            per_task=cfg.warm_per_task, images=[i for i in selected if i in counts],
        )
        for img in selected:
            sized.setdefault(img, cfg.max_warmpool_replicas)
        return sized

    def setup(self, tasks: list[Any] | None = None) -> None:
        """Run preflight, plan, and pre-warm all planned images."""
        if tasks is not None:
            self.load_tasks(tasks)
        self.preflight()
        plan = self.plan()
        self._warm_many(
            [(e.image, e.template_id, e.rootfs_image, e.replicas) for e in plan.entries],
            wait=True,
        )

    def warm_images(self, images: list[str], replicas: int | None = None, wait: bool = True) -> None:
        """Warm up pools for specific images (used by windowed strategies).

        Without ``replicas``, each image gets its share of ``max_concurrent``
        among ``images`` (``warm_per_task``: one replica per task), capped by
        ``max_warmpool_replicas``. Up to ``warm_parallelism`` images warm at
        once; the first error is raised after every image was tried.
        """
        if replicas is not None:
            sized = {img: replicas for img in images}
        else:
            sized = self._size_pools(self.image_counts(), images)
        work = []
        for img in dict.fromkeys(images):
            template_id, rootfs_image = self._ensure_template_for_image(img)
            work.append((img, template_id, rootfs_image, sized[img]))
        self._warm_many(work, wait=wait)

    def _warm_many(
        self, work: list[tuple[str, str, str | None, int]], wait: bool
    ) -> None:
        """Warm (image, template, rootfs_image, replicas) items, warm_parallelism at a time."""
        work = [w for w in work if w[3] > 0]
        if not work:
            return

        def _warm_one(item: tuple[str, str, str | None, int]) -> None:
            img, template_id, rootfs_image, rep = item
            with self._obs.phase("warm_pool", backend=self.backend_label, family=repo_family(img)):
                self.backend.warm_pool(template_id, rep, wait=wait, run_id=self.run_id,
                                       rootfs_image=rootfs_image)

        if len(work) == 1 or self.config.warm_parallelism == 1:
            errors: list[BaseException] = []
            for item in work:
                try:
                    _warm_one(item)
                except Exception as e:
                    errors.append(e)
        else:
            workers = min(self.config.warm_parallelism, len(work))
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ate-env-warm") as ex:
                # Each warm runs in a copy of the caller's context, so its
                # span nests under the caller's current span.
                futures = [ex.submit(contextvars.copy_context().run, _warm_one, item)
                           for item in work]
                errors = [e for e in (f.exception() for f in futures) if e is not None]
        if errors:
            if len(errors) > 1:
                logger.warning("%d of %d images failed to warm; raising the first",
                               len(errors), len(work))
            raise errors[0]

    def warm_image(self, image: str, replicas: int | None = None, wait: bool = True) -> None:
        """Warm one image's pool (agent-sandbox-rl's ``warm_image``)."""
        self.warm_images([image], replicas=replicas, wait=wait)

    def set_pool_replicas(self, image: str, replicas: int, wait: bool = True) -> None:
        """Size one image's warm pool to exactly ``replicas`` idle sandboxes,
        up or down, without dropping it (agent-sandbox-rl's
        ``set_pool_replicas``). Claimed sandboxes are not counted and never
        touched, so a caller can keep ``held + warm == K`` per image as it
        acquires. A backend without ``resize_pool`` grows through
        ``warm_pool`` and drains through ``unwarm_pool`` when ``replicas`` is 0.
        """
        replicas = max(0, int(replicas))
        template_id, rootfs_image = self._ensure_template_for_image(image)
        with self._obs.phase("warm_pool", backend=self.backend_label, family=repo_family(image)):
            try:
                self.backend.resize_pool(template_id, replicas, wait=wait, rootfs_image=rootfs_image)
            except NotImplementedError:
                if replicas == 0:
                    self.backend.unwarm_pool(template_id, rootfs_image=rootfs_image)
                else:
                    logger.warning(
                        "backend %s has no resize_pool: set_pool_replicas(%s, %d) can grow the "
                        "pool but cannot shrink it below its current size",
                        self.backend_label, image, replicas,
                    )
                    self.backend.warm_pool(template_id, replicas, wait=wait, rootfs_image=rootfs_image)

    def unwarm_image(self, image: str) -> None:
        """Drain warm pool for a specific image."""
        entry = self._image_to_template.get(image)
        if entry:
            template_id, rootfs_image = entry
            self.backend.unwarm_pool(template_id, rootfs_image=rootfs_image)

    def acquired_count(self, image: str) -> int:
        """Sandboxes this fleet has acquired for ``image`` so far, warm or cold.

        A monotonic counter: callers take differences between two reads.
        """
        with self._lock:
            return self._acquired[image]

    def _count_acquire(self, image: str) -> None:
        with self._lock:
            self._acquired[image] += 1

    def pool_size(self, image: str) -> int | None:
        """Idle warm replicas for ``image``; None when the driver cannot tell."""
        entry = self._image_to_template.get(image)
        if entry is None:
            return 0
        template_id, rootfs_image = entry
        return self.backend.pool_size(template_id, rootfs_image=rootfs_image)

    def acquire(
        self,
        task: Task | str | Sequence[Task | str],
        timeout_s: float | None = None,
    ) -> SandboxHandle | list[SandboxHandle]:
        """Acquire one or more live sandboxes bound to tasks.

        If a list or tuple of tasks is given, returns a list of SandboxHandle.
        Otherwise returns a single SandboxHandle.
        """
        if isinstance(task, (list, tuple)):
            return [self._acquire_one(t, timeout_s=timeout_s) for t in task]
        return self._acquire_one(task, timeout_s=timeout_s)

    def _acquire_one(
        self,
        task: Task | str,
        timeout_s: float | None = None,
    ) -> SandboxHandle:
        task_obj: Task
        if isinstance(task, str):
            found = next((t for t in self.tasks if t.id == task), None)
            if found:
                task_obj = found
            else:
                task_obj = Task(id=task, image="default")
        else:
            task_obj = task

        with self._obs.phase("acquire", backend=self.backend_label, family=repo_family(task_obj)):
            return self._acquire_task(task_obj, timeout_s)

    def _acquire_task(self, task_obj: Task, timeout_s: float | None) -> SandboxHandle:
        if task_obj.template:
            template_id = task_obj.template
            rootfs_image = None
        else:
            template_id, rootfs_image = self._ensure_template_for_image(task_obj.image)

        to_s = timeout_s or self.config.acquire_timeout_s
        with task_context(
            run_id=self.run_id, instance=task_obj.id, image=task_obj.image
        ):
            raw_inst = self.backend.acquire(
                template_id, self.run_id, timeout_s=to_s, rootfs_image=rootfs_image
            )
        register_env_meta(
            raw_inst.instance_id,
            instance=task_obj.id,
            image=task_obj.image,
            run_id=self.run_id,
        )
        if self.backend_label == "mock" and is_trace_mode():
            record_trace_event(
                "create",
                env=raw_inst.instance_id,
                dur_ms=0.0,
                ok=True,
                run=self.run_id,
                instance=task_obj.id,
                image=task_obj.image,
            )
        self._count_acquire(task_obj.image)

        try:
            runtime, data_plane = self._build_runtime(raw_inst)
        except Exception:
            try:
                self.backend.release(raw_inst.instance_id, recycle=False)
            except Exception:
                logger.warning(
                    "Failed to release sandbox %s after runtime setup error",
                    raw_inst.instance_id,
                    exc_info=True,
                )
            raise

        claim_source = str(raw_inst.metadata.get("claim_source", "direct"))
        return SandboxHandle(
            sandbox_id=raw_inst.instance_id,
            endpoint=data_plane.address if data_plane else raw_inst.endpoint,
            task=task_obj,
            run_id=self.run_id,
            backend=self.backend,
            runtime=runtime,
            data_plane=data_plane,
            default_timeout_s=self.config.step_timeout_s,
            tracer=self._tracer,
            claim_source=claim_source,
        )

    def _build_runtime(
        self, raw_inst: RawSandboxInstance
    ) -> tuple[RuntimeGuestHook, DataPlaneEndpoint | None]:
        """Build the RuntimeGuestHook from this sandbox's own data-plane coordinates."""
        if hasattr(self.backend, "build_runtime"):
            return self.backend.build_runtime(raw_inst, data_plane=self.config.data_plane)

        from .runtime.substrate_env_client import SubstrateEnvClientRuntime

        name = self.config.data_plane
        ep = raw_inst.data_planes.get(name) or raw_inst.data_planes.get("ate_env")
        client = getattr(self.backend, "client", None)
        loop_runner = getattr(self.backend, "_loop_runner", None)
        return SubstrateEnvClientRuntime(
            env_id=raw_inst.instance_id,
            endpoint=ep.address if ep else raw_inst.endpoint,
            atespace=self.config.tenancy or "ate-env",
            client=client,
            loop_runner=loop_runner,
            tracer_config=self._tracer_config,
        ), ep

    def release(self, handle: SandboxHandle | Sequence[SandboxHandle]) -> None:
        """Release sandbox instance(s) back to backend and close runtime(s)."""
        for h in handle if isinstance(handle, (list, tuple)) else (handle,):
            with self._obs.phase("release", backend=self.backend_label, family=repo_family(h.task)):
                h.release()
                if self.backend_label == "mock" and is_trace_mode():
                    record_trace_event(
                        "delete",
                        env=h.sandbox_id,
                        dur_ms=0.0,
                        ok=True,
                        run=self.run_id,
                        instance=h.task.id if h.task else None,
                        image=h.task.image if h.task else None,
                    )

    def recycle(self, handle: SandboxHandle | Sequence[SandboxHandle]) -> None:
        """Return sandbox instance(s) to the warm pool (see SandboxHandle.recycle)."""
        for h in handle if isinstance(handle, (list, tuple)) else (handle,):
            with self._obs.phase("recycle", backend=self.backend_label, family=repo_family(h.task)):
                h.recycle()
                if self.backend_label == "mock" and is_trace_mode():
                    record_trace_event(
                        "suspend",
                        env=h.sandbox_id,
                        dur_ms=0.0,
                        ok=True,
                        run=self.run_id,
                        instance=h.task.id if h.task else None,
                        image=h.task.image if h.task else None,
                    )

    def teardown(self) -> None:
        """Teardown all resources provisioned by this fleet's run_id."""
        try:
            with self._obs.phase("teardown", backend=self.backend_label):
                self.backend.reap(self.run_id)
        finally:
            self._obs.warm_reset()

    def run(self, process_fn: Callable[[Task, SandboxHandle], Any],
            concurrency: int | None = None) -> list[Any]:
        """Execute all tasks using the configured strategy.

        The run's RunReport is available as ``fleet.report`` afterwards;
        ``fleet.report.summary()`` gives the table (logged only at DEBUG).
        """
        strat_fn = STRATEGIES.get(self.config.strategy)
        if not strat_fn:
            raise ValueError(f"Unknown strategy: {self.config.strategy}")
        c = concurrency or min(self.config.max_concurrent, len(self._tasks))
        with self._obs.run(self.config.strategy, backend=self.backend_label) as report:
            self.report = report
            report.environment = self.describe_environment(concurrency=max(1, c))
            results = strat_fn(self, process_fn, max(1, c))
        logger.debug("\n%s", report.summary())
        return results

    def describe_environment(self, concurrency: int | None = None) -> dict[str, dict[str, Any]]:
        """The run's environment block for the RunReport, keyed by backend."""
        info: dict[str, Any] = {"atespace": self.config.tenancy, "run_id": self.run_id}
        if self.backend_label == "substrate":
            info["endpoint"] = getattr(self.backend, "api_endpoint", self.config.endpoint)
        info.update({
            "strategy": self.config.strategy,
            "concurrency": concurrency,
            "tasks": len(self._tasks),
            "images": len(self.image_counts()),
        })
        return {self.backend_label: info}

    def _ensure_template_for_image(self, image: str) -> tuple[str, str | None]:
        with self._lock:
            if image in self._image_to_template:
                return self._image_to_template[image]
            with self._obs.phase("ensure_template", backend=self.backend_label,
                                 family=repo_family(image)):
                res: tuple[str, str | None]
                if image in self.config.image_templates:
                    res = (self.config.image_templates[image], None)
                else:
                    base = self.config.template or "default-template"
                    res = (base, image)
                self._image_to_template[image] = res
            return res

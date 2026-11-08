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

import asyncio
import dataclasses
import logging
import os
import threading
import time
import uuid
import zlib
from collections.abc import Mapping
from typing import Any

from ..client import DEFAULT_ATESPACE, Client
from ..config import WARM_POOL_RUN_ID, TracerConfig
from ..errors import (
    FailedPreconditionError,
    InfrastructureError,
    NotFoundError,
    PreflightError,
    SandboxStartError,
)
from ..loop_runner import LoopRunner
from ..telemetry import metrics
from ..telemetry.trace_recorder import (
    current_slot_wait_s,
    current_warm_claim,
    in_driver_span,
)
from ..telemetry.tracing import (
    CLIENT_SPAN_PREFIX,
    mark_error,
    set_attributes,
    start_span,
    tracer_from_config,
)
from ..types import DataPlaneEndpoint, EnvironmentSpec, RawSandboxInstance
from .base import BackendDriver

logger = logging.getLogger("ate_env_rl.backend.substrate")


def _loop_running() -> bool:
    """Whether an event loop runs in this thread. A GeneratorExit thrown into
    a task by a closing loop finds none: there the delete is left to a later
    reap instead of failing in asyncio.create_task."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


class _WarmPoolDict(dict):
    """Warm paused pool dictionary keyed by (template, image).

    Allows string keys for backwards compatibility, treating 'template' as (template, None).
    """

    def _normalize(self, key: Any) -> Any:
        if isinstance(key, str):
            return (key, None)
        return key

    def __getitem__(self, key: Any) -> Any:
        return super().__getitem__(self._normalize(key))

    def __setitem__(self, key: Any, value: Any) -> None:
        super().__setitem__(self._normalize(key), value)

    def __contains__(self, key: Any) -> bool:
        return super().__contains__(self._normalize(key))

    def get(self, key: Any, default: Any = None) -> Any:
        return super().get(self._normalize(key), default)

    def pop(self, key: Any, *args: Any) -> Any:
        return super().pop(self._normalize(key), *args)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        return super().setdefault(self._normalize(key), default)


class SubstrateBackendDriver(BackendDriver):
    """Agent Substrate Backend Driver.

    Manages environment lifecycle directly against the ate-env-api gRPC service
    via ate_env_rl.Client: creating environments, suspending for pre-warmed pools,
    and deleting on release.

    tracer_config enables ``ate_env_client.ensure_template`` / ``.warm_pool`` /
    ``.acquire`` / ``.release`` / ``.reap`` spans and is handed to the Client
    the driver builds (needs the ``tracing`` extra).
    """

    backend_name = "substrate"

    def __init__(
        self,
        api_endpoint: str | None = None,
        atespace: str = DEFAULT_ATESPACE,
        worker_family: str = "c2",
        auth_token: str | None = None,
        *,
        client: Client | None = None,
        image_templates: Mapping[str, str] | None = None,
        default_template: str | None = None,
        labels: Mapping[str, str] | None = None,
        acquire_timeout_s: float = 180.0,
        ready_timeout_s: float = 180.0,
        loop_runner: LoopRunner | None = None,
        tracer_config: TracerConfig | None = None,
        connections: int = 1,
        worker_selector: Mapping[str, str] | None = None,
        placement: Mapping[str, Mapping[str, str]] | None = None,
    ):
        if not isinstance(connections, int) or connections < 1:
            raise ValueError(f"connections must be >= 1, got {connections!r}")
        target = (
            api_endpoint
            or os.environ.get("ATE_ENV_API_TARGET")
            or os.environ.get("SUBSTRATE_API_ENDPOINT")
            or "127.0.0.1:7777"
        )
        self.api_endpoint = target
        self.atespace = atespace or DEFAULT_ATESPACE
        self.worker_family = worker_family
        self.auth_token = auth_token
        self.image_templates = dict(image_templates or {})
        self.default_template = default_template
        self.labels = dict(labels or {})
        # The per-environment worker selector every create sends, and the
        # per-image labels merged over it (FleetConfig.node_selector and
        # FleetConfig.placement).
        self.worker_selector = dict(worker_selector or {})
        self.placement = {img: dict(sel) for img, sel in (placement or {}).items()}
        self.acquire_timeout_s = acquire_timeout_s
        self.ready_timeout_s = ready_timeout_s
        self.tracer_config = tracer_config or TracerConfig()
        self.tracer = tracer_from_config(self.tracer_config)

        self._loop_runner = loop_runner
        self._owns_loop_runner = loop_runner is None

        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            self._client = None
            self._owns_client = True
        # A caller-provided client is used for every call; the pool only
        # applies to clients the driver creates.
        self.connections = connections if self._owns_client else 1
        # Extra clients 1..connections-1; index 0 is self._client.
        self._extra_clients: list[Client] = []

        self._lock = threading.Lock()
        self._owned_actors: dict[str, RawSandboxInstance] = {}
        self._warm_paused_pool: _WarmPoolDict = _WarmPoolDict()
        # Bumped when a reap starts and when it ends; _reaping counts the reaps
        # in progress. A recycle, warm_pool or warm-claim put-back that awaited
        # across a reap, or finishes during one, deletes its actor instead of
        # pooling it (_may_pool).
        self._teardowns = 0
        self._reaping = 0
        # Per run id: reaps of that run in progress, and the _teardowns
        # value when its last reap ended. An acquire whose own run was reaped
        # while it created the actor owns it under WARM_POOL_RUN_ID (AC-15.10).
        self._run_reaps_running: dict[str, int] = {}
        self._run_reap_ended: dict[str, int] = {}
        # Bumped per pool key by unwarm_pool and by a resize_pool that shrinks
        # below what is pooled and in flight: a warm_pool call that started
        # before deletes what it created instead of pooling it.
        self._pool_gens: dict[tuple[str, str | None], int] = {}
        # Actors that warm_pool calls are still creating, per (pool key,
        # _teardowns, pool generation), so overlapping calls do not each
        # provision the full shortfall. A reap drops every reservation with
        # its generation; unwarm_pool and resize_pool start a new one.
        self._warm_inflight: dict[tuple[tuple[str, str | None], int, int], int] = {}
        self._background_tasks: set[asyncio.Task[Any]] = set()

    def _get_loop_runner(self) -> LoopRunner:
        with self._lock:
            if self._loop_runner is None:
                self._loop_runner = LoopRunner(name="ate-substrate-driver-sync")
                self._owns_loop_runner = True
            return self._loop_runner

    def _new_client(self) -> Client:
        # With more than one connection, every channel gets its own
        # subchannel pool: otherwise gRPC shares one TCP connection between
        # channels with the same target and arguments, and that connection's
        # HTTP/2 stream limit caps the calls in flight.
        options = [("grpc.use_local_subchannel_pool", 1)] if self.connections > 1 else None
        return Client(
            self.api_endpoint,
            auth_token=self.auth_token,
            tracer_config=self.tracer_config,
            channel_options=options,
        )

    @property
    def client(self) -> Client:
        # Under the lock, so concurrent first calls (for example from
        # _client_for with index 0) create one channel, not several.
        with self._lock:
            if self._client is None:
                self._client = self._new_client()
            return self._client

    def _client_for(self, actor_id: str) -> Client:
        """The client that carries every call for actor_id.

        Actors are spread over the pool by a stable hash, so all calls for
        one actor share a connection (and so one ate-env-api pod).
        """
        if self.connections == 1:
            return self.client
        index = zlib.crc32(actor_id.encode()) % self.connections
        if index == 0:
            return self.client
        with self._lock:
            while len(self._extra_clients) < self.connections - 1:
                self._extra_clients.append(self._new_client())
            return self._extra_clients[index - 1]

    def _take_owned_clients(self) -> list[Client]:
        with self._lock:
            clients = [c for c in [self._client, *self._extra_clients] if c is not None]
            self._client = None
            self._extra_clients = []
        return clients

    @staticmethod
    async def _close_clients(clients: list[Client]) -> None:
        """Close clients concurrently; every close runs even if one fails."""
        results = await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result

    def _sync(self, coro: Any) -> Any:
        return self._get_loop_runner().run(coro)

    async def close_async(self) -> None:
        if self._owns_client:
            await self._close_clients(self._take_owned_clients())
        if self._owns_loop_runner and self._loop_runner:
            self._loop_runner.stop()
            self._loop_runner = None

    def close(self) -> None:
        if self._loop_runner:
            if self._owns_client:
                self._loop_runner.run(self._close_clients(self._take_owned_clients()))
            if self._owns_loop_runner:
                self._loop_runner.stop()
                self._loop_runner = None
        elif self._owns_client and (self._client or self._extra_clients):
            self._sync(self.close_async())

    async def preflight_async(self) -> None:
        """Validate ate-env-api connectivity."""
        logger.info(
            "Running Substrate preflight checks (Atespace: %s, Endpoint: %s)...",
            self.atespace,
            self.api_endpoint,
        )
        try:
            await self.client.get(
                f"preflight-probe-{uuid.uuid4().hex[:6]}", atespace=self.atespace
            )
        except NotFoundError:
            # NotFoundError confirms the gRPC server is reachable and active.
            logger.debug("Preflight probe successfully reached ate-env-api")
        except Exception as e:
            raise PreflightError(
                f"Substrate preflight check failed to reach ate-env-api at {self.api_endpoint}: {e}"
            ) from e

    def preflight(self) -> None:
        self._sync(self.preflight_async())

    def worker_selector_for(self, rootfs_image: str | None) -> dict[str, str] | None:
        """The worker selector a create for rootfs_image sends: the fleet-wide
        labels with the image's placement labels merged over them, or None
        when there are none, which leaves the template's selection alone."""
        selector = dict(self.worker_selector)
        if rootfs_image:
            selector.update(self.placement.get(rootfs_image, {}))
        return selector or None

    def ensure_template(self, env: EnvironmentSpec | str) -> str:
        """Resolve ActorTemplate name from template ID or task image.

        Note: ResourceLimits, runtime_bundle, snapshot_storage_uri, and placement
        fields in EnvironmentSpec are reserved for future scheduling extensions.
        Current Substrate environments are provisioned from pre-registered ActorTemplates.
        """
        with start_span(self.tracer, f"{CLIENT_SPAN_PREFIX}.ensure_template") as span:
            template = self._resolve_template(env)
            set_attributes(span, {"ate.template.name": template})
            return template

    def _resolve_template(self, env: EnvironmentSpec | str) -> str:
        if isinstance(env, str):
            image = env
            if image in self.image_templates:
                return self.image_templates[image]
            return self.default_template or "default-template"

        image = env.image or ""
        mapped = self.image_templates.get(image)
        if mapped:
            return mapped
        if self.default_template:
            return self.default_template
        if isinstance(env, EnvironmentSpec):
            return env.template_key()
        return "default-template"

    def _make_instance(
        self, actor_id: str, template_id: str, run_id: str, rootfs_image: str | None = None
    ) -> RawSandboxInstance:
        planes = {
            "ate_env": DataPlaneEndpoint(
                protocol="grpc",
                address=self.api_endpoint,
                headers={"x-env-id": actor_id, "x-env-atespace": self.atespace},
            ),
        }
        metadata = {
            "created_at": time.time(),
            "template": template_id,
            "atespace": self.atespace,
        }
        if rootfs_image:
            metadata["rootfs_image"] = rootfs_image
        if self.labels:
            metadata["labels"] = dict(self.labels)
        return RawSandboxInstance(
            instance_id=actor_id,
            endpoint=self.api_endpoint,
            template_id=template_id,
            run_id=run_id,
            metadata=metadata,
            data_planes=planes,
            rootfs_image=rootfs_image,
        )

    async def acquire_async(
        self,
        template_id: str,
        run_id: str,
        timeout_s: float = 180.0,
        rootfs_image: str | None = None,
    ) -> RawSandboxInstance:
        """Acquire a live sandbox instance within timeout_s.

        An acquire that times out or is cancelled while its create() is in
        flight leaves the environment, if the create landed, to the driver:
        the next reap of any run deletes it (AC-15.2).
        """
        to_s = timeout_s or self.acquire_timeout_s
        slot_wait_s = current_slot_wait_s.get() or 0.0
        drv_token = in_driver_span.set(True)
        try:
            with start_span(
                self.tracer,
                f"{CLIENT_SPAN_PREFIX}.acquire",
                {"ate.template.name": template_id, "ate_env.run_id": run_id},
            ) as span:
                try:
                    with metrics.timed("acquire"):
                        instance = await asyncio.wait_for(
                            self._acquire_internal(template_id, run_id, rootfs_image=rootfs_image),
                            timeout=to_s,
                        )
                except asyncio.TimeoutError as e:
                    self._observe("claim", "error")
                    metrics.observe_subphase(
                        "slot_wait", slot_wait_s, status="error", source="direct"
                    )
                    raise SandboxStartError(
                        f"Timed out acquiring sandbox for template {template_id} after {to_s:g}s"
                    ) from e
                except BaseException as e:
                    st = metrics.status_of(e)
                    self._observe("claim", st)
                    metrics.observe_subphase("slot_wait", slot_wait_s, status=st, source="direct")
                    raise
                self._observe("claim", "ok")
                claim_source = str(instance.metadata.get("claim_source", "direct"))
                metrics.observe_subphase(
                    "slot_wait", slot_wait_s, status="ok", source=claim_source
                )
                set_attributes(span, {"ate.actor.name": instance.instance_id})
                return instance
        finally:
            in_driver_span.reset(drv_token)

    async def _acquire_internal(
        self,
        template_id: str,
        run_id: str,
        rootfs_image: str | None = None,
    ) -> RawSandboxInstance:
        """Acquire or claim an environment.

        Note on readiness: Substrate resumes suspended environments implicitly on first
        guest traffic, so ready_timeout_s does not measure readiness here.
        """
        pool_key = (template_id, rootfs_image)
        claim_source = "cold_create"
        while True:
            actor_id: str | None = None
            with self._lock:
                teardowns = self._teardowns
                warm_list = self._warm_paused_pool.get(pool_key)
                if warm_list:
                    actor_id = warm_list.pop()

            if actor_id:
                claim_source = "warm_pool"
                self._observe("warm_remove", 1)
                claim_start = time.monotonic()
                logger.info(
                    "Claimed warm paused actor %s for template %s (rootfs_image=%s)",
                    actor_id,
                    template_id,
                    rootfs_image,
                )
                # Verify warm actor still exists on the server
                wc_token = current_warm_claim.set(True)
                try:
                    await asyncio.wait_for(
                        self._client_for(actor_id).get(actor_id, atespace=self.atespace),
                        timeout=min(self.ready_timeout_s, 15.0),
                    )
                except NotFoundError:
                    logger.warning(
                        "Warm actor %s not found on server, dropping from pool", actor_id
                    )
                    with self._lock:
                        self._owned_actors.pop(actor_id, None)
                    continue
                except Exception as e:
                    # Put back on unexpected error and raise; the claim above
                    # counted it out of the warm pool, so count it back in.
                    # After a reap the pool is gone: delete the actor instead.
                    with self._lock:
                        put_back = self._may_pool(teardowns)
                        if put_back:
                            self._warm_paused_pool.setdefault(pool_key, []).append(actor_id)
                            stale = None
                        else:
                            # Still owned (a claim does not pop it): the delete
                            # below removes it, and a reap can still see it.
                            stale = self._owned_actors.get(actor_id)
                    if put_back:
                        self._observe("warm_add", 1)
                    elif stale is not None:
                        await self._delete_unpooled(actor_id, "after teardown")
                    metrics.observe_subphase(
                        "wire_claim",
                        time.monotonic() - claim_start,
                        status="error",
                        source="warm_pool",
                    )
                    raise SandboxStartError(f"Error checking warm actor {actor_id}: {e}") from e
                finally:
                    current_warm_claim.reset(wc_token)
                instance = self._make_instance(
                    actor_id, template_id, run_id, rootfs_image=rootfs_image
                )
                instance.metadata["claim_source"] = claim_source
                # Check for a reap and take ownership in one lock hold, so a
                # reap on another thread cannot run in between (AC-15.7).
                with self._lock:
                    reaped_since = not self._may_pool(teardowns)
                    if reaped_since:
                        stale = self._owned_actors.get(actor_id)
                    else:
                        self._owned_actors[actor_id] = instance
                if reaped_since:
                    # A reap ran during get(). It deleted the actor if it
                    # matched it (the actor's run, or WARM_POOL_RUN_ID); one of
                    # another run, or a reap cancelled before its delete, left
                    # it alive and unpooled. Do not hand it out; delete it if
                    # it is still owned, then claim or create another.
                    logger.info(
                        "Teardown ran while claiming warm actor %s; not handing it out", actor_id
                    )
                    # One wire_claim sample for the abandoned claim, as for a
                    # failed one; the claim that follows records its own.
                    metrics.observe_subphase(
                        "wire_claim",
                        time.monotonic() - claim_start,
                        status="error",
                        source="warm_pool",
                    )
                    if stale is not None:
                        await self._delete_unpooled(actor_id, "after teardown")
                    continue
                wire_dur = time.monotonic() - claim_start
                metrics.observe("warm_claim", wire_dur)
                metrics.observe_subphase("wire_claim", wire_dur, status="ok", source="warm_pool")
                return instance
            else:
                claim_source = "cold_create"
                actor_id = f"sb-{uuid.uuid4().hex[:16]}"
                logger.info(
                    "Creating environment %s (template=%s, atespace=%s, rootfs_image=%s)...",
                    actor_id,
                    template_id,
                    self.atespace,
                    rootfs_image,
                )
                wire_start = time.monotonic()
                last_err: Exception | None = None
                try:
                    for attempt in range(4):
                        try:
                            await self._client_for(actor_id).create(
                                actor_id,
                                template_name=template_id,
                                atespace=self.atespace,
                                rootfs_image=rootfs_image,
                                worker_selector=self.worker_selector_for(rootfs_image),
                            )
                            last_err = None
                            break
                        except (NotFoundError, FailedPreconditionError) as e:
                            metrics.observe_subphase(
                                "wire_claim",
                                time.monotonic() - wire_start,
                                status="error",
                                source="cold_create",
                            )
                            raise SandboxStartError(
                                f"ActorTemplate '{template_id}' invalid or does not exist on Substrate server: {e}"
                            ) from e
                        except Exception as e:
                            last_err = e
                            if "already exists" in str(e).lower():
                                last_err = None
                                break
                            if attempt < 3:
                                await asyncio.sleep(0.05 * (attempt + 1))
                except BaseException as e:
                    if isinstance(e, Exception):
                        raise
                    # Cancelled (for example by acquire's timeout_s), or the
                    # loop is closing, with a create() sent: the server may
                    # have made the actor, and no caller will hold it
                    # (AC-15.2). One wire_claim sample, as for a failed create.
                    metrics.observe_subphase(
                        "wire_claim",
                        time.monotonic() - wire_start,
                        status="error",
                        source="cold_create",
                    )
                    self._own_abandoned_create(actor_id, template_id, rootfs_image, teardowns)
                    raise
                if last_err is not None:
                    metrics.observe_subphase(
                        "wire_claim",
                        time.monotonic() - wire_start,
                        status="error",
                        source="cold_create",
                    )
                    raise SandboxStartError(
                        f"Failed to create environment {actor_id}: {last_err}"
                    ) from last_err
                metrics.observe_subphase(
                    "wire_claim",
                    time.monotonic() - wire_start,
                    status="ok",
                    source="cold_create",
                )
                break

        instance = self._make_instance(actor_id, template_id, run_id, rootfs_image=rootfs_image)
        instance.metadata["claim_source"] = claim_source
        with self._lock:
            if self._run_reaped_since(run_id, teardowns):
                # The run's own reap overlapped this create and may have
                # missed the actor, and a fleet's last teardown reaps its run
                # once: own it under WARM_POOL_RUN_ID, which every reap
                # deletes. The caller's handle keeps its run_id, and its
                # release deletes the actor as usual (AC-15.10).
                self._own_for_any_reap(actor_id, instance)
            else:
                self._owned_actors[actor_id] = instance
        return instance

    def acquire(
        self,
        template_id: str,
        run_id: str,
        timeout_s: float = 180.0,
        rootfs_image: str | None = None,
    ) -> RawSandboxInstance:
        return self._sync(
            self.acquire_async(
                template_id, run_id, timeout_s=timeout_s, rootfs_image=rootfs_image
            )
        )

    async def warm_pool_async(
        self,
        template_id: str,
        replicas: int,
        wait: bool = True,
        run_id: str | None = None,
        rootfs_image: str | None = None,
    ) -> None:
        """Pre-warm a pool of actors by creating and suspending them.

        Overlapping calls for one (template, image) share the work: a call
        counts the actors other calls are still creating and does not wait
        for them, so it can return before the pool is full.
        """
        with start_span(
            self.tracer,
            f"{CLIENT_SPAN_PREFIX}.warm_pool",
            {
                "ate.template.name": template_id,
                "ate_env.replicas": replicas,
                "ate_env.run_id": run_id,
            },
        ):
            await self._warm_pool_internal(
                template_id, replicas, wait=wait, run_id=run_id, rootfs_image=rootfs_image
            )

    async def _warm_pool_internal(
        self,
        template_id: str,
        replicas: int,
        wait: bool,
        run_id: str | None,
        rootfs_image: str | None,
    ) -> None:
        logger.info(
            "Sizing warm pool for template %s (rootfs_image=%s) to %d paused actors...",
            template_id,
            rootfs_image,
            replicas,
        )
        pool_key = (template_id, rootfs_image)
        with self._lock:
            teardowns = self._teardowns
            gen = self._pool_gens.get(pool_key, 0)
            reservation = (pool_key, teardowns, gen)
            current = len(self._warm_paused_pool.get(pool_key, []))
            inflight = self._warm_inflight.get(reservation, 0)
            needed = replicas - current - inflight
            if needed <= 0:
                return
            self._warm_inflight[reservation] = inflight + needed

        def _release_reservation() -> None:
            # Caller holds self._lock. A reap since then already dropped it.
            left = self._warm_inflight.get(reservation, 0) - needed
            if left > 0:
                self._warm_inflight[reservation] = left
            else:
                self._warm_inflight.pop(reservation, None)

        sem = asyncio.Semaphore(16)
        effective_run_id = run_id or WARM_POOL_RUN_ID
        # Every id a create() was sent for, so a cancelled call can still
        # account for the actors it made (AC-15.8).
        started: list[str] = []

        async def _warm_one() -> str | None:
            actor_id = f"warm-{uuid.uuid4().hex[:10]}"
            async with sem:
                started.append(actor_id)
                try:
                    drv_tok = in_driver_span.set(True)
                    try:
                        await self._client_for(actor_id).create(
                            actor_id,
                            template_name=template_id,
                            atespace=self.atespace,
                            rootfs_image=rootfs_image,
                            worker_selector=self.worker_selector_for(rootfs_image),
                        )
                    finally:
                        in_driver_span.reset(drv_tok)
                except (NotFoundError, FailedPreconditionError) as e:
                    raise SandboxStartError(
                        f"ActorTemplate '{template_id}' invalid or does not exist on Substrate server: {e}"
                    ) from e
                except Exception as e:
                    logger.warning("Failed to provision warm actor %s: %s", actor_id, e)
                    return None
                if wait:
                    try:
                        await self._client_for(actor_id).suspend(actor_id, atespace=self.atespace)
                    except Exception as e:
                        # Created but not paused: neither pooled nor returned.
                        # Own it under WARM_POOL_RUN_ID and delete it, so a
                        # failed delete leaves it for any reap (AC-15.8).
                        logger.warning("Failed to suspend warm actor %s: %s", actor_id, e)
                        with self._lock:
                            self._owned_actors.setdefault(
                                actor_id,
                                self._make_instance(
                                    actor_id,
                                    template_id,
                                    run_id=WARM_POOL_RUN_ID,
                                    rootfs_image=rootfs_image,
                                ),
                            )
                        await self._delete_unpooled(actor_id, "after failed warm suspend")
                        return None
                else:
                    task = asyncio.create_task(
                        self._client_for(actor_id).suspend(actor_id, atespace=self.atespace)
                    )
                    self._background_tasks.add(task)

                    def _done(t: asyncio.Task[Any]) -> None:
                        self._background_tasks.discard(t)
                        if not t.cancelled() and t.exception():
                            logger.warning(
                                "Background suspend for warm actor %s failed: %s",
                                actor_id,
                                t.exception(),
                            )

                    task.add_done_callback(_done)
                return actor_id

        tasks = [_warm_one() for _ in range(needed)]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except BaseException:
            # Cancelled: the actors created so far, and any whose create() was
            # in flight, are neither pooled nor owned. Own them under
            # WARM_POOL_RUN_ID, so any run's reap deletes them (one that was
            # never created is NotFound there, which counts as gone).
            with self._lock:
                _release_reservation()
                reaped_since = not self._may_pool(teardowns)
                for aid in started:
                    self._owned_actors.setdefault(
                        aid,
                        self._make_instance(
                            aid, template_id, run_id=WARM_POOL_RUN_ID, rootfs_image=rootfs_image
                        ),
                    )
            if reaped_since and _loop_running():
                # A reap ran or is running and may have taken its snapshot
                # before this handler, and a fleet reaps its run once: delete
                # them now, in background tasks so the cancel is not delayed,
                # as the wait=False suspends do. A delete that fails or never
                # runs leaves them for any later reap (AC-15.8).
                for aid in started:
                    task = asyncio.create_task(
                        self._delete_unpooled(aid, "after a cancelled warm_pool")
                    )
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
            raise

        created = [r for r in results if isinstance(r, str)]
        # The reservation turns into pool entries under one lock, so a
        # concurrent warm_pool always counts these actors in one of the two.
        with self._lock:
            _release_reservation()
            # After a reap, unwarm_pool or a shrinking resize_pool, the pool
            # these actors were made for is gone: delete them instead.
            torn_down = not self._may_pool(teardowns)
            drained = self._pool_gens.get(pool_key, 0) != gen
            # Owned either way. Actors to delete are owned under
            # WARM_POOL_RUN_ID from the start: the run's reap may already
            # have run, and a cancel can land before any delete below starts
            # (AC-15.9).
            owner = WARM_POOL_RUN_ID if (torn_down or drained) else effective_run_id
            for aid in created:
                self._owned_actors[aid] = self._make_instance(
                    aid, template_id, run_id=owner, rootfs_image=rootfs_image
                )
                if not (torn_down or drained):
                    self._warm_paused_pool.setdefault(pool_key, []).append(aid)
        if torn_down or drained:
            if created:
                logger.info(
                    "%s ran while warming %d actors for template %s; deleting them",
                    "Teardown" if torn_down else "unwarm_pool or resize_pool",
                    len(created),
                    template_id,
                )
            why = "after teardown" if torn_down else "after unwarm_pool or resize_pool"
            await asyncio.gather(*(self._delete_unpooled(aid, why) for aid in created))
        else:
            self._observe("warm_add", len(created))

        for res in results:
            if isinstance(res, SandboxStartError):
                raise res
            if isinstance(res, Exception):
                logger.warning("Error provisioning warm actor: %s", res)

    def _own_abandoned_create(
        self, actor_id: str, template_id: str, rootfs_image: str | None, teardowns: int
    ) -> None:
        """Own an actor whose cold create() was cancelled in flight.

        As a cancelled warm_pool does (AC-15.8): own it under
        WARM_POOL_RUN_ID, so any run's reap deletes it (one whose create never
        landed is NotFound there, which counts as gone). If a reap ran since
        the caller read ``teardowns``, that reap may have taken its snapshot
        first: delete it now too, in a background task so the cancel is not
        delayed. A delete that fails or never runs leaves it for any later
        reap.
        """
        with self._lock:
            reaped_since = not self._may_pool(teardowns)
            self._owned_actors.setdefault(
                actor_id,
                self._make_instance(
                    actor_id, template_id, run_id=WARM_POOL_RUN_ID, rootfs_image=rootfs_image
                ),
            )
        if reaped_since and _loop_running():
            task = asyncio.create_task(self._delete_unpooled(actor_id, "after a cancelled acquire"))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    def _may_pool(self, teardowns: int) -> bool:
        """Whether an actor may enter the pool: no reap ran since the caller
        read ``teardowns`` and none is running. Caller holds self._lock."""
        return self._reaping == 0 and self._teardowns == teardowns

    async def _delete_unpooled(self, actor_id: str, why: str) -> None:
        """Delete an owned actor that is not in the pool and that this run's
        reap may already have missed (``why`` goes in the log).

        The actor leaves _owned_actors only once the delete succeeds (or finds
        it gone). A failed delete is counted as a release failure; a failed or
        cancelled one re-tags the actor with WARM_POOL_RUN_ID, so whichever
        run reaps next deletes it (AC-15.5, AC-15.9).
        """
        try:
            await self._client_for(actor_id).delete(actor_id, atespace=self.atespace)
        except NotFoundError:
            pass
        except Exception as e:
            self._observe("release_failed")
            logger.warning("Failed deleting actor %s %s: %s", actor_id, why, e)
            self._keep_for_any_reap(actor_id)
            return
        except BaseException:
            # Cancelled: the run's reap may already have run, so leave the
            # actor where any later reap finds it, then let the cancel through.
            self._keep_for_any_reap(actor_id)
            raise
        with self._lock:
            self._owned_actors.pop(actor_id, None)

    def _return_popped(self, actor_id: str, instance: RawSandboxInstance, teardowns: int) -> None:
        """Own again an actor that release popped and did not delete.

        A reap since the pop (one read ``teardowns``) could not see the actor,
        and a fleet reaps its run once: then own it under WARM_POOL_RUN_ID,
        which every reap deletes; otherwise under its own run id, for the
        run's reap. The check and the update share one lock hold, so a reap
        that starts in between cannot miss the actor (AC-15.9). A copy, so
        the caller's handle keeps its run_id.
        """
        with self._lock:
            if self._may_pool(teardowns):
                self._owned_actors.setdefault(actor_id, instance)
            else:
                self._own_for_any_reap(actor_id, instance)

    def _keep_for_any_reap(self, actor_id: str) -> None:
        """Keep an owned actor under WARM_POOL_RUN_ID, which every reap deletes.

        An actor a reap already popped is not added back.
        """
        with self._lock:
            inst = self._owned_actors.get(actor_id)
            if inst is not None:
                self._own_for_any_reap(actor_id, inst)

    def _own_for_any_reap(self, actor_id: str, instance: RawSandboxInstance) -> None:
        """Own ``instance`` under WARM_POOL_RUN_ID. Caller holds self._lock.

        A copy, so the caller's handle keeps its run_id.
        """
        self._owned_actors[actor_id] = dataclasses.replace(instance, run_id=WARM_POOL_RUN_ID)

    def warm_pool(
        self,
        template_id: str,
        replicas: int,
        wait: bool = True,
        run_id: str | None = None,
        rootfs_image: str | None = None,
    ) -> None:
        self._sync(
            self.warm_pool_async(
                template_id,
                replicas,
                wait=wait,
                run_id=run_id,
                rootfs_image=rootfs_image,
            )
        )

    async def resize_pool_async(
        self,
        template_id: str,
        replicas: int,
        wait: bool = True,
        rootfs_image: str | None = None,
    ) -> None:
        """Set the warm pool of (template, image) to exactly ``replicas`` paused
        actors: grow through warm_pool_async, shrink by deleting idle paused
        actors. Claimed actors are not in the pool and are never touched, so
        ``held + warm`` stays what the caller asked for (the shape Tunix's
        prewarm loop keeps with set_pool_replicas). A resize below what is
        pooled plus what warm_pool calls are still creating cancels those
        calls: they delete their actors instead of pooling them."""
        replicas = max(0, replicas)
        pool_key = (template_id, rootfs_image)
        with self._lock:
            pool = self._warm_paused_pool.get(pool_key, [])
            current = len(pool)
            reservation = (pool_key, self._teardowns, self._pool_gens.get(pool_key, 0))
            if replicas < current + self._warm_inflight.get(reservation, 0):
                self._cancel_warm_calls([pool_key])
            if replicas >= current:
                surplus = []
            else:
                surplus = pool[replicas:]
                del pool[replicas:]
        if replicas > current:
            await self.warm_pool_async(
                template_id, replicas, wait=wait, rootfs_image=rootfs_image
            )
            return
        if not surplus:
            return
        # The actors left the pool above; the shared warm-replica gauge and the
        # run report follow, as in unwarm_pool_async.
        self._observe("warm_remove", len(surplus))
        logger.info(
            "Shrinking warm pool for template %s (rootfs_image=%s) from %d to %d paused actors",
            template_id, rootfs_image, current, replicas,
        )
        await self._delete_drained(surplus, "shrinking the warm pool")

    def resize_pool(
        self,
        template_id: str,
        replicas: int,
        wait: bool = True,
        rootfs_image: str | None = None,
    ) -> None:
        self._sync(
            self.resize_pool_async(template_id, replicas, wait=wait, rootfs_image=rootfs_image)
        )

    def pool_size(self, template_id: str, rootfs_image: str | None = None) -> int | None:
        """Paused actors waiting in the warm pool of (template_id, rootfs_image)."""
        with self._lock:
            return len(self._warm_paused_pool.get((template_id, rootfs_image), []))

    async def unwarm_pool_async(self, template_id: str, rootfs_image: str | None = None) -> None:
        """Drain and delete paused actors in the warm pool.

        warm_pool calls for the same pools that are still creating actors
        are cancelled: they delete their actors instead of pooling them.
        """
        logger.info(
            "Unwarming pool for template %s (rootfs_image=%s)...", template_id, rootfs_image
        )
        with self._lock:
            if rootfs_image is not None:
                keys_to_pop = [(template_id, rootfs_image)]
            else:
                pending = [r[0] for r in self._warm_inflight]
                keys_to_pop = [
                    k
                    for k in dict.fromkeys([*self._warm_paused_pool.keys(), *pending])
                    if (isinstance(k, tuple) and k[0] == template_id)
                ]
            to_delete = []
            for k in keys_to_pop:
                to_delete.extend(self._warm_paused_pool.pop(k, []))
            self._cancel_warm_calls(keys_to_pop)
        self._observe("warm_remove", len(to_delete))
        await self._delete_drained(to_delete, "unwarming the pool")

    def _cancel_warm_calls(self, pool_keys: list[tuple[str, str | None]]) -> None:
        """Start a new generation of these pools, so warm_pool calls in
        flight delete what they create. Caller holds self._lock."""
        for k in pool_keys:
            self._pool_gens[k] = self._pool_gens.get(k, 0) + 1

    async def _delete_drained(self, actor_ids: list[str], why: str) -> None:
        """Delete actors just taken out of the pool, 16 at a time."""
        sem = asyncio.Semaphore(16)

        async def _delete_one(actor_id: str) -> None:
            async with sem:
                await self._delete_unpooled(actor_id, why)

        await asyncio.gather(*[_delete_one(aid) for aid in actor_ids], return_exceptions=True)

    def unwarm_pool(self, template_id: str, rootfs_image: str | None = None) -> None:
        self._sync(self.unwarm_pool_async(template_id, rootfs_image=rootfs_image))

    async def release_async(self, instance_id: str, recycle: bool = False) -> None:
        """Release actor: deletes environment, or suspends if recycling.

        Note: Recycling suspends the actor and returns it to the warm pool
        without resetting guest filesystem or processes.

        A failed delete does not raise, so a cleanup error never stops a
        trainer, but it is not hidden either: the ``ate_env_client.release``
        span and the ``ate_env_client_release_seconds`` sample get status
        error, the fleet observer counts it
        (``ate_env_fleet_release_failures_total``, ``RunReport.release_errors``),
        and the environment stays owned by this driver so the run's ``reap``
        tries again and raises if it still cannot delete it. An environment
        that is already gone (NotFound) counts as released.
        """
        with start_span(
            self.tracer, f"{CLIENT_SPAN_PREFIX}.release", {"ate.actor.name": instance_id}
        ) as span:
            try:
                with metrics.timed("release"):
                    await self._release_internal(instance_id, recycle)
            # Exception, not BaseException: a cancellation (CancelledError)
            # must still propagate. _release_internal catches BaseException
            # only to keep the environment owned for reap, then re-raises.
            except Exception as e:
                # Only the error type reaches the trace; the message can carry
                # bucket paths and other server details.
                mark_error(span, f"release failed: {type(e).__name__}")
                set_attributes(span, {"error.type": type(e).__name__})
                self._observe("release_failed")
                logger.warning("Failed to delete environment %s: %s", instance_id, e)

    async def _release_internal(self, instance_id: str, recycle: bool) -> None:
        """Suspend into the warm pool or delete; raises when the delete fails."""
        with self._lock:
            instance = self._owned_actors.pop(instance_id, None)
            teardowns = self._teardowns

        if recycle and instance:
            try:
                await self._client_for(instance_id).suspend(instance_id, atespace=self.atespace)
                pool_key = (instance.template_id, instance.rootfs_image)
                with self._lock:
                    pooled = self._may_pool(teardowns)
                    if pooled:
                        self._warm_paused_pool.setdefault(pool_key, []).append(instance_id)
                        self._owned_actors[instance_id] = instance
                if pooled:
                    self._observe("warm_add", 1)
                    logger.info("Recycled actor %s to paused warm pool", instance_id)
                    return
                # The reap could not see this actor (it was popped above), so
                # pooling it now would outlive the teardown: delete it below.
                logger.info("Teardown ran while recycling actor %s; deleting it", instance_id)
            except Exception as e:
                logger.warning(
                    "Failed to suspend actor %s for recycle, deleting: %s", instance_id, e
                )
            except BaseException:
                # Cancelled during suspend(): own it again, for the run's reap
                # or, if a reap ran since the pop, for any reap.
                self._return_popped(instance_id, instance, teardowns)
                raise

        try:
            await self._client_for(instance_id).delete(instance_id, atespace=self.atespace)
        except NotFoundError:
            logger.info("Environment %s was already deleted", instance_id)
            return
        except BaseException:
            # Own it again, so a reap retries the delete: the run's reap, or
            # any reap if one ran since the pop (the recycle saw it, or it ran
            # during this delete).
            if instance is not None:
                self._return_popped(instance_id, instance, teardowns)
            raise
        logger.info("Deleted environment %s", instance_id)

    def release(self, instance_id: str, recycle: bool = False) -> None:
        self._sync(self.release_async(instance_id, recycle=recycle))

    async def reap_async(self, run_id: str) -> int:
        """Force delete all environments tagged with this run_id and all warm pool actors."""
        with start_span(self.tracer, f"{CLIENT_SPAN_PREFIX}.reap", {"ate_env.run_id": run_id}):
            return await self._reap_internal(run_id)

    async def _reap_internal(self, run_id: str) -> int:
        with self._lock:
            self._reaping += 1
            self._teardowns += 1
            self._warm_inflight.clear()
            self._run_reaps_running[run_id] = self._run_reaps_running.get(run_id, 0) + 1
        try:
            return await self._reap_actors(run_id)
        finally:
            # Work that started during the reap's own awaits also sees a
            # teardown, so nothing is pooled before the fleet resets the gauge.
            with self._lock:
                self._reaping -= 1
                self._teardowns += 1
                self._warm_inflight.clear()
                left = self._run_reaps_running.get(run_id, 1) - 1
                if left > 0:
                    self._run_reaps_running[run_id] = left
                else:
                    self._run_reaps_running.pop(run_id, None)
                self._run_reap_ended[run_id] = self._teardowns

    def _run_reaped_since(self, run_id: str, teardowns: int) -> bool:
        """Whether a reap of ``run_id`` is running, or ended after the caller
        read ``teardowns``: that reap may have taken its snapshot before an
        actor the caller is about to own. Caller holds self._lock."""
        return (
            self._run_reaps_running.get(run_id, 0) > 0
            or self._run_reap_ended.get(run_id, -1) > teardowns
        )

    async def _reap_actors(self, run_id: str) -> int:
        with self._lock:
            warm_actors: list[str] = []
            for tid, actors in list(self._warm_paused_pool.items()):
                warm_actors.extend(actors)
            self._warm_paused_pool.clear()

            to_reap = [
                aid
                for aid, inst in self._owned_actors.items()
                if inst.run_id == run_id or aid in warm_actors or inst.run_id == WARM_POOL_RUN_ID
            ]
            to_reap = list(dict.fromkeys(to_reap + warm_actors))
        # The pool is empty now; the gauge follows, so a direct reap (not
        # through a fleet, which also resets it) keeps the two equal (P-9).
        if warm_actors:
            self._observe("warm_remove", len(warm_actors))

        sem = asyncio.Semaphore(16)
        failed_actors: list[str] = []
        reaped_count = 0
        reap_lock = asyncio.Lock()

        async def _reap_one(aid: str) -> None:
            nonlocal reaped_count
            async with sem:
                try:
                    await self._client_for(aid).delete(aid, atespace=self.atespace)
                    async with reap_lock:
                        reaped_count += 1
                except NotFoundError:
                    pass  # already gone (e.g. a release that timed out after the delete landed)
                except Exception as e:
                    logger.warning("Failed deleting actor %s during reap: %s", aid, e)
                    async with reap_lock:
                        failed_actors.append(aid)
                with self._lock:
                    self._owned_actors.pop(aid, None)

        await asyncio.gather(*[_reap_one(aid) for aid in to_reap], return_exceptions=True)

        if failed_actors:
            raise InfrastructureError(
                f"Failed to delete {len(failed_actors)} environments during reap: {failed_actors}"
            )

        logger.info("Reaped %d Substrate environments for run %s", reaped_count, run_id)
        return reaped_count

    def reap(self, run_id: str) -> int:
        return self._sync(self.reap_async(run_id))

    def build_runtime(
        self, instance: RawSandboxInstance, data_plane: str = "ate_env"
    ) -> tuple[Any, Any | None]:
        from ..runtime.substrate_env_client import SubstrateEnvClientRuntime

        ep = instance.data_planes.get(data_plane) or instance.data_planes.get("ate_env")
        return SubstrateEnvClientRuntime(
            env_id=instance.instance_id,
            endpoint=ep.address if ep else instance.endpoint,
            atespace=self.atespace,
            client=self._client_for(instance.instance_id),
            loop_runner=self._get_loop_runner(),
        ), ep

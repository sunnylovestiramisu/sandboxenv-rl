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

"""`PreflightWarmer`: fill the node caches an image placement plan describes.

A placement plan (``image-placement.json`` from ``swebench-loader
plan-placement``) assigns each task image to placement domains: worker pools a
per-environment worker selector addresses. Before a run, the warmer puts every
image's layers and golden snapshot on every node of its domains, so the run's
first boot of an image on a node is a restore, not a pull::

    plan = ImagePlacement.from_file("image-placement.json")
    warmer = PreflightWarmer(client, plan, run_id="pf-7")
    coverage = await warmer.run()
    coverage.write("preflight-coverage.json")
    if not coverage.clean:
        print(coverage.shortfall())

It ensures each image's derived template, which starts the golden bake, then
warms every domain in parallel: as soon as an image's golden has settled it
creates probe environments with the domain's worker selector, round after
round, until every node of the domain has run the image once. A probe's first
command returns when the layers and the golden snapshot are on its node; the
node is read back and the probe deleted. The coverage report has the same
shape as the Go loader's ``preflight-coverage.json`` (``schema_version: v1``),
so ``swebench-loader report -preflight-coverage`` reads it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib
import re
import secrets
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import grpc

from .client import DEFAULT_ATESPACE, Client
from .errors import (
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
)
from .observability import Observer
from .types import GoldenSnapshotState, ImageStatus, PlacementDomain

logger = logging.getLogger("ate_env_rl.preflight")

SCHEMA_VERSION = "v1"

_RUN_ID = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
_MAX_RUN_ID = 20
_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
# A probe delete on a loaded node can take minutes; the Go loader gives a
# cleanup ten minutes (run.go cleanupTimeout), and so does this.
_DELETE_TIMEOUT = 600.0


# ---------------------------------------------------------------------------
# The plan


# A plan's domains are the API's PlacementDomain (types.py), as
# list_placement_domains() returns them; the file carries name, selector,
# nodes and node_slots.


@dataclass(frozen=True)
class PlacementEntry:
    """One image and the domains the plan assigns it to."""

    image: str
    domains: tuple[str, ...]


@dataclass(frozen=True)
class ImagePlacement:
    """An image placement plan: ``image-placement.json`` (``schema_version: v1``).

    Only the parts the warmer uses are kept; the planner's budgets travel in
    the file untouched.
    """

    domains: tuple[PlacementDomain, ...]
    images: tuple[PlacementEntry, ...]

    @classmethod
    def from_dict(cls, plan: Mapping[str, Any]) -> ImagePlacement:
        version = plan.get("schema_version")
        if version != SCHEMA_VERSION:
            raise ValueError(f"image placement: schema_version {version!r} is not {SCHEMA_VERSION!r}")
        domains: list[PlacementDomain] = []
        names: set[str] = set()
        for i, d in enumerate(plan.get("domains") or []):
            name = d.get("name") or ""
            selector = dict(d.get("selector") or {})
            nodes = d.get("nodes", 0)
            slots = d.get("node_slots", 0) or 0
            if not name or name in names:
                raise ValueError(f"image placement: domains[{i}]: a unique name is required")
            if not selector or not all(k and v for k, v in selector.items()):
                raise ValueError(f"image placement: domain {name!r}: a selector is required")
            if not isinstance(nodes, int) or isinstance(nodes, bool) or nodes < 1:
                raise ValueError(f"image placement: domain {name!r}: nodes must be an int >= 1")
            if not isinstance(slots, int) or isinstance(slots, bool) or slots < 0:
                raise ValueError(f"image placement: domain {name!r}: node_slots must be an int >= 0")
            names.add(name)
            domains.append(PlacementDomain(name=name, selector=selector, nodes=nodes, node_slots=slots))
        if not domains:
            raise ValueError("image placement: at least one domain is required")
        images: list[PlacementEntry] = []
        seen: set[str] = set()
        for i, e in enumerate(plan.get("images") or []):
            image = e.get("image") or ""
            assigned = tuple(e.get("domains") or ())
            if not _DIGEST.search(image):
                raise ValueError(f"image placement: images[{i}]: {image!r} is not pinned by digest")
            if image in seen:
                raise ValueError(f"image placement: images[{i}]: {image!r} is listed twice")
            if not assigned or any(d not in names for d in assigned):
                raise ValueError(f"image placement: {image!r}: every domain must be in the plan")
            seen.add(image)
            images.append(PlacementEntry(image, assigned))
        return cls(tuple(domains), tuple(images))

    @classmethod
    def from_file(cls, path: str | pathlib.Path) -> ImagePlacement:
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))

    def domain(self, name: str) -> PlacementDomain | None:
        return next((d for d in self.domains if d.name == name), None)

    def entry(self, image: str) -> PlacementEntry | None:
        return next((e for e in self.images if e.image == image), None)


# ---------------------------------------------------------------------------
# The coverage report


@dataclass
class GoldenRecord:
    """One image's derived template and golden snapshot after the pre-flight.

    ``state`` is ``ready``, ``none`` (goldens are disabled on the base
    template), ``failed``, or ``pending`` when the warmer gave up waiting.
    """

    image: str
    template: str = ""
    state: str = ""
    tag: str = ""
    error: str = ""
    wait_ms: float = 0.0

    @property
    def settled(self) -> bool:
        """True when the run needs no more waiting: ready, or none by design."""
        return self.state == "ready" or (self.state == "none" and not self.error)


@dataclass
class ImageCoverage:
    """One image's coverage inside one domain."""

    image: str
    nodes: list[str] = field(default_factory=list)
    wanted: int = 0
    probes: int = 0
    failed: int = 0
    rounds: int = 0
    complete: bool = False
    error: str = ""
    # Probe environments whose delete failed; they hold a slot until swept.
    undeleted_probes: list[str] = field(default_factory=list)
    # Time the image's rounds waited for probe slots held by other passes,
    # when the warmer reports it (ate-env-api's WarmImage does).
    waited_ms: float = 0.0


@dataclass
class DomainCoverage:
    """One domain's result: the nodes its probes landed on and each image's coverage."""

    name: str
    nodes: int
    nodes_seen: list[str] = field(default_factory=list)
    images: list[ImageCoverage] = field(default_factory=list)
    complete: bool = True


@dataclass
class PreflightCoverage:
    """What a pre-flight did for a plan: ``preflight-coverage.json``.

    ``complete`` is True when every golden the warmer waited for is ready (or
    none by design) and every (image, domain) pair reached its node count.
    """

    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    plan: str = ""
    goldens: list[GoldenRecord] = field(default_factory=list)
    domains: list[DomainCoverage] = field(default_factory=list)
    complete: bool = False

    def shortfall(self) -> str:
        """What kept the pass from completing, in a sentence."""
        parts = []
        not_ready = sum(1 for g in self.goldens if not g.settled)
        if not_ready:
            parts.append(f"{not_ready} goldens not ready")
        short = sum(1 for d in self.domains for ic in d.images if not ic.complete)
        if short:
            parts.append(f"{short} (image, domain) pairs short of their nodes")
        undeleted = self.undeleted_probes
        if undeleted:
            parts.append(f"{len(undeleted)} probes left undeleted (first: {undeleted[0]})")
        return "; ".join(parts) or ("complete" if self.complete else "interrupted")

    @property
    def undeleted_probes(self) -> list[str]:
        """Every probe the pass could not delete, sorted; sweep them."""
        return sorted(p for d in self.domains for ic in d.images for p in ic.undeleted_probes)

    @property
    def clean(self) -> bool:
        """``complete`` and no probe left behind: the one flag to gate a run
        on. ``complete`` alone says the nodes are warm; a leftover probe
        holds a slot until swept (``undeleted_probes``)."""
        return self.complete and not self.undeleted_probes

    def to_dict(self) -> dict[str, Any]:
        """The report as JSON data, in the Go loader's schema."""
        out: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "started_at": _rfc3339(self.started_at),
            "finished_at": _rfc3339(self.finished_at or self.started_at),
        }
        if self.plan:
            out["plan"] = self.plan
        out["goldens"] = [_drop_empty(asdict(g), ("template", "tag", "error")) for g in self.goldens]
        out["domains"] = [
            {
                "name": d.name,
                "nodes": d.nodes,
                "nodes_seen": list(d.nodes_seen),
                "images": [
                    _drop_empty(asdict(ic), ("error", "undeleted_probes", "waited_ms"))
                    for ic in d.images
                ],
                "complete": d.complete,
            }
            for d in self.domains
        ]
        out["complete"] = self.complete
        return out

    def write(self, path: str | pathlib.Path) -> pathlib.Path:
        """Write the report as indented JSON and return its path."""
        p = pathlib.Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict(), indent=2, allow_nan=False) + "\n", encoding="utf-8")
        return p


def _rfc3339(t: datetime) -> str:
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _drop_empty(d: dict[str, Any], keys: Iterable[str]) -> dict[str, Any]:
    for k in keys:
        if not d.get(k):
            d.pop(k, None)
    return d


# ---------------------------------------------------------------------------
# The warmer


class _Golden:
    """One image's golden while the pass runs; the coverage work waits on it."""

    __slots__ = ("event", "ensured", "settled", "ok", "record")

    def __init__(self, image: str, now: float):
        self.event = asyncio.Event()
        self.ensured = now
        self.settled = False
        self.ok = False
        self.record = GoldenRecord(image=image)


class PreflightWarmer:
    """Fills the node caches an image placement plan describes.

    Args:
        client: the ``Client`` to drive; the probes are created, run and
            deleted through it.
        plan: the ``ImagePlacement`` (or its ``image-placement.json`` as a
            dict) to fill.
        atespace: the atespace of the probes and the derived templates.
        template_name, template_atespace: the base template the derived
            templates come from; the server's defaults when None.
        run_id: the prefix of the probe environment names (at most 20
            characters, ``[a-z0-9-]``). Empty gives ``pf-<6 random characters>``,
            unique per pass, so two passes in one atespace never share a
            probe name.
        probes_per_node: how many probes a round starts per node of a
            domain; a round holds its probes until all are placed, so the
            scheduler spreads them.
        image_concurrency: how many images a domain warms at once, each with
            its own rounds; a domain's ``node_slots`` caps it at
            ``node_slots // probes_per_node``.
        max_rounds: the rounds per (image, domain) before the pair is
            reported short.
        golden_timeout: seconds; give up on the goldens still baking once
            none became ready or failed for that long.
        golden_poll: seconds between polls of the baking goldens.
        probe_timeout: seconds; bounds one probe's create, first command and
            node read together.
        ensure_width: how many templates are ensured, and later polled, at once.
        images, domains: restrict the pass to those of the plan.
        observer: records the phases ``ensure_template``, ``preflight_golden``
            and ``preflight_probe``; None records nothing.
    """

    def __init__(
        self,
        client: Client,
        plan: ImagePlacement | Mapping[str, Any],
        *,
        atespace: str = DEFAULT_ATESPACE,
        template_name: str | None = None,
        template_atespace: str | None = None,
        run_id: str = "",
        probes_per_node: int = 2,
        image_concurrency: int = 4,
        max_rounds: int = 3,
        golden_timeout: float = 30 * 60,
        golden_poll: float = 10.0,
        probe_timeout: float = 10 * 60,
        ensure_width: int = 8,
        images: Sequence[str] | None = None,
        domains: Sequence[str] | None = None,
        observer: Observer | None = None,
        plan_path: str = "",
    ):
        self.client = client
        self.plan = plan if isinstance(plan, ImagePlacement) else ImagePlacement.from_dict(plan)
        self.plan_path = plan_path
        self.atespace = atespace
        self.template_name = template_name
        self.template_atespace = template_atespace
        if not run_id:
            run_id = _new_run_id()
        if len(run_id) > _MAX_RUN_ID or not _RUN_ID.match(run_id):
            raise ValueError(
                f"run_id {run_id!r} must match {_RUN_ID.pattern} and be at most {_MAX_RUN_ID} characters"
            )
        self.run_id = run_id
        for name, value in (
            ("probes_per_node", probes_per_node),
            ("image_concurrency", image_concurrency),
            ("max_rounds", max_rounds),
            ("ensure_width", ensure_width),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be an int >= 1, got {value!r}")
        for name, value in (
            ("golden_timeout", golden_timeout),
            ("golden_poll", golden_poll),
            ("probe_timeout", probe_timeout),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be > 0, got {value!r}")
        self.probes_per_node = probes_per_node
        self.image_concurrency = image_concurrency
        self.max_rounds = max_rounds
        self.golden_timeout = golden_timeout
        self.golden_poll = golden_poll
        self.probe_timeout = probe_timeout
        self.ensure_width = ensure_width
        self.observer = observer
        for d in self.plan.domains:
            if d.node_slots and probes_per_node > d.node_slots:
                raise ValueError(
                    f"{probes_per_node} probes per node exceed the {d.node_slots} node slots of domain {d.name}"
                )
        for name in domains or ():
            if self.plan.domain(name) is None:
                raise ValueError(f"the plan has no domain {name!r}")
        for img in images or ():
            if self.plan.entry(img) is None:
                raise ValueError(f"the plan does not place {img!r}")
        self._domains = [
            d for d in self.plan.domains if not domains or d.name in set(domains)
        ]
        wanted = set(images or ())
        held = {d.name for d in self._domains}
        self._images = [
            e.image
            for e in self.plan.images
            if (not wanted or e.image in wanted) and any(d in held for d in e.domains)
        ]
        if not self._images:
            raise ValueError("no image of the pass is placed in its domains")
        self._goldens: dict[str, _Golden] = {}
        self._changed = asyncio.Event()
        self._progress = 0.0
        # The domains a cancelled run left part-way, by name (see run()).
        self._partial: dict[str, DomainCoverage] = {}
        # The last run's report, also after a cancel (see run()).
        self.coverage: PreflightCoverage | None = None

    @property
    def images(self) -> list[str]:
        """The images of the pass, in plan order."""
        return list(self._images)

    @property
    def domains(self) -> list[PlacementDomain]:
        """The domains of the pass, in plan order."""
        return list(self._domains)

    # -- run ---------------------------------------------------------------

    async def run(self) -> PreflightCoverage:
        """Run the pass and return the coverage report.

        The report is returned whether or not the pass completed, and a
        probe the pass could not delete does not raise either: check
        ``clean`` (``complete`` and no ``undeleted_probes``) and
        ``shortfall()``. A cancelled run stops its golden
        poller, deletes the probes in flight, and re-raises; what it covered
        until then stays in ``coverage``: the domains that finished, the
        domains cut part-way with the images they covered and the ones in
        flight marked ``cancelled``, the probes a cancelled round could not
        delete under ``undeleted_probes``, and the goldens it was still
        waiting for recorded as ``pending`` with the error ``cancelled``.
        """
        now = time.monotonic()
        coverage = PreflightCoverage(
            run_id=self.run_id, started_at=datetime.now(timezone.utc), plan=self.plan_path
        )
        self._goldens = {img: _Golden(img, now) for img in self._images}
        self._changed = asyncio.Event()
        self._progress = now
        self._partial = {}
        self.coverage = coverage
        domain_tasks: list[asyncio.Task[DomainCoverage]] = []
        try:
            await self._ensure_templates()
            poller = asyncio.create_task(self._poll_goldens())
            domain_tasks = [asyncio.create_task(self._cover_domain(d)) for d in self._domains]
            try:
                # return_exceptions keeps a cancelled or failed domain from
                # ending this await while the others still delete their
                # probes; the first error is raised below, once all are done.
                results = await asyncio.gather(*domain_tasks, return_exceptions=True)
            except asyncio.CancelledError:
                await _await_all(domain_tasks)
                raise
            finally:
                # A run that ends early (cancelled, or a domain failed) must
                # not wait for the goldens still baking: stop the poller, which
                # settles them as cancelled, and let a cancel of this task
                # through once it has.
                if not poller.done():
                    poller.cancel()
                await asyncio.gather(poller, return_exceptions=True)
        finally:
            coverage.goldens = [self._goldens[img].record for img in self._images]
            coverage.finished_at = datetime.now(timezone.utc)
            # What a run that ended early covered: the domains that finished,
            # and the ones a cancel cut part-way, in plan order.
            by_name = dict(self._partial)
            for t in domain_tasks:
                if t.done() and not t.cancelled() and t.exception() is None:
                    by_name[t.result().name] = t.result()
            coverage.domains = [by_name[d.name] for d in self._domains if d.name in by_name]
        domains: list[DomainCoverage] = []
        for r in results:
            if isinstance(r, BaseException):
                raise r
            domains.append(r)
        coverage.domains = domains
        coverage.complete = all(g.ok for g in self._goldens.values()) and all(
            d.complete for d in domains
        )
        return coverage

    # -- goldens -----------------------------------------------------------

    async def _ensure_templates(self) -> None:
        sem = asyncio.Semaphore(self.ensure_width)

        async def ensure(img: str) -> None:
            async with sem:
                golden = self._goldens[img]
                try:
                    with self._phase("ensure_template"):
                        st = await self.client.image_status(
                            img,
                            atespace=self.atespace,
                            template_name=self.template_name,
                            template_atespace=self.template_atespace,
                            ensure=True,
                        )
                except Exception as e:
                    golden.ensured = time.monotonic()
                    self._settle(img, GoldenRecord(image=img, state="failed", error=str(e)), False)
                    return
                golden.ensured = time.monotonic()
                self._settle_from_status(img, st, ensured=True)

        await asyncio.gather(*(ensure(img) for img in self._images))

    def _settle_from_status(self, img: str, st: ImageStatus, *, ensured: bool) -> None:
        rec = GoldenRecord(
            image=img, template=st.template.name, tag=st.golden_tag, error=st.golden_error
        )
        if st.golden == GoldenSnapshotState.READY:
            rec.state = "ready"
            self._settle(img, rec, True)
        elif st.golden == GoldenSnapshotState.FAILED:
            rec.state, rec.error = "failed", f"golden bake failed: {st.golden_error}"
            self._settle(img, rec, False)
        elif st.golden == GoldenSnapshotState.NONE:
            if st.template_exists:
                # Goldens are disabled on the base template: the probes boot
                # cold and still fill the layer caches.
                rec.state = "none"
                self._settle(img, rec, True)
            elif ensured:
                rec.state, rec.error = "failed", "the template was not created"
                self._settle(img, rec, False)

    def _settle(self, img: str, rec: GoldenRecord, ok: bool) -> None:
        golden = self._goldens[img]
        if golden.settled:
            return
        now = time.monotonic()
        rec.wait_ms = max(0.0, (now - golden.ensured) * 1000.0)
        golden.settled, golden.ok, golden.record = True, ok, rec
        self._progress = now
        golden.event.set()
        self._changed.set()
        self._changed = asyncio.Event()
        if self.observer is not None:
            self.observer.record_phase(
                "preflight_golden", rec.wait_ms / 1000.0, status="ok" if ok else "error"
            )
        if not ok:
            logger.warning("preflight: golden of %s not ready: %s %s", img, rec.state, rec.error)

    def _pending(self) -> list[str]:
        return [img for img in self._images if not self._goldens[img].settled]

    async def _poll_goldens(self) -> None:
        """Poll the baking goldens until all settle, none has settled for
        ``golden_timeout``, or the run is cancelled."""
        sem = asyncio.Semaphore(self.ensure_width)
        self._progress = time.monotonic()

        async def poll(img: str) -> None:
            async with sem:
                try:
                    st = await self.client.image_status(
                        img,
                        atespace=self.atespace,
                        template_name=self.template_name,
                        template_atespace=self.template_atespace,
                    )
                except Exception as e:
                    logger.warning("preflight: image status of %s failed: %s", img, e)
                    return
                self._settle_from_status(img, st, ensured=False)

        try:
            while pending := self._pending():
                await asyncio.sleep(self.golden_poll)
                await asyncio.gather(*(poll(img) for img in pending))
                if time.monotonic() - self._progress > self.golden_timeout:
                    for img in self._pending():
                        self._settle(
                            img,
                            GoldenRecord(
                                image=img,
                                state="pending",
                                error=f"golden still pending after {self.golden_timeout:g}s without progress",
                            ),
                            False,
                        )
                    return
        except asyncio.CancelledError:
            for img in self._pending():
                self._settle(img, GoldenRecord(image=img, state="pending", error="cancelled"), False)
            raise

    # -- coverage ----------------------------------------------------------

    def _image_width(self, d: PlacementDomain) -> int:
        k = self.image_concurrency
        if d.node_slots:
            k = min(k, d.node_slots // self.probes_per_node)
        return max(k, 1)

    async def _cover_domain(self, d: PlacementDomain) -> DomainCoverage:
        """Warm the domain's images, up to ``_image_width`` at once, each taken
        as soon as its golden has settled (earliest in plan order first)."""
        items = [
            (ii, e.image)
            for ii, e in enumerate(self.plan.images)
            if e.image in self._goldens and d.name in e.domains
        ]
        covs: dict[int, ImageCoverage] = {}
        started: set[int] = set()
        sem = asyncio.Semaphore(self._image_width(d))
        tasks: list[asyncio.Task[None]] = []

        async def cover(k: int, ii: int, image: str) -> None:
            async with sem:
                # The coverage object is registered before the rounds, so a
                # cancel leaves what the image covered until then in place.
                ic = ImageCoverage(image=image, wanted=d.nodes)
                covs[k] = ic
                await self._cover_image(d, ii, image, ic)

        try:
            while len(started) < len(items):
                changed = self._changed
                pick = next(
                    (
                        k
                        for k, (_ii, image) in enumerate(items)
                        if k not in started and self._goldens[image].settled
                    ),
                    None,
                )
                if pick is None:
                    await changed.wait()
                    continue
                started.add(pick)
                ii, image = items[pick]
                tasks.append(asyncio.create_task(cover(pick, ii, image)))
            for r in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(r, BaseException):
                    raise r
        except asyncio.CancelledError:
            # The images still running finish their probes' deletes before
            # this re-raises; a task that was already cancelled through the
            # gather above is not cancelled again. What the domain covered
            # until then is kept for the report.
            for t in tasks:
                if not t.done():
                    t.cancel()
            await _await_all(tasks)
            self._partial[d.name] = self._domain_coverage(d, items, covs, cancelled=True)
            raise
        return self._domain_coverage(d, items, covs, cancelled=False)

    def _domain_coverage(
        self,
        d: PlacementDomain,
        items: list[tuple[int, str]],
        covs: dict[int, ImageCoverage],
        *,
        cancelled: bool,
    ) -> DomainCoverage:
        dc = DomainCoverage(name=d.name, nodes=d.nodes)
        seen: set[str] = set()
        for k, (_ii, image) in enumerate(items):
            ic = covs.get(k)
            if ic is None:
                dc.complete = False
                dc.images.append(ImageCoverage(image=image, wanted=d.nodes, error="not started"))
                continue
            if cancelled and not ic.complete and not ic.error:
                ic.error = "cancelled"
            seen.update(ic.nodes)
            if not ic.complete:
                dc.complete = False
            dc.images.append(ic)
        dc.nodes_seen = sorted(seen)
        if len(dc.nodes_seen) > d.nodes:
            logger.warning(
                "preflight: domain %s has %d nodes, the plan says %d",
                d.name,
                len(dc.nodes_seen),
                d.nodes,
            )
        return dc

    async def _cover_image(
        self, d: PlacementDomain, ii: int, image: str, ic: ImageCoverage
    ) -> None:
        """Rounds of probes for one image in one domain until every node of
        the domain has run it or the rounds run out, filling ``ic`` as it
        goes, so a cancel leaves the nodes covered and the probes a round
        could not delete in it."""
        nodes: set[str] = set()
        width = d.nodes * self.probes_per_node
        di = self._domains.index(d)
        try:
            for rnd in range(1, self.max_rounds + 1):
                if len(nodes) >= d.nodes:
                    break
                ic.rounds = rnd
                # Create and wait for every probe of the round before deleting
                # any, so they occupy distinct slots while the scheduler places
                # the rest. A probe whose delete fails lands in
                # ic.undeleted_probes, also when the round is cancelled.
                outcomes = await asyncio.gather(
                    *(
                        self._probe(
                            f"{self.run_id}-{di}-{ii}-{rnd}-{k}", image, d, ic.undeleted_probes
                        )
                        for k in range(width)
                    ),
                    return_exceptions=True,
                )
                for outcome in outcomes:
                    if isinstance(outcome, BaseException):
                        # _probe reports its own errors; this is a cancel or
                        # a bug, and the round cannot be scored.
                        raise outcome
                    node, err = outcome
                    ic.probes += 1
                    if err:
                        ic.failed += 1
                        ic.error = err
                    else:
                        nodes.add(node)
                if ic.failed == ic.probes:
                    # Nothing of this round came up: the image, the domain or
                    # the cluster is the problem, and more rounds would not
                    # change it.
                    logger.warning(
                        "preflight: every probe of %s in %s failed: %s", image, d.name, ic.error
                    )
                    break
        finally:
            ic.nodes = sorted(nodes)
            ic.complete = len(ic.nodes) >= d.nodes
            ic.undeleted_probes.sort()

    async def _probe(
        self, env_id: str, image: str, d: PlacementDomain, stuck: list[str]
    ) -> tuple[str, str]:
        """One probe: create in the domain, run the first command, read the
        node, delete; ``probe_timeout`` bounds the three steps together.
        Returns (node, error); a probe whose delete failed and is still there
        is appended to ``stuck``, also when the probe was cancelled."""
        holder: list[Any] = []  # the handle once created, for the delete
        node = ""
        error = ""
        try:
            with self._phase("preflight_probe"):
                node = await asyncio.wait_for(
                    self._probe_steps(env_id, image, d, holder), self.probe_timeout
                )
        except asyncio.CancelledError:
            await self._delete_through_cancel(env_id, holder[0] if holder else None, stuck)
            raise
        except asyncio.TimeoutError:
            error = f"probe not ready after {self.probe_timeout:g}s"
        except Exception as e:
            error = str(e)
            if not holder and _create_refused(e):
                # The server made nothing: the name belongs to another pass or
                # to a user (AlreadyExists), or the request was bad. Deleting
                # by id here would take someone else's environment.
                return node, error
        await self._delete_through_cancel(env_id, holder[0] if holder else None, stuck)
        return node, error

    async def _probe_steps(
        self, env_id: str, image: str, d: PlacementDomain, holder: list[Any]
    ) -> str:
        env = await self.client.create(
            env_id,
            atespace=self.atespace,
            template_name=self.template_name,
            template_atespace=self.template_atespace,
            rootfs_image=image,
            worker_selector=d.selector,
        )
        holder.append(env)
        # The first command returns once the restore finished: the layers and
        # the golden snapshot are on the node by then.
        result = await env.shell("true", timeout=self.probe_timeout)
        if result.exit_code != 0:
            raise RuntimeError(f"first command exited {result.exit_code}")
        info = await self.client.get(env_id, atespace=self.atespace)
        if not info.node:
            raise RuntimeError("the environment reports no node")
        return info.node

    async def _delete_through_cancel(self, env_id: str, env: Any, stuck: list[str]) -> bool:
        """Delete the probe and wait for it even if this task is cancelled
        again meanwhile, so a cancel mid-round strands nothing; the delete's
        own timeout bounds the wait. True when the probe is gone. A probe
        whose delete failed is appended to ``stuck`` by the delete itself,
        so a cancel that lands as the delete ends (re-raised here, with no
        return value) does not lose it."""
        delete = asyncio.ensure_future(self._delete(env_id, env, stuck))
        while True:
            try:
                return await asyncio.shield(delete)
            except asyncio.CancelledError:
                if delete.done():
                    raise
                # Keep waiting for the delete; the cancel is re-raised by the
                # caller once the probe is gone.
                continue

    async def _delete(self, env_id: str, env: Any, stuck: list[str]) -> bool:
        """Delete the probe; True when it is gone (deleted, or never there).
        A probe the delete left in place is appended to ``stuck``."""
        try:
            # A create that failed client-side may exist server-side, so the
            # delete goes by id when there is no handle.
            await asyncio.wait_for(
                env.delete() if env is not None else self.client.delete(env_id, atespace=self.atespace),
                _DELETE_TIMEOUT,
            )
            return True
        except NotFoundError:
            return True
        except Exception as e:
            logger.warning("preflight: delete of %s failed: %s", env_id, e)
            stuck.append(env_id)
            return False

    def _phase(self, name: str) -> Any:
        if self.observer is None:
            return _NoPhase()
        return self.observer.phase(name)


async def _await_all(tasks: list[asyncio.Task[Any]]) -> None:
    """Wait for every task to finish, through cancels of the waiter: a task
    cancelled during its cleanup would otherwise strand what it was deleting."""
    while True:
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            return
        except asyncio.CancelledError:
            if all(t.done() for t in tasks):
                raise
            continue


def _new_run_id() -> str:
    """``pf-`` and six random characters, as ate-env-api's ``WarmImage`` does."""
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "pf-" + "".join(secrets.choice(alphabet) for _ in range(6))


_REFUSED = frozenset(
    (
        grpc.StatusCode.ALREADY_EXISTS,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNAUTHENTICATED,
        # No room: ate-env-api's create is one CreateActor call, and the
        # control plane says no before anything exists.
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    )
)


def _create_refused(e: BaseException) -> bool:
    """True when a failed create made no environment: the server refused it
    (the name exists, a bad argument, a missing or conflicting template, no
    permission, no room). A create that was cut off may still have landed."""
    if isinstance(e, (InvalidArgumentError, PermissionDeniedError, FailedPreconditionError)):
        return True
    # The SDK's RpcError carries the status as an attribute; a raw grpc error
    # has it as a method.
    code = e.code() if isinstance(e, grpc.RpcError) else getattr(e, "code", None)
    return code in _REFUSED


class _NoPhase:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> None:
        return None

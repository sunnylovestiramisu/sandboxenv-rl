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

"""PreflightWarmer against the in-process fakes."""

from __future__ import annotations

import asyncio
import json
import time

import grpc
import pytest

from ate_env_rl import ImagePlacement, Observer, PreflightWarmer
from ate_env_rl._gen.ateenv.v1alpha import env_pb2
from ate_env_rl.errors import (
    FailedPreconditionError,
    InvalidArgumentError,
    PermissionDeniedError,
    SandboxUnavailableError,
)
from ate_env_rl.preflight import _create_refused

IMG_A = "registry.example/swebench/a@sha256:" + "a" * 64
IMG_B = "registry.example/swebench/b@sha256:" + "b" * 64
IMG_C = "registry.example/swebench/c@sha256:" + "c" * 64

PLAN = {
    "schema_version": "v1",
    "unpack_factor": 1,
    "replication": 1,
    "domains": [
        {"name": "d0", "selector": {"ate-env/domain": "d0"}, "nodes": 2, "node_cache_bytes": 0,
         "assigned_bytes": 2, "image_count": 2},
        {"name": "d1", "selector": {"ate-env/domain": "d1"}, "nodes": 2, "node_cache_bytes": 0,
         "assigned_bytes": 1, "image_count": 1},
    ],
    "images": [
        {"image": IMG_A, "domains": ["d0"], "bytes_compressed": 1, "bytes_unpacked": 1},
        {"image": IMG_B, "domains": ["d0", "d1"], "bytes_compressed": 1, "bytes_unpacked": 1},
    ],
}

NODES = {"d0": ["node-d0-1", "node-d0-2"], "d1": ["node-d1-1", "node-d1-2"]}


def _key(image: str) -> tuple[str, str]:
    digest = image.split("@sha256:", 1)[1]
    return ("ate-env", f"default-template-{digest[:12]}")


def _set_golden(fakes, image: str, state, *, tag: str = "", error: str = "") -> None:
    fakes.environments.image_templates[_key(image)] = env_pb2.ImageStatus(
        golden=state, golden_tag=tag, golden_error=error
    )


def _warmer(client, **kw) -> PreflightWarmer:
    opts = dict(run_id="pf-test", probes_per_node=1, image_concurrency=1, golden_poll=0.005,
                golden_timeout=1.0, probe_timeout=5.0)
    opts.update(kw)
    return PreflightWarmer(client, PLAN, **opts)


async def test_covers_every_node_of_every_domain(fake_stack, tmp_path):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_READY, tag="golden-7")
    obs = Observer()
    with obs.run("preflight") as report:
        coverage = await _warmer(client, observer=obs, plan_path="image-placement.json").run()

    assert coverage.complete, coverage.shortfall()
    assert [g.image for g in coverage.goldens] == [IMG_A, IMG_B]
    assert all(g.state == "ready" and g.tag == "golden-7" and g.template for g in coverage.goldens)
    want = {
        "d0": {IMG_A: ["node-d0-1", "node-d0-2"], IMG_B: ["node-d0-1", "node-d0-2"]},
        "d1": {IMG_B: ["node-d1-1", "node-d1-2"]},
    }
    for d in coverage.domains:
        assert d.complete and d.nodes_seen == NODES[d.name]
        assert {ic.image: ic.nodes for ic in d.images} == want[d.name]
        for ic in d.images:
            assert ic.complete and ic.rounds == 1 and ic.probes == 2 and ic.failed == 0
    # Every probe was deleted and carried its domain's selector.
    assert fakes.environments.environments == {}
    assert fakes.environments.last_create_request.worker_selector["ate-env/domain"] in NODES
    # The observer saw the phases.
    assert report.phases["ensure_template"][0] == 2
    assert report.phases["preflight_golden"][0] == 2
    assert report.phases["preflight_probe"][0] == 6

    # The report writes in the loader's schema.
    out = coverage.write(tmp_path / "preflight-coverage.json")
    data = json.loads(out.read_text())
    assert data["schema_version"] == "v1" and data["run_id"] == "pf-test"
    assert data["plan"] == "image-placement.json" and data["complete"] is True
    # The keys of the Go loader's preflight-coverage.json (schema v1), fixed
    # here so the check does not depend on the Go fixture being on the branch.
    assert set(data) == {"schema_version", "run_id", "started_at", "finished_at", "plan",
                         "goldens", "domains", "complete"}
    assert set(data["goldens"][0]) == {"image", "template", "state", "tag", "wait_ms"}
    assert set(data["domains"][0]) == {"name", "nodes", "nodes_seen", "images", "complete"}
    assert set(data["domains"][0]["images"][0]) == {"image", "nodes", "wanted", "probes",
                                                   "failed", "rounds", "complete"}
    assert data["started_at"].endswith("Z") and data["finished_at"] >= data["started_at"]
    for g in data["goldens"]:
        assert g["state"] in {"ready", "none", "pending", "failed"} and g["wait_ms"] >= 0
    for d in data["domains"]:
        assert isinstance(d["nodes"], int) and isinstance(d["complete"], bool)
        for ic in d["images"]:
            assert all(isinstance(ic[k], int) for k in ("wanted", "probes", "failed", "rounds"))
            assert isinstance(ic["complete"], bool) and isinstance(ic["nodes"], list)


async def test_streams_ready_goldens_and_gives_up_on_a_stalled_one(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    # a (first in plan order) never gets its golden; b is ready at once.
    _set_golden(fakes, IMG_B, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    created: list[str] = []
    original = client.create

    async def record(env_id, **kw):
        created.append(kw["rootfs_image"])
        return await original(env_id, **kw)

    client.create = record  # type: ignore[method-assign]
    coverage = await _warmer(client, golden_timeout=0.1).run()

    assert not coverage.complete
    assert coverage.shortfall() == "1 goldens not ready"
    a = next(g for g in coverage.goldens if g.image == IMG_A)
    assert a.state == "pending" and "without progress" in a.error
    # b was covered in d0 before a's golden gave up; a was still probed after,
    # because a boot fills the layers too.
    first_a = created.index(IMG_A)
    assert created[:first_a].count(IMG_B) >= 2
    assert all(d.complete for d in coverage.domains)


async def test_golden_failure_and_disabled_goldens(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # b's bake fails after the warmer ensured it.
    run = asyncio.create_task(_warmer(client, golden_timeout=2.0).run())
    await asyncio.sleep(0.05)
    _set_golden(fakes, IMG_B, env_pb2.GOLDEN_SNAPSHOT_STATE_FAILED, error="GoldenActorCrashed: boom")
    coverage = await run
    b = next(g for g in coverage.goldens if g.image == IMG_B)
    assert b.state == "failed" and "boom" in b.error
    assert not coverage.complete and all(d.complete for d in coverage.domains)

    # Goldens disabled on the base template: none, no wait, complete.
    fakes.environments.environments.clear()
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_NONE)
    coverage = await _warmer(client).run()
    assert coverage.complete
    assert all(g.state == "none" and not g.error and g.template for g in coverage.goldens)


async def test_short_coverage_and_probe_failures(fake_stack):
    client, fakes = fake_stack
    # Every create of a domain lands on its first node.
    fakes.environments.domain_nodes = {d: [n[0]] for d, n in NODES.items()}
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    coverage = await _warmer(client, max_rounds=2).run()
    assert not coverage.complete
    assert coverage.shortfall() == "3 (image, domain) pairs short of their nodes"
    for d in coverage.domains:
        for ic in d.images:
            assert not ic.complete and ic.rounds == 2 and ic.probes == 4 and len(ic.nodes) == 1
    assert fakes.environments.environments == {}

    # A domain with no nodes behind it: the probes report no node and fail.
    fakes.environments.domain_nodes = {"d1": NODES["d1"]}
    coverage = await _warmer(client, domains=["d0"]).run()
    d0 = coverage.domains[0]
    assert not d0.complete and d0.images[0].failed == 2 and "no node" in d0.images[0].error
    # The first round failed entirely, so the warmer stopped there.
    assert d0.images[0].rounds == 1


async def test_subsets_concurrency_cap_and_validation(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    for img in (IMG_A, IMG_B):
        _set_golden(fakes, img, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # Only d1 wanted: a, which no selected domain holds, is not waited for.
    w = _warmer(client, domains=["d1"])
    assert w.images == [IMG_B] and [d.name for d in w.domains] == ["d1"]
    coverage = await w.run()
    assert coverage.complete and [g.image for g in coverage.goldens] == [IMG_B]

    # node_slots caps the concurrency and refuses too many probes per node.
    plan = json.loads(json.dumps(PLAN))
    plan["domains"][0]["node_slots"] = 3
    w = PreflightWarmer(client, plan, probes_per_node=2, image_concurrency=8)
    assert w._image_width(w.domains[0]) == 1 and w._image_width(w.domains[1]) == 8
    with pytest.raises(ValueError, match="node slots"):
        PreflightWarmer(client, plan, probes_per_node=4)

    for bad in (
        dict(images=[IMG_C]),
        dict(domains=["d9"]),
        dict(images=[IMG_A], domains=["d1"]),
        dict(run_id="Not Valid"),
        dict(probes_per_node=0),
        dict(golden_timeout=0),
    ):
        with pytest.raises(ValueError):
            PreflightWarmer(client, PLAN, **bad)
    with pytest.raises(ValueError, match="schema_version"):
        ImagePlacement.from_dict({"schema_version": "v0"})
    with pytest.raises(ValueError, match="pinned by digest"):
        ImagePlacement.from_dict({**PLAN, "images": [{"image": "reg/x:latest", "domains": ["d0"]}]})


async def test_cancel_mid_round_deletes_the_probes_and_returns_at_once(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # b's golden never comes, so a cancel also has to stop the golden poller
    # instead of waiting for golden_timeout.
    created = asyncio.Event()
    hold = asyncio.Event()
    original_get = client.get

    async def get(env_id, **kw):
        created.set()
        await hold.wait()
        return await original_get(env_id, **kw)

    client.get = get  # type: ignore[method-assign]
    warmer = _warmer(client, golden_timeout=30.0, probe_timeout=30.0)
    run = asyncio.create_task(warmer.run())
    await asyncio.wait_for(created.wait(), 5)
    assert fakes.environments.environments, "the probes of a's first round exist"
    started = time.monotonic()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert time.monotonic() - started < 5, "the cancel did not wait for golden_timeout"
    assert fakes.environments.environments == {}, "every probe was deleted"
    # What the run covered until then is kept, with the golden it was still
    # waiting for recorded as cancelled.
    cov = warmer.coverage
    assert cov is not None and cov.finished_at is not None
    assert {g.image: (g.state, g.error) for g in cov.goldens}[IMG_B] == ("pending", "cancelled")
    assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()
                and "preflight" in repr(t.get_coro())], "no warmer task is left pending"


async def test_cancel_during_the_golden_wait_returns_at_once(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    # Neither golden comes: the run sits in the golden wait until cancelled.
    run = asyncio.create_task(_warmer(client, golden_timeout=30.0).run())
    await asyncio.sleep(0.05)
    started = time.monotonic()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert time.monotonic() - started < 5
    assert fakes.environments.environments == {}


async def test_second_round_covers_the_node_the_first_missed(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    # The first two creates of a domain land on its first node, as a
    # scheduler that favours a warm node does; the next round spreads.
    fakes.environments.first_node_creates = 2
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"]).run()
    assert coverage.complete
    ic = coverage.domains[0].images[0]
    assert ic.rounds == 2 and ic.probes == 4 and ic.nodes == ["node-d0-1", "node-d0-2"]


async def test_probe_timeout_bounds_the_whole_probe(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # The node read never answers: the probe fails at probe_timeout, counted
    # from its create, and is deleted.
    original_get = client.get

    async def get(env_id, **kw):
        await asyncio.sleep(10)
        return await original_get(env_id, **kw)

    client.get = get  # type: ignore[method-assign]
    started = time.monotonic()
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"], max_rounds=1,
                             probe_timeout=0.3).run()
    assert time.monotonic() - started < 3
    ic = coverage.domains[0].images[0]
    assert not ic.complete and ic.failed == 2 and "not ready after 0.3s" in ic.error
    assert fakes.environments.environments == {}


def test_plan_domains_are_the_api_placement_domain() -> None:
    """The plan's domains and list_placement_domains() share one type."""
    from ate_env_rl import PlacementDomain, preflight

    selector = {"ate-env/domain": "a"}
    domain = {"name": "a", "selector": selector, "nodes": 2, "node_slots": 4}
    image = {"image": "img@sha256:" + "0" * 64, "domains": ["a"]}
    plan = ImagePlacement.from_dict(
        {"schema_version": "v1", "domains": [domain], "images": [image]}
    )
    want = PlacementDomain(name="a", selector=selector, nodes=2, node_slots=4)
    assert preflight.PlacementDomain is PlacementDomain
    assert [type(d) for d in plan.domains] == [PlacementDomain]
    assert plan.domains[0] == want
    assert plan.domains[0].worker_namespace == "" and plan.domains[0].workers == 0


async def test_a_colliding_name_is_left_alone(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # Someone else holds the name of our first probe.
    taken = "pf-test-0-0-1-0"
    await client.create(taken, rootfs_image=IMG_A)
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"], max_rounds=1).run()
    ic = coverage.domains[0].images[0]
    assert ic.probes == 2 and ic.failed == 1 and "already exists" in ic.error
    assert ("ate-env", taken) in fakes.environments.environments, "the environment that is not ours survived"
    assert len(fakes.environments.environments) == 1, "our own probe was deleted"


def test_default_run_id_is_unique_per_pass(fake_stack):
    client, _ = fake_stack
    a = PreflightWarmer(client, PLAN)
    b = PreflightWarmer(client, PLAN)
    for w in (a, b):
        assert w.run_id.startswith("pf-") and len(w.run_id) == 9
    assert a.run_id != b.run_id
    with pytest.raises(ValueError):
        PreflightWarmer(client, PLAN, run_id="Not Valid")


async def test_a_cut_off_create_is_deleted_by_id(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    _set_golden(fakes, IMG_A, env_pb2.GOLDEN_SNAPSHOT_STATE_READY)
    # The create lands on the server, then the call is cut off before the
    # handle comes back: the probe fails and is deleted by its id.
    original = client.create

    async def cut_off(env_id, **kw):
        await original(env_id, **kw)
        raise SandboxUnavailableError("cut off")

    client.create = cut_off  # type: ignore[method-assign]
    coverage = await _warmer(client, images=[IMG_A], domains=["d0"], max_rounds=1).run()
    ic = coverage.domains[0].images[0]
    assert ic.probes == 2 and ic.failed == 2 and "cut off" in ic.error
    assert fakes.environments.environments == {}, "the cut-off creates were deleted by id"


def test_create_refused_classifies_the_errors():
    class _Raw(grpc.RpcError):
        def __init__(self, code):
            self._code = code

        def code(self):
            return self._code

    class _Mapped(Exception):
        def __init__(self, code):
            self.code = code

    refused = [
        InvalidArgumentError("bad"),
        PermissionDeniedError("no"),
        FailedPreconditionError("no template"),
        _Raw(grpc.StatusCode.ALREADY_EXISTS),
        _Mapped(grpc.StatusCode.RESOURCE_EXHAUSTED),
        _Mapped(grpc.StatusCode.UNAUTHENTICATED),
    ]
    cut_off = [
        SandboxUnavailableError("down"),
        _Raw(grpc.StatusCode.DEADLINE_EXCEEDED),
        _Mapped(grpc.StatusCode.UNKNOWN),
        RuntimeError("plain"),
    ]
    assert all(_create_refused(e) for e in refused)
    assert not any(_create_refused(e) for e in cut_off)

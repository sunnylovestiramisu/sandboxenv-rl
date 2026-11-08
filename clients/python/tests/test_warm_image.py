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

"""list_placement_domains and warm_image against the fake API."""

from __future__ import annotations

import pytest

from ate_env_rl import (
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
)

IMAGE = "registry.example/swebench/a@sha256:" + "a" * 64
NODES = {"d0": ["node-d0-1", "node-d0-2"], "d1": ["node-d1-1"]}


async def test_list_placement_domains(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    fakes.environments.node_slots = {"d0": 16}
    domains = await client.list_placement_domains()
    assert [d.name for d in domains] == ["pool-d0", "pool-d1"]
    d0 = domains[0]
    assert d0.worker_namespace == "ate-system"
    assert d0.selector == {"ate-env/domain": "d0"}
    assert (d0.nodes, d0.workers, d0.node_slots) == (2, 2, 16)
    assert domains[1].node_slots == 0

    fakes.environments.deny_list_workers = True
    with pytest.raises(PermissionDeniedError):
        await client.list_placement_domains()


async def test_list_placement_domains_filters_by_template(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    fakes.environments.template_selectors[("ate-env", "d0-template")] = {"ate-env/domain": "d0"}
    # The default template carries no selector: every pool.
    assert [d.name for d in await client.list_placement_domains(atespace="ate-env")] == ["pool-d0", "pool-d1"]
    # A template pinned to d0 addresses pool-d0 only.
    assert [d.name for d in await client.list_placement_domains(template_name="d0-template")] == ["pool-d0"]
    with pytest.raises(NotFoundError):
        await client.list_placement_domains(template_name="nope")
    # The template is read first: an unknown one is NotFound even without
    # the role to list workers.
    fakes.environments.deny_list_workers = True
    with pytest.raises(NotFoundError):
        await client.list_placement_domains(template_name="nope")


async def test_warm_image_covers_the_domain_and_reports_each_probe(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    seen = []
    coverage = await client.warm_image(
        IMAGE,
        worker_selector={"ate-env/domain": "d0"},
        probes_per_node=1,
        run_id="pf-0-3",
        probe_timeout=30,
        on_probe=seen.append,
    )
    assert coverage.complete and coverage.nodes == ("node-d0-1", "node-d0-2")
    assert (coverage.wanted, coverage.probes, coverage.failed, coverage.rounds) == (2, 2, 0, 1)
    assert [p.id for p in seen] == ["pf-0-3-1-0", "pf-0-3-1-1"]
    assert all(p.round == 1 and p.node and not p.error and p.duration > 0 for p in seen)
    req = fakes.environments.warm_requests[-1]
    assert req.rootfs_image == IMAGE and req.probes_per_node == 1 and req.run_id == "pf-0-3"
    assert req.probe_timeout.ToTimedelta().total_seconds() == 30


async def test_warm_image_short_coverage_and_errors(fake_stack):
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    fakes.environments.warm_same_node = True
    coverage = await client.warm_image(IMAGE, worker_selector={"ate-env/domain": "d0"}, max_rounds=2)
    assert not coverage.complete and coverage.nodes == ("node-d0-1",)
    assert coverage.rounds == 2 and coverage.probes == 8

    with pytest.raises(InvalidArgumentError):
        await client.warm_image("registry.example/x:latest", nodes=1)
    with pytest.raises(FailedPreconditionError):
        await client.warm_image(IMAGE, worker_selector={"ate-env/domain": "d9"})
    fakes.environments.deny_list_workers = True
    with pytest.raises(PermissionDeniedError, match="pass nodes"):
        await client.warm_image(IMAGE, worker_selector={"ate-env/domain": "d0"})
    fakes.environments.warm_same_node = False
    coverage = await client.warm_image(IMAGE, worker_selector={"ate-env/domain": "d0"}, nodes=2, probes_per_node=1)
    assert coverage.complete
    assert coverage.undeleted_probes == () and coverage.waited == 0.0


async def test_warm_image_reports_undeleted_probes_and_the_wait_for_slots(fake_stack):
    # The server lists the probes it could not delete (a failed delete, or a
    # CRASHED probe) for the caller to sweep, and the time the rounds waited
    # for probe slots held by other calls.
    client, fakes = fake_stack
    fakes.environments.domain_nodes = dict(NODES)
    fakes.environments.warm_undeleted = ["pf-1-1-0", "pf-1-1-1"]
    fakes.environments.warm_waited = 2.5
    coverage = await client.warm_image(IMAGE, worker_selector={"ate-env/domain": "d0"}, run_id="pf-1")
    assert coverage.complete
    assert coverage.undeleted_probes == ("pf-1-1-0", "pf-1-1-1")
    assert coverage.waited == 2.5

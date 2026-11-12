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

"""The warm step against the fake API: templates ensured, goldens awaited, a domain warmed."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import shutil
import sys
from pathlib import Path

import grpc
import pytest
from ate_env_rl import Client, FailedPreconditionError, InvalidArgumentError

from ate_env_harbor.images import ImageError, ImageResolver, repository_of
from ate_env_harbor.templates import task_classes, task_environments
from ate_env_harbor.warm import (
    MAX_PROBE_TIMEOUT,
    DatasetImages,
    WarmError,
    dataset_images,
    main,
    pin_images,
    warm_dataset,
)

from .conftest import ALPINE, ECHO_TASK, _digest

OTHER = "alpine@sha256:" + "1" * 64
TAG = "alpine:3.20"
CLASS = "harbor-c1-m512"  # the echo task asks for cpus = 1, memory_mb = 512
A = (CLASS, ALPINE)
B = ("default-template", OTHER)
RUN_TB2 = Path(__file__).parents[3] / "examples" / "harbor" / "run-tb2.sh"


def make_tasks(root: Path, *, with_tag: bool = True, with_build_only: bool = True) -> Path:
    """Copies of the echo task under ``root``.

    ``echo-a`` keeps the task's class and digest; ``echo-b`` has another digest
    and no resources; ``echo-tag`` names a tag; ``build-only`` has no image.
    """
    tasks: dict[str, tuple[str | None, bool]] = {"echo-a": (ALPINE, True), "echo-b": (OTHER, False)}
    if with_tag:
        tasks["echo-tag"] = (TAG, True)
    if with_build_only:
        tasks["build-only"] = (None, True)
    for name, (image, resources) in tasks.items():
        task = root / name
        shutil.copytree(ECHO_TASK, task)
        toml = task / "task.toml"
        drop = ("docker_image",) if resources else ("docker_image", "cpus", "memory_mb")
        lines = [line for line in toml.read_text().splitlines() if not line.startswith(drop)]
        if image is not None:
            lines.insert(lines.index("[environment]") + 1, f'docker_image = "{image}"')
        toml.write_text("\n".join(lines) + "\n")
    return root


def test_dataset_images_pairs_each_image_with_its_class_template(tmp_path):
    root = make_tasks(tmp_path / "tasks")
    found = dataset_images(root)
    assert found.targets == {A: ["echo-a"], B: ["echo-b"], (CLASS, TAG): ["echo-tag"]}
    assert found.not_prebuilt == ["build-only"]
    # The same walk the registration script uses.
    assert task_classes(root) == {CLASS: (1, 512)}
    found = dataset_images(root, prefix="hb-", template="base")
    assert set(found.targets) == {("hb-c1-m512", ALPINE), ("base", OTHER), ("hb-c1-m512", TAG)}
    (tmp_path / "empty").mkdir()
    with pytest.raises(ImageError, match="no task.toml"):
        dataset_images(tmp_path / "empty")


async def test_pin_images_resolves_tags_and_merges_digests(stubs, tmp_path):
    found = DatasetImages(
        targets={(CLASS, TAG): ["t1"], A: ["t2"], ("base", "busybox:1.36"): ["t3"]}
    )
    resolver = ImageResolver(registry=None, build="none", docker=stubs.docker)
    pinned = await pin_images(found, resolver)
    assert pinned[A] == ["t2"]
    assert len(pinned) == 3 and all("@sha256:" in image for _, image in pinned)
    assert sum(c[:3] == ["buildx", "imagetools", "inspect"] for c in stubs.calls()) == 2
    # A tag and the digest it resolves to, on one base, are one target.
    pinned_tag = f"{repository_of(TAG)}@{_digest(TAG)}"
    found = DatasetImages(targets={(CLASS, TAG): ["t1"], (CLASS, pinned_tag): ["t4"]})
    assert await pin_images(found, resolver) == {(CLASS, pinned_tag): ["t1", "t4"]}
    without_docker = ImageResolver(registry=None, build="none", docker=str(tmp_path / "nope"))
    with pytest.raises(ImageError, match="install docker"):
        await pin_images(DatasetImages(targets={(CLASS, TAG): ["t1"]}), without_docker)


async def test_pin_images_turns_docker_failures_into_image_errors(stubs, tmp_path):
    # A reference never starts with "-"; as docker's last argument it would be a flag
    # (--help exits 0 with the help text), thus it is refused before docker runs.
    resolver = ImageResolver(registry=None, build="none", docker=stubs.docker)
    with pytest.raises(ImageError, match="not an image reference"):
        await pin_images(DatasetImages(targets={(CLASS, "--help"): ["t"]}), resolver)
    assert stubs.calls() == []
    # docker runs and prints no JSON, or is there and cannot run at all: both are
    # ImageErrors, which the step reports, not an ExceptionGroup out of the TaskGroup.
    chatty = tmp_path / "chatty"
    chatty.write_text("#!/bin/sh\necho hello\n")
    chatty.chmod(0o755)
    with pytest.raises(ImageError, match="unexpected digest"):
        await pin_images(
            DatasetImages(targets={(CLASS, TAG): ["t"]}),
            ImageResolver(registry=None, build="none", docker=str(chatty)),
        )
    broken = tmp_path / "broken"
    broken.write_bytes(b"\x00\x01 not an executable")
    broken.chmod(0o755)
    with pytest.raises(ImageError, match="cannot run"):
        await pin_images(
            DatasetImages(targets={(CLASS, TAG): ["t"]}),
            ImageResolver(registry=None, build="none", docker=str(broken)),
        )


def test_task_environments_names_a_task_dir_given_as_dot_and_the_file_in_errors(
    tmp_path, monkeypatch
):
    root = make_tasks(tmp_path / "tasks", with_tag=False, with_build_only=False)
    monkeypatch.chdir(root / "echo-a")
    assert [name for name, _ in task_environments(".")] == ["echo-a"]
    assert list(dataset_images(".").targets) == [A]
    bad = root / "echo-a" / "task.toml"
    bad.write_text(bad.read_text().replace("cpus = 1", 'cpus = "many"'))
    with pytest.raises(ValueError, match=r"echo-a/task\.toml: "):
        task_environments(root)
    bad.write_text("[environment\n")
    with pytest.raises(ValueError, match=r"echo-a/task\.toml: "):
        dataset_images(root)


@pytest.fixture
async def client(fake):
    client = Client(fake.addr)
    try:
        yield client
    finally:
        await client.close()


async def test_warm_dataset_ensures_waits_and_warms_each_target(fake, client):
    fake.state.golden_ready_after = 2
    seen: list[tuple[str, str, str]] = []
    report = await warm_dataset(
        client,
        {A: ["a"], B: ["b", "c"]},
        domain="pool-a",
        poll_interval=0.01,
        on_probe=lambda target, probe: seen.append((*target, probe.node)),
    )
    assert report.ok and report.domain == "pool-a" and report.selector == {"pool": "a"}
    assert sorted(fake.state.ensures) == sorted([A, B])
    assert fake.state.status_polls == {A: 2, B: 2}
    assert [w.golden for w in report.images] == ["READY", "READY"]
    digest = ALPINE.split("@sha256:")[1][:12]
    assert report.images[0].template == f"{CLASS}-{digest}"
    assert report.images[1].template == f"default-template-{OTHER.split('@sha256:')[1][:12]}"
    by_template = {c["template"]: c for c in fake.state.warm_calls}
    assert set(by_template) == {CLASS, "default-template"}
    assert by_template[CLASS]["image"] == ALPINE
    assert by_template["default-template"]["image"] == OTHER
    # The domain's selector goes on every warm; the server counts the nodes it matches.
    assert all(c["selector"] == {"pool": "a"} and c["nodes"] == 0 for c in fake.state.warm_calls)
    assert all(w.coverage is not None and w.coverage.complete for w in report.images)
    assert all(w.coverage.wanted == 2 for w in report.images)
    nodes = ["pool-a-node0", "pool-a-node1"]
    assert sorted(seen) == sorted((*t, node) for t in (A, B) for node in nodes)
    assert report.to_dict()["images"][1]["tasks"] == ["b", "c"]


async def test_a_domain_whose_labels_another_pool_carries_warms_that_pool_too(fake, client, caplog):
    # pool-a's selector is a subset of pool-b's labels: a run with
    # worker_selector=env=prod lands on both, thus the step warms all 5 nodes,
    # not the domain's 2, and says so.
    fake.state.domains = [
        {"name": "pool-a", "selector": {"env": "prod"}, "nodes": 2, "workers": 2},
        {"name": "pool-b", "selector": {"env": "prod", "gpu": "1"}, "nodes": 3, "workers": 3},
    ]
    with caplog.at_level(logging.WARNING, logger="ate_env_harbor.warm"):
        report = await warm_dataset(client, {A: ["a"]}, domain="pool-a", poll_interval=0.01)
    assert report.ok and report.selector == {"env": "prod"}
    assert report.images[0].coverage.wanted == 5 and fake.state.warm_calls[0]["nodes"] == 0
    assert "also matches pool-b" in caplog.text
    # An explicit count still bounds the warm.
    report = await warm_dataset(client, {A: ["a"]}, domain="pool-a", nodes=2, poll_interval=0.01)
    assert report.ok and report.images[0].coverage.wanted == 2
    # A pool name two worker namespaces share names two pools: refused.
    fake.state.domains = [
        {"name": "pool-a", "selector": {"pool": "a"}, "nodes": 2, "workers": 2},
        {
            "name": "pool-a",
            "selector": {"pool": "a2"},
            "nodes": 1,
            "workers": 1,
            "worker_namespace": "other",
        },
    ]
    with pytest.raises(WarmError, match=r"2 worker namespaces \(ate-system, other\)"):
        await warm_dataset(client, {A: ["a"]}, domain="pool-a")


async def test_wait_for_golden_outlives_transport_failures(fake, client):
    fake.state.golden_ready_after = 2
    fake.state.status_transient = [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    ]
    report = await warm_dataset(
        client, {A: ["a"]}, selector={"pool": "a"}, nodes=2, poll_interval=0.01
    )
    assert report.ok and report.images[0].golden == "READY"
    assert fake.state.status_transient == [] and fake.state.status_polls[A] == 2
    # Any other code is an answer: the target fails and is not warmed.
    fake.state.warm_calls.clear()
    fake.state.status_polls.clear()
    fake.state.status_transient = [grpc.StatusCode.INTERNAL]
    report = await warm_dataset(
        client, {A: ["a"]}, selector={"pool": "a"}, nodes=2, poll_interval=0.01
    )
    assert not report.ok and "RpcError" in report.images[0].error
    assert fake.state.warm_calls == []


async def test_timeouts_must_be_finite_and_positive(fake, client):
    for kwargs in (
        {"golden_timeout": math.nan},
        {"golden_timeout": 0},
        {"poll_interval": math.inf},
        {"poll_interval": -1},
        {"probe_timeout": math.nan},
        {"probe_timeout": math.inf},
    ):
        with pytest.raises(ValueError, match="must be positive"):
            await warm_dataset(client, {A: ["a"]}, **kwargs)
    assert fake.state.ensures == []


# What WarmImage refuses (internal/apiservice/placement.go) or the request cannot carry.
OUT_OF_BOUNDS = (
    ({"probes_per_node": 9}, "between 0 and 8"),
    ({"probes_per_node": -1}, "between 0 and 8"),
    ({"max_rounds": 11}, "between 0 and 10"),
    ({"max_rounds": -1}, "between 0 and 10"),
    ({"nodes": 257}, "between 0 and 256"),
    ({"nodes": -1}, "between 0 and 256"),
    ({"nodes": 3_000_000_000}, "between 0 and 256"),  # past int32: not serializable
    ({"nodes": 129, "probes_per_node": 2}, "exceed the 256 probes"),
    ({"nodes": 33, "probes_per_node": 8}, "exceed the 256 probes"),
    ({"probe_timeout": 1e20}, "seconds"),  # past timedelta: OverflowError in the client
    ({"probe_timeout": MAX_PROBE_TIMEOUT + 1e6}, "seconds"),
    ({"probe_timeout": 1e-7}, "seconds"),  # 0s as a timedelta, which the server refuses
)


async def test_warm_options_out_of_the_wire_bounds_are_refused_before_any_ensure(fake, client):
    not_integers = (
        ({"nodes": True}, "integer"),
        ({"probes_per_node": True}, "integer"),
        ({"max_rounds": False}, "integer"),
        ({"nodes": 2.0}, "integer"),
        ({"probe_timeout": True}, "number of seconds"),
    )
    for kwargs, message in (*OUT_OF_BOUNDS, *not_integers):
        with pytest.raises(ValueError, match=message):
            await warm_dataset(client, {A: ["a"]}, selector={"pool": "b"}, **kwargs)
    assert fake.state.ensures == [] and fake.state.warm_calls == []
    # The bounds themselves pass, to a pool of 3 nodes: short, not refused.
    report = await warm_dataset(
        client,
        {A: ["a"]},
        selector={"pool": "b"},
        nodes=32,
        probes_per_node=8,
        max_rounds=10,
        probe_timeout=1e-6,
        poll_interval=0.01,
    )
    assert report.images[0].coverage.wanted == 32
    report = await warm_dataset(
        client, {A: ["a"]}, selector={"pool": "b"}, nodes=256, probes_per_node=1, poll_interval=0.01
    )
    assert report.images[0].coverage.wanted == 256
    # With probes_per_node 0 the default is the server's, and so is the refusal: per target.
    report = await warm_dataset(
        client, {A: ["a"]}, selector={"pool": "b"}, nodes=256, poll_interval=0.01
    )
    assert "256 nodes x 2 probes per node exceed" in report.images[0].error


async def test_the_fake_refuses_what_warm_image_refuses(fake, client):
    """The fake's ``WarmImage`` against ``parseWarmOptions`` and ``domainNodes`` of ate-env-api.

    ``warm_dataset`` refuses these before a call, thus the client is called
    directly: what the fake answers is what a test of another caller sees.
    """
    for kwargs, message in (
        ({"nodes": -1}, "nodes must be >= 0"),
        ({"probes_per_node": 9}, "probes_per_node must be between 1 and 8"),
        ({"probes_per_node": -1}, "probes_per_node must be between 1 and 8"),
        ({"max_rounds": 11}, "max_rounds must be between 1 and 10"),
        ({"max_rounds": -1}, "max_rounds must be between 1 and 10"),
        ({"probe_timeout": 0}, "probe_timeout must be positive"),
        ({"probe_timeout": 1e-7}, "probe_timeout must be positive"),  # 0s on the wire
        ({"nodes": 129}, "129 nodes x 2 probes per node exceed the 256 probes"),  # the default 2
        ({"nodes": 33, "probes_per_node": 8}, "33 nodes x 8 probes per node exceed the 256"),
    ):
        with pytest.raises(InvalidArgumentError, match=message):
            await client.warm_image(ALPINE, worker_selector={"pool": "b"}, **kwargs)
    # The limits themselves pass.
    coverage = await client.warm_image(
        ALPINE, worker_selector={"pool": "b"}, nodes=32, probes_per_node=8, max_rounds=10
    )
    assert coverage.wanted == 32
    assert (await client.warm_image(ALPINE, worker_selector={"pool": "b"}, nodes=128)).wanted == 128
    # The options are parsed before the template is derived.
    fake.state.missing_templates.add("nope")
    with pytest.raises(InvalidArgumentError, match="max_rounds"):
        await client.warm_image(ALPINE, template_name="nope", max_rounds=11)
    # nodes 0 counts the nodes the selector matches: none is not a complete warm of 0 nodes.
    fake.state.domains.append(
        {"name": "pool-c", "selector": {"pool": "c"}, "nodes": 0, "workers": 0}
    )
    with pytest.raises(FailedPreconditionError, match="no active worker matches the selector"):
        await client.warm_image(ALPINE, worker_selector={"pool": "c"})
    with pytest.raises(FailedPreconditionError, match="no active worker matches the selector"):
        await client.warm_image(ALPINE, worker_selector={"pool": "z"})
    # The count is checked against the round's width too: 129 nodes x the default 2.
    fake.state.domains.append(
        {"name": "big", "selector": {"pool": "big"}, "nodes": 129, "workers": 129}
    )
    with pytest.raises(InvalidArgumentError, match="129 nodes x 2 probes per node"):
        await client.warm_image(ALPINE, worker_selector={"pool": "big"})
    assert (
        await client.warm_image(ALPINE, worker_selector={"pool": "big"}, probes_per_node=1)
    ).wanted == 129
    # Through the step: a domain without nodes fails its targets, it does not pass with 0/0.
    report = await warm_dataset(client, {A: ["a"]}, domain="pool-c", poll_interval=0.01)
    assert not report.ok and "no active worker" in report.images[0].error


async def test_warm_dataset_reports_a_failed_golden_and_a_short_warm(fake, client):
    fake.state.golden_failures[ALPINE] = "Bake: no space left on device"
    fake.state.warm_short.add(OTHER)
    report = await warm_dataset(
        client, {A: ["a"], B: ["b"]}, selector={"pool": "b"}, nodes=3, poll_interval=0.01
    )
    assert not report.ok
    a, b = report.images
    # The ensure answered PENDING (a recreate), the poll FAILED; the nodes are warmed anyway.
    assert fake.state.status_polls[A] >= 1
    assert a.golden == "FAILED" and a.error.startswith("golden snapshot failed: Bake: no space")
    assert a.coverage is not None and a.coverage.complete
    assert b.golden == "READY" and b.coverage is not None and not b.coverage.complete
    assert b.error.startswith("2 of 3 nodes warmed: probe did not start")
    assert sorted(c["image"] for c in fake.state.warm_calls) == sorted([ALPINE, OTHER])


async def test_warm_dataset_times_out_on_a_pending_golden(fake, client):
    fake.state.golden_ready_after = 10**6
    report = await warm_dataset(client, {A: ["a"]}, golden_timeout=0.2, poll_interval=0.02)
    assert not report.ok
    assert report.images[0].golden == "TIMEOUT" and "PENDING" in report.images[0].error
    assert fake.state.warm_calls == []


async def test_disabled_goldens_warm_the_templates_own_pools(fake, client):
    fake.state.goldens_disabled = True
    report = await warm_dataset(client, {A: ["a"]}, poll_interval=0.01)
    assert report.ok and report.images[0].golden == "NONE"
    assert fake.state.warm_calls[0]["selector"] == {}
    assert report.images[0].coverage.wanted == 5  # both pools' nodes


async def test_undeleted_probes_fail_the_report(fake, client):
    fake.state.warm_undeleted.add(ALPINE)
    report = await warm_dataset(
        client, {A: ["a"]}, selector={"pool": "b"}, nodes=3, poll_interval=0.01
    )
    assert not report.ok
    assert report.undeleted_probes == list(report.images[0].coverage.undeleted_probes)
    assert "probe environment(s) not deleted" in report.images[0].error


async def test_without_the_viewer_role_the_node_count_is_required(fake, client):
    fake.state.deny_list_workers = True
    with pytest.raises(WarmError, match="viewer role"):
        await warm_dataset(client, {A: ["a"]}, domain="pool-a")
    # Counting the nodes takes the same role: refused before a template is ensured.
    with pytest.raises(WarmError, match="viewer role"):
        await warm_dataset(client, {A: ["a"]}, selector={"pool": "a"}, poll_interval=0.01)
    assert fake.state.ensures == []
    report = await warm_dataset(
        client, {A: ["a"]}, selector={"pool": "a"}, nodes=2, poll_interval=0.01
    )
    assert report.ok


async def test_errors_name_what_exists(fake, client):
    with pytest.raises(WarmError, match="pool-a, pool-b"):
        await warm_dataset(client, {A: ["a"]}, domain="pool-z")
    with pytest.raises(ValueError, match="not both"):
        await warm_dataset(client, {A: ["a"]}, domain="pool-a", selector={"pool": "a"})
    fake.state.missing_templates.add(CLASS)
    report = await warm_dataset(
        client, {A: ["a"], B: ["b"]}, not_prebuilt=["build-only"], poll_interval=0.01
    )
    assert not report.ok and report.to_dict()["not_prebuilt"] == ["build-only"]
    assert "FailedPreconditionError" in report.images[0].error
    assert "register it" in report.images[0].error
    assert report.images[1].ok  # the other target went on
    assert [c["image"] for c in fake.state.warm_calls] == [OTHER]


async def test_cli_runs_against_the_fake(fake, tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("SUBSTRATE_ENV_API", raising=False)
    monkeypatch.setenv("SUBSTRATE_ENV_AUTH_TOKEN", "tok")
    fake.state.required_token = "tok"
    tasks = make_tasks(tmp_path / "tasks", with_tag=False, with_build_only=False)
    out = tmp_path / "reports" / "report.json"  # the directory is created
    argv = [str(tasks), "--api", fake.addr, "--poll-interval", "0.01", "--json", str(out)]
    code = await asyncio.to_thread(main, [*argv, "--domain", "pool-a"])
    assert code == 0
    report = json.loads(out.read_text())
    assert report["ok"] and report["selector"] == {"pool": "a"}
    assert [(w["base_template"], w["tasks"]) for w in report["images"]] == [
        (CLASS, ["echo-a"]),
        ("default-template", ["echo-b"]),
    ]
    printed = capsys.readouterr().out
    assert f"{ALPINE} on {CLASS}:" in printed and "golden READY, 2/2 nodes" in printed
    assert "--ek worker_selector=" in printed
    # --selector, --template, --template-prefix and --atespace reach the calls.
    fake.state.warm_calls.clear()
    code = await asyncio.to_thread(
        main,
        [
            *argv,
            "--selector",
            "pool=b",
            "--nodes",
            "3",
            "--template",
            "t",
            "--template-prefix",
            "hb-",
            "--atespace",
            "a",
            "--probe-timeout",
            "30",
        ],
    )
    assert code == 0
    calls = {c["template"]: c for c in fake.state.warm_calls}
    assert set(calls) == {"hb-c1-m512", "t"}
    assert all(c["selector"] == {"pool": "b"} and c["atespace"] == "a" for c in calls.values())
    assert all(c["nodes"] == 3 and c["probe_timeout"] == 30.0 for c in calls.values())
    capsys.readouterr()
    # Probes left behind: one ate-env delete per probe, each its own name, with the
    # step's endpoint and atespace, joined so the line pastes into a shell.
    fake.state.warm_undeleted.update({ALPINE, OTHER})
    code = await asyncio.to_thread(
        main, [*argv, "--selector", "pool=b", "--nodes", "3", "--atespace", "a"]
    )
    assert code == 1
    line = next(
        line for line in capsys.readouterr().out.splitlines() if line.startswith("probe environ")
    )
    deletes = line.split(": ", 1)[1].split("; ")
    assert len(deletes) == 2 and len(set(deletes)) == 2
    prefix = f"ate-env delete --api {fake.addr} --atespace a warm-fake"
    assert all(d.startswith(prefix) for d in deletes)
    fake.state.warm_undeleted.clear()
    # Without the viewer role, counting the nodes is a usage error, before any bake.
    fake.state.deny_list_workers = True
    fake.state.ensures.clear()
    code = await asyncio.to_thread(main, [*argv, "--selector", "pool=b"])
    assert code == 2 and "viewer role" in capsys.readouterr().err
    assert fake.state.ensures == []
    fake.state.deny_list_workers = False
    # A task without a docker_image makes the step fail, and says what to do.
    tasks = make_tasks(tmp_path / "tasks2", with_tag=False)
    code = await asyncio.to_thread(
        main, [str(tasks), "--api", fake.addr, "--poll-interval", "0.01"]
    )
    assert code == 1
    assert "upload-images --update-config" in capsys.readouterr().out
    # Usage errors: a tag with no docker to resolve it, an unknown domain, a bad value.
    tasks = make_tasks(tmp_path / "tasks3")
    code = await asyncio.to_thread(
        main, [str(tasks), "--api", fake.addr, "--docker", str(tmp_path / "nope")]
    )
    assert code == 2 and "install docker" in capsys.readouterr().err
    code = await asyncio.to_thread(main, [*argv, "--domain", "pool-z"])
    assert code == 2 and "pool-a, pool-b" in capsys.readouterr().err
    for flag, value in (
        ("--probe-timeout", "0"),
        ("--probe-timeout", "inf"),
        ("--golden-timeout", "nan"),
        ("--poll-interval", "nan"),
    ):
        with pytest.raises(SystemExit):
            await asyncio.to_thread(main, [*argv, flag, value])
        assert "must be positive" in capsys.readouterr().err
    # WarmImage's limits are usage errors too, named by flag, before any ensure.
    fake.state.ensures.clear()
    for kwargs, message in OUT_OF_BOUNDS:
        flags = [
            f"{flag}={value}"  # one word: a negative value is not read as a flag
            for name, value in kwargs.items()
            for flag in ["--" + name.replace("_", "-")]
        ]
        with pytest.raises(SystemExit) as exit_:
            await asyncio.to_thread(main, [*argv, "--selector", "pool=b", *flags])
        err = capsys.readouterr().err
        assert exit_.value.code == 2 and flags[0].split("=")[0] in err and message in err
    assert fake.state.ensures == []
    # A report that cannot be written is an error, not a traceback.
    code = await asyncio.to_thread(main, [*argv, "--domain", "pool-a", "--json", str(tmp_path)])
    assert code == 2 and "error: " in capsys.readouterr().err


async def test_run_tb2_warm_leaves_out_its_selector_when_the_caller_names_the_pool(fake, tmp_path):
    """``run-tb2.sh warm`` with ``HB_WORKER_SELECTOR`` set: a ``--domain`` or ``--selector`` wins.

    The two flags exclude each other, thus the script's ``--selector`` next to
    the caller's ``--domain`` was a usage error.
    """
    tasks = make_tasks(tmp_path / "tasks", with_tag=False, with_build_only=False)
    env = {
        **os.environ,
        "PYTHON": sys.executable,
        "SUBSTRATE_ENV_API": fake.addr,
        "HB_WORKER_SELECTOR": "pool=a",
    }
    env.pop("SUBSTRATE_ENV_AUTH_TOKEN", None)

    async def warm(*flags: str) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            "bash",
            str(RUN_TB2),
            "warm",
            str(tasks),
            "--poll-interval",
            "0.01",
            *flags,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        fake.state.warm_calls.clear()
        return proc.returncode, out.decode(), err.decode()

    # Without either flag the variable is the selector.
    code, out, err = await warm()
    assert code == 0, err
    assert "selector pool=a;" in out and "2/2 nodes" in out
    # Every spelling argparse takes for the two flags, a prefix included.
    for flags in (
        ("--domain", "pool-b"),
        ("--domain=pool-b",),
        ("--dom", "pool-b"),
        ("--selector", "pool=b"),
        ("--selector=pool=b",),
        ("--sel=pool=b",),
    ):
        code, out, err = await warm(*flags)
        assert code == 0, (flags, err)
        assert "selector pool=b;" in out and "3/3 nodes" in out, (flags, out)

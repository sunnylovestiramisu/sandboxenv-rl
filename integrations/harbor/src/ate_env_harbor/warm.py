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

"""Warm a dataset's images before ``harbor run``.

The first trial of an image on a node pulls the image there, and the first
trial of an image on a base template creates the derived template and starts
the golden snapshot later environments restore from; an environment created
before the snapshot is ready boots from the image. Node caches and templates
outlive the process, thus a step before the run pays both once.

The unit of work is a (base template, image) pair: the provider derives from
the class template a task's resources select (``<prefix>c<cpus>-m<memory_mb>``,
else the default template), thus one image used by tasks of two classes has
two derived templates. ``warm_dataset`` ensures every pair's derived template
(``EnsureImageTemplate``) so the bakes start together, then, ``parallel``
pairs at a time, waits until the golden snapshot is READY and puts the image
on every node of the job's placement domain (``WarmImage``).

``python -m ate_env_harbor.warm <tasks-dir>`` is the command form;
``examples/harbor/run-tb2.sh warm`` wraps it.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import grpc
from ate_env_rl import (
    Client,
    EnvError,
    GoldenSnapshotState,
    ImageStatus,
    PermissionDeniedError,
    PlacementDomain,
    RpcError,
    WarmImageCoverage,
    WarmProbe,
)

from ate_env_harbor.environment import (
    API_ENV,
    DEFAULT_API_URL,
    DEFAULT_ATESPACE,
    DEFAULT_TEMPLATE,
    TOKEN_ENV,
    parse_worker_selector,
)
from ate_env_harbor.images import ImageError, ImageResolver, is_digest_ref
from ate_env_harbor.templates import (
    DEFAULT_PREFIX,
    class_template_name,
    task_environments,
)

DEFAULT_GOLDEN_TIMEOUT = 1800.0
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_PARALLEL = 4
_ENSURE_PARALLEL = 8
# WarmImage's limits, as ate-env-api's parseWarmOptions and WarmImage have them
# (internal/apiservice/placement.go: MaxWarmProbesPerNode, MaxWarmRounds,
# MaxWarmProbes). A value past one is refused by the server, or cannot be put
# in the request at all; the step checks them before it bakes anything.
MAX_PROBES_PER_NODE = 8
MAX_ROUNDS = 10
MAX_PROBES = 256  # nodes x probes_per_node, what one round starts
# probe_timeout travels as a protobuf Duration, written from a timedelta:
# below a microsecond it is 0 on the wire, which the server refuses; the upper
# end is Duration's (10000 years).
MIN_PROBE_TIMEOUT = 1e-6
MAX_PROBE_TIMEOUT = 315_576_000_000.0
# The codes a golden poll outlives: the transport, not an answer. A 30 minute
# bake sees an api server restart; the poll goes on until its deadline.
_TRANSIENT_CODES = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})
_VIEWER_ROLE_HINT = (
    "takes the global viewer role (ate-env manifest access-policy --global); "
    "without it pass the node count and the selector"
)

Target = tuple[str, str]
"""A (base template, image reference) pair: what one derived template comes from."""

_logger = logging.getLogger(__name__)


class WarmError(Exception):
    """The server answered, but not with something the step can go on from."""


@dataclass
class DatasetImages:
    """What a tasks directory asks to warm: each target to the tasks that use it."""

    targets: dict[Target, list[str]] = field(default_factory=dict)
    # Tasks without a docker_image build from environment/Dockerfile at trial
    # time; there is nothing to warm for them until the dataset is prebuilt.
    not_prebuilt: list[str] = field(default_factory=list)


def dataset_images(
    root: str | Path, *, prefix: str = DEFAULT_PREFIX, template: str = DEFAULT_TEMPLATE
) -> DatasetImages:
    """The images the tasks under ``root`` name, each with the base template its task selects.

    The base is the class template the provider picks under its default
    policy, ``<prefix>c<cpus>-m<memory_mb>`` from the task's resources, else
    ``template``. A run with ``--override-cpus``, ``--override-memory-mb`` or
    ``--cpus ignore`` picks other bases and derives again at its first trial.
    """
    found = DatasetImages()
    for task, env in task_environments(root):
        if not env.docker_image:
            found.not_prebuilt.append(task)
            continue
        base = class_template_name(prefix, env.cpus, env.memory_mb) or template
        found.targets.setdefault((base, env.docker_image), []).append(task)
    if not found.targets and not found.not_prebuilt:
        raise ImageError(f"no task.toml under {root}")
    return found


async def pin_images(found: DatasetImages, resolver: ImageResolver) -> dict[Target, list[str]]:
    """``found.targets`` with every image by digest: a tag goes through ``resolver.resolve_tag``.

    Two references of one digest on one base merge into one target.
    """
    semaphore = asyncio.Semaphore(_ENSURE_PARALLEL)

    async def pin(ref: str) -> str:
        if is_digest_ref(ref):
            return ref
        async with semaphore:
            return await resolver.resolve_tag(ref)

    refs = sorted({image for _, image in found.targets})
    try:
        async with asyncio.TaskGroup() as group:
            tasks = {ref: group.create_task(pin(ref)) for ref in refs}
    except* ImageError as errors:
        raise errors.exceptions[0] from None
    pinned: dict[Target, list[str]] = {}
    for (base, image), names in found.targets.items():
        pinned.setdefault((base, tasks[image].result()), []).extend(names)
    return pinned


@dataclass
class ImageWarmth:
    """What the warm step did for one target."""

    image: str
    base_template: str
    tasks: list[str]
    template: str = ""  # the derived template
    golden: str = ""  # the GoldenSnapshotState name, or TIMEOUT
    golden_error: str = ""
    coverage: WarmImageCoverage | None = None
    error: str = ""

    @property
    def undeleted_probes(self) -> tuple[str, ...]:
        return self.coverage.undeleted_probes if self.coverage is not None else ()

    @property
    def ok(self) -> bool:
        return not self.error and self.coverage is not None and self.coverage.complete

    def to_dict(self) -> dict[str, Any]:
        coverage = None
        if self.coverage is not None:
            coverage = {
                "nodes": list(self.coverage.nodes),
                "wanted": self.coverage.wanted,
                "probes": self.coverage.probes,
                "failed": self.coverage.failed,
                "rounds": self.coverage.rounds,
                "complete": self.coverage.complete,
                "error": self.coverage.error,
                "undeleted_probes": list(self.coverage.undeleted_probes),
                "waited": self.coverage.waited,
            }
        return {
            "image": self.image,
            "base_template": self.base_template,
            "tasks": self.tasks,
            "template": self.template,
            "golden": self.golden,
            "golden_error": self.golden_error,
            "coverage": coverage,
            "error": self.error,
            "ok": self.ok,
        }


@dataclass
class WarmReport:
    """The warm step's result for a dataset."""

    domain: str | None
    selector: dict[str, str]
    images: list[ImageWarmth]
    not_prebuilt: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.not_prebuilt and all(w.ok for w in self.images)

    @property
    def undeleted_probes(self) -> list[str]:
        return [probe for w in self.images for probe in w.undeleted_probes]

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "selector": self.selector,
            "images": [w.to_dict() for w in self.images],
            "not_prebuilt": list(self.not_prebuilt),
            "ok": self.ok,
        }


async def list_domains(client: Client) -> list[PlacementDomain]:
    """``ListPlacementDomains``, every pool; a missing viewer role names what to pass instead."""
    try:
        return await client.list_placement_domains()
    except PermissionDeniedError as e:
        raise WarmError(f"listing the pools {_VIEWER_ROLE_HINT}: {e}") from e


async def select_domain(client: Client, domain: str) -> PlacementDomain:
    """The placement domain named ``domain``.

    A name two worker namespaces share is refused: the step cannot tell the
    pools apart, and the selector is what matters. A pool whose labels carry
    the chosen selector too is logged: the server counts and warms its nodes
    as well, since the run's environments can land there.
    """
    domains = await list_domains(client)
    matches = [d for d in domains if d.name == domain]
    if not matches:
        names = ", ".join(d.name for d in domains) or "none"
        raise WarmError(f"placement domain {domain!r} not found; domains: {names}")
    if len(matches) > 1:
        spaces = ", ".join(d.worker_namespace or "?" for d in matches)
        raise WarmError(
            f"placement domain {domain!r} is a pool in {len(matches)} worker namespaces "
            f"({spaces}); pass its labels as the selector instead"
        )
    found = matches[0]
    if not found.selector:
        raise WarmError(
            f"placement domain {domain!r} has no selector label; no worker selector addresses it"
        )
    also = [
        d.name
        for d in domains
        if d is not found and all(d.selector.get(k) == v for k, v in found.selector.items())
    ]
    if also:
        _logger.warning(
            "placement domain %s: its selector %s also matches %s; the nodes of both are "
            "warmed, as a run with the selector lands on both",
            domain,
            ",".join(f"{k}={v}" for k, v in found.selector.items()),
            ", ".join(also),
        )
    return found


def _positive(value: float | None) -> bool:
    """A finite number above 0; NaN compares False with everything, thus the explicit check."""
    return value is not None and math.isfinite(value) and value > 0


def check_warm_options(
    *,
    nodes: int = 0,
    probes_per_node: int = 0,
    max_rounds: int = 0,
    probe_timeout: float | None = None,
    label: Callable[[str], str] = str,
) -> None:
    """Refuse, with ``ValueError``, what ``WarmImage`` would refuse or the request cannot carry.

    0 leaves a knob to the server. ``nodes`` is at most ``MAX_PROBES``, since
    a node gets one probe or more; with ``probes_per_node`` given the product
    is checked. With ``probes_per_node`` 0 the server's default applies, and
    with ``nodes`` 0 the server's count: those products are the server's to
    refuse. ``label`` turns a parameter's name into what the caller typed.
    """
    limits = (
        ("nodes", nodes, MAX_PROBES),
        ("probes_per_node", probes_per_node, MAX_PROBES_PER_NODE),
        ("max_rounds", max_rounds, MAX_ROUNDS),
    )
    for name, value, most in limits:
        # bool is an int to Python and not to protobuf.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{label(name)} must be an integer, got {value!r}")
        if not 0 <= value <= most:
            raise ValueError(f"{label(name)} must be between 0 and {most}, got {value!r}")
    if probes_per_node and nodes * probes_per_node > MAX_PROBES:
        raise ValueError(
            f"{label('nodes')} {nodes} x {label('probes_per_node')} {probes_per_node} exceed "
            f"the {MAX_PROBES} probes a round may start"
        )
    if probe_timeout is None:
        return
    name = label("probe_timeout")
    if isinstance(probe_timeout, bool) or not isinstance(probe_timeout, (int, float)):
        raise ValueError(f"{name} must be a number of seconds, got {probe_timeout!r}")
    if not _positive(probe_timeout):
        raise ValueError(f"{name} must be positive, got {probe_timeout!r}")
    if not MIN_PROBE_TIMEOUT <= probe_timeout <= MAX_PROBE_TIMEOUT:
        raise ValueError(
            f"{name} must be between {MIN_PROBE_TIMEOUT:g} and {MAX_PROBE_TIMEOUT:g} seconds, "
            f"got {probe_timeout!r}"
        )


def _golden_settled(status: ImageStatus) -> bool:
    if status.golden in (GoldenSnapshotState.READY, GoldenSnapshotState.FAILED):
        return True
    # NONE with the template: it bakes no golden snapshot; nothing to wait for.
    return status.golden == GoldenSnapshotState.NONE and status.template_exists


async def wait_for_golden(
    client: Client,
    image: str,
    *,
    first: ImageStatus,
    atespace: str,
    template_name: str,
    timeout: float,
    poll_interval: float,
) -> ImageStatus:
    """From ``first`` (the ensure's answer), poll until the golden is READY, FAILED or disabled."""
    status = first
    deadline = time.monotonic() + timeout
    while not _golden_settled(status):
        if status.golden == GoldenSnapshotState.NONE and not status.template_exists:
            raise WarmError(f"derived template {status.template.name} was not created")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"golden snapshot of {status.template.name} still {status.golden.name} "
                f"after {timeout:g}s"
            )
        await asyncio.sleep(min(poll_interval, remaining))
        try:
            status = await client.image_status(
                image, atespace=atespace, template_name=template_name
            )
        except RpcError as e:
            if e.code not in _TRANSIENT_CODES:
                raise
            _logger.warning("%s: image status failed, polling again: %s", status.template.name, e)
    return status


async def warm_dataset(
    client: Client,
    targets: Mapping[Target, Sequence[str]],
    *,
    atespace: str = DEFAULT_ATESPACE,
    domain: str | None = None,
    selector: Mapping[str, str] | None = None,
    nodes: int = 0,
    probes_per_node: int = 0,
    max_rounds: int = 0,
    probe_timeout: float | None = None,
    golden_timeout: float = DEFAULT_GOLDEN_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    parallel: int = DEFAULT_PARALLEL,
    not_prebuilt: Sequence[str] = (),
    on_probe: Callable[[Target, WarmProbe], None] | None = None,
) -> WarmReport:
    """Ensure, wait for the golden and warm every target ((base template, digest) to tasks).

    ``domain`` names a placement domain (``ListPlacementDomains``, which takes
    the global viewer role) and its selector is used; ``selector`` gives the
    labels directly; neither warms the base templates' own pools. ``nodes`` 0
    makes the server count the nodes the selector matches, which takes the
    global viewer role too and is checked before anything is ensured, as are
    ``WarmImage``'s limits on the knobs (``check_warm_options``); the
    count is not taken from the domain, because a pool whose workers carry
    the same labels is matched as well, and the run lands there too. Every
    target is ensured first, so the bakes start together; then ``parallel``
    targets at a time wait for their golden and warm. A target's failure (a
    FAILED or late golden, an API error, a short coverage, probe environments
    the server could not delete) is recorded in its ``ImageWarmth`` and the
    others go on; ``WarmReport.ok`` sums it up. A FAILED golden is still
    warmed: the environments boot from the image, and the nodes then have it.
    """
    if domain and selector:
        raise ValueError("pass a domain name or a selector, not both")
    for name, value in (("golden_timeout", golden_timeout), ("poll_interval", poll_interval)):
        if not _positive(value):
            raise ValueError(f"{name} must be positive, got {value!r}")
    check_warm_options(
        nodes=nodes,
        probes_per_node=probes_per_node,
        max_rounds=max_rounds,
        probe_timeout=probe_timeout,
    )
    chosen: dict[str, str] = dict(selector or {})
    if domain:
        chosen = dict((await select_domain(client, domain)).selector)
    elif nodes == 0:
        # The same permission WarmImage needs to count the nodes: found out
        # now, as a usage error, not per target after the goldens baked.
        await list_domains(client)
    results = {
        target: ImageWarmth(image=target[1], base_template=target[0], tasks=list(tasks))
        for target, tasks in targets.items()
    }
    first: dict[Target, ImageStatus] = {}
    ensure_slots = asyncio.Semaphore(_ENSURE_PARALLEL)
    slots = asyncio.Semaphore(max(1, parallel))

    async def ensure(target: Target) -> None:
        base, image = target
        try:
            async with ensure_slots:
                first[target] = await client.image_status(
                    image, ensure=True, atespace=atespace, template_name=base
                )
        except EnvError as e:
            results[target].error = f"{type(e).__name__}: {e}"

    async def finish(target: Target) -> None:
        base, image = target
        result = results[target]
        async with slots:
            try:
                status = await wait_for_golden(
                    client,
                    image,
                    first=first[target],
                    atespace=atespace,
                    template_name=base,
                    timeout=golden_timeout,
                    poll_interval=poll_interval,
                )
                result.template = status.template.name
                result.golden = status.golden.name
                result.golden_error = status.golden_error
                problems: list[str] = []
                if status.golden == GoldenSnapshotState.FAILED:
                    problems.append(f"golden snapshot failed: {status.golden_error}")
                coverage = await client.warm_image(
                    image,
                    worker_selector=chosen or None,
                    nodes=nodes,
                    probes_per_node=probes_per_node,
                    max_rounds=max_rounds,
                    probe_timeout=probe_timeout,
                    atespace=atespace,
                    template_name=base,
                    on_probe=(lambda probe: on_probe(target, probe)) if on_probe else None,
                )
                result.coverage = coverage
                if not coverage.complete:
                    short = f"{len(coverage.nodes)} of {coverage.wanted} nodes warmed"
                    problems.append(f"{short}: {coverage.error}" if coverage.error else short)
                if coverage.undeleted_probes:
                    problems.append(
                        f"{len(coverage.undeleted_probes)} probe environment(s) not deleted: "
                        + ", ".join(coverage.undeleted_probes)
                    )
                result.error = "; ".join(problems)
            except TimeoutError as e:
                result.golden = "TIMEOUT"
                result.error = str(e)
            except WarmError as e:
                result.error = str(e)
            except EnvError as e:
                result.error = f"{type(e).__name__}: {e}"

    async with asyncio.TaskGroup() as group:
        for target in targets:
            group.create_task(ensure(target))
    async with asyncio.TaskGroup() as group:
        for target in targets:
            if target in first:
                group.create_task(finish(target))
    return WarmReport(
        domain=domain,
        selector=chosen,
        images=[results[target] for target in targets],
        not_prebuilt=list(not_prebuilt),
    )


def _line(warmth: ImageWarmth) -> str:
    covered = ""
    if warmth.coverage is not None:
        covered = f", {len(warmth.coverage.nodes)}/{warmth.coverage.wanted} nodes"
    tail = f" -- {warmth.error}" if warmth.error else ""
    return (
        f"{warmth.image} on {warmth.base_template}: {len(warmth.tasks)} task(s), "
        f"template {warmth.template or '?'}, golden {warmth.golden or '?'}{covered}{tail}"
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m ate_env_harbor.warm",
        description="Ensure the derived templates of a dataset's images, wait for their golden "
        "snapshots, and put the images on every node of a placement domain.",
    )
    parser.add_argument("tasks_dir", type=Path, help="the Harbor tasks directory")
    parser.add_argument(
        "--api",
        default=None,
        help=f"ate-env-api endpoint; default ${API_ENV}, else {DEFAULT_API_URL}",
    )
    parser.add_argument("--atespace", default=DEFAULT_ATESPACE)
    parser.add_argument(
        "--template",
        default=DEFAULT_TEMPLATE,
        help="the base template for tasks without resources (default: %(default)s)",
    )
    parser.add_argument(
        "--template-prefix",
        default=DEFAULT_PREFIX,
        help="the class templates' prefix, as the provider's template_prefix "
        "(default: %(default)s)",
    )
    where = parser.add_mutually_exclusive_group()
    where.add_argument("--domain", help="the placement domain (worker pool) to warm, by name")
    where.add_argument(
        "--selector",
        help="the domain's worker selector, key=value pairs separated by commas; "
        "default: the templates' own pools",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=0,
        help=f"nodes to cover, at most {MAX_PROBES}; 0 has the server count the nodes the "
        "selector matches (global viewer role)",
    )
    parser.add_argument("--probes-per-node",
        type=int,
        default=0,
        help=f"at most {MAX_PROBES_PER_NODE}, and {MAX_PROBES} probes over the nodes; "
        "server default: 2",
    )
    parser.add_argument(
        "--max-rounds", type=int, default=0, help=f"at most {MAX_ROUNDS}; server default: 3"
    )
    parser.add_argument(
        "--probe-timeout",
        type=float,
        default=None,
        help="seconds per probe; server default: 600",
    )
    parser.add_argument(
        "--golden-timeout",
        type=float,
        default=DEFAULT_GOLDEN_TIMEOUT,
        help="seconds to wait for a golden snapshot (default: %(default)s)",
    )
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument(
        "--parallel", type=int, default=DEFAULT_PARALLEL, help="targets warmed at once"
    )
    parser.add_argument("--docker", default="docker", help="the docker executable, to resolve tags")
    parser.add_argument("--json", type=Path, default=None, help="write the report here")
    args = parser.parse_args(argv)
    for name in ("golden_timeout", "poll_interval"):
        if not _positive(getattr(args, name)):
            parser.error(f"--{name.replace('_', '-')} must be positive, got {getattr(args, name)!r}")
    try:
        check_warm_options(
            nodes=args.nodes,
            probes_per_node=args.probes_per_node,
            max_rounds=args.max_rounds,
            probe_timeout=args.probe_timeout,
            label=lambda name: f"--{name.replace('_', '-')}",
        )
    except ValueError as e:
        parser.error(str(e))
    if args.parallel < 1:
        parser.error("--parallel must be 1 or more")
    return args


def _log_probe(target: Target, probe: WarmProbe) -> None:
    base, image = target
    failed = f" failed: {probe.error}" if probe.error else ""
    _logger.info("%s on %s: probe %s on %s%s", image, base, probe.id, probe.node or "?", failed)


async def _run(args: argparse.Namespace) -> int:
    found = dataset_images(args.tasks_dir, prefix=args.template_prefix, template=args.template)
    resolver = ImageResolver(registry=None, build="none", docker=args.docker, logger=_logger)
    targets = await pin_images(found, resolver)
    selector = parse_worker_selector(args.selector) if args.selector else None
    api = args.api or os.environ.get(API_ENV) or DEFAULT_API_URL
    client = Client(api, auth_token=os.environ.get(TOKEN_ENV) or None)
    try:
        report = await warm_dataset(
            client,
            targets,
            atespace=args.atespace,
            domain=args.domain,
            selector=selector,
            nodes=args.nodes,
            probes_per_node=args.probes_per_node,
            max_rounds=args.max_rounds,
            probe_timeout=args.probe_timeout,
            golden_timeout=args.golden_timeout,
            poll_interval=args.poll_interval,
            parallel=args.parallel,
            not_prebuilt=found.not_prebuilt,
            on_probe=_log_probe,
        )
    finally:
        await client.close()
    for warmth in report.images:
        print(_line(warmth))
    if report.not_prebuilt:
        print(
            f"{len(report.not_prebuilt)} task(s) without a docker_image, nothing to warm until "
            "the dataset is prebuilt (harbor admin upload-images --update-config): "
            + ", ".join(report.not_prebuilt)
        )
    if report.undeleted_probes:
        print(
            "probe environments left behind, delete them: "
            + "; ".join(_delete_command(args, probe) for probe in report.undeleted_probes)
        )
    if report.selector:
        print(
            "selector "
            + ",".join(f"{k}={v}" for k, v in report.selector.items())
            + "; pass it to the run as --ek worker_selector=..."
        )
    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report.to_dict(), indent=2) + "\n")
    return 0 if report.ok else 1


def _delete_command(args: argparse.Namespace, probe: str) -> str:
    """The ``ate-env delete`` that removes a probe: the step's endpoint and atespace, when given."""
    parts = ["ate-env", "delete"]
    if args.api:
        parts += ["--api", args.api]
    if args.atespace != DEFAULT_ATESPACE:
        parts += ["--atespace", args.atespace]
    return " ".join([*parts, probe])


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    try:
        return asyncio.run(_run(args))
    except (ImageError, ValueError, WarmError, EnvError, OSError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

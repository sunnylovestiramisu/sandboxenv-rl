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

"""Resource classes and the base ActorTemplates that carry them.

Substrate requires CPU and memory limits on every ActorTemplate, and
``ate-env-api`` derives the per-image template from a base, thus the limits a
task asks for are a property of the base. A base per (cpus, memory) pair is
named ``<prefix>c<cpus>-m<memory_mb>`` (``harbor-c1-m2048``); a fractional
CPU is spelled in millicores (``c500m``), the unit Substrate books.
Operators register them once with ``ate-env manifest template``;
``hack/register-templates.sh`` prints the commands for a tasks directory.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from harbor.models.task.config import EnvironmentConfig

DEFAULT_PREFIX = "harbor-"
_MAX_NAME_LEN = 63
_NAME = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def cpu_quantity(cpus: int | float) -> str:
    """A Kubernetes CPU quantity: ``2`` for whole cores, ``500m`` otherwise."""
    value = float(cpus)
    if value <= 0:
        raise ValueError(f"cpus must be positive, got {cpus!r}")
    if value.is_integer():
        return str(int(value))
    return f"{round(value * 1000)}m"


def memory_quantity(memory_mb: int) -> str:
    """A Kubernetes memory quantity in MiB, the unit Harbor's ``memory_mb`` uses."""
    if int(memory_mb) <= 0:
        raise ValueError(f"memory_mb must be positive, got {memory_mb!r}")
    return f"{int(memory_mb)}Mi"


def class_template_name(
    prefix: str, cpus: int | float | None, memory_mb: int | None
) -> str | None:
    """The base template for a resource class, or None when the task sets neither."""
    tokens: list[str] = []
    if cpus is not None:
        tokens.append("c" + cpu_quantity(cpus))
    if memory_mb is not None:
        tokens.append(f"m{int(memory_mb)}")
    if not tokens:
        return None
    name = prefix + "-".join(tokens)
    if len(name) > _MAX_NAME_LEN or not _NAME.match(name):
        raise ValueError(
            f"template name {name!r} is not a valid resource name "
            "(lowercase alphanumerics and '-', at most 63 characters); use another template_prefix"
        )
    return name


def manifest_template_args(cpus: int | float | None, memory_mb: int | None) -> list[str]:
    """The ``ate-env manifest template`` flags that give a base these limits."""
    args: list[str] = []
    if cpus is not None:
        args += ["--cpu", cpu_quantity(cpus)]
    if memory_mb is not None:
        args += ["--memory", memory_quantity(memory_mb)]
    return args


def register_command(
    prefix: str,
    cpus: int | float | None,
    memory_mb: int | None,
    extra: Sequence[str] = (),
) -> str | None:
    """The command that registers the class template, for messages and scripts."""
    name = class_template_name(prefix, cpus, memory_mb)
    if name is None:
        return None
    parts = ["ate-env", "manifest", "template", "--template", name]
    parts += manifest_template_args(cpus, memory_mb)
    parts += list(extra)
    return " ".join(parts) + " | kubectl-ate create actor-template -f -"


def task_environments(root: str | Path) -> list[tuple[str, EnvironmentConfig]]:
    """Each task under ``root`` (its directory, relative to ``root``) with its
    ``[environment]`` table, in the order the tasks are found.

    The table of each ``task.toml`` is parsed with Harbor's ``EnvironmentConfig``,
    thus the values are the ones ``AteEnvEnvironment`` sees: ``memory = "4G"``
    normalizes to ``memory_mb = 4096``, and ``cpus`` is normalized the same way.
    The rest of the file is not validated.
    """
    from harbor.models.task.config import EnvironmentConfig

    root = Path(root)
    tasks: list[tuple[str, EnvironmentConfig]] = []
    for task_toml in sorted(root.rglob("task.toml")):
        try:
            with task_toml.open("rb") as f:
                env = EnvironmentConfig.model_validate(tomllib.load(f).get("environment", {}))
        except ValueError as e:  # TOMLDecodeError and pydantic's ValidationError both are
            raise ValueError(f"{task_toml}: {e}") from e
        name = task_toml.parent.relative_to(root).as_posix()
        # root is the task directory itself: its own name, resolved, as "."
        # has none.
        tasks.append((task_toml.parent.resolve().name if name == "." else name, env))
    return tasks


def task_classes(
    root: str | Path, prefix: str = DEFAULT_PREFIX
) -> dict[str, tuple[int | float | None, int | None]]:
    """The resource classes the tasks under ``root`` ask for: template name to
    ``(cpus, memory_mb)``, in the order the tasks are found (``task_environments``).
    """
    classes: dict[str, tuple[int | float | None, int | None]] = {}
    for _, env in task_environments(root):
        name = class_template_name(prefix, env.cpus, env.memory_mb)
        if name is not None:
            classes.setdefault(name, (env.cpus, env.memory_mb))
    return classes


def register_commands(
    root: str | Path, prefix: str = DEFAULT_PREFIX, extra: Sequence[str] = ()
) -> list[str]:
    """One ``register_command`` per class under ``root`` (``hack/register-templates.sh``)."""
    commands: list[str] = []
    for cpus, memory_mb in task_classes(root, prefix).values():
        command = register_command(prefix, cpus, memory_mb, extra)
        if command is not None:
            commands.append(command)
    return commands

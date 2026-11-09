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

"""Task sources and dataset-row parsing.

A ``Task`` is what the fleet runs: an ``id``, a container ``image`` and
opaque ``metadata``. Datasets keep those under different keys: R2E-Gym and
SWE-bench rows carry ``docker_image`` and ``instance_id``, sometimes under
``metadata`` or under the ``env_config`` a trainer wraps a row in. This module
reads a row's image and id from those layouts, or from the paths the caller
names, and gives the fleet ``Task`` objects from a list or a file:

    fleet.load_tasks(rows, image_field="metadata.docker_image")
    fleet.load_tasks(JsonlSource("tasks.jsonl", id_field="instance_id"))

A row must carry its id. The fleet never makes one up from the row's
position: the same position would name another task in another process of
the job, or after a reload, and ``acquire`` by id, results and reports need
ids that come from the data.
"""

from __future__ import annotations

import codecs
import json
import os
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Protocol, runtime_checkable

from .types import Task

__all__ = [
    "DEFAULT_ID_FIELDS",
    "DEFAULT_IMAGE_FIELDS",
    "FieldSpec",
    "JsonlSource",
    "ListSource",
    "TaskSource",
    "extract_image",
    "row_to_task",
    "to_tasks",
]

# Where a row keeps its container image, in the order tried when no
# ``image_field`` is given: the SDK's own key, then the R2E-Gym and SWE-bench
# conventions at the top level, under ``metadata``, and under the
# ``env_config`` a trainer wraps a row in. Dotted paths descend into dicts.
DEFAULT_IMAGE_FIELDS: tuple[str, ...] = (
    "image",
    "docker_image",
    "image_name",
    "metadata.image",
    "metadata.docker_image",
    "metadata.env_config.entry.image",
    "metadata.env_config.entry.docker_image",
    "env_config.entry.image",
    "env_config.entry.docker_image",
)

# Where a row keeps its id when no ``id_field`` is given: the SDK's keys
# (``task_id`` before ``id``, as ``load_tasks`` has always read them), then
# SWE-bench's ``instance_id``.
DEFAULT_ID_FIELDS: tuple[str, ...] = ("task_id", "id", "instance_id")

#: One dotted path, several tried in order, or a callable ``row -> value``.
FieldSpec = str | Sequence[str] | Callable[[dict], Any] | None


@runtime_checkable
class TaskSource(Protocol):
    """Anything that can produce the fleet's tasks.

    ``SandboxFleet.load_tasks`` and ``add_tasks`` accept a source and call
    its ``load()``, synchronously: ``AsyncSandboxFleet`` calls it on the
    event loop thread. Any object with a callable ``load`` attribute matches.
    """

    def load(self) -> Iterable[Task]:
        ...


def _unwrap(value: Any) -> Any:
    """Unwrap what a columnar dataset hands back for one field: a one-element
    list, tuple or array (``tolist``) becomes its element."""
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        value = value.tolist()
    while isinstance(value, (list, tuple)) and len(value) == 1:
        value = value[0]
    return value


def _dig(row: dict, path: str) -> list[Any]:
    """The values ``path`` names in ``row``, in the order to try them: the
    literal key when it exists (flattened datasets have
    ``metadata.docker_image`` columns), then the dotted descent, which
    unwraps one-element lists on the way."""
    found = [row[path]] if path in row else []
    if "." not in path:
        return found
    node: Any = row
    for part in path.split("."):
        node = _unwrap(node)
        if not isinstance(node, dict) or part not in node:
            return found
        node = node[part]
    return found + [node]


def _first(row: dict, paths: Sequence[str], convert: Callable[[Any], str | None]) -> str | None:
    """The first value under ``paths`` that ``convert`` accepts."""
    for path in paths:
        for value in _dig(row, path):
            out = convert(value)
            if out is not None:
                return out
    return None


def _as_scalar(value: Any) -> Any:
    """A columnar field unwrapped (see ``_unwrap``). Several elements, a dict
    or None is no scalar and gives None."""
    value = _unwrap(value)
    if isinstance(value, (list, tuple, dict)):
        return None
    return value


def _as_image(value: Any) -> str | None:
    """One image reference out of a field: a string, UTF-8 bytes, or a
    one-element array of either. Blank strings and binary data (a PNG under
    ``image``) are not references."""
    value = _as_scalar(value)
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def _as_id(value: Any) -> str | None:
    """A task id out of a field: any scalar but None, a bool, a blank string
    and binary data. ``0`` is an id."""
    value = _as_scalar(value)
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value.strip()):
        return None
    return str(value)


def _paths(field: FieldSpec, defaults: Sequence[str], name: str) -> Sequence[str]:
    if field is None:
        return defaults
    paths = (field,) if isinstance(field, str) else tuple(field)
    if not paths:
        raise ValueError(f"{name} is an empty sequence; name at least one field")
    return paths


def _call(field: Callable[[dict], Any], row: dict, name: str, index: int) -> Any:
    try:
        return field(row)
    except Exception as e:
        raise ValueError(f"Task row at index {index}: {name} raised {type(e).__name__}: {e}") from e


def extract_image(row: dict, image_field: FieldSpec = None) -> str | None:
    """The container image of a dataset ``row``, or None when nothing matches.

    ``image_field`` is one dotted path (``"metadata.docker_image"``), several
    paths tried in order, a callable ``row -> value``, or None for
    ``DEFAULT_IMAGE_FIELDS``.
    """
    if callable(image_field):
        return _as_image(image_field(row))
    return _first(row, _paths(image_field, DEFAULT_IMAGE_FIELDS, "image_field"), _as_image)


def _keys(row: dict) -> str:
    return ", ".join(sorted(map(str, row))[:10]) or "none"


def row_to_task(row: dict, index: int, *, image_field: FieldSpec = None,
                id_field: FieldSpec = None) -> Task:
    """The ``Task`` for one dataset row; ``index`` is its position, for errors.

    The image comes from ``image_field`` (see ``extract_image``) and the id
    from ``id_field``: one dotted path, several tried in order, a callable,
    or None for ``DEFAULT_IMAGE_FIELDS`` and ``DEFAULT_ID_FIELDS``. The whole
    row is the task's ``metadata``, and its ``template`` key, when set, the
    task's ActorTemplate.

    Raises ValueError when no image or no id matches, or when a callable
    raises: a made-up id would name the row's position, not the row.
    """
    if callable(image_field):
        image = _as_image(_call(image_field, row, "image_field", index))
    else:
        image = extract_image(row, image_field)
    if not image:
        tried = "callable" if callable(image_field) else ", ".join(
            _paths(image_field, DEFAULT_IMAGE_FIELDS, "image_field"))
        raise ValueError(
            f"Task row at index {index} has no container image (tried: {tried}; its keys: {_keys(row)}). "
            "Name where the rows keep it with image_field=, for example "
            "image_field='metadata.docker_image'."
        )
    if callable(id_field):
        task_id = _as_id(_call(id_field, row, "id_field", index))
    else:
        task_id = _first(row, _paths(id_field, DEFAULT_ID_FIELDS, "id_field"), _as_id)
    if task_id is None:
        if id_field is None:
            raise ValueError(
                f"Task dict at index {index} has no 'task_id' or 'id' "
                f"(also tried: {', '.join(DEFAULT_ID_FIELDS[2:])}; "
                f"its keys: {_keys(row)}). Give each task a stable id from your data: pass "
                "Task(id=..., image=...), add an 'id' key, or name the field with id_field=, "
                "for example from 'instance_id' (SWE-bench) or 'repo_name' and 'commit_hash' "
                "(R2E-Gym, with a callable)."
            )
        tried = "callable" if callable(id_field) else ", ".join(_paths(id_field, (), "id_field"))
        raise ValueError(f"Task row at index {index} has no id (tried: {tried}; its keys: {_keys(row)}).")
    template = row.get("template")
    return Task(id=task_id, image=image, metadata=row,
                template=template if isinstance(template, str) and template else None)


def to_tasks(source: TaskSource | Iterable[Any], *, image_field: FieldSpec = None,
             id_field: FieldSpec = None) -> list[Task]:
    """Normalize ``source`` into ``list[Task]``.

    Accepts a ``TaskSource`` (its ``load()``), or an iterable of ``Task``
    objects and dataset rows (dicts, read with ``row_to_task``). The field
    options apply to rows only; a source reads its own.
    """
    if isinstance(source, TaskSource):
        if not callable(source.load):
            raise ValueError(f"Unsupported task source type: {type(source)} (its load is not callable)")
        if image_field is not None or id_field is not None:
            raise ValueError("image_field and id_field apply to rows; set them on the source instead")
        return list(source.load())
    if isinstance(source, (str, os.PathLike)):
        raise ValueError(f"Unsupported task source type: {type(source)}; for a JSON-lines file, "
                         "pass JsonlSource(path)")
    if isinstance(source, (bytes, dict)):
        raise ValueError(f"Unsupported task source type: {type(source)}")
    out: list[Task] = []
    for i, item in enumerate(source):
        if isinstance(item, Task):
            out.append(item)
        elif isinstance(item, dict):
            out.append(row_to_task(item, i, image_field=image_field, id_field=id_field))
        else:
            raise ValueError(f"Unsupported task item type: {type(item)}")
    return out


class ListSource:
    """A ``TaskSource`` over an in-memory list of ``Task`` objects and rows."""

    def __init__(self, items: Iterable[Any], *, image_field: FieldSpec = None,
                 id_field: FieldSpec = None):
        self._items = list(items)
        self.image_field = image_field
        self.id_field = id_field

    def load(self) -> list[Task]:
        return to_tasks(self._items, image_field=self.image_field, id_field=self.id_field)


class JsonlSource:
    """A ``TaskSource`` reading one JSON object per line from a local file.

    Args:
        path: a local ``.jsonl`` file. It is streamed, so ``limit`` stops the
            read early. Blank lines are skipped.
        image_field: where a row keeps its image (see ``extract_image``).
        id_field: where a row keeps its id (see ``row_to_task``).
        limit: the most tasks to read. None reads all; 0 reads none.

    The file is UTF-8, with or without a byte order mark. A line that is not
    UTF-8 or not a JSON object, or a row that has no image or no id, raises
    ValueError naming the file and the line.
    """

    def __init__(self, path: str | os.PathLike[str], *, image_field: FieldSpec = None,
                 id_field: FieldSpec = None, limit: int | None = None):
        if os.fspath(path).startswith("gs://"):
            raise ValueError(f"JsonlSource reads local files only, not {os.fspath(path)}")
        if limit is not None and limit < 0:
            raise ValueError(f"limit must be None or >= 0, not {limit}")
        self.path = path
        self.image_field = image_field
        self.id_field = id_field
        self.limit = limit

    def load(self) -> list[Task]:
        tasks: list[Task] = []
        path = os.path.expanduser(os.fspath(self.path))
        # Bytes split on b"\n" only, and each line is decoded on its own, so a
        # bad byte names its line. A JSON string may hold U+2028, U+2029 or
        # U+0085; str.splitlines() would split on them.
        with open(path, "rb") as fh:
            if self.limit == 0:
                return tasks
            for lineno, raw in enumerate(fh, 1):
                where = f"{self.path}:{lineno}"
                if lineno == 1:
                    raw = raw.removeprefix(codecs.BOM_UTF8)
                try:
                    line = raw.decode("utf-8").strip()
                except UnicodeDecodeError as e:
                    raise ValueError(f"{where}: not UTF-8: {e}") from e
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(f"{where}: not JSON: {e}") from e
                if not isinstance(row, dict):
                    raise ValueError(f"{where}: a task row must be a JSON object, not {type(row).__name__}")
                try:
                    tasks.append(row_to_task(row, len(tasks), image_field=self.image_field,
                                             id_field=self.id_field))
                except ValueError as e:
                    raise ValueError(f"{where}: {e}") from e
                if self.limit is not None and len(tasks) >= self.limit:
                    break
        return tasks

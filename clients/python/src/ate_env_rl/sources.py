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
import copy
import importlib
import json
import logging
import os
import shutil
import subprocess
from collections.abc import Callable, Iterable, Sequence
from typing import Any, Protocol, runtime_checkable

from .types import Task

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_ID_FIELDS",
    "DEFAULT_IMAGE_FIELDS",
    "DEFAULT_SWEBENCH_DATASET",
    "FieldSpec",
    "ImageListSource",
    "JsonlSource",
    "ListSource",
    "SweBenchSource",
    "TaskSource",
    "extract_image",
    "load_image_list",
    "read_bytes",
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
    """A ``TaskSource`` reading one JSON object per line.

    Args:
        path: a local ``.jsonl`` file, streamed so ``limit`` stops the read
            early, or a ``gs://`` object, downloaded whole (see
            ``read_bytes``). Blank lines are skipped.
        image_field: where a row keeps its image (see ``extract_image``).
        id_field: where a row keeps its id (see ``row_to_task``).
        limit: the most tasks to read. None reads all; 0 reads none.

    The file is UTF-8, with or without a byte order mark. A line that is not
    UTF-8 or not a JSON object, or a row that has no image or no id, raises
    ValueError naming the file and the line.
    """

    def __init__(self, path: str | os.PathLike[str], *, image_field: FieldSpec = None,
                 id_field: FieldSpec = None, limit: int | None = None):
        if limit is not None and limit < 0:
            raise ValueError(f"limit must be None or >= 0, not {limit}")
        self.path = path
        self.image_field = image_field
        self.id_field = id_field
        self.limit = limit

    def load(self) -> list[Task]:
        path = os.fspath(self.path)
        if path.startswith("gs://"):
            if self.limit == 0:
                return []
            return self._parse(read_bytes(path).split(b"\n"))
        with open(os.path.expanduser(path), "rb") as fh:
            if self.limit == 0:
                return []
            return self._parse(fh)

    def _parse(self, lines: Iterable[bytes]) -> list[Task]:
        tasks: list[Task] = []
        # Bytes split on b"\n" (and b"\r\n") only, and each line is decoded
        # on its own, so a bad byte names its line. A JSON string may hold
        # U+2028, U+2029 or U+0085; str.splitlines() would split on them.
        for lineno, raw in enumerate(lines, 1):
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


# ---------------------------------------------------------------------------
# Remote files: gs://


def _storage_module() -> Any:
    """``google.cloud.storage`` when it is installed, else None."""
    try:
        return importlib.import_module("google.cloud.storage")
    except ImportError:
        return None


#: How long ``read_bytes`` lets ``gcloud storage cat`` or ``gsutil cat`` run.
_CLI_TIMEOUT_S = 600

#: Characters ``gcloud storage cat`` and ``gsutil cat`` expand in an object name.
_GS_WILDCARDS = "*?["


def read_bytes(path: str | os.PathLike[str]) -> bytes:
    """The bytes of a local file or a ``gs://`` object.

    A ``gs://`` object is read with ``google.cloud.storage`` when it is
    installed (with the ambient credentials), else with ``gcloud storage cat``
    or ``gsutil cat`` from ``PATH``. A client that cannot read the object (for
    example no application default credentials on a machine where ``gcloud``
    is logged in) falls back to the command line. ``RuntimeError`` names the
    options when none works, or the command that failed, could not run or ran
    past ``_CLI_TIMEOUT_S``; either way it keeps the client's error too. An
    object name with ``*``, ``?`` or ``[`` is read with the client only: the
    command line would expand it, so it raises ``RuntimeError`` instead.
    """
    path = os.fspath(path)
    if not path.startswith("gs://"):
        with open(os.path.expanduser(path), "rb") as fh:
            return fh.read()
    bucket, _, blob = path[len("gs://"):].partition("/")
    if not bucket or not blob:
        raise ValueError(f"not a gs:// object path: {path}")
    client_error = None
    storage = _storage_module()
    if storage is not None:
        try:
            return storage.Client().bucket(bucket).blob(blob).download_as_bytes()
        except Exception as exc:  # noqa: BLE001 - the command line below may still work
            client_error = f"{type(exc).__name__}: {exc}"
            logger.info("google.cloud.storage could not read %s (%s); trying the command line", path, client_error)
    client_note = f" (google.cloud.storage: {client_error})" if client_error else ""
    if any(c in blob for c in _GS_WILDCARDS):
        # gcloud and gsutil expand these, so they could read other objects
        # (or several, concatenated); the client reads the name literally.
        raise RuntimeError(
            f"cannot read {path}: the object name holds a wildcard (*, ? or [), which gcloud and gsutil "
            f"expand; only google-cloud-storage reads it literally{client_note}"
        )
    for tool, argv in (("gcloud", ["gcloud", "storage", "cat", path]), ("gsutil", ["gsutil", "cat", path])):
        if shutil.which(tool):
            try:
                return subprocess.run(argv, check=True, capture_output=True, timeout=_CLI_TIMEOUT_S).stdout
            except subprocess.CalledProcessError as e:
                stderr = (e.stderr or b"").decode("utf-8", errors="replace").strip()
                raise RuntimeError(f"cannot read {path}: {tool} failed: {stderr}{client_note}") from e
            except subprocess.TimeoutExpired as e:
                raise RuntimeError(
                    f"cannot read {path}: {tool} did not finish in {_CLI_TIMEOUT_S} s{client_note}"
                ) from e
            except OSError as e:
                raise RuntimeError(f"cannot read {path}: cannot run {tool}: {e}{client_note}") from e
    raise RuntimeError(
        f"cannot read {path}: install google-cloud-storage, or put gcloud or gsutil on PATH{client_note}"
    )


# ---------------------------------------------------------------------------
# Image manifests


def load_image_list(path: str | os.PathLike[str]) -> list[str]:
    """The images of a manifest: one image per line, local or ``gs://``.

    Blank lines and ``#`` comments are skipped, and a repeated image is kept
    once, at its first line. A line that is not UTF-8, or whose image holds
    whitespace (two images on one line, say), raises ValueError naming the
    file and the line.
    """
    out: list[str] = []
    seen: set[str] = set()
    for lineno, raw in enumerate(read_bytes(path).removeprefix(codecs.BOM_UTF8).split(b"\n"), 1):
        where = f"{os.fspath(path)}:{lineno}"
        try:
            line = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ValueError(f"{where}: not UTF-8: {e}") from e
        image = line.split("#", 1)[0].strip()
        if any(c.isspace() for c in image):
            raise ValueError(f"{where}: an image reference cannot hold whitespace: {image!r}")
        if image and image not in seen:
            seen.add(image)
            out.append(image)
    return out


class ImageListSource:
    """A ``TaskSource`` over an image manifest (see ``load_image_list``): one
    task per image, whose id is the image reference itself, so it is stable
    across processes and reloads."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = path

    def load(self) -> list[Task]:
        return [Task(id=image, image=image) for image in load_image_list(self.path)]


# ---------------------------------------------------------------------------
# Hugging Face datasets

#: The SWE-bench Verified split with a ``docker_image`` per row.
DEFAULT_SWEBENCH_DATASET = "R2E-Gym/SWE-Bench-Verified"


class SweBenchSource:
    """A ``TaskSource`` over a SWE-bench-style Hugging Face dataset.

    Needs the ``datasets`` package (``pip install datasets``); it is imported
    when ``load()`` runs, not when the source is built.

    Args:
        dataset: the dataset id.
        split: the split to read: a plain name such as ``"test"``, without a
            slice (``offset`` and ``limit`` make the slice).
        limit: the most tasks. None reads all; 0 reads none (and imports
            nothing).
        offset: rows to skip first. With ``limit`` or ``offset``, only that
            slice of the split is read into memory (``"test[10:20]"``);
            ``datasets`` still downloads and prepares the whole config. An
            ``offset`` at or past the end of the split reads no tasks.
        image_field / id_field: as for ``row_to_task``. The defaults read
            ``docker_image`` or ``image_name``, and ``instance_id``.
        keep_row: also store a deep copy of the row under
            ``Task.metadata["ds"]``, which the R2E-Gym adapter
            (``ate_env_rl.adapters.r2egym``) needs for its environment and
            reward grading. The copy
            keeps the row the adapter grades with apart from ``Task.metadata``,
            which a rollout may edit; ``copy.deepcopy`` copies only the
            containers and shares the strings, so a large text field (such as
            ``parsed_commit_content``) is stored once.
        revision: the dataset revision (a branch, tag or commit), to pin it.
        name: the dataset config, for a dataset with several.
    """

    def __init__(self, dataset: str = DEFAULT_SWEBENCH_DATASET, split: str = "test", *,
                 limit: int | None = None, offset: int = 0, image_field: FieldSpec = None,
                 id_field: FieldSpec = None, keep_row: bool = False, revision: str | None = None,
                 name: str | None = None):
        if limit is not None and limit < 0:
            raise ValueError(f"limit must be None or >= 0, not {limit}")
        if offset < 0:
            raise ValueError(f"offset must be >= 0, not {offset}")
        if "[" in split:
            raise ValueError(f"split must be a plain split name, not {split!r}: use offset= and limit= for a slice")
        self.dataset = dataset
        self.split = split
        self.limit = limit
        self.offset = offset
        self.image_field = image_field
        self.id_field = id_field
        self.keep_row = keep_row
        self.revision = revision
        self.name = name

    def load(self) -> list[Task]:
        if self.limit == 0:
            return []
        try:
            datasets = importlib.import_module("datasets")
        except ImportError as e:
            raise ImportError("SweBenchSource needs the Hugging Face datasets package: pip install datasets") from e
        split = self.split
        if self.offset or self.limit is not None:
            end = "" if self.limit is None else self.offset + self.limit
            split = f"{self.split}[{self.offset}:{end}]"
        kwargs: dict[str, Any] = {"split": split}
        if self.name is not None:
            kwargs["name"] = self.name
        if self.revision is not None:
            kwargs["revision"] = self.revision
        logger.info("Loading %s split %s", self.dataset, split)
        try:
            rows = datasets.load_dataset(self.dataset, **kwargs)
        except ValueError as e:
            # datasets refuses a slice that starts at or past the end of the
            # split ('Instruction "test[500:]" corresponds to no data!').
            if self.offset and "corresponds to no data" in str(e):
                logger.info("%s split %s: offset %d is past the end; no tasks", self.dataset, split, self.offset)
                return []
            raise ValueError(f"{self.dataset} split {split}: {e}") from e
        tasks: list[Task] = []
        for i, item in enumerate(rows):
            row = dict(item)
            try:
                task = row_to_task(row, self.offset + i, image_field=self.image_field, id_field=self.id_field)
            except ValueError as e:
                raise ValueError(f"{self.dataset} [{self.split}] row {self.offset + i}: {e}") from e
            if self.keep_row:
                task = Task(id=task.id, image=task.image, metadata={**row, "ds": copy.deepcopy(row)},
                            template=task.template)
            tasks.append(task)
        logger.info("Loaded %d tasks (%d images) from %s", len(tasks), len({t.image for t in tasks}), self.dataset)
        return tasks

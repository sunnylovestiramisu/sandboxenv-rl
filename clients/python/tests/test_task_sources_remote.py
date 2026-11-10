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

"""gs:// reading, image manifests and Hugging Face datasets (gap #3 of #123)."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import types

import pytest

from ate_env_rl import (
    DEFAULT_SWEBENCH_DATASET,
    FleetConfig,
    ImageListSource,
    JsonlSource,
    SandboxFleet,
    SweBenchSource,
    load_image_list,
    read_bytes,
    sources,
)
from ate_env_rl.backend.mock import MockBackendDriver

# ---------------------------------------------------------------------------
# read_bytes and gs://


class _FakeStorage:
    """google.cloud.storage with one object, or a client that fails."""

    def __init__(self, objects=None, error=None):
        self.objects = objects or {}
        self.error = error
        self.reads = []

    def Client(self):  # noqa: N802 - the real module's name
        storage = self

        class _Blob:
            def __init__(self, bucket, name):
                self.bucket, self.name = bucket, name

            def download_as_bytes(self):
                storage.reads.append(f"gs://{self.bucket}/{self.name}")
                if storage.error:
                    raise storage.error
                return storage.objects[f"gs://{self.bucket}/{self.name}"]

        class _Bucket:
            def __init__(self, name):
                self.name = name

            def blob(self, name):
                return _Blob(self.name, name)

        class _Client:
            def bucket(self, name):
                return _Bucket(name)

        return _Client()


def _no_cli(monkeypatch):
    monkeypatch.setattr(sources.shutil, "which", lambda tool: None)


def test_read_bytes_reads_local_files(tmp_path):
    path = tmp_path / "f"
    path.write_bytes(b"\xffraw")
    assert read_bytes(path) == b"\xffraw"


def test_read_bytes_gs_uses_the_storage_client_first(monkeypatch):
    fake = _FakeStorage({"gs://b/dir/tasks.jsonl": b"data"})
    monkeypatch.setattr(sources, "_storage_module", lambda: fake)
    monkeypatch.setattr(sources.subprocess, "run", lambda *a, **k: pytest.fail("the CLI must not run"))
    assert read_bytes("gs://b/dir/tasks.jsonl") == b"data"
    assert fake.reads == ["gs://b/dir/tasks.jsonl"]


def test_read_bytes_gs_falls_back_to_gcloud_then_gsutil(monkeypatch):
    fake = _FakeStorage(error=RuntimeError("no application default credentials"))
    monkeypatch.setattr(sources, "_storage_module", lambda: fake)
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["check"] and kwargs["capture_output"] and kwargs["timeout"] == 600
        return subprocess.CompletedProcess(argv, 0, stdout=b"from-cli")

    monkeypatch.setattr(sources.subprocess, "run", run)
    monkeypatch.setattr(sources.shutil, "which", lambda tool: tool == "gsutil")
    assert read_bytes("gs://b/o") == b"from-cli"
    assert calls == [["gsutil", "cat", "gs://b/o"]]
    monkeypatch.setattr(sources.shutil, "which", lambda tool: True)
    calls.clear()
    assert read_bytes("gs://b/o") == b"from-cli"
    assert calls == [["gcloud", "storage", "cat", "gs://b/o"]]


def test_read_bytes_gs_reports_a_failed_cli_and_no_way_to_read(monkeypatch):
    monkeypatch.setattr(sources, "_storage_module", lambda: None)

    def run(argv, **kwargs):
        raise subprocess.CalledProcessError(1, argv, stderr=b"AccessDeniedException: 403")

    monkeypatch.setattr(sources.subprocess, "run", run)
    monkeypatch.setattr(sources.shutil, "which", lambda tool: tool == "gcloud")
    with pytest.raises(RuntimeError, match="gcloud failed: AccessDeniedException: 403"):
        read_bytes("gs://b/o")
    _no_cli(monkeypatch)
    with pytest.raises(RuntimeError, match="install google-cloud-storage, or put gcloud or gsutil on PATH"):
        read_bytes("gs://b/o")
    monkeypatch.setattr(sources, "_storage_module", lambda: _FakeStorage(error=PermissionError("denied")))
    with pytest.raises(RuntimeError, match=r"google\.cloud\.storage: PermissionError: denied"):
        read_bytes("gs://b/o")


def test_read_bytes_gs_keeps_the_client_error_when_the_cli_fails_too(monkeypatch):
    monkeypatch.setattr(sources, "_storage_module", lambda: _FakeStorage(error=PermissionError("denied by ADC")))
    monkeypatch.setattr(sources.shutil, "which", lambda tool: tool == "gcloud")

    def run(argv, **kwargs):
        raise subprocess.CalledProcessError(1, argv, stderr=b"not logged in")

    monkeypatch.setattr(sources.subprocess, "run", run)
    with pytest.raises(RuntimeError) as exc:
        read_bytes("gs://b/o")
    assert str(exc.value) == (
        "cannot read gs://b/o: gcloud failed: not logged in (google.cloud.storage: PermissionError: denied by ADC)"
    )
    assert isinstance(exc.value.__cause__, subprocess.CalledProcessError)


@pytest.mark.parametrize("storage", [None, _FakeStorage(error=PermissionError("denied by ADC"))])
def test_read_bytes_gs_reports_a_cli_that_runs_past_its_timeout_as_runtime_error(monkeypatch, storage):
    monkeypatch.setattr(sources, "_storage_module", lambda: storage)
    monkeypatch.setattr(sources.shutil, "which", lambda tool: tool == "gsutil")

    def run(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    monkeypatch.setattr(sources.subprocess, "run", run)
    with pytest.raises(RuntimeError, match=r"^cannot read gs://b/o: gsutil did not finish in 600 s") as exc:
        read_bytes("gs://b/o")
    assert isinstance(exc.value.__cause__, subprocess.TimeoutExpired)
    assert ("google.cloud.storage: PermissionError: denied by ADC" in str(exc.value)) == (storage is not None)


def test_read_bytes_gs_reports_a_cli_that_cannot_run_as_runtime_error(monkeypatch):
    monkeypatch.setattr(sources, "_storage_module", lambda: _FakeStorage(error=PermissionError("denied by ADC")))
    monkeypatch.setattr(sources.shutil, "which", lambda tool: tool == "gcloud")

    def run(argv, **kwargs):
        raise PermissionError(13, "Permission denied", argv[0])

    monkeypatch.setattr(sources.subprocess, "run", run)
    with pytest.raises(RuntimeError) as exc:
        read_bytes("gs://b/o")
    assert str(exc.value).startswith("cannot read gs://b/o: cannot run gcloud: [Errno 13] Permission denied")
    assert str(exc.value).endswith("(google.cloud.storage: PermissionError: denied by ADC)")
    assert isinstance(exc.value.__cause__, PermissionError)


@pytest.mark.parametrize("name", ["train?.jsonl", "*.jsonl", "train[12].jsonl"])
def test_read_bytes_gs_reads_a_wildcard_name_with_the_client_only(monkeypatch, name):
    path = f"gs://b/{name}"
    monkeypatch.setattr(sources, "_storage_module", lambda: _FakeStorage({path: b"literal"}))
    monkeypatch.setattr(sources.subprocess, "run", lambda *a, **k: pytest.fail("the CLI must not run"))
    monkeypatch.setattr(sources.shutil, "which", lambda tool: True)
    assert read_bytes(path) == b"literal"
    monkeypatch.setattr(sources, "_storage_module", lambda: _FakeStorage(error=FileNotFoundError("404 NotFound")))
    with pytest.raises(RuntimeError, match=r"wildcard \(\*, \? or \[\).*google\.cloud\.storage: FileNotFoundError"):
        read_bytes(path)
    monkeypatch.setattr(sources, "_storage_module", lambda: None)
    with pytest.raises(RuntimeError, match=r"only google-cloud-storage reads it literally$"):
        read_bytes(path)


@pytest.mark.parametrize("bad", ["gs://", "gs://bucket", "gs://bucket/", "gs:///object"])
def test_read_bytes_refuses_a_gs_path_without_bucket_and_object(bad):
    with pytest.raises(ValueError, match="not a gs:// object path"):
        read_bytes(bad)


def test_jsonl_source_reads_a_gs_object_with_the_same_rules(monkeypatch):
    rows = [{"instance_id": "a", "image_name": "img-a"}, {"instance_id": "b", "image_name": "img-b",
                                                         "text": "x\u2028y"}]
    body = "\ufeff" + "\r\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n\n"
    fake = _FakeStorage({"gs://b/t.jsonl": body.encode("utf-8")})
    monkeypatch.setattr(sources, "_storage_module", lambda: fake)
    tasks = JsonlSource("gs://b/t.jsonl").load()
    assert [(t.id, t.image) for t in tasks] == [("a", "img-a"), ("b", "img-b")]
    assert tasks[1].metadata["text"] == "x\u2028y"
    assert [t.id for t in JsonlSource("gs://b/t.jsonl", limit=1).load()] == ["a"]
    fake.reads.clear()
    assert JsonlSource("gs://b/t.jsonl", limit=0).load() == []
    assert fake.reads == []  # limit=0 fetches nothing
    fake.objects["gs://b/bad.jsonl"] = b'{"id": "x", "image": "i"}\n{"image": "i"}\n'
    with pytest.raises(ValueError, match=r"gs://b/bad\.jsonl:2: .*has no 'task_id' or 'id'"):
        JsonlSource("gs://b/bad.jsonl").load()


# ---------------------------------------------------------------------------
# Image manifests


def test_load_image_list_skips_blanks_comments_and_repeats(tmp_path):
    path = tmp_path / "images.txt"
    path.write_text("\ufeff# warm these\nimg-a\n\n  img-b  # trailing comment\nimg-a\r\nimg-c\n", encoding="utf-8")
    assert load_image_list(path) == ["img-a", "img-b", "img-c"]
    tasks = ImageListSource(path).load()
    assert [(t.id, t.image) for t in tasks] == [("img-a", "img-a"), ("img-b", "img-b"), ("img-c", "img-c")]


def test_load_image_list_reads_gs_and_refuses_bad_utf8(monkeypatch, tmp_path):
    monkeypatch.setattr(sources, "_storage_module", lambda: _FakeStorage({"gs://b/i.txt": b"img-x\n"}))
    assert load_image_list("gs://b/i.txt") == ["img-x"]
    path = tmp_path / "bad.txt"
    path.write_bytes(b"img-a\n# ok\nimg\xff\n")
    with pytest.raises(ValueError, match="^" + re.escape(f"{path}:3: not UTF-8")):
        load_image_list(path)


@pytest.mark.parametrize("line", ["img-a img-b", "img-a\timg-b  # two on one line"])
def test_load_image_list_refuses_whitespace_inside_an_image(tmp_path, line):
    path = tmp_path / "images.txt"
    path.write_text(f"img-0\n{line}\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"images\.txt:2: an image reference cannot hold whitespace: 'img-a(\\t| )img-b'"):
        load_image_list(path)


def test_an_image_list_feeds_the_fleet(tmp_path):
    path = tmp_path / "images.txt"
    path.write_text("img-a\nimg-b\n", encoding="utf-8")
    fleet = SandboxFleet(FleetConfig(backend="mock"), driver=MockBackendDriver())
    fleet.load_tasks(ImageListSource(path))
    assert fleet.image_counts() == {"img-a": 1, "img-b": 1}


# ---------------------------------------------------------------------------
# Hugging Face datasets

ROWS = [
    {"instance_id": f"repo__repo-{i}", "docker_image": f"img-{i}", "repo": "repo/repo",
     "base_commit": f"c{i}", "nested": {"tests": [f"t{i}"]}}
    for i in range(5)
]


@pytest.fixture
def fake_datasets(monkeypatch):
    """A ``datasets`` module whose load_dataset slices ROWS like HF does."""
    calls = []

    def load_dataset(path, split, **kwargs):  # datasets' own parameter names
        calls.append((path, split, kwargs))
        rows = ROWS
        if "[" in split:
            start, _, end = split[split.index("[") + 1:-1].partition(":")
            if int(start or 0) >= len(ROWS):  # as datasets 3.x to 5.x do
                raise ValueError(f'Instruction "{split}" corresponds to no data!')
            rows = ROWS[int(start or 0): int(end) if end else None]
        return [dict(r) for r in rows]

    module = types.ModuleType("datasets")
    module.load_dataset = load_dataset
    monkeypatch.setitem(sys.modules, "datasets", module)
    return calls


def test_swebench_source_reads_rows_with_the_default_fields(fake_datasets):
    tasks = SweBenchSource().load()
    assert [(t.id, t.image) for t in tasks] == [(f"repo__repo-{i}", f"img-{i}") for i in range(5)]
    assert tasks[0].metadata == ROWS[0] and "ds" not in tasks[0].metadata
    assert fake_datasets == [(DEFAULT_SWEBENCH_DATASET, "test", {})]


def test_swebench_source_loads_only_the_slice_it_needs(fake_datasets):
    tasks = SweBenchSource("org/ds", split="dev", limit=2, offset=1, revision="v1").load()
    assert [t.id for t in tasks] == ["repo__repo-1", "repo__repo-2"]
    assert fake_datasets[-1] == ("org/ds", "dev[1:3]", {"revision": "v1"})
    SweBenchSource(offset=3).load()
    assert fake_datasets[-1][1] == "test[3:]"
    SweBenchSource(limit=1).load()
    assert fake_datasets[-1][1] == "test[0:1]"


def test_swebench_source_keep_row_stores_an_owned_copy(fake_datasets):
    tasks = SweBenchSource(limit=2, keep_row=True).load()
    ds = tasks[0].metadata["ds"]
    assert ds == ROWS[0]
    ds["nested"]["tests"].append("mutated")
    assert ROWS[0]["nested"]["tests"] == ["t0"]
    assert tasks[1].metadata["ds"]["nested"]["tests"] == ["t1"]


def test_swebench_source_field_options_and_errors_name_the_row(fake_datasets):
    tasks = SweBenchSource(limit=1, id_field=lambda r: f"{r['repo']}@{r['base_commit']}").load()
    assert tasks[0].id == "repo/repo@c0"
    with pytest.raises(ValueError, match=r"org/ds \[test\] row 2: Task row at index 2 has no id \(tried: uid"):
        SweBenchSource("org/ds", offset=2, id_field="uid").load()
    # A bad row after good ones: its index is its row in the split, not the
    # count of tasks read so far (2) or its position in the slice (2).
    def id_but_row_3(row):
        if row["instance_id"] == "repo__repo-3":
            raise KeyError("uid")
        return row["instance_id"]

    with pytest.raises(ValueError, match=r"org/ds \[test\] row 3: Task row at index 3: id_field raised KeyError"):
        SweBenchSource("org/ds", offset=1, id_field=id_but_row_3).load()


def test_swebench_source_needs_datasets_only_when_it_loads(monkeypatch):
    monkeypatch.setitem(sys.modules, "datasets", None)  # import raises ImportError
    source = SweBenchSource()
    assert SweBenchSource(limit=0).load() == []
    with pytest.raises(ImportError, match="pip install datasets"):
        source.load()


@pytest.mark.parametrize("kwargs, match", [({"limit": -1}, "limit"), ({"offset": -1}, "offset")])
def test_swebench_source_rejects_negative_bounds(kwargs, match):
    with pytest.raises(ValueError, match=match):
        SweBenchSource(**kwargs)


def test_swebench_source_rejects_a_split_that_already_has_a_slice():
    with pytest.raises(ValueError, match=r"split must be a plain split name, not 'test\[:100\]'"):
        SweBenchSource(split="test[:100]")


@pytest.mark.parametrize("kwargs", [{"offset": 5}, {"offset": 7}, {"offset": 5, "limit": 10}])
def test_swebench_source_offset_past_the_end_reads_no_tasks(fake_datasets, kwargs):
    # Workers that page with offset=rank * n: the last one gets [], not
    # datasets' 'corresponds to no data' error.
    assert SweBenchSource("org/ds", **kwargs).load() == []
    assert fake_datasets[-1][1].startswith(f"test[{kwargs['offset']}:")


def test_swebench_source_names_the_dataset_when_load_dataset_refuses(monkeypatch):
    module = types.ModuleType("datasets")

    def load_dataset(path, split, **kwargs):
        raise ValueError("Config name is missing.")

    module.load_dataset = load_dataset
    monkeypatch.setitem(sys.modules, "datasets", module)
    with pytest.raises(ValueError, match=r"^org/ds split test: Config name is missing\.$"):
        SweBenchSource("org/ds").load()
    # Offset 0 is never "past the end": the error is re-raised with the dataset.
    def no_data(path, split, **kwargs):
        raise ValueError(f'Instruction "{split}" corresponds to no data!')

    module.load_dataset = no_data
    with pytest.raises(ValueError, match=r"^org/ds split test\[0:3\]: Instruction"):
        SweBenchSource("org/ds", limit=3).load()


def test_swebench_source_passes_the_config_name(fake_datasets):
    SweBenchSource("org/ds", name="lite", limit=1).load()
    assert fake_datasets[-1] == ("org/ds", "test[0:1]", {"name": "lite"})
    SweBenchSource("org/ds").load()
    assert fake_datasets[-1] == ("org/ds", "test", {})


def test_swebench_source_logs_the_split_it_reads(fake_datasets, caplog):
    with caplog.at_level("INFO", logger=sources.logger.name):
        SweBenchSource(limit=2, offset=1).load()
    assert f"Loading {DEFAULT_SWEBENCH_DATASET} split test[1:3]" in caplog.text

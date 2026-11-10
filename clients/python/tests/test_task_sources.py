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

"""Task sources and dataset-row parsing (gap #3 of issue #123)."""

from __future__ import annotations

import json

import pytest

from ate_env_rl import (
    AsyncSandboxFleet,
    FleetConfig,
    JsonlSource,
    ListSource,
    SandboxFleet,
    Task,
    TaskSource,
    extract_image,
    row_to_task,
    to_tasks,
)
from ate_env_rl.backend.mock import MockBackendDriver

R2E_ROW = {
    "repo_name": "pandas",
    "commit_hash": "abc123",
    "docker_image": "namanjain12/pandas_final:abc123",
}
SWEBENCH_ROW = {"instance_id": "django__django-11099", "image_name": "swebench/sweb.eval.django:latest"}


def _fleet() -> SandboxFleet:
    return SandboxFleet(FleetConfig(backend="mock"), driver=MockBackendDriver())


# ---------------------------------------------------------------------------
# extract_image


def test_extract_image_tries_the_sdk_key_then_the_r2e_and_swebench_conventions():
    assert extract_image({"image": "a", "docker_image": "b"}) == "a"
    assert extract_image(R2E_ROW) == "namanjain12/pandas_final:abc123"
    assert extract_image(SWEBENCH_ROW) == "swebench/sweb.eval.django:latest"
    assert extract_image({"metadata": {"docker_image": "m"}}) == "m"
    assert extract_image({"metadata": {"env_config": {"entry": {"docker_image": "e"}}}}) == "e"
    assert extract_image({"env_config": {"entry": {"image": "w"}}}) == "w"
    assert extract_image({"repo": "x"}) is None


def test_extract_image_unwraps_columnar_values_and_skips_blank_or_binary_ones():
    assert extract_image({"docker_image": ["one"]}) == "one"
    assert extract_image({"docker_image": [["nested"]]}) == "nested"
    assert extract_image({"docker_image": b"bytes:tag"}) == "bytes:tag"
    assert extract_image({"docker_image": "  spaced  "}) == "spaced"
    # A blank, multi-element, or binary value is not a reference: fall through.
    assert extract_image({"image": "  ", "docker_image": "d"}) == "d"
    assert extract_image({"image": ["a", "b"], "docker_image": "d"}) == "d"
    assert extract_image({"image": b"\x89PNG\r\n\x1a\n\xff", "docker_image": "d"}) == "d"

    class FakeArray:
        def tolist(self):
            return ["from-array"]

    assert extract_image({"docker_image": FakeArray()}) == "from-array"


def test_extract_image_with_a_path_a_sequence_or_a_callable():
    row = {"meta": {"img": "p"}, "alt": "q", "metadata.docker_image": "flat"}
    assert extract_image(row, "meta.img") == "p"
    assert extract_image(row, ["missing", "alt"]) == "q"
    assert extract_image(row, lambda r: r["alt"].upper()) == "Q"
    # A flattened dataset's literal dotted column wins over the descent.
    assert extract_image(row, "metadata.docker_image") == "flat"
    # An explicit field does not fall back to the defaults.
    assert extract_image({"image": "x"}, "docker_image") is None


# ---------------------------------------------------------------------------
# row_to_task


def test_row_to_task_reads_the_id_from_task_id_id_or_instance_id_and_keeps_the_row():
    t = row_to_task(SWEBENCH_ROW, 0)
    assert (t.id, t.image) == ("django__django-11099", "swebench/sweb.eval.django:latest")
    assert t.metadata == SWEBENCH_ROW
    assert row_to_task({"task_id": "t", "id": "i", "instance_id": "x", "image": "m"}, 0).id == "t"
    assert row_to_task({"id": 0, "image": "m"}, 0).id == "0"            # 0 is an id
    assert row_to_task({"id": ["one"], "image": "m"}, 0).id == "one"    # columnar
    assert row_to_task({"id": "", "instance_id": "x", "image": "m"}, 0).id == "x"


def test_row_to_task_takes_the_template_key():
    assert row_to_task({"id": "a", "image": "m", "template": "tpl"}, 0).template == "tpl"
    assert row_to_task({"id": "a", "image": "m"}, 0).template is None


def test_row_to_task_with_explicit_and_callable_id_fields():
    t = row_to_task(R2E_ROW, 3, id_field=lambda r: f"{r['repo_name']}@{r['commit_hash']}")
    assert t.id == "pandas@abc123"
    assert row_to_task({"meta": {"uid": "u"}, "image": "m"}, 0, id_field="meta.uid").id == "u"


def test_a_row_without_an_id_is_refused_never_given_its_position():
    with pytest.raises(ValueError, match=r"index 3 has no 'task_id' or 'id' \(also tried: instance_id"):
        row_to_task(R2E_ROW, 3)
    with pytest.raises(ValueError, match=r"index 0 has no id \(tried: uid"):
        row_to_task({"id": "present", "image": "m"}, 0, id_field="uid")
    with pytest.raises(ValueError, match=r"index 0 has no id \(tried: callable"):
        row_to_task({"image": "m"}, 0, id_field=lambda r: None)
    for empty in ("", None, [], [None], True):
        with pytest.raises(ValueError, match="no 'task_id' or 'id'"):
            row_to_task({"id": empty, "image": "m"}, 0)


def test_a_row_without_an_image_is_refused_with_what_was_tried():
    with pytest.raises(ValueError, match=r"index 2 has no container image \(tried: image, docker_image"):
        row_to_task({"id": "a"}, 2)
    with pytest.raises(ValueError, match=r"tried: meta\.img"):
        row_to_task({"id": "a", "image": "m"}, 0, image_field="meta.img")


# ---------------------------------------------------------------------------
# to_tasks and the sources


def test_to_tasks_mixes_tasks_and_rows_and_rejects_other_items():
    task = Task(id="t", image="i")
    out = to_tasks([task, SWEBENCH_ROW])
    assert out[0] is task and out[1].id == "django__django-11099"
    with pytest.raises(ValueError, match="Unsupported task item type"):
        to_tasks(["just-an-image"])
    for not_a_list in ("tasks.jsonl", {"id": "a", "image": "m"}):
        with pytest.raises(ValueError, match="Unsupported task source type"):
            to_tasks(not_a_list)


def test_a_source_reads_its_own_fields():
    source = ListSource([R2E_ROW], id_field="commit_hash")
    assert isinstance(source, TaskSource)
    assert [t.id for t in to_tasks(source)] == ["abc123"]
    with pytest.raises(ValueError, match="set them on the source"):
        to_tasks(source, id_field="repo_name")


def test_jsonl_source_streams_rows_with_the_same_field_options(tmp_path):
    path = tmp_path / "tasks.jsonl"
    rows = [
        {"meta": {"img": "a"}, "uid": "1"},
        {},  # placeholder, replaced by a blank line below
        {"meta": {"img": "b"}, "uid": "2", "text": "line\u2028separator\u0085inside"},
        {"meta": {"img": "c"}, "uid": "3"},
    ]
    lines = [json.dumps(r, ensure_ascii=False) if r else "" for r in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    tasks = JsonlSource(path, image_field="meta.img", id_field="uid").load()
    assert [(t.id, t.image) for t in tasks] == [("1", "a"), ("2", "b"), ("3", "c")]
    assert tasks[1].metadata["text"] == "line\u2028separator\u0085inside"
    assert [t.id for t in JsonlSource(path, image_field="meta.img", id_field="uid", limit=2).load()] == ["1", "2"]
    assert JsonlSource(path, limit=0).load() == []


def test_jsonl_source_names_the_file_and_line_of_a_bad_row(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text('{"id": "a", "image": "m"}\n{"image": "m"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"bad\.jsonl:2: Task dict at index 1 has no 'task_id' or 'id'"):
        JsonlSource(path).load()
    path.write_text('{"id": "a", "image": "m"}\n[1, 2]\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"bad\.jsonl:2: a task row must be a JSON object"):
        JsonlSource(path).load()
    path.write_text("{not json\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"bad\.jsonl:1: not JSON"):
        JsonlSource(path).load()


def test_jsonl_source_rejects_negative_limits():
    with pytest.raises(ValueError, match="limit"):
        JsonlSource("tasks.jsonl", limit=-1)


# ---------------------------------------------------------------------------
# The fleets


def test_fleet_load_and_add_tasks_take_rows_with_field_options_and_sources(tmp_path):
    fleet = _fleet()
    fleet.load_tasks([R2E_ROW], id_field=lambda r: r["commit_hash"])
    assert [(t.id, t.image) for t in fleet.tasks] == [("abc123", "namanjain12/pandas_final:abc123")]

    path = tmp_path / "more.jsonl"
    path.write_text(json.dumps(SWEBENCH_ROW) + "\n", encoding="utf-8")
    added = fleet.add_tasks(JsonlSource(path))
    assert [t.id for t in added] == ["django__django-11099"]
    assert [t.id for t in fleet.tasks] == ["abc123", "django__django-11099"]
    assert fleet.image_counts() == {
        "namanjain12/pandas_final:abc123": 1,
        "swebench/sweb.eval.django:latest": 1,
    }


def test_a_bad_row_leaves_the_loaded_tasks_alone():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    with pytest.raises(ValueError, match="index 1 has no container image"):
        fleet.load_tasks([{"id": "b", "image": "i"}, {"id": "c"}])
    with pytest.raises(ValueError, match="index 0 has no id"):
        fleet.add_tasks([{"id": "b", "image": "i"}], id_field="uid")
    assert [t.id for t in fleet.tasks] == ["a"]


def test_async_fleet_passes_the_field_options_through():
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock"), driver=MockBackendDriver())
    fleet.load_tasks([R2E_ROW], id_field="commit_hash")
    added = fleet.add_tasks(ListSource([SWEBENCH_ROW]))
    assert [t.id for t in fleet.tasks] == ["abc123", "django__django-11099"]
    assert [t.id for t in added] == ["django__django-11099"]


# ---------------------------------------------------------------------------
# Edge cases from review


def test_a_callable_field_that_raises_names_the_row():
    def r2e_id(r):
        return f"{r['repo_name']}@{r['commit_hash'][:12]}"

    with pytest.raises(ValueError, match=r"index 1: id_field raised KeyError: 'commit_hash'") as exc:
        to_tasks([R2E_ROW, {"repo_name": "x", "image": "m"}], id_field=r2e_id)
    assert isinstance(exc.value.__cause__, KeyError)
    with pytest.raises(ValueError, match=r"index 0: image_field raised KeyError"):
        row_to_task({"id": "a"}, 0, image_field=lambda r: r["img"])


def test_jsonl_source_names_the_file_and_line_when_a_callable_raises(tmp_path):
    path = tmp_path / "r2e.jsonl"
    path.write_text(json.dumps(R2E_ROW) + "\n" + json.dumps({"repo_name": "x", "docker_image": "d"}) + "\n",
                    encoding="utf-8")
    source = JsonlSource(path, id_field=lambda r: f"{r['repo_name']}@{r['commit_hash']}")
    with pytest.raises(ValueError, match=r"r2e\.jsonl:2: Task row at index 1: id_field raised KeyError"):
        source.load()


def test_jsonl_source_names_the_file_and_line_of_invalid_utf8_and_reads_a_bom(tmp_path):
    path = tmp_path / "enc.jsonl"
    path.write_bytes(b'{"id": "a", "image": "m"}\n{"id": "b", "image": "\xff"}\n')
    with pytest.raises(ValueError, match=r"enc\.jsonl:2: not UTF-8"):
        JsonlSource(path).load()
    path.write_bytes(b'\xef\xbb\xbf{"id": "a", "image": "m"}\n')
    assert [t.id for t in JsonlSource(path).load()] == ["a"]


def test_jsonl_source_gives_the_row_position_among_tasks_not_the_line(tmp_path):
    path = tmp_path / "p.jsonl"
    path.write_text('\n\n{"image": "m"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"p\.jsonl:3: Task dict at index 0 has no"):
        JsonlSource(path).load()


def test_jsonl_source_with_limit_zero_still_needs_the_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        JsonlSource(tmp_path / "missing.jsonl", limit=0).load()


def test_a_blank_or_binary_id_is_no_id():
    assert row_to_task({"id": "   ", "instance_id": "x", "image": "m"}, 0).id == "x"
    assert row_to_task({"id": b"\xff\xfe", "instance_id": "x", "image": "m"}, 0).id == "x"
    assert row_to_task({"id": b"ok", "image": "m"}, 0).id == "ok"
    for blank in ("   ", b"\xff\xfe"):
        with pytest.raises(ValueError, match="no 'task_id' or 'id'"):
            row_to_task({"id": blank, "image": "m"}, 0)


def test_a_blank_literal_key_falls_through_to_the_descent_and_the_descent_unwraps_lists():
    row = {"metadata.docker_image": None, "metadata": {"docker_image": "x"}}
    assert extract_image(row, "metadata.docker_image") == "x"
    assert extract_image({"metadata.docker_image": "  ", "metadata": {"docker_image": "x"}}) == "x"
    assert extract_image({"metadata": [{"docker_image": "x"}]}) == "x"
    assert extract_image({"metadata": [{"docker_image": "x"}, {}]}) is None
    assert row_to_task({"meta": [{"uid": "u"}], "image": "m"}, 0, id_field="meta.uid").id == "u"


def test_an_empty_field_sequence_is_refused():
    with pytest.raises(ValueError, match="image_field is an empty sequence"):
        extract_image({"image": "m"}, [])
    with pytest.raises(ValueError, match="id_field is an empty sequence"):
        row_to_task({"id": "a", "image": "m"}, 0, id_field=())


def test_to_tasks_points_a_path_at_jsonl_source_and_refuses_a_load_that_is_not_callable(tmp_path):
    from types import SimpleNamespace

    for path in ("tasks.jsonl", tmp_path / "tasks.jsonl"):
        with pytest.raises(ValueError, match=r"pass JsonlSource\(path\)"):
            to_tasks(path)
    with pytest.raises(ValueError, match="load is not callable"):
        to_tasks(SimpleNamespace(load=1))


def test_rows_and_sources_may_be_generators():
    from ate_env_rl import FieldSpec  # noqa: F401  (exported for the fleet signatures)

    class GenSource:
        def load(self):
            yield Task(id="g", image="i")

    fleet = _fleet()
    fleet.load_tasks(r for r in [SWEBENCH_ROW])
    assert [t.id for t in fleet.add_tasks(GenSource())] == ["g"]
    assert [t.id for t in fleet.tasks] == ["django__django-11099", "g"]

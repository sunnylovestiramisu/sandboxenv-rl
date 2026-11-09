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

import threading

import pytest

from ate_env_rl.async_fleet import AsyncSandboxFleet
from ate_env_rl.config import FleetConfig
from ate_env_rl.fleet import SandboxFleet
from ate_env_rl.types import Task


def _fleet() -> SandboxFleet:
    return SandboxFleet(FleetConfig(backend="mock", max_warmpool_replicas=4))


def test_add_tasks_appends_instead_of_replacing():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    added = fleet.add_tasks([{"id": "b", "image": "img-b"}, Task(id="c", image="img-a")])
    assert [t.id for t in added] == ["b", "c"]
    assert all(isinstance(t, Task) for t in added)
    assert [t.id for t in fleet.tasks] == ["a", "b", "c"]
    assert fleet.image_counts() == {"img-a": 2, "img-b": 1}


def test_add_tasks_extends_an_existing_plan():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    plan = fleet.plan()
    assert plan.for_image("img-b") is None

    fleet.add_tasks([Task(id="b", image="img-b"), Task(id="c", image="img-a")])
    plan = fleet._plan
    assert plan.for_image("img-b") is not None
    assert plan.for_image("img-a").tasks == 2


def test_add_tasks_before_plan_does_not_plan():
    fleet = _fleet()
    fleet.add_tasks([Task(id="a", image="img-a")])
    assert fleet._plan is None
    assert fleet.plan().for_image("img-a") is not None


def test_add_tasks_with_nothing_is_a_no_op():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    assert fleet.add_tasks([]) == []
    assert len(fleet.tasks) == 1


def test_add_tasks_rejects_unsupported_items():
    with pytest.raises(ValueError, match="Unsupported"):
        _fleet().add_tasks([42])


def test_add_tasks_from_many_threads_keeps_every_task():
    fleet = _fleet()
    fleet.plan()

    def add(worker: int) -> None:
        for i in range(50):
            fleet.add_tasks([Task(id=f"{worker}-{i}", image=f"img-{i % 5}")])

    threads = [threading.Thread(target=add, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(fleet.tasks) == 400
    assert sum(e.tasks for e in fleet._plan.entries) == 400


async def test_async_add_tasks():
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock"))
    added = fleet.add_tasks([Task(id="a", image="img-a")])
    assert [t.id for t in added] == ["a"]
    assert fleet.image_counts() == {"img-a": 1}


@pytest.mark.parametrize("strategy", ["none", "naive", "sliding", "pipelined"])
@pytest.mark.parametrize("concurrency", [1, 3])
def test_add_tasks_during_run_does_not_break_the_run(strategy, concurrency):
    # Review repro: run() iterated the live task list, so a task added from
    # process_fn was reached past the end of results (IndexError).
    fleet = SandboxFleet(FleetConfig(backend="mock", strategy=strategy, batch_size=1))
    fleet.load_tasks([Task(id=f"t{i}", image=f"img-{i % 2}") for i in range(4)])
    added = []

    def process(task, handle):
        added.extend(fleet.add_tasks([Task(id=f"late-{task.id}", image="img-late")]))
        return task.id

    results = fleet.run(process, concurrency=concurrency)
    assert results == ["t0", "t1", "t2", "t3"]       # the added tasks are not run
    assert len(added) == 4
    assert len(fleet.tasks) == 8                       # but they are in the fleet


async def test_add_tasks_during_async_run_does_not_break_the_run():
    fleet = AsyncSandboxFleet(FleetConfig(backend="mock"))
    fleet.load_tasks([Task(id=f"t{i}", image="img") for i in range(3)])

    async def process(task, handle):
        fleet.add_tasks([Task(id=f"late-{task.id}", image="img")])
        return task.id

    assert await fleet.run(process) == ["t0", "t1", "t2"]
    assert len(fleet.tasks) == 6


def test_tasks_is_a_snapshot():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    snapshot = fleet.tasks
    fleet.add_tasks([Task(id="b", image="img-b")])
    snapshot.append(Task(id="c", image="img-c"))
    assert [t.id for t in snapshot] == ["a", "c"]
    assert [t.id for t in fleet.tasks] == ["a", "b"]


def test_load_tasks_recomputes_an_existing_plan():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    fleet.plan()
    fleet.load_tasks([Task(id="b", image="img-b")])
    assert fleet._plan.for_image("img-a") is None
    assert fleet._plan.for_image("img-b") is not None


def test_add_tasks_rejects_ids_already_in_the_fleet():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    with pytest.raises(ValueError, match=r"\['a'\]"):
        fleet.add_tasks([Task(id="b", image="img-b"), Task(id="a", image="img-other")])
    assert [t.id for t in fleet.tasks] == ["a"]        # nothing added
    assert fleet.acquire("a").task.image == "img-a"


def test_add_tasks_rejects_ids_repeated_in_one_call():
    fleet = _fleet()
    with pytest.raises(ValueError, match=r"\['x'\]"):
        fleet.add_tasks([Task(id="x", image="img-a"), Task(id="x", image="img-b")])
    assert fleet.tasks == []
    fleet.add_tasks([Task(id="x", image="img-a")])     # the id is still free


def test_load_tasks_resets_the_known_ids():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    fleet.load_tasks([Task(id="b", image="img-b")])
    fleet.add_tasks([Task(id="a", image="img-a")])     # "a" was replaced away
    assert [t.id for t in fleet.tasks] == ["b", "a"]


def test_load_tasks_rejects_repeated_ids_and_keeps_the_old_tasks():
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    with pytest.raises(ValueError, match=r"\['x'\]"):
        fleet.load_tasks([Task(id="x", image="img-1"), Task(id="y", image="img-2"),
                          Task(id="x", image="img-3")])
    assert [t.id for t in fleet.tasks] == ["a"]         # nothing replaced
    fleet.add_tasks([Task(id="x", image="img-1")])      # "x" was never loaded


@pytest.mark.parametrize("change", ["add_tasks", "load_tasks"])
def test_task_change_during_first_plan_is_in_the_plan(change):
    """plan() snapshots the tasks, a task change lands, then plan() stores.

    The change must still reach the plan: the replan decision waits for the
    plan() in progress instead of reading ``_plan is None`` and skipping.
    """
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    in_plan, resume = threading.Event(), threading.Event()
    real = fleet._ensure_template_for_image

    def paused(img):
        if img == "img-a" and not resume.is_set():
            in_plan.set()          # plan() holds _plan_lock and has its snapshot
            assert resume.wait(5)
        return real(img)

    fleet._ensure_template_for_image = paused
    planner = threading.Thread(target=fleet.plan)
    planner.start()
    assert in_plan.wait(5)

    new = [Task(id="a", image="img-a"), Task(id="b", image="img-b")]
    changer = threading.Thread(
        target=lambda: fleet.add_tasks(new[1:]) if change == "add_tasks" else fleet.load_tasks(new))
    changer.start()
    for _ in range(500):           # wait until the change is in the task list
        if "b" in {t.id for t in fleet.tasks}:
            break
        threading.Event().wait(0.01)
    assert "b" in {t.id for t in fleet.tasks}
    resume.set()
    planner.join(5)
    changer.join(5)
    assert not planner.is_alive() and not changer.is_alive()
    assert fleet._plan.for_image("img-b") is not None


def test_dicts_without_an_id_are_refused_with_a_clear_error():
    """Rows with no id field used to become id "None" (and now collide). SWE-bench
    rows keyed by instance_id are read by default (sources.DEFAULT_ID_FIELDS)."""
    fleet = _fleet()
    fleet.load_tasks([Task(id="a", image="img-a")])
    rows = [{"repo_name": "x1", "image": "i"}, {"repo_name": "x2", "image": "i"}]
    with pytest.raises(ValueError, match=r"index 0 has no 'task_id' or 'id'.*instance_id"):
        fleet.load_tasks(rows)
    assert [t.id for t in fleet.tasks] == ["a"]          # nothing replaced
    with pytest.raises(ValueError, match=r"index 1 has no 'task_id' or 'id'"):
        fleet.add_tasks([{"id": "b", "image": "img-b"}, {"repo_name": "x3", "image": "i"}])
    assert [t.id for t in fleet.tasks] == ["a"]          # nothing added
    fleet.add_tasks([{"instance_id": "x1", "image": "i"}])
    assert [t.id for t in fleet.tasks] == ["a", "x1"]


@pytest.mark.parametrize("row", [{"id": "", "image": "i"}, {"id": None, "image": "i"},
                                 {"task_id": "", "image": "i"}])
def test_empty_ids_are_refused(row):
    with pytest.raises(ValueError, match="no 'task_id' or 'id'"):
        _fleet().load_tasks([row])


def test_dict_ids_from_task_id_or_id():
    fleet = _fleet()
    fleet.load_tasks([
        {"task_id": "t1", "id": "ignored", "image": "i"},
        {"id": "t2", "image": "i"},
        {"id": 7, "image": "i"},
        {"task_id": 0, "id": "not-this", "image": "i"},   # 0 is a valid id, not missing
    ])
    assert [t.id for t in fleet.tasks] == ["t1", "t2", "7", "0"]
    assert fleet.tasks[1].metadata == {"id": "t2", "image": "i"}

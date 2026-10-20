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

import asyncio
import json

import pytest

from ate_env import telemetry
from ate_env.telemetry import EpisodeContext, MemorySink, NullSink


@pytest.fixture(autouse=True)
def _reset_recorder():
    yield
    telemetry.shutdown()


def test_unconfigured_recorder_is_disabled(monkeypatch):
    monkeypatch.delenv("RL_OBS_SINK", raising=False)
    telemetry.shutdown()
    rec = telemetry.get_recorder()
    assert not rec.enabled
    assert isinstance(rec.sink, NullSink)
    rec.emit(telemetry.ACQUIRE_STARTED, foo=1)  # no-op, no error


def test_emit_includes_static_context_and_attrs():
    sink = MemorySink()
    rec = telemetry.configure(sink, run_id="run-1", worker_id="roll-3",
                              static_attrs={"rl_sandbox.backend": "mock"})
    with telemetry.episode(batch_id=2, group_id="p7", trajectory_id="p7_g1"):
        rec.emit(telemetry.ACQUIRE_READY, wait_s=0.5, skipped=None)
    (r,) = sink.records
    assert r["event"] == telemetry.ACQUIRE_READY
    assert r["rl.run_id"] == "run-1"
    assert r["rl.worker_id"] == "roll-3"
    assert r["rl_sandbox.backend"] == "mock"
    assert r["rl.batch_id"] == 2
    assert r["rl.group_id"] == "p7"
    assert r["rl.trajectory_id"] == "p7_g1"
    assert r["wait_s"] == 0.5
    assert "skipped" not in r
    assert isinstance(r["ts"], float)


def test_episode_context_overrides_run_id_and_nests():
    sink = MemorySink()
    rec = telemetry.configure(sink, run_id="default-run")
    with telemetry.episode(run_id="harness-run", batch_id=1):
        with telemetry.episode(trajectory_id="t1"):
            rec.emit(telemetry.STEP_COMPLETED)
        rec.emit(telemetry.STEP_COMPLETED)
    rec.emit(telemetry.STEP_COMPLETED)
    a, b, c = sink.records
    assert a["rl.run_id"] == "harness-run" and a["rl.batch_id"] == 1 and a["rl.trajectory_id"] == "t1"
    assert b["rl.run_id"] == "harness-run" and "rl.trajectory_id" not in b
    assert c["rl.run_id"] == "default-run" and "rl.batch_id" not in c


def test_explicit_ctx_wins_over_current():
    sink = MemorySink()
    rec = telemetry.configure(sink)
    captured = EpisodeContext(trajectory_id="captured")
    with telemetry.episode(trajectory_id="current"):
        rec.emit(telemetry.RELEASED, ctx=captured)
    assert sink.records[0]["rl.trajectory_id"] == "captured"


@pytest.mark.asyncio
async def test_episode_context_follows_asyncio_tasks():
    sink = MemorySink()
    rec = telemetry.configure(sink)

    async def one(i):
        with telemetry.episode(trajectory_id=f"t{i}"):
            await asyncio.sleep(0)
            rec.emit(telemetry.STEP_COMPLETED, i=i)

    await asyncio.gather(*(one(i) for i in range(5)))
    got = {r["i"]: r["rl.trajectory_id"] for r in sink.records}
    assert got == {i: f"t{i}" for i in range(5)}


def test_jsonl_sink_writes_one_line_per_event(tmp_path):
    rec = telemetry.configure(f"jsonl:{tmp_path}", run_id="r", worker_id="w/1")
    for i in range(3):
        rec.emit(telemetry.STEP_COMPLETED, i=i)
    path = rec.sink.path
    telemetry.shutdown()
    lines = [json.loads(line) for line in open(path)]
    assert [r["i"] for r in lines] == [0, 1, 2]
    assert all(r["rl.worker_id"] == "w/1" for r in lines)
    assert path.startswith(str(tmp_path)) and "w_1" in path


def test_get_recorder_reads_environment(tmp_path, monkeypatch):
    telemetry.shutdown()
    monkeypatch.setenv("RL_OBS_SINK", f"jsonl:{tmp_path}")
    monkeypatch.setenv("RL_OBS_RUN_ID", "env-run")
    monkeypatch.setenv("RL_OBS_WORKER_ID", "env-worker")
    rec = telemetry.get_recorder()
    rec.emit(telemetry.ACQUIRE_STARTED)
    path = rec.sink.path
    telemetry.shutdown()
    (r,) = [json.loads(line) for line in open(path)]
    assert r["rl.run_id"] == "env-run" and r["rl.worker_id"] == "env-worker"


def test_unknown_sink_spec_raises():
    with pytest.raises(ValueError):
        telemetry.configure("kafka:somewhere")


def test_phase_records_duration_and_error():
    sink = MemorySink()
    telemetry.configure(sink)
    with telemetry.phase("trainer_step", step=4):
        pass
    with pytest.raises(RuntimeError):
        with telemetry.phase("weight_sync"):
            raise RuntimeError("boom")
    ok, failed = sink.events(telemetry.PHASE)
    assert ok["phase"] == "trainer_step" and ok["step"] == 4 and ok["duration_s"] >= 0
    assert "error.type" not in ok
    assert failed["phase"] == "weight_sync" and failed["error.type"] == "RuntimeError"

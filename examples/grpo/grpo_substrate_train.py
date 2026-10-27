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


"""
Real RL loop on CPU: GRPO (TRL) + Substrate actors as the reward executor.

Port of agent-sandbox's examples/stress/snapshot_ray/harness/grpo_sandbox_train.py
(vicentefb/agent-sandbox#7) to the Substrate env client (ate_env). Same loop,
same knobs; only the sandbox calls changed:

  agent-sandbox SDK                      ate_env
  ---------------------------------      ----------------------------------------
  client.create_sandbox(warmpool=...)    client.create(id, template_name=...)
  sb.files.write(name, program)          env.write_file(name, program)
  sb.commands.run("python name")         env.shell("python3 name", timeout=...)
  sb.suspend(snapshot_before_suspend)    client.suspend(id)        (GCS snapshot)
  sb.resume(wait_timeout)                first request wakes it (router parking);
                                         we send `true` and time it = wake latency
  sb.terminate()                         client.delete(id)         (suspend+delete)

Policy = Qwen2.5-0.5B-Instruct, task = MBPP (write a Python function).
Reward = fraction of the problem's unit tests that PASS when the model's code
runs inside a gVisor actor. No GPU required.

Env knobs (same names as the original where they exist):
  MODEL (Qwen/Qwen2.5-0.5B-Instruct)   MAX_STEPS (50)
  REWARD_SANDBOXES (8)                 NUM_GENERATIONS (4)
  MAX_COMPLETION (256)                 DATASET_SLICE (train[:200])
  SUSPEND_MODE (off | per_step)        ROLLOUT_LEN (0 = never recycle)
  EPISODE_LEDGER (0|1)                 BF16 (0|1)
Substrate-specific:
  ATE_ENV_ENDPOINT (localhost:7777)    ATE_TEMPLATE (grpo-template)
  ATE_ENV_ENDPOINTS (unset)  comma-separated NAME=HOST:PORT list; when set, the reward fleet is
                             spread round-robin across these clusters (one ate-env-api each)
  ATE_ATESPACE (ate-env)               RUN_ID (grpo-<pid>)  -> actor names RUN_ID-<i>
  EXEC_TIMEOUT (30)                    WAKE_RETRIES (5)  WAKE_TIMEOUT (120)
  OUT (/tmp/grpo-substrate-<RUN_ID>.jsonl)  per-step timing records
  SMOKE (0|1)  1 = no model: create fleet, one eval, suspend, wake, delete, exit
  STATE_MB (0)  MiB of touched memory held by a detached process in each actor (realistic working set)
Telemetry (docs/observability.md):
  REPORT_DIR (unset)  write the run report there at exit, <strategy>_<n>tasks_<YYYYMMDD-HHMMSS>.txt
                      and .json (strategy "grpo"); the summary is printed either way
  ENABLE_TRACING (0|1)  ate_env_fleet.* phase spans with the client's spans under them, over OTLP
                        (OTEL_EXPORTER_OTLP_ENDPOINT); needs ate-env-client[tracing]
  METRICS_PORT (0)  serve the ate_env_fleet_* and ate_env_client_* Prometheus series on this port;
                    needs ate-env-client[metrics]
"""

import asyncio
import contextvars
import json
import sys
import os
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.stdout.reconfigure(line_buffering=True)  # step lines show up live when logging to a file

MODEL = os.environ.get("MODEL", "Qwen/Qwen2.5-0.5B-Instruct")
MAX_STEPS = int(os.environ.get("MAX_STEPS", "50"))
REWARD_SANDBOXES = int(os.environ.get("REWARD_SANDBOXES", "8"))
NUM_GENERATIONS = int(os.environ.get("NUM_GENERATIONS", "4"))
MAX_COMPLETION = int(os.environ.get("MAX_COMPLETION", "256"))
SUSPEND_MODE = os.environ.get("SUSPEND_MODE", "off")
DATASET_SLICE = os.environ.get("DATASET_SLICE", "train[:200]")
ROLLOUT_LEN = int(os.environ.get("ROLLOUT_LEN", "0"))
EPISODE_LEDGER = os.environ.get("EPISODE_LEDGER", "0") == "1"

ATE_ENV_ENDPOINT = os.environ.get("ATE_ENV_ENDPOINT", "localhost:7777")
ATE_ENV_ENDPOINTS = os.environ.get("ATE_ENV_ENDPOINTS", "")  # multi-cluster: "A=localhost:7777,B=localhost:7778"
ATE_TEMPLATE = os.environ.get("ATE_TEMPLATE", "grpo-template")
ATE_ATESPACE = os.environ.get("ATE_ATESPACE", "ate-env")
RUN_ID = os.environ.get("RUN_ID", f"grpo-{os.getpid()}")
EXEC_TIMEOUT = float(os.environ.get("EXEC_TIMEOUT", "30"))
WAKE_RETRIES = int(os.environ.get("WAKE_RETRIES", "5"))
WAKE_TIMEOUT = float(os.environ.get("WAKE_TIMEOUT", "120"))
OUT = os.environ.get("OUT", f"/tmp/grpo-substrate-{RUN_ID}.jsonl")
SMOKE = os.environ.get("SMOKE", "0") == "1"
STATE_MB = int(os.environ.get("STATE_MB", "0"))  # resident-memory ballast per actor (0 = none)
REPORT_DIR = os.environ.get("REPORT_DIR", "")
ENABLE_TRACING = os.environ.get("ENABLE_TRACING", "0") == "1"
METRICS_PORT = int(os.environ.get("METRICS_PORT", "0"))

# ---------------- Substrate adapter ----------------------------------------
# ate_env is asyncio; TRL's reward function is sync and already fans out on a
# ThreadPoolExecutor. One background event loop owns the grpc.aio channel and
# every coroutine is submitted to it (grpc.aio objects must be created and
# used on the same loop).
from ate_env import Client
from ate_env.config import ObservabilityConfig, TracerConfig
from ate_env.errors import EnvError
from ate_env.loop_runner import LoopRunner
from ate_env.observability import Observer, serve_metrics

_out_lock = threading.Lock()

# ---------------- telemetry ---------------------------------------------------
# The always-on RunReport, plus Prometheus series and spans when asked for.
# Phases use the fleet's vocabulary: acquire (create + boot), process (one
# reward eval), suspend, wake, recycle, release (delete), teardown.
BACKEND = "substrate"
OBS = Observer(ObservabilityConfig(enable_metrics=METRICS_PORT > 0, enable_tracing=ENABLE_TRACING))
if METRICS_PORT:
    serve_metrics(METRICS_PORT)
_run_scope = OBS.run("grpo", backend=BACKEND)
_run_scope.__enter__()


def _submit(pool, fn, *args):
    """pool.submit that carries the caller's context, so worker-thread phase
    spans nest under the run span."""
    return pool.submit(contextvars.copy_context().run, fn, *args)


def _map(pool, fn, items):
    return [f.result() for f in [_submit(pool, fn, x) for x in items]]


def _finish_report():
    """End the run scope once; print the report and write it to REPORT_DIR."""
    global _run_scope
    if _run_scope is None:
        return
    _run_scope.__exit__(None, None, None)
    _run_scope = None
    print(OBS.report.summary())
    if REPORT_DIR:
        base = OBS.report.write(REPORT_DIR)  # n = reward evals run
        print(f"run report -> {base}.txt / .json")


def record(**kv):
    kv["t"] = time.time()
    with _out_lock:
        with open(OUT, "a") as f:
            f.write(json.dumps(kv) + "\n")


class SubstrateFleet:
    def __init__(self, endpoint, template, atespace, name="default"):
        self.name = name
        self.endpoint = endpoint
        self.template = template
        self.atespace = atespace
        # LoopRunner attaches the caller's trace context inside each coroutine.
        self.runner = LoopRunner(name=f"grpo-{name}")
        self.client = self._run(self._mk(endpoint))

    async def _mk(self, endpoint):
        if ENABLE_TRACING:
            return Client(endpoint, tracer_config=TracerConfig(
                enable_tracing=True, trace_service_name=OBS.config.trace_service_name))
        return Client(endpoint)

    def _rec(self, **kv):
        record(cluster=self.name, **kv)

    def _run(self, coro, timeout=None):
        return self.runner.run(coro, timeout)

    # -- lifecycle --
    def create(self, name):
        t0 = time.time()
        with OBS.phase("acquire", backend=BACKEND):
            env = self._run(self.client.create(name, atespace=self.atespace, template_name=self.template), 120)
            # creation registers the actor; the first command boots it (golden restore).
            self.wake(env, why="boot")  # first command = golden restore
            if STATE_MB:
                self.add_ballast(env, STATE_MB)
        OBS.claim(BACKEND)
        self._rec(ev="create", actor=name, wall=time.time() - t0, state_mb=STATE_MB)
        return env

    def add_ballast(self, env, mb):
        """Hold `mb` MiB of touched anonymous memory in a detached process, so every
        suspend/wake carries a realistic working set (an RL env is a repo + services +
        caches, not an idle shell). Touched page by page so it is really resident."""
        # Random bytes, not zeros: a zero-filled buffer compresses to nothing in the
        # memory image and understates suspend/wake by several x (seen Oct 12).
        prog = (
            "import os, time\n"
            f"n = {mb} * 1024 * 1024; b = bytearray(n); mv = memoryview(b); c = 64 * 1024 * 1024\n"
            "for off in range(0, n, c): mv[off:off + min(c, n - off)] = os.urandom(min(c, n - off))\n"
            "open('/tmp/ballast.ready', 'w').write(str(len(b)))\n"
            "time.sleep(10**9)\n"
        )
        self.write(env, "/tmp/ballast.py", prog)
        t0 = time.time()
        r = self._run(asyncio.wait_for(env.shell(
            "rm -f /tmp/ballast.ready; (nohup python3 /tmp/ballast.py >/dev/null 2>&1 &); "
            "for i in $(seq 1 600); do [ -f /tmp/ballast.ready ] && cat /tmp/ballast.ready && exit 0; sleep 0.1; done; echo TIMEOUT; exit 1"
        ), 120), 125)
        self._rec(ev="ballast", actor=env.id, mb=mb, wall=time.time() - t0, exit=r.exit_code)
        if r.exit_code != 0:
            raise RuntimeError(f"ballast on {env.id} failed: {r.stdout[-200:]} {r.stderr[-200:]}")

    def wake(self, env, why="wake"):
        """Send the cheapest possible request; the router parks it until the actor
        is up. Retries cover the two failure modes measured Oct 12: 503 when the
        5 s park budget expires under a wake burst, 502 when the guest is not yet
        listening. Returns wall seconds; raises after WAKE_RETRIES."""
        t0 = time.time()
        last = None
        for attempt in range(WAKE_RETRIES):
            try:
                r = self._run(asyncio.wait_for(env.shell("true"), WAKE_TIMEOUT), WAKE_TIMEOUT + 5)
                if r.exit_code == 0:
                    wall = time.time() - t0
                    self._rec(ev=why, actor=env.id, wall=wall, retries=attempt)
                    return wall
                last = RuntimeError(f"exit {r.exit_code}: {r.stderr[:200]}")
            except Exception as e:  # EnvError (Unavailable/502/503 mapped) or timeout
                last = e
            time.sleep(min(2 ** attempt, 10))
        self._rec(ev=why + "_failed", actor=env.id, wall=time.time() - t0, err=str(last)[:300])
        raise RuntimeError(f"wake {env.id} failed after {WAKE_RETRIES} tries: {last}")

    def suspend(self, env):
        t0 = time.time()
        with OBS.phase("suspend", backend=BACKEND):
            self._run(self.client.suspend(env.id, atespace=self.atespace), 600)
        self._rec(ev="suspend", actor=env.id, wall=time.time() - t0)

    def delete(self, env):
        try:
            with OBS.phase("release", backend=BACKEND):
                self._run(self.client.delete(env.id, atespace=self.atespace), 600)
        except EnvError as e:
            print(f"  delete {env.id}: {e}")

    # -- data plane --
    def write(self, env, name, text):
        self._run(env.write_file(name, text.encode()), 60)

    def write_run(self, env, name, program):
        self.write(env, name, program)
        # client-side timeout (older installed clients lack shell(timeout=)); a timed-out
        # eval leaves the process running in the guest, same as a hang would.
        return self._run(asyncio.wait_for(env.shell(f"python3 {name}"), EXEC_TIMEOUT), EXEC_TIMEOUT + 10)


class FleetRouter:
    """One reward fleet spread over several clusters. create() hands actors out
    round-robin across the clusters; every later call is routed to the cluster
    that owns the actor. This is the harness-side half of MC-1: the trainer
    sees one fleet, each cluster sees ordinary single-cluster traffic."""

    def __init__(self, fleets):
        self.fleets = fleets
        self.owner = {}
        self._i = 0
        self._lock = threading.Lock()

    def create(self, name):
        with self._lock:
            f = self.fleets[self._i % len(self.fleets)]
            self._i += 1
        env = f.create(name)
        self.owner[env.id] = f
        return env

    def _of(self, env):
        return self.owner[env.id]

    def wake(self, env, why="wake"):
        return self._of(env).wake(env, why)

    def suspend(self, env):
        return self._of(env).suspend(env)

    def delete(self, env):
        f = self.owner.pop(env.id, None)
        if f is not None:
            f.delete(env)

    def write(self, env, name, text):
        return self._of(env).write(env, name, text)

    def write_run(self, env, name, program):
        return self._of(env).write_run(env, name, program)


def _build_fleet():
    if ATE_ENV_ENDPOINTS:
        fleets = []
        for item in ATE_ENV_ENDPOINTS.split(","):
            item = item.strip()
            if not item:
                continue
            name, _, ep = item.partition("=")
            if not ep:
                name, ep = f"c{len(fleets) + 1}", name
            fleets.append(SubstrateFleet(ep, ATE_TEMPLATE, ATE_ATESPACE, name=name))
        return FleetRouter(fleets), ", ".join(f"{f.name}={f.endpoint}" for f in fleets)
    return SubstrateFleet(ATE_ENV_ENDPOINT, ATE_TEMPLATE, ATE_ATESPACE), ATE_ENV_ENDPOINT


fleet, _where = _build_fleet()
print(f"run {RUN_ID}: {REWARD_SANDBOXES} reward actors from template {ATE_TEMPLATE} via {_where}; records -> {OUT}")

sandboxes = []
_name_seq = {"n": 0}
_name_lock = threading.Lock()  # _claim_one runs on several workers at once


def _claim_one(label):
    for attempt in range(4):
        with _name_lock:
            _name_seq["n"] += 1
            name = f"{RUN_ID}-{_name_seq['n']}"
        try:
            return fleet.create(name)
        except Exception as e:
            print(f"  claim {label} attempt {attempt} ({name}) failed: {e}")
            time.sleep(5)
    raise RuntimeError(f"claim {label} failed after 4 attempts")


t_fleet = time.time()
with ThreadPoolExecutor(max_workers=REWARD_SANDBOXES) as _p:
    sandboxes = _map(_p, _claim_one, range(REWARD_SANDBOXES))
print(f"  fleet of {len(sandboxes)} ready in {time.time() - t_fleet:.1f}s")
free: "queue.Queue" = queue.Queue()
for sb in sandboxes:
    free.put(sb)
pool = ThreadPoolExecutor(max_workers=REWARD_SANDBOXES)
fleet_suspended = False

# The model's code is loaded from its own file inside a try/except so a syntax
# error in it cannot skip the ledger prelude or the result marker. (The upstream
# harness concatenated code + tests into one file; a syntax error then aborted the
# whole program before the ledger ran, which shows up as a false "state loss".)
HARNESS = """
__code_ok__ = True
try:
    exec(compile(open({code_file!r}).read(), "model_code", "exec"), globals())
except BaseException:
    __code_ok__ = False
    print("__CODE_ERR__")
__passed__ = 0
__tests__ = {tests!r}
if __code_ok__:
    for __t__ in __tests__:
        try:
            exec(__t__, globals())
            __passed__ += 1
        except Exception:
            pass
print("__RESULT__", __passed__, len(__tests__))
"""


def extract_code(completion) -> str:
    if isinstance(completion, list):  # conversational format
        completion = completion[0]["content"]
    m = re.search(r"```(?:python)?\n(.*?)```", completion, re.DOTALL)
    return (m.group(1) if m else completion).strip()


_heal_lock = threading.Lock()
_comm_fails = {}
_eval_counts = {}


def _heal(sb):
    """Delete a wedged actor and create a replacement (one-strike, as upstream)."""
    global sandboxes
    with _heal_lock:
        fleet.delete(sb)
        new_sb = _claim_one("heal")
        sandboxes = [new_sb if x is sb else x for x in sandboxes]
        record(ev="heal", old=sb.id, new=new_sb.id)
        print(f"  [healer] replaced {sb.id} with {new_sb.id}")
        return new_sb


def run_in_sandbox(code: str, tests: list, tag: str) -> float:
    sb = free.get()
    try:
        code_file = f"/tmp/code_{tag}.py"
        program = HARNESS.format(code_file=code_file, tests=list(tests))
        if EPISODE_LEDGER:
            expected = _eval_counts.get(sb.id, 0)
            prelude = (
                "import os as _o\n"
                "_l = '/tmp/episode_ledger.bin'\n"
                "_prev = _o.path.getsize(_l) if _o.path.exists(_l) else 0\n"
                f"print('__LEDGER__', _prev, {expected})\n"
                "open(_l, 'ab').write(b'x')\n"
            )
            program = prelude + program
        name = f"/tmp/eval_{tag}.py"
        t0 = time.time()
        try:
            with OBS.phase("process", backend=BACKEND):
                fleet.write(sb, code_file, code)
                r = fleet.write_run(sb, name, program)
        except Exception:
            OBS.task_done(BACKEND, "-", "error", time.time() - t0)
            raise
        OBS.task_done(BACKEND, "-", "ok", time.time() - t0)
        m = re.search(r"__RESULT__ (\d+) (\d+)", r.stdout or "")
        lm = re.search(r"__LEDGER__ (\d+) (\d+)", r.stdout or "")
        if lm:
            # The prelude ran and appended its byte, so the next expectation is
            # what the guest saw plus one. A failure before this point leaves
            # the count untouched, and a mismatch is reported once rather than
            # at every later eval on the same actor.
            _eval_counts[sb.id] = int(lm.group(1)) + 1
            if lm.group(1) != lm.group(2):
                print(f"  LEDGER MISMATCH [{tag}] on {sb.id}: found {lm.group(1)} expected {lm.group(2)}")
                record(ev="ledger_mismatch", actor=sb.id, found=int(lm.group(1)), expected=int(lm.group(2)))
        _comm_fails[sb.id] = 0
        record(ev="eval", actor=sb.id, tag=tag, wall=time.time() - t0, exit=r.exit_code, ok=bool(m))
        if not m or "__CODE_ERR__" in (r.stdout or ""):
            return 0.0  # model code did not compile/run, or the harness crashed
        passed, total = int(m.group(1)), int(m.group(2))
        return 0.1 + 0.9 * passed / max(total, 1)
    except Exception as e:
        print(f"  reward-exec error [{tag}] on {sb.id}: {e}")
        record(ev="eval_error", actor=sb.id, tag=tag, err=str(e)[:300])
        n = _comm_fails.get(sb.id, 0) + 1
        _comm_fails[sb.id] = n
        if n >= 1:
            try:
                sb = _heal(sb)
            except Exception as he:
                print(f"  [healer] FAILED: {he}")
        return 0.0
    finally:
        free.put(sb)


def _suspend_one(sb):
    try:
        fleet.suspend(sb)
    except Exception as e:
        print(f"  suspend {sb.id} failed: {e}")
        record(ev="suspend_failed", actor=sb.id, err=str(e)[:300])


def _resume_one(sb):
    try:
        with OBS.phase("wake", backend=BACKEND):
            fleet.wake(sb, why="resume")
    except Exception as e:
        print(f"  resume {sb.id} failed: {e}")
        record(ev="resume_failed", actor=sb.id, err=str(e)[:300])


step_no = {"n": 0}


def sandbox_reward(prompts, completions, test_list=None, **kwargs):
    """TRL reward function: one score per completion, from actor execution."""
    global fleet_suspended, free, sandboxes
    step_no["n"] += 1
    tag = step_no["n"]
    step_rec = {"ev": "step", "step": tag}

    if ROLLOUT_LEN and tag > 1 and (tag - 1) % ROLLOUT_LEN == 0:
        t0 = time.time()
        with OBS.phase("recycle", backend=BACKEND):
            _map(pool, fleet.delete, sandboxes)
            with ThreadPoolExecutor(max_workers=REWARD_SANDBOXES) as p:
                sandboxes = _map(p, _claim_one, range(REWARD_SANDBOXES))
        _eval_counts.clear()  # fresh actors, fresh ledgers
        _comm_fails.clear()
        free = queue.Queue()
        for sb in sandboxes:
            free.put(sb)
        fleet_suspended = False
        step_rec["recycle_wall"] = time.time() - t0
        print(f"[step {tag}] recycled fleet (delete+create {REWARD_SANDBOXES}) in {time.time() - t0:.1f}s")

    if fleet_suspended:
        t0 = time.time()
        _map(pool, _resume_one, sandboxes)
        fleet_suspended = False
        step_rec["resume_wall"] = time.time() - t0
        print(f"[step {tag}] resumed {len(sandboxes)} actors in {time.time() - t0:.1f}s")

    t0 = time.time()
    codes = [extract_code(c) for c in completions]
    futs = [_submit(pool, run_in_sandbox, c, t, f"{tag}_{i}") for i, (c, t) in enumerate(zip(codes, test_list))]
    rewards = [f.result() for f in futs]
    step_rec["eval_wall"] = time.time() - t0
    step_rec["n_evals"] = len(rewards)
    step_rec["reward_mean"] = sum(rewards) / max(len(rewards), 1)
    print(f"[step {tag}] {len(rewards)} actor evals in {time.time() - t0:.1f}s rewards={[f'{r:.2f}' for r in rewards]}")

    if SUSPEND_MODE == "per_step":
        t0 = time.time()
        _map(pool, _suspend_one, sandboxes)
        fleet_suspended = True
        step_rec["suspend_wall"] = time.time() - t0
        print(f"[step {tag}] suspended fleet in {time.time() - t0:.1f}s")
    record(**step_rec)
    return rewards


def _cleanup():
    print("deleting reward actors...")
    t0 = time.time()
    with OBS.phase("teardown", backend=BACKEND):
        _map(pool, fleet.delete, sandboxes)
    record(ev="cleanup", n=len(sandboxes), wall=time.time() - t0)
    print(f"  deleted {len(sandboxes)} in {time.time() - t0:.1f}s")
    _finish_report()


# ---------------- smoke: wiring only, no model ------------------------------
if SMOKE:
    try:
        good = "def add(a, b):\n    return a + b\n"
        r = run_in_sandbox(good, ["assert add(1, 2) == 3", "assert add(0, 0) == 0"], "smoke_ok")
        bad = "def add(a, b):\n    return a - b\n"
        r2 = run_in_sandbox(bad, ["assert add(1, 2) == 3", "assert add(0, 0) == 0"], "smoke_bad")
        print(f"smoke rewards: good={r:.2f} (expect 1.00) bad={r2:.2f} (expect 0.55)")
        t0 = time.time(); _map(pool, _suspend_one, sandboxes); print(f"smoke suspend fleet: {time.time() - t0:.1f}s")
        t0 = time.time(); _map(pool, _resume_one, sandboxes); print(f"smoke wake fleet:    {time.time() - t0:.1f}s")
        r3 = run_in_sandbox(good, ["assert add(1, 2) == 3"], "smoke_after_wake")
        print(f"smoke after wake: {r3:.2f} (expect 1.00)")
    finally:
        _cleanup()
    raise SystemExit(0)

# ---------------- dataset ---------------------------------------------------
from datasets import load_dataset

ds = load_dataset("google-research-datasets/mbpp", split=DATASET_SLICE)


def to_prompt(row):
    tests = "\n".join(row["test_list"])
    user = (
        "You are an expert Python programmer. Write a Python function for this "
        "task. Output ONLY the code, no explanations.\n"
        f"Task: {row['text']}\n"
        f"Your code must pass these tests:\n{tests}\n"
    )
    return {"prompt": [{"role": "user", "content": user}], "test_list": row["test_list"]}


ds = ds.map(to_prompt, remove_columns=[c for c in ds.column_names if c != "test_list"])

# ---------------- trainer ---------------------------------------------------
from trl import GRPOConfig, GRPOTrainer

cfg = GRPOConfig(
    output_dir="/tmp/grpo-substrate",
    learning_rate=2e-6,
    beta=0.04,
    per_device_train_batch_size=NUM_GENERATIONS,
    gradient_accumulation_steps=2,
    num_generations=NUM_GENERATIONS,
    max_prompt_length=512,
    max_completion_length=MAX_COMPLETION,
    max_steps=MAX_STEPS,
    logging_steps=1,
    save_strategy="no",
    report_to="none",
    temperature=0.9,
    bf16=os.environ.get("BF16", "0") == "1",
    model_init_kwargs={"torch_dtype": "bfloat16", "device_map": "auto"} if os.environ.get("BF16", "0") == "1" else None,
    fp16=False,
)

trainer = GRPOTrainer(model=MODEL, reward_funcs=sandbox_reward, args=cfg, train_dataset=ds)

try:
    trainer.train()
finally:
    _cleanup()

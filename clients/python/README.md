# ate-env-rl — Python Client & Sandbox Fleet SDK

> [!WARNING]
> This is an alpha API and is likely to change until v1.0 is released.

Python client library for the [Agent Substrate Environment](../../README.md)
API (`ate-env-api`):
1. **Single Environment Lifecycle & Guest Operations**: `Client` and `Env` for fine-grained gRPC control, process execution, and file streaming.
2. **Fleet & Sandbox Orchestration**: `SandboxFleet` for managing batches of sandboxes with pool sizing, pre-warming, and execution strategies.

Requires Python >= 3.10, but only 3.12 and later are locked and tested: the
repository's `uv.lock` resolves for 3.12+ (the OpenHands integration needs
it), and CI runs the client's tests on 3.13. Everything is `asyncio`-native:
methods are coroutines, log/file streams are async iterators, and cancellation
works through standard task cancellation and `asyncio.timeout()`.

## How it works

The client talks to a single endpoint — the `ate-env-api` service — over
plain gRPC (h2c). Behind that endpoint there are two distinct paths:

```
       ate_env_rl (this package)                                    cluster
 ╭────────────────────────────╮
 │ Client                     │ EnvironmentService ╭───────────────╮ lifecycle  ╭────────╮
 │  create() get() suspend()  │───────────────────▶│               │───────────▶│ ateapi │
 │  delete() env()            │      (unary)       │               │            ╰────────╯
 ╰────────────────────────────╯                    │  ate-env-api  │ Substrate control plane
 ╭────────────────────────────╮                    │ (guest proxy) │
 │ Env / Process              │ ProcessService     │               │ x-env-id   ╭────────╮   ╭──────────────────╮
 │  shell() start_process()   │ FileSystemService  │               │───────────▶│ atenet │──▶│ actor            │
 │  output() write_input()    │───────────────────▶│               │  routing   │ router │   │ └ ate-env-guest  │
 │  signal() wait()           │ + routing metadata ╰───────────────╯            ╰────────╯   ╰──────────────────╯
 │  read_file() write_file()  │
 ╰────────────────────────────╯
```

**Lifecycle path.** `Client.create/get/suspend/delete` call
`EnvironmentService` (defined in [`proto/ateenv/v1alpha/env.proto`](../../proto/ateenv/v1alpha/env.proto)).
These RPCs terminate at `ate-env-api`, which translates them into Substrate
control-plane operations: creating an actor from an ActorTemplate,
reading its status, checkpointing it to a snapshot, deleting it.

**Guest path.** Everything on an `Env` handle that executes *inside* the
environment — processes and files — calls `ProcessService` and
`FileSystemService` (defined in [`proto/ateenv/v1alpha/guest.proto`](../../proto/ateenv/v1alpha/guest.proto)).
The client attaches `x-env-id` / `x-env-atespace` gRPC metadata to each of
these calls; `ate-env-api` uses that metadata to dial the atenet router
with the authority `<id>.<atespace>.<host-suffix>`, and the router carries
the request into the right actor, where the `ate-env-guest` daemon serves
it. The proxy is transparent: streaming responses (logs, file chunks)
flow end-to-end without buffering.

If the environment is suspended, routing traffic to it wakes it up —
Substrate resumes the actor from its latest snapshot on demand. There is
no explicit resume API; the first guest call after a suspend (or after
create) does the waking, and may take noticeably longer or fail while the
actor boots. Retry until it serves (see the how-to below).

> [!IMPORTANT]
> The guest path requires a Substrate deployment that carries gRPC
> (HTTP/2 + trailers) through the atenet router to actors — i.e.
> [substrate PR #1183](https://github.com/agent-substrate/substrate/pull/1183)
> ("atenet: h2 on the HTTPS ingress and mirror the protocol to actors").
> Without it, every process/file operation fails with
> `server closed the stream without sending trailers`, because the
> router's actor upstream is pinned to HTTP/1.1, which drops gRPC
> trailers. Environment lifecycle operations are unaffected.

### What calls where

| Client call | RPC | Handled by |
| --- | --- | --- |
| `Client.create()` | `EnvironmentService.CreateEnvironment` | ate-env-api → control plane |
| `Client.get()` / `Env.info()` | `EnvironmentService.GetEnvironment` | ate-env-api → control plane |
| `Client.suspend()` / `Env.suspend()` | `EnvironmentService.SuspendEnvironment` | ate-env-api → control plane |
| `Client.delete()` / `Env.delete()` | `EnvironmentService.DeleteEnvironment` | ate-env-api → control plane |
| `Client.server_info()` | `EnvironmentService.GetServerInfo` | answered by ate-env-api |
| `Client.env()` | *(no RPC — returns a handle)* | — |
| `Env.start_process()` | `ProcessService.StartProcess` | proxied to guest |
| `Env.process()` | *(no RPC — returns a handle)* | — |
| `Process.info()` | `ProcessService.GetProcess` | proxied to guest |
| `Process.output()` / `Process.wait()` | `ProcessService.StreamProcessOutput` *(server-streaming)* | proxied to guest |
| `Process.write_input()` / `close_input()` | `ProcessService.WriteProcessInput` *(client-streaming)* | proxied to guest |
| `Process.signal()` / `Process.kill()` | `ProcessService.SignalProcess` | proxied to guest |
| `Env.shell()` | `StartProcess` (+ `WriteProcessInput`) + `StreamProcessOutput` | proxied to guest |
| `Env.read_file()` / `read_file_bytes()` | `FileSystemService.ReadFile` *(server-streaming)* | proxied to guest |
| `Env.write_file()` | `FileSystemService.WriteFile` *(client-streaming)* | proxied to guest |

`Env.shell()` is a convenience composed from the process primitives: it
starts `sh -c <command>`, feeds it stdin if given, and follows the output
stream, which ends with the process's exit state.

## How-to

### Install

From a repo checkout:

```bash
pip install ./clients/python
```

The distribution is named `ate-env-rl`; the import package is
`ate_env_rl` (mirroring the Go client, which lives at `clients/go`).

### Connect

You need a reachable `ate-env-api`. On a cluster deployed with
`ate-env manifest`, port-forward it:

```bash
kubectl -n ate-env port-forward svc/ate-env-api 7777:7777
```

Then create a client. The connection is plain gRPC (no TLS), matching
the Go client and CLI. A single `Client` multiplexes any number of
concurrent operations over one HTTP/2 channel and is safe to share
across tasks — for long-lived programs (servers, multi-agent
orchestration), create one client, share it, and close it on shutdown:

```python
from ate_env_rl import Client

client = Client("localhost:7777")
try:
    env = await client.create("dev1")
    ...
finally:
    await client.close()
```

`Client(...)` accepts `host:port` or an `http://` URL. To manage the
channel yourself (e.g. custom gRPC options), pass
`Client(channel=your_grpc_aio_channel)` — the client then never closes it.

### Create an environment

```python
env = await client.create("dev1")
```

The server instantiates the environment from the `default-template`
ActorTemplate in the `ate-env` atespace unless you override it:

```python
env = await client.create("dev1", template_name="my-template",
                          template_atespace="my-atespace")
```

To run an arbitrary OCI image, pass it pinned by digest. The server derives
an ActorTemplate from the template above, which acts as the base: the image
becomes the container and the `ate-env-guest` is mounted into it as a
read-only image volume, so the image runs unmodified. The derived template
is created on first use and shared by every environment on that image:

```python
env = await client.create("py1", rootfs_image="docker.io/library/python@sha256:…")
# Keep it on the worker pools that carry a label (ANDed with the template's selector);
# info.node is the node it is on once placed, info.worker_selector what it asked for.
env = await client.create("t1", rootfs_image="…@sha256:…", worker_selector={"ate-env/domain": "d0"})
info = await client.get("t1")
print((await env.info()).template.name)   # default-template-<12 hex of the digest>
```

To get a handle to an environment that already exists (no RPC is made):

```python
env = client.env("dev1")            # atespace defaults to "ate-env"
```

A freshly created (or suspended) environment starts serving on first
contact. Gate on readiness by retrying a trivial command:

```python
from ate_env_rl import EnvError, SandboxStartError

async def wait_until_serving(env, timeout=180):
    async with asyncio.timeout(timeout):
        while True:
            try:
                if (await env.shell("true")).exit_code == 0:
                    return
            except SandboxStartError:
                raise  # crashed, or no room: waiting will not help
            except EnvError:
                pass
            await asyncio.sleep(2)
```

### Ask about an image before creating from it

`image_status` reports the ActorTemplate derived for a digest-pinned image and the
state of its golden snapshot, the snapshot every environment on that image restores
from instead of booting: `NONE` (no template, or the template disables golden
snapshots), `PENDING` (baking; environments created now boot from the image),
`READY` (with the tag) or `FAILED` (with the reason). `ensure=True` creates the
template when it is missing, which starts the bake, and creates no environment.

```python
from ate_env_rl import GoldenSnapshotState

status = await client.image_status(image, ensure=True)   # starts the bake if needed
while status.golden is GoldenSnapshotState.PENDING:
    await asyncio.sleep(10)
    status = await client.image_status(image)
assert status.golden is GoldenSnapshotState.READY, status.golden_error
```

The template and atespace default as in `create`; `server_info().features` lists
`image_status` when the server serves it.

### Run shell commands

```python
result = await env.shell("echo hello && uname -a")
print(result.exit_code)   # int; 128 + signal number if killed by a signal
print(result.stdout)      # str (utf-8, invalid bytes replaced)
print(result.stderr)

result = await env.shell("tr a-z A-Z", stdin="shout\n", timeout=30)
```

`shell()` buffers all output in memory and returns after the command
exits. `stdin` (bytes or str) is fed to the command and then closed;
`timeout` (seconds or `timedelta`) kills the command with SIGKILL when
it elapses. For long-running, chatty, or interactive commands, use the
process API instead.

### Run background processes and stream output

```python
proc = await env.start_process(
    ["python", "train.py"],
    cwd="/workspace",
    env={"EPOCHS": "10"},
    timeout=3600,
)

# Follow output live; the last message carries the exit state:
async for out in proc.output(follow=True):
    if out.stdout is not None:
        print(out.stdout.decode(), end="")
    elif out.stderr is not None:
        print(out.stderr.decode(), end="", file=sys.stderr)
    else:
        print("exited:", out.exit.exit_code)

info = await proc.wait()             # or: block until exit without reading output
info = await proc.info()             # snapshot: state, pid, exit_code, timestamps
```

Notes:

- `output(follow=True)` blocks until the process exits — consume it
  under `asyncio.timeout()` or in a task you can cancel. Breaking out of
  the `async for` cancels the underlying RPC cleanly.
- Replay from a byte offset with `stdout_offset=` / `stderr_offset=`;
  without `follow` you get the output written so far and the stream ends
  (with an `exit` message only if the process has already exited).
- `env.process(process_id)` returns a handle to a process started earlier.

### Feed stdin and send signals

```python
proc = await env.start_process(["python", "-i"], stdin=True)
await proc.write_input(b"print(6 * 7)\n")      # stdin stays open across calls
await proc.write_input(b"exit()\n", close=True) # close=True sends EOF

server = await env.start_process(["./serve"])
await server.signal(Signal.HUP)                # any POSIX signal, to the process group
await server.signal(Signal.TERM)
info = await server.wait()
assert info.exit_code == 143                   # 128 + SIGTERM

info = await server.kill()                     # SIGKILL + wait; idempotent
```

Signals and stdin need a running process; once it has exited they raise
`ProcessExitedError`. `write_input()` on a process started without
`stdin=True`, or after `close=True`, raises `FailedPreconditionError`.

### Read and write files

Paths are absolute or relative to the environment's workspace. Reads and
writes stream in 64 KiB chunks, so file size is not bounded by memory:

```python
# Small files, in one call:
await env.write_file("/workspace/config.json", b'{"debug": true}\n')
data = await env.read_file_bytes("/workspace/config.json")

# Large files, streamed:
async for chunk in env.read_file("/workspace/results.bin"):
    process(chunk)

async def produce():
    for shard in shards:
        yield shard.to_bytes()

await env.write_file("/workspace/dataset.bin", produce(), mode=0o600)
```

`write_file` accepts `bytes`/`bytearray`/`memoryview` (chunked for you)
or any sync/async iterable of bytes chunks (sent as-is). Writing `b""`
creates an empty file. By default the file is replaced; pass a positive
`seek_offset=` to keep existing content and write starting at that byte
(the file is zero-extended if it is shorter):

```python
await env.write_file("/workspace/data.bin", patch, seek_offset=4096)
```

### Suspend and delete

```python
await env.suspend()                        # checkpoint to object storage and free the worker
await env.suspend(mode=SuspendMode.LOCAL)  # checkpoint to the node and free the worker; the upload follows
await env.delete()                         # remove permanently, in any state
```

A suspended environment keeps its filesystem and can be woken again just
by sending it guest traffic. `SuspendMode.LOCAL` returns once the snapshot is
on the worker's node and leaves the upload to object storage in the
background: the environment resumes on that node until the upload lands, its
status is `PAUSED` meanwhile, and a node lost before then takes the snapshot
with it. One park-and-resume cycle is cheaper this way, which is what a loop
that parks a sandbox between every command wants. The default,
`SuspendMode.DURABLE`, returns once the snapshot is in object storage and puts
no mode on the wire, so it works against an ate-env-api that predates the
field; such a server refuses `LOCAL` with `InvalidArgumentError`, and
`server_info().features` lists `suspend_mode` once a server serves it.
Deletion is permanent.

### Warm the nodes of a worker pool before a run

An image's first boot on a node pulls and unpacks the image and fetches the
golden snapshot; the boots after it restore in seconds. `warm_image()` pays
that first boot ahead of the run, on every node of one worker pool, through
ate-env-api: the server creates probe environments with the worker selector,
holds them until they run, reads the node each landed on, deletes them, and
repeats until every node has run the image. `list_placement_domains()` names
the pools, with the selector each one answers to; `template_name=` keeps the
pools that template's own worker selector matches, the ones a run on it can
address:

```python
domains = await client.list_placement_domains()          # needs the global viewer role
pool = next(d for d in domains if d.name == "pool-d0")
coverage = await client.warm_image(
    image, worker_selector=pool.selector, run_id="pf-0",
    on_probe=lambda p: print(p.id, p.node or p.error),
)
if not coverage.complete:
    print(f"{len(coverage.nodes)}/{coverage.wanted} nodes: {coverage.error}")
```

Pass `nodes=` when ate-env-api may not list workers (`ate-env manifest
access-policy --global` grants it). The server's defaults apply to the knobs
left 0: 2 probes per node, 3 rounds, a 10 minute probe timeout. Wait for the
golden with `image_status()` first when the run relies on it. The call lasts
for the rounds, minutes for a cold image, so give it a timeout above
`probe_timeout x max_rounds`; `coverage.waited` is the time its rounds spent
waiting for probe slots the server caps per node across calls. Sweep
`coverage.undeleted_probes`, the probe environments the server could not
delete (a probe whose resume failed ends `CRASHED`, which Substrate cannot
delete today), with `delete()` once the control plane takes the delete.

### Handle errors

All failures raise subclasses of `ate_env_rl.EnvError`:

| Exception | gRPC code | Typical cause |
| --- | --- | --- |
| `NotFoundError` | `NOT_FOUND` | unknown environment, process id, or file path |
| `InvalidArgumentError` | `INVALID_ARGUMENT` | empty id, missing routing metadata, empty command |
| `PermissionDeniedError` | `PERMISSION_DENIED` | file path escapes the workspace sandbox |
| `FailedPreconditionError` | `FAILED_PRECONDITION` | stdin not opened or already closed |
| `ProcessExitedError` | `FAILED_PRECONDITION` | signal or stdin write to a process that has exited (subclass of `FailedPreconditionError`) |
| `SandboxStartError` | any, with the `ate-env-start-failure` trailer | the environment cannot start: it crashed while waking (`.reason == "crashed"`, `FAILED_PRECONDITION`) or no worker had room for it (`"no-capacity"`, `RESOURCE_EXHAUSTED`); retryable, on a fresh environment |
| `RpcError` | anything else | transport failures, `ALREADY_EXISTS`, actor still waking, … (`.code` holds the status) |

```python
from ate_env_rl import NotFoundError

try:
    data = await env.read_file_bytes("/no/such/file")
except NotFoundError:
    data = None
```

### Share one run id across a job's processes

Each `SandboxFleet` gets a random `run_id`. A training or eval job usually has
several processes (an orchestrator that plans and warms, workers that claim and
step), so give them all the job's id:

```python
cfg = FleetConfig(run_id=os.environ["JOB_PREFIX"])   # a DNS-1123 label
fleet = SandboxFleet(cfg)                             # same run_id in every process
```

Handles (`handle.run_id`), the warm-pool environments that `setup()` and
`warm_images()` create, `ate_env.run_id` on spans, and the run report's
`environment` block then all carry the job's id. `"warm-pool"` is reserved: it
is the run id of warm environments created by calling a driver's
`warm_pool()` without one.

The run id stays in the client. It is not sent to ate-env-api, and environment
names (`sb-…`, `warm-…`) don't contain it. So `teardown()` still releases only
the environments that this process created and tracks. An orchestrator can't
reap a crashed worker's environments by run id. Reaping across processes needs
server-side labels and listing in ate-env-api, which don't exist yet.

### Concurrency

One `Client` multiplexes any number of concurrent operations over a
single HTTP/2 connection — handles and methods are safe to use from
multiple tasks:

```python
results = await asyncio.gather(
    env.shell("make test"),
    env.shell("make lint"),
    other_env.shell("make build"),
)
```

### Grow the task list while the job runs

`load_tasks()` replaces the fleet's tasks. A worker that is handed an episode
nobody planned, or a trainer that streams batches from a prompt source, calls
`add_tasks()` instead: it appends, returns the new `Task` objects and, once
`plan()` has run, extends the plan with their images. It warms nothing, and
a `run()` already in progress does not run the added tasks (`fleet.tasks` is
a snapshot). Task ids must be new: `add_tasks` raises `ValueError` and adds
nothing if an id is already in the fleet or repeats within the call.
`load_tasks()` follows the same rule within its list: it raises `ValueError`
and keeps the tasks loaded before if an id repeats. It also recomputes an
existing plan.

Both take `Task` objects, dataset rows (dicts), or a task source.

A row gives its image and id from `image_field` and `id_field`. Each takes one
dotted path (`"metadata.docker_image"`), several paths tried in order, or a
callable `row -> value`. By default:

- the image comes from `image`, then the R2E-Gym and SWE-bench keys
  (`docker_image`, `image_name`, the same under `metadata`, and under the
  `env_config.entry` a trainer wraps a row in). A one-element list, as a
  columnar dataset returns, is unwrapped.
- the id comes from `task_id`, `id` or `instance_id`. A blank or binary
  value is skipped.

A dotted path reads a literal key of that name first, such as a flattened
`metadata.docker_image` column, and descends into `metadata` when that key
is missing or blank. A field you name replaces the defaults: it does not fall
back to them.

The whole row becomes `Task.metadata`, and its `template` key, when set, the
task's ActorTemplate. A row with no image or no id, or whose callable field
raises, raises `ValueError` with the row's position and what was tried. The
fleet never makes up an id from a row's position, because `acquire()`,
results and reports need ids that name the row in your data.

A source is any object with a `load()` method that returns `Task` objects
(`TaskSource`): `ListSource(items, image_field=, id_field=)` over a list, and
`JsonlSource(path, image_field=, id_field=, limit=)` over a UTF-8 JSON-lines
file, which names the file and line of a bad row. `load()` is synchronous:
`AsyncSandboxFleet.load_tasks` runs it on the event loop thread, so for a
remote source (`gs://`, Hugging Face) load it on a thread and pass the list:
`tasks = await asyncio.to_thread(source.load)`, then `fleet.load_tasks(tasks)`.

Two more sources read from outside the process:

- `SweBenchSource(dataset=, split=, limit=, offset=, keep_row=, revision=, name=)`
  reads a SWE-bench-style Hugging Face dataset (default
  `R2E-Gym/SWE-Bench-Verified`), reading only the `offset`/`limit` slice into
  memory; `datasets` still downloads and prepares the whole config. `split` is
  a plain name such as `test`, and an `offset` past the end reads no tasks. It
  needs the `datasets` package (`pip install datasets`), imported when `load()`
  runs. `keep_row=True` also stores a copy of the row under
  `Task.metadata["ds"]`, which the R2E-Gym adapter (`ate_env_rl.adapters.r2egym`) needs.
- `ImageListSource(path)` reads an image manifest, one image per line with `#`
  comments; each task's id is its image.

`JsonlSource` and `ImageListSource` take a local path or a `gs://` object.
`gs://` is read with `google-cloud-storage` when it is installed, else with
`gcloud storage cat` or `gsutil cat`; `read_bytes(path)` does the same for your
own files. A `gs://` object is downloaded whole: `limit` does not bound the
transfer. An object name with `*`, `?` or `[` is read with
`google-cloud-storage` only, since the command line would expand it.

```python
from ate_env_rl import ImageListSource, JsonlSource, SweBenchSource

def r2e_id(row):
    return f"{row['repo_name']}@{row['commit_hash'][:12]}"

fleet.load_tasks(rows)                                               # SWE-bench rows: instance_id, image_name
fleet.load_tasks(rows, id_field=r2e_id)                              # R2E-Gym
fleet.load_tasks(JsonlSource("tasks.jsonl", image_field="metadata.docker_image", limit=100))
fleet.load_tasks(JsonlSource("gs://my-bucket/r2e/train.jsonl", id_field=r2e_id))
fleet.load_tasks(SweBenchSource(limit=50, keep_row=True))          # pip install datasets
fleet.load_tasks(ImageListSource("images.txt"))                    # warm a fixed image set

new = fleet.add_tasks(next_batch)                    # Task objects, rows or a source
fleet.warm_images(sorted({t.image for t in new}), wait=False)
```

### Size warm pools

A warm pool's depth is its image's share of `max_concurrent`, not its task
count. At most `max_concurrent` claims are in flight, so an image with a tenth
of the tasks needs about a tenth of `max_concurrent` warm replicas:

```text
replicas = clamp(round(max_concurrent × tasks_image / tasks_total),
                 1, min(tasks_image, max_warmpool_replicas))
```

`plan()` and `setup()` share the budget among all images. `warm_images()`
shares it among the images it is given, so a window of the `sliding` and
`pipelined` strategies is sized for the claims it serves. `warm_images(imgs,
replicas=n)` still sets the depth directly. For RL, where the G rollouts of one
problem claim at the same time, `warm_per_task=True` gives every task its own
replica up to `max_warmpool_replicas`:

```python
FleetConfig(max_concurrent=64, max_warmpool_replicas=16, warm_per_task=True)
```

`setup()` and `warm_images()` warm up to `warm_parallelism` (16) images at
once. A failed image does not stop the others: the first error is raised once
every image was tried.

### Keep environments on the worker pools that hold their image

`FleetConfig.node_selector` is sent as the per-environment `worker_selector` on
every create: labels the worker pool must carry, which Substrate ANDs with the
template's own selector, so it narrows the template's pools and never widens
them, and holds for the environment's wakes. `FleetConfig.placement` adds labels
per task image, merged over `node_selector`: the placement plan that keeps an
image's environments on the pools whose nodes already hold it.

```python
FleetConfig(
    node_selector={"ate-env/workerpool": "c4"},
    placement={"ghcr.io/org/django@sha256:…": {"ate-env/domain": "d0"}},
)
```

Both are empty by default, which leaves the template's selection alone.
`placement` applies to creates that carry a digest-pinned `rootfs_image`; a task
routed through `image_templates` or `Task.template` sends no image, thus only
`node_selector` applies to it. `client.get(id).node` reports the node an
environment landed on.

### Warm the nodes before a run with `PreflightWarmer`

An image's first boot on a node pulls and unpacks the image and fetches the
golden snapshot; the boots after it restore in seconds. `PreflightWarmer` pays
that first boot before the run, for every image on every node of the worker
pools that will run it. It takes an image placement plan (`image-placement.json`
from `swebench-loader plan-placement`: each image assigned to placement
domains, worker pools a per-environment worker selector addresses) and drives
the low-level `Client`:

```python
import asyncio
from ate_env_rl import Client, ImagePlacement, PreflightWarmer

async def main():
    client = Client("127.0.0.1:7777")
    plan = ImagePlacement.from_file("image-placement.json")
    warmer = PreflightWarmer(client, plan, run_id="pf-7", probes_per_node=2)
    coverage = await warmer.run()
    coverage.write("preflight-coverage.json")
    if not coverage.clean:           # complete, and no probe left behind
        print(coverage.shortfall())  # what is missing, or the probes to sweep
    await client.close()

asyncio.run(main())
```

It ensures each image's derived template (`client.image_status(ensure=True)`),
which starts the golden bake, then warms every domain in parallel: as soon as
an image's golden is ready it creates probe environments with the domain's
worker selector, round after round, until every node of the domain has run
the image once. A domain warms `image_concurrency` (4) images at once; a
domain's `node_slots`, when the plan has it, caps that at `node_slots //
probes_per_node`. Each probe's first command returns when the layers and the
golden snapshot are on its node; the node is read back with `client.get()` and
the probe deleted. `golden_timeout` (30 min) gives up on the goldens still
baking once none became ready or failed for that long; a template with goldens disabled
is recorded as `none` and warmed by cold boots.

`PreflightCoverage` is the result: per image the golden's state, per domain
the nodes seen and per image the nodes covered, probes, rounds and failures;
`complete` says whether the run can expect warm first boots everywhere. A
probe whose delete failed is listed under its image's `undeleted_probes` (and
in `coverage.undeleted_probes`, which `shortfall()` names): it holds a node
slot until swept with `ate-env delete`. `run()` returns normally then and
`complete` stays `True`; gate on `coverage.clean`, which is `complete` with
no probe left behind.
`write()` produces the same `preflight-coverage.json` (`schema_version: v1`) as
the Go loader's pre-flight writes, so the two reports read alike. An
`Observer` passed in records the phases `ensure_template`, `preflight_golden`
and `preflight_probe`. The probes are deleted: `plan-placement` keeps each
node's assigned images under atelet's image-cache GC low watermark, so the
layers stay until the run.

### Follow a trainer's batch stream with `LookaheadWarmer`

An RL trainer owns its step loop. It knows the next batch or two ahead, and
with off-policy staleness it may still be claiming for a batch it dispatched
`max_staleness` steps ago. `LookaheadWarmer` keeps each image's pool sized for
exactly the batches in flight:

```python
from ate_env_rl import LookaheadWarmer

warmer = LookaheadWarmer(fleet, keep_finished=max_staleness)  # cap = max_warmpool_replicas
warmer.add(0, {img: n * G for img, n in batch0}, wait=True)   # start-up barrier
warmer.add(1, counts_of(batch1))      # warms while step 0 runs
...                                   # dispatch, fleet.acquire(task) per episode
warmer.add(2, counts_of(batch2))
warmer.finish(0)                      # kept for keep_finished more finishes, then drained
warmer.close()                        # drains every pool it asked for
```

Each step's counts are its demand. An image's target is the sum of the
*unclaimed* demand of live steps and the last `keep_finished` finished steps,
capped at `cap`, and it is applied as the pool's idle size. Before each call
the warmer reads how many sandboxes the fleet acquired per image since the last
one (`fleet.acquired_count(img)`, warm or cold) and takes them off the steps'
demand, oldest step first. Rollouts already served are not warmed for again,
even when `cap` is below the demand and most of them were cold creates, and
finishing a step drops only what its rollouts did not acquire, never the
replicas of steps still live. Images change in parallel (`warm_parallelism`). A failed image lands in
the returned `WarmReport.errors` and is retried on the next call, or is raised
with `fail_fast=True`. `add_async` / `finish_async` / `close_async` run off the
event loop, and an `AsyncSandboxFleet` works too.

The warmer sizes pools with `fleet.set_pool_replicas(img, n)` (shrinking deletes
idle paused actors only), reads them with `fleet.pool_size(img)` (None when the
driver cannot tell) to trim recycled surplus, and counts claims with
`fleet.acquired_count(img)`.

### RL rollouts and benchmark adapters

[USER_GUIDE.md](USER_GUIDE.md) covers what is specific to RL: overlapping
sandbox claims with policy decoding (`AsyncSandboxFleet.acquire_batch`), and
grading candidates with `ate_env_rl.adapters` (`SweBenchAdapter`). The VeRL
example in [`examples/verl_swebench`](../../examples/verl_swebench) uses both.

### Observability

Three layers, all off the hot path when disabled. The base install has
no telemetry dependency; the optional layers need extras. The
repository-wide guide, with ate-env-api, the guest, collectors, Cloud
Trace and the HTML report tool, is
[docs/observability.md](../../docs/observability.md):

```bash
pip install './clients/python[tracing,metrics]'
```

**Run report** (always on, no dependency). After `fleet.run(...)`,
`fleet.report` is a `RunReport` with per-phase count, total and max
(`preflight`, `plan`, `ensure_template`, `preflight_golden`, `preflight_probe`, `warm_pool`, `acquire`,
`process`, `recycle`, `release`, `teardown`), claims, task outcomes and
warm replicas:

```python
fleet = SandboxFleet(FleetConfig(backend="mock", strategy="naive"))
fleet.load_tasks(tasks)
fleet.run(process_fn)
print(fleet.report.summary())          # table, same shape as agent-sandbox-rl (run() logs it only at DEBUG)
fleet.report.to_dict()                 # JSON-ready
fleet.report.write(os.environ["REPORT_DIR"])   # <strategy>_<n>tasks_<stamp>.txt/.json
```

`release_errors` counts releases whose delete failed. `release()` logs the
failure and does not raise, so cleanup never stops a trainer, but it is
recorded: the `ate_env_client.release` span and the
`ate_env_client_release_seconds` sample get status `error`, the fleet counts
it in `ate_env_fleet_release_failures_total`, and the environment stays owned
by the driver, so `teardown()` (the run's reap) tries the delete again and
raises `InfrastructureError` if it still fails. An environment that is
already gone (NotFound) counts as released.

`write` never overwrites a report: when the name is taken (another report in
the same second, for example from a Ray worker that shares the directory), it
appends `-1`, `-2`, ... Runs on one fleet do not overlap: a second `run()`
while one is in progress raises `RuntimeError`.

`AsyncSandboxFleet.run` also fills `fleet.report`, but only with the phases
inside `run()`: `acquire`, `process`, `release` (and `ensure_template` on the
first use of an image). With `async with fleet:`, `setup()` and `teardown()`
run outside `run()`, so their `preflight`, `plan`, `warm_pool` and `teardown`
phases go to the Prometheus series and spans but not to the report. The sync
`SandboxFleet.run` does setup and teardown inside the run, so its report has
every phase.

**Prometheus** (`metrics` extra; `ObservabilityConfig.enable_metrics`).
The default, `None`, records the series whenever `prometheus_client` is
importable, whether from the `metrics` extra or another package (Ray
installs it, so inside a Ray trainer the series are on unless you set
`enable_metrics=False`), and otherwise logs one INFO message; `True` asks
for them, so a missing `prometheus_client` logs a WARNING; `False` turns
them off. The `ate_env_client_*` switch is process-global and one-way: once
any fleet in the process enables metrics, the client histograms keep
recording, even for a later fleet with `enable_metrics=False`. The fleet records
`ate_env_fleet_phase_latency_seconds`, `ate_env_fleet_task_latency_seconds`,
`ate_env_fleet_run_latency_seconds`, `ate_env_fleet_claims_total`,
`ate_env_fleet_tasks_total`, `ate_env_fleet_warm_replicas` and
`ate_env_fleet_release_failures_total`, and the client
records `ate_env_client_{acquire,exec,suspend,warm_claim,release}_seconds`
(`warm_claim` is the time to validate a warm claim; the resume itself
happens on the first guest request, so it lands in the first `exec`).
Collectors register on the first metrics-enabled `Observer`, never at
import. Labels are limited to `phase`, `backend` (`substrate` or `mock`),
`family` (the task's repo family, see `ate_env_rl.observability.repo_family`),
`strategy` and `status` (`ok`, `error`, or `cancelled` for a cancelled or
interrupted block); environment ids, run ids, commands, paths and image
tags never become labels. `family` has at most
`ate_env_rl.observability.MAX_FAMILIES` (64) values per process: the first 64
repo families keep their name and later ones are reported as `other`, so a
dataset with thousands of repos does not grow the series without limit.
A repo whose family is `other` or `unknown` is labelled `other_repo` or
`unknown_repo`, so it is never confused with the overflow or no-family value.
A repo already named like an escaped value gets one more suffix
(`other_repo` -> `other_repo_repo`), so two repos never share a label.
`repo_family` takes `metadata["repo"]` or parses a SWE-bench image name: set
`metadata["repo"]` for non-SWE-bench datasets (e.g. R2E-Gym); otherwise
their family is `unknown`.

The warm-replica gauge adds up across fleets in one process: each fleet
moves it by its own changes. A backend driver belongs to one fleet; a second
fleet on the same driver takes over its claim and warm events and logs a
warning.

Expose the series with:

```python
from ate_env_rl.observability import serve_metrics
serve_metrics(9095, addr="127.0.0.1")  # /metrics on the default registry, local only
```

`serve_metrics` listens on all interfaces (`0.0.0.0`) unless you pass
`addr`; a scraper in the cluster needs that, but on a laptop or a
host-network pod it exposes `/metrics` to the network. A busy port raises
`OSError` (`EADDRINUSE`), for example when two Ray workers on one node both
call `serve_metrics(9095)`: give each process its own port. Pass
`registry=` (a `prometheus_client.CollectorRegistry`) to `serve_metrics` and
to `Observer` to keep the `ate_env_fleet_*` series off the global registry.

**Tracing** (`tracing` extra; off by default). The client emits
OpenTelemetry spans and sends the W3C trace context to `ate-env-api`, so
you can see whether time goes to the client, the API server or Substrate.
Either fleet switch turns on tracing for the whole fleet, under one
service name:

```python
from ate_env_rl import Client, FleetConfig, ObservabilityConfig, TracerConfig

FleetConfig(observability=ObservabilityConfig(enable_tracing=True))   # service "ate-env-fleet"
FleetConfig(tracing=TracerConfig(enable_tracing=True))                # service "ate-env-client"
# Both on: ObservabilityConfig.trace_service_name wins ("ate-env-fleet" here).
FleetConfig(tracing=TracerConfig(enable_tracing=True, trace_service_name="trainer"),
            observability=ObservabilityConfig(enable_tracing=True))
FleetConfig(tracing={"enable_tracing": True})                         # a plain dict also works
Client("localhost:7777", tracer_config=TracerConfig(enable_tracing=True))  # client only
```

`TracerConfig` fields:

| Field | Default | Meaning |
| --- | --- | --- |
| `enable_tracing` | `False` | Emit spans and propagate the trace context. |
| `trace_service_name` | `"ate-env-client"` | OTel resource `service.name` of the client's own OTLP provider (case 2 below). Spans are always named `ate_env_client.<op>`. In a fleet with `ObservabilityConfig.enable_tracing` also on, `ObservabilityConfig.trace_service_name` takes precedence (logged at DEBUG). |

One `fleet.run(...)` is one trace:

```
ate_env_fleet.run
  ate_env_fleet.acquire
    ate_env_client.acquire
      ate_env_client.create_environment
  ate_env_fleet.process
    ate_env_client.exec
  ate_env_fleet.release
    ate_env_client.release
      ate_env_client.delete_environment
  ate_env_fleet.teardown
    ate_env_client.reap
```

- **Spans.** `Client`, `SandboxHandle` and the Substrate backend emit
  `ate_env_client.<op>` spans, for example `create_environment`, `acquire`,
  `exec`, `write_file`, `read_file` and `release`. Their attributes (below)
  hold ids, the executable name and sizes. The executable name is the first
  command word of the command line's first simple command, after leading
  `NAME=value` assignments and redirections (`cmd1 && cmd2` records `cmd1`);
  nothing is recorded when an assignment's value holds `$` or a backtick, or
  for an argv list whose first item is an assignment. They never hold command arguments, file content or
  image tags. A command that fails with `check=True` marks its `exec` span
  as an error.
- **Propagation.** Every gRPC call on a channel the client creates carries
  the W3C `traceparent` / `tracestate` (never baggage), so `ate-env-api`
  can continue the trace. A caller-owned `Client(channel=...)` gets spans
  but not the header: add `ate_env_rl.telemetry.TraceContextInterceptor`
  subclasses to your channel yourself.
- **Nesting.** Client spans nest under the caller's current span, including
  across the fleet's worker threads and background event loop.

**Span attributes.** Substrate identities use Substrate's own keys (its
`ateattr` keys), so a backend can join a client span to Substrate's logs,
traces and lifecycle records without a mapping. Client-only fields use the
`ate_env.` prefix.

| Key | Value | On |
| --- | --- | --- |
| `ate.actor.name` | environment id (the Substrate actor name) | every span of one environment: `Client` spans, `acquire`, `release`, `exec`, `write_file`, `read_file` |
| `ate.atespace` | atespace | `Client` spans; `exec`, `write_file`, `read_file` when the runtime has one (Substrate) |
| `ate.template.name` | template name | `create_environment`, `ensure_template`, `acquire`, `warm_pool` |
| `ate.template.atespace` | template atespace, when given | `create_environment` |
| `ate_env.run_id` | fleet run id | `exec`, `write_file`, `read_file`, `acquire`, `warm_pool`, `reap` |
| `ate_env.command.executable` | executable name only | `exec` |
| `ate_env.exit_code`, `ate_env.timed_out` | exit state | `exec` |
| `ate_env.first_command` | `true` on the first `exec` of each acquired sandbox | `exec` |
| `ate_env.file.path`, `ate_env.file.size` | path and size in bytes | `write_file`, `read_file` |
| `ate_env.replicas` | warm pool size | `warm_pool` |
| `ate_env.rootfs_image` | image reference without its tag | `create_environment` |

`Client` spans also carry `rpc.system.name` (`grpc`), `rpc.method` (the full
gRPC method, for example `ateenv.v1alpha.EnvironmentService/CreateEnvironment`),
`server.address` and `server.port`, and have kind `CLIENT`. These are the keys
that otelgrpc puts on the `ate-env-api` server spans and the Go client's spans.
Like otelgrpc, the client reads `OTEL_SEMCONV_STABILITY_OPT_IN` once at import:
`rpc/old` gives the older `rpc.system`, `rpc.service` and the short
`rpc.method` (`CreateEnvironment`), and `rpc/dup` gives both sets, with the full
`rpc.method`. Set it to the same value for the client and for `ate-env-api`.

**Time to first command (TTFC).** `acquire` ends when the create call
returns. The wake and readiness wait happen inside the first command, so the
first `exec` span of each sandbox holds wake time plus command time. Compute
TTFC per sandbox as the start of `ate_env_client.acquire` to the end of the
`ate_env_client.exec` span with `ate_env.first_command=true` and the same
`ate.actor.name`. This equals the swebench loader's `ttfi` (create start to
first command returning), so a real trainer and the loader report the same
client-side number.

**Where spans go.** The client never installs the process-global
OpenTelemetry `TracerProvider`. Its tracers choose a provider each time a span
starts, so there are two cases:

1. **Your application configures OpenTelemetry** (the usual case). Install
   your SDK `TracerProvider` and exporters as you normally do, before or after
   you create the client: the order does not matter, also when
   `OTEL_EXPORTER_OTLP_ENDPOINT` is set. From the moment your provider is
   installed, client spans go to it and nest under your spans.
2. **Your application does not configure OpenTelemetry.** Set
   `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`). On
   the first client span, the client builds its own `TracerProvider` with
   service name `trace_service_name`, which batches spans to an OTLP/gRPC
   exporter and is shut down (flushed) at exit. It is never made the global
   provider, so you can still install yours later (case 1). Configure the
   exporter with the standard OpenTelemetry environment variables, for
   example:

   ```bash
   export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
   export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer <token>"
   # Also: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE,
   # OTEL_EXPORTER_OTLP_CERTIFICATE, OTEL_EXPORTER_OTLP_TIMEOUT, ...
   ```

   **Size the batch processor for wide runs.** With the SDK defaults, the
   processor queues at most 2048 spans and exports 512 every 5 s. When the
   queue is full it drops spans. A 500-wide RL step emits a few thousand spans
   in seconds, so raise the limits, for example:

   ```bash
   export OTEL_BSP_MAX_QUEUE_SIZE=16384        # default 2048
   export OTEL_BSP_SCHEDULE_DELAY=1000         # ms between exports, default 5000
   export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=2048  # default 512; at most the queue size
   ```

   Each dropped span logs the SDK warning `Queue full, dropping Span.` (logger
   `opentelemetry.sdk._shared_internal`). If you see it, the trace is
   incomplete: raise the limits before you read latencies from it. In case 1,
   the same variables apply to an SDK `BatchSpanProcessor` that you create
   without arguments.

If neither applies (no provider and no OTLP endpoint), spans are created but
not exported, and nothing is sent over the network.

Without the extra, `import ate_env_rl` never loads OpenTelemetry. Enabling
tracing then logs one error naming the missing extra, and the client keeps
working without spans.

### Develop against a local guest daemon (no cluster)

The guest daemon example serves the process and filesystem services
standalone, so the whole guest path minus the proxy can run locally:

```bash
go run ./examples/guest-daemon --listen 127.0.0.1:8090 --workspace "$(mktemp -d)"
```

```python
client = Client("127.0.0.1:8090")
env = client.env("anything")            # daemon ignores the routing metadata
print(await env.shell("uname -a"))
await client.close()
```

Lifecycle calls (`create`, `get`, …) are unavailable in this mode — only
`ate-env-api` implements them.

See [examples/quickstart.py](examples/quickstart.py) for a complete
program against a real ate-env-api.

## Development

```bash
cd clients/python
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest                    # unit tests (in-process fake server)
```

### Regenerating gRPC stubs

Generated code under `src/ate_env_rl/_gen/` is committed. After changing
`proto/ateenv/v1alpha/*.proto`, regenerate from the repo root:

```bash
./clients/python/scripts/gen-protos.sh      # or: make python-protos
```

Keep the `grpcio-tools` pin, the regenerated stubs, and the `protobuf`
dependency floor in `pyproject.toml` in sync: stubs generated by a newer
protobuf require a matching or newer runtime.

### Integration tests without a cluster

```bash
go run ./examples/guest-daemon --listen 127.0.0.1:8090 --workspace "$(mktemp -d)"
ATE_ENV_GUEST_TARGET=127.0.0.1:8090 .venv/bin/pytest tests/e2e
```

### Full-stack tests against a cluster

With a cluster running Agent Substrate (including
[substrate PR #1183](https://github.com/agent-substrate/substrate/pull/1183),
required for the gRPC guest data plane — see the note under
"How it works") and a deployed ate-env-api built from current main:

```bash
kubectl -n ate-env port-forward svc/ate-env-api 17777:7777 &
ATE_ENV_API_TARGET=127.0.0.1:17777 .venv/bin/pytest tests/e2e/test_full_stack.py
```

`ATE_ENV_TEMPLATE` optionally overrides the ActorTemplate used for the
test environment; `ATE_ENV_READY_TIMEOUT` (default 180s) bounds the wait
for the environment to start serving. `ATE_ENV_ROOTFS_IMAGE`, a digest-pinned
image that has `sh`, enables the create-from-image test, which runs that
image unmodified with the guest injected and checks the derived template is
reused by a second environment.

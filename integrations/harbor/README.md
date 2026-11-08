# ate-env-harbor: Harbor environments on ate-env

[Harbor](https://github.com/harbor-framework/harbor) evaluates agents and models
on containerized tasks (Terminal-Bench 2.0, SWE-bench and the rest of its
registry) and selects where the containers run with `--env`. This package is
that provider for ate-env: each Harbor trial gets an ate-env environment, a
Substrate actor that runs the task image unmodified with the guest mounted in.
Commands go through the guest's process API, files through its file API, and
directories through tar over both, the way Harbor's cloud providers stage them.

```bash
pip install -e clients/python -e integrations/harbor
export SUBSTRATE_ENV_API=127.0.0.1:7777          # kubectl port-forward -n ate-env svc/ate-env-api 7777:7777
export SUBSTRATE_ENV_AUTH_TOKEN=...               # the caller token, when the API requires one
harbor run -p /path/to/task -a oracle -e ate_env_harbor:AteEnvEnvironment
```

Harbor loads the class from the import path, so no change to Harbor is needed.
Python 3.12 or newer (Harbor's floor); `ate-env-rl` from this checkout.

## Settings

Each setting is a constructor kwarg and an `--ek key=value` flag (repeatable).

| Kwarg | Default | Meaning |
|---|---|---|
| `api_url` | `$SUBSTRATE_ENV_API`, else `127.0.0.1:7777` | `ate-env-api` endpoint, the variable the `ate-env` CLI reads |
| `api_token` | `$SUBSTRATE_ENV_AUTH_TOKEN` | Caller token; sent as gRPC metadata. Set the variable rather than `--ek api_token=`: Harbor records the kwargs in each trial's `result.json`, and the provider warns when it sees the kwarg |
| `atespace` | `ate-env` | Atespace of the environments |
| `template` | `default-template` | Base template when the task sets no resources, and the fallback under the `auto` policy |
| `template_prefix` | `harbor-` | Resource class templates are named `<prefix>c<cpus>-m<memory_mb>` |
| `registry` | `$SUBSTRATE_ENV_HARBOR_REGISTRY` | Where images built from `environment/Dockerfile` are pushed |
| `build` | `local` | `local` (docker buildx), `cloud-build` (gcloud builds submit) or `none` (prebuilt `docker_image` only) |
| `platform` | `linux/amd64` | Build platform; the worker nodes' architecture |
| `create_timeout_sec` | `600` | Create plus the first guest answer |
| `exec_timeout_sec` | `86400` | Bound on a command Harbor runs without a timeout, such as the verifier's test command; the guest's own default would be one hour |
| `workdir` | none | Working directory when neither the task nor the Dockerfile sets one |
| `scratch_dir` | `/tmp` | Where tar archives are staged inside the environment |
| `docker_bin`, `gcloud_bin` | `docker`, `gcloud` | The executables used for images |
| `tool_paths` | none | Directories a runtime layer mounts into the environment, `:`-separated, put first on `PATH` for every command; `start` fails when one is missing. See [Tools as a second layer](#tools-as-a-second-layer) |
| `tool_paths_profile` | `/etc/profile.d/zz-harbor-tools.sh` | The drop-in that gives login shells the tool paths; written at `start` when `tool_paths` is set, a warning when the image user cannot write it; empty disables it |

`preflight()` checks `SUBSTRATE_ENV_API` (reachable, token accepted) before any
trial is queued; an `api_url` kwarg is checked when the first trial starts.

## Images

ate-env runs an image only by digest, because a changed image invalidates the
snapshots taken from it.

- A prebuilt `[environment] docker_image` is used as is when it carries
  `@sha256:...`. A tag is resolved to its digest with
  `docker buildx imagetools inspect`, once per process.
- `environment/Dockerfile` is built and pushed as `<registry>/<task>:<environment_id>`
  (Harbor's content hash of `environment/`, the scheme its GKE provider uses)
  and skipped when that tag already exists; `--force-build` rebuilds. Builds
  are deduplicated per tag inside the process. `build=local` needs docker with
  buildx and a login to the registry; `build=cloud-build` needs `gcloud` and
  a project. Only `platform` is built.
- To run a whole dataset without building at trial time, prebuild it once:
  `harbor admin upload-images -t <tasks-dir> -r <repo> --tag <tag> --push
  --update-config` writes `docker_image` tags into the task files; the
  provider resolves them. Then pass `--ek build=none`.

The worker nodes' service account must be able to pull from the registry.

## Resources: one base template per class

Substrate requires CPU and memory limits on every ActorTemplate, and
`ate-env-api` derives the per-image template from a base, thus a task's
`cpus` and `memory_mb` (with `--override-cpus` and `--override-memory-mb`
applied) select the base:

- `harbor-c1-m2048` for `cpus = 1`, `memory_mb = 2048`; a fractional CPU is
  spelled in millicores, `harbor-c500m-m2048`. A task without resources uses
  `template`.
- Register the classes a dataset needs once. `hack/register-templates.sh`
  prints the commands. It parses each `task.toml` with Harbor's own model, thus
  `memory = "4G"` and `memory_mb = 4096` name the same class, and it runs with
  the Python that has `ate-env-harbor` installed (`PYTHON=.venv/bin/python`):

  ```bash
  integrations/harbor/hack/register-templates.sh <tasks-dir> -- \
    --guest-image <registry>/ate-env-guest@sha256:<digest> | sh
  ```

- `--override-cpus` and `--override-memory-mb` select classes the script does
  not print (the tasks do not ask for them); register those by hand with the
  same command shape.
- Under the default `auto` policy a missing class template falls back to
  `template` with a warning that names the registration command. Under
  `--cpus limit` or `--memory limit` the trial fails instead. `ignore` uses
  `template` without looking at the task's resources. `storage_mb` has no
  counterpart and is ignored; GPU and TPU tasks are rejected before start.

## What a trial looks like

1. `start`: resolve the image, create the environment from the class template,
   wait for the guest to answer, learn the image's user, create the working
   directory when the image lacks it, upload `environment/` for prebuilt-image
   tasks, and write `trial_dir/ate_env.json` (environment id, atespace,
   template, image digest, session and context ids; never the token). A verifier that Harbor runs in a separate environment
   (`[verifier] environment_mode = "separate"`) records its own environment in
   `trial_dir/ate_env.verifier.<step>.json`, so the agent's record is kept.
2. `exec`: `sh -c <command>` through the guest, with the working directory
   from the call, the task, the Dockerfile's `WORKDIR` or the kwarg, in that
   order. At `timeout_sec` (or `exec_timeout_sec` when Harbor passes none) the
   guest kills the process group and the exit code is 137. A `user` that is the
   image's own user runs as is (root on a root image, the common case and what
   the verifier asks for); another user runs through `sudo -n`, or `su` when the
   image runs as root, with the command's environment inlined because sudo
   resets it; otherwise the call fails and says so.
3. Files: `upload_file` keeps the mode and creates the parent directory;
   `upload_dir` and `download_dir` stage a tar archive under `scratch_dir`,
   which keeps modes, symlinks and empty directories.
4. `stop(delete=True)` deletes the environment. `--no-delete` suspends it
   instead: it costs no worker, keeps its filesystem in the snapshot and wakes
   on the next call; the log names the `ate-env <id> shell '<command>'` form
   to inspect it.

## Tools as a second layer

Harbor's agents install their tools at the start of every trial (terminus-2:
tmux and asciinema through the image's package manager). A runtime layer, a
second digest-pinned image the template mounts read-only next to the guest
(`ate-env manifest template --layer tools=<image@sha256:...>=/opt/harbor-tools`),
can carry them instead. With `--ek tool_paths=/opt/harbor-tools/bin` the
provider puts that directory first on `PATH` for every command it runs, the
agent's, the verifier's and the user-switched ones alike, so the agents' own
`command -v` checks find the tools and skip the install. Login shells do not
inherit that prefix: `/etc/profile` on Debian, Ubuntu and Alpine images
overwrites `PATH`, and terminus-2 runs its tmux session with `bash --login`,
thus `start` also writes `/etc/profile.d/zz-harbor-tools.sh`
(`tool_paths_profile`), a warning when the image user cannot. At `start` the
provider checks that each path exists and fails with the `--layer` flag to add
when the template lacks the layer. `trial_dir/ate_env.json` records the paths.
An image without `sh` needs the directory on the template's own `PATH` as well
(`ate-env manifest template --env PATH=...`): the guest resolves `sh` before
any command runs.
[`examples/harbor-tools-layer`](../../examples/harbor-tools-layer/README.md)
builds a static tmux and asciinema layer and runs Terminal-Bench with it.

## Limits

- One container per environment: Docker Compose tasks are rejected.
- No egress control: `network_mode` other than `public` is rejected. ate-env
  sets no actor egress policy; what the actor can reach follows the Substrate
  defaults, and most tasks and installed agents need package indexes.
- No run-as-user in the guest: see `exec` above.
- No host mounts, no SSH stream mode, no `attach` (the error names the CLI
  command instead).
- No labels on environments: the mapping to Harbor's ids is in `ate_env.json`
  (and `ate_env.verifier.<step>.json` for a separate verifier environment).

## Tests

```bash
pip install -e 'clients/python[dev]' -e 'integrations/harbor[test]'
cd integrations/harbor && python -m pytest
```

The suite runs against an in-process fake `ate-env-api` (`tests/fakes.py`).
Unit tests run commands on this machine under temporary paths. `tests/test_trial.py`
drives `harbor run` itself, as a subprocess, with each environment a container
started from the task's image through the same fake; it needs a docker daemon
and is skipped without one. `tests/test_e2e.py` runs the same trial against a
live `ate-env-api` when `SUBSTRATE_E2E_API_URL` is set.

# Harbor with tools as a second layer

Harbor's agents install their tools into the task container at the start of
every trial: terminus-2 runs `apt-get install tmux asciinema` (or `apk`, `dnf`)
before its first command, and other installed agents fetch their CLIs with
`curl` or `npm`. On ate-env that step can come from a **runtime layer**
instead: a second, digest-pinned image that the template mounts read-only into
any task image, next to the guest. This example builds such a layer, registers
the resource class templates with it, and runs Harbor with the layer's
directory first on `PATH`, thus the agents find their tools and install nothing.

What it buys:

- No per-trial install. terminus-2 checks `tmux -V` and `asciinema --version`
  first and skips the install when both answer; other `BaseInstalledAgent`s
  check `command -v` the same way.
- No egress for the agent side. The verifier's own `apt-get` in a task's
  `test.sh` is unchanged; that need stays with the task.
- One tool version for every task image, whatever the image ships.

What it needs: the layer's binaries must run inside any task image, thus they
are **static** here (tmux built with `--enable-static` on musl, asciinema's
static musl release). The layer is `linux/amd64`, the worker nodes'
architecture. The [OpenHands layer](../../integrations/openhands/layer) takes
the other route, a relocatable runtime with a glibc floor; either works.

## 1. Build and push the layer

```bash
export LAYER_REPO=us-central1-docker.pkg.dev/<project>/harbor/tools-layer
docker buildx build --platform linux/amd64 --push --provenance=false -t "$LAYER_REPO:tmux-3.8" layer
LAYER_REF="$LAYER_REPO@$(docker buildx imagetools inspect --format '{{json .Manifest.Digest}}' "$LAYER_REPO:tmux-3.8" | tr -d '"')"
```

[`layer/Dockerfile`](layer/Dockerfile) pins tmux 3.8 and asciinema 3.2.1 by
checksum and ends in `FROM scratch` with `/bin/tmux` and `/bin/asciinema`:
mounted at `/opt/harbor-tools`, they are `/opt/harbor-tools/bin/{tmux,asciinema}`.

Check it against both C libraries before pushing. Docker 28 or newer mounts an
image into a container the way Substrate mounts a layer into an actor:

```bash
docker buildx build --platform linux/amd64 --load -t tools-layer:check layer
for img in ubuntu:24.04 alpine:3.20; do
  docker run --rm --platform linux/amd64 --mount type=image,source=tools-layer:check,target=/opt/harbor-tools "$img" \
    sh -c 'PATH=/opt/harbor-tools/bin:$PATH; tmux -V && asciinema --version'
done
```

Both print `tmux 3.8` and `asciinema 3.2.1`. (A bind mount of a build output
directory is not the same check: on Docker Desktop for macOS a directory under
`/tmp` is not shared with the VM and shows up empty.)

## 2. Register the resource class templates with the layer

Layers are a property of the template, thus every class template a dataset
needs carries it. The flag is `--layer <name>=<image@sha256:...>=<mount path>`:

```bash
integrations/harbor/hack/register-templates.sh ./tb2/terminal-bench -- \
  --guest-image <registry>/ate-env-guest@sha256:<digest> \
  --layer "tools=$LAYER_REF=/opt/harbor-tools" | bash -euo pipefail
```

## 3. Run with the layer on PATH

```bash
examples/harbor/run-tb2.sh -p ./tb2/terminal-bench -a terminus-2 -m <model> -n 10 \
  --ek tool_paths=/opt/harbor-tools/bin
```

`tool_paths` is the provider setting (see the
[provider README](../../integrations/harbor/README.md#settings)): the
directories, `:`-separated, that go first on `PATH` for every command the
provider runs, the agent's and the verifier's alike and the user-switched ones
too. Login shells get them as well: terminus-2 starts its tmux session with
`bash --login`, and `/etc/profile` on Debian, Ubuntu and Alpine images
overwrites `PATH`, thus the provider also writes
`/etc/profile.d/zz-harbor-tools.sh` at `start` (`tool_paths_profile` names the
file; it is a warning, not an error, when the image user cannot write there).
At `start` the provider checks that each directory exists in the environment
and fails with the `--layer` flag to add when it does not, which is what a
template without the layer looks like. `trial_dir/ate_env.json` records the
paths.

[`demo.sh`](demo.sh) does the three steps in a row for a tasks directory:

```bash
LAYER_REPO=... GUEST_IMAGE=... APPLY=1 \
  examples/harbor-tools-layer/demo.sh ./tb2/terminal-bench -a terminus-2 -m <model> -n 10
```

## Extending the layer

- More agent tools go next to tmux in `layer/Dockerfile`, as static binaries
  or as relocatable trees with their libraries under the mount. A Node runtime
  for `claude-code` or `codex` is such a tree; then pass `--ak skip_install=true`
  so the agent does not run its own installer.
- Images without `sh` or `tar` (distroless) can take a static busybox under a
  second directory, for example `/opt/harbor-tools/busybox`, added to
  `tool_paths` on purpose. It is not on `PATH` by default: busybox's `find`
  and `tar` would shadow the image's GNU tools for every task script. One more
  step for such images: the guest resolves `sh` from the container's own
  `PATH` before any command runs, thus the template must carry the directory
  too, with `ate-env manifest template --env PATH=/opt/harbor-tools/busybox:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`;
  `tool_paths` alone cannot start the first `sh`.
- Order matters: the first entry of `tool_paths` wins, and all of them come
  before the image's `PATH`.

## Limits

- The layer is per template, not per trial: one layer serves every task of a
  class template. Different tool sets mean different templates (`--ek template_prefix=`).
- The verifier's installs are the task's own (`tests/test.sh`) and still need
  egress.
- The layer is mounted read-only; tools that write next to themselves need a
  writable home (`HOME` is the image's).

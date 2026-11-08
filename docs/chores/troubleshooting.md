# Troubleshooting Guide

This document is the canonical reference for common failure modes, diagnostic steps, and fixes across local CI, end-to-end testing, Substrate bumps, and dependency updates.

---

## 1. Kubernetes & Kind Test Cluster Issues

### Linux inotify instance exhaustion
- **Symptom:** Kind cluster bootstrap fails, kube-proxy crashes with `too many open files`, or API server requests time out during cluster creation.
- **Cause:** Linux enforces a per-user limit on inotify instances (`fs.inotify.max_user_instances`). Each Kind node container consumes dozens of inotify watchers. When multiple Kind clusters or container runtimes run concurrently on the same host, the system inotify limit is exhausted.
- **Fix:** Increase the inotify instance limit:
  ```bash
  sudo sysctl fs.inotify.max_user_instances=8192
  ```
  To persist across reboots, add `fs.inotify.max_user_instances = 8192` to `/etc/sysctl.d/99-kind.conf`.

### `GoldenActorCrashed` and WorkerPool rollout
- **Symptom:** E2E setup fails during golden snapshot registration with `GoldenActorCrashed`, or snapshot creation hangs indefinitely.
- **Cause:** The `WorkerPool` Pods were not fully ready when the golden actor snapshot was triggered, or the worker runtime crashed during initialization.
- **Fix:** [`hack/ci/e2e.py`](../../hack/ci/e2e.py) automatically waits for the `deploy/<template>-workerpool` rollout before snapshot creation. If workers crash, inspect the worker container logs:
  ```bash
  kubectl -n ate-env logs deploy/default-template-workerpool -c worker
  ```
  Check for gVisor runtime initialization errors or missing snapshot bucket credentials.

### Port conflicts & busy local ports
- **Symptom:** E2E or smoke tests fail with `bind: address already in use` or connection refused on `127.0.0.1:17777`.
- **Cause:** An existing port-forward, previous test run, or unrelated local process is listening on the target port.
- **Fix:**
  - In E2E: [`hack/ci/e2e.py`](../../hack/ci/e2e.py) automatically picks an available OS-assigned port when `LOCAL_PORT` is unset. If `LOCAL_PORT` is explicitly set, either unset it or supply an unused port:
    ```bash
    LOCAL_PORT=18888 make ci-e2e
    ```
  - In smoke tests: Override the default port:
    ```bash
    LOCAL_PORT=18888 make smoke
    ```
  - Inspect what is listening on the port:
    ```bash
    lsof -i :17777 || ss -tlpn 'sport = :17777'
    ```

### Port-forward startup failures
- **Symptom:** Helper scripts fail with `port-forward exited early with code ...`.
- **Cause:** `kubectl port-forward` terminated immediately because the target service or Pod does not exist, the namespace is incorrect, or cluster credentials expired.
- **Fix:** The Python test helper [`hack/ci/lib.py`](../../hack/ci/lib.py) captures `kubectl port-forward` output to a temporary log file and prints it in the exception message. Check that the service exists:
  ```bash
  kubectl -n ate-env get svc/ate-env-api
  ```

### Router TLS and SPIFFE authorization
- **Symptom:** Remote shell execution or file I/O fails at once with `FailedPrecondition` (the router rejected `ate-env-api`'s client certificate) or `Unavailable: connection reset by peer` when talking through the `atenet` router.
- **Cause:** Substrate v0.6.0+ enforces mutual TLS (mTLS) and SPIFFE ID authorization on the router. Calls from `ate-env-api` to guest actors must present a certificate whose SPIFFE ID matches `ATE_INGRESS_ALLOWED_SPIFFE_IDS`.
- **Fix:** Verify that the router's allowed SPIFFE IDs include the `ate-env-api` service account identity and that the router trusts the podidentity signer. `ate-env-api` logs the ID it presents at startup.

### Router allow-list is empty
- **Symptom:** Creates succeed, but each guest call retries and then fails with `Unavailable` and `connection refused`. It retries for the running budget (`--wake-running-budget`, 20 s by default) when the environment is RUNNING, or for the wake budget (`--wake-budget`, 5 min by default) while it wakes. The port proxy answers 502 at once.
- **Cause:** The router's allow-list is empty. ate-setup does not check it (since Substrate [`c093a40c`](https://github.com/agent-substrate/substrate/commit/c093a40c)). With the default Envoy dataplane, a router without it serves no TLS listener, thus no client reaches any actor. The router logs `No --ingress-allowed-spiffe-ids, so the router serves no traffic` at startup.
- **Fix:** Set the router's allowed SPIFFE IDs to the `ate-env-api` service account identity and rerun `ate-setup deploy`:
  ```bash
  export ATE_INGRESS_ALLOWED_SPIFFE_IDS="spiffe://cluster.local/ns/ate-env/sa/ate-env-api"
  ```
  Add `spiffe://cluster.local/ns/ate-system/sa/atenet-client` only if you use `kubectl ate proxy actor`: that ID lets each user who can create PodCertificateRequests in `ate-system` reach each actor.

  In automated Kind runs, [`hack/ci/targets.py`](../../hack/ci/targets.py) sets `ATE_INGRESS_ALLOWED_SPIFFE_IDS` to `spiffe://cluster.local/ns/<namespace>/sa/ate-env-api,spiffe://cluster.local/ns/ate-system/sa/default` unless the environment sets it.

---

## 2. Substrate Module Resolution & Dependency Bumps

### Substrate module resolution and git fetches
- **Symptom:** `hack/ci/ensure-substrate.sh` fails with `ERROR: Commit <hash> for substrate version <ver> is not reachable from origin's branches or tags.`
- **Cause:** The pinned Substrate commit SHA in `go.mod` is not reachable from origin's advertised branches or tags. GitHub's git server rejects fetching arbitrary unadvertised commit SHAs directly, so `ensure-substrate.sh` refuses to perform fallback SHA fetches.
- **Fix:** Ensure the commit is pushed to an origin branch or tag in `agent-substrate/substrate`, or bump `go.mod` to a reachable tag or pseudo-version.
- **Atomic cache population:** [`hack/ci/ensure-substrate.sh`](../../hack/ci/ensure-substrate.sh) clones bloblessly (`--filter=blob:none`) into an atomic temporary directory (`${CACHE_DIR}.tmp.XXXXXX`) before moving it to `${CACHE_DIR}`, guaranteeing an interrupted clone never corrupts the cache or blocks future runs.
- **Strict hash check:** When checking out a remote pin into `${XDG_CACHE_HOME}/sandboxenv-rl/substrate-<ref>`, `ensure-substrate.sh` resolves the full 40-character commit hash via `go mod download -json` and strictly asserts that `git rev-parse HEAD` matches the expected hash. If the cache directory was modified or corrupted, delete the cached directory to force a fresh clone:
  ```bash
  rm -rf "${XDG_CACHE_HOME:-~/.cache}/sandboxenv-rl/substrate-*"
  ```

### Developing against a local Substrate checkout
- **Symptom:** Changes in a sibling `substrate/` checkout are ignored by `sandboxenv-rl`.
- **Cause:** By default, `sandboxenv-rl` resolves the Substrate dependency from the version pinned in [`go.mod`](../../go.mod).
- **Fix:** Use a Go workspace to link the local repository:
  ```bash
  go work init . ../substrate
  ```
  `ensure-substrate.sh` detects the active workspace or `replace` directive, prints the local directory path, and skips git fetching and strict hash checks entirely (`go.work` and `go.work.sum` are gitignored).

### Working tree restoration guarantees in `check-substrate-head.sh`
- **Symptom:** Aborting a `check-substrate-head` run (via `Ctrl-C` or early test failure) leaves modified `go.mod` or leftover `go.work` files.
- **Cause:** Testing compatibility against Substrate HEAD temporarily mutates `go.mod` or creates a temporary `go.work`.
- **Protection:** [`hack/ci/check-substrate-head.sh`](../../hack/ci/check-substrate-head.sh) backs up `go.mod`, `go.sum`, and workspace state before modification and registers a bash `EXIT` trap that guarantees the working tree is restored cleanly even on crash or abort.

---

## 3. Docker Runner & Local CI Environment

### Docker workspace isolation (copy vs. bind mount)
- **Design:** In Docker mode (`CI_RUNNER=docker` or Docker fallback), `hack/ci-compat.sh` builds the repository tree into the container image (`Dockerfile.ci`) and initializes a clean baseline git commit rather than bind-mounting the host directory.
- **Benefits:**
  - Sidesteps Linux host-permission collisions where files created inside the container are owned by `root`.
  - Works identically across rootless Docker, Podman, and Docker Desktop on macOS without requiring `chown` workarounds.
  - Supports git worktrees and uncommitted working-tree edits automatically.
- **Caveat (outside `go.work`):** If a local `go.work` file references paths outside the repository (e.g. `../substrate`), those external directories are not copied into the Docker container. For multi-repository development, use the Nix runner (`CI_RUNNER=nix`) or host runner (`CI_RUNNER=host`).

### Docker write-back mechanism
- **Design:** For code generation and pin update commands (`make protos`, `make update-pins`), `hack/ci-compat.sh` executes the command inside the container with standard output reserved for a `git diff` patch stream.
- **Why:** The container's diff is extracted as a binary patch and applied on the host by the invoking user. This ensures generated files and updated pins are created with host user ownership and permissions.
- **Troubleshooting failed patches:** If a patch fails to apply cleanly (e.g. due to conflicting host modifications during the run), the patch file is saved to `/tmp/sandboxenv-rl-patch-XXXXXX.patch` and reported to stderr.

### Docker socket access in E2E
- **Symptom:** `make ci-e2e` inside Docker fails with `Cannot connect to the Docker daemon`.
- **Cause:** Creating a Kind cluster and building images with `ko` requires access to the host Docker daemon.
- **Fix:** Targets requiring Docker access must supply `--docker-socket` to `hack/ci-compat.sh` (this is configured automatically by `make ci-e2e`; `make smoke` uses `--host-network` and mounts the host kubeconfig read-only without requiring Docker socket access). Ensure `/var/run/docker.sock` is readable.

### Neither Nix nor Docker found
- **Symptom:** `ERROR: Neither Nix nor Docker was found.`
- **Fix:** Install either [Nix with Flakes](https://nixos.org/download) (recommended for fastest local execution) or [Docker](https://docs.docker.com/get-docker/).

---

## 4. Flake & Dependency Pinning

### `uv.lock` out of date
- **Symptom:** `make check-pins` fails with:
  ```text
  pin mismatch: uv.lock is out of date with the pyproject.toml files: ...
  ```
- **Cause:** A `pyproject.toml` of the uv workspace (the root one, `clients/python` or an `integrations/*` project) changed without re-locking. The Nix envs are built from `uv.lock` as it is, so they don't have the change either.
- **Fix:** run `uv lock` in the devShell and commit `uv.lock` with the `pyproject.toml` change:
  ```bash
  nix develop -c uv lock
  ```
  It changes only what the edit requires. If uv can't resolve (for example a new `==` pin conflicts with another project's requirement), its error names the conflicting requirements; every project shares one lock, so they need versions that agree.

### Python env fails to build
- **Symptom:** `nix develop` (or `make ci`) fails building a Python package, with `No module named 'setuptools'` (or another build backend), or `autoPatchelfHook could not satisfy dependency <lib>.so`.
- **Cause:** [`nix/python.nix`](../../nix/python.nix) builds the envs from `uv.lock` with uv2nix, from wheels. A package without a wheel for the platform (sdist only, or a git source) is built from source, and `uv.lock` doesn't say which build system it needs ([astral-sh/uv#5190](https://github.com/astral-sh/uv/issues/5190)). A wheel that links a system library outside the manylinux set needs that library.
- **Fix:** in `buildSystemOverrides` in `nix/python.nix`, add the build system (`withBuildSystem "<name>" { setuptools = [ ]; }`), or add the library to the package's `buildInputs` with an `overrideAttrs`. See uv2nix's [overriding guide](https://pyproject-nix.github.io/uv2nix/overriding/index.html).

### `ModuleNotFoundError: No module named 'ate_env_rl'` from a Nix env
- **Symptom:** a Python env's interpreter can't import one of this repository's packages (`ate_env_rl`, `nemo_gym_substrate`, ...).
- **Cause:** the envs install them editable, relative to `$REPO_ROOT`, which the devShell and the `ci-*` scripts set to the repository root. Running an env's `bin/python` some other way (for example from `nix build`'s `result`) leaves it unset.
- **Fix:** `export REPO_ROOT="$(git rev-parse --show-toplevel)"`, or run the command through `nix develop -c`.

### Go toolchain directive vs. Nixpkgs
- **Symptom:** Nix evaluation fails with:
  ```text
  nix/ci.nix: go.mod asks for Go X.Y, but the locked nixpkgs has no pkgs.go_X_Y.
  ```
- **Cause:** The `go` directive in [`go.mod`](../../go.mod) was bumped to a version that the locked `nixpkgs` input does not yet package.
- **Fix:** Lower the `go` directive in `go.mod` to match the latest supported Go version in Nixpkgs, or update the `nixpkgs` flake input:
  ```bash
  make update-pins ARGS=nixpkgs
  ```
  `update-pins` never evaluates the devShell (under Docker it uses the image's `update-pins` stage), so it works even while the devShell is broken; its `uv lock --upgrade` step (which `ARGS=nixpkgs` skips anyway) runs uv through `nix shell`, not the devShell.

### GitHub Actions moving tags vs. immutable SHAs
- **Symptom:** `make lint-actions-locally` fails with a `zizmor` `ref-version-mismatch` error:
  ```text
  ref-version-mismatch: tag v31 does not match commit SHA
  ```
- **Cause:** Workflow actions must be pinned to full 40-character commit SHAs with a trailing version comment specifying an immutable release tag (e.g. `# 31.11.3`), not a moving major tag (e.g. `# v31`).
- **Fix:** Update the workflow action reference in `.github/workflows/*.yaml`:
  ```yaml
  # Correct:
  uses: cachix/install-nix-action@75c75c31ebece974e48ebc36b4ad125bd97bd97b # 31.11.3

  # Incorrect (fails zizmor audit):
  uses: cachix/install-nix-action@75c75c31ebece974e48ebc36b4ad125bd97bd97b # v31
  ```

# Releases and Published Deliverables

A release of `sandboxenv-rl` is anchored to a semantic version tag `vX.Y.Z` (or PEP 440-compatible pre-release tag such as `vX.Y.Z-rc1`) on `main`. The release process produces four core deliverables:

| Deliverable | Distribution Channel | Verification / Security |
| :--- | :--- | :--- |
| **Go CLI (`ate-env`)** | GitHub Release static binaries (`ate-env_<tag>_<os>_<arch>`, download and `chmod +x`) for Linux and macOS (`amd64`, `arm64`) | `SHA256SUMS_<tag>.txt` & Cosign Sigstore bundles (`.sigstore.json`) |
| **Container Images** (`ate-env-api`, `ate-env-guest`) | GitHub Container Registry (`ghcr.io/ai-on-gke/sandboxenv-rl/*`) | Multi-arch (`linux/amd64`, `linux/arm64`), pinned by digest, and signed with Cosign keyless signatures bound to GitHub Actions OIDC |
| **Python Client SDK (`ate-env-rl`)** | PyPI when `vars.PYPI_PUBLISH_ENABLED=true`, or Git tag install otherwise (`clients/python`; packages under `integrations/` are in-repo reference integrations/examples and are not released to PyPI) | Built with `setuptools-scm>=8` and published via PyPA OIDC Trusted Publishing (`id-token: write`, environment `pypi`) |
| **gRPC Contracts & Protos** | Git repository tags & GitHub Release notes | Wire-compatible Protocol Buffers (`proto/ateenv/v1alpha`) |

## What a Release Binary Knows

The `ate-env` binaries attached to a release are built after container images have been pushed and signed, baking their SHA-256 digests into the CLI defaults:
- `ate-env manifest` defaults `--api-image` to the release's `ate-env-api` digest.
- `ate-env manifest template` defaults `--guest-image` to the release's `ate-env-guest` digest.
- `ate-env --version` prints the release tag.

A build from source (`go install ...@latest`, `go build`) has no baked-in defaults and requires both flags. Running `make build-cli` locally reproduces a release-style binary using `ATE_ENV_API_IMAGE` and `ATE_ENV_GUEST_IMAGE`.

> [!NOTE]
>
> The worker image (`--worker-image`) is not published from this repository: it is `ateworker-gvisor` from the [Substrate repository](https://github.com/agent-substrate/substrate) and must match the `go.ate.dev/substrate` revision stamped in the release notes (`Server & Runtime Dependencies` and `## Images`). From a Substrate checkout at that commit, run `go run ./cmd/ate-setup publish worker-images` to build and push it and print its digest-pinned reference. Always use the digest: a registry's `:latest` is whatever was built last, and a stale worker image makes workers register with no capacity so no actor finds room.

## The OpenHands Runtime Layer

Releases after v0.1.0 do not publish the runtime layer that [`integrations/openhands`](../integrations/openhands/README.md) mounts into task images at `/opt/oh`. Build it from [`integrations/openhands/layer`](../integrations/openhands/layer) and push it to your own registry, as the integration's README shows. The integration (not published to PyPI) pins `openhands-sdk` to the same version as the layer's agent server, thus build the layer from the same checkout as the integration.

Release v0.1.0 published `ghcr.io/ai-on-gke/sandboxenv-rl/oh-agent-server-layer:1.55.0-v0.1.0`; that image gets no updates (it predates the #267 security re-lock of the layer's dependencies), and an org admin can delete the package.

## Pin by Digest

Always deploy images by digest, never by tag. Substrate requires `@sha256:` digests for `ActorTemplate`s because a changed image invalidates snapshots, and the `latest` tag moves with every stable release. Both the release notes (`## Images`) and `images_<tag>.txt` record the signed digests, and the released `ate-env` binaries default `--api-image` and `--guest-image` to those digests.

## Artifact & Image Verification

Container images are multi-platform (`linux/amd64`, `linux/arm64`) and signed using Cosign keyless signing bound to this repository's GitHub Actions workflow identity:

```bash
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/ai-on-gke/sandboxenv-rl/\.github/workflows/release\.yaml@refs/tags/' \
  ghcr.io/ai-on-gke/sandboxenv-rl/ate-env-guest@sha256:<digest>
```

The same command verifies `ate-env-api`; replace only the image reference. A valid signature proves the image was built by this repository's `release` workflow from a tag: the certificate identity names `.github/workflows/release.yaml` at a `refs/tags/` ref, so a signature from another workflow or a branch run does not verify.

The workflow signs with Cosign 3 (installed by `sigstore/cosign-installer` v4), which writes the standardized Sigstore bundle format and stores it as an OCI 1.1 referring artifact next to the image. Verify with Cosign 3 or newer; a Cosign 2 client does not find these signatures.

Release CLI binaries, `images_<tag>.txt`, and `SHA256SUMS_<tag>.txt` carry detached Sigstore bundles (`.sigstore.json`) verified with `cosign verify-blob`:

```bash
cosign verify-blob \
  --bundle SHA256SUMS_v0.1.0.txt.sigstore.json \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/ai-on-gke/sandboxenv-rl/\.github/workflows/release\.yaml@refs/tags/' \
  SHA256SUMS_v0.1.0.txt
sha256sum -c --ignore-missing SHA256SUMS_v0.1.0.txt
```

## Cutting a Release

[`CHANGELOG.md`](../CHANGELOG.md) tracks user-visible changes under `## Unreleased` as pull requests land. Keep `## Unreleased` current until `create-release.py context` runs (which extracts `## Unreleased` directly into `hack/release/release-context.md` alongside merged PR `release-note` blocks), then roll `## Unreleased` into a `## vX.Y.Z` section after the release is published.

The release cutting process can be run directly by a maintainer or delegated to an agent via the [`create-release`](../.agents/skills/create-release/SKILL.md) skill, with two mandatory human approval checkpoints before pushing a tag or publishing to PyPI:

1. Agent / Maintainer: Run `create-release.py context`, which extracts `CHANGELOG.md`'s `## Unreleased` section and queries the GitHub `generate-notes` API, merged PR descriptions, `go.mod` / `.ko.yaml` server pins, and contributor list to generate `hack/release/release-context.md`.
2. Agent / Maintainer: Synthesize `hack/release/RELEASE_NOTES_vX.Y.Z.md` from `release-context.md` and `hack/release/release-template.md`, and validate with `create-release.py draft --dry-run` (which verifies a clean working tree, `origin/main` ancestry, and PEP 440 tag compatibility).
3. Human (Checkpoint 1 — Approve Notes & Tag Push): Review and refine `hack/release/RELEASE_NOTES_vX.Y.Z.md`, then approve cutting the draft release.
4. Agent / Maintainer: Run `create-release.py draft`, which pushes the annotated `vX.Y.Z` Git tag and creates a Draft GitHub Release with the approved release notes.
5. CI (`publish` job): Automatically runs on the pushed tag, building and signing container images, compiling `ate-env` binaries, and attaching the 12 assets and `## Images` digest table to the Draft Release.
6. Human (Checkpoint 2 — Officiate Release): Inspect the Draft Release assets and `## Images` digest table on GitHub and click **Publish release**.
7. CI (`publish-pypi` job, optional): When repository variable `PYPI_PUBLISH_ENABLED` is set to `true`, automatically runs on release publication, building `ate-env-rl` (`clients/python`), verifying its built wheel/sdist version matches the release tag, and publishing to PyPI via OIDC Trusted Publishing. When `PYPI_PUBLISH_ENABLED` is unset or `false` (for example, before PyPI Trusted Publishing credentials are configured), `publish-pypi` skips cleanly and users install `ate-env-rl` from the Git tag (`pip install "git+https://github.com/ai-on-gke/sandboxenv-rl.git@<tag>#subdirectory=clients/python"`).

### Detailed Commands

#### Gather Context & Draft Release Notes
```bash
python3 hack/release/create-release.py context --tag v0.1.0 --output hack/release/release-context.md
```
Review `hack/release/release-context.md` and use `hack/release/release-template.md` to draft `hack/release/RELEASE_NOTES_v0.1.0.md`. Categorize changes into Highlights (Core Runtime, Python SDK), verify breaking changes against `CHANGELOG.md` and PR notes, and substitute `$TAG`, `$VERSION` (when PyPI publishing is enabled; otherwise replace the PyPI `pip install` block with the Git-tag `pip install` snippet from `release-template.md`), and `$SERVER_DEPENDENCIES` (the pre-rendered `go.mod` / `.ko.yaml` / OpenHands agent server pin table, including the exact `go.ate.dev/substrate` commit). Both `release-context.md` and `RELEASE_NOTES_*.md` are gitignored scratch files.

#### Push Tag and Create Draft Release
Perform a dry-run check (verifies clean checkout, `origin/main` ancestry, tag format, and unpublished status):
```bash
python3 hack/release/create-release.py draft --dry-run --tag v0.1.0 --notes-file hack/release/RELEASE_NOTES_v0.1.0.md
```
Create (or update) the draft release:
```bash
python3 hack/release/create-release.py draft --tag v0.1.0 --notes-file hack/release/RELEASE_NOTES_v0.1.0.md
```
If the `publish` job has already appended an `## Images` table to the Draft Release, re-running `create-release.py draft` to edit prose automatically preserves that `## Images` section.

#### How the `publish` Job Executes & Republishing on Partial Failure
When the tag push triggers [`.github/workflows/release.yaml`](../.github/workflows/release.yaml), the `publish` job runs in three ordered stages:

1. Builds and pushes `ate-env-api` and `ate-env-guest` via [`hack/release/ko-publish.sh`](../hack/release/ko-publish.sh) with `${TAG}` (plus `latest` on stable tags).
2. Signs both GHCR digests in one `cosign sign` step immediately after push.
3. Cross-compiles `ate-env` for four platforms with the digests baked in, signs the binaries, `images_<tag>.txt`, and `SHA256SUMS_<tag>.txt` into `.sigstore.json` bundles, uploads all 12 files to the Draft Release, and replaces or appends the `## Images` section.

If a step fails, the job stops there: after step 1, a tag points at an unsigned image only until step 2 finishes; if step 3 fails, the images are signed, while the CLI assets or `## Images` block are filled in on republish.

Monitor progress with:
```bash
gh run list --workflow release.yaml
```

To republish an existing tag after a transient failure, trigger `release.yaml` manually from the **Actions** tab: select the tag in **Use workflow from: Tag `vX.Y.Z`** and enter the same `vX.Y.Z` tag as the `tag` input. The workflow refuses any ref other than `refs/tags/<tag>` so the keyless OIDC signature identity matches the verification command above. Because the run executes the workflow file committed at that tag, a fix to `release.yaml` on `main` requires cutting a new tag. Republishing rebuilds the images (producing new digests) and replaces the Draft Release assets and `## Images` block rather than duplicating them.

#### Publish Release and Snowball to PyPI
Open `https://github.com/ai-on-gke/sandboxenv-rl/releases`, review the draft release notes, the 12 attached files (`4` `ate-env` binaries, `images_<tag>.txt`, `SHA256SUMS_<tag>.txt`, and `6` `.sigstore.json` bundles), and the `## Images` table, then click **Publish release**.

## First-Time Repository Setup & Publishing Operations

### Tag Protection & GitHub Environments (Required Before First Release)
Before cutting the first release, configure repository rules and environment gates so unreviewed branch refs cannot publish images or Python packages:

1. **Tag Ruleset for `v*`**: Under repository **Settings -> Rules -> Rulesets**, create a tag ruleset targeting `refs/tags/v*` restricting tag creation, update, and deletion to repository maintainers. Pushing a `v*` tag triggers `publish` (which pushes `:latest` to GHCR on stable tags before Checkpoint 2).
2. **`pypi` GitHub Environment**: Under **Settings -> Environments**, create the `pypi` environment with:
   - **Deployment branches and tags**: Restrict strictly to **Selected branches and tags -> `v*` tags only** (never branches).
   - **Required reviewers**: Configure maintainer reviewers so OIDC token minting for PyPI requires maintainer sign-off.

### GitHub Container Registry (GHCR)
GitHub Container Registry packages are created on the first push by the workflow's `GITHUB_TOKEN`:
1. Allow GitHub Actions to create packages under organization settings (**Packages**).
2. After the first release, open each package (`ate-env-api`, `ate-env-guest`) and confirm it is public and linked to this repository so anonymous pulls and repository permissions apply.

### PyPI Publishing Operations (Optional)
Both PR validation (`ci-release` in [`nix/ci.nix`](../nix/ci.nix)) and release publication (`publish-pypi` in [`.github/workflows/release.yaml`](../.github/workflows/release.yaml)) invoke [`hack/release/build-dists.sh`](../hack/release/build-dists.sh) to build sdist and wheel distributions only for `clients/python` (`ate-env-rl`) via `pyproject-build --no-isolation`, stamping the version from the Git tag via `setuptools-scm>=8` and checking out `refs/tags/<tag>` explicitly. Packages under `integrations/` are in-repo reference integrations/examples and are not published to PyPI.

Automatic PyPI upload on release publication is **opt-in** via the `PYPI_PUBLISH_ENABLED=true` Actions variable. Until PyPI Trusted Publishing is configured, leave `PYPI_PUBLISH_ENABLED` unset so clicking **Publish release** officiates GHCR images and `ate-env` CLI binaries without failing on `publish-pypi`.

1. **Configuring the Trusted Publisher & Enabling Uploads**: On [pypi.org](https://pypi.org) under **Publishing**, register a GitHub publisher (or a **Pending Publisher** prior to `ate-env-rl`'s very first PyPI upload) with:
   - **Owner**: `ai-on-gke`
   - **Repository**: `sandboxenv-rl`
   - **Workflow name**: `release.yaml`
   - **Environment name**: `pypi`
   No long-lived PyPI API tokens or secrets are stored in GitHub. Once the `pypi` GitHub Environment and Trusted Publisher are in place, set repository variable `PYPI_PUBLISH_ENABLED` to `true` under **Settings -> Secrets and variables -> Actions -> Variables**.
2. **Backfilling PyPI for an Already-Published Release**: If a release tag was published before PyPI Trusted Publishing was ready, you can publish `ate-env-rl` for that tag later without cutting a new version or rebuilding container images: configure the `pypi` Environment, register the Trusted Publisher, set `PYPI_PUBLISH_ENABLED=true`, and trigger `release.yaml` from **Actions -> release -> Run workflow** with **Use workflow from: Tag `vX.Y.Z`**, `tag: vX.Y.Z`, and `publish_pypi: true`. The job verifies that the GitHub Release is published (`isDraft == false`) and uploads from `refs/tags/vX.Y.Z` under `environment: pypi`.
3. **Staging with TestPyPI**: On a fork or staging repository, set Actions variables `PYPI_PUBLISH_ENABLED=true` and `PYPI_REPOSITORY_URL=https://test.pypi.org/legacy/` (and optionally register a matching TestPyPI Trusted Publisher) to validate the end-to-end release trigger without touching production PyPI.
4. **Recovering from `invalid-publisher` Failures**: If `publish-pypi` fails with `invalid-publisher` after a release is published because the Trusted Publisher entry was missing or mistyped on `pypi.org`, add or fix the publisher entry on `pypi.org` and click **Re-run failed jobs** on the `release` workflow run. Because `pypa/gh-action-pypi-publish` runs with `skip-existing: true`, re-running is idempotent and publishes cleanly without requiring a new Git tag.

## Reproducibility & Supply Chain Security Notes

- **Hermetic Nix Environment**: Release workflows execute CLI cross-compilation, container image builds, and Python packaging within the repository's Nix environment (`flake.lock` and `uv.lock`). Python distributions are built with `pyproject-build --no-isolation` using Nix-provided `build`, `setuptools`, `setuptools-scm`, and `wheel` rather than dynamic runtime `pip` installations.
- **Checksums**: Only a single versioned checksum file (`SHA256SUMS_${TAG}.txt`) is generated and uploaded alongside `.sigstore.json` Cosign bundles.
- **Base image pins**:
  - The guest base image is pinned by digest in [`.ko.yaml`](../.ko.yaml) (a `bash` image so templates whose container is the guest itself have a shell for `shell()`; rootfs-image templates use only the static binary). Bump this pin deliberately and call it out in release notes.
  - The API image uses `ko`'s default distroless base.
- **Action pinning & cache isolation**: All GitHub Actions are pinned by full commit SHA with a trailing version comment (`zizmor` enforces this). To bump an action, resolve the tag's commit via `git ls-remote --tags https://github.com/<owner>/<action> v<x.y.z>` and update the SHA and comment together. Release builds run with `persist-credentials: false` and Go module caching disabled so a release build cannot consume a cache seeded by another branch run.
- **Development vs. release images**: `make images` builds `ate-env-api` and `ate-env-guest` for a private development registry (`ATE_ENV_IMAGE_REPO`); always point users at the signed release digests.

## Troubleshooting

- **GHCR `context canceled` during `ko build`**: Concurrent multi-arch manifest uploads to GHCR can occasionally drop with `context canceled`. [`hack/release/ko-publish.sh`](../hack/release/ko-publish.sh) wraps `ko build` in a 3-attempt retry loop as a workaround. If a sustained GHCR brownout exhausts all 3 attempts, click **Re-run failed jobs** on the workflow run, or manually trigger `release.yaml` via `workflow_dispatch` with **Use workflow from: Tag `vX.Y.Z`**.
- **Online `zizmor` failing in CI when local `lint-actions-locally` passed**: `nix develop -c lint-actions-locally` runs `zizmor --offline`, which checks static workflow rules and SHA pinning without querying the live GitHub Advisory Database. CI runs online `zizmor` and fails when a new GHSA is published against a pinned action version. Reproduce locally with:

  ```bash
  GH_TOKEN="$(gh auth token)" nix develop -c zizmor --min-severity medium .github/workflows .github/actions
  ```

  Then update the flagged action to a patched commit SHA and trailing version comment.
- **Nix `PYTHONPATH` leakage breaking `ci-python` base-env checks**: `ci-python` asserts that `pythonEnvBase` cannot import optional extras (`opentelemetry`, `prometheus_client`). Adding raw `pkgs.python3Packages.*` packages directly to `ciPackages` in `nix/ci.nix` triggers nixpkgs' Python `setup-hook`, exporting their dependency closure onto `PYTHONPATH` inside `nix develop`. Always expose standalone Python CLI tools in `ciPackages` through a `pkgs.runCommand` symlink wrapper (see `pythonReleaseTools` and `diffCover` in [`nix/ci.nix`](../nix/ci.nix)) so no `setup-hook` leaks into test environments.

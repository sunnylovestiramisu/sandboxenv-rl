---
name: create-release
description:
  Draft and cut releases for sandboxenv-rl (Agent Substrate Environments), including release notes
  generation, human-in-the-loop review, git tag push, draft GitHub Release creation, and automated
  PyPI / container image publishing.
---

# Cutting Releases for Agent Substrate Environments (`sandboxenv-rl`)

Use this skill when preparing, drafting, and publishing a release of `sandboxenv-rl`.

## Release Architecture & Deliverables

Every `sandboxenv-rl` release produces four core deliverables:

| Deliverable                                           | Distribution Channel                                                                                                                                                             | Verification / Security                                                                    |
| :---------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------- |
| **Go CLI (`ate-env`)**                                | GitHub Release static binaries (`ate-env_<tag>_<os>_<arch>`, `chmod +x`)                                                                                                         | `SHA256SUMS_${TAG}.txt` & Cosign Sigstore bundles (`.sigstore.json`)                       |
| **Container Images** (`ate-env-api`, `ate-env-guest`) | GitHub Container Registry (`ghcr.io/ai-on-gke/sandboxenv-rl/*`)                                                                                                                  | Multi-arch (`linux/amd64`, `linux/arm64`), pinned by digest, and signed with Cosign OIDC   |
| **Python Client SDK (`ate-env-rl`)**                  | PyPI when `vars.PYPI_PUBLISH_ENABLED=true`, or Git tag install when PyPI is skipped (`clients/python`; packages under `integrations/` are in-repo examples and are not released) | `setuptools-scm>=8` & PyPA OIDC Trusted Publishing (`id-token: write`, environment `pypi`) |
| **gRPC Contracts & Protos**                           | Git repository tags & GitHub Release notes                                                                                                                                       | Protocol buffer wire compatibility                                                         |

## Release Procedure Overview

When assisting a maintainer with a release, the agent executes the scripts and prepares the
artifacts while the human inspects those artifacts and stamps approval at two explicit checkpoints:

1. Agent: Run `create-release.py context`, which queries the GitHub `generate-notes` API, merged PR
   descriptions, PyPI opt-in status (`vars.PYPI_PUBLISH_ENABLED`), and contributor list to generate
   `hack/release/release-context.md`.
2. Agent: Synthesize `hack/release/RELEASE_NOTES_vX.Y.Z.md` from `release-context.md` and
   `hack/release/release-template.md`, validate with `create-release.py draft --dry-run`, and
   present the drafted release notes to the human.
3. Human (Checkpoint 1 — Approve Notes & Tag Push): Review `hack/release/RELEASE_NOTES_vX.Y.Z.md`,
   request any edits, and explicitly approve pushing the tag and creating the draft release.
4. Agent: Run `create-release.py draft`, which pushes the annotated `vX.Y.Z` Git tag and creates a
   Draft GitHub Release with the approved release notes.
5. CI: Automatically runs on the pushed tag, building and signing container images, compiling
   `ate-env` binaries, and attaching assets and image digests to the Draft Release.
6. Agent: Monitor the `release.yaml` workflow run to completion, verify that the 12 assets and
   `## Images` digest table are attached to the Draft Release, and present the Draft Release URL to
   the human.
7. Human (Checkpoint 2 — Officiate Release): Inspect the Draft Release on GitHub and click **Publish
   release**.
8. CI (optional PyPI step): If `vars.PYPI_PUBLISH_ENABLED == 'true'`, automatically runs
   `publish-pypi` on release publication to build and publish `ate-env-rl` (`clients/python`) to
   PyPI via OIDC Trusted Publishing. If `vars.PYPI_PUBLISH_ENABLED` is unset/false (or PyPI is
   skipped for this release), `publish-pypi` is cleanly skipped without failing the workflow.

> [!IMPORTANT]
>
> **Hard Agent Guardrails:**
>
> - **Proactive Gate 1 Preparation, Mandatory Stop Before Tag Push**: When asked to cut or prepare a
>   release, immediately run `create-release.py context`, write
>   `hack/release/RELEASE_NOTES_<tag>.md`, and run `create-release.py draft --dry-run` without
>   waiting to be prompted for each sub-step. Then **stop and present the drafted notes**. Never run
>   `create-release.py draft` (without `--dry-run`) or `git push <remote> refs/tags/<tag>` until the
>   human explicitly approves.
> - **Never Publish a Release Directly (Gate 2)**: Never run `gh release edit --draft=false` or
>   `gh release create` without `--draft`. Clicking **Publish release** in the GitHub UI is strictly
>   a human approval gate because it officiates the public GitHub Release (and triggers immutable
>   PyPI uploads when `vars.PYPI_PUBLISH_ENABLED=true`). Never publish a draft release via CLI, even
>   when PyPI publishing is skipped.
> - **PyPI Publishing Is Optional (Proceed Normally When Skipped or Unconfigured)**: PyPI publishing
>   is opt-in (`vars.PYPI_PUBLISH_ENABLED=true`). If the release manager says PyPI credentials or
>   Trusted Publishing are not available yet (or asks to skip PyPI), or if `release-context.md`
>   reports `vars.PYPI_PUBLISH_ENABLED` is disabled, **do not block, hesitate, or abort the
>   release**. Proceed normally through all steps (context, release notes using the Git-tag
>   `pip install` fallback instead of PyPI, Checkpoint 1, tag push, GHCR + CLI `publish` CI, and
>   Checkpoint 2), passing `--skip-pypi` if explicitly requested.
> - **Pull Request Hygiene**: `hack/release/release-context.md` and
>   `hack/release/RELEASE_NOTES_*.md` are gitignored scratch files. Never force-add them to commits
>   or push release tags when asked to create or update a pull request.

## Step-by-Step Instructions

### Gather Release Context

Run the release context command for the target semantic version:

```bash
python3 hack/release/create-release.py context --tag v0.1.0 --output hack/release/release-context.md
```

Options:

- `--tag <vX.Y.Z>`: The semver tag being prepared (e.g. `v0.1.0` or `v0.1.0-rc1`). Required.
- `--previous-tag <vA.B.C>`: Previous release tag to diff from. Optional (GitHub auto-detects if
  omitted).
- `--target-commitish <sha|branch>`: Commit or branch to tag (defaults to `HEAD`).
- `--repo <owner/name>`: Explicit GitHub repository (defaults to detecting from `origin`).
- `--skip-pypi`: Explicitly mark PyPI publishing as skipped in `release-context.md` (auto-detected
  from `vars.PYPI_PUBLISH_ENABLED` when omitted).

This command:

1. Calls the GitHub `releases/generate-notes` API to retrieve the PR list and commit ranges.
2. Queries PR details (labels, author, title) and extracts explicit `## Release note` and `## What`
   blocks from PR descriptions.
3. Computes the PEP 440 Python version (`$VERSION`, e.g. `0.1.0` or `0.1.0rc1`), checks whether
   `vars.PYPI_PUBLISH_ENABLED` is enabled on the repository, extracts the `## Unreleased` section
   from `CHANGELOG.md`, and builds the `$SERVER_DEPENDENCIES` Markdown table from `go.mod`
   (including the resolved `go.ate.dev/substrate` commit link), `.ko.yaml`, and
   `integrations/openhands/layer/requirements.in`.
4. Compiles a deduplicated list of non-bot contributors and outputs
   `hack/release/release-context.md`.

### Synthesize Release Notes and Pause for Checkpoint 1

1. Read `hack/release/release-context.md` and `hack/release/release-template.md`.
2. Draft `hack/release/RELEASE_NOTES_<tag>.md` following the template:
   - **Title & Announcement**: Mention the release tag and short summary.
   - **Pre-Release Notice**: Uncomment if the version is an alpha, beta, or release candidate.
   - **Breaking Changes / Action Required**: Highlight any breaking protocol, API, or configuration
     changes.
   - **Highlights**: Group merged pull requests by area:
     - _Core Runtime, CLI & Substrate_
     - _Python SDK & Fleet_
     - _Observability & Benchmarking_ Link every bullet point to its PR
       (`* Description of feature or fix (#<pr_number>)`).
   - **Full Changelog**: Include the categorized list or compare URL from the generated notes.
   - **Installation**: Substitute `$TAG` into the CLI and GHCR/`cosign verify` blocks (adjusting
     `ai-on-gke/sandboxenv-rl` to `<owner>/<repo>` if cutting a release on a fork). For
     `#### Python Client SDK`:
     - **When PyPI publishing is enabled** (`vars.PYPI_PUBLISH_ENABLED=true` and not skipped):
       substitute `$VERSION` into `pip install "ate-env-rl==$VERSION"`.
     - **When PyPI publishing is skipped or not configured** (the user asked to skip PyPI or
       `release-context.md` shows `PyPI Publishing` is disabled/skipped): replace the PyPI `pip`
       snippet with the Git-tag installation snippet from `hack/release/release-template.md`
       (`pip install "git+https://github.com/<owner>/<repo>.git@$TAG#subdirectory=clients/python"`),
       removing the HTML comments.
   - **Server & Runtime Dependencies**: Copy the pre-rendered `$SERVER_DEPENDENCIES` table from
     `release-context.md` verbatim so cluster operators have the exact `go.ate.dev/substrate`
     revision and server build pins.
   - **Contributors**: List all contributor `@handles`.
3. Validate with a dry run:

   ```bash
   python3 hack/release/create-release.py draft --dry-run --tag v0.1.0 --notes-file hack/release/RELEASE_NOTES_v0.1.0.md
   ```

4. **Checkpoint 1**: Present `hack/release/RELEASE_NOTES_<tag>.md` (or a clean summary + link) to
   the human and wait for explicit approval before pushing the tag.

### Create the Draft Release (After Checkpoint 1 Approval)

Once the human approves the release notes:

```bash
python3 hack/release/create-release.py draft --tag v0.1.0 --notes-file hack/release/RELEASE_NOTES_v0.1.0.md
```

This command:

- Refuses to overwrite an already-published release.
- Creates the local annotated tag `vX.Y.Z` if not already present and pushes `refs/tags/vX.Y.Z` to
  `origin`.
- Creates or updates a **draft** release on GitHub (`--draft --verify-tag`), automatically
  preserving any `## Images` block already appended by CI if re-run to update release notes.

### Monitor Automated CI Build & Signing

The tag push triggers the `publish` job in `.github/workflows/release.yaml`:

- **Container Build & Sign**: Builds multi-arch images (`linux/amd64`, `linux/arm64`) for
  `ate-env-api` and `ate-env-guest`, pushes them to GHCR (updating `:latest` only on stable tags
  without a `-` pre-release suffix), and signs them with keyless Cosign OIDC.
- **CLI Compilation & Signing**: Compiles `ate-env` for Linux and macOS (`amd64`, `arm64`) with
  baked-in image digests, writes `images_${TAG}.txt` and `SHA256SUMS_${TAG}.txt`, and generates
  `.sigstore.json` Cosign bundles (`cosign sign-blob --bundle`).
- **Asset Attachment**: Uploads all 12 files (`4` static `ate-env` binaries, `images_${TAG}.txt`,
  `SHA256SUMS_${TAG}.txt`, and `6` `.sigstore.json` bundles) to the Draft GitHub Release and appends
  an idempotent `## Images` digest table to the draft notes.

Monitor the workflow run to completion:

```bash
gh run list --workflow release.yaml
```

### Present Draft for Checkpoint 2 (Officiate Release)

1. Once `publish` succeeds, verify via `gh release view <tag> --json isDraft,assets,body` that
   `isDraft` is `true`, all 12 assets are attached, and `## Images` contains the signed digests.
2. **Checkpoint 2**: Share the Draft Release URL with the human so they can inspect the artifacts
   and click **Publish release** (do **not** publish the draft via CLI).
3. Explain what happens when the human clicks **Publish release**:
   - **If `vars.PYPI_PUBLISH_ENABLED == 'true'`**: `release: types: [published]` triggers the
     `publish-pypi` job in `.github/workflows/release.yaml` to run `hack/release/build-dists.sh`
     (building `clients/python` sdist and wheel via `pyproject-build --no-isolation`, checking
     `twine`, and verifying the built PEP 440 version matches the release tag) and upload them to
     PyPI via OIDC Trusted Publishing.
   - **If `vars.PYPI_PUBLISH_ENABLED` is unset/false (or PyPI is skipped)**: `publish-pypi` is
     cleanly skipped (no workflow failure), officiating the GitHub Release, signed CLI binaries, and
     GHCR container images on their own.

## PyPI Publishing Operations (Optional)

Both `ci-release` (in `nix/ci.nix`) and `publish-pypi` (in `.github/workflows/release.yaml`) run
`hack/release/build-dists.sh` to build only `clients/python` (`ate-env-rl`), deriving its version
from the Git tag via `setuptools-scm>=8` (`root = "../.."`). Packages under `integrations/` are
in-repo reference integrations/examples and are not published to PyPI.

PyPI publishing is **optional** and gated on repository variable `PYPI_PUBLISH_ENABLED=true`. While
`PYPI_PUBLISH_ENABLED` is unset or `false`, releases publish GHCR images and `ate-env` CLI binaries
cleanly without running `publish-pypi`.

1. **Required Repository & Environment Guardrails (Before Enabling PyPI)**:
   - Configure a GitHub **Tag Ruleset** on `refs/tags/v*` restricting tag creation, update, and
     deletion to repository maintainers (required before the very first release regardless of PyPI).
   - Configure the `pypi` GitHub Environment under **Settings -> Environments** with **Deployment
     branches and tags** restricted strictly to `v*` tags (never branches) and **Required
     reviewers** enabled.
2. **Trusted Publisher Registration (`pypi.org`) & Enabling Automatic Uploads**: Register a GitHub
   Trusted Publisher (or a **Pending Publisher** prior to the very first PyPI upload) for
   `ate-env-rl` on [pypi.org](https://pypi.org) under **Publishing**:
   - **Owner**: `ai-on-gke`
   - **Repository**: `sandboxenv-rl`
   - **Workflow name**: `release.yaml`
   - **Environment name**: `pypi` (matching `environment: pypi` on the `publish-pypi` job) Once
     configured, set repository variable `PYPI_PUBLISH_ENABLED` to `true` so future published
     releases automatically trigger `publish-pypi`.
3. **Backfilling PyPI for an Already-Published Release**: If a release was cut and published while
   PyPI credentials were not yet ready (`PYPI_PUBLISH_ENABLED` unset), once Trusted Publishing and
   the `pypi` Environment are configured and `PYPI_PUBLISH_ENABLED=true` is set, trigger
   `release.yaml` manually via **Actions -> release -> Run workflow** with **Use workflow from: Tag
   `vX.Y.Z`**, `tag: vX.Y.Z`, and `publish_pypi: true`. The workflow verifies that the GitHub
   Release is no longer a draft and publishes `ate-env-rl` from `refs/tags/vX.Y.Z` without
   rebuilding container images.
4. **Staging / TestPyPI Rehearsals**: Set repository variables `PYPI_PUBLISH_ENABLED=true` and
   `PYPI_REPOSITORY_URL=https://test.pypi.org/legacy/` on a staging repository or fork to route
   `pypa/gh-action-pypi-publish` to TestPyPI instead of production PyPI.
5. **Recovering from `invalid-publisher` Failures**: If `publish-pypi` fails with
   `invalid-publisher` after a release is published (for example, the Trusted Publisher claim on
   `pypi.org` was missing or mistyped), register or fix the Trusted Publisher on `pypi.org` and
   click **Re-run failed jobs** on that `release` workflow run. Because
   `pypa/gh-action-pypi-publish` runs with `skip-existing: true`, re-running is idempotent without
   cutting a new tag.

## Troubleshooting

- **GHCR `context canceled` during `ko build`**: Concurrent multi-arch manifest uploads to GHCR can
  occasionally hit transient HTTP/2 stream resets. `hack/release/ko-publish.sh` wraps `ko build` in
  a 3-attempt retry helper as a workaround. If all 3 attempts fail during a GHCR brownout, click
  **Re-run failed jobs** in GitHub Actions, or trigger `workflow_dispatch` on `release.yaml` with
  **Use workflow from: Tag `vX.Y.Z`** (never from `Branch: main` — `Resolve the tag` enforces
  `GITHUB_REF == refs/tags/<tag>` so the Cosign OIDC certificate identity matches
  `docs/release.md`).
- **Online `zizmor` failing in CI after `lint-actions-locally` passed**:
  `nix develop -c lint-actions-locally` invokes `zizmor --offline`, which validates workflow
  structure and SHA pinning offline without querying the live GitHub Advisory Database. CI runs
  online `zizmor`, which will flag newly published GHSAs on pinned actions. Reproduce locally with:

  ```bash
  GH_TOKEN="$(gh auth token)" nix develop -c zizmor --min-severity medium .github/workflows .github/actions
  ```

  Then bump the flagged action to a patched commit SHA and update its version comment.

- **Nix `PYTHONPATH` leakage breaking `ci-python` base-env checks**: `ci-python` verifies that
  `pythonEnvBase` cannot import optional extras (`opentelemetry`, `prometheus_client`). Adding raw
  `pkgs.python3Packages.*` derivations directly to `ciPackages` in `nix/ci.nix` activates nixpkgs'
  Python `setup-hook`, leaking site-packages onto `PYTHONPATH` inside `nix develop`. Whenever adding
  Python CLI tools to `ciPackages` (such as `pyproject-build`, `twine`, or `diff-cover`), expose
  only their binaries through a `pkgs.runCommand` symlink wrapper (like `pythonReleaseTools` and
  `diffCover` in `nix/ci.nix`) so no `setup-hook` is propagated.

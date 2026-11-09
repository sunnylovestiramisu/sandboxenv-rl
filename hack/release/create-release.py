#!/usr/bin/env python3

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

"""Release automation tool for Agent Substrate Environments (sandboxenv-rl).

Subcommands:
  context: Gathers PR metadata, GitHub generated release notes, and contributor lists
           into a structured context file for drafting release notes.
  draft:   Verifies release notes, creates/pushes the git tag, and creates a draft
           GitHub Release (which triggers container build and CLI binary packaging CI).
  pep440:  Validates a release Git tag and prints its PEP 440 Python version string.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

SEMVER_REGEX = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|a|beta|b|rc|c|dev)\.?[0-9]+)?$")
RELEASE_NOTE_BLOCK_REGEX = re.compile(
    r"(?i)##\s*Release\s*note\s*[\r\n]+(?:<!--[\s\S]*?-->\s*)*```(?:release-note)?\s*([\s\S]*?)\s*```"
)
IMAGES_SECTION_REGEX = re.compile(r"(?m)^## Images\s*$[\s\S]*?(?=^## (?!Images\b)|\Z)")
CHANGELOG_UNRELEASED_REGEX = re.compile(r"(?m)^## Unreleased\s*$[\r\n]+([\s\S]*?)(?=^## |\Z)")


def pep440_version(tag: str) -> str:
    """Converts a semver git tag (e.g. v0.1.0 or v0.1.0-rc1) to its setuptools-scm PEP 440 version."""
    raw = tag.removeprefix("v")
    raw = re.sub(r"-(?:alpha|a)\.?(\d+)$", r"a\1", raw)
    raw = re.sub(r"-(?:beta|b)\.?(\d+)$", r"b\1", raw)
    raw = re.sub(r"-(?:rc|c)\.?(\d+)$", r"rc\1", raw)
    raw = re.sub(r"-dev\.?(\d+)$", r".dev\1", raw)
    return raw


def detect_openhands_layer_version(repo_root: Path) -> str | None:
    """Reads openhands-agent-server==<version> from integrations/openhands/layer/requirements.in."""
    req_in = repo_root / "integrations" / "openhands" / "layer" / "requirements.in"
    if not req_in.exists():
        return None
    for line in req_in.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^openhands-agent-server==([0-9][0-9.]*)\s*$", line.strip())
        if m:
            return m.group(1)
    return None


def parse_go_mod(repo_root: Path) -> tuple[str | None, dict[str, str]]:
    """Extracts the Go toolchain version and direct module requirements from go.mod."""
    go_mod = repo_root / "go.mod"
    if not go_mod.exists():
        return None, {}
    go_version: str | None = None
    direct_reqs: dict[str, str] = {}
    in_require = False
    for raw_line in go_mod.read_text(encoding="utf-8").splitlines():
        is_indirect = "// indirect" in raw_line
        line = raw_line.split("//", 1)[0].strip()
        if not line:
            continue
        if line.startswith("go "):
            parts = line.split()
            if len(parts) >= 2:
                go_version = parts[1]
            continue
        if line.startswith("require ("):
            in_require = True
            continue
        if in_require and line == ")":
            in_require = False
            continue
        if line.startswith("require ") and not is_indirect:
            parts = line.removeprefix("require ").split()
            if len(parts) >= 2:
                direct_reqs[parts[0]] = parts[1]
            continue
        if in_require and not is_indirect:
            parts = line.split()
            if len(parts) >= 2:
                direct_reqs[parts[0]] = parts[1]
    return go_version, direct_reqs


def detect_guest_base_image(repo_root: Path) -> str | None:
    """Reads the pinned ate-env-guest base image from .ko.yaml."""
    ko_yaml = repo_root / ".ko.yaml"
    if not ko_yaml.exists():
        return None
    for raw_line in ko_yaml.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        m = re.match(r"^github\.com/agent-substrate/env/cmd/ate-env-guest:\s*(\S+)\s*$", line)
        if m:
            return m.group(1)
    return None


def resolve_substrate_pin(sub_ver: str | None) -> tuple[str, str]:
    """Resolves a go.ate.dev/substrate version string to (short_ref, github_markdown_link)."""
    if not sub_ver:
        return "unknown", "`unknown`"
    pseudo_match = re.search(r"(?:^|-|\.)\d{14}-([0-9a-f]{12,40})$", sub_ver)
    ref = pseudo_match.group(1) if pseudo_match else sub_ver
    proc = run_cmd(
        ["gh", "api", f"/repos/agent-substrate/substrate/commits/{ref}", "--jq", ".sha"],
        check=False,
    )
    full_sha = proc.stdout.strip() if proc.returncode == 0 else ""
    if re.fullmatch(r"[0-9a-f]{40}", full_sha):
        short_sha = full_sha[:12]
        return short_sha, f"`{sub_ver}` ([`{short_sha}`](https://github.com/agent-substrate/substrate/commit/{full_sha}))"
    if pseudo_match:
        return ref, f"`{sub_ver}` ([`{ref}`](https://github.com/agent-substrate/substrate/commit/{ref}))"
    return sub_ver, f"[`{sub_ver}`](https://github.com/agent-substrate/substrate/releases/tag/{sub_ver})"


def build_server_dependencies_table(repo_root: Path, oh_ver: str | None) -> str:
    """Builds a Markdown table of server, cluster, and image dependencies for release notes."""
    go_ver, reqs = parse_go_mod(repo_root)
    _, sub_cell = resolve_substrate_pin(reqs.get("go.ate.dev/substrate"))
    guest_base = detect_guest_base_image(repo_root)

    rows: list[tuple[str, str, str]] = [
        (
            "**Agent Substrate (`go.ate.dev/substrate`)**",
            sub_cell,
            "Required cluster control plane (`ate-system`) & `--worker-image` (`ateworker-gvisor`)",
        ),
    ]
    if "k8s.io/api" in reqs:
        rows.append((
            "**Kubernetes API (`k8s.io/api`)**",
            f"`{reqs['k8s.io/api']}`",
            "Manifest & CRD client schema (`ate-env manifest`)",
        ))
    if go_ver:
        rows.append((
            "**Go Toolchain**",
            f"`{go_ver}`",
            "`ate-env`, `ate-env-api`, and `ate-env-guest` builds",
        ))
    grpc_parts = [
        f"`{label} {reqs[mod]}`"
        for mod, label in (("google.golang.org/grpc", "grpc"), ("google.golang.org/protobuf", "protobuf"))
        if mod in reqs
    ]
    if grpc_parts:
        rows.append((
            "**gRPC & Protobuf (`go.mod`)**",
            ", ".join(grpc_parts),
            "`ateenv.v1alpha` wire transport",
        ))
    if "github.com/modelcontextprotocol/go-sdk" in reqs:
        rows.append((
            "**MCP Go SDK (`modelcontextprotocol/go-sdk`)**",
            f"`{reqs['github.com/modelcontextprotocol/go-sdk']}`",
            "Built-in `POST /v1alpha/envs/{id}/mcp` server",
        ))
    telemetry_parts = [
        f"`{label} {reqs[mod]}`"
        for mod, label in (
            ("go.opentelemetry.io/otel", "otel"),
            ("go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc", "otelgrpc"),
            ("github.com/prometheus/client_golang", "prometheus"),
        )
        if mod in reqs
    ]
    if telemetry_parts:
        rows.append((
            "**Telemetry (`go.mod`)**",
            ", ".join(telemetry_parts),
            "OTLP/gRPC tracing & `/metrics` collector",
        ))
    if guest_base:
        rows.append((
            "**Guest Base Image (`.ko.yaml`)**",
            f"`{guest_base}`",
            "Base layer for `ate-env-guest`",
        ))
    if oh_ver:
        rows.append((
            "**OpenHands Agent Server**",
            f"`{oh_ver}`",
            "`integrations/openhands/layer` (not published; users build the layer into their own registry)",
        ))

    lines = [
        "| Dependency | Pinned Version | Scope |",
        "| :--- | :--- | :--- |",
    ]
    for dep, ver, scope in rows:
        lines.append(f"| {dep} | {ver} | {scope} |")
    return "\n".join(lines)


def preserve_images_section(new_notes: str, existing_body: str | None) -> str:
    """Appends an existing CI-generated '## Images' block if new_notes does not already contain one."""
    cleaned_new = new_notes.replace("\r", "").rstrip()
    if re.search(r"(?m)^## Images\s*$", cleaned_new):
        return cleaned_new + "\n"
    if not existing_body:
        return cleaned_new + "\n"
    match = IMAGES_SECTION_REGEX.search(existing_body.replace("\r", ""))
    if not match:
        return cleaned_new + "\n"
    return f"{cleaned_new}\n\n{match.group(0).strip()}\n"


def fetch_release_by_tag(repo: str, tag: str) -> dict | None:
    """Looks up a published or draft GitHub Release by tag via 'gh release view'."""
    proc = run_cmd(
        ["gh", "release", "view", tag, "--repo", repo, "--json", "isDraft,url,body"],
        check=False,
    )
    if proc.returncode != 0:
        return None
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse release JSON for tag {tag}: {e}", file=sys.stderr)
        return None


def detect_pypi_publish_status(repo: str, skip_pypi: bool = False) -> tuple[bool, str]:
    """Checks whether PyPI publishing is enabled via vars.PYPI_PUBLISH_ENABLED on repo."""
    if skip_pypi:
        return (
            False,
            "skipped (--skip-pypi passed — proceed with release; use the Git-tag pip install snippet in release notes)",
        )
    proc = run_cmd(
        ["gh", "api", f"/repos/{repo}/actions/variables/PYPI_PUBLISH_ENABLED", "--jq", ".value"],
        check=False,
    )
    enabled = proc.returncode == 0 and proc.stdout.strip().lower() == "true"
    if enabled:
        url_proc = run_cmd(
            ["gh", "api", f"/repos/{repo}/actions/variables/PYPI_REPOSITORY_URL", "--jq", ".value"],
            check=False,
        )
        repo_url = url_proc.stdout.strip() if url_proc.returncode == 0 else ""
        if repo_url:
            return True, f"enabled (vars.PYPI_PUBLISH_ENABLED=true, repository={repo_url})"
        return True, "enabled (vars.PYPI_PUBLISH_ENABLED=true)"
    return (
        False,
        (
            "disabled (vars.PYPI_PUBLISH_ENABLED != 'true' — optional publish-pypi job will be skipped; "
            "proceed with release and use the Git-tag pip install snippet in release notes)"
        ),
    )


def run_cmd(cmd: list[str], check: bool = True, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Runs a command and returns CompletedProcess with captured text output."""
    return subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=check,
    )


def detect_repo(repo_arg: str | None = None, remote: str = "origin") -> str:
    """Detects GitHub repository owner/name from --repo, GITHUB_REPOSITORY, or git remote URL."""
    if repo_arg:
        return repo_arg

    env_repo = os.environ.get("GITHUB_REPOSITORY")
    if env_repo:
        return env_repo

    proc = run_cmd(["git", "config", "--get", f"remote.{remote}.url"], check=False)
    url = proc.stdout.strip() if proc.returncode == 0 else ""
    match = re.search(r"github\.com[:/]([a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+)(?:\.git)?$", url)
    if match:
        return match.group(1).removesuffix(".git")

    print(
        f"Error: Could not detect GitHub repository from git remote '{remote}' (url={url!r}); "
        "pass --repo <owner/name> explicitly.",
        file=sys.stderr,
    )
    sys.exit(1)


def extract_changelog_unreleased(repo_root: Path) -> str | None:
    """Extracts the body of the '## Unreleased' section from CHANGELOG.md if present and non-empty."""
    changelog = repo_root / "CHANGELOG.md"
    if not changelog.exists():
        return None
    match = CHANGELOG_UNRELEASED_REGEX.search(changelog.read_text(encoding="utf-8"))
    if not match:
        return None
    body = match.group(1).strip()
    return body or None


def extract_release_note(pr_body: str | None) -> str | None:
    """Extracts the ```release-note block from PR body if present and non-trivial."""
    if not pr_body:
        return None
    match = RELEASE_NOTE_BLOCK_REGEX.search(pr_body)
    if not match:
        return None
    raw_note = match.group(1).strip()
    if not raw_note or raw_note.upper() in ("NONE", "N/A", "NO"):
        return None
    return raw_note


def extract_pr_summary(pr_body: str | None) -> str | None:
    """Extracts a short summary from the '## What' section of the PR body if available."""
    if not pr_body:
        return None
    match = re.search(r"(?i)##\s*What\s*[\r\n]+([\s\S]*?)(?=##|\Z)", pr_body)
    if not match:
        return None
    lines = [line.strip() for line in match.group(1).strip().splitlines() if line.strip() and not line.strip().startswith("<!--")]
    if not lines:
        return None
    # Return first 2 non-empty bullet points or sentences
    return " ".join(lines[:2])


def fetch_merged_prs(repo: str) -> dict[int, dict]:
    """Fetches recently merged PRs from GitHub as a lookup dict keyed by PR number."""
    cmd = [
        "gh", "pr", "list",
        "--repo", repo,
        "--state", "merged",
        "--limit", "150",
        "--json", "number,title,author,labels,body,url,mergedAt",
    ]
    proc = run_cmd(cmd, check=False)
    if proc.returncode != 0:
        print(f"Warning: Failed to fetch PR list via 'gh': {proc.stderr.strip()}", file=sys.stderr)
        return {}

    try:
        prs_list = json.loads(proc.stdout)
        return {pr["number"]: pr for pr in prs_list}
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse PR JSON: {e}", file=sys.stderr)
        return {}


def handle_context(args: argparse.Namespace) -> None:
    """Gathers release context and writes to output file."""
    repo = detect_repo(args.repo)
    tag = args.tag
    if not SEMVER_REGEX.match(tag):
        print(
            f"Error: Tag '{tag}' is not a valid PEP 440-compatible semver tag "
            "(expected format vX.Y.Z or vX.Y.Z-{alpha,beta,rc,dev}N)",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Collecting release notes context for {repo} tag {tag}...")

    # Call GitHub generate-notes API
    gen_cmd = [
        "gh", "api",
        "--method", "POST",
        "-H", "Accept: application/vnd.github+json",
        f"/repos/{repo}/releases/generate-notes",
        "-f", f"tag_name={tag}",
    ]
    if args.previous_tag:
        gen_cmd.extend(["-f", f"previous_tag_name={args.previous_tag}"])
    if args.target_commitish:
        gen_cmd.extend(["-f", f"target_commitish={args.target_commitish}"])

    gen_proc = run_cmd(gen_cmd, check=False)
    if gen_proc.returncode != 0:
        print(f"Error calling GitHub generate-notes API:\n{gen_proc.stderr}", file=sys.stderr)
        sys.exit(1)

    gen_data = json.loads(gen_proc.stdout)
    gh_notes = gen_data.get("body", "")

    # Extract all PR numbers referenced in generated notes
    pr_numbers = [int(n) for n in re.findall(r"/pull/(\d+)", gh_notes)]
    unique_pr_nums = sorted(set(pr_numbers), reverse=True)

    print(f"Found {len(unique_pr_nums)} PRs in generated notes range. Fetching metadata...")
    all_prs = fetch_merged_prs(repo)

    # Build detailed PR context
    prs_context = []
    contributors = set()

    for num in unique_pr_nums:
        pr = all_prs.get(num)
        if not pr:
            # Fallback to single fetch
            single_proc = run_cmd(
                ["gh", "pr", "view", str(num), "--repo", repo, "--json", "number,title,author,labels,body,url"],
                check=False,
            )
            if single_proc.returncode == 0:
                try:
                    pr = json.loads(single_proc.stdout)
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse PR #{num} JSON: {e}", file=sys.stderr)

        if pr:
            author_login = pr.get("author", {}).get("login", "unknown")
            if not pr.get("author", {}).get("is_bot", False) and author_login != "app/dependabot":
                contributors.add(f"@{author_login}")

            labels = [lbl.get("name", "") for lbl in pr.get("labels", []) if lbl.get("name")]
            rel_note = extract_release_note(pr.get("body"))
            summary = extract_pr_summary(pr.get("body"))

            prs_context.append({
                "number": pr["number"],
                "title": pr["title"],
                "author": f"@{author_login}",
                "url": pr.get("url", f"https://github.com/{repo}/pull/{num}"),
                "labels": labels,
                "release_note": rel_note,
                "summary": summary,
            })
        else:
            prs_context.append({
                "number": num,
                "title": f"PR #{num}",
                "author": "@unknown",
                "url": f"https://github.com/{repo}/pull/{num}",
                "labels": [],
                "release_note": None,
                "summary": None,
            })

    # Read base template and CHANGELOG.md Unreleased section if available
    repo_root = Path(__file__).resolve().parent.parent.parent
    template_path = Path(__file__).parent / "release-template.md"
    template_str = template_path.read_text(encoding="utf-8") if template_path.exists() else ""
    oh_ver = detect_openhands_layer_version(repo_root)
    py_ver = pep440_version(tag)
    _, pypi_status = detect_pypi_publish_status(repo, skip_pypi=args.skip_pypi)
    server_deps_md = build_server_dependencies_table(repo_root, oh_ver)
    changelog_unreleased = extract_changelog_unreleased(repo_root)

    # Assemble structured context output
    output_lines = [
        f"# Release Context for {tag}",
        "",
        f"- **Repository**: `{repo}`",
        f"- **Target Tag**: `{tag}`",
        f"- **PEP 440 Python Version (`$VERSION`)**: `{py_ver}`",
        f"- **PyPI Publishing (`vars.PYPI_PUBLISH_ENABLED`)**: `{pypi_status}`",
        f"- **Target Commitish**: `{args.target_commitish or 'default branch (HEAD)'}`",
        f"- **Previous Tag**: `{args.previous_tag or 'auto-detected by GitHub'}`",
        "",
        "## Server & Runtime Dependencies (`$SERVER_DEPENDENCIES`)",
        "```markdown",
        server_deps_md,
        "```",
        "",
        "## CHANGELOG.md Unreleased Section",
        "```markdown",
        changelog_unreleased or "(No entries under ## Unreleased in CHANGELOG.md)",
        "```",
        "",
        "## GitHub Generated Notes",
        "```markdown",
        gh_notes.strip(),
        "```",
        "",
        "## Extracted Pull Request Details",
        "",
    ]

    for p in prs_context:
        label_str = f" `[{', '.join(p['labels'])}]`" if p["labels"] else ""
        output_lines.append(f"### PR #{p['number']}: {p['title']}{label_str}")
        output_lines.append(f"- Author: {p['author']} ({p['url']})")
        if p["release_note"]:
            output_lines.append(f"- **Release Note**: {p['release_note']}")
        if p["summary"]:
            output_lines.append(f"- **Summary**: {p['summary']}")
        output_lines.append("")

    sorted_contributors = sorted(contributors, key=lambda s: s.lower())
    output_lines.extend([
        "## Contributors",
        "",
        ", ".join(sorted_contributors) if sorted_contributors else "None",
        "",
        "## Release Notes Template Reference",
        "Use the following template structure (`hack/release/release-template.md`) to synthesize the final release notes:",
        "```markdown",
        template_str.strip(),
        "```",
    ])

    out_content = "\n".join(output_lines) + "\n"

    out_file = Path(args.output)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(out_content, encoding="utf-8")
    print(f"Successfully generated release context: {out_file.resolve()}")


def handle_draft(args: argparse.Namespace) -> None:
    """Verifies notes, pushes tag, and creates draft GitHub release."""
    remote = args.remote
    repo = detect_repo(args.repo, remote=remote)
    tag = args.tag
    if not SEMVER_REGEX.match(tag):
        print(
            f"Error: Tag '{tag}' is not a valid PEP 440-compatible semver tag "
            "(expected format vX.Y.Z or vX.Y.Z-{alpha,beta,rc,dev}N)",
            file=sys.stderr,
        )
        sys.exit(1)

    notes_path = Path(args.notes_file)
    if not notes_path.exists():
        print(f"Error: Release notes file '{notes_path}' does not exist", file=sys.stderr)
        sys.exit(1)

    notes_content = notes_path.read_text(encoding="utf-8").strip()
    if not notes_content:
        print(f"Error: Release notes file '{notes_path}' is empty", file=sys.stderr)
        sys.exit(1)

    commit = args.target_commitish or "HEAD"

    # Verify working tree has no uncommitted or untracked changes (gitignored scratch files are ignored).
    status_proc = run_cmd(["git", "status", "--porcelain"], check=False)
    if status_proc.returncode != 0:
        print(f"Error checking git working tree status:\n{status_proc.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    if status_proc.stdout.strip():
        print(
            "Error: Working tree has uncommitted or untracked changes. "
            "Commit, stash, or clean the checkout before cutting a release:",
            file=sys.stderr,
        )
        print(status_proc.stdout.rstrip(), file=sys.stderr)
        sys.exit(1)

    # Resolve target commitish and verify it is an ancestor of <remote>/main.
    rev_proc = run_cmd(["git", "rev-parse", "--verify", f"{commit}^{{commit}}"], check=False)
    if rev_proc.returncode != 0:
        print(f"Error: Target commitish '{commit}' could not be resolved:\n{rev_proc.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    commit_sha = rev_proc.stdout.strip()

    remote_main_ref = f"refs/remotes/{remote}/main"
    remote_main_check = run_cmd(["git", "rev-parse", "-q", "--verify", f"{remote_main_ref}^{{commit}}"], check=False)
    if remote_main_check.returncode != 0:
        print(
            f"Error: Remote ref '{remote}/main' not found locally; run 'git fetch {remote} main' first.",
            file=sys.stderr,
        )
        sys.exit(1)

    anc_check = run_cmd(["git", "merge-base", "--is-ancestor", commit_sha, remote_main_ref], check=False)
    if anc_check.returncode != 0:
        print(
            f"Error: Target commit {commit_sha[:12]} ({commit}) is not on '{remote}/main'. "
            f"Releases must be cut from commits merged to '{remote}/main' (run 'git fetch {remote} main' if stale).",
            file=sys.stderr,
        )
        sys.exit(1)

    # If a local tag already exists, verify it points at the same commit rather than silently pushing a stale tag.
    tag_check = run_cmd(["git", "rev-parse", "-q", "--verify", f"refs/tags/{tag}^{{commit}}"], check=False)
    tag_exists = tag_check.returncode == 0
    if tag_exists:
        existing_tag_sha = tag_check.stdout.strip()
        if existing_tag_sha != commit_sha:
            print(
                f"Error: Local tag '{tag}' already exists at {existing_tag_sha[:12]}, "
                f"which does not match target commit {commit_sha[:12]} ({commit}).",
                file=sys.stderr,
            )
            sys.exit(1)

    # Check if a published (non-draft) release already exists for this tag
    existing_rel = fetch_release_by_tag(repo, tag)
    if existing_rel and not existing_rel.get("isDraft", False):
        print(
            f"Error: Tag '{tag}' is already an officiated (published) release on {repo} ({existing_rel.get('url')}).",
            file=sys.stderr,
        )
        print("Cannot overwrite an existing published release.", file=sys.stderr)
        sys.exit(1)

    pypi_enabled, pypi_status = detect_pypi_publish_status(repo, skip_pypi=args.skip_pypi)

    print(f"=== Creating / Updating Draft Release {tag} ===")
    print(f"- Repository: {repo}")
    print(f"- Git Remote: {remote}")
    print(f"- Target Commitish: {commit} ({commit_sha[:12]})")
    print(f"- Notes File: {notes_path} ({len(notes_content.splitlines())} lines)")
    print(f"- PyPI Publishing: {pypi_status}")

    if args.dry_run:
        print("\n[DRY RUN] Would execute the following steps:")
        print(f"1. git tag -a {tag} {commit_sha} -m \"Release {tag}\" (if not already tagged)")
        print(f"2. git push {remote} refs/tags/{tag}")
        if existing_rel is not None:
            print(
                f"3. gh release edit {tag} --repo {repo} --draft --title \"{tag}\" "
                "--notes-file <merged-notes> (preserving any ## Images block)"
            )
        else:
            print(
                f"3. gh release create {tag} --repo {repo} --draft --verify-tag "
                f"--title \"{tag}\" --notes-file {notes_path}"
            )
        return

    # Create annotated tag if not already present locally
    if not tag_exists:
        print(f"Creating local annotated tag '{tag}' at commit '{commit_sha[:12]}'...")
        run_cmd(["git", "tag", "-a", tag, commit_sha, "-m", f"Release {tag}"])
    else:
        print(f"Local tag '{tag}' already exists at {commit_sha[:12]}.")

    # Push tag to remote
    print(f"Pushing tag '{tag}' to remote '{remote}'...")
    push_proc = run_cmd(["git", "push", remote, f"refs/tags/{tag}"], check=False)
    if push_proc.returncode != 0:
        print(f"Error pushing tag to remote:\n{push_proc.stderr}", file=sys.stderr)
        sys.exit(1)

    # Check if a draft release exists for this tag (gh release view finds drafts, unlike REST /releases/tags/{tag})
    existing_rel = fetch_release_by_tag(repo, tag)
    if existing_rel is not None:
        print(f"Updating existing draft release notes for '{tag}' (preserving any CI '## Images' block)...")
        existing_body = existing_rel.get("body") or ""
        merged_notes = preserve_images_section(notes_content, existing_body)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as tmp:
            tmp.write(merged_notes)
            tmp_path = Path(tmp.name)
        try:
            edit_cmd = [
                "gh", "release", "edit", tag,
                "--repo", repo,
                "--draft",
                "--title", tag,
                "--notes-file", str(tmp_path),
            ]
            edit_proc = run_cmd(edit_cmd)
        finally:
            tmp_path.unlink(missing_ok=True)
        refreshed = fetch_release_by_tag(repo, tag)
        rel_url = (refreshed or existing_rel).get("url") or edit_proc.stdout.strip() or f"https://github.com/{repo}/releases"
    else:
        print(f"Creating new draft release '{tag}' on {repo}...")
        create_cmd = [
            "gh", "release", "create", tag,
            "--repo", repo,
            "--draft",
            "--verify-tag",
            "--title", tag,
            "--notes-file", str(notes_path.resolve()),
        ]
        create_proc = run_cmd(create_cmd)
        refreshed = fetch_release_by_tag(repo, tag)
        rel_url = (refreshed or {}).get("url") or create_proc.stdout.strip() or f"https://github.com/{repo}/releases"

    print("\n✅ Draft release successfully created/updated!")
    print(f"   Draft URL: {rel_url}")
    print("\nNext Automated CI Actions (kicked off by tag push):")
    print("  1. GitHub Actions workflow .github/workflows/release.yaml is building:")
    print("     - Multi-arch container images: ate-env-api, ate-env-guest")
    print("     - Signing images with Cosign and pushing to GHCR")
    print("     - Cross-compiling ate-env CLI binaries for Linux and macOS (amd64 / arm64)")
    print("     - Generating SHA256SUMS_${TAG}.txt checksums and Cosign signatures")
    print("     - Uploading CLI assets and image verification tables to the draft release")
    print("\nNext Human Action:")
    print("  2. Review the draft release in GitHub UI and click 'Publish release'.")
    if pypi_enabled:
        print("     Publishing the release automatically triggers PyPI publication via")
        print("     OIDC Trusted Publishing for ate-env-rl (clients/python).")
    else:
        print("     PyPI publishing is currently disabled/skipped (vars.PYPI_PUBLISH_ENABLED != 'true'),")
        print("     so publishing the release will officiate the GitHub Release, CLI binaries, and GHCR")
        print("     images cleanly while skipping the optional publish-pypi job.")


def handle_pep440(args: argparse.Namespace) -> None:
    """Validates a release Git tag and prints its PEP 440 version string."""
    tag = args.tag
    if not SEMVER_REGEX.match(tag):
        print(
            f"Error: Tag '{tag}' is not a valid PEP 440-compatible semver tag "
            "(expected format vX.Y.Z or vX.Y.Z-{alpha,beta,rc,dev}N)",
            file=sys.stderr,
        )
        sys.exit(1)
    print(pep440_version(tag))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Release automation tool for Agent Substrate Environments (sandboxenv-rl)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: context
    ctx_parser = subparsers.add_parser(
        "context",
        help="Gathers PR metadata, GitHub release notes, and contributors for drafting notes",
    )
    ctx_parser.add_argument("--tag", required=True, help="Target release tag (e.g. v0.1.0)")
    ctx_parser.add_argument("--previous-tag", help="Previous release tag to diff against")
    ctx_parser.add_argument("--target-commitish", help="Commitish value to tag (branch or SHA, defaults to HEAD)")
    ctx_parser.add_argument("--repo", help="GitHub repo owner/name (e.g. ai-on-gke/sandboxenv-rl)")
    ctx_parser.add_argument(
        "--skip-pypi",
        action="store_true",
        help="Mark PyPI publishing as skipped for this release regardless of repository variables",
    )
    ctx_parser.add_argument(
        "--output",
        default="hack/release/release-context.md",
        help="Path to write the gathered context markdown (default: hack/release/release-context.md)",
    )

    # Subcommand: draft
    draft_parser = subparsers.add_parser(
        "draft",
        help="Verifies release notes, pushes git tag, and creates draft GitHub release",
    )
    draft_parser.add_argument("--tag", required=True, help="Target release tag (e.g. v0.1.0)")
    draft_parser.add_argument(
        "--notes-file",
        required=True,
        help="Path to finalized release notes markdown file",
    )
    draft_parser.add_argument("--target-commitish", help="Target commitish to tag (defaults to HEAD)")
    draft_parser.add_argument("--remote", default="origin", help="Git remote to push tag to (default: origin)")
    draft_parser.add_argument("--repo", help="GitHub repo owner/name (e.g. ai-on-gke/sandboxenv-rl)")
    draft_parser.add_argument(
        "--skip-pypi",
        action="store_true",
        help="Mark PyPI publishing as skipped for this release regardless of repository variables",
    )
    draft_parser.add_argument("--dry-run", action="store_true", help="Print actions without modifying git or GitHub")

    # Subcommand: pep440
    pep440_parser = subparsers.add_parser(
        "pep440",
        help="Validates a release Git tag and prints its PEP 440 version string",
    )
    pep440_parser.add_argument("--tag", required=True, help="Release Git tag (e.g. v0.1.0 or v0.1.0-rc1)")

    args = parser.parse_args()

    try:
        if args.command == "context":
            handle_context(args)
        elif args.command == "draft":
            handle_draft(args)
        elif args.command == "pep440":
            handle_pep440(args)
    except subprocess.CalledProcessError as e:
        cmd_str = " ".join(str(part) for part in e.cmd) if isinstance(e.cmd, list) else str(e.cmd)
        print(f"Error: Command failed (exit {e.returncode}): {cmd_str}", file=sys.stderr)
        if e.stderr:
            print(e.stderr.rstrip(), file=sys.stderr)
        elif e.stdout:
            print(e.stdout.rstrip(), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

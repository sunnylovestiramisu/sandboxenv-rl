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

"""End-to-end integration test pipeline for sandboxenv-rl.

What it does:
  1. Validates Docker daemon access and locates/clones agent-substrate/substrate.
  2. Builds kubectl-ate and ate-env CLI binaries and places them in bin/.
  3. Provisions target cluster (Kind by default with local registry, or reuses existing).
  4. Deploys Agent Substrate control plane components (if provisioning fresh cluster).
  5. Compiles container images (ateworker-gvisor, ate-env-api, ate-env-guest) using ko to target registry.
  6. Applies Kubernetes manifests (atespace, access policy, caller auth Secret, WorkerPool, and ate-env-api with metrics).
  7. Registers the ActorTemplate (snapshots under the cluster snapshot location) with an environment marker, and waits for its golden snapshot.
  8. Forwards a free local port and runs portable smoke test, Python SDK full-stack tests, Go SDK full-stack tests, NeMo Gym provider live test, swebench-loader Go SDK smoke test, and Harbor provider live trial test.

When it runs:
  - CI: .github/workflows/ci.yaml in job `e2e-kind` on PR and push to main.
  - Wrapped by `make ci-e2e`.
  - Nix app: `nix run .#ci-e2e` (or `nix develop -c ci-e2e`).

How to run:
  - `make ci-e2e` (preferred; auto-selects Nix or Docker)
  - `nix run .#ci-e2e` / `nix develop -c ci-e2e`
  - Direct: `python3 hack/ci/e2e.py`
  Examples:
    make ci-e2e
    KEEP_CLUSTER=1 make ci-e2e
    REUSE_CLUSTER=sbx-e2e-abc123 make ci-e2e
    LOCAL_PORT=18888 REUSE_CLUSTER=sbx-e2e-abc123 make ci-e2e

Prerequisites:
  - Docker daemon running and accessible (socket mounted if inside container).
  - Tools on PATH: go, ko, kind, kubectl, pytest (all provided in nix develop or Dockerfile.ci).
  - Harbor runner: nix (for .#python-env-harbor), HARBOR_PYTEST_BIN, or harbor on PATH.

Environment variables:
  REUSE_CLUSTER                   (unset)             Existing cluster name to run against; never created or deleted
  KEEP_CLUSTER                    0                   Set to 1 to preserve cluster after run completes
  NAMESPACE                       ate-env             Kubernetes namespace for ate-env deployment
  LOCAL_PORT                      (free port)         Local port on host for ate-env-api port-forward (free port by default)
  TEMPLATE_NAME                   default-template    Name of ActorTemplate to register and test
  ATE_ENV_ROOTFS_IMAGE            (target default)    Rootfs image for create-from-image E2E and swebench-loader; empty skips them
  ATE_INGRESS_ALLOWED_SPIFFE_IDS  (auto)              Allowed SPIFFE IDs for router TLS ingress
  PYTEST_BIN                      (auto)              Path to pytest binary or python test runner
  HARBOR_PYTEST_BIN               (auto)              Path to pytest binary with harbor installed (defaults to .#python-env-harbor or host PATH)

Outputs / exit codes:
  - Provisions test cluster and Docker containers; publishes images to target registry.
  - Builds binaries into bin/ and generates golden snapshot.
  - Exits with 0 on pipeline success; non-zero if any provisioning, build, or test step fails.
  - Creates a Kind cluster named sbx-e2e-<8 hex> (random, not configurable; KIND_CLUSTER_NAME is
    ignored) and deletes it on completion or error unless KEEP_CLUSTER=1.

Troubleshooting:
  - See docs/chores/troubleshooting.md for common failure modes (inotify limits,
    GoldenActorCrashed, busy ports, and Substrate module resolution).

See also:
  hack/ci/targets.py, hack/ci/test_smoke.py, hack/ci/ensure-substrate.sh,
  clients/python/tests/e2e/test_full_stack.py, clients/go/e2e_test.go,
  integrations/nemo-gym/tests/test_e2e.py, integrations/harbor/tests/test_e2e.py,
  cmd/benchmarking/swebench-loader, Makefile, nix/ci.nix.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import json
import os
import re
import shutil
import socket
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib
from targets import KindTarget

TEMPLATE_MARKER = "injected"


def locate_substrate(root: Path) -> Path:
    cmd = ["ensure-substrate"] if shutil.which("ensure-substrate") else [str(root / "hack/ci/ensure-substrate.sh")]
    return Path(lib.run(*cmd).stdout.strip()).resolve()


def build_cli_tools(root: Path, sub_dir: Path) -> tuple[Path, Path]:
    lib.step("Building CLI tools (kubectl-ate, ate-env)...")
    ate_ctl = root / "bin/kubectl-ate"
    ate_env = root / "bin/ate-env"
    for d, out, pkg in [(sub_dir, ate_ctl, "./cmd/kubectl-ate"), (root, ate_env, "./cmd/ate-env")]:
        out.parent.mkdir(parents=True, exist_ok=True)
        lib.run("go", "-C", str(d), "build", "-o", str(out), pkg, capture=False)

    os.environ["PATH"] = f"{root / 'bin'}:{sub_dir / 'bin'}:{os.environ.get('PATH', '')}"
    return ate_ctl, ate_env


def build_images(root: Path, sub_dir: Path, target: KindTarget) -> dict[str, str]:
    lib.step("Building container images using ko...")
    imgs: dict[str, str] = {}
    for key, c_dir, pkg in [
        ("worker", sub_dir, "./cmd/ateworker-gvisor"),
        ("api", root, "./cmd/ate-env-api"),
        ("guest", root, "./cmd/ate-env-guest"),
    ]:
        ko_env = dict(
            os.environ,
            KO_DOCKER_REPO=f"{target.registry}/{pkg.split('/')[-1]}",
            KO_DEFAULTPLATFORMS=target.platform,
        )
        out = lib.run("ko", "build", "--bare", pkg, cwd=str(c_dir), env=ko_env).stdout.strip().splitlines()
        imgs[key] = out[-1].strip()
        lib.step(f"  {key}-image: {imgs[key]}")
    return imgs


def deploy_manifests(
    target: KindTarget,
    ate_ctl: Path,
    ate_env: Path,
    ns: str,
    tmpl: str,
    imgs: dict[str, str],
) -> str:
    ctl_str, env_str = str(ate_ctl), str(ate_env)
    lib.step("Ensuring atespace and access-policy in Substrate...")
    lib.run(ctl_str, f"--context={target.context}", "create", "atespace", ns, check=False)
    pipe_res = lib.pipe(
        [env_str, "manifest", "access-policy", "--namespace", ns],
        [ctl_str, f"--context={target.context}", "apply", "access-policy", "-a", ns, "-f", "-"],
        check=False,
        capture=True,
    )
    if pipe_res.returncode != 0:
        res = lib.pipe(
            [env_str, "manifest", "access-policy", "--namespace", ns],
            [ctl_str, f"--context={target.context}", "create", "access-policy", "-a", ns, "-f", "-"],
            check=False,
            capture=True,
        )
        if res.returncode != 0 and "AlreadyExists" not in (res.stderr or ""):
            raise RuntimeError(f"Failed to create access-policy: {res.stderr}")

    lib.step(f"Ensuring ate-env-api-token secret in namespace {ns}...")
    lib.pipe(
        ["kubectl", "create", "namespace", ns, "--dry-run=client", "-o", "yaml"],
        ["kubectl", f"--context={target.context}", "apply", "-f", "-"],
        check=True,
    )
    tok_res = lib.run(
        "kubectl", f"--context={target.context}", "-n", ns, "get", "secret",
        "ate-env-api-token", "-o", "jsonpath={.data.token}", check=False, capture=True,
    )
    raw_tok = tok_res.stdout.strip() if tok_res.returncode == 0 else ""
    if not raw_tok:
        lib.pipe(
            [env_str, "manifest", "auth-secret", "--namespace", ns],
            ["kubectl", f"--context={target.context}", "apply", "-f", "-"],
            check=True,
        )
        tok_res = lib.run(
            "kubectl", f"--context={target.context}", "-n", ns, "get", "secret",
            "ate-env-api-token", "-o", "jsonpath={.data.token}", check=True, capture=True,
        )
        raw_tok = tok_res.stdout.strip()
    if not raw_tok:
        raise RuntimeError(f"Secret ate-env-api-token in namespace {ns} has no token")
    auth_token = base64.b64decode(raw_tok).decode().strip()
    if not auth_token:
        raise RuntimeError(f"Decoded token from secret ate-env-api-token in namespace {ns} is empty")

    lib.step(f"Applying Kubernetes manifests (namespace/{ns}, WorkerPool, Deployment/ate-env-api)...")
    lib.pipe(
        [
            env_str,
            "manifest",
            "--namespace",
            ns,
            "--template",
            tmpl,
            "--api-image",
            imgs["api"],
            "--worker-image",
            imgs["worker"],
            "--replicas",
            "2",
            "--metrics-port",
            "9090",
            "--port-proxy-atespaces",
            ns,
        ],
        ["kubectl", f"--context={target.context}", "apply", "-f", "-"],
        check=True,
    )
    for target_deploy in ["deploy/ate-env-api", f"deploy/{tmpl}-workerpool"]:
        lib.run(
            "kubectl",
            f"--context={target.context}",
            "-n",
            ns,
            "rollout",
            "status",
            target_deploy,
            "--timeout=180s",
            capture=False,
        )

    lib.step(f"Cleaning stale templates and registering {tmpl} ActorTemplate in Substrate...")
    list_res = lib.run(
        ctl_str, f"--context={target.context}", "get", "actor-template", "-a", ns, "-o", "json",
        check=True, capture=True,
    )
    templates = json.loads(list_res.stdout or "{}").get("actorTemplates", [])
    derived_re = re.compile(rf"{re.escape(tmpl)}-[0-9a-f]{{12}}")
    for item in templates:
        name = item.get("metadata", {}).get("name")
        if name and (name == tmpl or derived_re.fullmatch(name)):
            lib.run(
                ctl_str, f"--context={target.context}", "delete", "actor-template",
                "-a", ns, name, check=True,
            )

    lib.pipe(
        [
            env_str,
            "manifest",
            "template",
            "--atespace",
            ns,
            "--template",
            tmpl,
            "--workerpool",
            f"{tmpl}-workerpool",
            "--guest-image",
            imgs["guest"],
            "--env",
            f"E2E_TEMPLATE_MARKER={TEMPLATE_MARKER}",
        ],
        [ctl_str, f"--context={target.context}", "create", "actor-template", "-f", "-"],
        check=True,
    )

    lib.step(f"Waiting for {tmpl} golden snapshot...")

    def check_golden():
        r = lib.run(
            ctl_str,
            f"--context={target.context}",
            "get",
            "actor-template",
            "-a",
            ns,
            tmpl,
            "-o",
            "json",
            check=False,
        )
        if r.returncode == 0 and r.stdout.strip():
            st = json.loads(r.stdout).get("status", {}).get("goldenSnapshotStatus", {})
            if err := st.get("errorMessage"):
                raise RuntimeError(f"Golden snapshot failed: {err}")
            return st.get("goldenTag", {}).get("name")

    tag = lib.wait_until(check_golden, timeout=180.0, interval=2.0, what=f"{tmpl} golden snapshot")
    lib.step(f"Golden snapshot ready: {tag}")
    return auth_token


def pick_port() -> int:
    if raw := os.environ.get("LOCAL_PORT"):
        port = int(raw)
        if lib.port_open("127.0.0.1", port):
            raise RuntimeError(
                f"LOCAL_PORT={port} is already in use on 127.0.0.1. "
                "Stop the existing listener or unset LOCAL_PORT to let e2e.py pick a free port."
            )
        return port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _candidate_bin_dirs(binary: Path) -> list[Path]:
    return list(dict.fromkeys((binary.absolute().parent, binary.resolve().parent)))


def resolve_harbor_runner(root: Path) -> tuple[list[str], str]:
    if hb_pytest := os.environ.get("HARBOR_PYTEST_BIN"):
        cand = Path(hb_pytest).expanduser()
        resolved = cand if cand.is_file() else Path(shutil.which(hb_pytest) or "")
        if not resolved.is_file():
            raise RuntimeError(f"HARBOR_PYTEST_BIN={hb_pytest} was not found")
        candidate_dirs = _candidate_bin_dirs(resolved)
        for parent_dir in candidate_dirs:
            if (parent_dir / "harbor").is_file():
                return [str(resolved.absolute())], str(parent_dir)
        searched = ", ".join(str(d) for d in candidate_dirs)
        raise RuntimeError(f"harbor binary not found alongside HARBOR_PYTEST_BIN in {searched}")
    if shutil.which("nix"):
        out_paths = (
            lib.run(
                "nix", "build", "--no-link", "--print-out-paths", ".#python-env-harbor",
                cwd=str(root),
            ).stdout.strip().splitlines()
        )
        harbor_env = Path(out_paths[-1])
        return [str(harbor_env / "bin/pytest")], str(harbor_env / "bin")
    if harbor_bin := shutil.which("harbor"):
        candidate_dirs = _candidate_bin_dirs(Path(harbor_bin))
        for harbor_dir in candidate_dirs:
            if (harbor_dir / "pytest").is_file():
                return [str(harbor_dir / "pytest")], str(harbor_dir)
        for harbor_dir in candidate_dirs:
            if (harbor_dir / "python").is_file():
                return [str(harbor_dir / "python"), "-m", "pytest"], str(harbor_dir)
        searched = ", ".join(str(d) for d in candidate_dirs)
        raise RuntimeError(f"Neither pytest nor python found alongside harbor in {searched}")
    raise RuntimeError(
        "Harbor live E2E requires Nix (.#python-env-harbor), HARBOR_PYTEST_BIN, or harbor on PATH"
    )


def run_tests(
    root: Path,
    target: KindTarget,
    ate_env: Path,
    ns: str,
    tmpl: str,
    port: int,
    auth_token: str,
    harbor_py: list[str],
    harbor_bin_dir: str,
) -> None:
    lib.step(f"Running test suites under port-forward on 127.0.0.1:{port}...")
    with lib.port_forward(target.context, ns, "svc/ate-env-api", port):
        test_env = dict(
            os.environ,
            KUBECTL_CONTEXT=target.context,
            NAMESPACE=ns,
            LOCAL_PORT=str(port),
            ATE_ENV_API_TARGET=f"127.0.0.1:{port}",
            SUBSTRATE_ENV_API=f"127.0.0.1:{port}",
            ATE_ENV_TEMPLATE=tmpl,
            ATE_ENV_BIN=str(ate_env),
            ATE_ENV_ROOTFS_IMAGE=target.rootfs_image or "",
            SUBSTRATE_ENV_AUTH_TOKEN=auth_token,
            ATE_ENV_EXPECT_METRICS="1",
            ATE_ENV_EXPECTED_MARKER=TEMPLATE_MARKER,
            ATE_ENV_EXPECT_PORT_PROXY_ALLOWLIST="1",
            # Editable .pth files in nix/python.nix:65 resolve workspace packages from
            # $REPO_ROOT at run time, and only pureEnvSetup exports it.
            REPO_ROOT=str(root),
        )
        py = [
            os.environ.get("PYTEST_BIN") or shutil.which("pytest") or sys.executable,
            *([] if os.environ.get("PYTEST_BIN") or shutil.which("pytest") else ["-m", "pytest"]),
        ]
        lib.step("Running portable smoke test (9 subtests)...")
        lib.run(*py, str(root / "hack/ci/test_smoke.py"), "-v", env=test_env, capture=False)
        lib.step("Running live Python SDK integration tests...")
        lib.run(*py, str(root / "clients/python/tests/e2e/test_full_stack.py"), "-v", env=test_env, capture=False)
        lib.step("Running live Go SDK integration tests...")
        lib.run(
            "go",
            "-C",
            str(root),
            "test",
            "-v",
            "-race",
            "-count=1",
            "-timeout",
            "25m",
            "./clients/go",
            "-run",
            "^TestE2EFullStack$",
            env={**test_env, "ATE_ENV_GUEST_TARGET": ""},
            capture=False,
        )

        lib.step("Running NeMo Gym provider live E2E tests...")
        lib.run(
            *py,
            str(root / "integrations/nemo-gym/tests/test_e2e.py"),
            "-k",
            "test_full_lifecycle or test_pause_policy_parks_and_the_next_command_wakes",
            "-v",
            env={
                **test_env,
                "SUBSTRATE_E2E_API_URL": f"127.0.0.1:{port}",
                "SUBSTRATE_E2E_TEMPLATE": tmpl,
                "SUBSTRATE_E2E_ATESPACE": ns,
            },
            capture=False,
        )

        if target.rootfs_image:
            lib.step("Running swebench-loader Go SDK live smoke test...")
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                manifest = tmp_path / "manifest.jsonl"
                manifest.write_text(
                    json.dumps(
                        {
                            "instance_id": "smoke-cmd-1",
                            "kind": "command",
                            "command": "echo swebench-live-ok",
                            "expected_stdout": "swebench-live-ok",
                            "image": target.rootfs_image,
                        }
                    )
                    + "\n"
                )
                events_jsonl = tmp_path / "events.jsonl"
                summary_json = tmp_path / "summary.json"
                loader_bin = tmp_path / "swebench-loader"
                lib.run(
                    "go", "-C", str(root), "build", "-o", str(loader_bin),
                    "./cmd/benchmarking/swebench-loader",
                    capture=False,
                )
                lib.run(
                    str(loader_bin),
                    "run",
                    "-dataset",
                    str(manifest),
                    "-endpoint",
                    f"127.0.0.1:{port}",
                    "-atespace",
                    ns,
                    "-base-template",
                    tmpl,
                    "-mode",
                    "eval",
                    "-images",
                    "1",
                    "-tasks-per-image",
                    "1",
                    "-width",
                    "1",
                    "-connections",
                    "1",
                    "-out",
                    str(events_jsonl),
                    env=test_env,
                    capture=False,
                )
                lib.run(
                    str(loader_bin),
                    "report",
                    str(events_jsonl),
                    "-json",
                    str(summary_json),
                    env=test_env,
                    capture=False,
                )
                summary = json.loads(summary_json.read_text())
                if (
                    summary.get("tasks") != 1
                    or summary.get("failed") != 0
                    or summary.get("rewarded") != 1
                ):
                    raise RuntimeError(f"swebench-loader run failed: {summary}")

        lib.step("Running Harbor provider live trial E2E test...")
        harbor_path = f"{harbor_bin_dir}:{test_env.get('PATH', '')}"
        lib.run(
            *harbor_py,
            str(root / "integrations/harbor/tests/test_e2e.py"),
            "-v",
            env={
                **test_env,
                "SUBSTRATE_E2E_API_URL": f"127.0.0.1:{port}",
                "HB_E2E_TEMPLATE": tmpl,
                "HB_E2E_ATESPACE": ns,
                "PATH": harbor_path,
            },
            capture=False,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end integration test pipeline for sandboxenv-rl on Kind.")
    parser.parse_args()

    if lib.run("docker", "info", check=False).returncode != 0:
        sys.exit("ERROR: Docker daemon is not running or accessible.")

    root = Path(__file__).resolve().parent.parent.parent
    sub_dir = locate_substrate(root)

    ns = os.environ.get("NAMESPACE", "ate-env")
    port = pick_port()
    tmpl = os.environ.get("TEMPLATE_NAME", "default-template")

    harbor_future: concurrent.futures.Future[tuple[list[str], str]] | None = None
    executor: concurrent.futures.ThreadPoolExecutor | None = None
    harbor_py: list[str] = []
    harbor_bin_dir = ""

    if not os.environ.get("HARBOR_PYTEST_BIN") and shutil.which("nix"):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        harbor_future = executor.submit(resolve_harbor_runner, root)
    else:
        harbor_py, harbor_bin_dir = resolve_harbor_runner(root)

    target = KindTarget(sub_dir=sub_dir, namespace=ns)

    try:
        target.provision()
        ate_ctl, ate_env = build_cli_tools(root, sub_dir)
        imgs = build_images(root, sub_dir, target)
        auth_token = deploy_manifests(target, ate_ctl, ate_env, ns, tmpl, imgs)
        if harbor_future is not None:
            harbor_py, harbor_bin_dir = harbor_future.result()
        run_tests(root, target, ate_env, ns, tmpl, port, auth_token, harbor_py, harbor_bin_dir)
        lib.step("PASS: E2E Kind pipeline succeeded!")
    finally:
        try:
            if executor is not None:
                executor.shutdown(wait=True)
        finally:
            target.teardown()


if __name__ == "__main__":
    main()

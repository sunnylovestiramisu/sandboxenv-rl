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

"""Kind target environment for sandboxenv-rl E2E tests.

Manages the lifecycle of a local Kind cluster with Agent Substrate.
"""

from __future__ import annotations

import os
import secrets
from pathlib import Path

import lib


def _check_inotify_limit() -> None:
    """Warns if fs.inotify.max_user_instances is low enough to risk Kind failure."""
    path = Path("/proc/sys/fs/inotify/max_user_instances")
    if path.is_file():
        try:
            val = int(path.read_text().strip())
            if val < 1024:
                print(
                    f"WARNING: fs.inotify.max_user_instances is {val} (< 1024). "
                    "Kind clusters may exhaust inotify instances under load. "
                    "Consider raising it: sudo sysctl fs.inotify.max_user_instances=8192"
                )
        except (ValueError, OSError):
            pass


# Digest resolved 2026-10-26, the test needs an image with sh and python3, refresh manually; see docs/chores/check-pins.md
DEFAULT_KIND_ROOTFS_IMAGE = (
    "docker.io/library/python:3.13-slim@sha256:d3449c0493345868b09708d7056dce5edda3a75233ea86008fc80c934d355a10"
)


class KindTarget:
    """Manages the lifecycle of a local Kind cluster with Agent Substrate."""

    context: str
    registry: str
    platform: str
    rootfs_image: str | None

    def __init__(
        self,
        sub_dir: Path,
        namespace: str = "ate-env",
        reuse: str | None = None,
        keep_cluster: bool | None = None,
    ) -> None:
        self.sub_dir = sub_dir
        self.namespace = namespace

        # A cluster this run creates is always named sbx-e2e-<random>: the name is
        # not configurable, so parallel runs never collide and a run can never
        # create over (and thereby delete) an existing cluster. KIND_CLUSTER_NAME
        # from the environment is ignored. The only way to use an existing
        # cluster is REUSE_CLUSTER=<name>, which never creates or deletes it.
        reuse_val = reuse if reuse is not None else os.environ.get("REUSE_CLUSTER")
        self.reuse = bool(reuse_val)
        self.cluster_name = reuse_val or f"sbx-e2e-{secrets.token_hex(4)}"

        if keep_cluster is not None:
            self.keep_cluster = keep_cluster
        else:
            self.keep_cluster = os.environ.get("KEEP_CLUSTER") in ("1", "true", "TRUE")

        # Fixed by Substrate's Kind profile (`ate-setup --kind`); deliberately not
        # read from the environment so a shell set up for GKE (KUBECTL_CONTEXT,
        # KO_DOCKER_REPO) cannot redirect a Kind run. The profile also puts the
        # cluster snapshot location on its bundled object store
        # (gs://ate-snapshots/), so templates name no bucket of their own.
        self.context = f"kind-{self.cluster_name}"
        self.registry = f"localhost:{os.environ.get('KIND_REGISTRY_PORT', '5001')}"
        goarch = lib.run("go", "env", "GOARCH").stdout.strip()
        self.platform = f"linux/{goarch}"

        # Rootfs image used by test_create_from_image.
        # Digest resolved 2026-10-26, the test needs an image with sh and python3, refresh manually; see docs/chores/check-pins.md
        if "ATE_ENV_ROOTFS_IMAGE" in os.environ:
            self.rootfs_image = os.environ["ATE_ENV_ROOTFS_IMAGE"] or None
        else:
            self.rootfs_image = DEFAULT_KIND_ROOTFS_IMAGE

        self.created = False
        self._existing_clusters_before: set[str] = set()

    def provision(self) -> None:
        _check_inotify_limit()

        existing = set(lib.run("kind", "get", "clusters", check=False).stdout.splitlines())
        self._existing_clusters_before = existing

        # Export environment variables for child scripts and tools
        os.environ["KIND_CLUSTER_NAME"] = self.cluster_name
        os.environ["KUBECTL_CONTEXT"] = self.context
        os.environ["KO_DOCKER_REPO"] = self.registry

        if self.reuse:
            if self.cluster_name not in existing:
                raise RuntimeError(
                    f"Cluster '{self.cluster_name}' requested via REUSE_CLUSTER does not exist. "
                    f"Existing clusters: {', '.join(sorted(existing)) if existing else '(none)'}"
                )
            lib.step(f"Reusing existing Kind cluster '{self.cluster_name}' (skipping create and Substrate install)...")
            return

        if self.cluster_name in existing:
            # Substrate's create-kind-cluster.sh deletes a same-named cluster
            # before creating it, so never "create" over an existing one.
            raise RuntimeError(
                f"Kind cluster '{self.cluster_name}' already exists, and creating it again would "
                f"delete it. Run against it with REUSE_CLUSTER={self.cluster_name}, or rerun "
                "for a new random name."
            )
        self.created = True

        lib.step(f"Creating Kind cluster '{self.cluster_name}'...")
        lib.run(str(self.sub_dir / "hack/create-kind-cluster.sh"), cwd=str(self.sub_dir), capture=False)

        lib.step("Installing Agent Substrate on Kind cluster...")
        spiffe = os.environ.get(
            "ATE_INGRESS_ALLOWED_SPIFFE_IDS",
            f"spiffe://cluster.local/ns/{self.namespace}/sa/ate-env-api,spiffe://cluster.local/ns/ate-system/sa/default",
        )
        # Substrate removed hack/install-ate-kind.sh (substrate#3354); --kind
        # selects the same profile: the kind-$KIND_CLUSTER_NAME context, the
        # local registry and the bundled object store.
        lib.run(
            "go",
            "run",
            "./cmd/ate-setup",
            "--kind",
            "deploy",
            "ate-system",
            '--credential-provider={"name":"k8s.io"}',
            "--rollout-timeout=300s",
            cwd=str(self.sub_dir),
            env=dict(os.environ, ATE_INGRESS_ALLOWED_SPIFFE_IDS=spiffe),
            capture=False,
        )

    def teardown(self) -> None:
        if not self.created:
            return

        if self.keep_cluster:
            print(f"KEEP_CLUSTER set: leaving Kind cluster '{self.cluster_name}' running.")
            print(f"  To reuse:  REUSE_CLUSTER={self.cluster_name} make ci-e2e")
            print(f"  To delete: kind delete cluster --name {self.cluster_name}")
            return

        lib.step(f"Deleting Kind cluster '{self.cluster_name}'...")
        lib.run("kind", "delete", "cluster", "--name", self.cluster_name, check=False)

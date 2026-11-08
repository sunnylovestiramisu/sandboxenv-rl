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

"""Shared by the examples: a workspace attached to an existing environment.

Environment: ``API`` (ate-env-api address, default 127.0.0.1:7777),
``API_TOKEN`` (ate-env-api's bearer token, when it runs with
``-auth-token-file``), ``ATESPACE`` (default ate-env), ``PORT`` (agent-server
port, default 8000), ``SESSION_KEY`` (the template's
``OH_SESSION_API_KEYS_0``), ``WORKDIR`` (where
commands run and what relative paths resolve against; default ``/``). The
image's WORKDIR does not apply: the SDK runs each command in ``WORKDIR``,
thus set ``WORKDIR=/testbed`` on a SWE-bench image.
"""

from __future__ import annotations

import os

from ate_env_openhands import AteEnvWorkspace


def attach(env_id: str, *, working_dir: str | None = None, api_key: str | None = None) -> AteEnvWorkspace:
    """An ``AteEnvWorkspace`` on an existing environment; it stays on exit."""
    return AteEnvWorkspace(
        env_id=env_id,
        api_url=os.environ.get("API", "127.0.0.1:7777"),
        api_token=os.environ.get("API_TOKEN") or None,
        atespace=os.environ.get("ATESPACE", "ate-env"),
        server_port=int(os.environ.get("PORT", "8000")),
        api_key=api_key or os.environ["SESSION_KEY"],
        # "/" and not AteEnvWorkspace's default "/workspace": these examples
        # attach to environments that demo.sh or another tool created, and "/"
        # is where the agent-server runs, thus a command without a cwd sees the
        # same directory here as through the agent-server's own API.
        working_dir=working_dir or os.environ.get("WORKDIR") or "/",
    )

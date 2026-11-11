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

"""Backwards-compatible re-export of ``ate_env_rl.adapters.r2egym``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ate_env_rl.adapters import r2egym as _r2egym
from ate_env_rl.adapters.r2egym import (
    fresh_eval_env,
    make_fleet_repo_env,
    r2egym_command_files,
)

if TYPE_CHECKING:
    AteEnvRepoEnv: type[Any]
    AteEnvRuntime: type[Any]

# As in ``ate_env_rl.adapters.r2egym``: ``AteEnvRuntime`` and ``AteEnvRepoEnv``
# are module attributes resolved on first access through ``__getattr__`` and
# stay out of ``__all__``, so a star import works without R2E-Gym.
__all__ = [
    "fresh_eval_env",
    "make_fleet_repo_env",
    "r2egym_command_files",
]


def __getattr__(name: str):
    """Forward the attributes resolved on first access, ``AteEnvRuntime`` and
    ``AteEnvRepoEnv``, to ``ate_env_rl.adapters.r2egym``."""
    return getattr(_r2egym, name)

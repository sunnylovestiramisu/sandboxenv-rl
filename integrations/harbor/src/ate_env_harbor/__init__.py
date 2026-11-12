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

"""Harbor environments on ate-env.

Select it with ``harbor run ... -e ate_env_harbor:AteEnvEnvironment`` and pass
settings with ``--ek key=value``; see :class:`AteEnvEnvironment` for the keys.
"""

from .environment import ENV_TYPE, AteEnvEnvironment, parse_worker_selector
from .images import ImageError

__all__ = ["ENV_TYPE", "AteEnvEnvironment", "ImageError", "parse_worker_selector"]

#!/usr/bin/env bash

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

# Prints one `ate-env manifest template` command per resource class the
# tasks under a directory ask for ([environment] cpus and memory_mb, or memory,
# in each task.toml), named the way ate_env_harbor picks them:
# <prefix>c<cpus>-m<memory_mb>. --override-cpus and --override-memory-mb select
# classes this script does not print; register those by hand.
# Pipe the output to a shell to register them.
#
#   register-templates.sh [--prefix harbor-] <tasks-dir> [-- <ate-env manifest template flags>]
#
# The flags after `--` go on every command: at least --guest-image (digest
# pinned), plus --atespace or --workerpool when they differ from the defaults.
set -o errexit -o nounset -o pipefail

PREFIX="harbor-"
if [[ "${1:-}" == "--prefix" ]]; then
  PREFIX="$2"
  shift 2
fi
TASKS_DIR="${1:?usage: $0 [--prefix PREFIX] <tasks-dir> [-- <ate-env manifest template flags>]}"
shift
if [[ "${1:-}" == "--" ]]; then
  shift
fi

# The parsing is the package's (ate_env_harbor.templates.task_classes), which
# reads each [environment] table with Harbor's EnvironmentConfig: memory = "4G" and memory_mb
# name the same class. Run it with the Python that has ate-env-harbor
# installed (PYTHON, default python3).
"${PYTHON:-python3}" -I -c '
import sys

from ate_env_harbor.templates import register_commands

prefix, root, *extra = sys.argv[1:]
for command in register_commands(root, prefix, extra):
    print(command)
' "${PREFIX}" "${TASKS_DIR}" ${@+"$@"}

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

# Runs a Harbor dataset or tasks directory on ate-env: `harbor run` with the
# ate-env provider selected and its settings taken from the environment.
#
#   run-tb2.sh [harbor run flags...]
#   run-tb2.sh warm <tasks-dir> [python -m ate_env_harbor.warm flags...]
#
# Without flags: `-d terminal-bench@2.0 -a oracle -n 10`. Everything given is
# passed to `harbor run` after the provider flags, thus `-p ./tb2 -a terminus-2
# -m <model> -n 50` works as it does with any other --env. `warm` puts the
# dataset's images on every node of the pool before a run (README.md, step 5).
#
# Environment:
#   SUBSTRATE_ENV_API              the port-forwarded ate-env-api (required)
#   SUBSTRATE_ENV_AUTH_TOKEN       the caller token, when the API requires one
#   SUBSTRATE_ENV_HARBOR_REGISTRY  where images built from a Dockerfile go
#   HB_BUILD                       local | cloud-build | none (default: none,
#                                  the dataset is prebuilt; see README.md)
#   HB_TEMPLATE                    the default base template (default-template)
#   HB_ATESPACE                    the atespace (ate-env)
#   HB_PLATFORM                    the build platform (linux/amd64)
#   HB_WORKER_SELECTOR             the pool's labels, key=value pairs separated by
#                                  commas: the run's --ek worker_selector and the
#                                  warm step's --selector (unless --domain or
#                                  --selector is given to warm)
set -o errexit -o nounset -o pipefail

: "${SUBSTRATE_ENV_API:?set SUBSTRATE_ENV_API to the port-forwarded ate-env-api, e.g. 127.0.0.1:7777}"

if [[ "${1:-}" == "warm" ]]; then
  shift
  warm=()
  if [[ -n "${HB_TEMPLATE:-}" ]]; then
    warm+=(--template "${HB_TEMPLATE}")
  fi
  if [[ -n "${HB_ATESPACE:-}" ]]; then
    warm+=(--atespace "${HB_ATESPACE}")
  fi
  # --domain and --selector exclude each other, thus the caller's choice of
  # either wins over HB_WORKER_SELECTOR. The patterns take in the prefixes
  # argparse accepts (--dom, --sel=...); --do is not one, --docker shares it.
  pool_given=""
  for arg in "$@"; do
    case "${arg}" in
      --dom* | --s*) pool_given=1 ;;
    esac
  done
  if [[ -n "${HB_WORKER_SELECTOR:-}" && -z "${pool_given}" ]]; then
    warm+=(--selector "${HB_WORKER_SELECTOR}")
  fi
  exec "${PYTHON:-python3}" -m ate_env_harbor.warm ${warm[@]+"${warm[@]}"} "$@"
fi

provider=(-e ate_env_harbor:AteEnvEnvironment --ek "build=${HB_BUILD:-none}")
if [[ -n "${SUBSTRATE_ENV_HARBOR_REGISTRY:-}" ]]; then
  provider+=(--ek "registry=${SUBSTRATE_ENV_HARBOR_REGISTRY}")
fi
if [[ -n "${HB_TEMPLATE:-}" ]]; then
  provider+=(--ek "template=${HB_TEMPLATE}")
fi
if [[ -n "${HB_ATESPACE:-}" ]]; then
  provider+=(--ek "atespace=${HB_ATESPACE}")
fi
if [[ -n "${HB_PLATFORM:-}" ]]; then
  provider+=(--ek "platform=${HB_PLATFORM}")
fi
if [[ -n "${HB_WORKER_SELECTOR:-}" ]]; then
  provider+=(--ek "worker_selector=${HB_WORKER_SELECTOR}")
fi

if [[ $# -eq 0 ]]; then
  set -- -d terminal-bench@2.0 -a oracle -n 10
fi

exec harbor run "${provider[@]}" "$@"

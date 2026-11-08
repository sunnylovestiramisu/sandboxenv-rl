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

# Harbor with tools as a second layer, end to end: build and push the tools
# layer, register the resource class templates with the layer mounted at
# /opt/harbor-tools, and run a Harbor dataset with the layer's directory first
# on PATH, thus terminus-2 finds tmux and asciinema and installs nothing.
#
#   demo.sh <tasks-dir> [harbor run flags...]
#
# Without flags after <tasks-dir>: `-a oracle -n 10`, as examples/harbor/run-tb2.sh
# does; a model run is `-a terminus-2 -m <model> -n 10`.
#
# Environment:
#   LAYER_REPO                   where the layer goes, e.g. us-central1-docker.pkg.dev/<project>/harbor/tools-layer
#   GUEST_IMAGE                  digest-pinned ate-env-guest image, for the templates
#   SNAPSHOTS_BUCKET             optional per-template snapshot location (needs Substrate's
#                                PerTemplateSnapshotLocation preview gate); empty uses the cluster's
#   SUBSTRATE_ENV_API            the port-forwarded ate-env-api (run-tb2.sh needs it)
#   SUBSTRATE_ENV_AUTH_TOKEN     the caller token, when the API requires one
#   TEMPLATE_FLAGS               more `ate-env manifest template` flags, e.g. --atespace or --workerpool
#   APPLY                        1 registers the templates (kubectl-ate); unset prints the commands
set -o errexit -o nounset -o pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
TASKS="${1:?usage: $0 <tasks-dir> [harbor run flags...]}"
shift

: "${LAYER_REPO:?set LAYER_REPO to the repository the layer is pushed to}"
: "${GUEST_IMAGE:?set GUEST_IMAGE to the digest-pinned ate-env-guest image}"
SNAPSHOTS_BUCKET="${SNAPSHOTS_BUCKET:-}"

MOUNT=/opt/harbor-tools
TAG="${LAYER_REPO}:tmux-3.8"
META="$(mktemp)"
trap 'rm -f "${META}"' EXIT

echo "== building and pushing the layer as ${TAG}"
docker buildx build --platform linux/amd64 --push --provenance=false \
  -t "${TAG}" --metadata-file "${META}" "${HERE}/layer"
DIGEST="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["containerimage.digest"])' "${META}")"
LAYER_REF="${LAYER_REPO}@${DIGEST}"
echo "== layer: ${LAYER_REF}"

echo "== resource class templates for ${TASKS}, with the layer at ${MOUNT}"
# shellcheck disable=SC2086
COMMANDS="$("${ROOT}/integrations/harbor/hack/register-templates.sh" "${TASKS}" -- \
  --guest-image "${GUEST_IMAGE}" ${SNAPSHOTS_BUCKET:+--snapshots-bucket "${SNAPSHOTS_BUCKET}"} \
  --layer "tools=${LAYER_REF}=${MOUNT}" ${TEMPLATE_FLAGS:-})"
printf '%s\n' "${COMMANDS}"
if [[ "${APPLY:-}" == "1" ]]; then
  printf '%s\n' "${COMMANDS}" | bash -euo pipefail
else
  echo "== (set APPLY=1 to register them; continuing with the run)"
fi

echo "== running Harbor with ${MOUNT}/bin first on PATH"
if [[ $# -eq 0 ]]; then
  set -- -a oracle -n 10
fi
exec "${ROOT}/examples/harbor/run-tb2.sh" -p "${TASKS}" --ek "tool_paths=${MOUNT}/bin" "$@"

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

# The OpenHands agent-server attached to unmodified images, end to end.
#
# The agent-server comes from a runtime layer (../../integrations/openhands/layer/Dockerfile) mounted read-only
# at /opt/oh; the guest starts it as a sidecar and holds the actor's readiness
# until it answers /health. Callers outside the cluster reach it through
# ate-env-api's port proxy with three routing headers plus the agent-server's
# own X-Session-API-Key. The script registers a base template when it does not
# exist, creates one environment per image, drives each agent-server from
# outside (health, a command, a file download, a call without the key) and
# deletes the environments, unless KEEP=1. It exits 1 if any agent-server did
# not answer.
#
# Templates are immutable and carry the session key, and the first create from
# each image derives a template (<base>-<12 hex digits of the image digest>)
# that copies the key. By default the script reuses an existing base template
# and its derived templates; it then needs the SESSION_KEY they carry. It
# deletes templates only with RECREATE=1: then it deletes the base template
# and every template derived from it, with their golden snapshots, and
# registers the base again. Use RECREATE=1 only for templates that nobody else
# uses. It deletes the derived templates too, because they copy the session
# key, and a new key makes them stale. A base registered without a glibc base
# image (its own golden actor crashes; see BASE_IMAGE) needs only the base
# registered again: with the same SESSION_KEY, the derived templates stay
# valid, because a template derived from either base is identical. RECREATE=1
# with that key also works; it bakes each golden snapshot again.
#
# The session key and the API token go to curl in a header file (mode 0600,
# removed on exit), not on its command line. The template registration still
# passes the key in an argument of `ate-env manifest template`.
#
# Required:
#   GUEST_IMAGE       digest-pinned ate-env-guest image (with the -sidecar flags)
#   LAYER_IMAGE       digest-pinned layer image built from ../../integrations/openhands/layer/Dockerfile
#   IMAGES            space-separated digest-pinned images to run the
#                     agent-server in; any image with /bin/sh and glibc 2.31
#                     or newer
# Optional:
#   SNAPSHOTS_BUCKET  per-template snapshot location (needs Substrate's
#                     PerTemplateSnapshotLocation preview gate); empty uses
#                     the cluster's
#   BASE_IMAGE        digest-pinned glibc image the base template runs in
#                     (default debian:bookworm-slim, pinned). The base
#                     template bakes its own golden snapshot with the layer
#                     started, thus it needs glibc 2.31 or newer and /bin/sh
#   SESSION_KEY       agent-server API key on the template. Required when the
#                     template exists (it carries its own key) and with KEEP=1;
#                     otherwise random and never printed
#   RECREATE          1 deletes the base template and its derived templates,
#                     with their golden snapshots, and registers the base again
#   API_TOKEN         ate-env-api's bearer token, when it runs with
#                     -auth-token-file: X-Env-Authorization for the port proxy,
#                     SUBSTRATE_ENV_AUTH_TOKEN for the ate-env CLI
#   PORT              agent-server port in the environment (default 8000)
#   AGENT_SERVER_ENV  space-separated NAME=VALUE settings for the agent-server,
#                     e.g. "LOG_JSON=true"; each becomes a template --env
#                     (values cannot contain spaces)
#   COMMAND           shell command the demo runs through the agent-server
#                     (default: distribution, user and working directory)
#   WORKDIR           directory COMMAND runs in (default: the agent-server's
#                     own, the image's WORKDIR: the guest starts there and
#                     the sidecar inherits it)
#   KEEP              1 keeps the environments and prints how to reach them
#   TEMPLATE          base template name (default oh-agent-server)
#   ATESPACE          atespace (default ate-env); another atespace must be in
#                     ate-env manifest --port-proxy-atespaces, or the proxy
#                     answers 403
#   API               ate-env-api address for the CLI and the proxy
#                     (default 127.0.0.1:7777; port-forward it)
#   ATE_ENV, KUBECTL_ATE  binaries (default: on PATH)
set -euo pipefail
umask 077

: "${GUEST_IMAGE:?set GUEST_IMAGE to the digest-pinned ate-env-guest image}"
# SNAPSHOTS_BUCKET is optional: a per-template location, which needs Substrate's
# PerTemplateSnapshotLocation preview gate; empty uses the cluster's location.
SNAPSHOTS_BUCKET="${SNAPSHOTS_BUCKET:-}"
: "${LAYER_IMAGE:?set LAYER_IMAGE to the digest-pinned agent-server layer image}"
: "${IMAGES:?set IMAGES to one or more digest-pinned images}"
PORT=${PORT:-8000}
# shellcheck disable=SC2016 # expanded by the environment's shell, not here
COMMAND=${COMMAND:-'head -1 /etc/os-release; echo "user: $(id -un)"; echo "cwd: $(pwd)"'}
TEMPLATE=${TEMPLATE:-oh-agent-server}
# debian:bookworm-slim: glibc 2.36, /bin/sh and bash, 26 MB compressed for amd64.
BASE_IMAGE=${BASE_IMAGE:-docker.io/library/debian@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8}
ATESPACE=${ATESPACE:-ate-env}
API=${API:-127.0.0.1:7777}
ATE_ENV=${ATE_ENV:-ate-env}
KUBECTL_ATE=${KUBECTL_ATE:-kubectl-ate}
if [ -n "${API_TOKEN:-}" ]; then
  export SUBSTRATE_ENV_AUTH_TOKEN="${API_TOKEN}"
fi
if [ -n "${REUSE:-}" ]; then
  echo "note: REUSE is gone; reusing the templates is the default, RECREATE=1 registers them again" >&2
fi

step() { printf '\n== %s\n' "$*"; }

# base_has_guest_volume: whether the base template mounts the guest as an image
# volume, which only a base registered with --rootfs-image does.
base_has_guest_volume() {
  "${KUBECTL_ATE}" get actor-template --atespace "${ATESPACE}" -o json |
    TEMPLATE="${TEMPLATE}" python3 -c '
import json, os, sys
d = json.load(sys.stdin)
for t in d.get("actorTemplates", d.get("actor_templates", [])):
    if t.get("metadata", {}).get("name", "") == os.environ["TEMPLATE"]:
        names = [v.get("name") for v in t.get("volumes", [])]
        sys.exit(0 if "guest" in names else 1)
sys.exit(1)'
}

# templates: the names of the base template and the templates derived from it.
templates() {
  "${KUBECTL_ATE}" get actor-template --atespace "${ATESPACE}" -o json |
    TEMPLATE="${TEMPLATE}" python3 -c '
import json, os, re, sys
base = os.environ["TEMPLATE"]
own = re.compile(re.escape(base) + r"(-[0-9a-f]{12})?")
d = json.load(sys.stdin)
for t in d.get("actorTemplates", d.get("actor_templates", [])):
    name = t.get("metadata", {}).get("name", "")
    if own.fullmatch(name):
        print(name)'
}

existing=$(templates)
base_exists=0
for name in ${existing}; do
  [ "${name}" = "${TEMPLATE}" ] && base_exists=1
done
# Register the base when it is missing, also when only templates derived from
# it remain, or with RECREATE=1.
register=1
if [ -n "${existing}" ] && [ "${RECREATE:-}" != 1 ]; then
  if [ -z "${SESSION_KEY:-}" ]; then
    # Derived templates reuse the key of the base they came from.
    echo "${ATESPACE}/${TEMPLATE} or a template derived from it exists and carries its own session key:" >&2
    echo "set SESSION_KEY to that key, or set RECREATE=1 to delete them and register the base again" >&2
    exit 2
  fi
  [ "${base_exists}" = 1 ] && register=0
fi
if [ -z "${SESSION_KEY:-}" ]; then
  if [ "${KEEP:-}" = 1 ]; then
    echo "set SESSION_KEY: with KEEP=1 you need the key later, and a random one is never printed" >&2
    exit 2
  fi
  SESSION_KEY=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
fi

# Header files for curl: the key and the token stay out of every argv.
headers=$(mktemp -d)
ids=()
cleanup() {
  rm -rf "${headers}"
  [ "${KEEP:-}" = 1 ] && return
  for id in "${ids[@]:-}"; do
    [ -n "${id}" ] && "${ATE_ENV}" --api "${API}" delete "${id}" --atespace "${ATESPACE}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT
printf 'X-Session-API-Key: %s\n' "${SESSION_KEY}" >"${headers}/key"
: >"${headers}/proxy"
if [ -n "${API_TOKEN:-}" ]; then
  printf 'X-Env-Authorization: Bearer %s\n' "${API_TOKEN}" >"${headers}/proxy"
  cat "${headers}/proxy" >>"${headers}/key"
fi

# oh <env-id> <curl args...>: one request to the agent-server in <env-id>,
# through ate-env-api.
oh() {
  local id=$1
  shift
  curl -sS --max-time 120 \
    -H "X-Env-Id: ${id}" -H "X-Env-Atespace: ${ATESPACE}" -H "X-Env-Port: ${PORT}" \
    -H "@${headers}/key" "$@"
}

read -r -a images <<<"${IMAGES}"
read -r -a settings <<<"${AGENT_SERVER_ENV:-}"
env_flags=(--env "OH_SESSION_API_KEYS_0=${SESSION_KEY}")
for kv in ${settings[@]+"${settings[@]}"}; do
  env_flags+=(--env "${kv}")
done

if [ "${register}" = 0 ]; then
  step "reusing ${ATESPACE}/${TEMPLATE} and the templates derived from it (RECREATE=1 registers it again)"
  if ! base_has_guest_volume; then
    echo "note: ${ATESPACE}/${TEMPLATE} has no glibc base image (no guest image volume), thus its own golden" >&2
    echo "actor crashes. Environments from task images still work. To fix the base, run again with" >&2
    echo "RECREATE=1 and the same SESSION_KEY." >&2
  fi
else
  if [ "${RECREATE:-}" = 1 ]; then
    for name in ${existing}; do
      step "delete ${ATESPACE}/${name} (RECREATE=1)"
      "${KUBECTL_ATE}" delete actor-template --atespace "${ATESPACE}" "${name}" >/dev/null
    done
  fi
  step "register ${ATESPACE}/${TEMPLATE}: ${BASE_IMAGE%%@*} + guest + agent-server layer on port ${PORT}"
  "${ATE_ENV}" manifest template --template "${TEMPLATE}" --atespace "${ATESPACE}" \
    --rootfs-image "${BASE_IMAGE}" \
    --guest-image "${GUEST_IMAGE}" ${SNAPSHOTS_BUCKET:+--snapshots-bucket "${SNAPSHOTS_BUCKET}"} \
    --layer "oh=${LAYER_IMAGE}=/opt/oh" \
    --sidecar "/opt/oh/python/bin/python3 -m openhands.agent_server --host 0.0.0.0 --port ${PORT}" \
    --sidecar-readyz "http://127.0.0.1:${PORT}/health" \
    "${env_flags[@]}" | "${KUBECTL_ATE}" create actor-template -f - >/dev/null
fi

body=$(COMMAND="${COMMAND}" WORKDIR="${WORKDIR:-}" python3 -c 'import json,os; d={"command": os.environ["COMMAND"], "timeout": 60}; w=os.environ.get("WORKDIR"); d.update({"cwd": w} if w else {}); print(json.dumps(d))')

n=0
failed=0
for image in "${images[@]}"; do
  n=$((n + 1))
  id="oh-$(date +%s)-${n}"
  ids+=("${id}")
  step "${id}: ${image}"
  t0=$(date +%s)
  "${ATE_ENV}" --api "${API}" create "${id}" --atespace "${ATESPACE}" --template "${TEMPLATE}" --rootfs-image "${image}" >/dev/null
  # The router wakes or waits for the actor; the agent-server answers once the
  # actor is ready, which the guest gates on the same /health.
  code=000
  for _ in $(seq 1 120); do
    code=$(oh "${id}" -o /dev/null -w '%{http_code}' "http://${API}/health" || true)
    [ "${code}" = 200 ] && break
    case "${code}" in
    400 | 401 | 403)
      echo "the port proxy refused ${id} (HTTP ${code}); check API_TOKEN and PORT" >&2
      break
      ;;
    esac
    sleep 5
  done
  if [ "${code}" != 200 ]; then
    echo "agent-server in ${id} did not answer (last status ${code})" >&2
    failed=$((failed + 1))
    continue
  fi
  echo "agent-server answering after $(($(date +%s) - t0))s"

  echo "-- command, run with the image's own /bin/sh:"
  oh "${id}" -X POST -H 'Content-Type: application/json' "http://${API}/api/bash/execute_bash_command" -d "${body}" |
    python3 -c 'import json,sys; r=json.load(sys.stdin); print(r.get("stdout", r).rstrip()); print("exit:", r.get("exit_code"))'
  echo "-- file download (/etc/os-release, first line):"
  oh "${id}" "http://${API}/api/file/download?path=/etc/os-release" | head -1
  echo "-- without the session key:"
  curl -sS -o /dev/null -w 'HTTP %{http_code}\n' -X POST -H 'Content-Type: application/json' \
    -H "X-Env-Id: ${id}" -H "X-Env-Atespace: ${ATESPACE}" -H "X-Env-Port: ${PORT}" \
    -H "@${headers}/proxy" -d '{"command": "true"}' "http://${API}/api/bash/execute_bash_command"
done

if [ "${KEEP:-}" = 1 ]; then
  step "kept: ${ids[*]}"
  echo "reach one with: -H 'X-Env-Id: <id>' -H 'X-Env-Port: ${PORT}' -H \"X-Session-API-Key: \$SESSION_KEY\" http://${API}/..."
  [ -n "${API_TOKEN:-}" ] && echo "  and -H \"X-Env-Authorization: Bearer \$API_TOKEN\""
  echo "delete with: ${ATE_ENV} --api ${API} delete <id> --atespace ${ATESPACE}"
else
  step "done; deleting the environments (the templates stay for reuse)"
fi
if [ "${failed}" -gt 0 ]; then
  echo "${failed} of ${n} agent-servers did not answer" >&2
  exit 1
fi

#!/usr/bin/env bash
#
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

# Run swebench-loader inside the cluster, next to ate-env-api, and bring the
# events file back. The dataset, offline-exclusions.txt, a -placement plan
# and any -trajectory file (with the files next to it) are baked into the
# loader image (ko's kodata directory), so the pod needs no bucket access; a
# sidecar keeps the pod alive after the loader exits so the results can be
# copied out with kubectl cp. The copy is retried; if it still fails, the Job
# is kept and the script prints the command to fetch the file by hand.
# Otherwise the Job is deleted when the script exits, on success or failure.
#
#   KO_DOCKER_REPO=gcr.io/PROJECT/repo \
#   benchmarking/swebench/run-in-cluster.sh RUN_ID DATASET.jsonl [swebench-loader run flags...]
#
# RUN_ID must be a DNS label (lowercase letters, digits, '-', at most 20
# characters). Results land in ./RUN_ID.jsonl and ./RUN_ID.json in the
# directory the script is run from, which can be outside the repository.
# NAMESPACE defaults to ate-env; CONTEXT selects the kubectl context (default
# the current one), so one shell can drive the same run on several clusters
# in turn: put the cluster in RUN_ID and compare the events files with
# `swebench-loader report a.jsonl b.jsonl`. START_TIMEOUT (seconds, default
# 600) bounds the wait for the pod to start. A failed kubectl call while
# waiting for the loader's exit code is retried; only a pod that is gone
# (NotFound) or Failed ends the wait early. If the script is killed before
# its trap runs, the Job ends by itself after 48 hours (activeDeadlineSeconds)
# and is removed an hour after it finishes; Ctrl-C runs the trap and deletes
# the Job at once, results included.
# AUTH_TOKEN_SECRET names a Secret in NAMESPACE whose key `token` holds
# ate-env-api's bearer token; the loader then reads it from
# SUBSTRATE_ENV_AUTH_TOKEN. Leave it unset for an API without a token.
# RESTORE_LOG, when set, is where atelet's restore records for the run are
# collected: harvest-restore-timings.sh runs every HARVEST_INTERVAL_MIN
# minutes (default 10) while the loader runs and once more at the end, and
# the parts are merged with duplicates dropped. Kubelet rotates a busy
# atelet's log within the hour, so one harvest at the end loses most of a
# long run. The report then reads the file through -restore-log.
#
# Optional telemetry bundle and GCS upload environment variables:
#   COLLECT_TELEMETRY=1       Enable node-cache sampling, restore harvesting,
#                             periodic K8s snapshots, and bundle assembly in
#                             ARTIFACT_DIR (default ./<RUN_ID>-bundle).
#   ARTIFACT_DIR=<dir>        Output v1 benchmark bundle directory.
#   MEASURE_INTERVAL_SEC=<n>  Periodic K8s snapshot interval in seconds
#                             (default 60 when COLLECT_TELEMETRY=1, else 0).
#   SAMPLE_CACHE=1            Scrape node image-cache counters before/after.
#   ATESPACE=<ns>             Substrate atespace (default NAMESPACE).
#   BASE_TEMPLATE=<name>      Base actor template (default default-template).
#   ATE_ENDPOINT=<host:port>  Optional ate-api gRPC endpoint for reap/measure.
#   PROM_URL=<http-url>       Optional Prometheus base URL for measure.
#   PREFLIGHT_COVERAGE=<file> Optional preflight-coverage.json to include.
#   FOOTING=<warm|cold>       Optional footing check for bundle validation.
#   ENABLE_TRACING=1          Pass -enable-tracing (plus OTEL_EXPORTER_OTLP_ENDPOINT).
#   VALIDATE_BUNDLE=1         Run bench validate -dest on the assembled bundle.
#   GCS_BENCHMARK_BUCKET=<b>  Validate and upload the bundle to gs://<b>/<folder>/
#                             (implies COLLECT_TELEMETRY=1 and VALIDATE_BUNDLE=1;
#                             never hardcoded).
#   GCS_BENCHMARK_FOLDER=<f>  Explicit <YYYY-MM-DD>_<model>_<dataset>_<nickname>[_traced].
#   GCS_BENCHMARK_NICKNAME=<n> Nickname segment when deriving the GCS folder.
#   GCS_DRY_RUN=1             Validate bundle and GCS URI without uploading.

set -o errexit -o nounset -o pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 RUN_ID DATASET.jsonl [swebench-loader run flags...]" >&2
  exit 2
fi
RUN_ID="$1"
DATASET="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
shift 2
NS="${NAMESPACE:-ate-env}"
CONTEXT="${CONTEXT:-}"
START_TIMEOUT="${START_TIMEOUT:-600}"
# Every kubectl call goes through kc so CONTEXT applies to all of them. The
# context in use is logged so each run's log names its cluster.
kc() { kubectl ${CONTEXT:+--context "${CONTEXT}"} "$@"; }
echo "kubectl context ${CONTEXT:-$(kubectl config current-context 2>/dev/null || echo unknown)}"
: "${KO_DOCKER_REPO:?set KO_DOCKER_REPO to the registry path the cluster can pull from}"
[[ "${RUN_ID}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#RUN_ID} -le 20 ]] || { echo "RUN_ID must be a DNS label of at most 20 characters" >&2; exit 2; }
[[ -f "${DATASET}" ]] || { echo "dataset ${DATASET} not found" >&2; exit 2; }
AUTH_TOKEN_SECRET="${AUTH_TOKEN_SECRET:-}"
# Checked as a DNS subdomain name, so the value cannot break the manifest.
[[ -z "${AUTH_TOKEN_SECRET}" || ( "${AUTH_TOKEN_SECRET}" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ && ${#AUTH_TOKEN_SECRET} -le 253 ) ]] || { echo "AUTH_TOKEN_SECRET must be a Secret name" >&2; exit 2; }
LOADER_ENV=""
ENABLE_TRACING="${ENABLE_TRACING:-0}"
OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT:-}"
LOADER_ENV_LINES=()
if [[ -n "${AUTH_TOKEN_SECRET}" ]]; then
  LOADER_ENV_LINES+=("            - name: SUBSTRATE_ENV_AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${AUTH_TOKEN_SECRET}
                  key: token")
fi
if [[ "${ENABLE_TRACING}" == "1" && -z "${OTEL_EXPORTER_OTLP_ENDPOINT}" ]]; then
  echo "warning: ENABLE_TRACING=1 with no OTEL_EXPORTER_OTLP_ENDPOINT: the loader exports its spans to the OTel default localhost:4317 in its pod; without a collector there each export fails" >&2
fi
if [[ "${ENABLE_TRACING}" == "1" && -n "${OTEL_EXPORTER_OTLP_ENDPOINT}" ]]; then
  OTLP_EP_JSON="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${OTEL_EXPORTER_OTLP_ENDPOINT}")"
  LOADER_ENV_LINES+=("            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: ${OTLP_EP_JSON}")
  if [[ -n "${OTEL_EXPORTER_OTLP_INSECURE:-}" ]]; then
    OTLP_INSEC_JSON="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${OTEL_EXPORTER_OTLP_INSECURE}")"
    LOADER_ENV_LINES+=("            - name: OTEL_EXPORTER_OTLP_INSECURE
              value: ${OTLP_INSEC_JSON}")
  fi
fi
if (( ${#LOADER_ENV_LINES[@]} > 0 )); then
  LOADER_ENV="$(printf 'env:\n%s' "$(IFS=$'\n'; echo "${LOADER_ENV_LINES[*]}")")"
fi
command -v python3 >/dev/null || { echo "python3 is needed to encode the loader arguments" >&2; exit 2; }
OUT_DIR="$(pwd)"
RESTORE_LOG="${RESTORE_LOG:-}"
HARVEST_INTERVAL_MIN="${HARVEST_INTERVAL_MIN:-10}"
[[ "${HARVEST_INTERVAL_MIN}" =~ ^[1-9][0-9]*$ ]] || { echo "HARVEST_INTERVAL_MIN must be a whole number of minutes" >&2; exit 2; }
if [[ -n "${RESTORE_LOG}" && "${RESTORE_LOG}" != /* ]]; then
  RESTORE_LOG="${OUT_DIR}/${RESTORE_LOG}"
fi

# The repository is found from the script's own location, so the script can
# run from any directory and the results land where it was invoked.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"
# shellcheck source=benchmarking/swebench/bundle-telemetry.sh
source "${ROOT}/benchmarking/swebench/bundle-telemetry.sh"

GCS_BENCHMARK_BUCKET="${GCS_BENCHMARK_BUCKET:-}"
if [[ -n "${GCS_BENCHMARK_BUCKET}" ]]; then
  GCS_BENCHMARK_BUCKET="${GCS_BENCHMARK_BUCKET#gs://}"
  GCS_BENCHMARK_BUCKET="${GCS_BENCHMARK_BUCKET%/}"
  if [[ -z "${GCS_BENCHMARK_BUCKET}" || "${GCS_BENCHMARK_BUCKET}" == */* || "${GCS_BENCHMARK_BUCKET}" =~ [[:space:]] ]]; then
    echo "GCS_BENCHMARK_BUCKET must be a plain bucket name (or gs://<bucket>) without object prefix" >&2
    exit 2
  fi
fi
GCS_BENCHMARK_FOLDER="${GCS_BENCHMARK_FOLDER:-}"
GCS_BENCHMARK_NICKNAME="${GCS_BENCHMARK_NICKNAME:-}"
GCS_DRY_RUN="${GCS_DRY_RUN:-0}"
if [[ -z "${COLLECT_TELEMETRY:-}" ]]; then
  if [[ -n "${GCS_BENCHMARK_BUCKET}" ]]; then
    COLLECT_TELEMETRY="1"
  else
    COLLECT_TELEMETRY="0"
  fi
fi
if [[ "${COLLECT_TELEMETRY}" != "0" && "${COLLECT_TELEMETRY}" != "1" ]]; then
  echo "COLLECT_TELEMETRY must be 0 or 1 (got '${COLLECT_TELEMETRY}')" >&2
  exit 2
fi
if [[ -z "${VALIDATE_BUNDLE:-}" ]]; then
  if [[ "${COLLECT_TELEMETRY}" == "1" || -n "${GCS_BENCHMARK_BUCKET}" ]]; then
    VALIDATE_BUNDLE="1"
  else
    VALIDATE_BUNDLE="0"
  fi
fi
BUNDLE_MODE=0
if [[ "${COLLECT_TELEMETRY}" == "1" || -n "${ARTIFACT_DIR:-}" || -n "${GCS_BENCHMARK_BUCKET}" || "${VALIDATE_BUNDLE}" == "1" ]]; then
  BUNDLE_MODE=1
fi
BUNDLE_DIR="${ARTIFACT_DIR:-${OUT_DIR}/${RUN_ID}-bundle}"
if [[ "${BUNDLE_DIR}" != /* ]]; then
  BUNDLE_DIR="${OUT_DIR}/${BUNDLE_DIR}"
fi
if [[ "${COLLECT_TELEMETRY}" == "1" ]]; then
  SAMPLE_CACHE="${SAMPLE_CACHE:-1}"
  MEASURE_INTERVAL_SEC="${MEASURE_INTERVAL_SEC:-60}"
else
  SAMPLE_CACHE="${SAMPLE_CACHE:-0}"
  MEASURE_INTERVAL_SEC="${MEASURE_INTERVAL_SEC:-0}"
fi
[[ "${MEASURE_INTERVAL_SEC}" =~ ^(0|[1-9][0-9]*)$ ]] || { echo "MEASURE_INTERVAL_SEC must be a non-negative integer" >&2; exit 2; }
ATESPACE="${ATESPACE:-${NS}}"
BASE_TEMPLATE="${BASE_TEMPLATE:-default-template}"
ATE_ENDPOINT="${ATE_ENDPOINT:-}"
PROM_URL="${PROM_URL:-}"
PREFLIGHT_COVERAGE="${PREFLIGHT_COVERAGE:-}"
if [[ -n "${PREFLIGHT_COVERAGE}" && "${PREFLIGHT_COVERAGE}" != /* ]]; then
  PREFLIGHT_COVERAGE="${OUT_DIR}/${PREFLIGHT_COVERAGE}"
fi
if [[ -n "${PREFLIGHT_COVERAGE}" && ! -f "${PREFLIGHT_COVERAGE}" ]]; then
  echo "PREFLIGHT_COVERAGE not found: ${PREFLIGHT_COVERAGE}" >&2
  exit 2
fi
FOOTING="${FOOTING:-}"
if [[ -n "${FOOTING}" && "${FOOTING}" != "warm" && "${FOOTING}" != "cold" ]]; then
  echo "FOOTING must be 'warm' or 'cold'" >&2
  exit 2
fi
if (( BUNDLE_MODE == 1 )); then
  mkdir -p "${BUNDLE_DIR}/events"
  if [[ "${COLLECT_TELEMETRY}" == "1" && -z "${RESTORE_LOG}" ]]; then
    RESTORE_LOG="${BUNDLE_DIR}/atelet_restores.jsonl"
  fi
fi
HARVEST="${ROOT}/benchmarking/swebench/harvest-restore-timings.sh"
HARVEST_DIR="$(mktemp -d)"
HARVEST_PID=""
KODATA=cmd/benchmarking/swebench-loader/kodata
rm -rf "${KODATA}" && mkdir -p "${KODATA}"
trap 'rm -rf "${KODATA}"' EXIT
cp "${DATASET}" "${KODATA}/dataset.jsonl"
cp benchmarking/swebench/offline-exclusions.txt "${KODATA}/offline-exclusions.txt"

# A -trajectory value that names a file on this machine, by an absolute path
# or one relative to the directory the script was invoked in, is baked into
# the image too, with the files its write steps name (a grader script next
# to its trajectory) and nothing else from its directory, and the flag is
# rewritten to the baked path. Built-in names pass through. A comma-separated
# mix is handled element by element. The result is left in BAKED, not
# printed, so the counter survives.
TRAJ_N=0
BAKED=""
bake_trajectory() {
  local -a parts out=()
  local el path dir base asset
  IFS=',' read -r -a parts <<< "$1"
  for el in "${parts[@]}"; do
    path="${el}"
    [[ "${path}" = /* ]] || path="${OUT_DIR}/${path}"
    if [[ -f "${path}" ]]; then
      dir="$(cd "$(dirname "${path}")" && pwd)"
      base="$(basename "${path}")"
      TRAJ_N=$((TRAJ_N + 1))
      mkdir -p "${KODATA}/traj/${TRAJ_N}"
      cp "${dir}/${base}" "${KODATA}/traj/${TRAJ_N}/"
      # The loader's own parser names the files the write steps need, so a
      # flow-style step or a "file:" inside a script cannot mislead this; it
      # allows plain file names only. A symlink is refused here, because cp
      # would follow it out of the directory.
      assets="$(go run ./cmd/benchmarking/swebench-loader trajectory-assets "${dir}/${base}")" || exit 2
      while IFS= read -r asset; do
        [[ -n "${asset}" ]] || continue
        case "${asset}" in
          */*|..) echo "${base}: write.file ${asset} must be a plain file name" >&2; exit 2 ;;
        esac
        [[ -f "${dir}/${asset}" && ! -L "${dir}/${asset}" ]] || { echo "${base}: write.file ${asset} is not a regular file next to the trajectory" >&2; exit 2; }
        cp "${dir}/${asset}" "${KODATA}/traj/${TRAJ_N}/${asset}"
      done <<< "${assets}"
      el="/var/run/ko/traj/${TRAJ_N}/${base}"
    fi
    out+=("${el}")
  done
  local IFS=','
  BAKED="${out[*]}"
}

# The -placement plan is baked next to the dataset, under its own name so the
# run's "placement" label (the file's base name) tells plans apart, and the
# flag is rewritten to the baked path. Relative paths resolve against the
# caller's directory, like the dataset and the trajectories.
bake_placement() {
  local path="$1"
  [[ "${path}" = /* ]] || path="${OUT_DIR}/${path}"
  [[ -f "${path}" ]] || { echo "-placement needs an existing file: $1" >&2; exit 2; }
  mkdir -p "${KODATA}/placement"
  cp "${path}" "${KODATA}/placement/$(basename "${path}")"
  BAKED="/var/run/ko/placement/$(basename "${path}")"
}
ARGS=()
while (( $# )); do
  case "$1" in
    -trajectory|--trajectory)
      [[ $# -ge 2 ]] || { echo "$1 needs a value" >&2; exit 2; }
      bake_trajectory "$2"; ARGS+=("$1" "${BAKED}"); shift 2 ;;
    -trajectory=*|--trajectory=*)
      bake_trajectory "${1#*=}"; ARGS+=("${1%%=*}=${BAKED}"); shift ;;
    -placement|--placement)
      [[ $# -ge 2 ]] || { echo "$1 needs an existing file" >&2; exit 2; }
      bake_placement "$2"; ARGS+=("$1" "${BAKED}"); shift 2 ;;
    -placement=*|--placement=*)
      bake_placement "${1#*=}"; ARGS+=("${1%%=*}=${BAKED}"); shift ;;
    *)
      ARGS+=("$1"); shift ;;
  esac
done
if [[ "${ENABLE_TRACING}" == "1" ]]; then
  ARGS+=("-enable-tracing")
fi
IMAGE="$(KO_DOCKER_REPO="${KO_DOCKER_REPO}/swebench-loader" ko build --bare ./cmd/benchmarking/swebench-loader)"
rm -rf "${KODATA}"
echo "loader image ${IMAGE}"

JOB="swebench-loader-${RUN_ID}"
# Every argument is encoded as a JSON string, so quotes and other YAML
# metacharacters in a flag value cannot break the manifest.
ARGS_JSON="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' \
  run "-endpoint=ate-env-api.${NS}.svc:7777" "-dataset=/var/run/ko/dataset.jsonl" \
  "-exclude-file=/var/run/ko/offline-exclusions.txt" "-run-id=${RUN_ID}" "-out=/work/${RUN_ID}.jsonl" ${ARGS[@]+"${ARGS[@]}"})"

KUBECTL_ERR="$(mktemp)"
# shellcheck disable=SC2329 # invoked by the EXIT trap
KEEP_JOB=""
cleanup() {
  if [[ -n "${KEEP_JOB}" ]]; then
    echo "keeping job ${JOB}: the events file was not copied out. Fetch it with" >&2
    echo "  kubectl -n ${NS} cp -c keep ${POD}:/work/${RUN_ID}.jsonl ${OUT_DIR}/${RUN_ID}.jsonl" >&2
    echo "then delete the job: kubectl -n ${NS} delete job ${JOB}" >&2
  else
    kc -n "${NS}" delete job "${JOB}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [[ -n "${HARVEST_PID}" ]]; then
    kill "${HARVEST_PID}" 2>/dev/null || true
  fi
  rm -rf "${KODATA}" "${HARVEST_DIR}"
  rm -f "${KUBECTL_ERR}"
}
trap cleanup EXIT

# harvest_part collects the restore records atelet logged in the last two
# intervals into a numbered part under HARVEST_DIR. The window overlaps the
# previous part on purpose; the merge drops the duplicates. The part is
# written under a temporary name and renamed when complete, so the merge
# never reads a part a harvest is still writing. atelet runs in ate-system
# whatever NAMESPACE the loader uses.
harvest_part() {
  mkdir -p "${HARVEST_DIR}"
  local part="${HARVEST_DIR}/part-$1"
  NAMESPACE=ate-system SINCE="$(( 2 * HARVEST_INTERVAL_MIN ))m" "${HARVEST}" "${RUN_ID}" "${part}.tmp" >&2 || true
  if [[ -s "${part}.tmp" ]]; then
    mv "${part}.tmp" "${part}.jsonl"
  fi
  return 0
}

# merge_harvest writes the union of the parts to RESTORE_LOG. No part is a
# run too short for one interval or a harvest that read nothing; the file is
# then empty, and the run's results are copied out regardless.
merge_harvest() {
  local parts=()
  shopt -s nullglob
  parts=("${HARVEST_DIR}"/part-*.jsonl)
  shopt -u nullglob
  if (( ${#parts[@]} > 0 )); then
    sort -u "${parts[@]}" > "${RESTORE_LOG}"
  else
    : > "${RESTORE_LOG}"
  fi
  echo "restore records in ${RESTORE_LOG}: $(wc -l < "${RESTORE_LOG}" | tr -d ' ')"
}

# shellcheck disable=SC2317,SC2329 # invoked by the EXIT trap
cleanup_with_telemetry() {
  bench_cleanup_telemetry
  cleanup
}
trap cleanup_with_telemetry EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if (( BUNDLE_MODE == 1 )); then
  bench_reset_bundle_telemetry "${BUNDLE_DIR}"
  if [[ "${SAMPLE_CACHE}" == "1" ]]; then
    if ! NAMESPACE=ate-system CONTEXT="${CONTEXT}" "${ROOT}/benchmarking/swebench/sample-node-cache.sh" "${BUNDLE_DIR}/cache_before.prom"; then
      echo "warning: sample-node-cache.sh failed before ${RUN_ID}" >&2
      rm -f "${BUNDLE_DIR}/cache_before.prom"
    fi
  fi
fi

kc -n "${NS}" delete job "${JOB}" --ignore-not-found --wait=true >/dev/null
# busybox 1.36, pinned by digest; Docker Hub rate-limits unauthenticated pulls.
cat <<EOF_JOB | kc apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB}
  namespace: ${NS}
spec:
  backoffLimit: 0
  # Bounds for a Job whose script died before its trap ran: the keep sidecar
  # sleeps 48 hours, and a finished Job is removed after an hour.
  activeDeadlineSeconds: 172800
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        app: swebench-loader
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        seccompProfile:
          type: RuntimeDefault
      volumes:
        - name: work
          emptyDir: {}
      containers:
        - name: loader
          image: ${IMAGE}
          args: ${ARGS_JSON}
          ${LOADER_ENV}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: work
              mountPath: /work
          resources:
            requests:
              cpu: "2"
              memory: 2Gi
            limits:
              memory: 4Gi
        - name: keep
          image: busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662
          command: ["sh", "-c", "sleep 172800"]
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: work
              mountPath: /work
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              memory: 64Mi
EOF_JOB

echo "waiting up to ${START_TIMEOUT}s for the pod"
POD=""
deadline=$((SECONDS + START_TIMEOUT))
while (( SECONDS < deadline )); do
  POD="$(kc -n "${NS}" get pod -l "job-name=${JOB}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${POD}" ]] && kc -n "${NS}" get pod "${POD}" -o jsonpath='{.status.containerStatuses[?(@.name=="loader")].state}' 2>/dev/null | grep -q 'running\|terminated'; then
    break
  fi
  sleep 5
done
if [[ -z "${POD}" ]] || ! kc -n "${NS}" get pod "${POD}" -o jsonpath='{.status.containerStatuses[?(@.name=="loader")].state}' 2>/dev/null | grep -q 'running\|terminated'; then
  echo "loader did not start within ${START_TIMEOUT}s" >&2
  if [[ -n "${POD}" ]]; then
    kc -n "${NS}" describe pod "${POD}" | tail -15 >&2 || true
  else
    echo "no pod was created for job ${JOB}; its events:" >&2
    kc -n "${NS}" describe job "${JOB}" | tail -15 >&2 || true
  fi
  exit 1
fi
echo "pod ${POD}; following the loader log"
if [[ -n "${RESTORE_LOG}" ]]; then
  # The loop outlives a killed sleep (a stray pkill on another run's loop
  # must not end this one); cleanup ends it by killing the subshell itself.
  ( n=0; while :; do sleep "$(( HARVEST_INTERVAL_MIN * 60 ))" || true; n=$(( n + 1 )); harvest_part "${n}"; done ) &
  HARVEST_PID=$!
fi
if (( BUNDLE_MODE == 1 && MEASURE_INTERVAL_SEC > 0 )); then
  bench_start_measure_loop "${ROOT}" "${CONTEXT}" "${BUNDLE_DIR}" "${RUN_ID}" "${ATESPACE}" "${BASE_TEMPLATE}" "${MEASURE_INTERVAL_SEC}" "${ATE_ENDPOINT}" "${PROM_URL}"
fi
kc -n "${NS}" logs -f "${POD}" -c loader || true

# The log stream can end before the container does (or drop with the
# connection); wait for the exit code, with the pod's own state as the
# deadline check. One failed kubectl call is not a lost pod: only NotFound
# means gone, and other errors are retried, MAX_API_ERRORS times in a row.
MAX_API_ERRORS="${MAX_API_ERRORS:-30}"
api_errors=0
until EXIT="$(kc -n "${NS}" get pod "${POD}" -o jsonpath='{.status.containerStatuses[?(@.name=="loader")].state.terminated.exitCode}' 2>/dev/null)" && [[ -n "${EXIT}" ]]; do
  if PHASE="$(kc -n "${NS}" get pod "${POD}" -o jsonpath='{.status.phase}' 2>"${KUBECTL_ERR}")"; then
    api_errors=0
  elif grep -q 'NotFound' "${KUBECTL_ERR}"; then
    PHASE=gone
  else
    api_errors=$((api_errors + 1))
    echo "kubectl get pod failed (${api_errors}/${MAX_API_ERRORS}): $(head -c 300 "${KUBECTL_ERR}")" >&2
    if (( api_errors >= MAX_API_ERRORS )); then
      exit 1
    fi
    sleep 10
    continue
  fi
  if [[ "${PHASE}" == "gone" || "${PHASE}" == "Failed" ]]; then
    echo "pod ${POD} is ${PHASE} before the loader reported an exit code" >&2
    exit 1
  fi
  sleep 10
done
echo "loader exit code ${EXIT}"
# The events file first: it is the run's only copy of the results. The copy
# is retried, because a cluster API server that times out once mid-copy is
# common on a large cluster; if it still fails, the Job is kept so the file
# can be fetched by hand (the keep sidecar holds the pod for 48 hours).
copied=""
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if kc -n "${NS}" cp -c keep "${POD}:/work/${RUN_ID}.jsonl" "${OUT_DIR}/${RUN_ID}.jsonl" 2>"${KUBECTL_ERR}" \
     && [[ -s "${OUT_DIR}/${RUN_ID}.jsonl" ]]; then
    copied=1
    break
  fi
  echo "events copy attempt ${attempt} failed: $(tail -1 "${KUBECTL_ERR}")" >&2
  sleep 15
done
if [[ -z "${copied}" ]]; then
  KEEP_JOB=1
  echo "could not copy the events file after 10 attempts; the job is kept" >&2
  exit "$(( EXIT == 0 ? 1 : EXIT ))"
fi
echo "events in ${OUT_DIR}/${RUN_ID}.jsonl"
if [[ -n "${RESTORE_LOG}" ]]; then
  kill "${HARVEST_PID}" 2>/dev/null || true
  wait "${HARVEST_PID}" 2>/dev/null || true
  HARVEST_PID=""
  harvest_part final
  merge_harvest
fi
REPORT_ARGS=()
if [[ -n "${RESTORE_LOG}" ]]; then
  REPORT_ARGS=(-restore-log "${RESTORE_LOG}")
fi
go run ./cmd/benchmarking/swebench-loader report "${OUT_DIR}/${RUN_ID}.jsonl" -json "${OUT_DIR}/${RUN_ID}.json" \
  ${REPORT_ARGS[@]+"${REPORT_ARGS[@]}"}

if (( BUNDLE_MODE == 1 )); then
  bench_stop_measure_loop "${BUNDLE_DIR}"
  if [[ "${SAMPLE_CACHE}" == "1" ]]; then
    if ! NAMESPACE=ate-system CONTEXT="${CONTEXT}" "${ROOT}/benchmarking/swebench/sample-node-cache.sh" "${BUNDLE_DIR}/cache_after.prom"; then
      echo "warning: sample-node-cache.sh failed after ${RUN_ID}" >&2
      rm -f "${BUNDLE_DIR}/cache_after.prom"
    fi
  fi
  mkdir -p "${BUNDLE_DIR}/events"
  cp "${OUT_DIR}/${RUN_ID}.jsonl" "${BUNDLE_DIR}/events/${RUN_ID}.jsonl"
  if [[ -n "${RESTORE_LOG}" && -s "${RESTORE_LOG}" && "${RESTORE_LOG}" != "${BUNDLE_DIR}/atelet_restores.jsonl" ]]; then
    cp "${RESTORE_LOG}" "${BUNDLE_DIR}/atelet_restores.jsonl"
  fi
  bench_finalize_bundle "${ROOT}" "${CONTEXT}" "${BUNDLE_DIR}" "${RUN_ID}" \
    "${ATESPACE}" "${BASE_TEMPLATE}" "${ATE_ENDPOINT}" "${PROM_URL}" "${PREFLIGHT_COVERAGE}"
  if [[ -n "${GCS_BENCHMARK_BUCKET}" || "${VALIDATE_BUNDLE}" == "1" ]]; then
    DATASET_SEG="$(basename "${DATASET}")"
    DATASET_SEG="${DATASET_SEG%.jsonl}"
    BUNDLE_FOLDER="$(bench_derive_gcs_folder "${BUNDLE_DIR}" "swebench-loader" "${DATASET_SEG}" "${GCS_BENCHMARK_NICKNAME:-${RUN_ID}}" "${GCS_BENCHMARK_FOLDER}" "" "0")"
    if ! bench_validate_and_upload "${ROOT}" "${CONTEXT}" "${BUNDLE_DIR}" "${BUNDLE_FOLDER}" "${GCS_BENCHMARK_BUCKET}" "${GCS_DRY_RUN}" "${VALIDATE_BUNDLE}" "${FOOTING}"; then
      echo "error: bundle validation or upload failed for ${BUNDLE_DIR}" >&2
      EXIT="$(( EXIT == 0 ? 1 : EXIT ))"
    fi
  fi
fi
exit "${EXIT}"

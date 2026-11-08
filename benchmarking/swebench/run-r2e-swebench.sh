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

# Simulated mixed R2E-Gym + SWE-bench benchmark runner with configurable
# concurrency, image count, replica rollouts per image, warm-pool orchestration
# strategies, and API/SDK client chains (Go ate-env-api, Go direct Substrate,
# and ate-env-api + Python SandboxFleet).
#
# Usage:
#   run-r2e-swebench.sh [dry-run|prewarm|run] [flags] [-- extra-loader-flags]
#
# Subcommands:
#   dry-run  Validate manifests, trajectories, client/strategy matrix, and <63-char actor names offline.
#   prewarm  Bake golden snapshots and warm node caches (1 actor per image).
#   run      Drive both shards concurrently per arm and emit report.json/report.md (plus comparison.{txt,json,md} for multi-arm sweeps).
#
# Flags (override corresponding environment variables):
#   -c, --concurrency N          Total active sandboxes across both shards (env: MAX_CONCURRENCY; default: 200)
#       --r2e-width N            Active sandbox cap for the R2E-Gym shard (env: R2E_WIDTH)
#       --swe-width N            Active sandbox cap for the SWE-bench shard (env: SWE_WIDTH)
#   -i, --images N               Distinct images selected per dataset (env: IMAGES; default: 20 -> 40 total)
#       --r2e-images N           Distinct images selected from R2E-Gym (env: R2E_IMAGES)
#       --swe-images N           Distinct images selected from SWE-bench (env: SWE_IMAGES)
#   -r, --rollouts-per-image N   Replica rollouts (or eval tasks) per image (env: ROLLOUTS_PER_IMAGE)
#       --rollouts N             Alias for --rollouts-per-image
#       --r2e-rollouts N         Replica rollouts per R2E-Gym image (env: R2E_FANOUT)
#       --swe-rollouts N         Replica rollouts per SWE-bench image (env: SWE_FANOUT)
#       --strategy STRATEGY      Orchestration strategy: none, naive, sliding, pipelined, all, or CSV (env: STRATEGY)
#       --window-size N          Distinct-image window size for sliding/pipelined (env: WINDOW_SIZE; default: 0 = auto)
#       --client, --chain CHAIN  Client/API chain: go, go-direct, python, all, or CSV (env: CLIENT; default: go)
#       --control-addr ADDR      Substrate ateapi gRPC host:port for --client go-direct (env: CONTROL_ADDR)
#       --router-addr ADDR       Substrate atenet-router host:port override for --client go-direct (env: ROUTER_ADDR)
#       --atenet-client-cert P   Client TLS PEM (cert + key) for atenet-router in --client go-direct (env: ATENET_CLIENT_CERT)
#       --atenet-ca-file P       CA PEM bundle verifying atenet-router in --client go-direct (env: ATENET_CA_FILE)
#       --atenet-server-name S   TLS SNI server name for atenet-router in --client go-direct (env: ATENET_SERVER_NAME)
#       --ateapi-token-file P    Bearer token file for ateapi in --client go-direct (env: ATEAPI_TOKEN_FILE)
#       --ateapi-ca-file P       CA PEM bundle verifying ateapi TLS in --client go-direct (env: ATEAPI_CA_FILE)
#       --auth-token-file P      Bearer token file for ate-env-api in local mode (env: AUTH_TOKEN_FILE)
#       --api-service NAME       In-cluster ate-env-api Service name (env: API_SERVICE; default: ate-env-api)
#       --python-bin PATH        Python interpreter for local --client python runs (env: PYTHON_BIN; default: python3)
#       --python-image IMAGE     Prebuilt image for in-cluster --client python Jobs (env: PYTHON_LOADER_IMAGE)
#       --r2e-dataset PATH       R2E-Gym JSONL manifest path (env: R2E_DATASET; required)
#       --swe-dataset PATH       SWE-bench JSONL manifest path (env: SWE_DATASET; required)
#       --artifact-dir DIR       Output artifact directory outside repo (env: ARTIFACT_DIR; default: /tmp/<RUN_ID>)
#       --run-id ID              DNS-label run prefix <= 14 chars (env: RUN_ID; default: r2eswe)
#       --workload graded|io     Trajectory profile: graded or io (env: WORKLOAD; default: graded)
#       --mode MODE              Loader mode: steps, eval, or open-loop (env: MODE; default: steps)
#       --steps N                Passes / training steps in steps mode (env: STEPS; default: 4, or 1 when --strategy is set)
#       --park POLICY            Park policy in steps mode without --strategy, which parks by itself (env: PARK; default: suspend)
#       --step-pause DUR         Optional pause between steps without --strategy, e.g. 5s (env: STEP_PAUSE; default: 0s)
#       --execution MODE         in-cluster or local (env: EXECUTION; auto-selects in-cluster with KO_DOCKER_REPO)
#       --collect-telemetry      Collect node cache deltas, atelet restores, and periodic K8s snapshots (env: COLLECT_TELEMETRY)
#       --measure-interval SEC   In-flight K8s snapshot interval in seconds; 0 disables (env: MEASURE_INTERVAL_SEC)
#       --ate-endpoint ADDR      Optional Substrate ateapi host:port for bench measure and bench reap (env: ATE_ENDPOINT)
#       --prom-url URL           Optional Prometheus HTTP base URL for bench measure (env: PROM_URL)
#       --preflight-coverage P   Optional preflight-coverage.json copied into each arm bundle (env: PREFLIGHT_COVERAGE)
#       --footing warm|cold      Optional footing assertion for bundle validation and upload (env: FOOTING)
#       --trace                  Enable SDK/OTel tracing and capture Python TraceEvent JSONL files (env: ENABLE_TRACING)
#       --otlp-endpoint ADDR     Optional OTLP/gRPC collector endpoint forwarded with --trace (env: OTEL_EXPORTER_OTLP_ENDPOINT)
#       --validate-bundle        Run bench validate -dest on each arm bundle after reporting (env: VALIDATE_BUNDLE)
#       --gcs-bucket BUCKET      Upload each validated arm bundle to gs://<BUCKET>/<folder>/ (env: GCS_BENCHMARK_BUCKET)
#       --gcs-folder FOLDER      Explicit GCS folder <YYYY-MM-DD>_<model>_<dataset>_<nickname>[_traced] (env: GCS_BENCHMARK_FOLDER)
#       --gcs-nickname NAME      Nickname segment override for auto-derived GCS folder names (env: GCS_BENCHMARK_NICKNAME)
#       --gcs-dry-run            Validate bundles and GCS destination URIs without uploading (env: GCS_DRY_RUN)
#   -h, --help                   Show this help text and exit.
#
# Examples:
#   # Sweep all 4 strategies across both Go API and ate-env-api + Python clients:
#   KO_DOCKER_REPO=us-central1-docker.pkg.dev/PROJECT/bench \
#   benchmarking/swebench/run-r2e-swebench.sh run \
#     --r2e-dataset /tmp/manifests/r2e-20.jsonl \
#     --swe-dataset /tmp/manifests/swebench-30.jsonl \
#     --concurrency 200 --images 20 --rollouts-per-image 5 \
#     --client all --strategy all \
#     --artifact-dir /tmp/r2eswe-sweep

set -o errexit -o nounset -o pipefail

usage() {
  sed -n '17,89p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

require_arg() {
  if (( $# < 2 )) || [[ -z "${2:-}" ]]; then
    echo "missing value for $1" >&2
    exit 2
  fi
}

require_pos_int() {
  local name="$1"
  local val="$2"
  if [[ ! "${val}" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer (got ${val})" >&2
    exit 2
  fi
}

require_nonneg_int() {
  local name="$1"
  local val="$2"
  if [[ ! "${val}" =~ ^(0|[1-9][0-9]*)$ ]]; then
    echo "${name} must be a non-negative integer (got ${val})" >&2
    exit 2
  fi
}

HAS_ENV_STEPS=0
if [[ -n "${STEPS:-}" ]]; then
  HAS_ENV_STEPS=1
fi

ACTION=""
CLI_CONCURRENCY=""
CLI_R2E_WIDTH=""
CLI_SWE_WIDTH=""
CLI_IMAGES=""
CLI_R2E_IMAGES=""
CLI_SWE_IMAGES=""
CLI_ROLLOUTS=""
CLI_R2E_ROLLOUTS=""
CLI_SWE_ROLLOUTS=""
CLI_STRATEGY=""
CLI_WINDOW_SIZE=""
CLI_CLIENT=""
CLI_STEPS=""
CLI_CONTROL_ADDR=""
CLI_ROUTER_ADDR=""
CLI_ATENET_CLIENT_CERT=""
CLI_ATENET_CA_FILE=""
CLI_ATENET_SERVER_NAME=""
CLI_ATEAPI_TOKEN_FILE=""
CLI_ATEAPI_CA_FILE=""
CLI_AUTH_TOKEN_FILE=""
CLI_API_SERVICE=""
CLI_PYTHON_BIN=""
CLI_PYTHON_IMAGE=""
CLI_COLLECT_TELEMETRY=""
CLI_MEASURE_INTERVAL=""
CLI_ATE_ENDPOINT=""
CLI_PROM_URL=""
CLI_PREFLIGHT_COVERAGE=""
CLI_FOOTING=""
CLI_ENABLE_TRACING=""
CLI_OTLP_ENDPOINT=""
CLI_VALIDATE_BUNDLE=""
CLI_GCS_BUCKET=""
CLI_GCS_FOLDER=""
CLI_GCS_NICKNAME=""
CLI_GCS_DRY_RUN=""
EXTRA_ARGS=()

while (( $# > 0 )); do
  case "$1" in
    dry-run|prewarm|run)
      if [[ -n "${ACTION}" ]]; then
        echo "subcommand already set to '${ACTION}' (got extra '$1')" >&2
        exit 2
      fi
      ACTION="$1"
      shift
      ;;
    -h|--help|help)
      usage
      exit 0
      ;;
    -c|--concurrency)
      require_arg "$1" "${2:-}"
      CLI_CONCURRENCY="$2"
      shift 2
      ;;
    --concurrency=*)
      CLI_CONCURRENCY="${1#*=}"
      shift
      ;;
    --r2e-width)
      require_arg "$1" "${2:-}"
      CLI_R2E_WIDTH="$2"
      shift 2
      ;;
    --r2e-width=*)
      CLI_R2E_WIDTH="${1#*=}"
      shift
      ;;
    --swe-width)
      require_arg "$1" "${2:-}"
      CLI_SWE_WIDTH="$2"
      shift 2
      ;;
    --swe-width=*)
      CLI_SWE_WIDTH="${1#*=}"
      shift
      ;;
    -i|--images)
      require_arg "$1" "${2:-}"
      CLI_IMAGES="$2"
      shift 2
      ;;
    --images=*)
      CLI_IMAGES="${1#*=}"
      shift
      ;;
    --r2e-images)
      require_arg "$1" "${2:-}"
      CLI_R2E_IMAGES="$2"
      shift 2
      ;;
    --r2e-images=*)
      CLI_R2E_IMAGES="${1#*=}"
      shift
      ;;
    --swe-images)
      require_arg "$1" "${2:-}"
      CLI_SWE_IMAGES="$2"
      shift 2
      ;;
    --swe-images=*)
      CLI_SWE_IMAGES="${1#*=}"
      shift
      ;;
    -r|--rollouts-per-image|--rollouts)
      require_arg "$1" "${2:-}"
      CLI_ROLLOUTS="$2"
      shift 2
      ;;
    --rollouts-per-image=*|--rollouts=*)
      CLI_ROLLOUTS="${1#*=}"
      shift
      ;;
    --r2e-rollouts)
      require_arg "$1" "${2:-}"
      CLI_R2E_ROLLOUTS="$2"
      shift 2
      ;;
    --r2e-rollouts=*)
      CLI_R2E_ROLLOUTS="${1#*=}"
      shift
      ;;
    --swe-rollouts)
      require_arg "$1" "${2:-}"
      CLI_SWE_ROLLOUTS="$2"
      shift 2
      ;;
    --swe-rollouts=*)
      CLI_SWE_ROLLOUTS="${1#*=}"
      shift
      ;;
    --strategy)
      require_arg "$1" "${2:-}"
      CLI_STRATEGY="$2"
      shift 2
      ;;
    --strategy=*)
      CLI_STRATEGY="${1#*=}"
      shift
      ;;
    --window-size)
      require_arg "$1" "${2:-}"
      CLI_WINDOW_SIZE="$2"
      shift 2
      ;;
    --window-size=*)
      CLI_WINDOW_SIZE="${1#*=}"
      shift
      ;;
    --client|--chain)
      require_arg "$1" "${2:-}"
      CLI_CLIENT="$2"
      shift 2
      ;;
    --client=*|--chain=*)
      CLI_CLIENT="${1#*=}"
      shift
      ;;
    --control-addr)
      require_arg "$1" "${2:-}"
      CLI_CONTROL_ADDR="$2"
      shift 2
      ;;
    --control-addr=*)
      CLI_CONTROL_ADDR="${1#*=}"
      shift
      ;;
    --router-addr)
      require_arg "$1" "${2:-}"
      CLI_ROUTER_ADDR="$2"
      shift 2
      ;;
    --router-addr=*)
      CLI_ROUTER_ADDR="${1#*=}"
      shift
      ;;
    --atenet-client-cert)
      require_arg "$1" "${2:-}"
      CLI_ATENET_CLIENT_CERT="$2"
      shift 2
      ;;
    --atenet-client-cert=*)
      CLI_ATENET_CLIENT_CERT="${1#*=}"
      shift
      ;;
    --atenet-ca-file)
      require_arg "$1" "${2:-}"
      CLI_ATENET_CA_FILE="$2"
      shift 2
      ;;
    --atenet-ca-file=*)
      CLI_ATENET_CA_FILE="${1#*=}"
      shift
      ;;
    --atenet-server-name)
      require_arg "$1" "${2:-}"
      CLI_ATENET_SERVER_NAME="$2"
      shift 2
      ;;
    --atenet-server-name=*)
      CLI_ATENET_SERVER_NAME="${1#*=}"
      shift
      ;;
    --ateapi-token-file)
      require_arg "$1" "${2:-}"
      CLI_ATEAPI_TOKEN_FILE="$2"
      shift 2
      ;;
    --ateapi-token-file=*)
      CLI_ATEAPI_TOKEN_FILE="${1#*=}"
      shift
      ;;
    --ateapi-ca-file)
      require_arg "$1" "${2:-}"
      CLI_ATEAPI_CA_FILE="$2"
      shift 2
      ;;
    --ateapi-ca-file=*)
      CLI_ATEAPI_CA_FILE="${1#*=}"
      shift
      ;;
    --auth-token-file)
      require_arg "$1" "${2:-}"
      CLI_AUTH_TOKEN_FILE="$2"
      shift 2
      ;;
    --auth-token-file=*)
      CLI_AUTH_TOKEN_FILE="${1#*=}"
      shift
      ;;
    --api-service)
      require_arg "$1" "${2:-}"
      CLI_API_SERVICE="$2"
      shift 2
      ;;
    --api-service=*)
      CLI_API_SERVICE="${1#*=}"
      shift
      ;;
    --python-bin)
      require_arg "$1" "${2:-}"
      CLI_PYTHON_BIN="$2"
      shift 2
      ;;
    --python-bin=*)
      CLI_PYTHON_BIN="${1#*=}"
      shift
      ;;
    --python-image)
      require_arg "$1" "${2:-}"
      CLI_PYTHON_IMAGE="$2"
      shift 2
      ;;
    --python-image=*)
      CLI_PYTHON_IMAGE="${1#*=}"
      shift
      ;;
    --r2e-dataset)
      require_arg "$1" "${2:-}"
      R2E_DATASET="$2"
      shift 2
      ;;
    --r2e-dataset=*)
      R2E_DATASET="${1#*=}"
      shift
      ;;
    --swe-dataset)
      require_arg "$1" "${2:-}"
      SWE_DATASET="$2"
      shift 2
      ;;
    --swe-dataset=*)
      SWE_DATASET="${1#*=}"
      shift
      ;;
    --artifact-dir)
      require_arg "$1" "${2:-}"
      ARTIFACT_DIR="$2"
      shift 2
      ;;
    --artifact-dir=*)
      ARTIFACT_DIR="${1#*=}"
      shift
      ;;
    --run-id)
      require_arg "$1" "${2:-}"
      RUN_ID="$2"
      shift 2
      ;;
    --run-id=*)
      RUN_ID="${1#*=}"
      shift
      ;;
    --workload)
      require_arg "$1" "${2:-}"
      WORKLOAD="$2"
      shift 2
      ;;
    --workload=*)
      WORKLOAD="${1#*=}"
      shift
      ;;
    --mode)
      require_arg "$1" "${2:-}"
      MODE="$2"
      shift 2
      ;;
    --mode=*)
      MODE="${1#*=}"
      shift
      ;;
    --steps)
      require_arg "$1" "${2:-}"
      CLI_STEPS="$2"
      shift 2
      ;;
    --steps=*)
      CLI_STEPS="${1#*=}"
      shift
      ;;
    --park)
      require_arg "$1" "${2:-}"
      PARK="$2"
      shift 2
      ;;
    --park=*)
      PARK="${1#*=}"
      shift
      ;;
    --step-pause)
      require_arg "$1" "${2:-}"
      STEP_PAUSE="$2"
      shift 2
      ;;
    --step-pause=*)
      STEP_PAUSE="${1#*=}"
      shift
      ;;
    --execution)
      require_arg "$1" "${2:-}"
      EXECUTION="$2"
      shift 2
      ;;
    --execution=*)
      EXECUTION="${1#*=}"
      shift
      ;;
    --collect-telemetry)
      CLI_COLLECT_TELEMETRY="1"
      shift
      ;;
    --no-collect-telemetry)
      CLI_COLLECT_TELEMETRY="0"
      shift
      ;;
    --measure-interval)
      require_arg "$1" "${2:-}"
      CLI_MEASURE_INTERVAL="$2"
      shift 2
      ;;
    --measure-interval=*)
      CLI_MEASURE_INTERVAL="${1#*=}"
      shift
      ;;
    --ate-endpoint)
      require_arg "$1" "${2:-}"
      CLI_ATE_ENDPOINT="$2"
      shift 2
      ;;
    --ate-endpoint=*)
      CLI_ATE_ENDPOINT="${1#*=}"
      shift
      ;;
    --prom-url)
      require_arg "$1" "${2:-}"
      CLI_PROM_URL="$2"
      shift 2
      ;;
    --prom-url=*)
      CLI_PROM_URL="${1#*=}"
      shift
      ;;
    --preflight-coverage)
      require_arg "$1" "${2:-}"
      CLI_PREFLIGHT_COVERAGE="$2"
      shift 2
      ;;
    --preflight-coverage=*)
      CLI_PREFLIGHT_COVERAGE="${1#*=}"
      shift
      ;;
    --footing)
      require_arg "$1" "${2:-}"
      CLI_FOOTING="$2"
      shift 2
      ;;
    --footing=*)
      CLI_FOOTING="${1#*=}"
      shift
      ;;
    --trace|--enable-tracing)
      CLI_ENABLE_TRACING="1"
      shift
      ;;
    --otlp-endpoint)
      require_arg "$1" "${2:-}"
      CLI_OTLP_ENDPOINT="$2"
      shift 2
      ;;
    --otlp-endpoint=*)
      CLI_OTLP_ENDPOINT="${1#*=}"
      shift
      ;;
    --validate-bundle)
      CLI_VALIDATE_BUNDLE="1"
      shift
      ;;
    --gcs-bucket)
      require_arg "$1" "${2:-}"
      CLI_GCS_BUCKET="$2"
      shift 2
      ;;
    --gcs-bucket=*)
      CLI_GCS_BUCKET="${1#*=}"
      shift
      ;;
    --gcs-folder)
      require_arg "$1" "${2:-}"
      CLI_GCS_FOLDER="$2"
      shift 2
      ;;
    --gcs-folder=*)
      CLI_GCS_FOLDER="${1#*=}"
      shift
      ;;
    --gcs-nickname)
      require_arg "$1" "${2:-}"
      CLI_GCS_NICKNAME="$2"
      shift 2
      ;;
    --gcs-nickname=*)
      CLI_GCS_NICKNAME="${1#*=}"
      shift
      ;;
    --gcs-dry-run)
      CLI_GCS_DRY_RUN="1"
      shift
      ;;
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "unknown argument: $1 (pass extra swebench-loader flags after --)" >&2
      exit 2
      ;;
  esac
done

ACTION="${ACTION:-run}"

INVOKED_DIR="$(pwd)"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

abs_path() {
  local p="$1"
  if [[ "${p}" == /* ]]; then
    printf '%s\n' "${p}"
  else
    printf '%s/%s\n' "${INVOKED_DIR}" "${p}"
  fi
}

append_unique() {
  local needle="$1"
  shift
  local existing
  for existing in "$@"; do
    if [[ "${existing}" == "${needle}" ]]; then
      return 1
    fi
  done
  return 0
}

: "${R2E_DATASET:?set --r2e-dataset or R2E_DATASET to the R2E-Gym JSONL manifest path}"
: "${SWE_DATASET:?set --swe-dataset or SWE_DATASET to the SWE-bench JSONL manifest path}"
R2E_DATASET="$(abs_path "${R2E_DATASET}")"
SWE_DATASET="$(abs_path "${SWE_DATASET}")"
[[ -f "${R2E_DATASET}" ]] || { echo "R2E_DATASET not found: ${R2E_DATASET}" >&2; exit 2; }
[[ -f "${SWE_DATASET}" ]] || { echo "SWE_DATASET not found: ${SWE_DATASET}" >&2; exit 2; }

if [[ -z "${SWE_EXCLUDE_FILE+x}" ]]; then
  SWE_EXCLUDE_FILE="${ROOT}/benchmarking/swebench/offline-exclusions.txt"
elif [[ -n "${SWE_EXCLUDE_FILE}" ]]; then
  SWE_EXCLUDE_FILE="$(abs_path "${SWE_EXCLUDE_FILE}")"
fi
if [[ -n "${SWE_EXCLUDE_FILE}" && ! -f "${SWE_EXCLUDE_FILE}" ]]; then
  echo "SWE_EXCLUDE_FILE not found: ${SWE_EXCLUDE_FILE}" >&2
  exit 2
fi

R2E_EXCLUDE_FILE="${R2E_EXCLUDE_FILE:-}"
if [[ -n "${R2E_EXCLUDE_FILE}" ]]; then
  R2E_EXCLUDE_FILE="$(abs_path "${R2E_EXCLUDE_FILE}")"
  [[ -f "${R2E_EXCLUDE_FILE}" ]] || { echo "R2E_EXCLUDE_FILE not found: ${R2E_EXCLUDE_FILE}" >&2; exit 2; }
fi

RUN_ID="${RUN_ID:-r2eswe}"
if [[ ! "${RUN_ID}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ || ${#RUN_ID} -gt 14 ]]; then
  echo "RUN_ID must be a DNS label of at most 14 characters (got ${RUN_ID})" >&2
  exit 2
fi

ARTIFACT_DIR="$(abs_path "${ARTIFACT_DIR:-/tmp/${RUN_ID}}")"

DEFAULT_IMAGES="${CLI_IMAGES:-${IMAGES:-20}}"
R2E_IMAGES="${CLI_R2E_IMAGES:-${CLI_IMAGES:-${R2E_IMAGES:-${DEFAULT_IMAGES}}}}"
SWE_IMAGES="${CLI_SWE_IMAGES:-${CLI_IMAGES:-${SWE_IMAGES:-${DEFAULT_IMAGES}}}}"
require_pos_int "R2E_IMAGES" "${R2E_IMAGES}"
require_pos_int "SWE_IMAGES" "${SWE_IMAGES}"

HAS_CONCURRENCY=0
if [[ -n "${CLI_CONCURRENCY}" || -n "${MAX_CONCURRENCY:-}" || -n "${CLI_R2E_WIDTH}" || -n "${R2E_WIDTH:-}" || -n "${CLI_SWE_WIDTH}" || -n "${SWE_WIDTH:-}" ]]; then
  HAS_CONCURRENCY=1
fi

HAS_ROLLOUTS=0
if [[ -n "${CLI_ROLLOUTS}" || -n "${ROLLOUTS_PER_IMAGE:-}" || -n "${CLI_R2E_ROLLOUTS}" || -n "${R2E_FANOUT:-}" || -n "${CLI_SWE_ROLLOUTS}" || -n "${SWE_FANOUT:-}" ]]; then
  HAS_ROLLOUTS=1
fi

EFF_CONCURRENCY="${CLI_CONCURRENCY:-${MAX_CONCURRENCY:-}}"
EFF_R2E_WIDTH="${CLI_R2E_WIDTH:-${R2E_WIDTH:-}}"
EFF_SWE_WIDTH="${CLI_SWE_WIDTH:-${SWE_WIDTH:-}}"
EFF_R2E_FANOUT="${CLI_R2E_ROLLOUTS:-${CLI_ROLLOUTS:-${R2E_FANOUT:-${ROLLOUTS_PER_IMAGE:-}}}}"
EFF_SWE_FANOUT="${CLI_SWE_ROLLOUTS:-${CLI_ROLLOUTS:-${SWE_FANOUT:-${ROLLOUTS_PER_IMAGE:-}}}}"

[[ -z "${EFF_CONCURRENCY}" ]] || require_pos_int "MAX_CONCURRENCY" "${EFF_CONCURRENCY}"
[[ -z "${EFF_R2E_WIDTH}" ]] || require_pos_int "R2E_WIDTH" "${EFF_R2E_WIDTH}"
[[ -z "${EFF_SWE_WIDTH}" ]] || require_pos_int "SWE_WIDTH" "${EFF_SWE_WIDTH}"
[[ -z "${EFF_R2E_FANOUT}" ]] || require_pos_int "R2E_FANOUT" "${EFF_R2E_FANOUT}"
[[ -z "${EFF_SWE_FANOUT}" ]] || require_pos_int "SWE_FANOUT" "${EFF_SWE_FANOUT}"

if (( HAS_CONCURRENCY == 0 && HAS_ROLLOUTS == 1 )); then
  R2E_FANOUT="${EFF_R2E_FANOUT:-${EFF_SWE_FANOUT}}"
  SWE_FANOUT="${EFF_SWE_FANOUT:-${EFF_R2E_FANOUT}}"
  R2E_WIDTH=$(( R2E_IMAGES * R2E_FANOUT ))
  SWE_WIDTH=$(( SWE_IMAGES * SWE_FANOUT ))
  MAX_CONCURRENCY=$(( R2E_WIDTH + SWE_WIDTH ))
else
  if [[ -n "${EFF_R2E_WIDTH}" && -n "${EFF_SWE_WIDTH}" ]]; then
    R2E_WIDTH="${EFF_R2E_WIDTH}"
    SWE_WIDTH="${EFF_SWE_WIDTH}"
    if [[ -n "${EFF_CONCURRENCY}" ]] && (( R2E_WIDTH + SWE_WIDTH != EFF_CONCURRENCY )); then
      echo "R2E_WIDTH (${R2E_WIDTH}) + SWE_WIDTH (${SWE_WIDTH}) must equal MAX_CONCURRENCY (${EFF_CONCURRENCY})" >&2
      exit 2
    fi
    MAX_CONCURRENCY=$(( R2E_WIDTH + SWE_WIDTH ))
  elif [[ -n "${EFF_R2E_WIDTH}" ]]; then
    R2E_WIDTH="${EFF_R2E_WIDTH}"
    if [[ -n "${EFF_CONCURRENCY}" ]]; then
      if (( EFF_CONCURRENCY <= R2E_WIDTH )); then
        echo "MAX_CONCURRENCY (${EFF_CONCURRENCY}) must exceed R2E_WIDTH (${R2E_WIDTH})" >&2
        exit 2
      fi
      SWE_WIDTH=$(( EFF_CONCURRENCY - R2E_WIDTH ))
    else
      SWE_WIDTH="${R2E_WIDTH}"
    fi
    MAX_CONCURRENCY=$(( R2E_WIDTH + SWE_WIDTH ))
  elif [[ -n "${EFF_SWE_WIDTH}" ]]; then
    SWE_WIDTH="${EFF_SWE_WIDTH}"
    if [[ -n "${EFF_CONCURRENCY}" ]]; then
      if (( EFF_CONCURRENCY <= SWE_WIDTH )); then
        echo "MAX_CONCURRENCY (${EFF_CONCURRENCY}) must exceed SWE_WIDTH (${SWE_WIDTH})" >&2
        exit 2
      fi
      R2E_WIDTH=$(( EFF_CONCURRENCY - SWE_WIDTH ))
    else
      R2E_WIDTH="${SWE_WIDTH}"
    fi
    MAX_CONCURRENCY=$(( R2E_WIDTH + SWE_WIDTH ))
  else
    MAX_CONCURRENCY="${EFF_CONCURRENCY:-200}"
    if (( MAX_CONCURRENCY < 2 )); then
      echo "MAX_CONCURRENCY must be >= 2 when driving both R2E and SWE-bench shards (got ${MAX_CONCURRENCY})" >&2
      exit 2
    fi
    W_R2E=$(( R2E_IMAGES * ${EFF_R2E_FANOUT:-1} ))
    W_SWE=$(( SWE_IMAGES * ${EFF_SWE_FANOUT:-1} ))
    R2E_WIDTH=$(( (MAX_CONCURRENCY * W_R2E) / (W_R2E + W_SWE) ))
    if (( R2E_WIDTH < 1 )); then
      R2E_WIDTH=1
    elif (( R2E_WIDTH >= MAX_CONCURRENCY )); then
      R2E_WIDTH=$(( MAX_CONCURRENCY - 1 ))
    fi
    SWE_WIDTH=$(( MAX_CONCURRENCY - R2E_WIDTH ))
  fi
  R2E_FANOUT="${EFF_R2E_FANOUT:-$(( (R2E_WIDTH + R2E_IMAGES - 1) / R2E_IMAGES ))}"
  SWE_FANOUT="${EFF_SWE_FANOUT:-$(( (SWE_WIDTH + SWE_IMAGES - 1) / SWE_IMAGES ))}"
fi

WORKLOAD="${WORKLOAD:-graded}"
IO_TRAJECTORY="${IO_TRAJECTORY:-workload-file-read}"
case "${WORKLOAD}" in
  graded)
    SWE_TRAJECTORY="${SWE_TRAJECTORY:-swebench-gold}"
    R2E_TRAJECTORY="${R2E_TRAJECTORY:-${ROOT}/benchmarking/swebench/r2e-gold.yaml}"
    ;;
  io)
    SWE_TRAJECTORY="${SWE_TRAJECTORY:-${IO_TRAJECTORY}}"
    R2E_TRAJECTORY="${R2E_TRAJECTORY:-${IO_TRAJECTORY}}"
    ;;
  *)
    echo "WORKLOAD must be 'graded' or 'io' (got ${WORKLOAD})" >&2
    exit 2
    ;;
esac

CONTROL_ADDR="${CLI_CONTROL_ADDR:-${CONTROL_ADDR:-}}"
ROUTER_ADDR="${CLI_ROUTER_ADDR:-${ROUTER_ADDR:-}}"
ATENET_CLIENT_CERT="${CLI_ATENET_CLIENT_CERT:-${ATENET_CLIENT_CERT:-}}"
ATENET_CA_FILE="${CLI_ATENET_CA_FILE:-${ATENET_CA_FILE:-}}"
ATENET_SERVER_NAME="${CLI_ATENET_SERVER_NAME:-${ATENET_SERVER_NAME:-}}"
ATEAPI_TOKEN_FILE="${CLI_ATEAPI_TOKEN_FILE:-${ATEAPI_TOKEN_FILE:-}}"
ATEAPI_CA_FILE="${CLI_ATEAPI_CA_FILE:-${ATEAPI_CA_FILE:-}}"
AUTH_TOKEN_FILE="${CLI_AUTH_TOKEN_FILE:-${AUTH_TOKEN_FILE:-}}"
API_SERVICE="${CLI_API_SERVICE:-${API_SERVICE:-ate-env-api}}"
PYTHON_BIN="${CLI_PYTHON_BIN:-${PYTHON_BIN:-python3}}"
PYTHON_LOADER_IMAGE="${CLI_PYTHON_IMAGE:-${PYTHON_LOADER_IMAGE:-}}"

if [[ "${PYTHON_BIN}" == */* ]]; then
  PYTHON_BIN="$(abs_path "${PYTHON_BIN}")"
fi
if [[ ! "${API_SERVICE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ || ${#API_SERVICE} -gt 63 ]]; then
  echo "API_SERVICE must be a valid Kubernetes Service DNS label (got ${API_SERVICE})" >&2
  exit 2
fi

if [[ -n "${ATENET_CLIENT_CERT}" ]]; then
  ATENET_CLIENT_CERT="$(abs_path "${ATENET_CLIENT_CERT}")"
  [[ -f "${ATENET_CLIENT_CERT}" ]] || { echo "ATENET_CLIENT_CERT not found: ${ATENET_CLIENT_CERT}" >&2; exit 2; }
fi
if [[ -n "${ATENET_CA_FILE}" ]]; then
  ATENET_CA_FILE="$(abs_path "${ATENET_CA_FILE}")"
  [[ -f "${ATENET_CA_FILE}" ]] || { echo "ATENET_CA_FILE not found: ${ATENET_CA_FILE}" >&2; exit 2; }
fi
if [[ -n "${ATEAPI_TOKEN_FILE}" ]]; then
  ATEAPI_TOKEN_FILE="$(abs_path "${ATEAPI_TOKEN_FILE}")"
  [[ -f "${ATEAPI_TOKEN_FILE}" ]] || { echo "ATEAPI_TOKEN_FILE not found: ${ATEAPI_TOKEN_FILE}" >&2; exit 2; }
fi
if [[ -n "${ATEAPI_CA_FILE}" ]]; then
  ATEAPI_CA_FILE="$(abs_path "${ATEAPI_CA_FILE}")"
  [[ -f "${ATEAPI_CA_FILE}" ]] || { echo "ATEAPI_CA_FILE not found: ${ATEAPI_CA_FILE}" >&2; exit 2; }
fi
if [[ -n "${AUTH_TOKEN_FILE}" ]]; then
  AUTH_TOKEN_FILE="$(abs_path "${AUTH_TOKEN_FILE}")"
  [[ -f "${AUTH_TOKEN_FILE}" ]] || { echo "AUTH_TOKEN_FILE not found: ${AUTH_TOKEN_FILE}" >&2; exit 2; }
fi

RAW_CLIENT="${CLI_CLIENT:-${CLIENT:-go}}"
CLIENTS=()
IFS=',' read -r -a RAW_CLIENT_PARTS <<< "${RAW_CLIENT}"
if (( ${#RAW_CLIENT_PARTS[@]} == 0 )); then
  echo "CLIENT must not be empty" >&2
  exit 2
fi
for tok in "${RAW_CLIENT_PARTS[@]}"; do
  case "${tok}" in
    go|go-api)
      if append_unique "go" ${CLIENTS[@]+"${CLIENTS[@]}"}; then CLIENTS+=("go"); fi
      ;;
    go-direct|direct)
      if append_unique "go-direct" ${CLIENTS[@]+"${CLIENTS[@]}"}; then CLIENTS+=("go-direct"); fi
      ;;
    python|python-api|ate-env-api-python)
      if append_unique "python" ${CLIENTS[@]+"${CLIENTS[@]}"}; then CLIENTS+=("python"); fi
      ;;
    all)
      if append_unique "go" ${CLIENTS[@]+"${CLIENTS[@]}"}; then CLIENTS+=("go"); fi
      if [[ -n "${CONTROL_ADDR}" ]]; then
        if append_unique "go-direct" ${CLIENTS[@]+"${CLIENTS[@]}"}; then CLIENTS+=("go-direct"); fi
      fi
      if append_unique "python" ${CLIENTS[@]+"${CLIENTS[@]}"}; then CLIENTS+=("python"); fi
      ;;
    *)
      echo "CLIENT must be 'go', 'go-direct', 'python', 'all', or a comma-separated list (got '${RAW_CLIENT}')" >&2
      exit 2
      ;;
  esac
done

RAW_STRATEGY="${CLI_STRATEGY:-${STRATEGY:-}}"
HAS_STRATEGY=0
STRATEGIES=()
if [[ -z "${RAW_STRATEGY}" ]]; then
  STRATEGIES=("")
else
  HAS_STRATEGY=1
  IFS=',' read -r -a RAW_STRAT_PARTS <<< "${RAW_STRATEGY}"
  for tok in "${RAW_STRAT_PARTS[@]}"; do
    case "${tok}" in
      none|naive|sliding|pipelined)
        if append_unique "${tok}" ${STRATEGIES[@]+"${STRATEGIES[@]}"}; then STRATEGIES+=("${tok}"); fi
        ;;
      all)
        for s_all in none naive sliding pipelined; do
          if append_unique "${s_all}" ${STRATEGIES[@]+"${STRATEGIES[@]}"}; then STRATEGIES+=("${s_all}"); fi
        done
        ;;
      *)
        echo "STRATEGY must be 'none', 'naive', 'sliding', 'pipelined', 'all', or a comma-separated list (got '${RAW_STRATEGY}')" >&2
        exit 2
        ;;
    esac
  done
fi

WINDOW_SIZE="${CLI_WINDOW_SIZE:-${WINDOW_SIZE:-0}}"
require_nonneg_int "WINDOW_SIZE" "${WINDOW_SIZE}"

MODE="${MODE:-steps}"
if [[ "${MODE}" != "steps" && "${MODE}" != "eval" && "${MODE}" != "open-loop" ]]; then
  echo "MODE must be 'steps', 'eval', or 'open-loop' (got ${MODE})" >&2
  exit 2
fi
if (( HAS_STRATEGY == 1 )) && [[ "${MODE}" == "open-loop" ]]; then
  echo "--strategy cannot be combined with --mode open-loop (use --mode steps or --mode eval)" >&2
  exit 2
fi

HAS_GO_API=0
HAS_GO_DIRECT=0
HAS_PYTHON=0
for c_chk in "${CLIENTS[@]}"; do
  if [[ "${c_chk}" == "go" ]]; then HAS_GO_API=1; fi
  if [[ "${c_chk}" == "go-direct" ]]; then HAS_GO_DIRECT=1; fi
  if [[ "${c_chk}" == "python" ]]; then HAS_PYTHON=1; fi
done

if [[ -n "${CLI_STEPS}" ]]; then
  STEPS="${CLI_STEPS}"
elif (( HAS_ENV_STEPS == 0 )); then
  if (( HAS_STRATEGY == 1 || HAS_PYTHON == 1 )); then
    STEPS="1"
  else
    STEPS="4"
  fi
fi
require_pos_int "STEPS" "${STEPS}"

if [[ "${MODE}" == "steps" ]] && (( STEPS > 1 && (HAS_STRATEGY == 1 || HAS_PYTHON == 1) )); then
  echo "--strategy (and --client python) are single-pass (require --steps 1 in steps mode; omit --strategy on --client go/go-direct for multi-step reset loops)" >&2
  exit 2
fi

if (( HAS_PYTHON == 1 )) && [[ "${MODE}" == "open-loop" ]]; then
  echo "--client python does not support --mode open-loop (use --mode steps or --mode eval)" >&2
  exit 2
fi
if (( HAS_GO_DIRECT == 1 )) && [[ "${ACTION}" != "dry-run" && -z "${CONTROL_ADDR}" ]]; then
  echo "--client go-direct requires --control-addr (or CONTROL_ADDR) pointing at Substrate ateapi host:port" >&2
  exit 2
fi

ARM_CLIENTS=()
ARM_STRATEGIES=()
ARM_NAMES=()
HAS_WINDOWED_ARM=0
for c_item in "${CLIENTS[@]}"; do
  for s_item in "${STRATEGIES[@]}"; do
    eff_s="${s_item}"
    if [[ -z "${eff_s}" ]] && (( HAS_PYTHON == 1 )); then
      if [[ "${MODE}" == "steps" ]]; then
        eff_s="pipelined"
      else
        eff_s="none"
      fi
    fi
    if [[ "${eff_s}" == "sliding" || "${eff_s}" == "pipelined" ]]; then
      HAS_WINDOWED_ARM=1
    fi
    if [[ -n "${eff_s}" ]]; then
      arm_lbl="${c_item}-${eff_s}"
    else
      arm_lbl="${c_item}-${MODE}"
    fi
    ARM_CLIENTS+=("${c_item}")
    ARM_STRATEGIES+=("${eff_s}")
    ARM_NAMES+=("${arm_lbl}")
  done
done

if (( WINDOW_SIZE > 0 && HAS_WINDOWED_ARM == 0 )); then
  echo "--window-size (${WINDOW_SIZE}) requires strategy 'sliding' or 'pipelined'" >&2
  exit 2
fi

PARK="${PARK:-suspend}"
STEP_PAUSE="${STEP_PAUSE:-0s}"
ARRIVAL_RATE="${ARRIVAL_RATE:-2.0}"
CONNECTIONS="${CONNECTIONS:-8}"
require_pos_int "CONNECTIONS" "${CONNECTIONS}"
WAKE_BUDGET="${WAKE_BUDGET:-5m}"
CREATE_TIMEOUT="${CREATE_TIMEOUT:-5m}"
STEP_TIMEOUT="${STEP_TIMEOUT:-15m}"

NAMESPACE="${NAMESPACE:-ate-env}"
CONTEXT="${CONTEXT:-}"
ENDPOINT="${ENDPOINT:-localhost:7777}"
ATESPACE="${ATESPACE:-${NAMESPACE}}"
BASE_TEMPLATE="${BASE_TEMPLATE:-default-template}"
AUTH_TOKEN_SECRET="${AUTH_TOKEN_SECRET:-}"
START_TIMEOUT="${START_TIMEOUT:-600}"
MAX_API_ERRORS="${MAX_API_ERRORS:-30}"

# shellcheck source=benchmarking/swebench/bundle-telemetry.sh
source "${ROOT}/benchmarking/swebench/bundle-telemetry.sh"

GCS_BENCHMARK_BUCKET="${CLI_GCS_BUCKET:-${GCS_BENCHMARK_BUCKET:-}}"
if [[ -n "${GCS_BENCHMARK_BUCKET}" ]]; then
  GCS_BENCHMARK_BUCKET="${GCS_BENCHMARK_BUCKET#gs://}"
  GCS_BENCHMARK_BUCKET="${GCS_BENCHMARK_BUCKET%/}"
  if [[ -z "${GCS_BENCHMARK_BUCKET}" || "${GCS_BENCHMARK_BUCKET}" == */* || "${GCS_BENCHMARK_BUCKET}" =~ [[:space:]] ]]; then
    echo "GCS_BENCHMARK_BUCKET must be a plain bucket name (or gs://<bucket>) without object prefix; use --gcs-folder for the folder name" >&2
    exit 2
  fi
fi
GCS_BENCHMARK_FOLDER="${CLI_GCS_FOLDER:-${GCS_BENCHMARK_FOLDER:-}}"
GCS_BENCHMARK_NICKNAME="${CLI_GCS_NICKNAME:-${GCS_BENCHMARK_NICKNAME:-}}"
GCS_DRY_RUN="${CLI_GCS_DRY_RUN:-${GCS_DRY_RUN:-0}}"

if [[ -n "${CLI_COLLECT_TELEMETRY}" ]]; then
  COLLECT_TELEMETRY="${CLI_COLLECT_TELEMETRY}"
elif [[ -z "${COLLECT_TELEMETRY:-}" ]]; then
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

if [[ "${COLLECT_TELEMETRY}" == "1" ]]; then
  SAMPLE_CACHE="${SAMPLE_CACHE:-1}"
  HARVEST_RESTORES="${HARVEST_RESTORES:-1}"
  MEASURE_INTERVAL_SEC="${CLI_MEASURE_INTERVAL:-${MEASURE_INTERVAL_SEC:-60}}"
else
  SAMPLE_CACHE="${SAMPLE_CACHE:-0}"
  HARVEST_RESTORES="${HARVEST_RESTORES:-0}"
  MEASURE_INTERVAL_SEC="${CLI_MEASURE_INTERVAL:-${MEASURE_INTERVAL_SEC:-0}}"
fi
require_nonneg_int "MEASURE_INTERVAL_SEC" "${MEASURE_INTERVAL_SEC}"
HARVEST_INTERVAL_MIN="${HARVEST_INTERVAL_MIN:-10}"
require_pos_int "HARVEST_INTERVAL_MIN" "${HARVEST_INTERVAL_MIN}"

ATE_ENDPOINT="${CLI_ATE_ENDPOINT:-${ATE_ENDPOINT:-}}"
PROM_URL="${CLI_PROM_URL:-${PROM_URL:-}}"
PREFLIGHT_COVERAGE="${CLI_PREFLIGHT_COVERAGE:-${PREFLIGHT_COVERAGE:-}}"
if [[ -n "${PREFLIGHT_COVERAGE}" ]]; then
  PREFLIGHT_COVERAGE="$(abs_path "${PREFLIGHT_COVERAGE}")"
  if [[ "${ACTION}" != "dry-run" && ! -f "${PREFLIGHT_COVERAGE}" ]]; then
    echo "PREFLIGHT_COVERAGE not found: ${PREFLIGHT_COVERAGE}" >&2
    exit 2
  fi
fi
FOOTING="${CLI_FOOTING:-${FOOTING:-}}"
if [[ -n "${FOOTING}" && "${FOOTING}" != "warm" && "${FOOTING}" != "cold" ]]; then
  echo "FOOTING must be 'warm' or 'cold' (got ${FOOTING})" >&2
  exit 2
fi
ENABLE_TRACING="${CLI_ENABLE_TRACING:-${ENABLE_TRACING:-0}}"
OTEL_EXPORTER_OTLP_ENDPOINT="${CLI_OTLP_ENDPOINT:-${OTEL_EXPORTER_OTLP_ENDPOINT:-}}"
if [[ "${ENABLE_TRACING}" == "1" && -z "${OTEL_EXPORTER_OTLP_ENDPOINT}" ]]; then
  for c_tr in "${CLIENTS[@]}"; do
    if [[ "${c_tr}" != "python" ]]; then
      echo "warning: --trace with no --otlp-endpoint: the ${c_tr} arms export their spans to the OTel default localhost:4317; without a collector there each export fails" >&2
    fi
  done
fi
if [[ -n "${CLI_VALIDATE_BUNDLE}" ]]; then
  VALIDATE_BUNDLE="${CLI_VALIDATE_BUNDLE}"
elif [[ -z "${VALIDATE_BUNDLE:-}" ]]; then
  if [[ "${COLLECT_TELEMETRY}" == "1" || -n "${GCS_BENCHMARK_BUCKET}" ]]; then
    VALIDATE_BUNDLE="1"
  else
    VALIDATE_BUNDLE="0"
  fi
fi
# bench_bundle_has_trace and bench_check_gcs_folder_syntax run python3 whenever
# a GCS folder is derived or checked: with a bucket or folder, and after each
# arm that is validated.
if [[ -n "${GCS_BENCHMARK_BUCKET}" || -n "${GCS_BENCHMARK_FOLDER}" || "${VALIDATE_BUNDLE}" == "1" ]]; then
  command -v python3 >/dev/null || { echo "python3 is required with --gcs-bucket, --gcs-folder and --validate-bundle (GCS folder checks)" >&2; exit 2; }
fi

if [[ -z "${EXECUTION:-}" ]]; then
  if [[ -n "${KO_DOCKER_REPO:-}" ]]; then
    EXECUTION="in-cluster"
  else
    EXECUTION="local"
  fi
fi
if [[ "${EXECUTION}" != "local" && "${EXECUTION}" != "in-cluster" ]]; then
  echo "EXECUTION must be 'local' or 'in-cluster' (got ${EXECUTION})" >&2
  exit 2
fi

if [[ -n "${AUTH_TOKEN_SECRET}" ]] && [[ ! "${AUTH_TOKEN_SECRET}" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ || ${#AUTH_TOKEN_SECRET} -gt 253 ]]; then
  echo "AUTH_TOKEN_SECRET must be a valid Kubernetes Secret name" >&2
  exit 2
fi
if [[ "${EXECUTION}" == "in-cluster" && -n "${AUTH_TOKEN_FILE}" && -z "${AUTH_TOKEN_SECRET}" ]] && (( HAS_GO_API == 1 || HAS_PYTHON == 1 )); then
  echo "--auth-token-file is a host file path and is not mounted into in-cluster Jobs; set AUTH_TOKEN_SECRET=<k8s-secret-name> (key: token) or use --execution local" >&2
  exit 2
fi

kc() { kubectl ${CONTEXT:+--context "${CONTEXT}"} "$@"; }

json_encode_args() {
  python3 - "$@" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1:]))
PY
}

json_encode_str() {
  python3 - "$1" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1]))
PY
}

extra_args_has_label_key() {
  local want_key="$1"
  local i=0
  local n="${#EXTRA_ARGS[@]}"
  local arg val
  while (( i < n )); do
    arg="${EXTRA_ARGS[$i]}"
    val=""
    case "${arg}" in
      -label|--label)
        if (( i + 1 < n )); then
          val="${EXTRA_ARGS[$(( i + 1 ))]}"
          i=$(( i + 1 ))
        fi
        ;;
      -label=*|--label=*)
        val="${arg#*=}"
        ;;
    esac
    if [[ -n "${val}" && "${val%%=*}" == "${want_key}" ]]; then
      return 0
    fi
    i=$(( i + 1 ))
  done
  return 1
}

PY_EXTRA_SET_ARGS=()
PY_EXTRA_LABEL_ARGS=()
GO_RENDER_SET_ARGS=()
parse_python_extra_args() {
  PY_EXTRA_SET_ARGS=()
  PY_EXTRA_LABEL_ARGS=()
  GO_RENDER_SET_ARGS=()
  local i=0
  local n="${#EXTRA_ARGS[@]}"
  local arg val
  while (( i < n )); do
    arg="${EXTRA_ARGS[$i]}"
    case "${arg}" in
      -set|--set)
        require_arg "${arg}" "${EXTRA_ARGS[$(( i + 1 ))]:-}"
        val="${EXTRA_ARGS[$(( i + 1 ))]}"
        PY_EXTRA_SET_ARGS+=("--set=${val}")
        GO_RENDER_SET_ARGS+=("-set=${val}")
        i=$(( i + 2 ))
        ;;
      -set=*|--set=*)
        val="${arg#*=}"
        PY_EXTRA_SET_ARGS+=("--set=${val}")
        GO_RENDER_SET_ARGS+=("-set=${val}")
        i=$(( i + 1 ))
        ;;
      -label|--label)
        require_arg "${arg}" "${EXTRA_ARGS[$(( i + 1 ))]:-}"
        val="${EXTRA_ARGS[$(( i + 1 ))]}"
        PY_EXTRA_LABEL_ARGS+=("--label=${val}")
        i=$(( i + 2 ))
        ;;
      -label=*|--label=*)
        val="${arg#*=}"
        PY_EXTRA_LABEL_ARGS+=("--label=${val}")
        i=$(( i + 1 ))
        ;;
      *)
        echo "extra flag '${arg}' after -- is not supported with --client python (supported: -set/--set, -label/--label)" >&2
        exit 2
        ;;
    esac
  done
}

if (( HAS_PYTHON == 1 && ${#EXTRA_ARGS[@]} > 0 )); then
  parse_python_extra_args
fi

arm_run_prefix() {
  local idx="$1"
  local total="$2"
  if (( total <= 1 )); then
    printf '%s\n' "${RUN_ID}"
    return 0
  fi
  local stem="${RUN_ID:0:10}"
  while [[ "${stem}" == *- ]]; do
    stem="${stem%-}"
  done
  [[ -n "${stem}" ]] || stem="r"
  printf '%s-a%d\n' "${stem}" "${idx}"
}

mode_args_for_go() {
  local shard_mode="$1"
  local fanout="$2"
  local shard_images="$3"
  local shard_width="$4"
  local arm_strategy="$5"
  local fleet=$(( shard_images * fanout ))
  local has_pause=0
  if [[ -n "${STEP_PAUSE}" && "${STEP_PAUSE}" != "0" && "${STEP_PAUSE}" != "0s" ]]; then
    has_pause=1
  fi

  MODE_ARGS=("-mode=${shard_mode}")
  if [[ -n "${arm_strategy}" ]]; then
    MODE_ARGS+=("-strategy=${arm_strategy}")
    if (( WINDOW_SIZE > 0 )) && [[ "${arm_strategy}" == "sliding" || "${arm_strategy}" == "pipelined" ]]; then
      MODE_ARGS+=("-window-size=${WINDOW_SIZE}")
    fi
    case "${shard_mode}" in
      steps)
        # The loader parks by the strategy (suspend; delete for none) and
        # runs one pass; it refuses -park and -step-pause with -strategy.
        if (( has_pause == 1 )); then
          echo "STEP_PAUSE (${STEP_PAUSE}) belongs to the step loop; it cannot be combined with --strategy ${arm_strategy}" >&2
          exit 2
        fi
        MODE_ARGS+=("-rollouts=${fanout}" "-steps=${STEPS}")
        ;;
      eval)
        MODE_ARGS+=("-tasks-per-image=${fanout}")
        ;;
      *)
        echo "MODE '${shard_mode}' cannot be combined with --strategy '${arm_strategy}'" >&2
        exit 2
        ;;
    esac
    return 0
  fi

  case "${shard_mode}" in
    steps)
      MODE_ARGS+=("-rollouts=${fanout}" "-steps=${STEPS}" "-park=${PARK}")
      if (( fleet > shard_width )); then
        if (( has_pause == 1 )); then
          echo "STEP_PAUSE (${STEP_PAUSE}) holds all ${fleet} shard environments awake until the step barrier, exceeding shard width ${shard_width}; omit --step-pause or raise --concurrency" >&2
          exit 2
        fi
        if [[ "${PARK}" != "suspend" && "${PARK}" != "suspend-sync" && "${PARK}" != "suspend-async" && "${PARK}" != "delete" ]]; then
          echo "PARK=${PARK} holds all ${fleet} shard environments awake until the step barrier, exceeding shard width ${shard_width}; use --park suspend, suspend-sync, suspend-async or delete, or raise --concurrency" >&2
          exit 2
        fi
        MODE_ARGS+=("-park-each")
      elif (( has_pause == 1 )); then
        MODE_ARGS+=("-step-pause=${STEP_PAUSE}")
      fi
      ;;
    eval)
      MODE_ARGS+=("-tasks-per-image=${fanout}")
      ;;
    open-loop)
      MODE_ARGS+=("-tasks-per-image=${fanout}" "-arrival-rate=${ARRIVAL_RATE}")
      ;;
    *)
      echo "MODE must be 'steps', 'eval', or 'open-loop' (got ${shard_mode})" >&2
      exit 2
      ;;
  esac
}

mode_args_for_python() {
  local shard_mode="$1"
  local fanout="$2"
  local arm_strategy="$3"
  PY_MODE_ARGS=("--mode=${shard_mode}" "--strategy=${arm_strategy}" "--steps=${STEPS}")
  if (( WINDOW_SIZE > 0 )) && [[ "${arm_strategy}" == "sliding" || "${arm_strategy}" == "pipelined" ]]; then
    PY_MODE_ARGS+=("--window-size=${WINDOW_SIZE}")
  fi
  case "${shard_mode}" in
    steps)
      PY_MODE_ARGS+=("--rollouts=${fanout}")
      ;;
    eval)
      PY_MODE_ARGS+=("--tasks-per-image=${fanout}")
      ;;
    *)
      echo "--client python supports --mode steps or --mode eval (got ${shard_mode})" >&2
      exit 2
      ;;
  esac
}

go_backend_args_local() {
  local arm_client="$1"
  GO_BACKEND_ARGS=("-backend=substrate")
  case "${arm_client}" in
    go)
      GO_BACKEND_ARGS+=("-access=ate-env-api" "-endpoint=${ENDPOINT}")
      if [[ -n "${AUTH_TOKEN_FILE}" ]]; then
        GO_BACKEND_ARGS+=("-auth-token-file=${AUTH_TOKEN_FILE}")
      fi
      ;;
    go-direct)
      GO_BACKEND_ARGS+=("-access=direct")
      if [[ -n "${CONTROL_ADDR}" ]]; then
        GO_BACKEND_ARGS+=("-control-addr=${CONTROL_ADDR}")
      fi
      if [[ -n "${ROUTER_ADDR}" ]]; then
        GO_BACKEND_ARGS+=("-router-addr=${ROUTER_ADDR}")
      fi
      if [[ -n "${ATENET_CLIENT_CERT}" ]]; then
        GO_BACKEND_ARGS+=("-atenet-client-cert=${ATENET_CLIENT_CERT}")
      fi
      if [[ -n "${ATENET_CA_FILE}" ]]; then
        GO_BACKEND_ARGS+=("-atenet-ca-file=${ATENET_CA_FILE}")
      fi
      if [[ -n "${ATENET_SERVER_NAME}" ]]; then
        GO_BACKEND_ARGS+=("-atenet-server-name=${ATENET_SERVER_NAME}")
      fi
      if [[ -n "${ATEAPI_TOKEN_FILE}" ]]; then
        GO_BACKEND_ARGS+=("-ateapi-token-file=${ATEAPI_TOKEN_FILE}")
      fi
      if [[ -n "${ATEAPI_CA_FILE}" ]]; then
        GO_BACKEND_ARGS+=("-ateapi-ca-file=${ATEAPI_CA_FILE}")
      fi
      ;;
  esac
}

arm_label_args_go() {
  local arm_client="$1"
  local arm_name="$2"
  GO_LABEL_ARGS=()
  if ! extra_args_has_label_key "client"; then
    GO_LABEL_ARGS+=("-label=client=${arm_client}")
  fi
  if ! extra_args_has_label_key "arm"; then
    GO_LABEL_ARGS+=("-label=arm=${arm_name}")
  fi
}

arm_label_args_python() {
  local arm_client="$1"
  local arm_name="$2"
  PY_LABEL_ARGS=()
  if ! extra_args_has_label_key "client"; then
    PY_LABEL_ARGS+=("--label=client=${arm_client}")
  fi
  if ! extra_args_has_label_key "arm"; then
    PY_LABEL_ARGS+=("--label=arm=${arm_name}")
  fi
}

render_python_plans() {
  local plan_dir="$1"
  local swe_traj="$2"
  local r2e_traj="$3"
  local swe_fanout="$4"
  local r2e_fanout="$5"
  mkdir -p "${plan_dir}"

  local -a swe_rp=(
    go run ./cmd/benchmarking/swebench-loader render-plan
    "-dataset=${SWE_DATASET}"
    "-images=${SWE_IMAGES}"
    "-trajectory=${swe_traj}"
    "-fanout=${swe_fanout}"
    "-out=${plan_dir}/swe-plan.json"
  )
  if [[ -n "${SWE_EXCLUDE_FILE}" ]]; then
    swe_rp+=("-exclude-file=${SWE_EXCLUDE_FILE}")
  fi
  if (( ${#GO_RENDER_SET_ARGS[@]} > 0 )); then
    swe_rp+=("${GO_RENDER_SET_ARGS[@]}")
  fi

  local -a r2e_rp=(
    go run ./cmd/benchmarking/swebench-loader render-plan
    "-dataset=${R2E_DATASET}"
    "-images=${R2E_IMAGES}"
    "-trajectory=${r2e_traj}"
    "-fanout=${r2e_fanout}"
    "-out=${plan_dir}/r2e-plan.json"
  )
  if [[ -n "${R2E_EXCLUDE_FILE}" ]]; then
    r2e_rp+=("-exclude-file=${R2E_EXCLUDE_FILE}")
  fi
  if (( ${#GO_RENDER_SET_ARGS[@]} > 0 )); then
    r2e_rp+=("${GO_RENDER_SET_ARGS[@]}")
  fi

  "${swe_rp[@]}"
  "${r2e_rp[@]}"
}

NUM_ARMS="${#ARM_NAMES[@]}"
ARM_LIST_STR="${ARM_NAMES[*]}"
echo "resolved plan: action=${ACTION} mode=${MODE} steps=${STEPS} workload=${WORKLOAD} arms=${NUM_ARMS} (${ARM_LIST_STR}) concurrency=${MAX_CONCURRENCY} images=$(( R2E_IMAGES + SWE_IMAGES )) (r2e=${R2E_IMAGES}x${R2E_FANOUT}@width=${R2E_WIDTH}, swe=${SWE_IMAGES}x${SWE_FANOUT}@width=${SWE_WIDTH})" >&2

cd "${ROOT}"

if [[ "${ACTION}" == "dry-run" ]]; then
  for (( idx = 0; idx < NUM_ARMS; idx++ )); do
    arm_client="${ARM_CLIENTS[$idx]}"
    arm_strategy="${ARM_STRATEGIES[$idx]}"
    arm_name="${ARM_NAMES[$idx]}"
    arm_prefix="$(arm_run_prefix "$(( idx + 1 ))" "${NUM_ARMS}")"
    swe_run="${arm_prefix}-swe"
    r2e_run="${arm_prefix}-r2e"
    if (( NUM_ARMS > 1 )); then
      arm_dir="${ARTIFACT_DIR}/arms/${arm_name}"
    else
      arm_dir="${ARTIFACT_DIR}"
    fi
    mkdir -p "${arm_dir}/events"

    if [[ "${arm_client}" == "python" ]]; then
      render_python_plans "${arm_dir}/plans" "${SWE_TRAJECTORY}" "${R2E_TRAJECTORY}" "${SWE_FANOUT}" "${R2E_FANOUT}"
      mode_args_for_python "${MODE}" "${SWE_FANOUT}" "${arm_strategy}"
      swe_py_mode=("${PY_MODE_ARGS[@]}")
      mode_args_for_python "${MODE}" "${R2E_FANOUT}" "${arm_strategy}"
      r2e_py_mode=("${PY_MODE_ARGS[@]}")
      arm_label_args_python "${arm_client}" "${arm_name}"
      swe_py_trace=()
      r2e_py_trace=()
      if [[ "${ENABLE_TRACING}" == "1" ]]; then
        swe_py_trace=("--enable-tracing" "--trace-out=${arm_dir}/traces/swe.jsonl")
        r2e_py_trace=("--enable-tracing" "--trace-out=${arm_dir}/traces/r2e.jsonl")
      fi

      "${PYTHON_BIN}" "${ROOT}/benchmarking/swebench/run_fleet_bench.py" --dry-run \
        "--rendered-plan=${arm_dir}/plans/swe-plan.json" \
        "--run-id=${swe_run}" \
        "--width=${SWE_WIDTH}" \
        "--events-dir=${arm_dir}" \
        "--worker=swe" \
        "--job=swe" \
        "--connections=${CONNECTIONS}" \
        "--atespace=${ATESPACE}" \
        "--base-template=${BASE_TEMPLATE}" \
        "--wake-budget=${WAKE_BUDGET}" \
        "--create-timeout=${CREATE_TIMEOUT}" \
        "--step-timeout=${STEP_TIMEOUT}" \
        "${swe_py_mode[@]}" \
        ${swe_py_trace[@]+"${swe_py_trace[@]}"} \
        ${PY_LABEL_ARGS[@]+"${PY_LABEL_ARGS[@]}"} \
        ${PY_EXTRA_SET_ARGS[@]+"${PY_EXTRA_SET_ARGS[@]}"} \
        ${PY_EXTRA_LABEL_ARGS[@]+"${PY_EXTRA_LABEL_ARGS[@]}"}

      "${PYTHON_BIN}" "${ROOT}/benchmarking/swebench/run_fleet_bench.py" --dry-run \
        "--rendered-plan=${arm_dir}/plans/r2e-plan.json" \
        "--run-id=${r2e_run}" \
        "--width=${R2E_WIDTH}" \
        "--events-dir=${arm_dir}" \
        "--worker=r2e" \
        "--job=r2e" \
        "--connections=${CONNECTIONS}" \
        "--atespace=${ATESPACE}" \
        "--base-template=${BASE_TEMPLATE}" \
        "--wake-budget=${WAKE_BUDGET}" \
        "--create-timeout=${CREATE_TIMEOUT}" \
        "--step-timeout=${STEP_TIMEOUT}" \
        "${r2e_py_mode[@]}" \
        ${r2e_py_trace[@]+"${r2e_py_trace[@]}"} \
        ${PY_LABEL_ARGS[@]+"${PY_LABEL_ARGS[@]}"} \
        ${PY_EXTRA_SET_ARGS[@]+"${PY_EXTRA_SET_ARGS[@]}"} \
        ${PY_EXTRA_LABEL_ARGS[@]+"${PY_EXTRA_LABEL_ARGS[@]}"}
    else
      go_backend_args_local "${arm_client}"
      arm_label_args_go "${arm_client}" "${arm_name}"

      mode_args_for_go "${MODE}" "${SWE_FANOUT}" "${SWE_IMAGES}" "${SWE_WIDTH}" "${arm_strategy}"
      SWE_DRY_ARGS=(
        run -dry-run
        "${GO_BACKEND_ARGS[@]}"
        "-dataset=${SWE_DATASET}"
        "-run-id=${swe_run}"
        "-images=${SWE_IMAGES}"
        "-width=${SWE_WIDTH}"
        "-trajectory=${SWE_TRAJECTORY}"
        "-events-dir=${arm_dir}"
        "-worker=swe"
        "-job=swe"
        "-connections=${CONNECTIONS}"
        "-atespace=${ATESPACE}"
        "-base-template=${BASE_TEMPLATE}"
        "-wake-budget=${WAKE_BUDGET}"
        "-create-timeout=${CREATE_TIMEOUT}"
        "-step-timeout=${STEP_TIMEOUT}"
        "${MODE_ARGS[@]}"
        ${GO_LABEL_ARGS[@]+"${GO_LABEL_ARGS[@]}"}
      )
      if [[ "${ENABLE_TRACING}" == "1" ]]; then
        SWE_DRY_ARGS+=("-enable-tracing")
      fi
      if [[ -n "${SWE_EXCLUDE_FILE}" ]]; then
        SWE_DRY_ARGS+=("-exclude-file=${SWE_EXCLUDE_FILE}")
      fi
      if (( ${#EXTRA_ARGS[@]} > 0 )); then
        SWE_DRY_ARGS+=("${EXTRA_ARGS[@]}")
      fi

      mode_args_for_go "${MODE}" "${R2E_FANOUT}" "${R2E_IMAGES}" "${R2E_WIDTH}" "${arm_strategy}"
      R2E_DRY_ARGS=(
        run -dry-run
        "${GO_BACKEND_ARGS[@]}"
        "-dataset=${R2E_DATASET}"
        "-run-id=${r2e_run}"
        "-images=${R2E_IMAGES}"
        "-width=${R2E_WIDTH}"
        "-trajectory=${R2E_TRAJECTORY}"
        "-events-dir=${arm_dir}"
        "-worker=r2e"
        "-job=r2e"
        "-connections=${CONNECTIONS}"
        "-atespace=${ATESPACE}"
        "-base-template=${BASE_TEMPLATE}"
        "-wake-budget=${WAKE_BUDGET}"
        "-create-timeout=${CREATE_TIMEOUT}"
        "-step-timeout=${STEP_TIMEOUT}"
        "${MODE_ARGS[@]}"
        ${GO_LABEL_ARGS[@]+"${GO_LABEL_ARGS[@]}"}
      )
      if [[ "${ENABLE_TRACING}" == "1" ]]; then
        R2E_DRY_ARGS+=("-enable-tracing")
      fi
      if [[ -n "${R2E_EXCLUDE_FILE}" ]]; then
        R2E_DRY_ARGS+=("-exclude-file=${R2E_EXCLUDE_FILE}")
      fi
      if (( ${#EXTRA_ARGS[@]} > 0 )); then
        R2E_DRY_ARGS+=("${EXTRA_ARGS[@]}")
      fi

      go run ./cmd/benchmarking/swebench-loader "${SWE_DRY_ARGS[@]}"
      go run ./cmd/benchmarking/swebench-loader "${R2E_DRY_ARGS[@]}"
    fi

    if [[ -n "${GCS_BENCHMARK_BUCKET}" || -n "${GCS_BENCHMARK_FOLDER}" ]]; then
      expect_traced="0"
      if [[ "${ENABLE_TRACING}" == "1" && "${arm_client}" == "python" ]]; then
        expect_traced="1"
      fi
      if (( NUM_ARMS > 1 )); then
        nick_seg="${GCS_BENCHMARK_NICKNAME:-${RUN_ID}}-${arm_name}"
        multi_suffix="${arm_name}"
      else
        nick_seg="${GCS_BENCHMARK_NICKNAME:-${RUN_ID}-${arm_name}}"
        multi_suffix=""
      fi
      dry_folder="$(bench_derive_gcs_folder "${arm_dir}" "simulated-w${MAX_CONCURRENCY}" "r2e${R2E_IMAGES}-swe${SWE_IMAGES}" "${nick_seg}" "${GCS_BENCHMARK_FOLDER}" "${multi_suffix}" "${expect_traced}")"
      bench_check_gcs_folder_syntax "${dry_folder}" "${expect_traced}"
      if [[ -n "${GCS_BENCHMARK_BUCKET}" ]]; then
        echo "dry-run gcs target (${arm_name}): gs://${GCS_BENCHMARK_BUCKET}/${dry_folder}/" >&2
      else
        echo "dry-run gcs folder (${arm_name}): ${dry_folder}" >&2
      fi
    fi
  done
  exit 0
fi

if [[ "${EXECUTION}" == "in-cluster" && ( "${ARTIFACT_DIR}" == "${ROOT}" || "${ARTIFACT_DIR}" == "${ROOT}"/* ) ]]; then
  echo "ARTIFACT_DIR (${ARTIFACT_DIR}) must be outside the repository (${ROOT}) when EXECUTION=in-cluster" >&2
  exit 2
fi

if [[ "${EXECUTION}" == "local" ]] && (( HAS_PYTHON == 1 )); then
  if [[ "${ACTION}" != "prewarm" || "${CLIENTS[0]}" == "python" ]]; then
    if ! "${PYTHON_BIN}" - "${ROOT}/benchmarking/swebench/run_fleet_bench.py" <<'PY'; then
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("run_fleet_bench", sys.argv[1])
assert spec is not None and spec.loader is not None
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.load_sdk()
PY
      echo "local Python SDK pre-flight failed with PYTHON_BIN='${PYTHON_BIN}'; pass --python-bin /path/to/venv/bin/python3 with grpcio>=1.84 and protobuf>=7.35 (e.g., pip install -e ./clients/python)" >&2
      exit 2
    fi
  fi
fi

mkdir -p "${ARTIFACT_DIR}/events" "${ARTIFACT_DIR}/prewarm"

KODATA="${ROOT}/cmd/benchmarking/swebench-loader/kodata"
SWE_JOB=""
R2E_JOB=""
DIRECT_CREDS_SECRET=""
HARVEST_DIR=""
HARVEST_PID=""
HARVEST_ACTIVE=0
LOCAL_PIDS=()
LOG_PIDS=()
KUBECTL_ERR=""
PY_BUILD_DIR=""
BUILT_GO_IMAGE=""
CACHED_BAKED_SWE_TRAJ=""
CACHED_BAKED_R2E_TRAJ=""
BUILT_PYTHON_IMAGE="${PYTHON_LOADER_IMAGE}"

# shellcheck disable=SC2317,SC2329 # invoked by the EXIT/INT/TERM trap and per-arm cleanup
cleanup_cluster() {
  local pid
  for pid in ${LOCAL_PIDS[@]+"${LOCAL_PIDS[@]}"} ${LOG_PIDS[@]+"${LOG_PIDS[@]}"}; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" 2>/dev/null || true
    fi
  done
  LOCAL_PIDS=()
  LOG_PIDS=()
  if [[ -n "${SWE_JOB}" || -n "${R2E_JOB}" ]]; then
    kc -n "${NAMESPACE}" delete job ${SWE_JOB:+"${SWE_JOB}"} ${R2E_JOB:+"${R2E_JOB}"} --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [[ -n "${DIRECT_CREDS_SECRET}" ]]; then
    kc -n "${NAMESPACE}" delete secret "${DIRECT_CREDS_SECRET}" --ignore-not-found >/dev/null 2>&1 || true
    DIRECT_CREDS_SECRET=""
  fi
  if [[ -n "${HARVEST_PID}" ]]; then
    kill "${HARVEST_PID}" 2>/dev/null || true
    HARVEST_PID=""
  fi
  rm -rf "${KODATA}"
  if [[ -n "${HARVEST_DIR}" ]]; then
    rm -rf "${HARVEST_DIR}"
    HARVEST_DIR=""
  fi
  if [[ -n "${KUBECTL_ERR}" ]]; then
    rm -f "${KUBECTL_ERR}"
    KUBECTL_ERR=""
  fi
  if [[ -n "${PY_BUILD_DIR}" ]]; then
    rm -rf "${PY_BUILD_DIR}"
    PY_BUILD_DIR=""
  fi
  bench_stop_measure_loop ""
}

# shellcheck disable=SC2317,SC2329 # invoked by the EXIT trap
cleanup_all() {
  cleanup_cluster
  bench_cleanup_telemetry
}

trap cleanup_all EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

TRAJ_N=0
BAKED=""
bake_trajectory() {
  local -a parts out=()
  local el path dir base assets asset
  IFS=',' read -r -a parts <<< "$1"
  for el in "${parts[@]}"; do
    path="${el}"
    [[ "${path}" == /* ]] || path="${INVOKED_DIR}/${path}"
    if [[ -f "${path}" ]]; then
      dir="$(cd "$(dirname "${path}")" && pwd)"
      base="$(basename "${path}")"
      TRAJ_N=$((TRAJ_N + 1))
      mkdir -p "${KODATA}/traj/${TRAJ_N}"
      cp "${dir}/${base}" "${KODATA}/traj/${TRAJ_N}/"
      assets="$(go run ./cmd/benchmarking/swebench-loader trajectory-assets "${dir}/${base}")" || exit 2
      while IFS= read -r asset; do
        [[ -n "${asset}" ]] || continue
        case "${asset}" in
          */*|..) echo "${base}: write.file ${asset} must be a plain file name" >&2; exit 2 ;;
        esac
        [[ -f "${dir}/${asset}" && ! -L "${dir}/${asset}" ]] || {
          echo "${base}: write.file ${asset} is not a regular file next to the trajectory" >&2
          exit 2
        }
        cp "${dir}/${asset}" "${KODATA}/traj/${TRAJ_N}/${asset}"
      done <<< "${assets}"
      el="/var/run/ko/traj/${TRAJ_N}/${base}"
    fi
    out+=("${el}")
  done
  local IFS=','
  BAKED="${out[*]}"
}

ensure_go_loader_image() {
  local swe_traj="$1"
  local r2e_traj="$2"
  if [[ -n "${BUILT_GO_IMAGE}" ]]; then
    return 0
  fi
  : "${KO_DOCKER_REPO:?set KO_DOCKER_REPO to the registry path the cluster can pull from}"
  rm -rf "${KODATA}" && mkdir -p "${KODATA}"
  TRAJ_N=0
  cp "${SWE_DATASET}" "${KODATA}/swe-dataset.jsonl"
  cp "${R2E_DATASET}" "${KODATA}/r2e-dataset.jsonl"
  if [[ -n "${SWE_EXCLUDE_FILE}" ]]; then
    cp "${SWE_EXCLUDE_FILE}" "${KODATA}/swe-exclude.txt"
  fi
  if [[ -n "${R2E_EXCLUDE_FILE}" ]]; then
    cp "${R2E_EXCLUDE_FILE}" "${KODATA}/r2e-exclude.txt"
  fi
  bake_trajectory "${swe_traj}"
  CACHED_BAKED_SWE_TRAJ="${BAKED}"
  bake_trajectory "${r2e_traj}"
  CACHED_BAKED_R2E_TRAJ="${BAKED}"

  BUILT_GO_IMAGE="$(KO_DOCKER_REPO="${KO_DOCKER_REPO}/swebench-loader" ko build --bare ./cmd/benchmarking/swebench-loader)"
  rm -rf "${KODATA}"
  echo "go loader image ${BUILT_GO_IMAGE}"
}

ensure_python_loader_image() {
  if [[ -n "${BUILT_PYTHON_IMAGE}" ]]; then
    return 0
  fi
  : "${KO_DOCKER_REPO:?set KO_DOCKER_REPO (or --python-image) so the cluster can pull the Python fleet loader image}"
  command -v docker >/dev/null || { echo "docker is required to build the Python fleet loader image (or pass --python-image)" >&2; exit 2; }
  PY_BUILD_DIR="$(mktemp -d)"
  mkdir -p "${PY_BUILD_DIR}/clients/python"
  cp -R "${ROOT}/clients/python/src" "${PY_BUILD_DIR}/clients/python/src"
  cp "${ROOT}/benchmarking/swebench/run_fleet_bench.py" "${PY_BUILD_DIR}/run_fleet_bench.py"
  cat > "${PY_BUILD_DIR}/Dockerfile" <<'EOF_PY_DOCKER'
FROM python:3.12-slim
RUN pip install --no-cache-dir "grpcio>=1.84" "protobuf>=7.35"
WORKDIR /app
COPY clients/python/src /app/clients/python/src
COPY run_fleet_bench.py /app/run_fleet_bench.py
ENV PYTHONPATH=/app/clients/python/src
USER 65532:65532
ENTRYPOINT ["python3", "/app/run_fleet_bench.py"]
EOF_PY_DOCKER
  local tag
  tag="${KO_DOCKER_REPO}/fleet-bench-loader:${RUN_ID}-$(date +%s)"
  docker build -t "${tag}" "${PY_BUILD_DIR}"
  docker push "${tag}"
  rm -rf "${PY_BUILD_DIR}"
  PY_BUILD_DIR=""
  BUILT_PYTHON_IMAGE="${tag}"
  echo "python loader image ${BUILT_PYTHON_IMAGE}"
}

# wait_for_pod prints the name of the Job's pod once its loader container has
# started (running, or already terminated) and its keep sidecar is running:
# the plan copy and the event copy go through keep, and a keep that is still
# ContainerCreating fails them.
wait_for_pod() {
  local job_name="$1"
  local pod=""
  local states=""
  local deadline=$((SECONDS + START_TIMEOUT))
  while (( SECONDS < deadline )); do
    pod="$(kc -n "${NAMESPACE}" get pod -l "job-name=${job_name}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [[ -n "${pod}" ]]; then
      states="$(kc -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{range .status.containerStatuses[*]}{.name}={.state}{"\n"}{end}' 2>/dev/null || true)"
      if grep -q '^loader=.*\(running\|terminated\)' <<<"${states}" && grep -q '^keep=.*running' <<<"${states}"; then
        printf '%s\n' "${pod}"
        return 0
      fi
    fi
    sleep 5
  done
  echo "loader and keep containers of ${job_name} did not start within ${START_TIMEOUT}s" >&2
  if [[ -n "${pod}" ]]; then
    kc -n "${NAMESPACE}" describe pod "${pod}" | tail -15 >&2 || true
  else
    kc -n "${NAMESPACE}" describe job "${job_name}" | tail -15 >&2 || true
  fi
  return 1
}

wait_for_exit() {
  local pod="$1"
  local exit_code=""
  local phase=""
  local api_errors=0
  until exit_code="$(kc -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.status.containerStatuses[?(@.name=="loader")].state.terminated.exitCode}' 2>/dev/null)" && [[ -n "${exit_code}" ]]; do
    if phase="$(kc -n "${NAMESPACE}" get pod "${pod}" -o jsonpath='{.status.phase}' 2>"${KUBECTL_ERR}")"; then
      api_errors=0
    elif grep -q 'NotFound' "${KUBECTL_ERR}"; then
      phase="gone"
    else
      api_errors=$((api_errors + 1))
      echo "kubectl get pod ${pod} failed (${api_errors}/${MAX_API_ERRORS}): $(head -c 300 "${KUBECTL_ERR}")" >&2
      if (( api_errors >= MAX_API_ERRORS )); then
        return 1
      fi
      sleep 10
      continue
    fi
    if [[ "${phase}" == "gone" || "${phase}" == "Failed" ]]; then
      echo "pod ${pod} is ${phase} before reporting an exit code" >&2
      return 1
    fi
    sleep 10
  done
  printf '%s\n' "${exit_code}"
}

apply_loader_job() {
  local job_name="$1"
  local image="$2"
  local args_json="$3"
  local command_json="$4"
  local creds_secret="$5"
  local loader_env=""
  local cmd_line=""
  local extra_volumes=""
  local extra_mounts=""
  local job_json ns_json image_json auth_sec_json creds_sec_json

  job_json="$(json_encode_str "${job_name}")"
  ns_json="$(json_encode_str "${NAMESPACE}")"
  image_json="$(json_encode_str "${image}")"

  local -a env_lines=()
  if [[ -n "${AUTH_TOKEN_SECRET}" ]]; then
    auth_sec_json="$(json_encode_str "${AUTH_TOKEN_SECRET}")"
    env_lines+=("            - name: SUBSTRATE_ENV_AUTH_TOKEN
              valueFrom:
                secretKeyRef:
                  name: ${auth_sec_json}
                  key: token")
  fi
  if [[ "${ENABLE_TRACING}" == "1" && -n "${OTEL_EXPORTER_OTLP_ENDPOINT}" ]]; then
    local otlp_ep_json
    otlp_ep_json="$(json_encode_str "${OTEL_EXPORTER_OTLP_ENDPOINT}")"
    env_lines+=("            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: ${otlp_ep_json}")
    if [[ -n "${OTEL_EXPORTER_OTLP_INSECURE:-}" ]]; then
      local otlp_insec_json
      otlp_insec_json="$(json_encode_str "${OTEL_EXPORTER_OTLP_INSECURE}")"
      env_lines+=("            - name: OTEL_EXPORTER_OTLP_INSECURE
              value: ${otlp_insec_json}")
    fi
  fi
  if (( ${#env_lines[@]} > 0 )); then
    local IFS=$'\n'
    loader_env="env:
${env_lines[*]}"
  fi
  if [[ -n "${command_json}" ]]; then
    cmd_line="command: ${command_json}"
  fi
  if [[ -n "${creds_secret}" ]]; then
    creds_sec_json="$(json_encode_str "${creds_secret}")"
    extra_volumes="        - name: direct-creds
          secret:
            secretName: ${creds_sec_json}
            defaultMode: 256"
    extra_mounts="            - name: direct-creds
              mountPath: /var/run/ate-direct-creds
              readOnly: true"
  fi

  kc -n "${NAMESPACE}" delete job "${job_name}" --ignore-not-found --wait=true >/dev/null
  cat <<EOF_JOB | kc apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_json}
  namespace: ${ns_json}
spec:
  backoffLimit: 0
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
${extra_volumes}
      containers:
        - name: loader
          image: ${image_json}
          ${cmd_line}
          args: ${args_json}
          ${loader_env}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: work
              mountPath: /work
${extra_mounts}
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
}

harvest_both_parts() {
  local swe_run="$1"
  local r2e_run="$2"
  local tag="$3"
  local harvest="${ROOT}/benchmarking/swebench/harvest-restore-timings.sh"
  local window="$(( 2 * HARVEST_INTERVAL_MIN ))m"
  NAMESPACE=ate-system CONTEXT="${CONTEXT}" SINCE="${window}" "${harvest}" "${swe_run}" "${HARVEST_DIR}/swe-${tag}.tmp" >&2 || true
  if [[ -s "${HARVEST_DIR}/swe-${tag}.tmp" ]]; then
    mv "${HARVEST_DIR}/swe-${tag}.tmp" "${HARVEST_DIR}/part-swe-${tag}.jsonl"
  fi
  NAMESPACE=ate-system CONTEXT="${CONTEXT}" SINCE="${window}" "${harvest}" "${r2e_run}" "${HARVEST_DIR}/r2e-${tag}.tmp" >&2 || true
  if [[ -s "${HARVEST_DIR}/r2e-${tag}.tmp" ]]; then
    mv "${HARVEST_DIR}/r2e-${tag}.tmp" "${HARVEST_DIR}/part-r2e-${tag}.jsonl"
  fi
}

# start_restore_harvest begins the periodic harvest of atelet's restore
# records for a timed go or go-direct arm when HARVEST_RESTORES=1, in local
# and in-cluster execution alike: the harvester reads kubectl logs from the
# host. Python arms are skipped, because SandboxFleet names its actors warm-*
# and sb-*, not <run_id>-*, and the harvester selects by run id.
start_restore_harvest() {
  local arm_client="$1"
  local arm_name="$2"
  local is_timed_run="$3"
  local swe_run="$4"
  local r2e_run="$5"
  HARVEST_ACTIVE=0
  if [[ "${is_timed_run}" != "1" || "${HARVEST_RESTORES}" != "1" ]]; then
    return 0
  fi
  if [[ "${arm_client}" == "python" ]]; then
    echo "note: skipping HARVEST_RESTORES for python arm ${arm_name} (Python SandboxFleet actors use warm-*/sb-* IDs rather than <run_id>-*)" >&2
    return 0
  fi
  HARVEST_DIR="$(mktemp -d)"
  (
    n=0
    while :; do
      sleep "$(( HARVEST_INTERVAL_MIN * 60 ))" || true
      n=$(( n + 1 ))
      harvest_both_parts "${swe_run}" "${r2e_run}" "${n}"
    done
  ) &
  HARVEST_PID=$!
  HARVEST_ACTIVE=1
}

# finish_restore_harvest stops the harvester, takes a last sample and writes
# the raw atelet records to ARM_DIR/atelet_restores.jsonl. They are not
# schema.RestoreSample records, which is what bench measure, bench report
# -dest and bench validate read from restore_timings.jsonl, so they do not go
# under that name; swebench-loader report -restore-log joins the raw records
# with the events itself (generate_arm_reports).
finish_restore_harvest() {
  local arm_dir="$1"
  local swe_run="$2"
  local r2e_run="$3"
  if (( HARVEST_ACTIVE != 1 )); then
    return 0
  fi
  kill "${HARVEST_PID}" 2>/dev/null || true
  wait "${HARVEST_PID}" 2>/dev/null || true
  HARVEST_PID=""
  harvest_both_parts "${swe_run}" "${r2e_run}" final
  local -a parts=()
  shopt -s nullglob
  parts=("${HARVEST_DIR}"/part-*.jsonl)
  shopt -u nullglob
  if (( ${#parts[@]} > 0 )); then
    sort -u "${parts[@]}" > "${arm_dir}/atelet_restores.jsonl"
  else
    : > "${arm_dir}/atelet_restores.jsonl"
  fi
  rm -rf "${HARVEST_DIR}"
  HARVEST_DIR=""
  HARVEST_ACTIVE=0
}

# stage_python_plan copies a rendered plan into the pod through the keep
# sidecar and releases the loader, which waits for /work/plan.ready.
stage_python_plan() {
  local pod="$1"
  local plan="$2"
  kc -n "${NAMESPACE}" cp -c keep "${plan}" "${pod}:/work/plan.json"
  kc -n "${NAMESPACE}" exec -c keep "${pod}" -- touch /work/plan.ready
}

SHARD_EXIT=0

run_local_arm_shards() {
  local arm_client="$1"
  local arm_strategy="$2"
  local arm_name="$3"
  local cur_mode="$4"
  local swe_run="$5"
  local r2e_run="$6"
  local swe_width="$7"
  local r2e_width="$8"
  local swe_fanout="$9"
  local r2e_fanout="${10}"
  local swe_traj="${11}"
  local r2e_traj="${12}"
  local swe_out="${13}"
  local r2e_out="${14}"
  local arm_dir="${15}"
  local pass_extra="${16}"
  local is_timed_run="${17}"

  mkdir -p "$(dirname "${swe_out}")" "$(dirname "${r2e_out}")"
  local swe_pid r2e_pid swe_rc=0 r2e_rc=0

  if [[ "${arm_client}" == "python" ]]; then
    render_python_plans "${arm_dir}/plans" "${swe_traj}" "${r2e_traj}" "${swe_fanout}" "${r2e_fanout}"
    mode_args_for_python "${cur_mode}" "${swe_fanout}" "${arm_strategy}"
    local -a swe_py_mode=("${PY_MODE_ARGS[@]}")
    mode_args_for_python "${cur_mode}" "${r2e_fanout}" "${arm_strategy}"
    local -a r2e_py_mode=("${PY_MODE_ARGS[@]}")
    arm_label_args_python "${arm_client}" "${arm_name}"

    local -a swe_cmd=(
      "${PYTHON_BIN}" "${ROOT}/benchmarking/swebench/run_fleet_bench.py"
      "--endpoint=${ENDPOINT}"
      "--rendered-plan=${arm_dir}/plans/swe-plan.json"
      "--run-id=${swe_run}"
      "--width=${swe_width}"
      "--out=${swe_out}"
      "--worker=swe"
      "--job=swe"
      "--connections=${CONNECTIONS}"
      "--atespace=${ATESPACE}"
      "--base-template=${BASE_TEMPLATE}"
      "--wake-budget=${WAKE_BUDGET}"
      "--create-timeout=${CREATE_TIMEOUT}"
      "--step-timeout=${STEP_TIMEOUT}"
      "--fleet-report-out=${arm_dir}/swe-fleet"
      "${swe_py_mode[@]}"
      ${PY_LABEL_ARGS[@]+"${PY_LABEL_ARGS[@]}"}
    )
    if [[ -n "${AUTH_TOKEN_FILE}" ]]; then
      swe_cmd+=("--auth-token-file=${AUTH_TOKEN_FILE}")
    fi
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      swe_cmd+=("--enable-tracing" "--trace-out=${arm_dir}/traces/swe.jsonl")
    fi
    if [[ "${pass_extra}" == "1" ]]; then
      swe_cmd+=(${PY_EXTRA_SET_ARGS[@]+"${PY_EXTRA_SET_ARGS[@]}"} ${PY_EXTRA_LABEL_ARGS[@]+"${PY_EXTRA_LABEL_ARGS[@]}"})
    fi

    local -a r2e_cmd=(
      "${PYTHON_BIN}" "${ROOT}/benchmarking/swebench/run_fleet_bench.py"
      "--endpoint=${ENDPOINT}"
      "--rendered-plan=${arm_dir}/plans/r2e-plan.json"
      "--run-id=${r2e_run}"
      "--width=${r2e_width}"
      "--out=${r2e_out}"
      "--worker=r2e"
      "--job=r2e"
      "--connections=${CONNECTIONS}"
      "--atespace=${ATESPACE}"
      "--base-template=${BASE_TEMPLATE}"
      "--wake-budget=${WAKE_BUDGET}"
      "--create-timeout=${CREATE_TIMEOUT}"
      "--step-timeout=${STEP_TIMEOUT}"
      "--fleet-report-out=${arm_dir}/r2e-fleet"
      "${r2e_py_mode[@]}"
      ${PY_LABEL_ARGS[@]+"${PY_LABEL_ARGS[@]}"}
    )
    if [[ -n "${AUTH_TOKEN_FILE}" ]]; then
      r2e_cmd+=("--auth-token-file=${AUTH_TOKEN_FILE}")
    fi
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      r2e_cmd+=("--enable-tracing" "--trace-out=${arm_dir}/traces/r2e.jsonl")
    fi
    if [[ "${pass_extra}" == "1" ]]; then
      r2e_cmd+=(${PY_EXTRA_SET_ARGS[@]+"${PY_EXTRA_SET_ARGS[@]}"} ${PY_EXTRA_LABEL_ARGS[@]+"${PY_EXTRA_LABEL_ARGS[@]}"})
    fi

    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" && -n "${OTEL_EXPORTER_OTLP_ENDPOINT}" ]]; then
      OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT}" "${swe_cmd[@]}" &
      swe_pid=$!
      OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT}" "${r2e_cmd[@]}" &
      r2e_pid=$!
    else
      "${swe_cmd[@]}" &
      swe_pid=$!
      "${r2e_cmd[@]}" &
      r2e_pid=$!
    fi
  else
    go_backend_args_local "${arm_client}"
    arm_label_args_go "${arm_client}" "${arm_name}"

    mode_args_for_go "${cur_mode}" "${swe_fanout}" "${SWE_IMAGES}" "${swe_width}" "${arm_strategy}"
    local -a swe_cmd=(
      go run ./cmd/benchmarking/swebench-loader run
      "${GO_BACKEND_ARGS[@]}"
      "-dataset=${SWE_DATASET}"
      "-run-id=${swe_run}"
      "-images=${SWE_IMAGES}"
      "-width=${swe_width}"
      "-trajectory=${swe_traj}"
      "-out=${swe_out}"
      "-worker=swe"
      "-job=swe"
      "-connections=${CONNECTIONS}"
      "-atespace=${ATESPACE}"
      "-base-template=${BASE_TEMPLATE}"
      "-wake-budget=${WAKE_BUDGET}"
      "-create-timeout=${CREATE_TIMEOUT}"
      "-step-timeout=${STEP_TIMEOUT}"
      "${MODE_ARGS[@]}"
      ${GO_LABEL_ARGS[@]+"${GO_LABEL_ARGS[@]}"}
    )
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      swe_cmd+=("-enable-tracing")
    fi
    if [[ -n "${SWE_EXCLUDE_FILE}" ]]; then
      swe_cmd+=("-exclude-file=${SWE_EXCLUDE_FILE}")
    fi
    if [[ "${pass_extra}" == "1" ]] && (( ${#EXTRA_ARGS[@]} > 0 )); then
      swe_cmd+=("${EXTRA_ARGS[@]}")
    fi

    mode_args_for_go "${cur_mode}" "${r2e_fanout}" "${R2E_IMAGES}" "${r2e_width}" "${arm_strategy}"
    local -a r2e_cmd=(
      go run ./cmd/benchmarking/swebench-loader run
      "${GO_BACKEND_ARGS[@]}"
      "-dataset=${R2E_DATASET}"
      "-run-id=${r2e_run}"
      "-images=${R2E_IMAGES}"
      "-width=${r2e_width}"
      "-trajectory=${r2e_traj}"
      "-out=${r2e_out}"
      "-worker=r2e"
      "-job=r2e"
      "-connections=${CONNECTIONS}"
      "-atespace=${ATESPACE}"
      "-base-template=${BASE_TEMPLATE}"
      "-wake-budget=${WAKE_BUDGET}"
      "-create-timeout=${CREATE_TIMEOUT}"
      "-step-timeout=${STEP_TIMEOUT}"
      "${MODE_ARGS[@]}"
      ${GO_LABEL_ARGS[@]+"${GO_LABEL_ARGS[@]}"}
    )
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      r2e_cmd+=("-enable-tracing")
    fi
    if [[ -n "${R2E_EXCLUDE_FILE}" ]]; then
      r2e_cmd+=("-exclude-file=${R2E_EXCLUDE_FILE}")
    fi
    if [[ "${pass_extra}" == "1" ]] && (( ${#EXTRA_ARGS[@]} > 0 )); then
      r2e_cmd+=("${EXTRA_ARGS[@]}")
    fi

    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" && -n "${OTEL_EXPORTER_OTLP_ENDPOINT}" ]]; then
      OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT}" "${swe_cmd[@]}" &
      swe_pid=$!
      OTEL_EXPORTER_OTLP_ENDPOINT="${OTEL_EXPORTER_OTLP_ENDPOINT}" "${r2e_cmd[@]}" &
      r2e_pid=$!
    else
      "${swe_cmd[@]}" &
      swe_pid=$!
      "${r2e_cmd[@]}" &
      r2e_pid=$!
    fi
  fi

  LOCAL_PIDS=("${swe_pid}" "${r2e_pid}")
  start_restore_harvest "${arm_client}" "${arm_name}" "${is_timed_run}" "${swe_run}" "${r2e_run}"
  if [[ "${is_timed_run}" == "1" ]] && (( MEASURE_INTERVAL_SEC > 0 )); then
    bench_start_measure_loop "${ROOT}" "${CONTEXT}" "${arm_dir}" "${swe_run%-swe}" "${ATESPACE}" "${BASE_TEMPLATE}" "${MEASURE_INTERVAL_SEC}" "${ATE_ENDPOINT}" "${PROM_URL}"
  fi
  wait "${swe_pid}" || swe_rc=$?
  wait "${r2e_pid}" || r2e_rc=$?
  LOCAL_PIDS=()
  bench_stop_measure_loop "${arm_dir}"
  finish_restore_harvest "${arm_dir}" "${swe_run}" "${r2e_run}"
  SHARD_EXIT=$(( swe_rc != 0 ? swe_rc : r2e_rc ))
}

run_in_cluster_arm_shards() {
  local arm_client="$1"
  local arm_strategy="$2"
  local arm_name="$3"
  local cur_mode="$4"
  local swe_run="$5"
  local r2e_run="$6"
  local swe_width="$7"
  local r2e_width="$8"
  local swe_fanout="$9"
  local r2e_fanout="${10}"
  local swe_traj="${11}"
  local r2e_traj="${12}"
  local swe_out="${13}"
  local r2e_out="${14}"
  local arm_dir="${15}"
  local pass_extra="${16}"
  local is_timed_run="${17}"

  command -v python3 >/dev/null || { echo "python3 is required to encode Job args" >&2; exit 2; }
  echo "kubectl context ${CONTEXT:-$(kubectl config current-context 2>/dev/null || echo unknown)}"
  mkdir -p "$(dirname "${swe_out}")" "$(dirname "${r2e_out}")" "${arm_dir}"

  SWE_JOB="swebench-loader-${swe_run}"
  R2E_JOB="swebench-loader-${r2e_run}"
  KUBECTL_ERR="$(mktemp)"

  local image=""
  local command_json=""
  local creds_secret=""
  local swe_json=""
  local r2e_json=""

  if [[ "${arm_client}" == "python" ]]; then
    ensure_python_loader_image
    image="${BUILT_PYTHON_IMAGE}"
    render_python_plans "${arm_dir}/plans" "${swe_traj}" "${r2e_traj}" "${swe_fanout}" "${r2e_fanout}"

    mode_args_for_python "${cur_mode}" "${swe_fanout}" "${arm_strategy}"
    local -a swe_py_mode=("${PY_MODE_ARGS[@]}")
    mode_args_for_python "${cur_mode}" "${r2e_fanout}" "${arm_strategy}"
    local -a r2e_py_mode=("${PY_MODE_ARGS[@]}")
    arm_label_args_python "${arm_client}" "${arm_name}"

    local -a swe_args=(
      "--endpoint=${API_SERVICE}.${NAMESPACE}.svc:7777"
      "--rendered-plan=/work/plan.json"
      "--run-id=${swe_run}"
      "--width=${swe_width}"
      "--out=/work/swe.jsonl"
      "--worker=swe"
      "--job=swe"
      "--connections=${CONNECTIONS}"
      "--atespace=${ATESPACE}"
      "--base-template=${BASE_TEMPLATE}"
      "--wake-budget=${WAKE_BUDGET}"
      "--create-timeout=${CREATE_TIMEOUT}"
      "--step-timeout=${STEP_TIMEOUT}"
      "${swe_py_mode[@]}"
      ${PY_LABEL_ARGS[@]+"${PY_LABEL_ARGS[@]}"}
    )
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      swe_args+=("--enable-tracing" "--trace-out=/work/traces/swe.jsonl")
    fi
    if [[ "${pass_extra}" == "1" ]]; then
      swe_args+=(${PY_EXTRA_SET_ARGS[@]+"${PY_EXTRA_SET_ARGS[@]}"} ${PY_EXTRA_LABEL_ARGS[@]+"${PY_EXTRA_LABEL_ARGS[@]}"})
    fi

    local -a r2e_args=(
      "--endpoint=${API_SERVICE}.${NAMESPACE}.svc:7777"
      "--rendered-plan=/work/plan.json"
      "--run-id=${r2e_run}"
      "--width=${r2e_width}"
      "--out=/work/r2e.jsonl"
      "--worker=r2e"
      "--job=r2e"
      "--connections=${CONNECTIONS}"
      "--atespace=${ATESPACE}"
      "--base-template=${BASE_TEMPLATE}"
      "--wake-budget=${WAKE_BUDGET}"
      "--create-timeout=${CREATE_TIMEOUT}"
      "--step-timeout=${STEP_TIMEOUT}"
      "${r2e_py_mode[@]}"
      ${PY_LABEL_ARGS[@]+"${PY_LABEL_ARGS[@]}"}
    )
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      r2e_args+=("--enable-tracing" "--trace-out=/work/traces/r2e.jsonl")
    fi
    if [[ "${pass_extra}" == "1" ]]; then
      r2e_args+=(${PY_EXTRA_SET_ARGS[@]+"${PY_EXTRA_SET_ARGS[@]}"} ${PY_EXTRA_LABEL_ARGS[@]+"${PY_EXTRA_LABEL_ARGS[@]}"})
    fi

    # The loader waits for the plan that stage_python_plan copies in after
    # wait_for_pod, up to START_TIMEOUT (the same bound as wait_for_pod, which
    # the other shard's pod may use up first).
    # shellcheck disable=SC2016 # $n and $@ expand inside the container shell
    command_json="$(json_encode_args sh -c 'n=0; while [ ! -f /work/plan.ready ]; do n=$((n+1)); if [ "$n" -ge '"$(( START_TIMEOUT * 2 ))"' ]; then echo "timed out waiting for /work/plan.ready" >&2; exit 2; fi; sleep 0.5; done; exec python3 /app/run_fleet_bench.py "$@"' --)"
    swe_json="$(json_encode_args "${swe_args[@]}")"
    r2e_json="$(json_encode_args "${r2e_args[@]}")"
  else
    ensure_go_loader_image "${swe_traj}" "${r2e_traj}"
    image="${BUILT_GO_IMAGE}"
    arm_label_args_go "${arm_client}" "${arm_name}"

    local -a cluster_backend_args=("-backend=substrate")
    if [[ "${arm_client}" == "go-direct" ]]; then
      cluster_backend_args+=("-access=direct" "-control-addr=${CONTROL_ADDR}")
      if [[ -n "${ROUTER_ADDR}" ]]; then
        cluster_backend_args+=("-router-addr=${ROUTER_ADDR}")
      fi
      if [[ -n "${ATENET_SERVER_NAME}" ]]; then
        cluster_backend_args+=("-atenet-server-name=${ATENET_SERVER_NAME}")
      fi
      local -a sec_from_files=()
      if [[ -n "${ATENET_CLIENT_CERT}" ]]; then
        sec_from_files+=("--from-file=client.pem=${ATENET_CLIENT_CERT}")
        cluster_backend_args+=("-atenet-client-cert=/var/run/ate-direct-creds/client.pem")
      fi
      if [[ -n "${ATENET_CA_FILE}" ]]; then
        sec_from_files+=("--from-file=ca.pem=${ATENET_CA_FILE}")
        cluster_backend_args+=("-atenet-ca-file=/var/run/ate-direct-creds/ca.pem")
      fi
      if [[ -n "${ATEAPI_TOKEN_FILE}" ]]; then
        sec_from_files+=("--from-file=token=${ATEAPI_TOKEN_FILE}")
        cluster_backend_args+=("-ateapi-token-file=/var/run/ate-direct-creds/token")
      fi
      if [[ -n "${ATEAPI_CA_FILE}" ]]; then
        sec_from_files+=("--from-file=ateapi-ca.pem=${ATEAPI_CA_FILE}")
        cluster_backend_args+=("-ateapi-ca-file=/var/run/ate-direct-creds/ateapi-ca.pem")
      fi
      if (( ${#sec_from_files[@]} > 0 )); then
        DIRECT_CREDS_SECRET="swebench-direct-${swe_run}"
        kc -n "${NAMESPACE}" delete secret "${DIRECT_CREDS_SECRET}" --ignore-not-found >/dev/null
        kc -n "${NAMESPACE}" create secret generic "${DIRECT_CREDS_SECRET}" "${sec_from_files[@]}" >/dev/null
        creds_secret="${DIRECT_CREDS_SECRET}"
      fi
    else
      cluster_backend_args+=("-access=ate-env-api" "-endpoint=${API_SERVICE}.${NAMESPACE}.svc:7777")
    fi

    mode_args_for_go "${cur_mode}" "${swe_fanout}" "${SWE_IMAGES}" "${swe_width}" "${arm_strategy}"
    local -a swe_args=(
      run
      "${cluster_backend_args[@]}"
      "-dataset=/var/run/ko/swe-dataset.jsonl"
      "-run-id=${swe_run}"
      "-images=${SWE_IMAGES}"
      "-width=${swe_width}"
      "-trajectory=${CACHED_BAKED_SWE_TRAJ}"
      "-out=/work/swe.jsonl"
      "-worker=swe"
      "-job=swe"
      "-connections=${CONNECTIONS}"
      "-atespace=${ATESPACE}"
      "-base-template=${BASE_TEMPLATE}"
      "-wake-budget=${WAKE_BUDGET}"
      "-create-timeout=${CREATE_TIMEOUT}"
      "-step-timeout=${STEP_TIMEOUT}"
      "${MODE_ARGS[@]}"
      ${GO_LABEL_ARGS[@]+"${GO_LABEL_ARGS[@]}"}
    )
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      swe_args+=("-enable-tracing")
    fi
    if [[ -n "${SWE_EXCLUDE_FILE}" ]]; then
      swe_args+=("-exclude-file=/var/run/ko/swe-exclude.txt")
    fi
    if [[ "${pass_extra}" == "1" ]] && (( ${#EXTRA_ARGS[@]} > 0 )); then
      swe_args+=("${EXTRA_ARGS[@]}")
    fi

    mode_args_for_go "${cur_mode}" "${r2e_fanout}" "${R2E_IMAGES}" "${r2e_width}" "${arm_strategy}"
    local -a r2e_args=(
      run
      "${cluster_backend_args[@]}"
      "-dataset=/var/run/ko/r2e-dataset.jsonl"
      "-run-id=${r2e_run}"
      "-images=${R2E_IMAGES}"
      "-width=${r2e_width}"
      "-trajectory=${CACHED_BAKED_R2E_TRAJ}"
      "-out=/work/r2e.jsonl"
      "-worker=r2e"
      "-job=r2e"
      "-connections=${CONNECTIONS}"
      "-atespace=${ATESPACE}"
      "-base-template=${BASE_TEMPLATE}"
      "-wake-budget=${WAKE_BUDGET}"
      "-create-timeout=${CREATE_TIMEOUT}"
      "-step-timeout=${STEP_TIMEOUT}"
      "${MODE_ARGS[@]}"
      ${GO_LABEL_ARGS[@]+"${GO_LABEL_ARGS[@]}"}
    )
    if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" ]]; then
      r2e_args+=("-enable-tracing")
    fi
    if [[ -n "${R2E_EXCLUDE_FILE}" ]]; then
      r2e_args+=("-exclude-file=/var/run/ko/r2e-exclude.txt")
    fi
    if [[ "${pass_extra}" == "1" ]] && (( ${#EXTRA_ARGS[@]} > 0 )); then
      r2e_args+=("${EXTRA_ARGS[@]}")
    fi

    swe_json="$(json_encode_args "${swe_args[@]}")"
    r2e_json="$(json_encode_args "${r2e_args[@]}")"
  fi

  apply_loader_job "${SWE_JOB}" "${image}" "${swe_json}" "${command_json}" "${creds_secret}"
  apply_loader_job "${R2E_JOB}" "${image}" "${r2e_json}" "${command_json}" "${creds_secret}"

  # Each plan goes in as soon as its own pod is up: the loader's wait for it
  # must not also cover the other pod's start.
  local swe_pod r2e_pod
  swe_pod="$(wait_for_pod "${SWE_JOB}")"
  if [[ "${arm_client}" == "python" ]]; then
    stage_python_plan "${swe_pod}" "${arm_dir}/plans/swe-plan.json"
  fi
  r2e_pod="$(wait_for_pod "${R2E_JOB}")"
  if [[ "${arm_client}" == "python" ]]; then
    stage_python_plan "${r2e_pod}" "${arm_dir}/plans/r2e-plan.json"
  fi
  echo "started pods: swe=${swe_pod} r2e=${r2e_pod}"

  start_restore_harvest "${arm_client}" "${arm_name}" "${is_timed_run}" "${swe_run}" "${r2e_run}"
  if [[ "${is_timed_run}" == "1" ]] && (( MEASURE_INTERVAL_SEC > 0 )); then
    bench_start_measure_loop "${ROOT}" "${CONTEXT}" "${arm_dir}" "${swe_run%-swe}" "${ATESPACE}" "${BASE_TEMPLATE}" "${MEASURE_INTERVAL_SEC}" "${ATE_ENDPOINT}" "${PROM_URL}"
  fi

  kc -n "${NAMESPACE}" logs -f "${swe_pod}" -c loader > "${arm_dir}/swe-loader.log" 2>&1 &
  local log_swe_pid=$!
  kc -n "${NAMESPACE}" logs -f "${r2e_pod}" -c loader > "${arm_dir}/r2e-loader.log" 2>&1 &
  local log_r2e_pid=$!
  LOG_PIDS=("${log_swe_pid}" "${log_r2e_pid}")

  local swe_rc r2e_rc
  swe_rc="$(wait_for_exit "${swe_pod}")" || swe_rc=1
  r2e_rc="$(wait_for_exit "${r2e_pod}")" || r2e_rc=1
  wait "${log_swe_pid}" 2>/dev/null || true
  wait "${log_r2e_pid}" 2>/dev/null || true
  LOG_PIDS=()
  bench_stop_measure_loop "${arm_dir}"
  echo "loader exit codes (${arm_name}): swe=${swe_rc} r2e=${r2e_rc}"

  if ! kc -n "${NAMESPACE}" cp -c keep "${swe_pod}:/work/swe.jsonl" "${swe_out}"; then
    echo "failed to copy swe events from ${swe_pod}" >&2
    swe_rc=$(( swe_rc == 0 ? 1 : swe_rc ))
  fi
  if ! kc -n "${NAMESPACE}" cp -c keep "${r2e_pod}:/work/r2e.jsonl" "${r2e_out}"; then
    echo "failed to copy r2e events from ${r2e_pod}" >&2
    r2e_rc=$(( r2e_rc == 0 ? 1 : r2e_rc ))
  fi
  if [[ "${is_timed_run}" == "1" && "${ENABLE_TRACING}" == "1" && "${arm_client}" == "python" ]]; then
    mkdir -p "${arm_dir}/traces"
    if ! kc -n "${NAMESPACE}" cp -c keep "${swe_pod}:/work/traces/swe.jsonl" "${arm_dir}/traces/swe.jsonl"; then
      echo "failed to copy swe traces from ${swe_pod}" >&2
      swe_rc=$(( swe_rc == 0 ? 1 : swe_rc ))
    fi
    if ! kc -n "${NAMESPACE}" cp -c keep "${r2e_pod}:/work/traces/r2e.jsonl" "${arm_dir}/traces/r2e.jsonl"; then
      echo "failed to copy r2e traces from ${r2e_pod}" >&2
      r2e_rc=$(( r2e_rc == 0 ? 1 : r2e_rc ))
    fi
  fi

  finish_restore_harvest "${arm_dir}" "${swe_run}" "${r2e_run}"

  cleanup_cluster
  SWE_JOB=""
  R2E_JOB=""
  SHARD_EXIT=$(( swe_rc != 0 ? swe_rc : r2e_rc ))
}

generate_arm_reports() {
  local arm_dir="$1"
  local run_prefix="$2"
  local arm_client="${3:-}"
  local -a shard_extra=()
  # Only this run's harvest: an earlier run's file in a reused artifact
  # directory must not be joined with these events.
  if [[ "${HARVEST_RESTORES}" == "1" && -s "${arm_dir}/atelet_restores.jsonl" ]]; then
    shard_extra+=("-restore-log" "${arm_dir}/atelet_restores.jsonl")
  fi

  if [[ -s "${arm_dir}/events/swe.jsonl" ]]; then
    go run ./cmd/benchmarking/swebench-loader report "${arm_dir}/events/swe.jsonl" \
      -json "${arm_dir}/swe-summary.json" ${shard_extra[@]+"${shard_extra[@]}"}
  fi
  if [[ -s "${arm_dir}/events/r2e.jsonl" ]]; then
    go run ./cmd/benchmarking/swebench-loader report "${arm_dir}/events/r2e.jsonl" \
      -json "${arm_dir}/r2e-summary.json" ${shard_extra[@]+"${shard_extra[@]}"}
  fi
  if [[ -s "${arm_dir}/events/swe.jsonl" || -s "${arm_dir}/events/r2e.jsonl" ]]; then
    bench_finalize_bundle "${ROOT}" "${CONTEXT}" "${arm_dir}" "${run_prefix}" \
      "${ATESPACE}" "${BASE_TEMPLATE}" "${ATE_ENDPOINT}" "${PROM_URL}" "${PREFLIGHT_COVERAGE}" "${arm_client}"
  fi
}

write_arm_comparison() {
  local out_dir="$1"
  shift
  local -a arm_names=("$@")
  local -a report_files=()
  local name src dst

  for name in "${arm_names[@]}"; do
    src="${out_dir}/arms/${name}/report.json"
    dst="${out_dir}/arms/${name}/arm-report.json"
    if [[ -s "${src}" ]]; then
      python3 - "${src}" "${dst}" "${name}" <<'PY'
import json
import pathlib
import sys

src_path, dst_path, arm_label = sys.argv[1], sys.argv[2], sys.argv[3]
data = json.loads(pathlib.Path(src_path).read_text(encoding="utf-8"))
data["run"] = arm_label
pathlib.Path(dst_path).write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
PY
      report_files+=("${dst}")
    fi
  done

  if (( ${#report_files[@]} == 0 )); then
    return 0
  fi

  # -md renders the arm table (swebench.WriteComparisonMarkdown) over the
  # text comparison.
  go run ./cmd/benchmarking/swebench-loader report "${report_files[@]}" \
    -json "${out_dir}/comparison.json" -md "${out_dir}/comparison.md" > "${out_dir}/comparison.txt"
  cat "${out_dir}/comparison.txt"
}

if [[ "${ACTION}" == "prewarm" ]]; then
  pw_client="${CLIENTS[0]}"
  pw_strategy=""
  if [[ "${pw_client}" == "python" ]]; then
    pw_strategy="none"
  fi
  pw_swe_width="${SWE_WIDTH}"
  if (( SWE_IMAGES < pw_swe_width )); then
    pw_swe_width="${SWE_IMAGES}"
  fi
  pw_r2e_width="${R2E_WIDTH}"
  if (( R2E_IMAGES < pw_r2e_width )); then
    pw_r2e_width="${R2E_IMAGES}"
  fi
  pw_swe_run="${RUN_ID}-pws"
  pw_r2e_run="${RUN_ID}-pwr"

  if [[ "${EXECUTION}" == "in-cluster" ]]; then
    BUILT_GO_IMAGE=""
    run_in_cluster_arm_shards \
      "${pw_client}" "${pw_strategy}" "${pw_client}-prewarm" "eval" \
      "${pw_swe_run}" "${pw_r2e_run}" "${pw_swe_width}" "${pw_r2e_width}" \
      1 1 "workload-file-read-one" "workload-file-read-one" \
      "${ARTIFACT_DIR}/prewarm/swe.jsonl" "${ARTIFACT_DIR}/prewarm/r2e.jsonl" \
      "${ARTIFACT_DIR}/prewarm" "0" "0"
  else
    run_local_arm_shards \
      "${pw_client}" "${pw_strategy}" "${pw_client}-prewarm" "eval" \
      "${pw_swe_run}" "${pw_r2e_run}" "${pw_swe_width}" "${pw_r2e_width}" \
      1 1 "workload-file-read-one" "workload-file-read-one" \
      "${ARTIFACT_DIR}/prewarm/swe.jsonl" "${ARTIFACT_DIR}/prewarm/r2e.jsonl" \
      "${ARTIFACT_DIR}/prewarm" "0" "0"
  fi
  echo "prewarm finished (client=${pw_client}, swe=${SWE_IMAGES} images, r2e=${R2E_IMAGES} images) -> ${ARTIFACT_DIR}/prewarm/"
  exit "${SHARD_EXIT}"
fi

OVERALL_EXIT=0
BUILT_GO_IMAGE=""

for (( idx = 0; idx < NUM_ARMS; idx++ )); do
  arm_client="${ARM_CLIENTS[$idx]}"
  arm_strategy="${ARM_STRATEGIES[$idx]}"
  arm_name="${ARM_NAMES[$idx]}"
  arm_prefix="$(arm_run_prefix "$(( idx + 1 ))" "${NUM_ARMS}")"
  swe_run="${arm_prefix}-swe"
  r2e_run="${arm_prefix}-r2e"
  if (( NUM_ARMS > 1 )); then
    arm_dir="${ARTIFACT_DIR}/arms/${arm_name}"
  else
    arm_dir="${ARTIFACT_DIR}"
  fi
  mkdir -p "${arm_dir}/events"
  bench_reset_bundle_telemetry "${arm_dir}"
  echo "=== running arm $(( idx + 1 ))/${NUM_ARMS}: ${arm_name} (client=${arm_client} strategy=${arm_strategy:-legacy-${MODE}} run_prefix=${arm_prefix}) ===" >&2

  if [[ "${SAMPLE_CACHE}" == "1" ]]; then
    if ! NAMESPACE=ate-system CONTEXT="${CONTEXT}" "${ROOT}/benchmarking/swebench/sample-node-cache.sh" "${arm_dir}/cache_before.prom"; then
      echo "warning: sample-node-cache.sh failed before arm ${arm_name}" >&2
      rm -f "${arm_dir}/cache_before.prom"
    fi
  fi

  SHARD_EXIT=0
  if [[ "${EXECUTION}" == "in-cluster" ]]; then
    run_in_cluster_arm_shards \
      "${arm_client}" "${arm_strategy}" "${arm_name}" "${MODE}" \
      "${swe_run}" "${r2e_run}" "${SWE_WIDTH}" "${R2E_WIDTH}" \
      "${SWE_FANOUT}" "${R2E_FANOUT}" "${SWE_TRAJECTORY}" "${R2E_TRAJECTORY}" \
      "${arm_dir}/events/swe.jsonl" "${arm_dir}/events/r2e.jsonl" \
      "${arm_dir}" "1" "1"
  else
    run_local_arm_shards \
      "${arm_client}" "${arm_strategy}" "${arm_name}" "${MODE}" \
      "${swe_run}" "${r2e_run}" "${SWE_WIDTH}" "${R2E_WIDTH}" \
      "${SWE_FANOUT}" "${R2E_FANOUT}" "${SWE_TRAJECTORY}" "${R2E_TRAJECTORY}" \
      "${arm_dir}/events/swe.jsonl" "${arm_dir}/events/r2e.jsonl" \
      "${arm_dir}" "1" "1"
  fi
  if (( SHARD_EXIT != 0 && OVERALL_EXIT == 0 )); then
    OVERALL_EXIT="${SHARD_EXIT}"
  fi

  if [[ "${SAMPLE_CACHE}" == "1" ]]; then
    if ! NAMESPACE=ate-system CONTEXT="${CONTEXT}" "${ROOT}/benchmarking/swebench/sample-node-cache.sh" "${arm_dir}/cache_after.prom"; then
      echo "warning: sample-node-cache.sh failed after arm ${arm_name}" >&2
      rm -f "${arm_dir}/cache_after.prom"
    fi
  fi

  generate_arm_reports "${arm_dir}" "${arm_prefix}" "${arm_client}"

  if [[ -n "${GCS_BENCHMARK_BUCKET}" || "${VALIDATE_BUNDLE}" == "1" ]]; then
    if (( NUM_ARMS > 1 )); then
      nick_seg="${GCS_BENCHMARK_NICKNAME:-${RUN_ID}}-${arm_name}"
      multi_suffix="${arm_name}"
    else
      nick_seg="${GCS_BENCHMARK_NICKNAME:-${RUN_ID}-${arm_name}}"
      multi_suffix=""
    fi
    arm_folder="$(bench_derive_gcs_folder "${arm_dir}" "simulated-w${MAX_CONCURRENCY}" "r2e${R2E_IMAGES}-swe${SWE_IMAGES}" "${nick_seg}" "${GCS_BENCHMARK_FOLDER}" "${multi_suffix}" "0")"
    if ! bench_validate_and_upload "${ROOT}" "${CONTEXT}" "${arm_dir}" "${arm_folder}" "${GCS_BENCHMARK_BUCKET}" "${GCS_DRY_RUN}" "${VALIDATE_BUNDLE}" "${FOOTING}"; then
      echo "error: bundle validation or upload failed for arm ${arm_name} (${arm_dir})" >&2
      if (( OVERALL_EXIT == 0 )); then
        OVERALL_EXIT=1
      fi
    fi
  fi
done

if (( NUM_ARMS > 1 )); then
  write_arm_comparison "${ARTIFACT_DIR}" "${ARM_NAMES[@]}"
fi

exit "${OVERALL_EXIT}"

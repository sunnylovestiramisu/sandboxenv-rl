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

# Run the VeRL SWE-bench RayJob end to end with telemetry and build the HTML
# run report (docs/observability.md):
#
#   1. checks the telemetry ate-env-api (--enable-tracing, :9090/metrics) and the
#      acceptance collector (tests/telemetry/otel-collector.yaml), deploying the
#      collector if it is missing; optionally rebuilds ate-env-api with ko
#   2. empties the collector's span file, scrapes ate-env-api's /metrics
#   3. packages clients/python/src + this example into a ConfigMap, renders
#      ray-job.async-verl.yaml with tracing on and applies it under a fresh name
#   4. waits for the RayJob, then collects the driver log (run report and the
#      evaluator's Prometheus exposition), the collector's spans and a second
#      ate-env-api scrape
#   5. builds report.html with benchmarking/telemetry_report and checks that
#      the telemetry showed up: a run report, and traces that cross from the
#      trainer through the fleet into ate-env-api
#
# Run from the repository root:
#
#   VERL_WORKER_IMAGE=REGISTRY/verl-worker:v1 \
#   SWEBENCH_IMAGE=REGISTRY/sweb.eval.pytest-5221:latest \
#   examples/verl_swebench/run-telemetry-e2e.sh
#
# Settings (environment):
#   CONTEXT               kubectl context (default: current context)
#   VERL_WORKER_IMAGE     Ray image with python3, ray and torch (required)
#   SWEBENCH_IMAGE        sandbox image; a tag is resolved to its digest with
#                         gcloud (Artifact Registry) because ate-env-api only
#                         accepts digest-pinned images (required)
#   SWEBENCH_TEMPLATE     ActorTemplate for the image; empty creates sandboxes
#                         from the image on default-template (default: empty)
#   JOB_NAME              RayJob name (default: verl-telemetry-<timestamp>)
#   RAY_NAMESPACE         namespace of the RayJob and ConfigMap (default: default)
#   API_NAMESPACE         namespace of the telemetry ate-env-api (default: ate-env)
#   API_SERVICE           its Deployment and Service (default: ate-env-api-telemetry-plan)
#   TELEMETRY_NAMESPACE   namespace of the collector (default: ate-env-telemetry)
#   PKG_CONFIGMAP         ConfigMap with the code (default: sandbox-sdk-pkg-telemetry)
#   NUM_ITERS, GROUP_SIZE pipeline size (default: 3, 4)
#   KO_DOCKER_REPO        if set, rebuild ate-env-api from this checkout with ko
#                         and roll the telemetry deployment to it
#   RESET_COLLECTOR       1 to start from an empty span file (default: 1)
#   TIMEOUT_S             how long to wait for the RayJob (default: 1200)
#   OUT                   output directory (default: /tmp/<JOB_NAME>)

set -o errexit -o nounset -o pipefail

: "${VERL_WORKER_IMAGE:?set VERL_WORKER_IMAGE}"
: "${SWEBENCH_IMAGE:?set SWEBENCH_IMAGE}"
CONTEXT="${CONTEXT:-$(kubectl config current-context)}"
SWEBENCH_TEMPLATE="${SWEBENCH_TEMPLATE:-}"
JOB_NAME="${JOB_NAME:-verl-telemetry-$(date -u +%Y%m%d-%H%M%S)}"
RAY_NAMESPACE="${RAY_NAMESPACE:-default}"
API_NAMESPACE="${API_NAMESPACE:-ate-env}"
API_SERVICE="${API_SERVICE:-ate-env-api-telemetry-plan}"
TELEMETRY_NAMESPACE="${TELEMETRY_NAMESPACE:-ate-env-telemetry}"
PKG_CONFIGMAP="${PKG_CONFIGMAP:-sandbox-sdk-pkg-telemetry}"
NUM_ITERS="${NUM_ITERS:-3}"
GROUP_SIZE="${GROUP_SIZE:-4}"
RESET_COLLECTOR="${RESET_COLLECTOR:-1}"
TIMEOUT_S="${TIMEOUT_S:-1200}"
OUT="${OUT:-/tmp/${JOB_NAME}}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"
k() { kubectl --context "${CONTEXT}" "$@"; }
log() { printf '\n==> %s\n' "$*"; }
scrape_api() { k get --raw "/api/v1/namespaces/${API_NAMESPACE}/services/${API_SERVICE}:9090/proxy/metrics"; }

mkdir -p "${OUT}/reports" "${OUT}/scrapes"
echo "context=${CONTEXT} job=${JOB_NAME} out=${OUT}"

# ------------------------------------------------------------------ 1. preflight
log "Checking ${API_NAMESPACE}/${API_SERVICE}"
k -n "${API_NAMESPACE}" get deploy "${API_SERVICE}" >/dev/null
args="$(k -n "${API_NAMESPACE}" get deploy "${API_SERVICE}" -o jsonpath='{.spec.template.spec.containers[0].args}')"
case "${args}" in
  *enable-tracing*) ;;
  *) echo "error: ${API_SERVICE} does not run with -enable-tracing (args: ${args})" >&2; exit 1 ;;
esac

if [[ -n "${KO_DOCKER_REPO:-}" ]]; then
  log "Rebuilding ate-env-api with ko"
  img="$(KO_DEFAULTPLATFORMS=linux/amd64 ko build --tags "telemetry-$(git rev-parse --short HEAD)" ./cmd/ate-env-api | tail -1)"
  k -n "${API_NAMESPACE}" set image "deploy/${API_SERVICE}" "ate-env-api=${img}"
  k -n "${API_NAMESPACE}" rollout status "deploy/${API_SERVICE}" --timeout=300s
fi

if ! k -n "${TELEMETRY_NAMESPACE}" get deploy otel-collector >/dev/null 2>&1; then
  log "Deploying the acceptance collector into ${TELEMETRY_NAMESPACE}"
  k create namespace "${TELEMETRY_NAMESPACE}" --dry-run=client -o yaml | k apply -f -
  k -n "${TELEMETRY_NAMESPACE}" apply -f tests/telemetry/otel-collector.yaml
  k -n "${TELEMETRY_NAMESPACE}" set env deploy/otel-collector -c collector \
    "METRICS_TARGET=${API_SERVICE}.${API_NAMESPACE}.svc.cluster.local:9090"
elif [[ "${RESET_COLLECTOR}" == "1" ]]; then
  log "Restarting the collector (empty span file)"
  k -n "${TELEMETRY_NAMESPACE}" rollout restart deploy/otel-collector
fi
k -n "${TELEMETRY_NAMESPACE}" rollout status deploy/otel-collector --timeout=180s

if [[ "${SWEBENCH_IMAGE}" != *@sha256:* ]]; then
  digest="$(gcloud artifacts docker images describe "${SWEBENCH_IMAGE}" --format='value(image_summary.digest)')"
  SWEBENCH_IMAGE="${SWEBENCH_IMAGE%:*}@${digest}"
fi
echo "sandbox image: ${SWEBENCH_IMAGE}"

scrape_api > "${OUT}/ate-env-api.before.prom"

# ------------------------------------------------------------------- 2. launch
log "Packaging the client and the example into ${RAY_NAMESPACE}/${PKG_CONFIGMAP}"
tar czf - --exclude=__pycache__ clients/python/src examples/verl_swebench/*.py \
  examples/verl_swebench/*.json | base64 -w0 > "${OUT}/pkg.tar.gz.b64"
k -n "${RAY_NAMESPACE}" create configmap "${PKG_CONFIGMAP}" \
  --from-file=pkg.tar.gz.b64="${OUT}/pkg.tar.gz.b64" --dry-run=client -o yaml | k apply -f -

log "Applying RayJob ${RAY_NAMESPACE}/${JOB_NAME}"
VERL_WORKER_IMAGE="${VERL_WORKER_IMAGE}" SWEBENCH_IMAGE="${SWEBENCH_IMAGE}" \
SWEBENCH_TEMPLATE="${SWEBENCH_TEMPLATE}" \
SUBSTRATE_API_ENDPOINT="${API_SERVICE}.${API_NAMESPACE}.svc.cluster.local:7777" \
ENABLE_TRACING=1 \
OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.${TELEMETRY_NAMESPACE}.svc.cluster.local:4317" \
  envsubst < examples/verl_swebench/ray-job.async-verl.yaml \
  | sed -e "s/name: async-verl-swebench-sandbox-sdk/name: ${JOB_NAME}/" \
        -e "s/namespace: default/namespace: ${RAY_NAMESPACE}/" \
        -e "s/name: sandbox-sdk-pkg\$/name: ${PKG_CONFIGMAP}/" \
        -e "s/--num-iters 3 --group-size 4/--num-iters ${NUM_ITERS} --group-size ${GROUP_SIZE}/" \
  > "${OUT}/rayjob.yaml"
k apply -f "${OUT}/rayjob.yaml"

# --------------------------------------------------------------------- 3. wait
log "Waiting for the RayJob (timeout ${TIMEOUT_S}s)"
deadline=$((SECONDS + TIMEOUT_S))
status=""
while (( SECONDS < deadline )); do
  status="$(k -n "${RAY_NAMESPACE}" get rayjob "${JOB_NAME}" -o jsonpath='{.status.jobStatus}' 2>/dev/null || true)"
  case "${status}" in
    SUCCEEDED|FAILED|STOPPED) break ;;
  esac
  sleep 10
done
echo "job status: ${status:-unknown}"

# ------------------------------------------------------------------ 4. collect
log "Collecting the driver log, spans and metrics"
k -n "${RAY_NAMESPACE}" logs "job/${JOB_NAME}" > "${OUT}/driver.log" || true
grep '=== ATE_ENV_RUN_REPORT_JSON ===' "${OUT}/driver.log" | sed 's/.*=== //' \
  > "${OUT}/reports/${JOB_NAME}.json" || true
sed -n '/=== ATE_ENV_METRICS_BEGIN ===/,/=== ATE_ENV_METRICS_END ===/p' "${OUT}/driver.log" \
  | grep -v '^=== ' > "${OUT}/scrapes/evaluator.prom" || true
# The collector batches spans; give the last batch time to reach the file.
sleep 10
k -n "${TELEMETRY_NAMESPACE}" exec deploy/otel-collector -c files -- cat /data/spans.jsonl \
  > "${OUT}/spans.jsonl"
scrape_api > "${OUT}/scrapes/ate-env-api.prom"

if [[ "${status}" != "SUCCEEDED" ]]; then
  echo "error: RayJob ended with status '${status}'; see ${OUT}/driver.log" >&2
  tail -n 40 "${OUT}/driver.log" >&2 || true
  exit 1
fi

# -------------------------------------------------------------- 5. report/check
log "Building the HTML report"
python3 benchmarking/telemetry_report/build_report.py \
  --report "${OUT}/reports" --spans "${OUT}/spans.jsonl" --metrics "${OUT}/scrapes" \
  --title "${JOB_NAME} (${CONTEXT##*_})" --out "${OUT}/report"

python3 - "${OUT}" <<'PY'
import json, sys

out = sys.argv[1]
data = json.load(open(f"{out}/report/run.json"))
runs, spans = data["report"]["runs"], data["spans"]
problems = []
if not runs:
    problems.append("no RunReport in the driver log")
iters = [t for t in spans.get("traces", []) if t["root"] == "verl.iteration"]
joined = [t for t in iters if {"verl-trainer", "ate-env-fleet", "ate-env-api"} <= set(t["services"])]
if not iters:
    problems.append("no verl.iteration traces in the collector")
elif len(joined) != len(iters):
    problems.append(f"only {len(joined)}/{len(iters)} iteration traces reach ate-env-api")
if not data["metrics"]["histograms"].get("ate_env_api_create_to_ready_seconds"):
    problems.append("ate_env_api_create_to_ready_seconds has no observations")

r = runs[0] if runs else {}
print(json.dumps({
    "tasks_ok": r.get("tasks_ok"), "tasks_err": r.get("tasks_err"), "claims": r.get("claims"),
    "total_s": r.get("total_s"), "spans": spans.get("n", 0), "traces": spans.get("n_traces", 0),
    "iteration_traces": len(iters), "iteration_traces_joined": len(joined),
    "services": spans.get("services", []),
}, indent=1))
for p in problems:
    print("FAIL:", p)
print(f"report: {out}/report/report.html")
sys.exit(1 if problems else 0)
PY

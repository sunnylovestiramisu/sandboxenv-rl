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

# Shared telemetry collection, bundle validation, and GCS upload helpers
# sourced by benchmarking/swebench/run-r2e-swebench.sh and run-in-cluster.sh.

BENCH_TMP_DIR="${BENCH_TMP_DIR:-}"
BENCH_BIN="${BENCH_BIN:-}"
BENCH_KUBECONFIG="${BENCH_KUBECONFIG:-}"
BENCH_MEASURE_PID="${BENCH_MEASURE_PID:-}"

bench_ensure_bin() {
  local root="$1"
  local kube_context="${2:-}"
  if [[ -z "${BENCH_TMP_DIR}" || ! -d "${BENCH_TMP_DIR}" ]]; then
    BENCH_TMP_DIR="$(mktemp -d)"
    BENCH_BIN=""
    BENCH_KUBECONFIG=""
  fi
  if [[ -z "${BENCH_BIN}" || ! -x "${BENCH_BIN}" ]]; then
    BENCH_BIN="${BENCH_TMP_DIR}/bench"
    (cd "${root}" && go build -o "${BENCH_BIN}" ./cmd/benchmarking/bench)
  fi
  if [[ -n "${kube_context}" && -z "${BENCH_KUBECONFIG}" ]]; then
    local kc_out="${BENCH_TMP_DIR}/kubeconfig"
    if (umask 077 && kubectl config view --raw --flatten > "${kc_out}" 2>/dev/null) && \
       KUBECONFIG="${kc_out}" kubectl config use-context "${kube_context}" >/dev/null 2>&1; then
      BENCH_KUBECONFIG="${kc_out}"
    else
      rm -f "${kc_out}"
      echo "error: failed to isolate kubeconfig for context '${kube_context}'" >&2
      return 1
    fi
  fi
}

# bench_reset_bundle_telemetry removes what an earlier run left in <arm_dir>
# that this run writes again or that bench report derives, so a reused artifact
# directory cannot carry an old run's telemetry into this run's bundle. It
# removes only those files, by name: anything else in the directory (an
# operator's own traces/, a hand-placed preflight-coverage.json) stays.
bench_reset_bundle_telemetry() {
  local arm_dir="${1:-}"
  if [[ -z "${arm_dir}" ]]; then
    return 0
  fi
  local f
  for f in \
    "${arm_dir}/events/reap.jsonl" \
    "${arm_dir}/cache_before.prom" \
    "${arm_dir}/cache_after.prom" \
    "${arm_dir}/atelet_restores.jsonl" \
    "${arm_dir}/restore_timings.jsonl" \
    "${arm_dir}/traces/swe.jsonl" \
    "${arm_dir}/traces/r2e.jsonl" \
    "${arm_dir}/measurements.json" \
    "${arm_dir}/system_description.json" \
    "${arm_dir}/report.json" \
    "${arm_dir}/report.md" \
    "${arm_dir}/run_window.json" \
    "${arm_dir}"/*-summary.json \
    "${arm_dir}"/measurements_snapshots/meas_*.json \
    "${arm_dir}"/measurements_snapshots/.meas_*.json.tmp; do
    if [[ -f "${f}" ]]; then
      rm -f "${f}"
      echo "reset: removed ${f} left by an earlier run" >&2
    fi
  done
}

bench_exec() {
  if [[ -n "${BENCH_KUBECONFIG}" ]]; then
    KUBECONFIG="${BENCH_KUBECONFIG}" "${BENCH_BIN}" "$@"
  else
    "${BENCH_BIN}" "$@"
  fi
}

bench_stop_measure_loop() {
  local arm_dir="${1:-}"
  if [[ -n "${BENCH_MEASURE_PID}" ]]; then
    kill "${BENCH_MEASURE_PID}" 2>/dev/null || true
    wait "${BENCH_MEASURE_PID}" 2>/dev/null || true
    BENCH_MEASURE_PID=""
  fi
  if [[ -n "${arm_dir}" && -d "${arm_dir}/measurements_snapshots" ]]; then
    rm -f "${arm_dir}/measurements_snapshots"/.*.tmp
  fi
}

bench_cleanup_telemetry() {
  bench_stop_measure_loop ""
  if [[ -n "${BENCH_TMP_DIR}" && -d "${BENCH_TMP_DIR}" ]]; then
    rm -rf "${BENCH_TMP_DIR}"
  fi
  BENCH_TMP_DIR=""
  BENCH_BIN=""
  BENCH_KUBECONFIG=""
}

bench_take_snapshot_once() {
  local arm_dir="$1"
  local run_id="$2"
  local atespace="$3"
  local base_template="$4"
  local seq="$5"
  local ate_endpoint="${6:-}"
  local prom_url="${7:-}"
  local snap_dir="${arm_dir}/measurements_snapshots"
  mkdir -p "${snap_dir}"
  local ts
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  local tmp_file="${snap_dir}/.meas_${ts}_${seq}.json.tmp"
  local out_file="${snap_dir}/meas_${ts}_${seq}.json"
  local -a args=(
    measure -k8s
    -atespace "${atespace}"
    -base-template "${base_template}"
    -run-id "${run_id}"
    -out "${tmp_file}"
  )
  if [[ -n "${ate_endpoint}" ]]; then
    args+=(-ate-endpoint "${ate_endpoint}")
  fi
  if [[ -n "${prom_url}" ]]; then
    args+=(-prom-url "${prom_url}")
  fi
  if bench_exec "${args[@]}" >/dev/null 2>&1 && [[ -s "${tmp_file}" ]]; then
    mv "${tmp_file}" "${out_file}"
  else
    rm -f "${tmp_file}"
  fi
}

# bench_start_measure_loop periodically captures K8s / Substrate / Prometheus
# snapshots into <arm_dir>/measurements_snapshots/ while a benchmark arm runs.
bench_start_measure_loop() {
  local root="$1"
  local kube_context="$2"
  local arm_dir="$3"
  local run_id="$4"
  local atespace="$5"
  local base_template="$6"
  local interval_sec="$7"
  local ate_endpoint="${8:-}"
  local prom_url="${9:-}"

  bench_stop_measure_loop "${arm_dir}"
  if [[ -z "${interval_sec}" || "${interval_sec}" == "0" ]]; then
    return 0
  fi
  bench_ensure_bin "${root}" "${kube_context}"
  mkdir -p "${arm_dir}/measurements_snapshots"
  local first_wait="${interval_sec}"
  if (( first_wait > 10 )); then
    first_wait=10
  fi
  (
    sleep_pid=""
    trap '[[ -n "${sleep_pid}" ]] && kill "${sleep_pid}" 2>/dev/null || true; exit 0' TERM INT
    sleep "${first_wait}" &
    sleep_pid=$!
    wait "${sleep_pid}" || exit 0
    sleep_pid=""
    seq=1
    bench_take_snapshot_once "${arm_dir}" "${run_id}" "${atespace}" "${base_template}" "${seq}" "${ate_endpoint}" "${prom_url}"
    while :; do
      sleep "${interval_sec}" &
      sleep_pid=$!
      wait "${sleep_pid}" || exit 0
      sleep_pid=""
      seq=$(( seq + 1 ))
      bench_take_snapshot_once "${arm_dir}" "${run_id}" "${atespace}" "${base_template}" "${seq}" "${ate_endpoint}" "${prom_url}"
    done
  ) &
  BENCH_MEASURE_PID=$!
}

# bench_finalize_bundle optionally runs server-queried reap, captures final
# measurements.json + system_description.json, and renders report.{json,md} +
# run_window.json in <arm_dir>.
bench_finalize_bundle() {
  local root="$1"
  local kube_context="$2"
  local arm_dir="$3"
  local run_id="${4%-}"
  local atespace="$5"
  local base_template="$6"
  local ate_endpoint="${7:-}"
  local prom_url="${8:-}"
  local preflight_coverage="${9:-}"
  local arm_client="${10:-}"
  local reap_prefix="${run_id}-"

  bench_ensure_bin "${root}" "${kube_context}"
  if [[ -n "${preflight_coverage}" && -f "${preflight_coverage}" ]]; then
    if [[ "${preflight_coverage}" != "${arm_dir}/preflight-coverage.json" ]]; then
      cp "${preflight_coverage}" "${arm_dir}/preflight-coverage.json"
    fi
  fi
  if [[ -n "${ate_endpoint}" && "${arm_client}" != "python" ]]; then
    if ! bench_exec reap \
      -ate-endpoint "${ate_endpoint}" \
      -atespace "${atespace}" \
      -prefix "${reap_prefix}" \
      -dest "${arm_dir}"; then
      echo "warning: bench reap failed for ${arm_dir} (prefix=${reap_prefix})" >&2
    fi
  fi

  local -a meas_args=(
    measure -k8s
    -atespace "${atespace}"
    -base-template "${base_template}"
    -run-id "${run_id}"
    -dest "${arm_dir}"
  )
  if [[ -n "${ate_endpoint}" ]]; then
    meas_args+=(-ate-endpoint "${ate_endpoint}")
  fi
  if [[ -n "${prom_url}" ]]; then
    meas_args+=(-prom-url "${prom_url}")
  fi
  if ! bench_exec "${meas_args[@]}"; then
    echo "warning: bench measure -k8s failed for ${arm_dir}; report.json has no system_description or measurements" >&2
  fi
  bench_exec report -dest "${arm_dir}"
}

# bench_sanitize_segment normalizes a string to lowercase [a-z0-9.-]+ with no underscores,
# matching cmd/benchmarking/bench/bundle.go ValidateGCSFolderName segment rules.
bench_sanitize_segment() {
  local raw="$1"
  local fallback="${2:-run}"
  local s
  s="$(printf '%s' "${raw}" | tr '[:upper:]_' '[:lower:]-' | sed -E 's/[^a-z0-9.-]+/-/g; s/-+/-/g; s/^[-.]+//; s/[-.]+$//')"
  if [[ -z "${s}" ]]; then
    s="${fallback}"
  fi
  printf '%s\n' "${s}"
}

# bench_bundle_has_trace mirrors bundleTraceEvidence in cmd/benchmarking/bench/bundle.go.
bench_bundle_has_trace() {
  local arm_dir="$1"
  local expect_traced="${2:-0}"
  if [[ "${expect_traced}" == "1" ]]; then
    return 0
  fi
  python3 - "${arm_dir}" <<'PY'
import glob
import json
import os
import sys

arm_dir = sys.argv[1]
patterns = [
    "trace.jsonl",
    "verl-live-trace.jsonl",
    os.path.join("traces", "*.jsonl"),
    os.path.join("traces", "*", "*.jsonl"),
    os.path.join("results", "traces", "*.jsonl"),
]
for pat in patterns:
    for match in glob.glob(os.path.join(arm_dir, pat)):
        if os.path.isfile(match) and os.path.getsize(match) > 0:
            sys.exit(0)

rep_path = os.path.join(arm_dir, "report.json")
if os.path.isfile(rep_path):
    try:
        with open(rep_path, encoding="utf-8") as fh:
            if json.load(fh).get("trace_captured") is True:
                sys.exit(0)
    except Exception:
        pass

rw_path = os.path.join(arm_dir, "run_window.json")
if os.path.isfile(rw_path):
    try:
        with open(rw_path, encoding="utf-8") as fh:
            md = json.load(fh).get("metadata") or {}
            if md.get("trace_captured") == "true":
                sys.exit(0)
    except Exception:
        pass

sys.exit(1)
PY
}

# bench_derive_gcs_folder builds <YYYY-MM-DD>_<model>_<dataset>_<nickname>[_traced],
# or adapts an explicit folder override across multi-arm sweeps without collisions.
bench_derive_gcs_folder() {
  local arm_dir="$1"
  local model_seg="$2"
  local dataset_seg="$3"
  local nickname_seg="$4"
  local explicit_folder="${5:-}"
  local multi_arm_suffix="${6:-}"
  local expect_traced="${7:-0}"

  local traced=0
  if bench_bundle_has_trace "${arm_dir}" "${expect_traced}"; then
    traced=1
  fi

  local folder=""
  if [[ -n "${explicit_folder}" ]]; then
    folder="${explicit_folder%/}"
    folder="${folder#/}"
    if [[ -n "${multi_arm_suffix}" ]]; then
      local stem="${folder%_traced}"
      local arm_clean
      arm_clean="$(bench_sanitize_segment "${multi_arm_suffix}" "arm")"
      folder="${stem}-${arm_clean}"
      if (( traced == 1 )); then
        folder="${folder}_traced"
      fi
    fi
  else
    local date_utc model_clean ds_clean nick_clean
    date_utc="$(date -u +%Y-%m-%d)"
    model_clean="$(bench_sanitize_segment "${model_seg}" "simulated")"
    ds_clean="$(bench_sanitize_segment "${dataset_seg}" "dataset")"
    nick_clean="$(bench_sanitize_segment "${nickname_seg}" "run")"
    folder="${date_utc}_${model_clean}_${ds_clean}_${nick_clean}"
    if (( traced == 1 )); then
      folder="${folder}_traced"
    fi
  fi
  printf '%s\n' "${folder}"
}

# bench_check_gcs_folder_syntax validates a GCS folder name offline (e.g. during dry-run)
# using the same regex and _traced suffix contract as ValidateGCSFolderName.
bench_check_gcs_folder_syntax() {
  local folder="$1"
  local traced="${2:-0}"
  python3 - "${folder}" "${traced}" <<'PY'
import datetime
import re
import sys

folder = sys.argv[1].strip().strip("/")
traced = sys.argv[2] == "1"
pat = re.compile(r"^\d{4}-\d{2}-\d{2}_[a-zA-Z0-9.-]+_[a-zA-Z0-9.-]+_[a-zA-Z0-9.-]+(_traced)?$", re.ASCII)
if not pat.match(folder):
    print(
        f'invalid benchmark folder name "{folder}": must match '
        "<YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>[_traced]",
        file=sys.stderr,
    )
    sys.exit(2)
try:
    datetime.date.fromisoformat(folder[:10])
except ValueError as exc:
    print(f'invalid date prefix in folder name "{folder}": {exc}', file=sys.stderr)
    sys.exit(2)
has_suffix = folder.endswith("_traced")
if traced and not has_suffix:
    print(f'folder name "{folder}" must end in _traced: trace capture is enabled', file=sys.stderr)
    sys.exit(2)
if not traced and has_suffix:
    print(f'folder name "{folder}" ends in _traced but trace capture is not enabled', file=sys.stderr)
    sys.exit(2)
PY
}

# bench_validate_and_upload validates <arm_dir> and optionally uploads it to GCS.
bench_validate_and_upload() {
  local root="$1"
  local kube_context="$2"
  local arm_dir="$3"
  local folder="$4"
  local gcs_bucket="${5:-}"
  local dry_run="${6:-0}"
  local validate_only="${7:-0}"
  local footing="${8:-}"

  bench_ensure_bin "${root}" "${kube_context}"
  if [[ -n "${gcs_bucket}" ]]; then
    local -a up_args=(
      upload
      -dest "${arm_dir}"
      -bucket "${gcs_bucket}"
      -folder "${folder}"
    )
    if [[ -n "${footing}" ]]; then
      up_args+=(-footing "${footing}")
    fi
    if [[ "${dry_run}" == "1" ]]; then
      up_args+=(-dry-run)
    fi
    bench_exec "${up_args[@]}"
  elif [[ "${validate_only}" == "1" ]]; then
    local -a val_args=(validate -dest "${arm_dir}")
    if [[ -n "${folder}" ]]; then
      val_args+=(-folder "${folder}")
    fi
    if [[ -n "${footing}" ]]; then
      val_args+=(-footing "${footing}")
    fi
    bench_exec "${val_args[@]}"
  fi
}

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

func TestValidateGCSFolderName(t *testing.T) {
	for _, tc := range []struct {
		folder  string
		traced  bool
		wantErr string
	}{
		{"2026-10-27_synthetic-w4_math-bb4dcec_suspend-sync", false, ""},
		{"2026-10-28_qwen3.5-35b_r2e-gym-easy_prewarmed_traced", true, ""},
		{" 2026-10-28_qwen3.5-35b_r2e-gym-easy_prewarmed_traced/ ", true, ""},
		{"2026-10-28_qwen3.5-35b_r2e-gym-easy_prewarmed", true, "must end in _traced"},
		{"2026-10-28_qwen3.5-35b_r2e-gym-easy_prewarmed_traced", false, "has no trace"},
		{"BadFolderName", false, "invalid benchmark folder name"},
		{"2026-13-40_a_b_c", false, "invalid date prefix"},
		{"2026-10-28_a_b", false, "invalid benchmark folder name"},
		{"2026-10-28_a_b_c_d", false, "invalid benchmark folder name"},
		{"2026-10-28_a_b_c_traced_traced", true, "invalid benchmark folder name"},
		{"2026-10-28_a_b_c_d_traced", true, "invalid benchmark folder name"},
		{"2026-10-28_a_b_snake_case", false, "invalid benchmark folder name"},
	} {
		err := ValidateGCSFolderName(tc.folder, tc.traced)
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%q traced=%t: unexpected error %v", tc.folder, tc.traced, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%q traced=%t: want error containing %q, got %v", tc.folder, tc.traced, tc.wantErr, err)
		}
	}
}

func TestGcloudUploadArgsFlattensBundleRoot(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "report.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".tmp-ignore"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	dest := "gs://unit-test-bucket/2026-11-07_simulated_r2e-swebench-2000_w500-go-none/"
	got, err := gcloudUploadArgs(dir, dest)
	if err != nil {
		t.Fatalf("gcloudUploadArgs: %v", err)
	}
	want := []string{
		"storage", "cp", "-r",
		filepath.Join(dir, "events"),
		filepath.Join(dir, "report.json"),
		dest,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gcloudUploadArgs = %v, want %v", got, want)
	}
}

func TestEnsureRunWindowUpgradesPlaceholderWindow(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 11, 7, 3, 8, 29, 0, time.UTC)
	if err := ensureRunWindowFile(dir, "r1", "c1", base.Add(-time.Second), base, map[string]string{"trace_captured": "false"}); err != nil {
		t.Fatal(err)
	}
	realStart := base.Add(-10 * time.Minute)
	if err := ensureRunWindowFile(dir, "r1", "c1", realStart, base, map[string]string{"trace_captured": "false"}); err != nil {
		t.Fatal(err)
	}
	rw, err := swebench.ReadRunWindow(filepath.Join(dir, "run_window.json"))
	if err != nil {
		t.Fatal(err)
	}
	if rw.StartTS != realStart.Format(time.RFC3339) || rw.EndTS != base.Format(time.RFC3339) {
		t.Fatalf("run_window bounds = (%s, %s), want (%s, %s)", rw.StartTS, rw.EndTS, realStart.Format(time.RFC3339), base.Format(time.RFC3339))
	}
}

func TestMaterializeRestoreTimingsJSONL(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "restore_timings.jsonl")
	ts := time.Date(2026, 11, 7, 3, 0, 0, 0, time.UTC)
	timings := map[string][]swebench.RestoreTiming{
		"env-1": {{
			Time:     ts,
			Actor:    "env-1",
			Fidelity: "warm",
			Phases:   map[string]float64{"total": 0.25, "download": 0.001, "worker_restore": 0.21},
		}},
	}
	if err := materializeRestoreTimingsJSONL(out, timings); err != nil {
		t.Fatalf("materializeRestoreTimingsJSONL: %v", err)
	}
	f, err := os.Open(out)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	parsed, err := swebench.ParseRestoreTimings(f)
	if err != nil || len(parsed["env-1"]) != 1 {
		t.Fatalf("ParseRestoreTimings roundtrip = %v, err=%v", parsed, err)
	}
}

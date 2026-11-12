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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

func TestResolveAuthToken(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte("  from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := func(v string) func(string) string {
		return func(k string) string {
			if k == authTokenEnv {
				return v
			}
			return ""
		}
	}
	for _, tc := range []struct {
		name, file, env, want, err string
	}{
		{name: "none", want: ""},
		{name: "env", env: " from-env ", want: "from-env"},
		{name: "file wins over env", file: file, env: "from-env", want: "from-file"},
		{name: "empty file", file: empty, err: "is empty"},
		{name: "missing file", file: filepath.Join(dir, "nope"), err: "reading -auth-token-file"},
	} {
		got, err := resolveAuthToken(tc.file, env(tc.env))
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("%s: err = %v, want %q", tc.name, err, tc.err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: got %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
}

func TestLeadingFiles(t *testing.T) {
	for _, tc := range []struct {
		args, files, rest []string
	}{
		{nil, nil, nil},
		{[]string{"a.jsonl"}, []string{"a.jsonl"}, nil},
		{[]string{"a.jsonl", "b.jsonl", "-json", "o.json"}, []string{"a.jsonl", "b.jsonl"}, []string{"-json", "o.json"}},
		{[]string{"-json", "o.json", "a.jsonl"}, nil, []string{"-json", "o.json", "a.jsonl"}},
	} {
		files, rest := leadingFiles(tc.args)
		if len(files) == 0 {
			files = nil
		}
		if len(rest) == 0 {
			rest = nil
		}
		if !reflect.DeepEqual(files, tc.files) || !reflect.DeepEqual(rest, tc.rest) {
			t.Errorf("leadingFiles(%q) = %q, %q; want %q, %q", tc.args, files, rest, tc.files, tc.rest)
		}
	}
}

func TestReportCmdRejectsFilesAfterFlags(t *testing.T) {
	err := reportCmd([]string{"a.jsonl", "-json", "o.json", "b.jsonl"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "before the flags") || !strings.Contains(err.Error(), "b.jsonl") {
		t.Fatalf("err = %v, want the files-before-flags error naming b.jsonl", err)
	}
}

type e2eFakeBackend struct{}

func (e2eFakeBackend) Create(_ context.Context, id, _ string) (swebench.Sandbox, error) {
	return e2eFakeSandbox{id: id}, nil
}

func (e2eFakeBackend) Delete(_ context.Context, _ string) error { return nil }

type e2eFakeSandbox struct {
	id string
}

func (s e2eFakeSandbox) Run(_ context.Context, command string, _ time.Duration) (swebench.Result, error) {
	if strings.Contains(command, "/tmp/grade.py") {
		return swebench.Result{
			ExitCode: 0,
			Stdout:   `{"valid":true,"resolved":true,"f2p":"1/1","p2p":"2/2"}` + "\n",
		}, nil
	}
	if strings.HasPrefix(command, "echo ") {
		return swebench.Result{
			ExitCode: 0,
			Stdout:   strings.TrimPrefix(command, "echo ") + "\n",
		}, nil
	}
	return swebench.Result{ExitCode: 0, Stdout: "ok\n"}, nil
}

func (e2eFakeSandbox) WriteFile(_ context.Context, _ string, _ []byte, _ fs.FileMode) error {
	return nil
}

func (e2eFakeSandbox) Suspend(_ context.Context) error { return nil }
func (e2eFakeSandbox) Delete(_ context.Context) error  { return nil }

func TestEndToEndSWEBenchAndCommandDatasets(t *testing.T) {
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	dir := t.TempDir()

	testBackendOverride = e2eFakeBackend{}
	t.Cleanup(func() { testBackendOverride = nil })

	// 1. End-to-end SWE-bench manifest run + report.
	sweManifest := filepath.Join(dir, "swebench.jsonl")
	sweRows := strings.Join([]string{
		`{"instance_id":"astropy__astropy-1","repo":"astropy/astropy","image":"reg/swe@` + digest + `","patch":"diff","eval_script":"#!/bin/sh\nexit 0","log_parser":"parse_log_pytest"}`,
		`{"instance_id":"django__django-2","repo":"django/django","image":"reg/swe@` + digest + `","patch":"diff","eval_script":"#!/bin/sh\nexit 0","log_parser":"parse_log_django"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(sweManifest, []byte(sweRows), 0o644); err != nil {
		t.Fatal(err)
	}
	sweEvents := filepath.Join(dir, "swe-events.jsonl")
	sweSummary := filepath.Join(dir, "swe-summary.json")
	if err := runCmd([]string{
		"-dataset", sweManifest,
		"-mode", "eval",
		"-width", "2",
		"-run-id", "swe-e2e",
		"-out", sweEvents,
	}); err != nil {
		t.Fatalf("runCmd(swebench): %v", err)
	}
	if err := reportCmd([]string{sweEvents, "-json", sweSummary}); err != nil {
		t.Fatalf("reportCmd(swebench): %v", err)
	}
	repSWE, err := summarizeFile(sweEvents)
	if err != nil {
		t.Fatalf("summarizeFile(sweEvents): %v", err)
	}
	if repSWE.Tasks != 2 || repSWE.Failed != 0 || repSWE.Rewarded != 2 || repSWE.RewardRate != 1.0 {
		t.Fatalf("unexpected SWE-bench report: %+v", repSWE)
	}

	// 2. End-to-end Command manifest run + report.
	cmdManifest := filepath.Join(dir, "command.jsonl")
	cmdRows := strings.Join([]string{
		`{"kind":"command","instance_id":"cmd-alpha","image":"reg/cmd@` + digest + `","command":"echo alpha-ok","setup_command":"mkdir -p /tmp/work","reset_command":"rm -rf /tmp/work/*","expected_stdout":"alpha-ok","expected_exit_code":0}`,
		`{"kind":"command","instance_id":"cmd-beta","image":"reg/cmd@` + digest + `","command":"echo beta-ok","expected_stdout":"beta-ok","expected_exit_code":0}`,
	}, "\n") + "\n"
	if err := os.WriteFile(cmdManifest, []byte(cmdRows), 0o644); err != nil {
		t.Fatal(err)
	}
	cmdEvents := filepath.Join(dir, "cmd-events.jsonl")
	cmdSummary := filepath.Join(dir, "cmd-summary.json")
	if err := runCmd([]string{
		"-dataset", cmdManifest,
		"-mode", "steps",
		"-rollouts", "1",
		"-steps", "2",
		"-park", "suspend",
		"-width", "2",
		"-run-id", "cmd-e2e",
		"-out", cmdEvents,
	}); err != nil {
		t.Fatalf("runCmd(command): %v", err)
	}
	if err := reportCmd([]string{cmdEvents, "-json", cmdSummary}); err != nil {
		t.Fatalf("reportCmd(command): %v", err)
	}
	repCmd, err := summarizeFile(cmdEvents)
	if err != nil {
		t.Fatalf("summarizeFile(cmdEvents): %v", err)
	}
	if repCmd.Tasks != 4 || repCmd.Failed != 0 || repCmd.Rewarded != 4 || repCmd.RewardRate != 1.0 {
		t.Fatalf("unexpected Command report: %+v", repCmd)
	}

	// 3. End-to-end resolve + scheduled WorkingSet run + report.
	schedPath := filepath.Join(dir, "schedule.json")
	if err := resolveCmd([]string{
		"-dataset", cmdManifest,
		"-steps", "3",
		"-tasks-per-step", "1",
		"-group-size", "2",
		"-park", "suspend-async",
		"-seed", "42",
		"-run-id", "sched-e2e",
		"-out", schedPath,
	}); err != nil {
		t.Fatalf("resolveCmd: %v", err)
	}
	schedEvents := filepath.Join(dir, "sched-events.jsonl")
	if err := runCmd([]string{
		"-dataset", cmdManifest,
		"-mode", "steps",
		"-schedule", schedPath,
		"-park", "suspend-async",
		"-step-pause", "5ms",
		"-prefetch",
		"-width", "2",
		"-run-id", "sched-e2e",
		"-out", schedEvents,
	}); err != nil {
		t.Fatalf("runCmd(schedule): %v", err)
	}
	repSched, err := summarizeFile(schedEvents)
	if err != nil {
		t.Fatalf("summarizeFile(schedEvents): %v", err)
	}
	if repSched.Tasks != 6 || repSched.Failed != 0 || repSched.Rewarded != 6 {
		t.Fatalf("unexpected scheduled report: %+v", repSched)
	}
	if repSched.WorkingSetHitRate <= 0 || repSched.Created == 0 || repSched.Reused == 0 {
		t.Fatalf("expected non-zero working-set hit rate and reuse in scheduled report: %+v", repSched)
	}

	// 4. Verify -backend and -access flags in runCmd.
	for _, tc := range []struct {
		backend, access string
		wantErr         bool
	}{
		{backend: "substrate", access: "ate-env-api", wantErr: false},
		{backend: "substrate", access: "direct", wantErr: false},
		{backend: "agent-sandbox", access: "sdk", wantErr: false},
		{backend: "bogus", access: "ate-env-api", wantErr: true},
		{backend: "substrate", access: "bogus", wantErr: true},
	} {
		err := runCmd([]string{
			"-dataset", cmdManifest,
			"-backend", tc.backend,
			"-access", tc.access,
			"-dry-run",
		})
		if (err != nil) != tc.wantErr {
			t.Fatalf("runCmd(-backend=%s -access=%s) err = %v, wantErr = %v", tc.backend, tc.access, err, tc.wantErr)
		}
	}
}

func TestLabelFlags(t *testing.T) {
	l := labelFlags{}
	for _, v := range []string{"cluster=east", "note=a=b", "empty="} {
		if err := l.Set(v); err != nil {
			t.Fatalf("Set(%q): %v", v, err)
		}
	}
	if l["cluster"] != "east" || l["note"] != "a=b" || l["empty"] != "" {
		t.Fatalf("labels = %v", l)
	}
	for _, bad := range []string{"cluster=west", "novalue", "=x", "tab=a\tb", "nl=a\nb"} {
		if err := l.Set(bad); err == nil {
			t.Errorf("Set(%q) must fail", bad)
		}
	}
	if got := l.String(); got != "cluster=east,empty=,note=a=b" {
		t.Fatalf("String() = %q", got)
	}
}

func TestReportCmdArmsRejectPositionalFiles(t *testing.T) {
	err := reportCmd([]string{"a.jsonl", "-arm", "pull=p.jsonl"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "inside an -arm value") {
		t.Fatalf("err = %v, want the arm-and-files error", err)
	}
	err = reportCmd([]string{"-arm", "pull=p.jsonl", "-top", "3"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not -arm") {
		t.Fatalf("err = %v, want the one-file flags error", err)
	}
	err = reportCmd([]string{"a.jsonl", "-top", "-3"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "-top wants 0 or more") {
		t.Fatalf("err = %v, want the negative -top error", err)
	}
}

func TestArmFlags(t *testing.T) {
	var a armFlags
	for _, v := range []string{"stream=s1.jsonl, s2.jsonl", "pull=p.jsonl"} {
		if err := a.Set(v); err != nil {
			t.Fatalf("Set(%q): %v", v, err)
		}
	}
	// Flag order is column order: the arm given first comes first.
	if len(a) != 2 || a[0].name != "stream" || strings.Join(a[0].files, "+") != "s1.jsonl+s2.jsonl" || a[1].name != "pull" {
		t.Fatalf("arms = %+v", a)
	}
	if got := a.String(); got != "stream=s1.jsonl,s2.jsonl pull=p.jsonl" {
		t.Fatalf("String() = %q", got)
	}
	for _, tc := range []struct{ v, err string }{
		{"pull=x.jsonl", "given twice"},
		{"nofile", "want NAME=FILE[,FILE...]"},
		{"empty=", "want NAME=FILE[,FILE...]"},
		{"=x.jsonl", "want NAME=FILE[,FILE...]"},
		{"gap=a.jsonl,,b.jsonl", "empty file name"},
		{"dup=a.jsonl,a.jsonl", "a.jsonl is given twice"},
	} {
		if err := a.Set(tc.v); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("Set(%q): err = %v, want %q", tc.v, err, tc.err)
		}
	}
}

// writeEvents writes a complete run: one create and its run event.
func writeEvents(t *testing.T, dir, name, runID string, createMS float64, labels map[string]string) string {
	t.Helper()
	var buf bytes.Buffer
	for _, e := range []swebench.Event{
		{RunID: runID, Event: swebench.EventCreate, DurMS: createMS, OK: true},
		{RunID: runID, Event: swebench.EventRun, Name: "eval", DurMS: 1000, OK: true, Count: 1, Labels: labels},
	} {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReportArms(t *testing.T) {
	dir := t.TempDir()
	a := writeEvents(t, dir, "a.jsonl", "r1", 100, map[string]string{"mode": "eval", "width": "2", "commit": "aaa"})
	b := writeEvents(t, dir, "b.jsonl", "r2", 300, map[string]string{"mode": "eval", "width": "2", "commit": "bbb"})
	c := writeEvents(t, dir, "c.jsonl", "r3", 50, map[string]string{"mode": "eval", "width": "2"})
	wide := writeEvents(t, dir, "wide.jsonl", "r4", 50, map[string]string{"mode": "eval", "width": "3"})
	out := filepath.Join(dir, "arms.json")

	var text bytes.Buffer
	if err := reportCmd([]string{"-arm", "pull=" + a + "," + b, "-arm", "stream=" + c, "-json", out}, &text); err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(text.String()), " ")
	// Columns in flag order; pull pools two reps: n=2, the median run, the
	// mixed commit shown as such.
	for _, want := range []string{"run pull stream", "reps 2 1", "create 100/300 n=2 50/50 n=1", "commit aaa|bbb -", "width 2 2"} {
		if !strings.Contains(flat, want) {
			t.Errorf("comparison lacks %q:\n%s", want, text.String())
		}
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var reps []swebench.Report
	if err := json.Unmarshal(raw, &reps); err != nil {
		t.Fatal(err)
	}
	if len(reps) != 2 || reps[0].RunID != "pull" || reps[0].Reps != 2 || strings.Join(reps[0].Runs, ",") != "r1,r2" || reps[1].RunID != "stream" || reps[1].Reps != 1 {
		t.Fatalf("-json: %+v", reps)
	}

	for _, tc := range []struct {
		name string
		args []string
		err  string
	}{
		{"missing file", []string{"-arm", "pull=" + filepath.Join(dir, "nope.jsonl")}, "arm pull"},
		{"empty arm", []string{"-arm", "pull="}, "want NAME=FILE[,FILE...]"},
		{"file twice", []string{"-arm", "pull=" + a + "," + a}, "is given twice"},
		{"other workload", []string{"-arm", "pull=" + a + "," + wide}, "differs from rep 1 in width"},
		{"run in two arms", []string{"-arm", "pull=" + a + "," + b, "-arm", "again=" + a}, "run r1 is in arms pull and again"},
	} {
		if err := reportCmd(tc.args, io.Discard); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.err)
		}
	}
}

func TestOwnLabels(t *testing.T) {
	l := ownLabels("https://host/bucket/swebench.jsonl?X-Goog-Signature=secret", 4, 200, "base", "lists/offline.txt", "plans/image-placement.json", []string{"x", "y"}, []string{"a", "b"}, map[string]string{"READ_FILES": "10"})
	want := map[string]string{"dataset": "swebench.jsonl", "connections": "4", "delete_rate": "200", "base_template": "base",
		"exclude_file": "offline.txt@" + idsDigest([]string{"x", "y"}), "instances": "2@" + idsDigest([]string{"a", "b"}), "set.READ_FILES": "10",
		"placement": "image-placement.json"}
	if !reflect.DeepEqual(l, want) {
		t.Fatalf("labels = %v, want %v", l, want)
	}
	// The digest tells two lists of one length apart and ignores their order.
	if idsDigest([]string{"a", "b"}) != idsDigest([]string{"b", "a"}) || idsDigest([]string{"a", "b"}) == idsDigest([]string{"a", "c"}) || len(idsDigest(nil)) != 8 {
		t.Errorf("idsDigest: %s %s %s", idsDigest([]string{"a", "b"}), idsDigest([]string{"b", "a"}), idsDigest([]string{"a", "c"}))
	}
	for k := range l {
		if !slices.Contains(swebench.LoaderLabelKeys, k) && !strings.HasPrefix(k, "set.") {
			t.Errorf("label %s is not in LoaderLabelKeys, so report -arm would not compare it", k)
		}
	}
	l = ownLabels("data/swebench.jsonl", 1, 0, "t", "", "", nil, nil, nil)
	if _, ok := l["exclude_file"]; ok || l["dataset"] != "swebench.jsonl" || len(l) != 4 {
		t.Fatalf("labels without an exclude file or instances = %v", l)
	}
	// The cap is recorded as given, 0 for none, so a capped and an uncapped
	// run do not pool as one workload; a negative value is no cap too.
	for in, want := range map[float64]string{0: "0", 12.5: "12.5", -1: "0"} {
		if got := ownLabels("d.jsonl", 1, in, "t", "", "", nil, nil, nil)["delete_rate"]; got != want {
			t.Errorf("delete_rate for %v = %q, want %q", in, got, want)
		}
	}
}

func TestAssetsCmd(t *testing.T) {
	var out bytes.Buffer
	if err := assetsCmd([]string{"../../../internal/benchmarking/swebench/trajectories/swebench-gold.yaml"}, &out); err != nil || out.String() != "swebench-grade.py\n" {
		t.Fatalf("trajectory-assets: %q, %v", out.String(), err)
	}
	if err := assetsCmd(nil, io.Discard); err == nil {
		t.Fatal("trajectory-assets without a file must fail")
	}
}

func TestCLIReportFlagsValidateCompareAndStats(t *testing.T) {
	dir := t.TempDir()
	rewardOne := 1
	ts := time.Date(2026, 10, 22, 1, 20, 1, 0, time.UTC)

	rec0, err := swebench.NewShardRecorder(dir, "cli-run-1", "worker-0")
	if err != nil {
		t.Fatalf("NewShardRecorder 0: %v", err)
	}
	rec0.Record(swebench.Event{Time: ts, Event: swebench.EventPrewarm, DurMS: 100, OK: true})
	rec0.Record(swebench.Event{Time: ts, Event: swebench.EventCreate, Step: 1, Task: 0, DurMS: 50, OK: true})
	rec0.Record(swebench.Event{Time: ts, Event: swebench.EventTTFI, Step: 1, Task: 0, DurMS: 200, OK: true})
	rec0.Record(swebench.Event{Time: ts, Event: swebench.EventTask, Step: 1, Task: 0, DurMS: 1000, OK: true, Reward: &rewardOne})
	_ = rec0.Close()

	rec1, err := swebench.NewShardRecorder(dir, "cli-run-1", "worker-1")
	if err != nil {
		t.Fatalf("NewShardRecorder 1: %v", err)
	}
	rec1.Record(swebench.Event{Time: ts.Add(time.Second), Event: swebench.EventCreate, Step: 1, Task: 1, DurMS: 70, OK: true})
	rec1.Record(swebench.Event{Time: ts.Add(time.Second), Event: swebench.EventTTFI, Step: 1, Task: 1, DurMS: 400, OK: true})
	rec1.Record(swebench.Event{Time: ts.Add(time.Second), Event: swebench.EventTask, Step: 1, Task: 1, DurMS: 1400, OK: true, Reward: &rewardOne})
	rec1.Record(swebench.Event{Time: ts.Add(2 * time.Second), Event: swebench.EventReap, DurMS: 40, OK: true, Metrics: map[string]float64{"orphan_count": 0}})
	_ = rec1.Close()

	rwPath := filepath.Join(dir, "run_window.json")
	if err := swebench.WriteRunWindow(rwPath, schema.RunWindow{
		RunID:    "cli-run-1",
		TestName: "swebench-cli",
		Tag:      "cand",
		StartTS:  "2026-10-22T01:20:00Z",
		EndTS:    "2026-10-22T01:20:10Z",
	}); err != nil {
		t.Fatalf("WriteRunWindow: %v", err)
	}

	outReportPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")
	statsPath := filepath.Join(dir, "stats.jsonl")
	globPattern := filepath.Join(dir, "events", "*.jsonl")

	var stdout, stderr bytes.Buffer
	code := runMain([]string{
		"report",
		"-events-glob", globPattern,
		"-run-window", rwPath,
		"-out", outReportPath,
		"-md", mdPath,
		"-stats-out", statsOutPath(statsPath),
		"-validate",
		"-footing", "warm",
		"-min-reward-rate", "0.9",
		"-min-tasks", "2",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit 0, got %d (stderr: %s)", code, stderr.String())
	}

	rawStats, err := os.ReadFile(statsPath)
	if err != nil {
		t.Fatalf("read stats.jsonl: %v", err)
	}
	if err := schema.ValidateJSONLAgainstJSONSchema("stats_row.schema.json", bytes.NewReader(rawStats)); err != nil {
		t.Fatalf("stats.jsonl failed schema validation: %v", err)
	}

	// Verify -validate exits non-zero when rule fails (e.g., min-tasks=10).
	stdout.Reset()
	stderr.Reset()
	code = runMain([]string{
		"report",
		"-events-glob", globPattern,
		"-validate",
		"-min-tasks", "10",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero exit code when -validate fails")
	}

	// Verify -compare with -thresholds pass and fail exit codes.
	threshPassPath := filepath.Join(dir, "thresholds-pass.yaml")
	if err := os.WriteFile(threshPassPath, []byte("max_latency_regression_pct: 50.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	code = runMain([]string{
		"report",
		"-in", outReportPath,
		"-compare", outReportPath,
		"-thresholds", threshPassPath,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected compare pass (exit 0), got %d (stderr: %s)", code, stderr.String())
	}

	// Create a baseline report with lower TTFI so candidate exceeds a 5% threshold.
	baseReportPath := filepath.Join(dir, "base-report.json")
	if err := os.WriteFile(baseReportPath, []byte(`{
  "run": "base-run",
  "tasks": 2,
  "rewarded": 2,
  "reward_rate": 1.0,
  "stats": [
    {"event": "ttfi", "count": 2, "p50_ms": 100.0, "p90_ms": 150.0, "p95_ms": 150.0, "max_ms": 150.0}
  ]
}`), 0o644); err != nil {
		t.Fatal(err)
	}
	threshFailPath := filepath.Join(dir, "thresholds-fail.yaml")
	if err := os.WriteFile(threshFailPath, []byte("max_p50_regression_pct:\n  ttfi: 5.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	code = runMain([]string{
		"report",
		"-in", outReportPath,
		"-compare", baseReportPath,
		"-thresholds", threshFailPath,
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero exit code when -thresholds detects regression")
	}
}

func TestCLIConvertTraceSubcommandAndFlag(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.jsonl")
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	traceLines := strings.Join([]string{
		`{"schema_version":"v1","ts":"2026-10-22T12:00:00.005Z","job":"grpo","step":1,"env":"sb-1","instance":"inst-1","image":"reg/img@` + digest + `","op":"create","dur_ms":5,"ok":true}`,
		`{"schema_version":"v1","ts":"2026-10-22T12:00:00.015Z","job":"grpo","step":1,"env":"sb-1","instance":"inst-1","op":"write_file","path":"/workspace/p.diff","bytes":32,"dur_ms":5,"ok":true}`,
		`{"schema_version":"v1","ts":"2026-10-22T12:00:00.045Z","job":"grpo","step":1,"env":"sb-1","instance":"inst-1","op":"exec","command":["pytest","-q"],"dur_ms":10,"exit":0,"ok":true}`,
		`{"schema_version":"v1","ts":"2026-10-22T12:00:00.050Z","job":"grpo","step":1,"env":"sb-1","instance":"inst-1","op":"delete","dur_ms":5,"ok":true}`,
	}, "\n") + "\n"
	if err := os.WriteFile(tracePath, []byte(traceLines), 0o644); err != nil {
		t.Fatal(err)
	}

	outDir1 := filepath.Join(dir, "out-subcmd")
	var stdout, stderr bytes.Buffer
	if code := runMain([]string{"convert-trace", "-in", tracePath, "-out", outDir1, "-scenario", "cli-trace"}, &stdout, &stderr); code != 0 {
		t.Fatalf("convert-trace subcommand failed (exit %d): %s", code, stderr.String())
	}
	sched, err := swebench.ReadSchedule(filepath.Join(outDir1, "schedule.json"))
	if err != nil {
		t.Fatalf("ReadSchedule(outDir1): %v", err)
	}
	if sched.ScenarioName != "cli-trace" || len(sched.Jobs) != 1 || len(sched.Jobs[0].Steps) != 1 {
		t.Fatalf("unexpected converted schedule: %+v", sched)
	}
	traj, err := swebench.LoadTrajectory(filepath.Join(outDir1, "trajectory.yaml"))
	if err != nil {
		t.Fatalf("LoadTrajectory(outDir1): %v", err)
	}
	if len(traj.Steps) != 2 {
		t.Fatalf("expected 2 steps in converted trajectory, got %+v", traj.Steps)
	}

	outDir2 := filepath.Join(dir, "out-flag")
	stdout.Reset()
	stderr.Reset()
	if code := runMain([]string{"run", "-convert-trace", tracePath, "-trace-out", outDir2}, &stdout, &stderr); code != 0 {
		t.Fatalf("run -convert-trace failed (exit %d): %s", code, stderr.String())
	}
	if _, err := swebench.ReadSchedule(filepath.Join(outDir2, "schedule.json")); err != nil {
		t.Fatalf("ReadSchedule(outDir2): %v", err)
	}
}

func statsOutPath(p string) string { return p }

func TestPlanPlacementCmd(t *testing.T) {
	dir := t.TempDir()
	imgA := "registry.example/swebench/a@sha256:" + strings.Repeat("a", 64)
	imgB := "registry.example/swebench/b@sha256:" + strings.Repeat("b", 64)
	manifest := filepath.Join(dir, "tasks.jsonl")
	if err := os.WriteFile(manifest, []byte(
		`{"instance_id":"a","repo":"","image":"`+imgA+`","image_bytes_compressed":3000000000,"patch":"","test_patch":"","eval_script":"","fail_to_pass":[],"pass_to_pass":[],"log_parser":"","eval_type":""}`+"\n"+
			`{"instance_id":"b","repo":"","image":"`+imgB+`","patch":"","test_patch":"","eval_script":"","fail_to_pass":[],"pass_to_pass":[],"log_parser":"","eval_type":""}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	domains := filepath.Join(dir, "domains.json")
	if err := os.WriteFile(domains, []byte(`{"domains":[
		{"name":"d0","selector":{"ate-env/domain":"d0"},"nodes":4,"node_cache_bytes":400000000000},
		{"name":"d1","selector":{"ate-env/domain":"d1"},"nodes":4,"node_cache_bytes":400000000000}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "plan", "image-placement.json")
	var stderr bytes.Buffer
	if err := planPlacementCmd([]string{"-dataset", manifest, "-domains", domains, "-default-image-size", "1GiB", "-out", out}, &stderr); err != nil {
		t.Fatalf("plan-placement: %v\n%s", err, stderr.String())
	}
	plan, err := swebench.ReadImagePlacement(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Images) != 2 || len(plan.Unplaced) != 0 {
		t.Fatalf("plan = %+v", plan)
	}
	if e := plan.Entry(imgA); e == nil || e.SizeAssumed || e.BytesUnpacked != 7500000000 || len(e.Domains) != 1 {
		t.Errorf("a = %+v", e)
	}
	if e := plan.Entry(imgB); e == nil || !e.SizeAssumed || e.BytesCompressed != 1<<30 {
		t.Errorf("b = %+v, want the assumed 1GiB", e)
	}
	if !strings.Contains(stderr.String(), "placed 2 images in 2 domains; 0 unplaced") || !strings.Contains(stderr.String(), "1 images had no size") {
		t.Errorf("summary:\n%s", stderr.String())
	}
	if err := planPlacementCmd([]string{"-dataset", manifest, "-domains", domains, "-hot-demand", "4"}, &stderr); err == nil {
		t.Error("-hot-demand without -schedule accepted")
	}
	sched := filepath.Join(dir, "schedule.json")
	all, err := swebench.LoadDataset(manifest)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := swebench.ResolveSingleJobSchedule("sched", 1, schema.ParkSuspendAsync, 1, 0, 1, all)
	if err != nil {
		t.Fatal(err)
	}
	if err := swebench.WriteSchedule(sched, resolved); err != nil {
		t.Fatal(err)
	}
	// A schedule names the images; the selection flags do not apply with it,
	// and the whole manifest supplies the sizes.
	if err := planPlacementCmd([]string{"-dataset", manifest, "-domains", domains, "-schedule", sched, "-images", "1", "-out", filepath.Join(dir, "p3.json")}, &stderr); err == nil {
		t.Error("-schedule with -images accepted")
	}
	if err := planPlacementCmd([]string{"-dataset", manifest, "-domains", domains, "-schedule", sched, "-default-image-size", "0", "-out", filepath.Join(dir, "p3.json")}, &stderr); err != nil {
		t.Fatalf("plan with schedule: %v\n%s", err, stderr.String())
	}
	withSched, err := swebench.ReadImagePlacement(filepath.Join(dir, "p3.json"))
	if err != nil {
		t.Fatal(err)
	}
	if e := withSched.Entry(imgA); e == nil || e.SizeAssumed || e.Demand != 1 {
		t.Errorf("a with schedule = %+v, want its manifest size and demand 1", e)
	}
	if err := planPlacementCmd([]string{"-dataset", manifest, "-domains", domains, "stray"}, &stderr); err == nil {
		t.Error("a positional argument was accepted")
	}
	if err := planPlacementCmd([]string{"-dataset", manifest}, &stderr); err == nil {
		t.Error("missing -domains accepted")
	}
	if runMain([]string{"plan-placement", "-dataset", manifest, "-domains", domains, "-out", filepath.Join(dir, "p2.json")}, &stderr, &stderr) != 0 {
		t.Errorf("runMain plan-placement failed:\n%s", stderr.String())
	}
}

// fakeWarmer is an in-memory swebench.ImageWarmer: goldens are ready at
// once, probes land round-robin on the two nodes of their domain.
type fakeWarmer struct {
	mu      sync.Mutex
	counter map[string]int
	nodes   map[string]string
	deleted []string
	// short leaves one node of every domain uncovered.
	short bool
	// onDelete, when set, runs after the n-th probe delete with the context
	// of the pass.
	onDelete func(n int, pass context.Context)
	pass     context.Context
	// failDeletes is how many probe deletes fail (the first ones); those
	// probes stay undeleted.
	failDeletes int
}

// spanSink is an in-process OTLP trace collector: the spans the exporter
// SetupOTel installs sends, by name, with whether each had a parent.
type spanSink struct {
	coltracepb.UnimplementedTraceServiceServer
	mu      sync.Mutex
	names   []string
	parents map[string]bool
}

func (s *spanSink) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				s.names = append(s.names, sp.GetName())
				s.parents[fmt.Sprintf("%x", sp.GetSpanId())] = len(sp.GetParentSpanId()) > 0
				if len(sp.GetParentSpanId()) == 0 {
					s.names = append(s.names, "orphan:"+sp.GetName())
				}
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (s *spanSink) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, x := range s.names {
		if x == name {
			n++
		}
	}
	return n
}

func (s *spanSink) orphans(name string) int { return s.count("orphan:" + name) }

func (s *spanSink) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.names...)
}

func (f *fakeWarmer) EnsureImageTemplate(ctx context.Context, _ string) (swebench.ImageState, error) {
	f.mu.Lock()
	f.pass = ctx
	f.mu.Unlock()
	return swebench.ImageState{Template: "default-template-x", Exists: true, Golden: swebench.GoldenReady, Tag: "golden-x"}, nil
}

func (f *fakeWarmer) ImageStatus(ctx context.Context, image string) (swebench.ImageState, error) {
	return f.EnsureImageTemplate(ctx, image)
}

func (f *fakeWarmer) CreateWith(_ context.Context, id, _ string, opts swebench.CreateOptions) (swebench.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	domain := opts.WorkerSelector["ate-env/domain"]
	f.nodes[id] = fmt.Sprintf("node-%s-%d", domain, f.counter[domain]%2)
	if f.short {
		f.nodes[id] = fmt.Sprintf("node-%s-0", domain)
	}
	f.counter[domain]++
	return &fakeWarmerSandbox{w: f, id: id}, nil
}

func (f *fakeWarmer) Node(_ context.Context, id string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nodes[id], nil
}

type fakeWarmerSandbox struct {
	w  *fakeWarmer
	id string
}

func (s *fakeWarmerSandbox) Run(context.Context, string, time.Duration) (swebench.Result, error) {
	return swebench.Result{}, nil
}
func (s *fakeWarmerSandbox) WriteFile(context.Context, string, []byte, fs.FileMode) error { return nil }
func (s *fakeWarmerSandbox) Suspend(context.Context) error                                { return nil }
func (s *fakeWarmerSandbox) Delete(context.Context) error {
	s.w.mu.Lock()
	if s.w.failDeletes > 0 {
		s.w.failDeletes--
		s.w.mu.Unlock()
		return errors.New("delete refused")
	}
	s.w.deleted = append(s.w.deleted, s.id)
	n, hook, pass := len(s.w.deleted), s.w.onDelete, s.w.pass
	s.w.mu.Unlock()
	if hook != nil {
		hook(n, pass)
	}
	return nil
}

func TestPreflightCmd(t *testing.T) {
	dir := t.TempDir()
	imgA := "registry.example/swebench/a@sha256:" + strings.Repeat("a", 64)
	plan := filepath.Join(dir, "image-placement.json")
	if err := os.WriteFile(plan, []byte(`{"schema_version":"v1","unpack_factor":1,"replication":1,
		"domains":[{"name":"d0","selector":{"ate-env/domain":"d0"},"nodes":2,"node_cache_bytes":0,"assigned_bytes":1,"image_count":1}],
		"images":[{"image":"`+imgA+`","domains":["d0"],"bytes_compressed":1,"bytes_unpacked":1}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	warmer := &fakeWarmer{counter: map[string]int{}, nodes: map[string]string{}}
	testPreflightBackend = warmer
	t.Cleanup(func() { testPreflightBackend = nil })
	events := filepath.Join(dir, "run")
	var stderr bytes.Buffer
	if err := preflightCmd([]string{"-plan", plan, "-events-dir", events, "-run-id", "pf-t", "-probes-per-node", "1", "-golden-poll", "1ms"}, &stderr); err != nil {
		t.Fatalf("preflight: %v\n%s", err, stderr.String())
	}
	report, err := os.ReadFile(filepath.Join(events, "preflight-coverage.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cov schema.PreflightCoverage
	if err := json.Unmarshal(report, &cov); err != nil {
		t.Fatal(err)
	}
	if err := cov.Validate(); err != nil {
		t.Fatal(err)
	}
	if !cov.Complete || cov.RunID != "pf-t" || len(cov.Domains) != 1 || len(cov.Domains[0].NodesSeen) != 2 {
		t.Errorf("coverage = %+v", cov)
	}
	if len(warmer.deleted) != 2 {
		t.Errorf("deleted = %v, want the two probes", warmer.deleted)
	}
	if _, err := os.Stat(filepath.Join(events, "preflight-events.jsonl")); err != nil {
		t.Errorf("events file: %v", err)
	}
	if !strings.Contains(stderr.String(), "pre-flight complete: goldens 1/1 ready") {
		t.Errorf("summary:\n%s", stderr.String())
	}
	if err := preflightCmd([]string{"-events-dir", events}, &stderr); err == nil {
		t.Error("missing -plan accepted")
	}
	if err := preflightCmd([]string{"-plan", plan, "-events-dir", events, "stray"}, &stderr); err == nil || !strings.Contains(err.Error(), "positional") {
		t.Errorf("a positional argument: err = %v, want a refusal", err)
	}
	if err := preflightCmd([]string{"-plan", plan, "-events-dir", events, "-out", filepath.Join(dir, "x.jsonl")}, &stderr); err == nil || !strings.Contains(err.Error(), "exclude") {
		t.Errorf("-events-dir with -out: err = %v, want a refusal", err)
	}
	if err := preflightCmd([]string{"-plan", plan}, &stderr); err == nil {
		t.Error("missing -events-dir and -out accepted")
	}
	outEvents := filepath.Join(dir, "nested", "pf.jsonl")
	// No -run-id: the default pf-<6 random characters> reaches the coverage.
	if runMain([]string{"preflight", "-plan", plan, "-out", outEvents, "-coverage-out", filepath.Join(dir, "cov.json"), "-probes-per-node", "1"}, &stderr, &stderr) != 0 {
		t.Fatalf("runMain preflight failed:\n%s", stderr.String())
	}
	if cov2, err := swebench.ReadPreflightCoverage(filepath.Join(dir, "cov.json")); err != nil || !regexp.MustCompile(`^pf-[a-z0-9]{6}$`).MatchString(cov2.RunID) {
		t.Errorf("default run id: coverage=%+v err=%v, want pf-<6 characters>", cov2, err)
	}
	rawEvents, err := os.ReadFile(outEvents)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(rawEvents)), "\n") {
		if err := schema.ValidateAgainstJSONSchema("event.schema.json", []byte(line)); err != nil {
			t.Errorf("preflight event %s: %v", line, err)
		}
	}
	var badCovStderr bytes.Buffer
	badCovPath := filepath.Join(outEvents, "unwritable-cov.json")
	if err := preflightCmd([]string{"-plan", plan, "-out", filepath.Join(dir, "pf2.jsonl"), "-coverage-out", badCovPath, "-probes-per-node", "1", "-golden-poll", "1ms"}, &badCovStderr); err == nil {
		t.Error("unwritable -coverage-out accepted")
	}
	if strings.Contains(badCovStderr.String(), "wrote "+badCovPath) {
		t.Errorf("stderr falsely reported wrote %s:\n%s", badCovPath, badCovStderr.String())
	}
	// -events-dir that names a run's events/ directory: the files go next to
	// it, so events/*.jsonl stays the run's shards only.
	bundle := filepath.Join(dir, "bundle")
	if err := preflightCmd([]string{"-plan", plan, "-events-dir", filepath.Join(bundle, "events"), "-probes-per-node", "1", "-golden-poll", "1ms"}, &stderr); err != nil {
		t.Fatalf("preflight -events-dir .../events: %v", err)
	}
	if inEvents, _ := filepath.Glob(filepath.Join(bundle, "events", "*")); len(inEvents) != 0 {
		t.Errorf("the pre-flight wrote into events/: %v", inEvents)
	}
	for _, name := range []string{"preflight-events.jsonl", "preflight-coverage.json"} {
		if _, err := os.Stat(filepath.Join(bundle, name)); err != nil {
			t.Errorf("%s next to events/: %v", name, err)
		}
	}
	// One path for the events and the coverage is refused before any file.
	same := filepath.Join(dir, "same.json")
	if err := preflightCmd([]string{"-plan", plan, "-out", same, "-coverage-out", filepath.Join(dir, ".", "same.json"), "-probes-per-node", "1", "-golden-poll", "1ms"}, &stderr); err == nil || !strings.Contains(err.Error(), "same file") {
		t.Errorf("-out and -coverage-out on one path: err = %v, want a refusal", err)
	}
	if _, err := os.Stat(same); err == nil {
		t.Errorf("the refused command created %s", same)
	}
}

// writeParkEachRun writes a complete -park-each steps run: two steps on one
// environment, parked inside the barrier (suspend events, no park event),
// with the loader's labels on the run event.
func writeParkEachRun(t *testing.T, dir, name, runID, park string) string {
	t.Helper()
	labels := map[string]string{"mode": "steps", "width": "1", "images": "1", "trajectory": "t",
		"rollouts": "1", "steps": "2", "park": park, "park_each": "true"}
	var buf bytes.Buffer
	for _, e := range []swebench.Event{
		{RunID: runID, Event: swebench.EventCreate, Env: "e1", Step: 1, DurMS: 100, OK: true},
		{RunID: runID, Event: swebench.EventTask, Env: "e1", Step: 1, DurMS: 50, OK: true},
		{RunID: runID, Event: swebench.EventSuspend, Env: "e1", Step: 1, DurMS: 20, OK: true},
		{RunID: runID, Event: swebench.EventBarrier, Step: 1, DurMS: 200, OK: true, Count: 1},
		{RunID: runID, Event: swebench.EventWake, Env: "e1", Step: 2, DurMS: 30, OK: true},
		{RunID: runID, Event: swebench.EventTask, Env: "e1", Step: 2, DurMS: 50, OK: true},
		{RunID: runID, Event: swebench.EventSuspend, Env: "e1", Step: 2, DurMS: 20, OK: true},
		{RunID: runID, Event: swebench.EventBarrier, Step: 2, DurMS: 120, OK: true, Count: 1},
		{RunID: runID, Event: swebench.EventDelete, Env: "e1", Step: 2, DurMS: 10, OK: true},
		{RunID: runID, Event: swebench.EventRun, Name: "steps", DurMS: 1000, OK: true, Count: 2, Labels: labels},
	} {
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

// A -park-each run records no park event; report and report -arm take the
// park mode from the run's park label, not from the suspend events.
func TestReportParkModeFromLabel(t *testing.T) {
	dir := t.TempDir()
	a := writeParkEachRun(t, dir, "a.jsonl", "r1", "suspend-async")
	b := writeParkEachRun(t, dir, "b.jsonl", "r2", "suspend-async")

	single := filepath.Join(dir, "report.json")
	if err := reportCmd([]string{a, "-json", single}, io.Discard); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(single)
	if err != nil {
		t.Fatal(err)
	}
	var rep swebench.Report
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.ParkMode != "suspend-async" || rep.LifecyclePolicy == nil || rep.LifecyclePolicy.ParkMode != "suspend-async" {
		t.Fatalf("report.json park_mode %q lifecycle %+v, want suspend-async", rep.ParkMode, rep.LifecyclePolicy)
	}

	arms := filepath.Join(dir, "arms.json")
	var text bytes.Buffer
	if err := reportCmd([]string{"-arm", "local=" + a + "," + b, "-json", arms}, &text); err != nil {
		t.Fatal(err)
	}
	flat := strings.Join(strings.Fields(text.String()), " ")
	for _, want := range []string{"reps 2", "park suspend-async", "park_each true"} {
		if !strings.Contains(flat, want) {
			t.Errorf("-arm output lacks %q:\n%s", want, text.String())
		}
	}
	raw, err = os.ReadFile(arms)
	if err != nil {
		t.Fatal(err)
	}
	var reps []swebench.Report
	if err := json.Unmarshal(raw, &reps); err != nil {
		t.Fatal(err)
	}
	if len(reps) != 1 || reps[0].ParkMode != "suspend-async" || reps[0].LifecyclePolicy == nil || reps[0].LifecyclePolicy.ParkMode != "suspend-async" {
		t.Fatalf("-arm -json: %+v", reps)
	}
}

// -placement is read and checked before -dry-run returns and whatever backend
// runs: the plan must know every selected image, and only the ate-env API
// driver places environments.
func TestRunCmdPlacementFlag(t *testing.T) {
	const digest = "sha256:" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	dir := t.TempDir()
	manifest := filepath.Join(dir, "cmd.jsonl")
	if err := os.WriteFile(manifest, []byte(
		`{"kind":"command","instance_id":"a","image":"reg/a@`+digest+`","command":"true"}`+"\n"+
			`{"kind":"command","instance_id":"b","image":"reg/b@`+digest+`","command":"true"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	domains := `"domains":[{"name":"d0","selector":{"ate-env/domain":"d0"},"nodes":1,"node_cache_bytes":0,"assigned_bytes":0,"image_count":1}]`
	partial := plan("partial.json", `{"schema_version":"v1","unpack_factor":1,"replication":1,`+domains+`,"images":[{"image":"reg/a@`+digest+`","domains":["d0"],"bytes_compressed":1,"bytes_unpacked":1}]}`)
	full := plan("full.json", `{"schema_version":"v1","unpack_factor":1,"replication":1,`+domains+`,"images":[{"image":"reg/a@`+digest+`","domains":["d0"],"bytes_compressed":1,"bytes_unpacked":1}],"unplaced":[{"image":"reg/b@`+digest+`","reason":"over budget"}]}`)

	err := runCmd([]string{"-dataset", manifest, "-dry-run", "-placement", partial})
	if err == nil || !strings.Contains(err.Error(), "neither places nor lists as unplaced 1 of the 2") {
		t.Errorf("-dry-run with a plan missing an image: err = %v, want the missing image named", err)
	}
	if err := runCmd([]string{"-dataset", manifest, "-dry-run", "-placement", full}); err != nil {
		t.Errorf("-dry-run with a complete plan: %v", err)
	}
	err = runCmd([]string{"-dataset", manifest, "-dry-run", "-placement", full, "-backend", "agent-sandbox", "-access", "sdk"})
	if err == nil || !strings.Contains(err.Error(), "cannot place") {
		t.Errorf("-placement on agent-sandbox: err = %v, want a refusal", err)
	}
	err = runCmd([]string{"-dataset", manifest, "-dry-run", "-placement", full, "-access", "sdk"})
	if err == nil || !strings.Contains(err.Error(), "cannot place") {
		t.Errorf("-placement on substrate/sdk: err = %v, want a refusal", err)
	}
	err = runCmd([]string{"-dataset", manifest, "-dry-run", "-placement", filepath.Join(dir, "none.json")})
	if err == nil {
		t.Error("a missing plan file was accepted")
	}

	// With the test backend the run goes through and the run event carries
	// the plan's name.
	testBackendOverride = e2eFakeBackend{}
	t.Cleanup(func() { testBackendOverride = nil })
	events := filepath.Join(dir, "events.jsonl")
	if err := runCmd([]string{"-dataset", manifest, "-placement", full, "-run-id", "pl", "-out", events, "-width", "2"}); err != nil {
		t.Fatalf("run with -placement: %v", err)
	}
	raw, err := os.ReadFile(events)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"placement":"full.json"`) {
		t.Errorf("run event lacks the placement label:\n%s", raw)
	}
}

func TestReportCmdPreflightCoverageTakesOneEventsFile(t *testing.T) {
	err := reportCmd([]string{"-arm", "a=nofile.jsonl", "-preflight-coverage", "cov.json"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "-preflight-coverage") {
		t.Errorf("-arm with -preflight-coverage: err = %v, want a refusal naming the flag", err)
	}
}

// e2ePreflightBackend is a Backend that is also an ImageWarmer, for run
// -preflight: the fake warmer's probes, the end-to-end fake's environments.
type e2ePreflightBackend struct {
	e2eFakeBackend
	*fakeWarmer
}

func TestRunCmdPreflight(t *testing.T) {
	dir := t.TempDir()
	imgA := "registry.example/swebench/a@sha256:" + strings.Repeat("a", 64)
	manifest := filepath.Join(dir, "tasks.jsonl")
	if err := os.WriteFile(manifest, []byte(`{"instance_id":"a","repo":"r","image":"`+imgA+`","patch":"diff","eval_script":"#!/bin/sh\nexit 0","log_parser":"parse_log_pytest"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan := filepath.Join(dir, "image-placement.json")
	if err := os.WriteFile(plan, []byte(`{"schema_version":"v1","unpack_factor":1,"replication":1,
		"domains":[{"name":"d0","selector":{"ate-env/domain":"d0"},"nodes":2,"node_cache_bytes":0,"assigned_bytes":1,"image_count":1}],
		"images":[{"image":"`+imgA+`","domains":["d0"],"bytes_compressed":1,"bytes_unpacked":1}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	warmer := &fakeWarmer{counter: map[string]int{}, nodes: map[string]string{}}
	testBackendOverride = e2ePreflightBackend{fakeWarmer: warmer}
	t.Cleanup(func() { testBackendOverride = nil })

	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-run-id", "pf-run", "-out", filepath.Join(dir, "x.jsonl"), "-preflight"}); err == nil || !strings.Contains(err.Error(), "needs -placement") {
		t.Fatalf("-preflight without -placement: err = %v, want a refusal", err)
	}
	events := filepath.Join(dir, "run")
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-run-id", "pf-run", "-events-dir", events, "-placement", plan, "-preflight"}); err != nil {
		t.Fatalf("run -preflight: %v", err)
	}
	// The pre-flight's files sit next to the run's events directory, not in
	// it, and the coverage is complete over the run's image.
	cov, err := swebench.ReadPreflightCoverage(filepath.Join(events, "preflight-coverage.json"))
	if err != nil {
		t.Fatal(err)
	}
	// The probes are named after the run and the worker, within WarmImage's
	// 20 characters, so two loaders on one cluster never name a probe alike.
	if !cov.Complete || len(cov.Domains) != 1 || len(cov.Domains[0].NodesSeen) != 2 || cov.RunID != preflightRunID("pf-run", "") || !regexp.MustCompile(`^pf-[0-9a-f]{6}$`).MatchString(cov.RunID) {
		t.Errorf("coverage = %+v, want run id %s", cov, preflightRunID("pf-run", ""))
	}
	if preflightRunID("pf-run", "w1") == preflightRunID("pf-run", "w2") || preflightRunID("a", "") == preflightRunID("b", "") {
		t.Error("the pre-flight run id must differ per run id and per worker")
	}
	if _, err := os.Stat(filepath.Join(events, "preflight-events.jsonl")); err != nil {
		t.Errorf("preflight events: %v", err)
	}
	// The pre-flight's defaults: 2 probes per node on the plan's 2 nodes.
	if len(warmer.deleted) != 4 {
		t.Errorf("probes deleted = %v, want the round's four", warmer.deleted)
	}
	shards, _ := filepath.Glob(filepath.Join(events, "events", "*.jsonl"))
	if len(shards) != 1 {
		t.Fatalf("event shards = %v, want the run's one; the probes must not be in it", shards)
	}
	raw, err := os.ReadFile(shards[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "preflight_probe") {
		t.Errorf("the run's shard carries pre-flight events:\n%s", raw)
	}
	// The report reads the coverage as Rule 1 evidence.
	summary := filepath.Join(dir, "summary.json")
	if err := reportCmd([]string{shards[0], "-validate", "-footing", "warm", "-preflight-coverage", filepath.Join(events, "preflight-coverage.json"), "-json", summary}); err != nil {
		t.Fatalf("report: %v", err)
	}
	rep, err := os.ReadFile(summary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rep), "pre-flight coverage complete") {
		t.Errorf("report validity evidence missing:\n%s", rep)
	}

	// With tracing on, the pre-flight's events become spans under the run
	// span, as the run's own events do: the OTLP exporter SetupOTel installs
	// sends them to an in-process collector.
	sink := &spanSink{parents: map[string]bool{}}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	collector := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(collector, sink)
	go func() { _ = collector.Serve(lis) }()
	t.Cleanup(collector.Stop)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+lis.Addr().String())
	events2 := filepath.Join(dir, "run-traced")
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-run-id", "pf-run-t", "-events-dir", events2, "-placement", plan, "-preflight", "-enable-tracing"}); err != nil {
		t.Fatalf("run -preflight -enable-tracing: %v", err)
	}
	probes, runSpans := sink.count("preflight_probe"), sink.count(tracingServiceName+".run")
	if probes != 4 || runSpans != 1 {
		t.Errorf("spans at the collector: %d preflight_probe (want 4), %d run (want 1); all: %v", probes, runSpans, sink.all())
	}
	if orphans := sink.orphans("preflight_probe"); orphans != 0 {
		t.Errorf("%d preflight_probe spans without a parent; want the run span above each", orphans)
	}

	// An incomplete pre-flight is logged and the run goes on; a coverage
	// that cannot be written fails the command, so no stale file is trusted.
	warmer.short = true
	events3 := filepath.Join(dir, "run-short")
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-run-id", "pf-run-s", "-events-dir", events3, "-placement", plan, "-preflight"}); err != nil {
		t.Fatalf("run with an incomplete pre-flight: %v, want the run to go on", err)
	}
	if cov, err := swebench.ReadPreflightCoverage(filepath.Join(events3, "preflight-coverage.json")); err != nil || cov.Complete {
		t.Errorf("short coverage: %+v err=%v, want written and incomplete", cov, err)
	}
	warmer.short = false
	events4 := filepath.Join(dir, "run-unwritable")
	if err := os.MkdirAll(filepath.Join(events4, "preflight-coverage.json"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-run-id", "pf-run-u", "-events-dir", events4, "-placement", plan, "-preflight"}); err == nil || !strings.Contains(err.Error(), "writing coverage") {
		t.Fatalf("coverage not writable: err = %v, want the command to fail", err)
	}

	// A plan that places none of the run's images fails before any file,
	// also under -dry-run.
	emptyPlan := filepath.Join(dir, "empty-plan.json")
	if err := os.WriteFile(emptyPlan, []byte(`{"schema_version":"v1","unpack_factor":1,"replication":1,
		"domains":[{"name":"d0","selector":{"ate-env/domain":"d0"},"nodes":2,"node_cache_bytes":0,"assigned_bytes":0,"image_count":0}],
		"images":[],"unplaced":[{"image":"`+imgA+`","reason":"no domain has room"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-run-id", "pf-run-e", "-events-dir", filepath.Join(dir, "run-empty"), "-placement", emptyPlan, "-preflight", "-dry-run"}); err == nil || !strings.Contains(err.Error(), "no image of the pass is placed") {
		t.Fatalf("-preflight -dry-run over an empty plan: err = %v, want the pass's refusal", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "run-empty", "preflight-events.jsonl")); err == nil {
		t.Error("an events file was created for a refused pass")
	}

	// -events-dir that names the events/ directory itself: the shard goes
	// into it (NewShardRecorder), the pre-flight's files next to it, so
	// events/*.jsonl stays the run's shards only.
	bundle := filepath.Join(dir, "bundle")
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-trajectory", "workload-file-read-one", "-run-id", "pf-run-b", "-events-dir", filepath.Join(bundle, "events") + "/", "-placement", plan, "-preflight"}); err != nil {
		t.Fatalf("run -preflight -events-dir .../events: %v", err)
	}
	if inEvents, _ := filepath.Glob(filepath.Join(bundle, "events", "*")); len(inEvents) != 1 || filepath.Base(inEvents[0]) != "worker-0.jsonl" {
		t.Errorf("%s holds %v, want the run's shard only", filepath.Join(bundle, "events"), inEvents)
	}
	for _, name := range []string{"preflight-events.jsonl", "preflight-coverage.json"} {
		if _, err := os.Stat(filepath.Join(bundle, name)); err != nil {
			t.Errorf("%s next to events/: %v", name, err)
		}
	}
	// The same for -out inside an events/ directory.
	bundle2 := filepath.Join(dir, "bundle-out")
	if err := os.MkdirAll(filepath.Join(bundle2, "events"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-trajectory", "workload-file-read-one", "-run-id", "pf-run-o", "-out", filepath.Join(bundle2, "events", "w0.jsonl"), "-placement", plan, "-preflight"}); err != nil {
		t.Fatalf("run -preflight -out .../events/w0.jsonl: %v", err)
	}
	if inEvents, _ := filepath.Glob(filepath.Join(bundle2, "events", "*")); len(inEvents) != 1 {
		t.Errorf("%s holds %v, want w0.jsonl only", filepath.Join(bundle2, "events"), inEvents)
	}
	if _, err := os.Stat(filepath.Join(bundle2, "preflight-coverage.json")); err != nil {
		t.Errorf("coverage next to events/: %v", err)
	}
	// -out that is one of the pre-flight's own files is refused before any
	// file is written: the run and the pass would write the same file.
	for _, name := range []string{"preflight-events.jsonl", "preflight-coverage.json"} {
		clash := filepath.Join(dir, name)
		err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-trajectory", "workload-file-read-one", "-run-id", "pf-run-c", "-out", clash, "-placement", plan, "-preflight"})
		if err == nil || !strings.Contains(err.Error(), "the pre-flight writes") {
			t.Errorf("-out %s with -preflight: err = %v, want a refusal", name, err)
		}
		if _, serr := os.Stat(clash); serr == nil {
			t.Errorf("the refused command created %s", clash)
		}
	}

	// A cancel that lands during the pass (here: SIGINT once the last probe
	// is deleted) fails the command and no task runs.
	warmer.deleted = nil
	warmer.onDelete = func(n int, pass context.Context) {
		if n == 4 {
			if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
				t.Error(err)
			}
			<-pass.Done()
		}
	}
	events5 := filepath.Join(dir, "run-cancelled")
	err = runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-trajectory", "workload-file-read-one", "-run-id", "pf-run-x", "-events-dir", events5, "-placement", plan, "-preflight"})
	warmer.onDelete = nil
	if err == nil || !strings.Contains(err.Error(), "-preflight:") || !errors.Is(err, context.Canceled) {
		t.Fatalf("run -preflight cancelled after the last probe: err = %v, want -preflight: context canceled", err)
	}
	if raw, _ := os.ReadFile(filepath.Join(events5, "events", "worker-0.jsonl")); len(raw) != 0 {
		t.Errorf("a cancelled pre-flight ran tasks:\n%s", raw)
	}

	// A refused -preflight (the backend is not the ate-env API) does not
	// truncate an existing events file.
	testBackendOverride = e2eFakeBackend{}
	keep := filepath.Join(dir, "keep.jsonl")
	if err := os.WriteFile(keep, []byte("{\"event\":\"run\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-run-id", "pf-run", "-out", keep, "-placement", plan, "-preflight"}); err == nil || !strings.Contains(err.Error(), "needs the ate-env API backend") {
		t.Fatalf("-preflight on a backend without WarmImage: err = %v, want a refusal", err)
	}
	if raw, _ := os.ReadFile(keep); !strings.Contains(string(raw), `"event":"run"`) {
		t.Errorf("the refusal truncated %s: %q", keep, raw)
	}
}

// A pass that covers every node but cannot delete a probe returns an error
// with a complete coverage. The run goes on, and its log says that: coverage
// complete, one probe not deleted, not "preflight incomplete".
func TestRunCmdPreflightUndeletedProbesLog(t *testing.T) {
	dir := t.TempDir()
	imgA := "registry.example/swebench/a@sha256:" + strings.Repeat("a", 64)
	manifest := filepath.Join(dir, "tasks.jsonl")
	if err := os.WriteFile(manifest, []byte(`{"instance_id":"a","repo":"r","image":"`+imgA+`","patch":"diff","eval_script":"#!/bin/sh\nexit 0","log_parser":"parse_log_pytest"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan := filepath.Join(dir, "image-placement.json")
	if err := os.WriteFile(plan, []byte(`{"schema_version":"v1","unpack_factor":1,"replication":1,
		"domains":[{"name":"d0","selector":{"ate-env/domain":"d0"},"nodes":2,"node_cache_bytes":0,"assigned_bytes":1,"image_count":1}],
		"images":[{"image":"`+imgA+`","domains":["d0"],"bytes_compressed":1,"bytes_unpacked":1}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	// runLogged runs the command and returns the "the run goes on" lines of
	// its log, which goes to os.Stderr.
	runLogged := func(warmer *fakeWarmer, events string) []string {
		t.Helper()
		testBackendOverride = e2ePreflightBackend{fakeWarmer: warmer}
		logPath := filepath.Join(dir, filepath.Base(events)+".stderr")
		logFile, err := os.Create(logPath)
		if err != nil {
			t.Fatal(err)
		}
		stderr := os.Stderr
		os.Stderr = logFile
		err = runCmd([]string{"-dataset", manifest, "-mode", "eval", "-width", "1", "-trajectory", "workload-file-read-one", "-run-id", "pf-run-l", "-events-dir", events, "-placement", plan, "-preflight"})
		os.Stderr = stderr
		logFile.Close()
		if err != nil {
			t.Fatalf("run -preflight: %v, want the run to go on", err)
		}
		raw, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		var lines []string
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(line, "the run goes on") {
				lines = append(lines, line)
			}
		}
		return lines
	}
	t.Cleanup(func() { testBackendOverride = nil })

	events := filepath.Join(dir, "run-leftover")
	lines := runLogged(&fakeWarmer{counter: map[string]int{}, nodes: map[string]string{}, failDeletes: 1}, events)
	cov, err := swebench.ReadPreflightCoverage(filepath.Join(events, "preflight-coverage.json"))
	if err != nil {
		t.Fatal(err)
	}
	if left := swebench.UndeletedProbes(cov); !cov.Complete || len(left) != 1 {
		t.Fatalf("coverage complete = %v, undeleted = %v; want complete with one probe left", cov.Complete, left)
	}
	if len(lines) != 1 || strings.Contains(lines[0], "preflight incomplete") || strings.Contains(lines[0], "cold first boots") ||
		!strings.Contains(lines[0], "preflight coverage complete") || !strings.Contains(lines[0], "1 probes not deleted and still holding slots") {
		t.Errorf("log of a complete pass with one probe left = %q, want one line: coverage complete, 1 probes not deleted", lines)
	}
	if raw, _ := os.ReadFile(filepath.Join(events, "events", "worker-0.jsonl")); !strings.Contains(string(raw), `"event":"run"`) {
		t.Errorf("the run did not go on after the pass:\n%s", raw)
	}

	// A pass that is short keeps the incomplete text.
	lines = runLogged(&fakeWarmer{counter: map[string]int{}, nodes: map[string]string{}, short: true}, filepath.Join(dir, "run-short"))
	if len(lines) != 1 || !strings.Contains(lines[0], "preflight incomplete") || !strings.Contains(lines[0], "cold first boots") || strings.Contains(lines[0], "not deleted") {
		t.Errorf("log of a short pass = %q, want the incomplete line", lines)
	}
	// A clean pass logs neither.
	if lines = runLogged(&fakeWarmer{counter: map[string]int{}, nodes: map[string]string{}}, filepath.Join(dir, "run-clean")); len(lines) != 0 {
		t.Errorf("log of a clean pass = %q, want no warning", lines)
	}
}

// TestPreflightVerdict covers what run -preflight does with the pass's
// outcome; the cancel that lands after the pass returned without an error
// cannot be timed through runCmd.
func TestPreflightVerdict(t *testing.T) {
	short := &schema.PreflightCoverage{}
	complete := &schema.PreflightCoverage{Complete: true}
	leftover := func(complete bool, ids ...string) *schema.PreflightCoverage {
		return &schema.PreflightCoverage{Complete: complete, Domains: []schema.DomainCoverage{{Images: []schema.ImageCoverage{{Complete: complete, UndeletedProbes: ids}}}}}
	}
	incomplete := errors.New("pre-flight incomplete: d0 short")
	undeleted := errors.New("pre-flight complete, 2 probes left undeleted (first: pf-x-0-0-1-0); sweep them with ate-env delete")
	const coldBoots = "preflight incomplete; the run goes on and pays cold first boots where it is short"
	for _, tc := range []struct {
		name     string
		ctxErr   error
		coverage *schema.PreflightCoverage
		perr     error
		want     string // "" = the run goes on
		warn     string // the whole warning of a run that goes on
	}{
		{name: "complete", coverage: complete},
		{name: "incomplete, coverage written", coverage: short, perr: incomplete, warn: coldBoots},
		{name: "complete, probes left", coverage: leftover(true, "pf-x-0-0-1-0", "pf-x-0-0-1-1"), perr: undeleted,
			warn: "preflight coverage complete; 2 probes not deleted and still holding slots; the run goes on"},
		{name: "incomplete, probes left", coverage: leftover(false, "pf-x-0-0-1-0"), perr: incomplete,
			warn: coldBoots + "; 1 probes not deleted and still holding slots"},
		{name: "complete, an error that names no probe", coverage: complete, perr: errors.New("other"),
			warn: "preflight returned an error with a complete coverage; the run goes on"},
		{name: "no coverage", perr: errors.New("writing coverage to x: denied"), want: "-preflight: writing coverage to x: denied"},
		{name: "no coverage, no error", want: "-preflight: no coverage"},
		{name: "cancelled during the pass", ctxErr: context.Canceled, coverage: short, perr: context.Canceled, want: "-preflight: context canceled"},
		{name: "cancelled after a complete pass", ctxErr: context.Canceled, coverage: complete, want: "-preflight: context canceled"},
		{name: "cancelled, probes left", ctxErr: context.Canceled, coverage: leftover(true, "pf-x-0-0-1-0"), perr: context.Canceled, want: "-preflight: context canceled"},
		{name: "cancelled, no coverage", ctxErr: context.Canceled, perr: errors.New("writing coverage to x: denied (after: context canceled)"), want: "-preflight: writing coverage to x: denied (after: context canceled)"},
	} {
		warn, err := preflightVerdict(tc.ctxErr, tc.coverage, tc.perr)
		if warn != tc.warn {
			t.Errorf("%s: warning = %q, want %q", tc.name, warn, tc.warn)
		}
		if tc.want == "" {
			if err != nil {
				t.Errorf("%s: err = %v, want the run to go on", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}

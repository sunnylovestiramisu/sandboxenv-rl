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
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

type recipeFakeBackend struct {
	mu              sync.Mutex
	creates         int
	awake           int
	maxAwake        int
	maxParked       int
	maxRunCmdLen    int
	maxWrittenBytes int
	parkedByID      map[string]bool
	opDelay         time.Duration
}

type recipeWrittenFile struct {
	content []byte
	mode    fs.FileMode
}

type recipeFakeSandbox struct {
	backend *recipeFakeBackend
	id      string
	mu      sync.Mutex
	files   map[string]recipeWrittenFile
}

func (b *recipeFakeBackend) Create(_ context.Context, id, _ string) (swebench.Sandbox, error) {
	b.mu.Lock()
	if b.parkedByID == nil {
		b.parkedByID = make(map[string]bool)
	}
	b.creates++
	b.awake++
	if b.awake > b.maxAwake {
		b.maxAwake = b.awake
	}
	delay := b.opDelay
	b.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	return &recipeFakeSandbox{backend: b, id: id}, nil
}

func (b *recipeFakeBackend) Delete(_ context.Context, id string) error {
	b.onDelete(id)
	return nil
}

func (b *recipeFakeBackend) onDelete(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.parkedByID[id] {
		delete(b.parkedByID, id)
		return
	}
	if b.awake > 0 {
		b.awake--
	}
}

func (s *recipeFakeSandbox) Run(_ context.Context, command string, _ time.Duration) (swebench.Result, error) {
	s.backend.mu.Lock()
	if len(command) > s.backend.maxRunCmdLen {
		s.backend.maxRunCmdLen = len(command)
	}
	if s.backend.parkedByID[s.id] {
		delete(s.backend.parkedByID, s.id)
		s.backend.awake++
		if s.backend.awake > s.backend.maxAwake {
			s.backend.maxAwake = s.backend.awake
		}
	}
	delay := s.backend.opDelay
	s.backend.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}

	switch {
	case strings.Contains(command, "/tmp/grade.py"):
		return swebench.Result{
			ExitCode: 0,
			Stdout:   `{"valid":true,"resolved":true,"f2p":"1/1","p2p":"2/2"}` + "\n",
		}, nil
	case strings.Contains(command, "/tmp/r2e-grade.sh"):
		s.mu.Lock()
		f, ok := s.files["/tmp/r2e-grade.sh"]
		s.mu.Unlock()
		if !ok {
			return swebench.Result{ExitCode: 127, Stderr: "missing /tmp/r2e-grade.sh\n"}, nil
		}
		// /bin/bash reads the script; the write step sets no mode, so the
		// WriteSpec default applies (and the Python arm runs no chmod).
		if f.mode.Perm() != 0o644 {
			return swebench.Result{ExitCode: 126, Stderr: fmt.Sprintf("unexpected mode %o\n", f.mode.Perm())}, nil
		}
		if !strings.Contains(string(f.content), "run_tests.sh") {
			return swebench.Result{ExitCode: 1, Stderr: "grade script missing run_tests.sh\n"}, nil
		}
		return swebench.Result{
			ExitCode: 0,
			Stdout:   `{"valid":true,"reward":1,"detail":"matched 2/2 tests"}` + "\n",
		}, nil
	case strings.Contains(command, "run_tests.sh"):
		return swebench.Result{
			ExitCode: 0,
			Stdout:   `{"valid":true,"reward":1,"detail":"matched 2/2 tests"}` + "\n",
		}, nil
	default:
		return swebench.Result{ExitCode: 0, Stdout: "42\n"}, nil
	}
}

func (s *recipeFakeSandbox) WriteFile(_ context.Context, path string, content []byte, mode fs.FileMode) error {
	s.mu.Lock()
	if s.files == nil {
		s.files = make(map[string]recipeWrittenFile)
	}
	cp := append([]byte(nil), content...)
	s.files[path] = recipeWrittenFile{content: cp, mode: mode}
	s.mu.Unlock()

	s.backend.mu.Lock()
	if len(content) > s.backend.maxWrittenBytes {
		s.backend.maxWrittenBytes = len(content)
	}
	s.backend.mu.Unlock()
	return nil
}

func (s *recipeFakeSandbox) Suspend(context.Context) error {
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	if !s.backend.parkedByID[s.id] {
		s.backend.parkedByID[s.id] = true
		if len(s.backend.parkedByID) > s.backend.maxParked {
			s.backend.maxParked = len(s.backend.parkedByID)
		}
		if s.backend.awake > 0 {
			s.backend.awake--
		}
	}
	return nil
}

func (s *recipeFakeSandbox) Delete(context.Context) error {
	s.backend.onDelete(s.id)
	return nil
}

func TestMixedR2ESWEBench200ConcurrencyShards(t *testing.T) {
	r2eTrajPath := filepath.Join("..", "..", "..", "benchmarking", "swebench", "r2e-gold.yaml")
	var assetsOut bytes.Buffer
	if err := assetsCmd([]string{r2eTrajPath}, &assetsOut); err != nil {
		t.Fatalf("trajectory-assets r2e-gold.yaml: %v", err)
	}
	if assetsOut.String() != "" {
		t.Fatalf("expected no external write.file assets in r2e-gold.yaml, got %q", assetsOut.String())
	}

	// Load and substitute zero think times so a 200-actor, 2-step simulation finishes in milliseconds.
	rawR2ETraj, err := os.ReadFile(r2eTrajPath)
	if err != nil {
		t.Fatalf("read r2e-gold.yaml: %v", err)
	}
	dir := t.TempDir()
	fastR2ETraj := filepath.Join(dir, "r2e-gold-fast.yaml")
	fastYAML := strings.NewReplacer("think: 1s", "think: 0s", "think: 2s", "think: 0s", "think: 3s", "think: 0s").Replace(string(rawR2ETraj))
	if err := os.WriteFile(fastR2ETraj, []byte(fastYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	rawSWETraj, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "benchmarking", "swebench", "trajectories", "swebench-gold.yaml"))
	if err != nil {
		t.Fatalf("read swebench-gold.yaml: %v", err)
	}
	fastSWETraj := filepath.Join(dir, "swebench-gold-fast.yaml")
	fastSWEYAML := strings.NewReplacer("think: 1s", "think: 0s", "think: 2s", "think: 0s", "think: 3s", "think: 0s").Replace(string(rawSWETraj))
	if err := os.WriteFile(fastSWETraj, []byte(fastSWEYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	graderPy, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "benchmarking", "swebench", "trajectories", "swebench-grade.py"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "swebench-grade.py"), graderPy, 0o644); err != nil {
		t.Fatal(err)
	}

	largeR2ECmd := strings.Repeat("# numpy expected_output_json padding\n", 7500) + "/bin/bash run_tests.sh\n"
	var r2eBuf, sweBuf bytes.Buffer
	for i := 0; i < 20; i++ {
		cmdStr := "/bin/bash run_tests.sh"
		if i == 0 {
			cmdStr = largeR2ECmd
		}
		r2eRec := map[string]any{
			"kind":               "command",
			"instance_id":        fmt.Sprintf("r2e-inst-%02d", i),
			"repo":               "demo/r2e",
			"image":              fmt.Sprintf("us-docker.pkg.dev/p/r2e/img-%02d@sha256:%064x", i, 100+i),
			"setup_command":      "test -d /testbed",
			"reset_command":      "cd /testbed && git reset -q --hard",
			"command":            cmdStr,
			"expected_exit_code": 0,
		}
		b, _ := json.Marshal(r2eRec)
		r2eBuf.Write(b)
		r2eBuf.WriteByte('\n')

		sweRec := map[string]any{
			"instance_id":  fmt.Sprintf("django__django-%d", 11000+i),
			"repo":         "django/django",
			"image":        fmt.Sprintf("us-docker.pkg.dev/p/swe/img-%02d@sha256:%064x", i, 200+i),
			"patch":        "diff --git a/a.py b/a.py\n",
			"test_patch":   "diff --git a/t.py b/t.py\n",
			"eval_script":  "#!/bin/bash\npytest\n",
			"fail_to_pass": []string{"test_a"},
			"pass_to_pass": []string{"test_b"},
			"log_parser":   "parse_log_pytest",
			"eval_type":    "pass_and_fail",
		}
		sb, _ := json.Marshal(sweRec)
		sweBuf.Write(sb)
		sweBuf.WriteByte('\n')
	}
	r2eManifest := filepath.Join(dir, "r2e-20.jsonl")
	sweManifest := filepath.Join(dir, "swe-20.jsonl")
	if err := os.WriteFile(r2eManifest, r2eBuf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sweManifest, sweBuf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	fake := &recipeFakeBackend{}
	prev := testBackendOverride
	testBackendOverride = fake
	t.Cleanup(func() { testBackendOverride = prev })

	artifactDir := filepath.Join(dir, "artifacts")
	var wg sync.WaitGroup
	var sweCode, r2eCode int
	var sweErr, r2eErr bytes.Buffer

	wg.Add(2)
	go func() {
		defer wg.Done()
		var out bytes.Buffer
		sweCode = runMain([]string{
			"run",
			"-dataset", sweManifest,
			"-run-id", "r2eswe20-swe",
			"-images", "20",
			"-rollouts", "5",
			"-width", "100",
			"-mode", "steps",
			"-steps", "2",
			"-park", "suspend",
			"-trajectory", fastSWETraj,
			"-events-dir", artifactDir,
			"-worker", "swe",
			"-job", "swe",
		}, &out, &sweErr)
	}()
	go func() {
		defer wg.Done()
		var out bytes.Buffer
		r2eCode = runMain([]string{
			"run",
			"-dataset", r2eManifest,
			"-run-id", "r2eswe20-r2e",
			"-images", "20",
			"-rollouts", "5",
			"-width", "100",
			"-mode", "steps",
			"-steps", "2",
			"-park", "suspend",
			"-trajectory", fastR2ETraj,
			"-events-dir", artifactDir,
			"-worker", "r2e",
			"-job", "r2e",
		}, &out, &r2eErr)
	}()
	wg.Wait()

	if sweCode != 0 {
		t.Fatalf("swe shard exited %d: %s", sweCode, sweErr.String())
	}
	if r2eCode != 0 {
		t.Fatalf("r2e shard exited %d: %s", r2eCode, r2eErr.String())
	}
	if fake.creates != 200 {
		t.Fatalf("expected 200 created sandboxes (100 swe + 100 r2e), got %d", fake.creates)
	}
	if fake.maxWrittenBytes < 250*1024 {
		t.Fatalf("expected >250 KiB R2E command staged via WriteFile, got maxWrittenBytes=%d", fake.maxWrittenBytes)
	}
	if fake.maxRunCmdLen >= 4096 {
		t.Fatalf("expected all Run commands to stay well under Linux MAX_ARG_STRLEN (<4 KiB), got maxRunCmdLen=%d", fake.maxRunCmdLen)
	}

	var combined bytes.Buffer
	for _, shard := range []string{"r2e.jsonl", "swe.jsonl"} {
		raw, err := os.ReadFile(filepath.Join(artifactDir, "events", shard))
		if err != nil {
			t.Fatalf("read %s: %v", shard, err)
		}
		combined.Write(raw)
	}
	rep, err := swebench.Summarize(&combined)
	if err != nil {
		t.Fatalf("Summarize combined shards: %v", err)
	}
	if rep.Tasks != 400 || rep.Rewarded != 400 || rep.RewardRate != 1.0 {
		t.Fatalf("expected 400/400 rewarded tasks across 2 steps, got tasks=%d rewarded=%d rate=%v", rep.Tasks, rep.Rewarded, rep.RewardRate)
	}
	if rep.DistinctInstances != 40 || rep.DistinctEnvs != 200 || rep.PeakActiveSandboxes != 200 {
		t.Fatalf("expected 40 instances, 200 envs, 200 peak active sandboxes; got instances=%d envs=%d peak=%d",
			rep.DistinctInstances, rep.DistinctEnvs, rep.PeakActiveSandboxes)
	}
	if rep.Jobs["swe"].Tasks != 200 || rep.Jobs["r2e"].Tasks != 200 {
		t.Fatalf("expected 200 tasks per job, got swe=%+v r2e=%+v", rep.Jobs["swe"], rep.Jobs["r2e"])
	}
	if rep.Interference == nil || rep.Interference.ActiveJobs != 2 {
		t.Fatalf("expected interference across 2 active jobs, got %+v", rep.Interference)
	}

	// Verify oversubscribed replica rollouts (10 images * 4 rollouts = 40 per shard = 80 total envs)
	// capped at concurrency 30 (width 15 per shard) via -park-each never exceed 30 awake sandboxes.
	fakeOver := &recipeFakeBackend{}
	testBackendOverride = fakeOver
	overDir := filepath.Join(dir, "artifacts-over")
	var overWG sync.WaitGroup
	var sweOverCode, r2eOverCode int
	overWG.Add(2)
	go func() {
		defer overWG.Done()
		var out, errBuf bytes.Buffer
		sweOverCode = runMain([]string{
			"run", "-dataset", sweManifest, "-run-id", "r2eswe-ov-swe",
			"-images", "10", "-rollouts", "4", "-width", "15",
			"-mode", "steps", "-steps", "2", "-park", "suspend", "-park-each",
			"-trajectory", fastSWETraj, "-events-dir", overDir, "-worker", "swe", "-job", "swe",
		}, &out, &errBuf)
	}()
	go func() {
		defer overWG.Done()
		var out, errBuf bytes.Buffer
		r2eOverCode = runMain([]string{
			"run", "-dataset", r2eManifest, "-run-id", "r2eswe-ov-r2e",
			"-images", "10", "-rollouts", "4", "-width", "15",
			"-mode", "steps", "-steps", "2", "-park", "suspend", "-park-each",
			"-trajectory", fastR2ETraj, "-events-dir", overDir, "-worker", "r2e", "-job", "r2e",
		}, &out, &errBuf)
	}()
	overWG.Wait()
	if sweOverCode != 0 || r2eOverCode != 0 {
		t.Fatalf("oversubscribed shards failed: swe=%d r2e=%d", sweOverCode, r2eOverCode)
	}
	if fakeOver.creates != 80 {
		t.Fatalf("expected 80 created replica sandboxes, got %d", fakeOver.creates)
	}
	if fakeOver.maxAwake > 30 {
		t.Fatalf("expected max awake sandboxes <= 30 with -park-each, got %d", fakeOver.maxAwake)
	}
}

func TestAllStrategiesRenderPlanAndDirectDriverValidation(t *testing.T) {
	r2eTrajPath := filepath.Join("..", "..", "..", "benchmarking", "swebench", "r2e-gold.yaml")
	rawR2ETraj, err := os.ReadFile(r2eTrajPath)
	if err != nil {
		t.Fatalf("read r2e-gold.yaml: %v", err)
	}
	dir := t.TempDir()
	fastR2ETraj := filepath.Join(dir, "r2e-gold-fast.yaml")
	fastYAML := strings.NewReplacer("think: 1s", "think: 0s", "think: 2s", "think: 0s", "think: 3s", "think: 0s").Replace(string(rawR2ETraj))
	if err := os.WriteFile(fastR2ETraj, []byte(fastYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	rawSWETraj, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "benchmarking", "swebench", "trajectories", "swebench-gold.yaml"))
	if err != nil {
		t.Fatalf("read swebench-gold.yaml: %v", err)
	}
	fastSWETraj := filepath.Join(dir, "swebench-gold-fast.yaml")
	fastSWEYAML := strings.NewReplacer("think: 1s", "think: 0s", "think: 2s", "think: 0s", "think: 3s", "think: 0s").Replace(string(rawSWETraj))
	if err := os.WriteFile(fastSWETraj, []byte(fastSWEYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	graderPy, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "benchmarking", "swebench", "trajectories", "swebench-grade.py"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "swebench-grade.py"), graderPy, 0o644); err != nil {
		t.Fatal(err)
	}

	var r2eBuf, sweBuf bytes.Buffer
	for i := 0; i < 6; i++ {
		r2eRec := map[string]any{
			"kind":               "command",
			"instance_id":        fmt.Sprintf("r2e-st-%02d", i),
			"repo":               "demo/r2e",
			"image":              fmt.Sprintf("us-docker.pkg.dev/p/r2e/img-%02d@sha256:%064x", i, 300+i),
			"setup_command":      "test -d /testbed",
			"reset_command":      "cd /testbed && git reset -q --hard",
			"command":            "/bin/bash run_tests.sh",
			"expected_exit_code": 0,
		}
		b, _ := json.Marshal(r2eRec)
		r2eBuf.Write(b)
		r2eBuf.WriteByte('\n')

		sweRec := map[string]any{
			"instance_id":  fmt.Sprintf("sympy__sympy-%d", 12000+i),
			"repo":         "sympy/sympy",
			"image":        fmt.Sprintf("us-docker.pkg.dev/p/swe/img-%02d@sha256:%064x", i, 400+i),
			"patch":        "diff --git a/a.py b/a.py\n",
			"test_patch":   "diff --git a/t.py b/t.py\n",
			"eval_script":  "#!/bin/bash\npytest\n",
			"fail_to_pass": []string{"test_a"},
			"pass_to_pass": []string{"test_b"},
			"log_parser":   "parse_log_pytest",
			"eval_type":    "pass_and_fail",
		}
		sb, _ := json.Marshal(sweRec)
		sweBuf.Write(sb)
		sweBuf.WriteByte('\n')
	}
	r2eManifest := filepath.Join(dir, "r2e-6.jsonl")
	sweManifest := filepath.Join(dir, "swe-6.jsonl")
	if err := os.WriteFile(r2eManifest, r2eBuf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sweManifest, sweBuf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	// 1. Verify render-plan subcommand for both R2E and SWE-bench trajectories.
	r2ePlanPath := filepath.Join(dir, "r2e-plan.json")
	swePlanPath := filepath.Join(dir, "swe-plan.json")
	var outBuf, errBuf bytes.Buffer
	if code := runMain([]string{
		"render-plan",
		"-dataset", r2eManifest,
		"-images", "4",
		"-trajectory", fastR2ETraj,
		"-fanout", "2",
		"-out", r2ePlanPath,
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("render-plan r2e exited %d: %s", code, errBuf.String())
	}
	var renderedR2E swebench.RenderedPlan
	rawPlan, err := os.ReadFile(r2ePlanPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rawPlan, &renderedR2E); err != nil {
		t.Fatalf("unmarshal r2e plan: %v", err)
	}
	if len(renderedR2E.Instances) != 4 || renderedR2E.DatasetLabel != "r2e-6.jsonl" {
		t.Fatalf("unexpected rendered R2E plan: %+v", renderedR2E)
	}
	if len(renderedR2E.Instances[0].Setup) == 0 || len(renderedR2E.Instances[0].Steps) == 0 {
		t.Fatalf("expected rendered setup and steps on R2E instance, got %+v", renderedR2E.Instances[0])
	}

	errBuf.Reset()
	if code := runMain([]string{
		"render-plan",
		"-dataset", sweManifest,
		"-images", "4",
		"-trajectory", fastSWETraj,
		"-out", swePlanPath,
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("render-plan swe exited %d: %s", code, errBuf.String())
	}

	// 2. Sweep all 4 strategies (none, naive, sliding, pipelined) and verify invariants + comparison report.
	prev := testBackendOverride
	t.Cleanup(func() { testBackendOverride = prev })

	var armReportPaths []string
	for idx, strat := range swebench.AllStrategies {
		fake := &recipeFakeBackend{}
		if strat == swebench.StrategyPipelined {
			fake.opDelay = 2 * time.Millisecond
		}
		testBackendOverride = fake

		outPath := filepath.Join(dir, fmt.Sprintf("events-%s.jsonl", strat))
		runArgs := []string{
			"run",
			"-dataset", r2eManifest,
			"-run-id", fmt.Sprintf("strat-a%d", idx+1),
			"-images", "6",
			"-rollouts", "2",
			"-width", "4",
			"-mode", "steps",
			"-strategy", string(strat),
			"-trajectory", fastR2ETraj,
			"-out", outPath,
			"-label", "client=go",
			"-label", fmt.Sprintf("arm=go-%s", strat),
		}
		if strat == swebench.StrategySliding || strat == swebench.StrategyPipelined {
			runArgs = append(runArgs, "-window-size", "2")
		}
		var runOut, runErr bytes.Buffer
		if code := runMain(runArgs, &runOut, &runErr); code != 0 {
			t.Fatalf("strategy %s exited %d: %s", strat, code, runErr.String())
		}

		rep, err := swebench.SummarizeFile(outPath)
		if err != nil {
			t.Fatalf("SummarizeFile(%s): %v", strat, err)
		}
		if rep.Tasks != 12 || rep.Rewarded != 12 || rep.RewardRate != 1.0 {
			t.Fatalf("strategy %s: expected 12/12 rewarded tasks, got tasks=%d rewarded=%d rate=%v", strat, rep.Tasks, rep.Rewarded, rep.RewardRate)
		}
		if fake.maxAwake > 4 {
			t.Fatalf("strategy %s: expected maxAwake <= width (4), got %d", strat, fake.maxAwake)
		}
		switch strat {
		case swebench.StrategyNone:
			if rep.Labels["park"] != "delete" {
				t.Fatalf("strategy none: expected park=delete label, got %q", rep.Labels["park"])
			}
			if rep.WorkingSetHitRate != 0 || rep.StandingPoolSize != 0 || rep.WorkingSetHits != 0 || fake.maxParked != 0 {
				t.Fatalf("strategy none: expected 0 warm hits and 0 parked sandboxes, got hits=%d misses=%d hitRate=%v pool=%d maxParked=%d", rep.WorkingSetHits, rep.WorkingSetMisses, rep.WorkingSetHitRate, rep.StandingPoolSize, fake.maxParked)
			}
		case swebench.StrategyNaive:
			if rep.Labels["park"] != "suspend" {
				t.Fatalf("strategy naive: expected park=suspend label, got %q", rep.Labels["park"])
			}
			if rep.WorkingSetHitRate != 1.0 || rep.StandingPoolSize != 12 || rep.WorkingSetHits != 12 || fake.maxParked != 12 {
				t.Fatalf("strategy naive: expected 100%% hit rate and 12 parked sandboxes, got hits=%d misses=%d hitRate=%v pool=%d maxParked=%d", rep.WorkingSetHits, rep.WorkingSetMisses, rep.WorkingSetHitRate, rep.StandingPoolSize, fake.maxParked)
			}
		case swebench.StrategySliding:
			if rep.Labels["park"] != "suspend" {
				t.Fatalf("strategy sliding: expected park=suspend label, got %q", rep.Labels["park"])
			}
			if rep.WorkingSetHitRate != 1.0 || rep.StandingPoolSize != 4 || rep.WorkingSetHits != 12 || fake.maxParked > 4 {
				t.Fatalf("strategy sliding: expected 100%% hit rate and <=4 parked sandboxes (window=2*2), got hits=%d misses=%d hitRate=%v pool=%d maxParked=%d", rep.WorkingSetHits, rep.WorkingSetMisses, rep.WorkingSetHitRate, rep.StandingPoolSize, fake.maxParked)
			}
		case swebench.StrategyPipelined:
			if rep.Labels["park"] != "suspend" {
				t.Fatalf("strategy pipelined: expected park=suspend label, got %q", rep.Labels["park"])
			}
			if rep.WorkingSetHitRate != 1.0 || rep.StandingPoolSize > 8 || rep.WorkingSetHits != 12 || fake.maxParked > 8 {
				t.Fatalf("strategy pipelined: expected 100%% hit rate and <=8 double-buffered parked sandboxes, got hits=%d misses=%d hitRate=%v pool=%d maxParked=%d", rep.WorkingSetHits, rep.WorkingSetMisses, rep.WorkingSetHitRate, rep.StandingPoolSize, fake.maxParked)
			}
		}

		rep.RunID = fmt.Sprintf("go-%s", strat)
		armJSONPath := filepath.Join(dir, fmt.Sprintf("go-%s-report.json", strat))
		b, err := json.Marshal(rep)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(armJSONPath, b, 0o644); err != nil {
			t.Fatal(err)
		}
		armReportPaths = append(armReportPaths, armJSONPath)
	}

	compJSON := filepath.Join(dir, "comparison.json")
	compMD := filepath.Join(dir, "comparison.md")
	var compOut, compErr bytes.Buffer
	reportArgs := append([]string{"report"}, armReportPaths...)
	reportArgs = append(reportArgs, "-json", compJSON, "-md", compMD)
	if code := runMain(reportArgs, &compOut, &compErr); code != 0 {
		t.Fatalf("report multi-arm comparison exited %d: %s", code, compErr.String())
	}
	for _, wantCol := range []string{"go-none", "go-naive", "go-sliding", "go-pipelined"} {
		if !strings.Contains(compOut.String(), wantCol) {
			t.Fatalf("comparison output missing column %q:\n%s", wantCol, compOut.String())
		}
	}
	// The Markdown arm table run-r2e-swebench.sh writes to comparison.md:
	// one row per arm from its labels and working-set figures, then the text
	// comparison in a code block.
	mdBytes, err := os.ReadFile(compMD)
	if err != nil {
		t.Fatal(err)
	}
	md := string(mdBytes)
	for _, want := range []string{
		"| Arm | Client | Strategy | Tasks | Failed | Infra Fault Rate (<2%) | Reward Rate | Wall Clock (s) | Throughput (tasks/min) | Warm Hit Rate | Peak Active | Standing Pool | TTFC (cmd complete) p50/p95/p99 | Wake (cmd complete) p50/p95/p99 |",
		"| `go-none` | `go` | `none` | 12 | 0 | 0.00% (PASS, 0/12) | 1.000 | ",
		"| `go-naive` | `go` | `naive` | 12 | 0 | 0.00% (PASS, 0/12) | 1.000 | ",
		"| `go-sliding` | `go` | `sliding` | 12 | 0 | 0.00% (PASS, 0/12) | 1.000 | ",
		"| `go-pipelined` | `go` | `pipelined` | 12 | 0 | 0.00% (PASS, 0/12) | 1.000 | ",
		" | 100.0% | ", // the warm arms' hit rate
		" | 12 | ",     // naive's standing pool
		"## Detailed Event Breakdown (`swebench-loader report`)",
		"```text\n" + strings.SplitN(compOut.String(), "\n", 2)[0],
	} {
		if !strings.Contains(md, want) {
			t.Fatalf("comparison.md lacks %q:\n%s", want, md)
		}
	}

	// 3. Verify fail-loud flag and direct-driver validation. The step loop's
	// flags are refused with -strategy; -dry-run makes Run's checks too.
	longManifest := filepath.Join(dir, "r2e-long-id.jsonl")
	longRec, _ := json.Marshal(map[string]any{
		"kind":               "command",
		"instance_id":        strings.Repeat("a", 40),
		"repo":               "demo/long",
		"image":              fmt.Sprintf("us-docker.pkg.dev/p/r2e/long@sha256:%064x", 999),
		"setup_command":      "test -d /testbed",
		"reset_command":      "cd /testbed && git reset -q --hard",
		"command":            "/bin/bash run_tests.sh",
		"expected_exit_code": 0,
	})
	if err := os.WriteFile(longManifest, append(longRec, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, badArgs := range [][]string{
		{"run", "-dataset", r2eManifest, "-strategy", "bogus"},
		{"run", "-dataset", r2eManifest, "-strategy", "naive", "-window-size", "2"},
		{"run", "-dataset", r2eManifest, "-strategy", "sliding", "-mode", "open-loop"},
		{"run", "-dataset", r2eManifest, "-strategy", "sliding", "-mode", "steps", "-steps", "2"},
		{"run", "-dataset", r2eManifest, "-strategy", "naive", "-mode", "steps", "-park", "delete"},
		{"run", "-dataset", r2eManifest, "-strategy", "naive", "-mode", "steps", "-park", "suspend"},
		{"run", "-dataset", r2eManifest, "-strategy", "naive", "-mode", "steps", "-step-pause", "5s"},
		{"run", "-dataset", r2eManifest, "-strategy", "none", "-mode", "steps", "-park-each"},
		{"run", "-dataset", r2eManifest, "-strategy", "none", "-mode", "steps", "-prefetch"},
		// -dry-run: a 64-character actor name (20 + 1 + 36 + 1 + 6) and a loader label key.
		{"run", "-dataset", longManifest, "-trajectory", fastR2ETraj, "-run-id", strings.Repeat("r", 20), "-mode", "eval", "-tasks-per-image", "100001", "-dry-run"},
		{"run", "-dataset", r2eManifest, "-trajectory", fastR2ETraj, "-label", "width=10", "-dry-run"},
	} {
		var bo, be bytes.Buffer
		if code := runMain(badArgs, &bo, &be); code == 0 {
			t.Fatalf("expected non-zero exit for invalid args %v", badArgs)
		}
	}
	var dryOut, dryErr bytes.Buffer
	if code := runMain([]string{"run", "-dataset", longManifest, "-trajectory", fastR2ETraj, "-run-id", strings.Repeat("r", 20), "-mode", "eval", "-tasks-per-image", "10", "-dry-run"}, &dryOut, &dryErr); code != 0 {
		t.Fatalf("dry-run with a 58-character name exited %d: %s", code, dryErr.String())
	}

	if _, _, err := swebench.NewBackendDriver(swebench.BackendDriverOptions{
		Backend:          "substrate",
		Access:           "direct",
		ControlAddr:      "127.0.0.1:7443",
		AtenetClientCert: filepath.Join(dir, "missing-cert.pem"),
	}); err == nil {
		t.Fatal("expected NewBackendDriver to fail on missing AtenetClientCert")
	}
	emptyToken := filepath.Join(dir, "empty-token.txt")
	if err := os.WriteFile(emptyToken, []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := swebench.NewBackendDriver(swebench.BackendDriverOptions{
		Backend:         "substrate",
		Access:          "direct",
		ControlAddr:     "127.0.0.1:7443",
		AteapiTokenFile: emptyToken,
	}); err == nil {
		t.Fatal("expected NewBackendDriver to fail on empty AteapiTokenFile")
	}

	// 4. Verify run-r2e-swebench.sh dry-run telemetry, tracing, and GCS folder validation.
	// The script derives and checks the GCS folder with python3 whenever a bucket,
	// a folder or bundle validation is in effect, so both dry-runs need it.
	scriptPath := filepath.Join("..", "..", "..", "benchmarking", "swebench", "run-r2e-swebench.sh")
	_, pyErr := exec.LookPath("python3")
	if pyErr != nil {
		t.Logf("python3 not found (%v); skipping the --gcs-bucket dry-runs", pyErr)
	}
	artDir := filepath.Join(dir, "dry-gcs")
	cmd := exec.Command("bash", scriptPath, "dry-run",
		"--r2e-dataset", r2eManifest,
		"--swe-dataset", sweManifest,
		"--concurrency", "20",
		"--images", "2",
		"--rollouts-per-image", "2",
		"--client", "go",
		"--strategy", "naive,sliding",
		"--window-size", "2",
		"--collect-telemetry",
		"--measure-interval", "30",
		"--gcs-bucket", "gs://unit-test-bucket",
		"--gcs-nickname", "smoke",
		"--gcs-dry-run",
		"--artifact-dir", artDir,
	)
	cmd.Env = append(os.Environ(), "SWE_EXCLUDE_FILE=")
	if pyErr == nil {
		outBytes, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("run-r2e-swebench.sh dry-run failed: %v\n%s", err, string(outBytes))
		}
		outText := string(outBytes)
		for _, want := range []string{
			"_simulated-w20_r2e2-swe2_smoke-go-naive/",
			"_simulated-w20_r2e2-swe2_smoke-go-sliding/",
		} {
			if !strings.Contains(outText, "gs://unit-test-bucket/") || !strings.Contains(outText, want) {
				t.Fatalf("dry-run output missing %q:\n%s", want, outText)
			}
		}
	}

	if pyErr == nil {
		pyArtDir := filepath.Join(dir, "dry-py-traced")
		pyCmd := exec.Command("bash", scriptPath, "dry-run",
			"--r2e-dataset", r2eManifest,
			"--swe-dataset", sweManifest,
			"--concurrency", "10",
			"--images", "2",
			"--rollouts-per-image", "1",
			"--client", "python",
			"--strategy", "none",
			"--trace",
			"--gcs-bucket", "unit-test-bucket",
			"--gcs-nickname", "pytrace",
			"--artifact-dir", pyArtDir,
		)
		pyCmd.Env = append(os.Environ(), "SWE_EXCLUDE_FILE=")
		pyOut, pyRunErr := pyCmd.CombinedOutput()
		if pyRunErr != nil {
			t.Fatalf("python --trace dry-run failed: %v\n%s", pyRunErr, string(pyOut))
		}
		if !strings.Contains(string(pyOut), "_simulated-w10_r2e2-swe2_pytrace_traced/") {
			t.Fatalf("expected _traced GCS folder suffix on python --trace dry-run, got:\n%s", string(pyOut))
		}
	}

	for _, badScriptArgs := range [][]string{
		{"dry-run", "--r2e-dataset", r2eManifest, "--swe-dataset", sweManifest, "--gcs-bucket", "gs://bucket/with/subpath"},
		{"dry-run", "--r2e-dataset", r2eManifest, "--swe-dataset", sweManifest, "--gcs-bucket", "unit-test-bucket", "--gcs-folder", "bad_folder_name"},
		{"dry-run", "--r2e-dataset", r2eManifest, "--swe-dataset", sweManifest, "--footing", "sideways"},
	} {
		badCmd := exec.Command("bash", append([]string{scriptPath}, badScriptArgs...)...)
		badCmd.Env = append(os.Environ(), "SWE_EXCLUDE_FILE=")
		if badOut, badErr := badCmd.CombinedOutput(); badErr == nil {
			t.Fatalf("expected failure for %v, succeeded with:\n%s", badScriptArgs, string(badOut))
		}
	}
}

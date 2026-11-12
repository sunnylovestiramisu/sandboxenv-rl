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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/job"
	"github.com/agent-substrate/env/internal/benchmarking/measure"
	"github.com/agent-substrate/env/internal/benchmarking/overlay"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/sut"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

func runScenarioCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	scenarioPath := fs.String("scenario", "", "Path to Scenario YAML file. Required.")
	destDir := fs.String("dest", "", "Destination artifact directory ($ARTIFACT_DIR).")
	outDir := fs.String("out-dir", "", "Alias for -dest: destination artifact directory.")
	overlayFlag := fs.String("overlay", "", "Optional overlay preset name or comma-separated pair <A>,<B>.")
	datasetPath := fs.String("dataset", "", "Optional dataset JSONL path.")
	instancesPath := fs.String("instances", "", "Alias for -dataset: optional dataset JSONL path.")
	runID := fs.String("run-id", "bench-run", "Run identifier.")
	cluster := fs.String("cluster", "local-cluster", "Cluster name recorded in system_description.json.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	targetDir := *destDir
	if targetDir == "" {
		targetDir = *outDir
	}
	if *scenarioPath == "" || targetDir == "" {
		return fmt.Errorf("-scenario and -dest (or -out-dir) are required")
	}

	raw, err := os.ReadFile(*scenarioPath)
	if err != nil {
		return err
	}
	scen, err := schema.DecodeYAMLStrict[schema.Scenario](raw)
	if err != nil {
		return err
	}
	// A relative preflight.coverage is next to the scenario file, as a
	// reader of the file expects, not relative to the working directory.
	if scen.Preflight != nil && scen.Preflight.Coverage != "" && !filepath.IsAbs(scen.Preflight.Coverage) {
		scen.Preflight.Coverage = filepath.Join(filepath.Dir(*scenarioPath), scen.Preflight.Coverage)
	}

	dsSource := *datasetPath
	if dsSource == "" {
		dsSource = *instancesPath
	}
	var insts []swebench.Instance
	if dsSource != "" {
		insts, err = swebench.LoadDataset(dsSource)
		if err != nil {
			return err
		}
	} else {
		insts = swebench.SyntheticInstances(16, "")
	}

	sched, err := job.GenerateSchedule(&scen, insts)
	if err != nil {
		return err
	}

	overlays := parseOverlayFlag(*overlayFlag)
	ctx := context.Background()
	if len(overlays) <= 1 {
		ovName := ""
		if len(overlays) == 1 {
			ovName = overlays[0]
		}
		if _, err := executeScenarioRun(ctx, &scen, sched, ovName, *runID, *cluster, targetDir); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "completed benchmark run in %s\n", targetDir)
		return nil
	}
	if len(overlays) != 2 {
		return fmt.Errorf("-overlay supports at most 2 comma-separated overlays, got %d", len(overlays))
	}

	nameA, nameB := overlays[0], overlays[1]
	dirA := filepath.Join(targetDir, nameA)
	dirB := filepath.Join(targetDir, nameB)
	repA, err := executeScenarioRun(ctx, &scen, sched, nameA, *runID+"-a", *cluster, dirA)
	if err != nil {
		return fmt.Errorf("run overlay %s: %w", nameA, err)
	}
	repB, err := executeScenarioRun(ctx, &scen, sched, nameB, *runID+"-b", *cluster, dirB)
	if err != nil {
		return fmt.Errorf("run overlay %s: %w", nameB, err)
	}

	cmp, err := overlay.CompareOverlayPair(nameA, repA, sched, nameB, repB, sched)
	if err != nil {
		return err
	}
	cmpJSON, err := json.MarshalIndent(cmp, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(targetDir, "comparison.json"), append(cmpJSON, '\n'), 0o644); err != nil {
		return err
	}
	md := overlay.RenderComparisonMarkdown(cmp)
	if err := os.WriteFile(filepath.Join(targetDir, "comparison.md"), []byte(md), 0o644); err != nil {
		return err
	}
	fmt.Fprint(stdout, md)
	return nil
}

func parseOverlayFlag(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func executeScenarioRun(
	ctx context.Context,
	scen *schema.Scenario,
	sched *schema.Schedule,
	overlayName string,
	runID string,
	cluster string,
	outDir string,
) (*swebench.Report, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}

	runScen := *scen
	if len(scen.Jobs) > 0 {
		runScen.Jobs = append([]schema.JobSpec(nil), scen.Jobs...)
		for i := range runScen.Jobs {
			if len(scen.Jobs[i].TrainerCommand) > 0 {
				runScen.Jobs[i].TrainerCommand = append([]string(nil), scen.Jobs[i].TrainerCommand...)
			}
			if len(scen.Jobs[i].EnvVars) > 0 {
				envCopy := make(map[string]string, len(scen.Jobs[i].EnvVars))
				for k, v := range scen.Jobs[i].EnvVars {
					envCopy[k] = v
				}
				runScen.Jobs[i].EnvVars = envCopy
			}
		}
	}
	if len(scen.SUT.ExtraArgs) > 0 {
		extraCopy := make(map[string]string, len(scen.SUT.ExtraArgs))
		for k, v := range scen.SUT.ExtraArgs {
			extraCopy[k] = v
		}
		runScen.SUT.ExtraArgs = extraCopy
	}
	if len(scen.SUT.Labels) > 0 {
		labelsCopy := make(map[string]string, len(scen.SUT.Labels))
		for k, v := range scen.SUT.Labels {
			labelsCopy[k] = v
		}
		runScen.SUT.Labels = labelsCopy
	}
	defaultSource := string(schema.SourceWarmPool)
	if overlayName != "" {
		ov, err := overlay.ResolveOverlay(overlayName, scen.Overlays)
		if err != nil {
			return nil, err
		}
		if ov.PrewarmPolicy == "warm" {
			runScen.Footing = "warm"
		} else if ov.PrewarmPolicy == "cold" || ov.PrewarmPolicy == "none" {
			runScen.Footing = "cold"
		}
		if ov.WarmPoolSize > 0 {
			defaultSource = string(schema.SourceWarmPool)
		} else {
			defaultSource = string(schema.SourceColdStart)
		}
	}

	// 1. Render SUT manifest and system_description.json.
	prov := sut.NewProvisioner(nil)
	if _, err := prov.Provision(ctx, sut.ProvisionOptions{
		Spec:        runScen.SUT,
		ArtifactDir: outDir,
		RenderOnly:  true,
	}); err != nil {
		return nil, err
	}
	sysDesc := sut.BuildSystemDescription(runScen.SUT, cluster, "v1.31", "HEAD", "verified", nil)
	if overlayName != "" {
		scenBytes, _ := json.Marshal(scen)
		scenSum := sha256.Sum256(scenBytes)
		ovSum := sha256.Sum256([]byte(overlayName))
		basePath := scen.Name
		if basePath == "" {
			basePath = "scenario.yaml"
		}
		sysDesc.Overlays = []schema.OverlayRef{{
			Name:           overlayName,
			Hash:           "sha256:" + hex.EncodeToString(ovSum[:8]),
			BaseRecipePath: basePath,
			BaseRecipeHash: "sha256:" + hex.EncodeToString(scenSum[:8]),
		}}
	}
	if err := sut.WriteSystemDescription(filepath.Join(outDir, "system_description.json"), sysDesc); err != nil {
		return nil, err
	}

	// 2. Write measurements.json.
	meas, err := measure.CollectMeasurements(ctx, measure.CollectOptions{
		RunID: runID,
	})
	if err != nil {
		return nil, err
	}
	if err := measure.WriteMeasurements(filepath.Join(outDir, "measurements.json"), meas); err != nil {
		return nil, err
	}

	// 3. Run ScenarioRunner across all jobs (writing schedule.json and events.jsonl).
	sr := &job.ScenarioRunner{
		RunID:    runID,
		Scenario: &runScen,
		Schedule: sched,
		OutDir:   outDir,
		ClientFunc: func(_ schema.JobSpec) swebench.EnvClient {
			return &localScenarioEnvClient{source: defaultSource}
		},
		SleepFunc: func(_ context.Context, _ time.Duration) error { return nil },
	}
	if err := sr.Run(ctx); err != nil {
		return nil, err
	}

	// 4. A scenario whose footing rests on a pre-flight names its coverage
	// file: for a warm arm it goes into the bundle as preflight-coverage.json,
	// the evidence Validity Rule 1 reads (a cold arm of an overlay run gets
	// none, as a pre-flight contradicts cold footing). Without one, a warm
	// footing gets the synthetic prewarm event this local run stands in
	// with.
	eventsPath := filepath.Join(outDir, "events.jsonl")
	var coverage *schema.PreflightCoverage
	coverageName := ""
	if runScen.Preflight != nil && runScen.Footing == "warm" {
		if runScen.Preflight.Coverage == "" {
			// A plan alone is not evidence: the warm footing would fall back
			// to the synthetic prewarm event and pass for the wrong reason.
			return nil, fmt.Errorf("scenario preflight names a plan (%s) but no coverage; a warm footing that rests on a pre-flight needs its preflight-coverage.json", runScen.Preflight.Plan)
		}
		cov, err := swebench.ReadPreflightCoverage(runScen.Preflight.Coverage)
		if err != nil {
			return nil, fmt.Errorf("scenario preflight.coverage: %w", err)
		}
		if runScen.Preflight.Plan != "" && (cov.Plan == "" || filepath.Base(cov.Plan) != filepath.Base(runScen.Preflight.Plan)) {
			return nil, fmt.Errorf("scenario preflight.plan %q is not the plan the coverage %s was written for (%q)", runScen.Preflight.Plan, runScen.Preflight.Coverage, cov.Plan)
		}
		coverageName = "preflight-coverage.json"
		if err := swebench.WritePreflightCoverage(filepath.Join(outDir, coverageName), cov); err != nil {
			return nil, err
		}
		coverage = cov
	}
	if runScen.Footing == "warm" && coverage == nil {
		f, err := os.OpenFile(eventsPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			rec := swebench.NewRecorder(f)
			rec.Emit(swebench.Event{
				RunID: runID,
				Event: swebench.EventPrewarm,
				Image: "prewarmed-image",
				DurMS: 10,
				OK:    true,
			})
			_ = rec.Close()
			_ = f.Close()
		}
	}

	// 5. Summarize and validate report.json, enrich system_description.json, and write report.md + run_window.json.
	rep, err := swebench.BuildReport(eventsPath)
	if err != nil {
		return nil, err
	}
	enrichSystemDescriptionFromArtifactDir(outDir, &sysDesc, rep, meas)
	if sysDesc.MaxConcurrentSandboxesCap <= 0 {
		if sysDesc.MaxActivePods > 0 {
			sysDesc.MaxConcurrentSandboxesCap = sysDesc.MaxActivePods
		} else if sysDesc.Load != nil && sysDesc.Load.MaxConcurrent > 0 {
			sysDesc.MaxConcurrentSandboxesCap = sysDesc.Load.MaxConcurrent
		} else {
			sysDesc.MaxConcurrentSandboxesCap = 16
		}
	}
	if sysDesc.LifecyclePolicy == nil {
		mode := "ephemeral"
		parkMode := "ephemeral"
		prewarm := false
		if runScen.Footing == "warm" {
			mode = "prewarmed"
			parkMode = "suspend"
			prewarm = true
		}
		sysDesc.LifecyclePolicy = &schema.SandboxLifecyclePolicy{
			Mode:           mode,
			ParkMode:       parkMode,
			PrewarmAtStart: prewarm,
		}
	}
	if err := sut.WriteSystemDescription(filepath.Join(outDir, "system_description.json"), sysDesc); err != nil {
		return nil, err
	}
	rep.AttachClusterTelemetry(&sysDesc, meas)
	swebench.Validate(rep, swebench.ValidateOptions{
		Footing:        runScen.Footing,
		Schedule:       sched,
		TraceCaptured:  slices.ContainsFunc(runScen.Jobs, func(j schema.JobSpec) bool { return j.TraceMode == "trace" }),
		TraceFileCount: len(swebench.FindTraceFiles(outDir)),
		// The coverage is given as read, and named by its place in the
		// bundle, so the evidence holds after the bundle moves.
		PreflightCoverage:     coverage,
		PreflightCoveragePath: coverageName,
	})

	var mdBuf bytes.Buffer
	rep.WriteMarkdown(&mdBuf)
	if err := os.WriteFile(filepath.Join(outDir, "report.md"), mdBuf.Bytes(), 0o644); err != nil {
		return nil, err
	}
	if err := ensureRunWindowFile(outDir, runID, cluster, time.Time{}, time.Time{}, rep.TraceMetadata()); err != nil {
		return nil, err
	}

	repBytes, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(outDir, "report.json"), append(repBytes, '\n'), 0o644); err != nil {
		return nil, err
	}
	return rep, nil
}

func validateCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	destDir := fs.String("dest", "", "Path to benchmark artifact directory ($ARTIFACT_DIR) for full bundle validation.")
	folderName := fs.String("folder", "", "Optional GCS folder name to validate (<YYYY-MM-DD>_<model>_<dataset>_<nickname>[_traced]; _traced iff the bundle captured a trace).")
	reportPath := fs.String("report", "", "Path to report.json (single-file mode when -dest is omitted).")
	eventsPath := fs.String("events", "", "Optional path to events.jsonl (single-file mode).")
	schedulePath := fs.String("schedule", "", "Optional path to schedule.json (single-file mode).")
	footing := fs.String("footing", "", "Declared footing (warm or cold).")
	minTasks := fs.Int("min-tasks", 0, "Minimum required tasks.")
	minRewardRate := fs.Float64("min-reward-rate", 0, "Minimum required reward rate.")
	coveragePath := fs.String("preflight-coverage", "", "Optional preflight-coverage.json for Validity Rule 1 (single-file mode; a bundle's own is found without this).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts := swebench.ValidateOptions{
		Footing:               *footing,
		MinTasks:              *minTasks,
		MinRewardRate:         *minRewardRate,
		CheckRewardRate:       *minRewardRate > 0,
		PreflightCoveragePath: *coveragePath,
	}
	if strings.TrimSpace(*destDir) != "" {
		bRes, err := ValidateArtifactBundle(*destDir, opts, *folderName)
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(bRes, "", "  ")
		fmt.Fprintln(stdout, string(data))
		if !bRes.Valid {
			return fmt.Errorf("bundle validation failed: %s", strings.Join(bRes.Reasons, "; "))
		}
		return nil
	}
	if *reportPath == "" && *eventsPath == "" {
		return fmt.Errorf("either -dest, -report, or -events is required")
	}
	_, v, err := swebench.ValidateReportOrEvents(*reportPath, *eventsPath, *schedulePath, opts)
	if err != nil {
		return err
	}
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Fprintln(stdout, string(data))
	if !v.Valid {
		return fmt.Errorf("report validation failed: %s", strings.Join(v.Reasons, "; "))
	}
	return nil
}

func compareCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	fs.SetOutput(stderr)
	basePath := fs.String("baseline", "", "Path to baseline report.json.")
	candPath := fs.String("candidate", "", "Path to candidate report.json.")
	thresholdsPath := fs.String("thresholds", "", "Optional path to thresholds YAML/JSON.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *basePath == "" || *candPath == "" {
		return fmt.Errorf("-baseline and -candidate are required")
	}
	res, err := swebench.CompareReportFiles(*basePath, *candPath, *thresholdsPath)
	if err != nil {
		return err
	}
	fmt.Fprint(stdout, swebench.FormatComparisonMarkdown(res))
	if !res.Passed {
		return fmt.Errorf("comparison failed thresholds: %s", strings.Join(res.Regressions, "; "))
	}
	return nil
}

type localScenarioEnvClient struct {
	seq    atomic.Int64
	source string
}

func (c *localScenarioEnvClient) Create(_ context.Context, _, _ string) (swebench.EnvHandle, error) {
	id := c.seq.Add(1)
	src := c.source
	if src == "" {
		src = string(schema.SourceWarmPool)
	}
	return swebench.EnvHandle{
		Name:   fmt.Sprintf("env-%d", id),
		Source: src,
	}, nil
}

func (c *localScenarioEnvClient) Exec(_ context.Context, _, _ string, _ time.Duration) (swebench.ExecResult, error) {
	return swebench.ExecResult{ExitCode: 0, Stdout: "ok"}, nil
}

func (c *localScenarioEnvClient) Delete(_ context.Context, _ string) error {
	return nil
}

func (c *localScenarioEnvClient) Snapshot(_ context.Context, envName string) (string, error) {
	return "snap-" + envName, nil
}

func (c *localScenarioEnvClient) Resume(_ context.Context, snapshot, _, _ string) (swebench.EnvHandle, error) {
	return swebench.EnvHandle{
		Name:   "resumed-" + snapshot,
		Source: string(schema.SourceSnapshotResume),
	}, nil
}

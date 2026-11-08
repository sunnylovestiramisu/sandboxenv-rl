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
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/job"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

// TracedFolderSuffix marks archive folders of runs that captured an SDK trace.
const TracedFolderSuffix = "_traced"

// <YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>[_traced]; segments
// exclude "_" so the suffix is unambiguous.
var gcsFolderNameRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}_[a-zA-Z0-9.-]+_[a-zA-Z0-9.-]+_[a-zA-Z0-9.-]+(_traced)?$`)

// ValidateGCSFolderName checks folder's form and that its _traced suffix
// agrees with whether the bundle captured a trace: traced latencies include
// trace overhead and must never be compared with untraced ones unknowingly.
func ValidateGCSFolderName(folder string, traced bool) error {
	folder = strings.Trim(strings.TrimSpace(folder), "/")
	if !gcsFolderNameRe.MatchString(folder) {
		return fmt.Errorf(
			"invalid benchmark folder name %q: must match <YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>[_traced] (e.g. 2026-10-27_synthetic-w4_math-bb4dcec_suspend-sync, 2026-10-28_qwen3.5-35b_r2e-gym-easy_prewarmed_traced)",
			folder,
		)
	}
	datePrefix := folder[:10]
	if _, err := time.Parse("2006-01-02", datePrefix); err != nil {
		return fmt.Errorf("invalid date prefix %q in folder name %q: %w", datePrefix, folder, err)
	}
	switch hasSuffix := strings.HasSuffix(folder, TracedFolderSuffix); {
	case traced && !hasSuffix:
		return fmt.Errorf("folder name %q must end in %s: the bundle captured an SDK trace", folder, TracedFolderSuffix)
	case !traced && hasSuffix:
		return fmt.Errorf("folder name %q ends in %s but the bundle has no trace", folder, TracedFolderSuffix)
	}
	return nil
}

// bundleTraceEvidence reports whether destDir's bundle captured a trace: trace
// files on disk, report.json trace_captured, or run_window.json metadata.
func bundleTraceEvidence(destDir string) (captured bool, traceFiles []string) {
	traceFiles = swebench.FindTraceFiles(destDir)
	captured = len(traceFiles) > 0
	if rep, err := swebench.LoadOrSummarizeReport(filepath.Join(destDir, "report.json")); err == nil && rep.TraceCaptured {
		captured = true
	}
	if rw, err := swebench.ReadRunWindow(filepath.Join(destDir, "run_window.json")); err == nil && rw.Metadata[swebench.TraceCapturedMetadataKey] == "true" {
		captured = true
	}
	return captured, traceFiles
}

// NormalizeGCSBucket strips optional gs:// prefix and trailing slashes from a bucket argument.
func NormalizeGCSBucket(raw string) (string, error) {
	b := strings.TrimSpace(raw)
	b = strings.TrimPrefix(b, "gs://")
	b = strings.Trim(b, "/")
	if b == "" {
		return "", fmt.Errorf("-bucket is required (pass a GCS bucket name or gs://<bucket>)")
	}
	if strings.Contains(b, "/") {
		return "", fmt.Errorf("-bucket %q must be a bucket name without subpath (use -folder for the run directory name)", raw)
	}
	return b, nil
}

// BundleValidationResult summarizes full artifact directory validation (`bench validate -dest`).
type BundleValidationResult struct {
	Valid   bool   `json:"valid"`
	Mode    string `json:"mode,omitempty"`
	Footing string `json:"footing,omitempty"`
	// TraceCaptured reports whether an SDK trace was captured during the run
	// (trace files in the bundle, or declared by the report / run_window
	// metadata). Tracing adds per-call latency, so it is always surfaced.
	TraceCaptured bool `json:"trace_captured"`
	// TraceFiles lists the trace JSONL files found in the bundle, relative to it.
	TraceFiles     []string               `json:"trace_files,omitempty"`
	CheckedFiles   []string               `json:"checked_files,omitempty"`
	Reasons        []string               `json:"reasons,omitempty"`
	ReportValidity *schema.ReportValidity `json:"report_validity,omitempty"`
}

// ValidateArtifactBundle checks that destDir conforms to the standardized v1 benchmark
// layout and schema rules for both Always-Required and Mode-Specific artifacts.
func ValidateArtifactBundle(destDir string, opts swebench.ValidateOptions, folderName string) (*BundleValidationResult, error) {
	res := &BundleValidationResult{Valid: true}
	addFailure := func(format string, args ...any) {
		res.Valid = false
		res.Reasons = append(res.Reasons, fmt.Sprintf(format, args...))
	}
	markChecked := func(rel string) {
		res.CheckedFiles = append(res.CheckedFiles, rel)
	}

	info, err := os.Stat(destDir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("artifact directory %q does not exist or is not a directory", destDir)
	}

	traceCaptured, traceFiles := bundleTraceEvidence(destDir)
	res.TraceCaptured = traceCaptured
	for _, tf := range traceFiles {
		rel, _ := filepath.Rel(destDir, tf)
		res.TraceFiles = append(res.TraceFiles, rel)
	}
	opts.TraceCaptured = opts.TraceCaptured || traceCaptured
	opts.TraceFileCount = len(traceFiles)
	// A pre-flight's coverage file in the bundle feeds Rule 1 without a flag.
	if opts.PreflightCoverage == nil && opts.PreflightCoveragePath == "" {
		if p := filepath.Join(destDir, "preflight-coverage.json"); fileExists(p) {
			opts.PreflightCoveragePath = p
		}
	}

	if strings.TrimSpace(folderName) != "" {
		if err := ValidateGCSFolderName(folderName, traceCaptured); err != nil {
			addFailure("%s", err.Error())
		}
	}

	// 1. Always Required: system_description.json
	var sysDesc schema.SystemDescription
	sysPath := filepath.Join(destDir, "system_description.json")
	if rawSys, err := os.ReadFile(sysPath); err != nil {
		addFailure("missing required file system_description.json")
	} else {
		markChecked("system_description.json")
		decoded, derr := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
		if derr != nil {
			addFailure("invalid system_description.json: %v", derr)
		} else {
			sysDesc = decoded
			if sysDesc.MaxConcurrentSandboxesCap <= 0 {
				addFailure("system_description.json: max_concurrent_sandboxes_cap must be > 0")
			}
			if sysDesc.Load == nil || sysDesc.Load.MaxConcurrent <= 0 || sysDesc.Load.TrajectoriesPerStep <= 0 {
				addFailure("system_description.json: load.max_concurrent and load.trajectories_per_step must be > 0")
			}
			if sysDesc.LifecyclePolicy == nil || strings.TrimSpace(sysDesc.LifecyclePolicy.Mode) == "" {
				addFailure("system_description.json: lifecycle_policy.mode is required")
			} else {
				res.Mode = sysDesc.LifecyclePolicy.Mode
			}
			for i, ov := range sysDesc.Overlays {
				if strings.TrimSpace(ov.BaseRecipePath) == "" || strings.TrimSpace(ov.BaseRecipeHash) == "" {
					addFailure("system_description.json: overlays[%d] (%q) must specify base_recipe_path and base_recipe_hash", i, ov.Name)
				}
			}
		}
	}

	// 2. Always Required: measurements.json
	measPath := filepath.Join(destDir, "measurements.json")
	if rawMeas, err := os.ReadFile(measPath); err != nil {
		addFailure("missing required file measurements.json")
	} else {
		markChecked("measurements.json")
		if _, derr := schema.DecodeJSONStrict[schema.Measurements](bytes.NewReader(rawMeas)); derr != nil {
			addFailure("invalid measurements.json: %v", derr)
		}
	}

	// 3. Always Required: run_window.json
	rwPath := filepath.Join(destDir, "run_window.json")
	var runWindow *schema.RunWindow
	if rawRW, err := os.ReadFile(rwPath); err != nil {
		addFailure("missing required file run_window.json")
	} else {
		markChecked("run_window.json")
		if rw, derr := schema.DecodeJSONStrict[schema.RunWindow](bytes.NewReader(rawRW)); derr != nil {
			addFailure("invalid run_window.json: %v", derr)
		} else {
			runWindow = &rw
		}
	}

	// 4. Always Required: report.md (non-empty)
	mdPath := filepath.Join(destDir, "report.md")
	if mdInfo, err := os.Stat(mdPath); err != nil || mdInfo.Size() == 0 {
		addFailure("missing or empty required file report.md")
	} else {
		markChecked("report.md")
	}

	// 5. Always Required: events/*.jsonl or events.jsonl
	eventFiles, _ := filepath.Glob(filepath.Join(destDir, "events", "*.jsonl"))
	topEvents := filepath.Join(destDir, "events.jsonl")
	if _, err := os.Stat(topEvents); err == nil {
		eventFiles = append(eventFiles, topEvents)
	}
	sort.Strings(eventFiles)
	if len(eventFiles) == 0 {
		addFailure("missing required event stream (events/*.jsonl or events.jsonl)")
	} else {
		totalEvents := 0
		for _, ef := range eventFiles {
			rel, _ := filepath.Rel(destDir, ef)
			markChecked(rel)
			rawEv, err := os.ReadFile(ef)
			if err != nil {
				addFailure("read %s: %v", rel, err)
				continue
			}
			err = schema.ForEachJSONL[schema.Event](bytes.NewReader(rawEv), func(_ int, _ schema.Event) error {
				totalEvents++
				return nil
			})
			if err != nil {
				addFailure("invalid event stream %s: %v", rel, err)
			}
		}
		if totalEvents == 0 {
			addFailure("event streams contain 0 events")
		}
	}

	// Determine footing and whether this is a synthetic scenario run vs. real cluster RL run.
	footing := strings.TrimSpace(opts.Footing)
	if footing == "" && sysDesc.LifecyclePolicy != nil {
		pm := strings.ToLower(strings.TrimSpace(sysDesc.LifecyclePolicy.ParkMode))
		mode := strings.ToLower(strings.TrimSpace(sysDesc.LifecyclePolicy.Mode))
		if sysDesc.LifecyclePolicy.PrewarmAtStart || strings.HasPrefix(pm, "suspend") || mode == "prewarmed" || mode == "warm" || mode == "warm_pool" {
			footing = "warm"
		} else {
			footing = "cold"
		}
	}
	res.Footing = footing

	schedPath := filepath.Join(destDir, "schedule.json")
	hasSchedule := fileExistsNonEmpty(schedPath)
	schedOptPath := ""
	if hasSchedule {
		schedOptPath = schedPath
	}

	// 6. Always Required: report.json + semantic validity rules
	repPath := filepath.Join(destDir, "report.json")
	if !fileExistsNonEmpty(repPath) {
		addFailure("missing required file report.json")
	} else {
		markChecked("report.json")
		_, v, verr := swebench.ValidateReportOrEvents(repPath, "", schedOptPath, opts)
		if verr != nil {
			addFailure("invalid report.json: %v", verr)
		} else {
			res.ReportValidity = &v
			if !v.Valid {
				for _, r := range v.Reasons {
					addFailure("report.json validity: %s", r)
				}
			}
		}
	}

	// Rule 6: a missing trace_captured key is tolerated (older bundles); an
	// explicit "false" contradicting the trace evidence is not.
	if runWindow != nil && traceCaptured {
		if declared, ok := runWindow.Metadata[swebench.TraceCapturedMetadataKey]; ok && declared != "true" {
			addFailure("run_window.json says %s=%q but the bundle holds %d trace file(s); re-run bench report -dest", swebench.TraceCapturedMetadataKey, declared, len(traceFiles))
		}
	}

	// Detect run type: real cluster RL run vs synthetic scenario run.
	isClusterRLRun := fileExistsNonEmpty(filepath.Join(destDir, "verl-head.log")) ||
		fileExistsNonEmpty(filepath.Join(destDir, "train.log")) ||
		fileExistsNonEmpty(filepath.Join(destDir, "logs", "verl-head.log")) ||
		fileExistsNonEmpty(filepath.Join(destDir, "logs", "train.log"))

	// 7. Mode-Specific Required: Synthetic Scenario Run (schedule.json present)
	if hasSchedule {
		markChecked("schedule.json")
		rawSched, err := os.ReadFile(schedPath)
		if err != nil {
			addFailure("read schedule.json: %v", err)
		} else if _, derr := schema.DecodeJSONStrict[schema.Schedule](bytes.NewReader(rawSched)); derr != nil {
			addFailure("invalid schedule.json: %v", derr)
		}
		if !fileExistsNonEmpty(filepath.Join(destDir, "sut-manifest.yaml")) {
			addFailure("synthetic scenario bundle requires sut-manifest.yaml")
		} else {
			markChecked("sut-manifest.yaml")
		}
	}

	// 8. Mode-Specific Required: Real Cluster RL Run
	if isClusterRLRun {
		if len(traceFiles) == 0 {
			addFailure("cluster RL bundle requires traces/*.jsonl")
		} else {
			totalTraces := 0
			for _, tf := range traceFiles {
				rel, _ := filepath.Rel(destDir, tf)
				markChecked(rel)
				evs, err := job.ParseTraceJSONLFile(tf, "")
				if err != nil {
					addFailure("invalid trace file %s: %v", rel, err)
					continue
				}
				totalTraces += len(evs)
			}
			if totalTraces == 0 {
				addFailure("traces/*.jsonl contain 0 trace records")
			}
		}

		// Require K8s manifests (configmap.yaml and job.yaml in root or logs/)
		if !fileExistsNonEmpty(filepath.Join(destDir, "configmap.yaml")) && !fileExistsNonEmpty(filepath.Join(destDir, "logs", "configmap.yaml")) {
			addFailure("cluster RL bundle requires configmap.yaml (or logs/configmap.yaml)")
		} else {
			markChecked("configmap.yaml")
		}
		if !fileExistsNonEmpty(filepath.Join(destDir, "job.yaml")) && !fileExistsNonEmpty(filepath.Join(destDir, "logs", "job.yaml")) {
			addFailure("cluster RL bundle requires job.yaml (or logs/job.yaml)")
		} else {
			markChecked("job.yaml")
		}

		// Require node image-cache snapshots (cache_before and cache_after)
		hasCacheBefore := fileExistsNonEmpty(filepath.Join(destDir, "cache_before.prom")) || fileExistsNonEmpty(filepath.Join(destDir, "cache_before.json"))
		hasCacheAfter := fileExistsNonEmpty(filepath.Join(destDir, "cache_after.prom")) || fileExistsNonEmpty(filepath.Join(destDir, "cache_after.json"))
		if !hasCacheBefore || !hasCacheAfter {
			addFailure("cluster RL bundle requires cache_before.{prom,json} and cache_after.{prom,json}")
		} else {
			markChecked("cache_before/cache_after")
		}
	}

	// 9. Mode-Specific Required: Warm / Suspend-Restore Runs
	isWarmOrSuspend := footing == "warm"
	if sysDesc.LifecyclePolicy != nil {
		pm := strings.ToLower(strings.TrimSpace(sysDesc.LifecyclePolicy.ParkMode))
		mode := strings.ToLower(strings.TrimSpace(sysDesc.LifecyclePolicy.Mode))
		if strings.HasPrefix(pm, "suspend") || sysDesc.LifecyclePolicy.PrewarmAtStart || (mode != "" && mode != "ephemeral" && mode != "on_demand" && mode != "cold") {
			isWarmOrSuspend = true
		}
	}
	if isWarmOrSuspend && isClusterRLRun {
		rtPath := filepath.Join(destDir, "restore_timings.jsonl")
		if !fileExistsNonEmpty(rtPath) {
			addFailure("warm/suspend cluster RL bundle requires non-empty restore_timings.jsonl")
		} else {
			markChecked("restore_timings.jsonl")
			rawRT, err := os.ReadFile(rtPath)
			if err != nil {
				addFailure("read restore_timings.jsonl: %v", err)
			} else {
				rtCount := 0
				err = schema.ForEachJSONL[schema.RestoreSample](bytes.NewReader(rawRT), func(_ int, _ schema.RestoreSample) error {
					rtCount++
					return nil
				})
				if err != nil {
					addFailure("invalid restore_timings.jsonl: %v", err)
				} else if rtCount == 0 {
					addFailure("restore_timings.jsonl has 0 restore samples in warm/suspend mode")
				}
			}
		}

		// Require extracted plain-text overlay/ directory and overlay/overlay.json capturing the behavioral changes and base recipe provenance
		ovDir := filepath.Join(destDir, "overlay")
		if ovInfo, err := os.Stat(ovDir); err != nil || !ovInfo.IsDir() {
			addFailure("warm/non-baseline cluster RL bundle requires overlay/ directory capturing feature overlay files and patch")
		} else {
			entries, _ := os.ReadDir(ovDir)
			if len(entries) == 0 {
				addFailure("overlay/ directory is empty")
			} else {
				markChecked("overlay/")
				ovMetaPath := filepath.Join(ovDir, "overlay.json")
				if rawMeta, err := os.ReadFile(ovMetaPath); err != nil {
					addFailure("overlay/ directory requires overlay/overlay.json with base_recipe_path and base_recipe_hash")
				} else {
					markChecked("overlay/overlay.json")
					if _, derr := schema.DecodeJSONStrict[schema.OverlayManifest](bytes.NewReader(rawMeta)); derr != nil {
						addFailure("invalid overlay/overlay.json: %v", derr)
					}
				}
			}
		}
	} else if fileExistsNonEmpty(filepath.Join(destDir, "restore_timings.jsonl")) {
		// If restore_timings.jsonl is non-empty on any run, validate its schema.
		rtPath := filepath.Join(destDir, "restore_timings.jsonl")
		markChecked("restore_timings.jsonl")
		if rawRT, err := os.ReadFile(rtPath); err == nil {
			if err := schema.ForEachJSONL[schema.RestoreSample](bytes.NewReader(rawRT), nil); err != nil {
				addFailure("invalid restore_timings.jsonl: %v", err)
			}
		}
	}

	return res, nil
}

func fileExistsNonEmpty(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir() && st.Size() > 0
}

// ensureRunWindowFile creates run_window.json if absent and merges metadata
// into it either way, so facts learned at report time land in the run metadata.
// If an existing run_window.json only holds a <=2s point-in-time placeholder
// and startTime/endTime span a real run window (>2s), start_ts and end_ts are
// upgraded to the real window bounds.
func ensureRunWindowFile(destDir, runID, cluster string, startTime, endTime time.Time, metadata map[string]string) error {
	rwPath := filepath.Join(destDir, "run_window.json")
	if fileExistsNonEmpty(rwPath) {
		existing, err := swebench.ReadRunWindow(rwPath)
		if err != nil {
			return err
		}
		if existing.Metadata == nil {
			existing.Metadata = map[string]string{}
		}
		maps.Copy(existing.Metadata, metadata)
		if !startTime.IsZero() && endTime.Sub(startTime) > 2*time.Second {
			exStart, errS := time.Parse(time.RFC3339, existing.StartTS)
			exEnd, errE := time.Parse(time.RFC3339, existing.EndTS)
			if errS != nil || errE != nil || exEnd.Sub(exStart) <= 2*time.Second {
				existing.StartTS = startTime.UTC().Format(time.RFC3339)
				existing.EndTS = endTime.UTC().Format(time.RFC3339)
			}
		}
		if strings.TrimSpace(existing.RunID) == "" && strings.TrimSpace(runID) != "" {
			existing.RunID = runID
		}
		if strings.TrimSpace(existing.Cluster) == "" && strings.TrimSpace(cluster) != "" {
			existing.Cluster = cluster
		}
		return swebench.WriteRunWindow(rwPath, *existing)
	}
	if strings.TrimSpace(runID) == "" {
		runID = "bench-run"
	}
	now := time.Now().UTC()
	if startTime.IsZero() {
		startTime = now.Add(-1 * time.Second)
	}
	if endTime.IsZero() || endTime.Before(startTime) {
		endTime = now
	}
	rw := schema.RunWindow{
		SchemaVersion: schema.SchemaVersion,
		RunID:         runID,
		StartTS:       startTime.UTC().Format(time.RFC3339),
		EndTS:         endTime.UTC().Format(time.RFC3339),
		Cluster:       cluster,
		Metadata:      maps.Clone(metadata),
	}
	b, err := json.MarshalIndent(rw, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(rwPath, append(b, '\n'), 0o644)
}

// GCSUploadRunner abstracts executing gcloud storage cp for unit testing.
type GCSUploadRunner interface {
	UploadDir(ctx context.Context, srcDir, destGCSURI string) ([]byte, error)
}

type gcloudUploadRunner struct{}

// gcloudUploadArgs builds the `gcloud storage cp -r` argument vector that copies
// the top-level contents of srcDir directly into destGCSURI (gs://<bucket>/<folder>/)
// without creating a nested <folder>/<basename(srcDir)>/ subdirectory (which
// `gcloud storage cp -r srcDir/.` does because Python os.path.normpath strips `/.`).
func gcloudUploadArgs(srcDir, destGCSURI string) ([]string, error) {
	cleanSrc := filepath.Clean(srcDir)
	entries, err := os.ReadDir(cleanSrc)
	if err != nil {
		return nil, fmt.Errorf("read upload directory %q: %w", cleanSrc, err)
	}
	var sources []string
	for _, entry := range entries {
		name := entry.Name()
		if name == "" || strings.HasPrefix(name, ".") {
			continue
		}
		sources = append(sources, filepath.Join(cleanSrc, name))
	}
	sort.Strings(sources)
	if len(sources) == 0 {
		return nil, fmt.Errorf("upload directory %q contains no files to upload", cleanSrc)
	}
	args := make([]string, 0, 4+len(sources))
	args = append(args, "storage", "cp", "-r")
	args = append(args, sources...)
	args = append(args, destGCSURI)
	return args, nil
}

func (gcloudUploadRunner) UploadDir(ctx context.Context, srcDir, destGCSURI string) ([]byte, error) {
	args, err := gcloudUploadArgs(srcDir, destGCSURI)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	return cmd.CombinedOutput()
}

var uploadRunner GCSUploadRunner = gcloudUploadRunner{}

func uploadCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("upload", flag.ContinueOnError)
	fs.SetOutput(stderr)
	destDir := fs.String("dest", "", "Local benchmark artifact directory ($ARTIFACT_DIR) to upload. Required.")
	bucketFlag := fs.String("bucket", "", "Target GCS bucket name or gs://<bucket> URI. Required (never hardcoded).")
	folderFlag := fs.String("folder", "", "Target folder name in bucket (<YYYY-MM-DD>_<model>_<dataset>_<nickname>[_traced]; _traced iff the bundle captured a trace); defaults to basename of -dest.")
	footing := fs.String("footing", "", "Optional footing override (warm or cold) for pre-upload validation.")
	skipValidate := fs.Bool("skip-validate", false, "Skip bundle validation prior to upload.")
	dryRun := fs.Bool("dry-run", false, "Validate bundle and target GCS URI without executing gcloud storage cp.")
	timeout := fs.Duration("timeout", 15*time.Minute, "Maximum time for gcloud storage upload.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*destDir) == "" {
		return fmt.Errorf("-dest is required")
	}
	bucket, err := NormalizeGCSBucket(*bucketFlag)
	if err != nil {
		return err
	}
	folder := strings.TrimSpace(*folderFlag)
	if folder == "" {
		folder = filepath.Base(filepath.Clean(*destDir))
	}
	traced, _ := bundleTraceEvidence(*destDir)
	if err := ValidateGCSFolderName(folder, traced); err != nil {
		return err
	}

	if !*skipValidate {
		vRes, verr := ValidateArtifactBundle(*destDir, swebench.ValidateOptions{Footing: *footing}, folder)
		if verr != nil {
			return verr
		}
		if !vRes.Valid {
			return fmt.Errorf("bundle validation failed prior to upload: %s", strings.Join(vRes.Reasons, "; "))
		}
	}

	destURI := fmt.Sprintf("gs://%s/%s/", bucket, folder)
	if *dryRun {
		fmt.Fprintf(stdout, "dry-run: validated %s -> %s\n", *destDir, destURI)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	out, err := uploadRunner.UploadDir(ctx, *destDir, destURI)
	if err != nil {
		return fmt.Errorf("upload to %s failed: %w\n%s", destURI, err, string(out))
	}
	fmt.Fprintf(stdout, "uploaded %s to %s\n", *destDir, destURI)
	return nil
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

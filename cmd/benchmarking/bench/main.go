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

// bench provides composable CLI subcommands (resolve, render, prewarm,
// measure, reap, report) for RL sandbox benchmarking. Each subcommand
// communicates strictly through $ARTIFACT_DIR (--dest) files.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/job"
	"github.com/agent-substrate/env/internal/benchmarking/measure"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/sut"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
)

func main() {
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr))
}

func execute(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		printUsage(stderr)
		return 2
	}
	var err error
	switch args[0] {
	case "resolve":
		err = resolveCmd(args[1:], stdout, stderr)
	case "render":
		err = renderCmd(args[1:], stdout, stderr)
	case "prewarm":
		err = prewarmCmd(args[1:], stdout, stderr)
	case "spinup":
		err = spinupCmd(args[1:], stdout, stderr)
	case "spindown":
		err = spindownCmd(args[1:], stdout, stderr)
	case "measure":
		err = measureCmd(args[1:], stderr)
	case "reap":
		err = reapCmd(args[1:], stdout, stderr)
	case "report":
		err = reportCmd(args[1:], stdout, stderr)
	case "validate":
		err = validateCmd(args[1:], stdout, stderr)
	case "upload":
		err = uploadCmd(args[1:], stdout, stderr)
	case "compare":
		err = compareCmd(args[1:], stdout, stderr)
	case "run":
		err = runScenarioCmd(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printUsage(stderr)
		return 0
	default:
		printUsage(stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: bench <resolve|render|prewarm|spinup|spindown|measure|reap|report|validate|upload|compare|run> [flags]")
}

func resolveCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("resolve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	scenarioPath := fs.String("scenario", "", "Path to Scenario YAML file. Required.")
	datasetPath := fs.String("dataset", "", "Default dataset JSONL path.")
	runID := fs.String("run-id", "rlb", "Run ID prefix.")
	destDir := fs.String("dest", "", "Artifact directory ($ARTIFACT_DIR).")
	outPath := fs.String("out", "", "Output schedule.json path (defaults to <dest>/schedule.json or schedule.json).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenarioPath == "" {
		return fmt.Errorf("-scenario is required")
	}
	if *outPath == "" {
		if *destDir != "" {
			*outPath = filepath.Join(*destDir, "schedule.json")
		} else {
			*outPath = "schedule.json"
		}
	}
	raw, err := os.ReadFile(*scenarioPath)
	if err != nil {
		return err
	}
	scen, err := schema.DecodeYAMLStrict[schema.Scenario](raw)
	if err != nil {
		return err
	}
	datasets := make(map[string][]swebench.Instance)
	for _, j := range scen.Jobs {
		dsSource := j.Dataset
		if dsSource == "" {
			dsSource = *datasetPath
		}
		if dsSource != "" {
			if _, ok := datasets[dsSource]; !ok {
				ds, derr := swebench.LoadDataset(dsSource)
				if derr != nil {
					return derr
				}
				datasets[dsSource] = ds
			}
		}
	}
	_ = *runID
	sched, err := swebench.ResolveScenarioSchedule(scen, *datasetPath, datasets)
	if err != nil {
		return err
	}
	if err := swebench.WriteSchedule(*outPath, sched); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "wrote %s (%d jobs)\n", *outPath, len(sched.Jobs))
	return nil
}

func renderCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	fs.SetOutput(stderr)
	scenarioPath := fs.String("scenario", "", "Path to Scenario YAML file. Required.")
	destDir := fs.String("dest", ".", "Destination artifact directory ($ARTIFACT_DIR).")
	cluster := fs.String("cluster", "local-cluster", "Cluster name recorded in system_description.json.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *scenarioPath == "" {
		return fmt.Errorf("-scenario is required")
	}
	raw, err := os.ReadFile(*scenarioPath)
	if err != nil {
		return err
	}
	scen, err := schema.DecodeYAMLStrict[schema.Scenario](raw)
	if err != nil {
		return err
	}
	prov := sut.NewProvisioner(nil)
	if _, err := prov.Provision(context.Background(), sut.ProvisionOptions{
		Spec:        scen.SUT,
		ArtifactDir: *destDir,
		RenderOnly:  true,
	}); err != nil {
		return err
	}
	sysDesc := sut.BuildSystemDescription(scen.SUT, *cluster, "v1.31", "HEAD", "verified", nil)
	if err := sut.WriteSystemDescription(filepath.Join(*destDir, "system_description.json"), sysDesc); err != nil {
		return err
	}
	sutPath := filepath.Join(*destDir, sut.DefaultManifestFilename)
	fmt.Fprintf(stdout, "rendered SUT manifest %s in %s\n", sutPath, *destDir)
	return nil
}

func prewarmCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("prewarm", flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoint := fs.String("endpoint", "localhost:7777", "ate-env API address.")
	atespace := fs.String("atespace", "ate-env", "Atespace.")
	baseTemplate := fs.String("base-template", "default-template", "Base template.")
	token := fs.String("token", "", "Optional bearer token for ate-env-api.")
	dataset := fs.String("dataset", "", "Dataset JSONL path. Required.")
	images := fs.Int("images", 1, "Number of distinct images to prewarm.")
	destDir := fs.String("dest", "", "Artifact directory ($ARTIFACT_DIR); writes events/prewarm.jsonl when set.")
	outPath := fs.String("out", "", "Optional explicit path for prewarm JSONL events.")
	timeout := fs.Duration("timeout", 30*time.Minute, "Bound on the whole step: every template is ensured and, with -golden-wait, every golden awaited within it. A bake takes about two minutes and an ateapi replica bakes five at a time, so -images 50 on two replicas needs about ten minutes.")
	waitGolden := fs.Bool("golden-wait", true, "Wait for each image's golden snapshot to settle (ready, failed, or none when the base template disables goldens); false ensures the templates only.")
	goldenPoll := fs.Duration("golden-poll", 10*time.Second, "How often to poll a baking golden.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dataset == "" {
		return fmt.Errorf("-dataset is required")
	}
	if *images <= 0 {
		return fmt.Errorf("-images must be greater than 0, got %d", *images)
	}
	all, err := swebench.LoadDataset(*dataset)
	if err != nil {
		return err
	}
	refs := distinctImages(all, *images)
	backend, err := swebench.NewEnvBackend(*endpoint, *atespace, *baseTemplate, 1, *token)
	if err != nil {
		return err
	}
	defer backend.Close()

	eventsFile := *outPath
	if eventsFile == "" && *destDir != "" {
		eventsFile = filepath.Join(*destDir, "events", "prewarm.jsonl")
	}
	var rec *swebench.Recorder
	var f *os.File
	if eventsFile != "" {
		if err := os.MkdirAll(filepath.Dir(eventsFile), 0o755); err != nil {
			return err
		}
		f, err = os.Create(eventsFile)
		if err != nil {
			return err
		}
		defer f.Close()
		rec = swebench.NewRecorder(f)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	results, perr := swebench.PrewarmTemplates(ctx, backend, refs, swebench.PrewarmOptions{
		WaitGolden: *waitGolden,
		GoldenPoll: *goldenPoll,
		Recorder:   rec,
	})
	for _, r := range results {
		if r.Err != nil {
			fmt.Fprintf(stdout, "prewarm %s: %v\n", r.Image, r.Err)
			continue
		}
		fmt.Fprintf(stdout, "prewarmed image %s: template %s, golden %s%s\n", r.Image, r.Template, r.Golden, map[bool]string{true: " " + r.Tag, false: ""}[r.Tag != "" && r.Golden == swebench.GoldenReady])
	}
	if perr != nil {
		return perr
	}
	if rec != nil {
		if err := rec.Err(); err != nil {
			return err
		}
	}
	return nil
}

// distinctImages returns the first n distinct images of the dataset, in
// dataset order. Tasks share images, so n counts images, not task rows. The
// result holds at most len(all) images, so n never sizes the allocation by
// itself.
func distinctImages(all []swebench.Instance, n int) []string {
	if n <= 0 {
		return nil
	}
	refs := make([]string, 0, min(n, len(all)))
	seen := map[string]bool{}
	for _, in := range all {
		if len(refs) == n {
			break
		}
		if !seen[in.Image] {
			seen[in.Image] = true
			refs = append(refs, in.Image)
		}
	}
	return refs
}

var lifecycleKubeRunner measure.KubeCommandRunner

func spinupCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("spinup", flag.ContinueOnError)
	fs.SetOutput(stderr)
	atespace := fs.String("atespace", "ate-env", "Kubernetes namespace / Substrate atespace.")
	namespace := fs.String("namespace", "", "Alias for -atespace: Kubernetes namespace containing the WorkerPool.")
	workerPool := fs.String("workerpool", "default-template-workerpool", "Substrate WorkerPool name.")
	replicas := fs.Int("replicas", 2, "Target WorkerPool replica count (> 0).")
	timeout := fs.Duration("timeout", 5*time.Minute, "Maximum time to wait for all WorkerPool pods to become Ready.")
	runID := fs.String("run-id", "rlb", "Run ID.")
	destDir := fs.String("dest", "", "Artifact directory ($ARTIFACT_DIR); merges spin-up metrics into measurements.json and writes events/substrate_lifecycle.jsonl.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *namespace != "" {
		*atespace = *namespace
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+10*time.Second)
	defer cancel()

	metrics, err := measure.SpinUpWorkerPool(ctx, lifecycleKubeRunner, *atespace, *workerPool, *replicas, *timeout)
	if err != nil {
		return err
	}
	wallMS := metrics["workerpool_spinup_wall_ms"].P50
	readyPods := int(metrics["worker_pod_count"].P50)
	if *destDir != "" {
		if err := recordLifecycleMetrics(*destDir, *runID, "workerpool_spinup", wallMS, readyPods, metrics); err != nil {
			return err
		}
	}
	fmt.Fprintf(stdout, "spun up workerpool/%s in %s (replicas=%d, wall_ms=%.1f)\n", *workerPool, *atespace, readyPods, wallMS)
	return nil
}

func spindownCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("spindown", flag.ContinueOnError)
	fs.SetOutput(stderr)
	atespace := fs.String("atespace", "ate-env", "Kubernetes namespace / Substrate atespace.")
	namespace := fs.String("namespace", "", "Alias for -atespace: Kubernetes namespace containing the WorkerPool.")
	workerPool := fs.String("workerpool", "default-template-workerpool", "Substrate WorkerPool name.")
	timeout := fs.Duration("timeout", 5*time.Minute, "Maximum time to wait for all WorkerPool pods to terminate.")
	runID := fs.String("run-id", "rlb", "Run ID.")
	destDir := fs.String("dest", "", "Artifact directory ($ARTIFACT_DIR); merges spin-down metrics into measurements.json and writes events/substrate_lifecycle.jsonl.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *namespace != "" {
		*atespace = *namespace
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+10*time.Second)
	defer cancel()

	metrics, err := measure.SpinDownWorkerPool(ctx, lifecycleKubeRunner, *atespace, *workerPool, *timeout)
	if err != nil {
		return err
	}
	wallMS := metrics["workerpool_spindown_wall_ms"].P50
	drained := int(metrics["workerpool_drained_pods"].P50)
	if *destDir != "" {
		if err := recordLifecycleMetrics(*destDir, *runID, "workerpool_spindown", wallMS, drained, metrics); err != nil {
			return err
		}
	}
	fmt.Fprintf(stdout, "spun down workerpool/%s in %s (drained=%d, wall_ms=%.1f)\n", *workerPool, *atespace, drained, wallMS)
	return nil
}

func recordLifecycleMetrics(destDir, runID, phaseName string, wallMS float64, count int, metrics map[string]schema.MetricSummary) error {
	eventsPath := filepath.Join(destDir, "events", "substrate_lifecycle.jsonl")
	if err := os.MkdirAll(filepath.Dir(eventsPath), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(eventsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	rec := swebench.NewRecorder(f)
	evMetrics := make(map[string]float64, len(metrics))
	for k, ms := range metrics {
		evMetrics[k] = ms.P50
	}
	rec.Record(swebench.Event{
		Time:    time.Now().UTC(),
		RunID:   runID,
		Event:   swebench.EventPhase,
		Name:    phaseName,
		Count:   count,
		DurMS:   wallMS,
		OK:      true,
		Metrics: evMetrics,
	})
	_ = f.Close()

	measPath := filepath.Join(destDir, "measurements.json")
	var meas schema.Measurements
	if raw, rerr := os.ReadFile(measPath); rerr == nil {
		_ = json.Unmarshal(raw, &meas)
	}
	if meas.SchemaVersion == "" {
		meas.SchemaVersion = schema.SchemaVersion
	}
	if meas.RunID == "" {
		meas.RunID = runID
	}
	if meas.CollectedAt.IsZero() {
		meas.CollectedAt = time.Now().UTC()
	}
	if meas.ServerSummary == nil {
		meas.ServerSummary = make(map[string]schema.MetricSummary, len(metrics))
	}
	if meas.CustomMetrics == nil {
		meas.CustomMetrics = make(map[string]schema.MetricSummary, len(metrics))
	}
	for k, ms := range metrics {
		meas.ServerSummary[k] = ms
		meas.CustomMetrics[k] = ms
	}
	if len(meas.PromMetrics) == 0 {
		val := wallMS
		meas.PromMetrics = []schema.PrometheusSeries{
			{
				Name:   phaseName + "_wall_ms",
				Labels: map[string]string{"unit": "ms"},
				Status: "ok",
				Value:  &val,
				Values: []float64{val},
			},
		}
	}
	return measure.WriteMeasurements(measPath, &meas)
}

type querySliceFlag []measure.PromQuery

func (q *querySliceFlag) String() string { return "" }

func (q *querySliceFlag) Set(val string) error {
	parts := strings.SplitN(val, "=", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return fmt.Errorf("expected name=expr, got %q", val)
	}
	*q = append(*q, measure.PromQuery{
		Name:     strings.TrimSpace(parts[0]),
		Expr:     strings.TrimSpace(parts[1]),
		Required: true,
	})
	return nil
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }

func (s *stringSliceFlag) Set(val string) error {
	*s = append(*s, val)
	return nil
}

func parseCLIPlugins(promFlags, logFlags, metricFlags, cmdFlags []string) ([]schema.TelemetryPluginSpec, error) {
	var out []schema.TelemetryPluginSpec
	for _, raw := range promFlags {
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("-plugin-prom expected name=expr, got %q", raw)
		}
		out = append(out, schema.TelemetryPluginSpec{
			Name: strings.TrimSpace(parts[0]),
			Kind: schema.PluginKindPromQuery,
			Expr: strings.TrimSpace(parts[1]),
		})
	}
	for _, raw := range logFlags {
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("-plugin-log expected name=namespace/labelSelector[:grepRegex], got %q", raw)
		}
		name := strings.TrimSpace(parts[0])
		specStr := strings.TrimSpace(parts[1])
		ns := "default"
		selectorAndGrep := specStr
		if slash := strings.IndexByte(specStr, '/'); slash > 0 {
			ns = specStr[:slash]
			selectorAndGrep = specStr[slash+1:]
		}
		selector := selectorAndGrep
		grep := ""
		if colon := strings.IndexByte(selectorAndGrep, ':'); colon > 0 {
			selector = selectorAndGrep[:colon]
			grep = selectorAndGrep[colon+1:]
		}
		out = append(out, schema.TelemetryPluginSpec{
			Name:          name,
			Kind:          schema.PluginKindK8sLogs,
			Namespace:     ns,
			LabelSelector: selector,
			Grep:          grep,
			Unit:          "lines",
		})
	}
	for _, raw := range metricFlags {
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("-plugin-metric expected name=glob:regex, got %q", raw)
		}
		name := strings.TrimSpace(parts[0])
		globAndRegex := strings.TrimSpace(parts[1])
		colon := strings.IndexByte(globAndRegex, ':')
		if colon <= 0 || colon == len(globAndRegex)-1 {
			return nil, fmt.Errorf("-plugin-metric expected name=glob:regex, got %q", raw)
		}
		out = append(out, schema.TelemetryPluginSpec{
			Name:  name,
			Kind:  schema.PluginKindRegexMetric,
			Glob:  globAndRegex[:colon],
			Regex: globAndRegex[colon+1:],
		})
	}
	for _, raw := range cmdFlags {
		parts := strings.SplitN(raw, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("-plugin-cmd expected name=command, got %q", raw)
		}
		out = append(out, schema.TelemetryPluginSpec{
			Name:    strings.TrimSpace(parts[0]),
			Kind:    schema.PluginKindCommand,
			Command: []string{"sh", "-c", strings.TrimSpace(parts[1])},
		})
	}
	return out, nil
}

func loadTelemetrySpecFromScenario(scenarioPath string) (*schema.TelemetrySpec, error) {
	if strings.TrimSpace(scenarioPath) == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(scenarioPath)
	if err != nil {
		return nil, err
	}
	scen, err := schema.DecodeYAMLStrict[schema.Scenario](raw)
	if err != nil {
		return nil, err
	}
	return scen.Telemetry, nil
}

func measureCmd(args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("measure", flag.ContinueOnError)
	fs.SetOutput(stderr)
	runID := fs.String("run-id", "rlb", "Run ID.")
	promURL := fs.String("prom-url", "", "Prometheus HTTP base URL.")
	cacheBefore := fs.String("cache-before", "", "Path to cache_before.json or cache_before.prom.")
	cacheAfter := fs.String("cache-after", "", "Path to cache_after.json or cache_after.prom.")
	sampleNodeCache := fs.String("sample-node-cache", "", "Scrape K8s node image-cache counters into this .prom file and exit.")
	restoreLog := fs.String("restore-log", "", "Path to restore_timings.jsonl.")
	restoreTimings := fs.String("restore-timings", "", "Alias for -restore-log: path to restore_timings.jsonl.")
	minRestores := fs.Int("min-restores", 0, "Minimum expected restore timing records.")
	destDir := fs.String("dest", "", "Artifact directory ($ARTIFACT_DIR).")
	outPath := fs.String("out", "", "Output measurements.json path (defaults to <dest>/measurements.json or measurements.json).")
	k8sCollect := fs.Bool("k8s", false, "Collect Kubernetes node/pod/metrics.k8s.io and Substrate telemetry.")
	ateEndpoint := fs.String("ate-endpoint", "", "Optional Substrate ate-api gRPC endpoint for ListActors/GetActorTemplate.")
	atespace := fs.String("atespace", "ate-env", "Substrate atespace for K8s/actor telemetry.")
	baseTemplate := fs.String("base-template", "default-template", "Base actor template name.")
	clusterName := fs.String("cluster", "", "Optional cluster name override for system_description.json.")
	sysOut := fs.String("sys-out", "", "Optional output path for system_description.json (defaults to <dest>/system_description.json when -k8s and -dest are set).")
	scopesFlag := fs.String("scopes", "", "Comma-separated telemetry scopes to collect (rl,user-sandbox,substrate,k8s or all; default all).")
	collectorsFlag := fs.String("collectors", "", "Optional comma-separated collector names to run.")
	scenarioPath := fs.String("scenario", "", "Optional Scenario YAML path to load telemetry.scopes and telemetry.plugins.")
	var queries querySliceFlag
	var pluginProms, pluginLogs, pluginMetrics, pluginCmds stringSliceFlag
	fs.Var(&queries, "query", "Prometheus query in name=expr format (repeatable, required).")
	fs.Var(&pluginProms, "plugin-prom", "Ad-hoc Prometheus feature plugin in name=expr format (repeatable).")
	fs.Var(&pluginLogs, "plugin-log", "Ad-hoc K8s pod log harvester in name=ns/labelSelector[:grepRegex] format (repeatable).")
	fs.Var(&pluginMetrics, "plugin-metric", "Ad-hoc log/JSONL regex metric extractor in name=glob:regex format (repeatable).")
	fs.Var(&pluginCmds, "plugin-cmd", "Ad-hoc diagnostic command plugin in name=cmd format (repeatable).")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *restoreLog == "" && *restoreTimings != "" {
		*restoreLog = *restoreTimings
	}
	if *sampleNodeCache != "" {
		promText, err := measure.CollectK8sNodeCachePromText(context.Background(), nil, nil)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(*sampleNodeCache), 0o755); err != nil {
			return err
		}
		return os.WriteFile(*sampleNodeCache, []byte(promText), 0o644)
	}
	if *outPath == "" {
		if *destDir != "" {
			*outPath = filepath.Join(*destDir, "measurements.json")
		} else {
			*outPath = "measurements.json"
		}
	}
	if *sysOut == "" && *k8sCollect && *destDir != "" {
		*sysOut = filepath.Join(*destDir, "system_description.json")
	}
	if *destDir != "" {
		if *cacheBefore == "" {
			for _, name := range []string{"cache_before.json", "cache_before.prom"} {
				p := filepath.Join(*destDir, name)
				if _, err := os.Stat(p); err == nil {
					*cacheBefore = p
					break
				}
			}
		}
		if *cacheAfter == "" {
			for _, name := range []string{"cache_after.json", "cache_after.prom"} {
				p := filepath.Join(*destDir, name)
				if _, err := os.Stat(p); err == nil {
					*cacheAfter = p
					break
				}
			}
		}
		if *restoreLog == "" {
			p := filepath.Join(*destDir, "restore_timings.jsonl")
			if _, err := os.Stat(p); err == nil {
				*restoreLog = p
			}
		}
	}
	telSpec, err := loadTelemetrySpecFromScenario(*scenarioPath)
	if err != nil {
		return err
	}
	var scopeInputs []string
	if *scopesFlag != "" {
		scopeInputs = append(scopeInputs, *scopesFlag)
	} else if telSpec != nil {
		for _, s := range telSpec.Scopes {
			scopeInputs = append(scopeInputs, string(s))
		}
	}
	activeScopes, err := schema.ParseTelemetryScopes(scopeInputs...)
	if err != nil {
		return err
	}
	var enabledCollectors []string
	if *collectorsFlag != "" {
		enabledCollectors = append(enabledCollectors, *collectorsFlag)
	} else if telSpec != nil && len(telSpec.Collectors) > 0 {
		enabledCollectors = append(enabledCollectors, telSpec.Collectors...)
	}
	cliPlugins, err := parseCLIPlugins(pluginProms, pluginLogs, pluginMetrics, pluginCmds)
	if err != nil {
		return err
	}
	var plugins []schema.TelemetryPluginSpec
	if telSpec != nil {
		plugins = append(plugins, telSpec.Plugins...)
	}
	plugins = append(plugins, cliPlugins...)

	var scraper *measure.PrometheusScraper
	if *promURL != "" {
		scraper = &measure.PrometheusScraper{BaseURL: *promURL}
		if len(queries) == 0 && len(pluginProms) == 0 {
			queries = measure.MustDefaultPromQueries()
		}
	}
	res, err := measure.RunCollectors(context.Background(), measure.CollectRequest{
		RunID:             *runID,
		DestDir:           *destDir,
		Scopes:            activeScopes,
		EnabledCollectors: enabledCollectors,
		Scraper:           scraper,
		Queries:           queries,
		CacheBeforePath:   *cacheBefore,
		CacheAfterPath:    *cacheAfter,
		RestoreTimingPath: *restoreLog,
		MinRestores:       *minRestores,
		EnableK8s:         *k8sCollect,
		K8s: measure.K8sCollectOptions{
			Cluster:      *clusterName,
			Atespace:     *atespace,
			BaseTemplate: *baseTemplate,
			ATEEndpoint:  *ateEndpoint,
		},
		Plugins: plugins,
	})
	if err != nil {
		return err
	}
	if *sysOut != "" && res.SystemDescription != nil {
		if err := sut.WriteSystemDescription(*sysOut, *res.SystemDescription); err != nil {
			return err
		}
	}
	return measure.WriteMeasurements(*outPath, res.Measurements)
}

// dialReaper opens the server-queried reaper reapCmd uses; tests replace it.
var dialReaper = func(endpoint string) (sut.LiveResourceListerDeleter, func() error, error) {
	reaper, closeFn, err := sut.DialATEClusterReaper(endpoint)
	if err != nil {
		return nil, nil, err
	}
	return reaper, closeFn, nil
}

func reapCmd(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("reap", flag.ContinueOnError)
	fs.SetOutput(stderr)
	ateEndpoint := fs.String("ate-endpoint", "", "Substrate ate-api endpoint for server-queried reap.")
	atespace := fs.String("atespace", "ate-env", "Atespace to reap.")
	prefix := fs.String("prefix", "", "Run ID prefix to reap.")
	timeout := fs.Duration("timeout", 2*time.Minute, "Reap timeout.")
	pollInterval := fs.Duration("poll-interval", 500*time.Millisecond, "Poll interval between reap checks.")
	destDir := fs.String("dest", "", "Artifact directory ($ARTIFACT_DIR); writes events/reap.jsonl when set.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *ateEndpoint == "" {
		fmt.Fprintf(stdout, "reap: no -ate-endpoint specified, 0 remaining\n")
		return nil
	}
	reaper, closeFn, err := dialReaper(*ateEndpoint)
	if err != nil {
		return err
	}
	defer func() { _ = closeFn() }()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	res, reapErr := sut.Reap(ctx, reaper, *atespace, *prefix, *pollInterval)
	if *destDir != "" && res != nil {
		eventsDir := filepath.Join(*destDir, "events")
		if mkErr := os.MkdirAll(eventsDir, 0o755); mkErr == nil {
			if f, ferr := os.Create(filepath.Join(eventsDir, "reap.jsonl")); ferr == nil {
				rec := swebench.NewRecorder(f)
				now := time.Now().UTC()
				evRunID := strings.TrimRight(*prefix, "-")
				if evRunID == "" {
					evRunID = *prefix
				}
				var delIDs []string
				for id := range res.DeleteDurations {
					delIDs = append(delIDs, id)
				}
				sort.Strings(delIDs)
				for _, id := range delIDs {
					d := res.DeleteDurations[id]
					rec.Record(swebench.Event{
						Time:  now,
						RunID: evRunID,
						Env:   id,
						Event: swebench.EventDelete,
						DurMS: float64(d.Microseconds()) / 1000.0,
						OK:    true,
					})
				}
				ev := swebench.Event{
					Time:  now,
					RunID: evRunID,
					Task:  0,
					Env:   evRunID,
					Event: swebench.EventReap,
					DurMS: float64(res.Duration.Microseconds()) / 1000.0,
					OK:    reapErr == nil && res.Remaining == 0,
					Metrics: map[string]float64{
						"orphan_count":  float64(res.Remaining),
						"deleted_count": float64(res.Deleted),
					},
				}
				if reapErr != nil {
					ev.Err = reapErr.Error()
				}
				rec.Record(ev)
				_ = rec.Flush()
				_ = f.Close()
			}
		}
	}
	if reapErr != nil {
		return reapErr
	}
	fmt.Fprintf(stdout, "reaped %d environments (remaining=%d, duration=%s)\n", res.Deleted, res.Remaining, res.Duration)
	return nil
}

type benchArtifactReport struct {
	*swebench.Report
	Measurements *schema.Measurements `json:"measurements,omitempty"`
}

func reportCmd(args []string, stdout, stderr io.Writer) error {
	var path string
	flagArgs := args
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		path = args[0]
		flagArgs = args[1:]
	}
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	destDir := fs.String("dest", "", "Artifact directory containing events/*.jsonl, measurements.json, system_description.json.")
	reportOnly := fs.Bool("report-only", false, "Generate report.json from existing artifacts in -dest.")
	jsonOut := fs.String("json", "", "Write the summary as JSON to this path.")
	unresolvedOut := fs.String("unresolved", "", "Write unresolved instances to this path in -exclude-file format.")
	unresolvedNote := fs.String("unresolved-note", "Instances graded unresolved", "First comment line of the -unresolved file.")
	scopesFlag := fs.String("scopes", "", "Comma-separated telemetry scopes to include (rl,user-sandbox,substrate,k8s or all; default all).")
	scenarioPath := fs.String("scenario", "", "Optional Scenario YAML path to load telemetry.scopes and telemetry.plugins.")
	var pluginMetrics stringSliceFlag
	fs.Var(&pluginMetrics, "plugin-metric", "Ad-hoc log/JSONL regex metric extractor in name=glob:regex format (repeatable).")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	_ = reportOnly
	if path == "" && fs.NArg() > 0 {
		path = fs.Arg(0)
	}
	telSpec, err := loadTelemetrySpecFromScenario(*scenarioPath)
	if err != nil {
		return err
	}
	var scopeInputs []string
	if *scopesFlag != "" {
		scopeInputs = append(scopeInputs, *scopesFlag)
	} else if telSpec != nil {
		for _, s := range telSpec.Scopes {
			scopeInputs = append(scopeInputs, string(s))
		}
	}
	activeScopes, err := schema.ParseTelemetryScopes(scopeInputs...)
	if err != nil {
		return err
	}
	cliPlugins, err := parseCLIPlugins(nil, nil, pluginMetrics, nil)
	if err != nil {
		return err
	}
	var plugins []schema.TelemetryPluginSpec
	if telSpec != nil {
		plugins = append(plugins, telSpec.Plugins...)
	}
	plugins = append(plugins, cliPlugins...)

	if *destDir != "" {
		return reportFromArtifactDir(*destDir, *jsonOut, activeScopes, plugins, stdout)
	}
	if path == "" {
		return fmt.Errorf("report needs a JSONL file or -dest DIR")
	}
	rep, err := swebench.LoadOrSummarizeReport(path)
	if err != nil {
		return err
	}
	if *scopesFlag != "" || (telSpec != nil && len(telSpec.Scopes) > 0) {
		rep.Scopes = activeScopes
	}
	rep.WriteText(stdout)
	return swebench.WriteReportFiles(rep, *jsonOut, "", *unresolvedOut, *unresolvedNote, "", "", "")
}

func reportFromArtifactDir(destDir, jsonOut string, activeScopes []schema.TelemetryScope, plugins []schema.TelemetryPluginSpec, stdout io.Writer) error {
	ingRes, err := job.IngestArtifactDir(job.IngestContext{
		DestDir: destDir,
		Scopes:  activeScopes,
		Plugins: plugins,
	})
	if err != nil {
		return err
	}
	matches, err := filepath.Glob(filepath.Join(destDir, "events", "*.jsonl"))
	if err != nil {
		return err
	}
	sort.Strings(matches)
	var combined bytes.Buffer
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			return err
		}
		combined.Write(b)
		if len(b) > 0 && b[len(b)-1] != '\n' {
			combined.WriteByte('\n')
		}
	}
	rawCombined := combined.Bytes()
	rep, err := swebench.Summarize(bytes.NewReader(rawCombined))
	if err != nil {
		return err
	}
	parsedEvents, parsedEventsErr := swebench.ReadEvents(bytes.NewReader(rawCombined))
	// atelet_restores.jsonl, the raw harvest, comes first: it keeps the failed
	// records and every phase, which the derived restore_timings.jsonl drops,
	// and a reused artifact directory can hold a restore_timings.jsonl from an
	// earlier run. Each time the raw file wins the join, restore_timings.jsonl
	// is rewritten from it, so bench validate reads this run's samples and a
	// second render gives the same restores block. restore_timings.jsonl is the
	// fallback for a bundle without the raw file.
	restoreTimingsPath := filepath.Join(destDir, "restore_timings.jsonl")
	for _, rtName := range []string{"atelet_restores.jsonl", "restore_timings.jsonl"} {
		rtFile, err := os.Open(filepath.Join(destDir, rtName))
		if err != nil {
			continue
		}
		timings, parseErr := swebench.ParseRestoreTimings(rtFile)
		_ = rtFile.Close()
		if parseErr != nil || len(timings) == 0 {
			continue
		}
		joined := false
		if parsedEventsErr == nil {
			if rb := swebench.JoinRestores(parsedEvents, timings); rb.First.Restores+rb.Wakes.Restores > 0 {
				rep.Restores = rb
				joined = true
			}
		}
		if rtName == "atelet_restores.jsonl" && (joined || !fileExistsNonEmpty(restoreTimingsPath)) {
			_ = materializeRestoreTimingsJSONL(restoreTimingsPath, timings)
		}
		if joined {
			break
		}
	}
	rep.Scopes = activeScopes
	if ingRes != nil && len(ingRes.CustomMetrics) > 0 {
		if rep.CustomMetrics == nil {
			rep.CustomMetrics = make(map[string]schema.MetricSummary, len(ingRes.CustomMetrics))
		}
		for k, v := range ingRes.CustomMetrics {
			rep.CustomMetrics[k] = v
		}
	}
	rep.WriteText(stdout)

	outRep := benchArtifactReport{Report: rep}
	measPath := filepath.Join(destDir, "measurements.json")
	if rawMeas, err := os.ReadFile(measPath); err == nil {
		meas, derr := schema.DecodeJSONStrict[schema.Measurements](bytes.NewReader(rawMeas))
		if derr != nil {
			return fmt.Errorf("invalid measurements.json: %w", derr)
		}
		if rollUpSnapshotsIntoMeasurements(destDir, &meas) || len(meas.PromMetrics) == 0 {
			meas.PromMetrics = buildPromSeriesFromSnapshotsOrSummary(destDir, &meas)
		}
		_ = measure.WriteMeasurements(measPath, &meas)
		outRep.Measurements = &meas
		if len(meas.CustomMetrics) > 0 {
			if rep.CustomMetrics == nil {
				rep.CustomMetrics = make(map[string]schema.MetricSummary, len(meas.CustomMetrics))
			}
			for k, v := range meas.CustomMetrics {
				if _, exists := rep.CustomMetrics[k]; !exists {
					rep.CustomMetrics[k] = v
				}
			}
		}
	}
	for _, pair := range [][2]string{
		{filepath.Join(destDir, "cache_before.prom"), filepath.Join(destDir, "cache_after.prom")},
		{filepath.Join(destDir, "cache_before.json"), filepath.Join(destDir, "cache_after.json")},
	} {
		cb, errB := swebench.LoadNodeCacheSnapshot(pair[0])
		ca, errA := swebench.LoadNodeCacheSnapshot(pair[1])
		if errB == nil && errA == nil {
			if d := cb.Delta(ca); d.Hits+d.Misses > 0 {
				rep.Cache = &d
				rep.PopulateCacheMissesPerFirstCmd()
			}
			break
		}
	}
	var sysDescPtr *schema.SystemDescription
	sysPath := filepath.Join(destDir, "system_description.json")
	if rawSys, err := os.ReadFile(sysPath); err == nil {
		sysDesc, derr := schema.DecodeJSONStrict[schema.SystemDescription](bytes.NewReader(rawSys))
		if derr != nil {
			return fmt.Errorf("invalid system_description.json: %w", derr)
		}
		sysDescPtr = &sysDesc
	}
	if sysDescPtr != nil {
		enrichSystemDescriptionFromArtifactDir(destDir, sysDescPtr, rep, outRep.Measurements)
	}
	rep.AttachClusterTelemetry(sysDescPtr, outRep.Measurements)
	swebench.NormalizeLegacyReport(rep)
	rep.RecordTraceCapture(false, len(swebench.FindTraceFiles(destDir)))
	if sysDescPtr != nil {
		_ = sut.WriteSystemDescription(sysPath, *sysDescPtr)
	}

	var mdBuf bytes.Buffer
	rep.WriteMarkdown(&mdBuf)
	_ = os.WriteFile(filepath.Join(destDir, "report.md"), mdBuf.Bytes(), 0o644)

	runID := rep.RunID
	if runID == "" && len(rep.Runs) > 0 {
		runID = rep.Runs[0]
	} else if runID == "" && outRep.Measurements != nil {
		runID = outRep.Measurements.RunID
	}
	clusterName := ""
	if sysDescPtr != nil {
		clusterName = sysDescPtr.Cluster
	}
	var startT, endT time.Time
	if outRep.Measurements != nil {
		if outRep.Measurements.StartTS != nil {
			startT = *outRep.Measurements.StartTS
		}
		if outRep.Measurements.EndTS != nil {
			endT = *outRep.Measurements.EndTS
		}
	}
	if parsedEventsErr == nil && (startT.IsZero() || endT.Sub(startT) <= 2*time.Second) {
		if evS, evE, ok := inferEventBounds(parsedEvents); ok {
			startT = evS
			endT = evE
		}
	}
	if err := ensureRunWindowFile(destDir, runID, clusterName, startT, endT, rep.TraceMetadata()); err != nil {
		return fmt.Errorf("run_window.json: %w", err)
	}

	targetJSON := jsonOut
	if targetJSON == "" {
		targetJSON = filepath.Join(destDir, "report.json")
	}
	b, err := json.MarshalIndent(outRep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(targetJSON, append(b, '\n'), 0o644)
}

func inferEventBounds(events []swebench.Event) (time.Time, time.Time, bool) {
	scan := func(filterTimed bool) (time.Time, time.Time, bool) {
		var minS, maxE time.Time
		found := false
		for _, ev := range events {
			if ev.Time.IsZero() {
				continue
			}
			if filterTimed && (ev.Event == swebench.EventPrewarm || ev.Event == swebench.EventRun || ev.Event == swebench.EventReap) {
				continue
			}
			evStart := ev.Time
			if ev.DurMS > 0 {
				evStart = ev.Time.Add(-time.Duration(ev.DurMS * float64(time.Millisecond)))
			}
			if !found || evStart.Before(minS) {
				minS = evStart
			}
			if !found || ev.Time.After(maxE) {
				maxE = ev.Time
			}
			found = true
		}
		return minS, maxE, found && maxE.After(minS)
	}
	if s, e, ok := scan(true); ok {
		return s, e, true
	}
	return scan(false)
}

func materializeRestoreTimingsJSONL(outPath string, timings map[string][]swebench.RestoreTiming) error {
	var samples []schema.RestoreSample
	for _, list := range timings {
		for _, rt := range list {
			if rt.Actor == "" || rt.Err != "" {
				continue
			}
			totalSec, hasTotal := rt.Phases["total"]
			if !hasTotal || totalSec < 0 {
				continue
			}
			var tsPtr *time.Time
			if !rt.Time.IsZero() {
				t := rt.Time.UTC()
				tsPtr = &t
			}
			samples = append(samples, schema.RestoreSample{
				SchemaVersion:   schema.SchemaVersion,
				Env:             rt.Actor,
				Kind:            rt.Fidelity,
				TS:              tsPtr,
				TotalMS:         totalSec * 1000.0,
				ManifestFetchMS: rt.Phases["manifest_fetch"] * 1000.0,
				DownloadMS:      rt.Phases["download"] * 1000.0,
				OCIUnpackMS:     rt.Phases["oci_unpack"] * 1000.0,
				WorkerRestoreMS: rt.Phases["worker_restore"] * 1000.0,
				PersistMS:       rt.Phases["persist"] * 1000.0,
			})
		}
	}
	if len(samples) == 0 {
		return nil
	}
	sort.Slice(samples, func(i, j int) bool {
		ti, tj := time.Time{}, time.Time{}
		if samples[i].TS != nil {
			ti = *samples[i].TS
		}
		if samples[j].TS != nil {
			tj = *samples[j].TS
		}
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return samples[i].Env < samples[j].Env
	})
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, s := range samples {
		if err := enc.Encode(s); err != nil {
			return err
		}
	}
	return os.WriteFile(outPath, buf.Bytes(), 0o644)
}

var (
	sdkRefRe        = regexp.MustCompile(`sandboxenv-rl\.git@([0-9a-fA-F]{7,40})`)
	hfDatasetRe     = regexp.MustCompile(`repo_id="([^"]+)"\s*,\s*repo_type="dataset"`)
	uniAgentDsRe    = regexp.MustCompile(`(?m)^(r2e_gym_easy|r2e_gym|swe_rebench|swe_bench):\s*loaded\s+(\d+),\s*excluded\s+\d+,\s*wrote\s+(\d+)\s+rows\s+to\s+([^\s]+)\s+\((\d+)\s+distinct\s+images`)
	trainFilesRe    = regexp.MustCompile(`data\.train_files=([^\s]+)`)
	filterLenRe     = regexp.MustCompile(`filter dataset len:\s*(\d+)`)
	datasetLenRe    = regexp.MustCompile(`dataset len:\s*(\d+)`)
	trainBatchSzRe  = regexp.MustCompile(`(?:data\.train_batch_size=|'train_batch_size'\s*:\s*)([1-9]\d*)`)
	ppoMiniBatchRe  = regexp.MustCompile(`(?:actor_rollout_ref\.actor\.ppo_mini_batch_size=|'ppo_mini_batch_size'\s*:\s*)(\d+)`)
	rolloutNRe      = regexp.MustCompile(`(?:actor_rollout_ref\.rollout\.n=|'rollout_n'\s*:\s*)(\d+)`)
	totalTrainStpRe = regexp.MustCompile(`(?:trainer\.total_training_steps|total_train_steps|'total_training_steps')\s*[:=]?\s*([1-9]\d*)`)
	maxSessionsRe   = regexp.MustCompile(`(?:max_concurrent_sessions=|'max_concurrent_sessions'\s*:\s*)(\d+)`)
	warmupBatchesRe = regexp.MustCompile(`(?:num_warmup_batches=|'num_warmup_batches'\s*:\s*)(\d+)`)
)

func enrichSystemDescriptionFromArtifactDir(destDir string, sys *schema.SystemDescription, rep *swebench.Report, meas *schema.Measurements) {
	if sys == nil {
		return
	}
	// 1. Check schedule.json if present in destDir.
	if rawSched, err := os.ReadFile(filepath.Join(destDir, "schedule.json")); err == nil {
		if sched, derr := schema.DecodeJSONStrict[schema.Schedule](bytes.NewReader(rawSched)); derr == nil && len(sched.Jobs) > 0 {
			j0 := sched.Jobs[0]
			instSet := make(map[string]bool)
			imgSet := make(map[string]bool)
			var firstImg string
			for _, st := range j0.Steps {
				for _, t := range st.Tasks {
					if t.InstanceID != "" {
						instSet[t.InstanceID] = true
					}
					if t.Image != "" {
						if firstImg == "" {
							firstImg = t.Image
						}
						imgSet[t.Image] = true
					}
				}
			}
			if sys.GuestImage == "" && firstImg != "" {
				sys.GuestImage = firstImg
			}
			if sys.DatasetRevision == "" {
				if rep != nil && rep.RunTags != nil && rep.RunTags["dataset"] != "" {
					sys.DatasetRevision = rep.RunTags["dataset"]
				} else if sched.ScenarioName != "" {
					sys.DatasetRevision = fmt.Sprintf("%s (seed=%d, %d instances)", sched.ScenarioName, sched.Seed, len(instSet))
				}
			}
			stepsCount := len(j0.Steps)
			var tasksPerStep, groupSize, trajPerStep int
			if stepsCount > 0 {
				step1 := j0.Steps[0]
				trajPerStep = len(step1.Tasks)
				step1Inst := make(map[string]int)
				maxRollout := 0
				for _, t := range step1.Tasks {
					step1Inst[t.InstanceID]++
					if t.Rollout+1 > maxRollout {
						maxRollout = t.Rollout + 1
					}
				}
				tasksPerStep = len(step1Inst)
				if maxRollout > 1 {
					groupSize = maxRollout
				} else if tasksPerStep > 0 {
					groupSize = trajPerStep / tasksPerStep
				}
				if groupSize <= 0 {
					groupSize = 1
				}
			}
			maxConc := trajPerStep
			if rep != nil && rep.StandingPoolSize > maxConc {
				maxConc = rep.StandingPoolSize
			}
			sys.Load = &schema.LoadParams{
				Tasks:               len(instSet),
				GroupSize:           groupSize,
				Steps:               stepsCount,
				TasksPerStep:        tasksPerStep,
				TrajectoriesPerStep: groupSize * tasksPerStep,
				Repeat:              1,
				MaxConcurrent:       maxConc,
				DistinctImages:      len(imgSet),
			}
		}
	}

	// 2. Check verl-head.log / train.log if present in destDir.
	for _, logName := range []string{"verl-head.log", "train.log"} {
		rawLog, err := os.ReadFile(filepath.Join(destDir, logName))
		if err != nil || len(rawLog) == 0 {
			continue
		}
		text := string(rawLog)
		if m := sdkRefRe.FindStringSubmatch(text); len(m) == 2 {
			if sys.SDKVersion == "" {
				sys.SDKVersion = m[1]
			}
			if sys.GitSHA == "" {
				sys.GitSHA = m[1]
			}
		}
		var dsName string
		var datasetTasks, tasksPerStep, groupSize, stepsCount, distinctImgs, maxConc int
		if m := uniAgentDsRe.FindStringSubmatch(text); len(m) == 6 {
			src := m[1]
			if src == "r2e_gym_easy" {
				dsName = fmt.Sprintf("hfilaretov/Benchmark-R2E-Gym-Easy (%s)", m[4])
			} else {
				dsName = fmt.Sprintf("%s (%s)", src, m[4])
			}
			datasetTasks, _ = strconv.Atoi(m[3])
			distinctImgs, _ = strconv.Atoi(m[5])
		} else {
			if m := hfDatasetRe.FindStringSubmatch(text); len(m) == 2 {
				dsName = m[1]
			}
			if m := trainFilesRe.FindStringSubmatch(text); len(m) == 2 {
				if dsName != "" {
					dsName = fmt.Sprintf("%s (%s)", dsName, m[1])
				} else {
					dsName = m[1]
				}
			}
		}
		if sys.DatasetRevision == "" && dsName != "" {
			sys.DatasetRevision = dsName
		}

		if datasetTasks <= 0 {
			if m := filterLenRe.FindStringSubmatch(text); len(m) == 2 {
				datasetTasks, _ = strconv.Atoi(m[1])
			} else if m := datasetLenRe.FindStringSubmatch(text); len(m) == 2 {
				datasetTasks, _ = strconv.Atoi(m[1])
			}
		}
		if m := trainBatchSzRe.FindStringSubmatch(text); len(m) == 2 {
			tasksPerStep, _ = strconv.Atoi(m[1])
		} else if m := ppoMiniBatchRe.FindStringSubmatch(text); len(m) == 2 {
			tasksPerStep, _ = strconv.Atoi(m[1])
		}
		if m := rolloutNRe.FindStringSubmatch(text); len(m) == 2 {
			groupSize, _ = strconv.Atoi(m[1])
		}
		if m := totalTrainStpRe.FindStringSubmatch(text); len(m) == 2 {
			stepsCount, _ = strconv.Atoi(m[1])
		}
		if m := maxSessionsRe.FindStringSubmatch(text); len(m) == 2 {
			maxConc, _ = strconv.Atoi(m[1])
		}
		if stepsCount <= 0 && rep != nil && len(rep.Steps) > 0 {
			stepsCount = len(rep.Steps)
		}
		if datasetTasks <= 0 && rep != nil {
			datasetTasks = rep.DistinctInstances
		}
		if groupSize <= 0 {
			groupSize = 1
		}
		if tasksPerStep <= 0 && rep != nil && len(rep.RolloutBreakdown) > 0 {
			tasksPerStep = rep.RolloutBreakdown[0].Tasks
		}
		if maxConc <= 0 && meas != nil && meas.ServerSummary != nil {
			if as, ok := meas.ServerSummary["active_sandboxes"]; ok && as.P50 > 0 {
				maxConc = int(as.P50)
			}
		}
		if maxConc <= 0 && rep != nil && rep.DistinctEnvs > 0 {
			maxConc = rep.DistinctEnvs
		}
		if distinctImgs <= 0 {
			distinctImgs = 1
			if rep != nil && len(rep.DistinctImages) > 0 {
				distinctImgs = len(rep.DistinctImages)
			}
		}
		if tasksPerStep > 0 || stepsCount > 0 {
			sys.Load = &schema.LoadParams{
				Tasks:               datasetTasks,
				GroupSize:           groupSize,
				Steps:               stepsCount,
				TasksPerStep:        tasksPerStep,
				TrajectoriesPerStep: groupSize * tasksPerStep,
				Repeat:              1,
				MaxConcurrent:       maxConc,
				DistinctImages:      distinctImgs,
			}
		}
		if m := warmupBatchesRe.FindStringSubmatch(text); len(m) == 2 {
			if wb, werr := strconv.Atoi(m[1]); werr == nil && wb >= 0 {
				if sys.LifecyclePolicy == nil {
					sys.LifecyclePolicy = &schema.SandboxLifecyclePolicy{}
				}
				sys.LifecyclePolicy.TrainerWarmupBatches = wb
			}
		}
		break
	}

	// 3. Populate GitSHA / SDKVersion from git rev-parse HEAD if still empty.
	if sys.GitSHA == "" || sys.SDKVersion == "" {
		if out, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
			sha := strings.TrimSpace(string(out))
			if sha != "" {
				if sys.GitSHA == "" {
					sys.GitSHA = sha
				}
				if sys.SDKVersion == "" {
					sys.SDKVersion = sha
				}
			}
		}
	}
	if sys.OSKernel == "" && sys.KubernetesVersion != "" {
		sys.OSKernel = "Container-Optimized OS from Google (Linux 6.6+)"
	}
	if sys.StorageDriverAndFS == "" && sys.KubernetesVersion != "" {
		sys.StorageDriverAndFS = "overlayfs / ext4 (containerd)"
	}

	// 4. Populate Overlays from overlay/overlay.json when present in destDir.
	if rawOv, err := os.ReadFile(filepath.Join(destDir, "overlay", "overlay.json")); err == nil {
		if ovMeta, derr := schema.DecodeJSONStrict[schema.OverlayManifest](bytes.NewReader(rawOv)); derr == nil {
			sys.Overlays = []schema.OverlayRef{{
				Name:             ovMeta.Name,
				Path:             ovMeta.Path,
				Hash:             ovMeta.Hash,
				BaseRecipePath:   ovMeta.BaseRecipePath,
				BaseRecipeHash:   ovMeta.BaseRecipeHash,
				BaseRecipeCommit: ovMeta.BaseRecipeCommit,
				PatchFile:        ovMeta.PatchFile,
				BaseFileHashes:   ovMeta.BaseFileHashes,
				SUTOverrides:     ovMeta.EnvOverrides,
			}}
		}
	}
}

func nearestRankPct(sorted []float64, p float64) float64 {
	return swebench.NearestRankPct(sorted, p)
}

func roundStatVal(v float64) float64 {
	return swebench.RoundStatVal(v)
}

func rollUpSnapshotsIntoMeasurements(destDir string, meas *schema.Measurements) bool {
	if meas == nil {
		return false
	}
	snapFiles, _ := filepath.Glob(filepath.Join(destDir, "measurements_snapshots", "*.json"))
	sort.Strings(snapFiles)
	if len(snapFiles) == 0 {
		return false
	}
	type snapAccum struct {
		unit       string
		allSingle  bool
		totalCount int
		totalSum   float64
		mins       []float64
		p50s       []float64
		p90s       []float64
		p95s       []float64
		p99s       []float64
		maxs       []float64
	}
	accums := make(map[string]*snapAccum)
	for _, sf := range snapFiles {
		raw, err := os.ReadFile(sf)
		if err != nil {
			continue
		}
		var snap schema.Measurements
		if err := json.Unmarshal(raw, &snap); err != nil || len(snap.ServerSummary) == 0 {
			continue
		}
		for k, ms := range snap.ServerSummary {
			if ms.Count <= 0 {
				continue
			}
			ac, ok := accums[k]
			if !ok {
				ac = &snapAccum{unit: ms.Unit, allSingle: true}
				accums[k] = ac
			}
			if ac.unit == "" && ms.Unit != "" {
				ac.unit = ms.Unit
			}
			if ms.Count > 1 {
				ac.allSingle = false
			}
			ac.totalCount += ms.Count
			if ms.Sum != 0 {
				ac.totalSum += ms.Sum
			} else {
				ac.totalSum += ms.P50 * float64(ms.Count)
			}
			ac.mins = append(ac.mins, ms.Min)
			ac.p50s = append(ac.p50s, ms.P50)
			p90 := ms.P90
			if p90 == 0 && ms.P50 > 0 {
				p90 = ms.P50
			}
			p95 := ms.P95
			if p95 == 0 {
				p95 = p90
			}
			p99 := ms.P99
			if p99 == 0 {
				p99 = p95
			}
			mx := ms.Max
			if mx == 0 {
				mx = p99
			}
			ac.p90s = append(ac.p90s, p90)
			ac.p95s = append(ac.p95s, p95)
			ac.p99s = append(ac.p99s, p99)
			ac.maxs = append(ac.maxs, mx)
		}
	}
	if len(accums) == 0 {
		return false
	}
	if meas.ServerSummary == nil {
		meas.ServerSummary = make(map[string]schema.MetricSummary, len(accums))
	}
	for k, ac := range accums {
		if len(ac.p50s) == 0 {
			continue
		}
		sort.Float64s(ac.mins)
		sort.Float64s(ac.p50s)
		sort.Float64s(ac.p90s)
		sort.Float64s(ac.p95s)
		sort.Float64s(ac.p99s)
		sort.Float64s(ac.maxs)
		if ac.allSingle {
			var sum float64
			for _, v := range ac.p50s {
				sum += v
			}
			meas.ServerSummary[k] = schema.MetricSummary{
				Count: len(ac.p50s),
				Sum:   roundStatVal(sum),
				Min:   roundStatVal(ac.p50s[0]),
				P50:   roundStatVal(nearestRankPct(ac.p50s, 0.50)),
				P90:   roundStatVal(nearestRankPct(ac.p50s, 0.90)),
				P95:   roundStatVal(nearestRankPct(ac.p50s, 0.95)),
				P99:   roundStatVal(nearestRankPct(ac.p50s, 0.99)),
				Max:   roundStatVal(ac.p50s[len(ac.p50s)-1]),
				Unit:  ac.unit,
			}
		} else {
			gMin := ac.mins[0]
			gMax := ac.maxs[len(ac.maxs)-1]
			p50 := nearestRankPct(ac.p50s, 0.50)
			p90 := math.Max(p50, nearestRankPct(ac.p90s, 0.90))
			p95 := math.Max(p90, nearestRankPct(ac.p95s, 0.95))
			p99 := math.Max(p95, nearestRankPct(ac.p99s, 0.99))
			if p99 > gMax {
				p99 = gMax
			}
			meas.ServerSummary[k] = schema.MetricSummary{
				Count: ac.totalCount,
				Sum:   roundStatVal(ac.totalSum),
				Min:   roundStatVal(gMin),
				P50:   roundStatVal(p50),
				P90:   roundStatVal(p90),
				P95:   roundStatVal(p95),
				P99:   roundStatVal(p99),
				Max:   roundStatVal(gMax),
				Unit:  ac.unit,
			}
		}
	}
	return true
}

func buildPromSeriesFromSnapshotsOrSummary(destDir string, meas *schema.Measurements) []schema.PrometheusSeries {
	if meas == nil {
		return nil
	}
	seriesByMetric := make(map[string]*schema.PrometheusSeries)
	var metricOrder []string

	snapFiles, _ := filepath.Glob(filepath.Join(destDir, "measurements_snapshots", "*.json"))
	sort.Strings(snapFiles)
	for _, sf := range snapFiles {
		raw, err := os.ReadFile(sf)
		if err != nil {
			continue
		}
		var snap schema.Measurements
		if err := json.Unmarshal(raw, &snap); err != nil || len(snap.ServerSummary) == 0 {
			continue
		}
		for k, ms := range snap.ServerSummary {
			ps, exists := seriesByMetric[k]
			if !exists {
				val := ms.P50
				ps = &schema.PrometheusSeries{
					Name:   k,
					Labels: map[string]string{"unit": ms.Unit},
					Status: "ok",
					Value:  &val,
				}
				seriesByMetric[k] = ps
				metricOrder = append(metricOrder, k)
			}
			val := ms.P50
			ps.Value = &val
			ps.Values = append(ps.Values, val)
		}
	}

	for k, ms := range meas.ServerSummary {
		ps, exists := seriesByMetric[k]
		if !exists {
			val := ms.P50
			ps = &schema.PrometheusSeries{
				Name:   k,
				Labels: map[string]string{"unit": ms.Unit},
				Status: "ok",
				Value:  &val,
				Values: []float64{val},
			}
			seriesByMetric[k] = ps
			metricOrder = append(metricOrder, k)
		}
	}
	for _, existing := range meas.PromMetrics {
		cp := existing
		ps, exists := seriesByMetric[cp.Name]
		if !exists {
			seriesByMetric[cp.Name] = &cp
			metricOrder = append(metricOrder, cp.Name)
		} else if len(cp.Values) > len(ps.Values) {
			seriesByMetric[cp.Name] = &cp
		}
	}
	sort.Strings(metricOrder)
	var out []schema.PrometheusSeries
	for _, k := range metricOrder {
		out = append(out, *seriesByMetric[k])
	}
	return out
}

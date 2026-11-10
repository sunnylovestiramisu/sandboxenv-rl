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

// swebench-loader drives SWE-bench task images through the ate-env API as a
// simulated RL job and writes one JSONL event per operation.
//
//	swebench-loader run    -dataset manifest.jsonl -mode eval  -images 500 -tasks-per-image 6 -width 500 -out run.jsonl
//	swebench-loader run    -dataset manifest.jsonl -mode steps -images 500 -rollouts 2 -steps 8 -park suspend -out run.jsonl
//	swebench-loader report run.jsonl [-json summary.json]
//	swebench-loader report a.jsonl b.jsonl   (one table, a column per run)
//	swebench-loader report -arm pull=p1.jsonl,p2.jsonl -arm stream=s1.jsonl,s2.jsonl   (a column per arm, reps merged)
//	swebench-loader report run.jsonl -cache-before before.txt -cache-after after.txt -restore-log restores.jsonl
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/benchmarking/swebench"
	"github.com/agent-substrate/env/internal/benchmarking/trace"
	"github.com/agent-substrate/env/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// tracingServiceName is the OpenTelemetry service.name of the loader's spans
// and the prefix of its run span.
const tracingServiceName = "swebench-loader"

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr))
}

func runMain(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		usage(stderr)
		return 2
	}
	var err error
	switch args[0] {
	case "run":
		err = runCmd(args[1:])
	case "render-plan":
		err = renderPlanCmd(args[1:])
	case "resolve":
		err = resolveCmd(args[1:])
	case "plan-placement":
		err = planPlacementCmd(args[1:], stderr)
	case "preflight":
		err = preflightCmd(args[1:], stderr)
	case "convert-trace":
		err = convertTraceCmd(args[1:])
	case "report", "compare":
		err = reportCmdWithIO(args[1:], stdout, stderr)
	case "trajectory-assets":
		err = assetsCmd(args[1:], stdout)
	case "-h", "--help", "help":
		usage(stderr)
		return 0
	default:
		if strings.HasPrefix(args[0], "-convert-trace") || strings.HasPrefix(args[0], "--convert-trace") {
			err = runCmd(args)
			break
		}
		usage(stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

// authTokenEnv names the environment variable that holds the bearer token,
// the same one the ate-env CLI reads.
const authTokenEnv = "SUBSTRATE_ENV_AUTH_TOKEN"

// testBackendOverride allows hermetic end-to-end CLI tests in main_test.go to
// exercise runCmd without dialing a live gRPC server.
var testBackendOverride swebench.Backend

// resolveAuthToken returns the token from file when file is set, otherwise
// from authTokenEnv, otherwise "" (no token sent). A file that holds only
// whitespace is an error, because it is a misconfiguration and not a request
// to run without the token.
func resolveAuthToken(file string, getenv func(string) string) (string, error) {
	if file == "" {
		return strings.TrimSpace(getenv(authTokenEnv)), nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("reading -auth-token-file: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("-auth-token-file %s is empty", file)
	}
	return token, nil
}

// labelFlags collects repeated KEY=VALUE flags (-label and -set on run).
type labelFlags map[string]string

func (l labelFlags) String() string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+l[k])
	}
	return strings.Join(parts, ",")
}

func (l labelFlags) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("want KEY=VALUE, got %q", v)
	}
	if strings.ContainsAny(v, "\t\n") {
		return fmt.Errorf("%q: no tabs or newlines, they break the report's table", v)
	}
	if _, dup := l[k]; dup {
		return fmt.Errorf("%s is given twice", k)
	}
	l[k] = val
	return nil
}

// arm is one -arm value: a column of the comparison and the files pooled
// into it.
type arm struct {
	name  string
	files []string
}

// armFlags collects repeated -arm NAME=FILE[,FILE...] flags in command-line
// order, so the baseline given first is the first column.
type armFlags []arm

func (a *armFlags) String() string {
	parts := make([]string, 0, len(*a))
	for _, x := range *a {
		parts = append(parts, x.name+"="+strings.Join(x.files, ","))
	}
	return strings.Join(parts, " ")
}

func (a *armFlags) Set(v string) error {
	name, list, ok := strings.Cut(v, "=")
	if !ok || name == "" || strings.TrimSpace(list) == "" {
		return fmt.Errorf("want NAME=FILE[,FILE...], got %q", v)
	}
	for _, x := range *a {
		if x.name == name {
			return fmt.Errorf("arm %s is given twice", name)
		}
	}
	var files []string
	for _, f := range strings.Split(list, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			return fmt.Errorf("arm %s: empty file name in %q", name, list)
		}
		if slices.Contains(files, f) {
			return fmt.Errorf("arm %s: %s is given twice", name, f)
		}
		files = append(files, f)
	}
	*a = append(*a, arm{name, files})
	return nil
}

// datasetLabel is the dataset's file name. For an https URL that is the last
// path element without the query, so a signed URL's signature stays out of
// the events file, which operators share.
func datasetLabel(dataset string) string {
	if u, err := url.Parse(dataset); err == nil && u.Scheme != "" && u.Host != "" {
		return path.Base(u.Path)
	}
	return filepath.Base(dataset)
}

// ownLabels are the loader's labels that only the command line knows (the
// rest come from Config.labels); every key is in swebench.LoaderLabelKeys or
// is a var's effective value under set.KEY. The exclude file and the
// instance list are named by their file name or count plus a digest of their
// ids, so two lists of one length differ. The delete rate is recorded as
// given, 0 for no cap, so report -arm keeps runs with different caps apart:
// the cap's queue is in their delete and park times.
func ownLabels(dataset string, connections int, deleteRate float64, baseTemplate, excludeFile, placement string, exclude, instances []string, vars map[string]string) map[string]string {
	l := map[string]string{
		"dataset":       datasetLabel(dataset),
		"connections":   strconv.Itoa(connections),
		"delete_rate":   strconv.FormatFloat(max(deleteRate, 0), 'g', -1, 64),
		"base_template": baseTemplate,
	}
	if placement != "" {
		l["placement"] = filepath.Base(placement)
	}
	for k, v := range vars {
		l["set."+k] = v
	}
	if excludeFile != "" {
		l["exclude_file"] = filepath.Base(excludeFile) + "@" + idsDigest(exclude)
	}
	if len(instances) > 0 {
		l["instances"] = strconv.Itoa(len(instances)) + "@" + idsDigest(instances)
	}
	return l
}

// placementMissing lists the selected images the plan neither places nor
// lists as unplaced, sorted: a run with -placement must know every image's
// fate, or the plan is for another dataset.
func placementMissing(plan *schema.ImagePlacement, selected []swebench.Instance) []string {
	known := map[string]bool{}
	for _, e := range plan.Images {
		known[e.Image] = true
	}
	for _, u := range plan.Unplaced {
		known[u.Image] = true
	}
	seen := map[string]bool{}
	var missing []string
	for _, inst := range selected {
		if !known[inst.Image] && !seen[inst.Image] {
			seen[inst.Image] = true
			missing = append(missing, inst.Image)
		}
	}
	slices.Sort(missing)
	return missing
}

// idsDigest is the first eight hex digits of the SHA-256 of the sorted ids.
func idsDigest(ids []string) string {
	sorted := slices.Clone(ids)
	slices.Sort(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:4])
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: swebench-loader run [flags] | swebench-loader render-plan [flags] | swebench-loader resolve [flags] | swebench-loader plan-placement [flags] | swebench-loader preflight [flags] | swebench-loader convert-trace [flags] | swebench-loader report FILE... [-json OUT] | swebench-loader trajectory-assets FILE")
}

func renderPlanCmd(args []string) error {
	fs := flag.NewFlagSet("render-plan", flag.ContinueOnError)
	var (
		dataset     = fs.String("dataset", "", "JSONL manifest of instances; a path or an https URL. Required.")
		images      = fs.Int("images", 0, "Use the first N instances of the dataset; 0 means all.")
		instances   = fs.String("instances", "", "Comma-separated instance ids to use instead of the first N.")
		excludeFile = fs.String("exclude-file", "", "File of instance ids to leave out, one per line.")
		trajectory  = fs.String("trajectory", swebench.DefaultTrajectory, fmt.Sprintf("Trajectory file or built-in name (%s); a comma-separated list mixes them.", strings.Join(swebench.Names(), ", ")))
		seed        = fs.Int64("seed", 1, "Seed for trajectory mix assignment.")
		fanout      = fs.Int("fanout", 1, "Rollouts / tasks per image when rendering a trajectory mix.")
		out         = fs.String("out", "", "Output path for rendered plan JSON. Required.")
		vars        = labelFlags{}
	)
	fs.Var(vars, "set", "KEY=VALUE for the trajectories' var function, repeatable.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dataset == "" {
		return fmt.Errorf("-dataset is required")
	}
	if *out == "" {
		return fmt.Errorf("-out is required")
	}
	all, err := swebench.LoadDataset(*dataset)
	if err != nil {
		return err
	}
	var ids []string
	if *instances != "" {
		ids = strings.Split(*instances, ",")
	}
	var exclude []string
	if *excludeFile != "" {
		if exclude, err = swebench.ReadIDList(*excludeFile); err != nil {
			return err
		}
	}
	selected, err := swebench.Select(all, ids, exclude, *images)
	if err != nil {
		return err
	}
	if *images > 0 && len(selected) < *images && len(ids) == 0 {
		fmt.Fprintf(os.Stderr, "note: %d instances requested, %d available after the exclude list\n", *images, len(selected))
	}
	trajSpec := *trajectory
	if trajSpec == swebench.DefaultTrajectory && len(selected) > 0 && selected[0].Kind == swebench.CommandKind {
		trajSpec = swebench.CommandTrajectoryName
	}
	var trajs []*swebench.Trajectory
	for _, source := range strings.Split(trajSpec, ",") {
		if source = strings.TrimSpace(source); source == "" {
			continue
		}
		t, err := swebench.LoadTrajectory(source)
		if err != nil {
			return err
		}
		t.Vars = vars
		if err := swebench.RequireGrading(selected, t); err != nil {
			return err
		}
		for _, in := range selected {
			if _, err := t.Render(in); err != nil {
				return err
			}
		}
		trajs = append(trajs, t)
	}
	if len(trajs) == 0 {
		return fmt.Errorf("-trajectory names no trajectory")
	}
	effective := map[string]string{}
	if len(selected) > 0 {
		for _, t := range trajs {
			u, err := t.VarsUsed(selected[0])
			if err != nil {
				return err
			}
			maps.Copy(effective, u)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(vars)) {
		if _, ok := effective[k]; !ok {
			return fmt.Errorf("-set %s: no trajectory reads it (they read: %s)", k, strings.Join(slices.Sorted(maps.Keys(effective)), ", "))
		}
	}
	var excludeLbl, instancesLbl string
	if *excludeFile != "" {
		excludeLbl = filepath.Base(*excludeFile) + "@" + idsDigest(exclude)
	}
	if len(ids) > 0 {
		instancesLbl = strconv.Itoa(len(ids)) + "@" + idsDigest(ids)
	}
	plan, err := swebench.BuildRenderedPlan(
		*dataset,
		datasetLabel(*dataset),
		excludeLbl,
		instancesLbl,
		selected,
		trajs,
		*seed,
		*fanout,
		effective,
	)
	if err != nil {
		return err
	}
	return swebench.WriteRenderedPlan(*out, plan)
}

// planPlacementCmd writes image-placement.json: which placement domains hold
// each task image, under each domain's per-node image-cache budget. The
// pre-flight fills the caches from it and the run steers creates with it.
func planPlacementCmd(args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("plan-placement", flag.ContinueOnError)
	var (
		dataset        = fs.String("dataset", "", "JSONL manifest of instances; a path or an https URL. Required. Image sizes come from its image_bytes_compressed (make-dataset.py).")
		schedule       = fs.String("schedule", "", "schedule.json from resolve. When given, its images are placed, with each image's demand (the most environments one step asks for), and the whole manifest supplies the sizes; -images, -instances and -exclude-file then do not apply. Otherwise every selected image of the manifest is placed.")
		domainsFile    = fs.String("domains", "", "domains.json: the placement domains, each with the worker-pool labels its per-environment worker selector names, its node count and node_cache_bytes, the unpacked bytes one node may hold. Required.")
		images         = fs.Int("images", 0, "Use the first N instances of the dataset; 0 means all.")
		instances      = fs.String("instances", "", "Comma-separated instance ids to use instead of the first N.")
		excludeFile    = fs.String("exclude-file", "", "File of instance ids to leave out, one per line.")
		replication    = fs.Int("replication", 1, "Domains per image.")
		hotDemand      = fs.Int("hot-demand", 0, "An image whose demand reaches this goes to -hot-replication domains; 0 disables. Needs -schedule.")
		hotReplication = fs.Int("hot-replication", 2, "Domains per hot image.")
		unpackFactor   = fs.Float64("unpack-factor", swebench.DefaultUnpackFactor, "Unpacked bytes on a node per compressed byte.")
		defaultSize    = fs.String("default-image-size", "3GiB", "Compressed size assumed for an image the manifest gives none for (size_assumed in the plan); 0 leaves such images unplaced.")
		out            = fs.String("out", "image-placement.json", "Output path.")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("plan-placement takes no positional arguments, got %q", fs.Args())
	}
	if *dataset == "" || *domainsFile == "" {
		return fmt.Errorf("-dataset and -domains are required")
	}
	if *hotDemand > 0 && *schedule == "" {
		return fmt.Errorf("-hot-demand needs -schedule, which gives each image's demand")
	}
	if *schedule != "" && (*images != 0 || *instances != "" || *excludeFile != "") {
		return fmt.Errorf("-schedule names the images to place; -images, -instances and -exclude-file apply only without it")
	}
	defaultBytes, err := swebench.ParseByteSize(*defaultSize)
	if err != nil {
		return fmt.Errorf("-default-image-size: %w", err)
	}
	all, err := swebench.LoadDataset(*dataset)
	if err != nil {
		return err
	}
	var ids []string
	if *instances != "" {
		ids = strings.Split(*instances, ",")
	}
	var exclude []string
	if *excludeFile != "" {
		if exclude, err = swebench.ReadIDList(*excludeFile); err != nil {
			return err
		}
	}
	// With a schedule the whole manifest supplies the sizes, so a scheduled
	// image is never left to the assumed size by a filter it did not go through.
	selected := all
	if *schedule == "" {
		if selected, err = swebench.Select(all, ids, exclude, *images); err != nil {
			return err
		}
	}
	domains, err := swebench.ReadPlacementDomains(*domainsFile)
	if err != nil {
		return err
	}
	opts := swebench.PlanOptions{
		Instances:              selected,
		Domains:                domains,
		Replication:            *replication,
		HotDemand:              *hotDemand,
		HotReplication:         *hotReplication,
		UnpackFactor:           *unpackFactor,
		DefaultBytesCompressed: defaultBytes,
	}
	if *schedule != "" {
		if opts.Schedule, err = swebench.ReadSchedule(*schedule); err != nil {
			return err
		}
	}
	plan, err := swebench.PlanImagePlacement(opts)
	if err != nil {
		return err
	}
	if err := swebench.WriteImagePlacement(*out, plan); err != nil {
		return err
	}
	fmt.Fprintf(stderr, "placed %d images in %d domains; %d unplaced; wrote %s\n", len(plan.Images), len(plan.Domains), len(plan.Unplaced), *out)
	for _, d := range plan.Domains {
		fill := "no budget"
		if d.NodeCacheBytes > 0 {
			fill = fmt.Sprintf("%.0f%% of %s", 100*float64(d.AssignedBytes)/float64(d.NodeCacheBytes), swebench.FormatByteSize(d.NodeCacheBytes))
		}
		fmt.Fprintf(stderr, "  %s: %d images, %s unpacked per node (%s), %d nodes\n", d.Name, d.ImageCount, swebench.FormatByteSize(d.AssignedBytes), fill, d.Nodes)
	}
	assumed := 0
	for _, e := range plan.Images {
		if e.SizeAssumed {
			assumed++
		}
	}
	if assumed > 0 {
		fmt.Fprintf(stderr, "  %d images had no size in the manifest; %s assumed for each (rebuild the manifest with make-dataset.py for real sizes)\n", assumed, *defaultSize)
	}
	if under := swebench.UnderReplicated(plan, *hotDemand, *hotReplication); len(under) > 0 {
		fmt.Fprintf(stderr, "  %d images fit fewer domains than asked\n", len(under))
	}
	for _, u := range plan.Unplaced {
		fmt.Fprintf(stderr, "  unplaced: %s: %s\n", u.Image, u.Reason)
	}
	return nil
}

// testPreflightBackend replaces the ate-env backend in the preflight
// subcommand's tests.
var testPreflightBackend swebench.ImageWarmer

// preflightCmd runs the PreflightWarmer over an image placement plan: it
// ensures each image's derived template, waits for the golden, and probes
// every node of every domain with the image, so the run that follows finds
// the layers and the golden snapshot on the node.
func preflightCmd(args []string, stderr io.Writer) (retErr error) {
	fs := flag.NewFlagSet("preflight", flag.ContinueOnError)
	var (
		plan          = fs.String("plan", "", "image-placement.json from plan-placement. Required.")
		endpoint      = fs.String("endpoint", "127.0.0.1:7777", "ate-env-api address.")
		atespace      = fs.String("atespace", "ate-env", "Substrate atespace of the environments and templates.")
		baseTemplate  = fs.String("base-template", "default-template", "Base ActorTemplate the derived templates come from.")
		connections   = fs.Int("connections", 1, "gRPC connections to ate-env-api.")
		authTokenFile = fs.String("auth-token-file", "", "File with the ate-env-api bearer token; SUBSTRATE_ENV_AUTH_TOKEN otherwise.")
		runID         = fs.String("run-id", "", "Prefix of the probe environment names (at most 20 characters; default pf-<6 random characters>, unique per pass).")
		eventsDir     = fs.String("events-dir", "", "Directory for preflight-events.jsonl and preflight-coverage.json; required unless -out is given.")
		out           = fs.String("out", "", "Events file to write instead of -events-dir.")
		coverageOut   = fs.String("coverage-out", "", "Where to write preflight-coverage.json; default <events-dir>/preflight-coverage.json.")
		probesPerNode = fs.Int("probes-per-node", 2, "Probes a round starts per node of a domain; the round holds them until all are placed.")
		imageConc     = fs.Int("image-concurrency", 4, "Images a domain warms at once; node_slots in the plan caps it at node_slots / probes-per-node.")
		maxRounds     = fs.Int("max-rounds", 3, "Rounds per (image, domain) before the pair is reported short.")
		goldenTimeout = fs.Duration("golden-timeout", 30*time.Minute, "Give up on the goldens still baking once none became ready for this long.")
		goldenPoll    = fs.Duration("golden-poll", 10*time.Second, "How often to poll a baking golden.")
		probeTimeout  = fs.Duration("probe-timeout", 10*time.Minute, "Bound on one probe's create and first command.")
		images        = fs.String("images", "", "Comma-separated images to restrict the pass to; default every image of the plan.")
		domains       = fs.String("domains", "", "Comma-separated domains to restrict the pass to; default every domain of the plan.")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("preflight takes no positional arguments, got %q", fs.Args())
	}
	if *plan == "" {
		return fmt.Errorf("-plan is required")
	}
	if *eventsDir == "" && *out == "" {
		return fmt.Errorf("-events-dir or -out is required")
	}
	if *eventsDir != "" && *out != "" {
		return fmt.Errorf("-events-dir and -out exclude each other")
	}
	placement, err := swebench.ReadImagePlacement(*plan)
	if err != nil {
		return err
	}
	coveragePath := *coverageOut
	if coveragePath == "" {
		if *eventsDir != "" {
			coveragePath = filepath.Join(*eventsDir, "preflight-coverage.json")
		} else {
			coveragePath = "preflight-coverage.json"
		}
	}

	var backend swebench.ImageWarmer
	if testPreflightBackend != nil {
		backend = testPreflightBackend
	} else {
		authToken, err := resolveAuthToken(*authTokenFile, os.Getenv)
		if err != nil {
			return err
		}
		b, err := swebench.NewEnvBackend(*endpoint, *atespace, *baseTemplate, *connections, authToken)
		if err != nil {
			return err
		}
		defer b.Close()
		backend = b
	}

	// The events go next to the run's events/ directory, not into it: the
	// run report aggregates events/*.jsonl and must not count the probes.
	eventsPath := *out
	if *eventsDir != "" {
		eventsPath = filepath.Join(*eventsDir, "preflight-events.jsonl")
	}
	rec, err := swebench.NewFileRecorder(eventsPath, *runID)
	if err != nil {
		return err
	}
	rec.SetWorker("preflight")
	defer func() {
		if rerr := rec.Close(); rerr != nil && retErr == nil {
			retErr = fmt.Errorf("writing events: %w", rerr)
		}
	}()

	log := slog.New(slog.NewJSONHandler(stderr, nil))
	warmer, err := swebench.NewPreflightWarmer(swebench.PreflightOptions{
		Plan:             placement,
		PlanPath:         *plan,
		Backend:          backend,
		Recorder:         rec,
		Logger:           log,
		RunID:            *runID,
		ProbesPerNode:    *probesPerNode,
		ImageConcurrency: *imageConc,
		MaxRounds:        *maxRounds,
		GoldenTimeout:    *goldenTimeout,
		GoldenPoll:       *goldenPoll,
		ProbeTimeout:     *probeTimeout,
		Images:           splitList(*images),
		Domains:          splitList(*domains),
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, runErr := warmer.Run(ctx)
	if report != nil {
		wrotePath := coveragePath
		if werr := swebench.WritePreflightCoverage(coveragePath, report); werr != nil {
			wrotePath = ""
			if runErr == nil {
				runErr = werr
			} else {
				log.Warn("preflight: writing coverage failed", slog.String("path", coveragePath), slog.Any("err", werr))
			}
		}
		printPreflightSummary(stderr, report, wrotePath)
	}
	return runErr
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func printPreflightSummary(w io.Writer, r *schema.PreflightCoverage, path string) {
	ready, none := 0, 0
	for _, g := range r.Goldens {
		switch {
		case g.State == "ready":
			ready++
		case swebench.GoldenSettled(g):
			none++
		}
	}
	fmt.Fprintf(w, "pre-flight %s: goldens %d/%d ready", map[bool]string{true: "complete", false: "INCOMPLETE"}[r.Complete], ready, len(r.Goldens))
	if none > 0 {
		fmt.Fprintf(w, ", %d without a golden (disabled on the template)", none)
	}
	if path != "" {
		fmt.Fprintf(w, "; wrote %s\n", path)
	} else {
		fmt.Fprintln(w)
	}
	for _, d := range r.Domains {
		done := 0
		for _, ic := range d.Images {
			if ic.Complete {
				done++
			}
		}
		fmt.Fprintf(w, "  %s: %d/%d images on all %d nodes (%d seen)\n", d.Name, done, len(d.Images), d.Nodes, len(d.NodesSeen))
		for _, ic := range d.Images {
			if !ic.Complete {
				fmt.Fprintf(w, "    short: %s on %d/%d nodes after %d rounds, %d/%d probes failed%s\n", ic.Image, len(ic.Nodes), ic.Wanted, ic.Rounds, ic.Failed, ic.Probes, map[bool]string{true: ": " + ic.Error, false: ""}[ic.Error != ""])
			}
		}
	}
	for _, g := range r.Goldens {
		if !swebench.GoldenSettled(g) {
			fmt.Fprintf(w, "  golden %s: %s %s\n", g.Image, g.State, g.Error)
		}
	}
}

// assetsCmd prints the files a trajectory's write steps name, one per line,
// for run-in-cluster.sh to bake next to it.
func assetsCmd(args []string, w io.Writer) error {
	if len(args) != 1 {
		return errors.New("usage: swebench-loader trajectory-assets FILE")
	}
	names, err := swebench.TrajectoryAssets(args[0])
	if err != nil {
		return err
	}
	for _, n := range names {
		fmt.Fprintln(w, n)
	}
	return nil
}

func convertTraceCmd(args []string) error {
	files, rest := leadingFiles(args)
	fs := flag.NewFlagSet("convert-trace", flag.ContinueOnError)
	var (
		in           = fs.String("in", "", "Input trace.jsonl path (or pass as first positional argument).")
		traceFlag    = fs.String("convert-trace", "", "Alias for -in: input trace.jsonl path.")
		outDir       = fs.String("out", "", "Output directory for schedule.json, placement.json, trajectory.yaml, and instances.jsonl.")
		traceOutFlag = fs.String("trace-out", "", "Alias for -out: output directory for converted trace artifacts.")
		scenarioName = fs.String("scenario", "", "Scenario name for converted schedule.json.")
		defaultJob   = fs.String("job", "", "Default job name when trace events omit job.")
		defaultImage = fs.String("default-image", "", "Default pinned digest image when trace events omit image.")
	)
	if err := fs.Parse(rest); err != nil {
		return err
	}
	tracePath := *in
	if tracePath == "" {
		tracePath = *traceFlag
	}
	if tracePath == "" && len(files) > 0 {
		tracePath = files[0]
	}
	if tracePath == "" && fs.NArg() > 0 {
		tracePath = fs.Arg(0)
	}
	if tracePath == "" {
		return fmt.Errorf("-in (or positional trace.jsonl) is required")
	}
	targetDir := *outDir
	if targetDir == "" {
		targetDir = *traceOutFlag
	}
	if targetDir == "" {
		return fmt.Errorf("-out (or -trace-out) directory is required")
	}
	f, err := os.Open(tracePath)
	if err != nil {
		return fmt.Errorf("open trace %s: %w", tracePath, err)
	}
	defer f.Close()
	conv, err := trace.ConvertTrace(f, trace.ConvertOptions{
		ScenarioName: *scenarioName,
		DefaultJob:   *defaultJob,
		DefaultImage: *defaultImage,
	})
	if err != nil {
		return err
	}
	return trace.WriteConvertedArtifacts(targetDir, conv)
}

func resolveCmd(args []string) error {
	fs := flag.NewFlagSet("resolve", flag.ContinueOnError)
	var (
		dataset      = fs.String("dataset", "", "JSONL manifest of instances; a path or an https URL. Required.")
		images       = fs.Int("images", 0, "Use the first N instances of the dataset; 0 means all.")
		instances    = fs.String("instances", "", "Comma-separated instance ids to use instead of the first N.")
		excludeFile  = fs.String("exclude-file", "", "File of instance ids to leave out, one per line.")
		steps        = fs.Int("steps", 8, "Number of training steps.")
		tasksPerStep = fs.Int("tasks-per-step", 0, "Tasks sampled per step; 0 means all selected instances.")
		groupSize    = fs.Int("group-size", 2, "Group size (rollouts) per sampled instance.")
		park         = fs.String("park", string(schema.ParkSuspendAsync), "Park policy: suspend-async, suspend-sync, suspend, fork, delete, or none.")
		seed         = fs.Uint64("seed", 42, "Deterministic RNG seed for step sampling.")
		runID        = fs.String("run-id", "swe-sched", "Job / schedule ID.")
		out          = fs.String("out", "schedule.json", "Output path for the resolved schedule JSON.")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dataset == "" {
		return fmt.Errorf("-dataset is required")
	}
	all, err := swebench.LoadDataset(*dataset)
	if err != nil {
		return err
	}
	var ids []string
	if *instances != "" {
		ids = strings.Split(*instances, ",")
	}
	var exclude []string
	if *excludeFile != "" {
		if exclude, err = swebench.ReadIDList(*excludeFile); err != nil {
			return err
		}
	}
	selected, err := swebench.Select(all, ids, exclude, *images)
	if err != nil {
		return err
	}
	sched, err := swebench.ResolveSingleJobSchedule(*runID, *seed, schema.ParkPolicy(*park), *steps, *tasksPerStep, *groupSize, selected)
	if err != nil {
		return err
	}
	return swebench.WriteSchedule(*out, sched)
}

func runCmd(args []string) (retErr error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var (
		backendFlag      = fs.String("backend", "substrate", "Backend driver: substrate | agent-sandbox.")
		accessFlag       = fs.String("access", "ate-env-api", "Access path: ate-env-api | direct | sdk.")
		endpoint         = fs.String("endpoint", "localhost:7777", "ate-env API or Agent Sandbox gateway address (host:port or URL).")
		controlAddr      = fs.String("control-addr", "", "Direct Substrate control plane address (defaults to -endpoint when -access=direct).")
		routerAddr       = fs.String("router-addr", "", "Direct Substrate atenet router address (when -access=direct).")
		atenetClientCert = fs.String("atenet-client-cert", "", "Path to client TLS cert+key bundle for atenet-router mTLS (when -access=direct).")
		atenetCAFile     = fs.String("atenet-ca-file", "", "Path to CA trust bundle for atenet-router TLS (when -access=direct).")
		atenetServerName = fs.String("atenet-server-name", "atenet-router.ate-system.svc", "TLS server name for atenet-router verification (when -access=direct).")
		ateapiTokenFile  = fs.String("ateapi-token-file", "", "Path to bearer token file for direct Substrate ateapi RPCs (when -access=direct).")
		ateapiCAFile     = fs.String("ateapi-ca-file", "", "Path to CA trust bundle for direct Substrate ateapi TLS verification (when -access=direct).")
		ateapiSkipVerify = fs.Bool("ateapi-skip-verify", true, "Skip TLS certificate verification on direct Substrate ateapi RPCs when -ateapi-ca-file is unset.")
		warmPool         = fs.String("warm-pool", "", "SandboxWarmPool name (when -backend=agent-sandbox).")
		connections      = fs.Int("connections", 4, "gRPC connections to the API, handed out round-robin per environment; a Service balances per connection, so use at least the number of API replicas.")
		atespace         = fs.String("atespace", "ate-env", "Atespace the environments are created in.")
		baseTemplate     = fs.String("base-template", "default-template", "Base ActorTemplate the per-image templates derive from (ate-env manifest template).")
		dataset          = fs.String("dataset", "", "JSONL manifest of SWE-bench instances; a path or an https URL. Required.")
		images           = fs.Int("images", 0, "Use the first N instances of the dataset; 0 means all.")
		instances        = fs.String("instances", "", "Comma-separated instance ids to use instead of the first N.")
		excludeFile      = fs.String("exclude-file", "", "File of instance ids to leave out, one per line; text after the id is a comment.")
		trajectory       = fs.String("trajectory", swebench.DefaultTrajectory, fmt.Sprintf("Trajectory file or built-in name (%s); a comma-separated list mixes them, one per environment, drawn with -seed.", strings.Join(swebench.Names(), ", ")))
		seed             = fs.Int64("seed", 1, "Seed for the trajectory mix and schedule/open-loop RNG, so reps assign the same trajectory to the same environment.")
		mode             = fs.String("mode", string(swebench.ModeEval), "eval, steps, or open-loop (Poisson arrivals at -arrival-rate).")
		strategy         = fs.String("strategy", "", "Warm-pool orchestration strategy: none, naive, sliding, or pipelined (matches Python SandboxFleet).")
		windowSize       = fs.Int("window-size", 0, "sliding/pipelined: distinct images per warm window (0 chooses min(8, ceil(images/2))).")
		width            = fs.Int("width", 50, "Most tasks in flight at once.")
		arrivalRate      = fs.Float64("arrival-rate", 0, "open-loop: Poisson task arrival rate in tasks/sec.")
		tasksPerImage    = fs.Int("tasks-per-image", 1, "eval: replays per image.")
		rollouts         = fs.Int("rollouts", 2, "steps: environments per image.")
		steps            = fs.Int("steps", 8, "steps: training steps.")
		park             = fs.String("park", string(swebench.ParkSuspend), "steps: what happens between steps. suspend and suspend-sync are durable: the call returns once the snapshot is in object storage. suspend-async is local: the call returns once the snapshot is on the node and the upload goes on in the background. fork replaces each environment per step (through ate-env-api a new environment created from the image; with -access direct a fork of its base) and parks it locally. delete deletes, and the next step creates again. none keeps the fleet running. Through ate-env-api a local suspend needs a server that serves suspend_mode, which the run checks before it starts.")
		imagesPerStep    = fs.Int("images-per-step", 0, "steps: each step takes the next N images (wrapping around the dataset) with fresh environments, retired after the step: the create-and-retire-pools-per-step shape; needs -park delete. 0 keeps one fleet across the steps.")
		parkEach         = fs.Bool("park-each", false, "steps: park each environment as soon as its step ends instead of after the whole fleet's step, so at most -width are awake at once and the fleet can be larger than the running slots; the barrier then includes the parking.")
		schedulePath     = fs.String("schedule", "", "steps: path to a pre-resolved schedule.json.")
		resolveOut       = fs.String("resolve-out", "", "steps: write the resolved schedule.json to this path before running.")
		tasksPerStep     = fs.Int("tasks-per-step", 0, "steps: tasks sampled per step when resolving a schedule (0 means all selected instances).")
		groupSize        = fs.Int("group-size", 0, "steps: group size per sampled task when resolving a schedule (0 uses -rollouts).")
		stepPause        = fs.Duration("step-pause", 0, "steps: simulated trainer pause between steps.")
		prefetch         = fs.Bool("prefetch", false, "steps: asynchronously prewarm next step's prefetch_images off the critical path.")
		placementPath    = fs.String("placement", "", "image-placement.json from plan-placement: each create carries the worker selector of one of its image's domains, round-robin, so environments land where the pre-flight put the image. ate-env API backend only.")
		createTimeout    = fs.Duration("create-timeout", 5*time.Minute, "Deadline for one create call.")
		stepTimeout      = fs.Duration("step-timeout", 10*time.Minute, "Default deadline for a run step without its own timeout.")
		wakeBudget       = fs.Duration("wake-budget", 3*time.Minute, "How long a task's first command is retried while the environment is still waking or its image is still being baked.")
		wakeBackoff      = fs.Duration("wake-backoff", swebench.DefaultWakeBackoff, "Initial pause between wake retries; doubles up to five times this.")
		deleteRate       = fs.Float64("delete-rate", swebench.DefaultDeleteRate, "Most environment deletes per second per loader process, through ate-env-api; the bucket rate-limits snapshot deletes, and a delete it refuses is retried with backoff for up to two minutes. 0 removes the cap. Recorded as the delete_rate label.")
		runID            = fs.String("run-id", "", "Run id, prefix of every environment name: lowercase letters, digits and '-', at most 20 characters; default swe-<unix time>.")
		out              = fs.String("out", "", "JSONL events file; default <run-id>.jsonl in the working directory.")
		eventsDir        = fs.String("events-dir", "", "Optional base directory for per-worker event shards (events/<worker>.jsonl).")
		worker           = fs.String("worker", "", "Optional worker shard name stamped on emitted events.")
		jobName          = fs.String("job", "", "Optional job name stamped on emitted events.")
		runWindow        = fs.String("run-window", "", "Optional path to write run_window.json.")
		statsOut         = fs.String("stats-out", "", "Optional path to write flat stats.jsonl rows after the run.")
		testName         = fs.String("test-name", "", "Optional test_name for run_window.json and stats.jsonl.")
		tag              = fs.String("tag", "", "Optional tag for run_window.json and stats.jsonl.")
		validate         = fs.Bool("validate", false, "Evaluate Validity Rules 1-7 after the run and fail if invalid.")
		convertTrace     = fs.String("convert-trace", "", "Convert a trace.jsonl file into schedule.json and trajectory.yaml before running (or exit if -dataset is omitted).")
		traceOut         = fs.String("trace-out", "", "Output directory for -convert-trace artifacts.")
		keep             = fs.Bool("keep", false, "Leave the environments in place when the run ends.")
		dryRun           = fs.Bool("dry-run", false, "Load and render everything, print the plan and exit without touching the API.")
		authTokenFile    = fs.String("auth-token-file", "", "File that holds the bearer token for ate-env-api; surrounding whitespace is ignored. Without it, SUBSTRATE_ENV_AUTH_TOKEN is used when set.")
		labels           = labelFlags{}
		vars             = labelFlags{}
		enableTracing    = fs.Bool("enable-tracing", false, "Also export every event as an OpenTelemetry span over OTLP/gRPC, configured by OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_INSECURE, all under one swebench-loader.run span per run. The JSONL is the same either way.")
	)
	fs.Var(vars, "set", "KEY=VALUE for the trajectories' var function, repeatable, e.g. READ_FILE=/testbed/setup.py or COMPILE_DIR=/testbed; a workload's defaults are in its file. A key no trajectory reads is an error; each value is recorded as a set.KEY label.")
	fs.Var(labels, "label", "KEY=VALUE recorded on the run event, repeatable: the Substrate commit, the cluster, the arm. The loader records its own flags itself (mode, width, images, trajectory, dataset, the shape flags, connections, base-template, exclude-file, instances and the timeouts); a -label with one of those keys is refused, so report -arm can trust them.")
	if err := fs.Parse(args); err != nil {
		return err
	}
	visited := map[string]bool{}
	fs.Visit(func(fl *flag.Flag) {
		visited[fl.Name] = true
	})
	if err := swebench.ValidateStrategy(swebench.Strategy(*strategy)); err != nil {
		return err
	}
	if *windowSize < 0 {
		return fmt.Errorf("-window-size must be >= 0, got %d", *windowSize)
	}
	if *windowSize > 0 && swebench.Strategy(*strategy) != swebench.StrategySliding && swebench.Strategy(*strategy) != swebench.StrategyPipelined {
		return fmt.Errorf("-window-size applies to -strategy sliding or pipelined")
	}
	if *strategy != "" {
		if swebench.Mode(*mode) == swebench.ModeOpenLoop {
			return fmt.Errorf("-strategy applies to -mode eval or steps, not open-loop")
		}
		if visited["rollouts"] && !visited["tasks-per-image"] && swebench.Mode(*mode) == swebench.ModeEval {
			*tasksPerImage = *rollouts
		}
		// A strategy owns the lifecycle: it parks by suspend (delete for
		// none) and runs one pass. The step loop's flags would be recorded
		// as labels of a run that never acted on them, so they are refused.
		for _, name := range []string{"park", "step-pause", "park-each", "images-per-step", "prefetch", "schedule"} {
			if visited[name] {
				return fmt.Errorf("-%s belongs to the step loop; -strategy %s parks by %s and runs one pass (omit -%s)",
					name, *strategy, strategyPark(swebench.Strategy(*strategy)), name)
			}
		}
		if swebench.Mode(*mode) == swebench.ModeSteps {
			if !visited["steps"] {
				*steps = 1
			} else if *steps > 1 {
				return fmt.Errorf("-strategy is single-pass (requires -steps 1 in steps mode; omit -strategy for multi-step reset loops)")
			}
			*park = string(strategyPark(swebench.Strategy(*strategy)))
		}
	}
	if *convertTrace != "" {
		targetDir := *traceOut
		if targetDir == "" {
			return fmt.Errorf("-trace-out is required with -convert-trace")
		}
		if err := convertTraceCmd([]string{"-in", *convertTrace, "-out", targetDir, "-job", *jobName}); err != nil {
			return err
		}
		if *dataset == "" {
			return nil
		}
	}
	if *dataset == "" {
		return fmt.Errorf("-dataset is required")
	}
	if err := swebench.ValidateBackendDriverFlags(*backendFlag, *accessFlag); err != nil {
		return err
	}
	authToken, err := resolveAuthToken(*authTokenFile, os.Getenv)
	if err != nil {
		return err
	}
	if *runID == "" {
		*runID = fmt.Sprintf("swe-%d", time.Now().Unix())
	}
	if *eventsDir != "" {
		wName := *worker
		if wName == "" {
			wName = "worker-0"
		}
		*out = swebench.ShardEventPath(*eventsDir, wName)
	} else if *out == "" {
		*out = *runID + ".jsonl"
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	all, err := swebench.LoadDataset(*dataset)
	if err != nil {
		return err
	}
	var ids []string
	if *instances != "" {
		ids = strings.Split(*instances, ",")
	}
	var exclude []string
	if *excludeFile != "" {
		if exclude, err = swebench.ReadIDList(*excludeFile); err != nil {
			return err
		}
	}
	selected, err := swebench.Select(all, ids, exclude, *images)
	if err != nil {
		return err
	}
	if *images > 0 && len(selected) < *images && len(ids) == 0 {
		fmt.Fprintf(os.Stderr, "note: %d instances requested, %d available after the exclude list\n", *images, len(selected))
	}
	trajSpec := *trajectory
	if trajSpec == swebench.DefaultTrajectory && len(selected) > 0 && selected[0].Kind == swebench.CommandKind {
		trajSpec = swebench.CommandTrajectoryName
	}
	var trajs []*swebench.Trajectory
	for _, source := range strings.Split(trajSpec, ",") {
		if source = strings.TrimSpace(source); source == "" {
			continue
		}
		t, err := swebench.LoadTrajectory(source)
		if err != nil {
			return err
		}
		t.Vars = vars
		if err := swebench.RequireGrading(selected, t); err != nil {
			return err
		}
		for _, in := range selected {
			if _, err := t.Render(in); err != nil {
				return err
			}
		}
		trajs = append(trajs, t)
	}
	if len(trajs) == 0 {
		return fmt.Errorf("-trajectory names no trajectory")
	}
	// The value each var resolved to, defaults included, goes on the run
	// event as a set.KEY label, so a run on the defaults and a run that
	// spelled the same values out pool together. A -set key no trajectory
	// reads is a typo, or a knob of another workload; with a default in
	// place it would pass unnoticed.
	effective := map[string]string{}
	if len(selected) > 0 {
		for _, t := range trajs {
			u, err := t.VarsUsed(selected[0])
			if err != nil {
				return err
			}
			maps.Copy(effective, u)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(vars)) {
		if _, ok := effective[k]; !ok {
			return fmt.Errorf("-set %s: no trajectory reads it (they read: %s)", k, strings.Join(slices.Sorted(maps.Keys(effective)), ", "))
		}
	}
	traj := trajs[0]

	var sched *schema.Schedule
	if *schedulePath != "" {
		sched, err = swebench.ReadSchedule(*schedulePath)
		if err != nil {
			return err
		}
	} else if *resolveOut != "" {
		gs := *groupSize
		if gs <= 0 {
			gs = *rollouts
		}
		sched, err = swebench.ResolveSingleJobSchedule(*runID, uint64(*seed), schema.ParkPolicy(*park), *steps, *tasksPerStep, gs, selected)
		if err != nil {
			return err
		}
	}
	if *resolveOut != "" && sched != nil {
		if err := swebench.WriteSchedule(*resolveOut, sched); err != nil {
			return err
		}
	}

	fanout := *tasksPerImage
	if swebench.Mode(*mode) == swebench.ModeSteps {
		fanout = *rollouts
	}
	environments := len(selected) * fanout
	if *imagesPerStep > 0 {
		environments = *imagesPerStep * fanout // per step
	}
	log.Info("plan", slog.String("run", *runID), slog.String("mode", *mode),
		slog.String("strategy", *strategy), slog.Int("window_size", *windowSize),
		slog.Int("images", len(selected)),
		slog.Int("images_per_step", *imagesPerStep),
		slog.Int("environments", environments), slog.Int("width", *width), slog.Int("steps", *steps),
		slog.String("park", *park), slog.String("trajectory", *trajectory), slog.String("out", *out))
	cfg := swebench.Config{
		RunID:         *runID,
		Dataset:       *dataset,
		Instances:     selected,
		Trajectory:    traj,
		Trajectories:  trajs,
		Seed:          *seed,
		Logger:        log,
		Mode:          swebench.Mode(*mode),
		Strategy:      swebench.Strategy(*strategy),
		WindowSize:    *windowSize,
		Width:         *width,
		ArrivalRate:   *arrivalRate,
		TasksPerImage: *tasksPerImage,
		Rollouts:      *rollouts,
		Steps:         *steps,
		Park:          swebench.Park(*park),
		ImagesPerStep: *imagesPerStep,
		ParkEach:      *parkEach,
		Schedule:      sched,
		TasksPerStep:  *tasksPerStep,
		GroupSize:     *groupSize,
		StepPause:     *stepPause,
		Prefetch:      *prefetch,
		CreateTimeout: *createTimeout,
		StepTimeout:   *stepTimeout,
		WakeBudget:    *wakeBudget,
		WakeBackoff:   *wakeBackoff,
		Keep:          *keep,
		Labels:        labels,
		Invocation:    ownLabels(*dataset, *connections, *deleteRate, *baseTemplate, *excludeFile, *placementPath, exclude, ids, effective),
	}
	// The checks Run makes before it touches the API, here so -dry-run makes
	// them too: the run id, the longest actor name against Substrate's 63
	// characters, the -label keys, and the mode, strategy and park flags.
	if err := swebench.ValidateConfig(cfg); err != nil {
		return err
	}
	// The placement plan is read and checked before -dry-run returns and
	// whatever backend runs, so a dry run and the CLI tests see its errors.
	var placement *schema.ImagePlacement
	if *placementPath != "" {
		if err := swebench.ValidatePlacementDriver(*backendFlag, *accessFlag, true); err != nil {
			return err
		}
		if placement, err = swebench.ReadImagePlacement(*placementPath); err != nil {
			return err
		}
		if missing := placementMissing(placement, selected); len(missing) > 0 {
			return fmt.Errorf("-placement %s neither places nor lists as unplaced %d of the %d selected images (first: %s): plan them, or run without -placement", *placementPath, len(missing), len(selected), missing[0])
		}
		log.Info("placement", slog.String("plan", *placementPath), slog.Int("images", len(placement.Images)), slog.Int("domains", len(placement.Domains)), slog.Int("unplaced", len(placement.Unplaced)))
	}
	if *dryRun {
		return nil
	}

	var backend swebench.Backend
	if testBackendOverride != nil {
		backend = testBackendOverride
	} else {
		var skipVerifyPtr *bool
		if visited["ateapi-skip-verify"] {
			skipVerifyPtr = ateapiSkipVerify
		}
		drv, closer, err := swebench.NewBackendDriver(swebench.BackendDriverOptions{
			Backend:          *backendFlag,
			Access:           *accessFlag,
			Endpoint:         *endpoint,
			ControlAddr:      *controlAddr,
			RouterAddr:       *routerAddr,
			AtenetClientCert: *atenetClientCert,
			AtenetCAFile:     *atenetCAFile,
			AtenetServerName: *atenetServerName,
			AteapiTokenFile:  *ateapiTokenFile,
			AteapiCAFile:     *ateapiCAFile,
			AteapiSkipVerify: skipVerifyPtr,
			Atespace:         *atespace,
			BaseTemplate:     *baseTemplate,
			WarmPool:         *warmPool,
			Connections:      *connections,
			AuthToken:        authToken,
			DeleteRate:       *deleteRate,
			Placement:        placement,
		})
		if err != nil {
			return err
		}
		defer closer.Close()
		backend = drv
	}

	var rec *swebench.Recorder
	var f *os.File
	if *eventsDir != "" {
		rec, err = swebench.NewShardRecorder(*eventsDir, *runID, *worker)
		if err != nil {
			return err
		}
	} else {
		f, err = os.Create(*out)
		if err != nil {
			return err
		}
		rec = swebench.NewRecorder(f)
		if *worker != "" {
			rec.SetWorker(*worker)
		}
	}
	if *jobName != "" {
		rec.SetJob(*jobName)
	}
	if *enableTracing {
		shutdown, err := telemetry.SetupOTel(context.Background(), tracingServiceName)
		if err != nil {
			return fmt.Errorf("-enable-tracing: %w", err)
		}
		// Deferred before the recorder close below, so it runs after it: the
		// run span ends, then the exporter flushes.
		defer shutdown()
		// SetupOTel installed the global provider; the scope is the service
		// name with '-' replaced by '_', as for ate-env-api.
		tracer := otel.Tracer(strings.ReplaceAll(tracingServiceName, "-", "_"))
		runCtx, runSpan := tracer.Start(context.Background(), tracingServiceName+".run",
			oteltrace.WithAttributes(attribute.String("run", *runID), attribute.String("mode", *mode)))
		defer runSpan.End()
		rec.SetTracer(tracer, runCtx)
		log.Info("tracing enabled", slog.String("otlp_endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")))
	}
	// Events lost to a write error, or a file that does not flush, must fail
	// the run rather than produce a report with holes.
	defer func() {
		if rerr := rec.Close(); rerr != nil && retErr == nil {
			retErr = fmt.Errorf("writing events to %s: %w", *out, rerr)
		}
		if f != nil {
			if cerr := f.Close(); cerr != nil && retErr == nil {
				retErr = fmt.Errorf("closing %s: %w", *out, cerr)
			}
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startTS := time.Now().UTC()
	cfg.Backend = backend
	cfg.Recorder = rec
	sum, err := swebench.Run(ctx, cfg)
	endTS := time.Now().UTC()
	_ = rec.Flush()

	if *runWindow != "" {
		tName := *testName
		if tName == "" {
			tName = "swebench-" + *mode
		}
		rw := schema.RunWindow{
			SchemaVersion: schema.SchemaVersion,
			RunID:         *runID,
			TestName:      tName,
			Tag:           *tag,
			StartTS:       startTS.Format(time.RFC3339Nano),
			EndTS:         endTS.Format(time.RFC3339Nano),
			SUTEndpoint:   *endpoint,
			// The Go loader captures no SDK trace; say so explicitly.
			Metadata: map[string]string{swebench.TraceCapturedMetadataKey: "false"},
		}
		if wErr := swebench.WriteRunWindow(*runWindow, rw); wErr != nil {
			return wErr
		}
	}

	if *statsOut != "" || *validate {
		rep, sErr := swebench.SummarizeFile(*out)
		if sErr != nil {
			return sErr
		}
		if *statsOut != "" {
			if wErr := swebench.WriteStatsJSONL(*statsOut, rep, *runID, *testName, *tag); wErr != nil {
				return wErr
			}
		}
		if *validate {
			v := swebench.Validate(rep, swebench.ValidateOptions{})
			if !v.Valid {
				return fmt.Errorf("report validation failed: %s", strings.Join(v.Reasons, "; "))
			}
		}
	}

	if sum != nil {
		log.Info("run finished", slog.Int("tasks", sum.Tasks), slog.Int("failed", sum.Failed),
			slog.Int("rewarded", sum.Rewarded), slog.Duration("duration", sum.Duration))
	}
	if err != nil {
		return err
	}
	if sum.Failed > 0 {
		return fmt.Errorf("%d of %d tasks failed", sum.Failed, sum.Tasks)
	}
	return nil
}

// strategyPark is the park policy a strategy run reports: none deletes each
// environment after its replay, the others suspend into the warm pool.
func strategyPark(s swebench.Strategy) swebench.Park {
	if s == swebench.StrategyNone {
		return swebench.ParkDelete
	}
	return swebench.ParkSuspend
}

func reportCmd(args []string, ws ...io.Writer) error {
	var stdout io.Writer = os.Stdout
	if len(ws) > 0 && ws[0] != nil {
		stdout = ws[0]
	}
	return reportCmdWithIO(args, stdout, os.Stderr)
}

func reportCmdWithIO(args []string, stdout, stderr io.Writer) error {
	files, rest := leadingFiles(args)
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var arms armFlags
	fs.Var(&arms, "arm", "NAME=FILE[,FILE...], repeatable: one column per arm, in flag order, its percentiles computed over the merged raw events of its files, the reps of one cluster or build, which must be complete runs of one workload (same loader labels, distinct run ids, a run in one arm only). A path may not contain a comma. No positional files with -arm.")
	var (
		top            = fs.Int("top", 0, "List the N slowest successful first commands and wakes by environment. One file only.")
		in             = fs.String("in", "", "Input events.jsonl file (alternative to positional FILE).")
		eventsGlob     = fs.String("events-glob", "", "Glob pattern for multi-worker event shards (e.g. 'events/*.jsonl').")
		jsonOut        = fs.String("json", "", "Also write the summary as JSON to this path (an array when several files are given).")
		out            = fs.String("out", "", "Alias for -json: write the summary as JSON to this path.")
		mdOut          = fs.String("md", "", "Optional path to write a Markdown report: one run's report (including ## Validity); for several runs the arm table over the text comparison, or with -thresholds the regression verdict.")
		unresolved     = fs.String("unresolved", "", "Write the unresolved instances to this path in -exclude-file format; one file only.")
		unresNote      = fs.String("unresolved-note", "Instances graded unresolved", "First comment line of the -unresolved file.")
		cacheBefore    = fs.String("cache-before", "", "Node cache sample taken before the run (benchmarking/swebench/sample-node-cache.sh); with -cache-after, the report adds the run's cache misses. One file only.")
		cacheAfter     = fs.String("cache-after", "", "Node cache sample taken after the run.")
		restoreLog     = fs.String("restore-log", "", "atelet restore records for the run (benchmarking/swebench/harvest-restore-timings.sh); the report adds the restore phases per environment. One file only.")
		restoreTimings = fs.String("restore-timings", "", "Alias for -restore-log: restore records for the run.")
		runWindow      = fs.String("run-window", "", "Optional path to run_window.json to validate and attach to the report.")
		statsOut       = fs.String("stats-out", "", "Optional path to write flat stats.jsonl rows.")
		testName       = fs.String("test-name", "", "Optional test_name for stats.jsonl (default: swebench-<mode>).")
		tag            = fs.String("tag", "", "Optional tag for stats.jsonl.")
		validate       = fs.Bool("validate", false, "Evaluate Validity Rules 1-7 and exit non-zero on failure.")
		footing        = fs.String("footing", "", "Expected footing for Validity Rule 1 (warm | cold).")
		preflightCov   = fs.String("preflight-coverage", "", "preflight-coverage.json of the pre-flight that ran before the run; Rule 1 takes a complete pre-flight as prewarm evidence on warm footing and fails on an incomplete one.")
		minReward      = fs.Float64("min-reward-rate", 0, "Minimum reward rate for Validity Rule 2.")
		minTasks       = fs.Int("min-tasks", 0, "Minimum task count for Validity Rule 3.")
		maxFailCr      = fs.Float64("max-failed-create-ratio", -1, "Maximum failed create ratio for Validity Rule 4.")
		schedPath      = fs.String("schedule", "", "Optional schedule.json path for Validity Rule 5.")
		traceMode      = fs.Bool("trace-mode", false, "Record that an SDK trace was captured (Validity Rule 6).")
		orphanCount    = fs.Int("orphan-count", 0, "Remaining orphan count after reap (Validity Rule 7).")
		compareFlag    = fs.String("compare", "", "Comma-separated events.jsonl or report.json files to compare against.")
		thresholds     = fs.String("thresholds", "", "Optional thresholds.yaml path for regression gating.")
	)
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		// flag stops at the first non-flag, so a file after a flag would be
		// dropped without a word.
		return fmt.Errorf("unexpected arguments after the flags: %s; put every events file before the flags", strings.Join(fs.Args(), " "))
	}
	if *top < 0 {
		return fmt.Errorf("-top wants 0 or more, got %d", *top)
	}
	if *out != "" && *jsonOut == "" {
		*jsonOut = *out
	}
	if *restoreTimings != "" && *restoreLog == "" {
		*restoreLog = *restoreTimings
	}
	if len(arms) > 0 {
		if len(files) > 0 || *in != "" {
			return fmt.Errorf("with -arm, give every events file inside an -arm value")
		}
		if *unresolved != "" || *cacheBefore != "" || *cacheAfter != "" || *restoreLog != "" || *preflightCov != "" || *top > 0 {
			return fmt.Errorf("-unresolved, -cache-before, -cache-after, -restore-log, -preflight-coverage and -top take one events file, not -arm")
		}
		return reportArms(arms, *jsonOut, stdout)
	}
	if *in != "" {
		files = append(files, *in)
	}

	var compareFiles []string
	if *compareFlag != "" {
		for _, p := range strings.Split(*compareFlag, ",") {
			if p = strings.TrimSpace(p); p != "" {
				compareFiles = append(compareFiles, p)
			}
		}
	}

	if len(files) == 0 && *eventsGlob == "" && len(compareFiles) == 0 {
		return fmt.Errorf("report needs at least one JSONL file, or -arm")
	}

	var rw *schema.RunWindow
	if *runWindow != "" {
		loadedRW, err := swebench.ReadRunWindow(*runWindow)
		if err != nil {
			return err
		}
		rw = loadedRW
	}

	var reps []*swebench.Report
	var labels []string
	var events []swebench.Event // of the single file, for the restore join

	if *eventsGlob != "" {
		rep, err := swebench.SummarizeGlob(*eventsGlob)
		if err != nil {
			return err
		}
		reps = append(reps, rep)
		labels = append(labels, *eventsGlob)
	}

	for _, path := range files {
		if (*restoreLog != "" || *top > 0) && len(files) == 1 && *eventsGlob == "" && len(compareFiles) == 0 {
			evs, err := readEventsFile(path)
			if err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			events = evs
			reps = append(reps, swebench.Build(evs))
			labels = append(labels, path)
			continue
		}
		rep, err := loadOrSummarizeFile(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		reps = append(reps, rep)
		labels = append(labels, path)
	}

	// If -compare was provided along with a single primary report (-in / -events-glob / single file)
	// and len(compareFiles) == 1, treat compareFiles[0] as baseline and the primary report as candidate.
	if len(compareFiles) > 0 {
		var compReps []*swebench.Report
		for _, p := range compareFiles {
			r, err := loadOrSummarizeFile(p)
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			compReps = append(compReps, r)
		}
		if len(reps) == 1 && len(compReps) == 1 {
			reps = []*swebench.Report{compReps[0], reps[0]}
			labels = []string{compareFiles[0], labels[0]}
		} else {
			reps = append(reps, compReps...)
			labels = append(labels, compareFiles...)
		}
	}

	if len(reps) > 1 {
		// Several runs: one table, a column per run, for the same workload on
		// different clusters or builds.
		if *unresolved != "" || (*cacheBefore != "" && !*validate) || (*cacheAfter != "" && !*validate) || *restoreLog != "" || *preflightCov != "" || *top > 0 {
			return fmt.Errorf("-unresolved, -cache-before, -cache-after, -restore-log, -preflight-coverage and -top take one events file")
		}
		swebench.WriteComparison(stdout, reps, labels)
		if *mdOut != "" && *thresholds == "" {
			var md bytes.Buffer
			swebench.WriteComparisonMarkdown(&md, reps, labels)
			if err := os.WriteFile(*mdOut, md.Bytes(), 0o644); err != nil {
				return err
			}
		}
		if *thresholds != "" {
			th, err := swebench.LoadThresholds(*thresholds)
			if err != nil {
				return err
			}
			res := swebench.Compare(reps[0], reps[len(reps)-1], th)
			fmt.Fprintln(stdout)
			fmt.Fprint(stdout, swebench.FormatComparisonMarkdown(res))
			if *mdOut != "" {
				if err := os.WriteFile(*mdOut, []byte(swebench.FormatComparisonMarkdown(res)), 0o644); err != nil {
					return err
				}
			}
			if !res.Passed {
				return fmt.Errorf("threshold regression detected: %s", strings.Join(res.Regressions, "; "))
			}
		}
		if *jsonOut != "" {
			b, err := json.MarshalIndent(reps, "", "  ")
			if err != nil {
				return err
			}
			return os.WriteFile(*jsonOut, b, 0o644)
		}
		return nil
	}

	rep := reps[0]
	if rw != nil {
		if *traceMode {
			if rw.Metadata == nil {
				rw.Metadata = map[string]string{}
			}
			rw.Metadata[swebench.TraceCapturedMetadataKey] = "true"
		}
		rep.RunWindow = rw
		if rep.RunID == "" {
			rep.RunID = rw.RunID
		}
	}
	if (*cacheBefore == "") != (*cacheAfter == "") {
		return fmt.Errorf("-cache-before and -cache-after go together")
	}
	if *cacheBefore != "" {
		before, errB := loadCacheSample(*cacheBefore)
		after, errA := loadCacheSample(*cacheAfter)
		if errB == nil && errA == nil {
			d := before.Delta(after)
			rep.Cache = &d
			rep.PopulateCacheMissesPerFirstCmd()
		} else if !*validate {
			if errB != nil {
				return errB
			}
			return errA
		}
	}
	if *restoreLog != "" {
		f, err := os.Open(*restoreLog)
		if err != nil {
			return err
		}
		timings, err := swebench.ParseRestoreTimings(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", *restoreLog, err)
		}
		rep.Restores = swebench.JoinRestores(events, timings)
	}
	if *top > 0 {
		rep.Slowest = swebench.Slowest(events, *top)
	}

	var valErr error
	if *validate {
		valOpts := swebench.ValidateOptions{
			Footing:         *footing,
			MinRewardRate:   *minReward,
			CheckRewardRate: *minReward > 0,
			MinTasks:        *minTasks,
			TraceCaptured:   *traceMode,
			OrphanCount:     *orphanCount,
			CacheBeforePath: *cacheBefore,
			CacheAfterPath:  *cacheAfter,

			PreflightCoveragePath: *preflightCov,
		}
		if *maxFailCr >= 0 {
			valOpts.MaxFailedCreateRatio = *maxFailCr
			valOpts.CheckFailedCreates = true
		}
		if *schedPath != "" {
			sched, sErr := swebench.ReadSchedule(*schedPath)
			if sErr != nil {
				return sErr
			}
			valOpts.Schedule = sched
		}
		validity := swebench.Validate(rep, valOpts)
		if !validity.Valid {
			valErr = fmt.Errorf("report validation failed: %s", strings.Join(validity.Reasons, "; "))
		}
	}

	rep.WriteText(stdout)
	if err := swebench.WriteReportFiles(rep, *jsonOut, *mdOut, *unresolved, *unresNote, *statsOut, *testName, *tag); err != nil {
		return err
	}
	return valErr
}

// leadingFiles splits report's arguments into the events files, which come
// first, and the flags that follow them.
func leadingFiles(args []string) (files, rest []string) {
	for i, a := range args {
		if strings.HasPrefix(a, "-") {
			return args[:i], args[i:]
		}
	}
	return args, nil
}

func summarizeFile(path string) (*swebench.Report, error) {
	return swebench.SummarizeFile(path)
}

func loadOrSummarizeFile(path string) (*swebench.Report, error) {
	return swebench.LoadOrSummarizeReport(path)
}
func readEventsFile(path string) ([]swebench.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return swebench.ReadEvents(f)
}

func loadCacheSample(path string) (*swebench.NodeCacheSample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s, err := swebench.ParseNodeCacheSample(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// reportArms prints one column per arm, each pooled over its files, so three
// reps of one cluster become one set of exact percentiles rather than three
// columns to read by eye.
func reportArms(arms armFlags, jsonOut string, w io.Writer) error {
	names := make([]string, 0, len(arms))
	reps := make([]*swebench.Report, 0, len(arms))
	owner := map[string]string{} // run id -> the arm that has it
	for _, a := range arms {
		runs := make([][]swebench.Event, 0, len(a.files))
		for _, file := range a.files {
			evs, err := readEventsFile(file)
			if err != nil {
				return fmt.Errorf("arm %s: %s: %w", a.name, file, err)
			}
			runs = append(runs, evs)
		}
		rep, err := swebench.Pool(runs)
		if err != nil {
			return fmt.Errorf("arm %s: %w", a.name, err)
		}
		for _, id := range rep.Runs {
			if other, dup := owner[id]; dup {
				return fmt.Errorf("run %s is in arms %s and %s; a run belongs to one arm", id, other, a.name)
			}
			owner[id] = a.name
		}
		rep.RunID = a.name
		names = append(names, a.name)
		reps = append(reps, rep)
	}
	swebench.WriteComparison(w, reps, names)
	if jsonOut != "" {
		b, err := json.MarshalIndent(reps, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(jsonOut, b, 0o644)
	}
	return nil
}

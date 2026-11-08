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
	"errors"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	env "github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/internal/apiservice"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// newPlacementDomainsCommand lists the worker pools a per-environment worker
// selector can address.
func newPlacementDomainsCommand() *cobra.Command {
	var (
		endpoint         string
		atespace         string
		auth             authFlags
		template         string
		templateAtespace string
		asJSON           bool
	)
	cmd := &cobra.Command{
		Use:   "placement-domains",
		Short: "List the worker pools a worker selector can address: labels, nodes, slots per node",
		Long: "List the placement domains: each worker pool with the labels its workers carry (the selector a\n" +
			"create --worker-selector or a placement plan uses to keep environments in it), its nodes, its\n" +
			"workers and the environments one node runs at once. With --template, only the pools that\n" +
			"template's own worker selector matches: the ones a worker selector for it can address.\n" +
			"ate-env-api reads them from Substrate's workers, which takes the global viewer role\n" +
			"(ate-env manifest access-policy --global), with or without a template.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{Endpoint: endpoint, AuthToken: token})
			if err != nil {
				return err
			}
			defer client.Close()
			req := &ateenvv1alpha.ListPlacementDomainsRequest{Atespace: atespace}
			if template != "" || templateAtespace != "" {
				req.Template = &ateenvv1alpha.Template{Name: template, Atespace: templateAtespace}
			}
			domains, err := client.ListPlacementDomains(cmd.Context(), req)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if asJSON {
				b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(&ateenvv1alpha.ListPlacementDomainsResponse{Domains: domains})
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(out, "%s\n", b)
				return err
			}
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tNAMESPACE\tNODES\tWORKERS\tSLOTS\tSELECTOR")
			for _, d := range domains {
				fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\n", d.GetName(), d.GetWorkerNamespace(), d.GetNodes(), d.GetWorkers(),
					map[bool]string{true: "-", false: fmt.Sprint(d.GetNodeSlots())}[d.GetNodeSlots() == 0], formatSelector(d.GetSelector()))
			}
			return tw.Flush()
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	auth.register(cmd, false)
	cmd.Flags().StringVar(&template, "template", "", "base ActorTemplate name: list only the pools its worker selector matches (defaults to the server default when --template-atespace or --atespace is set)")
	cmd.Flags().StringVar(&templateAtespace, "template-atespace", "", "Substrate atespace of the base ActorTemplate (defaults to --atespace)")
	cmd.Flags().StringVar(&atespace, "atespace", "", "Substrate atespace of the template (defaults to --template-atespace, else "+apiservice.DefaultAtespace+")")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the domains as JSON")
	return cmd
}

// formatSelector renders labels as k=v pairs, sorted, comma separated.
func formatSelector(sel map[string]string) string {
	if len(sel) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(sel))
	for k, v := range sel {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// newWarmImageCommand warms one image on every node of one domain.
func newWarmImageCommand() *cobra.Command {
	var (
		endpoint         string
		atespace         string
		auth             authFlags
		template         string
		templateAtespace string
		selector         []string
		nodes            int
		probesPerNode    int
		maxRounds        int
		probeTimeout     time.Duration
		runID            string
		asJSON           bool
	)
	cmd := &cobra.Command{
		Use:   "warm-image <rootfs-image>",
		Short: "Put an image's layers and golden snapshot on every node of a worker pool before a run",
		Long: "Warm one digest-pinned rootfs image on every node of one placement domain (a worker pool, named by\n" +
			"--worker-selector; the template's own pools when none is given): ate-env-api creates probe\n" +
			"environments with the selector, resumes them, holds them until they run, reads the node each\n" +
			"landed on, deletes them, and repeats until every node has run the image or the rounds run out.\n" +
			"One probe line is printed as each settles, then the coverage (with --json, one JSON line per\n" +
			"event). The command exits non-zero when the coverage is short or probes were left behind\n" +
			"(listed for a sweep with ate-env delete). Wait for the golden first\n" +
			"(ate-env image-status) when the run relies on it. --nodes is required when ate-env-api may not\n" +
			"list workers (ate-env manifest access-policy --global).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			sel, err := parseWorkerSelector(selector)
			if err != nil {
				return err
			}
			// Zero fails like a negative value: unset, the flag leaves the
			// request's probe_timeout empty and the server's default applies.
			if cmd.Flags().Changed("probe-timeout") && probeTimeout <= 0 {
				return fmt.Errorf("--probe-timeout must be positive, got %s", probeTimeout)
			}
			token, err := auth.resolve(cmd)
			if err != nil {
				return err
			}
			client, err := env.NewClient(env.ClientOptions{Endpoint: endpoint, AuthToken: token})
			if err != nil {
				return err
			}
			defer client.Close()
			var tmpl *ateenvv1alpha.Template
			if template != "" || templateAtespace != "" {
				tmpl = &ateenvv1alpha.Template{Name: template, Atespace: templateAtespace}
			}
			req := &ateenvv1alpha.WarmImageRequest{
				RootfsImage:    args[0],
				Template:       tmpl,
				Atespace:       atespace,
				WorkerSelector: sel,
				Nodes:          int32(nodes),
				ProbesPerNode:  int32(probesPerNode),
				MaxRounds:      int32(maxRounds),
				RunId:          runID,
			}
			if probeTimeout > 0 {
				req.ProbeTimeout = durationpb.New(probeTimeout)
			}
			out := cmd.OutOrStdout()
			jsonLine := func(ev *ateenvv1alpha.WarmImageEvent) error {
				b, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(ev)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(out, "%s\n", b)
				return err
			}
			var printErr error
			onProbe := func(p *ateenvv1alpha.WarmProbe) {
				if asJSON {
					// One line per event, as the stream delivers them.
					if err := jsonLine(&ateenvv1alpha.WarmImageEvent{Event: &ateenvv1alpha.WarmImageEvent_Probe{Probe: p}}); err != nil && printErr == nil {
						printErr = err
					}
					return
				}
				if p.GetError() != "" {
					fmt.Fprintf(out, "probe %s round %d failed after %s: %s\n", p.GetId(), p.GetRound(), p.GetDuration().AsDuration().Round(time.Millisecond), p.GetError())
					return
				}
				fmt.Fprintf(out, "probe %s round %d on %s in %s\n", p.GetId(), p.GetRound(), p.GetNode(), p.GetDuration().AsDuration().Round(time.Millisecond))
			}
			cov, err := client.WarmImage(cmd.Context(), req, onProbe)
			if err != nil {
				return err
			}
			if printErr != nil {
				return printErr
			}
			if asJSON {
				if err := jsonLine(&ateenvv1alpha.WarmImageEvent{Event: &ateenvv1alpha.WarmImageEvent_Coverage{Coverage: cov}}); err != nil {
					return err
				}
			} else {
				fmt.Fprintf(out, "coverage: %d/%d nodes after %d rounds, %d probes, %d failed%s\n",
					len(cov.GetNodes()), cov.GetWanted(), cov.GetRounds(), cov.GetProbes(), cov.GetFailed(),
					map[bool]string{true: "", false: ": " + cov.GetError()}[cov.GetError() == ""])
				if len(cov.GetNodes()) > 0 {
					fmt.Fprintf(out, "nodes: %s\n", strings.Join(cov.GetNodes(), ", "))
				}
				if w := cov.GetWaited().AsDuration(); w > 0 {
					fmt.Fprintf(out, "waited %s for probe slots held by other warm calls\n", w.Round(time.Millisecond))
				}
				if len(cov.GetUndeletedProbes()) > 0 {
					fmt.Fprintf(out, "undeleted probes (sweep with ate-env delete): %s\n", strings.Join(cov.GetUndeletedProbes(), ", "))
				}
			}
			if !cov.GetComplete() {
				return errors.New("coverage short of the domain's nodes")
			}
			if len(cov.GetUndeletedProbes()) > 0 {
				return fmt.Errorf("%d probes left behind, sweep them: %s", len(cov.GetUndeletedProbes()), strings.Join(cov.GetUndeletedProbes(), ", "))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&endpoint, "api", envOr("SUBSTRATE_ENV_API", "127.0.0.1:7777"), "address of the ate-env-api service (e.g. localhost:7777)")
	cmd.Flags().StringVar(&atespace, "atespace", "", "Substrate atespace of the probes and templates (defaults to --template-atespace, else "+apiservice.DefaultAtespace+")")
	auth.register(cmd, false)
	cmd.Flags().StringVar(&template, "template", "", "base ActorTemplate name (defaults to server default)")
	cmd.Flags().StringVar(&templateAtespace, "template-atespace", "", "Substrate atespace of the base ActorTemplate (defaults to --atespace)")
	cmd.Flags().StringSliceVar(&selector, "worker-selector", nil, "label the worker pool must carry, key=value (repeatable); the template's own pools when unset")
	cmd.Flags().IntVar(&nodes, "nodes", 0, "nodes to cover; 0 counts the nodes of the workers the selector matches (needs the global viewer role)")
	cmd.Flags().IntVar(&probesPerNode, "probes-per-node", 0, fmt.Sprintf("probes a round starts per node (default %d, at most %d)", apiservice.DefaultWarmProbesPerNode, apiservice.MaxWarmProbesPerNode))
	cmd.Flags().IntVar(&maxRounds, "max-rounds", 0, fmt.Sprintf("rounds before giving up (default %d, at most %d)", apiservice.DefaultWarmRounds, apiservice.MaxWarmRounds))
	cmd.Flags().DurationVar(&probeTimeout, "probe-timeout", 0, fmt.Sprintf("bound on one probe's create and start, positive (default %s)", apiservice.DefaultWarmProbeTimeout))
	cmd.Flags().StringVar(&runID, "run-id", "", "prefix of the probe environment names, unique per call (default "+apiservice.DefaultWarmRunID+"-<random>)")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print each event as one JSON line, the probes then the coverage, instead of text")
	return cmd
}

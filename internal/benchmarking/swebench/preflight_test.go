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

package swebench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

const (
	pfImgA = "registry.example/swebench/a@sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pfImgB = "registry.example/swebench/b@sha256:" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	pfImgC = "registry.example/swebench/c@sha256:" + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

// twoDomainPlan places a in d0 and b in both domains, two nodes each.
func twoDomainPlan() *schema.ImagePlacement {
	return &schema.ImagePlacement{
		SchemaVersion: schema.SchemaVersion,
		UnpackFactor:  1,
		Replication:   1,
		Domains: []schema.PlacementDomain{
			{Name: "d0", Selector: map[string]string{"ate-env/domain": "d0"}, Nodes: 2, NodeCacheBytes: 100, AssignedBytes: 2, ImageCount: 2},
			{Name: "d1", Selector: map[string]string{"ate-env/domain": "d1"}, Nodes: 2, NodeCacheBytes: 100, AssignedBytes: 1, ImageCount: 1},
		},
		Images: []schema.ImagePlacementEntry{
			{Image: pfImgA, Domains: []string{"d0"}, BytesCompressed: 1, BytesUnpacked: 1},
			{Image: pfImgB, Domains: []string{"d0", "d1"}, BytesCompressed: 1, BytesUnpacked: 1},
		},
	}
}

func newPreflightFake() *fakeAPI {
	return &fakeAPI{
		domainNodes:      map[string][]string{"d0": {"node-d0-1", "node-d0-2"}, "d1": {"node-d1-1", "node-d1-2"}},
		goldenReadyAfter: 2,
	}
}

func runPreflight(t *testing.T, api *fakeAPI, mutate func(*PreflightOptions)) (*schema.PreflightCoverage, error, []Event) {
	t.Helper()
	b := newFakeBackend(t, api)
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	opts := PreflightOptions{
		Plan:          twoDomainPlan(),
		PlanPath:      "image-placement.json",
		Backend:       b,
		Recorder:      rec,
		RunID:         "pf-test",
		ProbesPerNode: 1,
		MaxRounds:     3,
		GoldenTimeout: time.Second,
		GoldenPoll:    time.Millisecond,
		ProbeTimeout:  5 * time.Second,
	}
	if mutate != nil {
		mutate(&opts)
	}
	w, err := NewPreflightWarmer(opts)
	if err != nil {
		t.Fatal(err)
	}
	report, runErr := w.Run(context.Background())
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	var events []Event
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		if err := schema.ValidateAgainstJSONSchema("event.schema.json", []byte(line)); err != nil {
			t.Fatalf("event schema %q: %v", line, err)
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("event %q: %v", line, err)
		}
		events = append(events, e)
	}
	if err := report.Validate(); err != nil {
		t.Fatalf("report does not validate: %v", err)
	}
	rawReport, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateAgainstJSONSchema("preflight_coverage.schema.json", rawReport); err != nil {
		t.Fatalf("report JSON schema: %v", err)
	}
	return report, runErr, events
}

func countEvents(events []Event, kind string) int {
	n := 0
	for _, e := range events {
		if e.Event == kind {
			n++
		}
	}
	return n
}

func TestPreflightCoversEveryNodeOfEveryDomain(t *testing.T) {
	api := newPreflightFake()
	// One image at a time per domain: the fake spreads a domain's creates
	// round-robin over its nodes, so a round of two probes covers both nodes
	// only when no other image's probes interleave.
	report, err, events := runPreflight(t, api, func(o *PreflightOptions) { o.ImageConcurrency = 1 })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !report.Complete {
		t.Errorf("report incomplete: %+v", report)
	}
	// Goldens: ensured once per image (in parallel, so in any order), polled
	// until ready, tag recorded, in plan order.
	ensured := append([]string(nil), api.ensured...)
	sort.Strings(ensured)
	if !reflect.DeepEqual(ensured, []string{pfImgA, pfImgB}) {
		t.Errorf("ensured = %v", api.ensured)
	}
	if len(report.Goldens) != 2 || report.Goldens[0].Image != pfImgA || report.Goldens[1].Image != pfImgB {
		t.Errorf("goldens = %+v, want a then b", report.Goldens)
	}
	for _, g := range report.Goldens {
		if g.State != "ready" || !strings.HasPrefix(g.Tag, "golden-") || g.Template == "" {
			t.Errorf("golden %+v, want ready with a tag and template", g)
		}
	}
	// Coverage: a on both nodes of d0; b on both nodes of d0 and d1.
	want := map[string]map[string][]string{
		"d0": {pfImgA: {"node-d0-1", "node-d0-2"}, pfImgB: {"node-d0-1", "node-d0-2"}},
		"d1": {pfImgB: {"node-d1-1", "node-d1-2"}},
	}
	for _, d := range report.Domains {
		if !d.Complete {
			t.Errorf("domain %s incomplete: %+v", d.Name, d)
		}
		if len(d.Images) != len(want[d.Name]) {
			t.Errorf("domain %s covers %d images, want %d", d.Name, len(d.Images), len(want[d.Name]))
		}
		for _, ic := range d.Images {
			if got := want[d.Name][ic.Image]; !reflect.DeepEqual(ic.Nodes, got) || !ic.Complete || ic.Rounds != 1 || ic.Probes != 2 || ic.Failed != 0 {
				t.Errorf("%s/%s = %+v, want nodes %v in one round of two probes", d.Name, ic.Image, ic, got)
			}
		}
	}
	// Every probe carried its domain's selector and was deleted.
	for id, sel := range api.selectors {
		if sel["ate-env/domain"] == "" {
			t.Errorf("probe %s has no domain selector: %v", id, sel)
		}
	}
	if len(api.deleted) != len(api.created) || len(api.suspended) != 0 {
		t.Errorf("created %d, deleted %d, suspended %d; want all deleted", len(api.created), len(api.deleted), len(api.suspended))
	}
	// Events: a template and a golden per image, a probe per environment
	// with its node; no run event (prewarm) the report would count.
	if n := countEvents(events, EventPreflightTemplate); n != 2 {
		t.Errorf("template events = %d, want 2", n)
	}
	if n := countEvents(events, EventPreflightGolden); n != 2 {
		t.Errorf("golden events = %d, want 2", n)
	}
	if n := countEvents(events, EventPrewarm); n != 0 {
		t.Errorf("prewarm events = %d, want none", n)
	}
	probes := 0
	for _, e := range events {
		if e.Event != EventPreflightProbe {
			continue
		}
		probes++
		if e.Node == "" || e.Labels["domain"] == "" || !e.OK || e.Env == "" || e.Image == "" {
			t.Errorf("probe event lacks node, domain, env or image: %+v", e)
		}
	}
	if probes != len(api.created) {
		t.Errorf("probe events = %d, created = %d", probes, len(api.created))
	}
}

// With a concurrency above one a domain warms several images at once, so
// more than nodes x ProbesPerNode probes are alive in it; with one, never.
func TestPreflightImageConcurrency(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	// Slow probes: each first command takes a few polls, so the two images
	// of d0 overlap when the warmer lets them.
	api.pollsUntilExit = 3
	if _, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.ImageConcurrency = 2 }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if api.maxInflight["d0"] <= 2 {
		t.Errorf("d0 max in flight = %d with concurrency 2, want above nodes x probes (2)", api.maxInflight["d0"])
	}

	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.pollsUntilExit = 3
	if _, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.ImageConcurrency = 1 }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if api.maxInflight["d0"] > 2 {
		t.Errorf("d0 max in flight = %d with concurrency 1, want at most 2", api.maxInflight["d0"])
	}

	// node_slots caps the concurrency and refuses too many probes per node.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.pollsUntilExit = 3
	if _, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.ImageConcurrency = 4
		o.Plan.Domains[0].NodeSlots = 1
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if api.maxInflight["d0"] > 2 {
		t.Errorf("d0 max in flight = %d with one slot per node, want at most 2", api.maxInflight["d0"])
	}
	plan := twoDomainPlan()
	plan.Domains[0].NodeSlots = 1
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: newFakeBackend(t, newPreflightFake()), ProbesPerNode: 2}); err == nil || !strings.Contains(err.Error(), "node slots") {
		t.Errorf("2 probes per node on 1 slot: err = %v, want a node slots error", err)
	}
}

// A domain does not wait for a slow golden before warming the images behind
// it: a, first in plan order, never gets its golden; b, behind it in d0, is
// covered while a's golden is still pending, with one image at a time.
func TestPreflightStreamsReadyGoldensIntoCoverage(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.goldenPendingForever = map[string]bool{pfImgA: true}
	report, err, events := runPreflight(t, api, func(o *PreflightOptions) {
		o.ImageConcurrency = 1
		o.GoldenTimeout = 200 * time.Millisecond
	})
	if err == nil || !strings.Contains(err.Error(), "1 goldens not ready") {
		t.Fatalf("Run error = %v, want a's golden not ready", err)
	}
	// The recorder keeps the order: every probe of b in d0 is recorded
	// before a's golden gives up, and a's probes after it.
	aGolden, bProbesBefore, aProbesAfter := -1, 0, 0
	for i, e := range events {
		switch {
		case e.Event == EventPreflightGolden && e.Image == pfImgA:
			aGolden = i
		case e.Event == EventPreflightProbe && e.Image == pfImgB && e.Labels["domain"] == "d0" && aGolden < 0:
			bProbesBefore++
		case e.Event == EventPreflightProbe && e.Image == pfImgA && aGolden >= 0:
			aProbesAfter++
		}
	}
	if aGolden < 0 || bProbesBefore != 2 || aProbesAfter != 2 {
		t.Errorf("b probes in d0 before a's golden settled = %d, a probes after = %d; want 2 and 2", bProbesBefore, aProbesAfter)
	}
	for _, d := range report.Domains {
		if !d.Complete {
			t.Errorf("domain %s incomplete: a pending golden still boots and fills the layers: %+v", d.Name, d)
		}
	}
}

func TestPreflightReportsShortCoverageAfterItsRounds(t *testing.T) {
	api := newPreflightFake()
	api.sameNode = true
	report, err, _ := runPreflight(t, api, nil)
	if err == nil || !strings.Contains(err.Error(), "short of their nodes") {
		t.Fatalf("Run error = %v, want a shortfall", err)
	}
	if report.Complete {
		t.Error("report complete with one node per domain")
	}
	for _, d := range report.Domains {
		if d.Complete || len(d.NodesSeen) != 1 {
			t.Errorf("domain %s: complete=%v nodes seen %v", d.Name, d.Complete, d.NodesSeen)
		}
		for _, ic := range d.Images {
			if ic.Complete || ic.Rounds != 3 || len(ic.Nodes) != 1 || ic.Probes != 6 {
				t.Errorf("%s/%s = %+v, want 3 rounds of 2 probes on one node", d.Name, ic.Image, ic)
			}
		}
	}
	if len(api.deleted) != len(api.created) {
		t.Errorf("created %d, deleted %d", len(api.created), len(api.deleted))
	}
}

func TestPreflightGoldenFailure(t *testing.T) {
	api := newPreflightFake()
	api.goldenFail = map[string]string{pfImgB: "GoldenActorCrashed: boom"}
	report, err, events := runPreflight(t, api, nil)
	if err == nil || !strings.Contains(err.Error(), "1 goldens not ready") {
		t.Fatalf("Run error = %v, want one golden not ready", err)
	}
	var b schema.GoldenRecord
	for _, g := range report.Goldens {
		if g.Image == pfImgB {
			b = g
		}
	}
	if b.State != "failed" || !strings.Contains(b.Error, "boom") {
		t.Errorf("b golden = %+v, want failed with the reason", b)
	}
	// The coverage pass still ran: a failed golden means boots, and boots
	// fill the layers too.
	for _, d := range report.Domains {
		if !d.Complete {
			t.Errorf("domain %s incomplete with a failed golden: %+v", d.Name, d)
		}
	}
	failedGoldenEvents := 0
	for _, e := range events {
		if e.Event == EventPreflightGolden && !e.OK {
			failedGoldenEvents++
		}
	}
	if failedGoldenEvents != 1 {
		t.Errorf("failed golden events = %d, want 1", failedGoldenEvents)
	}
}

// Goldens disabled on the base template: the templates exist with no
// golden, the pass does not wait, and completes on coverage alone.
func TestPreflightGoldensDisabled(t *testing.T) {
	api := newPreflightFake()
	api.goldenDisabled = true
	report, err, _ := runPreflight(t, api, nil)
	if err != nil || !report.Complete {
		t.Fatalf("goldens disabled: err=%v complete=%v", err, report.Complete)
	}
	for _, g := range report.Goldens {
		if g.State != "none" || g.Error != "" || g.Template == "" {
			t.Errorf("golden = %+v, want none with the template", g)
		}
	}
	if n := api.statusCalls[pfImgA]; n != 1 {
		t.Errorf("status calls = %d, want the one from ensure", n)
	}
}

func TestPreflightGoldenTimeout(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 1 << 20
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.GoldenTimeout = 5 * time.Millisecond })
	if err == nil || !strings.Contains(err.Error(), "2 goldens not ready") {
		t.Fatalf("Run error = %v, want both goldens pending", err)
	}
	for _, g := range report.Goldens {
		if g.State != "pending" || !strings.Contains(g.Error, "still pending") || g.Template == "" {
			t.Errorf("golden = %+v, want pending with a timeout error and the template it was ensured as", g)
		}
	}
}

// A round in which every probe fails ends the pair, also after a round that
// placed some: round 1 lands both probes on one node, round 2's creates fail,
// and no round 3 follows.
func TestPreflightStopsWhenARoundFailsEntirely(t *testing.T) {
	// Creates the control plane cut off (Unavailable) may have landed: they
	// are deleted by id, next to the probes that were created.
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.sameNode = true
	api.createFailAfter = 2
	api.createFailCode = codes.Unavailable
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.MaxRounds = 3
	})
	if err == nil || !strings.Contains(err.Error(), "short of their nodes") {
		t.Fatalf("Run error = %v, want a shortfall", err)
	}
	ic := report.Domains[0].Images[0]
	if ic.Rounds != 2 || ic.Probes != 4 || ic.Failed != 2 || len(ic.Nodes) != 1 || !strings.Contains(ic.Error, "cut the create off") {
		t.Errorf("coverage = %+v, want two rounds, the second failed entirely", ic)
	}
	deleted := map[string]bool{}
	for _, id := range api.deleted {
		deleted[id] = true
	}
	for _, id := range api.created {
		if !deleted[id] {
			t.Errorf("probe %s was created and not deleted", id)
		}
	}
	if len(api.deleted) != 4 {
		t.Errorf("deletes = %d, want 4: two probes and two cut-off creates deleted by id", len(api.deleted))
	}

	// A create the control plane refused (ResourceExhausted: no room) made
	// nothing, and nothing is deleted for it.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.sameNode = true
	api.createFailAfter = 2
	report, err, _ = runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.MaxRounds = 3
	})
	if err == nil || report.Complete {
		t.Fatalf("refused creates: err=%v complete=%v, want a shortfall", err, report.Complete)
	}
	if ic := report.Domains[0].Images[0]; ic.Failed != 2 || !strings.Contains(ic.Error, "no worker has room") {
		t.Errorf("coverage = %+v, want two refused probes", ic)
	}
	if len(api.deleted) != 2 {
		t.Errorf("deletes = %v, want only the two probes that were created", api.deleted)
	}
}

// node_slots bounds only the domains of the pass.
func TestPreflightNodeSlotsApplyToSelectedDomains(t *testing.T) {
	plan := twoDomainPlan()
	plan.Domains[1].NodeSlots = 1
	backend := newFakeBackend(t, newPreflightFake())
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend, ProbesPerNode: 2, Domains: []string{"d0"}}); err != nil {
		t.Errorf("d0 alone with d1 at one slot: %v", err)
	}
	if _, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend, ProbesPerNode: 2}); err == nil || !strings.Contains(err.Error(), "node slots") {
		t.Errorf("both domains: err = %v, want the node slots error", err)
	}
}

func TestPreflightSubsetsAndValidation(t *testing.T) {
	api := newPreflightFake()
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.Images = []string{pfImgA}; o.Domains = []string{"d0"} })
	if err != nil || !report.Complete {
		t.Fatalf("subset run: err=%v complete=%v", err, report.Complete)
	}
	if len(report.Goldens) != 1 || len(report.Domains) != 1 || len(report.Domains[0].Images) != 1 {
		t.Errorf("subset report = %+v", report)
	}
	// A domain subset drops the images no selected domain holds, so the
	// pass does not wait for goldens it does not use. Its probe names still
	// carry the domain's index in the plan, so parallel -domains passes
	// sharing the default -run-id do not collide on pf-0-....
	apiD0 := newPreflightFake()
	if _, err, _ := runPreflight(t, apiD0, func(o *PreflightOptions) { o.RunID = ""; o.Domains = []string{"d0"} }); err != nil {
		t.Fatalf("d0 only: %v", err)
	}
	api = newPreflightFake()
	report, err, _ = runPreflight(t, api, func(o *PreflightOptions) { o.RunID = ""; o.Domains = []string{"d1"} })
	if err != nil || !report.Complete || len(report.Goldens) != 1 || report.Goldens[0].Image != pfImgB {
		t.Errorf("d1 only: err=%v goldens=%+v, want b alone", err, report.Goldens)
	}
	d0Names := map[string]bool{}
	for _, id := range apiD0.created {
		d0Names[id] = true
	}
	for _, id := range api.created {
		// The default run id is pf-<6 random characters>; the plan's domain
		// index follows it.
		if !strings.HasPrefix(id, "pf-") || !strings.Contains(id, "-1-") || !strings.HasPrefix(id[len("pf-")+6:], "-1-") {
			t.Errorf("d1 probe id = %q, want plan domain index 1 (pf-<rand>-1-...)", id)
		}
		if d0Names[id] {
			t.Errorf("d1 probe id %q collides with d0 pass", id)
		}
	}
	for _, bad := range []func(*PreflightOptions){
		func(o *PreflightOptions) { o.Images = []string{pfImgC} },
		func(o *PreflightOptions) { o.Domains = []string{"d9"} },
		func(o *PreflightOptions) { o.Images = []string{pfImgA}; o.Domains = []string{"d1"} },
		func(o *PreflightOptions) { o.RunID = "Not Valid" },
		func(o *PreflightOptions) { o.Plan = nil },
		func(o *PreflightOptions) { o.Backend = nil },
	} {
		opts := PreflightOptions{Plan: twoDomainPlan(), Backend: newFakeBackend(t, newPreflightFake())}
		bad(&opts)
		if _, err := NewPreflightWarmer(opts); err == nil {
			t.Error("NewPreflightWarmer accepted an invalid option")
		}
	}
}

func TestPreflightRetriesTransientFirstCommand(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.unavailableStarts = 3
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.wakeBackoff = time.Millisecond
	})
	if err != nil || !report.Complete {
		t.Fatalf("transient StartProcess Unavailable: err=%v complete=%v", err, report.Complete)
	}
	if ic := report.Domains[0].Images[0]; ic.Rounds != 1 || ic.Failed != 0 {
		t.Errorf("coverage after transient wake retries = %+v, want 1 round and 0 failed", ic)
	}

	// A non-zero exit from the probe command fails the probe instead of
	// counting the node as covered.
	api = newPreflightFake()
	api.goldenReadyAfter = 0
	api.exitCode = 1
	report, err, _ = runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.MaxRounds = 1
	})
	if err == nil || report.Complete {
		t.Fatalf("non-zero probe exit: err=%v complete=%v, want failure", err, report.Complete)
	}
}

type cancelAfterFirstErrCtx struct {
	context.Context
	cancel func()
	checks atomic.Int32
}

func (c *cancelAfterFirstErrCtx) Err() error {
	if c.checks.Add(1) == 1 {
		c.cancel()
		return nil
	}
	return c.Context.Err()
}

func TestPreflightCancellationSkipsUnstartedProbes(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	api.domainNodes["d0"] = []string{"n0", "n1", "n2", "n3"}

	plan := twoDomainPlan()
	plan.Domains[0].Nodes = 4
	w, err := NewPreflightWarmer(PreflightOptions{
		Plan:          plan,
		Backend:       newFakeBackend(t, api),
		RunID:         "pf-cancel",
		ProbesPerNode: 4,
		Images:        []string{pfImgA},
		Domains:       []string{"d0"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Deterministically cancel right after coverImage's round condition check
	// so forEach stops with unstarted outcome slots (oc.id == "").
	inner, innerCancel := context.WithCancel(context.Background())
	ic := w.coverImage(&cancelAfterFirstErrCtx{Context: inner, cancel: innerCancel}, 0, 0, plan.Domains[0], pfImgA)
	for _, n := range ic.Nodes {
		if n == "" {
			t.Fatalf("empty node recorded from unstarted slot: %+v", ic)
		}
	}
	if ic.Probes >= 16 {
		t.Fatalf("unstarted probe slots counted: probes=%d, want < 16", ic.Probes)
	}

	// Cancel mid-round during Run via CreateEnvironment callback.
	ctx, cancel := context.WithCancel(context.Background())
	api.onCreate = func(string) { cancel() }
	report, runErr := w.Run(ctx)
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", runErr)
	}
	if report.Complete {
		t.Fatalf("cancelled report marked complete: %+v", report)
	}
	if err := report.Validate(); err != nil {
		t.Fatalf("cancelled report.Validate: %v", err)
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateAgainstJSONSchema("preflight_coverage.schema.json", raw); err != nil {
		t.Fatalf("cancelled report schema: %v\n%s", err, string(raw))
	}

	// Pre-cancelled context before Run starts: domain coverage still
	// validates and can be written by WritePreflightCoverage.
	api.onCreate = nil
	preCtx, preCancel := context.WithCancel(context.Background())
	preCancel()
	report2, runErr2 := w.Run(preCtx)
	if !errors.Is(runErr2, context.Canceled) {
		t.Fatalf("pre-cancelled Run err = %v, want context.Canceled", runErr2)
	}
	if err := report2.Validate(); err != nil {
		t.Fatalf("pre-cancelled report.Validate: %v", err)
	}
	raw2, err := json.Marshal(report2)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.ValidateAgainstJSONSchema("preflight_coverage.schema.json", raw2); err != nil {
		t.Fatalf("pre-cancelled report schema: %v\n%s", err, string(raw2))
	}
}

// A probe name another pass or a user holds is not ours: the create fails
// with AlreadyExists, the probe counts as failed, and nothing deletes the
// environment behind the name (a cut-off create is deleted by id:
// TestPreflightStopsWhenARoundFailsEntirely).
func TestPreflightLeavesACollidingNameAlone(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	taken := probeID("pf-test", 0, 0, 1, 0)
	api.existing = map[string]bool{taken: true}
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		o.MaxRounds = 1
	})
	if err == nil || !strings.Contains(err.Error(), "short of their nodes") {
		t.Fatalf("Run error = %v, want a shortfall", err)
	}
	ic := report.Domains[0].Images[0]
	if ic.Probes != 2 || ic.Failed != 1 || !strings.Contains(ic.Error, "already exists") {
		t.Errorf("coverage = %+v, want one refused probe", ic)
	}
	for _, id := range api.deleted {
		if id == taken {
			t.Errorf("the environment %s, which is not ours, was deleted", taken)
		}
	}
	if len(api.deleted) != 1 {
		t.Errorf("deletes = %v, want only our own probe", api.deleted)
	}
}

func TestPreflightDefaultRunIDIsUniquePerPass(t *testing.T) {
	plan := twoDomainPlan()
	backend := newFakeBackend(t, newPreflightFake())
	a, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPreflightWarmer(PreflightOptions{Plan: plan, Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{a.opts.RunID, b.opts.RunID} {
		if !strings.HasPrefix(id, DefaultPreflightRunIDPrefix+"-") || len(id) != len(DefaultPreflightRunIDPrefix)+7 || !runIDPattern.MatchString(id) {
			t.Errorf("default run id %q, want pf-<6 characters> matching %s", id, runIDPattern)
		}
	}
	if a.opts.RunID == b.opts.RunID {
		t.Errorf("two passes got the same default run id %q", a.opts.RunID)
	}
	for _, err := range []error{
		status.Error(codes.AlreadyExists, "exists"), status.Error(codes.InvalidArgument, "bad"),
		status.Error(codes.FailedPrecondition, "no template"), status.Error(codes.PermissionDenied, "no"),
		status.Error(codes.ResourceExhausted, "no room"),
		fmt.Errorf("create x: %w", status.Error(codes.AlreadyExists, "exists")),
	} {
		if !createRefused(err) {
			t.Errorf("createRefused(%v) = false, want true", err)
		}
	}
	for _, err := range []error{
		status.Error(codes.Unavailable, "down"), status.Error(codes.DeadlineExceeded, "slow"),
		context.Canceled, errors.New("plain"), nil,
	} {
		if createRefused(err) {
			t.Errorf("createRefused(%v) = true, want false", err)
		}
	}
}

// A probe whose delete fails is listed under the image's undeleted_probes,
// the coverage still says what was covered, and the pass fails naming it.
func TestPreflightReportsUndeletedProbes(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	stuck := probeID("pf-test", 0, 0, 1, 1)
	api.deleteFail = map[string]bool{stuck: true}
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) {
		o.Images = []string{pfImgA}
		o.Domains = []string{"d0"}
		// A short delete budget: the stuck probe's Internal is retried a
		// few times within it, not for the two minutes of the default.
		o.Backend.(*EnvBackend).deletes = deleteLimits{budget: 20 * time.Millisecond, minWait: time.Millisecond, maxWait: 2 * time.Millisecond}
	})
	if err == nil || !strings.Contains(err.Error(), "1 probes left undeleted") || !strings.Contains(err.Error(), stuck) {
		t.Fatalf("Run error = %v, want the undeleted probe named", err)
	}
	if api.deleteCalls < 3 {
		t.Errorf("delete calls = %d, want the other probe's one and the stuck probe's retries", api.deleteCalls)
	}
	ic := report.Domains[0].Images[0]
	if !ic.Complete || !reflect.DeepEqual(ic.UndeletedProbes, []string{stuck}) {
		t.Errorf("coverage = %+v, want complete with %s undeleted", ic, stuck)
	}
	if !report.Complete {
		t.Error("report.Complete = false; the coverage held, the leftover is reported separately")
	}
	if got := UndeletedProbes(report); !reflect.DeepEqual(got, []string{stuck}) {
		t.Errorf("UndeletedProbes = %v", got)
	}
}

// A probe whose delete answers NotFound is gone already: it is not listed as
// undeleted and the pass is clean.
func TestPreflightTreatsNotFoundOnDeleteAsGone(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 0
	gone := probeID("pf-test", 0, 0, 1, 1)
	api.deleteNotFound = map[string]bool{gone: true}
	report, err, _ := runPreflight(t, api, func(o *PreflightOptions) { o.Images = []string{pfImgA}; o.Domains = []string{"d0"} })
	if err != nil || !report.Complete {
		t.Fatalf("Run: err=%v complete=%v, want a clean pass", err, report.Complete)
	}
	if got := UndeletedProbes(report); len(got) != 0 {
		t.Errorf("undeleted = %v, want none for a NotFound delete", got)
	}
}

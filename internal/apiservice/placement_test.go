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

package apiservice_test

import (
	"context"
	"errors"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	d0Labels = map[string]string{"workload": "default-env", "ate-env/domain": "d0"}
	d1Labels = map[string]string{"workload": "default-env", "ate-env/domain": "d1"}
)

// seedDomains registers two pools of two schedulable nodes each, plus a
// worker without capacity yet and a draining one that must not count, the
// base template and a ready derived template for testRootfsImage. Actors are
// born suspended, as on Substrate, so a probe runs only once it is resumed.
func seedDomains(t *testing.T, control *fakecontrol.Server) {
	t.Helper()
	control.BornSuspended = true
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-d0", "node-d0-1", d0Labels, 16))
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-d0", "node-d0-2", d0Labels, 12))
	// Registered, capacity not reported yet: the scheduler places nothing
	// on it.
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-d0", "node-d0-9", d0Labels, 0))
	// Each d1 worker carries a label of its own on top of the pool's.
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-d1", "node-d1-1", withLabel(d1Labels, "rack", "a"), 4))
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-d1", "node-d1-2", withLabel(d1Labels, "rack", "b"), 6))
	draining := fakecontrol.Worker("ate-system", "pool-d1", "node-d1-3", d1Labels, 4)
	draining.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
	control.AddWorker(draining)
	seedDerivedTemplate(t, control, &ateapipb.GoldenSnapshotStatus{GoldenTag: &ateapipb.ObjectRef{Name: "golden-7"}}, 0)
}

func withLabel(labels map[string]string, k, v string) map[string]string {
	out := maps.Clone(labels)
	out[k] = v
	return out
}

func TestListPlacementDomainsGroupsActiveWorkersByPool(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	resp, err := client.ListPlacementDomains(context.Background(), &ateenvv1alpha.ListPlacementDomainsRequest{})
	if err != nil {
		t.Fatalf("ListPlacementDomains: %v", err)
	}
	domains := resp.GetDomains()
	if len(domains) != 2 || domains[0].GetName() != "pool-d0" || domains[1].GetName() != "pool-d1" {
		t.Fatalf("domains = %v, want pool-d0 then pool-d1", domains)
	}
	d0, d1 := domains[0], domains[1]
	// The worker without capacity and its node do not count.
	if d0.GetWorkerNamespace() != "ate-system" || !maps.Equal(d0.GetSelector(), d0Labels) || d0.GetNodes() != 2 || d0.GetWorkers() != 2 || d0.GetNodeSlots() != 12 {
		t.Errorf("pool-d0 = %v, want 2 nodes, 2 workers, 12 slots (the smallest), labels %v", d0, d0Labels)
	}
	// The draining worker and its node do not count; the selector is the
	// labels every worker carries, not the first worker's.
	if !maps.Equal(d1.GetSelector(), d1Labels) || d1.GetNodes() != 2 || d1.GetWorkers() != 2 || d1.GetNodeSlots() != 4 {
		t.Errorf("pool-d1 = %v, want 2 nodes, 2 workers, 4 slots, labels %v", d1, d1Labels)
	}

	control.DenyListWorkers = true
	if _, err := client.ListPlacementDomains(context.Background(), &ateenvv1alpha.ListPlacementDomainsRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("without the global viewer role: err = %v, want PermissionDenied", err)
	}
}

// warm runs WarmImage and returns its probes and coverage.
func warm(t *testing.T, client ateenvv1alpha.EnvironmentServiceClient, req *ateenvv1alpha.WarmImageRequest) ([]*ateenvv1alpha.WarmProbe, *ateenvv1alpha.WarmImageCoverage, error) {
	t.Helper()
	stream, err := client.WarmImage(context.Background(), req)
	if err != nil {
		return nil, nil, err
	}
	var probes []*ateenvv1alpha.WarmProbe
	var cov *ateenvv1alpha.WarmImageCoverage
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return probes, cov, nil
		}
		if err != nil {
			return probes, cov, err
		}
		switch e := ev.GetEvent().(type) {
		case *ateenvv1alpha.WarmImageEvent_Probe:
			probes = append(probes, e.Probe)
		case *ateenvv1alpha.WarmImageEvent_Coverage:
			if cov != nil {
				t.Error("two coverage events")
			}
			cov = e.Coverage
		}
	}
}

// A template narrows the list to the pools its own selector matches, and a
// pool whose workers share no label is never addressable.
func TestListPlacementDomainsFiltersByTemplate(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	// A pool on another workload, and one whose two workers share no label.
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-other", "node-o-1", map[string]string{"workload": "other"}, 8))
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-mixed", "node-m-1", map[string]string{"rack": "a"}, 8))
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-mixed", "node-m-2", map[string]string{"rack": "b"}, 8))
	// A base template pinned to d0 on top of the default one (workload only).
	control.AddTemplate(&ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "d0-template"},
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"workload": "default-env", "ate-env/domain": "d0"}},
	})
	names := func(req *ateenvv1alpha.ListPlacementDomainsRequest) ([]string, error) {
		resp, err := client.ListPlacementDomains(context.Background(), req)
		if err != nil {
			return nil, err
		}
		out := []string{}
		for _, d := range resp.GetDomains() {
			out = append(out, d.GetName())
		}
		return out, nil
	}
	cases := []struct {
		name string
		req  *ateenvv1alpha.ListPlacementDomainsRequest
		want []string
	}{
		{"no template: every pool", &ateenvv1alpha.ListPlacementDomainsRequest{}, []string{"pool-d0", "pool-d1", "pool-mixed", "pool-other"}},
		{"atespace alone names the default template", &ateenvv1alpha.ListPlacementDomainsRequest{Atespace: "ate-env"}, []string{"pool-d0", "pool-d1"}},
		{"template by name", &ateenvv1alpha.ListPlacementDomainsRequest{Template: &ateenvv1alpha.Template{Name: "d0-template"}}, []string{"pool-d0"}},
		{"template with its atespace", &ateenvv1alpha.ListPlacementDomainsRequest{Template: &ateenvv1alpha.Template{Name: "d0-template", Atespace: "ate-env"}}, []string{"pool-d0"}},
	}
	for _, tc := range cases {
		got, err := names(tc.req)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: domains = %v, want %v", tc.name, got, tc.want)
		}
	}
	if _, err := names(&ateenvv1alpha.ListPlacementDomainsRequest{Template: &ateenvv1alpha.Template{Name: "nope"}}); status.Code(err) != codes.NotFound {
		t.Errorf("unknown template: err = %v, want NotFound", err)
	}
	// The template is read first: an unknown one is NotFound even when the
	// workers may not be listed, and a known one still needs the role.
	control.DenyListWorkers = true
	if _, err := names(&ateenvv1alpha.ListPlacementDomainsRequest{Template: &ateenvv1alpha.Template{Name: "nope"}}); status.Code(err) != codes.NotFound {
		t.Errorf("unknown template without the role: err = %v, want NotFound", err)
	}
	if _, err := names(&ateenvv1alpha.ListPlacementDomainsRequest{Template: &ateenvv1alpha.Template{Name: "d0-template"}}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("template without the role: err = %v, want PermissionDenied", err)
	}
}

func TestWarmImageCoversEveryNodeOfTheDomainAndDeletesTheProbes(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.PendingGets = 2
	probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage:    testRootfsImage,
		WorkerSelector: map[string]string{"ate-env/domain": "d0"},
		ProbesPerNode:  1,
		RunId:          "pf-0-3",
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if cov == nil || !cov.GetComplete() || !slices.Equal(cov.GetNodes(), []string{"node-d0-1", "node-d0-2"}) || cov.GetWanted() != 2 || cov.GetProbes() != 2 || cov.GetFailed() != 0 || cov.GetRounds() != 1 {
		t.Fatalf("coverage = %v, want both d0 nodes in one round of two probes", cov)
	}
	if len(probes) != 2 {
		t.Fatalf("probes = %v, want 2", probes)
	}
	for _, p := range probes {
		if !strings.HasPrefix(p.GetId(), "pf-0-3-1-") || p.GetNode() == "" || p.GetRound() != 1 || p.GetError() != "" || p.GetDuration().AsDuration() <= 0 {
			t.Errorf("probe %v, want a named, placed, timed success of round 1", p)
		}
	}
	// The probes ran on the derived template with the selector, were resumed
	// (a new actor is suspended until then), and are gone.
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d actors left after the call, want 0", n)
	}
	if n := control.Resumes(); n != 2 {
		t.Errorf("%d resumes, want one per probe (2)", n)
	}
	created := control.Created()
	if len(created) != 2 {
		t.Fatalf("created = %v, want 2 probes", created)
	}
	for _, a := range created {
		if a.GetActorTemplate().GetName() != testDerivedTemplate || a.GetWorkerSelector().GetMatchLabels()["ate-env/domain"] != "d0" {
			t.Errorf("probe actor %v, want template %s with the d0 selector", a, testDerivedTemplate)
		}
	}
}

func TestWarmImageResolvesNodesFromTheWorkersAndRefusesWithoutThem(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)

	// nodes 0: the distinct nodes of the matching workers, here d1's two.
	_, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d1"}, ProbesPerNode: 1})
	if err != nil || !cov.GetComplete() || cov.GetWanted() != 2 {
		t.Errorf("d1 with nodes 0: err=%v coverage=%v, want 2 wanted and complete", err, cov)
	}
	// An empty selector means the template's own pools: all four schedulable
	// nodes, and the probes land on those (never on the worker without
	// capacity or the draining one).
	_, cov, err = warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, ProbesPerNode: 1})
	if err != nil || !cov.GetComplete() || cov.GetWanted() != 4 || !slices.Equal(cov.GetNodes(), []string{"node-d0-1", "node-d0-2", "node-d1-1", "node-d1-2"}) {
		t.Errorf("template pools with nodes 0: err=%v coverage=%v, want the 4 schedulable nodes and complete", err, cov)
	}
	// A selector no worker carries.
	_, _, err = warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d9"}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("unknown domain: err = %v, want FailedPrecondition", err)
	}
	// Without the permission to list workers, nodes must be given.
	control.DenyListWorkers = true
	_, _, err = warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d1"}})
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "pass nodes") {
		t.Errorf("nodes 0 without the role: err = %v, want PermissionDenied naming the fix", err)
	}
	_, cov, err = warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d1"}, Nodes: 2, ProbesPerNode: 1})
	if err != nil || !cov.GetComplete() {
		t.Errorf("nodes given without the role: err=%v coverage=%v, want complete", err, cov)
	}
}

func TestWarmImageReportsShortCoverageAfterItsRounds(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.SameNode = true
	probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage:    testRootfsImage,
		WorkerSelector: map[string]string{"ate-env/domain": "d0"},
		ProbesPerNode:  2,
		MaxRounds:      2,
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if cov.GetComplete() || !slices.Equal(cov.GetNodes(), []string{"node-d0-1"}) || cov.GetRounds() != 2 || cov.GetProbes() != 8 || cov.GetFailed() != 0 {
		t.Errorf("coverage = %v, want one node after 2 rounds of 4 probes", cov)
	}
	if len(probes) != 8 || probes[7].GetRound() != 2 {
		t.Errorf("probes = %d (last round %d), want 8 over two rounds", len(probes), probes[len(probes)-1].GetRound())
	}
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d actors left, want 0", n)
	}
}

func TestWarmImageProbeFailures(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)

	// A probe that never gets RUNNING within the timeout fails, is deleted,
	// and the round reports it; the other probes still count.
	control.PendingGets = 1 << 20
	probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage:    testRootfsImage,
		WorkerSelector: map[string]string{"ate-env/domain": "d0"},
		ProbesPerNode:  1,
		MaxRounds:      1,
		ProbeTimeout:   durationpb.New(300 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if cov.GetComplete() || cov.GetFailed() != 2 || !strings.Contains(cov.GetError(), "not running after") {
		t.Errorf("coverage = %v, want two timed-out probes", cov)
	}
	for _, p := range probes {
		if p.GetError() == "" || p.GetNode() != "" {
			t.Errorf("probe %v, want an error and no node", p)
		}
	}
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d actors left after timeouts, want 0", n)
	}

	// A round in which every probe fails ends the call before max_rounds:
	// nodes given for a selector no worker carries leaves the probes
	// nowhere to resume (RESOURCE_EXHAUSTED).
	control.PendingGets = 0
	_, cov, err = warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage:    testRootfsImage,
		WorkerSelector: map[string]string{"ate-env/domain": "d9"},
		Nodes:          1,
		MaxRounds:      3,
		ProbeTimeout:   durationpb.New(300 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("WarmImage on an empty domain: %v", err)
	}
	if cov.GetRounds() != 1 || cov.GetProbes() != apiservice.DefaultWarmProbesPerNode || cov.GetFailed() != cov.GetProbes() || !strings.Contains(cov.GetError(), "resume: ") || !strings.Contains(cov.GetError(), "no worker matches") {
		t.Errorf("coverage = %v, want one round of %d probes that found no worker", cov, apiservice.DefaultWarmProbesPerNode)
	}
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d actors left, want 0", n)
	}
}

func TestWarmImageValidation(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	for name, req := range map[string]*ateenvv1alpha.WarmImageRequest{
		"no image":           {},
		"unpinned image":     {RootfsImage: "docker.io/library/python:3.11"},
		"negative nodes":     {RootfsImage: testRootfsImage, Nodes: -1},
		"too many probes":    {RootfsImage: testRootfsImage, ProbesPerNode: apiservice.MaxWarmProbesPerNode + 1},
		"too many rounds":    {RootfsImage: testRootfsImage, MaxRounds: apiservice.MaxWarmRounds + 1},
		"bad run id":         {RootfsImage: testRootfsImage, RunId: "Not Valid"},
		"long run id":        {RootfsImage: testRootfsImage, RunId: strings.Repeat("a", 21)},
		"round over the cap": {RootfsImage: testRootfsImage, Nodes: apiservice.MaxWarmProbes, ProbesPerNode: 2},
		"negative timeout":   {RootfsImage: testRootfsImage, ProbeTimeout: durationpb.New(-time.Second)},
		"zero timeout":       {RootfsImage: testRootfsImage, ProbeTimeout: durationpb.New(0)},
	} {
		_, _, err := warm(t, client, req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: err = %v, want InvalidArgument", name, err)
		}
	}
	// A missing base template is FailedPrecondition, as on CreateEnvironment.
	_, _, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, Template: &ateenvv1alpha.Template{Name: "nope"}, Nodes: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("missing base: err = %v, want FailedPrecondition", err)
	}
	// Defaults: 2 probes per node, 3 rounds, a run id of "warm" and a random
	// suffix, so two calls never name a probe alike.
	control.SameNode = true
	runID := regexp.MustCompile(`^` + apiservice.DefaultWarmRunID + `-[a-z0-9]{6}-1-`)
	probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}})
	if err != nil || cov.GetRounds() != apiservice.DefaultWarmRounds || cov.GetProbes() != 2*apiservice.DefaultWarmProbesPerNode*apiservice.DefaultWarmRounds || !runID.MatchString(probes[0].GetId()) {
		t.Errorf("defaults: err=%v coverage=%v first probe=%v", err, cov, probes[0])
	}
	again, _, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, MaxRounds: 1})
	if err != nil || !runID.MatchString(again[0].GetId()) || again[0].GetId() == probes[0].GetId() {
		t.Errorf("second call: err=%v first probe=%v, want a run id of its own (first call's %v)", err, again[0], probes[0])
	}
}

// A worker selector that sets a key of the template's selector to another
// value matches no worker; the call refuses it instead of failing every probe.
func TestWarmImageRefusesASelectorThatContradictsTheTemplate(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	_, _, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage:    testRootfsImage,
		WorkerSelector: map[string]string{"workload": "other", "ate-env/domain": "d0"},
		Nodes:          2,
	})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "workload=other conflicts with the template's workload=default-env") {
		t.Errorf("err = %v, want FailedPrecondition naming the conflict", err)
	}
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d probes created, want none", n)
	}
}

// A probe name that exists already is another call's probe or a user's
// environment: the create fails, is reported, and the environment is left as
// it was, whether the first attempt found the name taken or a retry did after
// an attempt the control plane refused (a refusal made nothing). Only a
// retry after an attempt cut off, which may have made the actor, claims the
// name: the run id is unique per call.
func TestWarmImageLeavesAnExistingEnvironmentAlone(t *testing.T) {
	apiservice.SetWarmTimings(t, 10*time.Millisecond, 50*time.Millisecond, time.Second)
	for name, tc := range map[string]struct {
		inject  func(*fakecontrol.Server)
		creates int
		claimed bool
	}{
		"name taken at once": {func(*fakecontrol.Server) {}, 1, false},
		"name taken after a refusal": {func(c *fakecontrol.Server) {
			c.FailCreates(1, codes.FailedPrecondition, "actor template not found; ObjectRef: ate-env/"+testDerivedTemplate)
		}, 2, false},
		"name taken after a create cut off": {func(c *fakecontrol.Server) {
			c.FailCreates(1, codes.Unavailable, "graceful_stop")
		}, 2, true},
	} {
		client, control := newTestEnv(t)
		seedDomains(t, control)
		if _, err := control.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "pf-x-1-0"},
			ActorTemplate:  &ateapipb.ObjectRef{Atespace: "ate-env", Name: testDerivedTemplate},
			WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"ate-env/domain": "d0"}},
		}}); err != nil {
			t.Fatal(err)
		}
		tc.inject(control)
		seeded := control.Creates()
		probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
			RootfsImage:    testRootfsImage,
			WorkerSelector: map[string]string{"ate-env/domain": "d0"},
			Nodes:          1,
			ProbesPerNode:  1,
			MaxRounds:      1,
			RunId:          "pf-x",
		})
		if err != nil {
			t.Fatalf("%s: WarmImage: %v", name, err)
		}
		if n := control.Creates() - seeded; n != tc.creates {
			t.Errorf("%s: %d creates, want %d", name, n, tc.creates)
		}
		if tc.claimed {
			// The cut-off create landed: the probe is this call's, runs and
			// is deleted.
			if len(probes) != 1 || probes[0].GetError() != "" || !cov.GetComplete() {
				t.Errorf("%s: probes = %v coverage = %v, want the probe claimed and the coverage complete", name, probes, cov)
			}
			if n := control.Resumes(); n != 1 {
				t.Errorf("%s: %d resumes, want 1", name, n)
			}
			if n := control.ActorCount(); n != 0 {
				t.Errorf("%s: %d actors left, want 0", name, n)
			}
			continue
		}
		if len(probes) != 1 || !strings.Contains(probes[0].GetError(), "create: ") || !strings.Contains(probes[0].GetError(), "already exists") || cov.GetFailed() != 1 || cov.GetComplete() {
			t.Errorf("%s: probes = %v coverage = %v, want one failed create and no coverage", name, probes, cov)
		}
		if n := control.ActorCount(); n != 1 {
			t.Errorf("%s: %d actors left, want the pre-existing one", name, n)
		}
		if n := control.Resumes(); n != 0 {
			t.Errorf("%s: %d resumes, want none: the environment is not this call's", name, n)
		}
		if n := control.Deletes(); n != 0 {
			t.Errorf("%s: %d deletes, want none: the environment is not this call's", name, n)
		}
	}
}

// The atespace is ensured once, before the rounds: a server that may not
// create it fails the call before any probe, instead of failing each probe's
// create.
func TestWarmImageEnsuresTheAtespaceBeforeTheRounds(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.DenyCreateAtespace = true
	probes, _, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1,
	})
	if status.Code(err) != codes.PermissionDenied || len(probes) != 0 {
		t.Errorf("err = %v, %d probes, want PermissionDenied before any probe", err, len(probes))
	}
	if n := control.Creates(); n != 0 {
		t.Errorf("%d creates, want none", n)
	}
}

func TestWarmImageCancelDeletesTheProbes(t *testing.T) {
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.PendingGets = 1 << 20
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.WarmImage(ctx, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Wait until the probes exist, then cancel.
	deadline := time.Now().Add(5 * time.Second)
	for control.ActorCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Errorf("Recv after cancel: %v, want Canceled", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for control.ActorCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d probes left after the cancel, want 0", n)
	}
}

// The transient failures a control plane under load hands a probe's create
// and resume are retried within the probe's budget, and the probe succeeds.
func TestWarmImageRetriesTransientCreateAndResumeFailures(t *testing.T) {
	apiservice.SetWarmTimings(t, 10*time.Millisecond, 50*time.Millisecond, time.Second)
	apiservice.SetWarmLateCreateSettle(t, 20*time.Millisecond)
	for name, inject := range map[string]struct {
		code codes.Code
		msg  string
	}{
		"template not yet visible": {codes.FailedPrecondition, "actor template not found; ObjectRef: ate-env/" + testDerivedTemplate},
		"api server restarting":    {codes.Unavailable, "error reading from server: EOF; graceful_stop"},
		"database queuing":         {codes.DeadlineExceeded, "context deadline exceeded"},
	} {
		client, control := newTestEnv(t)
		seedDomains(t, control)
		control.FailCreates(2, inject.code, inject.msg)
		_, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
			RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1,
		})
		if err != nil {
			t.Fatalf("%s: WarmImage: %v", name, err)
		}
		if !cov.GetComplete() || cov.GetFailed() != 0 || len(cov.GetUndeletedProbes()) != 0 {
			t.Errorf("%s: coverage = %v, want complete with no failure after the retries", name, cov)
		}
		if n := control.Creates(); n != 4 {
			t.Errorf("%s: %d creates, want 2 probes + 2 retried failures", name, n)
		}
		if n := control.ActorCount(); n != 0 {
			t.Errorf("%s: %d actors left, want 0", name, n)
		}
	}

	// A resume cut by an api server restart is retried the same way.
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.FailResumes(1, codes.Unavailable, "graceful_stop")
	_, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1,
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if !cov.GetComplete() || cov.GetFailed() != 0 {
		t.Errorf("coverage = %v, want complete after the resume retry", cov)
	}
	if n := control.Resumes(); n != 3 {
		t.Errorf("%d resumes, want 2 probes + 1 retried failure", n)
	}

	// Once the budget is spent the probe fails, with the attempts noted.
	apiservice.SetWarmLateCreateSettle(t, 20*time.Millisecond)
	client, control = newTestEnv(t)
	seedDomains(t, control)
	control.FailCreates(1<<20, codes.Unavailable, "graceful_stop")
	probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1,
		ProbeTimeout: durationpb.New(200 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if cov.GetFailed() != 2 || !strings.Contains(cov.GetError(), "create: ") || !strings.Contains(cov.GetError(), "graceful_stop") || !strings.Contains(cov.GetError(), "attempts)") {
		t.Errorf("coverage = %v, want two creates failed after their attempts", cov)
	}
	if len(probes) != 2 || probes[0].GetDuration().AsDuration() < 200*time.Millisecond {
		t.Errorf("probes = %v, want two that spent the budget", probes)
	}
	if n := control.Creates(); n < 6 {
		t.Errorf("%d creates, want several attempts per probe", n)
	}
	// A create cut off may have landed, so each probe is deleted, and since
	// its create was never confirmed the delete is repeated twice in the
	// settle window in case the create lands late. The NotFound that answers
	// is the probe gone, not one left for a sweep.
	if n := control.Deletes(); n != 2*3 {
		t.Errorf("%d deletes, want 3 per probe whose create may have landed (one, then two re-checks)", n)
	}
	if len(cov.GetUndeletedProbes()) != 0 {
		t.Errorf("undeleted = %v, want none: a NotFound delete is done", cov.GetUndeletedProbes())
	}
}

// A failure that waiting cannot fix is final: the probe fails at once, with
// no retry, and is cleaned up as before.
func TestWarmImageDoesNotRetryFinalCreateFailures(t *testing.T) {
	apiservice.SetWarmTimings(t, 10*time.Millisecond, 50*time.Millisecond, time.Second)
	for name, inject := range map[string]struct {
		code codes.Code
		msg  string
	}{
		"another precondition": {codes.FailedPrecondition, "MarkResuming prerequisite not met for Actor"},
		"invalid argument":     {codes.InvalidArgument, "metadata.name is invalid"},
		"no room":              {codes.ResourceExhausted, "atespace quota reached"},
	} {
		client, control := newTestEnv(t)
		seedDomains(t, control)
		control.FailCreates(1, inject.code, inject.msg)
		probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
			RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1,
		})
		if err != nil {
			t.Fatalf("%s: WarmImage: %v", name, err)
		}
		if cov.GetFailed() != 1 || !strings.Contains(cov.GetError(), "create: ") || !strings.Contains(cov.GetError(), inject.msg) || strings.Contains(cov.GetError(), "attempts") {
			t.Errorf("%s: coverage = %v, want one create failed at once", name, cov)
		}
		// The refused probe never existed: nothing of it is left for a sweep.
		if len(cov.GetUndeletedProbes()) != 0 {
			t.Errorf("%s: undeleted = %v, want none: the refused probe never existed", name, cov.GetUndeletedProbes())
		}
		if n := control.Creates(); n != 2 {
			t.Errorf("%s: %d creates, want 2: no retry", name, n)
		}
		// The refused probe made nothing to delete; the other probe is deleted.
		if n := control.Deletes(); n != 1 {
			t.Errorf("%s: %d deletes, want 1: a refused create is not deleted", name, n)
		}
		if len(probes) != 2 || control.ActorCount() != 0 {
			t.Errorf("%s: probes = %v, %d actors left, want 2 probes and none left", name, probes, control.ActorCount())
		}
	}
}

// A delete that fails Aborted, Unavailable or ResourceExhausted is retried
// within its budget, and the probe is gone.
func TestWarmImageRetriesProbeDeletes(t *testing.T) {
	apiservice.SetWarmTimings(t, 10*time.Millisecond, 50*time.Millisecond, time.Second)
	for _, code := range []codes.Code{codes.Aborted, codes.Unavailable, codes.ResourceExhausted} {
		client, control := newTestEnv(t)
		seedDomains(t, control)
		control.FailDeletes(3, code)
		_, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
			RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1,
		})
		if err != nil {
			t.Fatalf("%s: WarmImage: %v", code, err)
		}
		if !cov.GetComplete() || len(cov.GetUndeletedProbes()) != 0 {
			t.Errorf("%s: coverage = %v, want complete with nothing left behind", code, cov)
		}
		if n := control.ActorCount(); n != 0 {
			t.Errorf("%s: %d actors left after the retried deletes, want 0", code, n)
		}
		if n := control.Deletes(); n != 5 {
			t.Errorf("%s: %d deletes, want 2 probes + 3 retried failures", code, n)
		}
	}
}

// A probe still there when its delete budget ends is listed in the coverage,
// so the caller can sweep it; the coverage itself is what the probes earned.
func TestWarmImageListsTheProbesItCouldNotDelete(t *testing.T) {
	apiservice.SetWarmTimings(t, 10*time.Millisecond, 20*time.Millisecond, 200*time.Millisecond)
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.FailDeletes(1<<20, codes.ResourceExhausted)
	_, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1, RunId: "pf-u",
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if !cov.GetComplete() || cov.GetFailed() != 0 || !slices.Equal(cov.GetUndeletedProbes(), []string{"pf-u-1-0", "pf-u-1-1"}) {
		t.Errorf("coverage = %v, want complete and both probes listed undeleted", cov)
	}
	if n := control.ActorCount(); n != 2 {
		t.Errorf("%d actors left, want the 2 the control plane would not delete", n)
	}
	if n := control.Deletes(); n < 6 {
		t.Errorf("%d deletes, want several attempts per probe before giving up", n)
	}
}

// A probe whose resume fails ends CRASHED, which Substrate cannot delete
// today: the refusal is final, and the probe is listed for the sweep.
func TestWarmImageListsCrashedProbesForTheSweep(t *testing.T) {
	apiservice.SetWarmTimings(t, 10*time.Millisecond, 20*time.Millisecond, time.Second)
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.KeepCrashed = true
	control.PendingGets = 1 << 20
	done := make(chan struct{})
	var cov *ateenvv1alpha.WarmImageCoverage
	var err error
	go func() {
		defer close(done)
		_, cov, err = warm(t, client, &ateenvv1alpha.WarmImageRequest{
			RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 1, MaxRounds: 1, RunId: "pf-c",
			ProbeTimeout: durationpb.New(5 * time.Second),
		})
	}()
	// Once both probes were resumed, crash them, as a restore that failed
	// does.
	deadline := time.Now().Add(5 * time.Second)
	for control.Resumes() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	control.Crash("pf-c-1-0", "restore failed")
	control.Crash("pf-c-1-1", "restore failed")
	<-done
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if cov.GetFailed() != 2 || !strings.Contains(cov.GetError(), "crashed: restore failed") {
		t.Errorf("coverage = %v, want both probes crashed", cov)
	}
	if !slices.Equal(cov.GetUndeletedProbes(), []string{"pf-c-1-0", "pf-c-1-1"}) {
		t.Errorf("undeleted = %v, want both crashed probes listed", cov.GetUndeletedProbes())
	}
	if n := control.Deletes(); n != 2 {
		t.Errorf("%d deletes, want one per probe: the refusal is final", n)
	}
	if n := control.ActorCount(); n != 2 {
		t.Errorf("%d actors left, want the 2 crashed ones", n)
	}
}

// The server caps the probes in flight per node of one selector across
// calls: a second call on the same selector waits for the first round's
// slots, and together they never exceed nodes x the cap.
func TestWarmImageCapsProbesInFlightPerSelectorAcrossCalls(t *testing.T) {
	te := newFullTestEnv(t)
	client, control := te.envClient, te.control
	seedDomains(t, control)
	te.srv.WarmMaxProbesPerNode = 1
	// Each probe holds for a few polls, so the two calls overlap.
	control.PendingGets = 3

	// probes_per_node above the cap is refused, naming the flag.
	_, _, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, ProbesPerNode: 2})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "-warm-max-probes-per-node") {
		t.Fatalf("probes_per_node over the cap: err = %v, want InvalidArgument naming the flag", err)
	}
	if n := control.ActorCount(); n != 0 {
		t.Fatalf("%d probes created by a refused request, want 0", n)
	}

	// Two calls at once, the default probes_per_node (clamped to the cap).
	type result struct {
		cov *ateenvv1alpha.WarmImageCoverage
		err error
	}
	results := make(chan result, 2)
	for _, runID := range []string{"pf-a", "pf-b"} {
		go func() {
			_, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
				RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, MaxRounds: 1, RunId: runID,
			})
			results <- result{cov, err}
		}()
	}
	waited := 0
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatalf("WarmImage: %v", r.err)
		}
		if !r.cov.GetComplete() || r.cov.GetProbes() != 2 {
			t.Errorf("coverage = %v, want complete with 2 probes (2 nodes x the cap of 1)", r.cov)
		}
		if r.cov.GetWaited().AsDuration() > 0 {
			waited++
		}
	}
	if waited != 1 {
		t.Errorf("%d calls waited for slots, want exactly the second one", waited)
	}
	if n := control.MaxActors(); n != 2 {
		t.Errorf("%d probes existed at once, want the cap of 2 (2 nodes x 1) across both calls", n)
	}
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d actors left, want 0", n)
	}
}

func TestServerInfoAdvertisesPlacementFeatures(t *testing.T) {
	client, _ := newTestEnv(t)
	info, err := client.GetServerInfo(context.Background(), &ateenvv1alpha.GetServerInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{apiservice.FeaturePlacementDomains, apiservice.FeatureWarmImage} {
		if !slices.Contains(info.GetFeatures(), f) {
			t.Errorf("features %v lack %s", info.GetFeatures(), f)
		}
	}
}

// A create cut off may have landed, and a later attempt refused for good
// does not undo that: the probe is deleted all the same, and nothing is
// left for a sweep. Found by TestPropWarmImageLeavesNoProbe (1 node, 1
// probe, create plan: stored but reply lost, then refused).
func TestWarmImageDeletesAProbeWhoseLostCreateLanded(t *testing.T) {
	apiservice.SetWarmTimings(t, 10*time.Millisecond, 50*time.Millisecond, time.Second)
	apiservice.SetWarmLateCreateSettle(t, 20*time.Millisecond)
	client, control := newTestEnv(t)
	seedDomains(t, control)
	control.PlanCreates("pf-l-1-0",
		fakecontrol.CreateOutcome{Code: codes.Unavailable, Msg: "graceful_stop", Stored: true},
		fakecontrol.CreateOutcome{Code: codes.ResourceExhausted, Msg: "atespace quota reached"},
	)
	probes, cov, err := warm(t, client, &ateenvv1alpha.WarmImageRequest{
		RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"}, Nodes: 1, ProbesPerNode: 1, MaxRounds: 1, RunId: "pf-l",
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	if len(probes) != 1 || cov.GetFailed() != 1 || !strings.Contains(cov.GetError(), "create: ") || !strings.Contains(cov.GetError(), "atespace quota reached") {
		t.Errorf("probes = %v, coverage = %v, want one probe refused for good", probes, cov)
	}
	if n := control.Deletes(); n != 3 {
		t.Errorf("%d deletes, want 3: the lost create may have landed and is rechecked", n)
	}
	if n := control.ActorCount(); n != 0 {
		t.Errorf("%d actors left, want 0: the landed probe is deleted", n)
	}
	if len(cov.GetUndeletedProbes()) != 0 {
		t.Errorf("undeleted = %v, want none", cov.GetUndeletedProbes())
	}
}

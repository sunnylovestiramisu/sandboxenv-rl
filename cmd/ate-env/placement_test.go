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
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

// placementStub answers ListPlacementDomains and WarmImage the way
// ate-env-api does, and keeps the last WarmImage request.
type placementStub struct {
	ateenvv1alpha.UnimplementedEnvironmentServiceServer
	mu    sync.Mutex
	warms []*ateenvv1alpha.WarmImageRequest
	lists []*ateenvv1alpha.ListPlacementDomainsRequest
}

// ListPlacementDomains answers two pools, or pool-d0 alone when a template
// is named (as ate-env-api narrows the list to the template's pools), and
// keeps the request.
func (s *placementStub) ListPlacementDomains(_ context.Context, req *ateenvv1alpha.ListPlacementDomainsRequest) (*ateenvv1alpha.ListPlacementDomainsResponse, error) {
	s.mu.Lock()
	s.lists = append(s.lists, req)
	s.mu.Unlock()
	domains := []*ateenvv1alpha.PlacementDomain{
		{Name: "pool-d0", WorkerNamespace: "ate-system", Selector: map[string]string{"workload": "default-env", "ate-env/domain": "d0"}, Nodes: 2, Workers: 2, NodeSlots: 12},
		{Name: "pool-d1", WorkerNamespace: "ate-system", Nodes: 1, Workers: 1},
	}
	if req.GetTemplate() != nil || req.GetAtespace() != "" {
		domains = domains[:1]
	}
	return &ateenvv1alpha.ListPlacementDomainsResponse{Domains: domains}, nil
}

func (s *placementStub) lastList() *ateenvv1alpha.ListPlacementDomainsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.lists) == 0 {
		return nil
	}
	return s.lists[len(s.lists)-1]
}

// WarmImage streams two probes and the coverage: complete, unless the run id
// is "short"; with the run id "leftover" the coverage waited for slots and
// names an undeleted probe.
func (s *placementStub) WarmImage(req *ateenvv1alpha.WarmImageRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.WarmImageEvent]) error {
	s.mu.Lock()
	s.warms = append(s.warms, req)
	s.mu.Unlock()
	for i, node := range []string{"node-a", "node-b"} {
		if err := stream.Send(&ateenvv1alpha.WarmImageEvent{Event: &ateenvv1alpha.WarmImageEvent_Probe{Probe: &ateenvv1alpha.WarmProbe{
			Id: req.GetRunId() + "-1-" + string(rune('0'+i)), Node: node, Round: 1, Duration: durationpb.New(1500 * time.Millisecond),
		}}}); err != nil {
			return err
		}
	}
	cov := &ateenvv1alpha.WarmImageCoverage{Wanted: 2, Nodes: []string{"node-a", "node-b"}, Probes: 2, Rounds: 1, Complete: true}
	if req.GetRunId() == "short" {
		cov.Nodes, cov.Complete, cov.Failed, cov.Error = []string{"node-a"}, false, 1, "resume: no room"
	}
	if req.GetRunId() == "leftover" {
		cov.UndeletedProbes, cov.Waited = []string{"leftover-1-1"}, durationpb.New(2500*time.Millisecond)
	}
	return stream.Send(&ateenvv1alpha.WarmImageEvent{Event: &ateenvv1alpha.WarmImageEvent_Coverage{Coverage: cov}})
}

func (s *placementStub) lastWarm() *ateenvv1alpha.WarmImageRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.warms) == 0 {
		return nil
	}
	return s.warms[len(s.warms)-1]
}

func servePlacementStub(t *testing.T) (*placementStub, string) {
	t.Helper()
	stub := &placementStub{}
	grpcServer := grpc.NewServer()
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, stub)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)
	return stub, lis.Addr().String()
}

// runCLI runs ate-env with args and returns its stdout and error.
func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := newRootCommand(args)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestPlacementDomainsCommand(t *testing.T) {
	_, addr := servePlacementStub(t)
	out, err := runCLI(t, "placement-domains", "--api", addr)
	if err != nil {
		t.Fatalf("placement-domains: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "NAME") {
		t.Fatalf("output = %q, want a header and two rows", out)
	}
	if f := strings.Fields(lines[1]); len(f) != 6 || f[0] != "pool-d0" || f[2] != "2" || f[4] != "12" || f[5] != "ate-env/domain=d0,workload=default-env" {
		t.Errorf("pool-d0 row = %q, want name, namespace, 2 nodes, 2 workers, 12 slots and the sorted selector", lines[1])
	}
	if f := strings.Fields(lines[2]); len(f) != 6 || f[4] != "-" || f[5] != "-" {
		t.Errorf("pool-d1 row = %q, want - for unknown slots and no selector", lines[2])
	}

	out, err = runCLI(t, "placement-domains", "--api", addr, "--json")
	if err != nil {
		t.Fatalf("placement-domains --json: %v", err)
	}
	var resp ateenvv1alpha.ListPlacementDomainsResponse
	if err := protojson.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("output is not a ListPlacementDomainsResponse: %v\n%s", err, out)
	}
	if len(resp.GetDomains()) != 2 || resp.GetDomains()[0].GetNodeSlots() != 12 {
		t.Errorf("domains = %v, want the stub's two", resp.GetDomains())
	}
}

// --template, --template-atespace and --atespace reach the request, and the
// list narrows to the template's pools; without them the request is empty.
func TestPlacementDomainsCommandTemplate(t *testing.T) {
	stub, addr := servePlacementStub(t)
	out, err := runCLI(t, "placement-domains", "--api", addr, "--template", "d0-template", "--template-atespace", "ate-env", "--atespace", "ate-env")
	if err != nil {
		t.Fatalf("placement-domains --template: %v\n%s", err, out)
	}
	if lines := strings.Split(strings.TrimSpace(out), "\n"); len(lines) != 2 || !strings.HasPrefix(lines[1], "pool-d0") {
		t.Errorf("output = %q, want a header and pool-d0 only", out)
	}
	req := stub.lastList()
	if req.GetTemplate().GetName() != "d0-template" || req.GetTemplate().GetAtespace() != "ate-env" || req.GetAtespace() != "ate-env" {
		t.Errorf("request = %v, want template d0-template in ate-env and atespace ate-env", req)
	}
	if _, err := runCLI(t, "placement-domains", "--api", addr); err != nil {
		t.Fatal(err)
	}
	if req := stub.lastList(); req.GetTemplate() != nil || req.GetAtespace() != "" {
		t.Errorf("request without flags = %v, want no template and no atespace", req)
	}
}

func TestWarmImageCommand(t *testing.T) {
	stub, addr := servePlacementStub(t)
	out, err := runCLI(t, "warm-image", "example.com/task@sha256:abc", "--api", addr,
		"--worker-selector", "ate-env/domain=d0", "--nodes", "2", "--probe-timeout", "30s", "--run-id", "pf-7", "--probes-per-node", "1")
	if err != nil {
		t.Fatalf("warm-image: %v\n%s", err, out)
	}
	for _, want := range []string{
		"probe pf-7-1-0 round 1 on node-a in 1.5s\n",
		"probe pf-7-1-1 round 1 on node-b in 1.5s\n",
		"coverage: 2/2 nodes after 1 rounds, 2 probes, 0 failed\n",
		"nodes: node-a, node-b\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	req := stub.lastWarm()
	if req.GetRootfsImage() != "example.com/task@sha256:abc" || req.GetWorkerSelector()["ate-env/domain"] != "d0" || req.GetNodes() != 2 ||
		req.GetProbeTimeout().AsDuration() != 30*time.Second || req.GetRunId() != "pf-7" || req.GetProbesPerNode() != 1 {
		t.Errorf("request = %v, want the flags passed through", req)
	}
}

// --json prints every event the stream delivers, one JSON line each: the
// probes, then the coverage.
func TestWarmImageCommandJSON(t *testing.T) {
	_, addr := servePlacementStub(t)
	out, err := runCLI(t, "warm-image", "example.com/task@sha256:abc", "--api", addr, "--nodes", "2", "--json")
	if err != nil {
		t.Fatalf("warm-image --json: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 2 probes and the coverage:\n%s", len(lines), out)
	}
	for i, line := range lines {
		var ev ateenvv1alpha.WarmImageEvent
		if err := protojson.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatalf("line %d is not a WarmImageEvent: %v\n%s", i, err, line)
		}
		switch i {
		case 0, 1:
			if ev.GetProbe().GetNode() == "" {
				t.Errorf("line %d = %v, want a probe with its node", i, &ev)
			}
		case 2:
			if !ev.GetCoverage().GetComplete() || len(ev.GetCoverage().GetNodes()) != 2 {
				t.Errorf("last line = %v, want the complete coverage", &ev)
			}
		}
	}
}

func TestWarmImageCommandFailsOnShortCoverageAndBadTimeout(t *testing.T) {
	stub, addr := servePlacementStub(t)
	out, err := runCLI(t, "warm-image", "example.com/task@sha256:abc", "--api", addr, "--nodes", "2", "--run-id", "short")
	if err == nil || !strings.Contains(err.Error(), "coverage short") {
		t.Errorf("short coverage: err = %v, want a non-zero exit naming it\n%s", err, out)
	}
	if !strings.Contains(out, "coverage: 1/2 nodes after 1 rounds, 2 probes, 1 failed: resume: no room\n") {
		t.Errorf("short coverage output:\n%s", out)
	}
	// A complete coverage that left a probe behind still fails, naming the
	// probe to sweep, and reports the wait for slots.
	out, err = runCLI(t, "warm-image", "example.com/task@sha256:abc", "--api", addr, "--nodes", "2", "--run-id", "leftover")
	if err == nil || !strings.Contains(err.Error(), "1 probes left behind, sweep them: leftover-1-1") {
		t.Errorf("leftover: err = %v, want a non-zero exit naming the probe\n%s", err, out)
	}
	for _, want := range []string{
		"coverage: 2/2 nodes after 1 rounds, 2 probes, 0 failed\n",
		"waited 2.5s for probe slots held by other warm calls\n",
		"undeleted probes (sweep with ate-env delete): leftover-1-1\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("leftover output missing %q:\n%s", want, out)
		}
	}
	out, err = runCLI(t, "warm-image", "example.com/task@sha256:abc", "--api", addr, "--nodes", "2", "--run-id", "leftover", "--json")
	if err == nil {
		t.Error("leftover --json: want a non-zero exit")
	}
	if last := strings.Split(strings.TrimSpace(out), "\n"); !strings.Contains(last[len(last)-1], `"undeleted_probes":["leftover-1-1"]`) || !strings.Contains(last[len(last)-1], `"waited":"2.500s"`) {
		t.Errorf("leftover --json coverage line = %q, want undeleted_probes and waited", last[len(last)-1])
	}
	// A zero or negative timeout is refused before the request is sent; zero
	// would otherwise leave the request's probe_timeout empty and look like
	// the server's default.
	for _, timeout := range []string{"-1s", "0s", "0"} {
		before := len(stub.warms)
		_, err = runCLI(t, "warm-image", "example.com/task@sha256:abc", "--api", addr, "--nodes", "2", "--probe-timeout", timeout)
		if err == nil || !strings.Contains(err.Error(), "--probe-timeout must be positive") {
			t.Errorf("--probe-timeout %s: err = %v, want a refusal", timeout, err)
		}
		if len(stub.warms) != before {
			t.Errorf("--probe-timeout %s reached the server", timeout)
		}
	}
}

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
	"fmt"
	"io"
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
	"pgregory.net/rapid"
)

// A create attempt of a probe, as a control plane under load answers it.
type createStep int

const (
	// createRefused: Unavailable, nothing stored; the probe retries.
	createRefused createStep = iota
	// createLost: the actor is stored and the reply is lost (Unavailable or
	// DeadlineExceeded); the retry finds it AlreadyExists, or is refused.
	createLost
	// createFinal: ResourceExhausted; the probe fails at once.
	createFinal
)

// TestPropWarmImageLeavesNoProbe states the rule for any run of WarmImage:
// once the handler returns, every probe actor still in the control plane is
// listed in undeleted_probes and is CRASHED (the one state a delete is
// refused in), every probe slot is released, and the coverage matches the
// streamed probes. rapid draws the nodes, the probes per node, the rounds,
// each probe's create attempts (refused, stored but lost, refused for good),
// transient delete failures, crashed resumes and a cancel after the first
// probe, then shrinks a failure to the smallest run. The run that found the
// created flag ignoring an earlier cut-off attempt is pinned as
// TestWarmImageDeletesAProbeWhoseLostCreateLanded.
func TestPropWarmImageLeavesNoProbe(t *testing.T) {
	apiservice.SetWarmTimings(t, time.Millisecond, 5*time.Millisecond, 500*time.Millisecond)
	steps := []createStep{createRefused, createLost, createFinal}
	lostCodes := []codes.Code{codes.Unavailable, codes.DeadlineExceeded}
	deleteCodes := []codes.Code{codes.Aborted, codes.Unavailable, codes.ResourceExhausted}
	rapid.Check(t, func(rt *rapid.T) {
		te := newFullTestEnv(t)
		client, control := te.envClient, te.control
		seedDomains(t, control)
		control.KeepCrashed = true
		nodes := rapid.IntRange(1, 2).Draw(rt, "nodes")
		ppn := rapid.IntRange(1, 2).Draw(rt, "probes_per_node")
		rounds := rapid.IntRange(1, 2).Draw(rt, "max_rounds")
		width := nodes * ppn
		const run = "pp"
		for round := 1; round <= rounds; round++ {
			for k := range width {
				name := fmt.Sprintf("%s-%d-%d", run, round, k)
				var plan []fakecontrol.CreateOutcome
				for i, st := range rapid.SliceOfN(rapid.SampledFrom(steps), 0, 2).Draw(rt, name) {
					switch st {
					case createRefused:
						plan = append(plan, fakecontrol.CreateOutcome{Code: codes.Unavailable, Msg: "graceful_stop"})
					case createLost:
						code := rapid.SampledFrom(lostCodes).Draw(rt, fmt.Sprintf("%s-lost%d", name, i))
						plan = append(plan, fakecontrol.CreateOutcome{Code: code, Msg: "cut off", Stored: true})
					case createFinal:
						plan = append(plan, fakecontrol.CreateOutcome{Code: codes.ResourceExhausted, Msg: "atespace quota reached"})
					}
					if st == createFinal {
						break // nothing follows a final refusal
					}
				}
				control.PlanCreates(name, plan...)
			}
		}
		cancel := rapid.Bool().Draw(rt, "cancel")
		if !cancel {
			// Transient only, so a probe left behind is a crashed one.
			if n := rapid.IntRange(0, 3).Draw(rt, "delete_failures"); n > 0 {
				control.FailDeletes(n, rapid.SampledFrom(deleteCodes).Draw(rt, "delete_code"))
			}
		}
		if n := rapid.IntRange(0, 2).Draw(rt, "crashed_resumes"); n > 0 {
			control.CrashResumes(n, "restore failed")
		}

		ctx, stop := context.WithCancel(context.Background())
		defer stop()
		stream, err := client.WarmImage(ctx, &ateenvv1alpha.WarmImageRequest{
			RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"},
			Nodes: int32(nodes), ProbesPerNode: int32(ppn), MaxRounds: int32(rounds), RunId: run,
			ProbeTimeout: durationpb.New(300 * time.Millisecond),
		})
		if err != nil {
			rt.Fatalf("WarmImage: %v", err)
		}
		var probes []*ateenvv1alpha.WarmProbe
		var cov *ateenvv1alpha.WarmImageCoverage
		for {
			ev, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				if cancel && status.Code(err) == codes.Canceled {
					break
				}
				rt.Fatalf("Recv: %v", err)
			}
			switch e := ev.GetEvent().(type) {
			case *ateenvv1alpha.WarmImageEvent_Probe:
				probes = append(probes, e.Probe)
				if cancel && len(probes) == 1 {
					stop()
				}
			case *ateenvv1alpha.WarmImageEvent_Coverage:
				cov = e.Coverage
			}
		}
		// The handler finishes its round after the stream ended: its deletes
		// are done once the round's slots are given back.
		deadline := time.Now().Add(5 * time.Second)
		for te.srv.WarmProbesInFlight() > 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if n := te.srv.WarmProbesInFlight(); n != 0 {
			rt.Fatalf("%d probe slots held after the call, want 0", n)
		}
		left := control.ActorNames()
		slices.Sort(left)
		for _, name := range left {
			if !strings.HasPrefix(name, run+"-") {
				rt.Fatalf("actor %q left is not a probe of this run", name)
			}
			if st := control.Status(name); st != ateapipb.ActorState_ACTOR_STATE_CRASHED {
				rt.Fatalf("probe %s left in state %s: not deleted", name, st)
			}
		}
		if cancel {
			return
		}
		if cov == nil {
			rt.Fatalf("no coverage after %d probes", len(probes))
		}
		if !slices.Equal(cov.GetUndeletedProbes(), left) {
			rt.Fatalf("undeleted_probes = %v, actors left = %v", cov.GetUndeletedProbes(), left)
		}
		failed := 0
		seen := map[string]bool{}
		for _, p := range probes {
			if p.GetError() != "" {
				failed++
				continue
			}
			seen[p.GetNode()] = true
		}
		want := make([]string, 0, len(seen))
		for n := range seen {
			want = append(want, n)
		}
		slices.Sort(want)
		if int(cov.GetProbes()) != len(probes) || int(cov.GetFailed()) != failed || !slices.Equal(cov.GetNodes(), want) || cov.GetComplete() != (len(want) >= nodes) {
			rt.Fatalf("coverage = %v, want probes=%d failed=%d nodes=%v complete=%v", cov, len(probes), failed, want, len(want) >= nodes)
		}
	})
}

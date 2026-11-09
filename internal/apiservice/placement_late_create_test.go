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
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// lateCreateControl holds the CreateActor of probe `late` until the probe's
// first DeleteActor has returned, then commits it: a create whose caller gave
// up (its attempt deadline passed) and that the control plane commits after
// the caller's cleanup already found nothing to delete. A control plane
// whose database queues does this.
//
// With retried, only the first attempt is held, and the caller sees it fail
// Unavailable at once (an api server restarting while the create is queued);
// the next attempt is confirmed and makes the probe, which then runs and is
// deleted before the held attempt commits. With failList, ListActors fails.
type lateCreateControl struct {
	*fakecontrol.Server
	late       string
	retried    bool
	failList   bool
	deleted    chan struct{}
	landed     chan struct{}
	once       sync.Once
	landedOnce sync.Once
	mu         sync.Mutex
	attempts   int
}

func newLateCreateControl() *lateCreateControl {
	return &lateCreateControl{Server: fakecontrol.New(), late: "lc-1-0", deleted: make(chan struct{}), landed: make(chan struct{})}
}

func (c *lateCreateControl) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	if req.GetActor().GetMetadata().GetName() != c.late {
		return c.Server.CreateActor(ctx, req)
	}
	if c.retried {
		c.mu.Lock()
		c.attempts++
		first := c.attempts == 1
		c.mu.Unlock()
		if !first {
			return c.Server.CreateActor(ctx, req)
		}
		go c.commitAfterDelete(req)
		return nil, status.Error(codes.Unavailable, "graceful_stop")
	}
	return c.commitAfterDelete(req)
}

// commitAfterDelete commits req once the probe's first delete has returned.
func (c *lateCreateControl) commitAfterDelete(req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	select {
	case <-c.deleted:
	case <-time.After(10 * time.Second):
	}
	a, err := c.Server.CreateActor(context.Background(), req)
	c.landedOnce.Do(func() { close(c.landed) })
	return a, err
}

func (c *lateCreateControl) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	if c.failList {
		return nil, status.Error(codes.Unavailable, "graceful_stop")
	}
	select {
	case <-c.landed:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return c.Server.ListActors(ctx, req)
}

func (c *lateCreateControl) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	a, err := c.Server.DeleteActor(ctx, req)
	if req.GetActor().GetName() == c.late {
		c.once.Do(func() { close(c.deleted) })
	}
	return a, err
}

// warmLateCreate runs one WarmImage call of one probe, lc-1-0, on control,
// whose create the control plane commits only after the probe's first
// delete. Unless control.retried, the probe's budget (probeTimeout) ends
// while the create is held, so the create attempt is cut off and the call
// deletes the probe: NotFound, the create has not landed.
func warmLateCreate(t *testing.T, control *lateCreateControl, probeTimeout time.Duration) *ateenvv1alpha.WarmImageCoverage {
	t.Helper()
	apiservice.SetWarmTimings(t, time.Millisecond, 4*time.Millisecond, 2*time.Second)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cs := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{propCert(t)}})))
	ateapipb.RegisterControlServer(cs, control)
	go cs.Serve(lis)
	t.Cleanup(cs.Stop)
	client, err := ate.New(ate.Options{ControlAddr: lis.Addr().String(), SkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	srv := apiservice.New(client, "127.0.0.1:1", "")
	t.Cleanup(srv.Close)
	es := grpc.NewServer()
	ateenvv1alpha.RegisterEnvironmentServiceServer(es, srv)
	elis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go es.Serve(elis)
	t.Cleanup(es.Stop)
	conn, err := grpc.NewClient(elis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	control.BornSuspended = true
	control.AddWorker(fakecontrol.Worker("ate-system", "pool-d0", "node-0", d0Labels, 16))
	seedDerivedTemplate(t, control.Server, &ateapipb.GoldenSnapshotStatus{GoldenTag: &ateapipb.ObjectRef{Name: "golden"}}, 0)
	_, cov, err := warm(t, ateenvv1alpha.NewEnvironmentServiceClient(conn), &ateenvv1alpha.WarmImageRequest{
		RootfsImage: testRootfsImage, WorkerSelector: map[string]string{"ate-env/domain": "d0"},
		Nodes: 1, ProbesPerNode: 1, MaxRounds: 1, RunId: "lc", ProbeTimeout: durationpb.New(probeTimeout),
	})
	if err != nil {
		t.Fatalf("WarmImage: %v", err)
	}
	select {
	case <-control.landed:
	case <-time.After(10 * time.Second):
		t.Fatal("the held create never landed")
	}
	return cov
}

// A create that lands after its probe's delete found nothing, within the
// settle window: the repeated delete finds it and deletes it. Found by
// TestPropWarmImageLeavesNoProbe as a CI flake (a probe left SUSPENDED).
func TestWarmImageDeletesAProbeWhoseCreateLandsAfterItsDelete(t *testing.T) {
	control := newLateCreateControl()
	cov := warmLateCreate(t, control, 200*time.Millisecond)
	if st := control.Status("lc-1-0"); st != ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED {
		t.Errorf("probe lc-1-0 left in state %s, undeleted_probes %v: nothing will delete it", st, cov.GetUndeletedProbes())
	}
}

// With no settle window, the sweep before the round gives its slots back finds the probe
// whose create landed after its delete, and deletes it.
func TestWarmImageSweepsAProbeWhoseCreateLandsAfterTheSettleWindow(t *testing.T) {
	apiservice.SetWarmLateCreateSettle(t, 0)
	control := newLateCreateControl()
	cov := warmLateCreate(t, control, 200*time.Millisecond)
	if st := control.Status("lc-1-0"); st != ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED {
		t.Errorf("probe lc-1-0 left in state %s, undeleted_probes %v: the sweep missed it", st, cov.GetUndeletedProbes())
	}
}

// A create attempt that failed Unavailable while the control plane still
// queued it, then a confirmed attempt: the probe runs and its delete
// succeeds, and the first attempt commits after that delete. The probe is
// uncertain all the same, so the delete's re-check finds it and deletes it.
// The list fails, so the sweep cannot be what deletes it.
func TestWarmImageDeletesAProbeWhoseCutAttemptLandsAfterAConfirmedOne(t *testing.T) {
	control := newLateCreateControl()
	control.retried = true
	control.failList = true
	cov := warmLateCreate(t, control, 3*time.Second)
	if st := control.Status("lc-1-0"); st != ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED {
		t.Errorf("probe lc-1-0 left in state %s, undeleted_probes %v: nothing will delete it", st, cov.GetUndeletedProbes())
	}
	// With failList the sweep lists lc-1-0 although the re-check deleted it.
	// This misreport is accepted because the sweep cannot tell whether it landed.
	if got := cov.GetUndeletedProbes(); len(got) != 1 || got[0] != "lc-1-0" {
		t.Errorf("undeleted_probes = %v, want [lc-1-0]", got)
	}
}

// With no settle window and a list that fails, the call cannot tell whether
// the late create landed: the probe is listed in undeleted_probes for the
// caller to sweep, not left unreported.
func TestWarmImageListsUncertainProbesWhenTheSweepCannotList(t *testing.T) {
	apiservice.SetWarmLateCreateSettle(t, 0)
	control := newLateCreateControl()
	control.failList = true
	cov := warmLateCreate(t, control, 200*time.Millisecond)
	if got := cov.GetUndeletedProbes(); len(got) != 1 || got[0] != "lc-1-0" {
		t.Errorf("undeleted_probes = %v, want [lc-1-0] (probe in state %s)", got, control.Status("lc-1-0"))
	}
}

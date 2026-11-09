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
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// deleteHookControl runs beforeDelete once, before a DeleteActorTemplate is
// applied, i.e. after the deleting replica's in-use check (List).
type deleteHookControl struct {
	*fakecontrol.Server
	mu           sync.Mutex
	beforeDelete func(req *ateapipb.DeleteActorTemplateRequest)
}

func (h *deleteHookControl) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	h.mu.Lock()
	f := h.beforeDelete
	h.beforeDelete = nil
	h.mu.Unlock()
	if f != nil {
		f(req)
	}
	return h.Server.DeleteActorTemplate(ctx, req)
}

// A known limitation of AE-5 (docs/plans/ate-env-api/acceptance-criteria.md,
// "Known limitations"), reproduced with two ate-env-api replicas on one
// control plane. Replica B kept the failed template earlier (an actor used it
// then); that actor is gone. Replica A finds the template due and lists it
// (no users); before A's delete applies, B creates an actor on the failed
// template. A then recreates the template under B's RUNNING actor, because
// its in-use check and its delete are serialized by an in-process lock only.
// The test is skipped until a cross-replica guard exists; it then must pass.
func TestRecreateAcrossReplicasSparesActorCreatedAfterInUseCheck(t *testing.T) {
	t.Skip("known limitation: AE-5 holds per replica; the in-use check and the delete are not serialized across replicas (https://github.com/ai-on-gke/sandboxenv-rl/issues/273)")
	cert := propCert(t)
	control := fakecontrol.New()
	hooks := &deleteHookControl{Server: control}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})))
	ateapipb.RegisterControlServer(gs, hooks)
	go gs.Serve(lis)
	defer gs.Stop()
	newReplica := func() *apiservice.Server {
		c, err := ate.New(ate.Options{ControlAddr: lis.Addr().String(), SkipVerify: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return apiservice.New(c, "127.0.0.1:1", "")
	}
	replicaA, replicaB := newReplica(), newReplica()

	seedGuestTemplate(control, "ate-env", "default-template")
	derived, err := apiservice.DeriveImageTemplate(control.Template("ate-env", "default-template"), testDerivedTemplate, testRootfsImage)
	if err != nil {
		t.Fatal(err)
	}
	derived.Metadata.Uid = "seeded"
	derived.Metadata.UpdateTime = timestamppb.New(time.Now().Add(-time.Hour))
	derived.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: failedGolden()}
	control.AddTemplate(derived)

	ctx := context.Background()
	create := func(srv *apiservice.Server, id string) error {
		_, err := srv.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: id, RootfsImage: testRootfsImage})
		return err
	}

	// An actor uses the failed template; replica B's create sees it and
	// keeps the template.
	if _, err := replicaB.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{
		Id: "old", Template: &ateenvv1alpha.Template{Name: testDerivedTemplate},
	}); err != nil {
		t.Fatal(err)
	}
	control.SetState("old", ateapipb.ActorState_ACTOR_STATE_RUNNING)
	if err := create(replicaB, "b-1"); err != nil {
		t.Fatal(err)
	}
	if d, _ := control.TemplateCalls(); d != 0 {
		t.Fatalf("replica B deleted the template while it was in use (deletes=%d)", d)
	}
	// The users go away.
	for _, id := range []string{"old", "b-1"} {
		if _, err := control.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ate-env", Name: id}}); err != nil {
			t.Fatal(err)
		}
	}
	control.RequireTemplate = true

	// Between replica A's in-use check and its delete, replica B creates
	// an actor on the failed template.
	// The hook runs in the gRPC handler goroutine; the channel orders its
	// writes before the test goroutine's reads.
	type victimResult struct {
		uid string
		err error
	}
	victim := make(chan victimResult, 1)
	hooks.mu.Lock()
	hooks.beforeDelete = func(req *ateapipb.DeleteActorTemplateRequest) {
		var r victimResult
		r.err = create(replicaB, "victim")
		if r.err == nil {
			control.SetState("victim", ateapipb.ActorState_ACTOR_STATE_RUNNING)
			r.uid = control.Template("ate-env", testDerivedTemplate).GetMetadata().GetUid()
		}
		victim <- r
	}
	hooks.mu.Unlock()
	if err := create(replicaA, "a-1"); err != nil {
		t.Fatalf("replica A create: %v", err)
	}
	var r victimResult
	select {
	case r = <-victim:
	default:
		t.Fatal("hook did not run: replica A never deleted the template")
	}
	if r.err != nil {
		t.Fatalf("replica B create in the window: %v", r.err)
	}
	victimOnUID := r.uid
	now := control.Template("ate-env", testDerivedTemplate).GetMetadata().GetUid()
	if victimOnUID == "seeded" && now != "seeded" {
		t.Errorf("template %q was recreated (uid seeded -> %s) while RUNNING actor \"victim\", created by replica B on uid seeded after replica A's in-use check, used it; AE-5: recreate only when no actor a recreate harms uses it", testDerivedTemplate, now)
	}
}

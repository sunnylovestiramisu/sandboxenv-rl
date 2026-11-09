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
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/internaltest/testcerts"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// hookControl is the fake control plane with a hook that runs before a
// template create is applied, to steer the interleaving of concurrent creates.
type hookControl struct {
	*fakecontrol.Server
	beforeCreate func(name, image string)
}

func (h *hookControl) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if h.beforeCreate != nil {
		t := req.GetActorTemplate()
		h.beforeCreate(t.GetMetadata().GetName(), t.GetContainers()[0].GetImage())
	}
	return h.Server.CreateActorTemplate(ctx, req)
}

type propEnv struct {
	control *fakecontrol.Server
	hooks   *hookControl
	srv     *apiservice.Server
	stop    func()
}

// newPropEnv serves a hookControl over TLS and returns an apiservice.Server
// on it. cert is shared by the iterations of a property.
func newPropEnv(cert tls.Certificate) (*propEnv, error) {
	control := fakecontrol.New()
	hooks := &hookControl{Server: control}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})))
	ateapipb.RegisterControlServer(gs, hooks)
	go gs.Serve(lis)
	client, err := ate.New(ate.Options{ControlAddr: lis.Addr().String(), SkipVerify: true})
	if err != nil {
		gs.Stop()
		return nil, err
	}
	srv := apiservice.New(client, "127.0.0.1:1", "")
	return &propEnv{control: control, hooks: hooks, srv: srv, stop: func() { client.Close(); gs.Stop() }}, nil
}

func propCert(t *testing.T) tls.Certificate {
	return testcerts.NewAuthority(t, "prop").Server(t, "localhost")
}

func digestOf(image string) string {
	if i := strings.LastIndex(image, "@sha256:"); i >= 0 {
		return image[i+len("@sha256:"):]
	}
	return ""
}

var propRepos = []string{"docker.io/library/python", "example.com/mirror/python", "gcr.io/x/y/z", "localhost:5000/swe/task-1"}

// AE-1 (docs/plans/ate-env-api/acceptance-criteria.md). Rule (CreateEnvironment doc; docs/architecture.md "If an existing derived
// template exists with the same name but runs a different rootfs image,
// CreateEnvironment fails with codes.FailedPrecondition"; threat model: "A
// template with that name that runs another image is refused with
// FailedPrecondition"): a successful CreateEnvironment with rootfs_image X
// creates an actor whose template runs X's digest, whatever other creates
// run at the same time. Concurrent creates on one digest (any repository
// path) all succeed on a single template.
//
// The generator draws digests that share their first 12 hex digits (thus the
// derived name) but may differ after, and a delay schedule for the template
// RPCs, i.e. an interleaving.
func TestPropConcurrentCreatesUseRequestedImage(t *testing.T) {
	cert := propCert(t)
	rapid.Check(t, func(rt *rapid.T) {
		prefix := rapid.StringOfN(rapid.SampledFrom([]rune("0123456789abcdef")), 12, 12, -1).Draw(rt, "prefix")
		suffixes := []string{strings.Repeat("0", 52), strings.Repeat("f", 52)}
		n := rapid.IntRange(2, 5).Draw(rt, "callers")
		type caller struct {
			image                 string
			getDelay, createDelay time.Duration
		}
		callers := make([]caller, n)
		for i := range callers {
			d := prefix + rapid.SampledFrom(suffixes).Draw(rt, fmt.Sprintf("suffix%d", i))
			callers[i] = caller{
				image:       rapid.SampledFrom(propRepos).Draw(rt, fmt.Sprintf("repo%d", i)) + "@sha256:" + d,
				getDelay:    time.Duration(rapid.IntRange(0, 4).Draw(rt, fmt.Sprintf("get%d", i))) * time.Millisecond,
				createDelay: time.Duration(rapid.IntRange(0, 4).Draw(rt, fmt.Sprintf("create%d", i))) * time.Millisecond,
			}
		}

		env, err := newPropEnv(cert)
		if err != nil {
			rt.Fatal(err)
		}
		defer env.stop()
		seedGuestTemplate(env.control, "ate-env", "default-template")
		derived := "default-template-" + prefix
		// Delay each caller's template RPCs per its schedule; the
		// caller is identified by its image (several callers may share
		// one, then they share a schedule, which is fine).
		delays := map[string]caller{}
		for _, c := range callers {
			delays[c.image] = c
		}
		var mu sync.Mutex
		env.hooks.beforeCreate = func(name, image string) {
			mu.Lock()
			d := delays[image].createDelay
			mu.Unlock()
			time.Sleep(d)
		}

		errs := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, c := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				time.Sleep(c.getDelay)
				_, errs[i] = env.srv.CreateEnvironment(context.Background(), &ateenvv1alpha.CreateEnvironmentRequest{
					Id: fmt.Sprintf("env-%d", i), RootfsImage: c.image,
				})
			}()
		}
		close(start)
		wg.Wait()

		tmpl := env.control.Template("ate-env", derived)
		if tmpl == nil {
			rt.Fatalf("no derived template %q", derived)
		}
		if env.control.TemplateCount() != 2 {
			rt.Fatalf("template count = %d, want base + one derived", env.control.TemplateCount())
		}
		running := digestOf(tmpl.GetContainers()[0].GetImage())
		for i, c := range callers {
			want := digestOf(c.image)
			switch {
			case errs[i] == nil && want != running:
				rt.Fatalf("env-%d asked for digest %s…%s, succeeded, but its template %q runs %s…%s",
					i, want[:12], want[60:], derived, running[:12], running[60:])
			case errs[i] != nil && want == running:
				rt.Fatalf("env-%d on the template's own digest failed: %v", i, errs[i])
			case errs[i] != nil && status.Code(errs[i]) != codes.FailedPrecondition:
				rt.Fatalf("env-%d: %v, want FailedPrecondition", i, errs[i])
			}
		}
	})
}

// The shrunk counterexample of TestPropConcurrentCreatesUseRequestedImage,
// pinned: two creates on digests that share the first 12 hex digits. A reads
// the derived name (absent) and derives its template; B does the same and
// creates its template first; A's create gets AlreadyExists. A must then get
// FailedPrecondition, as it would sequentially, not go on with B's template.
func TestConcurrentCreateOnCollidingDigestFailsPrecondition(t *testing.T) {
	env, err := newPropEnv(propCert(t))
	if err != nil {
		t.Fatal(err)
	}
	defer env.stop()
	seedGuestTemplate(env.control, "ate-env", "default-template")
	const prefix = "0123456789ab"
	imageA := "example.com/a@sha256:" + prefix + strings.Repeat("a", 52)
	imageB := "example.com/b@sha256:" + prefix + strings.Repeat("b", 52)

	aAtCreate := make(chan struct{})
	bDone := make(chan struct{})
	env.hooks.beforeCreate = func(name, image string) {
		if image == imageA {
			close(aAtCreate)
			<-bDone // B's create lands first
		}
	}

	var errA error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errA = env.srv.CreateEnvironment(context.Background(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "a", RootfsImage: imageA})
	}()
	<-aAtCreate
	if _, err := env.srv.CreateEnvironment(context.Background(), &ateenvv1alpha.CreateEnvironmentRequest{Id: "b", RootfsImage: imageB}); err != nil {
		t.Fatalf("create b: %v", err)
	}
	close(bDone)
	wg.Wait()

	got := env.control.Template("ate-env", "default-template-"+prefix).GetContainers()[0].GetImage()
	if errA == nil {
		t.Errorf("CreateEnvironment(rootfs_image=%s) succeeded, but its actor's template runs %s; want FailedPrecondition", imageA, got)
	} else if status.Code(errA) != codes.FailedPrecondition {
		t.Errorf("create a: %v, want FailedPrecondition", errA)
	}
}

// AE-5 (docs/plans/ate-env-api/acceptance-criteria.md). Rule (recreateFailedImageTemplate and imageTemplateLock docs): concurrent
// creates in one process (one replica; see "Known limitations" in that file)
// on a derived template whose golden snapshot failed long enough ago all
// succeed; the template is recreated at most once (one delete, one create)
// and only when no actor that a recreate harms uses it; afterwards
// exactly one derived template exists, and every actor the creates made
// references it (RequireTemplate: no actor is created while it is absent).
func TestPropConcurrentRecreate(t *testing.T) {
	cert := propCert(t)
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 6).Draw(rt, "callers")
		failedAgo := time.Duration(rapid.SampledFrom([]int{0, 1, 9, 11, 60}).Draw(rt, "failedMinutesAgo")) * time.Minute
		failed := rapid.Bool().Draw(rt, "failed")
		// A pre-existing actor on the template, with a state and maybe a
		// snapshot under the failed template's uid.
		type user struct {
			state    ateapipb.ActorState
			snapshot bool
			golden   bool
		}
		nUsers := rapid.IntRange(0, 2).Draw(rt, "users")
		users := make([]user, nUsers)
		for i := range users {
			users[i] = user{
				state: rapid.SampledFrom([]ateapipb.ActorState{
					ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_CRASHED,
				}).Draw(rt, fmt.Sprintf("state%d", i)),
				snapshot: rapid.Bool().Draw(rt, fmt.Sprintf("snap%d", i)),
				golden:   rapid.Bool().Draw(rt, fmt.Sprintf("golden%d", i)),
			}
		}
		repos := make([]string, n)
		delays := make([]time.Duration, n)
		for i := range repos {
			repos[i] = rapid.SampledFrom(propRepos).Draw(rt, fmt.Sprintf("repo%d", i))
			delays[i] = time.Duration(rapid.IntRange(0, 3).Draw(rt, fmt.Sprintf("delay%d", i))) * time.Millisecond
		}

		env, err := newPropEnv(cert)
		if err != nil {
			rt.Fatal(err)
		}
		defer env.stop()
		seedGuestTemplate(env.control, "ate-env", "default-template")
		derivedTmpl, err := apiservice.DeriveImageTemplate(env.control.Template("ate-env", "default-template"), testDerivedTemplate, testRootfsImage)
		if err != nil {
			rt.Fatal(err)
		}
		derivedTmpl.Metadata.Uid = "seeded"
		if failedAgo != 0 {
			derivedTmpl.Metadata.UpdateTime = timestamppb.New(time.Now().Add(-failedAgo))
		}
		if failed {
			derivedTmpl.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: failedGolden()}
		}
		env.control.AddTemplate(derivedTmpl)
		blocking := 0
		for i, u := range users {
			id := fmt.Sprintf("old-%d", i)
			if u.golden {
				id = "seeded"
				if i > 0 && users[0].golden {
					continue
				}
			}
			if _, err := env.srv.CreateEnvironment(context.Background(), &ateenvv1alpha.CreateEnvironmentRequest{
				Id: id, Template: &ateenvv1alpha.Template{Name: testDerivedTemplate},
			}); err != nil {
				rt.Fatal(err)
			}
			env.control.MutateActor(id, func(a *ateapipb.Actor) {
				a.Status = &ateapipb.ActorStatus{State: u.state}
				if u.snapshot {
					a.Status.Snapshots = []*ateapipb.Snapshot{{Uuid: "s", ActorTemplateUid: "seeded"}}
				}
			})
			if !u.golden && (u.state != ateapipb.ActorState_ACTOR_STATE_CRASHED || u.snapshot) {
				blocking++
			}
		}
		env.control.RequireTemplate = true
		_, createsBefore := env.control.TemplateCalls()

		errs := make([]error, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				time.Sleep(delays[i])
				_, errs[i] = env.srv.CreateEnvironment(context.Background(), &ateenvv1alpha.CreateEnvironmentRequest{
					Id: fmt.Sprintf("img-%d", i), RootfsImage: repos[i] + "@sha256:" + digestOf(testRootfsImage),
				})
			}()
		}
		close(start)
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				rt.Fatalf("img-%d: %v", i, err)
			}
		}
		tmpl := env.control.Template("ate-env", testDerivedTemplate)
		if tmpl == nil {
			rt.Fatal("derived template gone")
		}
		if env.control.TemplateCount() != 2 {
			rt.Fatalf("template count %d", env.control.TemplateCount())
		}
		deletes, creates := env.control.TemplateCalls()
		creates -= createsBefore
		due := failed && failedAgo >= 10*time.Minute
		wantRecreate := due && blocking == 0
		recreated := tmpl.GetMetadata().GetUid() != "seeded"
		if recreated != wantRecreate {
			rt.Fatalf("recreated=%v want %v (due=%v blocking=%d)", recreated, wantRecreate, due, blocking)
		}
		if deletes > 1 || creates > 1 || (wantRecreate && (deletes != 1 || creates != 1)) {
			rt.Fatalf("deletes=%d creates=%d", deletes, creates)
		}
	})
}

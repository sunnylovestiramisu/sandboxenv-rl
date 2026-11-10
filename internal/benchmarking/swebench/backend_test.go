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
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	env "github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/guest"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/internaltest/fakerouter"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeAPI is an in-process ate-env API: enough of the environment, process
// and file services for the backend's start, poll and read path.
type fakeAPI struct {
	ateenvv1alpha.UnimplementedEnvironmentServiceServer
	ateenvv1alpha.UnimplementedProcessServiceServer
	ateenvv1alpha.UnimplementedFileSystemServiceServer

	mu sync.Mutex
	// pollsUntilExit is how many GetProcess calls a process stays running;
	// unavailableStarts is how many StartProcess calls answer Unavailable
	// first.
	pollsUntilExit    int
	unavailableStarts int
	exitCode          int32
	stdout, stderr    string

	// deleteRateLimited is how many DeleteEnvironment calls answer a 429
	// first; deleteCalls counts every call.
	deleteRateLimited int
	deleteCalls       int

	nextID    int
	polls     map[string]int
	exited    map[string]bool
	killed    []string
	created   []string
	deleted   []string
	commands  []string
	suspended []string
	// suspendModes is the mode of each SuspendEnvironment call, in order.
	suspendModes []ateenvv1alpha.SuspendMode
	// features, when set, is what GetServerInfo reports; nil leaves it
	// Unimplemented, as an ate-env-api that predates the RPC answers.
	features []string

	// Placement, for the pre-flight: the nodes of each domain (the value of
	// the ate-env/domain selector label), handed out round-robin per create,
	// or always the first with sameNode; the node and selector per
	// environment; the derived templates and their golden state, which
	// turns READY after goldenReadyAfter status calls unless goldenFail
	// names the image.
	domainNodes      map[string][]string
	domainCounter    map[string]int
	sameNode         bool
	nodes            map[string]string
	selectors        map[string]map[string]string
	templates        map[string]bool
	statusCalls      map[string]int
	goldenReadyAfter int
	goldenFail       map[string]string
	goldenDisabled   bool
	// createFailAfter makes every CreateEnvironment after that many fail
	// with ResourceExhausted, as a full pool does.
	createFailAfter int
	// goldenPendingForever names images whose golden never gets ready.
	goldenPendingForever map[string]bool
	ensured              []string
	// images is the rootfs image per environment.
	images map[string]string
	// inflight and maxInflight count the environments alive per domain.
	inflight, maxInflight map[string]int
	onCreate              func(id string)
}

func (f *fakeAPI) GetServerInfo(context.Context, *ateenvv1alpha.GetServerInfoRequest) (*ateenvv1alpha.GetServerInfoResponse, error) {
	if f.features == nil {
		return nil, status.Error(codes.Unimplemented, "unknown method GetServerInfo")
	}
	return &ateenvv1alpha.GetServerInfoResponse{Server: "sandboxenv-rl", Version: "test", Features: f.features}, nil
}

func (f *fakeAPI) CreateEnvironment(_ context.Context, req *ateenvv1alpha.CreateEnvironmentRequest) (*ateenvv1alpha.CreateEnvironmentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createFailAfter > 0 && len(f.created) >= f.createFailAfter {
		return nil, status.Error(codes.ResourceExhausted, "no worker has room for the actor")
	}
	f.created = append(f.created, req.GetId())
	f.selectors[req.GetId()] = req.GetWorkerSelector()
	f.images[req.GetId()] = req.GetRootfsImage()
	domain := req.GetWorkerSelector()["ate-env/domain"]
	node := "node-any"
	if list := f.domainNodes[domain]; len(list) > 0 {
		idx := 0
		if !f.sameNode {
			key := domain + "\x00" + req.GetRootfsImage()
			idx = f.domainCounter[key] % len(list)
			f.domainCounter[key]++
		}
		node = list[idx]
	}
	f.nodes[req.GetId()] = node
	f.inflight[domain]++
	f.maxInflight[domain] = max(f.maxInflight[domain], f.inflight[domain])
	cb := f.onCreate
	if cb != nil {
		cb(req.GetId())
	}
	return &ateenvv1alpha.CreateEnvironmentResponse{Environment: &ateenvv1alpha.Environment{Id: req.GetId(), Atespace: req.GetAtespace(), WorkerSelector: req.GetWorkerSelector()}}, nil
}

func (f *fakeAPI) GetEnvironment(_ context.Context, req *ateenvv1alpha.GetEnvironmentRequest) (*ateenvv1alpha.GetEnvironmentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	node, ok := f.nodes[req.GetId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "environment %q not found", req.GetId())
	}
	return &ateenvv1alpha.GetEnvironmentResponse{Environment: &ateenvv1alpha.Environment{
		Id: req.GetId(), Atespace: req.GetAtespace(), Node: node, WorkerSelector: f.selectors[req.GetId()],
		Status: ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_RUNNING,
	}}, nil
}

func (f *fakeAPI) imageStatusLocked(image string) *ateenvv1alpha.ImageStatus {
	digest := image[strings.LastIndex(image, ":")+1:]
	st := &ateenvv1alpha.ImageStatus{
		RootfsImage: image,
		Template:    &ateenvv1alpha.Template{Name: "default-template-" + digest[:12], Atespace: "ate-env"},
		Golden:      ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_NONE,
	}
	if !f.templates[image] {
		return st
	}
	st.TemplateExists = true
	f.statusCalls[image]++
	switch {
	case f.goldenDisabled:
	case f.goldenPendingForever[image]:
		st.Golden = ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_PENDING
	case f.goldenFail[image] != "":
		st.Golden = ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_FAILED
		st.GoldenError = f.goldenFail[image]
	case f.statusCalls[image] > f.goldenReadyAfter:
		st.Golden = ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_READY
		st.GoldenTag = "golden-" + digest[:12]
	default:
		st.Golden = ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_PENDING
	}
	return st
}

func (f *fakeAPI) GetImageStatus(_ context.Context, req *ateenvv1alpha.GetImageStatusRequest) (*ateenvv1alpha.ImageStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.Contains(req.GetRootfsImage(), "@sha256:") {
		return nil, status.Error(codes.InvalidArgument, "image is not pinned by digest")
	}
	return f.imageStatusLocked(req.GetRootfsImage()), nil
}

func (f *fakeAPI) EnsureImageTemplate(_ context.Context, req *ateenvv1alpha.EnsureImageTemplateRequest) (*ateenvv1alpha.ImageStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.Contains(req.GetRootfsImage(), "@sha256:") {
		return nil, status.Error(codes.InvalidArgument, "image is not pinned by digest")
	}
	f.ensured = append(f.ensured, req.GetRootfsImage())
	f.templates[req.GetRootfsImage()] = true
	return f.imageStatusLocked(req.GetRootfsImage()), nil
}

func (f *fakeAPI) SuspendEnvironment(_ context.Context, req *ateenvv1alpha.SuspendEnvironmentRequest) (*ateenvv1alpha.SuspendEnvironmentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspended = append(f.suspended, req.GetId())
	f.suspendModes = append(f.suspendModes, req.GetMode())
	return &ateenvv1alpha.SuspendEnvironmentResponse{}, nil
}

func (f *fakeAPI) DeleteEnvironment(_ context.Context, req *ateenvv1alpha.DeleteEnvironmentRequest) (*ateenvv1alpha.DeleteEnvironmentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	if f.deleteRateLimited > 0 {
		f.deleteRateLimited--
		return nil, status.Error(codes.ResourceExhausted, "deleting snapshot: googleapi: Error 429: rateLimitExceeded")
	}
	f.deleted = append(f.deleted, req.GetId())
	f.inflight[f.selectors[req.GetId()]["ate-env/domain"]]--
	return &ateenvv1alpha.DeleteEnvironmentResponse{}, nil
}

func (f *fakeAPI) StartProcess(_ context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailableStarts > 0 {
		f.unavailableStarts--
		return nil, status.Error(codes.Unavailable, "router: wake timed out")
	}
	f.nextID++
	id := fmt.Sprintf("p%d", f.nextID)
	f.commands = append(f.commands, strings.Join(req.GetCommand(), " "))
	return &ateenvv1alpha.Process{ProcessId: id, State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}, nil
}

func (f *fakeAPI) GetProcess(_ context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls[req.GetProcessId()]++
	p := &ateenvv1alpha.Process{ProcessId: req.GetProcessId(), State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}
	if f.polls[req.GetProcessId()] > f.pollsUntilExit {
		f.exited[req.GetProcessId()] = true
		p.State = ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED
		p.ExitCode = f.exitCode
	}
	return p, nil
}

func (f *fakeAPI) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	f.mu.Lock()
	stdout, stderr, exited, code := f.stdout, f.stderr, f.exited[req.GetProcessId()], f.exitCode
	f.mu.Unlock()
	if stdout != "" {
		if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(stdout)}}); err != nil {
			return err
		}
	}
	if stderr != "" {
		if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stderr{Stderr: []byte(stderr)}}); err != nil {
			return err
		}
	}
	if exited {
		return stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{
			ProcessId: req.GetProcessId(), State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: code}}})
	}
	return nil
}

func (f *fakeAPI) SignalProcess(_ context.Context, req *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killed = append(f.killed, req.GetProcessId())
	f.exited[req.GetProcessId()] = true
	return &ateenvv1alpha.Process{ProcessId: req.GetProcessId(), State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: 137}, nil
}

// init makes the maps the handlers write.
func (f *fakeAPI) init() {
	f.polls = map[string]int{}
	f.exited = map[string]bool{}
	f.domainCounter = map[string]int{}
	f.nodes = map[string]string{}
	f.selectors = map[string]map[string]string{}
	f.inflight = map[string]int{}
	f.maxInflight = map[string]int{}
	f.images = map[string]string{}
	f.templates = map[string]bool{}
	f.statusCalls = map[string]int{}
	if f.goldenFail == nil {
		f.goldenFail = map[string]string{}
	}
}

// newFakeBackend serves fakeAPI over an in-memory connection and returns an
// EnvBackend on it, the same path the binary uses with a real address.
func newFakeBackend(t *testing.T, api *fakeAPI) *EnvBackend {
	t.Helper()
	api.init()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	ateenvv1alpha.RegisterEnvironmentServiceServer(srv, api)
	ateenvv1alpha.RegisterProcessServiceServer(srv, api)
	ateenvv1alpha.RegisterFileSystemServiceServer(srv, api)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c, err := env.NewClient(env.ClientOptions{GRPCConn: conn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = conn.Close(); srv.Stop() })
	b := &EnvBackend{client: c, atespace: "ate-env", baseTemplate: "default-template", deletes: defaultDeleteLimits()}
	return b
}

// Run maps the SDK's polled run to a Result. The polling itself (retries,
// the kill of an abandoned process) is tested in clients/go.
func TestEnvBackendRunCollectsExitAndOutput(t *testing.T) {
	api := &fakeAPI{pollsUntilExit: 1, exitCode: 4, stdout: "out", stderr: "err"}
	b := newFakeBackend(t, api)
	sb, err := b.Create(context.Background(), "e1", "reg/img@"+goodDigest)
	if err != nil {
		t.Fatal(err)
	}
	res, err := sb.Run(context.Background(), "echo hi", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 4 || res.Stdout != "out" || res.Stderr != "err" {
		t.Fatalf("result %+v", res)
	}
	if len(api.killed) != 0 {
		t.Fatalf("a process that exited must not be killed: %v", api.killed)
	}
	if len(api.created) != 1 || api.created[0] != "e1" {
		t.Fatalf("created %v", api.created)
	}
	if !strings.HasSuffix(api.commands[0], "echo hi") {
		t.Fatalf("command %q", api.commands[0])
	}
}

func TestEnvBackendDeleteByName(t *testing.T) {
	api := &fakeAPI{}
	b := newFakeBackend(t, api)
	if err := b.Delete(context.Background(), "gone"); err != nil {
		t.Fatal(err)
	}
	if len(api.deleted) != 1 || api.deleted[0] != "gone" {
		t.Fatalf("deleted %v", api.deleted)
	}
}

// A delete the bucket rate-limits is retried until it lands, on the backend
// and on a sandbox alike.
func TestEnvBackendDeleteRetriesRateLimit(t *testing.T) {
	api := &fakeAPI{deleteRateLimited: 2}
	b := newFakeBackend(t, api)
	b.deletes.minWait, b.deletes.maxWait = time.Millisecond, 2*time.Millisecond
	if err := b.Delete(context.Background(), "busy"); err != nil {
		t.Fatalf("Delete after two 429s: %v", err)
	}
	if api.deleteCalls != 3 || len(api.deleted) != 1 || api.deleted[0] != "busy" {
		t.Fatalf("calls %d deleted %v, want 3 calls and one delete", api.deleteCalls, api.deleted)
	}
	api.deleteRateLimited, api.deleteCalls = 1, 0
	sb, err := b.Create(context.Background(), "env1", "reg/img@sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if err := sb.Delete(context.Background()); err != nil {
		t.Fatalf("sandbox Delete after a 429: %v", err)
	}
	if api.deleteCalls != 2 {
		t.Fatalf("sandbox delete made %d calls, want 2", api.deleteCalls)
	}
}

// Retries stop at the budget and surface the last error; other errors are
// not retried at all.
func TestEnvBackendDeleteGivesUpWithinBudget(t *testing.T) {
	api := &fakeAPI{deleteRateLimited: 1000}
	b := newFakeBackend(t, api)
	b.deletes.budget, b.deletes.minWait, b.deletes.maxWait = 100*time.Millisecond, 5*time.Millisecond, 10*time.Millisecond
	err := b.Delete(context.Background(), "busy")
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Delete past the budget: %v, want the 429 as ResourceExhausted", err)
	}
	if api.deleteCalls < 2 || api.deleteCalls > 12 {
		t.Fatalf("%d calls within a 100 ms budget at 5 to 10 ms backoff, want a few", api.deleteCalls)
	}
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{status.Error(codes.InvalidArgument, "id is required"), false},
		{status.Error(codes.FailedPrecondition, "deleting snapshot: googleapi: Error 429: rateLimitExceeded"), true},
		{status.Error(codes.FailedPrecondition, `actor: deleting "swe-1794290000-x": not running`), false},
		{status.Error(codes.ResourceExhausted, "too many requests"), true},
	} {
		if got := isRetryableDelete(tc.err); got != tc.want {
			t.Errorf("isRetryableDelete(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
	if zero := (deleteLimits{}); zero.do(context.Background(), func(context.Context) error {
		return status.Error(codes.ResourceExhausted, "busy")
	}) == nil {
		t.Fatal("the zero deleteLimits must not retry")
	}
}

// The cap paces deletes; one that cannot run before the context's deadline
// fails at once instead of waiting.
func TestEnvBackendDeleteRateCap(t *testing.T) {
	api := &fakeAPI{}
	b := newFakeBackend(t, api)
	WithDeleteRate(2)(b)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	for i := range 2 {
		if err := b.Delete(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("delete %d within the burst: %v", i, err)
		}
	}
	if err := b.Delete(ctx, "e2"); err == nil || !strings.Contains(err.Error(), "delete rate cap") {
		t.Fatalf("third delete at 2/s with 400 ms left: %v, want the cap to refuse it", err)
	}
	if len(api.deleted) != 2 {
		t.Fatalf("deleted %v, want the two the burst allowed", api.deleted)
	}
	WithDeleteRate(0)(b)
	if b.deletes.limiter != nil {
		t.Fatal("WithDeleteRate(0) must remove the cap")
	}
}

// The backend's connections reach the API as that many TCP connections,
// and every call of one sandbox goes on the connection it was created on.
// NewEnvBackend takes 0 for 1.
func TestEnvBackendSpreadsConnections(t *testing.T) {
	for _, tc := range []struct{ connections, want int }{{3, 3}, {0, 1}} {
		t.Run(fmt.Sprint(tc.connections), func(t *testing.T) {
			api := &fakeAPI{pollsUntilExit: 1}
			api.init()
			// peers records the client address, one per TCP connection,
			// of each call per sandbox: from the request on a create, from
			// the x-env-id header on a guest call.
			var (
				mu    sync.Mutex
				peers = map[string]map[string]bool{}
			)
			record := func(ctx context.Context, req any) {
				id := ""
				if r, ok := req.(interface{ GetId() string }); ok {
					id = r.GetId()
				}
				if md, _ := metadata.FromIncomingContext(ctx); len(md.Get("x-env-id")) > 0 {
					id = md.Get("x-env-id")[0]
				}
				p, _ := peer.FromContext(ctx)
				mu.Lock()
				defer mu.Unlock()
				if peers[id] == nil {
					peers[id] = map[string]bool{}
				}
				peers[id][p.Addr.String()] = true
			}
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := grpc.NewServer(
				grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
					record(ctx, req)
					return h(ctx, req)
				}),
				grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
					record(ss.Context(), nil)
					return h(srv, ss)
				}))
			ateenvv1alpha.RegisterEnvironmentServiceServer(srv, api)
			ateenvv1alpha.RegisterProcessServiceServer(srv, api)
			go func() { _ = srv.Serve(lis) }()
			t.Cleanup(srv.Stop)

			b, err := NewEnvBackend(lis.Addr().String(), "ate-env", "default-template", tc.connections, "")
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			ctx := context.Background()
			for i := range 6 {
				sb, err := b.Create(ctx, fmt.Sprintf("e%d", i), "reg/img@"+goodDigest)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := sb.Run(ctx, "true", time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			all := map[string]bool{}
			for i := range 6 {
				id := fmt.Sprintf("e%d", i)
				if len(peers[id]) != 1 {
					t.Errorf("sandbox %s used %d connections, want 1: %v", id, len(peers[id]), peers[id])
				}
				maps.Copy(all, peers[id])
			}
			if len(all) != tc.want {
				t.Errorf("%d connections used, want %d: %v", len(all), tc.want, all)
			}
		})
	}
}

// authRecorder answers DeleteEnvironment and records the authorization
// metadata of each call.
type authRecorder struct {
	ateenvv1alpha.UnimplementedEnvironmentServiceServer
	mu   sync.Mutex
	seen [][]string
}

func (a *authRecorder) DeleteEnvironment(ctx context.Context, _ *ateenvv1alpha.DeleteEnvironmentRequest) (*ateenvv1alpha.DeleteEnvironmentResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	a.mu.Lock()
	a.seen = append(a.seen, md.Get("authorization"))
	a.mu.Unlock()
	return &ateenvv1alpha.DeleteEnvironmentResponse{}, nil
}

func TestNewEnvBackendSendsTheAuthToken(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec := &authRecorder{}
	srv := grpc.NewServer()
	ateenvv1alpha.RegisterEnvironmentServiceServer(srv, rec)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	for _, token := range []string{"s3cret", ""} {
		b, err := NewEnvBackend(lis.Addr().String(), "ate-env", "default-template", 2, token)
		if err != nil {
			t.Fatal(err)
		}
		// Two deletes reach both connections of the pool.
		for i := 0; i < 2; i++ {
			if err := b.Delete(context.Background(), fmt.Sprintf("e%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		_ = b.Close()
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.seen) != 4 {
		t.Fatalf("%d calls recorded, want 4", len(rec.seen))
	}
	for i, got := range rec.seen[:2] {
		if len(got) != 1 || got[0] != "Bearer s3cret" {
			t.Errorf("call %d with a token: authorization = %q, want one %q", i, got, "Bearer s3cret")
		}
	}
	for i, got := range rec.seen[2:] {
		if len(got) != 0 {
			t.Errorf("call %d without a token: authorization = %q, want none", i+2, got)
		}
	}
}

// CheckSuspendLocal passes on a server that lists suspend_mode and fails,
// naming the feature, on one that does not or that predates GetServerInfo.
func TestEnvBackendCheckSuspendLocal(t *testing.T) {
	ctx := context.Background()
	if err := newFakeBackend(t, &fakeAPI{features: []string{"rootfs_image", "suspend_mode"}}).CheckSuspendLocal(ctx); err != nil {
		t.Fatalf("server with suspend_mode: %v", err)
	}
	err := newFakeBackend(t, &fakeAPI{features: []string{"rootfs_image"}}).CheckSuspendLocal(ctx)
	if err == nil || !strings.Contains(err.Error(), "suspend_mode") {
		t.Fatalf("server without suspend_mode: %v, want an error naming the feature", err)
	}
	err = newFakeBackend(t, &fakeAPI{}).CheckSuspendLocal(ctx)
	if status.Code(err) != codes.Unimplemented || !strings.Contains(err.Error(), "-park suspend") {
		t.Fatalf("server without GetServerInfo: %v, want Unimplemented and the fallback", err)
	}
}

// A steps run that parks with suspend-async or fork through an ate-env-api
// that does not serve suspend_mode fails before it creates anything.
func TestRunChecksSuspendLocalBeforeStarting(t *testing.T) {
	for _, park := range []Park{ParkSuspendAsync, ParkFork} {
		api := &fakeAPI{features: []string{"rootfs_image"}}
		b := newFakeBackend(t, api)
		var out bytes.Buffer
		cfg := baseConfig(t, b, &out)
		cfg.Mode = ModeSteps
		cfg.Rollouts = 1
		cfg.Steps = 2
		cfg.Park = park
		_, err := Run(context.Background(), cfg)
		if err == nil || !strings.Contains(err.Error(), "suspend_mode") {
			t.Fatalf("%s: Run = %v, want the suspend_mode error", park, err)
		}
		if len(api.created) != 0 {
			t.Fatalf("%s: created %v before the check", park, api.created)
		}
	}
	// One step without -park-each never parks, so the same server serves it.
	api := &fakeAPI{features: []string{"rootfs_image"}}
	var out bytes.Buffer
	cfg := baseConfig(t, newFakeBackend(t, api), &out)
	cfg.Mode = ModeSteps
	cfg.Rollouts = 1
	cfg.Steps = 1
	cfg.Park = ParkSuspendAsync
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("one step, no park: Run = %v, want no suspend_mode check", err)
	}
	if len(api.created) == 0 || len(api.suspendModes) != 0 {
		t.Fatalf("one step, no park: created %v, suspend modes %v; want creates and no suspend", api.created, api.suspendModes)
	}
}

func TestEnvBackendSuspendAndPrewarm(t *testing.T) {
	api := &fakeAPI{}
	b := newFakeBackend(t, api)
	ctx := context.Background()

	sb, err := b.Create(ctx, "e1", "reg/img@"+goodDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := sb.Suspend(ctx); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if err := sb.(AsyncSuspender).SuspendAsync(ctx); err != nil {
		t.Fatalf("SuspendAsync: %v", err)
	}
	if err := sb.(ModeSuspender).SuspendWithMode(ctx, false); err != nil {
		t.Fatalf("SuspendWithMode(false): %v", err)
	}
	if err := sb.(ModeSuspender).SuspendWithMode(ctx, true); err != nil {
		t.Fatalf("SuspendWithMode(true): %v", err)
	}
	if err := b.SuspendWithMode(ctx, "e1", true); err != nil {
		t.Fatalf("backend SuspendWithMode(true): %v", err)
	}
	if len(api.suspended) != 5 {
		t.Fatalf("suspended = %v, want 5 calls", api.suspended)
	}
	// Suspend and SuspendWithMode(false) send no mode (durable on any
	// server); SuspendAsync and SuspendWithMode(true) send the local mode.
	want := []ateenvv1alpha.SuspendMode{
		ateenvv1alpha.SuspendMode_SUSPEND_MODE_UNSPECIFIED,
		ateenvv1alpha.SuspendMode_SUSPEND_MODE_LOCAL,
		ateenvv1alpha.SuspendMode_SUSPEND_MODE_UNSPECIFIED,
		ateenvv1alpha.SuspendMode_SUSPEND_MODE_LOCAL,
		ateenvv1alpha.SuspendMode_SUSPEND_MODE_LOCAL,
	}
	if !slices.Equal(api.suspendModes, want) {
		t.Fatalf("suspend modes = %v, want %v", api.suspendModes, want)
	}
	if err := b.Prewarm(ctx, []string{"reg/img@" + goodDigest}); err != nil {
		t.Fatalf("Prewarm: %v", err)
	}
}

func TestDirectSubstrateBackend(t *testing.T) {
	control := fakecontrol.New()
	controlAddr, stopControl, err := control.Serve()
	if err != nil {
		t.Fatalf("control.Serve: %v", err)
	}
	t.Cleanup(stopControl)

	router := fakerouter.New()
	routerAddr, stopRouter := router.Serve()
	t.Cleanup(stopRouter)

	workDir := t.TempDir()
	grpcGuestServer, cleanupGuest, err := guest.NewServer(guest.Config{
		Workspace:        workDir,
		EnableProcess:    true,
		EnableFileSystem: true,
	})
	if err != nil {
		t.Fatalf("guest.NewServer: %v", err)
	}
	t.Cleanup(cleanupGuest)
	router.Register("ds-1", grpcGuestServer)
	router.Register("ds-1-fork", grpcGuestServer)

	control.AddTemplate(&ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "default-template"},
		Containers: []*ateapipb.Container{{
			Name:    "guest",
			Image:   "example.com/ate-env-guest@" + goodDigest,
			Command: []string{"/ko-app/ate-env-guest"},
		}},
	})

	ctx := context.Background()
	b, err := NewDirectSubstrateBackend(DirectSubstrateOptions{
		ATEOptions: ate.Options{
			ControlAddr: controlAddr,
			RouterAddr:  routerAddr,
			SkipVerify:  true,
		},
		Atespace:     "ate-env",
		BaseTemplate: "default-template",
	})
	if err != nil {
		t.Fatalf("NewDirectSubstrateBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	img := "reg/img@" + goodDigest
	if err := b.Prewarm(ctx, []string{img}); err != nil {
		t.Fatalf("Prewarm: %v", err)
	}

	sb, err := b.Create(ctx, "ds-1", img)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := sb.WriteFile(ctx, "greeting.txt", []byte("hello substrate"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	gotBytes, err := os.ReadFile(filepath.Join(workDir, "greeting.txt"))
	if err != nil || string(gotBytes) != "hello substrate" {
		t.Fatalf("ReadFile = %q, %v", gotBytes, err)
	}

	largePayload := strings.Repeat("echo r2e-grade-line\n", 12*1024)
	if err := sb.WriteFile(ctx, "large-grade.sh", []byte(largePayload), 0o755); err != nil {
		t.Fatalf("WriteFile(large): %v", err)
	}
	gotLarge, err := os.ReadFile(filepath.Join(workDir, "large-grade.sh"))
	if err != nil || string(gotLarge) != largePayload {
		t.Fatalf("ReadFile(large) len=%d err=%v, want len=%d", len(gotLarge), err, len(largePayload))
	}

	res, err := sb.Run(ctx, "printf direct-ok", 5*time.Second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 || res.Stdout != "direct-ok" {
		t.Fatalf("Run result = %+v", res)
	}

	if err := sb.(AsyncSuspender).SuspendAsync(ctx); err != nil {
		t.Fatalf("SuspendAsync: %v", err)
	}
	if n := len(control.SuspendModes); n == 0 || control.SuspendModes[n-1] != ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS {
		t.Fatalf("SuspendModes = %v, want last ASYNCHRONOUS", control.SuspendModes)
	}

	if err := sb.Suspend(ctx); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if n := len(control.SuspendModes); n == 0 || control.SuspendModes[n-1] != ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
		t.Fatalf("SuspendModes = %v, want last SYNCHRONOUS", control.SuspendModes)
	}

	forked, err := sb.(SandboxForker).Fork(ctx, "ds-1-fork")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if err := forked.Delete(ctx); err != nil {
		t.Fatalf("forked.Delete: %v", err)
	}
	if err := sb.Delete(ctx); err != nil {
		t.Fatalf("sb.Delete: %v", err)
	}
}

func TestAgentSandboxBackend(t *testing.T) {
	type recordedReq struct {
		Method string
		Path   string
		Auth   string
		Body   map[string]any
	}
	var (
		mu   sync.Mutex
		reqs []recordedReq
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
		mu.Lock()
		reqs = append(reqs, recordedReq{
			Method: r.Method,
			Path:   r.URL.RequestURI(),
			Auth:   r.Header.Get("Authorization"),
			Body:   body,
		})
		mu.Unlock()

		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "missing") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/exec") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"exit_code":0,"stdout":"agent-sandbox-out","stderr":""}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)

	b, err := NewAgentSandboxBackend(AgentSandboxOptions{
		Endpoint:  ts.URL,
		Namespace: "bench-ns",
		WarmPool:  "warm-pool-1",
		AuthToken: "secret-tok",
	})
	if err != nil {
		t.Fatalf("NewAgentSandboxBackend: %v", err)
	}

	ctx := context.Background()
	if err := b.Prewarm(ctx, []string{"img1"}); err != nil {
		t.Fatalf("Prewarm: %v", err)
	}
	sb, err := b.Create(ctx, "as-1", "img1")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := sb.WriteFile(ctx, "/tmp/a.txt", []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	res, err := sb.Run(ctx, "echo hi", 5*time.Second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 || res.Stdout != "agent-sandbox-out" {
		t.Fatalf("Run result = %+v", res)
	}
	if err := sb.Suspend(ctx); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if err := sb.(AsyncSuspender).SuspendAsync(ctx); err != nil {
		t.Fatalf("SuspendAsync: %v", err)
	}
	forked, err := sb.(SandboxForker).Fork(ctx, "as-1-fork")
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if err := forked.Delete(ctx); err != nil {
		t.Fatalf("forked.Delete: %v", err)
	}
	if err := sb.Delete(ctx); err != nil {
		t.Fatalf("sb.Delete: %v", err)
	}
	if err := b.Delete(ctx, "missing"); err != nil {
		t.Fatalf("Delete(missing) should ignore 404, got: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reqs) < 8 {
		t.Fatalf("recorded %d requests, want >= 8", len(reqs))
	}
	for i, r := range reqs {
		if r.Auth != "Bearer secret-tok" {
			t.Errorf("req %d (%s %s): auth = %q, want Bearer secret-tok", i, r.Method, r.Path, r.Auth)
		}
	}
}

func TestNewBackendDriver(t *testing.T) {
	control := fakecontrol.New()
	controlAddr, stopControl, err := control.Serve()
	if err != nil {
		t.Fatalf("control.Serve: %v", err)
	}
	t.Cleanup(stopControl)

	router := fakerouter.New()
	routerAddr, stopRouter := router.Serve()
	t.Cleanup(stopRouter)

	// 1. substrate + ate-env-api (default)
	bEnv, closer, err := NewBackendDriver(BackendDriverOptions{
		Backend:     "substrate",
		Access:      "ate-env-api",
		Endpoint:    "127.0.0.1:1",
		Connections: 1,
	})
	if err != nil {
		t.Fatalf("NewBackendDriver(substrate, ate-env-api): %v", err)
	}
	_ = closer.Close()
	if _, ok := bEnv.(*EnvBackend); !ok {
		t.Fatalf("got %T, want *EnvBackend", bEnv)
	}

	// 2. substrate + direct
	bDir, closer, err := NewBackendDriver(BackendDriverOptions{
		Backend:     "substrate",
		Access:      "direct",
		ControlAddr: controlAddr,
		RouterAddr:  routerAddr,
	})
	if err != nil {
		t.Fatalf("NewBackendDriver(substrate, direct): %v", err)
	}
	_ = closer.Close()
	if _, ok := bDir.(*DirectSubstrateBackend); !ok {
		t.Fatalf("got %T, want *DirectSubstrateBackend", bDir)
	}

	// 3. agent-sandbox + sdk
	bAS, closer, err := NewBackendDriver(BackendDriverOptions{
		Backend:  "agent-sandbox",
		Access:   "sdk",
		Endpoint: "http://127.0.0.1:8080",
	})
	if err != nil {
		t.Fatalf("NewBackendDriver(agent-sandbox, sdk): %v", err)
	}
	_ = closer.Close()
	if _, ok := bAS.(*AgentSandboxBackend); !ok {
		t.Fatalf("got %T, want *AgentSandboxBackend", bAS)
	}

	// 4. invalid backend or access
	if _, _, err := NewBackendDriver(BackendDriverOptions{Backend: "bogus"}); err == nil {
		t.Fatal("expected error for unknown backend")
	}
	if _, _, err := NewBackendDriver(BackendDriverOptions{Backend: "substrate", Access: "bogus"}); err == nil {
		t.Fatal("expected error for unknown access")
	}

	// 5. invalid ateapi CA file fails loudly
	badCA := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not-a-pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewBackendDriver(BackendDriverOptions{
		Backend:      "substrate",
		Access:       "direct",
		ControlAddr:  controlAddr,
		RouterAddr:   routerAddr,
		AteapiCAFile: badCA,
	}); err == nil {
		t.Fatal("expected error for invalid AteapiCAFile PEM")
	}

	// 6. missing base template on DirectSubstrateBackend fails loudly instead of silently ignoring image
	bMissing, err := NewDirectSubstrateBackend(DirectSubstrateOptions{
		ATEOptions: ate.Options{
			ControlAddr: controlAddr,
			RouterAddr:  routerAddr,
			SkipVerify:  true,
		},
		Atespace:     "ate-env",
		BaseTemplate: "nonexistent-base-template",
	})
	if err != nil {
		t.Fatalf("NewDirectSubstrateBackend: %v", err)
	}
	defer bMissing.Close()
	if _, err := bMissing.Create(context.Background(), "sb-missing", "us-docker.pkg.dev/p/r/img@sha256:0000000000000000000000000000000000000000000000000000000000000001"); err == nil {
		t.Fatal("expected Create to fail when base template is missing")
	}
}

func TestEnvBackendCreateFollowsThePlacementPlan(t *testing.T) {
	api := &fakeAPI{domainNodes: map[string][]string{"d0": {"n0"}, "d1": {"n1"}}}
	b := newFakeBackend(t, api)
	WithPlacement(twoDomainPlan())(b)
	ctx := context.Background()
	// b is in d0 and d1: creates alternate; a is in d0 only; an image the plan
	// does not place gets no selector.
	for i, want := range []map[string]string{{"ate-env/domain": "d0"}, {"ate-env/domain": "d1"}, {"ate-env/domain": "d0"}} {
		id := fmt.Sprintf("b-%d", i)
		if _, err := b.Create(ctx, id, pfImgB); err != nil {
			t.Fatal(err)
		}
		if got := api.selectors[id]; !reflect.DeepEqual(got, want) {
			t.Errorf("create %d of b sent selector %v, want %v", i, got, want)
		}
	}
	if _, err := b.Create(ctx, "a-0", pfImgA); err != nil {
		t.Fatal(err)
	}
	if got := api.selectors["a-0"]; !reflect.DeepEqual(got, map[string]string{"ate-env/domain": "d0"}) {
		t.Errorf("a sent selector %v", got)
	}
	other := "registry.example/swebench/z@sha256:" + strings.Repeat("c", 64)
	if _, err := b.Create(ctx, "z-0", other); err != nil {
		t.Fatal(err)
	}
	if got := api.selectors["z-0"]; len(got) != 0 {
		t.Errorf("an unplaced image sent selector %v, want none", got)
	}
}

func TestNewBackendDriverRefusesPlacementOffTheEnvAPI(t *testing.T) {
	plan := twoDomainPlan()
	for _, opts := range []BackendDriverOptions{
		{Backend: "agent-sandbox", Access: "sdk", Endpoint: "http://127.0.0.1:1", Placement: plan},
		{Backend: "substrate", Access: "direct", Endpoint: "127.0.0.1:1", Placement: plan},
	} {
		if _, _, err := NewBackendDriver(opts); err == nil || !strings.Contains(err.Error(), "placement plan needs the ate-env API backend") {
			t.Errorf("%s/%s with a plan: err = %v", opts.Backend, opts.Access, err)
		}
	}
}

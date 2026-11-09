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

// Package apiservice implements the EnvironmentService, ProcessService, and
// FileSystemService gRPC APIs for the ate-env API server, proxying in-actor
// execution and filesystem operations directly to the guest daemon via atenet.
package apiservice

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/routertls"
	"github.com/agent-substrate/env/internal/telemetry"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// DefaultTemplate is the ActorTemplate name used when a create request
// does not specify one.
const DefaultTemplate = "default-template"

// DefaultNamespace is the default Kubernetes namespace used when deploying
// ate-env manifests.
const DefaultNamespace = "ate-env"

// DefaultAtespace is the Substrate atespace used when a request does not
// specify one; the same one ate-env-api creates at startup.
const DefaultAtespace = ate.DefaultAtespace

// Server implements ateenvv1alpha.EnvironmentServiceServer, ateenvv1alpha.ProcessServiceServer,
// and ateenvv1alpha.FileSystemServiceServer.
type Server struct {
	ateenvv1alpha.UnimplementedEnvironmentServiceServer
	ateenvv1alpha.UnimplementedProcessServiceServer
	ateenvv1alpha.UnimplementedFileSystemServiceServer

	client     *ate.Client
	routerAddr string
	hostSuffix string

	// WakeBudget bounds how long a guest call waits for a suspended
	// environment to wake; see ensureAwake. Zero means DefaultWakeBudget.
	WakeBudget time.Duration
	// WakeRunningBudget bounds the wait once the control plane reports the
	// environment RUNNING; see ensureAwake. Zero means DefaultWakeRunningBudget.
	WakeRunningBudget time.Duration
	// WakeNoCapacityGrace, when positive, fails a guest call with
	// ResourceExhausted once the router has said for this long that no
	// worker has room for the environment; see ensureAwake. Zero (the
	// default) keeps waiting for the whole WakeBudget.
	WakeNoCapacityGrace time.Duration
	// WarmMaxProbesPerNode caps the WarmImage probes in flight per node of
	// one worker selector, across calls: a round takes nodes x
	// probes_per_node slots on its selector before it creates its probes
	// and gives them back once they are deleted, so a pre-flight that fans
	// out one call per image puts at most this many first boots (image
	// unpacks) on a node at once. A request's probes_per_node above it is
	// refused. Zero means DefaultWarmMaxProbesPerNode.
	WarmMaxProbesPerNode int
	// WakeStateReadCap bounds one control-plane state read of a wake; see
	// ensureAwake. Zero or less means DefaultWakeStateReadCap; the
	// -wake-state-read-cap flag sets it.
	WakeStateReadCap time.Duration
	// Info is the build and configuration GetServerInfo reports.
	Info ServerInfo
	// RouterTLS, when set, is the client TLS configuration for the router's
	// listeners (see internal/routertls); nil dials them in plaintext.
	RouterTLS *tls.Config
	// RouterIdentity is the SPIFFE ID of the certificate RouterTLS presents,
	// named in the error when the router rejects it.
	RouterIdentity string

	// Metrics, when set, receives the create_to_ready and wake_wait
	// observations and tracks the atespaces ate_env_api_environments
	// reports (see EnvironmentCounts). Nil records nothing.
	Metrics *telemetry.Metrics

	readyMu   sync.Mutex
	pending   map[string]pendingCreate // atespace/id -> creation start, until the guest first answers
	atespaces map[string]bool          // atespaces environments were created in, for EnvironmentCounts

	// imageTemplates serializes the recreate of a failed derived template
	// with the creates that use it.
	imageTemplates imageTemplateLocks
	// warmSlots are the WarmImage probe slots per worker selector; see
	// WarmMaxProbesPerNode.
	warmSlots warmSlotRegistry
}

// New creates a new Server.
func New(client *ate.Client, routerAddr, hostSuffix string) *Server {
	if hostSuffix == "" {
		hostSuffix = ate.DefaultHostSuffix
	}
	routerAddr = strings.TrimPrefix(routerAddr, "http://")
	routerAddr = strings.TrimPrefix(routerAddr, "https://")
	return &Server{
		client:            client,
		routerAddr:        routerAddr,
		hostSuffix:        hostSuffix,
		WakeBudget:        DefaultWakeBudget,
		WakeRunningBudget: DefaultWakeRunningBudget,
	}
}

// Close is a no-op retained for lifecycle compatibility.
func (s *Server) Close() {}

// ============================================================================
// --- ENVIRONMENT SERVICE ---
// ============================================================================

// CreateEnvironment registers and starts a new environment.
//
// Defaults: id is required. The template name defaults to DefaultTemplate.
// The atespace is the request's atespace, else template.atespace, else
// DefaultAtespace, and that one atespace is used both to resolve the template
// and to create the actor; a template.atespace that differs from a non-empty
// request atespace is ignored.
//
// When rootfs_image is set, the template acts as the base and the environment is
// created from a template derived from it, named
// "<base>-<first 12 hex digits of the image digest>" in the same atespace.
// The derived template keeps the base's settings, runs the rootfs image as
// the container image and mounts the base's guest as a read-only image volume,
// so the image runs unmodified (see ensureImageTemplate and
// DeriveImageTemplate). It is created on first use and reused by later
// creates on the same image (created again when its golden snapshot build
// failed long enough ago and no actor uses it; see
// recreateFailedImageTemplate), and the response reports it in
// Environment.template. When rootfs_image is empty the template is used as is.
//
// Errors: InvalidArgument for a missing id, a rootfs image not pinned by digest, a
// derived name that is too long or not a valid resource name, or a base that
// cannot be derived from (no containers, unpinned guest image, or a command
// that cannot be re-rooted under the guest volume). FailedPrecondition when
// the base template does not exist in the atespace, or when the derived name
// is already held by a template running a different image. DeadlineExceeded
// or Canceled when the request ends while a recreate of its derived template
// runs. Other control plane failures are mapped by toGRPCError.
func (s *Server) CreateEnvironment(ctx context.Context, req *ateenvv1alpha.CreateEnvironmentRequest) (resp *ateenvv1alpha.CreateEnvironmentResponse, retErr error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var start time.Time
	if s.Metrics != nil {
		start = requestedAt(ctx, time.Now())
	}
	templateName, atespace := templateDefaults(req.GetTemplate(), req.GetAtespace())
	if s.Metrics != nil {
		defer func() {
			if retErr != nil {
				s.Metrics.ObserveCreateToReady(atespace, telemetry.ReadyStatusError, time.Since(start))
				return
			}
			s.trackCreate(atespace, req.GetId(), start)
		}()
	}

	if image := req.GetRootfsImage(); image != "" {
		name, _, release, err := s.ensureImageTemplate(ctx, atespace, templateName, image)
		if err != nil {
			return nil, err
		}
		defer release()
		templateName = name
	}

	opts := ate.CreateOptions{
		ID:             req.GetId(),
		Template:       templateName,
		Atespace:       atespace,
		WorkerSelector: req.GetWorkerSelector(),
	}
	if err := s.client.Create(ctx, opts); err != nil {
		return nil, toGRPCError(err)
	}

	return &ateenvv1alpha.CreateEnvironmentResponse{
		Environment: &ateenvv1alpha.Environment{
			Id:       req.GetId(),
			Atespace: atespace,
			Template: &ateenvv1alpha.Template{
				Name:     templateName,
				Atespace: atespace,
			},
			Status:         ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_UNSPECIFIED,
			WorkerSelector: req.GetWorkerSelector(),
		},
	}, nil
}

// templateDefaults resolves a request's base template name and atespace the
// way CreateEnvironment does: the template name defaults to DefaultTemplate,
// the atespace to the template's, then to DefaultAtespace.
func templateDefaults(tmpl *ateenvv1alpha.Template, atespace string) (name, space string) {
	name = tmpl.GetName()
	if name == "" {
		name = DefaultTemplate
	}
	space = atespace
	if space == "" {
		space = tmpl.GetAtespace()
	}
	if space == "" {
		space = DefaultAtespace
	}
	return name, space
}

// GetImageStatus reports the template derived for a rootfs image and the
// state of its golden snapshot, without creating anything.
func (s *Server) GetImageStatus(ctx context.Context, req *ateenvv1alpha.GetImageStatusRequest) (*ateenvv1alpha.ImageStatus, error) {
	base, atespace := templateDefaults(req.GetTemplate(), req.GetAtespace())
	name, err := ImageTemplateName(base, req.GetRootfsImage())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	tmpl, err := s.client.GetActorTemplate(ctx, atespace, name)
	if err != nil && !errors.Is(err, ate.ErrNotFound) {
		return nil, toGRPCError(err)
	}
	// The same conflict a create reports: a template under the derived name
	// that runs another image is not this image's template.
	if got := firstContainerImage(tmpl); tmpl != nil && !sameImageDigest(got, req.GetRootfsImage()) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"actor template %q exists but runs %q, not %q; delete it or use another base template", name, got, req.GetRootfsImage())
	}
	return imageStatus(req.GetRootfsImage(), atespace, name, tmpl), nil
}

// EnsureImageTemplate creates the template derived for a rootfs image when
// it is missing, which starts its golden snapshot bake, and reports it. It
// takes the same path a create does, so a template whose bake failed is
// recreated under the same rules, but no actor is created.
func (s *Server) EnsureImageTemplate(ctx context.Context, req *ateenvv1alpha.EnsureImageTemplateRequest) (*ateenvv1alpha.ImageStatus, error) {
	base, atespace := templateDefaults(req.GetTemplate(), req.GetAtespace())
	name, tmpl, release, err := s.ensureImageTemplate(ctx, atespace, base, req.GetRootfsImage())
	if err != nil {
		return nil, err
	}
	release()
	return imageStatus(req.GetRootfsImage(), atespace, name, tmpl), nil
}

// imageStatus is the ImageStatus for the derived template tmpl, nil when it
// does not exist. The golden state follows Substrate's
// golden_snapshot_status: a tag means READY, an error message FAILED, a
// template with neither is still baking, and a template whose golden
// snapshots are disabled reports NONE, because none will come.
func imageStatus(image, atespace, name string, tmpl *ateapipb.ActorTemplate) *ateenvv1alpha.ImageStatus {
	out := &ateenvv1alpha.ImageStatus{
		RootfsImage: image,
		Template:    &ateenvv1alpha.Template{Name: name, Atespace: atespace},
		Golden:      ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_NONE,
	}
	if tmpl == nil {
		return out
	}
	out.TemplateExists = true
	if tmpl.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
		return out
	}
	golden := tmpl.GetStatus().GetGoldenSnapshotStatus()
	switch {
	case golden.GetErrorMessage() != "":
		out.Golden = ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_FAILED
		out.GoldenError = golden.GetErrorMessage()
	case golden.GetGoldenTag().GetName() != "":
		out.Golden = ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_READY
		out.GoldenTag = golden.GetGoldenTag().GetName()
	default:
		out.Golden = ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_PENDING
	}
	return out
}

// ensureImageTemplate returns the name of the ActorTemplate that runs image
// on top of base, creating it from base on first use. The derived template
// lives in the environment's atespace, next to base. The caller must call
// release once its actor is created: until then, this process does not
// delete the template (see recreateFailedImageTemplate).
func (s *Server) ensureImageTemplate(ctx context.Context, atespace, base, image string) (name string, tmpl *ateapipb.ActorTemplate, release func(), err error) {
	name, err = ImageTemplateName(base, image)
	if err != nil {
		return "", nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	lock := s.imageTemplates.get(atespace, name)
	if err := lock.rlock(ctx); err != nil {
		return "", nil, nil, err
	}
	tmpl, err = s.getOrCreateImageTemplate(ctx, atespace, base, name, image)
	if err != nil {
		lock.mu.RUnlock()
		return "", nil, nil, err
	}
	if !lock.recreateDue(tmpl, time.Now()) {
		return name, tmpl, lock.mu.RUnlock, nil
	}
	lock.mu.RUnlock()

	lock.mu.Lock()
	err = s.recreateFailedImageTemplate(ctx, lock, atespace, base, name, image)
	lock.mu.Unlock()
	if err != nil {
		return "", nil, nil, err
	}
	if err := lock.rlock(ctx); err != nil {
		return "", nil, nil, err
	}
	if tmpl, err = s.getOrCreateImageTemplate(ctx, atespace, base, name, image); err != nil {
		lock.mu.RUnlock()
		return "", nil, nil, err
	}
	return name, tmpl, lock.mu.RUnlock, nil
}

// getOrCreateImageTemplate returns the derived template name, creating it
// from base when it does not exist. A template that runs the same digest
// under another repository path is reused: the digest pins the bytes, and
// datasets list one image under many paths.
func (s *Server) getOrCreateImageTemplate(ctx context.Context, atespace, base, name, image string) (*ateapipb.ActorTemplate, error) {
	existing, err := s.client.GetActorTemplate(ctx, atespace, name)
	switch {
	case err == nil:
		if got := firstContainerImage(existing); !sameImageDigest(got, image) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"actor template %q exists but runs %q, not %q; delete it or use another base template", name, got, image)
		}
		return existing, nil
	case !errors.Is(err, ate.ErrNotFound):
		return nil, toGRPCError(err)
	}
	baseTmpl, err := s.client.GetActorTemplate(ctx, atespace, base)
	if err != nil {
		if errors.Is(err, ate.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"base actor template %q not found in atespace %q; register it first (ate-env manifest template)", base, atespace)
		}
		return nil, toGRPCError(err)
	}
	derived, err := DeriveImageTemplate(baseTmpl, name, image)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	created, err := s.client.CreateActorTemplate(ctx, derived)
	if err == nil {
		return created, nil
	}
	if !errors.Is(err, ate.ErrAlreadyExists) {
		return nil, toGRPCError(err)
	}
	// A concurrent create won the race. It may have built the template for
	// another digest that shares the first 12 hex digits (the derived name
	// keeps only those), so check the image as for a template that already
	// existed.
	if existing, err = s.client.GetActorTemplate(ctx, atespace, name); err != nil {
		return nil, toGRPCError(err)
	}
	if got := firstContainerImage(existing); !sameImageDigest(got, image) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"actor template %q exists but runs %q, not %q; delete it or use another base template", name, got, image)
	}
	return existing, nil
}

func firstContainerImage(t *ateapipb.ActorTemplate) string {
	if len(t.GetContainers()) == 0 {
		return ""
	}
	return t.GetContainers()[0].GetImage()
}

// sameImageDigest reports whether two digest-pinned references name the same
// image, whatever their repository paths. An unpinned reference matches
// nothing.
func sameImageDigest(a, b string) bool {
	da, err := ImageDigest(a)
	if err != nil {
		return false
	}
	db, err := ImageDigest(b)
	if err != nil {
		return false
	}
	return da == db
}

// GetEnvironment retrieves the status and configuration of an existing environment.
func (s *Server) GetEnvironment(ctx context.Context, req *ateenvv1alpha.GetEnvironmentRequest) (*ateenvv1alpha.GetEnvironmentResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = DefaultAtespace
	}

	actor, err := s.client.Get(ctx, atespace, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &ateenvv1alpha.GetEnvironmentResponse{
		Environment: ActorToEnvironment(actor),
	}, nil
}

// SuspendEnvironment checkpoints and stops an active environment.
func (s *Server) SuspendEnvironment(ctx context.Context, req *ateenvv1alpha.SuspendEnvironmentRequest) (*ateenvv1alpha.SuspendEnvironmentResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = DefaultAtespace
	}

	var err error
	switch req.GetMode() {
	case ateenvv1alpha.SuspendMode_SUSPEND_MODE_UNSPECIFIED, ateenvv1alpha.SuspendMode_SUSPEND_MODE_DURABLE:
		err = s.client.Suspend(ctx, atespace, req.GetId())
	case ateenvv1alpha.SuspendMode_SUSPEND_MODE_LOCAL:
		err = s.client.SuspendLocal(ctx, atespace, req.GetId())
	default:
		return nil, status.Errorf(codes.InvalidArgument, "mode %d is not a SuspendMode", req.GetMode())
	}
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &ateenvv1alpha.SuspendEnvironmentResponse{}, nil
}

// DeleteEnvironment permanently removes an environment and its resources.
func (s *Server) DeleteEnvironment(ctx context.Context, req *ateenvv1alpha.DeleteEnvironmentRequest) (*ateenvv1alpha.DeleteEnvironmentResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = DefaultAtespace
	}

	if err := s.client.Delete(ctx, atespace, req.GetId()); err != nil {
		return nil, toGRPCError(err)
	}
	s.finishCreate(atespace, req.GetId(), telemetry.ReadyStatusDeleted)

	return &ateenvv1alpha.DeleteEnvironmentResponse{}, nil
}

// ============================================================================
// --- PROCESS SERVICE ---
// ============================================================================

// processClient dials the guest of the environment named in ctx's metadata
// and returns a ProcessService client plus the outgoing context to use with
// it. The caller must invoke closeFn when done.
func (s *Server) processClient(ctx context.Context) (ateenvv1alpha.ProcessServiceClient, context.Context, func(), error) {
	envID, atespace, err := envFromContext(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := s.guestConn(ctx, atespace, envID)
	if err != nil {
		return nil, nil, nil, err
	}
	return ateenvv1alpha.NewProcessServiceClient(conn), ctx, func() { _ = conn.Close() }, nil
}

// StartProcess launches a process inside the target environment container.
func (s *Server) StartProcess(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	client, outCtx, closeFn, err := s.processClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	return client.StartProcess(outCtx, req)
}

// GetProcess retrieves the state of a process inside the environment.
func (s *Server) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	client, outCtx, closeFn, err := s.processClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	return client.GetProcess(outCtx, req)
}

// SignalProcess delivers a signal to a process inside the environment.
func (s *Server) SignalProcess(ctx context.Context, req *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	client, outCtx, closeFn, err := s.processClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	return client.SignalProcess(outCtx, req)
}

// ResizeProcess updates the window size of a process's pseudo-terminal inside the environment.
func (s *Server) ResizeProcess(ctx context.Context, req *ateenvv1alpha.ResizeProcessRequest) (*ateenvv1alpha.ResizeProcessResponse, error) {
	client, outCtx, closeFn, err := s.processClient(ctx)
	if err != nil {
		return nil, err
	}
	defer closeFn()
	return client.ResizeProcess(outCtx, req)
}

// StreamProcessOutput streams stdout, stderr and the exit message from a process.
func (s *Server) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	client, outCtx, closeFn, err := s.processClient(stream.Context())
	if err != nil {
		return err
	}
	defer closeFn()

	clientStream, err := client.StreamProcessOutput(outCtx, req)
	if err != nil {
		return err
	}
	for {
		msg, err := clientStream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
}

// WriteProcessInput forwards stdin chunks to a process inside the environment.
func (s *Server) WriteProcessInput(stream grpc.ClientStreamingServer[ateenvv1alpha.WriteProcessInputRequest, ateenvv1alpha.WriteProcessInputResponse]) error {
	// Receive the first message before waking the guest, so a request the API
	// rejects (one with unknown fields) does not resume a suspended environment.
	// A client that opens a stream and sends nothing therefore holds no guest
	// connection until it closes.
	req, err := nextMessage(stream)
	if err != nil {
		return err
	}
	client, outCtx, closeFn, err := s.processClient(stream.Context())
	if err != nil {
		return err
	}
	defer closeFn()

	clientStream, err := client.WriteProcessInput(outCtx)
	if err != nil {
		return err
	}
	for req != nil {
		if err := clientStream.Send(req); err != nil {
			// The guest rejected the stream; surface its status instead of io.EOF.
			if errors.Is(err, io.EOF) {
				_, err = clientStream.CloseAndRecv()
			}
			return err
		}
		if req, err = nextMessage(stream); err != nil {
			return err
		}
	}

	resp, err := clientStream.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

// ============================================================================
// --- FILESYSTEM SERVICE ---
// ============================================================================

// ReadFile streams file contents from the target environment.
func (s *Server) ReadFile(req *ateenvv1alpha.ReadFileRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ReadFileResponse]) error {
	ctx := stream.Context()
	envID, atespace, err := envFromContext(ctx)
	if err != nil {
		return err
	}
	conn, err := s.guestConn(ctx, atespace, envID)
	if err != nil {
		return err
	}
	defer conn.Close()

	clientStream, err := ateenvv1alpha.NewFileSystemServiceClient(conn).ReadFile(ctx, req)
	if err != nil {
		return err
	}

	for {
		chunk, err := clientStream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
}

// WriteFile streams file contents to the target environment.
func (s *Server) WriteFile(stream grpc.ClientStreamingServer[ateenvv1alpha.WriteFileRequest, ateenvv1alpha.WriteFileResponse]) error {
	ctx := stream.Context()
	envID, atespace, err := envFromContext(ctx)
	if err != nil {
		return err
	}
	// Receive the first message before waking the guest, as WriteProcessInput
	// does.
	req, err := nextMessage(stream)
	if err != nil {
		return err
	}
	conn, err := s.guestConn(ctx, atespace, envID)
	if err != nil {
		return err
	}
	defer conn.Close()

	clientStream, err := ateenvv1alpha.NewFileSystemServiceClient(conn).WriteFile(ctx)
	if err != nil {
		return err
	}

	for req != nil {
		if err := clientStream.Send(req); err != nil {
			return err
		}
		if req, err = nextMessage(stream); err != nil {
			return err
		}
	}

	resp, err := clientStream.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(resp)
}

// ============================================================================
// --- HELPERS ---
// ============================================================================

func envFromContext(ctx context.Context) (string, string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", "", status.Error(codes.InvalidArgument, "missing environment metadata in request context")
	}
	ids := md.Get("x-env-id")
	if len(ids) == 0 || ids[0] == "" {
		return "", "", status.Error(codes.InvalidArgument, "x-env-id header is required")
	}
	atespaces := md.Get("x-env-atespace")
	atespace := DefaultAtespace
	if len(atespaces) > 0 && atespaces[0] != "" {
		atespace = atespaces[0]
	}
	return ids[0], atespace, nil
}

// guestConn dials the environment's guest through the router and waits,
// within WakeBudget, for the guest to answer, so a suspended environment is
// awake before the caller's request goes through (see ensureAwake). Calls on the
// returned connection take the caller's context as is, for its deadline and
// cancellation only: the interceptors below are the only writer of
// ate-target-actor, and no caller metadata is forwarded. Forwarding it once
// sent that header twice, which the Substrate router rejects
// (agent-substrate/substrate#2308 answers duplicate ate-target-actor headers
// with "invalid actor reference"), and it would let a client pass routing
// headers through the proxy. Nothing in the guest reads caller metadata.
func (s *Server) guestConn(ctx context.Context, atespace, id string) (*grpc.ClientConn, error) {
	if s.routerAddr == "" {
		return nil, status.Error(codes.FailedPrecondition, "router address not configured for guest operations")
	}
	if atespace == "" {
		atespace = DefaultAtespace
	}
	targetActor := atespace + "/" + id
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(s.routerCredentials()),
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, ate.TargetActorHeader, targetActor)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = metadata.AppendToOutgoingContext(ctx, ate.TargetActorHeader, targetActor)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	}
	// With tracing on, the wake probe and the proxied call carry the
	// caller's trace context (ctx holds the server span), so a resume the
	// probe triggers in the router shows up in the caller's trace.
	if s.client != nil && s.client.TracingEnabled() {
		dialOpts = append(dialOpts, ate.Options{EnableTracing: true}.TracingDialOptions()...)
	}
	conn, err := grpc.NewClient(s.routerAddr, dialOpts...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "dialing guest router for %s/%s: %v", atespace, id, err)
	}
	if err := s.ensureAwake(ctx, conn, atespace, id); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// routerCredentials are the transport credentials for the router: TLS with
// the pod identity certificate when RouterTLS is set, else plaintext.
func (s *Server) routerCredentials() credentials.TransportCredentials {
	if s.RouterTLS != nil {
		return routertls.TransportCredentials(s.RouterTLS)
	}
	return insecure.NewCredentials()
}

// ActorToEnvironment converts an ateapipb Actor to an ateenvv1alpha Environment.
func ActorToEnvironment(actor *ateapipb.Actor) *ateenvv1alpha.Environment {
	if actor == nil {
		return nil
	}
	templateName := ""
	templateAtespace := ""
	if tmpl := actor.GetActorTemplate(); tmpl != nil {
		templateName = tmpl.GetName()
		templateAtespace = tmpl.GetAtespace()
	}
	return &ateenvv1alpha.Environment{
		Id:       actor.GetMetadata().GetName(),
		Atespace: actor.GetMetadata().GetAtespace(),
		Template: &ateenvv1alpha.Template{
			Name:     templateName,
			Atespace: templateAtespace,
		},
		Status:         ActorStatusToEnvStatus(actor.GetStatus()),
		Node:           actorNode(actor.GetStatus()),
		WorkerSelector: actor.GetWorkerSelector().GetMatchLabels(),
	}
}

// actorNode is the node an actor is on: the one hosting its worker, or its
// local snapshot while it is suspended (assigned_node), which the worker
// assignment confirms while it runs. Empty before the first placement and
// after a detach.
func actorNode(st *ateapipb.ActorStatus) string {
	if node := st.GetAssignedNode(); node != "" {
		return node
	}
	return st.GetWorkerAssignment().GetNodeName()
}

// ActorStatusToEnvStatus maps an ateapipb ActorStatus to an ateenvv1alpha EnvironmentStatus.
func ActorStatusToEnvStatus(st *ateapipb.ActorStatus) ateenvv1alpha.EnvironmentStatus {
	if st == nil {
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_UNSPECIFIED
	}
	switch st.GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_RESUMING
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_RUNNING
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_SUSPENDING
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		// Since substrate#2620 a paused actor is SUSPENDED too; Substrate has
		// no PAUSED or PAUSING state. The latest snapshot tells the two
		// apart: a local suspend (SUSPEND_MODE_LOCAL) leaves it on a node
		// with its upload pending, and the actor resumes on that node only.
		// Once the upload lands the environment is SUSPENDED like any other.
		// ENVIRONMENT_STATUS_PAUSING is never produced: Substrate reports
		// SUSPENDING for both modes.
		if localOnly(latestSnapshot(st)) {
			return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_PAUSED
		}
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_SUSPENDED
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_CRASHED
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_DELETING
	default:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_UNSPECIFIED
	}
}

// latestSnapshot returns the actor's snapshot with the highest generation, or
// nil when it has none.
func latestSnapshot(st *ateapipb.ActorStatus) *ateapipb.Snapshot {
	var latest *ateapipb.Snapshot
	for _, snap := range st.GetSnapshots() {
		if latest == nil || snap.GetGeneration() > latest.GetGeneration() {
			latest = snap
		}
	}
	return latest
}

// localOnly reports whether snap exists on a node and not yet in object
// storage: what a local suspend leaves until its background upload lands.
func localOnly(snap *ateapipb.Snapshot) bool {
	return snap.GetLocality() != "" &&
		snap.GetDurableSnapshot().GetState() != ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
}

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok && s.Code() != codes.Unknown {
		return err
	}
	switch {
	case errors.Is(err, ate.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

// nextMessage receives the next message of a client stream: nil when the
// client closed the stream.
func nextMessage[Req any](stream interface{ Recv() (*Req, error) }) (*Req, error) {
	req, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, nil
	}
	return req, err
}

// ============================================================================
// --- TELEMETRY ---
// ============================================================================

// pendingCreate is an environment created through this replica whose guest
// has not answered yet.
type pendingCreate struct {
	atespace string
	start    time.Time
}

// Bounds on the create_to_ready bookkeeping. An environment that is created
// and never used, deleted or crashed through this replica (another replica
// served its calls, or the client went away) would otherwise stay pending
// forever. Entries older than pendingTTL are dropped unobserved when the map
// is full; maxAtespaces caps the atespace label of ate_env_api_environments.
const (
	maxPending   = 10000
	pendingTTL   = time.Hour
	maxAtespaces = 64
)

// environmentListConcurrency bounds the ListActors calls EnvironmentCounts
// has in flight, so a scrape over up to maxAtespaces atespaces finishes
// within the collector's timeout without flooding the control plane.
const environmentListConcurrency = 8

// requestedAt returns the start of create_to_ready for a CreateEnvironment
// call received at now: the client's x-ate-env-requested-at when it is a
// valid RFC 3339 time no later than now, else now. A time in the future
// (client clock ahead) falls back to now rather than producing a negative
// or shortened duration.
func requestedAt(ctx context.Context, now time.Time) time.Time {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return now
	}
	vals := md.Get(telemetry.RequestedAtHeader)
	if len(vals) == 0 {
		return now
	}
	t, err := time.Parse(time.RFC3339Nano, vals[0])
	if err != nil || t.After(now) {
		return now
	}
	return t
}

// trackCreate starts the create_to_ready clock of a created environment.
func (s *Server) trackCreate(atespace, id string, start time.Time) {
	if s.Metrics == nil {
		return
	}
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if s.pending == nil {
		s.pending = make(map[string]pendingCreate)
	}
	if s.atespaces == nil {
		s.atespaces = map[string]bool{DefaultAtespace: true}
	}
	if !s.atespaces[atespace] && len(s.atespaces) < maxAtespaces {
		s.atespaces[atespace] = true
		s.Metrics.InitAtespace(atespace)
	}
	if len(s.pending) >= maxPending {
		cutoff := time.Now().Add(-pendingTTL)
		for k, p := range s.pending {
			if p.start.Before(cutoff) {
				delete(s.pending, k)
			}
		}
		if len(s.pending) >= maxPending {
			return
		}
	}
	s.pending[atespace+"/"+id] = pendingCreate{atespace: atespace, start: start}
}

// ObserveFirstContact ends create_to_ready (status ok) for an environment
// whose port the port proxy reached: like a guest RPC through ensureAwake,
// a CONNECT the router accepts means the environment answers. It does
// nothing for an environment that is not pending, or without Metrics.
func (s *Server) ObserveFirstContact(atespace, id string) {
	s.finishCreate(atespace, id, telemetry.ReadyStatusOK)
}

// ObserveCrashedContact ends create_to_ready (status crashed) for an
// environment that the port proxy found CRASHED behind a router answer, as
// ensureAwake does for a guest call. It does nothing for an environment that
// is not pending, or without Metrics.
func (s *Server) ObserveCrashedContact(atespace, id string) {
	s.finishCreate(atespace, id, telemetry.ReadyStatusCrashed)
}

// finishCreate observes create_to_ready with readyStatus for a pending
// environment, once; later calls for the same environment do nothing.
func (s *Server) finishCreate(atespace, id, readyStatus string) {
	if s.Metrics == nil {
		return
	}
	key := atespace + "/" + id
	s.readyMu.Lock()
	p, ok := s.pending[key]
	if ok {
		delete(s.pending, key)
	}
	s.readyMu.Unlock()
	if ok {
		s.Metrics.ObserveCreateToReady(p.atespace, readyStatus, time.Since(p.start))
	}
}

// environmentStates are the state label values of ate_env_api_environments,
// reported as zero when no environment is in them so the series stay put.
var environmentStates = []string{"unspecified", "resuming", "running", "suspending", "suspended", "crashed", "deleting"}

func environmentState(st *ateapipb.ActorStatus) string {
	return strings.ToLower(strings.TrimPrefix(ActorStatusToEnvStatus(st).String(), "ENVIRONMENT_STATUS_"))
}

// EnvironmentCounts lists the actors of every atespace this replica has
// created environments in, DefaultAtespace always included, and counts them
// by environment state. It is the source of ate_env_api_environments: the
// control plane is the truth, so the gauge survives restarts and agrees
// across replicas, at the cost of one ListActors per atespace per scrape
// (cached by the collector for --metrics-environments-ttl, default 60 s). The atespaces are listed concurrently,
// at most environmentListConcurrency at a time.
func (s *Server) EnvironmentCounts(ctx context.Context) ([]telemetry.EnvironmentCount, error) {
	s.readyMu.Lock()
	spaces := []string{DefaultAtespace}
	for a := range s.atespaces {
		if a != DefaultAtespace {
			spaces = append(spaces, a)
		}
	}
	s.readyMu.Unlock()
	return countEnvironments(ctx, spaces, environmentListConcurrency, s.client.List)
}

// countEnvironments lists the actors of each atespace in spaces with list,
// running at most limit calls at once, and counts them by environment
// state. The first error cancels the calls still running and is returned.
func countEnvironments(ctx context.Context, spaces []string, limit int,
	list func(ctx context.Context, atespace string) ([]*ateapipb.Actor, error)) ([]telemetry.EnvironmentCount, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([][]*ateapipb.Actor, len(spaces))
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	sem := make(chan struct{}, max(limit, 1))
	for i, a := range spaces {
		sem <- struct{}{}
		if ctx.Err() != nil {
			<-sem
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			actors, err := list(ctx, a)
			if err != nil {
				errOnce.Do(func() {
					firstErr = err
					cancel()
				})
				return
			}
			results[i] = actors
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	out := make([]telemetry.EnvironmentCount, 0, len(spaces)*len(environmentStates))
	for i, a := range spaces {
		counts := make(map[string]int, len(environmentStates))
		for _, st := range environmentStates {
			counts[st] = 0
		}
		for _, actor := range results[i] {
			counts[environmentState(actor.GetStatus())]++
		}
		for st, n := range counts {
			out = append(out, telemetry.EnvironmentCount{Atespace: a, State: st, Count: n})
		}
	}
	return out, nil
}

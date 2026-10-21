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
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/telemetry"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	// Metrics, when set, receives the create_to_ready and wake_wait
	// observations and tracks the atespaces ate_env_api_environments
	// reports (see EnvironmentCounts). Nil records nothing.
	Metrics *telemetry.Metrics

	readyMu   sync.Mutex
	pending   map[string]pendingCreate // atespace/id -> creation start, until the guest first answers
	atespaces map[string]bool          // atespaces environments were created in, for EnvironmentCounts
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
// creates on the same image, and the response reports it in
// Environment.template. When rootfs_image is empty the template is used as is.
//
// Errors: InvalidArgument for a missing id, a rootfs image not pinned by digest, a
// derived name that is too long or not a valid resource name, or a base that
// cannot be derived from (no containers, unpinned guest image, or a command
// that cannot be re-rooted under the guest volume). FailedPrecondition when
// the base template does not exist in the atespace, or when the derived name
// is already held by a template running a different image. Other control
// plane failures are mapped by toGRPCError.
func (s *Server) CreateEnvironment(ctx context.Context, req *ateenvv1alpha.CreateEnvironmentRequest) (resp *ateenvv1alpha.CreateEnvironmentResponse, retErr error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	var start time.Time
	if s.Metrics != nil {
		start = requestedAt(ctx, time.Now())
	}
	tmpl := req.GetTemplate()
	templateName := tmpl.GetName()
	if templateName == "" {
		templateName = DefaultTemplate
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = tmpl.GetAtespace()
	}
	if atespace == "" {
		atespace = DefaultAtespace
	}
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
		name, err := s.ensureImageTemplate(ctx, atespace, templateName, image)
		if err != nil {
			return nil, err
		}
		templateName = name
	}

	opts := ate.CreateOptions{
		ID:       req.GetId(),
		Template: templateName,
		Atespace: atespace,
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
			Status: ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_UNSPECIFIED,
		},
	}, nil
}

// ensureImageTemplate returns the name of the ActorTemplate that runs image
// on top of base, creating it from base on first use. The derived template
// lives in the environment's atespace, next to base.
func (s *Server) ensureImageTemplate(ctx context.Context, atespace, base, image string) (string, error) {
	name, err := ImageTemplateName(base, image)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	existing, err := s.client.GetActorTemplate(ctx, atespace, name)
	switch {
	case err == nil:
		if got := firstContainerImage(existing); got != image {
			return "", status.Errorf(codes.FailedPrecondition,
				"actor template %q exists but runs %q, not %q; delete it or use another base template", name, got, image)
		}
		return name, nil
	case !errors.Is(err, ate.ErrNotFound):
		return "", toGRPCError(err)
	}
	baseTmpl, err := s.client.GetActorTemplate(ctx, atespace, base)
	if err != nil {
		if errors.Is(err, ate.ErrNotFound) {
			return "", status.Errorf(codes.FailedPrecondition,
				"base actor template %q not found in atespace %q; register it first (ate-env manifest template)", base, atespace)
		}
		return "", toGRPCError(err)
	}
	derived, err := DeriveImageTemplate(baseTmpl, name, image)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	// A concurrent create may have won the race; it built the same template.
	if _, err := s.client.CreateActorTemplate(ctx, derived); err != nil && !errors.Is(err, ate.ErrAlreadyExists) {
		return "", toGRPCError(err)
	}
	return name, nil
}

func firstContainerImage(t *ateapipb.ActorTemplate) string {
	if len(t.GetContainers()) == 0 {
		return ""
	}
	return t.GetContainers()[0].GetImage()
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

	if err := s.client.Suspend(ctx, atespace, req.GetId()); err != nil {
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
	client, outCtx, closeFn, err := s.processClient(stream.Context())
	if err != nil {
		return err
	}
	defer closeFn()

	clientStream, err := client.WriteProcessInput(outCtx)
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := clientStream.Send(req); err != nil {
			// The guest rejected the stream; surface its status instead of io.EOF.
			if errors.Is(err, io.EOF) {
				_, err = clientStream.CloseAndRecv()
			}
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
	conn, err := s.guestConn(ctx, atespace, envID)
	if err != nil {
		return err
	}
	defer conn.Close()

	clientStream, err := ateenvv1alpha.NewFileSystemServiceClient(conn).WriteFile(ctx)
	if err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := clientStream.Send(req); err != nil {
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
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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
		Status: ActorStatusToEnvStatus(actor.GetStatus()),
	}
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
		// Since substrate#2620 a paused actor is SUSPENDED too, with a
		// node-local latest snapshot; Substrate has no PAUSED or PAUSING
		// state. ate-env never pauses, so ENVIRONMENT_STATUS_PAUSED and
		// _PAUSING are no longer produced.
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_SUSPENDED
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_CRASHED
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_DELETING
	default:
		return ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_UNSPECIFIED
	}
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
// (cached for 15 s by the collector).
func (s *Server) EnvironmentCounts(ctx context.Context) ([]telemetry.EnvironmentCount, error) {
	s.readyMu.Lock()
	spaces := []string{DefaultAtespace}
	for a := range s.atespaces {
		if a != DefaultAtespace {
			spaces = append(spaces, a)
		}
	}
	s.readyMu.Unlock()

	var out []telemetry.EnvironmentCount
	for _, a := range spaces {
		actors, err := s.client.List(ctx, a)
		if err != nil {
			return nil, err
		}
		counts := make(map[string]int, len(environmentStates))
		for _, st := range environmentStates {
			counts[st] = 0
		}
		for _, actor := range actors {
			counts[environmentState(actor.GetStatus())]++
		}
		for st, n := range counts {
			out = append(out, telemetry.EnvironmentCount{Atespace: a, State: st, Count: n})
		}
	}
	return out, nil
}

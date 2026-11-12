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

// Package fakecontrol implements an in-memory fake of the Substrate ateapi
// Control service for tests. It mimics the control plane's lifecycle
// semantics: actors are created suspended, resume/suspend/pause flip
// status, and, like Substrate v0.6.0, an actor in any state can be deleted
// (a CRASHED one too, unless KeepCrashed mirrors today's refusal). Tests
// inject the failures of a control plane under load with FailCreates,
// FailResumes, FailDeletes and, on state reads, FailGets.
package fakecontrol

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Server is a fake ateapi Control server.
type Server struct {
	ateapipb.UnimplementedControlServer

	mu        sync.Mutex
	actors    map[string]*ateapipb.Actor
	atespaces map[string]*ateapipb.Atespace
	// DenyCreateAtespace makes CreateAtespace fail with PermissionDenied, as
	// Substrate does for an identity allowed to use but not create atespaces.
	DenyCreateAtespace bool
	templates          map[string]*ateapipb.ActorTemplate
	// lastTemplateCreateMD is the incoming metadata of the last
	// CreateActorTemplate call, for tests of the preview opt-in.
	lastTemplateCreateMD metadata.MD
	templateSeq          int
	// RequireTemplate makes CreateActor fail with FailedPrecondition when
	// the actor's template does not exist, as Substrate does.
	RequireTemplate bool
	// DeleteTemplateErr, when set, is the error of each DeleteActorTemplate.
	DeleteTemplateErr error
	// templateLeases are the templates whose lease a test holds, as the
	// template reconciler or a running delete does.
	templateLeases map[string]bool
	// templateDeletes and templateCreates count the DeleteActorTemplate
	// and CreateActorTemplate calls.
	templateDeletes, templateCreates int
	// listCalls counts the ListActors calls.
	listCalls int
	// SuspendModes records the durability mode of each SuspendActor call.
	SuspendModes []ateapipb.DurabilityMode
	// created records every actor CreateActor accepted, for Created.
	created []*ateapipb.Actor
	// workers are the Workers ListWorkers reports (AddWorker seeds them).
	// CreateActor places an actor whose selector some ACTIVE worker with an
	// actor capacity carries in its labels on such a worker, round-robin
	// among them (always the first with SameNode): status.assigned_node and
	// worker_assignment.node_name are set. With no workers, or none
	// matching, the actor stays unplaced.
	workers      []*ateapipb.Worker
	placeCounter int
	SameNode     bool
	// PendingGets makes each new actor report RESUMING for that many GetActor
	// calls before RUNNING, as a fresh actor does while its wakeup probe runs.
	PendingGets int
	pendingGets map[string]int
	// DenyListWorkers makes ListWorkers fail with PermissionDenied, as
	// Substrate does for an identity without the global viewer role.
	DenyListWorkers bool
	// ListWorkersUnavailable is how many ListWorkers calls answer
	// Unavailable first, as an api server restarting does.
	ListWorkersUnavailable int
	// OnListWorkers, when set, runs at the start of each ListWorkers call,
	// without the server's lock. Set it before the calls start.
	OnListWorkers func()
	// BornSuspended makes CreateActor leave a new actor SUSPENDED and
	// unplaced, as Substrate does: only ResumeActor (the router's wake on a
	// guest call, or a direct resume) places it on a worker and runs it. Off,
	// a new actor is RUNNING and placed at once, a shortcut most tests rely
	// on; a test of a path that must resume turns this on.
	BornSuspended bool
	resumes       int
	// failCreates, failResumes and failDeletes make the next calls of
	// CreateActor, ResumeActor and DeleteActor fail, as a control plane
	// under load does; see FailCreates, FailResumes and FailDeletes.
	failCreates, failResumes, failDeletes injectedFailure
	// createPlans scripts the next CreateActor calls of an actor by name,
	// one outcome per call; see PlanCreates.
	createPlans map[string][]CreateOutcome
	// crashResumes makes the next ResumeActor calls crash their actor, as a
	// restore that fails does; see CrashResumes.
	crashResumes injectedFailure
	// KeepCrashed makes DeleteActor refuse a CRASHED actor with
	// FailedPrecondition, as Substrate does today (its DeleteVolumes
	// prerequisite is not met); off, an actor in any state is deleted.
	KeepCrashed bool
	// creates and deletes count the CreateActor and DeleteActor calls,
	// failed ones too; maxActors is the most actors that existed at once.
	creates, deletes, maxActors int
	// failGets makes the next GetActor calls fail, as a control plane under
	// load does; see FailGets.
	failGets injectedFailure
}

// injectedFailure fails the next n calls of one RPC with code and msg.
type injectedFailure struct {
	n    int
	code codes.Code
	msg  string
}

// take returns the injected error and spends one failure, or nil when none
// is left. Called with the lock held.
func (f *injectedFailure) take() error {
	if f.n <= 0 {
		return nil
	}
	f.n--
	return status.Error(f.code, f.msg)
}

// FailCreates makes the next n CreateActor calls fail with code and msg,
// before anything is stored, as a transient failure under load does.
func (s *Server) FailCreates(n int, code codes.Code, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failCreates = injectedFailure{n: n, code: code, msg: msg}
}

// CreateOutcome is what one planned CreateActor call does: it fails with
// Code and Msg, after storing the actor when Stored is set, as a create that
// landed while its reply was lost does.
type CreateOutcome struct {
	Code   codes.Code
	Msg    string
	Stored bool
}

// PlanCreates scripts the next CreateActor calls for the actor named name,
// one outcome per call in order; once the plan is spent, a call proceeds as
// usual (and finds the actor there when an outcome stored it). A planned
// call is counted like any other. Plans go before FailCreates.
func (s *Server) PlanCreates(name string, outcomes ...CreateOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createPlans == nil {
		s.createPlans = map[string][]CreateOutcome{}
	}
	s.createPlans[name] = append(s.createPlans[name], outcomes...)
}

// CrashResumes makes the next n ResumeActor calls fail Internal with msg and
// leave their actor CRASHED with msg as the crash reason, as a restore that
// fails does.
func (s *Server) CrashResumes(n int, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.crashResumes = injectedFailure{n: n, code: codes.Internal, msg: msg}
}

// ActorNames returns the names of the actors that exist, in no order.
func (s *Server) ActorNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.actors))
	for _, a := range s.actors {
		out = append(out, a.GetMetadata().GetName())
	}
	return out
}

// FailResumes makes the next n ResumeActor calls fail with code and msg,
// leaving the actor as it was.
func (s *Server) FailResumes(n int, code codes.Code, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failResumes = injectedFailure{n: n, code: code, msg: msg}
}

// FailDeletes makes the next n DeleteActor calls fail with code, leaving the
// actor in place.
func (s *Server) FailDeletes(n int, code codes.Code) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failDeletes = injectedFailure{n: n, code: code, msg: "injected delete failure"}
}

// Creates and Deletes are how many CreateActor and DeleteActor calls the fake
// took, failed ones included.
func (s *Server) Creates() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.creates
}

func (s *Server) Deletes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes
}

// MaxActors is the most actors that existed at once.
func (s *Server) MaxActors() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActors
}

// FailGets makes the next n GetActor calls fail with code, leaving the actor
// as it was, as a state read that the control plane drops, times out or
// rate-limits does.
func (s *Server) FailGets(n int, code codes.Code) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failGets = injectedFailure{n: n, code: code, msg: "injected get failure"}
}

// New returns an empty fake control server.
func New() *Server {
	return &Server{
		actors:    make(map[string]*ateapipb.Actor),
		atespaces: make(map[string]*ateapipb.Atespace),
		templates: make(map[string]*ateapipb.ActorTemplate),
	}
}

// AddWorker seeds a Worker, as a WorkerPool's pod registers one.
func (s *Server) AddWorker(w *ateapipb.Worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers = append(s.workers, proto.Clone(w).(*ateapipb.Worker))
}

// Worker builds an ACTIVE worker of pool in namespace on node with labels
// and an actor capacity of slots (0: none reported), for AddWorker.
func Worker(namespace, pool, node string, labels map[string]string, slots int32) *ateapipb.Worker {
	w := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: pool + "-" + node},
		WorkerNamespace: namespace,
		WorkerPool:      pool,
		WorkerPod:       pool + "-" + node,
		NodeName:        node,
		Labels:          labels,
		Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
	}
	if slots > 0 {
		w.Status.Capacity = &ateapipb.WorkerResources{Actors: slots}
	}
	return w
}

func (s *Server) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	if s.OnListWorkers != nil {
		s.OnListWorkers()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.DenyListWorkers {
		return nil, status.Error(codes.PermissionDenied, "not allowed to list workers")
	}
	if s.ListWorkersUnavailable > 0 {
		s.ListWorkersUnavailable--
		return nil, status.Error(codes.Unavailable, "api server restarting")
	}
	resp := &ateapipb.ListWorkersResponse{}
	for _, w := range s.workers {
		resp.Workers = append(resp.Workers, proto.Clone(w).(*ateapipb.Worker))
	}
	return resp, nil
}

func (s *Server) GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest) (*ateapipb.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workers {
		if w.GetMetadata().GetName() == req.GetWorker().GetName() {
			return proto.Clone(w).(*ateapipb.Worker), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "worker %q not found", req.GetWorker().GetName())
}

// place assigns a new actor to a worker whose labels carry its selector,
// when there is one. As Substrate's room check, it skips a worker without an
// actor capacity (registered, none reported yet) and one not ACTIVE. Called
// with the lock held.
func (s *Server) place(a *ateapipb.Actor) {
	var candidates []*ateapipb.Worker
	for _, w := range s.workers {
		if w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE || w.GetStatus().GetCapacity().GetActors() <= 0 {
			continue
		}
		ok := true
		for k, v := range a.GetWorkerSelector().GetMatchLabels() {
			if w.GetLabels()[k] != v {
				ok = false
				break
			}
		}
		if ok {
			candidates = append(candidates, w)
		}
	}
	if len(candidates) == 0 {
		return
	}
	idx := 0
	if !s.SameNode {
		idx = s.placeCounter % len(candidates)
	}
	s.placeCounter++
	w := candidates[idx]
	a.Status.AssignedNode = w.GetNodeName()
	a.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
		WorkerNamespace: w.GetWorkerNamespace(),
		WorkerPool:      w.GetWorkerPool(),
		WorkerPod:       w.GetWorkerPod(),
		NodeName:        w.GetNodeName(),
	}
}

// ActorCount is how many actors exist.
func (s *Server) ActorCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.actors)
}

// Created returns a copy of every actor CreateActor accepted, in order,
// whether or not it still exists.
func (s *Server) Created() []*ateapipb.Actor {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*ateapipb.Actor, 0, len(s.created))
	for _, a := range s.created {
		out = append(out, clone(a))
	}
	return out
}

// RemoveTemplate drops a stored ActorTemplate, as a delete behind the API's
// back would.
func (s *Server) RemoveTemplate(atespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.templates, key(atespace, name))
}

// AddTemplate seeds an ActorTemplate, as an operator would have registered it.
func (s *Server) AddTemplate(tmpl *ateapipb.ActorTemplate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.templates[key(tmpl.GetMetadata().GetAtespace(), tmpl.GetMetadata().GetName())] = proto.Clone(tmpl).(*ateapipb.ActorTemplate)
}

// Template returns a copy of a stored ActorTemplate, or nil.
func (s *Server) Template(atespace, name string) *ateapipb.ActorTemplate {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.templates[key(atespace, name)]
	if !ok {
		return nil
	}
	return proto.Clone(t).(*ateapipb.ActorTemplate)
}

// LastTemplateCreateMetadata returns the incoming gRPC metadata of the last
// CreateActorTemplate call, or nil before the first.
func (s *Server) LastTemplateCreateMetadata() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastTemplateCreateMD.Copy()
}

// TemplateCount is how many ActorTemplates are stored.
func (s *Server) TemplateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.templates)
}

func (s *Server) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref := req.GetActorTemplate()
	t, ok := s.templates[key(ref.GetAtespace(), ref.GetName())]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor template %q not found", ref.GetName())
	}
	return proto.Clone(t).(*ateapipb.ActorTemplate), nil
}

// HoldTemplateLease holds the lease of a template until release is called:
// meanwhile DeleteActorTemplate on it fails with Aborted, as in Substrate
// while the template reconciler or another delete holds it.
func (s *Server) HoldTemplateLease(atespace, name string) (release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.templateLeases == nil {
		s.templateLeases = make(map[string]bool)
	}
	s.templateLeases[key(atespace, name)] = true
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.templateLeases, key(atespace, name))
	}
}

// ListCalls returns how many ListActors calls the fake got.
func (s *Server) ListCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalls
}

// MutateActor applies f to the stored actor with the given name, for tests
// that need a status the fake's lifecycle does not produce.
func (s *Server) MutateActor(name string, f func(*ateapipb.Actor)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.find(name); a != nil {
		f(a)
	}
}

// TemplateCalls returns how many DeleteActorTemplate and CreateActorTemplate
// calls the fake got.
func (s *Server) TemplateCalls() (deletes, creates int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.templateDeletes, s.templateCreates
}

func (s *Server) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.templateCreates++
	s.lastTemplateCreateMD, _ = metadata.FromIncomingContext(ctx)
	t := req.GetActorTemplate()
	md := t.GetMetadata()
	if md.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "metadata.name is required")
	}
	k := key(md.GetAtespace(), md.GetName())
	if _, exists := s.templates[k]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "actor template %q already exists", md.GetName())
	}
	// Mirror the server's volume_mounts validation so a bad derivation fails here too.
	declared := map[string]bool{}
	for _, v := range t.GetVolumes() {
		declared[v.GetName()] = true
	}
	for _, c := range t.GetContainers() {
		for _, m := range c.GetVolumeMounts() {
			if !declared[m.GetName()] {
				return nil, status.Errorf(codes.InvalidArgument, "container %q mounts undeclared volume %q", c.GetName(), m.GetName())
			}
		}
	}
	stored := proto.Clone(t).(*ateapipb.ActorTemplate)
	s.templateSeq++
	stored.Metadata.Uid = fmt.Sprintf("tmpl-%s-%d", md.GetName(), s.templateSeq)
	stored.Metadata.Version = 1
	s.templates[k] = stored
	return proto.Clone(stored).(*ateapipb.ActorTemplate), nil
}

// DeleteActorTemplate deletes a template, with Substrate's uid and version
// preconditions: a mismatch is Aborted.
func (s *Server) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.templateDeletes++
	if s.DeleteTemplateErr != nil {
		return nil, s.DeleteTemplateErr
	}
	ref := req.GetActorTemplate()
	k := key(ref.GetAtespace(), ref.GetName())
	if s.templateLeases[k] {
		return nil, status.Errorf(codes.Aborted, "another operation is in progress for this ActorTemplate %s", ref.GetName())
	}
	t, ok := s.templates[k]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "ActorTemplate %s not found", ref.GetName())
	}
	if uid := req.GetOptions().GetUid(); uid != "" && uid != t.GetMetadata().GetUid() {
		return nil, status.Errorf(codes.Aborted, "ActorTemplate %s does not have uid %s", ref.GetName(), uid)
	}
	if v := req.GetOptions().GetVersion(); v != 0 && v != t.GetMetadata().GetVersion() {
		return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
	}
	delete(s.templates, k)
	return proto.Clone(t).(*ateapipb.ActorTemplate), nil
}

// Serve starts the fake on a random localhost port and returns its
// address and a shutdown function. Like the real ateapi, it serves TLS
// with a certificate no client can verify (clients set SkipVerify).
func (s *Server) Serve() (addr string, stop func(), err error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	cert, err := selfSignedCert()
	if err != nil {
		lis.Close()
		return "", nil, err
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	})))
	ateapipb.RegisterControlServer(grpcServer, s)
	go grpcServer.Serve(lis)
	return lis.Addr().String(), grpcServer.Stop, nil
}

// Client returns an in-process ControlClient backed directly by s without
// network sockets or background goroutines, suitable for testing/synctest
// bubbles via ate.NewControlClient(s.Client()).
func (s *Server) Client() ateapipb.ControlClient {
	return inProcessControlClient{s: s}
}

type inProcessControlClient struct {
	ateapipb.ControlClient
	s *Server
}

func ctxErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func (c inProcessControlClient) GetActor(ctx context.Context, in *ateapipb.GetActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.GetActor(ctx, in)
}

func (c inProcessControlClient) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.CreateActor(ctx, in)
}

func (c inProcessControlClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, _ ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.ResumeActor(ctx, in)
}

func (c inProcessControlClient) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.SuspendActor(ctx, in)
}

func (c inProcessControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.DeleteActor(ctx, in)
}

func (c inProcessControlClient) ListActors(ctx context.Context, in *ateapipb.ListActorsRequest, _ ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.ListActors(ctx, in)
}

func (c inProcessControlClient) CreateAtespace(ctx context.Context, in *ateapipb.CreateAtespaceRequest, _ ...grpc.CallOption) (*ateapipb.Atespace, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.CreateAtespace(ctx, in)
}

func (c inProcessControlClient) GetAtespace(ctx context.Context, in *ateapipb.GetAtespaceRequest, _ ...grpc.CallOption) (*ateapipb.Atespace, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.GetAtespace(ctx, in)
}

func (c inProcessControlClient) GetActorTemplate(ctx context.Context, in *ateapipb.GetActorTemplateRequest, _ ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.GetActorTemplate(ctx, in)
}

func (c inProcessControlClient) CreateActorTemplate(ctx context.Context, in *ateapipb.CreateActorTemplateRequest, _ ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.CreateActorTemplate(ctx, in)
}

func (c inProcessControlClient) DeleteActorTemplate(ctx context.Context, in *ateapipb.DeleteActorTemplateRequest, _ ...grpc.CallOption) (*ateapipb.ActorTemplate, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.DeleteActorTemplate(ctx, in)
}

func (c inProcessControlClient) ListWorkers(ctx context.Context, in *ateapipb.ListWorkersRequest, _ ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	return c.s.ListWorkers(ctx, in)
}

func selfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fakecontrol"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

func key(atespace, name string) string { return atespace + "/" + name }

// Status returns the current state of the actor with the given name, or
// ACTOR_STATE_UNSPECIFIED if it does not exist.
func (s *Server) Status(name string) ateapipb.ActorState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.find(name).GetStatus().GetState()
}

// SetState overrides an actor's state, for tests that need a state the fake's
// own transitions never produce, such as CRASHED, or need to hold an actor
// SUSPENDED after creation.
func (s *Server) SetState(name string, state ateapipb.ActorState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.find(name)
	if a == nil {
		return
	}
	if a.Status == nil {
		a.Status = &ateapipb.ActorStatus{}
	}
	a.Status.State = state
}

// Crash moves an actor to CRASHED with the control plane's crash reason, as
// a resume that cannot complete does.
func (s *Server) Crash(name, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.find(name)
	if a == nil {
		return
	}
	if a.Status == nil {
		a.Status = &ateapipb.ActorStatus{}
	}
	a.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
	a.Status.Crash = &ateapipb.ActorCrash{Message: message}
}

// find returns the actor with the given name, whichever atespace holds it, or
// nil when no actor has that name. The helpers above address actors by name
// alone so tests do not have to know which atespace a client placed them in.
func (s *Server) find(name string) *ateapipb.Actor {
	for _, a := range s.actors {
		if a.GetMetadata().GetName() == name {
			return a
		}
	}
	return nil
}

func (s *Server) get(ref *ateapipb.ObjectRef) (*ateapipb.Actor, error) {
	a, ok := s.actors[key(ref.GetAtespace(), ref.GetName())]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", ref.GetName())
	}
	return a, nil
}

func clone(a *ateapipb.Actor) *ateapipb.Actor {
	return proto.Clone(a).(*ateapipb.Actor)
}

func (s *Server) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failGets.take(); err != nil {
		return nil, err
	}
	a, err := s.get(req.GetActor())
	if err != nil {
		return nil, err
	}
	k := key(req.GetActor().GetAtespace(), req.GetActor().GetName())
	if n := s.pendingGets[k]; n > 0 && a.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
		s.pendingGets[k] = n - 1
		pending := clone(a)
		pending.Status.State = ateapipb.ActorState_ACTOR_STATE_RESUMING
		return pending, nil
	}
	return clone(a), nil
}

func (s *Server) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates++
	actor := req.GetActor()
	name := actor.GetMetadata().GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "metadata.name is required")
	}
	k := key(actor.GetMetadata().GetAtespace(), name)
	if plan := s.createPlans[name]; len(plan) > 0 {
		out := plan[0]
		s.createPlans[name] = plan[1:]
		if out.Stored && s.actors[k] == nil {
			s.store(k, actor)
		}
		return nil, status.Error(out.Code, out.Msg)
	}
	if err := s.failCreates.take(); err != nil {
		return nil, err
	}
	if _, ok := s.actors[k]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "actor %q already exists", name)
	}
	if ref := actor.GetActorTemplate(); s.RequireTemplate && s.templates[key(ref.GetAtespace(), ref.GetName())] == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "actor template not found; ObjectRef: %s/%s", ref.GetAtespace(), ref.GetName())
	}
	return clone(s.store(k, actor)), nil
}

// store keeps actor under k as an accepted create does: RUNNING and placed,
// or SUSPENDED and unplaced when BornSuspended; the slow-start reads, the
// high-water mark and the created log follow. Called with the lock held.
func (s *Server) store(k string, actor *ateapipb.Actor) *ateapipb.Actor {
	a := clone(actor)
	a.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}
	if s.BornSuspended {
		a.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	} else {
		s.place(a)
	}
	delete(s.pendingGets, k)
	if s.PendingGets > 0 {
		if s.pendingGets == nil {
			s.pendingGets = map[string]int{}
		}
		s.pendingGets[k] = s.PendingGets
	}
	s.actors[k] = a
	s.maxActors = max(s.maxActors, len(s.actors))
	s.created = append(s.created, clone(a))
	return a
}

func (s *Server) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.get(req.GetActor())
	if err != nil {
		return nil, err
	}
	s.resumes++
	if err := s.failResumes.take(); err != nil {
		return nil, err
	}
	if err := s.crashResumes.take(); err != nil {
		if a.Status == nil {
			a.Status = &ateapipb.ActorStatus{}
		}
		a.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		a.Status.Crash = &ateapipb.ActorCrash{Message: status.Convert(err).Message()}
		return nil, err
	}
	if s.BornSuspended {
		// Place as Substrate does: on a worker whose labels carry the
		// actor's selector; none is RESOURCE_EXHAUSTED, as a pool with no
		// room is. A RUNNING actor is returned as it is.
		if a.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			return &ateapipb.ResumeActorResponse{Actor: clone(a)}, nil
		}
		a.Status = &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING}
		s.place(a)
		if a.Status.WorkerAssignment == nil {
			a.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
			return nil, status.Errorf(codes.ResourceExhausted, "no worker matches the selector of actor %q", req.GetActor().GetName())
		}
		resp := clone(a)
		// A slow start (PendingGets): the resume answers before the actor
		// runs, and GetActor reports RESUMING for the next PendingGets
		// reads, as it does after a create.
		if s.pendingGets[key(req.GetActor().GetAtespace(), req.GetActor().GetName())] > 0 {
			resp.Status.State = ateapipb.ActorState_ACTOR_STATE_RESUMING
		}
		return &ateapipb.ResumeActorResponse{Actor: resp}, nil
	}
	// Placed at create (BornSuspended off): keep the placement, so the
	// node the create chose is the node the resume reports.
	if a.Status == nil || a.Status.WorkerAssignment == nil {
		a.Status = &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{
				WorkerNamespace: "ate-system",
				WorkerPod:       "worker-0",
				WorkerPodIps:    []string{"10.0.0.1"},
			},
		}
	}
	a.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
	return &ateapipb.ResumeActorResponse{Actor: clone(a)}, nil
}

// Resumes is how many ResumeActor calls the fake took.
func (s *Server) Resumes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resumes
}

func (s *Server) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.get(req.GetActor())
	if err != nil {
		return nil, err
	}
	// SuspendModes records each call, also a rejected one.
	s.SuspendModes = append(s.SuspendModes, req.GetOptions().GetMode())
	// Like Substrate v0.6.0: a SUSPENDED actor is a no-op, and only a
	// RUNNING or SUSPENDING one can be suspended.
	switch st := a.GetStatus().GetState(); st {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		return &ateapipb.SuspendActorResponse{Actor: clone(a)}, nil
	case ateapipb.ActorState_ACTOR_STATE_RUNNING, ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "MarkSuspending prerequisite not met for Actor: %s (got: %v, want %s)",
			req.GetActor().GetName(), st, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}
	s.suspend(a, req.GetOptions().GetMode())
	return &ateapipb.SuspendActorResponse{Actor: clone(a)}, nil
}

// suspend checkpoints a. The caller holds s.mu. A synchronous suspend leaves
// the snapshot in object storage; any other mode (asynchronous, which is also
// Substrate's default when unset) leaves it on a node with the upload pending,
// as Substrate does until atelet's background upload lands (CompleteUpload).
func (s *Server) suspend(a *ateapipb.Actor, mode ateapipb.DurabilityMode) {
	snap := &ateapipb.Snapshot{
		Uuid:       fmt.Sprintf("snapshot-%s", a.GetMetadata().GetName()),
		Generation: a.GetStatus().GetLastAssignedGeneration() + 1,
		Owner:      ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		Fidelity:   ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			State:  ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
			Object: &ateapipb.ObjectSnapshot{},
		},
	}
	if mode != ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
		snap.Locality = "node-1"
		snap.DurableSnapshot.State = ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
	}
	a.Status = &ateapipb.ActorStatus{
		State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		Snapshots:              []*ateapipb.Snapshot{snap},
		LastAssignedGeneration: snap.GetGeneration(),
	}
}

// CompleteUpload marks the actor's snapshots durable, as atelet's background
// upload after an asynchronous suspend does once it lands.
func (s *Server) CompleteUpload(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, snap := range s.find(name).GetStatus().GetSnapshots() {
		if snap.GetDurableSnapshot() != nil {
			snap.DurableSnapshot.State = ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		}
	}
}

func (s *Server) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	ref := req.GetActor()
	a, err := s.get(ref)
	if err != nil {
		return nil, err
	}
	if err := s.failDeletes.take(); err != nil {
		return nil, err
	}
	// Like Substrate v0.6.0, DeleteActor takes an actor in any state: it
	// terminates the workload itself. Except, today, a CRASHED one, with
	// KeepCrashed.
	if s.KeepCrashed && a.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED {
		return nil, status.Errorf(codes.FailedPrecondition, "DeleteVolumes prerequisite not met for Actor: %s (got: %v)", ref.GetName(), a.GetStatus().GetState())
	}
	delete(s.actors, key(ref.GetAtespace(), ref.GetName()))
	delete(s.pendingGets, key(ref.GetAtespace(), ref.GetName()))
	return clone(a), nil
}

func (s *Server) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	resp := &ateapipb.ListActorsResponse{}
	for _, a := range s.actors {
		if req.GetAtespace() != "" && a.GetMetadata().GetAtespace() != req.GetAtespace() {
			continue
		}
		resp.Actors = append(resp.Actors, clone(a))
	}
	return resp, nil
}

func (s *Server) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	space := req.GetAtespace()
	name := space.GetMetadata().GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "metadata.name is required")
	}
	if s.DenyCreateAtespace {
		return nil, status.Errorf(codes.PermissionDenied, "not allowed to create atespace %q", name)
	}
	if _, ok := s.atespaces[name]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "atespace %q already exists", name)
	}
	a := proto.Clone(space).(*ateapipb.Atespace)
	s.atespaces[name] = a
	return proto.Clone(a).(*ateapipb.Atespace), nil
}

func (s *Server) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := req.GetAtespace().GetName()
	a, ok := s.atespaces[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "atespace %q not found", name)
	}
	return proto.Clone(a).(*ateapipb.Atespace), nil
}

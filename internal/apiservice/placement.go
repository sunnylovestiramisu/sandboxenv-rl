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

package apiservice

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"maps"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/env/internal/ate"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// WarmImage limits and defaults. A round creates nodes x probes_per_node
// probe environments at once; MaxWarmProbes caps that so one call cannot
// fill a cluster.
const (
	DefaultWarmProbesPerNode = 2
	MaxWarmProbesPerNode     = 8
	DefaultWarmRounds        = 3
	MaxWarmRounds            = 10
	DefaultWarmProbeTimeout  = 10 * time.Minute
	MaxWarmProbes            = 256
	// DefaultWarmMaxProbesPerNode is the server's cap on the probes in
	// flight per node of one worker selector, across WarmImage calls (see
	// Server.WarmMaxProbesPerNode). It matches DefaultWarmProbesPerNode, so
	// one call with the defaults never waits.
	DefaultWarmMaxProbesPerNode = 2
	// DefaultWarmRunID prefixes the run id of a request that leaves it
	// empty; a random suffix makes each call's probe names its own, so two
	// calls in one atespace never create, or delete, each other's probes.
	DefaultWarmRunID   = "warm"
	warmRunIDSuffixLen = 6
	maxWarmRunIDLen    = 20

	warmPollInitial = 200 * time.Millisecond
	warmPollMax     = 2 * time.Second
	// warmCreateAttemptTimeout bounds one CreateActor attempt, so a call
	// that hangs while the control plane's database queues does not spend
	// the probe's whole budget before the retry. A resume has no such bound:
	// it returns once the template's wakeup probe answered, which a cold
	// restore takes minutes to do.
	warmCreateAttemptTimeout = 30 * time.Second
)

// The timings of a WarmImage probe's retries and delete. Atomic because a
// test sets them while the delete goroutines of an earlier test's call,
// which outlive that call, still read them; each function reads a knob once.
var (
	warmRetryInitial  = newAtomicDuration(500 * time.Millisecond)
	warmRetryMax      = newAtomicDuration(5 * time.Second)
	warmDeleteTimeout = newAtomicDuration(60 * time.Second)
	// warmLateCreateSettle is how long a probe whose create was never
	// confirmed (an attempt was cut off) is watched after its delete found
	// nothing: the control plane can still commit such a create after the
	// delete, and the call then deletes the probe again.
	warmLateCreateSettle = newAtomicDuration(2 * time.Second)
)

// atomicDuration is a duration read and written from different goroutines.
type atomicDuration struct{ ns atomic.Int64 }

func newAtomicDuration(d time.Duration) *atomicDuration {
	a := &atomicDuration{}
	a.Store(d)
	return a
}

func (a *atomicDuration) Load() time.Duration   { return time.Duration(a.ns.Load()) }
func (a *atomicDuration) Store(d time.Duration) { a.ns.Store(int64(d)) }

// warmLateCreateRechecks is how many more deletes an unconfirmed probe gets
// in its settle window, spread evenly, after one that found nothing. Few, so
// a call whose creates all time out does not load the control plane.
const warmLateCreateRechecks = 2

var warmRunIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// newWarmRunID is DefaultWarmRunID with a random suffix.
func newWarmRunID() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, warmRunIDSuffixLen)
	rand.Read(b) // never fails since Go 1.24
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return DefaultWarmRunID + "-" + string(b)
}

// ListPlacementDomains reports the worker pools a caller can address with a
// per-environment worker selector, read from the control plane's workers:
// the schedulable workers grouped by pool, with the labels all of them carry
// (the selector), their distinct nodes and the smallest actor capacity among
// them (the slots per node). Listing workers takes Substrate's global viewer
// role, which `ate-env manifest access-policy --global` grants; without it
// the call fails with PermissionDenied and WarmImage needs its nodes given.
func (s *Server) ListPlacementDomains(ctx context.Context, req *ateenvv1alpha.ListPlacementDomainsRequest) (*ateenvv1alpha.ListPlacementDomainsResponse, error) {
	// A template narrows the list to the pools its own selector matches; the
	// template is read first, so an unknown one is NotFound whether or not
	// the workers may be listed.
	filter := req.GetTemplate() != nil || req.GetAtespace() != ""
	var selector map[string]string
	if filter {
		name, atespace := templateDefaults(req.GetTemplate(), req.GetAtespace())
		tmpl, err := s.client.GetActorTemplate(ctx, atespace, name)
		if err != nil {
			return nil, toGRPCError(err)
		}
		selector = tmpl.GetWorkerSelector().GetMatchLabels()
	}
	workers, err := s.client.ListWorkers(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}
	domains := placementDomains(workers)
	if filter {
		domains = addressableDomains(domains, selector)
	}
	return &ateenvv1alpha.ListPlacementDomainsResponse{Domains: domains}, nil
}

// addressableDomains keeps the pools a worker_selector for a template with
// selector can address: Substrate ANDs the two, so a pool qualifies when its
// own selector carries every label of the template's, and a pool whose
// workers share no label is dropped, as no selector addresses it.
func addressableDomains(domains []*ateenvv1alpha.PlacementDomain, selector map[string]string) []*ateenvv1alpha.PlacementDomain {
	out := make([]*ateenvv1alpha.PlacementDomain, 0, len(domains))
	for _, d := range domains {
		if len(d.GetSelector()) > 0 && labelsCarry(d.GetSelector(), selector) {
			out = append(out, d)
		}
	}
	return out
}

// placementDomains groups the schedulable workers by pool, sorted by
// namespace and name.
func placementDomains(workers []*ateapipb.Worker) []*ateenvv1alpha.PlacementDomain {
	type pool struct {
		domain *ateenvv1alpha.PlacementDomain
		nodes  map[string]bool
	}
	pools := map[string]*pool{}
	for _, w := range workers {
		if !schedulable(w) {
			continue
		}
		k := w.GetWorkerNamespace() + "/" + w.GetWorkerPool()
		p := pools[k]
		if p == nil {
			p = &pool{
				domain: &ateenvv1alpha.PlacementDomain{
					Name:            w.GetWorkerPool(),
					WorkerNamespace: w.GetWorkerNamespace(),
					Selector:        maps.Clone(w.GetLabels()),
				},
				nodes: map[string]bool{},
			}
			pools[k] = p
		} else {
			// The selector is what every worker of the pool carries: a
			// label one worker lacks, or carries with another value, would
			// address part of the pool only.
			for key, v := range p.domain.Selector {
				if w.GetLabels()[key] != v {
					delete(p.domain.Selector, key)
				}
			}
		}
		p.domain.Workers++
		if n := w.GetNodeName(); n != "" {
			p.nodes[n] = true
		}
		if a := w.GetStatus().GetCapacity().GetActors(); p.domain.NodeSlots == 0 || a < p.domain.NodeSlots {
			p.domain.NodeSlots = a
		}
	}
	keys := make([]string, 0, len(pools))
	for k := range pools {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*ateenvv1alpha.PlacementDomain, 0, len(keys))
	for _, k := range keys {
		p := pools[k]
		p.domain.Nodes = int32(len(p.nodes))
		out = append(out, p.domain)
	}
	return out
}

// warmOptions are a WarmImageRequest's knobs with the defaults applied.
type warmOptions struct {
	probesPerNode int
	maxRounds     int
	probeTimeout  time.Duration
	runID         string
}

// parseWarmOptions applies the defaults and the limits to req. maxPerNode is
// the server's cap on the probes per node of one selector: probes_per_node
// defaults to DefaultWarmProbesPerNode or the cap, whichever is smaller, and
// a value above the cap is refused, because a round takes all its slots at
// once and could never get more than the cap.
func parseWarmOptions(req *ateenvv1alpha.WarmImageRequest, maxPerNode int) (warmOptions, error) {
	o := warmOptions{
		probesPerNode: int(req.GetProbesPerNode()),
		maxRounds:     int(req.GetMaxRounds()),
		probeTimeout:  req.GetProbeTimeout().AsDuration(),
		runID:         req.GetRunId(),
	}
	if req.GetRootfsImage() == "" {
		return o, status.Error(codes.InvalidArgument, "rootfs_image is required")
	}
	if req.GetNodes() < 0 {
		return o, status.Error(codes.InvalidArgument, "nodes must be >= 0")
	}
	switch {
	case o.probesPerNode == 0:
		o.probesPerNode = min(DefaultWarmProbesPerNode, maxPerNode)
	case o.probesPerNode < 0 || o.probesPerNode > MaxWarmProbesPerNode:
		return o, status.Errorf(codes.InvalidArgument, "probes_per_node must be between 1 and %d", MaxWarmProbesPerNode)
	case o.probesPerNode > maxPerNode:
		return o, status.Errorf(codes.InvalidArgument, "probes_per_node %d exceeds the %d probes per node this server runs on one worker selector at once (ate-env-api -warm-max-probes-per-node)", o.probesPerNode, maxPerNode)
	}
	switch {
	case o.maxRounds == 0:
		o.maxRounds = DefaultWarmRounds
	case o.maxRounds < 0 || o.maxRounds > MaxWarmRounds:
		return o, status.Errorf(codes.InvalidArgument, "max_rounds must be between 1 and %d", MaxWarmRounds)
	}
	switch {
	case req.GetProbeTimeout() == nil:
		o.probeTimeout = DefaultWarmProbeTimeout
	case o.probeTimeout <= 0:
		return o, status.Error(codes.InvalidArgument, "probe_timeout must be positive")
	}
	if o.runID == "" {
		o.runID = newWarmRunID()
	}
	if len(o.runID) > maxWarmRunIDLen || !warmRunIDPattern.MatchString(o.runID) {
		return o, status.Errorf(codes.InvalidArgument, "run_id must match %s and be at most %d characters", warmRunIDPattern, maxWarmRunIDLen)
	}
	return o, nil
}

// WarmImage puts one rootfs image on every node of one placement domain. It
// derives the image's template (as CreateEnvironment does), then runs rounds
// of probe environments created with the request's worker selector: a round
// starts nodes x probes_per_node probes at once, resumes each (a new actor is
// suspended and unplaced until a resume places it, which the router does on
// a guest call and a probe never gets) and holds it until the control plane
// reports it RUNNING, which it does after the template's wakeup probe
// answered, so the image layers and the golden snapshot are on the node by
// then; the node comes from the actor status. The round's probes are deleted
// together once all have settled, so they occupied distinct slots while the
// scheduler placed the rest. Rounds continue until every node ran the image
// or max_rounds is spent; a round in which nothing came up ends the call
// early, because more rounds would not change that. Each probe is streamed
// as it settles; the coverage closes the stream.
//
// The transient failures a control plane under load hands a probe's create
// and resume (an api server restarting, a call past its deadline while the
// database queues, a create a few seconds after the derived template was
// made) are retried within the probe's timeout, so one api-server restart
// does not fail every probe of a round, and with it the call. A probe's
// delete is retried too; a probe still there when the budget ends, which a
// CRASHED probe is because Substrate cannot delete one today, is listed in
// the coverage's undeleted_probes for the caller to sweep. A probe with a
// create attempt that was cut off can still be committed by the control
// plane after its delete, even when a later attempt was confirmed: its
// delete is repeated for a short settle window, and before the round gives
// its probe slots back it lists the atespace once and deletes any such probe
// that appeared later. When that list fails, such probes are listed in
// undeleted_probes, since the call cannot tell whether they are gone. A create
// that the control plane commits after the settle window and the round's list
// still leaks and is not listed.
//
// Across calls, the server caps the probes in flight per node of one worker
// selector (WarmMaxProbesPerNode): a round takes nodes x probes_per_node
// slots before it creates its probes and gives them back once they are
// deleted, so a pre-flight that fans out one call per image does not put
// more first boots on a node at once than the cap. A round that waits is
// logged, and the coverage's waited sums the wait.
//
// With nodes 0 the call counts the distinct nodes of the schedulable workers
// the selector matches (ANDed with the derived template's own selector),
// which takes ListPlacementDomains's permission. No command runs in the
// guest. A probe whose name exists already is another call's or a user's:
// it is reported as a failed create and left alone, unless an earlier create
// attempt of the probe was cut off and may have made it (the run id is
// unique per call, so the name is then this call's).
//
// Errors: InvalidArgument for a bad request, probes_per_node above the
// server's cap, nodes above the distinct nodes of the workers the selector
// matches (when the server can list them), or nodes x probes_per_node above
// MaxWarmProbes; FailedPrecondition when the base template is missing, the
// selector contradicts the template's or no active worker matches it;
// PermissionDenied when nodes is 0 and the server may not list workers
// (with nodes given, that permission's absence leaves nodes trusted); the
// control plane's own error when the workers cannot be listed for another
// reason after the transient failures are retried; Canceled or
// DeadlineExceeded when the stream ends first (the probes of the round are
// still deleted).
func (s *Server) WarmImage(req *ateenvv1alpha.WarmImageRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.WarmImageEvent]) error {
	ctx := stream.Context()
	maxPerNode := s.warmMaxProbesPerNode()
	opts, err := parseWarmOptions(req, maxPerNode)
	if err != nil {
		return err
	}
	baseName, atespace := templateDefaults(req.GetTemplate(), req.GetAtespace())
	templateName, tmpl, release, err := s.ensureImageTemplate(ctx, atespace, baseName, req.GetRootfsImage())
	if err != nil {
		return err
	}
	// The lease guards a recreate of the template during a create; the probes
	// below create by name and report a vanished template as a probe error.
	release()

	selector, err := warmSelector(tmpl.GetWorkerSelector().GetMatchLabels(), req.GetWorkerSelector())
	if err != nil {
		return err
	}
	// The workers give the node count a request leaves out and the count the
	// cap below multiplies. A request that claims more nodes than the
	// selector's workers run on would put more first boots on each real node
	// than the cap allows, so it is refused. Only the missing permission to
	// list the workers (PermissionDenied) leaves the request's nodes
	// trusted, as the README and the threat model say; a selector no active
	// worker carries, or a listing the control plane fails after the
	// transient failures are retried, fails the call.
	wanted := int(req.GetNodes())
	listed, lerr := s.listedNodes(ctx, selector)
	capNodes := wanted
	switch {
	case lerr == nil:
		if wanted == 0 {
			wanted = listed
		} else if wanted > listed {
			return status.Errorf(codes.InvalidArgument, "nodes %d exceeds the %d nodes the selector's workers run on", wanted, listed)
		}
		capNodes = listed
	case wanted > 0 && grpcCode(lerr) == codes.PermissionDenied:
		// Trusted: the cap multiplies the request's nodes.
	default:
		return lerr
	}
	width := wanted * opts.probesPerNode
	if width > MaxWarmProbes {
		return status.Errorf(codes.InvalidArgument, "%d nodes x %d probes per node exceed the %d probes a round may start", wanted, opts.probesPerNode, MaxWarmProbes)
	}
	// The atespace once, before the rounds: a probe's create is then one
	// CreateActor call, whose failure says whether the probe may exist.
	if err := s.client.EnsureAtespace(ctx, atespace); err != nil {
		return toGRPCError(err)
	}

	cov := &ateenvv1alpha.WarmImageCoverage{Wanted: int32(wanted), Nodes: []string{}, UndeletedProbes: []string{}}
	nodes := map[string]bool{}
	var sendMu sync.Mutex
	var sendErr error
	send := func(ev *ateenvv1alpha.WarmImageEvent) {
		sendMu.Lock()
		defer sendMu.Unlock()
		if sendErr == nil {
			sendErr = stream.Send(ev)
		}
	}
	// The cap on the selector: its nodes times the per-node cap, with the
	// nodes as the workers reported them, whatever the request asked to
	// cover, so two calls on one selector share one limit; the request's
	// nodes stand in only when the workers cannot be listed.
	key := selectorKey(selector)
	limit := capNodes * maxPerNode
	slots := s.warmSlots.get(key)
	var waited time.Duration
	for round := 1; round <= opts.maxRounds && len(nodes) < wanted && ctx.Err() == nil; round++ {
		cov.Rounds = int32(round)
		w, err := slots.acquire(ctx, width, limit, func(inflight int) {
			log.Printf("apiservice: warm %s round %d waits for %d probe slots on %s: %d of %d in flight, held by other warm calls", req.GetRootfsImage(), round, width, key, inflight, limit)
		})
		if err != nil {
			break
		}
		waited += w
		results := s.warmRound(ctx, atespace, templateName, req.GetWorkerSelector(), opts, round, width, send)
		undeleted, uncertain := s.deleteWarmProbes(ctx, atespace, results)
		// A probe whose create lands after its delete still holds a slot
		// until it is gone, so the round sweeps for such probes before it
		// gives its slots back.
		undeleted = s.sweepWarmProbes(ctx, atespace, uncertain, undeleted)
		slots.release(width)
		cov.UndeletedProbes = append(cov.UndeletedProbes, undeleted...)
		failed := 0
		for _, r := range results {
			cov.Probes++
			if r.err != nil {
				cov.Failed++
				failed++
				cov.Error = r.err.Error()
			} else {
				nodes[r.node] = true
			}
		}
		if sendErr != nil {
			return sendErr
		}
		if failed == width {
			log.Printf("apiservice: warm %s in %v: every probe of round %d failed: %v", req.GetRootfsImage(), req.GetWorkerSelector(), round, cov.Error)
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	for n := range nodes {
		cov.Nodes = append(cov.Nodes, n)
	}
	sort.Strings(cov.Nodes)
	cov.Complete = len(cov.Nodes) >= wanted
	if waited > 0 {
		cov.Waited = durationpb.New(waited)
	}
	return stream.Send(&ateenvv1alpha.WarmImageEvent{Event: &ateenvv1alpha.WarmImageEvent_Coverage{Coverage: cov}})
}

// warmRound starts width probes at once, streams each as it settles and
// returns their results once all have.
func (s *Server) warmRound(ctx context.Context, atespace, template string, selector map[string]string, opts warmOptions, round, width int, send func(*ateenvv1alpha.WarmImageEvent)) []warmResult {
	results := make([]warmResult, width)
	var wg sync.WaitGroup
	for k := range width {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			id := fmt.Sprintf("%s-%d-%d", opts.runID, round, k)
			results[k] = s.warmProbe(ctx, atespace, template, selector, id, opts.probeTimeout)
			r := results[k]
			send(&ateenvv1alpha.WarmImageEvent{Event: &ateenvv1alpha.WarmImageEvent_Probe{Probe: &ateenvv1alpha.WarmProbe{
				Id: r.id, Node: r.node, Round: int32(round), Duration: durationpb.New(r.dur), Error: errMessage(r.err),
			}}})
		}(k)
	}
	wg.Wait()
	return results
}

// deleteWarmProbes deletes the probes of a settled round that this call made,
// together, and returns the ids of the ones still there, sorted, plus the
// set of uncertain probes.
func (s *Server) deleteWarmProbes(ctx context.Context, atespace string, results []warmResult) ([]string, map[string]bool) {
	var mu sync.Mutex
	var undeleted []string
	uncertain := map[string]bool{}
	var wg sync.WaitGroup
	for _, r := range results {
		if r.uncertain {
			uncertain[r.id] = true
		}
		if !r.created {
			continue
		}
		wg.Add(1)
		go func(id string, uncertain bool) {
			defer wg.Done()
			if err := s.deleteWarmProbe(ctx, atespace, id, uncertain); err != nil {
				mu.Lock()
				undeleted = append(undeleted, id)
				mu.Unlock()
			}
		}(r.id, r.uncertain)
	}
	wg.Wait()
	sort.Strings(undeleted)
	return undeleted, uncertain
}

// warmMaxProbesPerNode is WarmMaxProbesPerNode with the default applied.
func (s *Server) warmMaxProbesPerNode() int {
	if s.WarmMaxProbesPerNode > 0 {
		return s.WarmMaxProbesPerNode
	}
	return DefaultWarmMaxProbesPerNode
}

// selectorKey names a selector the same whatever the order of its labels:
// its key=value pairs, sorted, comma separated.
func selectorKey(selector map[string]string) string {
	parts := make([]string, 0, len(selector))
	for k, v := range selector {
		parts = append(parts, k+"="+v)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// warmSlotRegistry holds the probe slots of each worker selector this server
// warmed, by selectorKey. An entry is never dropped: one per selector seen,
// a few dozen bytes each.
type warmSlotRegistry struct {
	mu sync.Mutex
	m  map[string]*warmSlots
}

func (r *warmSlotRegistry) get(key string) *warmSlots {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*warmSlots{}
	}
	w := r.m[key]
	if w == nil {
		w = &warmSlots{changed: make(chan struct{})}
		r.m[key] = w
	}
	return w
}

// warmSlots counts the probes in flight on one worker selector across the
// WarmImage calls of this server. A round takes its slots in one piece and
// gives them back in one piece, never a part, so two rounds that each hold a
// part of the cap cannot wait on each other.
type warmSlots struct {
	mu       sync.Mutex
	inflight int
	// changed is closed, and replaced, on each release, to wake the waiters.
	changed chan struct{}
}

// acquire takes n slots once inflight+n fits limit, or returns ctx's error
// when ctx ends first, and reports how long it waited. onWait is called once,
// when the first try finds no room, with the slots in flight then.
func (w *warmSlots) acquire(ctx context.Context, n, limit int, onWait func(inflight int)) (time.Duration, error) {
	var start time.Time
	for {
		w.mu.Lock()
		if w.inflight+n <= limit {
			w.inflight += n
			w.mu.Unlock()
			if start.IsZero() {
				return 0, nil
			}
			return time.Since(start), nil
		}
		inflight, changed := w.inflight, w.changed
		w.mu.Unlock()
		if start.IsZero() {
			start = time.Now()
			if onWait != nil {
				onWait(inflight)
			}
		}
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-changed:
		}
	}
}

// release gives n slots back and wakes the waiters.
func (w *warmSlots) release(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight -= n
	close(w.changed)
	w.changed = make(chan struct{})
}

// schedulable reports whether the scheduler places actors on w: it is
// active and has an actor capacity. A worker that registered but has not
// reported its capacity yet has none, and Substrate's room check places
// nothing on it.
func schedulable(w *ateapipb.Worker) bool {
	return w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE &&
		w.GetStatus().GetCapacity().GetActors() > 0
}

// warmSelector is the request's selector ANDed with the template's, as
// Substrate matches a probe. A key both set to different values matches no
// worker, so it is refused here rather than failing every probe.
func warmSelector(template, request map[string]string) (map[string]string, error) {
	selector := maps.Clone(template)
	if selector == nil {
		selector = map[string]string{}
	}
	for k, v := range request {
		if tv, ok := selector[k]; ok && tv != v {
			return nil, status.Errorf(codes.FailedPrecondition, "worker_selector %s=%s conflicts with the template's %s=%s: no worker carries both", k, v, k, tv)
		}
		selector[k] = v
	}
	return selector, nil
}

// domainNodes counts the distinct nodes of the schedulable workers whose
// labels carry every pair of selector.
func (s *Server) domainNodes(ctx context.Context, selector map[string]string) (int, error) {
	workers, err := s.client.ListWorkers(ctx)
	if err != nil {
		if grpcCode(err) == codes.PermissionDenied {
			return 0, status.Errorf(codes.PermissionDenied, "nodes is 0 and ate-env-api may not list workers: pass nodes, or grant it the global viewer role (ate-env manifest access-policy --global): %v", err)
		}
		return 0, toGRPCError(err)
	}
	nodes := map[string]bool{}
	for _, w := range workers {
		if !schedulable(w) || !labelsCarry(w.GetLabels(), selector) {
			continue
		}
		if n := w.GetNodeName(); n != "" {
			nodes[n] = true
		}
	}
	if len(nodes) == 0 {
		return 0, status.Errorf(codes.FailedPrecondition, "no active worker matches the selector %v", selector)
	}
	return len(nodes), nil
}

// listedNodes is domainNodes with the transient failures of ListWorkers
// (an api server restarting, a deadline on the call) retried a few times
// inside ctx; PermissionDenied and FailedPrecondition are final.
func (s *Server) listedNodes(ctx context.Context, selector map[string]string) (int, error) {
	wait, retryMax := warmRetryInitial.Load(), warmRetryMax.Load()
	for attempt := 1; ; attempt++ {
		n, err := s.domainNodes(ctx, selector)
		if err == nil || attempt == 3 || ctx.Err() != nil {
			return n, err
		}
		switch grpcCode(err) {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Aborted, codes.Unknown:
		default:
			return n, err
		}
		select {
		case <-ctx.Done():
			return n, err
		case <-time.After(wait):
		}
		wait = min(wait*2, retryMax)
	}
}

// labelsCarry reports whether labels has every pair of selector.
func labelsCarry(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

type warmResult struct {
	id   string
	node string
	dur  time.Duration
	err  error
	// created: the call made the probe, or may have (its last create
	// attempt was cut off, see createMayHaveLanded), so the call deletes it.
	// A create the control plane refused made nothing to delete.
	created bool
	// uncertain: created, and a create attempt was cut off. That attempt can
	// still be committed after the call's delete, even when a later attempt
	// was confirmed (succeeded or found the name taken); see deleteWarmProbe.
	uncertain bool
}

// warmProbe creates one probe environment, resumes it and waits until it is
// RUNNING on a node, within timeout. The create and the resume are retried
// on the transient failures of warmRetryable until then: a probe fails for
// one of those only once its budget is spent.
func (s *Server) warmProbe(ctx context.Context, atespace, template string, selector map[string]string, id string, timeout time.Duration) (r warmResult) {
	start := time.Now()
	defer func() { r.dur = time.Since(start) }()
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	r = warmResult{id: id}
	opts := ate.CreateOptions{ID: id, Template: template, Atespace: atespace, WorkerSelector: selector}
	// cut: an attempt was cut off and may have made the actor all the same.
	// The name a later attempt finds taken is this call's then (a run id is
	// unique per call), not a user's, and the probe goes on. Without such an
	// attempt, a name that exists is another call's probe or a user's
	// environment: a final failure, and not this call's to delete. The
	// atespace exists (WarmImage ensured it), so each attempt is one
	// CreateActor call and its failure is that call's.
	cut := false
	err := warmRetry(pctx, warmCreateAttemptTimeout, func(ctx context.Context, _ int) error {
		err := s.client.CreateActor(ctx, opts)
		if err == nil || (cut && grpcCode(err) == codes.AlreadyExists) {
			return nil
		}
		cut = cut || createMayHaveLanded(err)
		return err
	})
	// Created, or may be: the last attempt made it, or an attempt was cut
	// off (cut) and may have made it even when a later attempt was refused
	// for good. Either way the call deletes it.
	r.created = err == nil || cut || createMayHaveLanded(err)
	r.uncertain = cut
	if err != nil {
		r.err = fmt.Errorf("create: %w", err)
		return r
	}
	// A new actor is suspended and unplaced; ResumeActor places it and
	// returns once the template's wakeup probe answered (or fails, with the
	// actor CRASHED), so the response carries the node most of the time. No
	// bound per attempt: a cold restore legitimately takes minutes, and the
	// probe's timeout is the budget. A resume retried after the first one
	// reached the control plane finds the actor RUNNING and returns it.
	var actor *ateapipb.Actor
	err = warmRetry(pctx, 0, func(ctx context.Context, _ int) error {
		var err error
		actor, err = s.client.Resume(ctx, atespace, id)
		return err
	})
	if err != nil {
		r.err = fmt.Errorf("resume: %w", err)
		return r
	}
	if st := actor.GetStatus(); st.GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
		if node := actorNode(st); node != "" {
			r.node = node
			return r
		}
	}
	r.node, r.err = s.waitRunning(pctx, atespace, id, timeout)
	return r
}

// grpcCode is the status code of err, wrapped or not.
func grpcCode(err error) codes.Code {
	if c := status.Code(err); c != codes.Unknown {
		return c
	}
	return status.Code(errors.Unwrap(err))
}

// createMayHaveLanded reports whether a CreateActor that failed with err may
// have made the actor all the same: the call was cut off (an api server
// restarting, the attempt's deadline, the probe's budget or the stream
// ending) rather than refused, and the control plane may have kept the
// actor. A refusal (InvalidArgument, FailedPrecondition, AlreadyExists,
// ResourceExhausted, PermissionDenied, ...) made nothing.
func createMayHaveLanded(err error) bool {
	switch grpcCode(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return true
	}
	return false
}

// warmRetryable reports whether err is one of the transient failures a
// control plane under load hands a probe's create or resume, worth another
// try within the probe's budget: Unavailable (an api server restarting, which
// fails every call in flight with its graceful_stop), DeadlineExceeded on the
// call (a create while the database queues) and FailedPrecondition "actor
// template not found", which a create gets for a few seconds after the
// derived template was made, this call's own sequence. Any other
// FailedPrecondition (a state the actor is not in), InvalidArgument,
// AlreadyExists, ResourceExhausted and the rest are final.
func warmRetryable(err error) bool {
	st := grpcStatus(err)
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	case codes.FailedPrecondition:
		msg := strings.ToLower(st.Message())
		return strings.Contains(msg, "template") && strings.Contains(msg, "not found")
	}
	return false
}

// grpcStatus is the status of err, wrapped or not; Unknown with err's text
// when it carries none.
func grpcStatus(err error) *status.Status {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := status.FromError(e); ok {
			return st
		}
	}
	return status.New(codes.Unknown, err.Error())
}

// warmRetry runs call until it succeeds, fails for good, or ctx (the probe's
// budget) ends, with an exponential backoff from warmRetryInitial to
// warmRetryMax between the attempts a warmRetryable failure earns. A
// positive attemptTimeout bounds each attempt, so one hung call does not
// spend the whole budget. The last error is returned, with the attempt
// count once there were retries.
func warmRetry(ctx context.Context, attemptTimeout time.Duration, call func(ctx context.Context, attempt int) error) error {
	wait, retryMax := warmRetryInitial.Load(), warmRetryMax.Load()
	for attempt := 1; ; attempt++ {
		actx, cancel := ctx, context.CancelFunc(func() {})
		if attemptTimeout > 0 {
			actx, cancel = context.WithTimeout(ctx, attemptTimeout)
		}
		err := call(actx, attempt)
		cancel()
		if err == nil {
			return nil
		}
		if !warmRetryable(err) {
			return withAttempts(err, attempt)
		}
		select {
		case <-ctx.Done():
			return withAttempts(err, attempt)
		case <-time.After(wait):
		}
		wait = min(wait*2, retryMax)
	}
}

// withAttempts notes the attempts on err once there were retries.
func withAttempts(err error, attempts int) error {
	if attempts <= 1 {
		return err
	}
	return fmt.Errorf("%w (after %d attempts)", err, attempts)
}

// waitRunning polls the actor until the control plane reports it RUNNING on
// a node, it crashes or ends up in a state a fresh actor never reaches, or
// ctx ends. A failed read is retried until then.
func (s *Server) waitRunning(ctx context.Context, atespace, id string, timeout time.Duration) (string, error) {
	wait := warmPollInitial
	last := "unread"
	for {
		actor, err := s.client.Get(ctx, atespace, id)
		if err == nil {
			st := actor.GetStatus()
			last = st.GetState().String()
			switch st.GetState() {
			case ateapipb.ActorState_ACTOR_STATE_RUNNING:
				if node := actorNode(st); node != "" {
					return node, nil
				}
				last = "RUNNING without a node"
			case ateapipb.ActorState_ACTOR_STATE_CRASHED:
				return "", fmt.Errorf("crashed: %s", st.GetCrash().GetMessage())
			case ateapipb.ActorState_ACTOR_STATE_SUSPENDING, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_DELETING:
				return "", fmt.Errorf("unexpected state %s", st.GetState())
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("not running after %s (last state %s)", timeout, last)
		case <-time.After(wait):
		}
		wait = min(wait*2, warmPollMax)
	}
}

// deleteWarmProbe deletes a probe whatever happened to the call's context,
// within warmDeleteTimeout. Aborted, Unavailable and ResourceExhausted, the
// failures a control plane under load hands 3 to 13% of deletes, are retried
// with backoff; a probe that never existed is fine. The error returned is a
// probe still there, logged here and listed in the coverage for a sweep. A
// CRASHED probe (a resume that failed) is one: Substrate refuses to delete it
// today (its DeleteVolumes prerequisite is not met), and that refusal is
// final.
//
// For an uncertain probe (a create attempt was cut off), a probe that is gone
// is not final: the control plane can commit that attempt after this delete,
// whether the delete found nothing or deleted the probe a later attempt
// made. The delete is then repeated warmLateCreateRechecks times over
// warmLateCreateSettle, and a probe that appears in that time is deleted. A
// delete that the budget cuts off after an earlier one left the probe gone
// counts as done.
func (s *Server) deleteWarmProbe(ctx context.Context, atespace, id string, uncertain bool) error {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), warmDeleteTimeout.Load())
	defer cancel()
	wait, retryMax := warmRetryInitial.Load(), warmRetryMax.Load()
	rechecks := 0
	settle := warmLateCreateSettle.Load()
	step := time.Duration(0)
	if warmLateCreateRechecks > 0 {
		step = settle / time.Duration(warmLateCreateRechecks)
	}
	for attempt := 1; ; attempt++ {
		err := s.client.Delete(dctx, atespace, id)
		// A missing probe is ate.ErrNotFound: the ate client folds the
		// NotFound status into it, so no status code is left to read.
		if err == nil || errors.Is(err, ate.ErrNotFound) {
			if !uncertain || settle <= 0 || rechecks == warmLateCreateRechecks {
				return nil
			}
			if deadline, ok := dctx.Deadline(); ok && time.Until(deadline) < step {
				return nil
			}
			rechecks++
			select {
			case <-time.After(step):
				continue
			case <-dctx.Done():
				return nil
			}
		}
		if rechecks > 0 && (dctx.Err() != nil || grpcCode(err) == codes.DeadlineExceeded) {
			// An earlier delete left the probe gone; the budget ended while
			// this one watched for a late create.
			return nil
		}
		retry := false
		switch grpcCode(err) {
		case codes.Aborted, codes.Unavailable, codes.ResourceExhausted:
			retry = true
		}
		if retry {
			select {
			case <-time.After(wait):
				wait = min(wait*2, retryMax)
				continue
			case <-dctx.Done():
			}
		}
		err = withAttempts(err, attempt)
		log.Printf("apiservice: warm probe %s/%s not deleted, left for a sweep: %v", atespace, id, err)
		return err
	}
}

// sweepWarmProbes deletes the round's uncertain probes that are still in
// atespace after their deletes: a cut-off create that the control plane
// committed after the probe's delete and its settle window. The round runs it
// before it gives its probe slots back. It lists the atespace once, and only
// when some probe was uncertain, so a round whose creates were all confirmed
// costs nothing more. It returns undeleted with the probes it could not
// delete added, sorted. When the list fails, it adds every uncertain probe:
// the call cannot tell whether one landed, and the caller's sweep of a probe
// that is gone finds nothing.
func (s *Server) sweepWarmProbes(ctx context.Context, atespace string, uncertain map[string]bool, undeleted []string) []string {
	if len(uncertain) == 0 {
		return undeleted
	}
	known := map[string]bool{}
	for _, id := range undeleted {
		known[id] = true
	}
	lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), warmDeleteTimeout.Load())
	defer cancel()
	actors, err := s.client.List(lctx, atespace)
	if err != nil {
		log.Printf("apiservice: warm probes in %s not swept, listing them as undeleted: %v", atespace, err)
		for id := range uncertain {
			if !known[id] {
				undeleted = append(undeleted, id)
			}
		}
		sort.Strings(undeleted)
		return undeleted
	}
	var toDelete []warmResult
	for _, a := range actors {
		id := a.GetMetadata().GetName()
		if !uncertain[id] || known[id] {
			continue
		}
		if a.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_DELETING {
			continue
		}
		log.Printf("apiservice: warm probe %s/%s appeared after its delete; deleting it again", atespace, id)
		toDelete = append(toDelete, warmResult{id: id, created: true, uncertain: false})
		known[id] = true
	}
	if len(toDelete) > 0 {
		lateUndeleted, _ := s.deleteWarmProbes(lctx, atespace, toDelete)
		undeleted = append(undeleted, lateUndeleted...)
	}
	sort.Strings(undeleted)
	return undeleted
}

func errMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

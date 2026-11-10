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
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	env "github.com/agent-substrate/env/clients/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

// Pre-flight event names. They go to the pre-flight's own events file, not
// to the run's shards, so the run report does not count them.
const (
	EventPreflightTemplate = "preflight_template" // one image's derived template ensured
	EventPreflightGolden   = "preflight_golden"   // one image's golden settled: ready, none, failed, or given up
	EventPreflightProbe    = "preflight_probe"    // one probe environment: created, ready, node read, deleted
)

// PreflightOptions configure a PreflightWarmer.
type PreflightOptions struct {
	// Plan is the image placement to fill; PlanPath names it in the report.
	Plan     *schema.ImagePlacement
	PlanPath string
	Backend  ImageWarmer
	// Recorder receives the events; nil records nothing.
	Recorder *Recorder
	Logger   *slog.Logger
	// RunID prefixes the probe environment names (at most 20 characters).
	RunID string
	// ProbesPerNode is how many probes a round starts per node of a domain;
	// a round holds its probes alive until all are placed, so the scheduler
	// spreads them. Default 2.
	ProbesPerNode int
	// ImageConcurrency is how many images a domain warms at once, each with
	// its own rounds, so a domain keeps ImageConcurrency x nodes x
	// ProbesPerNode slots busy. A domain whose plan entry has node_slots
	// caps it at node_slots / ProbesPerNode. Default 4.
	ImageConcurrency int
	// MaxRounds caps the rounds per (image, domain); default 3.
	MaxRounds int
	// GoldenTimeout gives up on the goldens still baking once none became
	// ready or failed for that long; default 30 min. GoldenPoll is how often
	// the baking goldens are polled; default 10 s.
	GoldenTimeout time.Duration
	GoldenPoll    time.Duration
	// ProbeTimeout bounds one probe's create and first command; default 10 min.
	ProbeTimeout time.Duration
	// Images and Domains, when set, restrict the pass to those.
	Images  []string
	Domains []string
	// EnsureWidth is how many templates are ensured, and later polled, at
	// once; default 8.
	EnsureWidth int

	now         func() time.Time
	wakeBackoff time.Duration
}

// PreflightWarmer fills the node caches an image placement plan describes.
// It ensures each image's derived template, which starts the golden bake,
// then per domain warms images as their goldens become ready: for each
// (image, domain) it creates probe environments with the domain's worker
// selector, round after round, until every node of the domain has run the
// image once, reading the node each probe landed on, and deletes them.
type PreflightWarmer struct {
	opts PreflightOptions
}

// NewPreflightWarmer validates opts and applies the defaults.
func NewPreflightWarmer(opts PreflightOptions) (*PreflightWarmer, error) {
	if opts.Plan == nil {
		return nil, errors.New("preflight: a plan is required")
	}
	if err := opts.Plan.Validate(); err != nil {
		return nil, fmt.Errorf("preflight: %w", err)
	}
	if opts.Backend == nil {
		return nil, errors.New("preflight: a backend is required")
	}
	if opts.RunID == "" {
		// A random suffix, as ate-env-api's WarmImage uses: two passes in one
		// atespace must not share probe names (see createRefused).
		opts.RunID = newPreflightRunID()
	}
	if len(opts.RunID) > maxRunIDLen || !runIDPattern.MatchString(opts.RunID) {
		return nil, fmt.Errorf("preflight: run id %q must match %s and be at most %d characters", opts.RunID, runIDPattern, maxRunIDLen)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.ProbesPerNode <= 0 {
		opts.ProbesPerNode = 2
	}
	if opts.ImageConcurrency <= 0 {
		opts.ImageConcurrency = 4
	}
	if opts.MaxRounds <= 0 {
		opts.MaxRounds = 3
	}
	if opts.GoldenTimeout <= 0 {
		opts.GoldenTimeout = 30 * time.Minute
	}
	if opts.GoldenPoll <= 0 {
		opts.GoldenPoll = 10 * time.Second
	}
	if opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 10 * time.Minute
	}
	if opts.EnsureWidth <= 0 {
		opts.EnsureWidth = 8
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	if opts.wakeBackoff <= 0 {
		opts.wakeBackoff = DefaultWakeBackoff
	}
	for _, name := range opts.Domains {
		if opts.Plan.Domain(name) == nil {
			return nil, fmt.Errorf("preflight: the plan has no domain %q", name)
		}
	}
	for _, img := range opts.Images {
		if opts.Plan.Entry(img) == nil {
			return nil, fmt.Errorf("preflight: the plan does not place %q", img)
		}
	}
	w := &PreflightWarmer{opts: opts}
	// Only the domains of the pass: an unselected pool's slots do not bound it.
	for _, d := range w.domains() {
		if d.NodeSlots > 0 && opts.ProbesPerNode > d.NodeSlots {
			return nil, fmt.Errorf("preflight: %d probes per node exceed the %d node slots of domain %s", opts.ProbesPerNode, d.NodeSlots, d.Name)
		}
	}
	if len(w.images()) == 0 {
		return nil, errors.New("preflight: no image of the pass is placed in its domains")
	}
	return w, nil
}

// Run executes the pre-flight and returns the coverage report. The error is
// non-nil when the pass is incomplete (a golden not ready, a pair short of
// its nodes) or the context ended; the report is returned either way, so
// the caller can write it.
func (w *PreflightWarmer) Run(ctx context.Context) (*schema.PreflightCoverage, error) {
	o := w.opts
	started := o.now()
	images := w.images()
	domains := w.domains()

	report := &schema.PreflightCoverage{
		SchemaVersion: schema.SchemaVersion,
		RunID:         o.RunID,
		StartedAt:     started,
		Plan:          o.PlanPath,
	}

	goldens := newGoldenTracker(images, started)
	w.ensureTemplates(ctx, images, goldens)
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		w.pollGoldens(ctx, goldens)
	}()
	coverage, coverageOK := w.cover(ctx, domains, goldens)
	<-polled

	report.Goldens, report.Domains = goldens.records(images), coverage
	report.FinishedAt = o.now()
	report.Complete = goldens.allOK() && coverageOK && ctx.Err() == nil

	if o.Recorder != nil {
		_ = o.Recorder.Flush()
	}
	if ctx.Err() != nil {
		return report, ctx.Err()
	}
	if !report.Complete {
		return report, fmt.Errorf("pre-flight incomplete: %s", w.shortfall(report))
	}
	return report, nil
}

// images is the pass's images in plan order: those of Images, or all, that
// at least one domain of the pass holds.
func (w *PreflightWarmer) images() []string {
	wanted := map[string]bool{}
	for _, img := range w.opts.Images {
		wanted[img] = true
	}
	domains := map[string]bool{}
	for _, d := range w.domains() {
		domains[d.Name] = true
	}
	var out []string
	for _, e := range w.opts.Plan.Images {
		if len(wanted) > 0 && !wanted[e.Image] {
			continue
		}
		for _, d := range e.Domains {
			if domains[d] {
				out = append(out, e.Image)
				break
			}
		}
	}
	return out
}

func (w *PreflightWarmer) domains() []schema.PlacementDomain {
	if len(w.opts.Domains) == 0 {
		return w.opts.Plan.Domains
	}
	want := make(map[string]bool, len(w.opts.Domains))
	for _, n := range w.opts.Domains {
		want[n] = true
	}
	var out []schema.PlacementDomain
	for _, d := range w.opts.Plan.Domains {
		if want[d.Name] {
			out = append(out, d)
		}
	}
	return out
}

func (w *PreflightWarmer) domainIndex(name string) int {
	for i, d := range w.opts.Plan.Domains {
		if d.Name == name {
			return i
		}
	}
	return 0
}

// imageWidth is how many images the domain warms at once.
func (w *PreflightWarmer) imageWidth(d schema.PlacementDomain) int {
	k := w.opts.ImageConcurrency
	if d.NodeSlots > 0 {
		if c := d.NodeSlots / w.opts.ProbesPerNode; c < k {
			k = c
		}
	}
	return max(k, 1)
}

// goldenTracker holds each image's golden state while the pass runs. The
// coverage work of an image waits on its done channel; changed is closed and
// replaced whenever an image settles, so a waiter can pick the next settled
// image instead of a fixed one.
type goldenTracker struct {
	mu       sync.Mutex
	waits    map[string]*goldenWait
	changed  chan struct{}
	progress time.Time
}

type goldenWait struct {
	task    int
	done    chan struct{}
	ensured time.Time
	// template is the derived template's name once ensured, so a record
	// settled by a timeout or a cancel still names it.
	template string
	settled  bool
	ok       bool
	rec      schema.GoldenRecord
}

func newGoldenTracker(images []string, now time.Time) *goldenTracker {
	t := &goldenTracker{waits: make(map[string]*goldenWait, len(images)), changed: make(chan struct{}), progress: now}
	for i, img := range images {
		t.waits[img] = &goldenWait{task: i, done: make(chan struct{}), ensured: now, rec: schema.GoldenRecord{Image: img}}
	}
	return t
}

// settle records the final state of one image's golden and releases the
// work waiting on it; it returns false when the image was settled already.
func (t *goldenTracker) settle(img string, rec schema.GoldenRecord, ok bool, now time.Time) (*goldenWait, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	gw := t.waits[img]
	if gw.settled {
		return gw, false
	}
	gw.settled, gw.ok, gw.rec = true, ok, rec
	gw.rec.WaitMS = DurMS(now.Sub(gw.ensured))
	t.progress = now
	close(gw.done)
	close(t.changed)
	t.changed = make(chan struct{})
	return gw, true
}

// template is the derived template name of an ensured image, or "".
func (t *goldenTracker) template(img string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.waits[img].template
}

func (t *goldenTracker) pending() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for img, gw := range t.waits {
		if !gw.settled {
			out = append(out, img)
		}
	}
	sort.Strings(out)
	return out
}

func (t *goldenTracker) lastProgress() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.progress
}

func (t *goldenTracker) records(images []string) []schema.GoldenRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]schema.GoldenRecord, 0, len(images))
	for _, img := range images {
		out = append(out, t.waits[img].rec)
	}
	return out
}

func (t *goldenTracker) allOK() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, gw := range t.waits {
		if !gw.ok {
			return false
		}
	}
	return true
}

// ensureTemplates derives every image's template, which starts the bakes,
// and settles the images whose golden needs no wait: ready, failed, or none
// when goldens are disabled on the base template (the probes then boot cold
// and still fill the layer caches).
func (w *PreflightWarmer) ensureTemplates(ctx context.Context, images []string, t *goldenTracker) {
	o := w.opts
	forEach(ctx, len(images), o.EnsureWidth, func(i int) {
		img := images[i]
		t0 := o.now()
		st, err := o.Backend.EnsureImageTemplate(ctx, img)
		w.record(Event{Event: EventPreflightTemplate, Task: i, Image: img, Env: st.Template, DurMS: DurMS(o.now().Sub(t0)), OK: err == nil, Err: errString(err)})
		t.mu.Lock()
		t.waits[img].ensured = o.now()
		t.waits[img].template = st.Template
		t.mu.Unlock()
		if err != nil {
			w.settleGolden(t, img, schema.GoldenRecord{Image: img, State: "failed", Error: err.Error()}, false)
			return
		}
		w.settleFromState(t, img, st, true)
	})
}

// settleFromState settles the image when st is final; a pending golden stays
// open. When goldens are disabled the template exists with no golden, which
// ensured tells apart from a template that is not there.
func (w *PreflightWarmer) settleFromState(t *goldenTracker, img string, st ImageState, ensured bool) {
	rec := schema.GoldenRecord{Image: img, Template: st.Template, State: string(st.Golden), Tag: st.Tag, Error: st.Error}
	switch st.Golden {
	case GoldenReady:
		w.settleGolden(t, img, rec, true)
	case GoldenFailed:
		rec.Error = "golden bake failed: " + st.Error
		w.settleGolden(t, img, rec, false)
	case GoldenNone:
		if st.Exists {
			w.settleGolden(t, img, rec, true)
		} else if ensured {
			rec.State, rec.Error = "failed", "the template was not created"
			w.settleGolden(t, img, rec, false)
		}
	}
}

// settleGolden settles the image and records its golden event once.
func (w *PreflightWarmer) settleGolden(t *goldenTracker, img string, rec schema.GoldenRecord, ok bool) {
	gw, first := t.settle(img, rec, ok, w.opts.now())
	if !first {
		return
	}
	w.record(Event{Event: EventPreflightGolden, Task: gw.task, Image: img, Env: gw.rec.Template, DurMS: gw.rec.WaitMS, OK: ok, Err: gw.rec.Error})
	if !ok {
		w.opts.Logger.Warn("preflight: golden not ready", slog.String("image", img), slog.String("state", gw.rec.State), slog.String("err", gw.rec.Error))
	}
}

// pollGoldens polls the baking goldens until all settle, none has settled
// for GoldenTimeout, or the context ends. A status call that fails is
// retried at the next poll; the timeout bounds that too.
func (w *PreflightWarmer) pollGoldens(ctx context.Context, t *goldenTracker) {
	o := w.opts
	t.mu.Lock()
	t.progress = o.now()
	t.mu.Unlock()
	for {
		pending := t.pending()
		if len(pending) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			for _, img := range pending {
				w.settleGolden(t, img, schema.GoldenRecord{Image: img, Template: t.template(img), State: "pending", Error: ctx.Err().Error()}, false)
			}
			return
		case <-time.After(o.GoldenPoll):
		}
		forEach(ctx, len(pending), o.EnsureWidth, func(i int) {
			img := pending[i]
			st, err := o.Backend.ImageStatus(ctx, img)
			if err != nil {
				o.Logger.Warn("preflight: image status failed", slog.String("image", img), slog.Any("err", err))
				return
			}
			w.settleFromState(t, img, st, false)
		})
		if stalled := o.now().Sub(t.lastProgress()); stalled > o.GoldenTimeout {
			for _, img := range t.pending() {
				w.settleGolden(t, img, schema.GoldenRecord{Image: img, Template: t.template(img), State: "pending", Error: fmt.Sprintf("golden still pending after %s without progress", o.GoldenTimeout)}, false)
			}
			return
		}
	}
}

// cover runs the coverage pass, every domain in parallel. Inside a domain
// up to imageWidth images run at once, each taken as soon as its golden has
// settled (earliest in plan order first), so a slow bake does not hold up
// the images behind it and a domain never has more than
// imageWidth x nodes x ProbesPerNode probes in flight.
func (w *PreflightWarmer) cover(ctx context.Context, domains []schema.PlacementDomain, t *goldenTracker) ([]schema.DomainCoverage, bool) {
	results := make([]schema.DomainCoverage, len(domains))
	for di, d := range domains {
		imgs := []schema.ImageCoverage{}
		for _, e := range w.opts.Plan.Images {
			if _, ok := t.waits[e.Image]; ok && contains(e.Domains, d.Name) {
				imgs = append(imgs, schema.ImageCoverage{Image: e.Image, Nodes: []string{}, Wanted: d.Nodes, Error: "not started"})
			}
		}
		results[di] = schema.DomainCoverage{Name: d.Name, Nodes: d.Nodes, NodesSeen: []string{}, Images: imgs, Complete: len(imgs) == 0}
	}
	forEach(ctx, len(domains), len(domains), func(di int) {
		d := domains[di]
		planDI := w.domainIndex(d.Name)
		type item struct {
			index int
			image string
		}
		var items []item
		for ii, e := range w.opts.Plan.Images {
			if _, ok := t.waits[e.Image]; ok && contains(e.Domains, d.Name) {
				items = append(items, item{ii, e.Image})
			}
		}
		covs := make([]schema.ImageCoverage, len(items))
		ran := make([]bool, len(items))
		started := make([]bool, len(items))
		sem := make(chan struct{}, w.imageWidth(d))
		var wg sync.WaitGroup
		for n := 0; n < len(items); {
			// The first item, in plan order, whose golden has settled.
			t.mu.Lock()
			changed := t.changed
			pick := -1
			for k, it := range items {
				if !started[k] && t.waits[it.image].settled {
					pick = k
					break
				}
			}
			t.mu.Unlock()
			if pick < 0 {
				select {
				case <-ctx.Done():
					n = len(items)
					continue
				case <-changed:
					continue
				}
			}
			select {
			case <-ctx.Done():
				n = len(items)
				continue
			case sem <- struct{}{}:
			}
			started[pick] = true
			n++
			wg.Add(1)
			go func(k int, it item) {
				defer wg.Done()
				defer func() { <-sem }()
				covs[k] = w.coverImage(ctx, planDI, it.index, d, it.image)
				ran[k] = true
			}(pick, items[pick])
		}
		wg.Wait()

		dc := schema.DomainCoverage{Name: d.Name, Nodes: d.Nodes, NodesSeen: []string{}, Images: []schema.ImageCoverage{}, Complete: true}
		seen := map[string]bool{}
		for k, it := range items {
			if !ran[k] {
				dc.Complete = false
				dc.Images = append(dc.Images, schema.ImageCoverage{Image: it.image, Nodes: []string{}, Wanted: d.Nodes, Error: "not started"})
				continue
			}
			for _, node := range covs[k].Nodes {
				seen[node] = true
			}
			if !covs[k].Complete {
				dc.Complete = false
			}
			dc.Images = append(dc.Images, covs[k])
		}
		dc.NodesSeen = sortedKeys(seen)
		if len(dc.NodesSeen) > d.Nodes {
			w.opts.Logger.Warn("preflight: domain has more nodes than the plan says", slog.String("domain", d.Name), slog.Int("plan", d.Nodes), slog.Int("seen", len(dc.NodesSeen)))
		}
		results[di] = dc
	})
	allOK := ctx.Err() == nil
	for _, dc := range results {
		if !dc.Complete {
			allOK = false
		}
	}
	return results, allOK
}

// coverImage runs rounds of probes for one image in one domain until every
// node of the domain has run it or the rounds run out.
func (w *PreflightWarmer) coverImage(ctx context.Context, di, ii int, d schema.PlacementDomain, img string) schema.ImageCoverage {
	o := w.opts
	ic := schema.ImageCoverage{Image: img, Nodes: []string{}, Wanted: d.Nodes}
	nodes := map[string]bool{}
	width := d.Nodes * o.ProbesPerNode
	for round := 1; round <= o.MaxRounds && len(nodes) < d.Nodes && ctx.Err() == nil; round++ {
		ic.Rounds = round
		type outcome struct {
			id   string
			sb   Sandbox
			node string
			err  error
		}
		outcomes := make([]outcome, width)
		// Create and wait for every probe of the round before releasing any,
		// so they occupy distinct slots while the scheduler places the rest.
		forEach(ctx, width, width, func(k int) {
			id := probeID(o.RunID, di, ii, round, k)
			t0 := o.now()
			pctx, cancel := context.WithTimeout(ctx, o.ProbeTimeout)
			defer cancel()
			var node string
			sb, err := o.Backend.CreateWith(pctx, id, img, CreateOptions{WorkerSelector: d.Selector})
			if err == nil {
				// The first command returns once the restore finished: the
				// layers and the golden snapshot are on the node by then.
				if rerr := w.runProbe(pctx, sb); rerr != nil {
					err = fmt.Errorf("first command: %w", rerr)
				} else if node, err = o.Backend.Node(pctx, id); err == nil && node == "" {
					err = errors.New("the environment reports no node")
				}
			}
			w.record(Event{Event: EventPreflightProbe, Image: img, Env: id, Node: node, Attempts: round, DurMS: DurMS(o.now().Sub(t0)), OK: err == nil, Err: errString(err), Labels: map[string]string{"domain": d.Name}})
			outcomes[k] = outcome{id: id, sb: sb, node: node, err: err}
		})
		started, roundFailed := 0, 0
		for _, oc := range outcomes {
			if oc.id == "" {
				continue // a slot forEach never started: the context ended
			}
			started++
			ic.Probes++
			if oc.err != nil {
				ic.Failed++
				roundFailed++
				ic.Error = oc.err.Error()
			} else {
				nodes[oc.node] = true
			}
		}
		// Delete the round's probes together, each within the cleanup budget,
		// whatever happened to ctx: a wide domain would otherwise wait on one
		// delete after another.
		forEach(context.Background(), width, width, func(k int) {
			oc := outcomes[k]
			if oc.id == "" {
				return
			}
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
			defer cancel()
			if oc.sb == nil {
				// A create the server refused made nothing; one that was cut
				// off may have landed, so that one is deleted by id. The
				// distinction matters on AlreadyExists: the name belongs to
				// another pass or to a user, and deleting it would take their
				// environment.
				if createRefused(oc.err) {
					return
				}
				if del, ok := o.Backend.(Backend); ok {
					_ = del.Delete(rctx, oc.id)
				}
				return
			}
			if err := oc.sb.Delete(rctx); err != nil {
				o.Logger.Warn("preflight: delete failed", slog.String("env", oc.id), slog.Any("err", err))
			}
		})
		if started > 0 && roundFailed == started {
			// Nothing of this round came up: the image, the domain or the
			// cluster is the problem, and more rounds would not change it.
			o.Logger.Warn("preflight: every probe of the round failed", slog.String("image", img), slog.String("domain", d.Name), slog.String("err", ic.Error))
			break
		}
	}
	ic.Nodes = sortedKeys(nodes)
	ic.Complete = len(ic.Nodes) >= d.Nodes
	if o.Recorder != nil {
		_ = o.Recorder.Flush()
	}
	return ic
}

// runProbe runs the probe's first command, retrying transient wake or bake
// errors inside ctx with DefaultWakeBackoff the way doStep does: a cold pull
// or golden restore that exceeds one router forward timeout answers
// Unavailable or Unknown while ctx still has budget left.
func (w *PreflightWarmer) runProbe(ctx context.Context, sb Sandbox) error {
	wait := w.opts.wakeBackoff
	for {
		res, err := sb.Run(ctx, "true", w.opts.ProbeTimeout)
		if err == nil {
			if res.ExitCode != 0 {
				return fmt.Errorf("exit %d", res.ExitCode)
			}
			return nil
		}
		if !isTransient(err) || ctx.Err() != nil {
			return err
		}
		if dl, ok := ctx.Deadline(); ok && time.Now().Add(wait).After(dl) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*w.opts.wakeBackoff)
	}
}

// DefaultPreflightRunIDPrefix starts the run id of a pass that gives none;
// newPreflightRunID appends six random characters.
const DefaultPreflightRunIDPrefix = "pf"

func newPreflightRunID() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	rand.Read(b) // never fails since Go 1.24
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return DefaultPreflightRunIDPrefix + "-" + string(b)
}

// createRefused reports whether a failed create made no environment: the
// server refused the request (the name exists, a bad argument, a missing or
// conflicting template, no permission, no room: ate-env-api's
// CreateEnvironment is one CreateActor call, and ResourceExhausted is the
// control plane saying no before anything exists, as WarmImage's
// createMayHaveLanded reads it too). A create that was cut off
// (Unavailable, DeadlineExceeded, Canceled, Unknown) may still have landed.
func createRefused(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, env.ErrInvalidArgument) || errors.Is(err, env.ErrPermissionDenied) || errors.Is(err, env.ErrFailedPrecondition) {
		return true
	}
	switch status.Code(err) {
	case codes.AlreadyExists, codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated, codes.ResourceExhausted:
		return true
	}
	return false
}

// probeID names a probe environment: short, Substrate-valid, unique per
// run, domain, image, round and slot.
func probeID(runID string, domain, image, round, slot int) string {
	return fmt.Sprintf("%s-%d-%d-%d-%d", runID, domain, image, round, slot)
}

func (w *PreflightWarmer) record(e Event) {
	if w.opts.Recorder == nil {
		return
	}
	if e.RunID == "" {
		e.RunID = w.opts.RunID
	}
	w.opts.Recorder.Record(e)
}

// GoldenSettled is schema.GoldenSettled: ready, or none when goldens are
// disabled on the base template.
func GoldenSettled(g schema.GoldenRecord) bool { return schema.GoldenSettled(g) }

// shortfall summarises what kept the pass from completing.
func (w *PreflightWarmer) shortfall(r *schema.PreflightCoverage) string {
	return preflightShortfall(r)
}

// preflightShortfall names what a pre-flight report is missing: goldens not
// ready, (image, domain) pairs short of their nodes, or an interruption.
func preflightShortfall(r *schema.PreflightCoverage) string {
	var parts []string
	notReady := 0
	for _, g := range r.Goldens {
		if !GoldenSettled(g) {
			notReady++
		}
	}
	if notReady > 0 {
		parts = append(parts, fmt.Sprintf("%d goldens not ready", notReady))
	}
	short := 0
	for _, d := range r.Domains {
		for _, ic := range d.Images {
			if !ic.Complete {
				short++
			}
		}
	}
	if short > 0 {
		parts = append(parts, fmt.Sprintf("%d (image, domain) pairs short of their nodes", short))
	}
	if len(parts) == 0 {
		parts = append(parts, "interrupted")
	}
	return strings.Join(parts, "; ")
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// WritePreflightCoverage validates c and writes it as indented JSON.
func WritePreflightCoverage(path string, c *schema.PreflightCoverage) error {
	c.Normalize()
	if err := c.Validate(); err != nil {
		return err
	}
	return writeJSONFile(path, c)
}

// ReadPreflightCoverage reads and validates a preflight-coverage.json.
func ReadPreflightCoverage(path string) (*schema.PreflightCoverage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	c, err := schema.DecodeJSONStrict[schema.PreflightCoverage](f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

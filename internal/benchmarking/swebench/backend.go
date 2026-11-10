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
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"time"

	env "github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

// Result is what a Run step returned.
type Result struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

// Sandbox is the subset of an environment the loader drives.
type Sandbox interface {
	Run(ctx context.Context, command string, timeout time.Duration) (Result, error)
	WriteFile(ctx context.Context, path string, content []byte, mode fs.FileMode) error
	Suspend(ctx context.Context) error
	Delete(ctx context.Context) error
}

// ModeSuspender is an optional interface implemented by Sandboxes that can
// choose between synchronous and asynchronous checkpoint durability.
type ModeSuspender interface {
	SuspendWithMode(ctx context.Context, async bool) error
}

// AsyncSuspender is an optional interface implemented by Sandboxes that
// support a local suspend: the call returns once the snapshot is on the node
// and the upload goes on in the background (SUSPEND_MODE_LOCAL through
// ate-env-api, DURABILITY_MODE_ASYNCHRONOUS with direct Substrate access).
type AsyncSuspender interface {
	SuspendAsync(ctx context.Context) error
}

// SandboxForker is an optional interface implemented by Sandboxes that can
// fork a new Sandbox from their latest checkpoint.
type SandboxForker interface {
	Fork(ctx context.Context, newID string) (Sandbox, error)
}

// ForkBackend is an optional interface implemented by Backends that can fork a
// new Sandbox from an existing suspended environment ID.
type ForkBackend interface {
	Fork(ctx context.Context, fromID, newID string) (Sandbox, error)
}

// Prewarmer is an optional interface implemented by Backends that can prewarm
// image templates or warm pools ahead of a training step.
type Prewarmer interface {
	Prewarm(ctx context.Context, images []string) error
}

// GoldenState is the state of a derived template's golden snapshot as the
// pre-flight sees it: none while the template does not exist, pending while
// it bakes, ready once tagged, failed when the bake gave up.
type GoldenState string

const (
	GoldenNone    GoldenState = "none"
	GoldenPending GoldenState = "pending"
	GoldenReady   GoldenState = "ready"
	GoldenFailed  GoldenState = "failed"
)

// ImageState is what ate-env-api reports for a rootfs image: its derived
// template and that template's golden snapshot.
type ImageState struct {
	Template string
	Exists   bool
	Golden   GoldenState
	Tag      string
	Error    string
}

// CreateOptions are the per-environment choices of a create beyond its id
// and image. Create passes none; CreateWith takes them.
type CreateOptions struct {
	// WorkerSelector is the labels the environment's worker pool must carry;
	// Substrate ANDs it with the template's own selector.
	WorkerSelector map[string]string
}

// ImageWarmer is the optional interface the pre-flight needs from a
// backend: the golden state of an image's derived template, creates with
// options (a worker selector), and the node an environment landed on.
// EnvBackend implements it; Create and CreateWith share one code path.
type ImageWarmer interface {
	// EnsureImageTemplate creates the derived template when it is missing,
	// which starts its golden bake, and reports it; no environment is created.
	EnsureImageTemplate(ctx context.Context, image string) (ImageState, error)
	// ImageStatus reports the derived template without creating anything.
	ImageStatus(ctx context.Context, image string) (ImageState, error)
	// CreateWith is Create with options.
	CreateWith(ctx context.Context, id, image string, opts CreateOptions) (Sandbox, error)
	// Node is the node the environment is on, or "" before its placement. It
	// is a call of its own because the create returns before the scheduler
	// has placed the environment.
	Node(ctx context.Context, id string) (string, error)
}

// Backend creates sandboxes. The loader is written against this interface so
// tests run it against an in-memory fake and a second SDK or backend can be
// compared with the same workload.
type Backend interface {
	Create(ctx context.Context, id, image string) (Sandbox, error)
	// Delete removes an environment by name without a Sandbox handle, for
	// one whose create call failed on the client side but may have
	// succeeded on the server.
	Delete(ctx context.Context, id string) error
}

// EnvBackendOption configures an EnvBackend.
type EnvBackendOption func(*EnvBackend)

// WithPlacement makes Create steer each image's environments into the
// domains an image placement plan assigns it: the per-environment worker
// selector of one of the image's domains, round-robin per image. Images the
// plan does not place get no selector.
func WithPlacement(plan *schema.ImagePlacement) EnvBackendOption {
	return func(b *EnvBackend) {
		b.placement = plan
		b.placeNext = map[string]int{}
	}
}

// EnvBackend is the Backend over the ate-env API through the Go SDK.
//
// Its client holds several connections and hands them out round-robin per
// environment (env.ClientOptions.Connections): a Kubernetes Service
// balances per connection, so one connection would put every task on one
// API replica, and gRPC caps the concurrent streams on a connection.
type EnvBackend struct {
	client       *env.Client
	atespace     string
	baseTemplate string
	deletes      deleteLimits

	placement *schema.ImagePlacement
	placeMu   sync.Mutex
	placeNext map[string]int
}

// selectorFor is the worker selector Create sends for image under the
// placement plan: the next of the image's domains, round-robin; nil without
// a plan or for an image the plan does not place.
func (b *EnvBackend) selectorFor(image string) map[string]string {
	if b.placement == nil {
		return nil
	}
	entry := b.placement.Entry(image)
	if entry == nil || len(entry.Domains) == 0 {
		return nil
	}
	b.placeMu.Lock()
	i := b.placeNext[image]
	b.placeNext[image] = i + 1
	b.placeMu.Unlock()
	d := b.placement.Domain(entry.Domains[i%len(entry.Domains)])
	if d == nil {
		return nil
	}
	// A copy: the request keeps it, and the plan's map is shared by every create.
	return maps.Clone(d.Selector)
}

// DefaultDeleteRate is the loader's default cap on the environment deletes an
// ate-env-api backend issues per second, across all its connections. A
// delete removes the environment's snapshots from the bucket, and the bucket
// rate-limits deletes: a cleanup of 37,000 environments at about 1,850
// deletes per second lost 15,031 of them to GCS 429s. 200 per second clears
// that cleanup in about three minutes, inside the loader's ten-minute cleanup
// budget, and leaves room under the bucket's limit for Substrate's own
// snapshot traffic.
const DefaultDeleteRate = 200

// WithDeleteRate caps the environment deletes the backend issues at perSecond
// across all its connections, with a burst of one second's worth; 0 removes
// the cap. See DefaultDeleteRate.
func WithDeleteRate(perSecond float64) EnvBackendOption {
	return func(b *EnvBackend) {
		if perSecond <= 0 {
			b.deletes.limiter = nil
			return
		}
		b.deletes.limiter = rate.NewLimiter(rate.Limit(perSecond), max(1, int(perSecond)))
	}
}

// deleteLimits is how an EnvBackend paces and retries its deletes. The zero
// value neither paces nor retries.
type deleteLimits struct {
	limiter *rate.Limiter // nil: no cap
	// budget bounds the retries of one delete; minWait is the first pause
	// between attempts, doubling up to maxWait.
	budget, minWait, maxWait time.Duration
}

const (
	deleteRetryBudget = 2 * time.Minute
	deleteBackoffMin  = time.Second
	deleteBackoffMax  = 30 * time.Second
)

func defaultDeleteLimits() deleteLimits {
	return deleteLimits{budget: deleteRetryBudget, minWait: deleteBackoffMin, maxWait: deleteBackoffMax}
}

// do runs one delete under the cap and retries it on the errors a
// rate-limited bucket surfaces (isRetryableDelete) until the budget or ctx
// ends. The budget starts with the first attempt, after the cap's queue. A
// NotFound after a retry is success: an earlier attempt deleted the
// environment and its answer was lost. A NotFound on the first attempt is
// returned as it is, so a caller still learns of a name that was never there.
func (l deleteLimits) do(ctx context.Context, del func(context.Context) error) error {
	var deadline time.Time
	wait := l.minWait
	for attempt := 0; ; attempt++ {
		if l.limiter != nil {
			if err := l.limiter.Wait(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("delete rate cap: %w", err)
			}
		}
		if attempt == 0 {
			deadline = time.Now().Add(l.budget)
		}
		err := del(ctx)
		if err == nil || (attempt > 0 && isNotFound(err)) {
			return nil
		}
		if l.budget <= 0 || !isRetryableDelete(err) || ctx.Err() != nil || time.Now().Add(wait).After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
		wait = min(wait*2, l.maxWait)
	}
}

// isRetryableDelete reports whether a failed delete is worth another try:
// the codes isTransient retries, ResourceExhausted, and a message that
// carries the bucket's own rate-limit error under another code. Where each
// comes from: Substrate's DeleteActor returns the delete workflow's joined
// errors uncoded, the snapshot release among them, and ate-env-api's
// toGRPCError maps an uncoded or Unknown error to Internal, which
// isTransient covers; a coded error ate-env-api passes through unchanged,
// which is where a ResourceExhausted arrives. The message carries the
// environment id too, so a bare "429" is not matched: a run id such as
// swe-1794290000 contains one.
func isRetryableDelete(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	if st.Code() == codes.ResourceExhausted || isTransient(err) {
		return true
	}
	msg := st.Message()
	return strings.Contains(msg, "Error 429") || strings.Contains(msg, "rateLimitExceeded")
}

// NewEnvBackend dials the ate-env API over connections connections (at
// least one). Environments are created in atespace from baseTemplate with
// the task image as rootfs; the API derives and bakes one template per
// distinct image on first use. A non-empty authToken is sent as the bearer
// token on each call, for an ate-env-api that runs with -auth-token-file.
// Deletes retry the errors a rate-limited bucket surfaces and, with
// WithDeleteRate, are capped per second.
func NewEnvBackend(endpoint, atespace, baseTemplate string, connections int, authToken string, opts ...EnvBackendOption) (*EnvBackend, error) {
	if connections < 1 {
		connections = 1
	}
	b := &EnvBackend{atespace: atespace, baseTemplate: baseTemplate, deletes: defaultDeleteLimits()}
	for _, opt := range opts {
		opt(b)
	}
	c, err := env.NewClient(env.ClientOptions{Endpoint: endpoint, AuthToken: authToken, Connections: connections})
	if err != nil {
		return nil, err
	}
	b.client = c
	return b, nil
}

// Close releases the API connections.
func (b *EnvBackend) Close() error { return b.client.Close() }

// Delete implements Backend, under the backend's delete cap and retries.
func (b *EnvBackend) Delete(ctx context.Context, id string) error {
	return b.deletes.do(ctx, func(ctx context.Context) error {
		return b.client.Delete(ctx, b.atespace, id)
	})
}

// SuspendWithMode suspends the environment with id via the ate-env API:
// durably (the call returns once the snapshot is in object storage) or, with
// async, locally (SuspendLocal: the call returns once the snapshot is on the
// node and the upload goes on in the background).
func (b *EnvBackend) SuspendWithMode(ctx context.Context, id string, async bool) error {
	if async {
		return b.client.SuspendLocal(ctx, b.atespace, id)
	}
	return b.client.Suspend(ctx, b.atespace, id)
}

// CheckSuspendLocal fails when the ate-env-api does not serve the suspend
// mode SuspendLocal sends (the suspend_mode feature of GetServerInfo), so a
// run that parks with suspend-async stops before it starts instead of failing
// every suspend with InvalidArgument.
func (b *EnvBackend) CheckSuspendLocal(ctx context.Context) error {
	info, err := b.client.ServerInfo(ctx)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return fmt.Errorf("this ate-env-api predates GetServerInfo and cannot suspend locally; use -park suspend: %w", err)
		}
		return fmt.Errorf("checking the ate-env-api for suspend modes: %w", err)
	}
	if !slices.Contains(info.GetFeatures(), apiservice.FeatureSuspendMode) {
		return fmt.Errorf("ate-env-api %s %s does not serve %s, which -park suspend-async needs; use -park suspend or upgrade it", info.GetServer(), info.GetVersion(), apiservice.FeatureSuspendMode)
	}
	return nil
}

// Prewarm ensures derived ActorTemplates exist for each image by creating and
// immediately deleting a probe environment per image.
func (b *EnvBackend) Prewarm(ctx context.Context, images []string) error {
	for i, img := range images {
		id := fmt.Sprintf("prewarm-%d-%d", time.Now().UnixNano()%1_000_000, i)
		sb, err := b.Create(ctx, id, img)
		if err != nil {
			_ = b.Delete(ctx, id)
			return fmt.Errorf("prewarm %s: %w", img, err)
		}
		if err := sb.Delete(ctx); err != nil {
			return fmt.Errorf("prewarm cleanup %s: %w", id, err)
		}
	}
	return nil
}

// Create implements Backend. The environment keeps the connection it was
// created on for the rest of its life. With a placement plan (WithPlacement)
// it lands in one of the image's domains.
func (b *EnvBackend) Create(ctx context.Context, id, image string) (Sandbox, error) {
	return b.CreateWith(ctx, id, image, CreateOptions{WorkerSelector: b.selectorFor(image)})
}

// CreateWith implements ImageWarmer: Create with options.
func (b *EnvBackend) CreateWith(ctx context.Context, id, image string, opts CreateOptions) (Sandbox, error) {
	e, err := b.client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{
		Id:             id,
		Atespace:       b.atespace,
		Template:       &ateenvv1alpha.Template{Name: b.baseTemplate, Atespace: b.atespace},
		RootfsImage:    image,
		WorkerSelector: opts.WorkerSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", id, err)
	}
	return envSandbox{e: e, deletes: b.deletes}, nil
}

// Node implements ImageWarmer.
func (b *EnvBackend) Node(ctx context.Context, id string) (string, error) {
	e, err := b.client.Get(ctx, b.atespace, id)
	if err != nil {
		return "", fmt.Errorf("get %s: %w", id, err)
	}
	return e.GetNode(), nil
}

// EnsureImageTemplate implements ImageWarmer.
func (b *EnvBackend) EnsureImageTemplate(ctx context.Context, image string) (ImageState, error) {
	st, err := b.client.EnsureImageTemplate(ctx, &ateenvv1alpha.EnsureImageTemplateRequest{
		RootfsImage: image,
		Template:    &ateenvv1alpha.Template{Name: b.baseTemplate, Atespace: b.atespace},
		Atespace:    b.atespace,
	})
	if err != nil {
		return ImageState{}, fmt.Errorf("ensure image template %s: %w", image, err)
	}
	return imageStateFrom(st), nil
}

// ImageStatus implements ImageWarmer.
func (b *EnvBackend) ImageStatus(ctx context.Context, image string) (ImageState, error) {
	st, err := b.client.ImageStatus(ctx, &ateenvv1alpha.GetImageStatusRequest{
		RootfsImage: image,
		Template:    &ateenvv1alpha.Template{Name: b.baseTemplate, Atespace: b.atespace},
		Atespace:    b.atespace,
	})
	if err != nil {
		return ImageState{}, fmt.Errorf("image status %s: %w", image, err)
	}
	return imageStateFrom(st), nil
}

func imageStateFrom(st *ateenvv1alpha.ImageStatus) ImageState {
	out := ImageState{
		Template: st.GetTemplate().GetName(),
		Exists:   st.GetTemplateExists(),
		Tag:      st.GetGoldenTag(),
		Error:    st.GetGoldenError(),
	}
	switch st.GetGolden() {
	case ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_PENDING:
		out.Golden = GoldenPending
	case ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_READY:
		out.Golden = GoldenReady
	case ateenvv1alpha.GoldenSnapshotState_GOLDEN_SNAPSHOT_STATE_FAILED:
		out.Golden = GoldenFailed
	default:
		out.Golden = GoldenNone
	}
	return out
}

type envSandbox struct {
	e       *env.Env
	deletes deleteLimits
}

// Run starts the command, polls until it exits and then reads its output
// (env.ShellRequest.Poll). The SDK's streaming Run holds one request open
// through the router for the whole command, and the router's route timeout
// (five minutes by default) cuts it off; SWE-bench test runs take up to
// twelve minutes. If Run returns before the process has exited, the SDK
// kills the process, so a retry of the same command never runs beside the
// first copy.
func (s envSandbox) Run(ctx context.Context, command string, timeout time.Duration) (Result, error) {
	res, err := s.e.Run(ctx, env.ShellRequest{Command: command, Timeout: timeout, Poll: true})
	if err != nil {
		return Result{}, err
	}
	return Result{ExitCode: res.ExitCode, Stdout: res.Stdout, Stderr: res.Stderr}, nil
}

func (s envSandbox) WriteFile(ctx context.Context, path string, content []byte, mode fs.FileMode) error {
	return s.e.WriteFile(ctx, path, bytes.NewReader(content), mode)
}

// Suspend parks durably: the call returns once the snapshot is in object
// storage.
func (s envSandbox) Suspend(ctx context.Context) error { return s.e.Suspend(ctx) }

// SuspendAsync parks locally (SuspendLocal): the call returns once the
// snapshot is on the node, and the upload goes on in the background.
func (s envSandbox) SuspendAsync(ctx context.Context) error { return s.e.SuspendLocal(ctx) }

// SuspendWithMode parks durably, or locally with async; see Suspend and
// SuspendAsync.
func (s envSandbox) SuspendWithMode(ctx context.Context, async bool) error {
	if async {
		return s.e.SuspendLocal(ctx)
	}
	return s.e.Suspend(ctx)
}

func (s envSandbox) Delete(ctx context.Context) error { return s.deletes.do(ctx, s.e.Delete) }

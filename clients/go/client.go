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

// Package env is the Go SDK for the environment service. It talks to the
// ate-env-api service, which bridges to the Substrate
// control plane and router; `ate-env manifest` runs that service
// in-cluster.
package env

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// DefaultAtespace is the atespace ate-env-api uses for an environment when
// a request names none, and the one Client.Env uses for an empty atespace.
// It is the same string as the server's default (internal/ate's
// DefaultAtespace and the Python client's DEFAULT_ATESPACE); the client
// keeps its own copy so that importing it pulls in no Substrate package.
const DefaultAtespace = "ate-env"

// ClientOptions configures a Client.
type ClientOptions struct {
	// Endpoint is the address or base URL of the ate-env-api service, e.g.
	// "http://localhost:7777" or "localhost:7777". Required if GRPCConn is nil.
	Endpoint string

	// GRPCConn overrides the grpc.ClientConn used for ate-env-api RPCs.
	// Connections above one and DialOptions are errors with it.
	GRPCConn *grpc.ClientConn

	// AuthToken, if non-empty, is sent as an "Authorization: Bearer <token>"
	// metadata header on every RPC, matching the Python client's auth_token.
	// On the client's own connection it is a per-RPC credential, thus each
	// RPC carries it. With GRPCConn, each Client method attaches it instead.
	AuthToken string

	// TracerProvider, when set, makes the client emit OpenTelemetry spans
	// named "<TraceServiceName>.<operation>" (create_environment,
	// get_environment, suspend_environment, delete_environment,
	// get_server_info, exec, read_file, write_file) with ate.* / ate_env.*
	// attributes, the same as the Python client's, and send their W3C
	// trace context to ate-env-api so its spans join the same trace. Nil
	// means no spans; the client never creates an exporter itself (see
	// NewTracerProvider). With GRPCConn set, the client puts the trace
	// context into each call's metadata instead of adding the otelgrpc
	// handler to a connection it does not own.
	TracerProvider trace.TracerProvider

	// TraceServiceName prefixes span names and names the instrumentation
	// scope. Empty means DefaultTraceServiceName.
	TraceServiceName string

	// Connections is how many connections the client dials to Endpoint,
	// each its own grpc.ClientConn and TCP connection; 0 and 1 mean one.
	// The client sets no load-balancing policy (grpc-go's default,
	// pick_first, opens one subchannel per ClientConn). Create and Env hand
	// them out round robin, and an Env sends every call on the one it got,
	// so one environment's streams share a connection. The other Client
	// methods round robin call by call, on a counter of their own. A
	// Kubernetes Service balances per connection, and a connection caps its
	// concurrent streams, so use at least as many as ate-env-api has
	// replicas when many environments are in flight. More than one is an
	// error with GRPCConn.
	Connections int

	// DialOptions are added to every connection the client dials, after
	// its own (insecure transport credentials, the bearer token, the trace
	// handler), thus an option that sets one value replaces the client's:
	// grpc.WithTransportCredentials replaces the client's credentials,
	// while grpc.WithCredentialsBundle is an error alongside them. Use
	// them for keepalive, window sizes or the maximum message size, as the
	// Python client's channel_options. An error with GRPCConn, which the
	// caller dials itself.
	DialOptions []grpc.DialOption
}

// Client manages environments against an ate-env-api endpoint over gRPC.
type Client struct {
	endpoint string
	opts     ClientOptions
	connAuth bool // the connection sends AuthToken itself
	conns    []*conn
	// nextEnv picks the connection of a new Env, nextCall that of a
	// Client call, so neither skews the other's round robin.
	nextEnv, nextCall atomic.Uint64
	tracer            clientTracer
	// pollMin and pollMax override the polling bounds of ShellRequest.Poll
	// in tests (SetPollBoundsForTest); zero uses the defaults.
	pollMin, pollMax time.Duration
}

// conn is one connection of a Client and the service clients on it.
type conn struct {
	cc         *grpc.ClientConn
	env        ateenvv1alpha.EnvironmentServiceClient
	process    ateenvv1alpha.ProcessServiceClient
	filesystem ateenvv1alpha.FileSystemServiceClient
}

func newConn(cc *grpc.ClientConn) *conn {
	return &conn{
		cc:  cc,
		env: ateenvv1alpha.NewEnvironmentServiceClient(cc),
		// Guest calls read their trailer for a start failure (see
		// StartError).
		process:    ateenvv1alpha.NewProcessServiceClient(trailerConn{cc}),
		filesystem: ateenvv1alpha.NewFileSystemServiceClient(trailerConn{cc}),
	}
}

// NewClient returns a Client targeting endpoint.
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.Endpoint == "" && opts.GRPCConn == nil {
		return nil, errors.New("env: ClientOptions.Endpoint is required")
	}
	if opts.Connections < 0 {
		return nil, fmt.Errorf("env: ClientOptions.Connections is %d, want 0 or more", opts.Connections)
	}
	if opts.GRPCConn != nil && opts.Connections > 1 {
		return nil, errors.New("env: ClientOptions.Connections above one cannot be set with GRPCConn")
	}
	if opts.GRPCConn != nil && len(opts.DialOptions) > 0 {
		return nil, errors.New("env: ClientOptions.DialOptions cannot be set with GRPCConn")
	}
	endpoint := strings.TrimRight(opts.Endpoint, "/")
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") && endpoint != "" {
		endpoint = "http://" + endpoint
	}

	c := &Client{
		endpoint: endpoint,
		opts:     opts,
		connAuth: opts.GRPCConn == nil && opts.AuthToken != "",
		tracer:   newClientTracer(opts),
	}
	if opts.GRPCConn != nil {
		c.conns = []*conn{newConn(opts.GRPCConn)}
		return c, nil
	}
	target := strings.TrimPrefix(endpoint, "http://")
	target = strings.TrimPrefix(target, "https://")
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if opts.AuthToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerCredentials{token: opts.AuthToken}))
	}
	if opts.TracerProvider != nil {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithTracerProvider(opts.TracerProvider),
			otelgrpc.WithPropagators(tracePropagator))))
	}
	dialOpts = append(dialOpts, opts.DialOptions...)
	for range max(opts.Connections, 1) {
		cc, err := grpc.NewClient(target, dialOpts...)
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("env: connecting to gRPC %s: %w", target, err)
		}
		c.conns = append(c.conns, newConn(cc))
	}
	return c, nil
}

// pick returns the next connection for a Client call, round robin.
func (c *Client) pick() *conn { return c.next(&c.nextCall) }

// pickEnv returns the connection for a new Env, round robin.
func (c *Client) pickEnv() *conn { return c.next(&c.nextEnv) }

func (c *Client) next(counter *atomic.Uint64) *conn {
	if len(c.conns) == 1 {
		return c.conns[0]
	}
	return c.conns[(counter.Add(1)-1)%uint64(len(c.conns))]
}

// Close closes the connections the client dialed; a GRPCConn stays open.
func (c *Client) Close() error {
	if c.opts.GRPCConn != nil {
		return nil
	}
	var errs []error
	for _, cn := range c.conns {
		errs = append(errs, cn.cc.Close())
	}
	return errors.Join(errs...)
}

// bearerCredentials sends the token on each RPC of the client's own
// connection. That connection has no TLS (insecure credentials) unless
// DialOptions replace them, thus it cannot require transport security.
type bearerCredentials struct{ token string }

func (b bearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (bearerCredentials) RequireTransportSecurity() bool { return false }

// withAuth attaches the token for a caller-supplied GRPCConn. The client's
// own connection sends it as a per-RPC credential, thus withAuth adds
// nothing there, and the header is never sent twice.
func (c *Client) withAuth(ctx context.Context) context.Context {
	if c.opts.AuthToken == "" || c.connAuth {
		return ctx
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok && len(md.Get("authorization")) > 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.opts.AuthToken)
}

// Create registers a new env with the parameters given in req and
// starts it using the gRPC EnvironmentService. The returned Env keeps the
// connection the call went on (see ClientOptions.Connections).
func (c *Client) Create(ctx context.Context, req *ateenvv1alpha.CreateEnvironmentRequest) (_ *Env, err error) {
	ctx, span := c.tracer.startRPC(ctx, opCreateEnvironment, "CreateEnvironment",
		AttrActorName.String(req.GetId()),
		AttrAtespace.String(req.GetAtespace()),
		AttrTemplateName.String(req.GetTemplate().GetName()),
		AttrTemplateAtespace.String(req.GetTemplate().GetAtespace()),
		AttrRootfsImage.String(imageRefAttr(req.GetRootfsImage())))
	defer func() { endSpan(span, err) }()
	cn := c.pickEnv()
	resp, err := cn.env.CreateEnvironment(c.withAuth(ctx), req)
	if err != nil {
		return nil, fromGRPCError(err)
	}
	created := resp.GetEnvironment()
	// The server resolves the defaults; record what it chose.
	setAttributes(span,
		AttrAtespace.String(created.GetAtespace()),
		AttrTemplateName.String(created.GetTemplate().GetName()),
		AttrTemplateAtespace.String(created.GetTemplate().GetAtespace()))
	return c.envOn(cn, created.GetAtespace(), created.GetId()), nil
}

// Get returns the environment's current status and configuration using the
// gRPC EnvironmentService.
func (c *Client) Get(ctx context.Context, atespace, id string) (*ateenvv1alpha.Environment, error) {
	return c.get(ctx, c.pick(), atespace, id)
}

func (c *Client) get(ctx context.Context, cn *conn, atespace, id string) (_ *ateenvv1alpha.Environment, err error) {
	ctx, span := c.tracer.startRPC(ctx, opGetEnvironment, "GetEnvironment", AttrActorName.String(id), AttrAtespace.String(atespace))
	defer func() { endSpan(span, err) }()
	resp, err := cn.env.GetEnvironment(c.withAuth(ctx), &ateenvv1alpha.GetEnvironmentRequest{Id: id, Atespace: atespace})
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return resp.GetEnvironment(), nil
}

// Suspend checkpoints and stops the environment using the gRPC
// EnvironmentService. It returns once the snapshot is in object storage, so
// the environment survives the loss of its node. The request carries no mode,
// so it works against an ate-env-api that predates SuspendEnvironment's mode.
// An environment that is already suspended, or paused with its upload still
// pending, is left as it is: the call returns at once and does not wait for
// the upload.
func (c *Client) Suspend(ctx context.Context, atespace, id string) error {
	return c.suspend(ctx, c.pick(), atespace, id, ateenvv1alpha.SuspendMode_SUSPEND_MODE_UNSPECIFIED)
}

// SuspendLocal checkpoints and stops the environment and returns once the
// snapshot is on the worker's node; the upload to object storage goes on in
// the background. Until it lands the environment resumes on that node only,
// reports PAUSED, and a node lost before then takes the snapshot with it. One
// park-and-resume cycle is cheaper than with Suspend. An ate-env-api that
// predates the mode refuses the call with InvalidArgument; check
// ServerInfo's features for "suspend_mode" first. Like Suspend, it leaves an
// environment that is already suspended or paused as it is.
func (c *Client) SuspendLocal(ctx context.Context, atespace, id string) error {
	return c.suspend(ctx, c.pick(), atespace, id, ateenvv1alpha.SuspendMode_SUSPEND_MODE_LOCAL)
}

func (c *Client) suspend(ctx context.Context, cn *conn, atespace, id string, mode ateenvv1alpha.SuspendMode) (err error) {
	ctx, span := c.tracer.startRPC(ctx, opSuspendEnvironment, "SuspendEnvironment", AttrActorName.String(id), AttrAtespace.String(atespace))
	defer func() { endSpan(span, err) }()
	req := &ateenvv1alpha.SuspendEnvironmentRequest{
		Id:       id,
		Atespace: atespace,
		Mode:     mode,
	}
	_, err = cn.env.SuspendEnvironment(c.withAuth(ctx), req)
	if err != nil {
		return fromGRPCError(err)
	}
	return nil
}

// Delete removes the environment permanently using the gRPC EnvironmentService.
func (c *Client) Delete(ctx context.Context, atespace, id string) error {
	return c.delete(ctx, c.pick(), atespace, id)
}

func (c *Client) delete(ctx context.Context, cn *conn, atespace, id string) (err error) {
	ctx, span := c.tracer.startRPC(ctx, opDeleteEnvironment, "DeleteEnvironment", AttrActorName.String(id), AttrAtespace.String(atespace))
	defer func() { endSpan(span, err) }()
	req := &ateenvv1alpha.DeleteEnvironmentRequest{
		Id:       id,
		Atespace: atespace,
	}
	_, err = cn.env.DeleteEnvironment(c.withAuth(ctx), req)
	if err != nil {
		return fromGRPCError(err)
	}
	return nil
}

// ServerInfo returns the server implementation, its version, the Substrate
// version it was built against and its optional features. Check a feature
// with slices.Contains(info.GetFeatures(), name) before depending on it. A
// server that predates GetServerInfo returns an error with codes.Unimplemented.
func (c *Client) ServerInfo(ctx context.Context) (_ *ateenvv1alpha.GetServerInfoResponse, err error) {
	ctx, span := c.tracer.startRPC(ctx, opGetServerInfo, "GetServerInfo")
	defer func() { endSpan(span, err) }()
	resp, err := c.pick().env.GetServerInfo(c.withAuth(ctx), &ateenvv1alpha.GetServerInfoRequest{})
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return resp, nil
}

// ImageStatus reports the template derived for a rootfs image and the state
// of its golden snapshot, without creating anything.
func (c *Client) ImageStatus(ctx context.Context, req *ateenvv1alpha.GetImageStatusRequest) (_ *ateenvv1alpha.ImageStatus, err error) {
	ctx, span := c.tracer.startRPC(ctx, opGetImageStatus, "GetImageStatus",
		AttrRootfsImage.String(imageRefAttr(req.GetRootfsImage())),
		AttrTemplateName.String(req.GetTemplate().GetName()),
		AttrTemplateAtespace.String(req.GetTemplate().GetAtespace()),
		AttrAtespace.String(req.GetAtespace()))
	defer func() { endSpan(span, err) }()
	resp, err := c.pick().env.GetImageStatus(c.withAuth(ctx), req)
	if err != nil {
		return nil, fromGRPCError(err)
	}
	// The server resolves the derived template; record what it chose.
	setAttributes(span,
		AttrTemplateName.String(resp.GetTemplate().GetName()),
		AttrTemplateAtespace.String(resp.GetTemplate().GetAtespace()))
	return resp, nil
}

// EnsureImageTemplate creates the template derived for a rootfs image when
// it is missing, which starts its golden snapshot bake, and reports it as
// ImageStatus does. No environment is created.
func (c *Client) EnsureImageTemplate(ctx context.Context, req *ateenvv1alpha.EnsureImageTemplateRequest) (_ *ateenvv1alpha.ImageStatus, err error) {
	ctx, span := c.tracer.startRPC(ctx, opEnsureImageTemplate, "EnsureImageTemplate",
		AttrRootfsImage.String(imageRefAttr(req.GetRootfsImage())),
		AttrTemplateName.String(req.GetTemplate().GetName()),
		AttrTemplateAtespace.String(req.GetTemplate().GetAtespace()),
		AttrAtespace.String(req.GetAtespace()))
	defer func() { endSpan(span, err) }()
	resp, err := c.pick().env.EnsureImageTemplate(c.withAuth(ctx), req)
	if err != nil {
		return nil, fromGRPCError(err)
	}
	setAttributes(span,
		AttrTemplateName.String(resp.GetTemplate().GetName()),
		AttrTemplateAtespace.String(resp.GetTemplate().GetAtespace()))
	return resp, nil
}

// ListPlacementDomains reports the worker pools a per-environment worker
// selector can address, as ate-env-api reads them from Substrate's workers:
// name, namespace, the labels (the selector), nodes, workers and slots per
// node. A nil or empty request reports every pool; with a template, only the
// pools that template's own worker selector matches (NotFound for an unknown
// template). PermissionDenied when ate-env-api lacks the global viewer role.
func (c *Client) ListPlacementDomains(ctx context.Context, req *ateenvv1alpha.ListPlacementDomainsRequest) (_ []*ateenvv1alpha.PlacementDomain, err error) {
	if req == nil {
		req = &ateenvv1alpha.ListPlacementDomainsRequest{}
	}
	ctx, span := c.tracer.startRPC(ctx, opListPlacementDomains, "ListPlacementDomains",
		AttrTemplateName.String(req.GetTemplate().GetName()),
		AttrTemplateAtespace.String(req.GetTemplate().GetAtespace()),
		AttrAtespace.String(req.GetAtespace()))
	defer func() { endSpan(span, err) }()
	resp, err := c.pick().env.ListPlacementDomains(c.withAuth(ctx), req)
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return resp.GetDomains(), nil
}

// WarmImage puts one rootfs image on every node of one placement domain
// through ate-env-api's WarmImage and returns the coverage once the stream
// ends. onProbe, when non-nil, sees each probe as it settles. The call lasts
// for the server's rounds (minutes for a cold image), so give ctx a deadline
// above the request's probe_timeout times max_rounds.
func (c *Client) WarmImage(ctx context.Context, req *ateenvv1alpha.WarmImageRequest, onProbe func(*ateenvv1alpha.WarmProbe)) (_ *ateenvv1alpha.WarmImageCoverage, err error) {
	ctx, span := c.tracer.startRPC(ctx, opWarmImage, "WarmImage",
		AttrRootfsImage.String(imageRefAttr(req.GetRootfsImage())),
		AttrTemplateName.String(req.GetTemplate().GetName()),
		AttrTemplateAtespace.String(req.GetTemplate().GetAtespace()),
		AttrAtespace.String(req.GetAtespace()))
	defer func() { endSpan(span, err) }()
	stream, err := c.pick().env.WarmImage(c.withAuth(ctx), req)
	if err != nil {
		return nil, fromGRPCError(err)
	}
	var cov *ateenvv1alpha.WarmImageCoverage
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fromGRPCError(err)
		}
		switch e := ev.GetEvent().(type) {
		case *ateenvv1alpha.WarmImageEvent_Probe:
			if onProbe != nil {
				onProbe(e.Probe)
			}
		case *ateenvv1alpha.WarmImageEvent_Coverage:
			cov = e.Coverage
		}
	}
	if cov == nil {
		return nil, errors.New("ate-env: WarmImage ended without a coverage")
	}
	return cov, nil
}

// Env returns a handle to an environment without checking that it exists.
// The handle takes the next connection, round robin, and keeps it (see
// ClientOptions.Connections).
func (c *Client) Env(atespace, id string) *Env {
	return c.envOn(c.pickEnv(), atespace, id)
}

func (c *Client) envOn(cn *conn, atespace, id string) *Env {
	if atespace == "" {
		atespace = DefaultAtespace
	}
	return &Env{
		id:       id,
		atespace: atespace,
		client:   c,
		conn:     cn,
	}
}

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
	"strings"

	"github.com/agent-substrate/env/internal/ate"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ErrNotFound is returned when an env, process, file, or directory does not exist.
var ErrNotFound = errors.New("not found")

// ErrProcessExited is returned when an operation needs a running process
// (signalling it, writing to its stdin) but it has already exited.
var ErrProcessExited = errors.New("process has exited")

// ClientOptions configures a Client.
type ClientOptions struct {
	// Endpoint is the address or base URL of the ate-env-api service, e.g.
	// "http://localhost:7777" or "localhost:7777". Required if GRPCConn is nil.
	Endpoint string

	// GRPCConn overrides the grpc.ClientConn used for ate-env-api RPCs.
	GRPCConn *grpc.ClientConn

	// AuthToken, if non-empty, is sent as an "Authorization: Bearer <token>"
	// metadata header on every RPC, matching the Python client's auth_token.
	// On the client's own connection it is a per-RPC credential, thus each
	// RPC carries it. With GRPCConn, each Client method attaches it instead.
	AuthToken string

	// TracerProvider, when set, makes the client emit OpenTelemetry spans
	// named "<TraceServiceName>.<operation>" (create_environment,
	// get_environment, suspend_environment, delete_environment, exec,
	// read_file, write_file) with ate.* / ate_env.* attributes, and send their W3C
	// trace context to ate-env-api so its spans join the same trace. Nil
	// means no spans; the client never creates an exporter itself (see
	// NewTracerProvider). With GRPCConn set, the client puts the trace
	// context into each call's metadata instead of adding the otelgrpc
	// handler to a connection it does not own.
	TracerProvider trace.TracerProvider

	// TraceServiceName prefixes span names and names the instrumentation
	// scope. Empty means DefaultTraceServiceName.
	TraceServiceName string
}

// Client manages environments against an ate-env-api endpoint over gRPC.
type Client struct {
	endpoint   string
	opts       ClientOptions
	connAuth   bool // the connection sends AuthToken itself
	grpcConn   *grpc.ClientConn
	grpc       ateenvv1alpha.EnvironmentServiceClient
	process    ateenvv1alpha.ProcessServiceClient
	filesystem ateenvv1alpha.FileSystemServiceClient
	tracer     clientTracer
}

// NewClient returns a Client targeting endpoint.
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.Endpoint == "" && opts.GRPCConn == nil {
		return nil, errors.New("env: ClientOptions.Endpoint is required")
	}
	endpoint := strings.TrimRight(opts.Endpoint, "/")
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") && endpoint != "" {
		endpoint = "http://" + endpoint
	}

	var (
		grpcConn *grpc.ClientConn
		err      error
	)
	if opts.GRPCConn != nil {
		grpcConn = opts.GRPCConn
	} else {
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
		grpcConn, err = grpc.NewClient(target, dialOpts...)
		if err != nil {
			return nil, fmt.Errorf("env: connecting to gRPC %s: %w", target, err)
		}
	}

	return &Client{
		endpoint:   endpoint,
		opts:       opts,
		connAuth:   opts.GRPCConn == nil && opts.AuthToken != "",
		grpcConn:   grpcConn,
		grpc:       ateenvv1alpha.NewEnvironmentServiceClient(grpcConn),
		process:    ateenvv1alpha.NewProcessServiceClient(grpcConn),
		filesystem: ateenvv1alpha.NewFileSystemServiceClient(grpcConn),
		tracer:     newClientTracer(opts),
	}, nil
}

// Close releases the client's resources.
func (c *Client) Close() error {
	if c.grpcConn != nil && c.opts.GRPCConn == nil {
		return c.grpcConn.Close()
	}
	return nil
}

// bearerCredentials sends the token on each RPC of the client's own
// connection. That connection has no TLS (insecure credentials), thus it
// cannot require transport security.
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
// starts it using the gRPC EnvironmentService.
func (c *Client) Create(ctx context.Context, req *ateenvv1alpha.CreateEnvironmentRequest) (_ *Env, err error) {
	ctx, span := c.tracer.start(ctx, opCreateEnvironment,
		AttrActorName.String(req.GetId()),
		AttrAtespace.String(req.GetAtespace()),
		AttrTemplateName.String(req.GetTemplate().GetName()),
		AttrRootfsImage.String(req.GetRootfsImage()))
	defer func() { endSpan(span, err) }()
	resp, err := c.grpc.CreateEnvironment(c.withAuth(ctx), req)
	if err != nil {
		return nil, fromGRPCError(err)
	}
	created := resp.GetEnvironment()
	// The server resolves the defaults; record what it chose.
	span.SetAttributes(AttrAtespace.String(created.GetAtespace()), AttrTemplateName.String(created.GetTemplate().GetName()))
	return c.Env(created.GetAtespace(), created.GetId()), nil
}

// Get returns the environment's current status and configuration using the
// gRPC EnvironmentService.
func (c *Client) Get(ctx context.Context, atespace, id string) (_ *ateenvv1alpha.Environment, err error) {
	ctx, span := c.tracer.start(ctx, opGetEnvironment, AttrActorName.String(id), AttrAtespace.String(atespace))
	defer func() { endSpan(span, err) }()
	resp, err := c.grpc.GetEnvironment(c.withAuth(ctx), &ateenvv1alpha.GetEnvironmentRequest{Id: id, Atespace: atespace})
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return resp.GetEnvironment(), nil
}

// Suspend checkpoints and stops the environment using the gRPC EnvironmentService.
func (c *Client) Suspend(ctx context.Context, atespace, id string) (err error) {
	ctx, span := c.tracer.start(ctx, opSuspendEnvironment, AttrActorName.String(id), AttrAtespace.String(atespace))
	defer func() { endSpan(span, err) }()
	req := &ateenvv1alpha.SuspendEnvironmentRequest{
		Id:       id,
		Atespace: atespace,
	}
	_, err = c.grpc.SuspendEnvironment(c.withAuth(ctx), req)
	if err != nil {
		return fromGRPCError(err)
	}
	return nil
}

// Delete removes the environment permanently using the gRPC EnvironmentService.
func (c *Client) Delete(ctx context.Context, atespace, id string) (err error) {
	ctx, span := c.tracer.start(ctx, opDeleteEnvironment, AttrActorName.String(id), AttrAtespace.String(atespace))
	defer func() { endSpan(span, err) }()
	req := &ateenvv1alpha.DeleteEnvironmentRequest{
		Id:       id,
		Atespace: atespace,
	}
	_, err = c.grpc.DeleteEnvironment(c.withAuth(ctx), req)
	if err != nil {
		return fromGRPCError(err)
	}
	return nil
}

// ServerInfo returns the server implementation, its version, the Substrate
// version it was built against and its optional features. Check a feature
// with slices.Contains(info.GetFeatures(), name) before depending on it. A
// server that predates GetServerInfo returns an error with codes.Unimplemented.
func (c *Client) ServerInfo(ctx context.Context) (*ateenvv1alpha.GetServerInfoResponse, error) {
	resp, err := c.grpc.GetServerInfo(c.withAuth(ctx), &ateenvv1alpha.GetServerInfoRequest{})
	if err != nil {
		return nil, fromGRPCError(err)
	}
	return resp, nil
}

// Env returns a handle to an environment without checking that it exists.
func (c *Client) Env(atespace, id string) *Env {
	if atespace == "" {
		atespace = ate.DefaultAtespace
	}
	return &Env{
		id:       id,
		atespace: atespace,
		client:   c,
	}
}

func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("env: %w: %s", ErrNotFound, status.Convert(err).Message())
	case codes.FailedPrecondition:
		if strings.Contains(status.Convert(err).Message(), "has exited") {
			return fmt.Errorf("env: %w: %s", ErrProcessExited, status.Convert(err).Message())
		}
	}
	return fmt.Errorf("env: %w", err)
}

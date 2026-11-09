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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	env "github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/benchmarking/schema"
	"github.com/agent-substrate/env/internal/routertls"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// BackendDriverOptions configures NewBackendDriver across the supported
// (-backend, -access) combinations:
//   - Backend: "substrate" (default) or "agent-sandbox"
//   - Access:  "ate-env-api" (default), "direct", or "sdk"
type BackendDriverOptions struct {
	Backend          string
	Access           string
	Endpoint         string
	ControlAddr      string
	RouterAddr       string
	Atespace         string
	BaseTemplate     string
	WarmPool         string
	Connections      int
	AuthToken        string
	AtenetClientCert string
	AtenetCAFile     string
	AtenetServerName string
	AteapiTokenFile  string
	AteapiCAFile     string
	AteapiSkipVerify *bool
	// DeleteRate caps the environment deletes per second through the
	// ate-env-api backend (WithDeleteRate); 0 removes the cap.
	DeleteRate float64
	// Placement, when set, steers each image's environments into the domains
	// an image placement plan assigns it (EnvBackend only).
	Placement *schema.ImagePlacement
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// ValidateBackendDriverFlags validates the (-backend, -access) flag values.
func ValidateBackendDriverFlags(backend, access string) error {
	switch backend {
	case "", "substrate":
		switch access {
		case "", "ate-env-api", "direct", "sdk":
			return nil
		default:
			return fmt.Errorf("unsupported -access %q for -backend %q (want ate-env-api, direct, or sdk)", access, backend)
		}
	case "agent-sandbox":
		switch access {
		case "", "ate-env-api", "direct", "sdk":
			return nil
		default:
			return fmt.Errorf("unsupported -access %q for -backend %q (want sdk, direct, or ate-env-api)", access, backend)
		}
	default:
		return fmt.Errorf("unsupported -backend %q (want substrate or agent-sandbox)", backend)
	}
}

// ValidatePlacementDriver refuses a placement plan on any driver but the
// ate-env API over Substrate: only that one sends a per-environment worker
// selector. Backend and access default to that driver when empty.
func ValidatePlacementDriver(backend, access string, hasPlacement bool) error {
	if !hasPlacement {
		return nil
	}
	if (backend != "" && backend != "substrate") || (access != "" && access != "ate-env-api") {
		return fmt.Errorf("a placement plan needs the ate-env API backend (-backend substrate -access ate-env-api); %s/%s cannot place environments", backend, access)
	}
	return nil
}

// NewBackendDriver constructs the requested Backend implementation and an
// io.Closer for releasing its underlying connections.
func NewBackendDriver(opts BackendDriverOptions) (Backend, io.Closer, error) {
	if err := ValidateBackendDriverFlags(opts.Backend, opts.Access); err != nil {
		return nil, nil, err
	}
	if err := ValidatePlacementDriver(opts.Backend, opts.Access, opts.Placement != nil); err != nil {
		return nil, nil, err
	}
	switch opts.Backend {
	case "agent-sandbox":
		b, err := NewAgentSandboxBackend(AgentSandboxOptions{
			Endpoint:  opts.Endpoint,
			Namespace: opts.Atespace,
			WarmPool:  opts.WarmPool,
			AuthToken: opts.AuthToken,
		})
		if err != nil {
			return nil, nil, err
		}
		return b, nopCloser{}, nil

	default: // "substrate" or ""
		if opts.Access == "direct" {
			ctrl := opts.ControlAddr
			if ctrl == "" {
				ctrl = opts.Endpoint
			}
			routerAddr := opts.RouterAddr
			var routerTLS *tls.Config
			if opts.AtenetClientCert != "" || opts.AtenetCAFile != "" {
				serverName := opts.AtenetServerName
				if serverName == "" {
					serverName = "atenet-router.ate-system.svc"
				}
				tlsCfg, err := (routertls.Options{
					CredentialBundleFile: opts.AtenetClientCert,
					TrustBundleFile:      opts.AtenetCAFile,
					ServerName:           serverName,
				}).Config()
				if err != nil {
					return nil, nil, fmt.Errorf("router TLS: %w", err)
				}
				routerTLS = tlsCfg
				if routerAddr == "" || routerAddr == ate.DefaultRouterAddr {
					routerAddr = ate.DefaultRouterTLSAddr
				}
			}
			var controlTLS *tls.Config
			skipVerify := true
			if opts.AteapiSkipVerify != nil {
				skipVerify = *opts.AteapiSkipVerify
			}
			if opts.AteapiCAFile != "" {
				pemBytes, err := os.ReadFile(opts.AteapiCAFile)
				if err != nil {
					return nil, nil, fmt.Errorf("reading ateapi CA file %s: %w", opts.AteapiCAFile, err)
				}
				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(pemBytes) {
					return nil, nil, fmt.Errorf("ateapi CA file %s contains no valid PEM certificates", opts.AteapiCAFile)
				}
				if opts.AteapiSkipVerify == nil {
					skipVerify = false
				}
				controlTLS = &tls.Config{
					RootCAs:            pool,
					InsecureSkipVerify: skipVerify,
				}
			}
			b, err := NewDirectSubstrateBackend(DirectSubstrateOptions{
				ATEOptions: ate.Options{
					ControlAddr:     ctrl,
					RouterAddr:      routerAddr,
					TLSConfig:       controlTLS,
					RouterTLS:       routerTLS,
					BearerTokenFile: opts.AteapiTokenFile,
					SkipVerify:      skipVerify,
				},
				Atespace:     opts.Atespace,
				BaseTemplate: opts.BaseTemplate,
			})
			if err != nil {
				return nil, nil, err
			}
			return b, b, nil
		}
		envOpts := []EnvBackendOption{WithDeleteRate(opts.DeleteRate)}
		if opts.Placement != nil {
			envOpts = append(envOpts, WithPlacement(opts.Placement))
		}
		b, err := NewEnvBackend(opts.Endpoint, opts.Atespace, opts.BaseTemplate, opts.Connections, opts.AuthToken, envOpts...)
		if err != nil {
			return nil, nil, err
		}
		return b, b, nil
	}
}

type directFileTokenCreds struct {
	path string
}

func (f directFileTokenCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	raw, err := os.ReadFile(f.path)
	if err != nil {
		return nil, fmt.Errorf("reading ateapi token file %s: %w", f.path, err)
	}
	tok := strings.TrimSpace(string(raw))
	if tok == "" {
		return nil, fmt.Errorf("ateapi token file %s is empty", f.path)
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

func (directFileTokenCreds) RequireTransportSecurity() bool { return true }

// DirectSubstrateOptions configures a DirectSubstrateBackend that talks
// directly to the Substrate control plane and atenet router without routing
// directly through ate-env-api (isolating API-proxy overhead per DESIGN.md §4/§5.4).
type DirectSubstrateOptions struct {
	ATEOptions   ate.Options
	Atespace     string
	BaseTemplate string
}

// DirectSubstrateBackend implements Backend, ForkBackend, and Prewarmer
// directly against Substrate's control plane and atenet router.
type DirectSubstrateBackend struct {
	client       *ate.Client
	controlConn  *grpc.ClientConn
	control      ateapipb.ControlClient
	ownsClient   bool
	atespace     string
	baseTemplate string
}

// NewDirectSubstrateBackend dials the Substrate control plane and router.
func NewDirectSubstrateBackend(opts DirectSubstrateOptions) (*DirectSubstrateBackend, error) {
	if opts.ATEOptions.BearerTokenFile != "" {
		if _, err := (directFileTokenCreds{path: opts.ATEOptions.BearerTokenFile}).GetRequestMetadata(context.Background()); err != nil {
			return nil, err
		}
	}
	c, err := ate.New(opts.ATEOptions)
	if err != nil {
		return nil, err
	}
	var creds credentials.TransportCredentials
	if opts.ATEOptions.TLSConfig != nil {
		creds = credentials.NewTLS(opts.ATEOptions.TLSConfig)
	} else {
		creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: opts.ATEOptions.SkipVerify})
	}
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if opts.ATEOptions.BearerTokenFile != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(directFileTokenCreds{path: opts.ATEOptions.BearerTokenFile}))
	}
	controlConn, err := grpc.NewClient(opts.ATEOptions.ControlAddr, dialOpts...)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	b := NewDirectSubstrateBackendFromClient(c, opts.Atespace, opts.BaseTemplate)
	b.controlConn = controlConn
	b.control = ateapipb.NewControlClient(controlConn)
	b.ownsClient = true
	return b, nil
}

// NewDirectSubstrateBackendFromClient wraps an existing ate.Client as a
// DirectSubstrateBackend.
func NewDirectSubstrateBackendFromClient(client *ate.Client, atespace, baseTemplate string) *DirectSubstrateBackend {
	if atespace == "" {
		atespace = ate.DefaultAtespace
	}
	if baseTemplate == "" {
		baseTemplate = apiservice.DefaultTemplate
	}
	return &DirectSubstrateBackend{
		client:       client,
		atespace:     atespace,
		baseTemplate: baseTemplate,
	}
}

// Close releases the underlying control-plane and guest connections if owned.
func (b *DirectSubstrateBackend) Close() error {
	if b.controlConn != nil {
		_ = b.controlConn.Close()
	}
	if b.ownsClient && b.client != nil {
		return b.client.Close()
	}
	return nil
}

// ensureImageTemplate resolves or derives the per-image ActorTemplate on the
// Substrate control plane, returning an error if b.baseTemplate is missing.
func (b *DirectSubstrateBackend) ensureImageTemplate(ctx context.Context, image string) (string, error) {
	if image == "" {
		return b.baseTemplate, nil
	}
	name, err := apiservice.ImageTemplateName(b.baseTemplate, image)
	if err != nil {
		return "", err
	}
	if _, err := b.client.GetActorTemplate(ctx, b.atespace, name); err == nil {
		return name, nil
	} else if !errors.Is(err, ate.ErrNotFound) {
		return "", err
	}
	baseTmpl, err := b.client.GetActorTemplate(ctx, b.atespace, b.baseTemplate)
	if err != nil {
		return "", fmt.Errorf("base template %q: %w", b.baseTemplate, err)
	}
	derived, err := apiservice.DeriveImageTemplate(baseTmpl, name, image)
	if err != nil {
		return "", err
	}
	if _, err := b.client.CreateActorTemplate(ctx, derived); err != nil && !errors.Is(err, ate.ErrAlreadyExists) {
		return "", err
	}
	return name, nil
}

// Prewarm ensures derived ActorTemplates exist for each image in images.
func (b *DirectSubstrateBackend) Prewarm(ctx context.Context, images []string) error {
	for _, img := range images {
		if _, err := b.ensureImageTemplate(ctx, img); err != nil {
			return fmt.Errorf("prewarm %s: %w", img, err)
		}
	}
	return nil
}

// Create registers and starts an actor directly on the Substrate control plane.
func (b *DirectSubstrateBackend) Create(ctx context.Context, id, image string) (Sandbox, error) {
	tmplName, err := b.ensureImageTemplate(ctx, image)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", id, err)
	}
	if err := b.client.Create(ctx, ate.CreateOptions{
		ID:       id,
		Atespace: b.atespace,
		Template: tmplName,
	}); err != nil {
		return nil, fmt.Errorf("create %s: %w", id, err)
	}
	return &directSubstrateSandbox{b: b, id: id}, nil
}

func latestActorSnapshotID(actor *ateapipb.Actor) string {
	snaps := actor.GetStatus().GetSnapshots()
	if len(snaps) == 0 {
		return ""
	}
	latest := snaps[0]
	for _, s := range snaps[1:] {
		if s.GetGeneration() >= latest.GetGeneration() {
			latest = s
		}
	}
	return latest.GetUuid()
}

// Fork creates a new actor seeded from the latest snapshot of fromID.
func (b *DirectSubstrateBackend) Fork(ctx context.Context, fromID, newID string) (Sandbox, error) {
	if b.control == nil {
		return nil, fmt.Errorf("fork %s -> %s: direct control client is not configured", fromID, newID)
	}
	src, err := b.client.Get(ctx, b.atespace, fromID)
	if err != nil {
		return nil, fmt.Errorf("fork %s -> %s: %w", fromID, newID, err)
	}
	snapID := latestActorSnapshotID(src)
	if snapID == "" {
		if err := b.SuspendWithMode(ctx, fromID, true); err != nil {
			return nil, fmt.Errorf("fork %s -> %s: %w", fromID, newID, err)
		}
		src, err = b.client.Get(ctx, b.atespace, fromID)
		if err != nil {
			return nil, fmt.Errorf("fork %s -> %s: %w", fromID, newID, err)
		}
		snapID = latestActorSnapshotID(src)
	}
	tmplName := src.GetActorTemplate().GetName()
	if tmplName == "" {
		tmplName = b.baseTemplate
	}
	actor := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: b.atespace, Name: newID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: b.atespace, Name: tmplName},
	}
	if snapID != "" {
		actor.SourceTag = &ateapipb.ObjectRef{Atespace: b.atespace, Name: snapID}
	}
	if _, err := b.control.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: actor}); err != nil {
		return nil, fmt.Errorf("fork %s -> %s: %w", fromID, newID, err)
	}
	return &directSubstrateSandbox{b: b, id: newID}, nil
}

// SuspendWithMode suspends actor id using asynchronous or synchronous
// durability mode.
func (b *DirectSubstrateBackend) SuspendWithMode(ctx context.Context, id string, async bool) error {
	if async && b.control != nil {
		_, err := b.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor:   &ateapipb.ObjectRef{Atespace: b.atespace, Name: id},
			Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
		})
		return err
	}
	return b.client.Suspend(ctx, b.atespace, id)
}

// Delete suspends and deletes actor id directly on the Substrate control plane.
func (b *DirectSubstrateBackend) Delete(ctx context.Context, id string) error {
	return b.client.Delete(ctx, b.atespace, id)
}

type directSubstrateSandbox struct {
	b  *DirectSubstrateBackend
	id string
}

// Run runs the command through the SDK over the actor's guest connection,
// polled (env.ShellRequest.Poll), so no request outlives the router's route
// timeout.
func (s *directSubstrateSandbox) Run(ctx context.Context, command string, timeout time.Duration) (Result, error) {
	conn, err := s.b.client.DialGuest(s.b.atespace, s.id)
	if err != nil {
		return Result{}, err
	}
	c, err := env.NewClient(env.ClientOptions{GRPCConn: conn})
	if err != nil {
		return Result{}, err
	}
	res, err := c.Env(s.b.atespace, s.id).Run(ctx, env.ShellRequest{Command: command, Timeout: timeout, Poll: true})
	if err != nil {
		return Result{}, err
	}
	return Result{ExitCode: res.ExitCode, Stdout: res.Stdout, Stderr: res.Stderr}, nil
}

func (s *directSubstrateSandbox) WriteFile(ctx context.Context, path string, content []byte, mode fs.FileMode) error {
	conn, err := s.b.client.DialGuest(s.b.atespace, s.id)
	if err != nil {
		return err
	}
	fsClient := ateenvv1alpha.NewFileSystemServiceClient(conn)
	stream, err := fsClient.WriteFile(ctx)
	if err != nil {
		return err
	}
	const chunkSize = 64 * 1024
	first := content
	var rest []byte
	if len(first) > chunkSize {
		first, rest = content[:chunkSize], content[chunkSize:]
	}
	if err := stream.Send(&ateenvv1alpha.WriteFileRequest{
		Path:  path,
		Mode:  uint32(mode.Perm()),
		Chunk: first,
	}); err != nil {
		return err
	}
	for len(rest) > 0 {
		chunk := rest
		if len(chunk) > chunkSize {
			chunk = rest[:chunkSize]
		}
		if err := stream.Send(&ateenvv1alpha.WriteFileRequest{Chunk: chunk}); err != nil {
			return err
		}
		rest = rest[len(chunk):]
	}
	_, err = stream.CloseAndRecv()
	return err
}

func (s *directSubstrateSandbox) Suspend(ctx context.Context) error {
	return s.SuspendWithMode(ctx, false)
}

func (s *directSubstrateSandbox) SuspendAsync(ctx context.Context) error {
	return s.SuspendWithMode(ctx, true)
}

func (s *directSubstrateSandbox) SuspendWithMode(ctx context.Context, async bool) error {
	return s.b.SuspendWithMode(ctx, s.id, async)
}

func (s *directSubstrateSandbox) Fork(ctx context.Context, newID string) (Sandbox, error) {
	return s.b.Fork(ctx, s.id, newID)
}

func (s *directSubstrateSandbox) Delete(ctx context.Context) error {
	return s.b.Delete(ctx, s.id)
}

// AgentSandboxOptions configures an AgentSandboxBackend driving Kubernetes
// Agent Sandbox CRDs / HTTP gateway.
type AgentSandboxOptions struct {
	Endpoint   string
	Namespace  string
	WarmPool   string
	AuthToken  string
	HTTPClient *http.Client
}

// AgentSandboxBackend implements Backend, ForkBackend, and Prewarmer against
// the Kubernetes Agent Sandbox HTTP gateway / controller API.
type AgentSandboxBackend struct {
	endpoint  string
	namespace string
	warmPool  string
	authToken string
	http      *http.Client
}

// NewAgentSandboxBackend constructs an AgentSandboxBackend targeting
// opts.Endpoint.
func NewAgentSandboxBackend(opts AgentSandboxOptions) (*AgentSandboxBackend, error) {
	endpoint := strings.TrimRight(strings.TrimSpace(opts.Endpoint), "/")
	if endpoint == "" {
		return nil, errors.New("swebench: AgentSandboxOptions.Endpoint is required")
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	ns := opts.Namespace
	if ns == "" {
		ns = "default"
	}
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	return &AgentSandboxBackend{
		endpoint:  endpoint,
		namespace: ns,
		warmPool:  opts.WarmPool,
		authToken: opts.AuthToken,
		http:      hc,
	}, nil
}

func (b *AgentSandboxBackend) doJSON(ctx context.Context, method, path string, reqBody, respBody any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, b.endpoint+path, bodyReader)
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if b.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+b.authToken)
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", env.ErrNotFound, strings.TrimSpace(string(data)))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("agent-sandbox %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if respBody != nil && len(data) > 0 {
		if err := json.Unmarshal(data, respBody); err != nil {
			return fmt.Errorf("agent-sandbox decoding %s response: %w", path, err)
		}
	}
	return nil
}

// Create claims or creates an Agent Sandbox instance.
func (b *AgentSandboxBackend) Create(ctx context.Context, id, image string) (Sandbox, error) {
	payload := map[string]any{
		"id":        id,
		"namespace": b.namespace,
		"image":     image,
		"warm_pool": b.warmPool,
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/sandboxes", payload, nil); err != nil {
		return nil, fmt.Errorf("create %s: %w", id, err)
	}
	return &agentSandbox{b: b, id: id}, nil
}

// Delete removes an Agent Sandbox claim/instance by ID.
func (b *AgentSandboxBackend) Delete(ctx context.Context, id string) error {
	q := url.Values{"namespace": []string{b.namespace}}
	err := b.doJSON(ctx, http.MethodDelete, "/v1/sandboxes/"+url.PathEscape(id)+"?"+q.Encode(), nil, nil)
	if errors.Is(err, env.ErrNotFound) {
		return nil
	}
	return err
}

// SuspendWithMode suspends an Agent Sandbox instance with the requested
// durability mode.
func (b *AgentSandboxBackend) SuspendWithMode(ctx context.Context, id string, async bool) error {
	mode := "SYNCHRONOUS"
	if async {
		mode = "ASYNCHRONOUS"
	}
	payload := map[string]any{
		"namespace":       b.namespace,
		"async":           async,
		"durability_mode": mode,
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(id)+"/suspend", payload, nil)
}

// Fork clones a new Agent Sandbox instance from fromID.
func (b *AgentSandboxBackend) Fork(ctx context.Context, fromID, newID string) (Sandbox, error) {
	payload := map[string]any{
		"new_id":    newID,
		"namespace": b.namespace,
	}
	if err := b.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(fromID)+"/fork", payload, nil); err != nil {
		return nil, fmt.Errorf("fork %s -> %s: %w", fromID, newID, err)
	}
	return &agentSandbox{b: b, id: newID}, nil
}

// Prewarm warms the configured SandboxWarmPool with images.
func (b *AgentSandboxBackend) Prewarm(ctx context.Context, images []string) error {
	payload := map[string]any{
		"namespace": b.namespace,
		"warm_pool": b.warmPool,
		"images":    images,
	}
	return b.doJSON(ctx, http.MethodPost, "/v1/warmpools/prewarm", payload, nil)
}

type agentSandbox struct {
	b  *AgentSandboxBackend
	id string
}

func (s *agentSandbox) Run(ctx context.Context, command string, timeout time.Duration) (Result, error) {
	payload := map[string]any{
		"namespace":  s.b.namespace,
		"command":    []string{"sh", "-c", command},
		"timeout_ms": timeout.Milliseconds(),
	}
	var resp struct {
		ExitCode int    `json:"exit_code"`
		Stdout   string `json:"stdout"`
		Stderr   string `json:"stderr"`
	}
	if err := s.b.doJSON(ctx, http.MethodPost, "/v1/sandboxes/"+url.PathEscape(s.id)+"/exec", payload, &resp); err != nil {
		return Result{}, err
	}
	return Result{ExitCode: resp.ExitCode, Stdout: resp.Stdout, Stderr: resp.Stderr}, nil
}

func (s *agentSandbox) WriteFile(ctx context.Context, path string, content []byte, mode fs.FileMode) error {
	payload := map[string]any{
		"namespace": s.b.namespace,
		"path":      path,
		"content":   string(content),
		"mode":      uint32(mode.Perm()),
	}
	return s.b.doJSON(ctx, http.MethodPut, "/v1/sandboxes/"+url.PathEscape(s.id)+"/files", payload, nil)
}

func (s *agentSandbox) Suspend(ctx context.Context) error {
	return s.SuspendWithMode(ctx, false)
}

func (s *agentSandbox) SuspendAsync(ctx context.Context) error {
	return s.SuspendWithMode(ctx, true)
}

func (s *agentSandbox) SuspendWithMode(ctx context.Context, async bool) error {
	return s.b.SuspendWithMode(ctx, s.id, async)
}

func (s *agentSandbox) Fork(ctx context.Context, newID string) (Sandbox, error) {
	return s.b.Fork(ctx, s.id, newID)
}

func (s *agentSandbox) Delete(ctx context.Context) error {
	return s.b.Delete(ctx, s.id)
}

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

package env_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/env/clients/go"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// call is one RPC as the fixture's server saw it: the client address,
// which names the TCP connection, and the environment from the request or
// the x-env-id header.
type call struct {
	peer, method, envID string
}

// callLog records the fixture's RPCs and the TCP connections it closes.
type callLog struct {
	mu     sync.Mutex
	calls  []call
	closed chan string // the client address of each closed connection
}

func newCallLog() *callLog { return &callLog{closed: make(chan string, 1024)} }

func (l *callLog) record(ctx context.Context, method string, req any) {
	c := call{method: method}
	if p, ok := peer.FromContext(ctx); ok {
		c.peer = p.Addr.String()
	}
	if r, ok := req.(interface{ GetId() string }); ok {
		c.envID = r.GetId()
	}
	if md, _ := metadata.FromIncomingContext(ctx); len(md.Get("x-env-id")) > 0 {
		c.envID = md.Get("x-env-id")[0]
	}
	l.mu.Lock()
	l.calls = append(l.calls, c)
	l.mu.Unlock()
}

func (l *callLog) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	l.record(ctx, info.FullMethod, req)
	return handler(ctx, req)
}

func (l *callLog) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	l.record(ss.Context(), info.FullMethod, nil)
	return handler(srv, ss)
}

func (l *callLog) connState(c net.Conn, st http.ConnState) {
	if st == http.StateClosed {
		select {
		case l.closed <- c.RemoteAddr().String():
		default:
		}
	}
}

// mark is the index of the next call.
func (l *callLog) mark() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.calls)
}

// peers returns, from the call at index from on, the connections each
// environment's calls used, and all connections used.
func (l *callLog) peers(from int) (perEnv map[string]map[string]bool, all map[string]bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	perEnv, all = map[string]map[string]bool{}, map[string]bool{}
	for _, c := range l.calls[from:] {
		all[c.peer] = true
		if c.envID == "" {
			continue
		}
		if perEnv[c.envID] == nil {
			perEnv[c.envID] = map[string]bool{}
		}
		perEnv[c.envID][c.peer] = true
	}
	return perEnv, all
}

// exerciseGuest makes calls of every shape on e: unary (StartProcess,
// GetProcess, SignalProcess), server streaming (StreamProcessOutput,
// ReadFile), client streaming (WriteProcessInput, WriteFile), and Info.
func exerciseGuest(t *testing.T, e *env.Env) {
	t.Helper()
	ctx := t.Context()
	if res, err := e.Run(ctx, env.ShellRequest{Command: "cat", Stdin: []byte("ping")}); err != nil || res.Stdout != "ping" {
		t.Fatalf("Run on %s = %+v, %v", e.ID(), res, err)
	}
	if res, err := e.Run(ctx, env.ShellRequest{Command: "echo polled", Poll: true}); err != nil || res.Stdout != "polled\n" {
		t.Fatalf("polled Run on %s = %+v, %v", e.ID(), res, err)
	}
	if err := e.WriteFile(ctx, "f.txt", strings.NewReader("data"), 0o644); err != nil {
		t.Fatalf("WriteFile on %s: %v", e.ID(), err)
	}
	rc, err := e.ReadFile(ctx, "f.txt")
	if err != nil {
		t.Fatalf("ReadFile on %s: %v", e.ID(), err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	rc.Close()
	p, err := e.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"sleep", "60"}})
	if err != nil {
		t.Fatalf("StartProcess on %s: %v", e.ID(), err)
	}
	if _, err := p.Info(ctx); err != nil {
		t.Fatalf("Process.Info on %s: %v", e.ID(), err)
	}
	if _, err := p.Kill(ctx); err != nil {
		t.Fatalf("Kill on %s: %v", e.ID(), err)
	}
	if _, err := e.Info(ctx); err != nil {
		t.Fatalf("Info on %s: %v", e.ID(), err)
	}
}

// With Connections: 3 the client opens three TCP connections. Create and
// Env hand them out round robin, every call of one Env, unary or streaming,
// arrives on the same one, the Client's own calls round robin, and Close
// closes all three.
func TestConnectionsSpreadEnvsAndKeepEachOnOne(t *testing.T) {
	f := newFixture(t)
	c, err := env.NewClient(env.ClientOptions{Endpoint: f.apiURL, Connections: 3})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			c.Close()
		}
	})
	checkSpread := func(t *testing.T, from int, ids []string) {
		t.Helper()
		perEnv, _ := f.calls.peers(from)
		used := map[string]bool{}
		for _, id := range ids {
			if len(perEnv[id]) != 1 {
				t.Errorf("%s used %d connections, want 1: %v", id, len(perEnv[id]), perEnv[id])
			}
			for p := range perEnv[id] {
				used[p] = true
			}
		}
		if len(used) != len(ids) {
			t.Errorf("%d environments used %d connections, want one each: %v", len(ids), len(used), perEnv)
		}
	}

	// Create.
	var envs []*env.Env
	var ids []string
	for i := range 3 {
		e := f.createWith(t, c, fmt.Sprintf("sb-conn-%d", i))
		exerciseGuest(t, e)
		envs = append(envs, e)
		ids = append(ids, e.ID())
	}
	checkSpread(t, 0, ids)

	// Env, with the lifecycle calls of the handle.
	from := f.calls.mark()
	for _, e := range envs {
		h := c.Env(e.Atespace(), e.ID())
		exerciseGuest(t, h)
		if err := h.Suspend(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := h.Delete(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	checkSpread(t, from, ids)

	// The Client's own calls.
	from = f.calls.mark()
	for range 3 {
		if _, err := c.ServerInfo(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if _, used := f.calls.peers(from); len(used) != 3 {
		t.Errorf("three ServerInfo calls used %d connections, want 3: %v", len(used), used)
	}

	_, used := f.calls.peers(0)
	if len(used) != 3 {
		t.Fatalf("the client opened %d connections, want 3: %v", len(used), used)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	timeout := time.After(10 * time.Second)
	for len(used) > 0 {
		select {
		case addr := <-f.calls.closed:
			delete(used, addr)
		case <-timeout:
			t.Fatalf("connections still open after Close: %v", used)
		}
	}
}

// The Client's own calls do not move the round robin of Create: with two
// connections, a Create after each ServerInfo still alternates.
func TestConnectionsClientCallsDoNotSkewEnvs(t *testing.T) {
	f := newFixture(t)
	c, err := env.NewClient(env.ClientOptions{Endpoint: f.apiURL, Connections: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	for i := range 2 {
		f.createWith(t, c, fmt.Sprintf("sb-skew-%d", i))
		if _, err := c.ServerInfo(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	perEnv, _ := f.calls.peers(0)
	a, b := perEnv["sb-skew-0"], perEnv["sb-skew-1"]
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("each Create should use one connection: %v", perEnv)
	}
	for p := range a {
		if b[p] {
			t.Errorf("both environments landed on %s", p)
		}
	}
}

// Connections 0 and 1 both mean one connection.
func TestConnectionsZeroAndOneMeanOne(t *testing.T) {
	for _, n := range []int{0, 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := newFixture(t)
			c, err := env.NewClient(env.ClientOptions{Endpoint: f.apiURL, Connections: n})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			for i := range 2 {
				exerciseGuest(t, f.createWith(t, c, fmt.Sprintf("sb-one-%d", i)))
				if _, err := c.ServerInfo(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if _, used := f.calls.peers(0); len(used) != 1 {
				t.Errorf("Connections: %d used %d connections, want 1: %v", n, len(used), used)
			}
		})
	}
}

// A caller's GRPCConn is one connection the client does not dial, so
// Connections above one and DialOptions are errors with it.
func TestConnectionsAndDialOptionsWithGRPCConn(t *testing.T) {
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	for _, tc := range []struct {
		name    string
		opts    env.ClientOptions
		wantErr string
	}{
		{"two connections", env.ClientOptions{GRPCConn: conn, Connections: 2}, "Connections above one"},
		{"dial options", env.ClientOptions{GRPCConn: conn, DialOptions: []grpc.DialOption{grpc.WithUserAgent("x")}}, "DialOptions"},
		{"negative connections", env.ClientOptions{Endpoint: "127.0.0.1:1", Connections: -1}, "Connections"},
		{"one connection", env.ClientOptions{GRPCConn: conn, Connections: 1}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := env.NewClient(tc.opts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				// Close leaves the caller's connection open.
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
				if st := conn.GetState().String(); st == "SHUTDOWN" {
					t.Fatal("Close closed the caller's GRPCConn")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("NewClient = %v, want an error naming %s", err, tc.wantErr)
			}
		})
	}
}

// countingCreds counts the handshakes of the credentials it wraps.
type countingCreds struct {
	credentials.TransportCredentials
	n *atomic.Int32
}

func (c countingCreds) ClientHandshake(ctx context.Context, authority string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c.n.Add(1)
	return c.TransportCredentials.ClientHandshake(ctx, authority, conn)
}

func (c countingCreds) Clone() credentials.TransportCredentials {
	return countingCreds{c.TransportCredentials.Clone(), c.n}
}

// DialOptions apply to every connection the client dials, after the
// client's own: transport credentials passed there replace the client's, a
// dialer passed there opens each connection, and interceptors passed there
// see unary and streaming calls.
func TestDialOptionsApplyToEveryConnection(t *testing.T) {
	f := newFixture(t)
	var dials, handshakes, unary, streams atomic.Int32
	var d net.Dialer
	c, err := env.NewClient(env.ClientOptions{
		Endpoint:    f.apiURL,
		Connections: 2,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(countingCreds{insecure.NewCredentials(), &handshakes}),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				dials.Add(1)
				return d.DialContext(ctx, "tcp", addr)
			}),
			grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				unary.Add(1)
				return invoker(ctx, method, req, reply, cc, opts...)
			}),
			grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				streams.Add(1)
				return streamer(ctx, desc, cc, method, opts...)
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	for i := range 2 {
		e := f.createWith(t, c, fmt.Sprintf("sb-dial-%d", i))
		if _, err := e.Shell(t.Context(), "true"); err != nil {
			t.Fatal(err)
		}
	}
	if got := dials.Load(); got != 2 {
		t.Errorf("the dialer opened %d connections, want 2", got)
	}
	if got := handshakes.Load(); got != 2 {
		t.Errorf("the caller's credentials ran %d handshakes, want one per connection", got)
	}
	if got := unary.Load(); got < 4 {
		t.Errorf("the unary interceptor saw %d calls, want at least two creates and two starts", got)
	}
	if got := streams.Load(); got < 2 {
		t.Errorf("the stream interceptor saw %d calls, want at least an output stream per environment", got)
	}
	if _, used := f.calls.peers(0); len(used) != 2 {
		t.Errorf("%d connections used, want 2: %v", len(used), used)
	}
}

// Env.Atespace and Env.Info, Process.Env and Process.Info, as the Python
// client's Env.atespace, Env.info(), Process.env and Process.info().
func TestEnvAndProcessAccessors(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	e := f.create(t, "sb-info")
	// The create names no atespace; the server takes the template's.
	if got := e.Atespace(); got != "envs" {
		t.Errorf("Atespace = %q, want the template's atespace \"envs\" the server chose", got)
	}
	if got := f.client.Env("other", "x").Atespace(); got != "other" {
		t.Errorf("Env(\"other\", ...).Atespace = %q", got)
	}
	if got := f.client.Env("", "x").Atespace(); got != env.DefaultAtespace {
		t.Errorf("Env(\"\", ...).Atespace = %q, want %q", got, env.DefaultAtespace)
	}

	info, err := e.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.GetId() != "sb-info" || info.GetAtespace() != "envs" || info.GetTemplate().GetName() != "default-template" {
		t.Errorf("Info = %v", info)
	}
	if info.GetStatus() != ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_RUNNING {
		t.Errorf("Info status = %v, want RUNNING", info.GetStatus())
	}
	_, err = f.client.Env("", "sb-missing").Info(ctx)
	if !errors.Is(err, env.ErrNotFound) || status.Code(err) != codes.NotFound {
		t.Errorf("Info of a missing environment = %v, want ErrNotFound with codes.NotFound", err)
	}

	p, err := e.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"sleep", "60"}})
	if err != nil {
		t.Fatal(err)
	}
	if p.Env() != e {
		t.Error("Process.Env is not the Env that started it")
	}
	pi, err := p.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pi.GetProcessId() != p.ID() || pi.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING {
		t.Errorf("Process.Info = %v, want %s RUNNING", pi, p.ID())
	}
	if _, err := p.Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if pi, err := p.Info(ctx); err != nil || pi.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
		t.Errorf("Process.Info after Kill = %v, %v, want EXITED", pi, err)
	}
	_, err = e.Process("no-such-process").Info(ctx)
	if !errors.Is(err, env.ErrNotFound) || status.Code(err) != codes.NotFound {
		t.Errorf("Info of a missing process = %v, want ErrNotFound with codes.NotFound", err)
	}
}

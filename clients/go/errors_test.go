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
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/internal/apiservice"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// failingGuest answers every process and file call with code and msg, and
// with the StartFailureKey trailer set to reason when reason is not empty:
// what ate-env-api sends when the environment cannot start (WF-8).
type failingGuest struct {
	code   codes.Code
	msg    string
	reason string
}

func (g failingGuest) trailer() metadata.MD {
	if g.reason == "" {
		return nil
	}
	return metadata.Pairs(env.StartFailureKey, g.reason)
}

func (g failingGuest) unary(ctx context.Context) error {
	if md := g.trailer(); md != nil {
		grpc.SetTrailer(ctx, md)
	}
	return status.Error(g.code, g.msg)
}

func (g failingGuest) stream(ss grpc.ServerStream) error {
	if md := g.trailer(); md != nil {
		ss.SetTrailer(md)
	}
	return status.Error(g.code, g.msg)
}

type failingProcess struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	failingGuest
}

func (p failingProcess) StartProcess(ctx context.Context, _ *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	return nil, p.unary(ctx)
}

func (p failingProcess) GetProcess(ctx context.Context, _ *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	return nil, p.unary(ctx)
}

func (p failingProcess) SignalProcess(ctx context.Context, _ *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	return nil, p.unary(ctx)
}

func (p failingProcess) StreamProcessOutput(_ *ateenvv1alpha.StreamProcessOutputRequest, ss grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	return p.stream(ss)
}

func (p failingProcess) WriteProcessInput(ss grpc.ClientStreamingServer[ateenvv1alpha.WriteProcessInputRequest, ateenvv1alpha.WriteProcessInputResponse]) error {
	return p.stream(ss)
}

type failingFiles struct {
	ateenvv1alpha.UnimplementedFileSystemServiceServer
	failingGuest
}

func (f failingFiles) ReadFile(_ *ateenvv1alpha.ReadFileRequest, ss grpc.ServerStreamingServer[ateenvv1alpha.ReadFileResponse]) error {
	return f.stream(ss)
}

func (f failingFiles) WriteFile(ss grpc.ClientStreamingServer[ateenvv1alpha.WriteFileRequest, ateenvv1alpha.WriteFileResponse]) error {
	return f.stream(ss)
}

// earlyOKFiles takes the first WriteFile message and ends the stream with
// OK, without reading the rest.
type earlyOKFiles struct {
	ateenvv1alpha.UnimplementedFileSystemServiceServer
}

func (earlyOKFiles) WriteFile(ss grpc.ClientStreamingServer[ateenvv1alpha.WriteFileRequest, ateenvv1alpha.WriteFileResponse]) error {
	if _, err := ss.Recv(); err != nil {
		return err
	}
	return ss.SendAndClose(&ateenvv1alpha.WriteFileResponse{})
}

// Output's RecvMsg maps its error as Recv does: a marked call gives a
// StartError, not the unexported mark.
func TestOutputRecvMsgReturnsAStartError(t *testing.T) {
	g := failingGuest{code: codes.FailedPrecondition, msg: "env cannot start", reason: env.StartFailureCrashed}
	for conn, c := range failingClients(t, g) {
		stream, err := c.Env("envs", "dev1").Process("p1").Output(t.Context(), nil)
		if err == nil {
			err = stream.RecvMsg(new(ateenvv1alpha.ProcessOutput))
		}
		var se *env.StartError
		if !errors.As(err, &se) || se.Reason != env.StartFailureCrashed {
			t.Errorf("%s: RecvMsg = %v, want a StartError with reason crashed", conn, err)
		}
	}
}

// A server that ends a WriteFile with OK before it took all the data makes
// the write fail with io.ErrUnexpectedEOF, not succeed.
func TestWriteFileShortWriteFails(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	ateenvv1alpha.RegisterFileSystemServiceServer(srv, earlyOKFiles{})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	c, err := env.NewClient(env.ClientOptions{Endpoint: lis.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	err = c.Env("envs", "dev1").WriteFile(t.Context(), "x", bytes.NewReader(make([]byte, largeWrite)), 0o644)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("WriteFile = %v, want an error wrapping io.ErrUnexpectedEOF", err)
	}
}

// failingClients serves g and returns two clients for it: one on the
// client's own connection (Endpoint) and one on a caller-supplied GRPCConn.
func failingClients(t *testing.T, g failingGuest) map[string]*env.Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(srv, failingProcess{failingGuest: g})
	ateenvv1alpha.RegisterFileSystemServiceServer(srv, failingFiles{failingGuest: g})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	own, err := env.NewClient(env.ClientOptions{Endpoint: lis.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { own.Close() })
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	supplied, err := env.NewClient(env.ClientOptions{GRPCConn: conn})
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*env.Client{"own connection": own, "GRPCConn": supplied}
}

// largeWrite is big enough that WriteFile is still sending when the server
// has ended the stream, thus a Send returns io.EOF. That holds for the
// fake guests of the tables below and for the e2e crashed case. In the e2e
// no-capacity case the server waits out WakeNoCapacityGrace first, the
// client sends all of it, and the final CloseAndRecv reads the status.
const largeWrite = 8 << 20

// guestCalls are the guest calls of the SDK, one per RPC shape: unary,
// server streaming and client streaming, plus the client streaming cases
// where a Send runs after the server ended the stream and returns io.EOF.
var guestCalls = map[string]func(context.Context, *env.Env) error{
	"StartProcess (unary)": func(ctx context.Context, e *env.Env) error {
		_, err := e.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}})
		return err
	},
	"Signal (unary)": func(ctx context.Context, e *env.Env) error {
		return e.Process("p1").Signal(ctx, ateenvv1alpha.Signal_SIGNAL_TERM)
	},
	"FindProcess (unary)": func(ctx context.Context, e *env.Env) error {
		_, err := e.FindProcess(ctx, "p1")
		return err
	},
	"Wait (server streaming)": func(ctx context.Context, e *env.Env) error {
		_, err := e.Process("p1").Wait(ctx)
		return err
	},
	"Output (server streaming)": func(ctx context.Context, e *env.Env) error {
		stream, err := e.Process("p1").Output(ctx, nil)
		if err == nil {
			_, err = stream.Recv()
		}
		return err
	},
	"ReadFile (server streaming)": func(ctx context.Context, e *env.Env) error {
		rc, err := e.ReadFile(ctx, "x")
		if err == nil {
			_, err = io.ReadAll(rc)
			rc.Close()
		}
		return err
	},
	"WriteFile (client streaming)": func(ctx context.Context, e *env.Env) error {
		return e.WriteFile(ctx, "x", strings.NewReader("data"), 0o644)
	},
	"WriteFile large (client streaming, Send after the end)": func(ctx context.Context, e *env.Env) error {
		return e.WriteFile(ctx, "x", bytes.NewReader(make([]byte, largeWrite)), 0o644)
	},
	"Stdin after a delay (client streaming, Send after the end)": func(ctx context.Context, e *env.Env) error {
		w, err := e.Process("p1").Stdin(ctx)
		if err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
		_, werr := w.Write([]byte("x"))
		cerr := w.Close()
		return errors.Join(werr, cerr)
	},
	"Stdin (client streaming)": func(ctx context.Context, e *env.Env) error {
		w, err := e.Process("p1").Stdin(ctx)
		if err != nil {
			return err
		}
		_, werr := w.Write([]byte("x"))
		cerr := w.Close()
		return errors.Join(werr, cerr)
	},
}

// A guest call that ate-env-api marks with StartFailureKey returns a
// StartError with the reason and the environment, whatever its
// status code, on every RPC shape and on both kinds of connection; the
// status and the code's sentinel stay in the chain. Mirrors the Python
// client's test_marked_calls_map_to_a_retryable_start_error.
func TestMarkedGuestCallsReturnARetryableStartError(t *testing.T) {
	for _, tc := range []struct {
		code     codes.Code
		reason   string
		sentinel error
	}{
		{codes.FailedPrecondition, env.StartFailureCrashed, env.ErrFailedPrecondition},
		{codes.ResourceExhausted, env.StartFailureNoCapacity, nil},
		{codes.Unavailable, "some-future-reason", nil},
	} {
		clients := failingClients(t, failingGuest{code: tc.code, msg: "env cannot start", reason: tc.reason})
		for conn, client := range clients {
			e := client.Env("envs", "dev1")
			for name, call := range guestCalls {
				t.Run(tc.reason+"/"+conn+"/"+name, func(t *testing.T) {
					err := call(t.Context(), e)
					var se *env.StartError
					if !errors.As(err, &se) {
						t.Fatalf("err = %v, want a *StartError", err)
					}
					if se.Reason != tc.reason || se.Code != tc.code || se.EnvID != "dev1" {
						t.Errorf("StartError{Reason: %q, Code: %v, EnvID: %q}, want {%q, %v, dev1}", se.Reason, se.Code, se.EnvID, tc.reason, tc.code)
					}
					if !env.Retryable(err) {
						t.Errorf("Retryable(%v) = false", err)
					}
					if got := status.Code(err); got != tc.code {
						t.Errorf("status.Code = %v, want %v", got, tc.code)
					}
					if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
						t.Errorf("err %v should wrap %v", err, tc.sentinel)
					}
					if !strings.Contains(err.Error(), "env cannot start") || !strings.Contains(err.Error(), tc.reason) || !strings.Contains(err.Error(), `"envs/dev1"`) {
						t.Errorf("message %q should carry the status message, the reason and envs/dev1", err)
					}
				})
			}
		}
	}
}

// Without the trailer, a failed guest call maps its status code onto the
// sentinels, keeps the status, is not a StartError, and is retryable only
// for the codes of an unavailable environment. Mirrors test_errors.py,
// test_unmarked_calls_keep_their_mapping and the classification in
// SubstrateEnvClientRuntime.exec_async.
func TestUnmarkedGuestCallsKeepTheirMapping(t *testing.T) {
	for _, tc := range []struct {
		code      codes.Code
		msg       string
		sentinels []error
		not       []error
		retryable bool
	}{
		{codes.NotFound, "no such process", []error{env.ErrNotFound}, nil, false},
		{codes.NotFound, "environment ate-env/dev1 not found", []error{env.ErrNotFound}, nil, true},
		{codes.InvalidArgument, "bad command", []error{env.ErrInvalidArgument}, nil, false},
		{codes.PermissionDenied, "not yours", []error{env.ErrPermissionDenied}, nil, false},
		{codes.FailedPrecondition, "no tty", []error{env.ErrFailedPrecondition}, []error{env.ErrProcessExited}, false},
		{codes.FailedPrecondition, "process has exited", []error{env.ErrProcessExited, env.ErrFailedPrecondition}, nil, false},
		{codes.Unavailable, "router down", nil, []error{env.ErrNotFound, env.ErrInvalidArgument, env.ErrPermissionDenied, env.ErrFailedPrecondition}, true},
		{codes.DeadlineExceeded, "too slow", nil, nil, true},
		{codes.Internal, "boom", nil, nil, true},
		{codes.Aborted, "conflict", nil, nil, true},
		{codes.ResourceExhausted, "quota", nil, nil, false},
		{codes.Unimplemented, "old server", nil, nil, false},
	} {
		clients := failingClients(t, failingGuest{code: tc.code, msg: tc.msg})
		for conn, client := range clients {
			e := client.Env("envs", "dev1")
			for name, call := range guestCalls {
				t.Run(tc.code.String()+"/"+tc.msg+"/"+conn+"/"+name, func(t *testing.T) {
					err := call(t.Context(), e)
					if err == nil {
						t.Fatal("call succeeded")
					}
					var se *env.StartError
					if errors.As(err, &se) {
						t.Errorf("err %v: an unmarked call is not a StartError", err)
					}
					if got := env.Retryable(err); got != tc.retryable {
						t.Errorf("Retryable(%v) = %v, want %v", err, got, tc.retryable)
					}
					if got := status.Code(err); got != tc.code {
						t.Errorf("status.Code = %v, want %v", got, tc.code)
					}
					for _, s := range tc.sentinels {
						if !errors.Is(err, s) {
							t.Errorf("err %v should wrap %v", err, s)
						}
					}
					for _, s := range tc.not {
						if errors.Is(err, s) {
							t.Errorf("err %v should not wrap %v", err, s)
						}
					}
					if !strings.Contains(err.Error(), tc.msg) {
						t.Errorf("message %q should carry %q", err, tc.msg)
					}
				})
			}
		}
	}
}

// The message of an error with a sentinel keeps its earlier form.
func TestSentinelErrorMessage(t *testing.T) {
	client := failingClients(t, failingGuest{code: codes.NotFound, msg: "process p1 not found"})["own connection"]
	_, err := client.Env("envs", "dev1").FindProcess(t.Context(), "p1")
	if got, want := err.Error(), "env: not found: process p1 not found"; got != want {
		t.Errorf("message %q, want %q", got, want)
	}
}

// End to end against ate-env-api: an environment that crashed while waking,
// or that no worker has room for, fails the next guest call, unary or
// streaming, with a StartError that carries the reason and ate-env-api's
// message. Mirrors the Python client's test_start_failure.py.
func TestGuestCallOnAnEnvironmentThatCannotStart(t *testing.T) {
	calls := map[string]func(context.Context, *env.Env) error{
		"StartProcess":    guestCalls["StartProcess (unary)"],
		"ReadFile":        guestCalls["ReadFile (server streaming)"],
		"WriteFile":       guestCalls["WriteFile (client streaming)"],
		"WriteFile large": guestCalls["WriteFile large (client streaming, Send after the end)"],
	}
	for _, tc := range []struct {
		reason string
		code   codes.Code
		msg    string
		setup  func(f *fixture, id string)
	}{
		{env.StartFailureCrashed, codes.FailedPrecondition, "assigned worker is draining", func(f *fixture, id string) {
			f.control.Crash(id, "assigned worker is draining")
		}},
		{env.StartFailureNoCapacity, codes.ResourceExhausted, "no worker has room", func(f *fixture, id string) {
			f.service.WakeNoCapacityGrace = 200 * time.Millisecond
			f.router.NotRunningBody = func(id string) string {
				return "actor ate-env/" + id + " unavailable: no worker has room for the actor"
			}
			f.control.SetState(id, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
		}},
	} {
		for name, call := range calls {
			t.Run(tc.reason+"/"+name, func(t *testing.T) {
				f := newFixture(t)
				sb := f.create(t, "env1")
				tc.setup(f, "env1")

				err := call(t.Context(), sb)
				var se *env.StartError
				if !errors.As(err, &se) {
					t.Fatalf("err = %v, want a *StartError", err)
				}
				if se.Reason != tc.reason || se.Code != tc.code || se.EnvID != "env1" {
					t.Errorf("StartError{Reason: %q, Code: %v, EnvID: %q}, want {%q, %v, env1}", se.Reason, se.Code, se.EnvID, tc.reason, tc.code)
				}
				if !env.Retryable(err) || status.Code(err) != tc.code {
					t.Errorf("err %v: want retryable with code %v", err, tc.code)
				}
				if !strings.Contains(err.Error(), tc.msg) {
					t.Errorf("message %q should carry %q", err, tc.msg)
				}
			})
		}
	}
}

// The SDK's trailer key and reasons are the ones ate-env-api sends.
func TestStartFailureConstantsMatchTheServer(t *testing.T) {
	for _, c := range [][2]string{
		{env.StartFailureKey, apiservice.StartFailureKey},
		{env.StartFailureCrashed, apiservice.StartFailureCrashed},
		{env.StartFailureNoCapacity, apiservice.StartFailureNoCapacity},
	} {
		if c[0] != c[1] {
			t.Errorf("SDK %q, server %q", c[0], c[1])
		}
	}
}

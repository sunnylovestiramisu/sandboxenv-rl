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

package env

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The errors a Client returns wrap one of these sentinels for the status
// codes a caller acts on, as the Python client raises NotFoundError,
// InvalidArgumentError, PermissionDeniedError and FailedPreconditionError.
// The gRPC status stays in the chain too, so status.Code(err) still reports
// the code.
var (
	// ErrNotFound is returned when an env, process, file, or directory does
	// not exist (codes.NotFound).
	ErrNotFound = errors.New("not found")

	// ErrProcessExited is returned when an operation needs a running
	// process (signalling it, writing to its stdin) but it has already
	// exited. It is a failed precondition, thus such an error also wraps
	// ErrFailedPrecondition.
	ErrProcessExited = errors.New("process has exited")

	// ErrInvalidArgument is returned when ate-env-api or the guest rejects
	// a request as malformed (codes.InvalidArgument).
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrPermissionDenied is returned when the caller may not make the call
	// (codes.PermissionDenied).
	ErrPermissionDenied = errors.New("permission denied")

	// ErrFailedPrecondition is returned when the target is not in a state
	// that allows the call (codes.FailedPrecondition).
	ErrFailedPrecondition = errors.New("failed precondition")
)

// errShortWrite is the error of a client stream (WriteFile, Stdin) whose
// server ended it with OK before it took all the data: a Send failed with
// io.EOF and CloseAndRecv returned no error. ate-env-api does not do this
// today; the error keeps a server that would from turning a short write
// into a success.
var errShortWrite = fmt.Errorf("env: stream ended before all data was sent: %w", io.ErrUnexpectedEOF)

// StartFailureKey is the trailing metadata key ate-env-api sets on a guest
// call (a process or file call) that failed because the environment cannot
// start. Its value is StartFailureCrashed or StartFailureNoCapacity. It is
// the same key as the Python client's START_FAILURE_KEY.
const StartFailureKey = "ate-env-start-failure"

const (
	// StartFailureCrashed: the control plane moved the environment to
	// CRASHED while it was waking. The error's message carries the control
	// plane's crash reason.
	StartFailureCrashed = "crashed"
	// StartFailureNoCapacity: no worker had room to run the environment for
	// ate-env-api's --wake-no-capacity-grace.
	StartFailureNoCapacity = "no-capacity"
)

// StartError is returned by a guest call (a process or file call) that
// failed because the environment cannot start: it crashed while waking, or
// no worker had room for it. The work did not run, and running it again on
// a fresh environment can succeed, so do not score it as the work's failure.
// It matches the Python client's SandboxStartError. ate-env-api marks such a
// call with StartFailureKey, whatever its status code, and the client checks
// the mark before any other rule, as map_rpc_error does.
//
// StartError unwraps to the error the call would return otherwise, so
// errors.Is with the sentinels (ErrFailedPrecondition for a crash) and
// status.Code still work.
type StartError struct {
	// Reason is StartFailureCrashed, StartFailureNoCapacity, or a reason a
	// newer ate-env-api sends.
	Reason string
	// Code is the call's status code: FailedPrecondition for a crash,
	// ResourceExhausted for no capacity.
	Code codes.Code
	// EnvID is the environment the call was for.
	EnvID string

	atespace string
	err      error // the mapped error, which wraps the status error
	msg      string
}

func (e *StartError) Error() string {
	name := e.EnvID
	if e.atespace != "" {
		name = e.atespace + "/" + e.EnvID
	}
	return fmt.Sprintf("env: environment %q cannot start (%s): %s", name, e.Reason, e.msg)
}

func (e *StartError) Unwrap() error { return e.err }

// Retryable reports whether running the same work again on a fresh
// environment can succeed: true for a StartError and for the codes of an
// unavailable environment (Unavailable, DeadlineExceeded, Internal, Aborted,
// and NotFound on the environment itself), false for every other error
// (InvalidArgument, PermissionDenied, a missing file or process, ...). It is
// the classification the Python client's SubstrateEnvClientRuntime gives
// SandboxStartError, SandboxUnavailableError and SandboxProtocolError.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var se *StartError
	if errors.As(err, &se) {
		return true
	}
	switch st := status.Convert(err); st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Aborted:
		return true
	case codes.NotFound:
		// The environment is gone, not a file or process inside it. The
		// rule reads the server's message, as the Python client does, thus
		// a change of that message flips the result in both clients; the
		// error mapping vector of PR 7 of docs/plans/go-client-parity pins it.
		msg := strings.ToLower(st.Message())
		return strings.Contains(msg, "environment") || strings.Contains(msg, "actor")
	}
	return false
}

// rpcError is a failed call whose status code has a sentinel. Its message
// is "env: <sentinel>: <status message>"; it unwraps to the sentinels and to
// the gRPC status error.
type rpcError struct {
	sentinels []error
	st        error
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("env: %s: %s", e.sentinels[0], status.Convert(e.st).Message())
}

func (e *rpcError) Unwrap() []error { return append(append([]error{}, e.sentinels...), e.st) }

// fromGRPCError maps a failed RPC onto the SDK's errors: a StartError
// when ate-env-api marked the call with StartFailureKey (whatever its status
// code, as the Python client does), else an error wrapping the sentinel of
// its status code, else the error wrapped with the "env:" prefix.
func fromGRPCError(err error) error {
	if err == nil {
		return nil
	}
	var marked *startFailureMark
	if errors.As(err, &marked) {
		err = marked.err
	}
	mapped := mapStatus(err)
	if marked != nil {
		st := status.Convert(err)
		return &StartError{Reason: marked.reason, Code: st.Code(), EnvID: marked.envID, atespace: marked.atespace, err: mapped, msg: st.Message()}
	}
	return mapped
}

func mapStatus(err error) error {
	st := status.Convert(err)
	switch st.Code() {
	case codes.NotFound:
		return &rpcError{sentinels: []error{ErrNotFound}, st: err}
	case codes.InvalidArgument:
		return &rpcError{sentinels: []error{ErrInvalidArgument}, st: err}
	case codes.PermissionDenied:
		return &rpcError{sentinels: []error{ErrPermissionDenied}, st: err}
	case codes.FailedPrecondition:
		if strings.Contains(st.Message(), "has exited") {
			return &rpcError{sentinels: []error{ErrProcessExited, ErrFailedPrecondition}, st: err}
		}
		return &rpcError{sentinels: []error{ErrFailedPrecondition}, st: err}
	}
	return fmt.Errorf("env: %w", err)
}

// startFailureMark carries the StartFailureKey trailer of a failed call from
// the connection to fromGRPCError.
type startFailureMark struct {
	reason   string
	envID    string
	atespace string
	err      error
}

func (m *startFailureMark) Error() string { return m.err.Error() }
func (m *startFailureMark) Unwrap() error { return m.err }

// mark returns err carrying the StartFailureKey trailer, if md has one.
func mark(ctx context.Context, err error, md metadata.MD) error {
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	vals := md.Get(StartFailureKey)
	if len(vals) == 0 || vals[0] == "" {
		return err
	}
	m := &startFailureMark{reason: vals[0], err: err}
	if out, ok := metadata.FromOutgoingContext(ctx); ok {
		m.envID = last(out.Get("x-env-id"))
		m.atespace = last(out.Get("x-env-atespace"))
	}
	return m
}

func last(vals []string) string {
	if len(vals) == 0 {
		return ""
	}
	return vals[len(vals)-1]
}

// trailerConn reads the trailer of every call on the guest services (the
// process and file services), unary and streaming, so that fromGRPCError
// can tell a start failure from any other error. It wraps any connection,
// the client's own or a caller-supplied GRPCConn.
type trailerConn struct {
	grpc.ClientConnInterface
}

func (c trailerConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	var md metadata.MD
	err := c.ClientConnInterface.Invoke(ctx, method, args, reply, append(opts, grpc.Trailer(&md))...)
	return mark(ctx, err, md)
}

func (c trailerConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	s, err := c.ClientConnInterface.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, err
	}
	return &trailerStream{ClientStream: s, ctx: ctx}, nil
}

// trailerStream marks the error RecvMsg returns, the one place a stream's
// status (and with it its trailer) reaches the caller: CloseAndRecv ends in
// RecvMsg. A Send that returns io.EOF does not carry the status; the call
// site must then call CloseAndRecv (or RecvMsg) and map that error. Trailer
// may be read only once RecvMsg has returned an error.
type trailerStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *trailerStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil || errors.Is(err, io.EOF) {
		return err
	}
	return mark(s.ctx, err, s.Trailer())
}

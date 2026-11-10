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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Env is a handle to a single environment.
type Env struct {
	id        string
	atespace  string
	client    *Client
	conn      *conn // every call of the Env goes on it
	createdAt time.Time
	firstMu   sync.Mutex
	firstSent bool
}

// startCommandSpan opens the span for op (exec or start_process) and marks
// ate_env.first_command=true under one lock hold so that concurrent callers
// on one Env always attach first_command to the earliest-started span.
func (e *Env) startCommandSpan(ctx context.Context, op, execName string) (context.Context, trace.Span) {
	e.firstMu.Lock()
	first := !e.firstSent
	if first {
		e.firstSent = true
	}
	attrs := []attribute.KeyValue{
		AttrActorName.String(e.id),
		AttrAtespace.String(e.atespace),
		AttrCommandExecutable.String(execName),
	}
	if first {
		attrs = append(attrs, AttrFirstCommand.Bool(true))
	}
	ctx, span := e.client.tracer.start(ctx, op, attrs...)
	e.firstMu.Unlock()
	if first {
		e.client.metrics.observeReadyToFirstCommand(time.Since(e.createdAt))
	}
	return ctx, span
}

// ID returns the environment's identifier.
func (e *Env) ID() string { return e.id }

// Atespace returns the environment's atespace.
func (e *Env) Atespace() string { return e.atespace }

// Info returns the environment's current status and configuration, as
// Client.Get does.
func (e *Env) Info(ctx context.Context) (*ateenvv1alpha.Environment, error) {
	return e.client.get(ctx, e.conn, e.atespace, e.id)
}

func (e *Env) withEnv(ctx context.Context) context.Context {
	return e.client.withAuth(metadata.AppendToOutgoingContext(ctx, "x-env-id", e.id, "x-env-atespace", e.atespace))
}

// Suspend checkpoints and stops the environment using gRPC; it returns once
// the snapshot is in object storage.
func (e *Env) Suspend(ctx context.Context) error {
	return e.client.suspend(ctx, e.conn, e.atespace, e.id, ateenvv1alpha.SuspendMode_SUSPEND_MODE_UNSPECIFIED)
}

// SuspendLocal checkpoints and stops the environment and returns once the
// snapshot is on the node; see Client.SuspendLocal.
func (e *Env) SuspendLocal(ctx context.Context) error {
	return e.client.suspend(ctx, e.conn, e.atespace, e.id, ateenvv1alpha.SuspendMode_SUSPEND_MODE_LOCAL)
}

// Delete removes the environment permanently using gRPC.
func (e *Env) Delete(ctx context.Context) error {
	return e.client.delete(ctx, e.conn, e.atespace, e.id)
}

// Shell runs a shell command line inside the environment and captures its
// output. It is shorthand for Run with only Command set.
func (e *Env) Shell(ctx context.Context, commandLine string) (*ShellResponse, error) {
	return e.Run(ctx, ShellRequest{Command: commandLine})
}

// Run executes `sh -c req.Command` inside the environment, feeds it
// req.Stdin, and returns its buffered output and exit status once it exits.
// By default it follows the output in one stream while the process runs;
// with req.Poll it polls instead (see ShellRequest.Poll). For incremental
// output or interactive input use StartProcess.
func (e *Env) Run(ctx context.Context, req ShellRequest) (_ *ShellResponse, err error) {
	ctx, span := e.startCommandSpan(ctx, opExec, executable(req.Command))
	began := time.Now()
	defer func() {
		e.client.metrics.observeExec(ctx, time.Since(began), err)
		endSpan(span, err)
	}()
	start := &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", req.Command},
		Cwd:     req.Cwd,
		Env:     req.Env,
		Stdin:   req.Stdin != nil,
	}
	if req.Timeout > 0 {
		start.Timeout = durationpb.New(req.Timeout)
	}
	proc, err := e.startProcess(ctx, start)
	if err != nil {
		return nil, err
	}

	var res *ShellResponse
	var guestTimedOut *bool
	if req.Poll {
		res, guestTimedOut, err = e.runPolled(ctx, proc, req.Stdin, began)
	} else {
		res, guestTimedOut, err = runStreamed(ctx, proc, req.Stdin, began)
	}
	if err != nil {
		return nil, err
	}
	if guestTimedOut != nil {
		res.TimedOut = *guestTimedOut
	} else {
		// A guest that predates Process.timed_out: the exit-code heuristic.
		res.TimedOut = res.ExitCode == 137 && req.Timeout > 0 && res.Duration >= req.Timeout
	}
	span.SetAttributes(AttrExitCode.Int(res.ExitCode), AttrTimedOut.Bool(res.TimedOut))
	return res, nil
}

// writeStdin feeds data to the process's stdin and closes it; nil leaves
// stdin alone.
func writeStdin(ctx context.Context, proc *Process, data []byte) error {
	if data == nil {
		return nil
	}
	w, err := proc.Stdin(ctx)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

// runStreamed follows the process's output in one stream until it exits. It
// also returns the guest's Process.timed_out, nil when the guest does not
// report it.
func runStreamed(ctx context.Context, proc *Process, stdin []byte, began time.Time) (*ShellResponse, *bool, error) {
	if err := writeStdin(ctx, proc, stdin); err != nil {
		return nil, nil, err
	}
	stream, err := proc.Output(ctx, &ateenvv1alpha.StreamProcessOutputRequest{Follow: true})
	if err != nil {
		return nil, nil, err
	}
	var stdoutBuf, stderrBuf bytes.Buffer
	var exit *ateenvv1alpha.Process
	var exited time.Time
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		switch out := msg.GetOutput().(type) {
		case *ateenvv1alpha.ProcessOutput_Stdout:
			stdoutBuf.Write(out.Stdout)
		case *ateenvv1alpha.ProcessOutput_Stderr:
			stderrBuf.Write(out.Stderr)
		case *ateenvv1alpha.ProcessOutput_Exit:
			exit = out.Exit
			exited = time.Now()
		}
	}
	if exit == nil {
		return nil, nil, errors.New("env: output stream ended before the process exited")
	}
	return &ShellResponse{
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
		ExitCode: int(exit.GetExitCode()),
		Duration: exited.Sub(began),
	}, exit.TimedOut, nil
}

const (
	// Polling bounds for a running process with ShellRequest.Poll: short
	// at first so quick commands return quickly, backing off so a long
	// test run does not hammer the API.
	defaultPollMin = 50 * time.Millisecond
	defaultPollMax = 2 * time.Second
	// pollRetries is how many consecutive transient poll errors are
	// tolerated before Run fails; a wake or an API pod restart costs a few.
	pollRetries = 10
	// killTimeout bounds the kill of a process Run abandons.
	killTimeout = 10 * time.Second
)

// runPolled polls the process's state until it exits, then reads its
// buffered output in one non-following stream, so no call stays open while
// the process runs. If it returns before the process has exited, it kills
// the process, so a retry of the same command never runs beside the first
// copy.
func (e *Env) runPolled(ctx context.Context, proc *Process, stdin []byte, began time.Time) (_ *ShellResponse, _ *bool, err error) {
	exited := false
	defer func() {
		if exited {
			return
		}
		kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killTimeout)
		defer cancel()
		if _, kerr := proc.Kill(kctx); kerr != nil && !errors.Is(kerr, ErrProcessExited) && err != nil {
			err = fmt.Errorf("%w (and killing the process failed: %v)", err, kerr)
		}
	}()
	if err := writeStdin(ctx, proc, stdin); err != nil {
		return nil, nil, err
	}

	pMin, pMax := e.client.pollMin, e.client.pollMax
	if pMin <= 0 {
		pMin = defaultPollMin
	}
	if pMax <= 0 {
		pMax = defaultPollMax
	}
	wait := pMin
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(wait):
		}
		// No deadline of its own: a poll that finds the environment
		// suspended waits in ate-env-api for the wake (up to its
		// --wake-budget, five minutes by default), and a shorter deadline
		// would cut a wake that is going to succeed.
		p, perr := e.FindProcess(ctx, proc.ID())
		switch {
		case perr == nil:
			failures = 0
			if p.GetState() == ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
				exited = true
				// Measured before the output read, which a large
				// output over a slow link can stretch.
				d := time.Since(began)
				res, timedOut, err := collectOutput(ctx, proc, int(p.GetExitCode()), p.TimedOut)
				if err != nil {
					return nil, nil, err
				}
				res.Duration = d
				return res, timedOut, nil
			}
		case status.Code(perr) == codes.Unavailable || status.Code(perr) == codes.DeadlineExceeded:
			failures++
			if failures > pollRetries {
				return nil, nil, fmt.Errorf("polling process %s: %w", proc.ID(), perr)
			}
		default:
			// A cancel during the poll comes back as a gRPC Canceled
			// status, which errors.Is does not match to ctx.Err().
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			return nil, nil, perr
		}
		if wait < pMax {
			wait = min(wait*2, pMax)
		}
	}
}

// collectOutput reads an exited process's buffered output in one
// non-following stream until EOF. It returns the guest's Process.timed_out
// (timedOut, or the exit message's when it carries one), nil when the guest
// does not report it.
func collectOutput(ctx context.Context, proc *Process, exitCode int, timedOut *bool) (*ShellResponse, *bool, error) {
	stream, err := proc.Output(ctx, &ateenvv1alpha.StreamProcessOutputRequest{})
	if err != nil {
		return nil, nil, err
	}
	var stdout, stderr bytes.Buffer
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err // Output's Recv maps it already
		}
		switch out := msg.GetOutput().(type) {
		case *ateenvv1alpha.ProcessOutput_Stdout:
			stdout.Write(out.Stdout)
		case *ateenvv1alpha.ProcessOutput_Stderr:
			stderr.Write(out.Stderr)
		case *ateenvv1alpha.ProcessOutput_Exit:
			exitCode = int(out.Exit.GetExitCode())
			if out.Exit.TimedOut != nil {
				timedOut = out.Exit.TimedOut
			}
		}
	}
	return &ShellResponse{Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exitCode}, timedOut, nil
}

// ReadFile streams the contents of the file at path inside the environment using FileSystemService.
// The caller must close the returned reader.
func (e *Env) ReadFile(ctx context.Context, p string) (_ io.ReadCloser, err error) {
	ctx, span := e.client.tracer.start(ctx, opReadFile,
		AttrActorName.String(e.id), AttrAtespace.String(e.atespace), AttrFilePath.String(p))
	// The span stays open while the caller reads and ends, with the size
	// read, at EOF, on an error or on Close.
	defer func() {
		if err != nil {
			endSpan(span, err)
		}
	}()
	rc, err := e.readFile(ctx, p)
	if err != nil {
		return nil, err
	}
	if e.client.tracer.tracer == nil {
		return rc, nil
	}
	return &spanReader{ReadCloser: rc, span: span}, nil
}

func (e *Env) readFile(ctx context.Context, p string) (io.ReadCloser, error) {
	ctx = e.withEnv(ctx)
	stream, err := e.conn.filesystem.ReadFile(ctx, &ateenvv1alpha.ReadFileRequest{
		Path: p,
	})
	if err != nil {
		return nil, fromGRPCError(err)
	}

	// Read the first chunk eagerly to check for immediate errors (e.g. NotFound, PermissionDenied)
	firstChunk, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		// Empty file
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if err != nil {
		return nil, fromGRPCError(err)
	}

	pr, pw := io.Pipe()
	go func() {
		if len(firstChunk.GetChunk()) > 0 {
			if _, writeErr := pw.Write(firstChunk.GetChunk()); writeErr != nil {
				return
			}
		}
		for {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				_ = pw.Close()
				return
			}
			if err != nil {
				_ = pw.CloseWithError(fromGRPCError(err))
				return
			}
			if len(chunk.GetChunk()) > 0 {
				if _, writeErr := pw.Write(chunk.GetChunk()); writeErr != nil {
					return
				}
			}
		}
	}()

	return pr, nil
}

// WriteFile replaces the file at path inside the environment with the
// contents of r, creating it with the given permissions if needed.
func (e *Env) WriteFile(ctx context.Context, p string, r io.Reader, mode fs.FileMode) error {
	return e.writeFile(ctx, p, r, mode, 0)
}

// WriteFileAt writes the contents of r into the file at path starting at
// byte offset, keeping existing content outside the written range. The file
// is created with the given permissions if it does not exist, and extended
// with zero bytes if offset is past its end. An offset of zero replaces the
// file, like WriteFile.
func (e *Env) WriteFileAt(ctx context.Context, p string, offset int64, r io.Reader, mode fs.FileMode) error {
	if offset < 0 {
		return fmt.Errorf("env: negative offset %d for %q", offset, p)
	}
	return e.writeFile(ctx, p, r, mode, offset)
}

func (e *Env) writeFile(ctx context.Context, p string, r io.Reader, mode fs.FileMode, seekOffset int64) (err error) {
	ctx, span := e.client.tracer.start(ctx, opWriteFile,
		AttrActorName.String(e.id), AttrAtespace.String(e.atespace), AttrFilePath.String(p))
	defer func() { endSpan(span, err) }()
	ctx = e.withEnv(ctx)
	stream, err := e.conn.filesystem.WriteFile(ctx)
	if err != nil {
		return fromGRPCError(err)
	}
	// A Send that fails with io.EOF means the server already ended the
	// stream; its status, and the start failure trailer, come from
	// CloseAndRecv. A server that ended it with OK took a short write.
	sendErr := func(err error) error {
		if errors.Is(err, io.EOF) {
			if _, err = stream.CloseAndRecv(); err == nil {
				return errShortWrite
			}
		}
		return fromGRPCError(err)
	}

	buf := make([]byte, 64*1024)
	n, readErr := r.Read(buf)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fmt.Errorf("env: reading data for %q: %w", p, readErr)
	}

	firstReq := &ateenvv1alpha.WriteFileRequest{
		Path:       p,
		Mode:       uint32(mode.Perm()),
		Chunk:      buf[:n],
		SeekOffset: seekOffset,
	}
	if err := stream.Send(firstReq); err != nil {
		return sendErr(err)
	}

	if !errors.Is(readErr, io.EOF) {
		for {
			n, err := r.Read(buf)
			if n > 0 {
				if err := stream.Send(&ateenvv1alpha.WriteFileRequest{Chunk: buf[:n]}); err != nil {
					return sendErr(err)
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return fmt.Errorf("env: reading data for %q: %w", p, err)
			}
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fromGRPCError(err)
	}
	span.SetAttributes(AttrFileSize.Int64(resp.GetBytesWritten()))
	return nil
}

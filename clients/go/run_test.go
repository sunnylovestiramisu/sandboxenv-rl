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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/env/clients/go"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeProcesses is an in-process ProcessService with scripted polls: the
// cases of the loader's runPolledProcess, which this client's Poll mode
// replaces.
type fakeProcesses struct {
	ateenvv1alpha.UnimplementedProcessServiceServer

	mu sync.Mutex
	// pollsUntilExit is how many GetProcess calls a process stays running;
	// pollErrors is how many GetProcess calls answer pollCode first.
	// pollPlan, when set, scripts the first GetProcess calls before that:
	// codes.OK answers as usual, another code fails the call.
	pollsUntilExit int
	pollErrors     int
	pollCode       codes.Code
	pollPlan       []codes.Code
	// pollEntered, when set, makes each GetProcess signal it, then wait
	// for the call's ctx to end.
	pollEntered chan struct{}
	// outputDelay delays each StreamProcessOutput; outputCode, when set,
	// fails it with that code.
	outputDelay    time.Duration
	outputCode     codes.Code
	exitCode       int32
	timedOut       bool
	stdout, stderr string

	nextID     int
	polls      map[string]int
	exited     map[string]bool
	killed     []string
	outputReqs []*ateenvv1alpha.StreamProcessOutputRequest
}

func (f *fakeProcesses) StartProcess(context.Context, *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	return &ateenvv1alpha.Process{ProcessId: fmt.Sprintf("p%d", f.nextID), State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}, nil
}

func (f *fakeProcesses) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	if f.pollEntered != nil {
		f.pollEntered <- struct{}{}
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pollPlan) > 0 {
		code := f.pollPlan[0]
		f.pollPlan = f.pollPlan[1:]
		if code != codes.OK {
			return nil, status.Error(code, "router: no healthy upstream")
		}
	} else if f.pollErrors > 0 {
		f.pollErrors--
		return nil, status.Error(f.pollCode, "router: no healthy upstream")
	}
	id := req.GetProcessId()
	f.polls[id]++
	p := &ateenvv1alpha.Process{ProcessId: id, State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}
	if f.polls[id] > f.pollsUntilExit {
		f.exited[id] = true
	}
	if f.exited[id] {
		p.State = ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED
		p.ExitCode = f.exitCode
		p.TimedOut = f.timedOut
	}
	return p, nil
}

func (f *fakeProcesses) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	f.mu.Lock()
	f.outputReqs = append(f.outputReqs, req)
	stdout, stderr, exited, code, delay, tOut := f.stdout, f.stderr, f.exited[req.GetProcessId()], f.exitCode, f.outputDelay, f.timedOut
	outputCode := f.outputCode
	f.mu.Unlock()
	time.Sleep(delay)
	if outputCode != codes.OK {
		return status.Error(outputCode, "router: no healthy upstream")
	}
	if stdout != "" {
		if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(stdout)}}); err != nil {
			return err
		}
	}
	if stderr != "" {
		if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stderr{Stderr: []byte(stderr)}}); err != nil {
			return err
		}
	}
	if exited {
		return stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{
			ProcessId: req.GetProcessId(), State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: code, TimedOut: tOut}}})
	}
	return nil
}

func (f *fakeProcesses) SignalProcess(_ context.Context, req *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.killed = append(f.killed, req.GetProcessId())
	f.exited[req.GetProcessId()] = true
	f.exitCode = 137
	return &ateenvv1alpha.Process{ProcessId: req.GetProcessId(), State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: 137}, nil
}

// newFakeProcessEnv serves f over an in-memory connection and returns an
// Env on it whose polls run 1 ms apart, backing off to 5 ms.
func newFakeProcessEnv(t *testing.T, f *fakeProcesses) *env.Env {
	t.Helper()
	f.polls = map[string]int{}
	f.exited = map[string]bool{}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c, err := env.NewClient(env.ClientOptions{GRPCConn: conn})
	if err != nil {
		t.Fatal(err)
	}
	env.SetPollBoundsForTest(c, time.Millisecond, 5*time.Millisecond)
	t.Cleanup(func() { _ = c.Close(); _ = conn.Close(); srv.Stop() })
	return c.Env("ate-env", "e1")
}

func (f *fakeProcesses) snapshot() (killed []string, outputReqs []*ateenvv1alpha.StreamProcessOutputRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.killed...), append([]*ateenvv1alpha.StreamProcessOutputRequest(nil), f.outputReqs...)
}

// A polled Run returns the exit code and the output of one non-following
// read after the exit, and kills nothing.
func TestRunPollCollectsExitAndOutput(t *testing.T) {
	f := &fakeProcesses{pollsUntilExit: 3, exitCode: 4, stdout: "out", stderr: "err"}
	e := newFakeProcessEnv(t, f)
	res, err := e.Run(t.Context(), env.ShellRequest{Command: "echo hi", Timeout: time.Minute, Poll: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 4 || res.Stdout != "out" || res.Stderr != "err" {
		t.Fatalf("result %+v", res)
	}
	if res.Duration <= 0 || res.TimedOut {
		t.Fatalf("Duration %v, TimedOut %v; want > 0 and false", res.Duration, res.TimedOut)
	}
	killed, reqs := f.snapshot()
	if len(killed) != 0 {
		t.Fatalf("a process that exited must not be killed: %v", killed)
	}
	if len(reqs) != 1 || reqs[0].GetFollow() || reqs[0].GetProcessId() != "p1" {
		t.Fatalf("output requests %v, want one non-following read of p1", reqs)
	}
	f.mu.Lock()
	polls := f.polls["p1"]
	f.mu.Unlock()
	if polls != 4 {
		t.Fatalf("%d polls, want 4 (3 running, 1 exited)", polls)
	}
}

// PollRetries consecutive Unavailable or DeadlineExceeded polls are
// tolerated; one more fails the Run with that error and kills the process.
func TestRunPollTransientPollErrors(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			f := &fakeProcesses{pollsUntilExit: 1, pollErrors: env.PollRetries, pollCode: code}
			e := newFakeProcessEnv(t, f)
			if _, err := e.Run(t.Context(), env.ShellRequest{Command: "true", Poll: true}); err != nil {
				t.Fatalf("%d transient poll errors must be tolerated: %v", env.PollRetries, err)
			}

			f = &fakeProcesses{pollsUntilExit: 100, pollErrors: env.PollRetries + 1, pollCode: code}
			e = newFakeProcessEnv(t, f)
			_, err := e.Run(t.Context(), env.ShellRequest{Command: "sleep 100", Poll: true})
			if status.Code(err) != code {
				t.Fatalf("expected the poll error after %d transient errors, got %v", env.PollRetries+1, err)
			}
			if killed, _ := f.snapshot(); len(killed) != 1 {
				t.Fatalf("an abandoned process must be killed once, got %v", killed)
			}
		})
	}
}

// A successful poll resets the count of transient errors: two runs of
// PollRetries/2+1 errors, split by one success, do not fail the Run.
func TestRunPollTransientCountResetsOnSuccess(t *testing.T) {
	burst := env.PollRetries/2 + 1
	var plan []codes.Code
	for range burst {
		plan = append(plan, codes.Unavailable)
	}
	plan = append(plan, codes.OK)
	for range burst {
		plan = append(plan, codes.Unavailable)
	}
	f := &fakeProcesses{pollsUntilExit: 1, pollPlan: plan}
	e := newFakeProcessEnv(t, f)
	if _, err := e.Run(t.Context(), env.ShellRequest{Command: "true", Poll: true}); err != nil {
		t.Fatalf("%d transient errors, a success, %d more: %v", burst, burst, err)
	}
	if killed, _ := f.snapshot(); len(killed) != 0 {
		t.Fatalf("a process that exited must not be killed: %v", killed)
	}
}

// A cancel during an in-flight poll returns ctx.Err() and kills the
// process.
func TestRunPollCancelDuringPoll(t *testing.T) {
	f := &fakeProcesses{pollEntered: make(chan struct{}, 1)}
	e := newFakeProcessEnv(t, f)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := e.Run(ctx, env.ShellRequest{Command: "sleep 100", Poll: true})
		errc <- err
	}()
	<-f.pollEntered
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancelled during a poll = %v, want context.Canceled", err)
	}
	if killed, _ := f.snapshot(); len(killed) != 1 {
		t.Fatalf("an abandoned process must be killed once, got %v", killed)
	}
}

// A stdin write that fails fails the Run and kills the process. The fake
// has no WriteProcessInput, so the write answers Unimplemented.
func TestRunPollStdinFailureKills(t *testing.T) {
	f := &fakeProcesses{pollsUntilExit: 100}
	e := newFakeProcessEnv(t, f)
	_, err := e.Run(t.Context(), env.ShellRequest{Command: "cat", Stdin: []byte("in"), Poll: true})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Run = %v, want the Unimplemented stdin error", err)
	}
	if killed, _ := f.snapshot(); len(killed) != 1 {
		t.Fatalf("an abandoned process must be killed once, got %v", killed)
	}
}

// A polled Run whose output read fails after the exit returns that error
// mapped once: one "env:" prefix, the code readable with status.Code.
func TestRunPollOutputReadErrorMappedOnce(t *testing.T) {
	f := &fakeProcesses{pollsUntilExit: 1, outputCode: codes.Unavailable}
	e := newFakeProcessEnv(t, f)
	_, err := e.Run(t.Context(), env.ShellRequest{Command: "true", Poll: true})
	if err == nil {
		t.Fatal("Run succeeded, want the output read's error")
	}
	if n := strings.Count(err.Error(), "env:"); n != 1 {
		t.Fatalf("error %q has %d \"env:\" prefixes, want 1", err, n)
	}
	if status.Code(err) != codes.Unavailable || !env.Retryable(err) {
		t.Fatalf("status.Code = %v, Retryable = %v; want Unavailable and true", status.Code(err), env.Retryable(err))
	}
}

// Duration ends at the poll that saw the exit, before the output read.
func TestRunPollDurationExcludesOutputRead(t *testing.T) {
	const delay = 300 * time.Millisecond
	f := &fakeProcesses{pollsUntilExit: 1, outputDelay: delay}
	e := newFakeProcessEnv(t, f)
	res, err := e.Run(t.Context(), env.ShellRequest{Command: "true", Poll: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Duration <= 0 || res.Duration >= delay {
		t.Fatalf("Duration = %v, want > 0 and below the %v output read", res.Duration, delay)
	}
}

// Another poll error fails the Run at once and kills the process.
func TestRunPollOtherPollErrorFails(t *testing.T) {
	f := &fakeProcesses{pollsUntilExit: 100, pollErrors: 1, pollCode: codes.Internal}
	e := newFakeProcessEnv(t, f)
	_, err := e.Run(t.Context(), env.ShellRequest{Command: "sleep 100", Poll: true})
	if status.Code(err) != codes.Internal {
		t.Fatalf("Run = %v, want the Internal poll error", err)
	}
	if killed, _ := f.snapshot(); len(killed) != 1 {
		t.Fatalf("an abandoned process must be killed once, got %v", killed)
	}
}

// Cancelling ctx kills the process; FindProcess then reports it exited.
func TestRunPollKillsOnCancel(t *testing.T) {
	f := &fakeProcesses{pollsUntilExit: 1 << 30}
	e := newFakeProcessEnv(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := e.Run(ctx, env.ShellRequest{Command: "sleep 100", Poll: true}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a cancelled Run = %v, want context.DeadlineExceeded", err)
	}
	if killed, _ := f.snapshot(); len(killed) != 1 || killed[0] != "p1" {
		t.Fatalf("the process must be killed after a cancel, got %v", killed)
	}
	p, err := e.FindProcess(t.Context(), "p1")
	if err != nil || p.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
		t.Fatalf("FindProcess after the cancel = %v, %v; want EXITED", p, err)
	}
}

// A polled Run holds no guest stream while the process runs: the router
// sees no StreamProcessOutput until the exit, then one read. The streaming
// Run holds one, which shows the count sees it.
func TestRunPollHoldsNoStreamWhileRunning(t *testing.T) {
	f := newFixture(t)
	sb := f.create(t, "sb-poll-streams")
	const path = ateenvv1alpha.ProcessService_StreamProcessOutput_FullMethodName
	for _, poll := range []bool{true, false} {
		t.Run(fmt.Sprintf("poll=%v", poll), func(t *testing.T) {
			_, before := f.router.GuestRequests(path)
			type result struct {
				res *env.ShellResponse
				err error
			}
			done := make(chan result, 1)
			start := time.Now()
			go func() {
				res, err := sb.Run(t.Context(), env.ShellRequest{Command: "sleep 1; echo done", Poll: poll})
				done <- result{res, err}
			}()
			// Sample while the command sleeps.
			maxOpen := 0
			for time.Since(start) < 700*time.Millisecond {
				open, _ := f.router.GuestRequests(path)
				maxOpen = max(maxOpen, open)
				time.Sleep(5 * time.Millisecond)
			}
			r := <-done
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.res.Stdout != "done\n" || r.res.ExitCode != 0 {
				t.Fatalf("result %+v", r.res)
			}
			if r.res.Duration < time.Second {
				t.Errorf("Duration = %v, want at least the 1 s the command sleeps", r.res.Duration)
			}
			_, after := f.router.GuestRequests(path)
			if poll {
				if maxOpen != 0 || after-before != 1 {
					t.Errorf("polled Run: %d streams open while the process ran, %d in all; want 0 and 1", maxOpen, after-before)
				}
			} else if maxOpen != 1 {
				t.Errorf("streaming Run: %d streams open while the process ran, want 1", maxOpen)
			}
		})
	}
}

// TimedOut is exit 137 at or past Timeout, in both modes: a command the
// guest's timer kills, and not one that SIGKILLs itself early.
func TestRunTimedOutAndDuration(t *testing.T) {
	f := newFixture(t)
	sb := f.create(t, "sb-timed-out")
	for _, poll := range []bool{false, true} {
		t.Run(fmt.Sprintf("poll=%v", poll), func(t *testing.T) {
			res, err := sb.Run(t.Context(), env.ShellRequest{Command: "sleep 10", Timeout: time.Second, Poll: poll})
			if err != nil {
				t.Fatal(err)
			}
			if res.ExitCode != 137 || !res.TimedOut || res.Duration < time.Second {
				t.Errorf("sleep 10 with a 1 s timeout = %+v, want exit 137, TimedOut, Duration >= 1s", res)
			}

			res, err = sb.Run(t.Context(), env.ShellRequest{Command: "kill -9 $$", Timeout: time.Minute, Poll: poll})
			if err != nil {
				t.Fatal(err)
			}
			if res.ExitCode != 137 || res.TimedOut || res.Duration <= 0 {
				t.Errorf("kill -9 $$ = %+v, want exit 137, not TimedOut, Duration > 0", res)
			}

			res, err = sb.Run(t.Context(), env.ShellRequest{Command: "true", Poll: poll})
			if err != nil {
				t.Fatal(err)
			}
			if res.ExitCode != 0 || res.TimedOut || res.Duration <= 0 {
				t.Errorf("true = %+v, want exit 0, not TimedOut, Duration > 0", res)
			}
		})
	}
}

// Cancelling the ctx of a polled Run kills the guest process before Run
// returns.
func TestRunPollCancelKillsGuestProcess(t *testing.T) {
	f := newFixture(t)
	sb := f.create(t, "sb-poll-cancel")
	pidFile := filepath.Join(t.TempDir(), "pid")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := sb.Run(ctx, env.ShellRequest{Command: "echo $$ > " + pidFile + "; exec sleep 30", Poll: true})
		errc <- err
	}()
	var pid int
	for deadline := time.Now().Add(10 * time.Second); ; {
		if b, err := os.ReadFile(pidFile); err == nil && strings.HasSuffix(string(b), "\n") {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(b)))
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("process %d is not running before the cancel: %v", pid, err)
	}
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled Run = %v, want context.Canceled", err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process %d after the cancel: kill -0 = %v, want ESRCH", pid, err)
	}
}

func TestRunTimedOutGuestField(t *testing.T) {
	f := &fakeProcesses{pollsUntilExit: 1, exitCode: 137, timedOut: true}
	e := newFakeProcessEnv(t, f)
	res, err := e.Run(t.Context(), env.ShellRequest{Command: "sleep 10", Timeout: 0, Poll: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut {
		t.Errorf("expected TimedOut=true from guest field when Timeout=0, got false")
	}
}

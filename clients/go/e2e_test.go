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

// End-to-end tests for the Go client, mirroring
// clients/python/tests/e2e/test_guest_daemon.py and the client cases of
// clients/python/tests/e2e/test_full_stack.py.
//
// Both tests skip when their target environment variable is unset, so
// `go test ./clients/go` stays fast without external services.
//
// Against a standalone guest daemon (no cluster):
//
//	go run ./examples/guest-daemon --listen 127.0.0.1:8090 --workspace "$(mktemp -d)"
//	ATE_ENV_GUEST_TARGET=127.0.0.1:8090 go test ./clients/go -run TestE2E
//
// Against a reachable ate-env-api (port-forwarded from a cluster):
//
//	kubectl -n ate-env port-forward svc/ate-env-api 17777:7777
//	ATE_ENV_API_TARGET=127.0.0.1:17777 go test ./clients/go -run TestE2E
//
// Optional variables for TestE2EFullStack match test_full_stack.py:
// SUBSTRATE_ENV_AUTH_TOKEN, ATE_ENV_TEMPLATE, ATE_ENV_ROOTFS_IMAGE,
// ATE_ENV_READY_TIMEOUT, ATE_ENV_EXPECTED_MARKER, and NAMESPACE.
package env_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/internal/portproxy"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestE2EGuestDaemon(t *testing.T) {
	target := strings.TrimSpace(os.Getenv("ATE_ENV_GUEST_TARGET"))
	if target == "" {
		t.Skip("set ATE_ENV_GUEST_TARGET=host:port of a running examples/guest-daemon")
	}

	newGuestEnv := func(t *testing.T) *env.Env {
		t.Helper()
		c, err := env.NewClient(env.ClientOptions{Endpoint: target})
		if err != nil {
			t.Fatalf("NewClient(%q): %v", target, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c.Env("", "e2e")
	}

	t.Run("shell_roundtrip", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		res, err := guestEnv.Shell(t.Context(), "echo hello from e2e")
		if err != nil {
			t.Fatalf("Shell: %v", err)
		}
		if res.ExitCode != 0 {
			t.Errorf("ExitCode = %d, want 0", res.ExitCode)
		}
		if res.Stdout != "hello from e2e\n" {
			t.Errorf("Stdout = %q, want %q", res.Stdout, "hello from e2e\n")
		}
	})

	t.Run("shell_nonzero_exit_and_stderr", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		res, err := guestEnv.Shell(t.Context(), "echo oops >&2; exit 3")
		if err != nil {
			t.Fatalf("Shell: %v", err)
		}
		if res.ExitCode != 3 {
			t.Errorf("ExitCode = %d, want 3", res.ExitCode)
		}
		if !strings.Contains(res.Stderr, "oops") {
			t.Errorf("Stderr = %q, want substring %q", res.Stderr, "oops")
		}
	})

	t.Run("file_roundtrip", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		path := "ate-env-e2e-" + e2eRandomHex(t, 16) + ".txt"
		content := bytes.Repeat([]byte("line1\nline2\n"), 10_000)
		if err := guestEnv.WriteFile(ctx, path, bytes.NewReader(content), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if got := e2eReadFileBytes(t, ctx, guestEnv, path); !bytes.Equal(got, content) {
			t.Errorf("ReadFile len = %d, want %d matching bytes", len(got), len(content))
		}
	})

	t.Run("write_file_seek_offset", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		path := "ate-env-e2e-" + e2eRandomHex(t, 16) + ".txt"
		if err := guestEnv.WriteFile(ctx, path, strings.NewReader("hello world"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := guestEnv.WriteFileAt(ctx, path, 6, strings.NewReader("W"), 0o644); err != nil {
			t.Fatalf("WriteFileAt(6): %v", err)
		}
		if got := string(e2eReadFileBytes(t, ctx, guestEnv, path)); got != "hello World" {
			t.Errorf("after seek 6: got %q, want %q", got, "hello World")
		}
		if err := guestEnv.WriteFileAt(ctx, path, 13, strings.NewReader("!"), 0o644); err != nil {
			t.Fatalf("WriteFileAt(13): %v", err)
		}
		const wantExtended = "hello World\x00\x00!"
		if got := string(e2eReadFileBytes(t, ctx, guestEnv, path)); got != wantExtended {
			t.Errorf("after seek 13: got %q, want %q", got, wantExtended)
		}
	})

	t.Run("output_follow_ends_with_exit", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "echo one; echo two; echo err >&2; exit 4"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		stdout, stderr, exit, _ := e2eCollectOutput(t, ctx, proc, &ateenvv1alpha.StreamProcessOutputRequest{Follow: true})
		if string(stdout) != "one\ntwo\n" {
			t.Errorf("stdout = %q, want %q", stdout, "one\ntwo\n")
		}
		if string(stderr) != "err\n" {
			t.Errorf("stderr = %q, want %q", stderr, "err\n")
		}
		if exit == nil {
			t.Fatal("missing exit message")
		}
		if exit.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
			t.Errorf("state = %v, want EXITED", exit.GetState())
		}
		if exit.GetExitCode() != 4 {
			t.Errorf("exit_code = %d, want 4", exit.GetExitCode())
		}
	})

	t.Run("kill_process", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sleep", "30"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		markExited := e2eCleanupKill(t, proc)
		info, err := proc.Kill(ctx)
		if err != nil {
			t.Fatalf("Kill: %v", err)
		}
		markExited()
		if info.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
			t.Errorf("state = %v, want EXITED", info.GetState())
		}
		if info.GetExitCode() != 137 {
			t.Errorf("exit_code = %d, want 137", info.GetExitCode())
		}
		// Idempotent, and other signals are refused once exited.
		again, err := proc.Kill(ctx)
		if err != nil {
			t.Fatalf("second Kill: %v", err)
		}
		if again.GetExitCode() != 137 {
			t.Errorf("second Kill exit_code = %d, want 137", again.GetExitCode())
		}
		if err := proc.Signal(ctx, ateenvv1alpha.Signal_SIGNAL_TERM); !errors.Is(err, env.ErrProcessExited) {
			t.Errorf("Signal(TERM) after exit: err = %v, want ErrProcessExited", err)
		}
	})

	t.Run("signal_term", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sleep", "30"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		markExited := e2eCleanupKill(t, proc)
		if err := proc.Signal(ctx, ateenvv1alpha.Signal_SIGNAL_TERM); err != nil {
			t.Fatalf("Signal(TERM): %v", err)
		}
		info, err := proc.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		markExited()
		if info.GetExitCode() != 143 {
			t.Errorf("exit_code = %d, want 143", info.GetExitCode())
		}
	})

	t.Run("signal_trapped", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "trap 'echo got-usr1; exit 7' USR1; echo trap-ready; while :; do sleep 0.05; done"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		markExited := e2eCleanupKill(t, proc)
		e2eWaitForStdout(t, ctx, proc, "trap-ready", 5*time.Second)
		if err := proc.Signal(ctx, ateenvv1alpha.Signal_SIGNAL_USR1); err != nil {
			t.Fatalf("Signal(USR1): %v", err)
		}
		info, err := proc.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		markExited()
		if info.GetExitCode() != 7 {
			t.Errorf("exit_code = %d, want 7", info.GetExitCode())
		}
		stdout, _, _, _ := e2eCollectOutput(t, ctx, proc, nil)
		if !strings.Contains(string(stdout), "got-usr1") {
			t.Errorf("stdout = %q, want substring %q", stdout, "got-usr1")
		}
	})

	t.Run("stdin_streaming", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"cat"},
			Stdin:   true,
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		w, err := proc.Stdin(ctx)
		if err != nil {
			t.Fatalf("Stdin: %v", err)
		}
		if _, err := w.Write([]byte("hello ")); err != nil {
			t.Fatalf("Write 1: %v", err)
		}
		if _, err := w.Write([]byte("world\n")); err != nil {
			t.Fatalf("Write 2: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		info, err := proc.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if info.GetExitCode() != 0 {
			t.Errorf("exit_code = %d, want 0", info.GetExitCode())
		}
		stdout, _, _, _ := e2eCollectOutput(t, ctx, proc, nil)
		if string(stdout) != "hello world\n" {
			t.Errorf("stdout = %q, want %q", stdout, "hello world\n")
		}
	})

	t.Run("stdin_not_requested", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"cat"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		info, err := proc.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if info.GetExitCode() != 0 {
			t.Errorf("exit_code = %d, want 0", info.GetExitCode())
		}
		w, err := proc.Stdin(ctx)
		if err != nil {
			t.Fatalf("Stdin: %v", err)
		}
		_, werr := w.Write([]byte("x"))
		cerr := w.Close()
		err = errors.Join(werr, cerr)
		if !errors.Is(err, env.ErrFailedPrecondition) || !strings.Contains(err.Error(), "without stdin") {
			t.Errorf("stdin without request: err = %v, want ErrFailedPrecondition mentioning \"without stdin\"", err)
		}
	})

	t.Run("shell_with_stdin", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		res, err := guestEnv.Run(t.Context(), env.ShellRequest{
			Command: "tr a-z A-Z",
			Stdin:   []byte("shout\n"),
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.Stdout != "SHOUT\n" {
			t.Errorf("Stdout = %q, want %q", res.Stdout, "SHOUT\n")
		}
		if res.ExitCode != 0 {
			t.Errorf("ExitCode = %d, want 0", res.ExitCode)
		}
	})

	t.Run("shell_timeout", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		res, err := guestEnv.Run(t.Context(), env.ShellRequest{
			Command: "sleep 30",
			Timeout: 200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.ExitCode != 137 {
			t.Errorf("ExitCode = %d, want 137", res.ExitCode)
		}
		if !res.TimedOut {
			t.Errorf("TimedOut = false, want true (Duration = %v)", res.Duration)
		}
	})

	t.Run("cwd_passthrough", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"pwd"},
			Cwd:     "/",
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		info, err := proc.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if info.GetExitCode() != 0 {
			t.Errorf("exit_code = %d, want 0", info.GetExitCode())
		}
		stdout, _, _, _ := e2eCollectOutput(t, ctx, proc, nil)
		if string(stdout) != "/\n" {
			t.Errorf("stdout = %q, want %q", stdout, "/\n")
		}
	})

	t.Run("env_passthrough", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "echo $ATE_E2E_MARKER"},
			Env:     map[string]string{"ATE_E2E_MARKER": "marker-42"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		if _, err := proc.Wait(ctx); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		stdout, _, _, _ := e2eCollectOutput(t, ctx, proc, nil)
		if string(stdout) != "marker-42\n" {
			t.Errorf("stdout = %q, want %q", stdout, "marker-42\n")
		}
	})

	t.Run("process_info", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"true"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		info, err := proc.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if !slices.Equal(info.GetCommand(), []string{"true"}) {
			t.Errorf("command = %v, want [\"true\"]", info.GetCommand())
		}
		if info.GetPid() <= 0 {
			t.Errorf("pid = %d, want > 0", info.GetPid())
		}
		if info.GetStartedAt() == nil || info.GetFinishedAt() == nil {
			t.Fatalf("timestamps missing: started=%v finished=%v", info.GetStartedAt(), info.GetFinishedAt())
		}
		if info.GetFinishedAt().AsTime().Before(info.GetStartedAt().AsTime()) {
			t.Errorf("finished_at (%v) before started_at (%v)", info.GetFinishedAt().AsTime(), info.GetStartedAt().AsTime())
		}
		got, err := guestEnv.Process(proc.ID()).Info(ctx)
		if err != nil {
			t.Fatalf("Process.Info: %v", err)
		}
		if !proto.Equal(got, info) {
			t.Errorf("Process.Info = %v, want %v", got, info)
		}
	})

	t.Run("binary_file_roundtrip", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		path := "ate-env-e2e-" + e2eRandomHex(t, 16) + ".bin"
		content := e2eRandomBytes(t, 256*1024+7)
		if err := guestEnv.WriteFile(ctx, path, bytes.NewReader(content), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if got := e2eReadFileBytes(t, ctx, guestEnv, path); !bytes.Equal(got, content) {
			t.Errorf("ReadFile len = %d, want %d matching bytes", len(got), len(content))
		}
	})

	t.Run("empty_file_roundtrip", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		path := "ate-env-e2e-" + e2eRandomHex(t, 16) + ".empty"
		if err := guestEnv.WriteFile(ctx, path, bytes.NewReader(nil), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if got := e2eReadFileBytes(t, ctx, guestEnv, path); len(got) != 0 {
			t.Errorf("ReadFile = %q, want empty", got)
		}
	})

	t.Run("write_file_from_async_generator", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		path := "ate-env-e2e-" + e2eRandomHex(t, 16) + ".gen"
		chunks := make([][]byte, 5)
		for i := range chunks {
			chunks[i] = fmt.Appendf(nil, "part-%d;", i)
		}
		if err := guestEnv.WriteFile(ctx, path, &e2eChunkReader{chunks: chunks}, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		const want = "part-0;part-1;part-2;part-3;part-4;"
		if got := string(e2eReadFileBytes(t, ctx, guestEnv, path)); got != want {
			t.Errorf("ReadFile = %q, want %q", got, want)
		}
	})

	t.Run("output_offset_replay", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "printf abcdef; printf 123456 >&2"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		if _, err := proc.Wait(ctx); err != nil {
			t.Fatalf("Wait: %v", err)
		}

		stdout, stderr, _, full := e2eCollectOutput(t, ctx, proc, nil)
		if string(stdout) != "abcdef" {
			t.Errorf("full stdout = %q, want %q", stdout, "abcdef")
		}
		if string(stderr) != "123456" {
			t.Errorf("full stderr = %q, want %q", stderr, "123456")
		}
		if len(full) == 0 || full[len(full)-1].GetExit() == nil {
			t.Errorf("last message = %v, want non-nil exit", full)
		}

		repOut, repErr, _, _ := e2eCollectOutput(t, ctx, proc, &ateenvv1alpha.StreamProcessOutputRequest{
			StdoutOffset: 4,
			StderrOffset: 2,
		})
		if string(repOut) != "ef" {
			t.Errorf("replay stdout = %q, want %q", repOut, "ef")
		}
		if string(repErr) != "3456" {
			t.Errorf("replay stderr = %q, want %q", repErr, "3456")
		}
	})

	t.Run("concurrent_shells", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx := t.Context()
		const n = 4
		results := make([]*env.ShellResponse, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = guestEnv.Shell(ctx, fmt.Sprintf("echo job-%d; exit %d", i, i))
			}(i)
		}
		wg.Wait()
		for i := range n {
			if errs[i] != nil {
				t.Fatalf("Shell(%d): %v", i, errs[i])
			}
			want := fmt.Sprintf("job-%d\n", i)
			if results[i].Stdout != want {
				t.Errorf("Shell(%d) Stdout = %q, want %q", i, results[i].Stdout, want)
			}
			if results[i].ExitCode != i {
				t.Errorf("Shell(%d) ExitCode = %d, want %d", i, results[i].ExitCode, i)
			}
		}
	})

	t.Run("missing_file_maps_to_not_found", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		rc, err := guestEnv.ReadFile(t.Context(), "nope-"+e2eRandomHex(t, 16)+".txt")
		if err == nil {
			_ = rc.Close()
		}
		if !errors.Is(err, env.ErrNotFound) || status.Code(err) != codes.NotFound {
			t.Errorf("ReadFile missing: err = %v (code %v), want ErrNotFound / NotFound", err, status.Code(err))
		}
	})

	t.Run("missing_process_maps_to_not_found", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		_, err := guestEnv.Process("bogus-process-id").Info(t.Context())
		if !errors.Is(err, env.ErrNotFound) || status.Code(err) != codes.NotFound {
			t.Errorf("Process.Info missing: err = %v (code %v), want ErrNotFound / NotFound", err, status.Code(err))
		}
	})

	t.Run("sandbox_escape_maps_to_permission_denied", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		rc, err := guestEnv.ReadFile(t.Context(), "../escape-attempt.txt")
		if err == nil {
			_ = rc.Close()
		}
		if !errors.Is(err, env.ErrPermissionDenied) || status.Code(err) != codes.PermissionDenied {
			t.Fatalf("ReadFile escape: err = %v (code %v), want ErrPermissionDenied / PermissionDenied", err, status.Code(err))
		}
		if !strings.Contains(err.Error(), "outside sandbox root") {
			t.Errorf("ReadFile escape error = %q, want substring %q", err.Error(), "outside sandbox root")
		}
	})

	t.Run("kill_ends_follow_stream", func(t *testing.T) {
		guestEnv := newGuestEnv(t)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		proc, err := guestEnv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "echo started; sleep 30"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		markExited := e2eCleanupKill(t, proc)
		stream, err := proc.Output(ctx, &ateenvv1alpha.StreamProcessOutputRequest{Follow: true})
		if err != nil {
			t.Fatalf("Output: %v", err)
		}

		type result struct {
			stdout bytes.Buffer
			msgs   []*ateenvv1alpha.ProcessOutput
			err    error
		}
		started := make(chan struct{})
		done := make(chan result, 1)
		go func() {
			var res result
			var once sync.Once
			for {
				msg, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					res.err = err
					break
				}
				res.msgs = append(res.msgs, msg)
				if out := msg.GetStdout(); len(out) > 0 {
					res.stdout.Write(out)
					if strings.Contains(res.stdout.String(), "started") {
						once.Do(func() { close(started) })
					}
				}
			}
			done <- res
		}()

		select {
		case <-started:
		case res := <-done:
			t.Fatalf("stream ended before emitting \"started\" (err=%v, stdout=%q)", res.err, res.stdout.String())
		case <-ctx.Done():
			t.Fatalf("timed out waiting for \"started\": %v", ctx.Err())
		}

		if _, err := proc.Kill(ctx); err != nil {
			t.Fatalf("Kill: %v", err)
		}
		markExited()

		select {
		case res := <-done:
			if res.err != nil {
				t.Fatalf("stream Recv: %v", res.err)
			}
			if !strings.Contains(res.stdout.String(), "started") {
				t.Errorf("stdout = %q, want substring %q", res.stdout.String(), "started")
			}
			if len(res.msgs) == 0 || res.msgs[len(res.msgs)-1].GetExit() == nil {
				t.Fatalf("last message = %v, want non-nil exit", res.msgs)
			}
			if got := res.msgs[len(res.msgs)-1].GetExit().GetExitCode(); got != 137 {
				t.Errorf("exit_code = %d, want 137", got)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for follow stream to end after Kill")
		}
	})
}

const e2ePortServerPy = `
import socket, threading, base64, hashlib

def handle(conn):
    try:
        buf = bytearray()
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(1024)
            if not chunk:
                return
            buf.extend(chunk)
        headers_raw, _, leftover = bytes(buf).partition(b"\r\n\r\n")
        lines = headers_raw.decode("latin1").split("\r\n")
        req_line = lines[0]
        headers = {}
        for line in lines[1:]:
            if not line:
                break
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()

        if "authorization" in headers or "x-env-authorization" in headers:
            conn.sendall(b"HTTP/1.1 500 Credential Leaked\r\nConnection: close\r\n\r\n")
            return

        if headers.get("upgrade", "").lower() == "websocket":
            key = headers.get("sec-websocket-key", "")
            magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
            accept = base64.b64encode(hashlib.sha1((key + magic).encode()).digest()).decode()
            resp = (
                f"HTTP/1.1 101 Switching Protocols\r\n"
                f"Upgrade: websocket\r\n"
                f"Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                f"\r\n"
            ).encode("latin1")
            conn.sendall(resp)
            if leftover:
                conn.sendall(leftover)
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                conn.sendall(data)
            return

        if req_line.startswith("GET /hello"):
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 13\r\n"
                b"Connection: close\r\n"
                b"\r\n"
                b"port-proxy-ok"
            )
            return

        conn.sendall(b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n")
    finally:
        try:
            conn.close()
        except Exception:
            pass

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 8888))
s.listen(128)
while True:
    conn, _ = s.accept()
    threading.Thread(target=handle, args=(conn,), daemon=True).start()
`

func TestE2EFullStack(t *testing.T) {
	target := strings.TrimSpace(os.Getenv("ATE_ENV_API_TARGET"))
	if target == "" {
		t.Skip("set ATE_ENV_API_TARGET=host:port of a reachable ate-env-api")
	}

	httpTarget := e2eNormalizeHTTPTarget(t, target)
	readyTimeout := e2eParseReadyTimeout(t)
	templateName := os.Getenv("ATE_ENV_TEMPLATE")
	rootfsImage := os.Getenv("ATE_ENV_ROOTFS_IMAGE")
	authToken := os.Getenv("SUBSTRATE_ENV_AUTH_TOKEN")
	expectedMarker := os.Getenv("ATE_ENV_EXPECTED_MARKER")

	// When NAMESPACE is unset or the default atespace, leave request atespaces
	// empty so ate-env-api's default resolution to env.DefaultAtespace is
	// exercised, matching test_full_stack.py; otherwise route into NAMESPACE.
	ns := strings.TrimSpace(os.Getenv("NAMESPACE"))
	reqAtespace := ""
	wantAtespace := env.DefaultAtespace
	if ns != "" && ns != env.DefaultAtespace {
		reqAtespace = ns
		wantAtespace = ns
	}

	newAPIClient := func(t *testing.T) *env.Client {
		t.Helper()
		c, err := env.NewClient(env.ClientOptions{
			Endpoint:  target,
			AuthToken: authToken,
		})
		if err != nil {
			t.Fatalf("NewClient(%q): %v", target, err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	t.Run("get_missing_environment", func(t *testing.T) {
		client := newAPIClient(t)
		missingID := "goe2e-missing-" + e2eRandomHex(t, 4)
		_, err := client.Get(t.Context(), reqAtespace, missingID)
		if !errors.Is(err, env.ErrNotFound) || status.Code(err) != codes.NotFound {
			t.Errorf("Get(%q): err = %v (code %v), want ErrNotFound / NotFound", missingID, err, status.Code(err))
		}
	})

	t.Run("full_lifecycle", func(t *testing.T) {
		client := newAPIClient(t)
		ctx := t.Context()
		envID := "goe2e-" + e2eRandomHex(t, 4)
		sb, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{
			Id:       envID,
			Atespace: reqAtespace,
			Template: e2eOptionalTemplate(templateName),
		})
		if err != nil {
			t.Fatalf("Create(%q): %v", envID, err)
		}
		deleted := false
		t.Cleanup(func() {
			if deleted {
				return
			}
			delCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := e2eDeleteEnv(delCtx, sb); err != nil && !errors.Is(err, env.ErrNotFound) {
				t.Errorf("cleanup delete of %s failed: %v", envID, err)
			}
		})

		if sb.ID() != envID {
			t.Errorf("ID() = %q, want %q", sb.ID(), envID)
		}
		if sb.Atespace() != wantAtespace {
			t.Errorf("Atespace() = %q, want %q", sb.Atespace(), wantAtespace)
		}

		info, err := sb.Info(ctx)
		if err != nil {
			t.Fatalf("Info: %v", err)
		}
		if info.GetId() != envID {
			t.Errorf("Info.Id = %q, want %q", info.GetId(), envID)
		}
		if info.GetStatus() == ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_UNSPECIFIED {
			t.Errorf("Info.Status = UNSPECIFIED, want concrete status")
		}

		e2eWaitUntilServing(t, ctx, sb, readyTimeout)

		if expectedMarker != "" {
			res, err := sb.Shell(ctx, "echo $E2E_TEMPLATE_MARKER")
			if err != nil {
				t.Fatalf("Shell marker: %v", err)
			}
			if res.Stdout != expectedMarker+"\n" {
				t.Errorf("E2E_TEMPLATE_MARKER = %q, want %q", res.Stdout, expectedMarker+"\n")
			}
		}

		// Shell: output, stderr, exit codes.
		res, err := sb.Shell(ctx, "echo out; echo err >&2; exit 7")
		if err != nil {
			t.Fatalf("Shell: %v", err)
		}
		if res.Stdout != "out\n" || res.Stderr != "err\n" || res.ExitCode != 7 {
			t.Errorf("Shell = %+v, want stdout=%q stderr=%q exit=7", res, "out\n", "err\n")
		}

		// Files: binary roundtrip and NotFound mapping through the proxy.
		content := e2eRandomBytes(t, 128*1024+3)
		path := "/tmp/goe2e-" + e2eRandomHex(t, 4) + ".bin"
		if err := sb.WriteFile(ctx, path, bytes.NewReader(content), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if got := e2eReadFileBytes(t, ctx, sb, path); !bytes.Equal(got, content) {
			t.Errorf("ReadFile len = %d, want %d matching bytes", len(got), len(content))
		}
		rc, err := sb.ReadFile(ctx, "/tmp/goe2e-does-not-exist")
		if err == nil {
			_ = rc.Close()
		}
		if !errors.Is(err, env.ErrNotFound) {
			t.Errorf("ReadFile missing: err = %v, want ErrNotFound", err)
		}

		// Processes: follow-stream with exit, stdin, signals.
		proc, err := sb.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "echo one; echo two >&2"},
		})
		if err != nil {
			t.Fatalf("StartProcess: %v", err)
		}
		stdout, stderr, exit, _ := e2eCollectOutput(t, ctx, proc, &ateenvv1alpha.StreamProcessOutputRequest{Follow: true})
		if string(stdout) != "one\n" || string(stderr) != "two\n" {
			t.Errorf("output = (%q, %q), want (\"one\\n\", \"two\\n\")", stdout, stderr)
		}
		if exit == nil || exit.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED || exit.GetExitCode() != 0 {
			t.Errorf("exit = %v, want EXITED with 0", exit)
		}

		cat, err := sb.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"cat"},
			Stdin:   true,
		})
		if err != nil {
			t.Fatalf("StartProcess(cat): %v", err)
		}
		w, err := cat.Stdin(ctx)
		if err != nil {
			t.Fatalf("cat.Stdin: %v", err)
		}
		if _, err := w.Write([]byte("through the proxy\n")); err != nil {
			t.Fatalf("cat.Write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("cat.Close: %v", err)
		}
		catInfo, err := cat.Wait(ctx)
		if err != nil {
			t.Fatalf("cat.Wait: %v", err)
		}
		if catInfo.GetExitCode() != 0 {
			t.Errorf("cat exit_code = %d, want 0", catInfo.GetExitCode())
		}
		catOut, _, _, _ := e2eCollectOutput(t, ctx, cat, nil)
		if string(catOut) != "through the proxy\n" {
			t.Errorf("cat stdout = %q, want %q", catOut, "through the proxy\n")
		}

		sleeper, err := sb.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sleep", "300"},
		})
		if err != nil {
			t.Fatalf("StartProcess(sleep): %v", err)
		}
		if err := sleeper.Signal(ctx, ateenvv1alpha.Signal_SIGNAL_TERM); err != nil {
			t.Fatalf("sleeper.Signal(TERM): %v", err)
		}
		sleepInfo, err := sleeper.Wait(ctx)
		if err != nil {
			t.Fatalf("sleeper.Wait: %v", err)
		}
		if sleepInfo.GetExitCode() != 143 {
			t.Errorf("sleeper exit_code = %d, want 143", sleepInfo.GetExitCode())
		}

		// Lifecycle: suspend checkpoints the environment.
		if err := sb.Suspend(ctx); err != nil {
			t.Fatalf("Suspend: %v", err)
		}
		e2eWaitForStatus(t, ctx, sb, ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_SUSPENDED, 120*time.Second)

		if err := e2eDeleteEnv(ctx, sb); err != nil {
			t.Fatalf("Delete(%s): %v", envID, err)
		}
		deleted = true
		e2eWaitUntilDeleted(t, ctx, sb, 60*time.Second)
	})

	t.Run("create_from_image_rejects_unpinned_image", func(t *testing.T) {
		client := newAPIClient(t)
		_, err := client.Create(t.Context(), &ateenvv1alpha.CreateEnvironmentRequest{
			Id:          "goe2e-img-" + e2eRandomHex(t, 4),
			Atespace:    reqAtespace,
			RootfsImage: "python:3.12-slim",
		})
		if !errors.Is(err, env.ErrInvalidArgument) || status.Code(err) != codes.InvalidArgument {
			t.Errorf("Create unpinned image: err = %v (code %v), want ErrInvalidArgument / InvalidArgument", err, status.Code(err))
		}
	})

	t.Run("create_from_image_needs_a_base_template", func(t *testing.T) {
		client := newAPIClient(t)
		missing := "goe2e-nobase-" + e2eRandomHex(t, 4)
		_, err := client.Create(t.Context(), &ateenvv1alpha.CreateEnvironmentRequest{
			Id:          "goe2e-img-" + e2eRandomHex(t, 4),
			Atespace:    reqAtespace,
			Template:    &ateenvv1alpha.Template{Name: missing},
			RootfsImage: "docker.io/library/busybox@sha256:" + strings.Repeat("0", 64),
		})
		if !errors.Is(err, env.ErrFailedPrecondition) || status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("Create missing base: err = %v (code %v), want ErrFailedPrecondition / FailedPrecondition", err, status.Code(err))
		}
		if !strings.Contains(err.Error(), missing) {
			t.Errorf("error = %q, want substring %q", err.Error(), missing)
		}
	})

	t.Run("create_from_image", func(t *testing.T) {
		if rootfsImage == "" {
			t.Skip("set ATE_ENV_ROOTFS_IMAGE=repo@sha256:... (an image with sh and python3) for the create-from-image test")
		}
		_, digestHex, ok := strings.Cut(rootfsImage, "@sha256:")
		if !ok || len(digestHex) < 12 {
			t.Fatalf("ATE_ENV_ROOTFS_IMAGE = %q, want digest-pinned repo@sha256:<hex>", rootfsImage)
		}
		base := templateName
		if base == "" {
			base = "default-template"
		}
		wantTemplate := base + "-" + digestHex[:12]

		client := newAPIClient(t)
		ctx := t.Context()
		var envs []*env.Env
		deleted := false
		t.Cleanup(func() {
			if deleted {
				return
			}
			var cleanupErrs []string
			for _, e := range envs {
				delCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				err := e2eDeleteEnv(delCtx, e)
				cancel()
				if err != nil && !errors.Is(err, env.ErrNotFound) {
					cleanupErrs = append(cleanupErrs, fmt.Sprintf("%s: %v", e.ID(), err))
				}
			}
			if len(cleanupErrs) > 0 {
				t.Errorf("cleanup delete failed: %s", strings.Join(cleanupErrs, ", "))
			}
		})

		sb, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{
			Id:          "goe2e-img-" + e2eRandomHex(t, 4),
			Atespace:    reqAtespace,
			Template:    e2eOptionalTemplate(templateName),
			RootfsImage: rootfsImage,
		})
		if err != nil {
			t.Fatalf("Create from image: %v", err)
		}
		envs = append(envs, sb)

		// The derived template is reported at once, before the actor serves.
		info, err := sb.Info(ctx)
		if err != nil {
			t.Fatalf("Info: %v", err)
		}
		if info.GetTemplate() == nil {
			t.Fatal("Info.Template is nil")
		}
		if info.GetTemplate().GetName() != wantTemplate {
			t.Errorf("Template.Name = %q, want %q", info.GetTemplate().GetName(), wantTemplate)
		}
		if info.GetTemplate().GetAtespace() != sb.Atespace() {
			t.Errorf("Template.Atespace = %q, want %q", info.GetTemplate().GetAtespace(), sb.Atespace())
		}

		e2eWaitUntilServing(t, ctx, sb, readyTimeout)

		if expectedMarker != "" {
			res, err := sb.Shell(ctx, "echo $E2E_TEMPLATE_MARKER")
			if err != nil {
				t.Fatalf("Shell marker: %v", err)
			}
			if res.Stdout != expectedMarker+"\n" {
				t.Errorf("E2E_TEMPLATE_MARKER = %q, want %q", res.Stdout, expectedMarker+"\n")
			}
		}

		// The process runs in the rootfs image's rootfs, with the guest coming
		// from the image volume rather than from the image itself.
		res, err := sb.Shell(ctx,
			"test -x /ate/ko-app/ate-env-guest && echo guest:volume; "+
				"test -e /ko-app/ate-env-guest && echo guest:baked; "+
				"test -r /etc/os-release && echo rootfs:ok")
		if err != nil {
			t.Fatalf("Shell guest check: %v", err)
		}
		if res.ExitCode != 0 {
			t.Fatalf("Shell guest check exit_code = %d, stdout = %q, stderr = %q", res.ExitCode, res.Stdout, res.Stderr)
		}
		lines := strings.Split(strings.TrimSuffix(res.Stdout, "\n"), "\n")
		if !slices.Contains(lines, "guest:volume") {
			t.Errorf("lines = %v, want \"guest:volume\"", lines)
		}
		if slices.Contains(lines, "guest:baked") {
			t.Errorf("lines = %v: the rootfs image must not carry the guest", lines)
		}
		if !slices.Contains(lines, "rootfs:ok") {
			t.Errorf("lines = %v, want \"rootfs:ok\"", lines)
		}

		// The workspace is writable and the guest file path works unchanged.
		content := e2eRandomBytes(t, 64*1024+1)
		path := "/tmp/goe2e-img-" + e2eRandomHex(t, 4) + ".bin"
		if err := sb.WriteFile(ctx, path, bytes.NewReader(content), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if got := e2eReadFileBytes(t, ctx, sb, path); !bytes.Equal(got, content) {
			t.Errorf("ReadFile len = %d, want %d matching bytes", len(got), len(content))
		}

		// Port proxy: verify port 80 is rejected.
		portBase := fmt.Sprintf("/v1alpha/atespaces/%s/envs/%s/ports", sb.Atespace(), sb.ID())
		code, _, err := e2eHTTPGet(ctx, httpTarget, portBase+"/80/", authToken, "", 15*time.Second)
		if err != nil {
			t.Fatalf("port 80 GET: %v", err)
		}
		if code != http.StatusBadRequest {
			t.Errorf("port 80 status = %d, want 400", code)
		}

		// Start in-sandbox TCP server on port 8888.
		serverProc, err := sb.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"python3", "-u", "-c", e2ePortServerPy},
		})
		if err != nil {
			t.Fatalf("StartProcess(server): %v", err)
		}

		// Poll until the port proxy forwards to the server.
		pollDeadline := time.Now().Add(20 * time.Second)
		var body []byte
		code = 0
		for time.Now().Before(pollDeadline) {
			var getErr error
			code, body, getErr = e2eHTTPGet(ctx, httpTarget, portBase+"/8888/hello", authToken, "", 5*time.Second)
			if getErr == nil && code == http.StatusOK {
				break
			}
			if err := e2eSleep(ctx, 500*time.Millisecond); err != nil {
				t.Fatalf("waiting for port 8888: %v", err)
			}
		}
		if code != http.StatusOK || string(body) != "port-proxy-ok" {
			t.Fatalf("port 8888 /hello: status=%d body=%q, want 200 \"port-proxy-ok\"", code, body)
		}

		// WebSocket capability authentication and echo over hijacked stream.
		if authToken != "" {
			capToken := portproxy.Capability(authToken, sb.Atespace(), sb.ID(), 8888, time.Now().Add(time.Hour))
			capHeader := e2eCapabilityAuthHeader(capToken)

			plainCode, _, err := e2eHTTPGet(ctx, httpTarget, portBase+"/8888/hello", "", capHeader, 15*time.Second)
			if err != nil {
				t.Fatalf("plain GET with capability: %v", err)
			}
			if plainCode != http.StatusUnauthorized {
				t.Errorf("plain GET with capability: status = %d, want 401", plainCode)
			}

			wsCode, wsReader, wsConn, err := e2eWSConnect(ctx, httpTarget, portBase+"/8888/", capHeader)
			if err != nil {
				t.Fatalf("wsConnect valid capability: %v", err)
			}
			func() {
				defer wsConn.Close()
				if wsCode != http.StatusSwitchingProtocols {
					t.Fatalf("wsConnect valid capability: status = %d, want 101", wsCode)
				}
				const payload = "ping-ws\n"
				if err := wsConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
					t.Fatalf("SetDeadline: %v", err)
				}
				if _, err := io.WriteString(wsConn, payload); err != nil {
					t.Fatalf("ws write: %v", err)
				}
				echo := make([]byte, len(payload))
				if _, err := io.ReadFull(wsReader, echo); err != nil {
					t.Fatalf("ws read: %v", err)
				}
				if string(echo) != payload {
					t.Errorf("ws echo = %q, want %q", echo, payload)
				}
			}()

			flip := byte('0')
			if capToken[len(capToken)-1] == '0' {
				flip = '1'
			}
			badCap := capToken[:len(capToken)-1] + string(flip)
			badCode, _, badConn, err := e2eWSConnect(ctx, httpTarget, portBase+"/8888/", e2eCapabilityAuthHeader(badCap))
			if err != nil {
				t.Fatalf("wsConnect bad capability: %v", err)
			}
			_ = badConn.Close()
			if badCode != http.StatusUnauthorized {
				t.Errorf("wsConnect bad capability: status = %d, want 401", badCode)
			}
		}

		// Suspend and transparent wake-on-request through the port proxy.
		// Unlike gRPC guest calls, the port proxy does not retry when a cold
		// snapshot restore outlasts the router's parked-request hold and
		// returns 503 with Retry-After, so poll until readyTimeout.
		if err := sb.Suspend(ctx); err != nil {
			t.Fatalf("Suspend: %v", err)
		}
		e2eWaitForStatus(t, ctx, sb, ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_SUSPENDED, 120*time.Second)

		wakeCode, wakeBody := e2eWaitPortProxyWake(t, ctx, httpTarget, portBase+"/8888/hello", authToken, readyTimeout)
		if wakeCode != http.StatusOK || string(wakeBody) != "port-proxy-ok" {
			t.Fatalf("wake GET: status=%d body=%q, want 200 \"port-proxy-ok\"", wakeCode, wakeBody)
		}
		infoAfterWake, err := sb.Info(ctx)
		if err != nil {
			t.Fatalf("Info after wake: %v", err)
		}
		if infoAfterWake.GetStatus() != ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_RUNNING {
			t.Errorf("status after wake = %v, want RUNNING", infoAfterWake.GetStatus())
		}

		if err := serverProc.Signal(ctx, ateenvv1alpha.Signal_SIGNAL_TERM); err != nil && !errors.Is(err, env.ErrProcessExited) {
			t.Fatalf("serverProc.Signal(TERM): %v", err)
		}
		if _, err := serverProc.Wait(ctx); err != nil {
			t.Fatalf("serverProc.Wait: %v", err)
		}

		// A second environment on the same image reuses the derived template
		// instead of minting another one.
		sb2, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{
			Id:          "goe2e-img-" + e2eRandomHex(t, 4),
			Atespace:    reqAtespace,
			Template:    e2eOptionalTemplate(templateName),
			RootfsImage: rootfsImage,
		})
		if err != nil {
			t.Fatalf("Create second env: %v", err)
		}
		envs = append(envs, sb2)
		info2, err := sb2.Info(ctx)
		if err != nil {
			t.Fatalf("sb2.Info: %v", err)
		}
		if info2.GetTemplate().GetName() != wantTemplate {
			t.Errorf("sb2 Template.Name = %q, want %q", info2.GetTemplate().GetName(), wantTemplate)
		}
		e2eWaitUntilServing(t, ctx, sb2, readyTimeout)
		res2, err := sb2.Shell(ctx, "echo second")
		if err != nil {
			t.Fatalf("sb2.Shell: %v", err)
		}
		if res2.Stdout != "second\n" {
			t.Errorf("sb2.Shell Stdout = %q, want %q", res2.Stdout, "second\n")
		}

		var cleanupErrs []string
		for _, e := range envs {
			if err := e2eDeleteEnv(ctx, e); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Sprintf("%s: %v", e.ID(), err))
			}
		}
		deleted = true
		if len(cleanupErrs) > 0 {
			t.Fatalf("cleanup delete failed: %s", strings.Join(cleanupErrs, ", "))
		}
		for _, e := range envs {
			e2eWaitUntilDeleted(t, ctx, e, 60*time.Second)
		}
	})
}

type e2eChunkReader struct {
	chunks [][]byte
}

func (r *e2eChunkReader) Read(p []byte) (int, error) {
	for len(r.chunks) > 0 && len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[0])
	r.chunks[0] = r.chunks[0][n:]
	if len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

func e2eRandomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("crypto/rand.Read: %v", err)
	}
	return b
}

func e2eRandomHex(t *testing.T, nBytes int) string {
	t.Helper()
	return hex.EncodeToString(e2eRandomBytes(t, nBytes))
}

func e2eReadFileBytes(t *testing.T, ctx context.Context, sb *env.Env, path string) []byte {
	t.Helper()
	rc, err := sb.ReadFile(ctx, path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading %q: %v", path, err)
	}
	return got
}

func e2eCollectOutput(t *testing.T, ctx context.Context, proc *env.Process, req *ateenvv1alpha.StreamProcessOutputRequest) (stdout, stderr []byte, exit *ateenvv1alpha.Process, msgs []*ateenvv1alpha.ProcessOutput) {
	t.Helper()
	stream, err := proc.Output(ctx, req)
	if err != nil {
		t.Fatalf("Output: %v", err)
	}
	var outBuf, errBuf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Output.Recv: %v", err)
		}
		msgs = append(msgs, msg)
		switch o := msg.GetOutput().(type) {
		case *ateenvv1alpha.ProcessOutput_Stdout:
			outBuf.Write(o.Stdout)
		case *ateenvv1alpha.ProcessOutput_Stderr:
			errBuf.Write(o.Stderr)
		case *ateenvv1alpha.ProcessOutput_Exit:
			exit = o.Exit
		}
	}
	return outBuf.Bytes(), errBuf.Bytes(), exit, msgs
}

func e2eWaitForStdout(t *testing.T, ctx context.Context, proc *env.Process, sub string, timeout time.Duration) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := proc.Output(waitCtx, &ateenvv1alpha.StreamProcessOutputRequest{Follow: true})
	if err != nil {
		t.Fatalf("Output: %v", err)
	}
	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("waiting for %q on stdout: %v (got %q)", sub, err, buf.String())
		}
		if out := msg.GetStdout(); len(out) > 0 {
			buf.Write(out)
			if strings.Contains(buf.String(), sub) {
				return
			}
		}
	}
}

func e2eCleanupKill(t *testing.T, proc *env.Process) func() {
	t.Helper()
	var exited bool
	t.Cleanup(func() {
		if exited {
			return
		}
		kctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = proc.Kill(kctx)
	})
	return func() { exited = true }
}

func e2eDeleteEnv(ctx context.Context, sb *env.Env) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := sb.Delete(ctx)
		if err == nil {
			return nil
		}
		switch status.Code(err) {
		case codes.Aborted, codes.Unavailable, codes.ResourceExhausted:
			if time.Now().Before(deadline) {
				if serr := e2eSleep(ctx, min(time.Second, time.Until(deadline))); serr == nil {
					continue
				}
			}
		}
		return err
	}
}

func e2eWaitPortProxyWake(t *testing.T, ctx context.Context, hostPort, path, authToken string, timeout time.Duration) (int, []byte) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var (
		code    int
		body    []byte
		lastErr error
	)
	for time.Now().Before(deadline) {
		perAttempt := min(30*time.Second, time.Until(deadline))
		if perAttempt <= 0 {
			break
		}
		code, body, lastErr = e2eHTTPGet(ctx, hostPort, path, authToken, "", perAttempt)
		if lastErr == nil {
			switch code {
			case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
				// Transient router / wake hold response while the actor resumes.
			default:
				return code, body
			}
		}
		if err := e2eSleep(ctx, min(time.Second, time.Until(deadline))); err != nil {
			t.Fatalf("waiting for port proxy wake (%v): last status=%d err=%v", err, code, lastErr)
		}
	}
	if lastErr != nil {
		t.Fatalf("port proxy wake timed out after %v: %v", timeout, lastErr)
	}
	return code, body
}

func e2eOptionalTemplate(name string) *ateenvv1alpha.Template {
	if name == "" {
		return nil
	}
	return &ateenvv1alpha.Template{Name: name}
}

func e2eParseReadyTimeout(t *testing.T) time.Duration {
	t.Helper()
	const (
		defaultTimeout = 180 * time.Second
		maxTimeoutSec  = 24 * 60 * 60
	)
	raw := strings.TrimSpace(os.Getenv("ATE_ENV_READY_TIMEOUT"))
	if raw == "" {
		return defaultTimeout
	}
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		if math.IsNaN(secs) || math.IsInf(secs, 0) || secs <= 0 || secs > maxTimeoutSec {
			t.Fatalf("ATE_ENV_READY_TIMEOUT must be in (0, %d] seconds, got %q", maxTimeoutSec, raw)
		}
		return time.Duration(secs * float64(time.Second))
	}
	if d, err := time.ParseDuration(raw); err == nil {
		if d <= 0 || d > maxTimeoutSec*time.Second {
			t.Fatalf("ATE_ENV_READY_TIMEOUT must be in (0, 24h], got %q", raw)
		}
		return d
	}
	t.Fatalf("invalid ATE_ENV_READY_TIMEOUT %q: want positive seconds or Go duration", raw)
	return 0
}

func e2eNormalizeHTTPTarget(t *testing.T, raw string) string {
	t.Helper()
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
	if strings.HasPrefix(trimmed, "https://") {
		t.Fatalf("ATE_ENV_API_TARGET %q uses https; TestE2EFullStack expects plaintext host:port or http://host:port", raw)
	}
	if strings.HasPrefix(trimmed, "http://") {
		u, err := url.Parse(trimmed)
		if err != nil || u.Host == "" {
			t.Fatalf("invalid ATE_ENV_API_TARGET %q: %v", raw, err)
		}
		return u.Host
	}
	return trimmed
}

func e2eSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func e2eWaitUntilServing(t *testing.T, ctx context.Context, sb *env.Env, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		callCtx, cancel := context.WithDeadline(ctx, deadline)
		res, err := sb.Shell(callCtx, "echo ready")
		cancel()
		if err == nil && res.ExitCode == 0 && res.Stdout == "ready\n" {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("exit=%d stdout=%q stderr=%q", res.ExitCode, res.Stdout, res.Stderr)
		}
		if err := e2eSleep(ctx, min(2*time.Second, time.Until(deadline))); err != nil {
			t.Fatalf("environment %s not reachable before context ended (%v): last error: %v", sb.ID(), err, lastErr)
		}
	}
	t.Fatalf("environment %s never became reachable within %v: %v", sb.ID(), timeout, lastErr)
}

func e2eWaitForStatus(t *testing.T, ctx context.Context, sb *env.Env, want ateenvv1alpha.EnvironmentStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last ateenvv1alpha.EnvironmentStatus
	for time.Now().Before(deadline) {
		info, err := sb.Info(ctx)
		if err != nil {
			t.Fatalf("Info(%s): %v", sb.ID(), err)
		}
		last = info.GetStatus()
		if last == want {
			return
		}
		if err := e2eSleep(ctx, min(2*time.Second, time.Until(deadline))); err != nil {
			t.Fatalf("waiting for %s status %v: %v (last %v)", sb.ID(), want, err, last)
		}
	}
	t.Fatalf("environment %s stuck in %v, wanted %v", sb.ID(), last, want)
}

func e2eWaitUntilDeleted(t *testing.T, ctx context.Context, sb *env.Env, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, err := sb.Info(ctx)
		if errors.Is(err, env.ErrNotFound) {
			return
		}
		if err != nil {
			t.Fatalf("Info(%s) after delete: unexpected error %v", sb.ID(), err)
		}
		if err := e2eSleep(ctx, min(2*time.Second, time.Until(deadline))); err != nil {
			t.Fatalf("waiting for %s deletion: %v", sb.ID(), err)
		}
	}
	t.Fatalf("environment %s still exists after delete", sb.ID())
}

func e2eCapabilityAuthHeader(capability string) string {
	raw := portproxy.CapabilityUser + ":" + capability
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(raw))
}

func e2eHTTPGet(ctx context.Context, hostPort, path, authToken, authHeader string, timeout time.Duration) (int, []byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+hostPort+path, nil)
	if err != nil {
		return 0, nil, err
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	} else if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	hc := &http.Client{
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func e2eWSConnect(ctx context.Context, hostPort, path, authHeader string) (int, *bufio.Reader, net.Conn, error) {
	dialAddr := hostPort
	if _, _, err := net.SplitHostPort(dialAddr); err != nil {
		dialAddr = net.JoinHostPort(strings.Trim(dialAddr, "[]"), "80")
	}
	var d net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := d.DialContext(dialCtx, "tcp", dialAddr)
	if err != nil {
		return 0, nil, nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = conn.Close()
		return 0, nil, nil, err
	}
	var rawKey [16]byte
	if _, err := rand.Read(rawKey[:]); err != nil {
		_ = conn.Close()
		return 0, nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(rawKey[:])
	var hdr strings.Builder
	fmt.Fprintf(&hdr, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&hdr, "Host: %s\r\n", hostPort)
	hdr.WriteString("Upgrade: websocket\r\n")
	hdr.WriteString("Connection: Upgrade\r\n")
	fmt.Fprintf(&hdr, "Sec-WebSocket-Key: %s\r\n", key)
	hdr.WriteString("Sec-WebSocket-Version: 13\r\n")
	if authHeader != "" {
		fmt.Fprintf(&hdr, "Authorization: %s\r\n", authHeader)
	}
	hdr.WriteString("\r\n")
	if _, err := io.WriteString(conn, hdr.String()); err != nil {
		_ = conn.Close()
		return 0, nil, nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = conn.Close()
		return 0, nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = resp.Body.Close()
	}
	return resp.StatusCode, br, conn, nil
}

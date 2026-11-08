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

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/sidecarenv"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// runMainEnv makes the test binary run the guest's main instead of the tests,
// thus a test can start the real guest as a child process with an
// environment of its own.
const runMainEnv = "ATE_ENV_GUEST_TEST_RUN_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(runMainEnv) == "1" {
		os.Unsetenv(runMainEnv)
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

var listeningOn = regexp.MustCompile(`ate-env-guest listening on (\S+)`)

// The guest gives a sidecar-only variable to its sidecars, and not to the
// commands it starts for the process service or to their children, with or
// without request env. A plain container variable reaches both, which shows
// that the commands do inherit the guest's environment.
func TestSidecarEnvReachesOnlySidecars(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh on this host")
	}
	const secret = "s3cret-sidecar-only"
	dir := t.TempDir()
	sidecarOut := filepath.Join(dir, "sidecar.env")
	script := filepath.Join(dir, "sidecar.sh")
	if err := os.WriteFile(script, []byte("env > "+sidecarOut+".tmp && mv "+sidecarOut+".tmp "+sidecarOut+"\nexec sleep 60\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-listen=127.0.0.1:0", "-log-dir="+dir, "-workspace="+dir, "-sidecar=/bin/sh "+script, "-"+sidecarenv.RequiredFlag)
	cmd.Env = append(os.Environ(),
		runMainEnv+"=1",
		"PLAIN_VAR=visible",
		sidecarenv.Prefix+"OH_SESSION_API_KEYS_0="+secret,
	)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	addrCh := make(chan string, 1)
	var logMu sync.Mutex
	var guestLog strings.Builder
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			logMu.Lock()
			guestLog.WriteString(sc.Text() + "\n")
			logMu.Unlock()
			if m := listeningOn.FindStringSubmatch(sc.Text()); m != nil {
				addrCh <- m[1]
			}
		}
	}()
	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("the guest did not report its listen address")
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := ateenvv1alpha.NewProcessServiceClient(conn)

	type runCase struct {
		name string
		env  map[string]string
		tty  *ateenvv1alpha.TTY
	}
	cases := []runCase{{"no request env", nil, nil}, {"request env", map[string]string{"REQ_VAR": "1"}, nil}}
	// The PTY path builds the command's environment the same way; the guest
	// allocates a PTY on Linux only.
	if _, err := os.Stat("/dev/ptmx"); err == nil && runtime.GOOS == "linux" {
		cases = append(cases, runCase{"tty", nil, &ateenvv1alpha.TTY{Rows: 24, Cols: 80}})
	}
	for _, rc := range cases {
		out := runInGuest(t, client, rc.env, rc.tty, "echo SELF; env; echo CHILD; /bin/sh -c env")
		if strings.Contains(out, secret) || strings.Contains(out, "OH_SESSION_API_KEYS_0") || strings.Contains(out, sidecarenv.Prefix) {
			t.Errorf("%s: an exec'd command or its child sees the sidecar-only variable:\n%s", rc.name, out)
		}
		if strings.Count(out, "PLAIN_VAR=visible") != 2 {
			t.Errorf("%s: want PLAIN_VAR in the command's and its child's environment:\n%s", rc.name, out)
		}
	}

	var sidecar []byte
	deadline := time.Now().Add(10 * time.Second)
	for {
		if sidecar, err = os.ReadFile(sidecarOut); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("the sidecar did not write its environment: %v", err)
	}
	lines := strings.Split(string(sidecar), "\n")
	has := func(kv string) bool {
		for _, l := range lines {
			if l == kv {
				return true
			}
		}
		return false
	}
	if !has("OH_SESSION_API_KEYS_0=" + secret) {
		t.Errorf("the sidecar's environment lacks OH_SESSION_API_KEYS_0:\n%s", sidecar)
	}
	if !has("PLAIN_VAR=visible") {
		t.Errorf("the sidecar's environment lacks the guest's plain variable:\n%s", sidecar)
	}
	if strings.Contains(string(sidecar), sidecarenv.Prefix) {
		t.Errorf("the sidecar's environment has the prefixed variable:\n%s", sidecar)
	}

	// The guest logs the names it took, before the listening line, and never
	// a value.
	logMu.Lock()
	logged := guestLog.String()
	logMu.Unlock()
	if !strings.Contains(logged, "for the sidecars only: [OH_SESSION_API_KEYS_0]") {
		t.Errorf("the guest log does not name the sidecar-only variable it took:\n%s", logged)
	}
	if strings.Contains(logged, secret) {
		t.Errorf("the guest log shows the sidecar-only value:\n%s", logged)
	}

	// What remains, as docs/security/threat-model.md says: the environment
	// that a process was started with stays in /proc/<pid>/environ. The
	// guest removes the variable from the environment it gives to commands,
	// not from that memory, and a sidecar gets it through its environment.
	if runtime.GOOS == "linux" {
		environ, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", cmd.Process.Pid))
		if err != nil {
			t.Fatalf("reading the guest's /proc environ: %v", err)
		}
		if !strings.Contains(string(environ), sidecarenv.Prefix+"OH_SESSION_API_KEYS_0="+secret) {
			t.Errorf("the guest's /proc/<pid>/environ no longer has the variable; update the threat model")
		}
	}
}

// runInGuest runs script with /bin/sh through the guest's process service and
// returns its stdout.
func runInGuest(t *testing.T, client ateenvv1alpha.ProcessServiceClient, env map[string]string, tty *ateenvv1alpha.TTY, script string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := client.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"/bin/sh", "-c", script}, Env: env, Tty: tty})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	stream, err := client.StreamProcessOutput(ctx, &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: p.GetProcessId(), Follow: true})
	if err != nil {
		t.Fatalf("StreamProcessOutput: %v", err)
	}
	var out strings.Builder
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("StreamProcessOutput: %v", err)
		}
		out.Write(msg.GetStdout())
		if e := msg.GetExit(); e != nil {
			if e.GetExitCode() != 0 {
				t.Fatalf("command exited %d:\n%s", e.GetExitCode(), out.String())
			}
			break
		}
	}
	return out.String()
}

// With -sidecar-env-required, which `ate-env manifest template --sidecar-env`
// renders, the guest exits before it listens or starts a sidecar when the
// container has no sidecar-only variable. A guest older than the flag exits
// at flag parse ("flag provided but not defined"), thus a template with
// --sidecar-env never runs its sidecars without their values.
func TestSidecarEnvRequiredWithoutVariablesExits(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("no /bin/sh on this host")
	}
	dir := t.TempDir()
	started := filepath.Join(dir, "started")
	script := filepath.Join(dir, "sidecar.sh")
	if err := os.WriteFile(script, []byte("touch "+started+"\nexec sleep 60\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-listen=127.0.0.1:0", "-log-dir="+dir, "-workspace="+dir, "-sidecar=/bin/sh "+script, "-"+sidecarenv.RequiredFlag)
	cmd.Env = []string{runMainEnv + "=1", "PATH=" + os.Getenv("PATH"), "PLAIN_VAR=visible"}
	done := make(chan struct{})
	var out []byte
	var err error
	go func() { out, err = cmd.CombinedOutput(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the guest did not exit")
	}
	if err == nil {
		t.Fatalf("the guest exited 0, want an error:\n%s", out)
	}
	if !strings.Contains(string(out), "-"+sidecarenv.RequiredFlag+" is set") {
		t.Errorf("the guest's output does not name the flag:\n%s", out)
	}
	if strings.Contains(string(out), "listening on") {
		t.Errorf("the guest listened before it exited:\n%s", out)
	}
	if _, err := os.Stat(started); err == nil {
		t.Error("the sidecar started")
	}
}

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
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/env/internal/apiservice"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func TestCommandRouting(t *testing.T) {
	t.Run("root create command", func(t *testing.T) {
		args := []string{"create", "dev1"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "create" {
			t.Errorf("expected command 'create', got %q", cmd.Name())
		}
		if len(remaining) != 1 || remaining[0] != "dev1" {
			t.Errorf("expected remaining args [dev1], got %v", remaining)
		}
	})

	t.Run("root manifest command", func(t *testing.T) {
		args := []string{"manifest"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "manifest" {
			t.Errorf("expected command 'manifest', got %q", cmd.Name())
		}
		if len(remaining) != 0 {
			t.Errorf("expected 0 remaining args, got %v", remaining)
		}
	})

	t.Run("top-level suspend command", func(t *testing.T) {
		args := []string{"suspend", "dev1"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "suspend" {
			t.Errorf("expected command 'suspend', got %q", cmd.Name())
		}
		if len(remaining) != 1 || remaining[0] != "dev1" {
			t.Errorf("expected 1 remaining arg [dev1], got %v", remaining)
		}
	})

	t.Run("suspend --mode", func(t *testing.T) {
		args := []string{"suspend", "dev1", "--mode", "local"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if err := cmd.ParseFlags(remaining); err != nil {
			t.Fatalf("cmd.ParseFlags failed: %v", err)
		}
		if got := cmd.Flags().Lookup("mode").Value.String(); got != "local" {
			t.Errorf("--mode = %q, want local", got)
		}
		if got := newSuspendCommand().Flags().Lookup("mode").DefValue; got != "durable" {
			t.Errorf("--mode default = %q, want durable", got)
		}
		// A mode that is neither is refused before anything is dialed.
		bad := newRootCommand(nil)
		bad.SetArgs([]string{"suspend", "dev1", "--mode", "async"})
		bad.SetOut(io.Discard)
		bad.SetErr(io.Discard)
		if err := bad.Execute(); err == nil || !strings.Contains(err.Error(), "--mode must be durable or local") {
			t.Errorf("invalid --mode: got %v, want the usage error", err)
		}
	})

	t.Run("top-level delete command", func(t *testing.T) {
		args := []string{"delete", "dev1"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "delete" {
			t.Errorf("expected command 'delete', got %q", cmd.Name())
		}
		if len(remaining) != 1 || remaining[0] != "dev1" {
			t.Errorf("expected 1 remaining arg [dev1], got %v", remaining)
		}
	})

	t.Run("env read routes to env subcommand", func(t *testing.T) {
		args := []string{"dev1", "read", "/note.txt"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "read" {
			t.Errorf("expected cmd 'read', got %q", cmd.Name())
		}
		if len(remaining) != 1 || remaining[0] != "/note.txt" {
			t.Errorf("expected remaining args [/note.txt], got %v", remaining)
		}
	})

	t.Run("env write routes to env subcommand", func(t *testing.T) {
		args := []string{"dev1", "write", "/note.txt"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "write" {
			t.Errorf("expected cmd 'write', got %q", cmd.Name())
		}
		if len(remaining) != 1 || remaining[0] != "/note.txt" {
			t.Errorf("expected remaining args [/note.txt], got %v", remaining)
		}
	})

	t.Run("env shell routes to env subcommand", func(t *testing.T) {
		args := []string{"dev1", "shell", "uname", "-a"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "shell" {
			t.Errorf("expected cmd 'shell', got %q", cmd.Name())
		}
		if len(remaining) != 2 || remaining[0] != "uname" || remaining[1] != "-a" {
			t.Errorf("expected remaining args [uname -a], got %v", remaining)
		}
	})

	t.Run("env cmd alias routes to shell", func(t *testing.T) {
		args := []string{"dev1", "cmd", "uname", "-a"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "shell" {
			t.Errorf("expected cmd 'shell' for alias 'cmd', got %q", cmd.Name())
		}
		if len(remaining) != 2 {
			t.Errorf("expected 2 remaining args, got %v", remaining)
		}
	})

	t.Run("env with flags parses flags and routes to subcommand", func(t *testing.T) {
		args := []string{"dev1", "--api", "127.0.0.1:7777", "read", "/note.txt"}
		root := newRootCommand(args)
		cmd, remaining, err := root.Find(args)
		if err != nil {
			t.Fatalf("root.Find failed: %v", err)
		}
		if cmd.Name() != "read" {
			t.Errorf("expected cmd 'read', got %q", cmd.Name())
		}
		if err := cmd.ParseFlags(remaining); err != nil {
			t.Fatalf("cmd.ParseFlags failed: %v", err)
		}
		parsedArgs := cmd.Flags().Args()
		if len(parsedArgs) != 1 || parsedArgs[0] != "/note.txt" {
			t.Errorf("expected parsed args [/note.txt], got %v", parsedArgs)
		}
	})
}

func TestHelpOutput(t *testing.T) {
	t.Run("root help", func(t *testing.T) {
		root := newRootCommand(nil)

		var buf bytes.Buffer
		root.SetOut(&buf)
		root.SetArgs([]string{"--help"})
		if err := root.Execute(); err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		out := buf.String()
		for _, want := range []string{"create", "manifest", "suspend", "delete", "server-info", "Common environment commands", "ate-env <id> read <path>"} {
			if !bytes.Contains(buf.Bytes(), []byte(want)) {
				t.Errorf("help output missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("env help", func(t *testing.T) {
		args := []string{"dev1", "--help"}
		root := newRootCommand(args)

		var buf bytes.Buffer
		root.SetOut(&buf)
		root.SetArgs(args)
		if err := root.Execute(); err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		out := buf.String()
		for _, want := range []string{"read", "write", "shell"} {
			if !bytes.Contains(buf.Bytes(), []byte(want)) {
				t.Errorf("env help output missing %q:\n%s", want, out)
			}
		}
		for _, dontWant := range []string{"suspend", "delete"} {
			if strings.Contains(out, "Available Commands:\n") && strings.Contains(out, dontWant) {
				t.Errorf("env help should not list %q:\n%s", dontWant, out)
			}
		}
	})
}

// --help of each command with the token flags names the variable but never
// prints its value: help output lands in terminals, CI logs and screenshots.
func TestHelpDoesNotPrintAuthToken(t *testing.T) {
	const secret = "s3cr3t-token-value"
	t.Setenv("SUBSTRATE_ENV_AUTH_TOKEN", secret)
	for _, args := range [][]string{
		{"dev1", "--help"},
		{"dev1", "read", "--help"},
		{"create", "--help"},
		{"suspend", "--help"},
		{"delete", "--help"},
		{"server-info", "--help"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			root := newRootCommand(args)
			var buf bytes.Buffer
			root.SetOut(&buf)
			root.SetArgs(args)
			if err := root.Execute(); err != nil {
				t.Fatalf("Execute failed: %v", err)
			}
			out := buf.String()
			if strings.Contains(out, secret) {
				t.Errorf("help output prints the token:\n%s", out)
			}
			if !strings.Contains(out, "--auth-token") || !strings.Contains(out, "$SUBSTRATE_ENV_AUTH_TOKEN") {
				t.Errorf("help output must list --auth-token and name $SUBSTRATE_ENV_AUTH_TOKEN:\n%s", out)
			}
		})
	}
}

func TestCreateImageFlag(t *testing.T) {
	args := []string{"create", "dev1", "--rootfs-image", "example.com/task@sha256:abc"}
	root := newRootCommand(args)
	cmd, _, err := root.Find(args[:2])
	if err != nil {
		t.Fatalf("root.Find failed: %v", err)
	}
	if cmd.Flags().Lookup("rootfs-image") == nil {
		t.Fatal("create has no --rootfs-image flag")
	}
	if err := cmd.ParseFlags(args[2:]); err != nil {
		t.Fatalf("parsing --rootfs-image: %v", err)
	}
	if got, _ := cmd.Flags().GetString("rootfs-image"); got != "example.com/task@sha256:abc" {
		t.Errorf("--rootfs-image = %q", got)
	}
}

func TestManifestTemplateRootfsImageFlags(t *testing.T) {
	args := []string{"manifest", "template"}
	root := newRootCommand(args)
	cmd, _, err := root.Find(args)
	if err != nil {
		t.Fatalf("root.Find failed: %v", err)
	}
	for _, name := range []string{"rootfs-image", "workspace", "guest-image", "snapshots-bucket"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("manifest template has no --%s flag", name)
		}
	}
}

func TestAuthFlagsResolve(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte("  from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, env string
		args      []string
		want      string
		wantErr   string
	}{
		{name: "token flag", args: []string{"--auth-token", "flag-tok"}, want: "flag-tok"},
		{name: "environment variable", env: "env-tok", want: "env-tok"},
		{name: "token flag over environment variable", env: "env-tok", args: []string{"--auth-token", "flag-tok"}, want: "flag-tok"},
		{name: "file", args: []string{"--auth-token-file", file}, want: "from-file"},
		{name: "file over environment variable", env: "env-tok", args: []string{"--auth-token-file", file}, want: "from-file"},
		{name: "both flags", args: []string{"--auth-token", "x", "--auth-token-file", file}, wantErr: "cannot be used together"},
		{name: "empty file", args: []string{"--auth-token-file", empty}, wantErr: "is empty"},
		{name: "missing file", args: []string{"--auth-token-file", file + ".missing"}, wantErr: "reading --auth-token-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var a authFlags
			cmd := &cobra.Command{Use: "x"}
			a.register(cmd, false)
			// Set after the flags are registered: the variable is read when
			// the command runs, not when it is built.
			t.Setenv("SUBSTRATE_ENV_AUTH_TOKEN", tc.env)
			if err := cmd.ParseFlags(tc.args); err != nil {
				t.Fatal(err)
			}
			got, err := a.resolve(cmd)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("resolve = %q, %v; want error containing %q", got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("resolve = %q, %v; want %q", got, err, tc.want)
			}
		})
	}

	// The guest commands inherit the flags from their parent; the check
	// still sees both and stops before any connection.
	args := []string{"dev1", "read", "f", "--auth-token", "x", "--auth-token-file", file}
	root := newRootCommand(args)
	root.SetArgs(args)
	root.SetOut(&bytes.Buffer{})
	if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "cannot be used together") {
		t.Fatalf("guest command with both flags: %v, want the exclusion error", err)
	}
}

func TestServerInfoCommand(t *testing.T) {
	svc := apiservice.New(nil, "", "")
	svc.Info = apiservice.ServerInfo{Version: "v9.9.9", PortProxy: true}
	grpcServer := grpc.NewServer()
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, svc)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	args := []string{"server-info", "--api", lis.Addr().String()}
	root := newRootCommand(args)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		t.Fatalf("server-info: %v", err)
	}
	for _, want := range []string{
		"server: sandboxenv-rl\n",
		"version: v9.9.9\n",
		"substrate_version: ",
		"features: rootfs_image, pty, suspend_mode, image_status, placement_domains, warm_image, port_proxy\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("server-info output missing %q:\n%s", want, out.String())
		}
	}
}

func TestServerInfoCommandJSON(t *testing.T) {
	svc := apiservice.New(nil, "", "")
	svc.Info = apiservice.ServerInfo{Version: "v9.9.9", PortProxy: true}
	grpcServer := grpc.NewServer()
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, svc)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	args := []string{"server-info", "--json", "--api", lis.Addr().String()}
	root := newRootCommand(args)
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		t.Fatalf("server-info --json: %v", err)
	}
	var got struct {
		Server           string   `json:"server"`
		Version          string   `json:"version"`
		SubstrateVersion string   `json:"substrate_version"`
		Features         []string `json:"features"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("output is not one JSON object: %v\n%s", err, out.String())
	}
	if got.Server != "sandboxenv-rl" || got.Version != "v9.9.9" || got.SubstrateVersion == "" ||
		strings.Join(got.Features, ",") != "rootfs_image,pty,suspend_mode,image_status,placement_domains,warm_image,port_proxy" {
		t.Errorf("server-info --json = %+v", got)
	}
}

// An environment named like a subcommand cannot be reached with
// `ate-env <id> ...`; create refuses the id before it calls the API.
func TestCreateRejectsSubcommandID(t *testing.T) {
	for _, id := range []string{"create", "get", "image-status", "placement-domains", "warm-image", "delete", "suspend", "manifest", "server-info", "help", "completion"} {
		args := []string{"create", id, "--api", "127.0.0.1:1"}
		root := newRootCommand(args)
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(args)
		err := root.Execute()
		if err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Errorf("create %s: err = %v, want a reserved-id error", id, err)
		}
	}
}

func TestCreateWorkerSelectorFlag(t *testing.T) {
	args := []string{"create", "dev1", "--worker-selector", "ate-env/domain=d0,tier=hot", "--worker-selector", "zone=us-central1-a"}
	root := newRootCommand(args)
	cmd, _, err := root.Find(args[:2])
	if err != nil {
		t.Fatalf("root.Find failed: %v", err)
	}
	if cmd.Flags().Lookup("worker-selector") == nil {
		t.Fatal("create has no --worker-selector flag")
	}
	if err := cmd.ParseFlags(args[2:]); err != nil {
		t.Fatalf("parsing --worker-selector: %v", err)
	}
	items, _ := cmd.Flags().GetStringSlice("worker-selector")
	got, err := parseWorkerSelector(items)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"ate-env/domain": "d0", "tier": "hot", "zone": "us-central1-a"}
	if !maps.Equal(got, want) {
		t.Errorf("parseWorkerSelector(%q) = %v, want %v", items, got, want)
	}
}

func TestParseWorkerSelector(t *testing.T) {
	if got, err := parseWorkerSelector(nil); err != nil || got != nil {
		t.Errorf("parseWorkerSelector(nil) = %v, %v; want nil, nil", got, err)
	}
	if got, err := parseWorkerSelector([]string{"k="}); err != nil || got["k"] != "" {
		t.Errorf("an empty value is a value: got %v, %v", got, err)
	}
	if _, err := parseWorkerSelector([]string{"zone=a", "zone=b"}); err == nil || !strings.Contains(err.Error(), "given twice") {
		t.Errorf("a repeated key must be an error, got %v", err)
	}
	if _, err := parseWorkerSelector([]string{"zone=a", "zone=a"}); err == nil {
		t.Error("a repeated key with the same value must be an error too")
	}
	for _, bad := range []string{"novalue", "=v", ""} {
		if _, err := parseWorkerSelector([]string{bad}); err == nil {
			t.Errorf("parseWorkerSelector(%q) accepted, want an error", bad)
		}
	}
}

func TestImageStatusCommandFlags(t *testing.T) {
	args := []string{"image-status", "example.com/task@sha256:abc", "--ensure", "--template", "layered", "--json"}
	root := newRootCommand(args)
	cmd, _, err := root.Find(args[:1])
	if err != nil {
		t.Fatalf("root.Find failed: %v", err)
	}
	if err := cmd.ParseFlags(args[2:]); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}
	if got, _ := cmd.Flags().GetBool("ensure"); !got {
		t.Error("--ensure not set")
	}
	if got, _ := cmd.Flags().GetString("template"); got != "layered" {
		t.Errorf("--template = %q", got)
	}
	if got, _ := cmd.Flags().GetBool("json"); !got {
		t.Error("--json not set")
	}
}

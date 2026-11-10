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

	envclient "github.com/agent-substrate/env/clients/go"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc/status"
)

// ExampleNewClient runs under go test: NewClient does not dial until the
// first call, so it needs no server. The other examples need a reachable
// ate-env-api and are compiled only.
func ExampleNewClient() {
	client, err := envclient.NewClient(envclient.ClientOptions{
		Endpoint: "localhost:7777",
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	fmt.Println(client != nil)
	// Output: true
}

func ExampleClient_Create() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}
	fmt.Println(env.ID())
}

func ExampleClient_ImageStatus() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	status, err := client.ImageStatus(ctx, &ateenvv1alpha.GetImageStatusRequest{RootfsImage: "ubuntu:22.04"})
	if err != nil {
		panic(err)
	}
	fmt.Println(status.GetGolden())
}

func ExampleEnv_Shell() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}

	resp, err := env.Shell(ctx, "ls -l")
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Stdout)
}

func ExampleEnv_Run() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}

	resp, err := env.Run(ctx, envclient.ShellRequest{
		Command: "ls -l",
		Poll:    true,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Duration, resp.TimedOut)
}

func ExampleEnv_StartProcess() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}

	proc, err := env.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sleep", "60"},
	})
	if err != nil {
		panic(err)
	}

	p, err := proc.Wait(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(p.GetExitCode())
}

func ExampleProcess_Resize() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}

	// Tty runs the process on a PTY of the given initial size.
	proc, err := env.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"bash"},
		Stdin:   true,
		Tty:     &ateenvv1alpha.TTY{Rows: 24, Cols: 80},
	})
	if err != nil {
		panic(err)
	}
	if err := proc.Resize(ctx, 40, 120); err != nil {
		panic(err)
	}
}

func ExampleProcess_Stdin() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}

	proc, err := env.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"cat"},
		Stdin:   true,
	})
	if err != nil {
		panic(err)
	}

	w, err := proc.Stdin(ctx)
	if err != nil {
		panic(err)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
}

func ExampleEnv_ReadFile() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}

	rc, err := env.ReadFile(ctx, "/etc/os-release")
	if err != nil {
		panic(err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}

func ExampleClient_Suspend() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	if err := client.Suspend(ctx, envclient.DefaultAtespace, "dev1"); err != nil {
		panic(err)
	}
}

func ExampleClient_WarmImage() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	_, err = client.WarmImage(ctx, &ateenvv1alpha.WarmImageRequest{
		RootfsImage: "ubuntu:22.04",
	}, func(probe *ateenvv1alpha.WarmProbe) {
		fmt.Println(probe.GetNode())
	})
	if err != nil {
		panic(err)
	}
}

func ExampleStartError() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
	if err != nil {
		panic(err)
	}

	_, err = env.Shell(ctx, "ls")
	var startErr *envclient.StartError
	if errors.As(err, &startErr) {
		fmt.Println("Start failed:", startErr.Reason)
	} else if envclient.Retryable(err) {
		fmt.Println("Retryable error")
	}
}

func ExampleClient_Get() {
	ctx := context.Background()
	client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "localhost:7777"})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	_, err = client.Get(ctx, envclient.DefaultAtespace, "missing")
	if errors.Is(err, envclient.ErrNotFound) {
		// The gRPC status stays in the chain: status.Code(err) is codes.NotFound.
		fmt.Println("no such environment:", status.Code(err))
	}
}

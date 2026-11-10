# Go client: package `env` (`github.com/agent-substrate/env/clients/go`)

The Go client library for the Agent Substrate Environment API provides a programmatic interface to
lifecycle and guest operations.

## Install

The module path is a separate decision (README D8 of the Go client parity plan). Until it is made,
`github.com/agent-substrate/env` on the public proxy resolves to the previous repository, so point
the module at this repository with a `replace` directive, then resolve it (Go 1.27 or later):

```bash
go mod edit -replace github.com/agent-substrate/env=github.com/ai-on-gke/sandboxenv-rl@main
go mod tidy
```

`go mod tidy` pins `@main` to a pseudo-version. To build against a local clone instead, use
`go mod edit -replace github.com/agent-substrate/env=/path/to/sandboxenv-rl`.

## Connect

The snippets below use these two imports:

```go
import (
	envclient "github.com/agent-substrate/env/clients/go"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
)
```

A single `Client` multiplexes concurrent operations. Programs create one client, share it, and close
it on shutdown. You provide `Endpoint` in `ClientOptions`. Other fields include `AuthToken`,
`GRPCConn`, `Connections`, `DialOptions`, `TracerProvider`, `TraceServiceName`, and
`MetricsRegisterer`. `Connections` dials that many connections to `Endpoint` and hands them out
round robin; a Kubernetes Service balances per connection, so use at least as many as `ate-env-api`
has replicas when many environments are in flight. `DialOptions` are added to every connection the
client dials.

```go
client, err := envclient.NewClient(envclient.ClientOptions{
	Endpoint: "localhost:7777",
})
if err != nil {
	panic(err)
}
defer client.Close()
```

## Create an environment

The `Client` lifecycle methods operate on environments. You can `Create` an environment and get an
`Env` handle. The `Env` struct exposes `ID`, `Info`, and `Atespace` methods. Other lifecycle
operations include `Get`, `ServerInfo`, `EnsureImageTemplate`, and `ListPlacementDomains`.

```go
env, err := client.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "dev1"})
if err != nil {
	panic(err)
}
fmt.Println(env.ID())
```

## Ask about an image before creating from it

`ImageStatus` reports the status of an image.

```go
status, err := client.ImageStatus(ctx, &ateenvv1alpha.GetImageStatusRequest{RootfsImage: "ubuntu:22.04"})
if err != nil {
	panic(err)
}
fmt.Println(status.GetGolden())
```

## Run shell commands

The `Shell` method executes a shell command. The `Run` method accepts a `ShellRequest` with options
like `Poll`. The response includes `Duration` and `TimedOut`.

```go
resp, err := env.Shell(ctx, "ls -l")
if err != nil {
	panic(err)
}
fmt.Println(resp.Stdout)
```

## Run background processes and stream output

You can start a background process with `StartProcess`. `Env.Process` and `Env.FindProcess` retrieve
an existing process. The `Process` handle exposes `Wait`, `Info`, `Kill`, `Signal`, `Resize`,
`Output`, and `Stdin`. For a PTY, set `Tty` in the `StartProcessRequest` (an
`&ateenvv1alpha.TTY{Rows: 24, Cols: 80}` gives the initial size), then call `Resize` when the
terminal changes size.

```go
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
```

## Feed stdin and send signals

The `Stdin` method provides an `io.WriteCloser` to stream input. `Signal` sends a signal to the
process.

```go
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
```

## Read and write files

The client streams file contents directly. Methods include `ReadFile`, `WriteFile`, and
`WriteFileAt`.

```go
rc, err := env.ReadFile(ctx, "/etc/os-release")
if err != nil {
	panic(err)
}
defer rc.Close()
data, _ := io.ReadAll(rc)
fmt.Println(string(data))
```

## Suspend and delete

You suspend an environment with `Suspend` or `SuspendLocal`, and delete it with `Delete`. Both
`Client` and `Env` expose these methods.

```go
err := client.Suspend(ctx, envclient.DefaultAtespace, "dev1")
if err != nil {
	panic(err)
}
```

## Warm the nodes of a worker pool before a run

The `WarmImage` method prepares worker nodes. It invokes a callback for each probe.

```go
_, err := client.WarmImage(ctx, &ateenvv1alpha.WarmImageRequest{
	RootfsImage: "ubuntu:22.04",
}, func(probe *ateenvv1alpha.WarmProbe) {
	fmt.Println(probe.GetNode())
})
if err != nil {
	panic(err)
}
```

## Handle errors

The environment wakes on the first guest call. A failed wake produces a `*StartError`, which
`errors.As` unpacks. The `Retryable` function checks if a fresh environment can succeed.

Other errors wrap one of five sentinels, which `errors.Is` matches: `ErrNotFound`,
`ErrProcessExited`, `ErrInvalidArgument`, `ErrPermissionDenied`, and `ErrFailedPrecondition`. The
gRPC status stays in the chain, so `status.Code(err)` (`google.golang.org/grpc/status`) still
reports the code.

```go
_, err := env.Shell(ctx, "ls")
var startErr *envclient.StartError
if errors.As(err, &startErr) {
	fmt.Println("Start failed:", startErr.Reason)
} else if envclient.Retryable(err) {
	fmt.Println("Retryable error")
}

_, err = client.Get(ctx, envclient.DefaultAtespace, "missing")
if errors.Is(err, envclient.ErrNotFound) {
	fmt.Println("no such environment:", status.Code(err)) // codes.NotFound
}
```

## Observability

The `ClientOptions` struct accepts `TracerProvider`, `TraceServiceName`, and `MetricsRegisterer`.
With them the client emits spans and records the Prometheus histograms
`ate_env_client_exec_seconds`, `ate_env_client_suspend_seconds`, and
`ate_env_client_ready_to_first_command_seconds`. `NewTracerProvider` is the OTLP default; the caller
owns it and calls `Shutdown` to flush.

```go
tp, err := envclient.NewTracerProvider(ctx, "my-trainer")
if err != nil {
	panic(err)
}
defer tp.Shutdown(ctx)

client, err := envclient.NewClient(envclient.ClientOptions{
	Endpoint:          "localhost:7777",
	TracerProvider:    tp,
	MetricsRegisterer: prometheus.DefaultRegisterer,
})
```

See [docs/observability.md](../../docs/observability.md) for the collector setup and the other
components.

## Develop against a local guest daemon (no cluster)

The repository provides a local guest daemon at `examples/guest-daemon`. It serves the process and
filesystem services without a cluster:

```bash
go run ./examples/guest-daemon --listen 127.0.0.1:8090 --workspace "$(mktemp -d)"
```

```go
client, err := envclient.NewClient(envclient.ClientOptions{Endpoint: "127.0.0.1:8090"})
if err != nil {
	panic(err)
}
defer client.Close()
env := client.Env(envclient.DefaultAtespace, "anything") // the daemon ignores the routing metadata
resp, err := env.Shell(ctx, "uname -a")
```

Lifecycle calls (`Create`, `Get`, `Suspend`, `Delete`, ...) fail in this mode; only `ate-env-api`
implements them.

# ate-env-rl: Go Client

The Go client library for the Agent Substrate Environment API provides a programmatic interface to
lifecycle and guest operations.

## Install

```bash
go get github.com/agent-substrate/env/clients/go
```

The module path is a separate decision. Until the decision is final,
`go get github.com/agent-substrate/env` without a `replace` directive resolves to the previous
repository on the public proxy.

## Connect

A single `Client` multiplexes concurrent operations over one channel. Programs create one client,
share it, and close it on shutdown. You provide `Endpoint` in `ClientOptions`. Other fields include
`AuthToken`, `GRPCConn`, `Connections`, `DialOptions`, `TracerProvider`, `TraceServiceName`, and
`MetricsRegisterer`.

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
fmt.Println(string(resp.Stdout))
```

## Run background processes and stream output

You can start a background process with `StartProcess`. `Env.Process` and `Env.FindProcess` retrieve
an existing process. The `Process` handle exposes `Wait`, `Info`, `Kill`, `Signal`, `PTY`, `Resize`,
`Output`, and `Stdin`.

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
w.Write([]byte("hello"))
w.Close()
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
`errors.As` unpacks. The `Retryable` function checks if a fresh environment can succeed. The client
uses `errors.Is` with sentinels and wraps gRPC statuses accessible via `status.Code`.

```go
_, err := env.Shell(ctx, "ls")
var startErr *envclient.StartError
if errors.As(err, &startErr) {
	fmt.Println("Start failed:", startErr.Reason)
} else if envclient.Retryable(err) {
	fmt.Println("Retryable error")
}
```

## Observability

The `ClientOptions` struct accepts `TracerProvider`, `TraceServiceName`, and `MetricsRegisterer`.
The client emits spans and updates three Prometheus histograms when you configure these fields.

## Develop against a local guest daemon (no cluster)

The repository provides a local guest daemon at `examples/guest-daemon`. It serves guest operations
without a cluster.

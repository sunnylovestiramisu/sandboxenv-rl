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
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/mcp"
	"github.com/agent-substrate/env/internal/portproxy"
	"github.com/agent-substrate/env/internal/telemetry"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// serviceName is the OpenTelemetry service.name of the API server's spans.
const serviceName = "ate-env-api"

func main() {
	var (
		listen            = flag.String("listen", "0.0.0.0:7777", "address to serve the API on")
		ateapi            = flag.String("ateapi", ate.DefaultControlAddr, "address of the ateapi gRPC control plane")
		atenet            = flag.String("atenet", ate.DefaultRouterAddr, "address of the atenet HTTP router")
		atenetConn        = flag.String("atenet-connect", ate.DefaultRouterConnectAddr, "address of the atenet router's plaintext CONNECT listener; HTTP requests carrying X-Env-Id and X-Env-Port, or to /v1alpha/envs/{id}/ports/{port}/..., are forwarded through it to that port in the environment. Without -auth-token-file the forwarding does not authenticate callers, and it reaches any port of any actor the router serves. Empty disables it")
		ppOrigins         = flag.String("port-proxy-trusted-origins", "", "comma-separated browser origins (scheme://host[:port]) whose pages may use the port proxy's path form, next to the listener's own origin")
		ppHeaderTimeout   = flag.Duration("port-proxy-response-header-timeout", 0, "how long the port proxy waits for a daemon's response headers; 0 means no limit, and the caller's deadline applies")
		hostSuffix        = flag.String("host-suffix", ate.DefaultHostSuffix, "atenet router host suffix for actor routing")
		skipVerify        = flag.Bool("skip-verify", true, "skip TLS certificate verification on the control plane connection")
		tokenFile         = flag.String("ateapi-token-file", "", "file with a bearer token for the control plane, e.g. a projected ServiceAccount token with the ateapi audience")
		authTokenFile     = flag.String("auth-token-file", "", "file with a bearer token required from gRPC, MCP and port-proxy callers; re-read when the file changes")
		wakeBudget        = flag.Duration("wake-budget", apiservice.DefaultWakeBudget, "how long a guest call or a port-proxy request waits for a suspended environment to wake before failing; cold image pulls and cross-region snapshot reads exceed the router's own parking budget")
		wakeRunningBudget = flag.Duration("wake-running-budget", apiservice.DefaultWakeRunningBudget, "how long a guest call waits when the environment is already RUNNING but the router still answers Unavailable (guest not yet listening vs. path down)")
		enableTracing     = flag.Bool("enable-tracing", false, "export OpenTelemetry traces over OTLP/gRPC, configured by OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_INSECURE; incoming W3C trace context is continued and passed on to ateapi and the guests")
		metricsAddr       = flag.String("metrics-listen-addr", ":9090", "address to serve Prometheus metrics on, at /metrics, on a listener of its own; empty disables metrics")
	)
	flag.Parse()

	var callerAuth *apiservice.BearerAuth
	if *authTokenFile != "" {
		var err error
		callerAuth, err = apiservice.NewBearerAuthFromFile(*authTokenFile)
		if err != nil {
			log.Fatalf("loading --auth-token-file: %v", err)
		}
	} else if !isLoopbackAddr(*listen) {
		log.Printf("WARNING: listening on %s without --auth-token-file; all gRPC, MCP and port-proxy callers are unauthenticated", *listen)
	}
	if *enableTracing {
		_, shutdown, err := telemetry.SetupOTel(context.Background(), serviceName)
		if err != nil {
			log.Fatalf("--enable-tracing: %v", err)
		}
		defer shutdown()
		log.Printf("tracing enabled (OTLP endpoint %q)", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	var metrics *telemetry.Metrics
	if *metricsAddr != "" {
		metrics = telemetry.NewMetrics()
	}

	client, err := ate.New(ate.Options{
		ControlAddr:     *ateapi,
		RouterAddr:      *atenet,
		HostSuffix:      *hostSuffix,
		SkipVerify:      *skipVerify,
		BearerTokenFile: *tokenFile,
		EnableTracing:   *enableTracing,
	})
	if err != nil {
		log.Fatalf("creating env client: %v", err)
	}
	defer client.Close()

	// Create the default atespace up front so ActorTemplates can be registered
	// before the first environment is created (templates live in an atespace).
	// Bounded so a hung control plane fails the pod instead of leaving it
	// unlistening; the restart backoff retries until Substrate answers.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := client.EnsureAtespace(ctx, ate.DefaultAtespace); err != nil {
		log.Fatalf("ensuring default atespace %q: %v", ate.DefaultAtespace, err)
	}
	cancel()
	log.Printf("default atespace %q ready", ate.DefaultAtespace)

	apisvc := apiservice.New(client, *atenet, *hostSuffix)
	apisvc.WakeBudget = *wakeBudget
	apisvc.WakeRunningBudget = *wakeRunningBudget
	apisvc.Metrics = metrics
	defer apisvc.Close()

	var serverOpts []grpc.ServerOption
	if *enableTracing {
		serverOpts = append(serverOpts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}
	// Metrics run before auth so rejected calls are counted too.
	if metrics != nil {
		serverOpts = append(serverOpts,
			grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor()),
			grpc.ChainStreamInterceptor(metrics.StreamServerInterceptor()))
	}
	if callerAuth != nil {
		serverOpts = append(serverOpts,
			grpc.ChainUnaryInterceptor(callerAuth.UnaryInterceptor()),
			grpc.ChainStreamInterceptor(callerAuth.StreamInterceptor()))
	}
	grpcServer := grpc.NewServer(serverOpts...)
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, apisvc)
	ateenvv1alpha.RegisterProcessServiceServer(grpcServer, apisvc)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcServer, apisvc)

	mcpHandler := mcp.NewHandler(client)
	if callerAuth != nil {
		mcpHandler = callerAuth.WrapHTTP(mcpHandler)
	}

	var metricsSrv *http.Server
	if metrics != nil {
		metrics.InitMethods(grpcServer.GetServiceInfo())
		metrics.InitAtespace(ate.DefaultAtespace)
		metrics.SetEnvironmentSource(apisvc.EnvironmentCounts)
		metricsMux := http.NewServeMux()
		metricsMux.Handle("GET /metrics", metrics.Handler())
		metricsSrv = &http.Server{Addr: *metricsAddr, Handler: metricsMux}
		go func() {
			log.Printf("metrics listening on %s", *metricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("metrics server error: %v", err)
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})
	mux.Handle("/v1alpha/envs/{id}/mcp", mcpHandler)
	mux.Handle("/", grpcServer)

	// Requests that name an environment port go to the port proxy before
	// anything else, whatever their path: the path belongs to the daemon.
	var handler http.Handler = mux
	if *atenetConn != "" {
		var origins []string
		for o := range strings.SplitSeq(*ppOrigins, ",") {
			if o = strings.TrimSpace(o); o != "" {
				origins = append(origins, o)
			}
		}
		ppOpts := portproxy.Options{
			ConnectAddr:           *atenetConn,
			HostSuffix:            *hostSuffix,
			DefaultAtespace:       ate.DefaultAtespace,
			ResponseHeaderTimeout: *ppHeaderTimeout,
			TrustedOrigins:        origins,
			WakeBudget:            *wakeBudget,
		}
		if callerAuth != nil {
			ppOpts.Authenticate = callerAuth.Check
		}
		pp, err := portproxy.New(ppOpts)
		if err != nil {
			log.Fatalf("--port-proxy-trusted-origins: %v", err)
		}
		// Header form and path form, /v1alpha/envs/{id}/ports/{port}/...
		handler = portproxy.Dispatch(pp, mux)
	}

	// Enable both HTTP/1.1 (for /healthz and /v1alpha/ HTTP endpoints) and
	// unencrypted HTTP/2 (h2c, for gRPC services) on the same plaintext TCP listener.
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:      *listen,
		Handler:   handler,
		Protocols: &protocols,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
		_ = srv.Shutdown(context.Background())
		if metricsSrv != nil {
			_ = metricsSrv.Shutdown(context.Background())
		}
	}()

	log.Printf("ate-env-api listening on %s (ateapi %s, atenet %s, atenet CONNECT %q)", *listen, *ateapi, *atenet, *atenetConn)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

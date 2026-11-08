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
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/hostcheck"
	"github.com/agent-substrate/env/internal/mcp"
	"github.com/agent-substrate/env/internal/portproxy"
	"github.com/agent-substrate/env/internal/routertls"
	"github.com/agent-substrate/env/internal/telemetry"
	"github.com/agent-substrate/env/internal/unknownfields"
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

func splitCSV(s string) []string {
	var out []string
	for v := range strings.SplitSeq(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// portProxyAtespaces parses -port-proxy-atespaces; "*" allows any atespace and empty is an error.
func portProxyAtespaces(v string) ([]string, error) {
	if strings.TrimSpace(v) == "*" {
		return nil, nil
	}
	out := splitCSV(v)
	if len(out) == 0 {
		return nil, errors.New("-port-proxy-atespaces is empty; name the atespaces, or * for any")
	}
	if slices.Contains(out, "*") {
		return nil, errors.New("-port-proxy-atespaces cannot combine * with other names")
	}
	return out, nil
}

// Files that tell a pod its namespace and cluster domain.
const (
	resolvConfPath   = "/etc/resolv.conf"
	podNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

// serviceHostNames returns the in-cluster names of the Service service: the
// namespace and cluster domain come from the resolv.conf search list, which
// kubelet writes for each pod; else the namespace comes from POD_NAMESPACE or
// the ServiceAccount namespace file, without the cluster-domain name. An
// upgrade that keeps the old Deployment arguments thus keeps these names.
func serviceHostNames(service string, readFile func(string) ([]byte, error), getenv func(string) string) []string {
	if service == "" {
		return nil
	}
	var namespace, domain string
	if b, err := readFile(resolvConfPath); err == nil {
		namespace, domain = hostcheck.ClusterDNS(string(b))
	}
	if namespace == "" {
		namespace = strings.TrimSpace(getenv("POD_NAMESPACE"))
	}
	if namespace == "" {
		if b, err := readFile(podNamespacePath); err == nil {
			namespace = strings.TrimSpace(string(b))
		}
	}
	return hostcheck.ServiceHosts(service, namespace, domain)
}

// otelServiceName is the OpenTelemetry service.name of the API server's spans.
const otelServiceName = "ate-env-api"

func main() {
	var (
		listen            = flag.String("listen", "0.0.0.0:7777", "address to serve the API on")
		ateapi            = flag.String("ateapi", ate.DefaultControlAddr, "address of the ateapi gRPC control plane")
		atenet            = flag.String("atenet", ate.DefaultRouterAddr, "address of the atenet router for the guest's gRPC: its HTTPS listener (443) with -atenet-client-cert, else the plaintext one, which only the test fakes serve")
		atenetConn        = flag.String("atenet-connect", ate.DefaultRouterConnectAddr, "address of the atenet router's CONNECT listener: the TLS one (8444) with -atenet-client-cert, else the plaintext one, which only the test fakes serve; HTTP requests carrying X-Env-Id and X-Env-Port, or to /v1alpha/envs/{id}/ports/{port}/..., are forwarded through it to that port in the environment. Without -auth-token-file the forwarding does not authenticate callers, and it reaches any port of any actor in the atespaces -port-proxy-atespaces names. Empty disables it")
		ppAtespaces       = flag.String("port-proxy-atespaces", ate.DefaultAtespace, "comma-separated atespaces whose environments the port proxy forwards to, or * for any")
		ppOrigins         = flag.String("port-proxy-trusted-origins", "", "comma-separated browser origins (scheme://host[:port]) whose pages may use the port proxy's path form, next to the listener's own origin")
		ppHeaderTimeout   = flag.Duration("port-proxy-response-header-timeout", 0, "how long the port proxy waits for a daemon's response headers; 0 means no limit, and the caller's deadline applies")
		hostSuffix        = flag.String("host-suffix", ate.DefaultHostSuffix, "atenet router host suffix for actor routing")
		atenetClientCert  = flag.String("atenet-client-cert", "", "PEM credential bundle (certificate chain and key) presented to the atenet router's TLS listeners: the pod identity bundle the kubelet projects. Substrate v0.6.0 serves only those listeners; with this flag, -atenet and -atenet-connect default to them (443 and 8444). The file is re-read at every handshake, so rotation needs no restart")
		atenetCAFile      = flag.String("atenet-ca-file", "", "PEM trust bundle that signs the router's serving certificate (the servicedns bundle); required with -atenet-client-cert")
		atenetServerName  = flag.String("atenet-server-name", "atenet-router.ate-system.svc", "name the router's serving certificate must carry; used with -atenet-client-cert")
		skipVerify        = flag.Bool("skip-verify", true, "skip TLS certificate verification on the control plane connection")
		tokenFile         = flag.String("ateapi-token-file", "", "file with a bearer token for the control plane, e.g. a projected ServiceAccount token with the ateapi audience")
		authTokenFile     = flag.String("auth-token-file", "", "file with a bearer token required from gRPC, MCP and port-proxy callers; re-read when the file changes")
		wakeBudget        = flag.Duration("wake-budget", apiservice.DefaultWakeBudget, "how long a guest gRPC call waits for a suspended environment to wake before failing; cold image pulls, cross-region snapshot reads and the template's wakeup probe exceed the router's own hold of about 10 s. A port-proxy request waits on the router instead and gets 503 with Retry-After while the wake goes on; this budget applies to it only when the router refuses the tunnel itself")
		wakeRunningBudget = flag.Duration("wake-running-budget", apiservice.DefaultWakeRunningBudget, "how long a guest call waits when the environment is already RUNNING but the router still answers Unavailable (guest not yet listening vs. path down)")
		warmMaxProbes     = flag.Int("warm-max-probes-per-node", apiservice.DefaultWarmMaxProbesPerNode, "most WarmImage probe environments in flight per node of one worker selector, across calls: a round takes nodes x probes_per_node slots on its selector before it creates its probes and gives them back once they are deleted, so a pre-flight that fans out one WarmImage per image puts at most this many first boots (image unpacks) on a node at once; a request's probes_per_node above it is refused, and a round that waits for slots is logged")
		wakeNoCapGrace    = flag.Duration("wake-no-capacity-grace", 0, "when positive, fail a guest call with RESOURCE_EXHAUSTED (trailer ate-env-start-failure: no-capacity) once the router has said for this long that no worker has room for the environment, instead of waiting the whole --wake-budget; 0 (the default) keeps waiting, since capacity can come back as nodes scale up or other environments stop")
		allowedHosts      = flag.String("allowed-hosts", "", "comma-separated host names that HTTP callers may send in the Host header, next to IP addresses, localhost, names under .localhost and the names of --service-name, which are always allowed; a leading *. allows the names below it. Other names get 403, which blocks DNS rebinding against the listener, the MCP endpoint and the port proxy. gRPC requests are not checked")
		serviceName       = flag.String("service-name", "ate-env-api", "name of the Kubernetes Service in front of ate-env-api; its in-cluster names are allowed hosts (<name>, <name>.<namespace>, <name>.<namespace>.svc and <name>.<namespace>.svc.<cluster domain>, with the namespace and cluster domain read from /etc/resolv.conf, else the namespace from POD_NAMESPACE). Empty allows none of them")
		maxStreams        = flag.Int("max-concurrent-streams", DefaultMaxConcurrentStreams, "most concurrent HTTP/2 streams (gRPC calls) one client connection may have open; calls beyond it queue in the client. Go's default is 250, which a single trainer process reaches with a few hundred sandboxes, because every exec holds a stream for the whole command and its wake")
		enableTracing     = flag.Bool("enable-tracing", false, "export OpenTelemetry traces over OTLP/gRPC, configured by OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_EXPORTER_OTLP_INSECURE; incoming W3C trace context is continued and passed on to ateapi and the guests")
		metricsAddr       = flag.String("metrics-listen-addr", "", "address to serve Prometheus metrics on, at /metrics, on a listener of its own, e.g. :9090; empty (the default) disables metrics. The listener has no authentication and no Host check: it serves request counts by method and code, atespace names with environment counts by state, and the build version to anyone who can reach it, so bind it where only trusted scrapers can")
		environmentsTTL   = flag.Duration("metrics-environments-ttl", telemetry.DefaultEnvironmentsCacheTTL, "how long ate_env_api_environments serves one read of the control plane (a ListActors per atespace) before a scrape reads it again")
	)
	flag.Parse()

	// exitErr, when set, ends the process with status 1 after every other
	// deferred function (OTel shutdown, client and service Close) has run:
	// this defer is registered first, so it runs last.
	var exitErr error
	var serveErr atomic.Pointer[error]
	defer func() {
		if exitErr != nil {
			log.Fatal(exitErr)
		}
	}()

	serviceHosts := serviceHostNames(*serviceName, os.ReadFile, os.Getenv)
	if len(serviceHosts) > 0 {
		log.Printf("hostcheck: allowing the Service names %s", strings.Join(serviceHosts, ", "))
	}
	hosts, err := hostcheck.New(append(serviceHosts, hostcheck.ParseList(*allowedHosts)...))
	if err != nil {
		log.Fatalf("--allowed-hosts: %v", err)
	}

	var callerAuth *apiservice.BearerAuth
	if *authTokenFile != "" {
		callerAuth, err = apiservice.NewBearerAuthFromFile(*authTokenFile)
		if err != nil {
			log.Fatalf("loading --auth-token-file: %v", err)
		}
	} else if !isLoopbackAddr(*listen) {
		log.Printf("WARNING: listening on %s without --auth-token-file; all gRPC, MCP and port-proxy callers are unauthenticated", *listen)
	}
	if *enableTracing {
		shutdown, err := telemetry.SetupOTel(context.Background(), otelServiceName)
		if err != nil {
			log.Fatalf("--enable-tracing: %v", err)
		}
		defer shutdown()
		log.Printf("tracing enabled (OTLP endpoint %q)", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	var metrics *telemetry.Metrics
	if *metricsAddr != "" {
		metrics = telemetry.NewMetrics()
		metrics.SetEnvironmentsCacheTTL(*environmentsTTL)
	}

	// With a client certificate the router is reached on its TLS listeners,
	// the only ones a v0.6.0 Substrate serves; the plaintext defaults are
	// replaced unless the operator named other addresses.
	var routerTLS *tls.Config
	var routerIdentity string
	if *atenetClientCert != "" || *atenetCAFile != "" {
		opts := routertls.Options{
			CredentialBundleFile: *atenetClientCert,
			TrustBundleFile:      *atenetCAFile,
			ServerName:           *atenetServerName,
		}
		cfg, err := opts.Config()
		if err != nil {
			log.Fatalf("router TLS: %v", err)
		}
		routerTLS = cfg
		if routerIdentity, err = opts.Identity(); err != nil {
			log.Fatalf("router TLS: %v", err)
		}
		log.Printf("router TLS: presenting %s to the atenet router, whose --ingress-allowed-spiffe-ids (ATE_INGRESS_ALLOWED_SPIFFE_IDS for ate-setup) must list it", routerIdentity)
		if *atenet == ate.DefaultRouterAddr {
			*atenet = ate.DefaultRouterTLSAddr
		}
		if *atenetConn == ate.DefaultRouterConnectAddr {
			*atenetConn = ate.DefaultRouterConnectTLSAddr
		}
	} else {
		log.Printf("WARNING: no -atenet-client-cert: dialing the router's plaintext listeners (%s, CONNECT %q), which no Substrate release serves since v0.6.0; only the test fakes do", *atenet, *atenetConn)
	}

	client, err := ate.New(ate.Options{
		ControlAddr:     *ateapi,
		RouterAddr:      *atenet,
		HostSuffix:      *hostSuffix,
		SkipVerify:      *skipVerify,
		BearerTokenFile: *tokenFile,
		RouterTLS:       routerTLS,
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
	apisvc.WakeNoCapacityGrace = *wakeNoCapGrace
	apisvc.WarmMaxProbesPerNode = *warmMaxProbes
	apisvc.Info = apiservice.ServerInfo{
		Version:   buildVersion(),
		PortProxy: *atenetConn != "",
	}
	apisvc.RouterTLS = routerTLS
	apisvc.RouterIdentity = routerIdentity
	apisvc.Metrics = metrics
	defer apisvc.Close()

	grpcServer := grpc.NewServer(grpcServerOptions(callerAuth, metrics, *enableTracing)...)
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, apisvc)
	ateenvv1alpha.RegisterProcessServiceServer(grpcServer, apisvc)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcServer, apisvc)

	mcpHandler := mcp.NewHandler(client)
	if callerAuth != nil {
		mcpHandler = callerAuth.WrapHTTP(mcpHandler)
	}

	var metricsSrv *http.Server
	var metricsErr <-chan error
	if metrics != nil {
		metrics.InitMethods(grpcServer.GetServiceInfo())
		metrics.InitAtespace(ate.DefaultAtespace)
		metrics.SetEnvironmentSource(apisvc.EnvironmentCounts)
		// A bind failure returns through main, so the deferred OTel
		// shutdown and apisvc.Close still run.
		metricsSrv, metricsErr, err = serveMetrics(*metricsAddr, metrics.Handler())
		if err != nil {
			exitErr = err
			return
		}
		log.Printf("metrics listening on %s (no authentication, no Host check)", metricsSrv.Addr)
	}

	// The Host check wraps the HTTP routes (/healthz, MCP and the port
	// proxy), while the gRPC route on / stays unwrapped.
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", hosts.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok\n")
	})))
	mux.Handle("/v1alpha/envs/{id}/mcp", hosts.Wrap(mcpHandler))
	mux.Handle("/", grpcServer)

	// Requests that name an environment port go to the port proxy before
	// anything else, whatever their path: the path belongs to the daemon.
	var handler http.Handler = mux
	if *atenetConn != "" {
		origins := splitCSV(*ppOrigins)
		atespaces, err := portProxyAtespaces(*ppAtespaces)
		if err != nil {
			log.Fatalf("port proxy: %v", err)
		}
		ppOpts := portproxy.Options{
			ConnectAddr:           *atenetConn,
			TLS:                   routerTLS,
			HostSuffix:            *hostSuffix,
			DefaultAtespace:       ate.DefaultAtespace,
			ResponseHeaderTimeout: *ppHeaderTimeout,
			TrustedOrigins:        origins,
			WakeBudget:            *wakeBudget,
			Atespaces:             atespaces,
		}
		if metrics != nil {
			// A first contact through the port proxy ends create_to_ready too.
			ppOpts.OnConnected = apisvc.ObserveFirstContact
			ppOpts.OnCrashed = apisvc.ObserveCrashedContact
		}
		pp, err := portproxy.New(portProxyOptions(ppOpts, apisvc, callerAuth))
		if err != nil {
			log.Fatalf("port proxy: %v", err)
		}
		// Header form and path form, /v1alpha/envs/{id}/ports/{port}/...
		handler = portproxy.Dispatch(pp, mux, hosts.Wrap)
	}

	// Enable both HTTP/1.1 (for /healthz and /v1alpha/ HTTP endpoints) and
	// unencrypted HTTP/2 (h2c, for gRPC services) on the same plaintext TCP listener.
	srv := newHTTPServer(*listen, handler, *maxStreams)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A metrics server that stops with an error stops the API server too.
	// main returns after that, so the deferred functions run, and exits
	// non-zero through exitErr.
	if metricsErr != nil {
		go func() {
			if err, ok := <-metricsErr; ok {
				serveErr.Store(&err)
				stop()
			}
		}()
	}

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
		_ = srv.Shutdown(context.Background())
		if metricsSrv != nil {
			_ = metricsSrv.Shutdown(context.Background())
		}
	}()

	log.Printf("ate-env-api %s listening on %s (ateapi %s, atenet %s, atenet CONNECT %q, router TLS %v)", apisvc.Info.Version, *listen, *ateapi, *atenet, *atenetConn, routerTLS != nil)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		exitErr = fmt.Errorf("server error: %w", err)
		return
	}
	if err := serveErr.Load(); err != nil {
		exitErr = *err
	}
}

// metricsReadHeaderTimeout bounds how long the metrics listener waits for a
// request's headers, so idle or slow connections do not pile up.
const metricsReadHeaderTimeout = 5 * time.Second

// serveMetrics binds addr and serves handler at /metrics on it, on a server
// of its own with no authentication and no Host check. A bind failure is
// returned at once. An error that stops the server later is sent on the
// returned channel, which is closed when the server stops.
func serveMetrics(addr string, handler http.Handler) (*http.Server, <-chan error, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("metrics listener %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", handler)
	srv := &http.Server{Addr: ln.Addr().String(), Handler: mux, ReadHeaderTimeout: metricsReadHeaderTimeout}
	errc := make(chan error, 1)
	go func() {
		defer close(errc)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("metrics server error: %w", err)
		}
	}()
	return srv, errc, nil
}

// grpcServerOptions returns the interceptor chain of the API's gRPC server.
// Authentication runs first, so an unauthenticated caller learns nothing from
// the unknown-field check. callerAuth is nil when no token is required.
func grpcServerOptions(callerAuth *apiservice.BearerAuth, metrics *telemetry.Metrics, tracing bool) []grpc.ServerOption {
	var opts []grpc.ServerOption
	if tracing {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}
	var unary []grpc.UnaryServerInterceptor
	var stream []grpc.StreamServerInterceptor
	// Metrics run before auth so rejected calls are counted too.
	if metrics != nil {
		unary = append(unary, metrics.UnaryServerInterceptor())
		stream = append(stream, metrics.StreamServerInterceptor())
	}
	if callerAuth != nil {
		unary = append(unary, callerAuth.UnaryInterceptor())
		stream = append(stream, callerAuth.StreamInterceptor())
	}
	unary = append(unary, unknownfields.UnaryServerInterceptor())
	stream = append(stream, unknownfields.StreamServerInterceptor())
	return append(opts, grpc.ChainUnaryInterceptor(unary...), grpc.ChainStreamInterceptor(stream...))
}

// DefaultMaxConcurrentStreams is the default -max-concurrent-streams. Each
// in-flight exec holds one stream for the whole command, including the wait
// for a suspended environment to wake, so one trainer process with a few
// hundred sandboxes reaches Go's built-in limit of 250 and its further calls
// queue in the client before ate-env-api sees them.
const DefaultMaxConcurrentStreams = 4096

// newHTTPServer serves handler on addr over HTTP/1.1 (for /healthz and the
// /v1alpha/ HTTP endpoints) and unencrypted HTTP/2 (h2c, for the gRPC
// services) on one plaintext listener, allowing maxStreams concurrent HTTP/2
// streams per client connection (Go's default when maxStreams <= 0).
func newHTTPServer(addr string, handler http.Handler, maxStreams int) *http.Server {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Addr:      addr,
		Handler:   handler,
		Protocols: &protocols,
	}
	if maxStreams > 0 {
		srv.HTTP2 = &http.HTTP2Config{MaxConcurrentStreams: maxStreams}
	}
	return srv
}

// portProxyOptions adds what the port proxy takes from the API server to
// base: the state read that makes a crashed or deleted environment fail at
// once, and the caller check when -auth-token-file is set.
func portProxyOptions(base portproxy.Options, apisvc *apiservice.Server, callerAuth *apiservice.BearerAuth) portproxy.Options {
	base.ActorState = apisvc.ActorState
	if callerAuth != nil {
		base.Authenticate = callerAuth.Check
		base.AuthenticateCapability = callerAuth.CheckCapability
	}
	return base
}

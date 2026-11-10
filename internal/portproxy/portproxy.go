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

// Package portproxy forwards HTTP requests from callers outside the actor
// network to a TCP port inside an environment, such as a daemon that a runtime
// layer starts next to the guest (docs/rootfs-images/RUNTIMES.md).
//
// A request names its target with three headers, EnvIDHeader,
// EnvAtespaceHeader (optional) and EnvPortHeader, or, for clients that cannot
// set headers (WebSocket clients, browsers), with a path prefix that the proxy
// strips: /v1alpha/envs/{id}/ports/{port}/... in the default atespace, or
// /v1alpha/atespaces/{atespace}/envs/{id}/ports/{port}/... . The proxy opens
// an HTTP CONNECT tunnel through the atenet router's CONNECT listener, naming
// the actor in ate-target-actor and the port in the CONNECT authority. The
// router (Substrate v0.6.0) accepts the tunnel at once and resumes a suspended
// actor for each request inside it, holding the request while the actor wakes
// (see modifyResponse). The proxy speaks HTTP/1.1 to the daemon through the
// tunnel and keeps idle tunnels for reuse, one pool for each environment and
// port.
//
// Trust model: without Options.Authenticate, the proxy does not authenticate
// callers. Whoever can reach the listener it is served on can reach any TCP
// port of any actor the router serves in the atespaces Options.Atespaces
// allows (all of them when it is empty), including actors that ate-env did not
// create: Substrate actors carry no label that tells them apart, thus a
// dedicated atespace is the boundary. With Authenticate, a caller must present
// the API's bearer token in EnvAuthorizationHeader or Authorization, and the
// token does not reach the daemon. A WebSocket client that cannot set headers
// presents a port capability instead (see CapabilityUser). Either way, a daemon
// behind it must
// authenticate its own clients. The path form refuses
// requests that a browser sends from another origin (see Options.TrustedOrigins),
// so a web page cannot drive a daemon through a port-forwarded listener.
package portproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/routertls"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The routing headers. They match the x-env-id and x-env-atespace metadata
// keys of the gRPC API, so a caller uses one vocabulary for both.
const (
	EnvIDHeader       = "X-Env-Id"
	EnvAtespaceHeader = "X-Env-Atespace"
	EnvPortHeader     = "X-Env-Port"
)

// EnvAuthorizationHeader carries the caller's credential for the API itself,
// "Bearer <token>", on a forwarded request. It leaves the request's own
// Authorization header free for the daemon's credential. The proxy never
// forwards it.
const EnvAuthorizationHeader = "X-Env-Authorization"

// DefaultHandshakeTimeout bounds dialing the router and waiting for its
// answer to CONNECT, which includes resuming a suspended actor.
const DefaultHandshakeTimeout = 2 * time.Minute

// DefaultWakeBudget bounds how long dial retries a CONNECT that the router
// refuses with 503. Substrate v0.6.0's router refuses a CONNECT only at the
// Envoy level, for example when overloaded: it accepts the tunnel for any
// actor state and resumes the actor for the request inside it, which it
// holds itself (see modifyResponse). The same five minutes as ate-env-api's
// guest calls (apiservice.DefaultWakeBudget), which share the --wake-budget
// flag.
const DefaultWakeBudget = 5 * time.Minute

// Idle tunnels: each one is a TCP stream from the API to an actor, held open
// by the router and the worker. Keep few, and not for long, so an idle
// environment is not held awake and a large fleet does not pin thousands
// of sockets in the API pod.
const (
	idleTunnelTimeout  = 10 * time.Second
	idleTunnelsPerPort = 4
	idleTunnelsTotal   = 1024
)

const (
	wakeRetryInitialBackoff = 200 * time.Millisecond
	wakeRetryMaxBackoff     = 2 * time.Second
)

// guestPort is where the ate-env guest listens in every environment. It is
// not a daemon port: the API already serves it, and a daemon cannot bind it.
const guestPort = 80

// Options configures a Proxy.
type Options struct {
	// ConnectAddr is the router's CONNECT listener, host:port: the TLS one
	// (8444) with TLS set, else the plaintext one, which only the test fakes
	// serve.
	ConnectAddr string
	// TLS, when set, wraps each connection to ConnectAddr in TLS with it: the
	// pod identity certificate and the router's trust bundle (see
	// internal/routertls). The CONNECT and the tunnel are unchanged inside.
	TLS *tls.Config
	// HostSuffix is the router's actor host suffix; the CONNECT authority is
	// <id>.<suffix>:<port>. The router reads only the port from it.
	HostSuffix string
	// DefaultAtespace applies when a request carries no EnvAtespaceHeader.
	DefaultAtespace string
	// HandshakeTimeout bounds the CONNECT handshake. Zero means
	// DefaultHandshakeTimeout.
	HandshakeTimeout time.Duration
	// ResponseHeaderTimeout bounds the wait for the daemon's response headers
	// after the request is sent. Zero means no limit: a daemon that answers a
	// long command only when it finishes is served, and the caller's own
	// deadline ends a wait on a daemon that never answers.
	ResponseHeaderTimeout time.Duration
	// TrustedOrigins are browser origins (scheme://host[:port]) allowed to use
	// the path form from a page they serve, next to the listener's own origin.
	TrustedOrigins []string
	// WakeBudget bounds how long dial retries a CONNECT that the router
	// refuses with 503, which Substrate v0.6.0's router does only at the
	// Envoy level (overload, drain), never for an actor state. Zero means
	// DefaultWakeBudget.
	WakeBudget time.Duration
	// ActorState, when set, reads an environment's state from the control
	// plane. The proxy calls it only for an answer that is the router's 503
	// or 404 for the environment (see modifyResponse), never on the fast
	// path, and at most once per stateCacheTTL for one environment. The
	// router answers a CRASHED or DELETING actor with the same 503 as a
	// passing state; with this read, a crashed environment gets 410 and a
	// deleting one 404, each with EnvStateHeader. An error with code NotFound
	// means the environment is gone (404). Other errors leave the router's
	// answer unchanged.
	ActorState func(ctx context.Context, atespace, id string) (ateapipb.ActorState, error)
	// Authenticate, when set, checks the caller before a request is
	// forwarded. It gets the value of EnvAuthorizationHeader, or of
	// Authorization when the request has no EnvAuthorizationHeader. An error
	// refuses the request with 401. The credential does not reach the
	// daemon: the proxy drops EnvAuthorizationHeader, and drops Authorization
	// when it carried the credential.
	Authenticate func(credential string) error
	// AuthenticateCapability, when set next to Authenticate, checks a port
	// capability (see CapabilityUser) for the environment port of the
	// request. The proxy calls it only for a WebSocket upgrade request whose
	// Authorization is Basic with the user CapabilityUser and that has no
	// EnvAuthorizationHeader. An error refuses the request with 401.
	AuthenticateCapability func(capability, atespace, id string, port int) error

	// Atespaces, when set, are the only atespaces whose environments the
	// proxy forwards to; a request that names another gets 403, after
	// Authenticate. Empty means any atespace the router serves.
	// DefaultAtespace is not implied: list it to serve requests that name
	// no atespace.
	Atespaces []string
	// OnConnected, when set, is called with the environment's atespace and
	// id when an answer from the environment's port reaches the caller: the
	// daemon answered, after any wake. It is not called for the router's or
	// Envoy's own answers, which stand for an environment that sleeps,
	// crashed, is deleting or is gone (see modifyResponse). ate-env-api uses
	// it to end create_to_ready when a port is an environment's first
	// contact. It can be called more than once for one environment.
	OnConnected func(atespace, id string)
	// OnCrashed, when set, is called with the environment's atespace and id
	// when the state read behind a router answer finds the environment
	// CRASHED, as the caller gets 410. ate-env-api uses it to end
	// create_to_ready with status crashed.
	OnCrashed func(atespace, id string)
}

// Proxy is an http.Handler that forwards each request to the environment port
// its headers name.
type Proxy struct {
	opts      Options
	atespaces map[string]bool // nil: any atespace
	rp        *httputil.ReverseProxy
	tr        *http.Transport
	cop       *http.CrossOriginProtection

	stateMu    sync.Mutex
	states     map[string]*stateEntry // see actorState
	stateLogAt time.Time              // last failed state read logged
	// statePanicAt is when a panicking state read was last logged.
	statePanicAt time.Time
}

// New returns a Proxy. It fails only on a malformed trusted origin or
// atespace.
func New(opts Options) (*Proxy, error) {
	if opts.HostSuffix == "" {
		opts.HostSuffix = ate.DefaultHostSuffix
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if opts.WakeBudget <= 0 {
		opts.WakeBudget = DefaultWakeBudget
	}
	p := &Proxy{opts: opts, cop: http.NewCrossOriginProtection()}
	for _, o := range opts.TrustedOrigins {
		if err := p.cop.AddTrustedOrigin(o); err != nil {
			return nil, fmt.Errorf("trusted origin: %w", err)
		}
	}
	if len(opts.Atespaces) > 0 {
		p.atespaces = map[string]bool{}
		for _, a := range opts.Atespaces {
			if !label.MatchString(a) {
				return nil, fmt.Errorf("atespace %q is not valid", a)
			}
			p.atespaces[a] = true
		}
		p.opts.Atespaces = nil
	}
	p.tr = &http.Transport{
		DialContext:           p.dial,
		MaxIdleConnsPerHost:   idleTunnelsPerPort,
		MaxIdleConns:          idleTunnelsTotal,
		IdleConnTimeout:       idleTunnelTimeout,
		ResponseHeaderTimeout: opts.ResponseHeaderTimeout,
		// Pass encodings through untouched; the daemon and the caller
		// negotiate them.
		DisableCompression: true,
	}
	p.rp = &httputil.ReverseProxy{
		Rewrite:   p.rewrite,
		Transport: p.tr,
		// Flush each write at once, so streamed responses (server-sent events,
		// chunked output) reach the caller as the daemon produces them.
		FlushInterval:  -1,
		ErrorHandler:   p.handleError,
		ModifyResponse: p.modifyResponse,
	}
	return p, nil
}

// Wants reports whether r uses the header form: it carries EnvPortHeader and
// is not a gRPC call. Callers of the gRPC API never send the header, and the
// tunnel carries HTTP/1.1 only.
// Close drops the idle tunnels. Requests in flight are not affected.
func (p *Proxy) Close() { p.tr.CloseIdleConnections() }

func Wants(r *http.Request) bool {
	return r.Header.Get(EnvPortHeader) != "" &&
		!strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc")
}

// Dispatch returns a handler that sends header-form requests to p.ServeHTTP,
// path-form requests to p.ServePath, and all others to next. Any mw wrappers
// wrap the proxy routes without wrapping next. The path form is matched here
// rather than on a ServeMux, so the daemon gets the path exactly as sent: a
// mux would redirect a missing trailing slash, "//" and "..", and WebSocket
// clients do not follow redirects. Both forms route on the path as sent (see
// requestPath) and forward the path and query of the request-target as sent.
func Dispatch(p *Proxy, next http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	var proxy http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPathForm(requestPath(r.URL)) {
			p.ServePath(w, r)
			return
		}
		p.ServeHTTP(w, r)
	})
	for _, m := range mw {
		proxy = m(proxy)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if Wants(r) || isPathForm(requestPath(r.URL)) {
			proxy.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type target struct {
	atespace string
	id       string
	port     int
	// path and rawPath, when set, replace the request's path upstream:
	// rawPath is the path as sent, byte for byte, and path its decoding.
	// The path form strips its routing prefix; the header form keeps the
	// whole path.
	path, rawPath string
	// dropAuthorization removes the Authorization header upstream: it
	// carried the caller's credential for the API, not the daemon's.
	dropAuthorization bool
}

type targetKey struct{}

// label is a DNS-1123 label, the shape of environment ids and atespaces. The
// values go into a CONNECT request line and header, so nothing else may pass.
var label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// canonicalPort is a TCP port in decimal: no sign, no leading zero.
var canonicalPort = regexp.MustCompile(`^[1-9][0-9]{0,4}$`)

func parseTarget(r *http.Request, defaultAtespace string) (target, error) {
	for _, h := range []string{EnvIDHeader, EnvAtespaceHeader, EnvPortHeader} {
		if len(r.Header.Values(h)) > 1 {
			return target{}, fmt.Errorf("%s appears more than once", h)
		}
	}
	if r.Header.Get(EnvIDHeader) == "" {
		return target{}, fmt.Errorf("%s is required next to %s", EnvIDHeader, EnvPortHeader)
	}
	return makeTarget(r.Header.Get(EnvIDHeader), r.Header.Get(EnvAtespaceHeader), r.Header.Get(EnvPortHeader), defaultAtespace)
}

// makeTarget validates a target from either form.
func makeTarget(id, atespace, portStr, defaultAtespace string) (target, error) {
	t := target{id: id, atespace: atespace}
	if !label.MatchString(t.id) {
		return t, fmt.Errorf("environment id %q is not valid (lowercase alphanumerics and '-', at most 63)", t.id)
	}
	if t.atespace == "" {
		t.atespace = defaultAtespace
	}
	if !label.MatchString(t.atespace) {
		return t, fmt.Errorf("atespace %q is not valid", t.atespace)
	}
	n, err := strconv.Atoi(portStr)
	if !canonicalPort.MatchString(portStr) || err != nil || n > 65535 {
		return t, fmt.Errorf("port %q is not a TCP port (1-65535)", portStr)
	}
	if n == guestPort {
		return t, fmt.Errorf("port %d is the ate-env guest, which ate-env-api serves itself; a daemon listens on another port", guestPort)
	}
	t.port = n
	return t, nil
}

// pathPrefix splits raw, the path of the path form as sent, into its routing
// segments and the rest. n is 0 when the path does not have that shape. The
// split is by segment on the path as sent, the way a ServeMux matches, so an
// escaped routing segment (%65nv1 for env1) still ends the prefix where it
// should.
func pathPrefix(raw string) (id, atespace, portStr, rest string, n int) {
	segs := strings.Split(raw, "/")
	switch {
	case len(segs) >= 6 && segs[0] == "" && segs[1] == "v1alpha" && segs[2] == "envs" && segs[4] == "ports":
		id, portStr, n = segs[3], segs[5], 6
	case len(segs) >= 8 && segs[0] == "" && segs[1] == "v1alpha" && segs[2] == "atespaces" && segs[4] == "envs" && segs[6] == "ports":
		atespace, id, portStr, n = segs[3], segs[5], segs[7], 8
	default:
		return "", "", "", "", 0
	}
	return id, atespace, portStr, "/" + strings.Join(segs[n:], "/"), n
}

func isPathForm(raw string) bool {
	_, _, _, _, n := pathPrefix(raw)
	return n > 0
}

func parsePath(raw, defaultAtespace string) (target, error) {
	id, atespace, portStr, rest, n := pathPrefix(raw)
	if n == 0 {
		return target{}, errors.New("path is not /v1alpha/[atespaces/{atespace}/]envs/{id}/ports/{port}[/rest]")
	}
	// Validation admits only labels and digits, thus an escaped routing
	// segment is unescaped first and the value that passes reads the same both
	// ways.
	for _, s := range []*string{&id, &atespace, &portStr} {
		v, err := url.PathUnescape(*s)
		if err != nil {
			return target{}, fmt.Errorf("bad path escaping: %w", err)
		}
		*s = v
	}
	t, err := makeTarget(id, atespace, portStr, defaultAtespace)
	if err != nil {
		return t, err
	}
	// The rest keeps its own escaping. The bare prefix, with or without a
	// trailing slash, is the daemon's root.
	t.rawPath = rest
	if t.path, err = url.PathUnescape(rest); err != nil {
		return t, fmt.Errorf("bad path escaping: %w", err)
	}
	// rewrite sends rawPath as it is (see setPath): url.EscapedPath would
	// re-escape bytes that are valid as sent (! ' ( ) *) and, worse, drop a
	// rawPath with bytes it calls invalid ({ } | ^, raw UTF-8), so that
	// "/a{b%2Fc" would reach the daemon as "/a%7Bb/c", one segment more.
	return t, nil
}

// requestPath returns the path of the request-target as the caller sent it.
// Go's server keeps it in URL.RawPath when it differs from the default
// escaping of URL.Path, and url.EscapedPath ignores a RawPath that holds
// bytes the server accepts but EscapedPath would escape ({ } | ^ #, raw UTF-8),
// so it is read here directly. A RawPath that does not decode to Path (set by
// a handler that changed Path) is not trusted.
func requestPath(u *url.URL) string {
	if u.RawPath != "" {
		if p, err := url.PathUnescape(u.RawPath); err == nil && p == u.Path {
			return u.RawPath
		}
	}
	return u.EscapedPath()
}

// setPath makes raw, the path as sent, the request-target path of u, byte
// for byte; path is its decoding. url.RequestURI writes Opaque as it is,
// except that an Opaque starting with "//" would read as an authority (it
// gains the scheme, "http://..."), so such a path goes in RawPath instead.
// There the bytes RawPath cannot hold raw ({ } | ^ " < > \ ` # and non-ASCII)
// are percent-encoded and every other byte, escapes included, is kept, so the
// daemon sees the same segments.
func setPath(u *url.URL, path, raw string) {
	u.Path = path
	if !strings.HasPrefix(raw, "//") {
		u.Opaque, u.RawPath = raw, ""
		return
	}
	var b strings.Builder
	for i := 0; i < len(raw); i++ {
		if c := raw[i]; c >= 0x7f || c <= ' ' || strings.IndexByte("\"#<>\\^`{|}", c) >= 0 {
			fmt.Fprintf(&b, "%%%02X", c)
		} else {
			b.WriteByte(c)
		}
	}
	u.Opaque, u.RawPath = "", b.String()
}

// ServePath forwards a path-form request to the environment port its path
// names, after it strips the routing prefix. It refuses a request that a
// browser sends from another origin: a state-changing method, or a WebSocket
// handshake, which is a GET that opens a channel both ways. The header form
// needs no such check, because a browser sends custom headers to another
// origin only after a CORS preflight, which the proxy never approves.
func (p *Proxy) ServePath(w http.ResponseWriter, r *http.Request) {
	t, err := parsePath(requestPath(r.URL), p.opts.DefaultAtespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !p.authenticate(w, r, &t) || !p.inScope(w, t) {
		return
	}
	check := r
	if r.Header.Get("Upgrade") != "" {
		// CrossOriginProtection lets safe methods through; judge a handshake
		// as the state-changing request it is.
		check = r.WithContext(r.Context())
		check.Method = http.MethodPost
	}
	if err := p.cop.Check(check); err != nil {
		http.Error(w, "cross-origin request to an environment port refused; "+
			"add the page's origin to the port proxy's trusted origins", http.StatusForbidden)
		return
	}
	p.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), targetKey{}, t)))
}

// ServeHTTP forwards r to the environment port its headers name.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := parseTarget(r, p.opts.DefaultAtespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.URL.Opaque == "" {
		t.path, t.rawPath = r.URL.Path, requestPath(r.URL)
	}
	if !p.authenticate(w, r, &t) || !p.inScope(w, t) {
		return
	}
	p.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), targetKey{}, t)))
}

// inScope answers 403 and returns false when t's atespace is not one that
// Options.Atespaces allows.
func (p *Proxy) inScope(w http.ResponseWriter, t target) bool {
	if p.atespaces == nil || p.atespaces[t.atespace] {
		return true
	}
	http.Error(w, fmt.Sprintf("the port proxy does not forward to atespace %q", t.atespace), http.StatusForbidden)
	return false
}

// authenticate checks the caller with Options.Authenticate, or a port
// capability with Options.AuthenticateCapability, and records in t which
// header to drop upstream. It answers 401 and returns false when the check
// fails.
func (p *Proxy) authenticate(w http.ResponseWriter, r *http.Request, t *target) bool {
	capability, hasCapability, ambiguous := capabilityCredential(r)
	if ambiguous {
		// Refused whether or not the proxy checks callers: forwarding would
		// hand the daemon a capability, and dropping all Authorization would
		// silently drop the daemon's own credential.
		http.Error(w, "a port capability must be the only Authorization credential", http.StatusBadRequest)
		return false
	}
	if hasCapability {
		// A capability is the API's credential, never the daemon's: drop it
		// whether or not the proxy checks it.
		t.dropAuthorization = true
	}
	if p.opts.Authenticate == nil {
		return true
	}
	var err error
	switch credential := r.Header.Get(EnvAuthorizationHeader); {
	case credential != "":
		err = p.opts.Authenticate(credential)
	case hasCapability:
		if p.opts.AuthenticateCapability == nil || !isWebSocketUpgrade(r) {
			err = errCapability
		} else {
			err = p.opts.AuthenticateCapability(capability, t.atespace, t.id, t.port)
		}
	default:
		t.dropAuthorization = true
		err = p.opts.Authenticate(r.Header.Get("Authorization"))
	}
	if err != nil {
		if status.Code(err) == codes.Internal {
			http.Error(w, status.Convert(err).Message(), http.StatusInternalServerError)
			return false
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "missing or invalid bearer token; send it in "+EnvAuthorizationHeader+
			" or Authorization", http.StatusUnauthorized)
		return false
	}
	return true
}

func (p *Proxy) rewrite(pr *httputil.ProxyRequest) {
	t := pr.In.Context().Value(targetKey{}).(target)
	// The transport dials with a context that does not end with the
	// request; dial reads the request's own context back from this value.
	pr.Out = pr.Out.WithContext(context.WithValue(pr.Out.Context(), callerKey{}, pr.Out.Context()))
	pr.SetXForwarded()
	// The URL host is the transport's pool key, not an address: one pool for
	// each environment and port. dial reads the target back from it.
	pr.Out.URL.Scheme = "http"
	pr.Out.URL.Host = net.JoinHostPort(t.id+"."+t.atespace, strconv.Itoa(t.port))
	// What a client inside the environment would send.
	pr.Out.Host = net.JoinHostPort("127.0.0.1", strconv.Itoa(t.port))
	if t.path != "" {
		setPath(pr.Out.URL, t.path, t.rawPath)
	}
	// ReverseProxy cleans RawQuery before Rewrite, which drops ';'
	// parameters and re-encodes on bad '%' escapes. The proxy never
	// inspects the query, so forward RawQuery as sent. ForceQuery (a bare
	// '?') is copied from the inbound request by ReverseProxy itself.
	pr.Out.URL.RawQuery = pr.In.URL.RawQuery
	for _, h := range []string{EnvIDHeader, EnvAtespaceHeader, EnvPortHeader, EnvAuthorizationHeader} {
		pr.Out.Header.Del(h)
	}
	if t.dropAuthorization {
		pr.Out.Header.Del("Authorization")
	}
}

// routerError is a CONNECT the router refused.
type routerError struct {
	status int
	msg    string
}

func (e *routerError) Error() string {
	return fmt.Sprintf("router answered CONNECT with %d: %s", e.status, e.msg)
}

// callerKey holds the context of the request a dial is for (see rewrite).
type callerKey struct{}

// routerParkMargin is how long past the wake budget the dial context lasts,
// beyond one retry interval: the attempt after the last wait can start at
// the end of the budget, and the router holds (parks) a CONNECT for a
// resuming actor before it answers, up to its --parked-request-budget, 5 s by
// default in Substrate v0.6.0 (DefaultParkedRequestBudget,
// cmd/atenet/internal/router/ingress/parking.go:29). The proxy does not know
// the router's flag, thus twice the default. A variable for the tests.
var routerParkMargin = 10 * time.Second

// dialContext returns the context for one dial and its cancel. Since Go 1.23
// http.Transport dials with context.WithoutCancel of the request context, so
// that a finished dial can serve another request; a wake loop would then
// keep sending CONNECTs for up to WakeBudget after its caller left. The
// returned context ends when the request ends, and at the latest one retry
// interval plus routerParkMargin after WakeBudget, so the last attempt gets
// its answer from a router that parks it. The CONNECT attempts and anything
// else the loop does with it stop then; dial reports that end as a spent
// wake budget (see stopped).
func (p *Proxy) dialContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, p.opts.WakeBudget+wakeRetryMaxBackoff+routerParkMargin)
	caller, ok := ctx.Value(callerKey{}).(context.Context)
	if !ok {
		return ctx, cancel
	}
	stopAfter := context.AfterFunc(caller, cancel)
	return ctx, func() { stopAfter(); cancel() }
}

// notWoken returns the error for a spent wake budget, last being the final
// attempt's: a router 503, which handleError answers with 503. Logged here:
// handleError keeps router 503s out of the log, and a spent wake budget is
// the one 503 an operator wants to see, as the close of the "waiting" line
// from the first attempt.
func (p *Proxy) notWoken(atespace, id string, attempts int, last error) error {
	var re *routerError
	if !errors.As(last, &re) {
		last = errors.Join(&routerError{status: http.StatusServiceUnavailable, msg: "the wake budget ended during the attempt"}, last)
	}
	err := fmt.Errorf("environment %s/%s did not wake within %s (%d attempts): %w", atespace, id, p.opts.WakeBudget, attempts, last)
	log.Printf("port proxy: %v", err)
	return err
}

// stopped returns the error for a dial whose context ended: the caller left,
// which needs no answer and no log line, or the dial deadline passed, which
// is a spent wake budget and gets the same 503.
func (p *Proxy) stopped(ctx context.Context, atespace, id string, attempts int, last error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return p.notWoken(atespace, id, attempts, last)
	}
	return ctx.Err()
}

// dial opens a CONNECT tunnel to the environment port encoded in addr (see
// rewrite). While the router refuses the CONNECT with 503, it tries again
// with backoff until WakeBudget is spent. Substrate v0.6.0's router refuses
// only at the Envoy level (overload, drain); it accepts the tunnel for any
// actor state and resumes the actor for the request inside it.
func (p *Proxy) dial(ctx context.Context, _, addr string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	id, atespace, _ := strings.Cut(host, ".")
	ctx, stop := p.dialContext(ctx)
	defer stop()
	deadline := time.Now().Add(p.opts.WakeBudget)
	backoff := wakeRetryInitialBackoff
	for attempt := 1; ; attempt++ {
		conn, err := p.connect(ctx, id, atespace, portStr)
		if err != nil && ctx.Err() != nil {
			return nil, p.stopped(ctx, atespace, id, attempt, err)
		}
		var re *routerError
		if !errors.As(err, &re) || re.status != http.StatusServiceUnavailable {
			return conn, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, p.notWoken(atespace, id, attempt, err)
		}
		if ctx.Err() != nil {
			return nil, p.stopped(ctx, atespace, id, attempt, err)
		}
		if attempt == 1 {
			log.Printf("port proxy: waiting for environment %s/%s to wake (wake budget %s)", atespace, id, p.opts.WakeBudget)
		}
		select {
		case <-ctx.Done():
			return nil, p.stopped(ctx, atespace, id, attempt, err)
		case <-time.After(min(backoff, remaining)):
		}
		backoff = min(backoff*2, wakeRetryMaxBackoff)
	}
}

// EnvStateHeader names the actor state on an answer for an environment that
// will not wake, so a client can stop without matching the body.
const EnvStateHeader = "X-Env-State"

// stateReadLogEvery bounds how often a failed state read is logged.
const stateReadLogEvery = 10 * time.Second

// stateReadTimeout bounds one state read, so a stalled control plane does
// not hold the answer. A variable for the tests.
var stateReadTimeout = 10 * time.Second

// stateCacheTTL is how long a state read answers for one environment: a
// burst of router errors for it costs one control-plane read.
const stateCacheTTL = 2 * time.Second

// stateEntry is one state read, shared by the answers that need it while it
// runs and for stateCacheTTL after. st, err and at are set before done is
// closed.
type stateEntry struct {
	done chan struct{}
	st   ateapipb.ActorState
	err  error
	at   time.Time
}

// modifyResponse turns the router's answers for an environment that is not
// serving into answers a client can act on. Substrate v0.6.0's router accepts
// every CONNECT and resumes the actor for the request inside the tunnel:
//
//   - a CRASHED or DELETING actor gets 503 "actor <atespace>/<id>
//     unavailable: ..." after the router's 5 s park, the same as a passing
//     state, and a deleted one 404 "actor ... not found", each a text/plain
//     ext_proc answer with a Content-Length;
//   - a wake that outlasts Envoy's ext_proc message timeout (the park budget
//     plus 5 s, 10 s by default) gets Envoy's own 504 with an empty body; the
//     wake goes on.
//
// For such an answer, and only for it, the proxy reads the actor state:
// CRASHED gets 410 Gone, DELETING 404, a deleted actor 404, and, for the
// empty 504 or 500, SUSPENDED or RESUMING gets 503 with Retry-After: 1, each
// with EnvStateHeader. Otherwise the answer passes unchanged: the daemon's own
// 503, 404, 504 or 500 included (a RUNNING actor), and when the read fails.
// The proxy never sends a request again. A refused WebSocket upgrade comes
// here too.
func (p *Proxy) modifyResponse(resp *http.Response) error {
	t, ok := resp.Request.Context().Value(targetKey{}).(target)
	if !ok {
		return nil
	}
	if p.opts.ActorState == nil {
		// Without a state read the proxy cannot tell the router's 5xx from
		// the daemon's; a lower status is the daemon's answer.
		if resp.StatusCode < http.StatusInternalServerError {
			p.connected(t)
		}
		return nil
	}
	head := resp.Request.Method == http.MethodHead
	switch resp.StatusCode {
	case http.StatusServiceUnavailable, http.StatusNotFound:
		if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
			p.connected(t)
			return nil
		}
		// HEAD has no body to match; status and content type decide, and
		// the state read confirms.
		if !head && !routerBody(resp, t) {
			p.connected(t)
			return nil
		}
	case http.StatusGatewayTimeout, http.StatusInternalServerError:
		// Envoy's own answer when the wake outlasts its ext_proc timeout:
		// no body. A daemon's error with a body is the daemon's.
		if !head && resp.ContentLength != 0 {
			p.connected(t)
			return nil
		}
	default:
		// The daemon answered, thus the environment is awake and serving.
		p.connected(t)
		return nil
	}
	st, err := p.actorState(resp.Request.Context(), t.atespace, t.id)
	switch {
	case status.Code(err) == codes.NotFound, errors.Is(err, ate.ErrNotFound):
		if resp.StatusCode != http.StatusNotFound {
			replaceAnswer(resp, t, http.StatusNotFound, "", "was not found")
		}
	case err != nil:
		if resp.Request.Context().Err() == nil { // not a caller that left
			p.logStateReadError(t.atespace, t.id, err)
		}
	case st == ateapipb.ActorState_ACTOR_STATE_CRASHED:
		replaceAnswer(resp, t, http.StatusGone, "CRASHED", "has crashed (actor state CRASHED); delete it and create a new one")
		p.crashed(t)
	case st == ateapipb.ActorState_ACTOR_STATE_DELETING:
		replaceAnswer(resp, t, http.StatusNotFound, "DELETING", "is being deleted (actor state DELETING)")
	case resp.StatusCode == http.StatusGatewayTimeout || resp.StatusCode == http.StatusInternalServerError:
		// A wake that ends after Envoy's timeout but before this read
		// reads RUNNING, and the empty 504 passes unchanged: the proxy
		// cannot tell it from a daemon's empty 504. The window is short,
		// and a retry then reaches the daemon.
		if name, waking := wakingStates[st]; waking {
			replaceAnswer(resp, t, http.StatusServiceUnavailable, name, "is still waking; retry")
			resp.Header.Set("Retry-After", "1")
		}
	}
	return nil
}

// connected reports an answer the daemon served to Options.OnConnected.
func (p *Proxy) connected(t target) {
	if p.opts.OnConnected != nil {
		p.opts.OnConnected(t.atespace, t.id)
	}
}

// crashed reports a CRASHED environment to Options.OnCrashed.
func (p *Proxy) crashed(t target) {
	if p.opts.OnCrashed != nil {
		p.opts.OnCrashed(t.atespace, t.id)
	}
}

// wakingStates are the states in which a timed-out wake goes on.
var wakingStates = map[ateapipb.ActorState]string{
	ateapipb.ActorState_ACTOR_STATE_SUSPENDED: "SUSPENDED",
	ateapipb.ActorState_ACTOR_STATE_RESUMING:  "RESUMING",
}

// routerBodyMax bounds the body read to match the router's text. The
// router's errors are short and carry a Content-Length; a body without one,
// such as a daemon's streamed answer, passes without a read, thus with no
// delay.
const routerBodyMax = 256

// routerBody reports whether resp's body starts the way the router's errors
// for this actor do (ingress/errors.go in Substrate v0.6.0): "actor
// <atespace>/<id> ...", or `actor "<atespace>/<id>" unavailable: router at
// capacity` (parkingFullErr gets actorRef.String(), ingress/resumer.go:301).
// It reads the body only when its length is known and at most
// routerBodyMax, and puts it back. A daemon's error does not match, so it
// costs no state read; one that copies the text costs one read and passes
// unchanged unless the actor really is crashed or deleting.
func routerBody(resp *http.Response, t target) bool {
	if resp.ContentLength <= 0 || resp.ContentLength > routerBodyMax {
		return false
	}
	buf := make([]byte, resp.ContentLength)
	n, _ := io.ReadFull(resp.Body, buf)
	buf = buf[:n]
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(buf), resp.Body), resp.Body}
	ref := t.atespace + "/" + t.id
	body := string(buf)
	return strings.HasPrefix(body, "actor "+ref+" ") || strings.HasPrefix(body, `actor "`+ref+`"`)
}

// replaceAnswer replaces resp with the proxy's own text answer.
func replaceAnswer(resp *http.Response, t target, code int, state, cause string) {
	resp.Body.Close()
	body := fmt.Sprintf("environment %s/%s port %d %s\n", t.atespace, t.id, t.port, cause)
	resp.StatusCode = code
	resp.Status = fmt.Sprintf("%d %s", code, http.StatusText(code))
	resp.Header = http.Header{}
	resp.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp.Header.Set("X-Content-Type-Options", "nosniff")
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	if state != "" {
		resp.Header.Set(EnvStateHeader, state)
	}
	resp.ContentLength = int64(len(body))
	resp.TransferEncoding = nil
	resp.Body = io.NopCloser(strings.NewReader(body))
}

// actorState reads the actor's state, at most once per stateCacheTTL for one
// environment: concurrent callers share the read in flight. The read gets
// its own stateReadTimeout and does not end with the caller that started it,
// so the callers that wait on it are not cut short.
func (p *Proxy) actorState(ctx context.Context, atespace, id string) (st ateapipb.ActorState, err error) {
	key := atespace + "/" + id
	p.stateMu.Lock()
	if e := p.states[key]; e != nil {
		select {
		case <-e.done:
			if time.Since(e.at) < stateCacheTTL {
				p.stateMu.Unlock()
				return e.st, e.err
			}
		default:
			p.stateMu.Unlock()
			select {
			case <-e.done:
				return e.st, e.err
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
	}
	e := &stateEntry{done: make(chan struct{})}
	if p.states == nil {
		p.states = make(map[string]*stateEntry)
	}
	if len(p.states) >= 1024 {
		for k, old := range p.states {
			select {
			case <-old.done:
				if time.Since(old.at) >= stateCacheTTL {
					delete(p.states, k)
				}
			default:
			}
		}
	}
	p.states[key] = e
	p.stateMu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			e.st, e.err = 0, fmt.Errorf("reading the state panicked: %v", r)
			p.logStatePanic(atespace, id, r, debug.Stack())
		}
		e.at = time.Now()
		close(e.done) // never leave the entry in flight
		st, err = e.st, e.err
	}()
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stateReadTimeout)
	defer cancel()
	e.st, e.err = p.opts.ActorState(rctx, atespace, id)
	return e.st, e.err
}

// logStatePanic logs a panic in the state read with its stack, under its
// own limit, so an earlier ordinary failure cannot hide it.
func (p *Proxy) logStatePanic(atespace, id string, r any, stack []byte) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if now := time.Now(); now.Sub(p.statePanicAt) >= stateReadLogEvery {
		p.statePanicAt = now
		log.Printf("port proxy: reading the state of environment %s/%s panicked: %v\n%s", atespace, id, r, stack)
	}
}

func (p *Proxy) logStateReadError(atespace, id string, err error) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if now := time.Now(); now.Sub(p.stateLogAt) >= stateReadLogEvery {
		p.stateLogAt = now
		log.Printf("port proxy: reading the state of environment %s/%s failed; its router error passes unchanged: %v", atespace, id, err)
	}
}

// connect sends one CONNECT for the environment port and returns the tunnel.
func (p *Proxy) connect(ctx context.Context, id, atespace, portStr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, p.opts.HandshakeTimeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", p.opts.ConnectAddr)
	if err != nil {
		return nil, fmt.Errorf("dialing the router's CONNECT listener %s: %w", p.opts.ConnectAddr, err)
	}
	if p.opts.TLS != nil {
		tc := tls.Client(conn, p.opts.TLS.Clone())
		if err := tc.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("TLS handshake with the router's CONNECT listener %s: %w", p.opts.ConnectAddr, err)
		}
		conn = tc
	}
	// A context that ends mid-handshake unblocks the write or read below.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })

	authority := net.JoinHostPort(id+"."+p.opts.HostSuffix, portStr)
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: authority},
		Host:   authority,
		Header: http.Header{ate.TargetActorHeader: {atespace + "/" + id}},
	}
	br := bufio.NewReader(conn)
	resp, err := func() (*http.Response, error) {
		if err := req.Write(conn); err != nil {
			return nil, err
		}
		return http.ReadResponse(br, req)
	}()
	if !stop() {
		err = errors.Join(ctx.Err(), err)
	}
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("CONNECT to %s/%s port %s: %w", atespace, id, portStr, err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		conn.Close()
		return nil, &routerError{status: resp.StatusCode, msg: strings.TrimSpace(string(body))}
	}
	if br.Buffered() > 0 {
		return &bufferedConn{Conn: conn, r: br}, nil
	}
	return conn, nil
}

// bufferedConn reads what the handshake reader buffered past the CONNECT
// response before it reads the connection again.
type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(b []byte) (int, error) { return c.r.Read(b) }

// CloseWrite half-closes the tunnel when the connection can: after an
// upgrade, httputil.ReverseProxy half-closes the backend once the client
// side is done, and embedding net.Conn would otherwise hide the method.
func (c *bufferedConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// handleError answers a failed forward. The body names the target and a
// short cause; the full error, which can name the router's address, goes to
// the log only.
func (p *Proxy) handleError(w http.ResponseWriter, r *http.Request, err error) {
	t, _ := r.Context().Value(targetKey{}).(target)
	var re *routerError
	var ne net.Error
	status, cause := http.StatusBadGateway, "is unreachable; the ate-env-api log has the cause"
	switch {
	case errors.Is(err, context.Canceled):
		return // the caller went away; nobody reads an answer
	case routertls.RejectedCertificate(err):
		cause = "is behind a router that rejected ate-env-api's client certificate; " + routertls.RejectionAdvice
	case errors.As(err, &re):
		// 404 (no such environment) and 503 (not running) mean the same to
		// the caller as to the router; anything else is a gateway failure.
		switch re.status {
		case http.StatusNotFound:
			status, cause = re.status, "was not found"
		case http.StatusServiceUnavailable:
			status, cause = re.status, "is not available (not running, or still waking)"
		case http.StatusBadGateway:
			cause = "refused the connection; a daemon must listen on 0.0.0.0, not only on 127.0.0.1"
		}
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		status, cause = http.StatusGatewayTimeout, "did not answer in time"
	}
	// A 404 or 503 from the router is the caller's answer, not an operator's
	// problem; a log line for each would flood the log when a fleet wakes.
	if status != http.StatusNotFound && status != http.StatusServiceUnavailable {
		log.Printf("port proxy: %s/%s port %d: %v", t.atespace, t.id, t.port, err)
	}
	http.Error(w, fmt.Sprintf("environment %s/%s port %d %s", t.atespace, t.id, t.port, cause), status)
}

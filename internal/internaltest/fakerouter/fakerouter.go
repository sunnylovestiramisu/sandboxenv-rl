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

// Package fakerouter implements a test double for the atenet router: it
// resolves the target environment from the request's ate-target-actor header
// (or, failing that, its Host header) and forwards to that environment's guest
// handler, returning 404 for an invalid or duplicated actor reference and 503
// when the environment is not running.
//
// CONNECT tunnels behave like Substrate v0.6.0's router: Envoy terminates
// each CONNECT that names a port with 200 and runs no ext_proc on it
// (cmd/atenet/internal/router/xds.go, buildConnectTerminateHCM). The resume
// and its errors apply to each request inside the tunnel, which gets the
// router's text/plain answer: 503 "actor <ref> unavailable" for an actor that
// is not running, or what Deny returns, such as the 503 for a CRASHED actor
// or the 404 for a deleted one (ingress.go and errors.go there). Only
// RefuseConnect, standing in for an Envoy-level refusal such as overload,
// answers the CONNECT itself.
package fakerouter

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/env/internal/ate"
)

// Router is a fake atenet router.
type Router struct {
	// Running reports whether the actor with the given ID is currently
	// running. Non-running actors get a 503.
	Running func(id string) bool
	// Deny, when set, returns the router's answer for a request inside a
	// CONNECT tunnel to the actor, such as 503 with the FailedPrecondition
	// text for a CRASHED actor or 404 "actor <ref> not found"; code 0 lets
	// the request through. It applies before Running.
	Deny func(id string) (code int, body string)
	// NotRunningBody, when set, is the router's text for a gRPC request to
	// an actor that is not running, such as "actor <ref> unavailable: no
	// worker has room for the actor" while the scheduler cannot place it.
	// The answer is what Envoy makes of the router's 503 for a gRPC caller:
	// a trailers-only response with grpc-status UNAVAILABLE and the text as
	// grpc-message. Without it the request gets a plain-text 503.
	NotRunningBody func(id string) string
	// EnvoyTimeout, when set, makes a request inside a tunnel to an actor
	// that is not running wait like the real router's resume: it is served
	// if the actor runs within EnvoyTimeout, else it gets Envoy's own 504
	// with an empty body (the ext_proc message timeout, 10 s by default in
	// Substrate v0.6.0). Unset, such a request gets 503 "actor <ref>
	// unavailable" at once.
	EnvoyTimeout time.Duration
	// RefuseConnect, when set, answers the CONNECT itself when it returns a
	// non-zero code. The real router does that only at the Envoy level
	// (overload, drain), never for an actor state.
	RefuseConnect func(id string) (code int, body string)

	mu     sync.Mutex
	guests map[string]http.Handler
	ports  map[portKey]http.Handler
	// open and total count the requests forwarded to guest handlers, by
	// URL path (the gRPC method): in flight, and since New.
	open, total map[string]int
}

type portKey struct {
	id   string
	port int
}

// New returns a Router that considers every actor running unless a Running
// callback is set.
func New() *Router {
	return &Router{
		guests: make(map[string]http.Handler),
		ports:  make(map[portKey]http.Handler),
		open:   make(map[string]int),
		total:  make(map[string]int),
	}
}

// GuestRequests reports the requests for path (a gRPC method such as
// "/ateenv.v1alpha.ProcessService/StreamProcessOutput") that the router has
// forwarded to guest handlers: open is how many are in flight now, total how
// many there have been. A gRPC stream is one request for its whole life.
func (r *Router) GuestRequests(path string) (open, total int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.open[path], r.total[path]
}

// serveGuest forwards req to a guest handler and counts it while it runs.
func (r *Router) serveGuest(h http.Handler, w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	r.mu.Lock()
	r.open[path]++
	r.total[path]++
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.open[path]--
		r.mu.Unlock()
	}()
	h.ServeHTTP(w, req)
}

// RegisterPort installs the handler that serves CONNECT tunnels to port on
// environment id, standing in for a daemon listening there.
func (r *Router) RegisterPort(id string, port int, h http.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ports[portKey{id, port}] = h
}

// Register installs the guest handler serving a environment ID.
func (r *Router) Register(id string, h http.Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.guests[id] = h
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	id, ok := actorID(req)
	if req.Method == http.MethodConnect {
		r.serveConnect(w, req, id, ok)
		return
	}
	if !ok {
		http.Error(w, "invalid actor reference", http.StatusNotFound)
		return
	}
	r.mu.Lock()
	guest := r.guests[id]
	r.mu.Unlock()
	if guest == nil {
		http.Error(w, "no actor "+id, http.StatusNotFound)
		return
	}
	if r.Running != nil && !r.Running(id) {
		if r.NotRunningBody != nil && strings.HasPrefix(req.Header.Get("Content-Type"), "application/grpc") {
			grpcUnavailable(w, r.NotRunningBody(id))
			return
		}
		http.Error(w, "actor "+id+" is not running", http.StatusServiceUnavailable)
		return
	}
	r.serveGuest(guest, w, req)
}

func actorID(req *http.Request) (string, bool) {
	// Like the real router since agent-substrate/substrate#2308: a request
	// carrying ate-target-actor more than once is an invalid actor reference,
	// even when every copy agrees.
	values := req.Header.Values(ate.TargetActorHeader)
	if len(values) > 1 {
		return "", false
	}
	if len(values) == 1 && values[0] != "" {
		_, actor, ok := strings.Cut(values[0], "/")
		return actor, ok && actor != ""
	}
	if !strings.HasSuffix(req.Host, "."+ate.DefaultHostSuffix) {
		return "", false
	}
	id, _, ok := strings.Cut(req.Host, ".")
	return id, ok && id != ""
}

// Serve starts the router on a random localhost port and returns its
// address and a shutdown function.
func (r *Router) Serve() (addr string, stop func()) {
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := httptest.NewUnstartedServer(r)
	srv.Config.Protocols = &protocols
	srv.Start()
	return strings.TrimPrefix(srv.URL, "http://"), srv.Close
}

// ServeTLS serves the router over TLS with cfg, HTTP/1.1 and HTTP/2, the way
// Substrate v0.6.0's router does: set cfg.ClientAuth and cfg.ClientCAs to
// require a client certificate. It returns the host:port and a stop function.
func (r *Router) ServeTLS(cfg *tls.Config) (addr string, stop func()) {
	srv := httptest.NewUnstartedServer(r)
	srv.TLS = cfg
	srv.EnableHTTP2 = true
	srv.StartTLS()
	return strings.TrimPrefix(srv.URL, "https://"), srv.Close
}

// serveConnect answers a CONNECT like the real router: 200 for any actor
// once the authority names a port, then each request inside the tunnel is
// resolved like ext_proc resolves it (see serveTunneled).
func (r *Router) serveConnect(w http.ResponseWriter, req *http.Request, id string, validRef bool) {
	if r.RefuseConnect != nil {
		if code, body := r.RefuseConnect(id); code != 0 {
			routerError(w, code, body)
			return
		}
	}
	_, portStr, err := net.SplitHostPort(req.Host)
	port, perr := strconv.Atoi(portStr)
	if err != nil || perr != nil {
		http.Error(w, "CONNECT authority names no port", http.StatusBadRequest)
		return
	}
	ref := req.Header.Get(ate.TargetActorHeader)
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		http.Error(w, "hijack: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n")); err != nil {
		conn.Close()
		return
	}
	inner := http.HandlerFunc(func(w http.ResponseWriter, in *http.Request) {
		r.serveTunneled(w, in, ref, id, validRef, port)
	})
	l := &oneConnListener{conn: &tunnelConn{Conn: conn, r: rw.Reader}, done: make(chan struct{})}
	_ = (&http.Server{Handler: inner}).Serve(l)
}

// serveTunneled answers one request inside a CONNECT tunnel the way the real
// router's ext_proc does: an invalid reference is 404, a denial or an actor
// that is not running gets the router's text/plain error, a running actor
// gets the daemon on the port, and no daemon there is 502.
func (r *Router) serveTunneled(w http.ResponseWriter, req *http.Request, ref, id string, validRef bool, port int) {
	if !validRef {
		routerError(w, http.StatusNotFound, "invalid actor reference")
		return
	}
	if r.Deny != nil {
		if code, body := r.Deny(id); code != 0 {
			routerError(w, code, body)
			return
		}
	}
	r.mu.Lock()
	h := r.ports[portKey{id, port}]
	r.mu.Unlock()
	if h == nil && !r.known(id) {
		routerError(w, http.StatusNotFound, "actor "+ref+" not found")
		return
	}
	if r.Running != nil && !r.Running(id) && !r.waitRunning(req, id) {
		if r.EnvoyTimeout > 0 {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusGatewayTimeout)
			return
		}
		routerError(w, http.StatusServiceUnavailable, "actor "+ref+" unavailable")
		return
	}
	if h == nil {
		http.Error(w, "connection refused", http.StatusBadGateway)
		return
	}
	h.ServeHTTP(w, req)
}

// waitRunning holds req up to EnvoyTimeout for the actor to run, and reports
// whether it does. Without EnvoyTimeout it does not wait.
func (r *Router) waitRunning(req *http.Request, id string) bool {
	if r.EnvoyTimeout <= 0 {
		return false
	}
	deadline := time.Now().Add(r.EnvoyTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-req.Context().Done():
			return false
		case <-time.After(10 * time.Millisecond):
		}
		if r.Running(id) {
			return true
		}
	}
	return false
}

// known reports whether any handler is registered for the actor.
func (r *Router) known(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.guests[id] != nil {
		return true
	}
	for k := range r.ports {
		if k.id == id {
			return true
		}
	}
	return false
}

// routerError writes an answer the way the router's ext_proc immediate
// response does: the status, content-type text/plain, the message as body.
func routerError(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body)
}

// tunnelConn reads what the hijacked reader buffered before the connection.
type tunnelConn struct {
	net.Conn
	r       *bufio.Reader
	closeFn func()
}

func (c *tunnelConn) Read(b []byte) (int, error) { return c.r.Read(b) }

func (c *tunnelConn) Close() error {
	if c.closeFn != nil {
		c.closeFn()
	}
	return c.Conn.Close()
}

// oneConnListener hands out one connection, then blocks until it is closed.
type oneConnListener struct {
	conn   *tunnelConn
	once   sync.Once
	closed sync.Once
	done   chan struct{}
}

func (l *oneConnListener) Accept() (net.Conn, error) {
	var c net.Conn
	l.once.Do(func() {
		l.conn.closeFn = func() { l.closed.Do(func() { close(l.done) }) }
		c = l.conn
	})
	if c != nil {
		return c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *oneConnListener) Close() error {
	l.closed.Do(func() { close(l.done) })
	return nil
}
func (l *oneConnListener) Addr() net.Addr { return l.conn.LocalAddr() }

// grpcUnavailable answers a gRPC request the way Envoy converts a local
// reply for one: HTTP 200 with a trailers-only gRPC status, UNAVAILABLE (14),
// and msg percent-encoded as grpc-message.
func grpcUnavailable(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Grpc-Status", "14")
	w.Header().Set("Grpc-Message", url.PathEscape(msg))
	w.WriteHeader(http.StatusOK)
}

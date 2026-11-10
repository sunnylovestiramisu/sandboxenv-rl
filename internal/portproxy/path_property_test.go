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

package portproxy_test

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// pathRestGen draws a path suffix ("/seg/seg...", at times with a query) as
// a client may write it on the wire: unreserved, sub-delims, ':' '@',
// percent-escapes (an escaped slash among them), and bytes Go's server
// accepts though url.EscapedPath calls them invalid in a raw path ({ } | ^
// " < > \ ` # and raw UTF-8). An empty first segment makes a path that starts
// with "//". Queries also draw bare '?', ';', '&', '/', '?', and bad '%'
// escapes that Go's server accepts in RawQuery.
func pathRestGen() *rapid.Generator[string] {
	atom := rapid.OneOf(
		rapid.SampledFrom([]string{"a", "b", "z", "0", "-", "_", ".", "~"}),
		rapid.SampledFrom([]string{"!", "$", "&", "'", "(", ")", "*", "+", ",", ";", "=", ":", "@", "[", "]"}),
		rapid.SampledFrom([]string{"%20", "%2F", "%2f", "%41", "%21", "%25", "%3F", "%7B"}),
		rapid.SampledFrom([]string{"{", "}", "|", "^", "\"", "<", ">", "\\", "`", "#", "\xc3\xa9", "\xe2\x82\xac"}),
	)
	seg := rapid.Custom(func(t *rapid.T) string {
		return strings.Join(rapid.SliceOfN(atom, 0, 6).Draw(t, "atoms"), "")
	})
	queryAtom := rapid.OneOf(
		atom,
		rapid.SampledFrom([]string{"/", "?", ";", "&", "=", "%", "%2", "%ZZ"}),
	)
	query := rapid.Custom(func(t *rapid.T) string {
		return strings.Join(rapid.SliceOfN(queryAtom, 0, 8).Draw(t, "queryAtoms"), "")
	})
	return rapid.Custom(func(t *rapid.T) string {
		rest := "/" + strings.Join(rapid.SliceOfN(seg, 1, 4).Draw(t, "segs"), "/")
		if rapid.Bool().Draw(t, "hasQuery") {
			rest += "?" + query.Draw(t, "query")
		}
		return rest
	})
}

// sameSegments reports whether got and want have the same segments, each
// decoding to the same bytes, and the same query (including a bare '?').
func sameSegments(got, want string) bool {
	gp, gq, gHasQ := strings.Cut(got, "?")
	wp, wq, wHasQ := strings.Cut(want, "?")
	g, w := strings.Split(gp, "/"), strings.Split(wp, "/")
	if len(g) != len(w) || gHasQ != wHasQ || gq != wq {
		return false
	}
	for i := range g {
		gs, err1 := url.PathUnescape(g[i])
		ws, err2 := url.PathUnescape(w[i])
		if err1 != nil || err2 != nil || gs != ws {
			return false
		}
	}
	return true
}

// PP-1, PP-2 (docs/plans/port-proxy/acceptance-criteria.md). Rule (Dispatch
// doc; TestPathFormKeepsPathAsSent, TestRawBytesKeptAsSent,
// TestRawQueryKeptAsSent): the daemon gets the path after the routing prefix,
// the header form's path, and the query of the request-target, exactly as
// sent. Each request line is written on a raw TCP connection, so no client
// escapes it. A path that starts with "//" cannot be written as is (it would
// read as an authority); for it the daemon must get the same segments, each
// decoding to the same bytes, and the query byte for byte. The routing prefix
// is matched on the path as sent: with one of its separators escaped (%2F),
// the request is not a path form and reaches no daemon, whatever the rest
// holds.
func TestPathFormDeliversPathAsSentProperty(t *testing.T) {
	s, base := pathSetup(t)
	var got string
	s.router.RegisterPort("env1", 8000, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.RequestURI
		io.WriteString(w, "daemon")
	}))
	send := func(t *rapid.T, target, extra string) string {
		got = ""
		if code, body := sendRaw(t, base, target, extra); body != "daemon" {
			t.Fatalf("GET %q: %d %q", target, code, body)
		}
		return got
	}
	rapid.Check(t, func(rt *rapid.T) {
		rest := pathRestGen().Draw(rt, "rest")
		ok := func(got string) bool { return got == rest }
		if strings.HasPrefix(rest, "//") {
			ok = func(got string) bool { return sameSegments(got, rest) }
		}
		viaPath := send(rt, "/v1alpha/envs/env1/ports/8000"+rest, "")
		viaHeader := send(rt, rest, headerForm)
		if !ok(viaPath) || !ok(viaHeader) {
			rt.Fatalf("sent %q: path form delivered %q, header form %q", rest, viaPath, viaHeader)
		}
		if strings.HasPrefix(rest, "/?") {
			viaBarePrefix := send(rt, "/v1alpha/envs/env1/ports/8000"+rest[1:], "")
			if viaBarePrefix != rest {
				rt.Fatalf("sent bare prefix %q: delivered %q, want %q", rest[1:], viaBarePrefix, rest)
			}
		}
		segs := []string{"", "v1alpha", "envs", "env1", "ports", "8000"}
		i := rapid.IntRange(1, len(segs)-2).Draw(rt, "escapedSeparator")
		esc := rapid.SampledFrom([]string{"%2F", "%2f"}).Draw(rt, "escapedSlash")
		mangled := strings.Join(segs[:i+1], "/") + esc + strings.Join(segs[i+1:], "/") + rest
		got = ""
		if code, body := sendRaw(rt, base, mangled, ""); body == "daemon" || got != "" {
			rt.Fatalf("GET %q: %d %q, daemon got %q; an escaped slash must not end a routing segment", mangled, code, body, got)
		}
	})
}

// Deterministic reduction of the counterexample.
func TestPathFormKeepsSubDelimsAsSent(t *testing.T) {
	s, base := pathSetup(t)
	var got string
	s.router.RegisterPort("env1", 8000, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.RequestURI
	}))
	for _, rest := range []string{"/!", "/files/report(1).txt", "/a*b", "/it's"} {
		resp, err := http.Get(base + "/v1alpha/envs/env1/ports/8000" + rest)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got != rest {
			t.Errorf("sent %q after the prefix, daemon got %q", rest, got)
		}
	}
}

// sendRaw writes a GET for target on a fresh TCP connection, so no client
// escapes or cleans the request-target, and returns the status and body.
func sendRaw(t interface {
	Helper()
	Fatal(...any)
}, base, target, extra string) (int, string) {
	t.Helper()
	host := strings.TrimPrefix(base, "http://")
	c, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	io.WriteString(c, "GET "+target+" HTTP/1.1\r\nHost: "+host+"\r\n"+extra+"Connection: close\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, string(b)
}

const headerForm = "X-Env-Id: env1\r\nX-Env-Port: 8000\r\n"

// PP-1: bytes Go's server accepts in a request-target but url.EscapedPath
// escapes ({ } | ^, raw UTF-8) reach the daemon as sent in both forms, and an
// escaped slash next to them stays escaped. A path that starts with "//"
// cannot be written as is (it would read as an authority): only those bytes
// are escaped, and the segments stay.
func TestRawBytesKeptAsSent(t *testing.T) {
	s, base := pathSetup(t)
	var got, host string
	s.router.RegisterPort("env1", 8000, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, host = r.RequestURI, r.Host
		io.WriteString(w, "daemon")
	}))
	for _, c := range []struct{ rest, want string }{
		{"/a{b}", "/a{b}"},
		{"/a|b", "/a|b"},
		{"/a^b", "/a^b"},
		{"/caf\xc3\xa9", "/caf\xc3\xa9"},
		{"/a{b%2Fc", "/a{b%2Fc"},
		{"/a\"b<c>d\\e`f", "/a\"b<c>d\\e`f"},
		{"/a{b?x={y}", "/a{b?x={y}"},
		{"//a{b%2Fc", "//a%7Bb%2Fc"},
		{"//evil.example/x", "//evil.example/x"},
		{"/a#b%2Fc", "/a#b%2Fc"},
		{"//a#b%2Fc", "//a%23b%2Fc"},
	} {
		for _, form := range []struct{ name, target, extra string }{
			{"path form", "/v1alpha/envs/env1/ports/8000" + c.rest, ""},
			{"header form", c.rest, headerForm},
		} {
			got, host = "", ""
			code, body := sendRaw(t, base, form.target, form.extra)
			if code != http.StatusOK || body != "daemon" || got != c.want || host != "127.0.0.1:8000" {
				t.Errorf("%s: sent %q, got %d %q, daemon got %q (Host %q), want %q", form.name, c.rest, code, body, got, host, c.want)
			}
		}
	}
}

// The routing prefix is matched on the path as sent: an escaped slash or a
// stray byte inside it does not make a path form, and does not reach the
// daemon.
func TestRawBytesDoNotMoveRoutingPrefix(t *testing.T) {
	s, base := pathSetup(t)
	s.router.RegisterPort("env1", 8000, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "daemon")
	}))
	for _, c := range []struct {
		target string
		code   int
		body   string
	}{
		{"/v1alpha%2Fenvs%2Fenv1%2Fports%2F8000/a{", http.StatusOK, "api"},
		{"/v1alpha/envs%2Fenv1/ports/8000/a{", http.StatusOK, "api"},
		{"/v1alpha/envs/env1%2Fports%2F8000/ports/1/a{", http.StatusBadRequest, ""},
		{"/v1alpha/envs/env1{/ports/8000/a", http.StatusBadRequest, ""},
		{"/v1alpha/envs/env1/ports/8000{/a", http.StatusBadRequest, ""},
		{"/v1alpha/envs/env1/ports/8000%2F{", http.StatusBadRequest, ""},
		{"/v1alpha/envs/caf\xc3\xa9/ports/8000/a", http.StatusBadRequest, ""},
	} {
		code, body := sendRaw(t, base, c.target, "")
		if code != c.code || (c.body != "" && body != c.body) || body == "daemon" {
			t.Errorf("GET %q: got %d %q, want %d %q", c.target, code, body, c.code, c.body)
		}
	}
}

// PP-2: the query of the request-target reaches the daemon byte for byte in
// both forms, including bare '?', ';' parameters, bad '%' escapes, '{ } | ^',
// '#', raw UTF-8, '//' paths, and a bare path-form prefix without a slash
// before '?'.
func TestRawQueryKeptAsSent(t *testing.T) {
	s, base := pathSetup(t)
	var got, host string
	s.router.RegisterPort("env1", 8000, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, host = r.RequestURI, r.Host
		io.WriteString(w, "daemon")
	}))
	for _, c := range []struct{ rest, want string }{
		{"/?", "/?"},
		{"/x?", "/x?"},
		{"/?;", "/?;"},
		{"/x?a=1;b=2", "/x?a=1;b=2"},
		{"/?a=1&b;c&d=2", "/?a=1&b;c&d=2"},
		{"/?z=1&a=2%203&semi;drop", "/?z=1&a=2%203&semi;drop"},
		{"/x?%", "/x?%"},
		{"/x?a=1&bad=%ZZ&b=2", "/x?a=1&bad=%ZZ&b=2"},
		{"/x?p=%2F&q={a}|b^c&u=caf\xc3\xa9#frag", "/x?p=%2F&q={a}|b^c&u=caf\xc3\xa9#frag"},
		{"//double-slash?", "//double-slash?"},
		{"//double-slash?;", "//double-slash?;"},
		{"//a{b%2Fc?a=1;b=2&q={x}|y^z&u=\xc3\xa9&bad=%ZZ", "//a%7Bb%2Fc?a=1;b=2&q={x}|y^z&u=\xc3\xa9&bad=%ZZ"},
	} {
		for _, form := range []struct{ name, target, extra string }{
			{"path form", "/v1alpha/envs/env1/ports/8000" + c.rest, ""},
			{"header form", c.rest, headerForm},
		} {
			got, host = "", ""
			code, body := sendRaw(t, base, form.target, form.extra)
			if code != http.StatusOK || body != "daemon" || got != c.want || host != "127.0.0.1:8000" {
				t.Errorf("%s: sent %q, got %d %q, daemon got %q (Host %q), want %q", form.name, c.rest, code, body, got, host, c.want)
			}
		}
	}
	for _, c := range []struct{ target, want string }{
		{"/v1alpha/envs/env1/ports/8000?", "/?"},
		{"/v1alpha/envs/env1/ports/8000?;", "/?;"},
		{"/v1alpha/atespaces/ate-env/envs/env1/ports/8000?a=1;b=2", "/?a=1;b=2"},
	} {
		got, host = "", ""
		code, body := sendRaw(t, base, c.target, "")
		if code != http.StatusOK || body != "daemon" || got != c.want || host != "127.0.0.1:8000" {
			t.Errorf("GET %q: got %d %q, daemon got %q (Host %q), want %q", c.target, code, body, got, host, c.want)
		}
	}
}

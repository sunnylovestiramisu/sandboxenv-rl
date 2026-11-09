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

package fakerouter

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/ate"
)

func TestServeHTTPTargetActorHeader(t *testing.T) {
	r := New()
	r.Register("a", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name   string
		values []string
		want   int
	}{
		{"single", []string{"ns/a"}, http.StatusNoContent},
		{"duplicate identical", []string{"ns/a", "ns/a"}, http.StatusNotFound},
		{"duplicate different", []string{"ns/a", "ns/b"}, http.StatusNotFound},
		{"empty actor", []string{"ns/"}, http.StatusNotFound},
		{"unknown actor", []string{"ns/zzz"}, http.StatusNotFound},
		{"absent", nil, http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://router.test/", nil)
			for _, v := range tc.values {
				req.Header.Add(ate.TargetActorHeader, v)
			}
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d (body %q)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// GuestRequests counts a guest request while its handler runs and after.
func TestGuestRequests(t *testing.T) {
	const path = "/ateenv.v1alpha.ProcessService/StreamProcessOutput"
	r := New()
	entered, release := make(chan struct{}), make(chan struct{})
	r.Register("a", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		req := httptest.NewRequest(http.MethodPost, "http://router.test"+path, nil)
		req.Header.Set(ate.TargetActorHeader, "ns/a")
		r.ServeHTTP(httptest.NewRecorder(), req)
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the guest handler was not called")
	}
	if open, total := r.GuestRequests(path); open != 1 || total != 1 {
		t.Errorf("while the handler runs: open %d, total %d; want 1, 1", open, total)
	}
	close(release)
	<-done
	if open, total := r.GuestRequests(path); open != 0 || total != 1 {
		t.Errorf("after the handler returned: open %d, total %d; want 0, 1", open, total)
	}
	if open, total := r.GuestRequests("/other"); open != 0 || total != 0 {
		t.Errorf("another path: open %d, total %d; want 0, 0", open, total)
	}
}

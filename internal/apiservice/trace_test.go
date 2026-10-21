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

package apiservice_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

// Guest calls reach the router with the caller's traceparent: the router's
// wake-on-request resume then continues the caller's trace.
func TestGuestCallsPropagateTraceContextToRouter(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	te := newFullTestEnv(t)
	var mu sync.Mutex
	var seen []string
	te.router.Seen = func(req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, req.Header.Get("traceparent"))
	}

	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true,
	}))
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-env-id", "sb-trace", "x-env-atespace", "default"))

	// The environment does not exist; the call fails after the wake probe,
	// which is the request that would trigger a resume.
	_, _ = te.srv.StartProcess(ctx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}})

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("router saw no request")
	}
	for _, tp := range seen {
		if !strings.Contains(tp, traceID.String()) {
			t.Fatalf("router request traceparent = %q, want trace id %s", tp, traceID)
		}
	}
}

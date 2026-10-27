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

// P-20 (properties.md): for any sequence of client calls and any
// TraceServiceName, with a provider there is exactly one client span per call,
// named {svc}.{op}, with only documented attribute keys; without a provider no
// span is recorded. Validates: AC-11.1, AC-11.2. Property-based with testing/quick.

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"testing/quick"

	"github.com/agent-substrate/env/clients/go"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var documentedGoAttrs = map[string]bool{
	"ate.actor.name": true, "ate.atespace": true, "ate.template.name": true,
	"ate.template.atespace": true, "ate_env.run_id": true, "ate_env.rootfs_image": true,
	"ate_env.command.executable": true, "ate_env.exit_code": true, "ate_env.timed_out": true,
	"ate_env.file.path": true, "ate_env.file.size": true, "ate_env.first_command": true,
}

func TestPBT_P20_SpansFollowProvider(t *testing.T) {
	f := newTracingFixture(t)
	n := 0
	prop := func(seed int64, withProvider bool, svcPick uint8) bool {
		r := rand.New(rand.NewSource(seed))
		n++
		id := fmt.Sprintf("pbt-%d", n)
		f.serveGuest(t, id)
		svc := []string{"", "my-trainer", "x", "rl-loop-2"}[int(svcPick)%4]
		want := svc
		if want == "" {
			want = env.DefaultTraceServiceName
		}
		rec := tracetest.NewSpanRecorder()
		global := tracetest.NewSpanRecorder()
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(global)))
		opts := env.ClientOptions{TraceServiceName: svc}
		if withProvider {
			opts.TracerProvider = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
		}
		c := f.client(t, opts)
		ctx := t.Context()
		e, err := c.Create(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: id})
		if err != nil {
			t.Logf("Create: %v", err)
			return false
		}
		calls := map[string]int{"create_environment": 1}
		for i := 0; i < r.Intn(6); i++ {
			switch r.Intn(3) {
			case 0:
				if _, err := c.Get(ctx, "", id); err == nil {
					calls["get_environment"]++
				}
			case 1:
				if _, err := e.Shell(ctx, "/bin/echo hi"); err == nil {
					calls["exec"]++
				}
			case 2:
				if err := e.WriteFile(ctx, "f.txt", strings.NewReader("x"), 0o644); err == nil {
					calls["write_file"]++
				}
			}
		}
		if err := e.Delete(ctx); err == nil {
			calls["delete_environment"]++
		}
		got := map[string]int{}
		for _, s := range rec.Ended() {
			if !strings.HasPrefix(s.Name(), want+".") {
				continue // otelgrpc RPC spans under the client's spans
			}
			got[strings.TrimPrefix(s.Name(), want+".")]++
			for _, kv := range s.Attributes() {
				if !documentedGoAttrs[string(kv.Key)] {
					t.Logf("undocumented attribute %q on %s", kv.Key, s.Name())
					return false
				}
			}
		}
		if !withProvider {
			if len(rec.Ended()) != 0 || len(global.Ended()) != 0 {
				t.Logf("spans recorded without a provider: %d local, %d global",
					len(rec.Ended()), len(global.Ended()))
				return false
			}
			return true
		}
		for op, k := range calls {
			if got[op] != k {
				t.Logf("svc=%q op %s: %d spans for %d calls (all: %v)", want, op, got[op], k, got)
				return false
			}
		}
		return len(got) == len(calls)
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 25}); err != nil {
		t.Error(err)
	}
}

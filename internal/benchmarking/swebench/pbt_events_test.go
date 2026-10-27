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

package swebench

// P-19 (properties.md): for any event sequence, the recorder's JSONL is
// identical with and without a tracer, and every event yields exactly one span
// named after it, with documented attributes and status from OK.
// Validates: AC-10.2, AC-10.3. Property-based with testing/quick (stdlib).
// testing/quick does not shrink; a failure prints the full generated input.

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type eventSeq []Event

var pbtEventNames = []string{EventCreate, EventTTFI, EventWake, EventStep, EventTask,
	EventSuspend, EventDelete, EventBarrier, EventPark, EventRun}

// Generate implements quick.Generator: a random, well-formed event sequence.
func (eventSeq) Generate(r *rand.Rand, size int) reflect.Value {
	n := r.Intn(size + 1)
	base := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	seq := make(eventSeq, n)
	for i := range seq {
		e := Event{
			Time:  base.Add(time.Duration(r.Intn(1e6)) * time.Millisecond),
			RunID: []string{"r1", "r2"}[r.Intn(2)],
			Event: pbtEventNames[r.Intn(len(pbtEventNames))],
			Env:   []string{"", "sb-1", "sb-2"}[r.Intn(3)],
			Step:  r.Intn(4),
			Task:  r.Intn(4),
			DurMS: float64(r.Intn(100000)) / 10,
			OK:    r.Intn(3) != 0,
		}
		if r.Intn(2) == 0 {
			e.Instance = "django__django-1"
		}
		if r.Intn(3) == 0 {
			e.Name = "edit"
		}
		if r.Intn(3) == 0 {
			x := r.Intn(3)
			e.Exit = &x
		}
		if r.Intn(4) == 0 {
			e.Attempts = 1 + r.Intn(3)
		}
		if !e.OK && r.Intn(2) == 0 {
			e.Err = "boom"
		}
		seq[i] = e
	}
	return reflect.ValueOf(seq)
}

var documentedEventAttrs = map[string]bool{"run": true, "env": true, "instance": true, "step": true,
	"task": true, "name": true, "attempts": true, "count": true, "exit": true, "reward": true}

func TestPBT_P19_TracingIsTransparent(t *testing.T) {
	prop := func(seq eventSeq) bool {
		var plain, traced bytes.Buffer
		sr := tracetest.NewSpanRecorder()
		tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)).Tracer("pbt")
		r1, r2 := NewRecorder(&plain), NewRecorder(&traced, WithTracer(tracer))
		for _, e := range seq {
			r1.Record(e)
			r2.Record(e)
		}
		if !bytes.Equal(plain.Bytes(), traced.Bytes()) {
			t.Logf("JSONL differs with tracing")
			return false
		}
		spans := sr.Ended()
		if len(spans) != len(seq) {
			t.Logf("%d spans for %d events", len(spans), len(seq))
			return false
		}
		for i, s := range spans {
			e := seq[i]
			if s.Name() != e.Event {
				t.Logf("span %q for event %q", s.Name(), e.Event)
				return false
			}
			if (s.Status().Code == codes.Ok) != e.OK {
				t.Logf("status %v for ok=%v", s.Status().Code, e.OK)
				return false
			}
			for _, kv := range s.Attributes() {
				if !documentedEventAttrs[string(kv.Key)] {
					t.Logf("undocumented span attribute %q (event %+v)", kv.Key, e)
					return false
				}
			}
		}
		return true
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 300}); err != nil {
		t.Error(err)
	}
}

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

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/events.golden.txt from the current Recorder")

// goldenEvents is a fixed sequence covering every event kind and every
// optional field, as one steps-mode task with a failure, a wake and a park
// produces them.
func goldenEvents() []Event {
	t0 := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	return []Event{
		{Time: at(1200), RunID: "swe-golden", Event: EventCreate, Env: "swe-golden-astropy-1-0", Instance: "astropy__astropy-1", Step: 1, DurMS: 1200.5, OK: true},
		{Time: at(3400), RunID: "swe-golden", Event: EventTTFI, Env: "swe-golden-astropy-1-0", Instance: "astropy__astropy-1", Step: 1, DurMS: 3400, OK: true, Attempts: 2},
		{Time: at(3400), RunID: "swe-golden", Event: EventStep, Name: "checkout", Env: "swe-golden-astropy-1-0", Instance: "astropy__astropy-1", Step: 1, DurMS: 2200.25, OK: true, Exit: intp(0)},
		{Time: at(9000), RunID: "swe-golden", Event: EventStep, Name: "grade", Env: "swe-golden-astropy-1-0", Instance: "astropy__astropy-1", Step: 1, DurMS: 5600, OK: true, Exit: intp(1), Reward: intp(0), Tail: "FAILED tests/test_a.py::test_b\nunresolved\n"},
		{Time: at(9000), RunID: "swe-golden", Event: EventTask, Env: "swe-golden-astropy-1-0", Instance: "astropy__astropy-1", Step: 1, DurMS: 7800, OK: true, Reward: intp(0)},
		{Time: at(9100), RunID: "swe-golden", Event: EventStep, Name: "build", Env: "swe-golden-django-2-1", Instance: "django__django-2", Step: 1, Task: 1, DurMS: 90, OK: false, Exit: intp(2), Err: "exit 2, expected 0", Tail: "make: *** [all] Error 2"},
		{Time: at(9100), RunID: "swe-golden", Event: EventTask, Env: "swe-golden-django-2-1", Instance: "django__django-2", Step: 1, Task: 1, DurMS: 4000, OK: false},
		{Time: at(9200), RunID: "swe-golden", Event: EventBarrier, Step: 1, DurMS: 8000, OK: false, Count: 2},
		{Time: at(9700), RunID: "swe-golden", Event: EventSuspend, Env: "swe-golden-astropy-1-0", Instance: "astropy__astropy-1", Step: 1, DurMS: 500, OK: true},
		{Time: at(9800), RunID: "swe-golden", Event: EventPark, Name: "suspend", Step: 1, DurMS: 600, OK: true, Count: 2},
		{Time: at(12000), RunID: "swe-golden", Event: EventWake, Env: "swe-golden-astropy-1-0", Instance: "astropy__astropy-1", Step: 2, DurMS: 2100, OK: true, Attempts: 1},
		{Time: at(13000), RunID: "swe-golden", Event: EventDelete, Env: "swe-golden-django-2-1", Instance: "django__django-2", Step: 2, Task: 1, DurMS: 300, OK: false, Err: "rpc error: code = Unavailable desc = connection refused"},
		{Time: at(14000), RunID: "swe-golden", Event: EventRun, Name: "steps", DurMS: 14000, OK: false, Count: 2},
	}
}

// TestRecorderGoldenJSONL pins the JSONL bytes: testdata/events.golden.txt
// (JSON lines; .txt keeps the license-header check off it) was written by the
// Recorder of upstream 8ff5c3e, before tracing was added, and the Recorder
// with tracing off must still produce it byte for byte. Do not regenerate it
// with -update unless the JSONL format is meant to change.
func TestRecorderGoldenJSONL(t *testing.T) {
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	for _, e := range goldenEvents() {
		rec.Record(e)
	}
	if err := rec.Err(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("testdata", "events.golden.txt")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("Recorder JSONL differs from %s\ngot:\n%s\nwant:\n%s", path, buf.Bytes(), want)
	}
}

// TestRecorderSpans checks that with a tracer every event is one span
// named after the event, timed by the event, with the event's ids as
// attributes and its outcome as the status, and that the JSONL is unchanged.
func TestRecorderSpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	tracer := tp.Tracer("swebench_loader")
	parentCtx, parentSpan := tracer.Start(context.Background(), "swebench-loader.run")

	var buf bytes.Buffer
	rec := NewRecorder(&buf, WithTracer(tracer), WithParent(parentCtx))
	events := goldenEvents()
	for _, e := range events {
		rec.Record(e)
	}
	parentSpan.End()

	want, err := os.ReadFile(filepath.Join("testdata", "events.golden.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("JSONL with tracing on differs from the golden file\ngot:\n%s", buf.Bytes())
	}

	var spans []sdktrace.ReadOnlySpan
	var parent sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == "swebench-loader.run" {
			parent = s
			continue
		}
		spans = append(spans, s)
	}
	if parent == nil {
		t.Fatal("parent span not recorded")
	}
	if len(spans) != len(events) {
		t.Fatalf("got %d event spans, want %d (one per event)", len(spans), len(events))
	}
	for i, s := range spans {
		e := events[i]
		if s.Name() != e.Event {
			t.Errorf("span %d name = %q, want %q", i, s.Name(), e.Event)
		}
		if s.Parent().SpanID() != parent.SpanContext().SpanID() || s.SpanContext().TraceID() != parent.SpanContext().TraceID() {
			t.Errorf("span %d (%s) is not a child of the run span", i, e.Event)
		}
		if !s.EndTime().Equal(e.Time) {
			t.Errorf("span %d end = %v, want %v", i, s.EndTime(), e.Time)
		}
		if got, want := s.EndTime().Sub(s.StartTime()), time.Duration(e.DurMS*float64(time.Millisecond)); got != want {
			t.Errorf("span %d duration = %v, want %v", i, got, want)
		}
		attrs := map[string]string{}
		for _, kv := range s.Attributes() {
			attrs[string(kv.Key)] = kv.Value.Emit()
		}
		wantAttrs := map[string]string{"run": e.RunID, "env": e.Env, "instance": e.Instance,
			"step": strconv.Itoa(e.Step), "task": strconv.Itoa(e.Task)}
		for k, v := range wantAttrs {
			if v == "" {
				if _, ok := attrs[k]; ok {
					t.Errorf("span %d (%s) has empty attribute %s set", i, e.Event, k)
				}
				continue
			}
			if attrs[k] != v {
				t.Errorf("span %d (%s) attribute %s = %q, want %q", i, e.Event, k, attrs[k], v)
			}
		}
		if e.Name != "" && attrs["name"] != e.Name {
			t.Errorf("span %d (%s) attribute name = %q, want %q", i, e.Event, attrs["name"], e.Name)
		}
		switch {
		case e.OK && s.Status().Code != codes.Ok:
			t.Errorf("span %d (%s) status = %v, want Ok", i, e.Event, s.Status())
		case !e.OK && s.Status().Code != codes.Error:
			t.Errorf("span %d (%s) status = %v, want Error", i, e.Event, s.Status())
		case !e.OK && e.Err != "" && s.Status().Description != e.Err:
			t.Errorf("span %d (%s) status description = %q, want %q", i, e.Event, s.Status().Description, e.Err)
		}
	}
}

// TestRecorderNoTracerRecordsNoSpans checks that the default Recorder
// leaves the global tracer provider alone.
func TestRecorderNoTracerRecordsNoSpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	rec := NewRecorder(nil)
	for _, e := range goldenEvents() {
		rec.Record(e)
	}
	if n := len(sr.Ended()); n != 0 {
		t.Errorf("default Recorder produced %d spans, want 0", n)
	}
}

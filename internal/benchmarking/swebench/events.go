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
	"context"
	"encoding/json"
	"io"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Event names. Every timed operation of a run is one of these.
const (
	EventCreate  = "create"       // CreateEnvironment call
	EventTTFI    = "ttfi"         // create start to the first command's return
	EventWake    = "wake"         // first command after a park, including the wake it triggers
	EventStep    = "step"         // one trajectory step (name is the step name)
	EventTask    = "task"         // one trajectory replay end to end
	EventSuspend = "suspend"      // SuspendEnvironment call
	EventDelete  = "delete"       // DeleteEnvironment call
	EventBarrier = "step_barrier" // steps mode: first task start to last task end in a training step
	EventPark    = "park"         // steps mode: parking the whole fleet between steps
	EventRun     = "run"          // whole run
)

// Event is one JSONL record. Dur is wall time as seen by the loader; it
// includes the network and the API server, which is what a trainer sees.
type Event struct {
	Time  time.Time `json:"ts"`
	RunID string    `json:"run"`
	Event string    `json:"event"`
	Name  string    `json:"name,omitempty"`
	// Env is the environment's name. With a delete park, an environment is
	// recreated under "<name>-s<step>" from step 2 on, thus group a replay's
	// events by Instance and Task, not by Env.
	Env      string `json:"env,omitempty"`
	Instance string `json:"instance,omitempty"`
	// Step is the training step in steps mode (1-based); 0 in eval mode.
	Step int `json:"step,omitempty"`
	// Task numbers the replays of one instance (eval: task index; steps:
	// rollout index).
	Task   int     `json:"task,omitempty"`
	DurMS  float64 `json:"dur_ms"`
	OK     bool    `json:"ok"`
	Exit   *int    `json:"exit,omitempty"`
	Reward *int    `json:"reward,omitempty"`
	Err    string  `json:"err,omitempty"`
	// Count is the number of tasks or environments an aggregate event covers.
	Count int `json:"count,omitempty"`
	// Tail is the last bytes of a failed or reward step's output.
	Tail string `json:"tail,omitempty"`
	// Attempts is how many tries a ttfi or wake took; above 1 means the
	// environment was not serving when first asked.
	Attempts int `json:"attempts,omitempty"`
	// Labels describe the run, on its run event: the loader's own (mode,
	// width, images, trajectory, the shape flags) and the operator's -label
	// values, which win on a clash.
	Labels map[string]string `json:"labels,omitempty"`
}

// Recorder writes events as JSON lines. It is safe for concurrent use.
//
// With a tracer (WithTracer), every event is also one span. An
// event is written when its operation finishes, with the finish time and the
// duration, so the span is recorded after the fact: it starts at Time minus
// DurMS and ends at Time. The span is named after Event.Event (a fixed
// vocabulary, never an id), carries run, env, instance, step and task as
// attributes, and has status Ok when the event is ok and Error otherwise.
// The JSONL is the same with or without a tracer.
type Recorder struct {
	mu  sync.Mutex
	w   io.Writer
	enc *json.Encoder
	n   int
	err error

	tracer trace.Tracer // nil: no spans
	parent context.Context
}

// RecorderOption configures a Recorder.
type RecorderOption func(*Recorder)

// WithTracer makes the Recorder record one span per event on tracer. The
// default is no tracer, which records none.
func WithTracer(tracer trace.Tracer) RecorderOption {
	return func(r *Recorder) {
		r.tracer = tracer
	}
}

// WithParent makes the event spans children of the span in ctx, so a whole
// run lands in one trace. The default is a span-less context: every event
// span is a root.
func WithParent(ctx context.Context) RecorderOption {
	return func(r *Recorder) {
		if ctx != nil {
			r.parent = ctx
		}
	}
}

// NewRecorder writes to w; a nil w discards events.
func NewRecorder(w io.Writer, opts ...RecorderOption) *Recorder {
	if w == nil {
		w = io.Discard
	}
	r := &Recorder{w: w, enc: json.NewEncoder(w), parent: context.Background()}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Record writes one event, stamping the time if unset.
func (r *Recorder) Record(e Event) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	r.mu.Lock()
	if err := r.enc.Encode(e); err != nil && r.err == nil {
		r.err = err
	}
	r.n++
	r.mu.Unlock()
	if r.tracer != nil {
		r.span(e)
	}
}

// span records e as a span that ran for DurMS up to e.Time.
func (r *Recorder) span(e Event) {
	end := e.Time
	start := end.Add(-time.Duration(e.DurMS * float64(time.Millisecond)))
	attrs := []attribute.KeyValue{attribute.String("run", e.RunID)}
	set := func(k, v string) {
		if v != "" {
			attrs = append(attrs, attribute.String(k, v))
		}
	}
	set("env", e.Env)
	set("instance", e.Instance)
	set("step", strconv.Itoa(e.Step))
	set("task", strconv.Itoa(e.Task))
	set("name", e.Name)
	if e.Attempts > 0 {
		set("attempts", strconv.Itoa(e.Attempts))
	}
	if e.Count > 0 {
		set("count", strconv.Itoa(e.Count))
	}
	if e.Exit != nil {
		set("exit", strconv.Itoa(*e.Exit))
	}
	if e.Reward != nil {
		set("reward", strconv.Itoa(*e.Reward))
	}
	_, span := r.tracer.Start(r.parent, e.Event, trace.WithTimestamp(start), trace.WithAttributes(attrs...))
	if e.OK {
		span.SetStatus(codes.Ok, "")
	} else {
		msg := e.Err
		if msg == "" {
			msg = "not ok"
		}
		span.SetStatus(codes.Error, msg)
	}
	span.End(trace.WithTimestamp(end))
}

// Len is the number of events written so far.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// Err is the first write error, if any; a run whose events were lost must
// not report success.
func (r *Recorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func intp(v int) *int { return &v }

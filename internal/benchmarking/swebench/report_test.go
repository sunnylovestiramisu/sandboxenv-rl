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
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

func TestPercentile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if p := percentile(s, 0.5); p != 5 {
		t.Errorf("p50 = %v", p)
	}
	if p := percentile(s, 0.95); p != 10 {
		t.Errorf("p95 = %v", p)
	}
	if p := percentile(s, 1); p != 10 {
		t.Errorf("max = %v", p)
	}
	if p := percentile(nil, 0.5); p != 0 {
		t.Errorf("empty = %v", p)
	}
	thirteen := make([]float64, 13)
	for i := range thirteen {
		thirteen[i] = float64(i + 1)
	}
	if p := percentile(thirteen, 0.95); p != 13 {
		t.Errorf("nearest rank p95 of 13 samples = %v, want the 13th", p)
	}
}

func TestSummarizeRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	rec.Record(Event{RunID: "r", Event: EventCreate, Env: "e1", DurMS: 100, OK: true})
	rec.Record(Event{RunID: "r", Event: EventCreate, Env: "e2", DurMS: 300, OK: false, Err: "boom"})
	rec.Record(Event{RunID: "r", Event: EventStep, Name: "run_tests", DurMS: 50, OK: true, Exit: intp(0)})
	rec.Record(Event{RunID: "r", Event: EventStep, Name: "explore", DurMS: 5, OK: true, Exit: intp(0)})
	rec.Record(Event{RunID: "r", Event: EventTask, Step: 1, DurMS: 500, OK: true, Reward: intp(1)})
	rec.Record(Event{RunID: "r", Event: EventTask, Step: 1, DurMS: 600, OK: false})
	rec.Record(Event{RunID: "r", Event: EventWake, Step: 2, DurMS: 40, OK: true, Attempts: 3})
	rec.Record(Event{RunID: "r", Event: EventStep, Name: "grade", Instance: "repo__x-1", DurMS: 9, OK: true, Reward: intp(0),
		Detail: "f2p 1/1 p2p 3/4"})
	rec.Record(Event{RunID: "r", Event: EventStep, Name: "grade", Instance: "repo__x-2", DurMS: 9, OK: true, Reward: intp(0), Tail: "no json"})
	rec.Record(Event{RunID: "r", Event: EventStep, Name: "grade", Instance: "repo__x-3", DurMS: 9, OK: true, Reward: intp(0),
		Tail: `{"valid": true, "resolved": false, "f2p": "0/1", "p2p": "2/2"}`})
	rec.Record(Event{RunID: "r", Event: EventStep, Name: "grade", Instance: "repo__x-1", DurMS: 9, OK: true, Reward: intp(0),
		Tail: `{"valid": false, "resolved": false}`})
	rec.Record(Event{RunID: "r", Event: EventTask, Step: 2, DurMS: 700, OK: true, Reward: intp(0)})
	rec.Record(Event{RunID: "r", Event: EventBarrier, Step: 1, DurMS: 650, OK: true, Count: 2})
	rec.Record(Event{RunID: "r", Event: EventPark, Name: "suspend", Step: 1, DurMS: 90, OK: true, Count: 1})
	rec.Record(Event{RunID: "r", Event: EventRun, Name: "steps", DurMS: 2000, OK: false, Count: 3})
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	if rec.Len() != 15 {
		t.Fatalf("recorded %d", rec.Len())
	}

	rep, err := Summarize(strings.NewReader(buf.String()))
	if err != nil {
		t.Fatal(err)
	}
	if rep.RunID != "r" || rep.Mode != "steps" || rep.RunMS != 2000 {
		t.Fatalf("header: %+v", rep)
	}
	if rep.Tasks != 3 || rep.Failed != 1 || rep.Rewarded != 1 {
		t.Fatalf("tasks: %+v", rep)
	}
	find := func(event, name string) Stat {
		for _, s := range rep.Stats {
			if s.Event == event && s.Name == name {
				return s
			}
		}
		t.Fatalf("no stat %s/%s in %+v", event, name, rep.Stats)
		return Stat{}
	}
	if c := find(EventCreate, ""); c.Count != 2 || c.Failed != 1 || c.P50MS != 100 || c.MaxMS != 100 {
		t.Fatalf("create: %+v", c)
	}
	if s := find(EventStep, "run_tests"); s.Count != 1 || s.P50MS != 50 {
		t.Fatalf("step: %+v", s)
	}
	if len(rep.Steps) != 2 || rep.Steps[0].BarrierMS != 650 || rep.Steps[0].ParkMS != 90 || rep.Steps[0].Live != 2 || rep.Steps[1].WakeP50MS != 40 {
		t.Fatalf("steps: %+v", rep.Steps)
	}
	if len(rep.Errors) != 1 || rep.Errors[0].Err != "boom" || rep.Errors[0].Count != 1 {
		t.Fatalf("errors: %+v", rep.Errors)
	}
	if w := find(EventWake, ""); w.Retried != 1 {
		t.Fatalf("wake retried: %+v", w)
	}
	if len(rep.Unresolved) != 3 || rep.Unresolved[0].Instance != "repo__x-1" || rep.Unresolved[0].Reason != "f2p 1/1 p2p 3/4" ||
		rep.Unresolved[1].Reason != "reward 0" || rep.Unresolved[2].Reason != "f2p 0/1 p2p 2/2" {
		t.Fatalf("unresolved: %+v", rep.Unresolved)
	}
	var excl bytes.Buffer
	rep.WriteExclusions(&excl, "test list")
	if !strings.Contains(excl.String(), "# test list\n") || !strings.Contains(excl.String(), "repo__x-1  f2p 1/1 p2p 3/4\n") {
		t.Fatalf("exclusions:\n%s", excl.String())
	}
	if v := verdict(`{"valid": false}`); v != "invalid run" {
		t.Fatalf("verdict: %q", v)
	}
	var text bytes.Buffer
	rep.WriteText(&text)
	for _, want := range []string{"run r", "create", "run_tests", "barrier_s", "boom"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, text.String())
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestRecorderKeepsFirstWriteError(t *testing.T) {
	rec := NewRecorder(failingWriter{})
	rec.Record(Event{RunID: "r", Event: EventCreate})
	rec.Record(Event{RunID: "r", Event: EventDelete})
	if err := rec.Err(); err == nil || err.Error() != "disk full" {
		t.Fatalf("Err = %v", err)
	}
	if NewRecorder(nil).Err() != nil {
		t.Fatal("a discarding recorder has no error")
	}
}

type spyWriter struct {
	buf    bytes.Buffer
	writes int
}

func (s *spyWriter) Write(p []byte) (int, error) {
	s.writes++
	return s.buf.Write(p)
}

func TestRecorderBufferedFlushAndClose(t *testing.T) {
	spy := &spyWriter{}
	rec := NewRecorder(spy)
	// Normal fine-grained events stay in the 64 KiB buffer until a boundary event or Flush/Close.
	rec.Record(Event{RunID: "r", Event: EventCreate, Env: "e1", DurMS: 10, OK: true})
	rec.Record(Event{RunID: "r", Event: EventStep, Name: "explore", Env: "e1", DurMS: 20, OK: true})
	if spy.writes != 0 {
		t.Fatalf("writes before boundary/flush = %d, want 0", spy.writes)
	}
	// Coarse boundary events (step_barrier, park, run) auto-flush immediately.
	rec.Record(Event{RunID: "r", Event: EventBarrier, Step: 1, DurMS: 30, OK: true, Count: 1})
	if spy.writes != 1 {
		t.Fatalf("writes after EventBarrier = %d, want 1", spy.writes)
	}
	rec.Record(Event{RunID: "r", Event: EventPark, Name: "suspend", Step: 1, DurMS: 15, OK: true, Count: 1})
	if spy.writes != 2 {
		t.Fatalf("writes after EventPark = %d, want 2", spy.writes)
	}
	rec.Record(Event{RunID: "r", Event: EventDelete, Env: "e1", DurMS: 5, OK: true})
	if spy.writes != 2 {
		t.Fatalf("writes after EventDelete = %d, want 2 (still buffered)", spy.writes)
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if spy.writes != 3 {
		t.Fatalf("writes after Close() = %d, want 3", spy.writes)
	}
	evs, err := ReadEvents(&spy.buf)
	if err != nil || len(evs) != 5 {
		t.Fatalf("ReadEvents = %d, %v; want 5 events", len(evs), err)
	}
}

func TestBuildAndSummarizeParity(t *testing.T) {
	var buf bytes.Buffer
	rec := NewRecorder(&buf)
	input := []Event{
		{RunID: "parity", Event: EventCreate, Env: "e1", Instance: "repo__a-1", DurMS: 110, OK: true},
		{RunID: "parity", Event: EventCreate, Env: "e2", Instance: "repo__a-2", DurMS: 210, OK: false, Err: "create boom"},
		{RunID: "parity", Event: EventTTFI, Env: "e1", Instance: "repo__a-1", Step: 1, DurMS: 950, OK: true, Attempts: 2},
		{RunID: "parity", Event: EventStep, Name: "grade", Env: "e1", Instance: "repo__a-1", Step: 1, DurMS: 45, OK: true, Reward: intp(0), Detail: "f2p 0/1 p2p 3/3"},
		{RunID: "parity", Event: EventStep, Name: "grade", Env: "e2", Instance: "repo__a-2", Step: 1, DurMS: 55, OK: true, Reward: intp(0), Tail: `{"valid": true, "resolved": false, "f2p": "0/2", "p2p": "1/1"}`},
		{RunID: "parity", Event: EventTask, Env: "e1", Instance: "repo__a-1", Step: 1, DurMS: 1000, OK: true, Reward: intp(0)},
		{RunID: "parity", Event: EventTask, Env: "e2", Instance: "repo__a-2", Step: 1, DurMS: 1200, OK: false},
		{RunID: "parity", Event: EventBarrier, Step: 1, DurMS: 1250, OK: true, Count: 2},
		{RunID: "parity", Event: EventPark, Name: "suspend", Step: 1, DurMS: 80, OK: true, Count: 1},
		{RunID: "parity", Event: EventWake, Env: "e1", Instance: "repo__a-1", Step: 2, DurMS: 35, OK: true, Attempts: 1},
		{RunID: "parity", Event: EventTask, Env: "e1", Instance: "repo__a-1", Step: 2, DurMS: 800, OK: true, Reward: intp(1)},
		{RunID: "parity", Event: EventRun, Name: "steps", DurMS: 2500, OK: false, Count: 3},
	}
	for _, e := range input {
		rec.Record(e)
	}
	if err := rec.Close(); err != nil {
		t.Fatal(err)
	}
	evs, err := ReadEvents(strings.NewReader(buf.String()))
	if err != nil {
		t.Fatal(err)
	}
	fromBuild := Build(evs)
	fromStream, err := Summarize(strings.NewReader(buf.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fromBuild, fromStream) {
		t.Fatalf("Build vs Summarize mismatch:\nBuild:     %+v\nSummarize: %+v", fromBuild, fromStream)
	}
	if fromStream.RunID != "parity" || fromStream.Mode != "steps" || fromStream.RunMS != 2500 {
		t.Fatalf("unexpected header: %+v", fromStream)
	}
	if fromStream.Tasks != 3 || fromStream.Failed != 1 || fromStream.Rewarded != 1 || fromStream.RewardRate != 1.0/3.0 {
		t.Fatalf("unexpected task totals: Tasks=%d Failed=%d Rewarded=%d RewardRate=%v",
			fromStream.Tasks, fromStream.Failed, fromStream.Rewarded, fromStream.RewardRate)
	}
	if len(fromStream.Errors) != 1 || fromStream.Errors[0] != (ErrorCount{Err: "create boom", Count: 1}) {
		t.Fatalf("unexpected errors: %+v", fromStream.Errors)
	}
	wantUnresolved := []Unresolved{
		{Instance: "repo__a-1", Reason: "f2p 0/1 p2p 3/3"},
		{Instance: "repo__a-2", Reason: "f2p 0/2 p2p 1/1"},
	}
	if !reflect.DeepEqual(fromStream.Unresolved, wantUnresolved) {
		t.Fatalf("unexpected unresolved: got %+v, want %+v", fromStream.Unresolved, wantUnresolved)
	}
	wantSteps := []StepStat{
		{Step: 1, Tasks: 2, Failed: 1, Rewarded: 0, Live: 2, BarrierMS: 1250, ParkMS: 80},
		{Step: 2, Tasks: 1, Failed: 0, Rewarded: 1, WakeP50MS: 35, WakeP95MS: 35},
	}
	if !reflect.DeepEqual(fromStream.Steps, wantSteps) {
		t.Fatalf("unexpected steps: got %+v, want %+v", fromStream.Steps, wantSteps)
	}
}

func TestWriteComparison(t *testing.T) {
	a := Build([]Event{
		{RunID: "a", Event: EventCreate, DurMS: 100, OK: true},
		{RunID: "a", Event: EventTTFI, DurMS: 900, OK: true, Attempts: 2},
		{RunID: "a", Event: EventTask, Step: 1, DurMS: 500, OK: true, Reward: intp(1)},
		{RunID: "a", Event: EventWake, Step: 1, DurMS: 40, OK: true},
		{RunID: "a", Event: EventBarrier, Step: 1, DurMS: 700, OK: true, Count: 3},
		{RunID: "a", Event: EventPark, Name: "suspend", Step: 1, DurMS: 100, OK: true, Count: 1},
		{RunID: "a", Event: EventRun, Name: "steps", DurMS: 2000, OK: true, Count: 1, Labels: map[string]string{"cluster": "east", "mode": "steps"}},
	})
	b := Build([]Event{
		{RunID: "b", Event: EventCreate, DurMS: 50, OK: true},
		{RunID: "b", Event: EventCreate, DurMS: 70, OK: false, Err: "boom"},
		{RunID: "b", Event: EventDelete, DurMS: 70, OK: false, Err: "boom"},
		{RunID: "b", Event: EventTask, DurMS: 400, OK: true, Reward: intp(1)}, // eval mode: step 0
		{RunID: "b", Event: EventRun, Name: "eval", DurMS: 1000, OK: true, Count: 1, Labels: map[string]string{"tasks": "operator"}},
	})
	var out bytes.Buffer
	WriteComparison(&out, []*Report{a, b}, []string{"runs/a.jsonl", "runs/b.jsonl"})
	text := out.String()
	for _, want := range []string{"100/100 n=1", "50/50 n=2 f=1", "900/900 n=1 r=1", "-/- n=1 f=1", "live=3 tasks=1 0.7/0.1 40/40"} {
		if !strings.Contains(text, want) {
			t.Errorf("comparison lacks %q:\n%s", want, text)
		}
	}
	if strings.Count(text, "cells:") != 1 || !strings.HasPrefix(text, "cells:") {
		t.Errorf("the legend must appear once, above the table:\n%s", text)
	}
	flat := strings.Join(strings.Fields(text), " ")
	if !strings.Contains(flat, "cluster east -") {
		t.Errorf("label row: want \"cluster east -\" in:\n%s", text)
	}
	// trace_captured sits beside mode: a traced run's latencies include SDK
	// trace overhead, so the comparison must say which runs were traced.
	if !strings.Contains(flat, "mode steps eval trace_captured false false reps 1 1") || strings.Count(flat, "mode ") != 1 {
		t.Errorf("want one mode row (the label repeats it), a trace_captured row and a reps row in:\n%s", text)
	}
	// An operator label named like a built-in row keeps its own row, prefixed.
	if !strings.Contains(flat, "label.tasks - operator") || strings.Count(flat, " tasks ") != 2 {
		t.Errorf("want a label.tasks row next to the tasks row in:\n%s", text)
	}
	// A run without the event, or without the step, shows a dash in its column.
	for _, label := range []string{"ttfi", "step 1"} {
		found := false
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, label+" ") {
				found = true
				if !strings.HasSuffix(strings.TrimSpace(line), "-") {
					t.Errorf("%s row should end with a dash for run b: %q", label, line)
				}
			}
		}
		if !found {
			t.Errorf("no %s row:\n%s", label, text)
		}
	}
	header := strings.Split(text, "\n")[1]
	if fields := strings.Fields(header); len(fields) != 3 || fields[0] != "run" || fields[1] != "a" || fields[2] != "b" {
		t.Errorf("header row: %q", header)
	}

	// Two files with the same run id, or none, are told apart by file name,
	// and by the full path when the file names repeat too.
	same := Build([]Event{{RunID: "a", Event: EventCreate, DurMS: 1, OK: true}, {RunID: "a", Event: EventRun, Name: "eval", DurMS: 1, OK: true}})
	empty := Build(nil)
	got := columnLabels([]*Report{a, same, empty}, []string{"x/first.jsonl", "x/second.jsonl", ""})
	if want := []string{"first", "second", "run 3"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("labels = %q, want %q", got, want)
	}
	got = columnLabels([]*Report{a, same}, []string{"east/run.jsonl", "west/run.jsonl"})
	if want := []string{"east/run.jsonl", "west/run.jsonl"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("labels = %q, want %q", got, want)
	}

	// A step with no wake, the first one or -park none, shows a dash for wake.
	nowake := Build([]Event{
		{RunID: "n", Event: EventTask, Step: 1, DurMS: 500, OK: true, Reward: intp(1)},
		{RunID: "n", Event: EventBarrier, Step: 1, DurMS: 700, OK: true, Count: 1},
		{RunID: "n", Event: EventRun, Name: "steps", DurMS: 1000, OK: true, Count: 1},
	})
	out.Reset()
	WriteComparison(&out, []*Report{nowake}, []string{"n.jsonl"})
	if !strings.Contains(out.String(), "live=1 tasks=1 0.7/0.0 -/-") {
		t.Errorf("step without wakes:\n%s", out.String())
	}
}

func TestNodeCacheDelta(t *testing.T) {
	const scope = `otel_scope_name="go.ate.dev/substrate/internal/imagecache",otel_scope_version=""`
	req := func(node, outcome string, v int) string {
		return node + "\tate_imagecache_requests_total{ate_imagecache_outcome=\"" + outcome + "\"," + scope + "} " + strconv.Itoa(v) + "\n"
	}
	start := func(node string, unix int) string {
		return node + "\tprocess_start_time_seconds " + strconv.Itoa(unix) + "\n"
	}
	before := "n1\t# sampled 1000\n" + start("n1", 500) +
		"n1\t# HELP ate_imagecache_requests_total Number of image lookups\n" +
		req("n1", "hit", 10) + req("n1", "miss", 4) +
		"n1\tate_imagecache_requests_created{ate_imagecache_outcome=\"miss\"} 1.7e+09\n" + // not the counter
		"n2\t# sampled 1000\n" + start("n2", 500) + req("n2", "miss", 7) +
		"n3\t# sampled 1000\n" + start("n3", 500) + req("n3", "hit", 1)
	after := "n1\t# sampled 2000\n" + start("n1", 500) +
		req("n1", "hit", 12) + req("n1", "miss", 9) +
		"n1\tate_imagecache_requests_total{ate_imagecache_outcome=\"error\",error_type=\"429\"," + scope + "} 1\n" +
		"n2\t# sampled 2000\n" + start("n2", 1500) + req("n2", "miss", 2) + // atelet restarted: count from zero
		"n4\t# sampled 2000\n" + "n4\tprocess_start_time_seconds{otel_scope_name=\"x\"} 1.5e+03\n" + req("n4", "hit", 1) + req("n4", "miss", 3) + // node added after the before sample; labeled form
		"n6\t# sampled 2000\n" + start("n6", 400) + req("n6", "miss", 100) // older than the before sample: its before scrape failed
	b, err := ParseNodeCacheSample(strings.NewReader(before))
	if err != nil {
		t.Fatal(err)
	}
	a, err := ParseNodeCacheSample(strings.NewReader(after))
	if err != nil {
		t.Fatal(err)
	}
	if !b.Time.Equal(time.Unix(1000, 0)) || !b.Nodes["n1"].Equal(time.Unix(500, 0)) || b.Requests["n1"]["miss"] != 4 {
		t.Fatalf("before sample: %+v", b)
	}
	d := b.Delta(a)
	want := CacheDelta{Nodes: 1, Restarted: 1, Added: 1, Removed: 1, Skipped: 1, Hits: 3, Misses: 10, Errors: 1, NodesWithMisses: 3}
	if d != want {
		t.Fatalf("delta = %+v, want %+v", d, want)
	}
	for _, bad := range []string{
		"n1 ate_imagecache_requests_total{ate_imagecache_outcome=\"hit\"} 1\n",
		"n1\tate_imagecache_requests_total{otel_scope_name=\"x\"} 1\n",
		"n1\tate_imagecache_requests_total{ate_imagecache_outcome=\"hit\"} many\n",
		"n1\t# sampled soon\n",
	} {
		if _, err := ParseNodeCacheSample(strings.NewReader(bad)); err == nil {
			t.Errorf("line %q must be rejected", bad)
		}
	}

	rep := Build([]Event{
		{RunID: "r", Event: EventTTFI, DurMS: 100, OK: true},
		{RunID: "r", Event: EventTTFI, DurMS: 100, OK: true},
		{RunID: "r", Event: EventRun, Name: "eval", DurMS: 1000, OK: true, Count: 2},
	})
	rep.Cache = &d
	var text bytes.Buffer
	rep.WriteText(&text)
	want2 := "image cache during the run: 1 nodes differenced, 1 restarted, counted from zero, 1 added, counted from zero, 1 removed, left out, 1 with no before sample, left out; misses (cold pulls) 10 on 3 nodes, hits 3, errors 1; first commands 2, misses per first command 5.00"
	if !strings.Contains(text.String(), want2) {
		t.Fatalf("text report lacks %q:\n%s", want2, text.String())
	}
}

func TestJoinRestores(t *testing.T) {
	rec := func(ts, actor, errType string, phases string) string {
		e := ""
		if errType != "" {
			e = `,"error.type":"` + errType + `"`
		}
		return `{"time":"2026-10-21T` + ts + `Z","level":"INFO","msg":"Restore timing breakdown","ate.atespace":"ate-env","ate.actor.name":"` + actor +
			`","ate.actor.uid":"u","ate.template.name":"t","ate.snapshot.fidelity":"memory"` + e + phases + "}\n"
	}
	const total = `,"ate.actor.restore.duration.total":`
	log := `{"time":"2026-10-21T10:00:00Z","level":"INFO","msg":"Something else","ate.actor.name":"r-a-0"}` + "\n" +
		// The worker's own record: same message, other key prefix, skipped.
		`{"time":"2026-10-21T10:00:03Z","msg":"Restore timing breakdown","ate.actor.name":"r-a-0","ateworker.actor.restore.duration.total":2}` + "\n" +
		rec("09:00:00", "r-a-0", "", total+"1") + // an earlier run with the same names
		rec("10:00:01", "r-a-0", "", `,"ate.actor.restore.duration.manifest_fetch":0.1,"ate.actor.restore.duration.download":2,"ate.actor.restore.duration.oci_unpack":5,"ate.actor.restore.duration.worker_restore":1`+total+"8.1") +
		rec("10:03:00", "r-a-0", "", total+"1") + // inside the run, inside no window
		rec("10:05:01", "r-a-0", "", `,"ate.actor.restore.duration.download":0.5,"ate.actor.restore.duration.worker_restore":0.5`+total+"1") +
		rec("10:00:00.5", "r-b-0", "DeadlineExceeded", `,"ate.actor.restore.duration.download":30`+total+"30.5") +
		rec("10:00:40", "r-b-0", "", `,"ate.actor.restore.duration.download":3`+total+"4") +
		rec("10:00:18", "r-c-0", "", total+"2") +
		rec("10:10:01", "r-c-0", "", total+"1") +
		rec("10:20:01", "r-c-0", "", total+"1.5") + // its wake event failed
		rec("10:30:01", "r-c-0", "", `,"ate.actor.restore.duration.ateom_restore":0.4`+total+"1") + // an older atelet's phase name
		rec("10:00:02", "other-x-0", "", total+"1")
	timings, err := ParseRestoreTimings(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	if len(timings["r-a-0"]) != 4 || timings["r-a-0"][1].Phases["total"] != 8.1 || timings["r-b-0"][0].Err != "DeadlineExceeded" {
		t.Fatalf("timings: %+v", timings)
	}
	for _, bad := range []string{
		"not json\n",
		`{"time":"2026-10-21T10:00:00Z","msg":"Restore timing breakdown"` + total + "1}\n",
		`{"msg":"Restore timing breakdown","ate.actor.name":"x"` + total + "1}\n",
		`{"time":"garbage","msg":"Restore timing breakdown","ate.actor.name":"x"` + total + "1}\n",
	} {
		if _, err := ParseRestoreTimings(strings.NewReader(bad)); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}

	at := func(s string) time.Time {
		v, _ := time.Parse(time.RFC3339, "2026-10-21T"+s+"Z")
		return v
	}
	events := []Event{
		{Time: at("10:00:10"), RunID: "r", Event: EventTTFI, Env: "r-a-0", DurMS: 10100, OK: true},
		{Time: at("10:05:02"), RunID: "r", Event: EventWake, Env: "r-a-0", Step: 2, DurMS: 1500, OK: true},
		{Time: at("10:00:45"), RunID: "r", Event: EventTTFI, Env: "r-b-0", DurMS: 46000, OK: true, Attempts: 2},
		{Time: at("10:00:20"), RunID: "r", Event: EventTTFI, Env: "r-c-0", DurMS: 5000, OK: true},
		{Time: at("10:10:02"), RunID: "r", Event: EventWake, Env: "r-c-0", Step: 2, DurMS: 2000, OK: true},
		{Time: at("10:20:03"), RunID: "r", Event: EventWake, Env: "r-c-0", Step: 3, DurMS: 3000, OK: false, Err: "timeout"},
		{Time: at("10:30:02"), RunID: "r", Event: EventWake, Env: "r-c-0", Step: 4, DurMS: 2000, OK: true},
	}
	b := JoinRestores(events, timings)
	if b.OutsideRun != 1 || b.Unmatched != 1 || b.Unpaired != 1 {
		t.Fatalf("counts: %+v", b)
	}
	if b.First.Restores != 3 || b.First.Failed != 1 || b.First.FailedEvents != 0 {
		t.Fatalf("first: %+v", b.First)
	}
	if b.Wakes.Restores != 4 || b.Wakes.Failed != 0 || b.Wakes.FailedEvents != 1 {
		t.Fatalf("wakes: %+v", b.Wakes)
	}
	find := func(g RestoreGroup, phase string) PhaseStat {
		for _, p := range g.Phases {
			if p.Phase == phase {
				return p
			}
		}
		t.Fatalf("no %s in %+v", phase, g.Phases)
		return PhaseStat{}
	}
	if p := find(b.First, "download"); p.Count != 2 || p.MaxMS != 3000 {
		t.Fatalf("first download: %+v", p)
	}
	if p := find(b.First, "total"); p.Count != 3 || p.P50MS != 4000 || p.P90MS <= 0 || p.MaxMS != 8100 {
		t.Fatalf("first total: %+v", p)
	}
	if b.First.Phases[0].Phase != "manifest_fetch" || b.First.Phases[len(b.First.Phases)-1].Phase != "total" {
		t.Fatalf("first phase order: %+v", b.First.Phases)
	}
	// outside = event span - total: a 10100-8100, b 46000-4000, c 5000-2000.
	if o := b.First.Outside; o == nil || o.Count != 3 || o.P50MS != 3000 || o.MaxMS != 42000 {
		t.Fatalf("first outside: %+v", o)
	}
	// Wakes: a 1500-1000, c 2000-1000, c 2000-1000; the failed wake's restore is left out.
	if o := b.Wakes.Outside; o == nil || o.Count != 3 || o.P50MS != 1000 || o.MaxMS != 1000 {
		t.Fatalf("wake outside: %+v", o)
	}
	n := len(b.Wakes.Phases)
	if n < 2 || b.Wakes.Phases[n-1].Phase != "ateom_restore" || b.Wakes.Phases[n-2].Phase != "total" {
		t.Fatalf("unknown phase must follow the known ones: %+v", b.Wakes.Phases)
	}
	rep := Build(events)
	rep.Restores = b
	var text bytes.Buffer
	rep.WriteText(&text)
	flat := strings.Join(strings.Fields(text.String()), " ")
	for _, want := range []string{"1 records before or after this run", "1 records for actors not in this run", "1 records inside no ttfi or wake window", "first failed 1", "wake failed_events 1", "wake total 4"} {
		if !strings.Contains(flat, want) {
			t.Errorf("text report lacks %q:\n%s", want, text.String())
		}
	}
}

func TestBuildPoolsStepsAcrossRuns(t *testing.T) {
	// Two reps of one arm merged: per-step figures become medians, counts sum.
	rep := Build([]Event{
		{RunID: "r1", Event: EventTask, Step: 1, DurMS: 500, OK: true, Reward: intp(1)},
		{RunID: "r1", Event: EventBarrier, Step: 1, DurMS: 600, OK: true, Count: 2},
		{RunID: "r1", Event: EventPark, Name: "suspend", Step: 1, DurMS: 100, OK: true, Count: 2},
		{RunID: "r1", Event: EventRun, Name: "steps", DurMS: 1000, OK: true, Count: 1, Labels: map[string]string{"arm": "pull"}},
		{RunID: "r2", Event: EventTask, Step: 1, DurMS: 400, OK: true, Reward: intp(1)},
		{RunID: "r2", Event: EventBarrier, Step: 1, DurMS: 800, OK: true, Count: 4},
		{RunID: "r2", Event: EventPark, Name: "suspend", Step: 1, DurMS: 300, OK: true, Count: 4},
		{RunID: "r2", Event: EventRun, Name: "steps", DurMS: 1200, OK: true, Count: 1, Labels: map[string]string{"arm": "other"}},
	})
	if len(rep.Steps) != 1 {
		t.Fatalf("steps: %+v", rep.Steps)
	}
	s := rep.Steps[0]
	// Counts add up, durations take the median.
	if s.Tasks != 2 || s.Live != 6 || s.BarrierMS != 700 || s.ParkMS != 200 {
		t.Fatalf("pooled step: %+v (want tasks 2, live 6, barrier 700, park 200)", s)
	}
	if rep.Reps != 2 || strings.Join(rep.Runs, ",") != "r1,r2" || rep.RunMS != 1100 || rep.Mode != "steps" {
		t.Fatalf("reps %d runs %v run_ms %v mode %q, want 2, r1,r2, 1100 (the median), steps", rep.Reps, rep.Runs, rep.RunMS, rep.Mode)
	}
	if rep.Labels["arm"] != "pull|other" {
		t.Fatalf("a label that differs across the reps shows every value: %v", rep.Labels)
	}
}

func TestMedian(t *testing.T) {
	for _, tc := range []struct {
		in   []float64
		want float64
	}{{nil, 0}, {[]float64{7}, 7}, {[]float64{4, 1, 3}, 3}, {[]float64{800, 600}, 700}, {[]float64{1, 2, 3, 4}, 2.5}} {
		if got := median(append([]float64(nil), tc.in...)); got != tc.want {
			t.Errorf("median(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestPool(t *testing.T) {
	run := func(id string, labels map[string]string, events ...Event) []Event {
		out := append([]Event{{RunID: id, Event: EventCreate, DurMS: 100, OK: true}}, events...)
		return append(out, Event{RunID: id, Event: EventRun, Name: "eval", DurMS: 1000, OK: true, Count: 1, Labels: labels})
	}
	same := map[string]string{"mode": "eval", "width": "2", "commit": "aaa"}
	other := map[string]string{"mode": "eval", "width": "2", "commit": "bbb"}
	rep, err := Pool([][]Event{run("r1", same), run("r2", other)})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Reps != 2 || strings.Join(rep.Runs, ",") != "r1,r2" || rep.Labels["commit"] != "aaa|bbb" || rep.Labels["width"] != "2" {
		t.Fatalf("pooled: reps %d runs %v labels %v", rep.Reps, rep.Runs, rep.Labels)
	}
	for _, tc := range []struct {
		name string
		runs [][]Event
		err  string
	}{
		{"no run event", [][]Event{run("r1", same), {{RunID: "r2", Event: EventCreate, DurMS: 1, OK: true}}}, "rep 2 has no run event"},
		{"run id twice", [][]Event{run("r1", same), run("r1", same)}, `rep 2 repeats run id "r1" of rep 1`},
		{"other workload", [][]Event{run("r1", same), run("r2", map[string]string{"mode": "eval", "width": "3"})}, `rep 2 differs from rep 1 in width ("3", "2")`},
		{"no loader labels", [][]Event{run("r1", same), run("r2", nil)}, `rep 2 (run "r2") has no loader labels`},
		{"two runs in one file", [][]Event{append(run("r1", same), run("r2", same)...)}, "rep 1 holds 2 run events"},
		{"other -set value", [][]Event{run("r1", same), run("r2", map[string]string{"mode": "eval", "width": "2", "set.READ_FILES": "10"})}, `rep 2 differs from rep 1 in set.READ_FILES ("10", "")`},
	} {
		if _, err := Pool(tc.runs); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.err)
		}
	}
}

func TestSlowest(t *testing.T) {
	events := []Event{
		{Event: EventTTFI, Env: "e1", Instance: "i1", DurMS: 10, OK: true},
		{Event: EventTTFI, Env: "e2", Instance: "i2", DurMS: 30, OK: true, Attempts: 2},
		{Event: EventTTFI, Env: "e3", Instance: "i3", DurMS: 20, OK: true},
		{Event: EventTTFI, Env: "e4", Instance: "i4", DurMS: 99, OK: false},
		{Event: EventWake, Env: "e1", Step: 2, DurMS: 5, OK: true},
		{Event: EventWake, Env: "e2", Step: 2, DurMS: 7, OK: true},
	}
	got := Slowest(events, 2)
	if len(got) != 4 || got[0].Env != "e2" || got[0].Attempts != 2 || got[1].Env != "e3" || got[2].Event != EventWake || got[2].Env != "e2" || got[3].Env != "e1" {
		t.Fatalf("slowest: %+v", got)
	}
	rep := Build(events)
	rep.Slowest = got
	var text bytes.Buffer
	rep.WriteText(&text)
	if flat := strings.Join(strings.Fields(text.String()), " "); !strings.Contains(flat, "slowest first commands and wakes: event env instance step ms attempts ttfi e2 i2 0 30 2") {
		t.Fatalf("slowest table:\n%s", text.String())
	}
}

func TestBuildInterferenceSummaryMultiJob(t *testing.T) {
	t0 := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	events := []Event{
		{Time: t0.Add(100 * time.Millisecond), RunID: "r", Job: "job-a", Event: EventCreate, DurMS: 50, OK: true, Source: "warm_pool"},
		{Time: t0.Add(200 * time.Millisecond), RunID: "r", Job: "job-a", Event: EventTask, Step: 1, DurMS: 100, OK: true, Reward: intp(1)},
		{Time: t0.Add(500 * time.Millisecond), RunID: "r", Job: "job-a", Event: EventBarrier, Step: 1, DurMS: 500, OK: true, Count: 1},
		{Time: t0.Add(150 * time.Millisecond), RunID: "r", Job: "job-b", Event: EventCreate, DurMS: 60, OK: true, Source: "cold_start"},
		{Time: t0.Add(250 * time.Millisecond), RunID: "r", Job: "job-b", Event: EventTask, Step: 1, DurMS: 120, OK: true, Reward: intp(1)},
		{Time: t0.Add(450 * time.Millisecond), RunID: "r", Job: "job-b", Event: EventBarrier, Step: 1, DurMS: 400, OK: true, Count: 1},
	}
	rep := Build(events)
	if rep.Interference == nil {
		t.Fatal("expected non-nil Interference summary for overlapping multi-job steps")
	}
	if rep.Interference.OverlapWindowCount == 0 {
		t.Fatalf("expected OverlapWindowCount > 0, got %+v", rep.Interference)
	}
	if rep.AcquisitionSources["warm_pool"] != 1 || rep.AcquisitionSources["cold_start"] != 1 {
		t.Fatalf("unexpected AcquisitionSources: %+v", rep.AcquisitionSources)
	}
}

func TestActiveVsPausedSandboxesAndConcurrencyCap(t *testing.T) {
	t0 := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	events := []Event{
		{Time: t0.Add(10 * time.Millisecond), RunID: "warm", Event: EventCreate, Env: "e1", Step: 1, DurMS: 50, OK: true},
		{Time: t0.Add(20 * time.Millisecond), RunID: "warm", Event: EventCreate, Env: "e2", Step: 1, DurMS: 60, OK: true},
		{Time: t0.Add(100 * time.Millisecond), RunID: "warm", Event: EventTask, Env: "e1", Step: 1, DurMS: 80, OK: true, Reward: intp(1)},
		{Time: t0.Add(110 * time.Millisecond), RunID: "warm", Event: EventTask, Env: "e2", Step: 1, DurMS: 90, OK: true, Reward: intp(1)},
		{Time: t0.Add(120 * time.Millisecond), RunID: "warm", Event: EventBarrier, Step: 1, DurMS: 120, OK: true, Count: 2, StandingPoolSize: 2},
		{Time: t0.Add(140 * time.Millisecond), RunID: "warm", Event: EventSuspend, Env: "e1", Step: 1, DurMS: 20, OK: true, SizeMB: 128},
		{Time: t0.Add(150 * time.Millisecond), RunID: "warm", Event: EventSuspend, Env: "e2", Step: 1, DurMS: 25, OK: true, SizeMB: 132},
		{Time: t0.Add(160 * time.Millisecond), RunID: "warm", Event: EventPark, Name: "suspend-sync", Step: 1, DurMS: 30, OK: true, StandingPoolSize: 2},
		{Time: t0.Add(200 * time.Millisecond), RunID: "warm", Event: EventWake, Env: "e1", Step: 2, DurMS: 15, OK: true},
		{Time: t0.Add(210 * time.Millisecond), RunID: "warm", Event: EventTask, Env: "e1", Step: 2, DurMS: 70, OK: true, Reward: intp(1)},
		{Time: t0.Add(220 * time.Millisecond), RunID: "warm", Event: EventBarrier, Step: 2, DurMS: 80, OK: true, Count: 1, StandingPoolSize: 2},
		{Time: t0.Add(250 * time.Millisecond), RunID: "warm", Event: EventRun, Name: "steps", DurMS: 250, OK: true, StandingPoolSize: 2},
	}
	rep := Build(events)
	if rep.PeakActiveSandboxes != 2 || rep.PeakParkedSandboxes != 2 {
		t.Fatalf("unexpected peak active/paused sandboxes: active=%d paused=%d", rep.PeakActiveSandboxes, rep.PeakParkedSandboxes)
	}
	sys := &schema.SystemDescription{
		SchemaVersion: schema.SchemaVersion,
		Cluster:       "test-cluster",
		SandboxFootprint: &schema.SandboxFootprint{
			CPU:       2,
			MemoryGiB: 6,
		},
	}
	meas := &schema.Measurements{
		SchemaVersion: schema.SchemaVersion,
		ServerSummary: map[string]schema.MetricSummary{
			"active_sandboxes":        {Count: 1, P50: 1},
			"parked_sandboxes":        {Count: 1, P50: 1},
			"total_sandboxes":         {Count: 1, P50: 2},
			"worker_node_count":       {Count: 1, P50: 10},
			"worker_allocatable_vcpu": {Count: 1, P50: 318.5},
			"worker_allocatable_gib":  {Count: 1, P50: 1172.29},
		},
	}
	rep.AttachClusterTelemetry(sys, meas)
	if rep.HardwareCost == nil {
		t.Fatal("expected non-nil HardwareCost")
	}
	if rep.HardwareCost.ActiveSandboxes != 1 || rep.HardwareCost.PausedSandboxes != 1 || rep.HardwareCost.ParkedSandboxes != 1 || rep.HardwareCost.TotalSandboxes != 2 {
		t.Fatalf("unexpected active/paused/total sandboxes: %+v", rep.HardwareCost)
	}
	if rep.HardwareCost.PeakActiveSandboxes != 2 || rep.HardwareCost.PeakParkedSandboxes != 2 {
		t.Fatalf("unexpected peak active/paused in HardwareCost: %+v", rep.HardwareCost)
	}
	// 10 nodes * min(floor(31.85/2)=15, floor(117.229/6)=19) = 150 concurrent sandboxes cap.
	if rep.HardwareCost.MaxConcurrentSandboxesCap != 150 || sys.MaxConcurrentSandboxesCap != 150 {
		t.Fatalf("unexpected MaxConcurrentSandboxesCap: hc=%d sys=%d, want 150", rep.HardwareCost.MaxConcurrentSandboxesCap, sys.MaxConcurrentSandboxesCap)
	}
	if len(rep.Steps) != 2 || rep.Steps[0].ActiveSandboxes != 2 || rep.Steps[0].PausedSandboxes != 2 {
		t.Fatalf("unexpected step[0] active/paused sandboxes: %+v", rep.Steps)
	}
	if rep.LifecyclePolicy == nil || rep.LifecyclePolicy.Mode != "warm_pool" || rep.LifecyclePolicy.ParkMode != "suspend-sync" || rep.LifecyclePolicy.PerStepWarming != "wake_on_demand" {
		t.Fatalf("unexpected LifecyclePolicy for warm run: %+v", rep.LifecyclePolicy)
	}
}

func TestBuildMultiJobWorkingSetAndRunDuration(t *testing.T) {
	t0 := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	events := []Event{
		{
			Time: t0.Add(100 * time.Millisecond), RunID: "swe-shard", Job: "swe",
			Event: EventTask, Step: 1, DurMS: 800, OK: true, Reward: intp(1),
		},
		{
			Time: t0.Add(900 * time.Millisecond), RunID: "swe-shard", Job: "swe",
			Event: EventBarrier, Step: 1, DurMS: 900, OK: true, Count: 4,
			WorkingSetHits: 4, WorkingSetMisses: 0, WorkingSetHitRate: 1.0,
			StandingPoolSize: 4, Created: 4, Reused: 4, Retired: 4, PrefetchLeadMarginMS: 25,
		},
		{
			Time: t0.Add(950 * time.Millisecond), RunID: "swe-shard", Job: "swe",
			Event: EventRun, Name: "steps", DurMS: 950, OK: true,
			WorkingSetHits: 4, WorkingSetMisses: 0, WorkingSetHitRate: 1.0,
			StandingPoolSize: 4, Created: 4, Reused: 4, Retired: 4, PrefetchLeadMarginMS: 25,
		},
		{
			Time: t0.Add(120 * time.Millisecond), RunID: "r2e-shard", Job: "r2e",
			Event: EventTask, Step: 1, DurMS: 1100, OK: true, Reward: intp(1),
		},
		{
			Time: t0.Add(1300 * time.Millisecond), RunID: "r2e-shard", Job: "r2e",
			Event: EventBarrier, Step: 1, DurMS: 1300, OK: true, Count: 6,
			WorkingSetHits: 3, WorkingSetMisses: 3, WorkingSetHitRate: 0.5,
			StandingPoolSize: 6, Created: 6, Reused: 3, Retired: 6, PrefetchLeadMarginMS: 40,
		},
		{
			Time: t0.Add(1400 * time.Millisecond), RunID: "r2e-shard", Job: "r2e",
			Event: EventRun, Name: "steps", DurMS: 1400, OK: true,
			WorkingSetHits: 3, WorkingSetMisses: 3, WorkingSetHitRate: 0.5,
			StandingPoolSize: 6, Created: 6, Reused: 3, Retired: 6, PrefetchLeadMarginMS: 40,
		},
	}
	rep := Build(events)
	if rep.RunMS != 1400 {
		t.Fatalf("multi-job RunMS = %v, want max shard duration 1400", rep.RunMS)
	}
	if rep.WorkingSetHits != 7 || rep.WorkingSetMisses != 3 {
		t.Fatalf("multi-job hits/misses = %d/%d, want 7/3", rep.WorkingSetHits, rep.WorkingSetMisses)
	}
	if math.Abs(rep.WorkingSetHitRate-0.7) > 1e-6 {
		t.Fatalf("multi-job hit rate = %v, want 0.7", rep.WorkingSetHitRate)
	}
	if rep.StandingPoolSize != 10 || rep.Created != 10 || rep.Reused != 7 || rep.Retired != 10 {
		t.Fatalf("multi-job pool/created/reused/retired = %d/%d/%d/%d, want 10/10/7/10",
			rep.StandingPoolSize, rep.Created, rep.Reused, rep.Retired)
	}
	if rep.PrefetchLeadMarginMS != 65 {
		t.Fatalf("multi-job PrefetchLeadMarginMS = %v, want 65", rep.PrefetchLeadMarginMS)
	}
	if len(rep.Steps) != 1 || rep.Steps[0].BarrierMS != 1300 || rep.Steps[0].WorkingSetHits != 7 || rep.Steps[0].StandingPoolSize != 10 {
		t.Fatalf("multi-job step[0] = %+v, want barrier=1300 hits=7 pool=10", rep.Steps)
	}
}

// A -park-each run parks inside the barrier and records no park event, so
// the report takes the park mode from the run's park label: report.json,
// the lifecycle policy and the setup table all name the mode the run used.
func TestParkEachRunReportsParkMode(t *testing.T) {
	for _, park := range []Park{ParkSuspendAsync, ParkSuspendSync} {
		t.Run(string(park), func(t *testing.T) {
			f := newFake()
			var out bytes.Buffer
			cfg := baseConfig(t, f, &out)
			cfg.Mode = ModeSteps
			cfg.Rollouts = 1
			cfg.Steps = 2
			cfg.Park = park
			cfg.ParkEach = true
			if _, err := Run(context.Background(), cfg); err != nil {
				t.Fatal(err)
			}
			rep := Build(parseEvents(t, out.String()))
			for _, s := range rep.Stats {
				if s.Event == EventPark {
					t.Fatalf("-park-each recorded a park event %q", s.Name)
				}
			}
			if st, ok := rep.StatFor(EventSuspend, ""); !ok || st.Count == 0 {
				t.Fatal("no suspend events: the fallback would not be exercised")
			}
			if rep.ParkMode != string(park) {
				t.Fatalf("park_mode %q, want %s from the park label", rep.ParkMode, park)
			}
			if rep.LifecyclePolicy == nil || rep.LifecyclePolicy.ParkMode != string(park) {
				t.Fatalf("lifecycle policy %+v, want park_mode %s", rep.LifecyclePolicy, park)
			}
			js, err := json.Marshal(rep)
			if err != nil {
				t.Fatal(err)
			}
			if want := `"park_mode":"` + string(park) + `"`; !strings.Contains(string(js), want) {
				t.Fatalf("report.json lacks %s", want)
			}
			rep.AttachClusterTelemetry(&schema.SystemDescription{SchemaVersion: schema.SchemaVersion}, nil)
			var md bytes.Buffer
			rep.WriteMarkdown(&md)
			if want := "(`park_mode=" + string(park) + "`"; !strings.Contains(md.String(), want) {
				t.Fatalf("lifecycle table lacks %s:\n%s", want, md.String())
			}
		})
	}
}

func TestMultiJobRunTagsMergeAndClusterTelemetry(t *testing.T) {
	t0 := time.Date(2026, 11, 6, 18, 0, 0, 0, time.UTC)
	imgR2E1 := "us-central1-docker.pkg.dev/p/r2e/a@sha256:" + strings.Repeat("a", 64)
	imgR2E2 := "us-central1-docker.pkg.dev/p/r2e/b@sha256:" + strings.Repeat("b", 64)
	imgSWE1 := "us-central1-docker.pkg.dev/p/swe/c@sha256:" + strings.Repeat("c", 64)

	events := []Event{
		{Time: t0.Add(10 * time.Millisecond), RunID: "arm-r2e", Job: "r2e", Task: 0, Step: 1, Instance: "r2e-1", Image: imgR2E1, Env: "arm-r2e-0", Event: EventTask, DurMS: 100, OK: true, Reward: intp(1)},
		{Time: t0.Add(20 * time.Millisecond), RunID: "arm-r2e", Job: "r2e", Task: 1, Step: 1, Instance: "r2e-2", Image: imgR2E2, Env: "arm-r2e-1", Event: EventTask, DurMS: 110, OK: true, Reward: intp(1)},
		{Time: t0.Add(30 * time.Millisecond), RunID: "arm-swe", Job: "swe", Task: 0, Step: 1, Instance: "swe-1", Image: imgSWE1, Env: "arm-swe-0", Event: EventTask, DurMS: 120, OK: true, Reward: intp(1)},
		{
			Time: t0.Add(150 * time.Millisecond), RunID: "arm-r2e", Job: "r2e", Event: EventRun, Name: "steps", DurMS: 150, OK: true, Count: 4,
			Labels: map[string]string{"mode": "steps", "strategy": "naive", "park": "suspend"},
			Tags: map[string]string{
				"dataset":                    "/var/run/ko/r2e-dataset.jsonl",
				"guest_image":                imgR2E1,
				"load_tasks":                 "2",
				"load_group_size":            "2",
				"load_steps":                 "1",
				"load_tasks_per_step":        "2",
				"load_trajectories_per_step": "4",
				"load_max_concurrent":        "120",
				"load_distinct_images":       "2",
				"load_repeat":                "1",
			},
		},
		{
			Time: t0.Add(180 * time.Millisecond), RunID: "arm-swe", Job: "swe", Event: EventRun, Name: "steps", DurMS: 180, OK: true, Count: 2,
			Labels: map[string]string{"mode": "steps", "strategy": "naive", "park": "suspend"},
			Tags: map[string]string{
				"dataset":                    "/var/run/ko/swe-dataset.jsonl",
				"guest_image":                imgSWE1,
				"load_tasks":                 "1",
				"load_group_size":            "2",
				"load_steps":                 "1",
				"load_tasks_per_step":        "1",
				"load_trajectories_per_step": "2",
				"load_max_concurrent":        "80",
				"load_distinct_images":       "1",
				"load_repeat":                "1",
			},
		},
	}

	rep := Build(events)
	if got := rep.RunTags["dataset"]; got != "r2e-dataset+swe-dataset" {
		t.Fatalf("merged dataset tag = %q, want r2e-dataset+swe-dataset", got)
	}
	if got := rep.RunTags["load_max_concurrent"]; got != "200" {
		t.Fatalf("load_max_concurrent = %q, want 200", got)
	}
	if got := rep.RunTags["load_trajectories_per_step"]; got != "6" {
		t.Fatalf("load_trajectories_per_step = %q, want 6", got)
	}
	if got := rep.RunTags["load_distinct_images"]; got != "3" {
		t.Fatalf("load_distinct_images = %q, want 3", got)
	}

	sys := &schema.SystemDescription{SchemaVersion: schema.SchemaVersion}
	rep.AttachClusterTelemetry(sys, nil)
	if sys.DatasetRevision != "r2e-dataset+swe-dataset" {
		t.Fatalf("sys.DatasetRevision = %q, want r2e-dataset+swe-dataset", sys.DatasetRevision)
	}
	if sys.Load == nil || sys.Load.Tasks != 3 || sys.Load.GroupSize != 2 || sys.Load.TrajectoriesPerStep != 6 || sys.Load.MaxConcurrent != 200 || sys.Load.DistinctImages != 3 {
		t.Fatalf("unexpected sys.Load: %+v", sys.Load)
	}
}

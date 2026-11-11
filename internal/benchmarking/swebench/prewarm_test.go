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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
	"testing"
	"time"
)

func prewarmEvents(t *testing.T, buf *bytes.Buffer) []Event {
	t.Helper()
	var events []Event
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var e Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("event %q: %v", line, err)
		}
		events = append(events, e)
	}
	return events
}

// PrewarmTemplates ensures the derived templates and waits for the goldens
// without creating an environment: ready, failed and disabled goldens each
// land in their image's result and event.
func TestPrewarmTemplatesEnsuresAndWaitsForGoldens(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 2
	api.goldenFail = map[string]string{pfImgB: "GoldenActorCrashed: boom"}
	var buf bytes.Buffer
	results, err := PrewarmTemplates(context.Background(), newFakeBackend(t, api), []string{pfImgA, pfImgB}, PrewarmOptions{
		WaitGolden: true, GoldenPoll: time.Millisecond, Recorder: NewRecorder(&buf),
	})
	if err == nil || !strings.Contains(err.Error(), "prewarm "+pfImgB) || !strings.Contains(err.Error(), "golden snapshot failed: GoldenActorCrashed") {
		t.Fatalf("err = %v, want b's failed golden", err)
	}
	if len(results) != 2 || !results[0].OK() || results[0].Golden != GoldenReady || results[0].Tag == "" || results[0].Template == "" {
		t.Errorf("a = %+v, want a ready golden with its tag and template", results[0])
	}
	if results[1].OK() || results[1].Golden != GoldenFailed {
		t.Errorf("b = %+v, want a failed golden", results[1])
	}
	if len(api.created) != 0 {
		t.Errorf("environments created = %v, want none: the prewarm ensures templates only", api.created)
	}
	if len(api.ensured) != 2 {
		t.Errorf("ensured = %v, want both images", api.ensured)
	}
	events := prewarmEvents(t, &buf)
	if len(events) != 2 || events[0].Event != EventPrewarm || !events[0].OK || events[0].Labels["golden"] != "ready" || events[0].Labels["golden_tag"] == "" || events[0].Image != pfImgA {
		t.Errorf("a's event = %+v", events[0])
	}
	if events[1].OK || events[1].Labels["golden"] != "failed" || !strings.Contains(events[1].Err, "GoldenActorCrashed") || events[1].Labels["golden_tag"] != "" {
		t.Errorf("b's event = %+v, want the failure in err and no golden_tag", events[1])
	}
}

// A status poll the control plane cuts off keeps the image pending for the
// next poll; a template that is gone between polls never settles.
func TestPrewarmTemplatesPollFailures(t *testing.T) {
	api := newPreflightFake()
	api.goldenReadyAfter = 2
	api.statusUnavailableFirst = map[string]int{pfImgA: 1}
	results, err := PrewarmTemplates(context.Background(), newFakeBackend(t, api), []string{pfImgA}, PrewarmOptions{WaitGolden: true, GoldenPoll: time.Millisecond})
	if err != nil || !results[0].OK() || results[0].Golden != GoldenReady {
		t.Errorf("Unavailable once then ready: err=%v result=%+v, want ok", err, results[0])
	}

	api = newPreflightFake()
	api.goldenReadyAfter = 100
	api.templateGone = map[string]bool{pfImgA: true}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	results, err = PrewarmTemplates(ctx, newFakeBackend(t, api), []string{pfImgA}, PrewarmOptions{WaitGolden: true, GoldenPoll: time.Millisecond})
	if err == nil || results[0].OK() || !strings.Contains(err.Error(), "the derived template is gone") {
		t.Errorf("template gone during the bake: err=%v result=%+v, want a failure at the deadline", err, results[0])
	}

	// A poll that fails until the deadline names the last poll's error.
	api = newPreflightFake()
	api.goldenReadyAfter = 100
	api.statusUnavailableFirst = map[string]int{pfImgA: 1 << 30}
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	results, err = PrewarmTemplates(ctx, newFakeBackend(t, api), []string{pfImgA}, PrewarmOptions{WaitGolden: true, GoldenPoll: time.Millisecond})
	if err == nil || results[0].OK() || !strings.Contains(err.Error(), "still pending") || !strings.Contains(err.Error(), "(last poll: ") || !strings.Contains(err.Error(), "api server restarting") {
		t.Errorf("Unavailable until the deadline: err=%v result=%+v, want a timeout that names the last poll", err, results[0])
	}

	// A poll the control plane refuses is final: the image fails at that
	// poll, long before the deadline.
	for _, code := range []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated} {
		api = newPreflightFake()
		api.goldenReadyAfter = 100
		api.templates = map[string]bool{}
		b := newFakeBackend(t, api)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		results, err = PrewarmTemplates(ctx, refusingStatus{ImageWarmer: b, code: code}, []string{pfImgA}, PrewarmOptions{WaitGolden: true, GoldenPoll: time.Millisecond})
		cancel()
		if err == nil || results[0].OK() || status.Code(results[0].Err) != code || strings.Contains(err.Error(), "still pending") {
			t.Errorf("poll refused with %s: err=%v result=%+v, want a failure at once", code, err, results[0])
		}
	}
}

// refusingStatus answers every ImageStatus with code.
type refusingStatus struct {
	ImageWarmer
	code codes.Code
}

func (r refusingStatus) ImageStatus(context.Context, string) (ImageState, error) {
	return ImageState{}, status.Error(r.code, "refused: the base template is gone")
}

// stubWarmer answers the two template calls from functions, without a server.
type stubWarmer struct {
	ImageWarmer
	ensure, status func(image string) (ImageState, error)
}

func (s stubWarmer) EnsureImageTemplate(_ context.Context, image string) (ImageState, error) {
	return s.ensure(image)
}

func (s stubWarmer) ImageStatus(_ context.Context, image string) (ImageState, error) {
	return s.status(image)
}

// An ensure that answers without a template fails the image at once: there
// is nothing to bake from, so no poll follows.
func TestPrewarmTemplatesEnsureWithoutATemplate(t *testing.T) {
	polls := 0
	w := stubWarmer{
		ensure: func(string) (ImageState, error) {
			return ImageState{Template: "default-template-abc", Golden: GoldenNone}, nil
		},
		status: func(string) (ImageState, error) {
			polls++
			return ImageState{Template: "default-template-abc", Golden: GoldenNone}, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := PrewarmTemplates(ctx, w, []string{pfImgA}, PrewarmOptions{WaitGolden: true, GoldenPoll: time.Millisecond})
	if err == nil || results[0].OK() || !strings.Contains(err.Error(), "does not exist after the ensure") {
		t.Errorf("ensure without a template: err=%v result=%+v, want a failure", err, results[0])
	}
	if polls != 0 {
		t.Errorf("status polls = %d, want none: the image fails at the ensure", polls)
	}
}

// Each image's duration and event time end when that image settles: an
// image that is ready at its ensure does not inherit the bake of a slower
// one.
func TestPrewarmTemplatesTimesEachImageToItsOwnSettle(t *testing.T) {
	// A clock that moves one second per reading.
	var mu sync.Mutex
	clock := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		clock = clock.Add(time.Second)
		return clock
	}
	polls := 0
	w := stubWarmer{
		ensure: func(image string) (ImageState, error) {
			if image == pfImgA {
				return ImageState{Template: "a", Exists: true, Golden: GoldenReady, Tag: "golden-a"}, nil
			}
			return ImageState{Template: "b", Exists: true, Golden: GoldenPending}, nil
		},
		status: func(string) (ImageState, error) {
			if polls++; polls < 5 {
				return ImageState{Template: "b", Exists: true, Golden: GoldenPending}, nil
			}
			return ImageState{Template: "b", Exists: true, Golden: GoldenReady, Tag: "golden-b"}, nil
		},
	}
	var buf bytes.Buffer
	results, err := PrewarmTemplates(context.Background(), w, []string{pfImgA, pfImgB}, PrewarmOptions{
		WaitGolden: true, GoldenPoll: time.Millisecond, Recorder: NewRecorder(&buf), now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	// a: one reading before its ensure and one after it.
	if results[0].Duration != time.Second {
		t.Errorf("a's duration = %s, want 1s: the time of its own ensure", results[0].Duration)
	}
	if results[1].Duration <= results[0].Duration {
		t.Errorf("b's duration = %s, want more than a's %s: b waited for its bake", results[1].Duration, results[0].Duration)
	}
	events := prewarmEvents(t, &buf)
	if len(events) != 2 {
		t.Fatalf("events = %+v, want two", events)
	}
	if events[0].DurMS != 1000 || events[0].DurMS >= events[1].DurMS {
		t.Errorf("dur_ms = %v and %v, want 1000 for a and more for b", events[0].DurMS, events[1].DurMS)
	}
	if !events[0].Time.Before(events[1].Time) {
		t.Errorf("event times = %s and %s, want a's before b's: each is the moment its image settled", events[0].Time, events[1].Time)
	}
}

// A failed image does not hide an events file that could not be written:
// the error returned carries both.
func TestPrewarmTemplatesJoinsTheImageAndTheFlushError(t *testing.T) {
	errImage := errors.New("image refused")
	w := stubWarmer{ensure: func(string) (ImageState, error) { return ImageState{}, errImage }}
	_, err := PrewarmTemplates(context.Background(), w, []string{pfImgA}, PrewarmOptions{Recorder: NewRecorder(failingWriter{})})
	if !errors.Is(err, errImage) || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("err = %v, want the image's error and the flush error", err)
	}
	if err == nil || !strings.Contains(err.Error(), "prewarm "+pfImgA) {
		t.Errorf("err = %v, want the failing image named", err)
	}

	// Without a failed image the flush error is the error.
	ok := stubWarmer{ensure: func(string) (ImageState, error) {
		return ImageState{Template: "a", Exists: true, Golden: GoldenPending}, nil
	}}
	_, err = PrewarmTemplates(context.Background(), ok, []string{pfImgA}, PrewarmOptions{Recorder: NewRecorder(failingWriter{})})
	if err == nil || err.Error() != "disk full" {
		t.Errorf("err = %v, want the flush error", err)
	}
}

func TestPrewarmTemplatesWithoutTheWaitAndWithGoldensDisabled(t *testing.T) {
	// Templates only: the golden is still pending when the prewarm returns.
	api := newPreflightFake()
	api.goldenReadyAfter = 100
	results, err := PrewarmTemplates(context.Background(), newFakeBackend(t, api), []string{pfImgA}, PrewarmOptions{GoldenPoll: time.Millisecond})
	if err != nil || !results[0].OK() || results[0].Golden != GoldenPending || results[0].Template == "" {
		t.Errorf("without the wait: err=%v result=%+v, want ok with a pending golden", err, results[0])
	}
	if len(api.created) != 0 {
		t.Errorf("environments created = %v, want none", api.created)
	}

	// Goldens disabled on the base template: none settles at once.
	api = newPreflightFake()
	api.goldenDisabled = true
	results, err = PrewarmTemplates(context.Background(), newFakeBackend(t, api), []string{pfImgA}, PrewarmOptions{WaitGolden: true, GoldenPoll: time.Millisecond})
	if err != nil || !results[0].OK() || results[0].Golden != GoldenNone {
		t.Errorf("goldens disabled: err=%v result=%+v, want ok with golden none", err, results[0])
	}

	// A golden that never settles fails the image when the timeout ends.
	api = newPreflightFake()
	api.goldenPendingForever = map[string]bool{pfImgA: true}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	results, err = PrewarmTemplates(ctx, newFakeBackend(t, api), []string{pfImgA}, PrewarmOptions{WaitGolden: true, GoldenPoll: time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "still pending") || results[0].OK() {
		t.Errorf("golden pending forever: err=%v result=%+v, want a timeout failure", err, results[0])
	}
}

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
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PrewarmOptions configure PrewarmTemplates.
type PrewarmOptions struct {
	// WaitGolden waits for each image's golden snapshot to settle (ready,
	// failed, or none when the base template disables goldens) within ctx;
	// without it an image is done once its derived template exists.
	WaitGolden bool
	// GoldenPoll is how often the baking goldens are polled; default 10 s.
	GoldenPoll time.Duration
	// Recorder receives one prewarm event per image; nil records nothing.
	Recorder *Recorder
	now      func() time.Time
}

// PrewarmResult is what PrewarmTemplates did for one image: its derived
// template (Exists says whether it is there), the golden's state, tag and
// failure reason, and Err when the image is not ready for a warm footing.
// Duration runs from the image's ensure to the moment the image settled
// (or to the deadline), not to the end of the whole prewarm.
type PrewarmResult struct {
	Image       string
	Template    string
	Exists      bool
	Golden      GoldenState
	Tag         string
	GoldenError string
	Duration    time.Duration
	Err         error
	pollErr     error
}

// OK reports whether the image is ready for a warm footing: its derived
// template exists and, when waited for, its golden settled ready or none.
func (r PrewarmResult) OK() bool { return r.Err == nil }

// PrewarmTemplates prepares images for a run without creating an
// environment: it ensures each image's derived template, which starts its
// golden bake, and with WaitGolden waits for the goldens to settle. The
// layers of an image reach a node only when an environment boots there;
// the pre-flight (PreflightWarmer) does that per placement domain. One
// prewarm event per image carries its template, golden state and tag, and
// the time and duration of the moment the image settled. A status poll
// that fails keeps the image pending and is retried at the next poll, as
// the pre-flight does, so an api server restart during a bake does not
// fail the image; InvalidArgument, FailedPrecondition, PermissionDenied
// and Unauthenticated are final. The error returned is the first failing
// image's, after every image was tried, joined with the recorder's flush
// error when the events did not reach the writer.
func PrewarmTemplates(ctx context.Context, w ImageWarmer, images []string, opts PrewarmOptions) ([]PrewarmResult, error) {
	if opts.GoldenPoll <= 0 {
		opts.GoldenPoll = 10 * time.Second
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	results := make([]PrewarmResult, len(images))
	starts := make([]time.Time, len(images))
	// ends[i] is when image i settled: after its ensure, or at the poll (or
	// the deadline) that took it out of pending.
	ends := make([]time.Time, len(images))
	pending := map[int]bool{}
	for i, img := range images {
		starts[i] = opts.now()
		results[i] = PrewarmResult{Image: img}
		st, err := w.EnsureImageTemplate(ctx, img)
		ends[i] = opts.now()
		if err != nil {
			results[i].Err = err
			continue
		}
		settle(&results[i], st)
		if !st.Exists {
			// The ensure answered without a template: nothing to bake from.
			results[i].Err = errors.New("the derived template does not exist after the ensure")
			continue
		}
		if opts.WaitGolden && !goldenSettled(st) {
			pending[i] = true
		}
	}
	for len(pending) > 0 && ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-time.After(opts.GoldenPoll):
		}
		if ctx.Err() != nil {
			break
		}
		for i := range pending {
			st, err := w.ImageStatus(ctx, images[i])
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				// A poll the control plane refused is the image's fault; any
				// other failure (a restart, a deadline on the call) keeps
				// the image pending for the next poll.
				switch status.Code(err) {
				case codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated:
					results[i].Err = err
					ends[i] = opts.now()
					delete(pending, i)
				default:
					results[i].pollErr = err
				}
				continue
			}
			results[i].pollErr = nil
			settle(&results[i], st)
			// A template that is gone between two polls (golden none without
			// a template) stays pending: the deadline reports it.
			if goldenSettled(st) && st.Exists {
				ends[i] = opts.now()
				delete(pending, i)
			}
		}
	}
	for i := range pending {
		state := string(results[i].Golden)
		if !results[i].Exists {
			state = "none, and the derived template is gone"
		}
		ends[i] = opts.now()
		results[i].Err = fmt.Errorf("golden snapshot still %s after %s: %w", state, ends[i].Sub(starts[i]).Round(time.Second), context.Cause(ctx))
		if results[i].pollErr != nil {
			results[i].Err = fmt.Errorf("%w (last poll: %v)", results[i].Err, results[i].pollErr)
		}
	}
	var first error
	for i := range results {
		r := &results[i]
		r.Duration = ends[i].Sub(starts[i])
		if r.Err == nil && opts.WaitGolden && r.Golden == GoldenFailed {
			r.Err = fmt.Errorf("golden snapshot failed: %s", lastNonEmpty(r.GoldenError, "no reason"))
		}
		if r.Err != nil && first == nil {
			first = fmt.Errorf("prewarm %s: %w", r.Image, r.Err)
		}
		if opts.Recorder != nil {
			ev := Event{
				Time:   ends[i].UTC(),
				Task:   i,
				Image:  r.Image,
				Event:  EventPrewarm,
				DurMS:  DurMS(r.Duration),
				OK:     r.Err == nil,
				Err:    errString(r.Err),
				Labels: map[string]string{"template": r.Template, "golden": string(r.Golden)},
			}
			if r.Tag != "" {
				ev.Labels["golden_tag"] = r.Tag
			}
			opts.Recorder.Record(ev)
		}
	}
	if opts.Recorder != nil {
		if err := opts.Recorder.Flush(); err != nil {
			if first == nil {
				first = err
			} else {
				first = errors.Join(first, err)
			}
		}
	}
	return results, first
}

func settle(r *PrewarmResult, st ImageState) {
	r.Template = st.Template
	r.Exists = st.Exists
	r.Golden = st.Golden
	r.Tag = st.Tag
	r.GoldenError = st.Error
}

func goldenSettled(st ImageState) bool {
	return st.Golden == GoldenReady || st.Golden == GoldenFailed || st.Golden == GoldenNone
}

func lastNonEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

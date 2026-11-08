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

package apiservice

import (
	"testing"
	"time"
)

// SetWarmTimings shortens the backoff between the attempts of a WarmImage
// probe's create, resume and delete, and the budget of its delete, for the
// duration of t.
func SetWarmTimings(t *testing.T, retryInitial, retryMax, deleteTimeout time.Duration) {
	oldInitial, oldMax, oldDelete := warmRetryInitial, warmRetryMax, warmDeleteTimeout
	warmRetryInitial, warmRetryMax, warmDeleteTimeout = retryInitial, retryMax, deleteTimeout
	t.Cleanup(func() { warmRetryInitial, warmRetryMax, warmDeleteTimeout = oldInitial, oldMax, oldDelete })
}

// SetTemplateRecreateTimeout sets the bound of the recreate loop for the
// duration of t.
func SetTemplateRecreateTimeout(t *testing.T, d time.Duration) {
	old := templateRecreateTimeout
	templateRecreateTimeout = d
	t.Cleanup(func() { templateRecreateTimeout = old })
}

// WarmProbesInFlight reports the probe slots WarmImage calls hold on this
// server, over every selector, for tests that wait for a cancelled call's
// cleanup to end.
func (s *Server) WarmProbesInFlight() int {
	s.warmSlots.mu.Lock()
	defer s.warmSlots.mu.Unlock()
	total := 0
	for _, w := range s.warmSlots.m {
		w.mu.Lock()
		total += w.inflight
		w.mu.Unlock()
	}
	return total
}

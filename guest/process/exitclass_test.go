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

package process

import (
	"sync"
	"testing"
	"time"
)

// OnExit is called once per reaped process with the exit class: zero,
// nonzero, signal, or timeout when the tracker's timeout killed it.
func TestOnExitReportsExitClass(t *testing.T) {
	var mu sync.Mutex
	got := map[string]time.Duration{}
	cfg := DefaultConfig(t.TempDir())
	cfg.Workspace = t.TempDir()
	cfg.OnExit = func(d time.Duration, class string) {
		mu.Lock()
		defer mu.Unlock()
		got[class] = d
	}
	tracker, err := NewTracker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tracker.Close)

	cases := []struct {
		opts StartOptions
		want string
	}{
		{StartOptions{Command: []string{"true"}}, ExitClassZero},
		{StartOptions{Command: []string{"false"}}, ExitClassNonzero},
		{StartOptions{Command: []string{"sh", "-c", "kill -TERM $$"}}, ExitClassSignal},
		{StartOptions{Command: []string{"sleep", "30"}, Timeout: 200 * time.Millisecond}, ExitClassTimeout},
	}
	for _, tc := range cases {
		st, err := tracker.Start(tc.opts)
		if err != nil {
			t.Fatalf("%v: %v", tc.opts.Command, err)
		}
		select {
		case <-st.Done():
		case <-time.After(10 * time.Second):
			t.Fatalf("%v did not exit", tc.opts.Command)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, tc := range cases {
		if _, ok := got[tc.want]; !ok {
			t.Errorf("%v: no OnExit with class %q (got %v)", tc.opts.Command, tc.want, got)
		}
	}
	if d := got[ExitClassTimeout]; d < 150*time.Millisecond || d > 10*time.Second {
		t.Errorf("timeout run time = %s, want about the 200ms timeout", d)
	}
}

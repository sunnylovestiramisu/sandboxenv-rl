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
	"syscall"
	"testing"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
)

// ToProto reports timed_out only for an exited process that a signal killed
// after the timeout fired. The timer can fire between cmd.Wait and
// timer.Stop in reap, after the process exited on its own; that exit is not
// a timeout (the same guard as exitClassOf).
func TestToProtoTimedOut(t *testing.T) {
	cases := []struct {
		name     string
		state    ateenvv1alpha.ProcessState
		timedOut bool
		signal   syscall.Signal
		want     *bool // nil: field absent
	}{
		{"timer fired, killed", ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, true, syscall.SIGKILL, ptr(true)},
		{"timer fired after a normal exit", ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, true, 0, ptr(false)},
		{"killed by another signal", ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, false, syscall.SIGKILL, ptr(false)},
		{"exited on its own", ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, false, 0, ptr(false)},
		{"running", ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING, true, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &ProcessState{State: tc.state, Signal: tc.signal}
			p.timedOut.Store(tc.timedOut)
			got := p.ToProto().TimedOut
			switch {
			case tc.want == nil && got != nil:
				t.Errorf("TimedOut = %v, want absent", *got)
			case tc.want != nil && got == nil:
				t.Errorf("TimedOut absent, want %v", *tc.want)
			case tc.want != nil && *got != *tc.want:
				t.Errorf("TimedOut = %v, want %v", *got, *tc.want)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

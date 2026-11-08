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

package sidecarenv

import (
	"os"
	"slices"
	"testing"
)

func TestTake(t *testing.T) {
	t.Setenv(Prefix+"KEY", "a=b")
	t.Setenv(Prefix+"EMPTY", "")
	t.Setenv(Prefix, "nameless")
	t.Setenv("PLAIN", "stays")

	got := Take()
	slices.Sort(got)
	if want := []string{"EMPTY=", "KEY=a=b"}; !slices.Equal(got, want) {
		t.Errorf("Take = %q, want %q", got, want)
	}
	for _, name := range []string{Prefix + "KEY", Prefix + "EMPTY", Prefix} {
		if v, ok := os.LookupEnv(name); ok {
			t.Errorf("%s=%q is still in the environment", name, v)
		}
	}
	if os.Getenv("PLAIN") != "stays" {
		t.Error("Take removed a variable without the prefix")
	}
	if got := Take(); len(got) != 0 {
		t.Errorf("second Take = %q, want nothing", got)
	}
}

func TestNames(t *testing.T) {
	got := Names([]string{"KEY=a=b", "EMPTY="})
	if want := []string{"KEY", "EMPTY"}; !slices.Equal(got, want) {
		t.Errorf("Names = %q, want %q", got, want)
	}
	if got := Names(nil); len(got) != 0 {
		t.Errorf("Names(nil) = %q, want nothing", got)
	}
}

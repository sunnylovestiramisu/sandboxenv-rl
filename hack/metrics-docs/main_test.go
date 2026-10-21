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

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/agent-substrate/env/internal/telemetry"
)

// docs/metrics.md is what the generator prints for the current definitions;
// regenerate it with `go run ./hack/metrics-docs > docs/metrics.md`.
func TestMetricsDocIsFresh(t *testing.T) {
	var buf bytes.Buffer
	render(&buf)
	committed, err := os.ReadFile("../../docs/metrics.md")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), committed) {
		t.Error("docs/metrics.md is stale; run: go run ./hack/metrics-docs > docs/metrics.md")
	}
	for _, d := range telemetry.Definitions {
		if !strings.Contains(buf.String(), "`"+d.Name+"`") {
			t.Errorf("rendered doc lacks %s", d.Name)
		}
	}
}

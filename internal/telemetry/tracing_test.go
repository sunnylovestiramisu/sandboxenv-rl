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

package telemetry

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestExportEnabled(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, tracesEndpoint, exporter string
		want                                     bool
	}{
		{name: "no endpoint"},
		{name: "endpoint", endpoint: "http://collector:4317", want: true},
		{name: "traces endpoint", tracesEndpoint: "http://collector:4317", want: true},
		{name: "exporter none", endpoint: "http://collector:4317", exporter: "none"},
		{name: "exporter list with none", endpoint: "http://c:4317", exporter: "otlp, None"},
		{name: "exporter otlp", endpoint: "http://c:4317", exporter: "otlp", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.endpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tc.tracesEndpoint)
			t.Setenv("OTEL_TRACES_EXPORTER", tc.exporter)
			if got := exportEnabled(); got != tc.want {
				t.Errorf("exportEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInitTracingWithoutEndpointPropagatesOnly(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	tr, err := InitTracing(context.Background(), "ate-env-api")
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}
	if tr.Exporting {
		t.Error("Exporting = true without an endpoint")
	}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	fields := otel.GetTextMapPropagator().Fields()
	for _, want := range []string{"traceparent", "baggage"} {
		if !slices.Contains(fields, want) {
			t.Errorf("propagator fields %v missing %q", fields, want)
		}
	}
}

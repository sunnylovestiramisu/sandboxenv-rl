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

package schema

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

func TestEmbeddedSchemasResolve(t *testing.T) {
	schemas := []string{
		"scenario.schema.json",
		"schedule.schema.json",
		"placement.schema.json",
		"event.schema.json",
		"run_window.schema.json",
		"measurements.schema.json",
		"node_cache.schema.json",
		"restore_sample.schema.json",
		"report.schema.json",
		"stats_row.schema.json",
		"system_description.schema.json",
		"trace_event.schema.json",
		"image_placement.schema.json",
		"preflight_coverage.schema.json",
	}
	for _, name := range schemas {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadJSONSchema(name); err != nil {
				t.Fatalf("LoadJSONSchema(%q) failed: %v", name, err)
			}
		})
	}
}

func TestFixturesValidateAndRoundTrip(t *testing.T) {
	t.Run("scenario.yaml", func(t *testing.T) {
		rawYAML := mustRead(t, "testdata/scenario.yaml")
		jsonBytes, err := yaml.YAMLToJSON(rawYAML)
		if err != nil {
			t.Fatalf("yaml.YAMLToJSON: %v", err)
		}
		if err := ValidateAgainstJSONSchema("scenario.schema.json", jsonBytes); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeYAMLStrict[Scenario](rawYAML)
		if err != nil {
			t.Fatalf("DecodeYAMLStrict: %v", err)
		}
		marshaledYAML, err := yaml.Marshal(v)
		if err != nil {
			t.Fatalf("yaml.Marshal: %v", err)
		}
		rt, err := DecodeYAMLStrict[Scenario](marshaledYAML)
		if err != nil {
			t.Fatalf("round-trip DecodeYAMLStrict: %v", err)
		}
		if !reflect.DeepEqual(v, rt) {
			t.Fatalf("Scenario round-trip mismatch:\ngot:  %+v\nwant: %+v", rt, v)
		}
	})

	t.Run("schedule.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/schedule.json")
		if err := ValidateAgainstJSONSchema("schedule.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[Schedule](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "schedule.schema.json", v)
	})

	t.Run("preflight_coverage.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/preflight_coverage.json")
		if err := ValidateAgainstJSONSchema("preflight_coverage.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[PreflightCoverage](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		roundTripJSON(t, "preflight_coverage.schema.json", v)
	})

	t.Run("preflight_coverage validate bounds", func(t *testing.T) {
		load := func() PreflightCoverage {
			v, err := DecodeJSONStrict[PreflightCoverage](bytes.NewReader(mustRead(t, "testdata/preflight_coverage.json")))
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
		good := load()
		good.Domains[0].Images[0].UndeletedProbes = []string{"pf-x-0-0-1-0"}
		good.Domains[0].Images[0].WaitedMS = 5
		if err := good.Validate(); err != nil {
			t.Errorf("undeleted_probes and waited_ms: %v", err)
		}
		bad := load()
		bad.Domains[0].Images[0].UndeletedProbes = []string{""}
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "empty undeleted probe id") {
			t.Errorf("empty undeleted probe id: err = %v", err)
		}
		for _, w := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			v := load()
			v.Domains[0].Images[0].WaitedMS = w
			if err := v.Validate(); err == nil || !strings.Contains(err.Error(), "waited_ms must be a finite number >= 0") {
				t.Errorf("waited_ms %v: err = %v", w, err)
			}
		}
	})

	t.Run("image_placement.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/image_placement.json")
		if err := ValidateAgainstJSONSchema("image_placement.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[ImagePlacement](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		roundTripJSON(t, "image_placement.schema.json", v)
	})

	t.Run("placement.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/placement.json")
		if err := ValidateAgainstJSONSchema("placement.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[PlacementPlan](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "placement.schema.json", v)
	})

	t.Run("event.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/event.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("event.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[Event](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("len(items) = %d, want 2", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "event.schema.json", item)
		}
	})

	t.Run("run_window.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/run_window.json")
		if err := ValidateAgainstJSONSchema("run_window.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[RunWindow](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "run_window.schema.json", v)
	})

	t.Run("measurements.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/measurements.json")
		if err := ValidateAgainstJSONSchema("measurements.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[Measurements](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "measurements.schema.json", v)
	})

	t.Run("cache_before.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/cache_before.json")
		if err := ValidateAgainstJSONSchema("node_cache.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[NodeCacheArtifact](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "node_cache.schema.json", v)
	})

	t.Run("restore_timings.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/restore_timings.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("restore_sample.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[RestoreSample](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("len(items) = %d, want 2", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "restore_sample.schema.json", item)
		}
	})

	t.Run("report.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/report.json")
		if err := ValidateAgainstJSONSchema("report.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[Report](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "report.schema.json", v)
	})

	t.Run("stats.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/stats.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("stats_row.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[StatsRow](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("len(items) = %d, want 2", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "stats_row.schema.json", item)
		}
	})

	t.Run("system_description.json", func(t *testing.T) {
		raw := mustRead(t, "testdata/system_description.json")
		if err := ValidateAgainstJSONSchema("system_description.schema.json", raw); err != nil {
			t.Fatalf("schema validate: %v", err)
		}
		v, err := DecodeJSONStrict[SystemDescription](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONStrict: %v", err)
		}
		roundTripJSON(t, "system_description.schema.json", v)
	})

	t.Run("trace_event.jsonl", func(t *testing.T) {
		raw := mustRead(t, "testdata/trace_event.jsonl")
		if err := ValidateJSONLAgainstJSONSchema("trace_event.schema.json", bytes.NewReader(raw)); err != nil {
			t.Fatalf("schema validate jsonl: %v", err)
		}
		items, err := DecodeJSONLStrict[TraceEvent](bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("DecodeJSONLStrict: %v", err)
		}
		if len(items) != 4 {
			t.Fatalf("len(items) = %d, want 4", len(items))
		}
		for _, item := range items {
			roundTripJSON(t, "trace_event.schema.json", item)
		}
	})
}

func TestBaselineEventStreamsDecode(t *testing.T) {
	for _, tc := range []struct {
		file      string
		wantCount int
	}{
		{file: "testdata/baseline-eval.jsonl", wantCount: 29},
		{file: "testdata/baseline-steps.jsonl", wantCount: 58},
	} {
		t.Run(tc.file, func(t *testing.T) {
			raw := mustRead(t, tc.file)
			if err := ValidateJSONLAgainstJSONSchema("event.schema.json", bytes.NewReader(raw)); err != nil {
				t.Fatalf("ValidateJSONLAgainstJSONSchema(%s): %v", tc.file, err)
			}
			events, err := DecodeJSONLStrict[Event](bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("DecodeJSONLStrict(%s): %v", tc.file, err)
			}
			if len(events) != tc.wantCount {
				t.Fatalf("len(events) = %d, want %d", len(events), tc.wantCount)
			}
			for i, ev := range events {
				data, err := json.Marshal(ev)
				if err != nil {
					t.Fatalf("event %d marshal: %v", i, err)
				}
				if err := ValidateAgainstJSONSchema("event.schema.json", data); err != nil {
					t.Fatalf("event %d re-validate: %v", i, err)
				}
			}
		})
	}
}

func TestStrictDecodeRejectsUnknownField(t *testing.T) {
	t.Run("JSON unknown field", func(t *testing.T) {
		payload := `{"schema_version":"v1","run_id":"r1","start_ts":"2026-10-22T01:20:00Z","end_ts":"2026-10-22T01:25:00Z","bogus":123}`
		if _, err := DecodeJSONStrict[RunWindow](strings.NewReader(payload)); err == nil {
			t.Fatal("expected error for unknown JSON field, got nil")
		}
		if err := ValidateAgainstJSONSchema("run_window.schema.json", []byte(payload)); err == nil {
			t.Fatal("expected JSON Schema error for additional property, got nil")
		}
	})

	t.Run("YAML unknown field", func(t *testing.T) {
		payload := []byte(`
schema_version: v1
scenario_name: test
unknown_top_level: true
jobs: []
`)
		if _, err := DecodeYAMLStrict[Schedule](payload); err == nil {
			t.Fatal("expected error for unknown YAML field, got nil")
		}
	})
}

func TestStrictDecodeRejectsTrailingPayload(t *testing.T) {
	payload := `{"schema_version":"v1","run_id":"r1","start_ts":"2026-10-22T01:20:00Z","end_ts":"2026-10-22T01:25:00Z"} {"extra":true}`
	if _, err := DecodeJSONStrict[RunWindow](strings.NewReader(payload)); err == nil {
		t.Fatal("expected error for trailing JSON token, got nil")
	}
	if err := ValidateAgainstJSONSchema("run_window.schema.json", []byte(payload)); err == nil {
		t.Fatal("expected ValidateAgainstJSONSchema error for trailing JSON token, got nil")
	}
}

func TestJSONSchemaRejectsMissingRequiredAndUnknownField(t *testing.T) {
	cases := []struct {
		name       string
		schemaName string
		payload    string
	}{
		{
			name:       "schedule missing jobs",
			schemaName: "schedule.schema.json",
			payload:    `{"schema_version":"v1","scenario_name":"s"}`,
		},
		{
			name:       "placement missing env_id",
			schemaName: "placement.schema.json",
			payload:    `{"schema_version":"v1","scenario_name":"s","jobs":[{"job":"j","assignments":[{"step":1,"instance_id":"i"}]}]}`,
		},
		{
			name:       "event missing dur_ms",
			schemaName: "event.schema.json",
			payload:    `{"ts":"2026-10-22T01:23:23Z","run":"r","event":"create","ok":true}`,
		},
		{
			name:       "event invalid source enum",
			schemaName: "event.schema.json",
			payload:    `{"ts":"2026-10-22T01:23:23Z","run":"r","event":"create","dur_ms":1,"ok":true,"source":"invalid_source"}`,
		},
		{
			name:       "run_window missing end_ts",
			schemaName: "run_window.schema.json",
			payload:    `{"schema_version":"v1","run_id":"r","start_ts":"2026-10-22T01:20:00Z"}`,
		},
		{
			name:       "report missing validity",
			schemaName: "report.schema.json",
			payload:    `{"schema_version":"v1","run_id":"r","scenario_name":"s","generated_at":"2026-10-22T01:20:00Z"}`,
		},
		{
			name:       "stats_row empty measurements",
			schemaName: "stats_row.schema.json",
			payload:    `{"schema_version":"v1","timestamp":"2026-10-22T01:20:00Z","test_name":"t","metric":"m","measurements":{}}`,
		},
		{
			name:       "system_description missing cluster",
			schemaName: "system_description.schema.json",
			payload:    `{"schema_version":"v1"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateAgainstJSONSchema(tc.schemaName, []byte(tc.payload)); err == nil {
				t.Fatalf("expected JSON Schema error for %s, got nil", tc.name)
			}
		})
	}
}

func TestJSONLRejectsMalformedLineWithLineNumber(t *testing.T) {
	input := strings.Join([]string{
		`{"ts":"2026-10-22T01:23:23Z","run":"r","event":"create","dur_ms":10,"ok":true}`,
		`{"ts":"not-a-timestamp","run":"r","event":"create","dur_ms":10,"ok":true}`,
	}, "\n")
	_, err := DecodeJSONLStrict[Event](strings.NewReader(input))
	if err == nil {
		t.Fatal("expected error on line 2, got nil")
	}
	if !strings.Contains(err.Error(), "line 2:") {
		t.Fatalf("expected error to mention 'line 2:', got %v", err)
	}
}

func TestParkPolicyNormalization(t *testing.T) {
	for _, tc := range []struct {
		in      ParkPolicy
		want    ParkPolicy
		wantErr bool
	}{
		{in: "", want: ParkSuspendSync},
		{in: ParkSuspend, want: ParkSuspendSync},
		{in: ParkSuspendSync, want: ParkSuspendSync},
		{in: ParkSuspendAsync, want: ParkSuspendAsync},
		{in: ParkFork, want: ParkFork},
		{in: ParkDelete, want: ParkDelete},
		{in: ParkNone, want: ParkNone},
		{in: "bogus", wantErr: true},
	} {
		got, err := NormalizeParkPolicy(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("NormalizeParkPolicy(%q) succeeded, want error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("NormalizeParkPolicy(%q) failed: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("NormalizeParkPolicy(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestValidationRules(t *testing.T) {
	t.Run("RunWindow end before start", func(t *testing.T) {
		rw := RunWindow{
			SchemaVersion: SchemaVersion,
			RunID:         "r1",
			StartTS:       "2026-10-22T01:25:00Z",
			EndTS:         "2026-10-22T01:20:00Z",
		}
		if err := rw.Validate(); err == nil {
			t.Fatal("expected error when end_ts < start_ts")
		}
	})

	t.Run("Report invalid without reasons", func(t *testing.T) {
		rep := Report{
			SchemaVersion: SchemaVersion,
			RunID:         "r1",
			ScenarioName:  "s1",
			GeneratedAt:   "2026-10-22T01:25:00Z",
			Validity:      ReportValidity{Valid: false},
		}
		if err := rep.Validate(); err == nil {
			t.Fatal("expected error when validity.valid=false and reasons is empty")
		}
	})

	t.Run("StatsRow NaN measurement", func(t *testing.T) {
		row := StatsRow{
			SchemaVersion: SchemaVersion,
			Timestamp:     "2026-10-22T01:25:00Z",
			TestName:      "t",
			Metric:        "m",
			Measurements:  map[string]float64{"x": math.NaN()},
		}
		if err := row.Validate(); err == nil {
			t.Fatal("expected error for NaN measurement")
		}
	})

	t.Run("TraceEvent invalid op", func(t *testing.T) {
		ev := TraceEvent{
			SchemaVersion: SchemaVersion,
			TS:            time.Now().UTC(),
			Env:           "sb-1",
			Op:            "bogus_op",
			DurMS:         10,
			OK:            true,
		}
		if err := ev.Validate(); err == nil {
			t.Fatal("expected error for invalid trace op")
		}
	})

	t.Run("JobSpec and PrometheusSeries invariants", func(t *testing.T) {
		simSteps := JobSpec{
			Name:       "sim-steps",
			Kind:       "simulated",
			LoaderMode: "steps",
			Dataset:    "d.jsonl",
			Trajectory: "t.yaml",
			Steps:      0,
		}
		if err := simSteps.Validate(); err == nil {
			t.Fatal("expected simulated steps job with steps=0 to fail validation")
		}
		if _, err := DecodeJSONStrict[PrometheusSeries](strings.NewReader(`{"name":"m1","status":"ok"}`)); err == nil {
			t.Fatal("expected PrometheusSeries with status=ok and no value to fail")
		}
		if _, err := DecodeJSONStrict[PrometheusSeries](strings.NewReader(`{"name":"m1","status":"error","err":"connection refused"}`)); err != nil {
			t.Fatalf("expected PrometheusSeries with status=error and err message to pass, got: %v", err)
		}
	})
}

func TestPythonSchemaValidatorCLI(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found on PATH")
	}

	t.Run("valid trace_event.jsonl", func(t *testing.T) {
		cmd := exec.Command(py, "validate_schema.py", "--schema", "trace_event", "--jsonl", "testdata/trace_event.jsonl")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("validate_schema.py failed on valid trace_event.jsonl: %v\n%s", err, string(out))
		}
	})

	t.Run("valid schedule.json", func(t *testing.T) {
		cmd := exec.Command(py, "validate_schema.py", "--schema", "schedule", "testdata/schedule.json")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("validate_schema.py failed on valid schedule.json: %v\n%s", err, string(out))
		}
	})

	t.Run("rejects unknown field", func(t *testing.T) {
		tmp := t.TempDir()
		badPath := filepath.Join(tmp, "bad.jsonl")
		if err := os.WriteFile(badPath, []byte(`{"schema_version":"v1","ts":"2026-10-22T01:20:01Z","env":"sb-1","op":"create","dur_ms":10,"ok":true,"bogus":1}`+"\n"), 0o644); err != nil {
			t.Fatalf("write bad.jsonl: %v", err)
		}
		cmd := exec.Command(py, "validate_schema.py", "--schema", "trace_event", "--jsonl", badPath)
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("expected validate_schema.py to fail on unknown field, got success:\n%s", string(out))
		}
	})
}

func mustRead(t *testing.T, rel string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return b
}

func roundTripJSON[T any](t *testing.T, schemaName string, val T) {
	t.Helper()
	encoded, err := json.Marshal(val)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if err := ValidateAgainstJSONSchema(schemaName, encoded); err != nil {
		t.Fatalf("round-trip ValidateAgainstJSONSchema(%s): %v\npayload: %s", schemaName, err, string(encoded))
	}
	if _, err := DecodeJSONStrict[T](bytes.NewReader(encoded)); err != nil {
		t.Fatalf("round-trip DecodeJSONStrict: %v", err)
	}
}

func TestPreflightCoverageValidateEnforcesStatesAndConsistency(t *testing.T) {
	raw := mustRead(t, "testdata/preflight_coverage.json")
	var base PreflightCoverage
	if err := json.Unmarshal(raw, &base); err != nil {
		t.Fatal(err)
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	cases := map[string]func(c *PreflightCoverage){
		"unknown golden state": func(c *PreflightCoverage) { c.Goldens[0].State = "skipped" },
		"domain complete with an incomplete image": func(c *PreflightCoverage) {
			c.Domains[1].Complete = true
		},
		"coverage complete with a golden not ready": func(c *PreflightCoverage) {
			c.Domains[1].Images[0].Nodes = []string{"a", "b"}
			c.Domains[1].Images[0].Complete = true
			c.Domains[1].Complete = true
			c.Complete = true // goldens[1] is failed
		},
		"empty node": func(c *PreflightCoverage) { c.Domains[0].Images[0].Nodes[0] = "" },
	}
	for name, mutate := range cases {
		var c PreflightCoverage
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
	}

	// Normalize turns nil slices into empty arrays, so the JSON matches the schema.
	c := PreflightCoverage{SchemaVersion: SchemaVersion, RunID: "pf", StartedAt: base.StartedAt, FinishedAt: base.FinishedAt,
		Domains: []DomainCoverage{{Name: "d0", Nodes: 1, Images: []ImageCoverage{{Image: "img", Wanted: 1}}}}}
	c.Normalize()
	out, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAgainstJSONSchema("preflight_coverage.schema.json", out); err != nil {
		t.Errorf("normalized empty coverage does not validate: %v\n%s", err, out)
	}
	if err := c.Validate(); err != nil {
		t.Errorf("normalized empty coverage: %v", err)
	}
}

func TestFaultClassificationSchemaAndValidation(t *testing.T) {
	raw := mustRead(t, "testdata/report.json")
	rep, err := DecodeJSONStrict[Report](bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("DecodeJSONStrict: %v", err)
	}
	rep.FaultClassification = &FaultClassification{
		AllRollouts:       100,
		InfraDropped:      1,
		Tier1Dropped:      1,
		Tier2Policy:       2,
		Tier3Dropped:      0,
		RecoveredRetries:  3,
		PostRolloutErrors: 1,
		InfraFaultRate:    0.01,
		TargetRate:        DefaultInfraFaultRateTarget,
		MeetsTarget:       true,
		Categories: []FaultCategorySummary{
			{
				Tier:       FaultTierBeforeEpisode,
				Category:   FaultCatNoCapacity,
				Dropped:    1,
				Recovered:  0,
				Events:     1,
				ExampleErr: "no worker has room",
			},
			{
				Tier:       FaultTierPolicy,
				Category:   FaultCatNonZeroExit,
				Dropped:    2,
				Recovered:  0,
				Events:     2,
				ExampleErr: "exit 1, expected 0",
			},
			{
				Tier:       FaultTierDuringEpisode,
				Category:   FaultCatWakeResumeFailure,
				Dropped:    0,
				Recovered:  3,
				Events:     0,
				ExampleErr: "wake succeeded after 2 attempts",
			},
		},
	}
	if err := rep.Validate(); err != nil {
		t.Fatalf("Validate valid FaultClassification: %v", err)
	}
	roundTripJSON(t, "report.schema.json", rep)

	badSum := *rep.FaultClassification
	badSum.InfraDropped = 5 // != Tier1Dropped + Tier3Dropped
	if err := badSum.Validate(); err == nil {
		t.Fatal("expected Validate error when infra_dropped != tier1_dropped + tier3_dropped")
	}

	badDrops := *rep.FaultClassification
	badDrops.Tier2Policy = badDrops.AllRollouts // 1 + 100 + 0 > 100
	if err := badDrops.Validate(); err == nil {
		t.Fatal("expected Validate error when the tier drops exceed all_rollouts")
	}

	badTier := *rep.FaultClassification
	badTier.Categories = []FaultCategorySummary{{Tier: "tier4_unknown", Category: "x"}}
	if err := badTier.Validate(); err == nil {
		t.Fatal("expected Validate error for invalid fault tier")
	}

	// The category list is closed: "other" is a member, a made-up name is not.
	withOther := *rep.FaultClassification
	withOther.Categories = append([]FaultCategorySummary(nil), rep.FaultClassification.Categories...)
	withOther.Categories = append(withOther.Categories, FaultCategorySummary{
		Tier: FaultTierDuringEpisode, Category: FaultCatOther, Dropped: 0, Events: 1, ExampleErr: "no space left on device",
	})
	if err := withOther.Validate(); err != nil {
		t.Fatalf("Validate with an other category: %v", err)
	}
	rep.FaultClassification = &withOther
	roundTripJSON(t, "report.schema.json", rep)

	badCat := *rep.FaultClassification
	badCat.Categories = []FaultCategorySummary{{Tier: FaultTierBeforeEpisode, Category: "disk_full"}}
	if err := badCat.Validate(); err == nil {
		t.Fatal("expected Validate error for a category outside the closed list")
	}
}

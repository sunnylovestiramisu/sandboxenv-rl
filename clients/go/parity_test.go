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

package env_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/env/clients/go"
	"github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"go.opentelemetry.io/otel/attribute"
)

func findRepoRoot(t *testing.T) string {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "proto/testdata/client-parity")); err == nil {
			return wd
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			t.Fatal("repo root not found")
		}
		wd = parent
	}
}

func readJSON(t *testing.T, filename string, v any) {
	t.Helper()
	path := filepath.Join(findRepoRoot(t), "proto/testdata/client-parity", filename)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", filename, err)
	}
}

func TestImageRefAttrParity(t *testing.T) {
	var vectors []struct {
		Ref  *string
		Want *string
	}
	readJSON(t, "image_ref.json", &vectors)
	for _, v := range vectors {
		ref := ""
		if v.Ref != nil {
			ref = *v.Ref
		}
		want := ""
		if v.Want != nil {
			want = *v.Want
		}
		if got := env.ImageRefAttrForTest(ref); got != want {
			t.Errorf("imageRefAttr(%q) = %q, want %q", ref, got, want)
		}
	}
}

func TestExecutableParity(t *testing.T) {
	var vectors []struct {
		Line string
		Want *string
	}
	readJSON(t, "executable.json", &vectors)
	for _, v := range vectors {
		want := ""
		if v.Want != nil {
			want = *v.Want
		}
		if got := env.ExecutableForTest(v.Line); got != want {
			t.Errorf("executable(%q) = %q, want %q", v.Line, got, want)
		}
	}
}

func TestRPCAttrsParity(t *testing.T) {
	var vectors []struct {
		Mode    string
		Target  string
		Service string
		Method  string
		Want    map[string]any
	}
	readJSON(t, "rpc_attrs.json", &vectors)
	for _, v := range vectors {
		env.SetRPCSemconvModeForTest(t, env.ParseRPCSemconvModeForTest(v.Mode))
		gotList := env.RPCAttrsForTest(v.Service, v.Method, v.Target)
		got := make(map[string]any)
		for _, kv := range gotList {
			switch kv.Value.Type() {
			case attribute.INT64:
				got[string(kv.Key)] = float64(kv.Value.AsInt64())
			default:
				got[string(kv.Key)] = kv.Value.AsString()
			}
		}
		if len(got) != len(v.Want) {
			t.Errorf("rpcAttrs(%q, %q, %q, %q) = %v, want %v", v.Mode, v.Service, v.Method, v.Target, got, v.Want)
			continue
		}
		for k, wantVal := range v.Want {
			if got[k] != wantVal {
				t.Errorf("rpcAttrs(%q, %q, %q, %q)[%q] = %v, want %v", v.Mode, v.Service, v.Method, v.Target, k, got[k], wantVal)
			}
		}
	}
}

func TestErrorMappingParity(t *testing.T) {
	var vectors []struct {
		Code    codes.Code
		Message string
		Trailer string
		Want    struct {
			Kind      string
			Code      codes.Code
			Reason    string
			Retryable bool
		}
	}
	readJSON(t, "error_mapping.json", &vectors)

	allSentinels := []error{env.ErrNotFound, env.ErrInvalidArgument, env.ErrPermissionDenied, env.ErrFailedPrecondition, env.ErrProcessExited}

	for _, v := range vectors {
		st := status.New(v.Code, v.Message)
		var err error = st.Err()
		if v.Trailer != "" {
			err = env.MarkStartFailureForTest(context.Background(), err, v.Trailer)
		}
		got := env.FromGRPCErrorForTest(err)

		if env.Retryable(got) != v.Want.Retryable {
			t.Errorf("Retryable(%v) = %v, want %v", got, env.Retryable(got), v.Want.Retryable)
		}

		if v.Want.Kind == "start_error" {
			var se *env.StartError
			if !errors.As(got, &se) {
				t.Errorf("fromGRPCError(%v) = %T, want *StartError", err, got)
			} else if se.Reason != v.Want.Reason {
				t.Errorf("StartError.Reason = %q, want %q", se.Reason, v.Want.Reason)
			}
		} else {
			var sentinel error
			switch v.Want.Kind {
			case "not_found":
				sentinel = env.ErrNotFound
			case "invalid_argument":
				sentinel = env.ErrInvalidArgument
			case "permission_denied":
				sentinel = env.ErrPermissionDenied
			case "failed_precondition":
				sentinel = env.ErrFailedPrecondition
			case "process_exited":
				sentinel = env.ErrProcessExited
			}

			if sentinel != nil {
				if !errors.Is(got, sentinel) {
					t.Errorf("fromGRPCError(%v) = %v, want it to wrap %v", err, got, sentinel)
				}
			} else {
				for _, s := range allSentinels {
					if errors.Is(got, s) {
						t.Errorf("fromGRPCError(%v) = %v, should not wrap %v", err, got, s)
					}
				}
				var se *env.StartError
				if errors.As(got, &se) {
					t.Errorf("fromGRPCError(%v) = %T, should not be *StartError", err, got)
				}
			}

			if status.Code(got) != v.Want.Code {
				t.Errorf("status.Code(%v) = %v, want %v", got, status.Code(got), v.Want.Code)
			}
		}
	}

	for _, v := range vectors {
		guest := failingGuest{code: v.Code, msg: v.Message, reason: v.Trailer}
		clients := failingClients(t, guest)
		for conn, client := range clients {
			e := client.Env("envs", "dev1")

			// Unary: GetProcess
			t.Run(conn+"/GetProcess", func(t *testing.T) {
				_, err := e.Process("p1").Wait(context.Background())
				if err == nil {
					t.Fatalf("expected error")
				}
				if v.Want.Kind == "start_error" {
					var se *env.StartError
					if !errors.As(err, &se) {
						t.Errorf("err = %T, want *StartError", err)
					}
				} else {
					if status.Code(err) != v.Want.Code {
						t.Errorf("status.Code(%v) = %v, want %v", err, status.Code(err), v.Want.Code)
					}
				}
			})

			// Stream: WriteFile
			t.Run(conn+"/WriteFile", func(t *testing.T) {
				err := e.WriteFile(context.Background(), "a.txt", strings.NewReader("hi"), 0644)
				if err == nil {
					t.Fatalf("expected error")
				}
				if v.Want.Kind == "start_error" {
					var se *env.StartError
					if !errors.As(err, &se) {
						t.Errorf("err = %T, want *StartError", err)
					}
				} else {
					if status.Code(err) != v.Want.Code {
						t.Errorf("status.Code(%v) = %v, want %v", err, status.Code(err), v.Want.Code)
					}
				}
			})
		}
	}
}

type mockReg struct {
	prometheus.Registerer
	prometheus.Gatherer
	descs []*prometheus.Desc
}

func (m *mockReg) Register(c prometheus.Collector) error {
	ch := make(chan *prometheus.Desc, 10)
	c.Describe(ch)
	close(ch)
	for d := range ch {
		m.descs = append(m.descs, d)
	}
	return m.Registerer.Register(c)
}

func (m *mockReg) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := m.Register(c); err != nil {
			panic(err)
		}
	}
}

func TestMetricFamiliesParity(t *testing.T) {
	var vectors []struct {
		Name    string
		Help    string
		Labels  []string
		Buckets []float64
	}
	readJSON(t, "metric_families.json", &vectors)

	r := prometheus.NewRegistry()
	reg := &mockReg{Registerer: r, Gatherer: r}
	_, _ = env.NewClient(env.ClientOptions{Endpoint: "dns:///localhost:1", MetricsRegisterer: reg})

	for _, v := range vectors {
		var foundDesc *prometheus.Desc
		for _, d := range reg.descs {
			if strings.Contains(d.String(), `fqName: "`+v.Name+`"`) {
				foundDesc = d
				break
			}
		}
		if foundDesc == nil {
			t.Errorf("metric family %q not found", v.Name)
			continue
		}

		wantLabels := strings.Join(v.Labels, ",")
		if len(v.Labels) > 0 {
			if !strings.Contains(foundDesc.String(), `variableLabels: {`+wantLabels+`}`) && !strings.Contains(foundDesc.String(), `variableLabels: [`+wantLabels+`]`) {
				t.Errorf("metric %q labels != %v (desc: %s)", v.Name, v.Labels, foundDesc.String())
			}
		} else {
			if strings.Contains(foundDesc.String(), `variableLabels:`) {
				t.Errorf("metric %q should have no labels", v.Name)
			}
		}
	}

	gotFamilies, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	gotMap := make(map[string]interface{})
	for _, f := range gotFamilies {
		gotMap[f.GetName()] = f
	}

	for _, v := range vectors {
		fany, ok := gotMap[v.Name]
		if !ok {
			continue
		}

		fVal := reflect.ValueOf(fany)
		help := fVal.MethodByName("GetHelp").Call(nil)[0].String()
		if help != v.Help {
			t.Errorf("metric %q help = %q, want %q", v.Name, help, v.Help)
		}

		metrics := fVal.MethodByName("GetMetric").Call(nil)[0]
		if metrics.Len() > 0 {
			m := metrics.Index(0)
			histMethod := m.MethodByName("GetHistogram")
			if histMethod.IsValid() && !histMethod.Call(nil)[0].IsNil() {
				hist := histMethod.Call(nil)[0]
				buckets := hist.MethodByName("GetBucket").Call(nil)[0]
				var gotBuckets []float64
				for i := 0; i < buckets.Len(); i++ {
					b := buckets.Index(i)
					gotBuckets = append(gotBuckets, b.MethodByName("GetUpperBound").Call(nil)[0].Float())
				}
				if !reflect.DeepEqual(gotBuckets, v.Buckets) {
					t.Errorf("metric %q buckets = %v, want %v", v.Name, gotBuckets, v.Buckets)
				}
			}
		}
	}
}
func TestRPCCoverageParity(t *testing.T) {
	var vectors map[string]map[string]string
	readJSON(t, "rpc_coverage.json", &vectors)

	fd := ateenvv1alpha.File_proto_ateenv_v1alpha_env_proto
	services := fd.Services()
	envSvc := services.ByName("EnvironmentService")
	if envSvc == nil {
		t.Fatal("EnvironmentService not found in proto")
	}

	methods := envSvc.Methods()
	for i := 0; i < methods.Len(); i++ {
		m := methods.Get(i)
		rpcName := string(m.Name())

		mapping, ok := vectors[rpcName]
		if !ok {
			t.Errorf("RPC %q not found in rpc_coverage.json vector", rpcName)
			continue
		}

		goMethod := mapping["go"]
		clientType := reflect.TypeOf((*env.Client)(nil))
		if _, ok := clientType.MethodByName(goMethod); !ok {
			t.Errorf("Go Client missing method %q for RPC %q", goMethod, rpcName)
		}
	}
}

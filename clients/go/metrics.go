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

package env

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Metric family names, help strings, label keys and buckets shared with
// clients/python/src/ate_env_rl/telemetry/metrics.py.
const (
	metricExecSeconds                = "ate_env_client_exec_seconds"
	metricSuspendSeconds             = "ate_env_client_suspend_seconds"
	metricReadyToFirstCommandSeconds = "ate_env_client_ready_to_first_command_seconds"

	helpExecSeconds                = "Time to run a command in an environment"
	helpSuspendSeconds             = "Time to suspend an environment"
	helpReadyToFirstCommandSeconds = "Time from environment acquire ready until the first command begins"

	metricSourceDirect = "direct"
)

// secondsBuckets is the shared latency bucket list (50 ms to 10 min) used by
// SEC_BUCKETS in the Python client and internal/telemetry.SecondsBuckets.
var secondsBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}

// clientMetrics holds the client's optional Prometheus histograms. A zero
// value (MetricsRegisterer == nil) records nothing.
type clientMetrics struct {
	exec                *prometheus.HistogramVec
	suspend             *prometheus.HistogramVec
	readyToFirstCommand *prometheus.HistogramVec
}

func newClientMetrics(reg prometheus.Registerer) (clientMetrics, error) {
	if reg == nil {
		return clientMetrics{}, nil
	}
	var added []prometheus.Collector
	rollback := func() {
		for _, col := range added {
			reg.Unregister(col)
		}
	}
	regOne := func(name, help string, labels []string) (*prometheus.HistogramVec, error) {
		h, created, err := registerHistogramVec(reg, name, help, labels)
		if err != nil {
			rollback()
			return nil, err
		}
		if created {
			added = append(added, h)
		}
		return h, nil
	}

	execHist, err := regOne(metricExecSeconds, helpExecSeconds, []string{"status"})
	if err != nil {
		return clientMetrics{}, err
	}
	suspendHist, err := regOne(metricSuspendSeconds, helpSuspendSeconds, []string{"status"})
	if err != nil {
		return clientMetrics{}, err
	}
	readyHist, err := regOne(metricReadyToFirstCommandSeconds, helpReadyToFirstCommandSeconds, []string{"status", "source"})
	if err != nil {
		return clientMetrics{}, err
	}
	return clientMetrics{
		exec:                execHist,
		suspend:             suspendHist,
		readyToFirstCommand: readyHist,
	}, nil
}

// registerHistogramVec registers a HistogramVec on reg, reusing an existing
// HistogramVec only when its descriptor (including variable-label order) and
// bucket upper bounds match.
func registerHistogramVec(reg prometheus.Registerer, name, help string, labels []string) (*prometheus.HistogramVec, bool, error) {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: slices.Clone(secondsBuckets),
	}, labels)
	if err := reg.Register(h); err != nil {
		var are prometheus.AlreadyRegisteredError
		if errors.As(err, &are) {
			existing, ok := are.ExistingCollector.(*prometheus.HistogramVec)
			if !ok {
				return nil, false, fmt.Errorf("env: metric %s already registered as %T, want *prometheus.HistogramVec", name, are.ExistingCollector)
			}
			if err := verifyExistingHistogramVec(existing, h, name, labels); err != nil {
				return nil, false, err
			}
			return existing, false, nil
		}
		return nil, false, fmt.Errorf("env: registering metric %s: %w", name, err)
	}
	return h, true, nil
}

func verifyExistingHistogramVec(existing, want *prometheus.HistogramVec, name string, labels []string) error {
	if gotDesc, wantDesc := descString(existing), descString(want); gotDesc != wantDesc {
		return fmt.Errorf("env: metric %s already registered with descriptor %s, want %s", name, gotDesc, wantDesc)
	}
	bounds, err := histogramVecBounds(existing, labels)
	if err != nil {
		return fmt.Errorf("env: inspecting existing metric %s: %w", name, err)
	}
	if !slices.Equal(bounds, secondsBuckets) {
		return fmt.Errorf("env: metric %s already registered with buckets %v, want %v", name, bounds, secondsBuckets)
	}
	return nil
}

func descString(c prometheus.Collector) string {
	ch := make(chan *prometheus.Desc, 1)
	go func() {
		c.Describe(ch)
		close(ch)
	}()
	var out string
	for d := range ch {
		if out == "" {
			out = d.String()
		}
	}
	return out
}

func histogramVecBounds(hv *prometheus.HistogramVec, labels []string) ([]float64, error) {
	ch := make(chan prometheus.Metric, 1)
	go func() {
		hv.Collect(ch)
		close(ch)
	}()
	var sample prometheus.Metric
	for m := range ch {
		if sample == nil {
			sample = m
		}
	}
	if sample == nil {
		probe := make([]string, len(labels))
		for i := range probe {
			probe[i] = "__ate_env_bucket_probe__"
		}
		obs, err := hv.GetMetricWithLabelValues(probe...)
		if err != nil {
			return nil, err
		}
		defer hv.DeleteLabelValues(probe...)
		m, ok := obs.(prometheus.Metric)
		if !ok {
			return nil, fmt.Errorf("observer %T does not implement prometheus.Metric", obs)
		}
		sample = m
	}
	var dm dto.Metric
	if err := sample.Write(&dm); err != nil {
		return nil, err
	}
	var bounds []float64
	for _, b := range dm.GetHistogram().GetBucket() {
		bounds = append(bounds, b.GetUpperBound())
	}
	return bounds, nil
}

func (m clientMetrics) observeExec(ctx context.Context, d time.Duration, err error) {
	if m.exec == nil {
		return
	}
	m.exec.WithLabelValues(metricStatus(ctx, err)).Observe(max(0, d.Seconds()))
}

func (m clientMetrics) observeSuspend(ctx context.Context, d time.Duration, err error) {
	if m.suspend == nil {
		return
	}
	m.suspend.WithLabelValues(metricStatus(ctx, err)).Observe(max(0, d.Seconds()))
}

func (m clientMetrics) observeReadyToFirstCommand(d time.Duration) {
	if m.readyToFirstCommand == nil {
		return
	}
	m.readyToFirstCommand.WithLabelValues("ok", metricSourceDirect).Observe(max(0, d.Seconds()))
}

// metricStatus maps an operation's outcome to "ok", "cancelled" or "error",
// matching ate_env_rl.telemetry.metrics.status_of.
func metricStatus(ctx context.Context, err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return "cancelled"
	}
	return "error"
}

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
	"log"
	"net/http"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Metric family names served by ate-env-api.
const (
	MetricRequestsTotal          = "ate_env_api_requests_total"
	MetricRequestDurationSeconds = "ate_env_api_request_duration_seconds"
	MetricCreateToReadySeconds   = "ate_env_api_create_to_ready_seconds"
	MetricWakeWaitSeconds        = "ate_env_api_wake_wait_seconds"
	MetricEnvironments           = "ate_env_api_environments"
	MetricBuildInfo              = "ate_env_api_build_info"
)

// Values of the outcome label of ate_env_api_wake_wait_seconds.
const (
	WakeOutcomeWoke    = "woke"
	WakeOutcomeTimeout = "timeout"
	WakeOutcomeError   = "error"
)

// Values of the status label of ate_env_api_create_to_ready_seconds.
const (
	// ReadyStatusOK: the environment's guest answered its first request.
	ReadyStatusOK = "ok"
	// ReadyStatusError: CreateEnvironment itself failed.
	ReadyStatusError = "error"
	// ReadyStatusCrashed: the environment crashed before its guest answered.
	ReadyStatusCrashed = "crashed"
	// ReadyStatusDeleted: the environment was deleted before its guest answered.
	ReadyStatusDeleted = "deleted"
)

// RequestedAtHeader is the gRPC metadata key a client may set on
// CreateEnvironment to the time it decided to create the environment, in RFC
// 3339 with nanoseconds. ate-env-api starts create_to_ready there when the
// header is present and parses, and at its own receipt time otherwise.
const RequestedAtHeader = "x-ate-env-requested-at"

// SecondsBuckets is the one bucket set every ate-env latency histogram uses,
// from 50 ms to 10 min.
var SecondsBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600}

// Metric types, as Definition.Type.
const (
	TypeCounter   = "counter"
	TypeGauge     = "gauge"
	TypeHistogram = "histogram"
)

// Definition describes one metric family. Definitions is the single source
// for both the collectors NewMetrics registers and docs/metrics.md (see
// hack/metrics-docs).
type Definition struct {
	Name    string
	Type    string
	Help    string
	Labels  []string
	Buckets []float64 // histograms only
}

// Definitions lists the families ate-env-api serves, in documentation order.
var Definitions = []Definition{
	{
		Name:   MetricRequestsTotal,
		Type:   TypeCounter,
		Help:   "gRPC requests handled, by full method name (/package.Service/Method) and status code.",
		Labels: []string{"method", "code"},
	},
	{
		Name:    MetricRequestDurationSeconds,
		Type:    TypeHistogram,
		Help:    "gRPC request latency in seconds, from the server receiving the call to the handler returning (streams: until the stream ends), by full method name and status code.",
		Labels:  []string{"method", "code"},
		Buckets: SecondsBuckets,
	},
	{
		Name: MetricCreateToReadySeconds,
		Type: TypeHistogram,
		Help: "Seconds from an environment's creation request to its guest answering the first request routed through this replica. " +
			"The start is the client's x-ate-env-requested-at header on CreateEnvironment when present, else the receipt of the call. " +
			"status is ok (guest answered), error (CreateEnvironment failed), crashed (crashed before answering) or deleted (deleted before answering).",
		Labels:  []string{"atespace", "status"},
		Buckets: SecondsBuckets,
	},
	{
		Name: MetricWakeWaitSeconds,
		Type: TypeHistogram,
		Help: "Seconds a proxied guest call (process or file RPC) spent waiting for a suspended or still-starting environment to answer, observed only when the first probe found the guest unreachable. " +
			"outcome is woke, timeout (wake or running budget spent) or error (crashed, deleted, control-plane error or caller gone).",
		Labels:  []string{"outcome"},
		Buckets: SecondsBuckets,
	},
	{
		Name: MetricEnvironments,
		Type: TypeGauge,
		Help: "Environments by atespace and state, read from the Substrate control plane (ListActors) at scrape time, cached for 15 s. " +
			"Every ate-env-api replica reports the same totals; aggregate with max, not sum.",
		Labels: []string{"atespace", "state"},
	},
	{
		Name:   MetricBuildInfo,
		Type:   TypeGauge,
		Help:   "Always 1; the labels carry the ate-env-api build's module version and VCS commit.",
		Labels: []string{"version", "commit"},
	},
}

// Version and Commit override the build_info labels; set them with
// -ldflags "-X github.com/agent-substrate/env/internal/telemetry.Version=...".
// Empty means the module version and vcs.revision Go embeds in the binary.
var (
	Version string
	Commit  string
)

// BuildVersion returns the version and commit for build_info.
func BuildVersion() (version, commit string) {
	version, commit = Version, Commit
	if bi, ok := debug.ReadBuildInfo(); ok {
		if version == "" {
			version = bi.Main.Version
		}
		if commit == "" {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" {
					commit = s.Value
				}
			}
		}
	}
	if version == "" {
		version = "unknown"
	}
	if commit == "" {
		commit = "unknown"
	}
	return version, commit
}

// EnvironmentCount is one sample of ate_env_api_environments.
type EnvironmentCount struct {
	Atespace string
	State    string
	Count    int
}

// EnvironmentSource returns the current environment counts. It is called at
// most once per environmentsCacheTTL, on scrape.
type EnvironmentSource func(ctx context.Context) ([]EnvironmentCount, error)

const (
	environmentsCacheTTL = 15 * time.Second
	environmentsTimeout  = 5 * time.Second
)

// Metrics holds the ate-env-api collectors on a private registry. A nil
// *Metrics is valid and records nothing, so callers need no checks when
// metrics are disabled.
type Metrics struct {
	registry      *prometheus.Registry
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	createToReady *prometheus.HistogramVec
	wakeWait      *prometheus.HistogramVec
	buildInfo     *prometheus.GaugeVec
	environments  *environmentsCollector
}

func definition(name string) Definition {
	for _, d := range Definitions {
		if d.Name == name {
			return d
		}
	}
	panic("telemetry: no definition for " + name)
}

func newHistogram(name string) *prometheus.HistogramVec {
	d := definition(name)
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: d.Name, Help: d.Help, Buckets: d.Buckets}, d.Labels)
}

// NewMetrics creates the ate-env-api families on a new private registry and
// sets build_info from BuildVersion. Only the six documented families are
// registered.
func NewMetrics() *Metrics {
	reqDef := definition(MetricRequestsTotal)
	biDef := definition(MetricBuildInfo)
	envDef := definition(MetricEnvironments)
	m := &Metrics{
		registry:      prometheus.NewRegistry(),
		requests:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: reqDef.Name, Help: reqDef.Help}, reqDef.Labels),
		duration:      newHistogram(MetricRequestDurationSeconds),
		createToReady: newHistogram(MetricCreateToReadySeconds),
		wakeWait:      newHistogram(MetricWakeWaitSeconds),
		buildInfo:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: biDef.Name, Help: biDef.Help}, biDef.Labels),
		environments: &environmentsCollector{
			desc: prometheus.NewDesc(envDef.Name, envDef.Help, envDef.Labels, nil),
		},
	}
	m.registry.MustRegister(m.requests, m.duration, m.createToReady, m.wakeWait, m.buildInfo, m.environments)
	for _, o := range []string{WakeOutcomeWoke, WakeOutcomeTimeout, WakeOutcomeError} {
		m.wakeWait.WithLabelValues(o)
	}
	m.buildInfo.WithLabelValues(BuildVersion()).Set(1)
	return m
}

// Registry returns the private registry, for tests and for Handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// InitMethods creates the request series of every method in info with code
// OK, so the request families are visible on /metrics before the first call.
// Pass grpc.Server.GetServiceInfo().
func (m *Metrics) InitMethods(info map[string]grpc.ServiceInfo) {
	if m == nil {
		return
	}
	for svc, si := range info {
		for _, mi := range si.Methods {
			method := "/" + svc + "/" + mi.Name
			m.requests.WithLabelValues(method, "OK")
			m.duration.WithLabelValues(method, "OK")
		}
	}
}

// InitAtespace creates the create_to_ready series of atespace for every
// status, so the family is visible before the first environment is ready.
func (m *Metrics) InitAtespace(atespace string) {
	if m == nil {
		return
	}
	for _, s := range []string{ReadyStatusOK, ReadyStatusError, ReadyStatusCrashed, ReadyStatusDeleted} {
		m.createToReady.WithLabelValues(atespace, s)
	}
}

// ObserveCreateToReady records one create_to_ready sample.
func (m *Metrics) ObserveCreateToReady(atespace, status string, d time.Duration) {
	if m == nil {
		return
	}
	m.createToReady.WithLabelValues(atespace, status).Observe(d.Seconds())
}

// ObserveWakeWait records one wake wait with outcome.
func (m *Metrics) ObserveWakeWait(outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.wakeWait.WithLabelValues(outcome).Observe(d.Seconds())
}

// SetEnvironmentSource sets where ate_env_api_environments reads its counts.
// Without a source the family has no samples.
func (m *Metrics) SetEnvironmentSource(src EnvironmentSource) {
	if m == nil {
		return
	}
	m.environments.mu.Lock()
	defer m.environments.mu.Unlock()
	m.environments.source = src
	m.environments.fetched = time.Time{}
}

func (m *Metrics) observeRequest(method string, err error, d time.Duration) {
	if m == nil {
		return
	}
	code := status.Code(err).String()
	m.requests.WithLabelValues(method, code).Inc()
	m.duration.WithLabelValues(method, code).Observe(d.Seconds())
}

// UnaryServerInterceptor counts and times unary calls. The method label is
// the registered full method name, so it is bounded by the served services.
func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		m.observeRequest(info.FullMethod, err, time.Since(start))
		return resp, err
	}
}

// StreamServerInterceptor counts and times streaming calls, until the
// handler returns.
func (m *Metrics) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		m.observeRequest(info.FullMethod, err, time.Since(start))
		return err
	}
}

// environmentsCollector reads environment counts from its source on
// scrape, at most once per environmentsCacheTTL, and serves the last good
// answer in between. A failed read keeps the previous answer and is logged.
type environmentsCollector struct {
	desc *prometheus.Desc

	mu      sync.Mutex
	source  EnvironmentSource
	fetched time.Time
	counts  []EnvironmentCount
}

func (c *environmentsCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *environmentsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.source == nil {
		return
	}
	if time.Since(c.fetched) >= environmentsCacheTTL {
		// Untraced: a scrape every few seconds must not start a root trace
		// per control-plane read.
		ctx, cancel := context.WithTimeout(Untraced(context.Background()), environmentsTimeout)
		counts, err := c.source(ctx)
		cancel()
		if err != nil {
			log.Printf("metrics: reading environment counts: %v", err)
		} else {
			sort.Slice(counts, func(i, j int) bool {
				if counts[i].Atespace != counts[j].Atespace {
					return counts[i].Atespace < counts[j].Atespace
				}
				return counts[i].State < counts[j].State
			})
			c.counts = counts
		}
		c.fetched = time.Now()
	}
	for _, e := range c.counts {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(e.Count), e.Atespace, e.State)
	}
}

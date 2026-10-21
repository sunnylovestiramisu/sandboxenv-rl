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

// Package telemetry holds the tracing and metrics plumbing of the ate-env
// servers: an Instrumenter that is either a no-op or backed by OpenTelemetry
// (SetupOTel), and the Prometheus families ate-env-api serves on its metrics
// listener (Metrics, Definitions).
//
// Nothing here has side effects at import: no global tracer provider is set
// and no collector is registered until SetupOTel or NewMetrics is called.
package telemetry

import "context"

// Instrumenter is the tracing surface the servers use. Spans continue
// whatever span ctx carries; across gRPC the otelgrpc stats handlers put the
// caller's W3C trace context there.
type Instrumenter interface {
	// StartSpan starts a span named name, a child of the span in ctx, with
	// attrs as string attributes. It returns the context carrying the new
	// span and a function that ends it.
	StartSpan(ctx context.Context, name string, attrs map[string]string) (context.Context, func())
	// AddEvent records an event with attrs on the span in ctx.
	AddEvent(ctx context.Context, name string, attrs map[string]string)
	// IsRecording reports whether the span in ctx is sampled and recording.
	IsRecording(ctx context.Context) bool
	// TraceContext returns the W3C trace context of ctx as a JSON object
	// ({"traceparent": "...", "tracestate": "..."}), or "" when there is
	// none.
	TraceContext(ctx context.Context) string
}

type noopInstrumenter struct{}

// NewNoOp returns an Instrumenter whose every method is a no-op.
func NewNoOp() Instrumenter { return noopInstrumenter{} }

func (noopInstrumenter) StartSpan(ctx context.Context, _ string, _ map[string]string) (context.Context, func()) {
	return ctx, func() {}
}

func (noopInstrumenter) AddEvent(context.Context, string, map[string]string) {}

func (noopInstrumenter) IsRecording(context.Context) bool { return false }

func (noopInstrumenter) TraceContext(context.Context) string { return "" }

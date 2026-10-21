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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Propagator is the only propagator the servers install: W3C Trace Context,
// without Baggage, so no caller-chosen key/value pairs ride along.
var Propagator propagation.TextMapPropagator = propagation.TraceContext{}

type otelInstrumenter struct {
	tracer trace.Tracer
}

// newOTelInstrumenter returns an Instrumenter whose spans come from tp, with
// the instrumentation scope named after serviceName ('-' replaced by '_').
func newOTelInstrumenter(tp trace.TracerProvider, serviceName string) Instrumenter {
	return &otelInstrumenter{tracer: tp.Tracer(strings.ReplaceAll(serviceName, "-", "_"))}
}

func (o *otelInstrumenter) StartSpan(ctx context.Context, name string, attrs map[string]string) (context.Context, func()) {
	var opts []trace.SpanStartOption
	if len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(stringAttrs(attrs)...))
	}
	ctx, span := o.tracer.Start(ctx, name, opts...)
	return ctx, func() { span.End() }
}

func (o *otelInstrumenter) AddEvent(ctx context.Context, name string, attrs map[string]string) {
	trace.SpanFromContext(ctx).AddEvent(name, trace.WithAttributes(stringAttrs(attrs)...))
}

func (o *otelInstrumenter) IsRecording(ctx context.Context) bool {
	return trace.SpanFromContext(ctx).IsRecording()
}

func (o *otelInstrumenter) TraceContext(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	Propagator.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return ""
	}
	data, err := json.Marshal(map[string]string(carrier))
	if err != nil {
		return ""
	}
	return string(data)
}

// Untraced returns ctx with a remote parent span context that is not
// sampled, so calls made with it are dropped by the default ParentBased
// samplers: here, and in ateapi, which receives the unsampled traceparent.
// Background reads such as the environments gauge use it so that each
// scrape does not start a new root trace.
func Untraced(ctx context.Context) context.Context {
	var tid trace.TraceID
	var sid trace.SpanID
	_, _ = rand.Read(tid[:])
	_, _ = rand.Read(sid[:])
	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid,
		SpanID:  sid,
		Remote:  true,
		// TraceFlags zero: not sampled.
	}))
}

func stringAttrs(attrs map[string]string) []attribute.KeyValue {
	kv := make([]attribute.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kv = append(kv, attribute.String(k, v))
	}
	return kv
}

// SetupOTel installs a global OpenTelemetry tracer provider that exports
// spans over OTLP/gRPC and returns an Instrumenter on it, plus a function
// that flushes and shuts the provider down.
//
// The exporter is configured by the standard environment variables, chiefly
// OTEL_EXPORTER_OTLP_ENDPOINT (default localhost:4317) and
// OTEL_EXPORTER_OTLP_INSECURE; an http:// endpoint also means plaintext. The
// resource carries service.name = serviceName. The global propagator is set
// to W3C Trace Context only (no Baggage), which is what the otelgrpc stats
// handlers read and write.
func SetupOTel(ctx context.Context, serviceName string) (Instrumenter, func(), error) {
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: creating OTLP trace exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", serviceName)))
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, nil, fmt.Errorf("telemetry: building resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(Propagator)

	shutdown := func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(sctx)
	}
	return newOTelInstrumenter(tp, serviceName), shutdown, nil
}

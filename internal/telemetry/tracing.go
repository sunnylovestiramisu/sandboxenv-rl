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

// Package telemetry sets up OpenTelemetry tracing for the ate-env-api server.
//
// It follows Substrate's conventions so ate-env-api spans join the same traces
// as ateapi, atelet and ateom: the standard OTEL_* environment (the deployment
// can mount Substrate's ate-otel-config ConfigMap), a parent-based sampler,
// and W3C trace context. Baggage is propagated too, so the rl.* episode
// identifiers set by the Python SDK reach the server.
package telemetry

import (
	"context"
	"errors"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// DefaultSampleRatio is the share of parentless requests ate-env-api roots a
// trace for, matching Substrate's control-plane components. Requests that
// arrive with a sampled parent (e.g. an SDK episode trace) are always traced.
const DefaultSampleRatio = 0.1

// Tracing describes what InitTracing set up.
type Tracing struct {
	// Exporting is true when spans are exported over OTLP.
	Exporting bool
	// Shutdown flushes and stops the tracer provider; a no-op when not exporting.
	Shutdown func(context.Context) error
}

// InitTracing installs the global propagator (W3C trace context + baggage)
// and, when an OTLP endpoint is configured and OTEL_TRACES_EXPORTER is not
// "none", a tracer provider exporting spans over OTLP/gRPC.
//
// Without an exporter no SDK provider is installed: the server still
// forwards incoming trace context to ateapi and the router unchanged, so
// downstream spans stay attached to the caller's trace.
func InitTracing(ctx context.Context, serviceName string) (*Tracing, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	noop := &Tracing{Shutdown: func(context.Context) error { return nil }}
	if !exportEnabled() {
		return noop, nil
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return noop, err
	}
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", serviceName)),
		resource.WithFromEnv(),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return noop, err
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	}
	// OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG, when set, are read by the
	// SDK itself; otherwise use the Substrate-style default.
	if os.Getenv("OTEL_TRACES_SAMPLER") == "" {
		opts = append(opts, sdktrace.WithSampler(
			sdktrace.ParentBased(sdktrace.TraceIDRatioBased(DefaultSampleRatio)),
		))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return &Tracing{Exporting: true, Shutdown: tp.Shutdown}, nil
}

func exportEnabled() bool {
	for _, item := range strings.Split(os.Getenv("OTEL_TRACES_EXPORTER"), ",") {
		if strings.TrimSpace(strings.ToLower(item)) == "none" {
			return false
		}
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// Package observability is the shared metrics, tracing and log-correlation
// setup for every Go service in this repository.
//
// A service calls Init once at startup, wraps its HTTP handler with
// Middleware, wraps outgoing HTTP clients with Transport, and serves
// MetricsHandler on /metrics. Message producers and consumers use Inject and
// Extract to carry the trace across NATS, and pgxtrace records database
// queries as spans. Log lines get trace_id and correlation_id through
// LogHook.
//
// Tracing is exported over OTLP/HTTP when OTEL_EXPORTER_OTLP_ENDPOINT is set
// (for example http://jaeger:4318). Without it, trace context is still
// propagated between services, so turning the collector on later needs no code
// change. Sampling follows the standard OTEL_TRACES_SAMPLER variables and
// defaults to sampling everything.
package observability

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/wiwekaputera/if4031-tubes-imade/observability"

// Init sets up tracing for the named service and returns a function that
// flushes pending spans. Call the shutdown function before exiting.
func Init(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		// No collector configured: keep propagating trace context, export nothing.
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL, semconv.ServiceName(service),
	))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the tracer used for spans this package and services create.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// TraceID returns the current trace ID, or "" when ctx carries no trace.
func TraceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}

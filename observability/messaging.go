package observability

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	messages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messaging_messages_total",
		Help: "Messages published or consumed, by service, subject, operation and outcome.",
	}, []string{"service", "subject", "operation", "outcome"})

	messageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "messaging_process_duration_seconds",
		Help:    "Time to publish or handle one message.",
		Buckets: latencyBuckets,
	}, []string{"service", "subject", "operation"})
)

// Headers is the shape of message headers; nats.Header can be passed directly.
type Headers = map[string][]string

// Inject writes the trace context and correlation ID of ctx into message headers.
func Inject(ctx context.Context, h Headers) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
	if id := CorrelationID(ctx); id != "" {
		http.Header(h).Set(CorrelationHeader, id)
	}
}

// Extract continues the trace and correlation ID carried in message headers.
func Extract(ctx context.Context, h Headers) context.Context {
	if h == nil {
		return ctx
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(h))
	if id := http.Header(h).Get(CorrelationHeader); id != "" {
		ctx = WithCorrelationID(ctx, id)
	}
	return ctx
}

// Operation is one publish or one message handled. Start it before the work
// and call End with the outcome when done.
type Operation struct {
	service, subject, op string
	start                time.Time
	span                 trace.Span
}

// StartPublish starts a producer span. Call Inject with the returned context
// to put the trace into the outgoing message headers.
func StartPublish(ctx context.Context, service, subject string) (context.Context, *Operation) {
	return start(ctx, service, subject, "publish", trace.SpanKindProducer)
}

// StartConsume continues the trace from the message headers and starts a
// consumer span for handling the message.
func StartConsume(ctx context.Context, service, subject string, h Headers) (context.Context, *Operation) {
	return start(Extract(ctx, h), service, subject, "process", trace.SpanKindConsumer)
}

func start(ctx context.Context, service, subject, op string, kind trace.SpanKind) (context.Context, *Operation) {
	ctx, span := Tracer().Start(ctx, op+" "+subject,
		trace.WithSpanKind(kind),
		trace.WithAttributes(
			attribute.String("messaging.system", "nats"),
			attribute.String("messaging.destination.name", subject),
			attribute.String("messaging.operation", op),
		))
	return ctx, &Operation{service: service, subject: subject, op: op, start: time.Now(), span: span}
}

// End records the outcome ("ok", "duplicate", "error", ...) and closes the span.
// err, when not nil, marks the span as failed.
func (o *Operation) End(outcome string, err error) {
	if err != nil {
		o.span.RecordError(err)
		o.span.SetStatus(codes.Error, err.Error())
	}
	o.span.SetAttributes(attribute.String("outcome", outcome))
	o.span.End()
	messages.WithLabelValues(o.service, o.subject, o.op, outcome).Inc()
	messageDuration.WithLabelValues(o.service, o.subject, o.op).Observe(time.Since(o.start).Seconds())
}

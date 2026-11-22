package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Latency buckets in seconds, from 1 ms to 10 s. The steps are finer between
// 0.5 s and 5 s, where slow upstream calls land, because histogram_quantile
// interpolates inside a bucket: with only 2.5 s and 5 s, requests that all
// take under 3 s would be reported with a p95 above 4 s.
var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, .75, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10}

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests served, by service, method, route and status code.",
	}, []string{"service", "method", "route", "code"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "Time to serve an HTTP request.",
		Buckets: latencyBuckets,
	}, []string{"service", "method", "route"})

	httpInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "http_requests_in_flight",
		Help: "HTTP requests being served right now.",
	}, []string{"service"})

	clientRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_client_requests_total",
		Help: "Outgoing HTTP requests, by service, target host and status code (\"error\" when no response).",
	}, []string{"service", "peer", "code"})

	clientDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_client_request_duration_seconds",
		Help:    "Time for an outgoing HTTP request to get a response.",
		Buckets: latencyBuckets,
	}, []string{"service", "peer"})
)

// MetricsHandler serves every registered metric in the Prometheus format.
func MetricsHandler() http.Handler { return promhttp.Handler() }

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// Flush keeps streaming responses working through the recorder.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Middleware wraps a service's whole HTTP handler. For every request it:
//   - continues the caller's trace from the traceparent header, or starts one;
//   - makes sure X-Correlation-ID is set on the request and the response, so
//     the service's own correlation middleware reuses the same ID;
//   - records http_requests_total, http_request_duration_seconds and
//     http_requests_in_flight.
//
// The route label is the request path. None of the services put IDs in their
// paths, so this stays bounded; requests that end in 404 are labelled
// "unmatched" so unknown paths cannot create new series. /metrics is served
// without being measured.
func Middleware(service string, next http.Handler) http.Handler {
	prop := otel.GetTextMapPropagator
	inFlight := httpInFlight.WithLabelValues(service)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()

		corrID := r.Header.Get(CorrelationHeader)
		if corrID == "" {
			corrID = NewCorrelationID()
			r.Header.Set(CorrelationHeader, corrID)
		}
		w.Header().Set(CorrelationHeader, corrID)

		ctx := prop().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx = WithCorrelationID(ctx, corrID)
		ctx, span := Tracer().Start(ctx, r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("service", service),
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
				attribute.String("correlation_id", corrID),
			))
		defer span.End()

		inFlight.Inc()
		defer inFlight.Dec()

		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(ctx))

		route := r.URL.Path
		if rec.code == http.StatusNotFound {
			route = "unmatched"
		}
		code := strconv.Itoa(rec.code)
		span.SetAttributes(attribute.Int("http.response.status_code", rec.code))
		if rec.code >= 500 {
			span.SetStatus(codes.Error, http.StatusText(rec.code))
		}
		httpRequests.WithLabelValues(service, r.Method, route, code).Inc()
		httpDuration.WithLabelValues(service, r.Method, route).Observe(time.Since(start).Seconds())
	})
}

// Transport wraps an HTTP client transport. Each outgoing request gets a
// client span, the traceparent header, and the X-Correlation-ID from its
// context (unless the caller already set one), and is recorded in
// http_client_requests_total and http_client_request_duration_seconds.
// base may be nil for http.DefaultTransport.
func Transport(service string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &transport{service: service, base: base}
}

type transport struct {
	service string
	base    http.RoundTripper
}

func (t *transport) RoundTrip(r *http.Request) (*http.Response, error) {
	start := time.Now()
	ctx, span := Tracer().Start(r.Context(), r.Method+" "+r.URL.Host+r.URL.Path,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("server.address", r.URL.Host),
			attribute.String("url.path", r.URL.Path),
		))
	defer span.End()

	r = r.Clone(ctx) // RoundTrippers must not modify the caller's request
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(r.Header))
	if r.Header.Get(CorrelationHeader) == "" {
		if id := CorrelationID(ctx); id != "" {
			r.Header.Set(CorrelationHeader, id)
		}
	}

	resp, err := t.base.RoundTrip(r)
	clientDuration.WithLabelValues(t.service, r.URL.Host).Observe(time.Since(start).Seconds())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		clientRequests.WithLabelValues(t.service, r.URL.Host, "error").Inc()
		return nil, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 500 {
		span.SetStatus(codes.Error, http.StatusText(resp.StatusCode))
	}
	clientRequests.WithLabelValues(t.service, r.URL.Host, strconv.Itoa(resp.StatusCode)).Inc()
	return resp, nil
}

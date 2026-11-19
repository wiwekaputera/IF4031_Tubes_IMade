package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// recordSpans installs an in-memory tracer provider for one test.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return rec
}

// One request through two services must form one trace, and the downstream
// service must see the same correlation ID.
func TestTraceAndCorrelationCrossServices(t *testing.T) {
	spans := recordSpans(t)

	var gotCorr, gotParent string
	downstream := httptest.NewServer(Middleware("test-down", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCorr = r.Header.Get(CorrelationHeader)
		gotParent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	})))
	defer downstream.Close()

	client := &http.Client{Transport: Transport("test-up", nil)}
	upstream := httptest.NewServer(Middleware("test-up", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, downstream.URL+"/inner", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		resp.Body.Close()
		w.WriteHeader(http.StatusOK)
	})))
	defer upstream.Close()

	req, _ := http.NewRequest(http.MethodGet, upstream.URL+"/outer", nil)
	req.Header.Set(CorrelationHeader, "corr-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if gotCorr != "corr-123" {
		t.Fatalf("downstream correlation ID = %q, want corr-123", gotCorr)
	}
	if gotParent == "" {
		t.Fatal("downstream got no traceparent header")
	}
	if resp.Header.Get(CorrelationHeader) != "corr-123" {
		t.Fatalf("response correlation ID = %q", resp.Header.Get(CorrelationHeader))
	}

	ended := spans.Ended()
	if len(ended) != 3 { // upstream server, client call, downstream server
		t.Fatalf("got %d spans, want 3", len(ended))
	}
	traceID := ended[0].SpanContext().TraceID()
	for _, s := range ended {
		if s.SpanContext().TraceID() != traceID {
			t.Fatalf("span %q is in a different trace", s.Name())
		}
	}
}

func TestMetricsAndUnmatchedRoute(t *testing.T) {
	recordSpans(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /known", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) })
	srv := httptest.NewServer(Middleware("test-metrics", mux))
	defer srv.Close()

	for _, p := range []string{"/known", "/known", "/random-1", "/random-2"} {
		resp, err := http.Get(srv.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if n := testutil.ToFloat64(httpRequests.WithLabelValues("test-metrics", "GET", "/known", "418")); n != 2 {
		t.Fatalf("known route count = %v, want 2", n)
	}
	if n := testutil.ToFloat64(httpRequests.WithLabelValues("test-metrics", "GET", "unmatched", "404")); n != 2 {
		t.Fatalf("unmatched count = %v, want 2 (unknown paths must share one label)", n)
	}
}

func TestGeneratesCorrelationIDWhenMissing(t *testing.T) {
	recordSpans(t)
	var seen string
	srv := httptest.NewServer(Middleware("test-gen", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Get(CorrelationHeader)
		if CorrelationID(r.Context()) != seen {
			t.Error("context and header disagree")
		}
	})))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if seen == "" || resp.Header.Get(CorrelationHeader) != seen {
		t.Fatalf("seen=%q response=%q", seen, resp.Header.Get(CorrelationHeader))
	}
}

// Trace and correlation ID must survive a trip through message headers.
func TestMessageHeadersCarryTrace(t *testing.T) {
	spans := recordSpans(t)
	ctx := WithCorrelationID(context.Background(), "corr-msg")
	ctx, pub := StartPublish(ctx, "producer", "bnpb.events.x")
	h := Headers{}
	Inject(ctx, h)
	pub.End("ok", nil)

	_, con := StartConsume(context.Background(), "consumer", "bnpb.events.x", h)
	con.End("ok", nil)

	got := Extract(context.Background(), h)
	if CorrelationID(got) != "corr-msg" {
		t.Fatalf("correlation = %q", CorrelationID(got))
	}
	ended := spans.Ended()
	if len(ended) != 2 || ended[0].SpanContext().TraceID() != ended[1].SpanContext().TraceID() {
		t.Fatalf("producer and consumer spans are not one trace: %d spans", len(ended))
	}
	if n := testutil.ToFloat64(messages.WithLabelValues("consumer", "bnpb.events.x", "process", "ok")); n != 1 {
		t.Fatalf("consumed count = %v", n)
	}
}

func TestLogHookAddsTraceID(t *testing.T) {
	recordSpans(t)
	ctx, span := Tracer().Start(context.Background(), "op")
	defer span.End()

	var buf bytes.Buffer
	logger := zerolog.New(&buf).Hook(LogHook{})
	logger.Info().Ctx(ctx).Msg("hello")
	logger.Info().Msg("no context")

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	var first, second map[string]any
	_ = json.Unmarshal(lines[0], &first)
	_ = json.Unmarshal(lines[1], &second)
	if first["trace_id"] != TraceID(ctx) || first["trace_id"] == "" {
		t.Fatalf("first line = %s", lines[0])
	}
	if _, ok := second["trace_id"]; ok {
		t.Fatalf("line without context got a trace_id: %s", lines[1])
	}
}

func TestInitWithoutCollectorIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	shutdown, err := Init(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsEndpointIsNotMeasured(t *testing.T) {
	recordSpans(t)
	mux := http.NewServeMux()
	mux.Handle("/metrics", MetricsHandler())
	srv := httptest.NewServer(Middleware("test-self", mux))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte("go_goroutines")) {
		t.Fatal("metrics endpoint did not serve Prometheus text")
	}
	if n := testutil.ToFloat64(httpRequests.WithLabelValues("test-self", "GET", "/metrics", "200")); n != 0 {
		t.Fatalf("/metrics was counted %v times", n)
	}
}

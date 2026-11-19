// Package pgxtrace records PostgreSQL queries made through pgx as trace spans
// and in the db_query_duration_seconds histogram. Attach it to a pool with:
//
//	cfg, _ := pgxpool.ParseConfig(dsn)
//	cfg.ConnConfig.Tracer = pgxtrace.Tracer{Service: "aggregator"}
package pgxtrace

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/wiwekaputera/if4031-tubes-imade/observability"
)

// maxStatement caps the SQL text stored on a span.
const maxStatement = 1000

var queryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "db_query_duration_seconds",
	Help:    "Time for one database query, by service, operation and result.",
	Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
}, []string{"service", "operation", "result"})

// Tracer implements pgx.QueryTracer.
type Tracer struct {
	Service string
}

type startKey struct{}

type started struct {
	at time.Time
	op string
}

// TraceQueryStart opens a span for the query.
func (t Tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	op := Operation(data.SQL)
	stmt := strings.TrimSpace(data.SQL)
	if len(stmt) > maxStatement {
		stmt = stmt[:maxStatement]
	}
	ctx, _ = observability.Tracer().Start(ctx, "db "+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation.name", op),
			attribute.String("db.query.text", stmt),
		))
	return context.WithValue(ctx, startKey{}, started{at: time.Now(), op: op})
}

// TraceQueryEnd closes the span and records the duration.
func (t Tracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	result := "ok"
	if data.Err != nil {
		result = "error"
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())
	}
	span.End()
	if s, ok := ctx.Value(startKey{}).(started); ok {
		queryDuration.WithLabelValues(t.Service, s.op, result).Observe(time.Since(s.at).Seconds())
	}
}

// Operation returns the SQL verb of a statement in upper case, e.g. SELECT.
// A leading WITH clause is reported as WITH.
func Operation(sql string) string {
	s := strings.TrimSpace(sql)
	for strings.HasPrefix(s, "--") { // skip leading comment lines
		if i := strings.IndexByte(s, '\n'); i >= 0 {
			s = strings.TrimSpace(s[i+1:])
		} else {
			return "UNKNOWN"
		}
	}
	end := strings.IndexFunc(s, func(r rune) bool { return r == ' ' || r == '\n' || r == '\t' || r == '(' })
	if end < 0 {
		end = len(s)
	}
	if end == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(s[:end])
}

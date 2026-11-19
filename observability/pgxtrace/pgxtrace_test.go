package pgxtrace

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOperation(t *testing.T) {
	cases := map[string]string{
		"SELECT 1":                             "SELECT",
		"  insert into t values (1)":           "INSERT",
		"\n\t\tUPDATE x SET y = 1":             "UPDATE",
		"WITH a AS (SELECT 1) SELECT * FROM a": "WITH",
		"-- note\nDELETE FROM t":               "DELETE",
		"select(1)":                            "SELECT",
		"":                                     "UNKNOWN",
	}
	for sql, want := range cases {
		if got := Operation(sql); got != want {
			t.Errorf("Operation(%q) = %q, want %q", sql, got, want)
		}
	}
}

// TestQueriesBecomeSpans needs OBS_TEST_DATABASE_URL and is skipped otherwise.
func TestQueriesBecomeSpans(t *testing.T) {
	dsn := os.Getenv("OBS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("OBS_TEST_DATABASE_URL not set")
	}
	rec := tracetest.NewSpanRecorder()
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	defer otel.SetTracerProvider(prev)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = Tracer{Service: "test"}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var n int
	if err := pool.QueryRow(context.Background(), "SELECT 41 + 1").Scan(&n); err != nil || n != 42 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	_, _ = pool.Exec(context.Background(), "SELECT * FROM table_that_does_not_exist")

	var names []string
	for _, s := range rec.Ended() {
		names = append(names, s.Name()+"/"+s.Status().Code.String())
	}
	want := map[string]bool{"db SELECT/Unset": false, "db SELECT/Error": false}
	for _, n := range names {
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	for k, ok := range want {
		if !ok {
			t.Errorf("missing span %s; got %v", k, names)
		}
	}
}

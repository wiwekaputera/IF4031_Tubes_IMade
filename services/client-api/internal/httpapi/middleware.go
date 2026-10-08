package httpapi

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

type contextKey string

const correlationIDKey contextKey = "correlation_id"

// CorrelationIDFromContext retrieves the correlation ID from context.
func CorrelationIDFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(correlationIDKey).(string); ok {
		return val
	}
	return ""
}

func generateCorrelationID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("corr-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// CorrelationIDMiddleware extracts or injects an X-Correlation-ID header into the request and response.
func CorrelationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corrID := r.Header.Get("X-Correlation-ID")
		if corrID == "" {
			corrID = generateCorrelationID()
		}
		w.Header().Set("X-Correlation-ID", corrID)
		ctx := context.WithValue(r.Context(), correlationIDKey, corrID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

// LoggingMiddleware logs all incoming requests in structured JSON using zerolog.
func LoggingMiddleware(logger zerolog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(rec, r)

		latencyMs := time.Since(start).Milliseconds()
		corrID := CorrelationIDFromContext(r.Context())

		event := logger.Info()
		if rec.statusCode >= 500 {
			event = logger.Error()
		} else if rec.statusCode >= 400 {
			event = logger.Warn()
		}

		event.
			Str("service", "client-api").
			Str("correlation_id", corrID).
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("query", r.URL.RawQuery).
			Int("status", rec.statusCode).
			Int64("latency_ms", latencyMs).
			Str("remote_addr", r.RemoteAddr).
			Msg("request processed")
	})
}

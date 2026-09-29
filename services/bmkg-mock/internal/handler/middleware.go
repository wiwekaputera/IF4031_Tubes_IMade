package handler

import (
	"context"
	"crypto/rand"
	"fmt"
	mathrand "math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// Middleware is the standard idiomatic net/http middleware signature.
type Middleware func(http.Handler) http.Handler

// Chain builds a middleware stack that executes middlewares in the order they are passed.
func Chain(middlewares ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

type contextKey string

const (
	correlationIDKey contextKey = "correlation_id"
)

// CorrelationIDFromContext extracts the correlation ID from context.
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
	// UUID v4 format
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// CorrelationIDMiddleware ensures every request has an X-Correlation-ID header and context value.
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

// LatencyMiddleware returns a Middleware that injects a random jitter delay between minMs and maxMs.
func LatencyMiddleware(minMs, maxMs int) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var delayMs int
			if maxMs > minMs {
				delayMs = minMs + mathrand.Intn(maxMs-minMs+1)
			} else {
				delayMs = minMs
			}

			if delayMs > 0 {
				time.Sleep(time.Duration(delayMs) * time.Millisecond)
			}

			next.ServeHTTP(w, r)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

// LoggingMiddleware returns a Middleware that logs request details in structured JSON using zerolog.
func LoggingMiddleware(logger zerolog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
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
				Str("service", "bmkg-mock").
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
}

// AuthMiddleware returns a Middleware that validates BMKG credentials and rejects PVMBG cross-credentials.
func AuthMiddleware(expectedKey string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 1. Cross-Credential Handling:
			// If Authorization header (e.g. Bearer token from PVMBG) is present, explicitly reject with 403 Forbidden.
			authHeader := r.Header.Get("Authorization")
			if authHeader != "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"Forbidden: cross-credential rejected (PVMBG Bearer credentials presented to BMKG endpoint)"}`))
				return
			}

			// 2. BMKG API Key Validation:
			// Validate incoming header X-BMKG-Key against expected key.
			bmkgKey := strings.TrimSpace(r.Header.Get("X-BMKG-Key"))
			if bmkgKey == "" || bmkgKey != expectedKey {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"Unauthorized: invalid or missing X-BMKG-Key"}`))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

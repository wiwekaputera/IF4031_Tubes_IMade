package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
)

const maxNotifyBody = 1 << 20

type notification struct {
	IdempotencyKey string         `json:"idempotency_key"`
	ReceivedAt     time.Time      `json:"received_at"`
	Event          event.Envelope `json:"event"`
}

type sink struct {
	mu     sync.RWMutex
	byKey  map[string]notification
	order  []string
	logger zerolog.Logger
}

func newSink(logger zerolog.Logger) *sink {
	return &sink{
		byKey:  make(map[string]notification),
		logger: logger,
	}
}

func (s *sink) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /notify", s.notify)
	mux.HandleFunc("GET /received", s.received)
	return mux
}

func (s *sink) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *sink) notify(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxNotifyBody)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	var envelope event.Envelope
	if err := decoder.Decode(&envelope); err != nil {
		http.Error(w, "request body must be a valid event envelope", http.StatusBadRequest)
		return
	}
	if err := ensureEOF(decoder); err != nil {
		http.Error(w, "request body must contain one JSON event envelope", http.StatusBadRequest)
		return
	}
	if envelope.EventID == "" || envelope.Version < 1 {
		http.Error(w, "event_id and a positive version are required", http.StatusBadRequest)
		return
	}
	expectedKey := fmt.Sprintf("%s:%d", envelope.EventID, envelope.Version)
	if key != expectedKey {
		http.Error(w, "Idempotency-Key must match event_id:version", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	if _, exists := s.byKey[key]; exists {
		s.mu.Unlock()
		s.logger.Info().
			Str("event_key", key).
			Str("correlation_id", envelope.TraceID).
			Dur("latency", time.Since(started)).
			Msg("duplicate notification ignored")
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate", "idempotency_key": key})
		return
	}
	s.byKey[key] = notification{IdempotencyKey: key, ReceivedAt: time.Now().UTC(), Event: envelope}
	s.order = append(s.order, key)
	s.mu.Unlock()

	s.logger.Info().
		Str("event_key", key).
		Str("correlation_id", envelope.TraceID).
		Dur("latency", time.Since(started)).
		Msg("notification accepted")
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "idempotency_key": key})
}

func (s *sink) received(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	items := make([]notification, 0, len(s.order))
	for _, key := range s.order {
		items = append(items, s.byKey[key])
	}
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"count": len(items), "data": items})
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected additional JSON value")
		}
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("service", "stub-sink").Logger()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8085"
	}
	addr := port
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           newSink(logger).routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", addr).Msg("stub sink listening")
		serverErr <- server.ListenAndServe()
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-sigCtx.Done():
		ctx, cancel := newShutdownContext()
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Error().Err(err).Msg("stub sink shutdown failed")
		}
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("stub sink failed")
		}
	}
}

func newShutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

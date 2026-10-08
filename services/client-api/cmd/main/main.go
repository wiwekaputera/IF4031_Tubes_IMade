package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/httpapi"
)

func main() {
	// Configure zerolog structured logger
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Load configuration with fail-fast validation
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("client-api: failed to load configuration")
	}

	// Set configured log level
	if lvl, err := zerolog.ParseLevel(cfg.LogLevel); err == nil {
		logger = logger.Level(lvl)
	}

	logger.Info().
		Str("port", cfg.Port).
		Str("aggregator_base_url", cfg.AggregatorBaseURL).
		Str("jwks_url", cfg.JWKSURL).
		Float64("rps_limit", cfg.RPSLimit).
		Int("burst", cfg.Burst).
		Msg("client-api: starting up")

	// Setup HTTP routing
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpapi.HealthHandler())

	// Chain middlewares: CorrelationID -> Logging -> Handlers
	handler := httpapi.CorrelationIDMiddleware(httpapi.LoggingMiddleware(logger, mux))

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info().Str("addr", server.Addr).Msg("client-api: listening")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Err(err).Msg("client-api: server error")
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("client-api: shutting down gracefully")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("client-api: shutdown error")
	}
	logger.Info().Msg("client-api: stopped")
}

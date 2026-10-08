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

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/httpapi"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/jwks"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/ratelimit"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Initialize JWKS cache and fetch public keys with retry & backoff
	jwksCache := jwks.New(jwks.Options{
		JWKSURL: cfg.JWKSURL,
		Logger:  logger,
	})

	initCtx, cancelInit := context.WithTimeout(ctx, 30*time.Second)
	defer cancelInit()

	if err := jwksCache.FetchInitial(initCtx, 10, 500*time.Millisecond); err != nil {
		logger.Fatal().Err(err).Msg("client-api: failed to fetch initial JWKS from token-service")
	}

	jwksCache.StartPeriodicRefresh(ctx)

	// Rate Limiter per client identity (sub)
	limiter := ratelimit.New(ratelimit.Options{
		RPS:         cfg.RPSLimit,
		Burst:       cfg.Burst,
		IdleTimeout: 10 * time.Minute,
		SkipPaths:   []string{"/health"},
	})
	limiter.StartCleanupWorker(ctx, 5*time.Minute)

	// Auth Middleware with JWKS cache, ES256 enforcement, issuer & audience validation
	authMiddleware := auth.Middleware(auth.Config{
		KeyProvider: jwksCache,
		Issuer:      cfg.TokenIssuer,
		Audience:    cfg.TokenAudience,
		Leeway:      5 * time.Second,
		SkipPaths:   []string{"/health"},
		Logger:      logger,
	})

	// Upstream Aggregator client
	aggClient := upstream.NewClient(upstream.Options{
		BaseURL: cfg.AggregatorBaseURL,
		Timeout: 2 * time.Second,
		Logger:  logger,
	})

	// Setup HTTP routing
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpapi.HealthHandler())
	mux.HandleFunc("GET /v1/events", httpapi.EventsHandler(aggClient))

	// Chain middlewares: CorrelationID -> Logging -> Auth -> Rate limit -> Handler
	handler := httpapi.CorrelationIDMiddleware(
		httpapi.LoggingMiddleware(logger,
			authMiddleware(
				limiter.Middleware(mux),
			),
		),
	)

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

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

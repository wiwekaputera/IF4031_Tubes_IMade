package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/clients"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/httpapi"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/issuer"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/keys"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/store"
)

func main() {
	// Configure zerolog structured logger
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Load configuration with fail-fast validation
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("token-service: failed to load configuration")
	}

	// Set configured log level
	if lvl, err := zerolog.ParseLevel(cfg.LogLevel); err == nil {
		logger = logger.Level(lvl)
	}

	// Initialize ES256 private key and JWKS
	keyManager, err := keys.NewKeyManager(cfg.PrivateKeyB64)
	if err != nil {
		logger.Fatal().Err(err).Msg("token-service: failed to initialize signing key")
	}

	logger.Info().
		Str("port", cfg.Port).
		Str("kid", keyManager.ActiveKeyID()).
		Str("issuer", cfg.TokenIssuer).
		Str("audience", cfg.TokenAudience).
		Dur("access_token_ttl", cfg.AccessTokenTTL).
		Int("refresh_token_ttl_days", cfg.RefreshTokenTTLDays).
		Msg("token-service: starting up")

	// Connect to token-db
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dbCancel()

	poolConfig, err := pgxpool.ParseConfig(cfg.DBDSN)
	if err != nil {
		logger.Fatal().Err(err).Msg("token-service: invalid database DSN")
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2
	poolConfig.MaxConnLifetime = 30 * time.Minute

	dbPool, err := pgxpool.NewWithConfig(dbCtx, poolConfig)
	if err != nil {
		logger.Fatal().Err(err).Msg("token-service: failed to create token-db pool")
	}
	defer dbPool.Close()

	if err := dbPool.Ping(dbCtx); err != nil {
		logger.Fatal().Err(err).Msg("token-service: cannot ping token-db")
	}
	logger.Info().Msg("token-service: connected to token-db")

	// Setup client registry, refresh token store, and issuer
	clientRegistry := clients.NewRegistry(cfg)
	refreshStore := store.NewPGStore(dbPool)
	tokenIssuer := issuer.NewIssuer(cfg, keyManager, clientRegistry, refreshStore)

	// Setup HTTP routing
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpapi.HealthHandler(dbPool))
	mux.HandleFunc("GET /.well-known/jwks.json", httpapi.JWKSHandler(keyManager))
	mux.HandleFunc("POST /oauth/token", httpapi.TokenHandler(tokenIssuer))

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
		logger.Info().Str("addr", server.Addr).Msg("token-service: listening")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal().Err(err).Msg("token-service: server error")
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("token-service: shutting down gracefully")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("token-service: shutdown error")
	}
	logger.Info().Msg("token-service: stopped")
}

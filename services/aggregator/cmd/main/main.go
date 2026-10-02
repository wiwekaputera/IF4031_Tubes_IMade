package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/poller"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/queryapi"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

func main() {
	// 1. Load runtime configuration
	cfg := config.Load()

	// 2. Initialize structured logging
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logLevel, err := zerolog.ParseLevel(strings.ToLower(cfg.LogLevel))
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(logLevel)

	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "aggregator").
		Logger()

	logger.Info().
		Str("port", cfg.Port).
		Str("bmkg_base_url", cfg.BMKGBaseURL).
		Str("pvmbg_base_url", cfg.PVMBGBaseURL).
		Str("db_dsn", cfg.DBDSN).
		Str("nats_url", cfg.NATSURL).
		Msg("Starting aggregator service")

	// 3. Connect to canonical PostgreSQL store via connection pool
	dbPool, err := pgxpool.New(context.Background(), cfg.DBDSN)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer dbPool.Close()

	ctx := context.Background()
	if err := dbPool.Ping(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to ping database")
	}
	logger.Info().Msg("Connected to canonical store")

	repo := store.NewPostgresRepo(dbPool, logger)

	// 4. Load static volcano references for normalizer lookups
	volcanoRefs, err := repo.GetVolcanoReferences(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to load volcano references, using empty map")
		volcanoRefs = make(map[string]store.VolcanoReference)
	}
	logger.Info().Int("volcano_count", len(volcanoRefs)).Msg("Loaded volcano references")

	// 5. Initialize normalizer, poller, and query API handlers
	norm := normalizer.New(logger, volcanoRefs)
	p := poller.New(cfg, norm, repo, logger)

	healthFn := func() (string, string, *time.Time) {
		hs := p.Health()
		var pvmbgStaleSince *time.Time
		if hs.PVMBG.StaleSince != nil {
			t := *hs.PVMBG.StaleSince
			pvmbgStaleSince = &t
		}
		return hs.BMKG.Status, hs.PVMBG.Status, pvmbgStaleSince
	}

	h := queryapi.New(repo, logger, healthFn)

	// 6. Start background polling loop for upstream agencies (BMKG & PVMBG)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(1)
	go poller.StartPolling(ctx, &wg, p, cfg, logger)

	// 7. Initialize HTTP server for internal query API
	addr := cfg.Port
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	httpServer := queryapi.Server(queryapi.Routes(h), addr)

	serverErrChan := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", addr).Msg("HTTP server listening")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrChan <- err
		}
	}()

	// 8. Graceful shutdown on SIGINT or SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Info().Str("signal", sig.String()).Msg("Received termination signal, shutting down gracefully")
	case err := <-serverErrChan:
		logger.Error().Err(err).Msg("Server encountered fatal error")
	}

	// Stop background polling workers and wait for in-flight cycles to finish
	cancel()
	wg.Wait()

	// Shutdown HTTP server with 5s deadline
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("HTTP server forced to shutdown")
	} else {
		logger.Info().Msg("HTTP server shut down gracefully")
	}
}

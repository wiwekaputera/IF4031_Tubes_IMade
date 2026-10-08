package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/outboxrelay"
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
		Msg("Starting aggregator service")

	if u, err := url.Parse(cfg.DBDSN); err == nil {
		logger.Info().Str("db_host", u.Host).Str("db_name", strings.TrimPrefix(u.Path, "/")).Msg("Configured database")
	} else {
		logger.Info().Msg("Configured database (DSN unparseable)")
	}
	if u, err := url.Parse(cfg.NATSURL); err == nil {
		logger.Info().Str("nats_host", u.Host).Msg("Configured NATS")
	} else {
		logger.Info().Msg("Configured NATS (URL unparseable)")
	}

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

	// 4. Connect to JetStream and ensure the event stream is ready before
	// starting ingestion, so every committed outbox row has a delivery path.
	nc, err := nats.Connect(
		cfg.NATSURL,
		nats.Name("BNPB Aggregator"),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to NATS")
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize JetStream")
	}
	if err := outboxrelay.EnsureStream(js); err != nil {
		logger.Fatal().Err(err).Msg("failed to ensure JetStream event stream")
	}
	logger.Info().Str("stream", "BNPB_EVENTS").Msg("JetStream event stream ready")

	relay := outboxrelay.New(
		repo,
		outboxrelay.NewJetStreamPublisher(js),
		logger,
		cfg.OutboxPollInterval,
		cfg.OutboxBatchSize,
	)

	// 5. Load static volcano references for normalizer lookups
	volcanoRefs, err := repo.GetVolcanoReferences(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to load volcano references, using empty map")
		volcanoRefs = make(map[string]store.VolcanoReference)
	}
	logger.Info().Int("volcano_count", len(volcanoRefs)).Msg("Loaded volcano references")

	// 6. Initialize normalizer, poller, and query API handlers
	norm := normalizer.New(logger, volcanoRefs)
	p := poller.New(cfg, norm, repo, logger)
	if err := p.HydrateWatermarks(ctx); err != nil {
		logger.Warn().Err(err).Msg("failed to hydrate poller watermarks on startup")
	}

	healthFn := func() (string, string, time.Time, time.Time, *time.Time) {
		hs := p.Health()
		var pvmbgStaleSince *time.Time
		if hs.PVMBG.StaleSince != nil {
			t := *hs.PVMBG.StaleSince
			pvmbgStaleSince = &t
		}
		return hs.BMKG.Status, hs.PVMBG.Status, hs.BMKG.LastSynced, hs.PVMBG.LastSynced, pvmbgStaleSince
	}

	h := queryapi.New(repo, logger, healthFn)

	// 7. Start background polling loop and transactional outbox relay.
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg.Add(2)
	go poller.StartPolling(ctx, &wg, p, cfg, logger)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	// 8. Initialize HTTP server for internal query API
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

	// 9. Graceful shutdown on SIGINT or SIGTERM
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

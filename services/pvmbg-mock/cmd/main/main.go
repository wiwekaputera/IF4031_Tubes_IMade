package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/admin"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/generator"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/handler"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/seed"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "pvmbg-mock").
		Logger()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("pvmbg-mock: failed to load configuration")
	}

	logLevel, err := zerolog.ParseLevel(strings.ToLower(cfg.LogLevel))
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(logLevel)

	logger.Info().
		Str("port", cfg.Port).
		Int("delay_min_ms", cfg.DelayMinMs).
		Int("delay_max_ms", cfg.DelayMaxMs).
		Int("event_interval_s", cfg.EventIntervalS).
		Msg("Starting pvmbg-mock service")

	memStore := store.NewMemoryStore(cfg.StoreMaxCapacity)
	seed.PopulateSeeds(memStore)

	seedCount := memStore.Count()
	logger.Info().
		Int("seeded_volcanic_reports", seedCount).
		Msg("Historical volcanic data seeded successfully")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	outage := admin.NewOutageState()
	schemaVersion := admin.NewSchemaVersionState()

	eventGen := generator.NewGenerator(memStore, cfg.EventIntervalS, schemaVersion, logger)
	go eventGen.Start(ctx)

	h := handler.NewHandler(memStore, cfg, outage, schemaVersion, logger)

	addr := cfg.Port
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrChan := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", addr).Msg("HTTP server listening")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrChan <- err
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Info().Str("signal", sig.String()).Msg("Received termination signal, shutting down gracefully")
	case err := <-serverErrChan:
		logger.Error().Err(err).Msg("Server encountered fatal error")
	}

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("HTTP server forced to shutdown")
	} else {
		logger.Info().Msg("HTTP server shut down gracefully")
	}
}

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
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/generator"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/handler"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/seed"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
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
		Str("service", "bmkg-mock").
		Logger()

	logger.Info().
		Str("port", cfg.Port).
		Int("delay_min_ms", cfg.DelayMinMs).
		Int("delay_max_ms", cfg.DelayMaxMs).
		Int("event_interval_s", cfg.EventIntervalS).
		Msg("Starting bmkg-mock service")

	// 3. Initialize in-memory store and populate historical seed data
	memStore := store.NewMemoryStore(cfg.StoreMaxCapacity)
	seed.PopulateSeeds(memStore)

	seismicCount, tsunamiCount := memStore.Count()
	logger.Info().
		Int("seeded_seismic_events", seismicCount).
		Int("seeded_tsunami_warnings", tsunamiCount).
		Msg("Historical disaster data seeded successfully")

	// 4. Start background dynamic event generator
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventGen := generator.NewGenerator(memStore, cfg.EventIntervalS, logger)
	go eventGen.Start(ctx)

	// 5. Initialize HTTP router and server
	h := handler.NewHandler(memStore, cfg, logger)

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

	// 6. Graceful shutdown on SIGINT or SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Info().Str("signal", sig.String()).Msg("Received termination signal, shutting down gracefully")
	case err := <-serverErrChan:
		logger.Error().Err(err).Msg("Server encountered fatal error")
	}

	// Cancel background workers
	cancel()

	// Shutdown HTTP server with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("HTTP server forced to shutdown")
	} else {
		logger.Info().Msg("HTTP server shut down gracefully")
	}
}

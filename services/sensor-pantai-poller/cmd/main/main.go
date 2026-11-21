// sensor-pantai-poller reads the coastal sensor (Sensor Pantai) and publishes
// coastal HazardEvents, critical alerts and rejected data to JetStream.
//
// It runs as its own process with its own connection pool, timeouts and
// circuit breaker (a bulkhead): however the sensor misbehaves, the aggregator's
// BMKG and PVMBG polling shares nothing with it.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/observability"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/breaker"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/jetstream"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/poller"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/sensor"
	"github.com/wiwekaputera/if4031-tubes-imade/sensor-pantai-poller/internal/validate"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("service", "sensor-pantai-poller").Logger().Hook(observability.LogHook{})

	shutdownTracing, err := observability.Init(context.Background(), "sensor-pantai-poller")
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to set up tracing")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

	cfg, err := loadConfig()
	if err != nil {
		logger.Fatal().Err(err).Msg("invalid configuration")
	}
	logger.Info().Str("sensor_url", cfg.sensorURL).Dur("poll_interval", cfg.poller.PollInterval).
		Dur("request_timeout", cfg.poller.RequestTimeout).Int("breaker_threshold", cfg.breaker.FailureThreshold).
		Dur("breaker_open", cfg.breaker.BaseOpen).Dur("breaker_max_open", cfg.breaker.MaxOpen).
		Dur("max_observation_age", cfg.poller.Limits.MaxAge).Msg("starting")

	conn, err := nats.Connect(cfg.natsURL, nats.Name("BNPB Sensor Pantai Poller"),
		nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		logger.Fatal().Err(err).Msg("failed connecting to NATS")
	}
	defer conn.Close()
	js, err := conn.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed creating JetStream context")
	}
	stream := jetstream.New(js)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for ctx.Err() == nil {
		if err := stream.EnsureAlertStream(); err == nil {
			break
		} else {
			logger.Warn().Err(err).Msg("alert stream not ready; retrying in 2s")
		}
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}

	client := sensor.New(cfg.sensorURL, cfg.token, cfg.poller.RequestTimeout, cfg.maxConns, nil)
	p := poller.New(cfg.poller, client, stream, stream, breaker.New(cfg.breaker), logger)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", observability.MetricsHandler())
	// Health reflects this process only. A sick sensor is not a reason to
	// restart the poller; that would only add load on the sensor.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !conn.IsConnected() {
			http.Error(w, `{"status":"nats_disconnected"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
	server := &http.Server{Addr: cfg.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("health server failed")
		}
	}()

	p.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	logger.Info().Msg("stopped")
}

type config struct {
	sensorURL string
	token     string
	natsURL   string
	addr      string
	maxConns  int
	poller    poller.Config
	breaker   breaker.Config
}

func loadConfig() (config, error) {
	var errs []string
	ms := func(key string, def int) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return time.Duration(def) * time.Millisecond
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer (ms)", key))
			return time.Duration(def) * time.Millisecond
		}
		return time.Duration(n) * time.Millisecond
	}
	num := func(key string, def int) int {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be a positive integer", key))
			return def
		}
		return n
	}
	dur := func(key string, def time.Duration) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Sprintf("%s must be a positive duration like 2h", key))
			return def
		}
		return d
	}

	limits := validate.DefaultLimits()
	limits.MaxAge = dur("SENSOR_MAX_OBSERVATION_AGE", limits.MaxAge)

	cfg := config{
		sensorURL: strings.TrimRight(envOr("SENSOR_PANTAI_URL", "http://sensor-pantai:8080"), "/"),
		token:     os.Getenv("PANTAI_AUTH_TOKEN"),
		natsURL:   envOr("NATS_URL", "nats://nats-jetstream:4222"),
		addr:      ":" + envOr("PORT", "8089"),
		maxConns:  num("SENSOR_MAX_CONNS", 2),
		poller: poller.Config{
			PollInterval:       ms("SENSOR_POLL_INTERVAL_MS", 1000),
			PageLimit:          num("SENSOR_PAGE_LIMIT", 200),
			MaxPagesPerRun:     num("SENSOR_MAX_PAGES_PER_CYCLE", 20),
			RequestTimeout:     ms("SENSOR_REQUEST_TIMEOUT_MS", 2000),
			RetryBase:          ms("SENSOR_RETRY_BASE_MS", 500),
			RetryMax:           ms("SENSOR_RETRY_MAX_MS", 8000),
			StallAfter:         ms("SENSOR_STALL_AFTER_MS", 15000),
			SilenceAfter:       dur("SENSOR_SILENCE_AFTER", 10*time.Minute),
			StationUpdateEvery: dur("SENSOR_STATION_UPDATE_EVERY", 5*time.Minute),
			StationsRefresh:    5 * time.Minute,
			Limits:             limits,
			Thresholds:         poller.DefaultThresholds(),
		},
		breaker: breaker.Config{
			FailureThreshold: num("SENSOR_BREAKER_FAILURES", 3),
			BaseOpen:         ms("SENSOR_BREAKER_OPEN_MS", 5000),
			MaxOpen:          ms("SENSOR_BREAKER_MAX_OPEN_MS", 30000),
		},
	}
	if cfg.token == "" {
		errs = append(errs, "PANTAI_AUTH_TOKEN is required")
	}
	if len(errs) > 0 {
		return cfg, errors.New(strings.Join(errs, "; "))
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

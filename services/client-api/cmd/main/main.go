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
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/fieldreports"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/httpapi"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/jwks"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/loadshed"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/ratelimit"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/sensorstatus"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
)

func main() {
	// Configure zerolog structured logger
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger().Hook(observability.LogHook{})

	shutdownTracing, err := observability.Init(context.Background(), "client-api")
	if err != nil {
		logger.Fatal().Err(err).Msg("client-api: failed to set up tracing")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

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
		Str("nats_url", cfg.NATSURL).
		Int("max_inflight", cfg.MaxInflight).
		Float64("media_inflight_share", cfg.MediaInflightShare).
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

	// Field reports are stored in JetStream. The connection is retried in the
	// background so reads keep working while the broker is unreachable.
	reportPublisher, err := fieldreports.NewNATSPublisher(cfg.NATSURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("client-api: failed to set up field report publisher")
	}
	defer reportPublisher.Close()
	go func() {
		// Create the stream early so the consumer can bind before the first report.
		for ctx.Err() == nil {
			if err := reportPublisher.EnsureStream(); err == nil {
				logger.Info().Msg("client-api: field report stream ready")
				return
			}
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}()
	fieldReports := httpapi.FieldReportsHandler(httpapi.FieldReportsOptions{
		Publisher:      reportPublisher,
		PublishTimeout: cfg.FieldReportPublishTimeout,
		Logger:         logger,
	})

	// Setup HTTP routing
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpapi.HealthHandler())
	coastal := sensorstatus.New(sensorstatus.FromJetStream(reportPublisher.JetStream()), time.Second, 30*time.Second)
	mux.HandleFunc("GET /v1/events", httpapi.EventsHandler(aggClient, httpapi.WithSensorStatus(coastal.Meta)))
	mux.HandleFunc("POST /field-reports", fieldReports)
	mux.HandleFunc("POST /v1/field-reports", fieldReports)

	// Prioritised load shedding: under overload media is refused first and the
	// field team keeps its reserved share of capacity.
	shedder := loadshed.New(loadshed.Config{
		MaxInflight: cfg.MaxInflight,
		MediaShare:  cfg.MediaInflightShare,
		SkipPaths:   []string{"/health"},
	})

	// Chain middlewares: CorrelationID -> Logging -> Auth -> Load shedding -> Rate limit -> Handler
	handler := httpapi.CorrelationIDMiddleware(
		httpapi.LoggingMiddleware(logger,
			authMiddleware(
				shedder.Middleware(
					limiter.Middleware(mux),
				),
			),
		),
	)

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           observability.Middleware("client-api", handler),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Metrics go on their own port so they are not exposed on the public edge.
	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", observability.MetricsHandler())
	metricsServer := &http.Server{Addr: metricsAddr, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("client-api: metrics server error")
		}
	}()

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
	_ = metricsServer.Shutdown(shutdownCtx)
	logger.Info().Msg("client-api: stopped")
}

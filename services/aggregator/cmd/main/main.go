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
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/auditapi"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/dlq"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/ingest"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/lease"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/metrics"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/outboxrelay"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/poller"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/queryapi"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
	"github.com/wiwekaputera/if4031-tubes-imade/observability/pgxtrace"
)

func main() {
	// 1. Initialize structured logging
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "aggregator").
		Logger().
		Hook(observability.LogHook{})

	shutdownTracing, err := observability.Init(context.Background(), "aggregator")
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to set up tracing")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

	// 2. Load runtime configuration with fail-fast validation
	cfg, err := config.Load()
	if err != nil {
		logger.Fatal().Err(err).Msg("aggregator: failed to load configuration")
	}

	logLevel, err := zerolog.ParseLevel(strings.ToLower(cfg.LogLevel))
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(logLevel)

	logger.Info().
		Str("port", cfg.Port).
		Str("instance_id", cfg.InstanceID).
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
	poolCfg, err := pgxpool.ParseConfig(cfg.DBDSN)
	if err != nil {
		logger.Fatal().Err(err).Msg("invalid database DSN")
	}
	poolCfg.ConnConfig.Tracer = pgxtrace.Tracer{Service: "aggregator"}
	dbPool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to database")
	}
	defer dbPool.Close()

	ctx := context.Background()
	if err := dbPool.Ping(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to ping database")
	}
	logger.Info().Msg("Connected to canonical store")

	repo := store.NewPostgresRepo(dbPool, logger).WithShardQueryTimeout(cfg.ShardQueryTimeout)

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

	if err := ingest.EnsureIngestStream(js); err != nil {
		logger.Fatal().Err(err).Msg("failed to ensure JetStream ingest stream")
	}
	logger.Info().Str("stream", event.IngestStreamName).Msg("JetStream ingest stream ready")

	ingestConsumer := ingest.New(js, repo, logger)
	if err := ingestConsumer.Subscribe(); err != nil {
		logger.Fatal().Err(err).Msg("failed to subscribe to JetStream ingest consumer")
	}

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

	// 6. Initialize leader elector, normalizer, poller, and query API handlers
	metrics.Leader.Set(0)
	elector := lease.New(repo, store.DefaultLeaseName, cfg.InstanceID, cfg.LeaseTTL, cfg.LeaseRenewInterval, logger,
		lease.Hooks{
			OnPromote: func(int64) { metrics.Leader.Set(1) },
			OnDemote:  func() { metrics.Leader.Set(0) },
		})
	isLeader := func() bool {
		_, ok := elector.IsLeader()
		return ok
	}
	relay.WithLeaderCheck(isLeader)
	norm := normalizer.New(logger, volcanoRefs)
	p := poller.New(cfg, norm, repo, logger).WithElector(elector)

	healthFn := func() (string, string, time.Time, time.Time, *time.Time) {
		hs := p.Health()
		var pvmbgStaleSince *time.Time
		if hs.PVMBG.StaleSince != nil {
			t := *hs.PVMBG.StaleSince
			pvmbgStaleSince = &t
		}
		return hs.BMKG.Status, hs.PVMBG.Status, hs.BMKG.LastSynced, hs.PVMBG.LastSynced, pvmbgStaleSince
	}

	h := queryapi.New(repo, logger, healthFn).WithInstanceID(cfg.InstanceID)
	h.SetReady(true)
	auditH := auditapi.New(repo, cfg.AuditToken, logger)

	retryHandler := p.PVMBGRetryHandler()
	dlqHandler, err := dlq.NewHTTPHandler(repo, retryHandler, cfg.InternalOpsToken)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to build DLQ routes")
	}

	// 7. Start leader election, background polling loop, DLQ retry worker, transactional outbox relay, and ingest consumer.
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go metrics.Run(ctx, metrics.Snapshot{
		Sources: func() []metrics.Source {
			hs := p.Health()
			return []metrics.Source{
				{Name: "BMKG", Status: hs.BMKG.Status, LastSynced: hs.BMKG.LastSynced},
				{Name: "PVMBG", Status: hs.PVMBG.Status, LastSynced: hs.PVMBG.LastSynced},
			}
		},
		DeadLetters:   repo.CountDeadLetters,
		OutboxBacklog: repo.OutboxBacklog,
		IsLeader:      isLeader,
	}, 5*time.Second, logger)

	wg.Add(5)
	go func() {
		defer wg.Done()
		elector.Run(ctx)
	}()
	go poller.StartPolling(ctx, &wg, p, cfg, logger)
	go poller.RunDLQWorkerWithLeader(ctx, &wg, repo, retryHandler, 5*time.Second, isLeader, logger)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		ingestConsumer.Run(ctx)
	}()

	// 8. Initialize HTTP server for internal query API
	addr := cfg.Port
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", observability.MetricsHandler())
	mux.Handle("/internal/dlq", dlqHandler)
	mux.Handle("/internal/dlq/", dlqHandler)
	mux.Handle("/", queryapi.Routes(h, auditH))
	httpServer := queryapi.Server(observability.Middleware("aggregator", mux), addr)

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

	// Mark not-ready immediately so kube-proxy stops routing new requests,
	// stop background workers, release leader lease, wait drain period, then shut down HTTP.
	h.SetReady(false)
	cancel()
	wg.Wait()

	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer releaseCancel()
	if err := elector.Release(releaseCtx); err != nil {
		logger.Warn().Err(err).Msg("failed to release leader lease on shutdown")
	}

	if cfg.ShutdownDrain > 0 {
		logger.Info().Dur("drain", cfg.ShutdownDrain).Msg("Draining connections before HTTP shutdown")
		time.Sleep(cfg.ShutdownDrain)
	}

	// Shutdown HTTP server with 5s deadline
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("HTTP server forced to shutdown")
	} else {
		logger.Info().Msg("HTTP server shut down gracefully")
	}
}

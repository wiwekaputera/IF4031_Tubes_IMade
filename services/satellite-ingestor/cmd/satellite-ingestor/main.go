// Command satellite-ingestor reads the satellite fire feed, turns detections
// into fires, and publishes each fire change to the aggregator through
// JetStream. Configuration comes from the environment:
//
//	DATABASE_URL          Postgres connection string for the satellite schema
//	SATELLITE_FEED_TOKEN  group Bearer token; keep it in a Secret, never in the repo
//	SATELLITE_FEED_URL    optional; defaults to the course feed
//	NATS_URL              optional; defaults to nats://nats-jetstream:4222
//	METRICS_ADDR          optional; defaults to :9090, serves /metrics and /healthz
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
	"github.com/wiwekaputera/if4031-tubes-imade/observability/pgxtrace"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/metrics"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/relay"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/migrations"
	"go.opentelemetry.io/otel/attribute"
)

const (
	defaultFeedURL = "https://satellite.naufarrel.dev"
	defaultNATSURL = "nats://nats-jetstream:4222"
	monitorEvery   = 5 * time.Second
)

func main() {
	// Same field names as the zerolog services: "message", "service".
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.MessageKey {
				a.Key = "message"
			}
			if a.Key == slog.LevelKey {
				a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
			}
			return a
		},
	})).With("service", "satellite-ingestor")
	if err := run(log); err != nil {
		log.Error("satellite-ingestor stopped", "err", err)
		os.Exit(1)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func run(log *slog.Logger) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is not set")
	}
	token := os.Getenv("SATELLITE_FEED_TOKEN")
	if token == "" {
		return errors.New("SATELLITE_FEED_TOKEN is not set")
	}
	feedURL := getenv("SATELLITE_FEED_URL", defaultFeedURL)
	natsURL := getenv("NATS_URL", defaultNATSURL)
	metricsAddr := getenv("METRICS_ADDR", ":9090")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.Init(ctx, "satellite-ingestor")
	if err != nil {
		return fmt.Errorf("set up tracing: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}()

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	poolCfg.ConnConfig.Tracer = pgxtrace.Tracer{Service: "satellite-ingestor"}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, migrations.Init); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	nc, err := nats.Connect(natsURL, nats.Name("satellite-ingestor"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Drain()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	if err := relay.EnsureIngestStream(js); err != nil {
		return fmt.Errorf("ensure ingest stream: %w", err)
	}

	s := store.NewSatellite(pool)
	proc, err := ingest.New(ctx, s)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	log.Info("starting feed", "url", feedURL, "nats", natsURL, "resume_offset", proc.State().NextOffset)

	client := feed.NewClient(feedURL, token)
	client.Log = log
	client.HTTP = &http.Client{Transport: observability.Transport("satellite-ingestor", nil)}
	metrics.SetState(proc.State())

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		if !nc.IsConnected() {
			http.Error(w, "nats unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: metricsAddr, Handler: observability.Middleware("satellite-ingestor", mux), ReadHeaderTimeout: 5 * time.Second}

	// The relay runs beside the feed. The feed only writes to Postgres, so a
	// broker outage delays publishing but never stops ingestion.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		(&relay.Relay{Store: s, Publisher: relay.NewJetStream(js), Log: log, OnSuperseded: metrics.AddSuperseded}).Run(ctx)
	}()
	go func() {
		defer wg.Done()
		monitor(ctx, log, client, proc, s)
	}()
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "err", err)
		}
	}()

	handle := func(ctx context.Context, m feed.Message) error {
		// One span per feed message, so its database queries and any publish
		// nest under it. Compose samples a fraction of these traces.
		ctx, span := observability.Tracer().Start(ctx, "satellite "+m.Type)
		span.SetAttributes(attribute.Int64("feed.offset", m.Offset))
		defer span.End()
		before := proc.State()
		if err := proc.Handle(ctx, m); err != nil {
			span.RecordError(err)
			return err
		}
		metrics.Observe(m, before, proc.State())
		return nil
	}
	err = client.Run(ctx, proc.NextOffset, handle)
	stop() // stop the other goroutines too if the feed ended on its own
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = srv.Shutdown(shutdownCtx)
	cancel()
	wg.Wait()

	st := proc.State()
	log.Info("shutdown", "next_offset", st.NextOffset, "invalid", st.Invalid,
		"low_conf", st.LowConf, "late", st.Late, "unknown", st.Unknown)
	if errors.Is(err, context.Canceled) {
		return nil // interrupted on purpose
	}
	return err
}

// monitor refreshes the lag and backlog gauges. The feed session tells how far
// ahead the feed is; the database tells what is still waiting to be published.
func monitor(ctx context.Context, log *slog.Logger, client *feed.Client, proc *ingest.Processor, s *store.Satellite) {
	t := time.NewTicker(monitorEvery)
	defer t.Stop()
	for {
		st := proc.State()
		if sess, err := client.Session(ctx); err == nil {
			metrics.SetLag(sess.LatestOffset, st.NextOffset, sess.VirtualNow, st.Watermark)
		} else if ctx.Err() == nil {
			log.Warn("feed session check failed", "err", err)
		}
		outbox, err1 := s.OutboxBacklog(ctx)
		dead, err2 := s.DeadLetterCount(ctx)
		if err1 == nil && err2 == nil {
			metrics.SetBacklog(outbox, dead)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

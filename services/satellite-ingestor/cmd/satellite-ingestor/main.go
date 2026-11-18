// Command satellite-ingestor reads the satellite fire feed, turns detections
// into fires, and publishes each fire change to the aggregator through
// JetStream. Configuration comes from the environment:
//
//	DATABASE_URL          Postgres connection string for the satellite schema
//	SATELLITE_FEED_TOKEN  group Bearer token; keep it in a Secret, never in the repo
//	SATELLITE_FEED_URL    optional; defaults to the course feed
//	NATS_URL              optional; defaults to nats://nats-jetstream:4222
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/relay"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/migrations"
)

const (
	defaultFeedURL = "https://satellite.naufarrel.dev"
	defaultNATSURL = "nats://nats-jetstream:4222"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
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

	// The relay runs beside the feed. The feed only writes to Postgres, so a
	// broker outage delays publishing but never stops ingestion.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		(&relay.Relay{Store: s, Publisher: relay.NewJetStream(js), Log: log}).Run(ctx)
	}()

	client := feed.NewClient(feedURL, token)
	client.Log = log
	err = client.Run(ctx, proc.NextOffset, proc.Handle)
	stop() // stop the relay too if the feed ended on its own
	wg.Wait()

	st := proc.State()
	log.Info("shutdown", "next_offset", st.NextOffset, "invalid", st.Invalid,
		"low_conf", st.LowConf, "late", st.Late, "unknown", st.Unknown)
	if errors.Is(err, context.Canceled) {
		return nil // interrupted on purpose
	}
	return err
}

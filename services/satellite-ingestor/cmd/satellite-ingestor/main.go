// Command satellite-ingestor reads the satellite fire feed (M2 P4) and writes
// fire events to Postgres. Configuration comes from the environment:
//
//	DATABASE_URL          Postgres connection string for the satellite schema
//	SATELLITE_FEED_TOKEN  group Bearer token; keep it in a Secret, never in the repo
//	SATELLITE_FEED_URL    optional; defaults to the course feed
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/feed"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/ingest"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/migrations"
)

const defaultFeedURL = "https://satellite.naufarrel.dev"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("satellite-ingestor stopped", "err", err)
		os.Exit(1)
	}
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
	feedURL := os.Getenv("SATELLITE_FEED_URL")
	if feedURL == "" {
		feedURL = defaultFeedURL
	}

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

	s := store.NewSatellite(pool)
	proc, err := ingest.New(ctx, s)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}
	log.Info("starting feed", "url", feedURL, "resume_offset", proc.State().NextOffset)

	client := feed.NewClient(feedURL, token)
	client.Log = log
	err = client.Run(ctx, proc.NextOffset, proc.Handle)
	if ctx.Err() != nil {
		st := proc.State()
		log.Info("shutdown", "next_offset", st.NextOffset, "invalid", st.Invalid, "low_conf", st.LowConf, "late", st.Late)
		return nil // interrupted on purpose
	}
	return err
}

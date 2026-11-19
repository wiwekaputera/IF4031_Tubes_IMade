package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
)

const maxLoggedInvalidRows = 20

type loaderConfig struct {
	File        string
	Format      string
	Cols        ColumnMap
	Concurrency int
	BatchSize   int
	WithOutbox  bool
	DryRun      bool
	DSN         string
}

type loaderSummary struct {
	Read     int64
	Inserted int64
	Skipped  int64
	Invalid  int64
	Elapsed  time.Duration
}

func runLoader(ctx context.Context, cfg loaderConfig, inserter BatchInserter, logger zerolog.Logger) (loaderSummary, error) {
	if cfg.File == "" {
		return loaderSummary{}, fmt.Errorf("--file is required")
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 500
	}

	format, err := detectFormat(cfg.File, cfg.Format)
	if err != nil {
		return loaderSummary{}, err
	}

	logger.Info().
		Str("file", cfg.File).
		Str("format", format).
		Int("concurrency", cfg.Concurrency).
		Int("batch_size", cfg.BatchSize).
		Bool("with_outbox", cfg.WithOutbox).
		Bool("dry_run", cfg.DryRun).
		Msg("starting historical catalog loader")

	start := time.Now()
	reqCh := make(chan store.UpsertRequest, cfg.BatchSize*cfg.Concurrency)
	batchCh := make(chan []store.UpsertRequest, cfg.Concurrency*2)

	var (
		insertedCount atomic.Int64
		skippedCount  atomic.Int64
		loggedInvalid atomic.Int64
	)

	// Start worker pool to write batches (or count in dry-run).
	var workersWG sync.WaitGroup
	var firstWorkerErr error
	var errOnce sync.Once

	for i := 0; i < cfg.Concurrency; i++ {
		workersWG.Add(1)
		go func(workerID int) {
			defer workersWG.Done()
			for batch := range batchCh {
				if cfg.DryRun {
					insertedCount.Add(int64(len(batch)))
					continue
				}
				ins, skip, err := inserter.InsertBatch(ctx, batch, cfg.WithOutbox)
				if err != nil {
					errOnce.Do(func() { firstWorkerErr = fmt.Errorf("worker %d: %w", workerID, err) })
					return
				}
				insertedCount.Add(ins)
				skippedCount.Add(skip)
			}
		}(i)
	}

	// Batcher goroutine groups incoming UpsertRequests into slices of cfg.BatchSize.
	var batcherWG sync.WaitGroup
	batcherWG.Add(1)
	go func() {
		defer batcherWG.Done()
		defer close(batchCh)

		batch := make([]store.UpsertRequest, 0, cfg.BatchSize)
		for req := range reqCh {
			batch = append(batch, req)
			if len(batch) >= cfg.BatchSize {
				batchCh <- batch
				batch = make([]store.UpsertRequest, 0, cfg.BatchSize)
			}
		}
		if len(batch) > 0 {
			batchCh <- batch
		}
	}()

	// Progress logger.
	progressDone := make(chan struct{})
	var readAtomic atomic.Int64
	var invalidAtomic atomic.Int64
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				elapsed := time.Since(start).Seconds()
				ins := insertedCount.Load()
				skip := skippedCount.Load()
				rate := 0.0
				if elapsed > 0 {
					rate = float64(ins+skip) / elapsed
				}
				logger.Info().
					Int64("read", readAtomic.Load()).
					Int64("inserted", ins).
					Int64("skipped", skip).
					Int64("invalid", invalidAtomic.Load()).
					Float64("rows_per_sec", rate).
					Msg("catalog load progress")
			}
		}
	}()

	onError := func(re RowError) {
		invalidAtomic.Add(1)
		n := loggedInvalid.Add(1)
		if n <= maxLoggedInvalidRows {
			logger.Warn().
				Int64("row", re.Row).
				Str("reason", re.Reason).
				Msg("skipping invalid catalog row")
		}
	}

	// Stream file on the caller goroutine, updating readAtomic via wrapper?
	// streamCatalog returns final ParseStats; we also track invalidAtomic in onError.
	stats, parseErr := streamCatalog(cfg.File, format, cfg.Cols, reqCh, onError)
	readAtomic.Store(stats.Read)
	invalidAtomic.Store(stats.Invalid)
	close(reqCh)

	batcherWG.Wait()
	workersWG.Wait()
	close(progressDone)

	if parseErr != nil {
		return loaderSummary{}, parseErr
	}
	if firstWorkerErr != nil {
		return loaderSummary{}, firstWorkerErr
	}

	elapsed := time.Since(start)
	summary := loaderSummary{
		Read:     stats.Read,
		Inserted: insertedCount.Load(),
		Skipped:  skippedCount.Load(),
		Invalid:  stats.Invalid,
		Elapsed:  elapsed,
	}

	rate := 0.0
	if elapsed.Seconds() > 0 {
		rate = float64(summary.Inserted+summary.Skipped) / elapsed.Seconds()
	}
	logger.Info().
		Int64("read", summary.Read).
		Int64("inserted", summary.Inserted).
		Int64("skipped", summary.Skipped).
		Int64("invalid", summary.Invalid).
		Dur("elapsed", summary.Elapsed).
		Float64("rows_per_sec", rate).
		Bool("dry_run", cfg.DryRun).
		Msg("historical catalog load complete")

	return summary, nil
}

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "historical-loader").
		Logger()

	var cfg loaderConfig
	flag.StringVar(&cfg.File, "file", "", "Path to catalog file (CSV, JSON, or NDJSON)")
	flag.StringVar(&cfg.Format, "format", "", "Input format: csv, json, or ndjson (auto-detected by extension if omitted)")
	flag.StringVar(&cfg.Cols.ID, "col-id", "", "Column name for event ID")
	flag.StringVar(&cfg.Cols.Time, "col-time", "", "Column name for event timestamp")
	flag.StringVar(&cfg.Cols.Lat, "col-lat", "", "Column name for latitude")
	flag.StringVar(&cfg.Cols.Lon, "col-lon", "", "Column name for longitude")
	flag.StringVar(&cfg.Cols.Depth, "col-depth", "", "Column name for depth (km)")
	flag.StringVar(&cfg.Cols.Mag, "col-mag", "", "Column name for magnitude")
	flag.StringVar(&cfg.Cols.Region, "col-region", "", "Column name for region / area name")
	flag.StringVar(&cfg.Cols.TimeLayout, "time-layout", "", "Go time.Parse layout for timestamps (auto-detected if omitted)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 4, "Number of concurrent batch writer workers")
	flag.IntVar(&cfg.BatchSize, "batch-size", 500, "Number of rows per batch insert")
	flag.BoolVar(&cfg.WithOutbox, "with-outbox", false, "Write outbox rows for newly inserted historical events")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Parse and validate without writing to database")
	flag.StringVar(&cfg.DSN, "dsn", os.Getenv("DB_DSN"), "CockroachDB / PostgreSQL DSN (defaults to DB_DSN env var)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var inserter BatchInserter
	if !cfg.DryRun {
		if cfg.DSN == "" {
			logger.Fatal().Msg("DB_DSN environment variable or --dsn flag is required when not using --dry-run")
		}
		poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to parse database DSN")
		}
		if int(poolCfg.MaxConns) < cfg.Concurrency+2 {
			poolCfg.MaxConns = int32(cfg.Concurrency + 2)
		}
		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to connect to database")
		}
		defer pool.Close()

		if err := pool.Ping(ctx); err != nil {
			logger.Fatal().Err(err).Msg("failed to ping database")
		}
		inserter = &pgBatchInserter{pool: pool}
	}

	if _, err := runLoader(ctx, cfg, inserter, logger); err != nil {
		logger.Fatal().Err(err).Msg("historical catalog load failed")
	}
}

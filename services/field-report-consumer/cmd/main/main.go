// field-report-consumer delivers field reports stored in JetStream to the
// BNPB operations receiver (stub-sink), at least once from the broker and
// exactly once in effect thanks to a local dedup store:
//
//   - a report is acknowledged to JetStream only after the receiver accepted
//     it and the dedup store recorded it, so a crash at any point causes a
//     redelivery, never a loss;
//   - a redelivered report already in the dedup store is acknowledged without
//     being sent again; the receiver also deduplicates by the same message ID,
//     covering a crash between its answer and the local write.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	bolt "go.etcd.io/bbolt"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/fieldreport"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
)

const (
	consumerDurable = "field-report-consumer"
	defaultNATSURL  = "nats://nats-jetstream:4222"
	defaultSinkURL  = "http://stub-sink:8085"
	defaultDBPath   = "/data/field-reports.db"
	defaultPort     = "8088"
	ackWait         = 30 * time.Second
	fetchBatch      = 16
	fetchWait       = time.Second
	sinkTimeout     = 5 * time.Second
	maxRetryDelay   = 30 * time.Second
)

var processedBucket = []byte("processed_reports")

// dedupStore remembers which message IDs were already delivered.
type dedupStore interface {
	IsProcessed(id string) (bool, error)
	MarkProcessed(id string) error
}

type boltStore struct{ db *bolt.DB }

func openStore(path string) (*boltStore, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open dedup store: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(processedBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize dedup store: %w", err)
	}
	return &boltStore{db: db}, nil
}

func (s *boltStore) IsProcessed(id string) (bool, error) {
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket(processedBucket).Get([]byte(id)) != nil
		return nil
	})
	return found, err
}

func (s *boltStore) MarkProcessed(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(processedBucket).Put([]byte(id), []byte(time.Now().UTC().Format(time.RFC3339Nano)))
	})
}

// message is the part of *nats.Msg the consumer uses, so tests can fake it.
type message interface {
	Ack(opts ...nats.AckOpt) error
	NakWithDelay(delay time.Duration, opts ...nats.AckOpt) error
	Term(opts ...nats.AckOpt) error
}

type consumer struct {
	store      dedupStore
	sinkURL    string
	httpClient *http.Client
	logger     zerolog.Logger
}

// handle processes one report and returns the outcome used for metrics.
// deliveries is the JetStream delivery count, used to back off retries.
func (c *consumer) handle(ctx context.Context, msg message, data []byte, deliveries uint64) string {
	var report fieldreport.Report
	if err := json.Unmarshal(data, &report); err != nil || report.ReportID == "" || report.ClientID == "" {
		// Only client-api publishes here, after validation; a message that does
		// not decode can never succeed, so stop redelivering it.
		c.logger.Error().Err(err).Msg("undecodable field report; terminating message")
		_ = msg.Term()
		return "invalid"
	}
	id := report.MessageID()
	log := c.logger.With().Str("message_id", id).Str("correlation_id", report.CorrelationID).
		Uint64("delivery", deliveries).Logger()

	done, err := c.store.IsProcessed(id)
	if err != nil {
		c.retry(msg, log, deliveries, err)
		return "retry"
	}
	if done {
		if err := msg.Ack(); err != nil {
			log.Error().Err(err).Msg("failed acknowledging already delivered report")
		}
		log.Info().Msg("field report already delivered; acknowledged without resending")
		return "duplicate"
	}

	if err := c.deliver(ctx, id, data); err != nil {
		c.retry(msg, log, deliveries, err)
		return "retry"
	}
	if err := c.store.MarkProcessed(id); err != nil {
		c.retry(msg, log, deliveries, err)
		return "retry"
	}
	if err := msg.Ack(); err != nil {
		// The report is delivered and recorded; a redelivery will be
		// recognised as a duplicate.
		log.Error().Err(err).Msg("failed acknowledging delivered report")
		return "ack_failed"
	}
	log.Info().Ctx(ctx).Str("severity", report.Severity).
		Dur("since_accepted", time.Since(report.AcceptedAt)).Msg("field report delivered")
	return "ok"
}

func (c *consumer) deliver(parent context.Context, id string, data []byte) error {
	ctx, cancel := context.WithTimeout(parent, sinkTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.sinkURL+"/field-reports", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", id)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("receiver request failed: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("receiver returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// retry asks JetStream to redeliver later. There is no delivery limit: a
// report accepted with 202 must eventually arrive, so it waits out any outage.
func (c *consumer) retry(msg message, log zerolog.Logger, deliveries uint64, cause error) {
	delay := retryDelay(deliveries)
	log.Warn().Err(cause).Dur("retry_in", delay).Msg("field report not delivered; scheduling redelivery")
	if err := msg.NakWithDelay(delay); err != nil {
		log.Error().Err(err).Msg("failed requesting redelivery")
	}
}

// retryDelay doubles from 1s per delivery, capped at maxRetryDelay.
func retryDelay(deliveries uint64) time.Duration {
	delay := time.Second
	for i := uint64(1); i < deliveries && delay < maxRetryDelay; i++ {
		delay *= 2
	}
	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}
	return delay
}

func streamConfig() *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:       fieldreport.StreamName,
		Subjects:   []string{fieldreport.SubjectFilter},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     fieldreport.StreamMaxAge,
		Duplicates: fieldreport.DuplicateWindow,
		Replicas:   1,
	}
}

func subscribe(js nats.JetStreamContext) (*nats.Subscription, error) {
	if _, err := js.StreamInfo(fieldreport.StreamName); errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := js.AddStream(streamConfig()); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			return nil, fmt.Errorf("create stream: %w", err)
		}
	} else if err != nil {
		return nil, err
	}
	return js.PullSubscribe(
		fieldreport.SubjectFilter,
		consumerDurable,
		nats.BindStream(fieldreport.StreamName),
		nats.AckExplicit(),
		nats.AckWait(ackWait),
		nats.MaxDeliver(-1),
		nats.MaxAckPending(fetchBatch*4),
		nats.DeliverAll(),
	)
}

func run(ctx context.Context, sub *nats.Subscription, c *consumer, logger zerolog.Logger) {
	for ctx.Err() == nil {
		msgs, err := sub.Fetch(fetchBatch, nats.MaxWait(fetchWait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			logger.Warn().Err(err).Msg("JetStream fetch failed; retrying")
			time.Sleep(fetchWait)
			continue
		}
		for _, msg := range msgs {
			var deliveries uint64 = 1
			if meta, err := msg.Metadata(); err == nil {
				deliveries = meta.NumDelivered
			}
			opCtx, op := observability.StartConsume(context.Background(), "field-report-consumer", msg.Subject, msg.Header)
			op.End(c.handle(opCtx, msg, msg.Data, deliveries), nil)
		}
	}
}

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("service", "field-report-consumer").Logger().Hook(observability.LogHook{})

	shutdownTracing, err := observability.Init(context.Background(), "field-report-consumer")
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to set up tracing")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

	natsURL := envOr("NATS_URL", defaultNATSURL)
	sinkURL := strings.TrimRight(envOr("STUB_SINK_URL", defaultSinkURL), "/")
	dbPath := envOr("BBOLT_PATH", defaultDBPath)

	conn, err := nats.Connect(natsURL, nats.Name("BNPB Field Report Consumer"),
		nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		logger.Fatal().Err(err).Msg("failed connecting to NATS")
	}
	defer conn.Close()
	js, err := conn.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed creating JetStream context")
	}

	store, err := openStore(dbPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed opening dedup store")
	}
	defer store.db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var sub *nats.Subscription
	for ctx.Err() == nil {
		if sub, err = subscribe(js); err == nil {
			break
		}
		logger.Warn().Err(err).Msg("JetStream not ready; retrying subscription in 2s")
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
	if sub == nil {
		return
	}
	logger.Info().Str("stream", fieldreport.StreamName).Str("durable", consumerDurable).Msg("consuming field reports")

	c := &consumer{
		store:      store,
		sinkURL:    sinkURL,
		httpClient: &http.Client{Timeout: sinkTimeout, Transport: observability.Transport("field-report-consumer", nil)},
		logger:     logger,
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", observability.MetricsHandler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !conn.IsConnected() {
			http.Error(w, `{"status":"nats_disconnected"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
	addr := envOr("PORT", defaultPort)
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error().Err(err).Msg("health server failed")
		}
	}()

	run(ctx, sub, c, logger)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	logger.Info().Msg("stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

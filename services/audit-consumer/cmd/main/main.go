package main

import (
	"context"
	"encoding/binary"
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
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	bolt "go.etcd.io/bbolt"
)

const (
	consumerDurable = "audit-consumer"
	defaultNATSURL  = "nats://nats-jetstream:4222"
	defaultDBPath   = "/data/audit.db"
	healthPort      = "8087"
	ackWait         = 30 * time.Second
	fetchWait       = time.Second
)

var (
	versionsBucket = []byte("last_versions")
	historyBucket  = []byte("event_history")
)

type auditStore struct {
	db *bolt.DB
}

func openAuditStore(path string) (*auditStore, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create audit directory: %w", err)
		}
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open audit database: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(versionsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(historyBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize audit database: %w", err)
	}
	return &auditStore{db: db}, nil
}

// appendOnce atomically records both the event history row and the latest
// processed version, so a redelivery cannot create a second history entry.
func (s *auditStore) appendOnce(envelope event.Envelope) (bool, error) {
	var inserted bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		versions := tx.Bucket(versionsBucket)
		current := versions.Get([]byte(envelope.EventID))
		if len(current) == 8 && envelope.Version <= int64(binary.BigEndian.Uint64(current)) {
			return nil
		}

		payload, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s:%d", envelope.EventID, envelope.Version)
		if err := tx.Bucket(historyBucket).Put([]byte(key), payload); err != nil {
			return err
		}
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(envelope.Version))
		if err := versions.Put([]byte(envelope.EventID), value); err != nil {
			return err
		}
		inserted = true
		return nil
	})
	return inserted, err
}

type auditConsumer struct {
	store  *auditStore
	logger zerolog.Logger
}

func (c *auditConsumer) process(msg *nats.Msg) {
	var envelope event.Envelope
	if err := json.Unmarshal(msg.Data, &envelope); err != nil || envelope.EventID == "" || envelope.Version < 1 {
		c.logger.Error().Err(err).Msg("invalid event envelope; terminating message")
		if termErr := msg.Term(); termErr != nil {
			c.logger.Error().Err(termErr).Msg("failed terminating invalid message")
		}
		return
	}
	inserted, err := c.store.appendOnce(envelope)
	if err != nil {
		c.logger.Error().Err(err).Str("event_id", envelope.EventID).Str("correlation_id", envelope.TraceID).Msg("failed appending audit event")
		if nakErr := msg.NakWithDelay(time.Second); nakErr != nil {
			c.logger.Error().Err(nakErr).Msg("failed requesting redelivery")
		}
		return
	}
	if err := msg.Ack(); err != nil {
		c.logger.Error().Err(err).Str("event_id", envelope.EventID).Msg("failed acknowledging audit event")
		return
	}
	if inserted {
		c.logger.Info().
			Str("event_key", fmt.Sprintf("%s:%d", envelope.EventID, envelope.Version)).
			Str("correlation_id", envelope.TraceID).
			Msg("event appended to audit history")
	}
}

func healthHandler(conn *nats.Conn) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !conn.IsConnected() {
			http.Error(w, `{"status":"nats_disconnected"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
}

func runConsumer(ctx context.Context, sub *nats.Subscription, consumer *auditConsumer, logger zerolog.Logger) {
	for ctx.Err() == nil {
		messages, err := sub.Fetch(1, nats.MaxWait(fetchWait))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			logger.Warn().Err(err).Msg("JetStream fetch failed; retrying")
			time.Sleep(fetchWait)
			continue
		}
		for _, msg := range messages {
			consumer.process(msg)
		}
	}
}

func main() {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("service", "audit-consumer").Logger()
	natsURL := envOr("NATS_URL", defaultNATSURL)
	dbPath := envOr("BBOLT_PATH", defaultDBPath)

	conn, err := nats.Connect(natsURL, nats.Name("BNPB Audit Consumer"), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		logger.Fatal().Err(err).Msg("failed connecting to NATS")
	}
	defer conn.Close()
	js, err := conn.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed creating JetStream context")
	}
	var sub *nats.Subscription
	for retries := 0; retries < 30; retries++ {
		sub, err = js.PullSubscribe(
			event.SubjectFilter,
			consumerDurable,
			nats.BindStream(event.StreamName),
			nats.AckExplicit(),
			nats.AckWait(ackWait),
			nats.MaxDeliver(5),
			nats.MaxAckPending(1),
		)
		if err == nil {
			break
		}
		logger.Warn().Err(err).Msg("stream not ready yet, retrying subscription in 1s...")
		time.Sleep(time.Second)
	}
	if err != nil {
		logger.Fatal().Err(err).Msg("failed creating durable JetStream subscription after retries")
	}
	state, err := openAuditStore(dbPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed opening audit store")
	}
	defer state.db.Close()
	consumer := &auditConsumer{store: state, logger: logger}

	port := envOr("PORT", healthPort)
	addr := port
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	server := &http.Server{Addr: addr, Handler: healthHandler(conn), ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		runConsumer(ctx, sub, consumer, logger)
	}()

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("health server failed")
		}
	case <-consumerDone:
		logger.Error().Msg("consumer loop exited unexpectedly")
	}
	stop()
	<-consumerDone
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("health server shutdown failed")
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

package main

import (
	"bytes"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
	bolt "go.etcd.io/bbolt"
)

const (
	consumerDurable = "notification-consumer"
	// alertDurable reads the priority alert stream. It is a separate durable
	// consumer with its own loop, so a backlog of regular events never delays
	// a critical alert.
	alertDurable   = "notification-consumer-alerts"
	defaultNATSURL = "nats://nats-jetstream:4222"
	defaultSinkURL = "http://stub-sink:8085"
	defaultDBPath  = "/data/notification.db"
	healthPort     = "8086"
	ackWait        = 30 * time.Second
	fetchWait      = time.Second
	sinkTimeout    = 5 * time.Second
)

var processedBucket = []byte("processed_versions")

// alertLatency measures, for events carrying the detected_at attribute, the
// time from BNPB first reading the observation to the receiver accepting it.
var alertLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "notification_delivery_latency_seconds",
	Help:    "Time from detection (detected_at attribute) to the field team receiver accepting the notification, by path and severity.",
	Buckets: []float64{.1, .25, .5, 1, 2, 3, 4, 5, 7.5, 10, 30},
}, []string{"path", "severity"})

type stateStore struct {
	db *bolt.DB
}

func openState(path string) (*stateStore, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open notification state: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(processedBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize notification state: %w", err)
	}
	return &stateStore{db: db}, nil
}

func (s *stateStore) isProcessed(eventID string, version int64) (bool, error) {
	processed := false
	err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(processedBucket).Get([]byte(eventID))
		processed = len(value) == 8 && version <= int64(binary.BigEndian.Uint64(value))
		return nil
	})
	return processed, err
}

func (s *stateStore) markProcessed(eventID string, version int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(processedBucket)
		current := bucket.Get([]byte(eventID))
		if len(current) == 8 && version <= int64(binary.BigEndian.Uint64(current)) {
			return nil
		}
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(version))
		return bucket.Put([]byte(eventID), value)
	})
}

type notificationConsumer struct {
	// path labels metrics: "regular" (BNPB_EVENTS) or "priority" (BNPB_ALERTS).
	path       string
	state      *stateStore
	sinkURL    string
	httpClient *http.Client
	logger     zerolog.Logger
}

// process handles one message inside a consumer span that continues the
// producer's trace, and records the outcome.
func (c *notificationConsumer) process(msg *nats.Msg) {
	ctx, op := observability.StartConsume(context.Background(), "notification-consumer", msg.Subject, msg.Header)
	op.End(c.handle(ctx, msg), nil)
}

// handle delivers one event to the sink and returns the outcome for metrics.
func (c *notificationConsumer) handle(parent context.Context, msg *nats.Msg) string {
	var envelope event.Envelope
	if err := json.Unmarshal(msg.Data, &envelope); err != nil || envelope.EventID == "" || envelope.Version < 1 {
		c.logger.Error().Err(err).Msg("invalid event envelope; terminating message")
		if termErr := msg.Term(); termErr != nil {
			c.logger.Error().Err(termErr).Msg("failed terminating invalid message")
		}
		return "invalid"
	}

	duplicate, err := c.state.isProcessed(envelope.EventID, envelope.Version)
	if err != nil {
		c.retry(msg, envelope, err)
		return "retry"
	}
	if duplicate {
		if err := msg.Ack(); err != nil {
			c.logger.Error().Err(err).Str("event_id", envelope.EventID).Msg("failed acknowledging duplicate event")
		}
		return "duplicate"
	}

	key := fmt.Sprintf("%s:%d", envelope.EventID, envelope.Version)
	ctx, cancel := context.WithTimeout(parent, sinkTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.sinkURL+"/notify", bytes.NewReader(msg.Data))
	if err != nil {
		c.retry(msg, envelope, err)
		return "retry"
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	if envelope.TraceID != "" {
		request.Header.Set("X-Correlation-ID", envelope.TraceID)
	}

	started := time.Now()
	response, err := c.httpClient.Do(request)
	latency := time.Since(started)
	if err != nil {
		c.logger.Error().Err(err).Str("event_key", key).Str("correlation_id", envelope.TraceID).Dur("latency", latency).Msg("notification sink request failed")
		c.retry(msg, envelope, err)
		return "retry"
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		err := fmt.Errorf("notification sink returned HTTP %d", response.StatusCode)
		c.logger.Error().Err(err).Str("event_key", key).Str("correlation_id", envelope.TraceID).Dur("latency", latency).Msg("notification sink rejected event")
		c.retry(msg, envelope, err)
		return "retry"
	}

	// The sink also deduplicates by this key, covering a crash between its
	// successful response and this local bbolt write.
	if err := c.state.markProcessed(envelope.EventID, envelope.Version); err != nil {
		c.retry(msg, envelope, err)
		return "retry"
	}
	if err := msg.Ack(); err != nil {
		c.logger.Error().Err(err).Str("event_key", key).Msg("failed acknowledging notification")
		return "ack_failed"
	}
	if detected, ok := envelope.Data.Attributes[event.AttrDetectedAt].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, detected); err == nil {
			alertLatency.WithLabelValues(c.path, string(envelope.Data.Severity)).Observe(time.Since(t).Seconds())
		}
	}
	c.logger.Info().Ctx(ctx).Str("event_key", key).Str("path", c.path).Str("correlation_id", envelope.TraceID).Dur("latency", latency).Msg("notification delivered")
	return "ok"
}

func (c *notificationConsumer) retry(msg *nats.Msg, envelope event.Envelope, cause error) {
	c.logger.Warn().Err(cause).Str("event_id", envelope.EventID).Str("correlation_id", envelope.TraceID).Msg("event processing failed; scheduling redelivery")
	if err := msg.NakWithDelay(time.Second); err != nil {
		c.logger.Error().Err(err).Str("event_id", envelope.EventID).Msg("failed requesting redelivery")
	}
}

func makeHealthHandler(conn *nats.Conn) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !conn.IsConnected() {
			http.Error(w, `{"status":"nats_disconnected"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
}

func runConsumer(ctx context.Context, sub *nats.Subscription, consumer *notificationConsumer, logger zerolog.Logger) {
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
	logger := zerolog.New(os.Stdout).With().Timestamp().Str("service", "notification-consumer").Logger().Hook(observability.LogHook{})

	shutdownTracing, err := observability.Init(context.Background(), "notification-consumer")
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

	conn, err := nats.Connect(natsURL, nats.Name("BNPB Notification Consumer"), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
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
	alertSub, err := subscribeAlerts(js, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed creating durable subscription to the alert stream")
	}
	state, err := openState(dbPath)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed opening idempotency store")
	}
	defer state.db.Close()
	consumer := &notificationConsumer{
		path:  "regular",
		state: state, sinkURL: sinkURL,
		httpClient: &http.Client{Timeout: sinkTimeout, Transport: observability.Transport("notification-consumer", nil)},
		logger:     logger,
	}

	port := envOr("PORT", healthPort)
	addr := port
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}
	server := &http.Server{Addr: addr, Handler: withMetrics(makeHealthHandler(conn)), ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	alertConsumer := *consumer
	alertConsumer.path = "priority"
	alertConsumer.httpClient = &http.Client{Timeout: sinkTimeout, Transport: observability.Transport("notification-consumer", nil)}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		runConsumer(ctx, sub, consumer, logger)
	}()
	alertDone := make(chan struct{})
	go func() {
		defer close(alertDone)
		runConsumer(ctx, alertSub, &alertConsumer, logger)
	}()

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("health server failed")
		}
	case <-consumerDone:
		logger.Error().Msg("consumer loop exited unexpectedly")
	case <-alertDone:
		logger.Error().Msg("alert consumer loop exited unexpectedly")
	}
	stop()
	<-consumerDone
	<-alertDone
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("health server shutdown failed")
	}
}

// subscribeAlerts binds the priority consumer, creating BNPB_ALERTS with the
// same settings as the publisher if this service starts first. Alerts are
// redelivered without limit: a critical alert must never be dropped.
func subscribeAlerts(js nats.JetStreamContext, logger zerolog.Logger) (*nats.Subscription, error) {
	var lastErr error
	for retries := 0; retries < 30; retries++ {
		if _, err := js.StreamInfo(event.AlertStreamName); errors.Is(err, nats.ErrStreamNotFound) {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:       event.AlertStreamName,
				Subjects:   []string{event.AlertSubjectFilter},
				Storage:    nats.FileStorage,
				Retention:  nats.LimitsPolicy,
				MaxAge:     event.AlertMaxAge,
				Duplicates: event.AlertDuplicateWindow,
				Replicas:   1,
			})
			if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
				lastErr = err
				logger.Warn().Err(err).Msg("alert stream not ready yet, retrying in 1s...")
				time.Sleep(time.Second)
				continue
			}
		}
		sub, err := js.PullSubscribe(
			event.AlertSubjectFilter,
			alertDurable,
			nats.BindStream(event.AlertStreamName),
			nats.AckExplicit(),
			nats.AckWait(ackWait),
			nats.MaxDeliver(-1),
			nats.MaxAckPending(16),
		)
		if err == nil {
			return sub, nil
		}
		lastErr = err
		logger.Warn().Err(err).Msg("alert subscription not ready yet, retrying in 1s...")
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

// withMetrics serves /metrics beside the existing health handler.
func withMetrics(health http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", observability.MetricsHandler())
	mux.Handle("/", health)
	return mux
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

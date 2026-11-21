// Package ingest consumes canonical hazard ingest messages from NATS JetStream
// and applies them idempotently to the canonical store.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
)

const (
	// DurableName is the shared durable pull consumer name across all aggregator replicas.
	DurableName = "aggregator-ingest"

	// MaxPayloadBytes caps individual ingest message size at 256 KiB.
	MaxPayloadBytes = 256 * 1024

	AckWait       = 30 * time.Second
	MaxDeliver    = 10
	MaxAckPending = 256

	streamMaxAge     = 24 * time.Hour
	duplicateWindow  = 2 * time.Minute
	defaultBatchSize = 32
	defaultFetchWait = 2 * time.Second
	defaultNakDelay  = 2 * time.Second
)

// IngestStore is the subset of the canonical repository needed by the ingest consumer.
type IngestStore interface {
	ApplyIngest(ctx context.Context, msg event.IngestMessage) (bool, error)
}

// UnprocessedRecorder optionally records terminated poison messages for audit inspection.
type UnprocessedRecorder interface {
	RecordUnprocessed(ctx context.Context, source, sourceRefID string, raw []byte, reason string) error
}

// Msg abstracts a JetStream message for unit testing.
type Msg interface {
	Subject() string
	Data() []byte
	MsgID() string
	Ack() error
	NakWithDelay(delay time.Duration) error
	Term() error
}

type natsMessage struct {
	msg *nats.Msg
}

func (m natsMessage) Subject() string { return m.msg.Subject }
func (m natsMessage) Data() []byte    { return m.msg.Data }
func (m natsMessage) MsgID() string {
	if m.msg.Header == nil {
		return ""
	}
	return m.msg.Header.Get(nats.MsgIdHdr)
}
func (m natsMessage) Ack() error                         { return m.msg.Ack() }
func (m natsMessage) NakWithDelay(d time.Duration) error { return m.msg.NakWithDelay(d) }
func (m natsMessage) Term() error                        { return m.msg.Term() }

// EnsureIngestStream creates the HAZARD_INGEST stream on first start or updates
// the existing stream to the declared subjects, file storage, and retention policy.
func EnsureIngestStream(js nats.JetStreamContext) error {
	cfg := &nats.StreamConfig{
		Name:       event.IngestStreamName,
		Subjects:   []string{event.IngestSubjectFilter},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     streamMaxAge,
		Duplicates: duplicateWindow,
	}

	if _, err := js.StreamInfo(event.IngestStreamName); err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			_, err = js.AddStream(cfg)
		}
		return err
	}
	_, err := js.UpdateStream(cfg)
	return err
}

// Consumer pulls hazard.ingest.> messages from JetStream and applies them to the store.
type Consumer struct {
	js              nats.JetStreamContext
	store           IngestStore
	logger          zerolog.Logger
	sub             *nats.Subscription
	maxPayloadBytes int
	batchSize       int
	fetchWait       time.Duration
	nakDelay        time.Duration
}

// New creates a Consumer configured with the default limits and backoff.
func New(js nats.JetStreamContext, store IngestStore, logger zerolog.Logger) *Consumer {
	return &Consumer{
		js:              js,
		store:           store,
		logger:          logger.With().Str("component", "ingest-consumer").Logger(),
		maxPayloadBytes: MaxPayloadBytes,
		batchSize:       defaultBatchSize,
		fetchWait:       defaultFetchWait,
		nakDelay:        defaultNakDelay,
	}
}

// Subscribe creates or binds to the shared durable pull consumer on HAZARD_INGEST.
func (c *Consumer) Subscribe() error {
	if c.sub != nil {
		return nil
	}
	sub, err := c.js.PullSubscribe(
		event.IngestSubjectFilter,
		DurableName,
		nats.BindStream(event.IngestStreamName),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.AckWait(AckWait),
		nats.MaxDeliver(MaxDeliver),
		nats.MaxAckPending(MaxAckPending),
	)
	if err != nil {
		return err
	}
	c.sub = sub
	return nil
}

// Run fetches batches from JetStream until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	if err := c.Subscribe(); err != nil {
		c.logger.Error().Err(err).Msg("failed subscribing to ingest stream")
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}
		// nats.go rejects MaxWait together with Context, so the wait is a
		// per-fetch deadline on a context derived from ctx.
		fetchCtx, cancel := context.WithTimeout(ctx, c.fetchWait)
		msgs, err := c.sub.Fetch(c.batchSize, nats.Context(fetchCtx))
		cancel()
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, context.Canceled) || ctx.Err() != nil {
				continue
			}
			c.logger.Error().Err(err).Msg("ingest pull fetch failed")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for _, m := range msgs {
			// Continue the producer's trace (for example satellite-ingestor), so a
			// fire shows as one trace from the feed to the canonical store.
			mctx, op := observability.StartConsume(ctx, "aggregator", m.Subject, m.Header)
			c.ProcessMessage(mctx, natsMessage{msg: m})
			op.End("processed", nil)
		}
	}
}

// ProcessMessage validates, decodes, and applies one ingest message with panic recovery.
func (c *Consumer) ProcessMessage(ctx context.Context, msg Msg) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error().
				Interface("panic", r).
				Str("subject", msg.Subject()).
				Str("nats_msg_id", msg.MsgID()).
				Msg("panic while processing ingest message; terminating")
			c.recordParked(ctx, msg, "", "", "panic while processing ingest message")
			if err := msg.Term(); err != nil {
				c.logger.Error().Err(err).Str("subject", msg.Subject()).Msg("failed terminating panicked ingest message")
			}
		}
	}()

	data := msg.Data()
	if len(data) > c.maxPayloadBytes {
		c.logger.Error().
			Str("reason", "payload exceeds max size").
			Int("size_bytes", len(data)).
			Int("max_bytes", c.maxPayloadBytes).
			Str("subject", msg.Subject()).
			Str("nats_msg_id", msg.MsgID()).
			Msg("terminating oversized ingest message")
		c.recordParked(ctx, msg, "", "", "payload exceeds max size")
		if err := msg.Term(); err != nil {
			c.logger.Error().Err(err).Str("subject", msg.Subject()).Msg("failed terminating oversized ingest message")
		}
		return
	}

	var ingestMsg event.IngestMessage
	if err := json.Unmarshal(data, &ingestMsg); err != nil {
		c.logger.Error().
			Err(err).
			Str("reason", "invalid json").
			Str("subject", msg.Subject()).
			Str("nats_msg_id", msg.MsgID()).
			Msg("terminating undecodable ingest message")
		c.recordParked(ctx, msg, "", "", "invalid json")
		if termErr := msg.Term(); termErr != nil {
			c.logger.Error().Err(termErr).Str("subject", msg.Subject()).Msg("failed terminating undecodable ingest message")
		}
		return
	}

	if err := ingestMsg.Validate(); err != nil {
		reason := "validation failed: " + err.Error()
		c.logger.Error().
			Err(err).
			Str("reason", reason).
			Str("subject", msg.Subject()).
			Str("nats_msg_id", msg.MsgID()).
			Str("source", string(ingestMsg.Source)).
			Str("source_ref_id", ingestMsg.SourceRefID).
			Msg("terminating invalid ingest message")
		c.recordParked(ctx, msg, string(ingestMsg.Source), ingestMsg.SourceRefID, reason)
		if termErr := msg.Term(); termErr != nil {
			c.logger.Error().Err(termErr).Str("subject", msg.Subject()).Msg("failed terminating invalid ingest message")
		}
		return
	}

	applied, err := c.store.ApplyIngest(ctx, ingestMsg)
	if err != nil {
		c.logger.Error().
			Err(err).
			Str("subject", msg.Subject()).
			Str("nats_msg_id", msg.MsgID()).
			Str("source", string(ingestMsg.Source)).
			Str("source_ref_id", ingestMsg.SourceRefID).
			Int64("source_version", ingestMsg.SourceVersion).
			Msg("failed applying ingest message; naking with delay")
		if nakErr := msg.NakWithDelay(c.nakDelay); nakErr != nil {
			c.logger.Error().Err(nakErr).Str("subject", msg.Subject()).Msg("failed naking ingest message")
		}
		return
	}

	if err := msg.Ack(); err != nil {
		c.logger.Error().
			Err(err).
			Str("subject", msg.Subject()).
			Str("nats_msg_id", msg.MsgID()).
			Msg("failed acknowledging ingest message")
		return
	}

	if applied {
		c.logger.Info().
			Str("subject", msg.Subject()).
			Str("nats_msg_id", msg.MsgID()).
			Str("op", string(ingestMsg.Op)).
			Str("source", string(ingestMsg.Source)).
			Str("source_ref_id", ingestMsg.SourceRefID).
			Int64("source_version", ingestMsg.SourceVersion).
			Msg("applied ingest message")
	} else {
		c.logger.Debug().
			Str("subject", msg.Subject()).
			Str("nats_msg_id", msg.MsgID()).
			Str("op", string(ingestMsg.Op)).
			Str("source", string(ingestMsg.Source)).
			Str("source_ref_id", ingestMsg.SourceRefID).
			Int64("source_version", ingestMsg.SourceVersion).
			Msg("ignored stale or duplicate ingest message")
	}
}

func (c *Consumer) recordParked(ctx context.Context, msg Msg, source, sourceRefID, reason string) {
	rec, ok := c.store.(UnprocessedRecorder)
	if !ok {
		return
	}
	if source == "" {
		source = strings.TrimPrefix(msg.Subject(), "hazard.ingest.")
		if source == "" || source == msg.Subject() {
			source = "UNKNOWN"
		}
	}
	if err := rec.RecordUnprocessed(ctx, source, sourceRefID, msg.Data(), reason); err != nil {
		c.logger.Warn().
			Err(err).
			Str("subject", msg.Subject()).
			Str("source", source).
			Str("reason", reason).
			Msg("failed recording terminated ingest message to dead_letters")
	}
}

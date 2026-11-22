// Package outboxrelay publishes committed canonical events to NATS JetStream.
package outboxrelay

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
)

const (
	defaultPollInterval = 500 * time.Millisecond
	defaultBatchSize    = 100
	streamMaxAge        = 24 * time.Hour
	duplicateWindow     = 2 * time.Minute
)

// OutboxStore is the portion of the canonical repository used by the relay.
type OutboxStore interface {
	GetUnpublishedOutbox(context.Context, int) ([]store.OutboxRecord, error)
	MarkOutboxPublished(context.Context, int64) error
}

// BatchOutboxStore is an optional extension allowing the relay to mark an
// entire batch of published outbox rows in a single SQL statement.
type BatchOutboxStore interface {
	OutboxStore
	MarkOutboxPublishedBatch(context.Context, []int64) error
}

// Publisher publishes one durable message and returns only after JetStream
// acknowledges it.
type Publisher interface {
	Publish(context.Context, string, string, []byte) error
}

type JetStreamPublisher struct {
	js nats.JetStreamContext
}

func NewJetStreamPublisher(js nats.JetStreamContext) *JetStreamPublisher {
	return &JetStreamPublisher{js: js}
}

func (p *JetStreamPublisher) Publish(ctx context.Context, subject, messageID string, payload []byte) error {
	ctx, op := observability.StartPublish(ctx, "aggregator", subject)
	msg := nats.NewMsg(subject)
	msg.Data = payload
	msg.Header.Set(nats.MsgIdHdr, messageID)
	observability.Inject(ctx, msg.Header)
	_, err := p.js.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		op.End("error", err)
		return err
	}
	op.End("ok", nil)
	return nil
}

type Relay struct {
	store        OutboxStore
	publisher    Publisher
	logger       zerolog.Logger
	pollInterval time.Duration
	batchSize    int
	isLeader     func() bool
}

func New(repo OutboxStore, publisher Publisher, logger zerolog.Logger, pollInterval time.Duration, batchSize int) *Relay {
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	return &Relay{
		store:        repo,
		publisher:    publisher,
		logger:       logger.With().Str("component", "outbox-relay").Logger(),
		pollInterval: pollInterval,
		batchSize:    batchSize,
	}
}

// WithLeaderCheck restricts outbox polling to ticks where fn returns true,
// avoiding redundant multi-replica scans and UPDATE contention on the outbox table.
func (r *Relay) WithLeaderCheck(fn func() bool) *Relay {
	r.isLeader = fn
	return r
}

// Run polls immediately, then on each configured interval. A failed publish
// leaves the row pending and stops this batch so later events cannot overtake
// it; the next poll retries the same row.
func (r *Relay) Run(ctx context.Context) {
	if r.isLeader == nil || r.isLeader() {
		if err := r.PublishPending(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error().Err(err).Msg("outbox poll failed; will retry")
		}
	}

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.isLeader != nil && !r.isLeader() {
				continue
			}
			if err := r.PublishPending(ctx); err != nil && !errors.Is(err, context.Canceled) {
				r.logger.Error().Err(err).Msg("outbox poll failed; will retry")
			}
		}
	}
}

// PublishPending delivers up to one batch of events. Each row is marked
// published only after JetStream's publish acknowledgement succeeds.
func (r *Relay) PublishPending(ctx context.Context) error {
	rows, err := r.store.GetUnpublishedOutbox(ctx, r.batchSize)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	batchStore, canBatch := r.store.(BatchOutboxStore)
	var publishedIDs []int64
	if canBatch {
		publishedIDs = make([]int64, 0, len(rows))
	}

	for _, row := range rows {
		var envelope event.Envelope
		if err := json.Unmarshal(row.Payload, &envelope); err != nil {
			if canBatch && len(publishedIDs) > 0 {
				_ = batchStore.MarkOutboxPublishedBatch(ctx, publishedIDs)
			}
			return errors.New("decode outbox event " + row.EventKey + ": " + err.Error())
		}

		started := time.Now()
		// The envelope's trace_id is the correlation ID of the ingest that
		// produced the event; carrying it lets consumers' logs join up.
		pubCtx := observability.WithCorrelationID(ctx, envelope.TraceID)
		if err := r.publisher.Publish(pubCtx, event.EventSubject, row.EventKey, row.Payload); err != nil {
			r.logger.Error().
				Err(err).
				Str("event_key", row.EventKey).
				Str("correlation_id", envelope.TraceID).
				Dur("latency", time.Since(started)).
				Msg("JetStream publish failed; event remains in outbox")
			if canBatch && len(publishedIDs) > 0 {
				_ = batchStore.MarkOutboxPublishedBatch(ctx, publishedIDs)
			}
			return err
		}

		if canBatch {
			publishedIDs = append(publishedIDs, row.ID)
		} else {
			if err := r.store.MarkOutboxPublished(ctx, row.ID); err != nil {
				r.logger.Error().
					Err(err).
					Str("event_key", row.EventKey).
					Str("correlation_id", envelope.TraceID).
					Dur("latency", time.Since(started)).
					Msg("failed marking published outbox event; broker may receive a deduplicated retry")
				return err
			}
		}

		r.logger.Info().
			Str("event_key", row.EventKey).
			Str("correlation_id", envelope.TraceID).
			Dur("latency", time.Since(started)).
			Msg("outbox event published")
	}

	if canBatch && len(publishedIDs) > 0 {
		if err := batchStore.MarkOutboxPublishedBatch(ctx, publishedIDs); err != nil {
			r.logger.Error().
				Err(err).
				Int("count", len(publishedIDs)).
				Msg("failed marking published outbox batch; broker may receive deduplicated retries")
			return err
		}
	}
	return nil
}

// EnsureStream creates the project event stream on first start or updates the
// existing stream to the declared subject, file storage, and retention policy.
func EnsureStream(js nats.JetStreamContext) error {
	config := &nats.StreamConfig{
		Name:       event.StreamName,
		Subjects:   []string{event.SubjectFilter},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     streamMaxAge,
		Duplicates: duplicateWindow,
	}

	if _, err := js.StreamInfo(event.StreamName); err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			_, err = js.AddStream(config)
		}
		return err
	}
	_, err := js.UpdateStream(config)
	return err
}

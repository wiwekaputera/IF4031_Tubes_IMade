// Package relay publishes the satellite outbox to JetStream. A row is marked
// published only after JetStream acknowledges it. If the process dies between
// the acknowledgement and the mark, the row is sent again with the same
// Nats-Msg-Id and JetStream drops the duplicate.
package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/observability"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/store"
)

const (
	defaultInterval = 500 * time.Millisecond
	defaultBatch    = 100
	streamMaxAge    = 24 * time.Hour
	duplicateWindow = 2 * time.Minute
)

// Store is the part of the satellite store the relay needs.
type Store interface {
	PendingOutbox(ctx context.Context, limit int) ([]store.OutboxRow, error)
	MarkPublished(ctx context.Context, id int64) error
}

// Publisher sends one message and returns after the broker acknowledges it.
type Publisher interface {
	Publish(ctx context.Context, subject, messageID string, payload []byte) error
}

// JetStream publishes with Nats-Msg-Id set, so JetStream deduplicates retries.
type JetStream struct {
	js nats.JetStreamContext
}

// NewJetStream wraps a JetStream context.
func NewJetStream(js nats.JetStreamContext) *JetStream { return &JetStream{js: js} }

// Publish implements Publisher.
func (p *JetStream) Publish(ctx context.Context, subject, messageID string, payload []byte) error {
	ctx, op := observability.StartPublish(ctx, "satellite-ingestor", subject)
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

// EnsureIngestStream creates the ingest stream if it does not exist yet. An
// existing stream is left alone, because the aggregator owns its settings.
func EnsureIngestStream(js nats.JetStreamContext) error {
	_, err := js.StreamInfo(event.IngestStreamName)
	if err == nil {
		return nil
	}
	if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:       event.IngestStreamName,
		Subjects:   []string{event.IngestSubjectFilter},
		Storage:    nats.FileStorage,
		Retention:  nats.LimitsPolicy,
		MaxAge:     streamMaxAge,
		Duplicates: duplicateWindow,
	})
	return err
}

// Relay moves outbox rows to the broker in order.
type Relay struct {
	Store     Store
	Publisher Publisher
	Log       *slog.Logger
	Interval  time.Duration
	Batch     int
}

// Run publishes pending rows every Interval until ctx ends.
func (r *Relay) Run(ctx context.Context) {
	interval := r.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.PublishPending(ctx); err != nil && ctx.Err() == nil {
			r.logger().Warn("outbox publish failed, will retry", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PublishPending sends one batch and returns how many rows were published.
// It stops at the first failure so a later message never overtakes an
// earlier one for the same fire.
func (r *Relay) PublishPending(ctx context.Context) (int, error) {
	batch := r.Batch
	if batch <= 0 {
		batch = defaultBatch
	}
	rows, err := r.Store.PendingOutbox(ctx, batch)
	if err != nil {
		return 0, err
	}
	for i, row := range rows {
		if err := r.Publisher.Publish(ctx, row.Subject, row.MessageID, row.Payload); err != nil {
			return i, err
		}
		if err := r.Store.MarkPublished(ctx, row.ID); err != nil {
			return i, err
		}
	}
	return len(rows), nil
}

func (r *Relay) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

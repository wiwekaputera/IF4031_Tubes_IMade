package outboxrelay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
)

type fakeOutboxStore struct {
	rows      []store.OutboxRecord
	markedIDs []int64
	markErr   error
}

func (s *fakeOutboxStore) GetUnpublishedOutbox(_ context.Context, _ int) ([]store.OutboxRecord, error) {
	return s.rows, nil
}

func (s *fakeOutboxStore) MarkOutboxPublished(_ context.Context, id int64) error {
	if s.markErr != nil {
		return s.markErr
	}
	s.markedIDs = append(s.markedIDs, id)
	return nil
}

type publishedMessage struct {
	subject   string
	messageID string
	payload   []byte
}

type fakePublisher struct {
	messages []publishedMessage
	err      error
}

func (p *fakePublisher) Publish(_ context.Context, subject, messageID string, payload []byte) error {
	p.messages = append(p.messages, publishedMessage{
		subject: subject, messageID: messageID, payload: payload,
	})
	return p.err
}

func TestPublishPendingPublishesAndMarksAcknowledgedRows(t *testing.T) {
	firstPayload := []byte(`{"event_id":"hazard-1","version":1,"trace_id":"corr-1","data":{}}`)
	secondPayload := []byte(`{"event_id":"hazard-2","version":2,"trace_id":"corr-2","data":{}}`)
	repo := &fakeOutboxStore{rows: []store.OutboxRecord{
		{ID: 11, EventKey: "hazard-1:1", Payload: firstPayload},
		{ID: 12, EventKey: "hazard-2:2", Payload: secondPayload},
	}}
	publisher := &fakePublisher{}
	relay := New(repo, publisher, zerolog.Nop(), time.Second, 10)

	if err := relay.PublishPending(context.Background()); err != nil {
		t.Fatalf("PublishPending() error = %v", err)
	}
	if len(publisher.messages) != 2 {
		t.Fatalf("published %d messages, want 2", len(publisher.messages))
	}
	if got := publisher.messages[0]; got.subject != event.EventSubject || got.messageID != "hazard-1:1" {
		t.Fatalf("first publish = (%q, %q), want (%q, %q)", got.subject, got.messageID, event.EventSubject, "hazard-1:1")
	}
	if len(repo.markedIDs) != 2 || repo.markedIDs[0] != 11 || repo.markedIDs[1] != 12 {
		t.Fatalf("marked IDs = %v, want [11 12]", repo.markedIDs)
	}
}

func TestPublishPendingLeavesFailedPublishUnmarkedAndStopsBatch(t *testing.T) {
	repo := &fakeOutboxStore{rows: []store.OutboxRecord{
		{ID: 11, EventKey: "hazard-1:1", Payload: []byte(`{"event_id":"hazard-1","version":1,"data":{}}`)},
		{ID: 12, EventKey: "hazard-2:1", Payload: []byte(`{"event_id":"hazard-2","version":1,"data":{}}`)},
	}}
	publisher := &fakePublisher{err: errors.New("broker unavailable")}
	relay := New(repo, publisher, zerolog.Nop(), time.Second, 10)

	if err := relay.PublishPending(context.Background()); err == nil {
		t.Fatal("PublishPending() error = nil, want broker error")
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("attempted %d publishes, want 1 before stopping the batch", len(publisher.messages))
	}
	if len(repo.markedIDs) != 0 {
		t.Fatalf("marked IDs = %v, want none after failed publish", repo.markedIDs)
	}
}

func TestPublishPendingLeavesPublishedRowUnmarkedWhenDatabaseUpdateFails(t *testing.T) {
	repo := &fakeOutboxStore{
		rows: []store.OutboxRecord{{
			ID: 11, EventKey: "hazard-1:1", Payload: []byte(`{"event_id":"hazard-1","version":1,"data":{}}`),
		}},
		markErr: errors.New("database unavailable"),
	}
	publisher := &fakePublisher{}
	relay := New(repo, publisher, zerolog.Nop(), time.Second, 10)

	if err := relay.PublishPending(context.Background()); err == nil {
		t.Fatal("PublishPending() error = nil, want mark error")
	}
	if len(publisher.messages) != 1 {
		t.Fatalf("published %d messages, want one acknowledged publish", len(publisher.messages))
	}
	if len(repo.markedIDs) != 0 {
		t.Fatalf("marked IDs = %v, want none when update failed", repo.markedIDs)
	}
}

func TestPublishPendingRejectsInvalidEnvelope(t *testing.T) {
	repo := &fakeOutboxStore{rows: []store.OutboxRecord{{
		ID: 11, EventKey: "hazard-1:1", Payload: []byte("not-json"),
	}}}
	publisher := &fakePublisher{}
	relay := New(repo, publisher, zerolog.Nop(), time.Second, 10)

	if err := relay.PublishPending(context.Background()); err == nil {
		t.Fatal("PublishPending() error = nil, want invalid-envelope error")
	}
	if len(publisher.messages) != 0 || len(repo.markedIDs) != 0 {
		t.Fatal("invalid envelope must not be published or marked")
	}
}

type fakeBatchOutboxStore struct {
	fakeOutboxStore
	batchCalls int
}

func (s *fakeBatchOutboxStore) MarkOutboxPublishedBatch(_ context.Context, ids []int64) error {
	s.batchCalls++
	if s.markErr != nil {
		return s.markErr
	}
	s.markedIDs = append(s.markedIDs, ids...)
	return nil
}

func TestPublishPendingUsesBatchMarkWhenSupported(t *testing.T) {
	repo := &fakeBatchOutboxStore{
		fakeOutboxStore: fakeOutboxStore{
			rows: []store.OutboxRecord{
				{ID: 21, EventKey: "hazard-1:1", Payload: []byte(`{"event_id":"hazard-1","version":1,"data":{}}`)},
				{ID: 22, EventKey: "hazard-2:1", Payload: []byte(`{"event_id":"hazard-2","version":1,"data":{}}`)},
				{ID: 23, EventKey: "hazard-3:1", Payload: []byte(`{"event_id":"hazard-3","version":1,"data":{}}`)},
			},
		},
	}
	publisher := &fakePublisher{}
	relay := New(repo, publisher, zerolog.Nop(), time.Second, 10)

	if err := relay.PublishPending(context.Background()); err != nil {
		t.Fatalf("PublishPending() error = %v", err)
	}
	if repo.batchCalls != 1 {
		t.Fatalf("expected 1 MarkOutboxPublishedBatch call, got %d", repo.batchCalls)
	}
	if len(repo.markedIDs) != 3 || repo.markedIDs[0] != 21 || repo.markedIDs[2] != 23 {
		t.Fatalf("marked IDs = %v, want [21 22 23]", repo.markedIDs)
	}
}

func TestRelaySkipsPollingWhenNotLeader(t *testing.T) {
	repo := &fakeBatchOutboxStore{
		fakeOutboxStore: fakeOutboxStore{
			rows: []store.OutboxRecord{
				{ID: 31, EventKey: "hazard-1:1", Payload: []byte(`{"event_id":"hazard-1","version":1,"data":{}}`)},
			},
		},
	}
	publisher := &fakePublisher{}
	relay := New(repo, publisher, zerolog.Nop(), 10*time.Millisecond, 10).WithLeaderCheck(func() bool { return false })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Millisecond)
	defer cancel()
	relay.Run(ctx)

	if len(publisher.messages) != 0 || len(repo.markedIDs) != 0 {
		t.Fatalf("non-leader relay must not publish; got %d published, %d marked", len(publisher.messages), len(repo.markedIDs))
	}
}

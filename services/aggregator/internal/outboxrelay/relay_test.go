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

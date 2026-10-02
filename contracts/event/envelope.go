// Package event defines the NATS event envelope published to the message broker.
// This package contains ONLY struct definitions and JSON tags — no business logic.
package event

import "github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"

// Envelope is the message published to NATS JetStream for every new or updated HazardEvent.
// The idempotency key is EventID:Version, set as Nats-Msg-Id header by the Outbox Relay.
type Envelope struct {
	EventID    string             `json:"event_id"`
	Version    int64              `json:"version"`
	EventType  hazard.HazardType  `json:"event_type"`
	OccurredAt string             `json:"occurred_at"`
	TraceID    string             `json:"trace_id"` // correlation_id from the ingest cycle
	Data       hazard.HazardEvent `json:"data"`
}

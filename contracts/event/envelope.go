// Package event defines the NATS event envelope published to the message broker.
// This package contains ONLY struct definitions and JSON tags — no business logic.
package event

// Envelope is the message published to NATS JetStream for every new or updated HazardEvent.
// The idempotency key is EventID:Version, set as Nats-Msg-Id header by the Outbox Relay.
type Envelope struct {
	EventID    string      `json:"event_id"`   // hazard_id
	Version    int64       `json:"version"`
	EventType  string      `json:"event_type"` // "SEISMIC" | "VOLCANIC"
	OccurredAt string      `json:"occurred_at"` // ISO 8601
	TraceID    string      `json:"trace_id"`    // correlation_id propagated from ingest cycle
	Data       interface{} `json:"data"`        // full HazardEvent payload
}

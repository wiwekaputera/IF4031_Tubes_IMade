package event

import "github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"

// JetStream names shared by the Aggregator publisher and BNPB consumers.
const (
	StreamName    = "BNPB_EVENTS"
	EventSubject  = "bnpb.events.hazard"
	SubjectFilter = "bnpb.events.>"
)

// Envelope is the message published to NATS JetStream for every new or updated HazardEvent.
// The idempotency key is EventID:Version, set as Nats-Msg-Id header by the Outbox Relay.
type Envelope struct {
	EventID    string             `json:"event_id"`
	Version    int64              `json:"version"`
	EventType  hazard.HazardType  `json:"event_type"`
	OccurredAt string             `json:"occurred_at"`
	TraceID    string             `json:"trace_id"`
	Data       hazard.HazardEvent `json:"data"`
}

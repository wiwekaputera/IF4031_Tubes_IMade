package hazard

// SourceHealthMeta defines the runtime health status and sync/staleness timestamps of an upstream agency source.
type SourceHealthMeta struct {
	Status       string  `json:"status"`                   // "UP" | "DOWN" | "OUTAGE"
	LastSyncedAt string  `json:"last_synced_at,omitempty"` // RFC3339 timestamp of last successful sync
	StaleSince   *string `json:"stale_since,omitempty"`    // RFC3339 timestamp when circuit breaker entered OUTAGE
}

// SourcesMeta tracks the runtime status of individual upstream sources (BMKG and PVMBG).
type SourcesMeta struct {
	BMKG  SourceHealthMeta `json:"bmkg"`
	PVMBG SourceHealthMeta `json:"pvmbg"`
}

// EventsMeta contains query count and optional upstream agency health/staleness metadata.
type EventsMeta struct {
	Count   int          `json:"count"`
	Sources *SourcesMeta `json:"sources,omitempty"`
}

// EventsResponse is the formal HTTP response contract for query endpoints (GET /internal/events, GET /v1/events).
type EventsResponse struct {
	Data []HazardEvent `json:"data"`
	Meta EventsMeta    `json:"meta"`
}

// HealthResponse is the formal HTTP response contract for GET /internal/health and service healthchecks.
type HealthResponse struct {
	Status string `json:"status"`
	Time   string `json:"time"`
}

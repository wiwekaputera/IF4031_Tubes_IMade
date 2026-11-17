package audit

import (
	"encoding/json"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// ErrorDetail describes a structured error returned by the Audit API.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorBody is the standard error envelope for Audit API responses.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// EventsPage is the paginated response for GET /internal/audit/events.
type EventsPage struct {
	Items            []hazard.HazardEvent `json:"items"`
	NextCursor       *string              `json:"next_cursor"`
	Partial          bool                 `json:"partial"`
	UnavailableUnits []string             `json:"unavailable_units"`
}

// MarshalJSON ensures Items and UnavailableUnits serialize as [] rather than null.
func (p EventsPage) MarshalJSON() ([]byte, error) {
	type alias EventsPage
	a := alias(p)
	if a.Items == nil {
		a.Items = []hazard.HazardEvent{}
	}
	if a.UnavailableUnits == nil {
		a.UnavailableUnits = []string{}
	}
	return json.Marshal(a)
}

// Checksum is the response for GET /internal/audit/checksum.
type Checksum struct {
	Source           string   `json:"source"`
	Count            int64    `json:"count"`
	Checksum         string   `json:"checksum"`
	Algorithm        string   `json:"algorithm"`
	AsOf             string   `json:"as_of"`
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
}

// MarshalJSON ensures UnavailableUnits serializes as [] rather than null.
func (c Checksum) MarshalJSON() ([]byte, error) {
	type alias Checksum
	a := alias(c)
	if a.UnavailableUnits == nil {
		a.UnavailableUnits = []string{}
	}
	return json.Marshal(a)
}

// Location describes a single replica placement for a hazard event.
type Location struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

// LocationResponse is the response for GET /internal/audit/location.
type LocationResponse struct {
	HazardID  string     `json:"hazard_id"`
	Locations []Location `json:"locations"`
	Partial   bool       `json:"partial"`
}

// MarshalJSON ensures Locations serializes as [] rather than null.
func (r LocationResponse) MarshalJSON() ([]byte, error) {
	type alias LocationResponse
	a := alias(r)
	if a.Locations == nil {
		a.Locations = []Location{}
	}
	return json.Marshal(a)
}

// UnprocessedStatus represents the processing state of a DLQ / unprocessed item.
type UnprocessedStatus string

const (
	UnprocessedStatusRetrying UnprocessedStatus = "retrying"
	UnprocessedStatusParked   UnprocessedStatus = "parked"
)

// UnprocessedItem represents a single failed or retrying message in the audit view.
type UnprocessedItem struct {
	MessageID  string            `json:"message_id"`
	Source     string            `json:"source"`
	ReceivedAt string            `json:"received_at"`
	Reason     string            `json:"reason"`
	Retryable  bool              `json:"retryable"`
	Attempts   int               `json:"attempts"`
	Status     UnprocessedStatus `json:"status"`
}

// UnprocessedPage is the paginated response for GET /internal/audit/unprocessed.
type UnprocessedPage struct {
	Items      []UnprocessedItem `json:"items"`
	NextCursor *string           `json:"next_cursor"`
}

// MarshalJSON ensures Items serializes as [] rather than null.
func (p UnprocessedPage) MarshalJSON() ([]byte, error) {
	type alias UnprocessedPage
	a := alias(p)
	if a.Items == nil {
		a.Items = []UnprocessedItem{}
	}
	return json.Marshal(a)
}

package httpapi

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

var uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// EventQueryParams holds validated query parameters for GET /v1/events.
type EventQueryParams struct {
	Type     string
	Limit    int
	Since    string
	HazardID string
	View     string
}

// ValidateEventQuery validates query parameters
func ValidateEventQuery(query url.Values) (*EventQueryParams, error) {
	// 1. Validate 'type'
	hazardType := query.Get("type")
	if hazardType != "" {
		if hazardType != "SEISMIC" && hazardType != "VOLCANIC" {
			return nil, fmt.Errorf("invalid type parameter %q: must be 'SEISMIC' or 'VOLCANIC'", hazardType)
		}
	}

	// 2. Validate 'limit' (1–100, default 20
	limit := 20
	if limStr := query.Get("limit"); limStr != "" {
		parsed, err := strconv.Atoi(limStr)
		if err != nil || parsed < 1 || parsed > 100 {
			return nil, fmt.Errorf("invalid limit parameter %q: must be an integer between 1 and 100", limStr)
		}
		limit = parsed
	}

	// 3. Validate 'since' (RFC3339 format)
	since := query.Get("since")
	if since != "" {
		if _, err := time.Parse(time.RFC3339, since); err != nil {
			if _, err := time.Parse(time.RFC3339Nano, since); err != nil {
				return nil, fmt.Errorf("invalid since parameter %q: must be a valid RFC3339 timestamp", since)
			}
		}
	}

	// 4. Validate 'hazard_id' (canonical UUID format)
	hazardID := query.Get("hazard_id")
	if hazardID != "" {
		if !uuidRegex.MatchString(hazardID) {
			return nil, fmt.Errorf("invalid hazard_id parameter %q: must be a valid UUID", hazardID)
		}
	}

	// 5. Validate 'view'
	view := query.Get("view")
	if view != "" && view != "summary" && view != "full" {
		return nil, fmt.Errorf("invalid view parameter %q: must be 'summary' or 'full'", view)
	}

	return &EventQueryParams{
		Type:     hazardType,
		Limit:    limit,
		Since:    since,
		HazardID: hazardID,
		View:     view,
	}, nil
}

// ToUpstreamQuery encodes parameters into a URL query string for upstream Aggregator.
func (p *EventQueryParams) ToUpstreamQuery() string {
	vals := url.Values{}
	if p.Type != "" {
		vals.Set("type", p.Type)
	}
	if p.Limit > 0 {
		vals.Set("limit", strconv.Itoa(p.Limit))
	}
	if p.Since != "" {
		vals.Set("since", p.Since)
	}
	if p.HazardID != "" {
		vals.Set("hazard_id", p.HazardID)
	}
	return vals.Encode()
}

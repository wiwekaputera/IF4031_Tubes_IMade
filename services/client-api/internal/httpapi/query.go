package httpapi

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

var (
	uuidRegex = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	// canonicalIDRegex matches hazard.ID: "hz-" and 16 hex characters.
	canonicalIDRegex = regexp.MustCompile(`^hz-[0-9a-f]{16}$`)
	// cursorRegex: the aggregator's opaque pagination cursor (base64url).
	cursorRegex = regexp.MustCompile(`^[A-Za-z0-9_=.-]{1,512}$`)
)

// EventQueryParams holds validated query parameters for GET /v1/events.
type EventQueryParams struct {
	Type     string
	Limit    int
	Since    string
	HazardID string
	View     string
	Cursor   string
}

// ValidateEventQuery validates query parameters
func ValidateEventQuery(query url.Values) (*EventQueryParams, error) {
	// 1. Validate 'type'
	hazardType := query.Get("type")
	if hazardType != "" {
		if !hazard.HazardType(hazardType).Valid() {
			return nil, fmt.Errorf("invalid type parameter %q: must be SEISMIC, VOLCANIC, WILDFIRE or COASTAL", hazardType)
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

	// 4. Validate 'hazard_id' (canonical "hz-…" ID, or a UUID from older data)
	hazardID := query.Get("hazard_id")
	if hazardID != "" {
		if !canonicalIDRegex.MatchString(hazardID) && !uuidRegex.MatchString(hazardID) {
			return nil, fmt.Errorf("invalid hazard_id parameter %q: must be a canonical hz-… ID", hazardID)
		}
	}

	// 4b. Validate 'cursor' (opaque pagination cursor from a previous response)
	cursor := query.Get("cursor")
	if cursor != "" && !cursorRegex.MatchString(cursor) {
		return nil, fmt.Errorf("invalid cursor parameter: use next_cursor from a previous response")
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
		Cursor:   cursor,
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
	if p.Cursor != "" {
		vals.Set("cursor", p.Cursor)
	}
	return vals.Encode()
}

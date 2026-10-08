package httpapi

import (
	"net/url"
	"strings"
	"testing"
)

func TestValidateEventQuery_ValidCases(t *testing.T) {
	tests := []struct {
		name          string
		query         url.Values
		expectedType  string
		expectedLimit int
		expectedSince string
		expectedHID   string
		expectedView  string
	}{
		{
			name:          "empty query defaults to limit 20",
			query:         url.Values{},
			expectedType:  "",
			expectedLimit: 20,
			expectedSince: "",
			expectedHID:   "",
			expectedView:  "",
		},
		{
			name: "all valid parameters",
			query: url.Values{
				"type":      []string{"SEISMIC"},
				"limit":     []string{"50"},
				"since":     []string{"2026-09-21T20:00:00Z"},
				"hazard_id": []string{"550e8400-e29b-41d4-a716-446655440000"},
				"view":      []string{"full"},
			},
			expectedType:  "SEISMIC",
			expectedLimit: 50,
			expectedSince: "2026-09-21T20:00:00Z",
			expectedHID:   "550e8400-e29b-41d4-a716-446655440000",
			expectedView:  "full",
		},
		{
			name: "volcanic and boundary limits 1 and 100",
			query: url.Values{
				"type":  []string{"VOLCANIC"},
				"limit": []string{"1"},
			},
			expectedType:  "VOLCANIC",
			expectedLimit: 1,
		},
		{
			name: "limit 100",
			query: url.Values{
				"limit": []string{"100"},
			},
			expectedLimit: 100,
		},
		{
			name: "rfc3339 nano timestamp",
			query: url.Values{
				"since": []string{"2026-09-21T20:00:00.123456Z"},
			},
			expectedLimit: 20,
			expectedSince: "2026-09-21T20:00:00.123456Z",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params, err := ValidateEventQuery(tc.query)
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if params.Type != tc.expectedType {
				t.Errorf("expected type %q, got %q", tc.expectedType, params.Type)
			}
			if params.Limit != tc.expectedLimit {
				t.Errorf("expected limit %d, got %d", tc.expectedLimit, params.Limit)
			}
			if params.Since != tc.expectedSince {
				t.Errorf("expected since %q, got %q", tc.expectedSince, params.Since)
			}
			if params.HazardID != tc.expectedHID {
				t.Errorf("expected hazard_id %q, got %q", tc.expectedHID, params.HazardID)
			}
			if params.View != tc.expectedView {
				t.Errorf("expected view %q, got %q", tc.expectedView, params.View)
			}
		})
	}
}

func TestValidateEventQuery_InvalidCases(t *testing.T) {
	tests := []struct {
		name        string
		query       url.Values
		errContains string
	}{
		{
			name:        "invalid hazard type",
			query:       url.Values{"type": []string{"TSUNAMI"}},
			errContains: "invalid type parameter",
		},
		{
			name:        "limit zero",
			query:       url.Values{"limit": []string{"0"}},
			errContains: "invalid limit parameter",
		},
		{
			name:        "limit negative",
			query:       url.Values{"limit": []string{"-10"}},
			errContains: "invalid limit parameter",
		},
		{
			name:        "limit exceeds 100",
			query:       url.Values{"limit": []string{"101"}},
			errContains: "invalid limit parameter",
		},
		{
			name:        "limit non-integer",
			query:       url.Values{"limit": []string{"ten"}},
			errContains: "invalid limit parameter",
		},
		{
			name:        "invalid since timestamp format",
			query:       url.Values{"since": []string{"2026-09-21"}},
			errContains: "invalid since parameter",
		},
		{
			name:        "invalid since string",
			query:       url.Values{"since": []string{"yesterday"}},
			errContains: "invalid since parameter",
		},
		{
			name:        "invalid hazard_id non-uuid",
			query:       url.Values{"hazard_id": []string{"evt-12345"}},
			errContains: "invalid hazard_id parameter",
		},
		{
			name:        "invalid view string",
			query:       url.Values{"view": []string{"raw_everything"}},
			errContains: "invalid view parameter",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateEventQuery(tc.query)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("expected error to contain %q, got %q", tc.errContains, err.Error())
			}
		})
	}
}

func TestToUpstreamQuery(t *testing.T) {
	params := &EventQueryParams{
		Type:     "SEISMIC",
		Limit:    25,
		Since:    "2026-09-21T20:00:00Z",
		HazardID: "550e8400-e29b-41d4-a716-446655440000",
		View:     "full",
	}

	qStr := params.ToUpstreamQuery()
	vals, err := url.ParseQuery(qStr)
	if err != nil {
		t.Fatalf("failed to parse upstream query: %v", err)
	}

	if vals.Get("type") != "SEISMIC" {
		t.Errorf("expected type SEISMIC, got %q", vals.Get("type"))
	}
	if vals.Get("limit") != "25" {
		t.Errorf("expected limit 25, got %q", vals.Get("limit"))
	}
	if vals.Get("since") != "2026-09-21T20:00:00Z" {
		t.Errorf("expected since 2026-09-21T20:00:00Z, got %q", vals.Get("since"))
	}
	if vals.Get("hazard_id") != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("expected hazard_id, got %q", vals.Get("hazard_id"))
	}
	if vals.Get("view") != "" {
		t.Errorf("SECURITY/DESIGN VIOLATION: 'view' parameter should not be forwarded upstream, got %q", vals.Get("view"))
	}
}

package projection

import (
	"errors"
	"testing"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

func sampleEvents() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"hazard_id":        "evt-100",
			"source":           "BMKG",
			"source_ref_id":    "bmkg-999",
			"hazard_type":      "SEISMIC",
			"severity":         "SIAGA",
			"area_name":        "Sulawesi Tengah",
			"latitude":         -1.4,
			"longitude":        119.8,
			"occurred_at":      "2026-09-21T20:00:00Z",
			"ingested_at":      "2026-09-21T20:00:05Z",
			"attributes":       map[string]interface{}{"magnitude": 6.7},
			"internal_payload": "secret-upstream-data",
		},
	}
}

func TestValidateViewScope(t *testing.T) {
	tests := []struct {
		name        string
		scope       auth.Scope
		viewStr     string
		expectedErr error
		expectedVT  ViewType
	}{
		{
			name:        "media default view",
			scope:       auth.ScopeMedia,
			viewStr:     "",
			expectedErr: nil,
			expectedVT:  ViewDefault,
		},
		{
			name:        "media explicit summary",
			scope:       auth.ScopeMedia,
			viewStr:     "summary",
			expectedErr: nil,
			expectedVT:  ViewSummary,
		},
		{
			name:        "media explicit full view rejected",
			scope:       auth.ScopeMedia,
			viewStr:     "full",
			expectedErr: ErrInsufficientScope,
		},
		{
			name:        "field_team full view allowed",
			scope:       auth.ScopeFieldTeam,
			viewStr:     "full",
			expectedErr: nil,
			expectedVT:  ViewFull,
		},
		{
			name:        "field_team summary view allowed",
			scope:       auth.ScopeFieldTeam,
			viewStr:     "summary",
			expectedErr: nil,
			expectedVT:  ViewSummary,
		},
		{
			name:        "internal_ops full view allowed",
			scope:       auth.ScopeInternalOps,
			viewStr:     "full",
			expectedErr: nil,
			expectedVT:  ViewFull,
		},
		{
			name:        "unknown scope rejected",
			scope:       "guest",
			viewStr:     "",
			expectedErr: ErrUnknownScope,
		},
		{
			name:        "invalid view parameter rejected",
			scope:       auth.ScopeFieldTeam,
			viewStr:     "everything",
			expectedErr: ErrInvalidView,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vt, err := ValidateViewScope(tc.scope, tc.viewStr)
			if tc.expectedErr != nil {
				if !errors.Is(err, tc.expectedErr) {
					t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if vt != tc.expectedVT {
					t.Fatalf("expected view type %q, got %q", tc.expectedVT, vt)
				}
			}
		})
	}
}

func TestProjectEvents_Media_AllowlistOnly(t *testing.T) {
	events := sampleEvents()
	projected := ProjectEvents(events, auth.ScopeMedia, ViewDefault)

	if len(projected) != 1 {
		t.Fatalf("expected 1 projected event, got %d", len(projected))
	}

	evt := projected[0]

	// Verify required summary fields exist
	for _, field := range SummaryFields {
		if _, ok := evt[field]; !ok {
			t.Errorf("expected summary field %q to be present", field)
		}
	}

	// Verify forbidden raw fields do NOT leak
	forbidden := []string{"latitude", "longitude", "attributes", "source_ref_id", "internal_payload"}
	for _, field := range forbidden {
		if _, ok := evt[field]; ok {
			t.Errorf("SECURITY LEAK: field %q must NOT be present in media scope", field)
		}
	}
}

func TestProjectEvents_FieldTeam_FullFields(t *testing.T) {
	events := sampleEvents()
	projected := ProjectEvents(events, auth.ScopeFieldTeam, ViewFull)

	if len(projected) != 1 {
		t.Fatalf("expected 1 projected event, got %d", len(projected))
	}

	evt := projected[0]
	// Verify coordinates and attributes are preserved
	if evt["latitude"] != -1.4 {
		t.Fatalf("expected latitude -1.4, got %v", evt["latitude"])
	}
	if evt["longitude"] != 119.8 {
		t.Fatalf("expected longitude 119.8, got %v", evt["longitude"])
	}
	if evt["attributes"] == nil {
		t.Fatal("expected attributes to be present")
	}
}

func TestProjectEvents_FieldTeam_ExplicitSummary(t *testing.T) {
	events := sampleEvents()
	projected := ProjectEvents(events, auth.ScopeFieldTeam, ViewSummary)

	evt := projected[0]
	if _, ok := evt["latitude"]; ok {
		t.Fatal("expected latitude to be stripped when view=summary is requested")
	}
	if _, ok := evt["attributes"]; ok {
		t.Fatal("expected attributes to be stripped when view=summary is requested")
	}
	if _, ok := evt["hazard_id"]; !ok {
		t.Fatal("expected hazard_id to be present in summary view")
	}
}

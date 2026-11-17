package schema

import (
	"strings"
	"testing"
)

const validReport = `{"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","eruption_count_24h":2,"ash_column_height_m":1200.5,"reported_at":"2026-11-17T10:00:00Z"}`

func TestClassifyVolcanicReport(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantReason string // empty means accepted
	}{
		{name: "valid", body: validReport},
		{
			name: "unknown field is accepted",
			body: `{"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","eruption_count_24h":2,"ash_column_height_m":1200.5,"reported_at":"2026-11-17T10:00:00Z","new_field":"x"}`,
		},
		{
			name: "optional field may be absent",
			body: `{"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z"}`,
		},
		{
			name:       "renamed required field is rejected",
			body:       `{"report_id":"R-1","volcano_code":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z"}`,
			wantReason: "missing_required:volcano_id",
		},
		{
			name:       "null required field is rejected",
			body:       `{"report_id":null,"volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z"}`,
			wantReason: "missing_required:report_id",
		},
		{
			name:       "number where string expected is rejected",
			body:       `{"report_id":"R-1","volcano_id":42,"alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z"}`,
			wantReason: "type_mismatch:volcano_id",
		},
		{
			name:       "string where number expected is rejected",
			body:       `{"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","ash_column_height_m":"high","reported_at":"2026-11-17T10:00:00Z"}`,
			wantReason: "type_mismatch:ash_column_height_m",
		},
		{
			name:       "truncated JSON is rejected",
			body:       `{"report_id":"R-1","volcano_id":"GNG-0`,
			wantReason: "malformed_json",
		},
		{
			name:       "top-level array is rejected",
			body:       `[1,2,3]`,
			wantReason: "malformed_json",
		},
		{
			name:       "invalid UTF-8 is rejected",
			body:       "{\"report_id\":\"R-\xff\",\"volcano_id\":\"GNG-001\",\"alert_level\":\"SIAGA\",\"reported_at\":\"2026-11-17T10:00:00Z\"}",
			wantReason: "invalid_utf8",
		},
		{
			name:       "deep nesting is rejected",
			body:       `{"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z","x":` + strings.Repeat("[", 40) + strings.Repeat("]", 40) + `}`,
			wantReason: "nesting_too_deep",
		},
		{
			name:       "oversized body is rejected",
			body:       `{"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z","pad":"` + strings.Repeat("a", MaxBodyBytes) + `"}`,
			wantReason: "body_too_large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report, rej := ClassifyVolcanicReport([]byte(tt.body))
			if tt.wantReason == "" {
				if rej != nil {
					t.Fatalf("expected accept, got rejection %q", rej.Reason)
				}
				if report.ReportID == "" {
					t.Fatalf("accepted report has empty ReportID")
				}
				return
			}
			if rej == nil {
				t.Fatalf("expected rejection %q, got accept", tt.wantReason)
			}
			if rej.Reason != tt.wantReason {
				t.Fatalf("reason = %q, want %q", rej.Reason, tt.wantReason)
			}
			if rej.Retryable {
				t.Fatalf("schema/format rejections must not be retryable")
			}
		})
	}
}

func TestUnknownFieldsKeptAsExtra(t *testing.T) {
	body := `{"report_id":"R-1","volcano_id":"GNG-001","alert_level":"SIAGA","reported_at":"2026-11-17T10:00:00Z","new_field":"x"}`
	report, rej := ClassifyVolcanicReport([]byte(body))
	if rej != nil {
		t.Fatalf("unexpected rejection: %v", rej)
	}
	if _, ok := report.Extra["new_field"]; !ok {
		t.Fatalf("unknown field not preserved in Extra")
	}
}

func TestMaxNestingIgnoresBracketsInStrings(t *testing.T) {
	if got := maxNesting([]byte(`{"a":"[[[[[[[["}`)); got != 1 {
		t.Fatalf("maxNesting = %d, want 1", got)
	}
	if got := maxNesting([]byte(`{"a":"\"[[[","b":[[1]]}`)); got != 3 {
		t.Fatalf("maxNesting with escaped quote = %d, want 3", got)
	}
}

func TestPeek(t *testing.T) {
	ref, at := Peek([]byte(`{"report_id":"R-7","volcano_code":"x","reported_at":"2026-11-17T10:00:00Z"}`))
	if ref != "R-7" || at.IsZero() {
		t.Fatalf("Peek = (%q, %v), want (R-7, non-zero time)", ref, at)
	}
	ref, at = Peek([]byte(`not json`))
	if ref != "" || !at.IsZero() {
		t.Fatalf("Peek on malformed input = (%q, %v), want zero values", ref, at)
	}
}

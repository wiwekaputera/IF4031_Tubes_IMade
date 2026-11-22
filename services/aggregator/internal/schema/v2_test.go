package schema

import (
	"encoding/json"
	"testing"
	"time"
)

var received = time.Date(2026, 11, 22, 3, 4, 5, 0, time.UTC)

// A report exactly as the PVMBG mock sends it in incompatible mode.
const v2Report = `{"report_id":"PVMBG-1","volcano_code":"GNG-001","alert_level":"SIAGA","eruption_count_24h":3,"ash_column_height_m":"1500.0"}`

func TestV2ReportIsRejectedWithoutAdapter(t *testing.T) {
	if _, rej := ClassifyVolcanicReport([]byte(v2Report)); rej == nil {
		t.Fatal("v2 report was accepted without the adapter")
	}
}

func TestAdapterMakesV2ReportValid(t *testing.T) {
	adapted, ok := AdaptVolcanicV2([]byte(v2Report), received)
	if !ok {
		t.Fatal("v2 report not recognised")
	}
	report, rej := ClassifyVolcanicReport(adapted)
	if rej != nil {
		t.Fatalf("adapted report rejected: %s (%s)", rej.Reason, adapted)
	}
	if report.VolcanoID != "GNG-001" || report.AshColumnHeightM != 1500 || report.ReportedAt != "2026-11-22T03:04:05Z" {
		t.Fatalf("report = %+v", report)
	}
	var fields map[string]any
	_ = json.Unmarshal(adapted, &fields)
	if fields["reported_at_source"] != "received_at" {
		t.Fatalf("substituted reported_at is not marked: %s", adapted)
	}
}

func TestAdapterLeavesV1AndKeepsRealTimestamp(t *testing.T) {
	v1 := `{"report_id":"PVMBG-2","volcano_id":"GNG-002","alert_level":"WASPADA","reported_at":"2026-11-20T00:00:00Z"}`
	if out, ok := AdaptVolcanicV2([]byte(v1), received); ok || string(out) != v1 {
		t.Fatalf("v1 report changed: ok=%v out=%s", ok, out)
	}
	// A v2 report that still carries reported_at keeps it.
	withTime := `{"report_id":"PVMBG-3","volcano_code":"GNG-003","alert_level":"AWAS","reported_at":"2026-11-21T10:00:00Z"}`
	out, ok := AdaptVolcanicV2([]byte(withTime), received)
	report, rej := ClassifyVolcanicReport(out)
	if !ok || rej != nil || report.ReportedAt != "2026-11-21T10:00:00Z" {
		t.Fatalf("ok=%v rej=%v report=%+v", ok, rej, report)
	}
}

func TestAdapterKeepsRejectingNonNumericHeight(t *testing.T) {
	bad := `{"report_id":"PVMBG-4","volcano_code":"GNG-004","alert_level":"SIAGA","ash_column_height_m":"tall"}`
	out, _ := AdaptVolcanicV2([]byte(bad), received)
	if _, rej := ClassifyVolcanicReport(out); rej == nil || rej.Reason != "type_mismatch:ash_column_height_m" {
		t.Fatalf("non-numeric height accepted or wrong reason: %+v", rej)
	}
}

func TestAdapterIgnoresMalformedInput(t *testing.T) {
	for _, in := range []string{`{"report_id":`, `[]`, `null`, `"x"`} {
		if out, ok := AdaptVolcanicV2([]byte(in), received); ok || string(out) != in {
			t.Fatalf("malformed %q changed: ok=%v out=%s", in, ok, out)
		}
	}
}

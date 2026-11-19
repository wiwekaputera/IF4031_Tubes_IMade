package model

import "testing"

func TestIncompatibleViewBreaksContract(t *testing.T) {
	out := IncompatibleView([]VolcanicReport{{
		ReportID: "R-1", VolcanoID: "GNG-001", AlertLevel: AlertSiaga,
		EruptionCount24h: 2, AshColumnHeightM: 1200.5, ReportedAt: "2026-11-17T10:00:00Z",
	}})
	if len(out) != 1 {
		t.Fatalf("len = %d, want 1", len(out))
	}
	row := out[0]
	if _, ok := row["volcano_id"]; ok {
		t.Error("volcano_id should be renamed away")
	}
	if row["volcano_code"] != "GNG-001" {
		t.Errorf("volcano_code = %v", row["volcano_code"])
	}
	if s, ok := row["ash_column_height_m"].(string); !ok || s != "1200.5" {
		t.Errorf("ash_column_height_m = %#v, want string \"1200.5\"", row["ash_column_height_m"])
	}
	if _, ok := row["reported_at"]; ok {
		t.Error("reported_at should be removed")
	}
}

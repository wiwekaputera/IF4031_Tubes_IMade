package model

import "fmt"

// IncompatibleView renders reports in the incompatible schema version used to test
// M2 P3 (T3.1): volcano_id is renamed to volcano_code, ash_column_height_m becomes a
// string, and reported_at is removed. The aggregator must reject these messages.
func IncompatibleView(reports []VolcanicReport) []map[string]any {
	out := make([]map[string]any, 0, len(reports))
	for _, r := range reports {
		out = append(out, map[string]any{
			"report_id":           r.ReportID,
			"volcano_code":        r.VolcanoID,
			"alert_level":         r.AlertLevel,
			"eruption_count_24h":  r.EruptionCount24h,
			"ash_column_height_m": fmt.Sprintf("%.1f", r.AshColumnHeightM),
		})
	}
	return out
}

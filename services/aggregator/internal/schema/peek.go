package schema

import (
	"encoding/json"
	"time"
)

// Peek extracts the report_id and reported_at from a message that failed
// classification, on a best-effort basis, so a rejected message can still be
// recorded against its source reference and advance the poll watermark.
// Missing or mistyped values come back as zero values.
func Peek(raw []byte) (sourceRefID string, reportedAt time.Time) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return "", time.Time{}
	}
	var ref string
	if v, ok := fields["report_id"]; ok {
		_ = json.Unmarshal(v, &ref)
	}
	var ts string
	if v, ok := fields["reported_at"]; ok {
		_ = json.Unmarshal(v, &ts)
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			reportedAt = t
		}
	}
	return ref, reportedAt
}

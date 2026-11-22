package schema

import (
	"encoding/json"
	"strconv"
	"time"
)

// AdaptVolcanicV2 is the normalizer update for PVMBG's incompatible schema
// version, turned on with PVMBG_SCHEMA_V2_ADAPTER=true. Off, v2 reports are
// rejected into the DLQ as before; on, they are rewritten into the v1 shape
// and accepted, so parked v2 reports can be replayed into the canonical store.
//
// The v2 differences and how each is handled:
//   - volcano_id renamed to volcano_code: mapped back.
//   - ash_column_height_m sent as a string: parsed as a number. A value that is
//     not a number is left as it is, so the type check still rejects it.
//   - reported_at removed: the time the aggregator received the report is used
//     instead, and reported_at_source="received_at" is added so the substitution
//     stays visible in attributes.
//
// ok is false, and raw is returned unchanged, when the report is not v2 (it
// already has volcano_id, has no volcano_code, or is not a JSON object).
func AdaptVolcanicV2(raw []byte, receivedAt time.Time) (adapted []byte, ok bool) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return raw, false
	}
	code, hasCode := fields["volcano_code"]
	if _, hasID := fields["volcano_id"]; hasID || !hasCode {
		return raw, false
	}

	fields["volcano_id"] = code
	delete(fields, "volcano_code")

	if v, ok := fields["ash_column_height_m"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				fields["ash_column_height_m"] = json.RawMessage(strconv.FormatFloat(f, 'f', -1, 64))
			}
		}
	}

	if _, ok := fields["reported_at"]; !ok {
		ts, _ := json.Marshal(receivedAt.UTC().Format(time.RFC3339))
		fields["reported_at"] = ts
		fields["reported_at_source"] = json.RawMessage(`"received_at"`)
	}

	out, err := json.Marshal(fields)
	if err != nil {
		return raw, false
	}
	return out, true
}

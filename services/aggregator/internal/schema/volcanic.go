// Package schema decides whether an incoming agency message is safe to accept.
//
// Policy (M2 T3.2):
//   - Accepted: unknown extra fields (stored as attributes), missing optional fields.
//   - Rejected (permanent, parked in the DLQ): missing required fields (renames
//     look like this), type changes such as number -> string, malformed JSON,
//     invalid UTF-8, oversized bodies, and excessively nested bodies (T3.4).
package schema

import (
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
)

const (
	// MaxBodyBytes caps a single agency message (T3.4).
	MaxBodyBytes = 1 << 20
	// MaxDepth caps JSON nesting to protect the decoder (T3.4).
	MaxDepth = 32
)

// Rejection explains why a message was refused.
// Retryable is always false for schema and format problems; transient errors
// (timeouts, DB down) are handled by the retry path, not here.
type Rejection struct {
	Reason    string
	Retryable bool
}

func (r *Rejection) Error() string { return r.Reason }

var requiredVolcanicFields = []string{"report_id", "volcano_id", "alert_level", "reported_at"}

// ClassifyVolcanicReport validates one raw PVMBG volcanic report.
// It returns the decoded report on success, or a *Rejection describing the failure.
func ClassifyVolcanicReport(raw []byte) (agency.VolcanicReport, *Rejection) {
	if len(raw) > MaxBodyBytes {
		return agency.VolcanicReport{}, &Rejection{Reason: "body_too_large"}
	}
	if !utf8.Valid(raw) {
		return agency.VolcanicReport{}, &Rejection{Reason: "invalid_utf8"}
	}
	if maxNesting(raw) > MaxDepth {
		return agency.VolcanicReport{}, &Rejection{Reason: "nesting_too_deep"}
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return agency.VolcanicReport{}, &Rejection{Reason: "malformed_json"}
	}
	for _, name := range requiredVolcanicFields {
		if v, ok := fields[name]; !ok || string(v) == "null" {
			return agency.VolcanicReport{}, &Rejection{Reason: "missing_required:" + name}
		}
	}

	var report agency.VolcanicReport
	if err := json.Unmarshal(raw, &report); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) && typeErr.Field != "" {
			return agency.VolcanicReport{}, &Rejection{Reason: "type_mismatch:" + typeErr.Field}
		}
		return agency.VolcanicReport{}, &Rejection{Reason: "malformed_json"}
	}
	return report, nil
}

// maxNesting returns the deepest {}/[] nesting in raw, ignoring brackets inside strings.
func maxNesting(raw []byte) int {
	depth, max := 0, 0
	inString, escaped := false, false
	for _, c := range raw {
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > max {
				max = depth
			}
		case '}', ']':
			depth--
		}
	}
	return max
}

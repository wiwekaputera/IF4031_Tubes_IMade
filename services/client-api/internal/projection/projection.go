package projection

import (
	"errors"
	"fmt"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

var (
	ErrInsufficientScope = errors.New("insufficient scope for requested view")

	ErrUnknownScope = errors.New("unknown client scope")

	ErrInvalidView = errors.New("invalid view parameter")
)

// SummaryFields is the allowlist of fields served to media scope.
var SummaryFields = []string{
	"hazard_id",
	"source",
	"hazard_type",
	"severity",
	"area_name",
	"occurred_at",
	"ingested_at",
}

type ViewType string

const (
	ViewDefault ViewType = ""
	ViewSummary ViewType = "summary"
	ViewFull    ViewType = "full"
)

// ValidateViewScope checks if the client scope is allowed to access the requested view.
func ValidateViewScope(scope auth.Scope, viewStr string) (ViewType, error) {
	switch scope {
	case auth.ScopeMedia, auth.ScopeFieldTeam, auth.ScopeInternalOps:
		// authorized scopes
	default:
		return "", ErrUnknownScope
	}

	view := ViewType(viewStr)
	switch view {
	case ViewDefault, ViewSummary, ViewFull:
		// authorized views
	default:
		return "", fmt.Errorf("%w: must be 'summary' or 'full'", ErrInvalidView)
	}

	if scope == auth.ScopeMedia && view == ViewFull {
		return "", ErrInsufficientScope
	}

	return view, nil
}

// ProjectEvents applies server-side field projection on raw hazard events.
func ProjectEvents(events []map[string]interface{}, scope auth.Scope, view ViewType) []map[string]interface{} {
	shouldProjectSummary := false
	if scope == auth.ScopeMedia || view == ViewSummary {
		shouldProjectSummary = true
	}

	if !shouldProjectSummary {
		return events
	}

	projected := make([]map[string]interface{}, len(events))
	for i, evt := range events {
		summary := make(map[string]interface{}, len(SummaryFields))
		for _, field := range SummaryFields {
			if val, ok := evt[field]; ok {
				summary[field] = val
			}
		}
		projected[i] = summary
	}

	return projected
}

//go:build integration

package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// TestIntegration_ScatterPagination verifies that following next_cursor on
// GET /internal/events?limit=50 visits every non-deleted row in canonical_events
// in strict (occurred_at DESC, hazard_id DESC) order.
func TestIntegration_ScatterPagination(t *testing.T) {
	aggURL := os.Getenv("AGGREGATOR_URL")
	if aggURL == "" {
		aggURL = "http://localhost:8090"
	}
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgresql://aggregator@localhost:26257/canonical?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect DB: %v", err)
	}
	defer pool.Close()

	var expectedCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM canonical_events WHERE deleted_at IS NULL`).Scan(&expectedCount); err != nil {
		t.Fatalf("count canonical_events: %v", err)
	}

	seen := make(map[string]bool)
	var prevTime time.Time
	var prevID string
	var cursor string

	client := &http.Client{Timeout: 5 * time.Second}
	for page := 1; ; page++ {
		u := aggURL + "/internal/events?limit=50"
		if cursor != "" {
			u += "&cursor=" + url.QueryEscape(cursor)
		}
		resp, err := client.Get(u)
		if err != nil {
			t.Fatalf("GET %s: %v", u, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			t.Fatalf("GET %s returned status %d", u, resp.StatusCode)
		}

		var body hazard.EventsResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			t.Fatalf("decode page %d: %v", page, err)
		}
		resp.Body.Close()

		for _, ev := range body.Data {
			if seen[ev.HazardID] {
				t.Fatalf("duplicate hazard_id %s on page %d", ev.HazardID, page)
			}
			seen[ev.HazardID] = true

			occ, err := time.Parse(time.RFC3339, ev.OccurredAt)
			if err != nil {
				t.Fatalf("parse occurred_at %q: %v", ev.OccurredAt, err)
			}
			if !prevTime.IsZero() {
				if occ.After(prevTime) || (occ.Equal(prevTime) && ev.HazardID > prevID) {
					t.Fatalf("order violation at page %d: (%s, %s) came after (%s, %s)",
						page, ev.OccurredAt, ev.HazardID, prevTime.Format(time.RFC3339), prevID)
				}
			}
			prevTime = occ
			prevID = ev.HazardID
		}

		if body.Meta.NextCursor == nil {
			break
		}
		cursor = *body.Meta.NextCursor
	}

	if len(seen) != expectedCount {
		t.Fatalf("visited %d unique events, DB has %d", len(seen), expectedCount)
	}
}

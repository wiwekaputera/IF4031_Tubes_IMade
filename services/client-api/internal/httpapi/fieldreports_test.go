package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/fieldreports"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/fieldreport"
)

// fakePublisher behaves like JetStream: same message ID -> stored once.
type fakePublisher struct {
	mu     sync.Mutex
	stored map[string]fieldreport.Report
	err    error
	delay  time.Duration
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{stored: map[string]fieldreport.Report{}}
}

func (f *fakePublisher) Publish(ctx context.Context, r fieldreport.Report) (bool, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return false, fmt.Errorf("%w: %v", fieldreports.ErrUnavailable, ctx.Err())
		}
	}
	if f.err != nil {
		return false, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.stored[r.MessageID()]; ok {
		return true, nil
	}
	f.stored[r.MessageID()] = r
	return false, nil
}

var fixedNow = time.Date(2026, 11, 20, 3, 0, 0, 0, time.UTC)

func reportBody(extra string) string {
	return `{"category":"DAMAGE","severity":"HIGH","area_name":"Palu","latitude":-0.9,"longitude":119.87,` +
		`"description":"Jembatan putus","reported_at":"2026-11-20T02:55:00Z"` + extra + `}`
}

func postReport(t *testing.T, pub fieldreports.Publisher, scope contractsauth.Scope, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	h := FieldReportsHandler(FieldReportsOptions{
		Publisher: pub, PublishTimeout: 50 * time.Millisecond,
		Logger: zerolog.Nop(), Now: func() time.Time { return fixedNow },
	})
	req := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	claims := &auth.AccessTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "client-field-team"},
		Scope:            scope,
	}
	req = req.WithContext(auth.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

func TestFieldReportAccepted(t *testing.T) {
	pub := newFakePublisher()
	rec := postReport(t, pub, contractsauth.ScopeFieldTeam, "rpt-1", reportBody(""))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	var resp FieldReportResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.ReportID != "rpt-1" || resp.Duplicate {
		t.Fatalf("unexpected response %+v", resp)
	}
	stored := pub.stored["client-field-team/rpt-1"]
	if stored.AreaName != "Palu" || stored.ClientID != "client-field-team" || !stored.AcceptedAt.Equal(fixedNow) {
		t.Fatalf("unexpected stored report %+v", stored)
	}
}

func TestFieldReportRetryIsStoredOnce(t *testing.T) {
	pub := newFakePublisher()
	first := postReport(t, pub, contractsauth.ScopeFieldTeam, "rpt-1", reportBody(""))
	second := postReport(t, pub, contractsauth.ScopeFieldTeam, "rpt-1", reportBody(""))
	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("both attempts must be 202, got %d and %d", first.Code, second.Code)
	}
	if !strings.Contains(second.Body.String(), `"duplicate":true`) || len(pub.stored) != 1 {
		t.Fatalf("retry must be reported as duplicate and stored once: %s, stored=%d", second.Body, len(pub.stored))
	}
}

func TestFieldReportIDFromBody(t *testing.T) {
	pub := newFakePublisher()
	rec := postReport(t, pub, contractsauth.ScopeFieldTeam, "", reportBody(`,"report_id":"from-body"`))
	if rec.Code != http.StatusAccepted || pub.stored["client-field-team/from-body"].ReportID != "from-body" {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
}

func TestFieldReportRejections(t *testing.T) {
	cases := []struct {
		name   string
		scope  contractsauth.Scope
		key    string
		body   string
		status int
	}{
		{"media scope", contractsauth.ScopeMedia, "k1", reportBody(""), http.StatusForbidden},
		{"no key", contractsauth.ScopeFieldTeam, "", reportBody(""), http.StatusBadRequest},
		{"key mismatch", contractsauth.ScopeFieldTeam, "k1", reportBody(`,"report_id":"k2"`), http.StatusBadRequest},
		{"bad key", contractsauth.ScopeFieldTeam, "has space", reportBody(""), http.StatusBadRequest},
		{"truncated json", contractsauth.ScopeFieldTeam, "k1", `{"category":"DAMAGE"`, http.StatusBadRequest},
		{"unknown field", contractsauth.ScopeFieldTeam, "k1", reportBody(`,"admin":true`), http.StatusBadRequest},
		{"two objects", contractsauth.ScopeFieldTeam, "k1", reportBody("") + "{}", http.StatusBadRequest},
		{"invalid fields", contractsauth.ScopeFieldTeam, "k1", `{"category":"X","reported_at":"2026-11-20T02:55:00Z"}`, http.StatusUnprocessableEntity},
		{"too large", contractsauth.ScopeFieldTeam, "k1", reportBody(`,"description":"` + strings.Repeat("x", fieldreport.MaxBodyBytes) + `"`), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := newFakePublisher()
			rec := postReport(t, pub, tc.scope, tc.key, tc.body)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tc.status, rec.Body)
			}
			if len(pub.stored) != 0 {
				t.Fatal("a rejected report must not be stored")
			}
		})
	}
}

func TestFieldReportBrokerDownIs503NotAccepted(t *testing.T) {
	pub := newFakePublisher()
	pub.err = fmt.Errorf("%w: not connected", fieldreports.ErrUnavailable)
	rec := postReport(t, pub, contractsauth.ScopeFieldTeam, "rpt-1", reportBody(""))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status = %d, Retry-After=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
}

func TestFieldReportSlowBrokerTimesOutQuickly(t *testing.T) {
	pub := newFakePublisher()
	pub.delay = time.Second
	start := time.Now()
	rec := postReport(t, pub, contractsauth.ScopeFieldTeam, "rpt-1", reportBody(""))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("handler waited %v, the publish timeout is 50ms", elapsed)
	}
}

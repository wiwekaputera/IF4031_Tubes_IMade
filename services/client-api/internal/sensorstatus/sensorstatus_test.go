package sensorstatus

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/sensorpantai"
)

func statusJSON(t *testing.T, st sensorpantai.Status) []byte {
	t.Helper()
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMetaFromPublishedStatus(t *testing.T) {
	now := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
	stale := now.Add(-time.Minute)
	synced := now.Add(-2 * time.Minute)
	raw := statusJSON(t, sensorpantai.Status{Status: "OUTAGE", LastSyncedAt: &synced, StaleSince: &stale, UpdatedAt: now})
	r := New(func() ([]byte, error) { return raw, nil }, time.Second, 30*time.Second)
	r.now = func() time.Time { return now }
	m := r.Meta()
	if m.Status != "OUTAGE" || m.LastSyncedAt != "2026-11-20T00:58:00Z" || m.StaleSince == nil || *m.StaleSince != "2026-11-20T00:59:00Z" {
		t.Fatalf("meta %+v", m)
	}
}

func TestOldStatusMeansPollerDown(t *testing.T) {
	now := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
	raw := statusJSON(t, sensorpantai.Status{Status: "UP", UpdatedAt: now.Add(-5 * time.Minute)})
	r := New(func() ([]byte, error) { return raw, nil }, time.Second, 30*time.Second)
	r.now = func() time.Time { return now }
	if m := r.Meta(); m.Status != "DOWN" || m.StaleSince == nil {
		t.Fatalf("meta %+v", m)
	}
}

func TestMissingStatusIsDownAndCached(t *testing.T) {
	calls := 0
	r := New(func() ([]byte, error) { calls++; return nil, errors.New("no bucket") }, time.Second, 30*time.Second)
	now := time.Now()
	r.now = func() time.Time { return now }
	for i := 0; i < 100; i++ {
		if r.Meta().Status != "DOWN" {
			t.Fatal("expected DOWN")
		}
	}
	if calls != 1 {
		t.Fatalf("KV read %d times within one refresh interval", calls)
	}
}

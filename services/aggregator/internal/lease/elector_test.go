package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock {
	return &fakeClock{now: t}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Since(t time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Sub(t)
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fakeLeaseStore struct {
	mu           sync.Mutex
	dbNow        time.Time
	holder       string
	fencingToken int64
	expiresAt    time.Time
	err          error
}

func newFakeLeaseStore(start time.Time) *fakeLeaseStore {
	return &fakeLeaseStore{
		dbNow:     start,
		expiresAt: time.Unix(0, 0),
	}
}

func (s *fakeLeaseStore) AdvanceDBTime(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dbNow = s.dbNow.Add(d)
}

func (s *fakeLeaseStore) AcquireOrRenewLease(_ context.Context, _, holder string, ttl time.Duration) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, false, s.err
	}
	if s.expiresAt.After(s.dbNow) && s.holder != holder {
		return 0, false, nil
	}
	if s.holder == holder && s.expiresAt.After(s.dbNow) {
		// Renewal keeps token
		s.expiresAt = s.dbNow.Add(ttl)
		return s.fencingToken, true, nil
	}
	// Takeover or expired re-acquire increments token
	s.fencingToken++
	s.holder = holder
	s.expiresAt = s.dbNow.Add(ttl)
	return s.fencingToken, true, nil
}

func (s *fakeLeaseStore) ReleaseLease(_ context.Context, _, holder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holder == holder {
		s.expiresAt = s.dbNow
	}
	return nil
}

func TestElector_SingleLeaderAmongThreeAndRenewPreservesToken(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	db := newFakeLeaseStore(start)
	clk := newFakeClock(start)

	e1 := New(db, "ingest-poller", "agg-1", 15*time.Second, 5*time.Second, zerolog.Nop()).WithClock(clk)
	e2 := New(db, "ingest-poller", "agg-2", 15*time.Second, 5*time.Second, zerolog.Nop()).WithClock(clk)
	e3 := New(db, "ingest-poller", "agg-3", 15*time.Second, 5*time.Second, zerolog.Nop()).WithClock(clk)

	ctx := context.Background()
	tok1, ok1 := e1.RenewNow(ctx)
	tok2, ok2 := e2.RenewNow(ctx)
	tok3, ok3 := e3.RenewNow(ctx)

	if !ok1 || tok1 != 1 {
		t.Fatalf("expected agg-1 to acquire token 1, got tok=%d ok=%v", tok1, ok1)
	}
	if ok2 || tok2 != 0 || ok3 || tok3 != 0 {
		t.Fatalf("expected agg-2 and agg-3 to not be leader, got (%d,%v) (%d,%v)", tok2, ok2, tok3, ok3)
	}

	// Renew by agg-1 keeps token 1
	db.AdvanceDBTime(5 * time.Second)
	clk.Advance(5 * time.Second)

	tok1Renew, ok1Renew := e1.RenewNow(ctx)
	if !ok1Renew || tok1Renew != 1 {
		t.Fatalf("expected renew to preserve token 1, got tok=%d ok=%v", tok1Renew, ok1Renew)
	}
}

func TestElector_TakeoverAfterExpiryIncrementsToken(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	db := newFakeLeaseStore(start)
	clk1 := newFakeClock(start)
	clk2 := newFakeClock(start)

	var demoted1, promoted2 int
	e1 := New(db, "ingest-poller", "agg-1", 15*time.Second, 5*time.Second, zerolog.Nop(), Hooks{
		OnDemote: func() { demoted1++ },
	}).WithClock(clk1)
	e2 := New(db, "ingest-poller", "agg-2", 15*time.Second, 5*time.Second, zerolog.Nop(), Hooks{
		OnPromote: func(tok int64) { promoted2++ },
	}).WithClock(clk2)

	ctx := context.Background()
	if tok, ok := e1.RenewNow(ctx); !ok || tok != 1 {
		t.Fatalf("expected agg-1 to acquire token 1, got %d %v", tok, ok)
	}

	// agg-1 stops renewing; DB time advances past 15s TTL
	db.AdvanceDBTime(16 * time.Second)
	clk2.Advance(16 * time.Second)

	tok2, ok2 := e2.RenewNow(ctx)
	if !ok2 || tok2 != 2 {
		t.Fatalf("expected agg-2 to take over with token 2, got tok=%d ok=%v", tok2, ok2)
	}
	if promoted2 != 1 {
		t.Fatalf("expected OnPromote called once for agg-2, got %d", promoted2)
	}

	// Frozen agg-1 wakes up and tries to renew -> fails and demotes
	if tok, ok := e1.RenewNow(ctx); ok || tok != 0 {
		t.Fatalf("expected woken agg-1 renew to fail, got tok=%d ok=%v", tok, ok)
	}
	if demoted1 != 1 {
		t.Fatalf("expected OnDemote called once for agg-1, got %d", demoted1)
	}
}

func TestElector_ExpiredHolderReacquiringGetsNewToken(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	db := newFakeLeaseStore(start)
	clk := newFakeClock(start)

	var promoted []int64
	var demoted int
	e1 := New(db, "ingest-poller", "agg-1", 15*time.Second, 5*time.Second, zerolog.Nop(), Hooks{
		OnPromote: func(tok int64) { promoted = append(promoted, tok) },
		OnDemote:  func() { demoted++ },
	}).WithClock(clk)

	ctx := context.Background()
	tok1, ok := e1.RenewNow(ctx)
	if !ok || tok1 != 1 {
		t.Fatalf("expected token 1, got %d %v", tok1, ok)
	}

	// Lease expires in DB while agg-1 was frozen, then agg-1 re-acquires before anyone else
	db.AdvanceDBTime(20 * time.Second)
	clk.Advance(20 * time.Second)

	tok2, ok := e1.RenewNow(ctx)
	if !ok || tok2 != 2 {
		t.Fatalf("expected re-acquire after expiry to get token 2, got %d %v", tok2, ok)
	}
	if demoted != 1 || len(promoted) != 2 || promoted[0] != 1 || promoted[1] != 2 {
		t.Fatalf("unexpected hooks: demoted=%d promoted=%v", demoted, promoted)
	}
}

func TestElector_ReleaseAllowsImmediateTakeover(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	db := newFakeLeaseStore(start)
	clk := newFakeClock(start)

	e1 := New(db, "ingest-poller", "agg-1", 15*time.Second, 5*time.Second, zerolog.Nop()).WithClock(clk)
	e2 := New(db, "ingest-poller", "agg-2", 15*time.Second, 5*time.Second, zerolog.Nop()).WithClock(clk)

	ctx := context.Background()
	e1.RenewNow(ctx)
	if err := e1.Release(ctx); err != nil {
		t.Fatalf("unexpected release error: %v", err)
	}
	if _, ok := e1.IsLeader(); ok {
		t.Fatal("expected agg-1 to not be leader after Release")
	}

	// Even without advancing DB time, agg-2 can take over immediately
	tok2, ok2 := e2.RenewNow(ctx)
	if !ok2 || tok2 != 2 {
		t.Fatalf("expected agg-2 to take over with token 2 after release, got %d %v", tok2, ok2)
	}
}

func TestElector_DBErrorPreventsRenewNow(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	db := newFakeLeaseStore(start)
	clk := newFakeClock(start)

	e1 := New(db, "ingest-poller", "agg-1", 15*time.Second, 5*time.Second, zerolog.Nop()).WithClock(clk)
	ctx := context.Background()
	if _, ok := e1.RenewNow(ctx); !ok {
		t.Fatal("expected initial acquire to succeed")
	}

	db.mu.Lock()
	db.err = errors.New("connection refused")
	db.mu.Unlock()

	if _, ok := e1.RenewNow(ctx); ok {
		t.Fatal("expected RenewNow to return false on DB error")
	}
}

package lease

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// LeaseStore abstracts the database operations required for leader election.
type LeaseStore interface {
	AcquireOrRenewLease(ctx context.Context, leaseName, holder string, ttl time.Duration) (token int64, acquired bool, err error)
	ReleaseLease(ctx context.Context, leaseName, holder string) error
}

// Clock abstracts time measurement for monotonic local lease expiry checks.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
}

type realClock struct{}

func (realClock) Now() time.Time                  { return time.Now() }
func (realClock) Since(t time.Time) time.Duration { return time.Since(t) }

// Hooks receives notifications when leadership is acquired or lost.
type Hooks struct {
	OnPromote func(token int64)
	OnDemote  func()
}

// Elector manages single-active leader election using a database-backed lease.
type Elector struct {
	store         LeaseStore
	leaseName     string
	holder        string
	ttl           time.Duration
	renewInterval time.Duration
	safetyMargin  time.Duration
	clock         Clock
	logger        zerolog.Logger
	hooks         Hooks

	renewMu sync.Mutex

	mu        sync.RWMutex
	isLeader  bool
	token     int64
	renewedAt time.Time
}

// New creates an Elector for leaseName and holder.
func New(store LeaseStore, leaseName, holder string, ttl, renewInterval time.Duration, logger zerolog.Logger, hooks ...Hooks) *Elector {
	margin := 2 * time.Second
	if fifth := ttl / 5; fifth < margin {
		margin = fifth
	}
	var h Hooks
	if len(hooks) > 0 {
		h = hooks[0]
	}
	return &Elector{
		store:         store,
		leaseName:     leaseName,
		holder:        holder,
		ttl:           ttl,
		renewInterval: renewInterval,
		safetyMargin:  margin,
		clock:         realClock{},
		logger:        logger.With().Str("component", "lease-elector").Str("instance_id", holder).Logger(),
		hooks:         h,
	}
}

// WithClock overrides the clock (intended for deterministic unit tests).
func (e *Elector) WithClock(c Clock) *Elector {
	e.clock = c
	return e
}

// Run periodically attempts to acquire or renew the leader lease until ctx is cancelled.
func (e *Elector) Run(ctx context.Context) {
	e.RenewNow(ctx)

	ticker := time.NewTicker(e.renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.RenewNow(ctx)
		}
	}
}

// RenewNow synchronously attempts to acquire or renew the lease in the store.
// It returns (fencingToken, true) only if the DB confirmed leadership.
func (e *Elector) RenewNow(ctx context.Context) (int64, bool) {
	e.renewMu.Lock()
	defer e.renewMu.Unlock()

	t0 := e.clock.Now()
	tok, acquired, err := e.store.AcquireOrRenewLease(ctx, e.leaseName, e.holder, e.ttl)

	var (
		callPromote bool
		promoteTok  int64
		callDemote  bool
	)

	e.mu.Lock()
	wasLeader := e.isLeader
	prevToken := e.token

	if err == nil && acquired {
		e.isLeader = true
		e.token = tok
		e.renewedAt = t0

		if !wasLeader {
			e.logger.Info().Int64("fencing_token", tok).Msg("leader_acquired")
			callPromote = true
			promoteTok = tok
		} else if tok != prevToken {
			e.logger.Warn().Int64("fencing_token", prevToken).Msg("leader_lost")
			e.logger.Info().Int64("fencing_token", tok).Msg("leader_acquired")
			callDemote = true
			callPromote = true
			promoteTok = tok
		} else {
			e.logger.Debug().Int64("fencing_token", tok).Msg("leader_renewed")
		}
		e.mu.Unlock()

		if callDemote && e.hooks.OnDemote != nil {
			e.hooks.OnDemote()
		}
		if callPromote && e.hooks.OnPromote != nil {
			e.hooks.OnPromote(promoteTok)
		}
		return tok, true
	}

	if err != nil {
		e.logger.Warn().Err(err).Msg("lease acquire/renew failed")
	}

	if wasLeader && (!acquired || e.clock.Since(e.renewedAt) >= e.ttl-e.safetyMargin) {
		e.isLeader = false
		e.token = 0
		e.logger.Warn().Int64("fencing_token", prevToken).Msg("leader_lost")
		callDemote = true
	}
	e.mu.Unlock()

	if callDemote && e.hooks.OnDemote != nil {
		e.hooks.OnDemote()
	}
	return 0, false
}

// IsLeader returns the current fencing token and true if this instance holds
// an unexpired local view of the lease.
func (e *Elector) IsLeader() (int64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.isLeader {
		return 0, false
	}
	if e.clock.Since(e.renewedAt) >= e.ttl-e.safetyMargin {
		return 0, false
	}
	return e.token, true
}

// Demote clears local leadership immediately (e.g. after ErrFenced on a write).
func (e *Elector) Demote() {
	e.mu.Lock()
	if !e.isLeader {
		e.mu.Unlock()
		return
	}
	prevToken := e.token
	e.isLeader = false
	e.token = 0
	e.mu.Unlock()

	e.logger.Warn().Int64("fencing_token", prevToken).Msg("leader_lost")
	if e.hooks.OnDemote != nil {
		e.hooks.OnDemote()
	}
}

// Release voluntarily releases the lease on graceful shutdown.
func (e *Elector) Release(ctx context.Context) error {
	err := e.store.ReleaseLease(ctx, e.leaseName, e.holder)

	e.mu.Lock()
	wasLeader := e.isLeader
	prevToken := e.token
	e.isLeader = false
	e.token = 0
	e.mu.Unlock()

	if wasLeader {
		e.logger.Info().Int64("fencing_token", prevToken).Msg("lease_released")
		if e.hooks.OnDemote != nil {
			e.hooks.OnDemote()
		}
	}
	return err
}

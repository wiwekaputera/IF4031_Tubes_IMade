// Package breaker is the circuit breaker in front of the coastal sensor.
//
//   - closed: requests flow. FailureThreshold consecutive failures open it.
//   - open: no request is sent until the open period ends. Each consecutive
//     opening doubles the period (BaseOpen, 2×, 4× … up to MaxOpen) with ±20%
//     jitter, so a sick sensor sees fewer and fewer requests, and several
//     pollers never retry in lockstep.
//   - half-open: exactly one probe request is allowed. Success closes the
//     breaker and resets the back-off; failure opens it again for longer.
package breaker

import (
	"math/rand"
	"sync"
	"time"
)

// State of the breaker.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Config holds the breaker settings.
type Config struct {
	FailureThreshold int
	BaseOpen         time.Duration
	MaxOpen          time.Duration
}

// Breaker is safe for concurrent use.
type Breaker struct {
	cfg  Config
	now  func() time.Time
	rand func() float64

	mu            sync.Mutex
	state         State
	failures      int
	openings      int
	openUntil     time.Time
	probeInFlight bool
	onChange      func(from, to State)
}

// New returns a closed breaker.
func New(cfg Config) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.BaseOpen <= 0 {
		cfg.BaseOpen = 5 * time.Second
	}
	if cfg.MaxOpen < cfg.BaseOpen {
		cfg.MaxOpen = cfg.BaseOpen
	}
	return &Breaker{cfg: cfg, now: time.Now, rand: rand.Float64}
}

// OnChange registers a callback for state transitions (called with the lock held
// released; keep it short).
func (b *Breaker) OnChange(fn func(from, to State)) {
	b.mu.Lock()
	b.onChange = fn
	b.mu.Unlock()
}

// Allow reports whether a request may be sent now. In half-open it lets one
// probe through and refuses the rest until that probe reports back.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	var from, to State
	changed := false
	defer func() {
		cb := b.onChange
		b.mu.Unlock()
		if changed && cb != nil {
			cb(from, to)
		}
	}()

	switch b.state {
	case Closed:
		return true
	case Open:
		if b.now().Before(b.openUntil) {
			return false
		}
		from, to, changed = Open, HalfOpen, true
		b.state = HalfOpen
		b.probeInFlight = true
		return true
	default: // HalfOpen
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	}
}

// Success records a successful request.
func (b *Breaker) Success() {
	b.mu.Lock()
	from := b.state
	b.failures = 0
	b.openings = 0
	b.probeInFlight = false
	b.state = Closed
	cb := b.onChange
	b.mu.Unlock()
	if from != Closed && cb != nil {
		cb(from, Closed)
	}
}

// Failure records a failed request.
func (b *Breaker) Failure() {
	b.mu.Lock()
	from := b.state
	b.probeInFlight = false
	b.failures++
	opened := false
	if b.state == HalfOpen || b.failures >= b.cfg.FailureThreshold {
		b.openings++
		b.state = Open
		b.openUntil = b.now().Add(b.openPeriod())
		opened = true
	}
	cb := b.onChange
	b.mu.Unlock()
	if opened && from != Open && cb != nil {
		cb(from, Open)
	}
}

// openPeriod is BaseOpen·2^(openings-1), capped at MaxOpen, with ±20% jitter.
func (b *Breaker) openPeriod() time.Duration {
	d := b.cfg.BaseOpen
	for i := 1; i < b.openings && d < b.cfg.MaxOpen; i++ {
		d *= 2
	}
	if d > b.cfg.MaxOpen {
		d = b.cfg.MaxOpen
	}
	jitter := 0.8 + 0.4*b.rand()
	return time.Duration(float64(d) * jitter)
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// RetryIn is how long until the breaker lets a request through (0 if now).
func (b *Breaker) RetryIn() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != Open {
		return 0
	}
	if d := b.openUntil.Sub(b.now()); d > 0 {
		return d
	}
	return 0
}

// Backoff is the delay before retrying after `failures` consecutive failures
// while the breaker is still closed: base·2^(failures-1), capped, with full
// jitter in [50%, 100%] of that value.
func Backoff(failures int, base, max time.Duration, rnd func() float64) time.Duration {
	if failures <= 0 {
		return 0
	}
	d := base
	for i := 1; i < failures && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return time.Duration(float64(d) * (0.5 + 0.5*rnd()))
}

// SetClock replaces the time source and the jitter source (tests, simulations).
func (b *Breaker) SetClock(now func() time.Time, rnd func() float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now != nil {
		b.now = now
	}
	if rnd != nil {
		b.rand = rnd
	}
}

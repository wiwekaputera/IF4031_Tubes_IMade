package circuitbreaker

import (
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

type Breaker struct {
	mu              sync.Mutex
	state           State
	failureCount    int
	threshold       int
	probeInterval   time.Duration
	lastFailureTime time.Time
}

func New(threshold int, probeInterval time.Duration) *Breaker {
	return &Breaker{
		state:         StateClosed,
		threshold:     threshold,
		probeInterval: probeInterval,
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) IsOpen() bool {
	return b.State() == StateOpen
}

func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(b.lastFailureTime) >= b.probeInterval {
			b.state = StateHalfOpen
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return true
	}
}

func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failureCount = 0
	if b.state == StateHalfOpen {
		b.state = StateClosed
	}
}

func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failureCount++
	b.lastFailureTime = time.Now()

	if b.state == StateHalfOpen {
		b.state = StateOpen
		return
	}

	if b.failureCount >= b.threshold {
		b.state = StateOpen
	}
}

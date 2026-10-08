package ratelimit

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
)

const DefaultIdleTimeout = 10 * time.Minute

type Options struct {
	RPS         float64
	Burst       int
	IdleTimeout time.Duration
	SkipPaths   []string
}

type clientEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// ClientLimiter manages token bucket rate limiters per client identity (sub).
type ClientLimiter struct {
	rps         rate.Limit
	burst       int
	idleTimeout time.Duration
	skipPaths   map[string]bool

	mu      sync.Mutex
	clients map[string]*clientEntry
}

// New creates a new ClientLimiter with the specified options.
func New(opts Options) *ClientLimiter {
	idle := opts.IdleTimeout
	if idle <= 0 {
		idle = DefaultIdleTimeout
	}

	skip := make(map[string]bool, len(opts.SkipPaths))
	for _, p := range opts.SkipPaths {
		skip[p] = true
	}

	return &ClientLimiter{
		rps:         rate.Limit(opts.RPS),
		burst:       opts.Burst,
		idleTimeout: idle,
		skipPaths:   skip,
		clients:     make(map[string]*clientEntry),
	}
}

// Allow checks if the given client is allowed to proceed according to its token bucket.
func (cl *ClientLimiter) Allow(clientID string) (bool, int) {
	cl.mu.Lock()
	entry, exists := cl.clients[clientID]
	now := time.Now()
	if !exists {
		entry = &clientEntry{
			limiter: rate.NewLimiter(cl.rps, cl.burst),
		}
		cl.clients[clientID] = entry
	}
	entry.lastSeen = now
	lim := entry.limiter
	cl.mu.Unlock()

	res := lim.Reserve()
	if !res.OK() {
		return false, 1
	}

	delay := res.Delay()
	if delay == 0 {
		return true, 0
	}

	res.Cancel()
	retryAfter := int(math.Ceil(delay.Seconds()))
	if retryAfter < 1 {
		retryAfter = 1
	}
	return false, retryAfter
}

// EvictIdle removes limiters for clients that haven't been seen for longer than maxIdle.
func (cl *ClientLimiter) EvictIdle(maxIdle time.Duration) int {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	cutoff := time.Now().Add(-maxIdle)
	evicted := 0
	for id, entry := range cl.clients {
		if entry.lastSeen.Before(cutoff) {
			delete(cl.clients, id)
			evicted++
		}
	}
	return evicted
}

// StartCleanupWorker runs a periodic background task to evict idle limiters.
func (cl *ClientLimiter) StartCleanupWorker(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = cl.idleTimeout / 2
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cl.EvictIdle(cl.idleTimeout)
			}
		}
	}()
}

// ActiveClientsCount returns the number of active clients tracked in memory.
func (cl *ClientLimiter) ActiveClientsCount() int {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return len(cl.clients)
}

// Middleware returns an HTTP middleware enforcing rate limits per client identity (sub).
func (cl *ClientLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cl.skipPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		clientID := auth.ClientIDFromContext(r.Context())
		if clientID == "" {
			clientID = "anonymous"
		}

		allowed, retryAfter := cl.Allow(clientID)
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error":             "too_many_requests",
				"error_description": "rate limit exceeded, please retry later",
				"retry_after":       retryAfter,
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

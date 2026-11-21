// Package loadshed rejects work early when client-api is over capacity, in
// priority order.
//
// Capacity is the number of requests being served at once (in-flight). Field
// team and internal ops requests may use all of it; media requests only the
// first MediaShare of it. When the system slows down, in-flight requests pile
// up, media is refused first with a fast 503 + Retry-After, and the reserved
// share keeps serving the field team. Nothing waits in a queue: a refused
// request costs microseconds.
//
// This differs from the per-client rate limit, which caps each client's
// request rate regardless of how loaded the system is.
package loadshed

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

var (
	inflightGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "client_api_inflight_requests",
		Help: "Requests client-api is serving right now.",
	})
	shed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "client_api_shed_total",
		Help: "Requests refused by load shedding, by client scope.",
	}, []string{"scope"})
	admitted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "client_api_admitted_total",
		Help: "Requests admitted by load shedding, by client scope.",
	}, []string{"scope"})
)

// Config of the shedder.
type Config struct {
	// MaxInflight is the total capacity.
	MaxInflight int
	// MediaShare is the fraction of MaxInflight media requests may occupy.
	MediaShare float64
	// SkipPaths are never shed (health checks).
	SkipPaths []string
}

// Shedder is the middleware state.
type Shedder struct {
	max      int64
	mediaMax int64
	skip     map[string]bool
	inflight atomic.Int64
}

// New builds a shedder.
func New(cfg Config) *Shedder {
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 256
	}
	if cfg.MediaShare <= 0 || cfg.MediaShare > 1 {
		cfg.MediaShare = 0.6
	}
	s := &Shedder{
		max:      int64(cfg.MaxInflight),
		mediaMax: int64(float64(cfg.MaxInflight) * cfg.MediaShare),
		skip:     map[string]bool{},
	}
	if s.mediaMax < 1 {
		s.mediaMax = 1
	}
	for _, p := range cfg.SkipPaths {
		s.skip[p] = true
	}
	return s
}

// limitFor returns the in-flight ceiling for a scope.
func (s *Shedder) limitFor(scope contractsauth.Scope) int64 {
	switch scope {
	case contractsauth.ScopeFieldTeam, contractsauth.ScopeInternalOps:
		return s.max
	default:
		return s.mediaMax
	}
}

// Middleware must run after authentication (it needs the scope).
func (s *Shedder) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.skip[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		scope := auth.ScopeFromContext(r.Context())
		label := string(scope)
		if label == "" {
			label = "unknown"
		}
		n := s.inflight.Add(1)
		if n > s.limitFor(scope) {
			s.inflight.Add(-1)
			shed.WithLabelValues(label).Inc()
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{
				Error:            "overloaded",
				ErrorDescription: "server is at capacity; lower-priority requests are refused, retry later",
			})
			return
		}
		inflightGauge.Set(float64(n))
		admitted.WithLabelValues(label).Inc()
		defer func() { inflightGauge.Set(float64(s.inflight.Add(-1))) }()
		next.ServeHTTP(w, r)
	})
}

// Inflight returns the current number of admitted requests.
func (s *Shedder) Inflight() int64 { return s.inflight.Load() }

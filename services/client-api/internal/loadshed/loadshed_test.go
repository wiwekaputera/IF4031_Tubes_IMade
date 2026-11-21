package loadshed

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

func request(scope contractsauth.Scope, path string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if scope == "" {
		return req
	}
	claims := &auth.AccessTokenClaims{RegisteredClaims: jwt.RegisteredClaims{Subject: string(scope)}, Scope: scope}
	return req.WithContext(auth.ContextWithClaims(req.Context(), claims))
}

// TestFieldTeamServedWhileMediaShed fills client-api with blocked requests and
// checks who still gets in.
func TestFieldTeamServedWhileMediaShed(t *testing.T) {
	s := New(Config{MaxInflight: 10, MediaShare: 0.6, SkipPaths: []string{"/health"}})
	release := make(chan struct{})
	entered := make(chan struct{}, 20)
	h := s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	start := func(scope contractsauth.Scope) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ServeHTTP(httptest.NewRecorder(), request(scope, "/v1/events"))
		}()
		<-entered
	}
	for i := 0; i < 6; i++ { // media fills its 60% share
		start(contractsauth.ScopeMedia)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, request(contractsauth.ScopeMedia, "/v1/events"))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("7th media request: %d", rec.Code)
	}

	for i := 0; i < 4; i++ { // the field team still gets the reserved 40%
		start(contractsauth.ScopeFieldTeam)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, request(contractsauth.ScopeFieldTeam, "/v1/events"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("beyond total capacity the field team is refused too: %d", rec.Code)
	}

	// Health checks are never shed.
	go func() { <-entered }()
	healthRec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(healthRec, request("", "/health")); close(done) }()

	close(release)
	wg.Wait()
	<-done
	if healthRec.Code != http.StatusOK {
		t.Fatalf("health: %d", healthRec.Code)
	}
	if s.Inflight() != 0 {
		t.Fatalf("in-flight counter leaked: %d", s.Inflight())
	}
}

func TestUnknownScopeTreatedAsLowPriority(t *testing.T) {
	s := New(Config{MaxInflight: 10, MediaShare: 0.5})
	if s.limitFor("") != 5 || s.limitFor(contractsauth.ScopeInternalOps) != 10 {
		t.Fatalf("limits %d %d", s.limitFor(""), s.limitFor(contractsauth.ScopeInternalOps))
	}
}

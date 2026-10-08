package ratelimit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

func setContextClient(req *http.Request, sub string) *http.Request {
	claims := &auth.AccessTokenClaims{
		RegisteredClaims: auth.AccessTokenClaims{}.RegisteredClaims,
		Scope:            contractsauth.ScopeMedia,
	}
	claims.Subject = sub
	ctx := auth.ContextWithClaims(req.Context(), claims)
	return req.WithContext(ctx)
}

func TestLimiter_BurstAllowed(t *testing.T) {
	limiter := New(Options{
		RPS:   10,
		Burst: 5,
	})

	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
		req = setContextClient(req, "client-a")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("request %d expected 200, got %d", i+1, rec.Code)
		}
	}
}

func TestLimiter_ExceedBurstReturns429AndRetryAfter(t *testing.T) {
	limiter := New(Options{
		RPS:   1,
		Burst: 2,
	})

	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First 2 requests consume burst
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
		req = setContextClient(req, "client-a")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d expected 200, got %d", i+1, rec.Code)
		}
	}

	// 3rd request exceeds burst -> 429
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req = setContextClient(req, "client-a")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rec.Code)
	}

	retryAfterStr := rec.Header().Get("Retry-After")
	if retryAfterStr == "" {
		t.Fatal("expected Retry-After header to be set")
	}

	retrySec, err := strconv.Atoi(retryAfterStr)
	if err != nil || retrySec < 1 {
		t.Fatalf("expected positive integer Retry-After, got %q (err: %v)", retryAfterStr, err)
	}
}

func TestLimiter_PerClientIsolation(t *testing.T) {
	limiter := New(Options{
		RPS:   1,
		Burst: 1,
	})

	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Client A request 1 -> OK
	reqA1 := setContextClient(httptest.NewRequest(http.MethodGet, "/v1/events", nil), "client-a")
	recA1 := httptest.NewRecorder()
	handler.ServeHTTP(recA1, reqA1)
	if recA1.Code != http.StatusOK {
		t.Fatalf("client-a request 1 expected 200, got %d", recA1.Code)
	}

	// Client A request 2 -> 429
	reqA2 := setContextClient(httptest.NewRequest(http.MethodGet, "/v1/events", nil), "client-a")
	recA2 := httptest.NewRecorder()
	handler.ServeHTTP(recA2, reqA2)
	if recA2.Code != http.StatusTooManyRequests {
		t.Fatalf("client-a request 2 expected 429, got %d", recA2.Code)
	}

	// Client B request 1 -> must still be 200 OK (isolated)
	reqB1 := setContextClient(httptest.NewRequest(http.MethodGet, "/v1/events", nil), "client-b")
	recB1 := httptest.NewRecorder()
	handler.ServeHTTP(recB1, reqB1)
	if recB1.Code != http.StatusOK {
		t.Fatalf("client-b request 1 expected 200, got %d", recB1.Code)
	}
}

func TestLimiter_SkipPaths(t *testing.T) {
	limiter := New(Options{
		RPS:       1,
		Burst:     1,
		SkipPaths: []string{"/health"},
	})

	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Send 10 requests to /health rapidly: none should be 429
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d to skipped path /health expected 200, got %d", i+1, rec.Code)
		}
	}
}

func TestLimiter_IdleEviction(t *testing.T) {
	limiter := New(Options{
		RPS:         10,
		Burst:       5,
		IdleTimeout: 50 * time.Millisecond,
	})

	limiter.Allow("active-client")
	limiter.Allow("stale-client")

	if limiter.ActiveClientsCount() != 2 {
		t.Fatalf("expected 2 clients, got %d", limiter.ActiveClientsCount())
	}

	// Wait past idle timeout
	time.Sleep(60 * time.Millisecond)

	// Keep active-client refreshed
	limiter.Allow("active-client")

	// Evict entries older than 50ms
	evicted := limiter.EvictIdle(50 * time.Millisecond)
	if evicted != 1 {
		t.Fatalf("expected exactly 1 client evicted, got %d", evicted)
	}

	if limiter.ActiveClientsCount() != 1 {
		t.Fatalf("expected 1 remaining client, got %d", limiter.ActiveClientsCount())
	}
}

func TestLimiter_Concurrency(t *testing.T) {
	limiter := New(Options{
		RPS:   100,
		Burst: 50,
	})

	handler := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	const numGoroutines = 20
	const requestsPerGoroutine = 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		clientID := "client-" + strconv.Itoa(i%4)
		go func(cid string) {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				req := setContextClient(httptest.NewRequest(http.MethodGet, "/v1/events", nil), cid)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
			}
		}(clientID)
	}

	wg.Wait()

	if limiter.ActiveClientsCount() != 4 {
		t.Fatalf("expected 4 tracked clients, got %d", limiter.ActiveClientsCount())
	}
}

func TestLimiter_StartCleanupWorker(t *testing.T) {
	limiter := New(Options{
		RPS:         10,
		Burst:       5,
		IdleTimeout: 30 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limiter.Allow("temp-client")
	if limiter.ActiveClientsCount() != 1 {
		t.Fatalf("expected 1 client, got %d", limiter.ActiveClientsCount())
	}

	limiter.StartCleanupWorker(ctx, 20*time.Millisecond)

	var evicted bool
	for start := time.Now(); time.Since(start) < 500*time.Millisecond; {
		if limiter.ActiveClientsCount() == 0 {
			evicted = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if !evicted {
		t.Fatalf("expected cleanup worker to evict idle client, count: %d", limiter.ActiveClientsCount())
	}
}

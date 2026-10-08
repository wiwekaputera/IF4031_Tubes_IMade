package jwks

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
)

func generateTestKeyPair(t *testing.T) (*ecdsa.PrivateKey, JWK) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ecdsa key: %v", err)
	}

	const coordLen = 32
	pad := func(val *big.Int) []byte {
		b := val.Bytes()
		if len(b) >= coordLen {
			return b[len(b)-coordLen:]
		}
		padded := make([]byte, coordLen)
		copy(padded[coordLen-len(b):], b)
		return padded
	}

	xEnc := base64.RawURLEncoding.EncodeToString(pad(priv.PublicKey.X))
	yEnc := base64.RawURLEncoding.EncodeToString(pad(priv.PublicKey.Y))

	// Simple unique kid for testing
	kidBytes := make([]byte, 8)
	_, _ = rand.Read(kidBytes)
	kid := base64.RawURLEncoding.EncodeToString(kidBytes)

	jwk := JWK{
		Kty: "EC",
		Crv: "P-256",
		X:   xEnc,
		Y:   yEnc,
		Kid: kid,
		Alg: "ES256",
		Use: "sig",
	}

	return priv, jwk
}

func TestFetchInitial_Success(t *testing.T) {
	_, jwk := generateTestKeyPair(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{jwk}})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL: server.URL,
		Logger:  zerolog.Nop(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := cache.FetchInitial(ctx, 3, 10*time.Millisecond); err != nil {
		t.Fatalf("FetchInitial failed: %v", err)
	}

	if cache.KeyCount() != 1 {
		t.Fatalf("expected 1 cached key, got %d", cache.KeyCount())
	}

	key, err := cache.GetKey(context.Background(), jwk.Kid)
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}
	if key == nil {
		t.Fatal("expected non-nil public key")
	}
}

func TestFetchInitial_RetryWithBackoff(t *testing.T) {
	_, jwk := generateTestKeyPair(t)

	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		curr := atomic.AddInt32(&attempts, 1)
		if curr < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{jwk}})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL: server.URL,
		Logger:  zerolog.Nop(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := cache.FetchInitial(ctx, 5, 20*time.Millisecond); err != nil {
		t.Fatalf("FetchInitial failed: %v", err)
	}

	if atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", atomic.LoadInt32(&attempts))
	}
	if cache.KeyCount() != 1 {
		t.Fatalf("expected 1 key in cache, got %d", cache.KeyCount())
	}
}

func TestFetchInitial_FailureExhausted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL: server.URL,
		Logger:  zerolog.Nop(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := cache.FetchInitial(ctx, 3, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected FetchInitial to fail after exhausted retries, got nil")
	}
}

func TestGetKey_CacheHitLocalVerification(t *testing.T) {
	_, jwk := generateTestKeyPair(t)

	var reqCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{jwk}})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL: server.URL,
		Logger:  zerolog.Nop(),
	})

	ctx := context.Background()
	if err := cache.FetchInitial(ctx, 3, 10*time.Millisecond); err != nil {
		t.Fatalf("FetchInitial failed: %v", err)
	}

	initialRequests := atomic.LoadInt32(&reqCount)
	if initialRequests != 1 {
		t.Fatalf("expected 1 initial HTTP request, got %d", initialRequests)
	}

	for i := 0; i < 50; i++ {
		key, err := cache.GetKey(ctx, jwk.Kid)
		if err != nil {
			t.Fatalf("iteration %d: GetKey failed: %v", i, err)
		}
		if key == nil {
			t.Fatalf("iteration %d: nil key returned", i)
		}
	}

	if count := atomic.LoadInt32(&reqCount); count != initialRequests {
		t.Fatalf("expected 0 extra HTTP requests for cached kid, got %d", count-initialRequests)
	}
}

func TestGetKey_UnknownKidTriggersRefetch_Rotation(t *testing.T) {
	_, jwk1 := generateTestKeyPair(t)
	_, jwk2 := generateTestKeyPair(t)

	var mu sync.Mutex
	currentKeys := []JWK{jwk1}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: currentKeys})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL:            server.URL,
		MinRefetchInterval: -1, // disable rate limit for instant refetch test
		Logger:             zerolog.Nop(),
	})

	ctx := context.Background()
	if err := cache.FetchInitial(ctx, 3, 10*time.Millisecond); err != nil {
		t.Fatalf("FetchInitial failed: %v", err)
	}

	if cache.KeyCount() != 1 {
		t.Fatalf("expected 1 initial key, got %d", cache.KeyCount())
	}

	mu.Lock()
	currentKeys = []JWK{jwk1, jwk2}
	mu.Unlock()
	key2, err := cache.GetKey(ctx, jwk2.Kid)
	if err != nil {
		t.Fatalf("GetKey(jwk2.Kid) failed after key rotation: %v", err)
	}
	if key2 == nil {
		t.Fatal("expected non-nil key2")
	}

	if cache.KeyCount() != 2 {
		t.Fatalf("expected 2 keys in cache after rotation refetch, got %d", cache.KeyCount())
	}
}

func TestGetKey_RefetchRateLimiting(t *testing.T) {
	_, jwk := generateTestKeyPair(t)

	var reqCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{jwk}})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL:            server.URL,
		MinRefetchInterval: 500 * time.Millisecond,
		Logger:             zerolog.Nop(),
	})

	ctx := context.Background()
	if err := cache.FetchInitial(ctx, 3, 10*time.Millisecond); err != nil {
		t.Fatalf("FetchInitial failed: %v", err)
	}

	initialReqs := atomic.LoadInt32(&reqCount)

	_, err := cache.GetKey(ctx, "non-existent-kid-1")
	if err == nil {
		t.Fatal("expected error for non-existent kid, got nil")
	}

	refetchedReqs := atomic.LoadInt32(&reqCount)
	if refetchedReqs != initialReqs+1 {
		t.Fatalf("expected exactly 1 refetch, got %d", refetchedReqs-initialReqs)
	}

	_, err2 := cache.GetKey(ctx, "non-existent-kid-2")
	if err2 == nil {
		t.Fatal("expected error for second non-existent kid, got nil")
	}

	finalReqs := atomic.LoadInt32(&reqCount)
	if finalReqs != refetchedReqs {
		t.Fatalf("expected rate-limited refetch to NOT make HTTP call, but made %d calls", finalReqs-refetchedReqs)
	}
}

func TestGetKey_SingleflightCoalescing(t *testing.T) {
	_, jwk1 := generateTestKeyPair(t)
	_, jwk2 := generateTestKeyPair(t)

	var reqCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&reqCount, 1)
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{jwk1, jwk2}})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL:            server.URL,
		MinRefetchInterval: -1,
		Logger:             zerolog.Nop(),
	})

	ctx := context.Background()
	// Initial fetch only loads jwk1
	cache.mu.Lock()
	pub1, _ := ParseECPublicKey(jwk1)
	cache.keys[jwk1.Kid] = pub1
	cache.mu.Unlock()

	// Launch 20 concurrent queries for unknown jwk2
	const numGoroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := cache.GetKey(ctx, jwk2.Kid)
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d failed: %v", i, err)
		}
	}

	if count := atomic.LoadInt32(&reqCount); count != 1 {
		t.Fatalf("expected exactly 1 coalesced HTTP request via singleflight, got %d", count)
	}
}

func TestPeriodicRefresh(t *testing.T) {
	_, jwk1 := generateTestKeyPair(t)
	_, jwk2 := generateTestKeyPair(t)

	var mu sync.Mutex
	keys := []JWK{jwk1}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: keys})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL:         server.URL,
		RefreshInterval: 50 * time.Millisecond,
		Logger:          zerolog.Nop(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cache.FetchInitial(ctx, 3, 10*time.Millisecond); err != nil {
		t.Fatalf("FetchInitial failed: %v", err)
	}
	if cache.KeyCount() != 1 {
		t.Fatalf("expected 1 initial key, got %d", cache.KeyCount())
	}

	// Start background refresh
	cache.StartPeriodicRefresh(ctx)

	// Update server keys
	mu.Lock()
	keys = []JWK{jwk1, jwk2}
	mu.Unlock()

	// Wait for periodic refresh ticker to pick up jwk2
	var found bool
	for start := time.Now(); time.Since(start) < 1*time.Second; {
		if cache.KeyCount() == 2 {
			found = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !found {
		t.Fatalf("periodic refresh did not update cache within timeout (count: %d)", cache.KeyCount())
	}
}

func TestKeyfunc_TokenValidation(t *testing.T) {
	priv, jwk := generateTestKeyPair(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{jwk}})
	}))
	defer server.Close()

	cache := New(Options{
		JWKSURL: server.URL,
		Logger:  zerolog.Nop(),
	})

	if err := cache.FetchInitial(context.Background(), 3, 10*time.Millisecond); err != nil {
		t.Fatalf("FetchInitial failed: %v", err)
	}

	// Create a valid ES256 token signed by priv
	claims := jwt.MapClaims{
		"sub":   "test-client",
		"scope": "media",
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = jwk.Kid

	tokenString, err := token.SignedString(priv)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	// Verify using cache.Keyfunc
	parsedToken, err := jwt.Parse(tokenString, cache.Keyfunc)
	if err != nil {
		t.Fatalf("jwt.Parse failed: %v", err)
	}
	if !parsedToken.Valid {
		t.Fatal("expected token to be valid")
	}

	// Test rejection on wrong algorithm (e.g. HS256)
	hsToken := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	hsToken.Header["kid"] = jwk.Kid
	_, hsErr := cache.Keyfunc(hsToken)
	if hsErr == nil {
		t.Fatal("expected Keyfunc to reject non-ES256 algorithm")
	}

	// Test rejection on missing kid
	noKidToken := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	_, noKidErr := cache.Keyfunc(noKidToken)
	if noKidErr == nil {
		t.Fatal("expected Keyfunc to reject token without kid header")
	}
}

func TestParseECPublicKey_Errors(t *testing.T) {
	tests := []struct {
		name string
		jwk  JWK
	}{
		{
			name: "unsupported kty",
			jwk:  JWK{Kty: "RSA", Crv: "P-256", X: "abc", Y: "def"},
		},
		{
			name: "unsupported crv",
			jwk:  JWK{Kty: "EC", Crv: "P-384", X: "abc", Y: "def"},
		},
		{
			name: "unsupported alg",
			jwk:  JWK{Kty: "EC", Crv: "P-256", X: "abc", Y: "def", Alg: "RS256"},
		},
		{
			name: "unsupported use",
			jwk:  JWK{Kty: "EC", Crv: "P-256", X: "abc", Y: "def", Use: "enc"},
		},
		{
			name: "invalid base64",
			jwk:  JWK{Kty: "EC", Crv: "P-256", X: "!!!invalid", Y: "def"},
		},
		{
			name: "point not on curve",
			jwk:  JWK{Kty: "EC", Crv: "P-256", X: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", Y: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseECPublicKey(tc.jwk)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

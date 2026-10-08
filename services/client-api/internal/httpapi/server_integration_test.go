package httpapi

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/jwks"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/ratelimit"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/upstream"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

type testServerEnv struct {
	clientAPIURL        string
	jwksServer          *httptest.Server
	aggregatorServer    *httptest.Server
	clientAPIServer     *httptest.Server
	privKey1            *ecdsa.PrivateKey
	jwk1                jwks.JWK
	privKey2            *ecdsa.PrivateKey
	jwk2                jwks.JWK
	keysMu              sync.Mutex
	activeJWKs          []jwks.JWK
	capturedAggCorrID   string
	capturedAggQuery    string
	aggregatorDown      int32
}

func setupTestServer(t *testing.T) *testServerEnv {
	t.Helper()

	env := &testServerEnv{}

	// Generate initial key pair 1
	env.privKey1 = generateIntegrationECKey(t)
	env.jwk1 = keyToJWK(env.privKey1, "initial-key-id-1")

	// Generate secondary key pair 2 (for key rotation test)
	env.privKey2 = generateIntegrationECKey(t)
	env.jwk2 = keyToJWK(env.privKey2, "rotated-key-id-2")

	env.activeJWKs = []jwks.JWK{env.jwk1}

	// 1. Mock JWKS Server (representing token-service /.well-known/jwks.json)
	env.jwksServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env.keysMu.Lock()
		defer env.keysMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks.JWKS{Keys: env.activeJWKs})
	}))

	// 2. Mock Aggregator Server (representing aggregator /internal/events)
	env.aggregatorServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&env.aggregatorDown) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		env.capturedAggCorrID = r.Header.Get("X-Correlation-ID")
		env.capturedAggQuery = r.URL.RawQuery

		resp := upstream.EventsResponse{
			Data: []map[string]interface{}{
				{
					"hazard_id":     "evt-550e8400-e29b-41d4-a716-446655440000",
					"source":        "BMKG",
					"source_ref_id": "bmkg-ref-001",
					"hazard_type":   "SEISMIC",
					"severity":      "SIAGA",
					"area_name":     "Sulawesi Tengah",
					"latitude":      -1.4,
					"longitude":     119.8,
					"occurred_at":   "2026-09-21T20:00:00Z",
					"ingested_at":   "2026-09-21T20:00:05Z",
					"attributes": map[string]interface{}{
						"magnitude": 6.7,
						"depth_km":  15.0,
					},
				},
			},
			Meta: map[string]interface{}{
				"count": 1,
				"sources": map[string]interface{}{
					"bmkg": map[string]interface{}{
						"status":         "UP",
						"last_synced_at": "2026-09-21T22:40:00Z",
					},
					"pvmbg": map[string]interface{}{
						"status":      "OUTAGE",
						"stale_since": "2026-09-21T22:35:00Z",
					},
				},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))

	// 3. Setup client-api JWKS cache with initial fetch
	logger := zerolog.Nop()
	keyCache := jwks.New(jwks.Options{
		JWKSURL:            env.jwksServer.URL,
		MinRefetchInterval: -1,
		Logger:             logger,
	})

	if err := keyCache.FetchInitial(context.Background(), 3, 10*time.Millisecond); err != nil {
		t.Fatalf("failed to fetch initial JWKS: %v", err)
	}

	// 4. Setup per-client rate limiter (RPS=2, Burst=2)
	limiter := ratelimit.New(ratelimit.Options{
		RPS:       2,
		Burst:     2,
		SkipPaths: []string{"/health"},
	})

	// 5. Setup JWT auth middleware
	authMiddleware := auth.Middleware(auth.Config{
		KeyProvider: keyCache,
		Issuer:      contractsauth.DefaultIssuer,
		Audience:    contractsauth.DefaultAudience,
		SkipPaths:   []string{"/health"},
		Logger:      logger,
	})

	// 6. Setup upstream aggregator client
	aggClient := upstream.NewClient(upstream.Options{
		BaseURL: env.aggregatorServer.URL,
		Timeout: 2 * time.Second,
		Logger:  logger,
	})

	// 7. Setup HTTP mux and chain middlewares matching main.go exactly
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", HealthHandler())
	mux.HandleFunc("GET /v1/events", EventsHandler(aggClient))

	fullHandler := CorrelationIDMiddleware(
		LoggingMiddleware(logger,
			authMiddleware(
				limiter.Middleware(mux),
			),
		),
	)

	env.clientAPIServer = httptest.NewServer(fullHandler)
	env.clientAPIURL = env.clientAPIServer.URL

	t.Cleanup(func() {
		env.clientAPIServer.Close()
		env.aggregatorServer.Close()
		env.jwksServer.Close()
	})

	return env
}

func generateIntegrationECKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ecdsa key: %v", err)
	}
	return priv
}

func keyToJWK(priv *ecdsa.PrivateKey, kid string) jwks.JWK {
	const coordLen = 32
	pad := func(val *big.Int) []byte {
		b := val.Bytes()
		if len(b) >= coordLen {
			return b[len(b)-coordLen:]
		}
		p := make([]byte, coordLen)
		copy(p[coordLen-len(b):], b)
		return p
	}
	return jwks.JWK{
		Kty: "EC",
		Crv: "P-256",
		X:   base64.RawURLEncoding.EncodeToString(pad(priv.PublicKey.X)),
		Y:   base64.RawURLEncoding.EncodeToString(pad(priv.PublicKey.Y)),
		Kid: kid,
		Alg: "ES256",
		Use: "sig",
	}
}

func signJWT(t *testing.T, priv *ecdsa.PrivateKey, kid, sub string, scope contractsauth.Scope, exp time.Time) string {
	t.Helper()
	claims := auth.AccessTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    contractsauth.DefaultIssuer,
			Audience:  jwt.ClaimStrings{contractsauth.DefaultAudience},
			Subject:   sub,
			ExpiresAt: jwt.NewNumericDate(exp),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		Scope: scope,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = kid

	signed, err := token.SignedString(priv)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return signed
}

func TestB27_TokenValid_Returns200(t *testing.T) {
	env := setupTestServer(t)

	token := signJWT(t, env.privKey1, env.jwk1.Kid, "field-team-client", contractsauth.ScopeFieldTeam, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var body upstream.EventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	if len(body.Data) != 1 {
		t.Fatalf("expected 1 event, got %d", len(body.Data))
	}
	evt := body.Data[0]
	if evt["latitude"] != -1.4 || evt["longitude"] != 119.8 {
		t.Fatalf("expected coordinates to be present for field_team, got lat=%v lon=%v", evt["latitude"], evt["longitude"])
	}
}

func TestB27_TokenExpired_Returns401(t *testing.T) {
	env := setupTestServer(t)

	// Token expired 10 minutes ago
	token := signJWT(t, env.privKey1, env.jwk1.Kid, "media-client", contractsauth.ScopeMedia, time.Now().Add(-10*time.Minute))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for expired token, got %d", resp.StatusCode)
	}

	if authHdr := resp.Header.Get("WWW-Authenticate"); authHdr != `Bearer error="invalid_token"` {
		t.Fatalf("expected WWW-Authenticate: Bearer error=\"invalid_token\", got %q", authHdr)
	}
}

func TestB27_SignatureFromAnotherKey_Returns401(t *testing.T) {
	env := setupTestServer(t)

	otherPriv := generateIntegrationECKey(t)
	// Signed with other key, but claiming jwk1.Kid
	token := signJWT(t, otherPriv, env.jwk1.Kid, "media-client", contractsauth.ScopeMedia, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid signature, got %d", resp.StatusCode)
	}
}

func TestB27_AlgorithmConfusion_HS256_Returns401(t *testing.T) {
	env := setupTestServer(t)

	claims := auth.AccessTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    contractsauth.DefaultIssuer,
			Audience:  jwt.ClaimStrings{contractsauth.DefaultAudience},
			Subject:   "media-client",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		},
		Scope: contractsauth.ScopeMedia,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = env.jwk1.Kid
	signed, _ := token.SignedString([]byte("shared-secret"))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+signed)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for HS256 alg confusion attack, got %d", resp.StatusCode)
	}
}

func TestB27_AlgorithmNone_Returns401(t *testing.T) {
	env := setupTestServer(t)

	claims := auth.AccessTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    contractsauth.DefaultIssuer,
			Audience:  jwt.ClaimStrings{contractsauth.DefaultAudience},
			Subject:   "media-client",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		},
		Scope: contractsauth.ScopeMedia,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	token.Header["kid"] = env.jwk1.Kid
	signed, _ := token.SignedString(jwt.UnsafeAllowNoneSignatureType)

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+signed)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for alg:none attack, got %d", resp.StatusCode)
	}
}

func TestB27_UnknownKid_TriggersRefetch_Rotation(t *testing.T) {
	env := setupTestServer(t)

	// Rotate key upstream on JWKS server: add jwk2
	env.keysMu.Lock()
	env.activeJWKs = []jwks.JWK{env.jwk1, env.jwk2}
	env.keysMu.Unlock()

	// Token signed with newly rotated key 2 (kid not yet in cache)
	rotatedToken := signJWT(t, env.privKey2, env.jwk2.Kid, "field-team-client", contractsauth.ScopeFieldTeam, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+rotatedToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK after on-demand JWKS key rotation refetch, got %d", resp.StatusCode)
	}
}

func TestB27_MediaScope_SummaryFieldsOnly(t *testing.T) {
	env := setupTestServer(t)

	mediaToken := signJWT(t, env.privKey1, env.jwk1.Kid, "media-portal", contractsauth.ScopeMedia, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+mediaToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var body upstream.EventsResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode body: %v", err)
	}

	if len(body.Data) != 1 {
		t.Fatalf("expected 1 event, got %d", len(body.Data))
	}

	evt := body.Data[0]
	// Verify allowed fields
	for _, f := range []string{"hazard_id", "source", "hazard_type", "severity", "area_name", "occurred_at", "ingested_at"} {
		if _, ok := evt[f]; !ok {
			t.Errorf("expected summary field %q to be present", f)
		}
	}

	// Verify raw fields stripped
	for _, f := range []string{"latitude", "longitude", "attributes", "source_ref_id"} {
		if _, ok := evt[f]; ok {
			t.Errorf("SECURITY LEAK: field %q must not be present in media scope", f)
		}
	}
}

func TestB27_MediaScope_ViewFull_Returns403(t *testing.T) {
	env := setupTestServer(t)

	mediaToken := signJWT(t, env.privKey1, env.jwk1.Kid, "media-portal", contractsauth.ScopeMedia, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events?view=full", nil)
	req.Header.Set("Authorization", "Bearer "+mediaToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for media requesting view=full, got %d", resp.StatusCode)
	}

	if authHdr := resp.Header.Get("WWW-Authenticate"); authHdr != `Bearer error="insufficient_scope"` {
		t.Fatalf("expected WWW-Authenticate: Bearer error=\"insufficient_scope\", got %q", authHdr)
	}
}

func TestB27_FieldTeamScope_ViewFull_ReturnsAllFields(t *testing.T) {
	env := setupTestServer(t)

	fieldToken := signJWT(t, env.privKey1, env.jwk1.Kid, "basarnas-team", contractsauth.ScopeFieldTeam, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events?view=full", nil)
	req.Header.Set("Authorization", "Bearer "+fieldToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var body upstream.EventsResponse
	_ = json.NewDecoder(resp.Body).Decode(&body)

	evt := body.Data[0]
	if evt["latitude"] != -1.4 || evt["longitude"] != 119.8 {
		t.Errorf("expected full coordinates, got lat=%v lon=%v", evt["latitude"], evt["longitude"])
	}
	if evt["attributes"] == nil {
		t.Error("expected attributes to be present")
	}
}

func TestB27_BurstExceeded_Returns429AndRetryAfter(t *testing.T) {
	env := setupTestServer(t)

	token := signJWT(t, env.privKey1, env.jwk1.Kid, "rate-limited-client", contractsauth.ScopeInternalOps, time.Now().Add(1*time.Hour))

	// Burst is 2: first 2 requests must succeed
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("req %d failed: %v", i+1, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("req %d expected 200 OK within burst, got %d", i+1, resp.StatusCode)
		}
	}

	// 3rd request exceeds burst -> 429
	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("3rd req failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", resp.StatusCode)
	}

	retryAfter := resp.Header.Get("Retry-After")
	if retryAfter == "" {
		t.Fatal("expected Retry-After header to be present on 429")
	}
	sec, err := strconv.Atoi(retryAfter)
	if err != nil || sec < 1 {
		t.Fatalf("expected positive integer Retry-After, got %q", retryAfter)
	}
}

func TestB27_CorrelationID_PropagatedToAggregator(t *testing.T) {
	env := setupTestServer(t)

	token := signJWT(t, env.privKey1, env.jwk1.Kid, "ops-client", contractsauth.ScopeInternalOps, time.Now().Add(1*time.Hour))

	customCorrID := "my-trace-correlation-id-98765"
	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Correlation-ID", customCorrID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	// Verify aggregator received correlation ID
	if env.capturedAggCorrID != customCorrID {
		t.Fatalf("expected aggregator to receive correlation ID %q, got %q", customCorrID, env.capturedAggCorrID)
	}

	// Verify client response has correlation ID
	if clientRespCorrID := resp.Header.Get("X-Correlation-ID"); clientRespCorrID != customCorrID {
		t.Fatalf("expected client response to have correlation ID %q, got %q", customCorrID, clientRespCorrID)
	}
}

func TestB27_HealthEndpoint_PublicNoAuth(t *testing.T) {
	env := setupTestServer(t)

	// GET /health without Authorization header
	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /health, got %d", resp.StatusCode)
	}

	var status map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&status)
	if status["status"] != "UP" {
		t.Fatalf("expected status UP, got %v", status)
	}
}

func TestB27_MethodNotAllowed(t *testing.T) {
	env := setupTestServer(t)

	token := signJWT(t, env.privKey1, env.jwk1.Kid, "ops-client", contractsauth.ScopeInternalOps, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodPost, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, "GET") {
		t.Fatalf("expected Allow header to contain GET, got %q", allow)
	}
}

func TestB27_InvalidQueryParams_Returns400(t *testing.T) {
	env := setupTestServer(t)

	badQueries := []string{
		"/v1/events?type=INVALID_TYPE",
		"/v1/events?limit=0",
		"/v1/events?limit=200",
		"/v1/events?since=not-rfc3339",
		"/v1/events?hazard_id=not-a-uuid",
		"/v1/events?view=invalid_view",
	}

	for i, q := range badQueries {
		token := signJWT(t, env.privKey1, env.jwk1.Kid, "ops-client-"+strconv.Itoa(i), contractsauth.ScopeInternalOps, time.Now().Add(1*time.Hour))

		req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+q, nil)
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("for %q expected 400 Bad Request, got %d", q, resp.StatusCode)
		}
	}
}

func TestB27_AggregatorDown_Returns503(t *testing.T) {
	env := setupTestServer(t)

	// Simulate aggregator outage
	atomic.StoreInt32(&env.aggregatorDown, 1)

	token := signJWT(t, env.privKey1, env.jwk1.Kid, "ops-client", contractsauth.ScopeInternalOps, time.Now().Add(1*time.Hour))

	req, _ := http.NewRequest(http.MethodGet, env.clientAPIURL+"/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("expected 502 or 503 when aggregator is down, got %d", resp.StatusCode)
	}
}

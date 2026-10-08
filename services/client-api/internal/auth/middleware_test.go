package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

type mockKeyProvider struct {
	keyFunc func(token *jwt.Token) (interface{}, error)
}

func (m *mockKeyProvider) Keyfunc(token *jwt.Token) (interface{}, error) {
	return m.keyFunc(token)
}

func generateECKeyPair(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ecdsa key: %v", err)
	}
	return priv
}

func signTestToken(t *testing.T, priv *ecdsa.PrivateKey, kid, iss, aud, sub string, scope contractsauth.Scope, exp time.Time) string {
	t.Helper()
	claims := AccessTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    iss,
			Audience:  jwt.ClaimStrings{aud},
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

func TestMiddleware_ValidToken(t *testing.T) {
	priv := generateECKeyPair(t)
	kid := "test-kid-1"

	keyProvider := &mockKeyProvider{
		keyFunc: func(token *jwt.Token) (interface{}, error) {
			if token.Header["kid"] != kid {
				return nil, errors.New("key not found")
			}
			return &priv.PublicKey, nil
		},
	}

	middleware := Middleware(Config{
		KeyProvider: keyProvider,
		Issuer:      contractsauth.DefaultIssuer,
		Audience:    contractsauth.DefaultAudience,
	})

	var reachedNext bool
	var capturedSub string
	var capturedScope contractsauth.Scope

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reachedNext = true
		capturedSub = ClientIDFromContext(r.Context())
		capturedScope = ScopeFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	tokenStr := signTestToken(t, priv, kid, contractsauth.DefaultIssuer, contractsauth.DefaultAudience, "media-app", contractsauth.ScopeMedia, time.Now().Add(1*time.Hour))

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d (body: %s)", rec.Code, rec.Body.String())
	}
	if !reachedNext {
		t.Fatal("expected next handler to be called")
	}
	if capturedSub != "media-app" {
		t.Fatalf("expected sub 'media-app', got %q", capturedSub)
	}
	if capturedScope != contractsauth.ScopeMedia {
		t.Fatalf("expected scope %q, got %q", contractsauth.ScopeMedia, capturedScope)
	}
}

func TestMiddleware_MissingAuthorizationHeader(t *testing.T) {
	middleware := Middleware(Config{
		KeyProvider: &mockKeyProvider{},
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be reached")
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if authHeader := rec.Header().Get("WWW-Authenticate"); authHeader != `Bearer error="invalid_token"` {
		t.Fatalf("expected WWW-Authenticate: Bearer error=\"invalid_token\", got %q", authHeader)
	}

	var oauthErr contractsauth.OAuthError
	if err := json.NewDecoder(rec.Body).Decode(&oauthErr); err != nil {
		t.Fatalf("failed to parse json error: %v", err)
	}
	if oauthErr.Error != contractsauth.ErrInvalidToken {
		t.Fatalf("expected error code invalid_token, got %q", oauthErr.Error)
	}
}

func TestMiddleware_MalformedAuthorizationHeader(t *testing.T) {
	middleware := Middleware(Config{
		KeyProvider: &mockKeyProvider{},
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be reached")
	}))

	cases := []string{
		"Basic xyz123",
		"Bearer",
		"Bearer   ",
		"Token abc",
	}

	for _, headerVal := range cases {
		t.Run(headerVal, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
			req.Header.Set("Authorization", headerVal)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401 for header %q, got %d", headerVal, rec.Code)
			}
		})
	}
}

func TestMiddleware_ExpiredToken(t *testing.T) {
	priv := generateECKeyPair(t)
	kid := "test-kid-expired"

	keyProvider := &mockKeyProvider{
		keyFunc: func(token *jwt.Token) (interface{}, error) {
			return &priv.PublicKey, nil
		},
	}

	middleware := Middleware(Config{
		KeyProvider: keyProvider,
		Issuer:      contractsauth.DefaultIssuer,
		Audience:    contractsauth.DefaultAudience,
		Leeway:      1 * time.Second,
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be reached")
	}))

	// Token expired 10 minutes ago
	expiredToken := signTestToken(t, priv, kid, contractsauth.DefaultIssuer, contractsauth.DefaultAudience, "client-app", contractsauth.ScopeFieldTeam, time.Now().Add(-10*time.Minute))

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+expiredToken)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_InvalidSignature(t *testing.T) {
	privA := generateECKeyPair(t)
	privB := generateECKeyPair(t)
	kid := "test-kid-sig"

	keyProvider := &mockKeyProvider{
		keyFunc: func(token *jwt.Token) (interface{}, error) {
			// Returns public key of B, but token was signed with A
			return &privB.PublicKey, nil
		},
	}

	middleware := Middleware(Config{
		KeyProvider: keyProvider,
		Issuer:      contractsauth.DefaultIssuer,
		Audience:    contractsauth.DefaultAudience,
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be reached")
	}))

	tokenStr := signTestToken(t, privA, kid, contractsauth.DefaultIssuer, contractsauth.DefaultAudience, "client-app", contractsauth.ScopeMedia, time.Now().Add(1*time.Hour))

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+tokenStr)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_AlgorithmConfusion_HS256(t *testing.T) {
	keyProvider := &mockKeyProvider{
		keyFunc: func(token *jwt.Token) (interface{}, error) {
			return []byte("secret"), nil
		},
	}

	middleware := Middleware(Config{
		KeyProvider: keyProvider,
		Issuer:      contractsauth.DefaultIssuer,
		Audience:    contractsauth.DefaultAudience,
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be reached")
	}))

	claims := jwt.MapClaims{
		"iss":   contractsauth.DefaultIssuer,
		"aud":   contractsauth.DefaultAudience,
		"sub":   "client-app",
		"scope": "media",
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+tokenString)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for HS256 alg confusion attempt, got %d", rec.Code)
	}
}

func TestMiddleware_AlgorithmNone(t *testing.T) {
	keyProvider := &mockKeyProvider{
		keyFunc: func(token *jwt.Token) (interface{}, error) {
			return jwt.UnsafeAllowNoneSignatureType, nil
		},
	}

	middleware := Middleware(Config{
		KeyProvider: keyProvider,
		Issuer:      contractsauth.DefaultIssuer,
		Audience:    contractsauth.DefaultAudience,
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be reached")
	}))

	claims := jwt.MapClaims{
		"iss":   contractsauth.DefaultIssuer,
		"aud":   contractsauth.DefaultAudience,
		"sub":   "client-app",
		"scope": "media",
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	tokenString, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("Authorization", "Bearer "+tokenString)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for alg:none attempt, got %d", rec.Code)
	}
}

func TestMiddleware_WrongIssuerOrAudience(t *testing.T) {
	priv := generateECKeyPair(t)
	kid := "test-kid-iss-aud"

	keyProvider := &mockKeyProvider{
		keyFunc: func(token *jwt.Token) (interface{}, error) {
			return &priv.PublicKey, nil
		},
	}

	middleware := Middleware(Config{
		KeyProvider: keyProvider,
		Issuer:      contractsauth.DefaultIssuer,
		Audience:    contractsauth.DefaultAudience,
	})

	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("next handler should not be reached")
	}))

	// Wrong issuer
	tokenWrongIss := signTestToken(t, priv, kid, "evil-issuer", contractsauth.DefaultAudience, "client-app", contractsauth.ScopeMedia, time.Now().Add(1*time.Hour))
	req1 := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req1.Header.Set("Authorization", "Bearer "+tokenWrongIss)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong issuer, got %d", rec1.Code)
	}

	// Wrong audience
	tokenWrongAud := signTestToken(t, priv, kid, contractsauth.DefaultIssuer, "evil-audience", "client-app", contractsauth.ScopeMedia, time.Now().Add(1*time.Hour))
	req2 := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req2.Header.Set("Authorization", "Bearer "+tokenWrongAud)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong audience, got %d", rec2.Code)
	}
}

func TestMiddleware_SkipPaths(t *testing.T) {
	middleware := Middleware(Config{
		KeyProvider: &mockKeyProvider{},
		SkipPaths:   []string{"/health"},
	})

	var reachedHealth bool
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reachedHealth = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for skipped path /health, got %d", rec.Code)
	}
	if !reachedHealth {
		t.Fatal("expected handler to be reached for skipped path")
	}
}

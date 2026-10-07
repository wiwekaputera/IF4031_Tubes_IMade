package issuer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/clients"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/keys"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/store"
)

type memoryStore struct {
	tokens map[string]*storedToken
}

type storedToken struct {
	clientID  string
	scope     string
	expiresAt time.Time
	revoked   bool
}

func newMemoryStore() *memoryStore {
	return &memoryStore{tokens: make(map[string]*storedToken)}
}

func (m *memoryStore) Insert(_ context.Context, tokenHash, clientID, scope string, expiresAt time.Time) error {
	m.tokens[tokenHash] = &storedToken{
		clientID:  clientID,
		scope:     scope,
		expiresAt: expiresAt,
		revoked:   false,
	}
	return nil
}

func (m *memoryStore) Rotate(_ context.Context, oldHash, newHash, clientID, scope string, newExpiresAt time.Time) error {
	tok, exists := m.tokens[oldHash]
	if !exists {
		return store.ErrTokenNotFound
	}
	if tok.clientID != clientID {
		return store.ErrTokenNotFound
	}
	if tok.revoked {
		for _, t := range m.tokens {
			if t.clientID == clientID {
				t.revoked = true
			}
		}
		return store.ErrTokenReused
	}
	if time.Now().After(tok.expiresAt) {
		tok.revoked = true
		return store.ErrTokenExpired
	}

	tok.revoked = true
	m.tokens[newHash] = &storedToken{
		clientID:  clientID,
		scope:     scope,
		expiresAt: newExpiresAt,
		revoked:   false,
	}
	return nil
}

func (m *memoryStore) RevokeAllForClient(_ context.Context, clientID string) error {
	for _, t := range m.tokens {
		if t.clientID == clientID {
			t.revoked = true
		}
	}
	return nil
}

func setupTestIssuer(t *testing.T) (*Issuer, *keys.KeyManager) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ecdsa key: %v", err)
	}
	pkcs8Bytes, _ := x509.MarshalPKCS8PrivateKey(priv)
	pemBlock := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})
	keyB64 := base64.StdEncoding.EncodeToString(pemBlock)

	km, err := keys.NewKeyManager(keyB64)
	if err != nil {
		t.Fatalf("failed to create key manager: %v", err)
	}

	cfg := &config.Config{
		AccessTokenTTL:          60 * time.Second,
		RefreshTokenTTLDays:     7,
		ClientMediaID:           "media-client",
		ClientMediaSecret:       "media-secret",
		ClientFieldTeamID:       "field-client",
		ClientFieldTeamSecret:   "field-secret",
		ClientInternalOpsID:     "ops-client",
		ClientInternalOpsSecret: "ops-secret",
		TokenIssuer:             "bnpb-token-service",
		TokenAudience:           "bnpb-client-api",
	}

	reg := clients.NewRegistry(cfg)
	memStore := newMemoryStore()

	iss := NewIssuer(cfg, km, reg, memStore)
	return iss, km
}

func TestIssueClientCredentials(t *testing.T) {
	iss, km := setupTestIssuer(t)
	ctx := context.Background()

	resp, err := iss.IssueClientCredentials(ctx, "media-client", "media-secret")
	if err != nil {
		t.Fatalf("media token issue failed: %v", err)
	}
	if resp.TokenType != "Bearer" || resp.ExpiresIn != 60 || resp.Scope != auth.ScopeMedia {
		t.Fatalf("unexpected token response: %+v", resp)
	}
	if resp.RefreshToken != "" {
		t.Fatalf("media client MUST NOT receive a refresh token")
	}

	parsed, err := jwt.Parse(resp.AccessToken, func(token *jwt.Token) (interface{}, error) {
		if token.Header["kid"] != km.ActiveKeyID() {
			t.Fatalf("expected kid %s, got %v", km.ActiveKeyID(), token.Header["kid"])
		}
		return &km.ActivePrivateKey().PublicKey, nil
	})
	if err != nil || !parsed.Valid {
		t.Fatalf("JWT verification failed: %v", err)
	}

	fieldResp, err := iss.IssueClientCredentials(ctx, "field-client", "field-secret")
	if err != nil {
		t.Fatalf("field team token issue failed: %v", err)
	}
	if fieldResp.RefreshToken == "" {
		t.Fatal("field team MUST receive a refresh token")
	}

	_, err = iss.IssueClientCredentials(ctx, "field-client", "wrong-secret")
	if err == nil {
		t.Fatal("expected failure on wrong secret")
	}
}

func TestIssueRefreshToken_RotationAndReuse(t *testing.T) {
	iss, _ := setupTestIssuer(t)
	ctx := context.Background()

	initial, err := iss.IssueClientCredentials(ctx, "field-client", "field-secret")
	if err != nil {
		t.Fatalf("initial issue failed: %v", err)
	}
	initialRefresh := initial.RefreshToken

	refreshed, err := iss.IssueRefreshToken(ctx, "field-client", "field-secret", initialRefresh)
	if err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	newRefresh := refreshed.RefreshToken
	if newRefresh == initialRefresh {
		t.Fatal("refresh token was not rotated")
	}

	_, err = iss.IssueRefreshToken(ctx, "field-client", "field-secret", initialRefresh)
	if err == nil {
		t.Fatal("expected reuse detection error")
	}

	_, err = iss.IssueRefreshToken(ctx, "field-client", "field-secret", newRefresh)
	if err == nil {
		t.Fatal("expected new token to also be revoked due to reuse theft detection")
	}
}

func TestIssueRefreshToken_UnauthorizedClient(t *testing.T) {
	iss, _ := setupTestIssuer(t)
	ctx := context.Background()

	_, err := iss.IssueRefreshToken(ctx, "media-client", "media-secret", "some-opaque-token")
	if err == nil {
		t.Fatal("expected unauthorized client error for media client")
	}
}

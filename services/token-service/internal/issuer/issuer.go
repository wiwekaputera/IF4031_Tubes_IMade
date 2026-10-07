package issuer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/clients"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/config"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/keys"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/store"
)

var (
	ErrInvalidClient      = errors.New("invalid client credentials")
	ErrUnauthorizedClient = errors.New("client not authorized for this grant")
	ErrInvalidGrant       = errors.New("invalid refresh token")
)

// Issuer issues signed JWT access tokens and manages opaque refresh tokens.
type Issuer struct {
	cfg      *config.Config
	km       *keys.KeyManager
	registry *clients.Registry
	store    store.Store
}

// NewIssuer creates a new token issuer.
func NewIssuer(cfg *config.Config, km *keys.KeyManager, reg *clients.Registry, st store.Store) *Issuer {
	return &Issuer{
		cfg:      cfg,
		km:       km,
		registry: reg,
		store:    st,
	}
}

// IssueClientCredentials issues an access token for client_credentials grant.
func (iss *Issuer) IssueClientCredentials(ctx context.Context, clientID, clientSecret string) (*auth.TokenResponse, error) {
	client, ok := iss.registry.Authenticate(clientID, clientSecret)
	if !ok {
		return nil, ErrInvalidClient
	}

	accessToken, err := iss.generateJWT(client.ID, client.Scope)
	if err != nil {
		return nil, fmt.Errorf("failed to sign access token: %w", err)
	}

	resp := &auth.TokenResponse{
		AccessToken: accessToken,
		TokenType:   auth.TokenTypeBearer,
		ExpiresIn:   int64(iss.cfg.AccessTokenTTL.Seconds()),
		Scope:       client.Scope,
	}

	if client.AllowRefreshToken {
		rawRefreshToken, tokenHash, err := iss.generateOpaqueToken()
		if err != nil {
			return nil, fmt.Errorf("failed to generate refresh token: %w", err)
		}

		expiresAt := time.Now().AddDate(0, 0, iss.cfg.RefreshTokenTTLDays)
		if err := iss.store.Insert(ctx, tokenHash, client.ID, string(client.Scope), expiresAt); err != nil {
			return nil, fmt.Errorf("failed to persist refresh token: %w", err)
		}

		resp.RefreshToken = rawRefreshToken
	}

	return resp, nil
}

// IssueRefreshToken performs token refresh with single-use rotation and reuse detection.
func (iss *Issuer) IssueRefreshToken(ctx context.Context, clientID, clientSecret, rawRefreshToken string) (*auth.TokenResponse, error) {
	if rawRefreshToken == "" {
		return nil, ErrInvalidGrant
	}

	client, ok := iss.registry.Authenticate(clientID, clientSecret)
	if !ok {
		return nil, ErrInvalidClient
	}

	if !client.AllowRefreshToken {
		return nil, ErrUnauthorizedClient
	}

	oldHash := HashToken(rawRefreshToken)
	newRawToken, newHash, err := iss.generateOpaqueToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate new refresh token: %w", err)
	}

	newExpiresAt := time.Now().AddDate(0, 0, iss.cfg.RefreshTokenTTLDays)
	if err := iss.store.Rotate(ctx, oldHash, newHash, client.ID, string(client.Scope), newExpiresAt); err != nil {
		if errors.Is(err, store.ErrTokenNotFound) || errors.Is(err, store.ErrTokenExpired) || errors.Is(err, store.ErrTokenReused) {
			return nil, ErrInvalidGrant
		}
		return nil, fmt.Errorf("failed to rotate refresh token: %w", err)
	}

	newAccessToken, err := iss.generateJWT(client.ID, client.Scope)
	if err != nil {
		return nil, fmt.Errorf("failed to sign access token: %w", err)
	}

	return &auth.TokenResponse{
		AccessToken:  newAccessToken,
		TokenType:    auth.TokenTypeBearer,
		ExpiresIn:    int64(iss.cfg.AccessTokenTTL.Seconds()),
		RefreshToken: newRawToken,
		Scope:        client.Scope,
	}, nil
}

// generateJWT generates a new JWT access token.
func (iss *Issuer) generateJWT(clientID string, scope auth.Scope) (string, error) {
	now := time.Now().UTC()
	jtiBytes := make([]byte, 16)
	_, _ = rand.Read(jtiBytes)

	claims := jwt.MapClaims{
		"iss":   iss.cfg.TokenIssuer,
		"aud":   iss.cfg.TokenAudience,
		"sub":   clientID,
		"scope": string(scope),
		"jti":   hex.EncodeToString(jtiBytes),
		"iat":   now.Unix(),
		"nbf":   now.Unix(),
		"exp":   now.Add(iss.cfg.AccessTokenTTL).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = iss.km.ActiveKeyID()

	return token.SignedString(iss.km.ActivePrivateKey())
}

// generateOpaqueToken generates a random opaque token and its hash.
func (iss *Issuer) generateOpaqueToken() (string, string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	rawToken := base64.RawURLEncoding.EncodeToString(b)
	hash := HashToken(rawToken)
	return rawToken, hash, nil
}

// HashToken computes the SHA-256 hex string of the raw token.
func HashToken(rawToken string) string {
	sum := sha256.Sum256([]byte(rawToken))
	return hex.EncodeToString(sum[:])
}

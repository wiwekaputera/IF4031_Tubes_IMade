package jwks

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

var (
	ErrKeyNotFound = errors.New("jwks: key not found")

	ErrRefetchRateLimited = errors.New("jwks: on-demand refetch rate limited")

	ErrUnsupportedKeyType = errors.New("jwks: unsupported key type")

	ErrUnsupportedCurve = errors.New("jwks: unsupported elliptic curve")

	ErrInvalidPoint = errors.New("jwks: public key coordinate not on curve")
)

const (
	DefaultRefreshInterval = 10 * time.Minute
	
	DefaultMinRefetchInterval = 30 * time.Second
)

type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	Kid string `json:"kid"`
	Alg string `json:"alg,omitempty"`
	Use string `json:"use,omitempty"`
}

type JWKS struct {
	Keys []JWK `json:"keys"`
}

type Options struct {
	JWKSURL            string
	HTTPClient         *http.Client
	RefreshInterval    time.Duration
	MinRefetchInterval time.Duration
	Logger             zerolog.Logger
}

type KeyCache struct {
	jwksURL            string
	httpClient         *http.Client
	refreshInterval    time.Duration
	minRefetchInterval time.Duration
	logger             zerolog.Logger

	mu                 sync.RWMutex
	keys               map[string]*ecdsa.PublicKey
	lastOnDemandRefetch time.Time

	sfGroup singleflight.Group
}

// New creates a new KeyCache with the provided options.
func New(opts Options) *KeyCache {
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}

	refreshInterval := opts.RefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = DefaultRefreshInterval
	}

	minRefetch := opts.MinRefetchInterval
	if minRefetch == 0 {
		minRefetch = DefaultMinRefetchInterval
	} else if minRefetch < 0 {
		minRefetch = 0
	}

	return &KeyCache{
		jwksURL:            opts.JWKSURL,
		httpClient:         client,
		refreshInterval:    refreshInterval,
		minRefetchInterval: minRefetch,
		logger:             opts.Logger,
		keys:               make(map[string]*ecdsa.PublicKey),
	}
}

// ParseECPublicKey parses a P-256 JWK into an *ecdsa.PublicKey.
func ParseECPublicKey(jwk JWK) (*ecdsa.PublicKey, error) {
	if jwk.Kty != "EC" {
		return nil, fmt.Errorf("%w: expected 'EC', got %q", ErrUnsupportedKeyType, jwk.Kty)
	}
	if jwk.Crv != "P-256" {
		return nil, fmt.Errorf("%w: expected 'P-256', got %q", ErrUnsupportedCurve, jwk.Crv)
	}
	if jwk.Alg != "" && jwk.Alg != "ES256" {
		return nil, fmt.Errorf("jwks: unsupported alg %q: expected 'ES256'", jwk.Alg)
	}
	if jwk.Use != "" && jwk.Use != "sig" {
		return nil, fmt.Errorf("jwks: unsupported use %q: expected 'sig'", jwk.Use)
	}

	xBytes, err := decodeBase64URL(jwk.X)
	if err != nil {
		return nil, fmt.Errorf("jwks: invalid x coordinate: %w", err)
	}
	yBytes, err := decodeBase64URL(jwk.Y)
	if err != nil {
		return nil, fmt.Errorf("jwks: invalid y coordinate: %w", err)
	}

	x := new(big.Int).SetBytes(xBytes)
	y := new(big.Int).SetBytes(yBytes)

	curve := elliptic.P256()
	if !curve.IsOnCurve(x, y) {
		return nil, ErrInvalidPoint
	}

	return &ecdsa.PublicKey{
		Curve: curve,
		X:     x,
		Y:     y,
	}, nil
}

// decodeBase64URL safely decodes both unpadded and padded base64url strings.
func decodeBase64URL(s string) ([]byte, error) {
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}

// fetchKeys perform HTTP GET to token-service JWKS endpoint.
func (c *KeyCache) fetchKeys(ctx context.Context) (map[string]*ecdsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.jwksURL, nil)
	if err != nil {
		return nil, fmt.Errorf("jwks: failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	latencyMs := time.Since(start).Milliseconds()

	if err != nil {
		c.logger.Error().
			Err(err).
			Int64("latency_ms", latencyMs).
			Str("upstream", "token-service").
			Str("target", "jwks").
			Msg("failed to fetch JWKS from token-service")
		return nil, fmt.Errorf("jwks: HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Error().
			Int("upstream_status", resp.StatusCode).
			Int64("latency_ms", latencyMs).
			Str("upstream", "token-service").
			Str("target", "jwks").
			Msg("token-service returned non-200 status for JWKS")
		return nil, fmt.Errorf("jwks: unexpected status %d from endpoint", resp.StatusCode)
	}

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		c.logger.Error().
			Err(err).
			Int64("latency_ms", latencyMs).
			Str("upstream", "token-service").
			Str("target", "jwks").
			Msg("failed to decode JWKS JSON")
		return nil, fmt.Errorf("jwks: failed to decode JSON: %w", err)
	}

	parsed := make(map[string]*ecdsa.PublicKey, len(jwks.Keys))
	for _, k := range jwks.Keys {
		pub, err := ParseECPublicKey(k)
		if err != nil {
			c.logger.Warn().
				Err(err).
				Str("kid", k.Kid).
				Msg("skipping invalid key in JWKS")
			continue
		}
		kid := k.Kid
		if kid == "" {
			canonicalJSON := fmt.Sprintf(`{"crv":"P-256","kty":"EC","x":"%s","y":"%s"}`, k.X, k.Y)
			sum := sha256.Sum256([]byte(canonicalJSON))
			kid = base64.RawURLEncoding.EncodeToString(sum[:])
		}
		parsed[kid] = pub
	}

	c.logger.Info().
		Int("upstream_status", resp.StatusCode).
		Int64("latency_ms", latencyMs).
		Str("upstream", "token-service").
		Str("target", "jwks").
		Int("keys_count", len(parsed)).
		Msg("successfully fetched JWKS from token-service")

	return parsed, nil
}

// FetchInitial fetches JWKS keys during service startup with retry and exponential backoff.
func (c *KeyCache) FetchInitial(ctx context.Context, maxRetries int, baseBackoff time.Duration) error {
	if maxRetries <= 0 {
		maxRetries = 10
	}
	if baseBackoff <= 0 {
		baseBackoff = 500 * time.Millisecond
	}

	var lastErr error
	backoff := baseBackoff

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		keys, err := c.fetchKeys(ctx)
		if err == nil {
			c.mu.Lock()
			c.keys = keys
			c.mu.Unlock()

			c.logger.Info().
				Int("key_count", len(keys)).
				Msg("initial JWKS loaded successfully")
			return nil
		}

		lastErr = err
		c.logger.Warn().
			Err(err).
			Int("attempt", attempt).
			Int("max_retries", maxRetries).
			Dur("backoff", backoff).
			Msg("token-service JWKS not ready, retrying with backoff...")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}

	return fmt.Errorf("failed to fetch initial JWKS after %d attempts: %w", maxRetries, lastErr)
}

// StartPeriodicRefresh runs a background ticker to refresh JWKS keys periodically.
func (c *KeyCache) StartPeriodicRefresh(ctx context.Context) {
	ticker := time.NewTicker(c.refreshInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				c.logger.Debug().Msg("JWKS periodic refresh worker stopped")
				return
			case <-ticker.C:
				c.logger.Debug().Msg("running periodic JWKS refresh")
				keys, err := c.fetchKeys(ctx)
				if err != nil {
					c.logger.Warn().
						Err(err).
						Msg("periodic JWKS refresh failed; retaining existing cached keys")
					continue
				}
				c.mu.Lock()
				c.keys = keys
				c.mu.Unlock()
				c.logger.Info().
					Int("key_count", len(keys)).
					Msg("periodic JWKS refresh succeeded")
			}
		}
	}()
}

// GetKey retrieve public key by kid.
func (c *KeyCache) GetKey(ctx context.Context, kid string) (*ecdsa.PublicKey, error) {
	if kid == "" {
		return nil, fmt.Errorf("%w: empty kid", ErrKeyNotFound)
	}

	// Fast path: cached key in memory (local verification, 0 HTTP overhead)
	c.mu.RLock()
	if key, ok := c.keys[kid]; ok {
		c.mu.RUnlock()
		return key, nil
	}
	c.mu.RUnlock()

	_, err, _ := c.sfGroup.Do("refetch", func() (interface{}, error) {
		// Double-check if another concurrent goroutine already refreshed the cache
		c.mu.RLock()
		if _, ok := c.keys[kid]; ok {
			c.mu.RUnlock()
			return nil, nil
		}
		c.mu.RUnlock()

		// Enforce on-demand refetch rate limit
		c.mu.Lock()
		if c.minRefetchInterval > 0 && !c.lastOnDemandRefetch.IsZero() && time.Since(c.lastOnDemandRefetch) < c.minRefetchInterval {
			c.mu.Unlock()
			c.logger.Warn().
				Str("kid", kid).
				Dur("elapsed", time.Since(c.lastOnDemandRefetch)).
				Dur("min_interval", c.minRefetchInterval).
				Msg("JWKS on-demand refetch suppressed: rate limit active")
			return nil, ErrRefetchRateLimited
		}
		c.mu.Unlock()

		c.logger.Info().
			Str("kid", kid).
			Msg("unknown kid encountered, fetching updated JWKS from token-service")

		newKeys, fetchErr := c.fetchKeys(ctx)
		if fetchErr != nil {
			c.mu.Lock()
			c.lastOnDemandRefetch = time.Now()
			c.mu.Unlock()
			return nil, fetchErr
		}

		c.mu.Lock()
		c.keys = newKeys
		c.lastOnDemandRefetch = time.Now()
		c.mu.Unlock()

		return nil, nil
	})

	// Check cache again after singleflight execution
	c.mu.RLock()
	key, ok := c.keys[kid]
	c.mu.RUnlock()

	if ok {
		return key, nil
	}

	if errors.Is(err, ErrRefetchRateLimited) {
		return nil, fmt.Errorf("%w: %s (refetch rate limited)", ErrKeyNotFound, kid)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to refresh JWKS for unknown kid %q: %w", kid, err)
	}

	return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, kid)
}

// Keyfunc provide jwt.Keyfunc implementation for token verification.
func (c *KeyCache) Keyfunc(token *jwt.Token) (interface{}, error) {
	if token.Method == nil || token.Method.Alg() != "ES256" {
		alg := "unknown"
		if token.Method != nil {
			alg = token.Method.Alg()
		}
		return nil, fmt.Errorf("unexpected signing method %q: expected ES256", alg)
	}

	kidRaw, ok := token.Header["kid"]
	if !ok {
		return nil, errors.New("missing 'kid' in token header")
	}

	kid, ok := kidRaw.(string)
	if !ok || kid == "" {
		return nil, errors.New("invalid 'kid' in token header: must be non-empty string")
	}

	return c.GetKey(context.Background(), kid)
}

// KeyCount returns the number of currently cached keys.
func (c *KeyCache) KeyCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.keys)
}

// Keys returns a copy of the current cached public keys map.
func (c *KeyCache) Keys() map[string]*ecdsa.PublicKey {
	c.mu.RLock()
	defer c.mu.RUnlock()
	copied := make(map[string]*ecdsa.PublicKey, len(c.keys))
	for k, v := range c.keys {
		copied[k] = v
	}
	return copied
}

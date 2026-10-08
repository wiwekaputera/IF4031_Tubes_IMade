package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

// AccessTokenClaims represents the claims expected in downstream JWT access tokens.
type AccessTokenClaims struct {
	jwt.RegisteredClaims
	Scope auth.Scope `json:"scope"`
}

// KeyProvider provides public keys for JWT validation.
type KeyProvider interface {
	Keyfunc(token *jwt.Token) (interface{}, error)
}

// Config holds configuration parameters for the JWT authentication middleware.
type Config struct {
	KeyProvider KeyProvider
	Issuer      string
	Audience    string
	Leeway      time.Duration
	SkipPaths   []string
	Logger      zerolog.Logger
}

// Middleware creates an HTTP middleware that verifies incoming JWT Bearer tokens.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	if cfg.Issuer == "" {
		cfg.Issuer = auth.DefaultIssuer
	}
	if cfg.Audience == "" {
		cfg.Audience = auth.DefaultAudience
	}
	if cfg.Leeway <= 0 {
		cfg.Leeway = 5 * time.Second
	}

	skipMap := make(map[string]bool, len(cfg.SkipPaths))
	for _, p := range cfg.SkipPaths {
		skipMap[p] = true
	}

	parserOpts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"ES256"}),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithAudience(cfg.Audience),
		jwt.WithLeeway(cfg.Leeway),
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skipMap[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeAuthError(w, http.StatusUnauthorized, auth.ErrInvalidToken, "missing Authorization header")
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], auth.TokenTypeBearer) {
				writeAuthError(w, http.StatusUnauthorized, auth.ErrInvalidToken, "authorization header must be Bearer token")
				return
			}

			tokenStr := strings.TrimSpace(parts[1])
			if tokenStr == "" {
				writeAuthError(w, http.StatusUnauthorized, auth.ErrInvalidToken, "empty bearer token")
				return
			}

			claims := &AccessTokenClaims{}
			token, err := jwt.ParseWithClaims(tokenStr, claims, cfg.KeyProvider.Keyfunc, parserOpts...)
			if err != nil || !token.Valid {
				desc := "token is invalid or expired"
				if err != nil {
					if errors.Is(err, jwt.ErrTokenExpired) {
						desc = "token has expired"
					} else if errors.Is(err, jwt.ErrTokenSignatureInvalid) {
						desc = "invalid token signature"
					}
				}
				writeAuthError(w, http.StatusUnauthorized, auth.ErrInvalidToken, desc)
				return
			}

			if claims.Subject == "" {
				writeAuthError(w, http.StatusUnauthorized, auth.ErrInvalidToken, "token missing sub claim")
				return
			}

			if claims.Scope == "" {
				writeAuthError(w, http.StatusUnauthorized, auth.ErrInvalidToken, "token missing scope claim")
				return
			}

			ctx := ContextWithClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func writeAuthError(w http.ResponseWriter, statusCode int, errCode, errDesc string) {
	w.Header().Set("WWW-Authenticate", `Bearer error="`+errCode+`"`)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(auth.OAuthError{
		Error:            errCode,
		ErrorDescription: errDesc,
	})
}

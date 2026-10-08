package auth

import (
	"context"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
)

type contextKey struct {
	name string
}

var (
	claimsContextKey = &contextKey{name: "jwt_claims"}
)

// ContextWithClaims returns a copy of parent context with the claims attached.
func ContextWithClaims(ctx context.Context, claims *AccessTokenClaims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

// ClaimsFromContext retrieves AccessTokenClaims from context if present.
func ClaimsFromContext(ctx context.Context) (*AccessTokenClaims, bool) {
	claims, ok := ctx.Value(claimsContextKey).(*AccessTokenClaims)
	return claims, ok && claims != nil
}

// ClientIDFromContext retrieves client identity (sub claim) from context.
func ClientIDFromContext(ctx context.Context) string {
	if claims, ok := ClaimsFromContext(ctx); ok {
		return claims.Subject
	}
	return ""
}

// ScopeFromContext retrieves the client scope from context.
func ScopeFromContext(ctx context.Context) auth.Scope {
	if claims, ok := ClaimsFromContext(ctx); ok {
		return claims.Scope
	}
	return ""
}

package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/token-service/internal/issuer"
)

// TokenHandler processes OAuth 2.0 token requests (RFC 6749).
func TokenHandler(iss *issuer.Issuer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")

		ct := r.Header.Get("Content-Type")
		if !strings.HasPrefix(ct, "application/x-www-form-urlencoded") {
			writeOAuthError(w, http.StatusBadRequest, auth.ErrInvalidRequest, "Content-Type must be application/x-www-form-urlencoded")
			return
		}

		if err := r.ParseForm(); err != nil {
			writeOAuthError(w, http.StatusBadRequest, auth.ErrInvalidRequest, "failed to parse form data")
			return
		}

		clientID, clientSecret, hasBasic := r.BasicAuth()
		if !hasBasic {
			clientID = r.FormValue("client_id")
			clientSecret = r.FormValue("client_secret")
		}

		if clientID == "" || clientSecret == "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="token-service"`)
			writeOAuthError(w, http.StatusUnauthorized, auth.ErrInvalidClient, "client credentials missing")
			return
		}

		grantType := r.FormValue("grant_type")
		switch grantType {
		case auth.GrantTypeClientCredentials:
			resp, err := iss.IssueClientCredentials(r.Context(), clientID, clientSecret)
			if err != nil {
				if errors.Is(err, issuer.ErrInvalidClient) {
					w.Header().Set("WWW-Authenticate", `Basic realm="token-service"`)
					writeOAuthError(w, http.StatusUnauthorized, auth.ErrInvalidClient, "invalid client credentials")
					return
				}
				writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to issue token")
				return
			}

			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)

		case auth.GrantTypeRefreshToken:
			rawRefreshToken := r.FormValue("refresh_token")
			if rawRefreshToken == "" {
				writeOAuthError(w, http.StatusBadRequest, auth.ErrInvalidRequest, "missing refresh_token parameter")
				return
			}

			resp, err := iss.IssueRefreshToken(r.Context(), clientID, clientSecret, rawRefreshToken)
			if err != nil {
				if errors.Is(err, issuer.ErrInvalidClient) {
					w.Header().Set("WWW-Authenticate", `Basic realm="token-service"`)
					writeOAuthError(w, http.StatusUnauthorized, auth.ErrInvalidClient, "invalid client credentials")
					return
				}
				if errors.Is(err, issuer.ErrUnauthorizedClient) {
					writeOAuthError(w, http.StatusBadRequest, auth.ErrUnauthorizedClient, "client is not authorized to use refresh token grant")
					return
				}
				if errors.Is(err, issuer.ErrInvalidGrant) {
					writeOAuthError(w, http.StatusBadRequest, auth.ErrInvalidGrant, "refresh token is invalid, expired, or was already used")
					return
				}
				writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to refresh token")
				return
			}

			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)

		case "":
			writeOAuthError(w, http.StatusBadRequest, auth.ErrInvalidRequest, "missing grant_type parameter")
		default:
			writeOAuthError(w, http.StatusBadRequest, auth.ErrUnsupportedGrantType, "unsupported grant_type: "+grantType)
		}
	}
}

func writeOAuthError(w http.ResponseWriter, statusCode int, errCode, description string) {
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(auth.OAuthError{
		Error:            errCode,
		ErrorDescription: description,
	})
}

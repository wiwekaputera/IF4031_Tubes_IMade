package dlq

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Routes for BNPB Internal Ops. Access requires the internal bearer token.
//
//	GET  /internal/dlq?status=parked|retrying|resolved&limit=N   list rows
//	POST /internal/dlq/replay?limit=N                            replay parked rows
const (
	pathList     = "/internal/dlq"
	pathReplay   = "/internal/dlq/replay"
	defaultLimit = 100
	maxLimit     = 1000
)

// ErrNoToken is returned when the handler is built without an internal token.
var ErrNoToken = errors.New("dlq: internal token must not be empty")

// NewHTTPHandler serves the DLQ inspection and replay routes. token guards every route.
func NewHTTPHandler(s Store, h Handler, token string) (http.Handler, error) {
	if token == "" {
		return nil, ErrNoToken
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+pathList, func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, token) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		status := r.URL.Query().Get("status")
		if status == "" {
			status = "parked"
		}
		if !validStatus(status) {
			writeError(w, http.StatusBadRequest, "status must be parked, retrying or resolved")
			return
		}
		rows, err := s.List(r.Context(), status, parseLimit(r))
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "dlq unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": rows})
	})
	mux.HandleFunc("POST "+pathReplay, func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, token) {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		res, err := Replay(ctx, s, h, parseLimit(r))
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "replay failed")
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	return mux, nil
}

func authorized(r *http.Request, token string) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

func validStatus(s string) bool {
	return s == "parked" || s == "retrying" || s == "resolved"
}

func parseLimit(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return defaultLimit
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

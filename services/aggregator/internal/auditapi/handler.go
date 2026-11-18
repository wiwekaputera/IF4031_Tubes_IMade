package auditapi

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/audit"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

const (
	defaultAuditLimit = 100
	maxAuditLimit     = 1000
)

// Handler serves the internal Audit API endpoints.
type Handler struct {
	repo       store.CanonicalRepo
	logger     zerolog.Logger
	auditToken []byte
}

// New constructs an auditapi Handler.
func New(repo store.CanonicalRepo, auditToken string, logger zerolog.Logger) *Handler {
	return &Handler{
		repo:       repo,
		logger:     logger.With().Str("component", "auditapi").Logger(),
		auditToken: []byte(auditToken),
	}
}

// RegisterRoutes attaches the Audit API routes to mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/internal/audit/events", h.requireAuth(h.Events))
	mux.HandleFunc("/internal/audit/checksum", h.requireAuth(h.Checksum))
	mux.HandleFunc("/internal/audit/location", h.requireAuth(h.Location))
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Error().Err(err).Msg("failed to encode JSON response")
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	h.writeJSON(w, status, audit.ErrorBody{
		Error: audit.ErrorDetail{
			Code:    code,
			Message: message,
		},
	})
}

func (h *Handler) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) || len(h.auditToken) == 0 {
			h.writeError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		token := auth[len(prefix):]
		if subtle.ConstantTimeCompare([]byte(token), h.auditToken) != 1 {
			h.writeError(w, http.StatusUnauthorized, "unauthorized", "token tidak valid")
			return
		}
		next(w, r)
	}
}

// Events handles GET /internal/audit/events.
func (h *Handler) Events(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	source := q.Get("source")
	if source != "" && !hazard.Source(source).Valid() {
		h.writeError(w, http.StatusBadRequest, "bad_request", "parameter source tidak valid")
		return
	}

	limit := defaultAuditLimit
	if limStr := q.Get("limit"); limStr != "" {
		lim, err := strconv.Atoi(limStr)
		if err != nil || lim < 1 || lim > maxAuditLimit {
			h.writeError(w, http.StatusBadRequest, "bad_request", "parameter limit harus antara 1 dan 1000")
			return
		}
		limit = lim
	}

	var sincePtr *time.Time
	if sinceStr := q.Get("since"); sinceStr != "" {
		t, err := time.Parse(time.RFC3339, sinceStr)
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, sinceStr)
		}
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "bad_request", "parameter since harus berformat ISO 8601")
			return
		}
		sinceUTC := t.UTC()
		sincePtr = &sinceUTC
	}

	ctx := store.WithCorrelationID(r.Context(), r.Header.Get("X-Correlation-ID"))
	page, err := h.repo.ListAuditEvents(ctx, store.AuditEventsFilter{
		Source: source,
		Since:  sincePtr,
		Limit:  limit,
		Cursor: q.Get("cursor"),
	})
	if err != nil {
		if errors.Is(err, store.ErrInvalidCursor) {
			h.writeError(w, http.StatusBadRequest, "bad_request", "cursor tidak valid")
			return
		}
		h.logger.Error().Err(err).Msg("failed to list audit events")
		h.writeError(w, http.StatusInternalServerError, "internal_error", "gagal mengambil audit events")
		return
	}

	h.writeJSON(w, http.StatusOK, page)
}

// Checksum handles GET /internal/audit/checksum.
func (h *Handler) Checksum(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	source := r.URL.Query().Get("source")
	if source != "" && !hazard.Source(source).Valid() {
		h.writeError(w, http.StatusBadRequest, "bad_request", "parameter source tidak valid")
		return
	}

	ctx := store.WithCorrelationID(r.Context(), r.Header.Get("X-Correlation-ID"))
	cs, err := h.repo.ComputeChecksum(ctx, source)
	if err != nil {
		h.logger.Error().Err(err).Msg("failed to compute audit checksum")
		h.writeError(w, http.StatusInternalServerError, "internal_error", "gagal menghitung checksum")
		return
	}

	h.writeJSON(w, http.StatusOK, cs)
}

// Location handles GET /internal/audit/location.
func (h *Handler) Location(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	hazardID := r.URL.Query().Get("hazard_id")
	if hazardID == "" {
		h.writeError(w, http.StatusBadRequest, "bad_request", "parameter hazard_id wajib diisi")
		return
	}

	ctx := store.WithCorrelationID(r.Context(), r.Header.Get("X-Correlation-ID"))
	loc, err := h.repo.LocateEvent(ctx, hazardID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			h.writeError(w, http.StatusNotFound, "not_found", "event tidak ditemukan")
			return
		}
		h.logger.Error().Err(err).Msg("failed to locate event")
		h.writeError(w, http.StatusInternalServerError, "internal_error", "gagal mencari lokasi event")
		return
	}

	h.writeJSON(w, http.StatusOK, loc)
}

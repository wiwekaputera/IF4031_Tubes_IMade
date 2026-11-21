package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/client-api/internal/fieldreports"
	contractsauth "github.com/wiwekaputera/if4031-tubes-imade/contracts/auth"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/fieldreport"
)

// FieldReportResponse is the 202 body.
type FieldReportResponse struct {
	ReportID  string `json:"report_id"`
	Status    string `json:"status"`
	Duplicate bool   `json:"duplicate"`
}

// FieldReportsOptions configures FieldReportsHandler.
type FieldReportsOptions struct {
	Publisher      fieldreports.Publisher
	PublishTimeout time.Duration
	Logger         zerolog.Logger
	Now            func() time.Time
}

// writeFieldReportScopes are the scopes allowed to submit field reports.
var writeFieldReportScopes = map[contractsauth.Scope]bool{
	contractsauth.ScopeFieldTeam:   true,
	contractsauth.ScopeInternalOps: true,
}

// FieldReportsHandler handles POST /field-reports.
//
// The report is acknowledged with 202 only after it is persisted in JetStream,
// so a 202 is never lost even if client-api or any downstream component dies
// right after answering. The client's Idempotency-Key (or report_id) becomes
// the JetStream message ID: a retry of the same report is answered 202 again
// but stored once.
func FieldReportsHandler(opts FieldReportsOptions) http.HandlerFunc {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = 2 * time.Second
	}

	return func(w http.ResponseWriter, r *http.Request) {
		corrID := CorrelationIDFromContext(r.Context())
		if corrID != "" {
			w.Header().Set("X-Correlation-ID", corrID)
		}

		scope := auth.ScopeFromContext(r.Context())
		if !writeFieldReportScopes[scope] {
			w.Header().Set("WWW-Authenticate", `Bearer error="insufficient_scope"`)
			writeError(w, http.StatusForbidden, contractsauth.ErrInsufficientScope,
				"only field team clients may submit field reports")
			return
		}

		if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
			writeError(w, http.StatusUnsupportedMediaType, contractsauth.ErrInvalidRequest,
				"Content-Type must be application/json")
			return
		}

		submission, err := decodeSubmission(w, r)
		if err != nil {
			status := http.StatusBadRequest
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			writeError(w, status, contractsauth.ErrInvalidRequest, err.Error())
			return
		}

		reportID, err := resolveReportID(r.Header.Get("Idempotency-Key"), submission.ReportID)
		if err != nil {
			writeError(w, http.StatusBadRequest, contractsauth.ErrInvalidRequest, err.Error())
			return
		}

		now := opts.Now()
		if err := submission.Validate(now); err != nil {
			writeError(w, http.StatusUnprocessableEntity, contractsauth.ErrInvalidRequest, err.Error())
			return
		}

		report := submission.ToReport(reportID, auth.ClientIDFromContext(r.Context()), corrID, now)

		ctx, cancel := context.WithTimeout(r.Context(), opts.PublishTimeout)
		defer cancel()
		duplicate, err := opts.Publisher.Publish(ctx, report)
		if err != nil {
			opts.Logger.Error().Ctx(r.Context()).Err(err).
				Str("report_id", reportID).Str("correlation_id", corrID).
				Msg("field report not stored; client must retry")
			w.Header().Set("Retry-After", "2")
			writeError(w, http.StatusServiceUnavailable, "temporarily_unavailable",
				"the report was not stored; retry with the same Idempotency-Key")
			return
		}

		opts.Logger.Info().Ctx(r.Context()).
			Str("report_id", reportID).Str("message_id", report.MessageID()).
			Str("correlation_id", corrID).Bool("duplicate", duplicate).
			Msg("field report accepted")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(FieldReportResponse{
			ReportID: reportID, Status: "accepted", Duplicate: duplicate,
		})
	}
}

func decodeSubmission(w http.ResponseWriter, r *http.Request) (fieldreport.Submission, error) {
	var s fieldreport.Submission
	body := http.MaxBytesReader(w, r.Body, fieldreport.MaxBodyBytes)
	defer body.Close()
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return s, err
		}
		return s, errors.New("body must be one valid JSON field report: " + err.Error())
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return s, errors.New("body must contain exactly one JSON object")
	}
	return s, nil
}

// resolveReportID picks the idempotency key from the header or the body.
func resolveReportID(header, body string) (string, error) {
	header, body = strings.TrimSpace(header), strings.TrimSpace(body)
	if header != "" && body != "" && header != body {
		return "", errors.New("Idempotency-Key header and report_id differ")
	}
	id := header
	if id == "" {
		id = body
	}
	return id, fieldreport.ValidateReportID(id)
}

func writeError(w http.ResponseWriter, status int, code, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(contractsauth.OAuthError{Error: code, ErrorDescription: description})
}

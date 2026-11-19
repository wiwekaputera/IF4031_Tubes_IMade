package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/fieldreport"
)

// receivedReport is one field report delivered to the BNPB operations receiver.
type receivedReport struct {
	MessageID  string             `json:"message_id"`
	ReceivedAt time.Time          `json:"received_at"`
	Report     fieldreport.Report `json:"report"`
}

// reportInbox stands in for the operations desk that receives field reports.
// It keeps each report once, keyed by its message ID, and counts the repeated
// deliveries it ignored so the end-to-end check can show none was processed twice.
type reportInbox struct {
	mu         sync.RWMutex
	byID       map[string]receivedReport
	order      []string
	duplicates int
}

func newReportInbox() *reportInbox {
	return &reportInbox{byID: make(map[string]receivedReport)}
}

func (s *sink) receiveFieldReport(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxNotifyBody)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	var report fieldreport.Report
	if err := decoder.Decode(&report); err != nil {
		http.Error(w, "request body must be a valid field report", http.StatusBadRequest)
		return
	}
	if err := ensureEOF(decoder); err != nil {
		http.Error(w, "request body must contain one JSON field report", http.StatusBadRequest)
		return
	}
	if report.ReportID == "" || report.ClientID == "" || key != report.MessageID() {
		http.Error(w, "Idempotency-Key must match client_id/report_id", http.StatusBadRequest)
		return
	}

	in := s.reports
	in.mu.Lock()
	if _, exists := in.byID[key]; exists {
		in.duplicates++
		in.mu.Unlock()
		s.logger.Info().Str("message_id", key).Str("correlation_id", report.CorrelationID).
			Msg("duplicate field report ignored")
		writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate", "message_id": key})
		return
	}
	in.byID[key] = receivedReport{MessageID: key, ReceivedAt: time.Now().UTC(), Report: report}
	in.order = append(in.order, key)
	in.mu.Unlock()

	s.logger.Info().Str("message_id", key).Str("correlation_id", report.CorrelationID).
		Str("severity", report.Severity).Msg("field report received")
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "message_id": key})
}

func (s *sink) listFieldReports(w http.ResponseWriter, _ *http.Request) {
	in := s.reports
	in.mu.RLock()
	items := make([]receivedReport, 0, len(in.order))
	for _, key := range in.order {
		items = append(items, in.byID[key])
	}
	duplicates := in.duplicates
	in.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"count":              len(items),
		"duplicates_ignored": duplicates,
		"data":               items,
	})
}

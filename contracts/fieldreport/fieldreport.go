// Package fieldreport is the contract for reports sent by field teams through
// client-api and processed by field-report-consumer.
package fieldreport

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// JetStream names shared by client-api (publisher) and field-report-consumer.
const (
	StreamName    = "FIELD_REPORTS"
	Subject       = "field.reports.submitted"
	SubjectFilter = "field.reports.>"
)

// Limits on the request accepted from a field team.
const (
	MaxBodyBytes       = 64 << 10
	MaxDescriptionLen  = 4000
	MaxAreaNameLen     = 200
	MaxReportIDLen     = 128
	MaxReportedAtSkew  = 10 * time.Minute
	MaxReportedAtDelay = 7 * 24 * time.Hour
)

// Categories a field report can carry.
var Categories = map[string]bool{
	"DAMAGE":     true,
	"CASUALTY":   true,
	"EVACUATION": true,
	"LOGISTICS":  true,
	"HAZARD":     true,
	"OTHER":      true,
}

// Severities a field report can carry.
var Severities = map[string]bool{
	"LOW":      true,
	"MEDIUM":   true,
	"HIGH":     true,
	"CRITICAL": true,
}

var reportIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

// Submission is the JSON body a field team posts.
type Submission struct {
	ReportID    string    `json:"report_id,omitempty"`
	Category    string    `json:"category"`
	Severity    string    `json:"severity"`
	AreaName    string    `json:"area_name"`
	Latitude    *float64  `json:"latitude"`
	Longitude   *float64  `json:"longitude"`
	Description string    `json:"description"`
	ReportedAt  time.Time `json:"reported_at"`
}

// Report is the message stored in JetStream once client-api accepted it.
type Report struct {
	ReportID      string    `json:"report_id"`
	ClientID      string    `json:"client_id"`
	Category      string    `json:"category"`
	Severity      string    `json:"severity"`
	AreaName      string    `json:"area_name"`
	Latitude      float64   `json:"latitude"`
	Longitude     float64   `json:"longitude"`
	Description   string    `json:"description"`
	ReportedAt    time.Time `json:"reported_at"`
	AcceptedAt    time.Time `json:"accepted_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
}

// MessageID is the JetStream Nats-Msg-Id and the consumer's dedup key. It is
// scoped by client so two teams can never collide on the same report_id.
func (r Report) MessageID() string {
	return r.ClientID + "/" + r.ReportID
}

// ValidateReportID checks an idempotency key / report_id.
func ValidateReportID(id string) error {
	switch {
	case id == "":
		return errors.New("report_id (or the Idempotency-Key header) is required")
	case len(id) > MaxReportIDLen:
		return fmt.Errorf("report_id must be at most %d characters", MaxReportIDLen)
	case !reportIDPattern.MatchString(id):
		return errors.New("report_id may only contain letters, digits and . _ : -")
	}
	return nil
}

// Validate checks a submission against the contract. now is the server time
// used to reject reports dated in the future or too far in the past.
func (s Submission) Validate(now time.Time) error {
	var problems []string
	if !Categories[s.Category] {
		problems = append(problems, "category must be one of DAMAGE, CASUALTY, EVACUATION, LOGISTICS, HAZARD, OTHER")
	}
	if !Severities[s.Severity] {
		problems = append(problems, "severity must be one of LOW, MEDIUM, HIGH, CRITICAL")
	}
	area := strings.TrimSpace(s.AreaName)
	if area == "" || utf8.RuneCountInString(area) > MaxAreaNameLen {
		problems = append(problems, fmt.Sprintf("area_name is required (max %d characters)", MaxAreaNameLen))
	}
	if s.Latitude == nil || math.IsNaN(*s.Latitude) || *s.Latitude < -90 || *s.Latitude > 90 {
		problems = append(problems, "latitude is required and must be between -90 and 90")
	}
	if s.Longitude == nil || math.IsNaN(*s.Longitude) || *s.Longitude < -180 || *s.Longitude > 180 {
		problems = append(problems, "longitude is required and must be between -180 and 180")
	}
	desc := strings.TrimSpace(s.Description)
	if desc == "" || utf8.RuneCountInString(desc) > MaxDescriptionLen {
		problems = append(problems, fmt.Sprintf("description is required (max %d characters)", MaxDescriptionLen))
	}
	switch {
	case s.ReportedAt.IsZero():
		problems = append(problems, "reported_at is required (RFC 3339)")
	case s.ReportedAt.After(now.Add(MaxReportedAtSkew)):
		problems = append(problems, "reported_at is in the future")
	case s.ReportedAt.Before(now.Add(-MaxReportedAtDelay)):
		problems = append(problems, "reported_at is more than 7 days old")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// ToReport builds the stored report from a validated submission.
func (s Submission) ToReport(reportID, clientID, correlationID string, acceptedAt time.Time) Report {
	return Report{
		ReportID:      reportID,
		ClientID:      clientID,
		Category:      s.Category,
		Severity:      s.Severity,
		AreaName:      strings.TrimSpace(s.AreaName),
		Latitude:      *s.Latitude,
		Longitude:     *s.Longitude,
		Description:   strings.TrimSpace(s.Description),
		ReportedAt:    s.ReportedAt.UTC(),
		AcceptedAt:    acceptedAt.UTC(),
		CorrelationID: correlationID,
	}
}

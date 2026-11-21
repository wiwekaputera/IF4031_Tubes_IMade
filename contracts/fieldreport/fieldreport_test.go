package fieldreport

import (
	"strings"
	"testing"
	"time"
)

func ptr(f float64) *float64 { return &f }

func validSubmission(now time.Time) Submission {
	return Submission{
		Category:    "DAMAGE",
		Severity:    "HIGH",
		AreaName:    "Palu, Sulawesi Tengah",
		Latitude:    ptr(-0.9),
		Longitude:   ptr(119.87),
		Description: "Jembatan utama putus, akses kendaraan terhenti",
		ReportedAt:  now.Add(-time.Minute),
	}
}

func TestValidateAcceptsValidSubmission(t *testing.T) {
	now := time.Now()
	if err := validSubmission(now).Validate(now); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
}

func TestValidateRejectsBadFields(t *testing.T) {
	now := time.Now()
	cases := map[string]func(*Submission){
		"category":    func(s *Submission) { s.Category = "FLOOD!" },
		"severity":    func(s *Submission) { s.Severity = "" },
		"area_name":   func(s *Submission) { s.AreaName = "   " },
		"latitude":    func(s *Submission) { s.Latitude = ptr(91) },
		"missing lat": func(s *Submission) { s.Latitude = nil },
		"longitude":   func(s *Submission) { s.Longitude = ptr(-181) },
		"description": func(s *Submission) { s.Description = strings.Repeat("x", MaxDescriptionLen+1) },
		"reported_at": func(s *Submission) { s.ReportedAt = time.Time{} },
		"future":      func(s *Submission) { s.ReportedAt = now.Add(time.Hour) },
		"too old":     func(s *Submission) { s.ReportedAt = now.Add(-8 * 24 * time.Hour) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSubmission(now)
			mutate(&s)
			if err := s.Validate(now); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestValidateReportID(t *testing.T) {
	for _, ok := range []string{"rpt-001", "2026-11-20T01:02:03Z.palu", "a_b.c:d"} {
		if err := ValidateReportID(ok); err != nil {
			t.Errorf("%q: unexpected error %v", ok, err)
		}
	}
	for _, bad := range []string{"", "has space", "slash/inside", strings.Repeat("a", MaxReportIDLen+1)} {
		if err := ValidateReportID(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestMessageIDIsScopedByClient(t *testing.T) {
	a := Report{ReportID: "r1", ClientID: "team-a"}
	b := Report{ReportID: "r1", ClientID: "team-b"}
	if a.MessageID() == b.MessageID() {
		t.Fatal("same report_id from different clients must not share a message ID")
	}
}

func TestToReportTrimsAndNormalisesTime(t *testing.T) {
	now := time.Now()
	s := validSubmission(now)
	s.AreaName = "  Palu  "
	r := s.ToReport("r1", "team-a", "corr-1", now)
	if r.AreaName != "Palu" || r.ReportedAt.Location() != time.UTC || r.Latitude != -0.9 {
		t.Fatalf("unexpected report: %+v", r)
	}
}

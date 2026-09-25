package seed

import (
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

type seedItem struct {
	report agency.VolcanicReport
}

var seedItems = []seedItem{
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-001",
		VolcanoID:        "GNG-001",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2024-01-02T08:00:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-002",
		VolcanoID:        "GNG-001",
		AlertLevel:       agency.AlertWaspada,
		EruptionCount24h: 1,
		AshColumnHeightM: 200,
		ReportedAt:       "2024-02-10T14:30:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-003",
		VolcanoID:        "GNG-001",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 2,
		AshColumnHeightM: 800,
		ReportedAt:       "2024-03-15T09:15:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-004",
		VolcanoID:        "GNG-002",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2024-01-05T06:00:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-005",
		VolcanoID:        "GNG-002",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2024-04-20T12:00:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-006",
		VolcanoID:        "GNG-002",
		AlertLevel:       agency.AlertWaspada,
		EruptionCount24h: 1,
		AshColumnHeightM: 350,
		ReportedAt:       "2024-05-12T18:45:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-007",
		VolcanoID:        "GNG-003",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2024-02-18T07:30:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-008",
		VolcanoID:        "GNG-003",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 3,
		AshColumnHeightM: 1200,
		ReportedAt:       "2024-06-08T16:20:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-009",
		VolcanoID:        "GNG-003",
		AlertLevel:       agency.AlertAwas,
		EruptionCount24h: 5,
		AshColumnHeightM: 5000,
		ReportedAt:       "2024-07-01T03:10:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-010",
		VolcanoID:        "GNG-004",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2024-01-15T10:00:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-011",
		VolcanoID:        "GNG-004",
		AlertLevel:       agency.AlertWaspada,
		EruptionCount24h: 1,
		AshColumnHeightM: 450,
		ReportedAt:       "2024-03-22T15:40:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-012",
		VolcanoID:        "GNG-004",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 2,
		AshColumnHeightM: 900,
		ReportedAt:       "2024-08-14T11:25:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-013",
		VolcanoID:        "GNG-005",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2024-02-01T09:00:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-014",
		VolcanoID:        "GNG-005",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2024-04-11T14:00:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-015",
		VolcanoID:        "GNG-005",
		AlertLevel:       agency.AlertWaspada,
		EruptionCount24h: 1,
		AshColumnHeightM: 600,
		ReportedAt:       "2024-05-30T20:15:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-016",
		VolcanoID:        "GNG-005",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 2,
		AshColumnHeightM: 1100,
		ReportedAt:       "2024-09-03T05:50:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-017",
		VolcanoID:        "GNG-001",
		AlertLevel:       agency.AlertAwas,
		EruptionCount24h: 4,
		AshColumnHeightM: 3500,
		ReportedAt:       "2023-11-28T22:30:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-018",
		VolcanoID:        "GNG-003",
		AlertLevel:       agency.AlertWaspada,
		EruptionCount24h: 1,
		AshColumnHeightM: 500,
		ReportedAt:       "2023-12-14T13:05:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-019",
		VolcanoID:        "GNG-002",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 3,
		AshColumnHeightM: 1500,
		ReportedAt:       "2023-10-09T08:20:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-020",
		VolcanoID:        "GNG-004",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2023-08-17T17:00:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-021",
		VolcanoID:        "GNG-005",
		AlertLevel:       agency.AlertNormal,
		EruptionCount24h: 0,
		AshColumnHeightM: 0,
		ReportedAt:       "2023-09-21T10:30:00Z",
	}},
	{report: agency.VolcanicReport{
		ReportID:         "rpt-pvmbg-hist-022",
		VolcanoID:        "GNG-001",
		AlertLevel:       agency.AlertSiaga,
		EruptionCount24h: 2,
		AshColumnHeightM: 750,
		ReportedAt:       "2023-07-04T19:45:00Z",
	}},
}

// PopulateSeeds populates the given store with at least 20 historical realistic
// volcanic reports using volcano IDs that match the volcano_reference table.
func PopulateSeeds(s store.Store) {
	now := time.Now().UTC()
	for _, item := range seedItems {
		rpt := item.report
		rpt.ConfidenceLevel = nil
		s.AddVolcanicReport(rpt)
	}
	_ = now
}

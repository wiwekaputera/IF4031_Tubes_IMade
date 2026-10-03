package normalizer

import (
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/agency"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

func SeismicSeverity(magnitude float64, hasTsunami bool, tsunamiLevel agency.TsunamiThreatLevel) hazard.Severity {
	if hasTsunami {
		switch tsunamiLevel {
		case agency.ThreatAwas:
			return hazard.SeverityAwas
		case agency.ThreatSiaga:
			return hazard.SeveritySiaga
		case agency.ThreatWaspada:
			return hazard.SeverityWaspada
		}
	}

	if magnitude < 5.0 {
		return hazard.SeverityNormal
	}
	if magnitude < 6.5 {
		return hazard.SeverityWaspada
	}
	return hazard.SeveritySiaga
}

func VolcanicSeverity(alertLevel agency.VolcanicAlertLevel) hazard.Severity {
	switch alertLevel {
	case agency.AlertNormal:
		return hazard.SeverityNormal
	case agency.AlertWaspada:
		return hazard.SeverityWaspada
	case agency.AlertSiaga:
		return hazard.SeveritySiaga
	case agency.AlertAwas:
		return hazard.SeverityAwas
	}
	return hazard.SeverityNormal
}

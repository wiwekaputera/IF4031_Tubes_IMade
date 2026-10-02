package generator

import "github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"

type volcanoTemplate struct {
	id     string
	name   string
	lat    float64
	lon    float64
	alerts []model.VolcanicAlertLevel
}

var volcanoTemplates = []volcanoTemplate{
	{id: "GNG-001", name: "Gunung Merapi", lat: -7.5407, lon: 110.4457, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga, model.AlertAwas}},
	{id: "GNG-002", name: "Gunung Semeru", lat: -8.1080, lon: 112.9220, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga}},
	{id: "GNG-003", name: "Gunung Krakatau", lat: -6.1020, lon: 105.4230, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada}},
	{id: "GNG-004", name: "Gunung Rinjani", lat: -8.4167, lon: 116.4667, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga}},
	{id: "GNG-005", name: "Gunung Sinabung", lat: 3.1700, lon: 98.3920, alerts: []model.VolcanicAlertLevel{model.AlertNormal, model.AlertWaspada, model.AlertSiaga, model.AlertAwas}},
}

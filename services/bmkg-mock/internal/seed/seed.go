package seed

import (
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/agency"
)

type seedItem struct {
	event   agency.SeismicEvent
	warning *agency.TsunamiWarning
}

// PopulateSeeds populates the given store with at least 20 seismic events.
func PopulateSeeds(s store.Store) {
	seeds := []seedItem{
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-001",
				Magnitude:        7.5,
				DepthKm:          10.0,
				EpicenterLat:     -0.8988,
				EpicenterLon:     119.8474,
				RegionName:       "Sulawesi Tengah (Palu)",
				OccurredAt:       "2018-09-28T10:02:43Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-001",
				RelatedEventID:   "evt-bmkg-hist-001",
				ThreatLevel:      agency.ThreatSiaga,
				AffectedZones:    []string{"Palu", "Donggala"},
				EstimatedArrival: "2018-09-28T10:25:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-002",
				Magnitude:        9.1,
				DepthKm:          30.0,
				EpicenterLat:     3.3160,
				EpicenterLon:     95.8540,
				RegionName:       "Aceh (Samudera Hindia)",
				OccurredAt:       "2004-12-26T00:58:53Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-002",
				RelatedEventID:   "evt-bmkg-hist-002",
				ThreatLevel:      agency.ThreatAwas,
				AffectedZones:    []string{"Banda Aceh", "Meulaboh", "Aceh Barat"},
				EstimatedArrival: "2004-12-26T01:30:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-003",
				Magnitude:        6.3,
				DepthKm:          12.0,
				EpicenterLat:     -7.9620,
				EpicenterLon:     110.4580,
				RegionName:       "D.I. Yogyakarta (Bantul)",
				OccurredAt:       "2006-05-27T05:54:00Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-004",
				Magnitude:        7.8,
				DepthKm:          20.0,
				EpicenterLat:     -3.4840,
				EpicenterLon:     100.1140,
				RegionName:       "Sumatera Barat (Mentawai)",
				OccurredAt:       "2010-10-25T14:42:22Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-004",
				RelatedEventID:   "evt-bmkg-hist-004",
				ThreatLevel:      agency.ThreatAwas,
				AffectedZones:    []string{"Kepulauan Mentawai", "Padang"},
				EstimatedArrival: "2010-10-25T15:20:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-005",
				Magnitude:        6.2,
				DepthKm:          55.0,
				EpicenterLat:     -9.7990,
				EpicenterLon:     114.5300,
				RegionName:       "Bali (Nusa Dua)",
				OccurredAt:       "2011-10-13T03:16:00Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-006",
				Magnitude:        6.9,
				DepthKm:          31.0,
				EpicenterLat:     -8.2870,
				EpicenterLon:     116.4510,
				RegionName:       "Lombok, NTB",
				OccurredAt:       "2018-08-05T11:46:35Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-006",
				RelatedEventID:   "evt-bmkg-hist-006",
				ThreatLevel:      agency.ThreatWaspada,
				AffectedZones:    []string{"Lombok Barat", "Lombok Utara"},
				EstimatedArrival: "2018-08-05T12:05:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-007",
				Magnitude:        5.6,
				DepthKm:          10.0,
				EpicenterLat:     -6.8340,
				EpicenterLon:     107.0950,
				RegionName:       "Jawa Barat (Cianjur)",
				OccurredAt:       "2022-11-21T06:21:10Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-008",
				Magnitude:        7.6,
				DepthKm:          81.0,
				EpicenterLat:     -0.7250,
				EpicenterLon:     99.8670,
				RegionName:       "Sumatera Barat (Padang)",
				OccurredAt:       "2009-09-30T10:16:09Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-009",
				Magnitude:        7.8,
				DepthKm:          28.0,
				EpicenterLat:     -8.4800,
				EpicenterLon:     122.3380,
				RegionName:       "Nusa Tenggara Timur (Flores)",
				OccurredAt:       "1992-12-12T05:29:00Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-009",
				RelatedEventID:   "evt-bmkg-hist-009",
				ThreatLevel:      agency.ThreatAwas,
				AffectedZones:    []string{"Maumere", "Flores Timur"},
				EstimatedArrival: "1992-12-12T06:00:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-010",
				Magnitude:        6.2,
				DepthKm:          18.0,
				EpicenterLat:     -2.9770,
				EpicenterLon:     118.9030,
				RegionName:       "Sulawesi Barat (Majene)",
				OccurredAt:       "2021-01-14T18:28:17Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-011",
				Magnitude:        6.5,
				DepthKm:          18.0,
				EpicenterLat:     -3.6860,
				EpicenterLon:     128.3370,
				RegionName:       "Maluku (Ambon)",
				OccurredAt:       "2019-09-25T23:46:44Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-012",
				Magnitude:        8.6,
				DepthKm:          30.0,
				EpicenterLat:     2.0740,
				EpicenterLon:     97.0130,
				RegionName:       "Sumatera Utara (Nias)",
				OccurredAt:       "2005-03-28T16:09:36Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-012",
				RelatedEventID:   "evt-bmkg-hist-012",
				ThreatLevel:      agency.ThreatAwas,
				AffectedZones:    []string{"Gunungsitoli", "Nias Selatan"},
				EstimatedArrival: "2005-03-28T16:45:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-013",
				Magnitude:        7.7,
				DepthKm:          34.0,
				EpicenterLat:     -9.2840,
				EpicenterLon:     107.4190,
				RegionName:       "Jawa Barat (Pangandaran)",
				OccurredAt:       "2006-07-17T08:19:28Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-013",
				RelatedEventID:   "evt-bmkg-hist-013",
				ThreatLevel:      agency.ThreatSiaga,
				AffectedZones:    []string{"Pangandaran", "Cilacap"},
				EstimatedArrival: "2006-07-17T09:00:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-014",
				Magnitude:        5.4,
				DepthKm:          10.0,
				EpicenterLat:     -2.5330,
				EpicenterLon:     140.6830,
				RegionName:       "Papua (Jayapura)",
				OccurredAt:       "2023-02-09T06:28:02Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-015",
				Magnitude:        6.1,
				DepthKm:          82.0,
				EpicenterLat:     -8.9500,
				EpicenterLon:     112.4800,
				RegionName:       "Jawa Timur (Malang)",
				OccurredAt:       "2021-04-10T07:00:16Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-016",
				Magnitude:        8.4,
				DepthKm:          34.0,
				EpicenterLat:     -4.5200,
				EpicenterLon:     101.3740,
				RegionName:       "Bengkulu",
				OccurredAt:       "2007-09-12T11:10:26Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-016",
				RelatedEventID:   "evt-bmkg-hist-016",
				ThreatLevel:      agency.ThreatSiaga,
				AffectedZones:    []string{"Kota Bengkulu", "Mukomuko"},
				EstimatedArrival: "2007-09-12T11:50:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-017",
				Magnitude:        6.0,
				DepthKm:          15.0,
				EpicenterLat:     -8.1500,
				EpicenterLon:     124.9500,
				RegionName:       "Nusa Tenggara Timur (Alor)",
				OccurredAt:       "2004-11-12T04:26:00Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-018",
				Magnitude:        7.1,
				DepthKm:          24.0,
				EpicenterLat:     -3.5700,
				EpicenterLon:     135.5600,
				RegionName:       "Papua (Nabire)",
				OccurredAt:       "2004-11-26T02:25:00Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-019",
				Magnitude:        5.9,
				DepthKm:          10.0,
				EpicenterLat:     -9.9200,
				EpicenterLon:     120.2100,
				RegionName:       "Nusa Tenggara Timur (Sumba)",
				OccurredAt:       "2018-10-02T00:16:00Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-020",
				Magnitude:        7.3,
				DepthKm:          40.0,
				EpicenterLat:     1.8800,
				EpicenterLon:     126.5200,
				RegionName:       "Sulawesi Utara (Laut Maluku)",
				OccurredAt:       "2014-11-15T02:31:41Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-020",
				RelatedEventID:   "evt-bmkg-hist-020",
				ThreatLevel:      agency.ThreatWaspada,
				AffectedZones:    []string{"Bitung", "Manado"},
				EstimatedArrival: "2014-11-15T03:00:00Z",
			},
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-021",
				Magnitude:        7.2,
				DepthKm:          10.0,
				EpicenterLat:     -0.5300,
				EpicenterLon:     128.0200,
				RegionName:       "Maluku Utara (Halmahera)",
				OccurredAt:       "2019-07-14T09:10:50Z",
				PotentialTsunami: false,
			},
			warning: nil,
		},
		{
			event: agency.SeismicEvent{
				EventID:          "evt-bmkg-hist-022",
				Magnitude:        6.9,
				DepthKm:          52.0,
				EpicenterLat:     -7.5400,
				EpicenterLon:     104.5800,
				RegionName:       "Banten (Sumur)",
				OccurredAt:       "2019-08-02T12:03:25Z",
				PotentialTsunami: true,
			},
			warning: &agency.TsunamiWarning{
				WarningID:        "warn-bmkg-hist-022",
				RelatedEventID:   "evt-bmkg-hist-022",
				ThreatLevel:      agency.ThreatWaspada,
				AffectedZones:    []string{"Pandeglang", "Lebak"},
				EstimatedArrival: "2019-08-02T12:40:00Z",
			},
		},
	}

	for _, sItem := range seeds {
		s.AddSeismicEvent(sItem.event)
		if sItem.warning != nil {
			t, err := time.Parse(time.RFC3339, sItem.event.OccurredAt)
			if err != nil {
				t = time.Now().UTC()
			}
			s.AddTsunamiWarning(*sItem.warning, t)
		}
	}
}

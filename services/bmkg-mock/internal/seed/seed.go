package seed

import (
	_ "embed"
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/agency"
)

//go:embed data/seismic_seeds.json
var seismicSeedsJSON []byte

type seedItem struct {
	Event   agency.SeismicEvent    `json:"event"`
	Warning *agency.TsunamiWarning `json:"warning"`
}

// PopulateSeeds populates the given store with at least 20 seismic events.
func PopulateSeeds(s store.Store) {
	var seeds []seedItem
	if err := json.Unmarshal(seismicSeedsJSON, &seeds); err != nil {
		log.Fatal().Err(err).Msg("failed to unmarshal embedded seismic seed data")
	}

	for _, sItem := range seeds {
		s.AddSeismicEvent(sItem.Event)
		if sItem.Warning != nil {
			t, err := time.Parse(time.RFC3339, sItem.Event.OccurredAt)
			if err != nil {
				t = time.Now().UTC()
			}
			s.AddTsunamiWarning(*sItem.Warning, t)
		}
	}
}

package seed

import (
	_ "embed"
	"encoding/json"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/model"
	"github.com/wiwekaputera/if4031-tubes-imade/pvmbg-mock/internal/store"
)

//go:embed data/volcanic_seeds.json
var volcanicSeedsJSON []byte

// PopulateSeeds populates the given store with at least 20 historical realistic
// volcanic reports using volcano IDs that match the volcano_reference table.
func PopulateSeeds(s store.Store) {
	var reports []model.VolcanicReport
	if err := json.Unmarshal(volcanicSeedsJSON, &reports); err != nil {
		log.Fatal().Err(err).Msg("failed to unmarshal embedded volcanic seed data")
	}

	now := time.Now().UTC()
	for _, rpt := range reports {
		rpt.ConfidenceLevel = nil
		s.AddVolcanicReport(rpt)
	}
	_ = now
}

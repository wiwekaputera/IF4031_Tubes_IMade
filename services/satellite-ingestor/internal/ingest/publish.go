package ingest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/contracts/event"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/area"
	"github.com/wiwekaputera/if4031-tubes-imade/satellite-ingestor/internal/fire"
)

// Outgoing is one message waiting in the outbox. It is written in the same
// transaction as the fire change it describes and published later by the relay.
type Outgoing struct {
	MessageID string
	Subject   string
	Payload   []byte
}

// messageID is unique per fire and version, and the same on every replay, so
// JetStream drops a message that is published twice.
func messageID(sourceRefID string, version int64) string {
	return fmt.Sprintf("%s:%s:%d", hazard.SourceSatelit, sourceRefID, version)
}

// upsertMessage maps a fire to a WILDFIRE hazard event. version is the feed
// position that produced this state; it only grows, so the aggregator can
// ignore stale or repeated deliveries.
func upsertMessage(f fire.Fire, version int64) (Outgoing, error) {
	m := event.IngestMessage{
		MessageID:     messageID(f.SourceRefID, version),
		Op:            event.IngestOpUpsert,
		Source:        hazard.SourceSatelit,
		SourceRefID:   f.SourceRefID,
		SourceVersion: version,
		HazardType:    hazard.HazardTypeWildfire,
		Severity:      hazard.Severity(f.Severity),
		AreaName:      area.Nearest(f.CenterLat, f.CenterLon).Name,
		Latitude:      f.CenterLat,
		Longitude:     f.CenterLon,
		OccurredAt:    f.OccurredAt.UTC().Format(time.RFC3339),
		Attributes: map[string]any{
			"detection_count": f.DetectionCount,
			"satellites":      f.Satellites,
			"max_confidence":  f.MaxConfidence,
			"total_frp_mw":    f.TotalFRPMW,
			"detection_keys":  f.DetectionKeys,
		},
	}
	return encode(m)
}

// deleteMessage removes a fire that no longer exists: all of its detections
// were retracted or dropped, or it merged into an older fire.
func deleteMessage(sourceRefID string, version int64) (Outgoing, error) {
	return encode(event.IngestMessage{
		MessageID:     messageID(sourceRefID, version),
		Op:            event.IngestOpDelete,
		Source:        hazard.SourceSatelit,
		SourceRefID:   sourceRefID,
		SourceVersion: version,
	})
}

func encode(m event.IngestMessage) (Outgoing, error) {
	if err := m.Validate(); err != nil {
		return Outgoing{}, fmt.Errorf("ingest message for %s: %w", m.SourceRefID, err)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return Outgoing{}, err
	}
	return Outgoing{MessageID: m.MessageID, Subject: event.IngestSubject(m.Source), Payload: b}, nil
}

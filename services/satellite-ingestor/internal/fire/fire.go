package fire

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"time"
)

// Severity levels used across the canonical format.
const (
	SeverityWaspada = "WASPADA"
	SeveritySiaga   = "SIAGA"
	SeverityAwas    = "AWAS"
)

// Fire is the summary of one cluster, ready to map to a WILDFIRE HazardEvent.
type Fire struct {
	SourceRefID    string    // stable identifier: hash of the earliest detection key
	CenterLat      float64   // mean latitude of the detections
	CenterLon      float64   // mean longitude of the detections
	OccurredAt     time.Time // earliest detection time
	DetectionCount int
	Satellites     []string // distinct satellites, sorted
	MaxConfidence  string   // h > n > l
	TotalFRPMW     float64
	DetectionKeys  []string // sorted
	Severity       string
}

// Summarize builds the Fire summary for one cluster.
func Summarize(c Component) Fire {
	ds := c.Detections
	first := earliest(ds)
	firstTime, _ := first.Time()

	var sumLat, sumLon, frp float64
	sats := map[string]bool{}
	keys := make([]string, 0, len(ds))
	maxConf := ""
	for _, d := range ds {
		lat, _ := d.Lat()
		lon, _ := d.Lon()
		sumLat += lat
		sumLon += lon
		frp += d.FRP
		sats[d.Satellite] = true
		keys = append(keys, d.Key())
		if rank(d.Confidence) > rank(maxConf) {
			maxConf = d.Confidence
		}
	}
	satList := make([]string, 0, len(sats))
	for s := range sats {
		satList = append(satList, s)
	}
	sort.Strings(satList)
	sort.Strings(keys)

	sum := sha256.Sum256([]byte(first.Key()))
	return Fire{
		SourceRefID:    hex.EncodeToString(sum[:16]),
		CenterLat:      sumLat / float64(len(ds)),
		CenterLon:      sumLon / float64(len(ds)),
		OccurredAt:     firstTime,
		DetectionCount: len(ds),
		Satellites:     satList,
		MaxConfidence:  maxConf,
		TotalFRPMW:     frp,
		DetectionKeys:  keys,
		Severity:       SeverityFor(len(ds), frp),
	}
}

// SeverityFor applies the M2 rule: AWAS at ≥ 20 detections or ≥ 100 MW total FRP;
// SIAGA at ≥ 6 detections or ≥ 30 MW; otherwise WASPADA.
func SeverityFor(detections int, totalFRPMW float64) string {
	switch {
	case detections >= 20 || totalFRPMW >= 100:
		return SeverityAwas
	case detections >= 6 || totalFRPMW >= 30:
		return SeveritySiaga
	default:
		return SeverityWaspada
	}
}

// rank orders VIIRS confidence codes so the highest one is kept.
func rank(c string) int {
	switch c {
	case "h", "high":
		return 3
	case "n", "nominal":
		return 2
	case "l", "low":
		return 1
	default:
		return 0
	}
}

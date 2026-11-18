package handler

import (
	"encoding/json"
	"fmt"
	"math"
	mathrand "math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/bmkg-mock/internal/model"
)

const (
	maxBurstCount     = 100000
	maxBurstDurationS = 3600
)

var burstCounter uint64

// BurstRequest is the JSON body for POST /admin/burst.
type BurstRequest struct {
	RegionName string  `json:"region_name"`
	Count      int     `json:"count"`
	DurationS  int     `json:"duration_s"`
	Lat        float64 `json:"lat"`
	Lon        float64 `json:"lon"`
}

// PostAdminBurst handles POST /admin/burst to generate a regional burst of seismic events.
func (h *Handler) PostAdminBurst(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"error":"Method not allowed"}`))
		return
	}

	var req BurstRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"Invalid JSON body"}`))
		return
	}

	if req.RegionName == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"region_name is required"}`))
		return
	}
	if req.Count <= 0 || req.Count > maxBurstCount {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"count must be between 1 and 100000"}`))
		return
	}
	if req.DurationS < 0 || req.DurationS > maxBurstDurationS {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"duration_s must be between 0 and 3600"}`))
		return
	}
	if req.Lat < -90 || req.Lat > 90 || req.Lon < -180 || req.Lon > 180 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"lat must be [-90,90] and lon must be [-180,180]"}`))
		return
	}

	if req.DurationS == 0 {
		h.emitBurstBatch(req, req.Count)
	} else {
		go h.runBurst(req)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":      "accepted",
		"region_name": req.RegionName,
		"count":       req.Count,
		"duration_s":  req.DurationS,
	})
}

func (h *Handler) runBurst(req BurstRequest) {
	totalDur := time.Duration(req.DurationS) * time.Second
	interval := totalDur / time.Duration(req.Count)
	if interval < time.Millisecond {
		// High-rate burst: emit in batches per 10ms tick
		ticks := int(totalDur / (10 * time.Millisecond))
		if ticks < 1 {
			ticks = 1
		}
		perTick := req.Count / ticks
		rem := req.Count % ticks
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < ticks; i++ {
			<-ticker.C
			n := perTick
			if i < rem {
				n++
			}
			if n > 0 {
				h.emitBurstBatch(req, n)
			}
		}
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for i := 0; i < req.Count; i++ {
		<-ticker.C
		h.emitBurstBatch(req, 1)
	}
}

func (h *Handler) emitBurstBatch(req BurstRequest, n int) {
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; i++ {
		now := time.Now().UTC()
		seq := atomic.AddUint64(&burstCounter, 1)
		eventID := fmt.Sprintf("evt-bmkg-burst-%s-%06d", now.Format("20060102T150405"), seq)

		latJitter := (rng.Float64() - 0.5) * 0.3
		lonJitter := (rng.Float64() - 0.5) * 0.3
		lat := math.Round((req.Lat+latJitter)*10000) / 10000
		lon := math.Round((req.Lon+lonJitter)*10000) / 10000
		mag := math.Round((4.0+rng.Float64()*3.8)*10) / 10
		depth := math.Round((10.0+rng.Float64()*110.0)*10) / 10

		h.store.AddSeismicEvent(model.SeismicEvent{
			EventID:          eventID,
			Magnitude:        mag,
			DepthKm:          depth,
			EpicenterLat:     lat,
			EpicenterLon:     lon,
			RegionName:       req.RegionName,
			OccurredAt:       now.Format(time.RFC3339),
			PotentialTsunami: depth <= 60.0 && mag >= 6.8,
		})
	}
}

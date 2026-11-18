package fire

import (
	"math"
	"sort"
	"time"
)

const (
	// LinkDistanceKm is the largest distance between two linked detections.
	LinkDistanceKm = 1.0
	// LinkWindow is the largest time difference between two linked detections.
	LinkWindow = 360 * time.Minute
	// gridDeg is the bucket size in degrees; it is at least 1 km in both directions
	// for latitudes up to 60, so neighbours within 1 km always share a 3x3 block.
	gridDeg = 0.02
)

// Component is a set of detections linked together (directly or through a chain).
type Component struct {
	Detections []Detection
}

// Cluster groups detections by the linking rule and removes duplicates.
// Duplicates (same key) count once. Low-confidence detections are excluded.
// Components with one detection are reported as singles, not fires.
func Cluster(in []Detection) (fires []Component, singles int) {
	seen := map[string]bool{}
	var dets []Detection
	var times []time.Time
	var lats, lons []float64
	for _, d := range in {
		if d.IsLowConfidence() {
			continue
		}
		if seen[d.Key()] {
			continue
		}
		if d.Validate() != nil {
			continue
		}
		t, _ := d.Time()
		lat, _ := d.Lat()
		lon, _ := d.Lon()
		seen[d.Key()] = true
		dets = append(dets, d)
		times = append(times, t)
		lats = append(lats, lat)
		lons = append(lons, lon)
	}

	parent := make([]int, len(dets))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		for parent[i] != i {
			parent[i] = parent[parent[i]]
			i = parent[i]
		}
		return i
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[ra] = rb
		}
	}

	type cell struct{ r, c int }
	grid := map[cell][]int{}
	for i := range dets {
		grid[cell{cellOf(lats[i]), cellOf(lons[i])}] = append(grid[cell{cellOf(lats[i]), cellOf(lons[i])}], i)
	}
	for i := range dets {
		ci := cell{cellOf(lats[i]), cellOf(lons[i])}
		for dr := -1; dr <= 1; dr++ {
			for dc := -1; dc <= 1; dc++ {
				for _, j := range grid[cell{ci.r + dr, ci.c + dc}] {
					if j <= i {
						continue
					}
					if withinLink(lats[i], lons[i], times[i], lats[j], lons[j], times[j]) {
						union(i, j)
					}
				}
			}
		}
	}

	groups := map[int][]Detection{}
	for i := range dets {
		r := find(i)
		groups[r] = append(groups[r], dets[i])
	}
	for _, g := range groups {
		if len(g) < 2 {
			singles += len(g)
			continue
		}
		fires = append(fires, Component{Detections: g})
	}
	sort.Slice(fires, func(i, j int) bool { return firstKey(fires[i]) < firstKey(fires[j]) })
	return fires, singles
}

func cellOf(deg float64) int { return int(math.Floor(deg / gridDeg)) }

func withinLink(lat1, lon1 float64, t1 time.Time, lat2, lon2 float64, t2 time.Time) bool {
	dt := t1.Sub(t2)
	if dt < 0 {
		dt = -dt
	}
	if dt > LinkWindow {
		return false
	}
	return DistanceKm(lat1, lon1, lat2, lon2) <= LinkDistanceKm
}

// DistanceKm is the great-circle distance between two points (haversine).
func DistanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthKm = 6371.0088
	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthKm * math.Asin(math.Min(1, math.Sqrt(a)))
}

func firstKey(c Component) string {
	return earliest(c.Detections).Key()
}

// earliest returns the detection with the earliest time, breaking ties by key.
func earliest(ds []Detection) Detection {
	best := ds[0]
	bt, _ := best.Time()
	for _, d := range ds[1:] {
		t, _ := d.Time()
		if t.Before(bt) || (t.Equal(bt) && d.Key() < best.Key()) {
			best, bt = d, t
		}
	}
	return best
}

// Linked reports whether two detections are directly linked by the fire rule:
// within LinkDistanceKm and LinkWindow of each other. Invalid detections never link.
func Linked(a, b Detection) bool {
	ta, err1 := a.Time()
	tb, err2 := b.Time()
	la, err3 := a.Lat()
	oa, err4 := a.Lon()
	lb, err5 := b.Lat()
	ob, err6 := b.Lon()
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
		return false
	}
	return withinLink(la, oa, ta, lb, ob, tb)
}

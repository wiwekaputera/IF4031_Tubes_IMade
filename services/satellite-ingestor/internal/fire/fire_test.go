package fire

import (
	"encoding/json"
	"testing"
	"time"
)

func det(sat, date, tm, lat, lon, conf string, frp float64) Detection {
	return Detection{
		Satellite: sat, AcqDate: date, AcqTime: json.Number(tm),
		Latitude: json.Number(lat), Longitude: json.Number(lon),
		Confidence: conf, FRP: frp,
	}
}

func TestDetectionKeyKeepsRawCoordinates(t *testing.T) {
	d := det("SNPP", "2026-11-17", "0530", "-0.123456789", "101.5", "n", 5)
	want := "SNPP|2026-11-17|0530|-0.123456789|101.5"
	if d.Key() != want {
		t.Fatalf("Key = %q, want %q", d.Key(), want)
	}
}

func TestTimeIsUTCOverpassTime(t *testing.T) {
	d := det("SNPP", "2026-11-17", "530", "0", "0", "n", 1)
	got, err := d.Time()
	if err != nil || got.Format("2006-01-02 15:04") != "2026-11-17 05:30" {
		t.Fatalf("Time = %v, %v", got, err)
	}
}

func TestLowConfidenceAndDuplicatesAreExcluded(t *testing.T) {
	a := det("SNPP", "2026-11-17", "0530", "-1.0", "110.0", "n", 10)
	dup := a
	low := det("NOAA20", "2026-11-17", "0530", "-1.0005", "110.0005", "l", 10)
	fires, singles := Cluster([]Detection{a, dup, low})
	if len(fires) != 0 || singles != 1 {
		t.Fatalf("fires=%d singles=%d, want 0 fires and 1 single (duplicate and low excluded)", len(fires), singles)
	}
}

func TestTwoSatellitesSameFire(t *testing.T) {
	a := det("SNPP", "2026-11-17", "0530", "-1.0000", "110.0000", "n", 10)
	b := det("NOAA20", "2026-11-17", "0545", "-1.0050", "110.0030", "h", 12)
	fires, singles := Cluster([]Detection{a, b})
	if len(fires) != 1 || singles != 0 {
		t.Fatalf("fires=%d singles=%d, want one fire from two satellites", len(fires), singles)
	}
	f := Summarize(fires[0])
	if f.DetectionCount != 2 || len(f.Satellites) != 2 || f.MaxConfidence != "h" {
		t.Fatalf("summary = %+v", f)
	}
}

func TestChainLinksAreTransitive(t *testing.T) {
	// A-B are 0.8 km apart and C-B are 0.8 km apart, but A-C are 1.6 km apart.
	a := det("SNPP", "2026-11-17", "0530", "-1.0000", "110.0000", "n", 1)
	b := det("SNPP", "2026-11-17", "0530", "-1.0072", "110.0000", "n", 1)
	c := det("SNPP", "2026-11-17", "0530", "-1.0144", "110.0000", "n", 1)
	fires, singles := Cluster([]Detection{a, b, c})
	if len(fires) != 1 || singles != 0 || fires[0].Detections == nil || len(fires[0].Detections) != 3 {
		t.Fatalf("fires=%v singles=%d, want one fire of three chained detections", fires, singles)
	}
}

func TestTimeWindowSeparatesFires(t *testing.T) {
	a := det("SNPP", "2026-11-17", "0000", "-1.0", "110.0", "n", 1)
	b := det("SNPP", "2026-11-17", "0700", "-1.0", "110.0", "n", 1) // 7 h later
	fires, singles := Cluster([]Detection{a, b})
	if len(fires) != 0 || singles != 2 {
		t.Fatalf("fires=%d singles=%d, want two singles outside the 360-minute window", len(fires), singles)
	}
}

func TestDistanceSeparatesFires(t *testing.T) {
	a := det("SNPP", "2026-11-17", "0530", "-1.0", "110.0", "n", 1)
	b := det("SNPP", "2026-11-17", "0530", "-1.0", "110.02", "n", 1) // about 2.2 km east
	fires, singles := Cluster([]Detection{a, b})
	if len(fires) != 0 || singles != 2 {
		t.Fatalf("fires=%d singles=%d, want two singles beyond 1 km", len(fires), singles)
	}
}

func TestSeverityThresholds(t *testing.T) {
	tests := []struct {
		n    int
		frp  float64
		want string
	}{
		{1, 1, SeverityWaspada},
		{6, 1, SeveritySiaga},
		{1, 30, SeveritySiaga},
		{20, 1, SeverityAwas},
		{1, 100, SeverityAwas},
	}
	for _, tt := range tests {
		if got := SeverityFor(tt.n, tt.frp); got != tt.want {
			t.Errorf("SeverityFor(%d, %v) = %s, want %s", tt.n, tt.frp, got, tt.want)
		}
	}
}

func TestSourceRefIDIsStableAcrossOrder(t *testing.T) {
	a := det("SNPP", "2026-11-17", "0530", "-1.0000", "110.0000", "n", 10)
	b := det("NOAA20", "2026-11-17", "0545", "-1.0050", "110.0030", "h", 12)
	f1, _ := Cluster([]Detection{a, b})
	f2, _ := Cluster([]Detection{b, a})
	if Summarize(f1[0]).SourceRefID != Summarize(f2[0]).SourceRefID {
		t.Fatal("source_ref_id changed with input order")
	}
}

// The live feed sends acq_time as a JSON number; this must decode, not fail.
func TestDetectionDecodesFeedNumericTime(t *testing.T) {
	var d Detection
	raw := `{"satellite":"N21","acq_date":"2026-11-22","acq_time":45,"latitude":-1.98,"longitude":112.6,"confidence":"n","frp":1.5}`
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, err := d.Time()
	if err != nil {
		t.Fatal(err)
	}
	if want := "2026-11-22T00:45:00Z"; got.Format(time.RFC3339) != want {
		t.Fatalf("time = %s, want %s", got.Format(time.RFC3339), want)
	}
}

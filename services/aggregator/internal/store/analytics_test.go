package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

type fakeAnalyticsQuerier struct {
	results map[int]shardAnalyticsResult
	errs    map[int]error
	delays  map[int]time.Duration
}

func (f *fakeAnalyticsQuerier) queryAnalyticsShard(ctx context.Context, shard int, _ AnalyticsFilter) (shardAnalyticsResult, error) {
	if d, ok := f.delays[shard]; ok {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return shardAnalyticsResult{}, ctx.Err()
		}
	}
	if err, ok := f.errs[shard]; ok {
		return shardAnalyticsResult{}, err
	}
	return f.results[shard], nil
}

func TestCategorizeHazard(t *testing.T) {
	cases := []struct {
		name       string
		hazardType hazard.HazardType
		tsunami    bool
		want       string
	}{
		{"seismic without tsunami", hazard.HazardTypeSeismic, false, "SEISMIC"},
		{"seismic with tsunami_warning", hazard.HazardTypeSeismic, true, "TSUNAMI"},
		{"volcanic", hazard.HazardTypeVolcanic, false, "VOLCANIC"},
		{"wildfire", hazard.HazardTypeWildfire, false, "WILDFIRE"},
		{"coastal", hazard.HazardTypeCoastal, false, "COASTAL"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CategorizeHazard(tc.hazardType, tc.tsunami); got != tc.want {
				t.Fatalf("CategorizeHazard(%q, %v) = %q, want %q", tc.hazardType, tc.tsunami, got, tc.want)
			}
		})
	}
}

func TestGetAnalyticsSummary_MergeAndCategoryMapping(t *testing.T) {
	t1 := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 17, 11, 0, 0, 0, time.UTC)

	q := &fakeAnalyticsQuerier{
		results: map[int]shardAnalyticsResult{
			0: {
				Series: []shardSeriesRow{
					{BucketTime: t2, HazardType: hazard.HazardTypeSeismic, IsTsunami: false, Count: 3},
					{BucketTime: t1, HazardType: hazard.HazardTypeSeismic, IsTsunami: true, Count: 2},
				},
				Severities: []shardSeverityRow{
					{Severity: "NORMAL", Count: 3},
					{Severity: "AWAS", Count: 2},
				},
				Areas: []shardAreaRow{
					{AreaName: "Maluku", Count: 4},
					{AreaName: "Aceh", Count: 1},
				},
			},
			3: {
				Series: []shardSeriesRow{
					{BucketTime: t1, HazardType: hazard.HazardTypeSeismic, IsTsunami: false, Count: 4},
					{BucketTime: t1, HazardType: hazard.HazardTypeVolcanic, IsTsunami: false, Count: 5},
					{BucketTime: t2, HazardType: hazard.HazardTypeWildfire, IsTsunami: false, Count: 1},
					{BucketTime: t2, HazardType: hazard.HazardTypeCoastal, IsTsunami: false, Count: 2},
				},
				Severities: []shardSeverityRow{
					{Severity: "WASPADA", Count: 7},
					{Severity: "SIAGA", Count: 5},
				},
				Areas: []shardAreaRow{
					{AreaName: "Maluku", Count: 6},
					{AreaName: "Jawa Timur", Count: 6},
				},
			},
		},
	}

	from := t1.Add(-time.Hour)
	to := t2.Add(time.Hour)
	summary, err := getAnalyticsSummaryWithQuerier(
		context.Background(),
		q,
		500*time.Millisecond,
		zerolog.Nop(),
		AnalyticsFilter{From: &from, To: to, TopAreas: 2},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if summary.Bucket != "hour" {
		t.Fatalf("Bucket = %q, want hour", summary.Bucket)
	}
	if summary.Total != 17 {
		t.Fatalf("Total = %d, want 17 (no double counting)", summary.Total)
	}
	if summary.Partial {
		t.Fatalf("Partial = true, want false")
	}
	if summary.UnavailableUnits == nil || len(summary.UnavailableUnits) != 0 {
		t.Fatalf("UnavailableUnits = %#v, want non-nil empty slice", summary.UnavailableUnits)
	}

	// Verify JSON marshaling produces [] for unavailable_units.
	raw, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if units, ok := decoded["unavailable_units"].([]any); !ok || len(units) != 0 {
		t.Fatalf("decoded unavailable_units = %#v, want []", decoded["unavailable_units"])
	}

	wantCats := map[string]int64{
		"SEISMIC":  7,
		"TSUNAMI":  2,
		"VOLCANIC": 5,
		"WILDFIRE": 1,
		"COASTAL":  2,
	}
	if !reflect.DeepEqual(summary.ByCategory, wantCats) {
		t.Fatalf("ByCategory = %#v, want %#v", summary.ByCategory, wantCats)
	}

	// SEISMIC series should be sorted by timestamp ascending: t1 (4), t2 (3).
	wantSeismic := []AnalyticsSeriesPoint{
		{T: t1.Format(time.RFC3339), Count: 4},
		{T: t2.Format(time.RFC3339), Count: 3},
	}
	if !reflect.DeepEqual(summary.Series["SEISMIC"], wantSeismic) {
		t.Fatalf("Series[SEISMIC] = %#v, want %#v", summary.Series["SEISMIC"], wantSeismic)
	}

	// TopAreas = 2: Maluku (10), Jawa Timur (6); Aceh (1) truncated.
	wantAreas := []AnalyticsAreaCount{
		{AreaName: "Maluku", Count: 10},
		{AreaName: "Jawa Timur", Count: 6},
	}
	if !reflect.DeepEqual(summary.ByArea, wantAreas) {
		t.Fatalf("ByArea = %#v, want %#v", summary.ByArea, wantAreas)
	}
}

func TestGetAnalyticsSummary_PartialPropagation(t *testing.T) {
	t1 := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	q := &fakeAnalyticsQuerier{
		results: map[int]shardAnalyticsResult{
			0: {
				Series: []shardSeriesRow{
					{BucketTime: t1, HazardType: hazard.HazardTypeSeismic, IsTsunami: false, Count: 5},
				},
			},
		},
		errs: map[int]error{
			2: errors.New("connection refused"),
		},
		delays: map[int]time.Duration{
			5: 200 * time.Millisecond,
		},
	}

	from := t1.Add(-24 * time.Hour)
	to := t1.Add(48 * time.Hour)
	summary, err := getAnalyticsSummaryWithQuerier(
		context.Background(),
		q,
		50*time.Millisecond,
		zerolog.Nop(),
		AnalyticsFilter{From: &from, To: to},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !summary.Partial {
		t.Fatalf("Partial = false, want true")
	}
	wantUnits := []string{"shard-2", "shard-5"}
	if !reflect.DeepEqual(summary.UnavailableUnits, wantUnits) {
		t.Fatalf("UnavailableUnits = %#v, want %#v", summary.UnavailableUnits, wantUnits)
	}
	if summary.Total != 5 {
		t.Fatalf("Total = %d, want 5 from surviving shard", summary.Total)
	}
	if summary.Bucket != "day" {
		t.Fatalf("Bucket = %q, want day for >48h range", summary.Bucket)
	}
}

func TestNormalizeAnalyticsFilter(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	// Default 7d range -> day bucket, 20 top areas.
	f := normalizeAnalyticsFilter(AnalyticsFilter{}, now)
	if f.From == nil || !f.From.Equal(now.Add(-7*24*time.Hour)) {
		t.Fatalf("default From = %v, want %v", f.From, now.Add(-7*24*time.Hour))
	}
	if f.Bucket != "day" {
		t.Fatalf("default Bucket = %q, want day", f.Bucket)
	}
	if f.TopAreas != 20 {
		t.Fatalf("default TopAreas = %d, want 20", f.TopAreas)
	}

	// 24h range -> hour bucket.
	from24 := now.Add(-24 * time.Hour)
	f24 := normalizeAnalyticsFilter(AnalyticsFilter{From: &from24, To: now, TopAreas: 500}, now)
	if f24.Bucket != "hour" {
		t.Fatalf("24h Bucket = %q, want hour", f24.Bucket)
	}
	if f24.TopAreas != 100 {
		t.Fatalf("capped TopAreas = %d, want 100", f24.TopAreas)
	}

	// All mode -> day bucket, nil From allowed.
	fAll := normalizeAnalyticsFilter(AnalyticsFilter{All: true, To: now}, now)
	if fAll.Bucket != "day" {
		t.Fatalf("all Bucket = %q, want day", fAll.Bucket)
	}
	if !fAll.All {
		t.Fatalf("All = false, want true")
	}
}

package store

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

const (
	// AnalyticsCategoryTsunami is the derived analytics category for SEISMIC events
	// that carry a non-null tsunami_warning attribute (spec D7).
	AnalyticsCategoryTsunami = "TSUNAMI"

	defaultAnalyticsTopAreas = 20
	maxAnalyticsTopAreas     = 100
)

// AnalyticsCategories lists the 5 mutually exclusive hazard categories shown on the dashboard.
var AnalyticsCategories = []string{
	string(hazard.HazardTypeSeismic),
	AnalyticsCategoryTsunami,
	string(hazard.HazardTypeVolcanic),
	string(hazard.HazardTypeWildfire),
	string(hazard.HazardTypeCoastal),
}

// AnalyticsFilter defines parameters for scatter-gather analytics aggregation.
type AnalyticsFilter struct {
	From     *time.Time
	To       time.Time
	All      bool
	Bucket   string // "hour" or "day"
	TopAreas int
}

// AnalyticsSeriesPoint is a single time-bucket count for a hazard category.
type AnalyticsSeriesPoint struct {
	T     string `json:"t"`
	Count int64  `json:"count"`
}

// AnalyticsAreaCount is the event count for a region (area_name).
type AnalyticsAreaCount struct {
	AreaName string `json:"area_name"`
	Count    int64  `json:"count"`
}

// AnalyticsSummary is the aggregated response returned by GET /internal/analytics/summary.
type AnalyticsSummary struct {
	From             string                            `json:"from"`
	To               string                            `json:"to"`
	Bucket           string                            `json:"bucket"`
	Total            int64                             `json:"total"`
	ByCategory       map[string]int64                  `json:"by_category"`
	Series           map[string][]AnalyticsSeriesPoint `json:"series"`
	BySeverity       map[string]int64                  `json:"by_severity"`
	ByArea           []AnalyticsAreaCount              `json:"by_area"`
	GeneratedAt      string                            `json:"generated_at"`
	Partial          bool                              `json:"partial"`
	UnavailableUnits []string                          `json:"unavailable_units"`
}

// CategorizeHazard maps a canonical hazard_type and tsunami_warning presence to an
// analytics category. SEISMIC events with a non-null tsunami_warning attribute map
// to TSUNAMI and are not double-counted in SEISMIC, so the sum across all 5
// categories equals the canonical row count.
func CategorizeHazard(hazardType hazard.HazardType, hasTsunamiWarning bool) string {
	if hazardType == hazard.HazardTypeSeismic && hasTsunamiWarning {
		return AnalyticsCategoryTsunami
	}
	return string(hazardType)
}

type shardSeriesRow struct {
	BucketTime time.Time
	HazardType hazard.HazardType
	IsTsunami  bool
	Count      int64
}

type shardSeverityRow struct {
	Severity string
	Count    int64
}

type shardAreaRow struct {
	AreaName string
	Count    int64
}

type shardAnalyticsResult struct {
	Series     []shardSeriesRow
	Severities []shardSeverityRow
	Areas      []shardAreaRow
}

type analyticsShardQuerier interface {
	queryAnalyticsShard(ctx context.Context, shard int, filter AnalyticsFilter) (shardAnalyticsResult, error)
}

type pgAnalyticsShardQuerier struct {
	repo *PostgresRepo
}

func buildAnalyticsWhereClause(shard int, filter AnalyticsFilter) (string, []any) {
	var where strings.Builder
	where.WriteString("WHERE shard = $1 AND deleted_at IS NULL")
	args := make([]any, 0, 3)
	args = append(args, shard)
	argIdx := 2

	if !filter.All && filter.From != nil {
		where.WriteString(" AND occurred_at >= $")
		where.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.From.UTC())
		argIdx++
	}
	if !filter.To.IsZero() {
		where.WriteString(" AND occurred_at <= $")
		where.WriteString(strconv.Itoa(argIdx))
		args = append(args, filter.To.UTC())
	}
	return where.String(), args
}

func (q *pgAnalyticsShardQuerier) queryAnalyticsShard(ctx context.Context, shard int, filter AnalyticsFilter) (shardAnalyticsResult, error) {
	whereSQL, whereArgs := buildAnalyticsWhereClause(shard, filter)
	var out shardAnalyticsResult

	// 1. Time-series counts grouped by bucket, hazard_type, and tsunami_warning presence.
	bucketArgIdx := len(whereArgs) + 1
	seriesSQL := `
		SELECT date_trunc($` + strconv.Itoa(bucketArgIdx) + `, occurred_at AT TIME ZONE 'UTC') AS bucket_ts,
		       hazard_type,
		       (hazard_type = 'SEISMIC' AND attributes ? 'tsunami_warning' AND attributes->'tsunami_warning' != 'null'::jsonb) AS is_tsunami,
		       COUNT(*) AS cnt
		FROM canonical_events
		` + whereSQL + `
		GROUP BY 1, 2, 3
	`
	seriesArgs := append(append([]any(nil), whereArgs...), filter.Bucket)
	rows, err := q.repo.db.Query(ctx, seriesSQL, seriesArgs...)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var r shardSeriesRow
		var ht string
		if err := rows.Scan(&r.BucketTime, &ht, &r.IsTsunami, &r.Count); err != nil {
			rows.Close()
			return out, err
		}
		r.HazardType = hazard.HazardType(ht)
		r.BucketTime = r.BucketTime.UTC()
		out.Series = append(out.Series, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return out, err
	}
	rows.Close()

	// 2. Counts grouped by severity.
	sevSQL := `
		SELECT severity, COUNT(*) AS cnt
		FROM canonical_events
		` + whereSQL + `
		GROUP BY severity
	`
	sevRows, err := q.repo.db.Query(ctx, sevSQL, whereArgs...)
	if err != nil {
		return out, err
	}
	for sevRows.Next() {
		var r shardSeverityRow
		if err := sevRows.Scan(&r.Severity, &r.Count); err != nil {
			sevRows.Close()
			return out, err
		}
		out.Severities = append(out.Severities, r)
	}
	if err := sevRows.Err(); err != nil {
		sevRows.Close()
		return out, err
	}
	sevRows.Close()

	// 3. Counts grouped by area_name.
	areaSQL := `
		SELECT area_name, COUNT(*) AS cnt
		FROM canonical_events
		` + whereSQL + `
		GROUP BY area_name
	`
	areaRows, err := q.repo.db.Query(ctx, areaSQL, whereArgs...)
	if err != nil {
		return out, err
	}
	for areaRows.Next() {
		var r shardAreaRow
		if err := areaRows.Scan(&r.AreaName, &r.Count); err != nil {
			areaRows.Close()
			return out, err
		}
		out.Areas = append(out.Areas, r)
	}
	if err := areaRows.Err(); err != nil {
		areaRows.Close()
		return out, err
	}
	areaRows.Close()

	return out, nil
}

// GetAnalyticsSummary aggregates hazard counts across all 8 shards, returning partial
// results with unavailable_units populated if any shard fails or times out.
func (r *PostgresRepo) GetAnalyticsSummary(ctx context.Context, filter AnalyticsFilter) (AnalyticsSummary, error) {
	return getAnalyticsSummaryWithQuerier(ctx, &pgAnalyticsShardQuerier{repo: r}, r.shardTimeout(), r.logger, filter)
}

func normalizeAnalyticsFilter(filter AnalyticsFilter, now time.Time) AnalyticsFilter {
	if filter.To.IsZero() {
		filter.To = now.UTC()
	} else {
		filter.To = filter.To.UTC()
	}
	if !filter.All && filter.From == nil {
		defaultFrom := filter.To.Add(-7 * 24 * time.Hour)
		filter.From = &defaultFrom
	} else if filter.From != nil {
		f := filter.From.UTC()
		filter.From = &f
	}
	if filter.Bucket != "hour" && filter.Bucket != "day" {
		if !filter.All && filter.From != nil && filter.To.Sub(*filter.From) <= 48*time.Hour {
			filter.Bucket = "hour"
		} else {
			filter.Bucket = "day"
		}
	}
	if filter.TopAreas <= 0 {
		filter.TopAreas = defaultAnalyticsTopAreas
	} else if filter.TopAreas > maxAnalyticsTopAreas {
		filter.TopAreas = maxAnalyticsTopAreas
	}
	return filter
}

func getAnalyticsSummaryWithQuerier(
	ctx context.Context,
	querier analyticsShardQuerier,
	timeout time.Duration,
	logger zerolog.Logger,
	filter AnalyticsFilter,
) (AnalyticsSummary, error) {
	now := time.Now().UTC()
	filter = normalizeAnalyticsFilter(filter, now)

	var (
		mu      sync.Mutex
		results = make([]shardAnalyticsResult, 0, NumShards)
	)

	unavailable := scatterShards(ctx, timeout, logger, allShards(), func(shardCtx context.Context, shard int) error {
		res, err := querier.queryAnalyticsShard(shardCtx, shard, filter)
		if err != nil {
			return err
		}
		mu.Lock()
		results = append(results, res)
		mu.Unlock()
		return nil
	})

	if unavailable == nil {
		unavailable = []string{}
	}

	byCategory := make(map[string]int64, len(AnalyticsCategories))
	seriesBuckets := make(map[string]map[time.Time]int64, len(AnalyticsCategories))
	for _, cat := range AnalyticsCategories {
		byCategory[cat] = 0
		seriesBuckets[cat] = make(map[time.Time]int64)
	}

	bySeverity := map[string]int64{
		string(hazard.SeverityNormal):  0,
		string(hazard.SeverityWaspada): 0,
		string(hazard.SeveritySiaga):   0,
		string(hazard.SeverityAwas):    0,
	}

	areaTotals := make(map[string]int64)
	var total int64

	for _, res := range results {
		for _, s := range res.Series {
			cat := CategorizeHazard(s.HazardType, s.IsTsunami)
			if _, ok := byCategory[cat]; !ok {
				byCategory[cat] = 0
				seriesBuckets[cat] = make(map[time.Time]int64)
			}
			byCategory[cat] += s.Count
			seriesBuckets[cat][s.BucketTime.UTC()] += s.Count
			total += s.Count
		}
		for _, sv := range res.Severities {
			bySeverity[sv.Severity] += sv.Count
		}
		for _, ar := range res.Areas {
			areaTotals[ar.AreaName] += ar.Count
		}
	}

	series := make(map[string][]AnalyticsSeriesPoint, len(seriesBuckets))
	for cat, buckets := range seriesBuckets {
		times := make([]time.Time, 0, len(buckets))
		for t := range buckets {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool {
			return times[i].Before(times[j])
		})
		pts := make([]AnalyticsSeriesPoint, len(times))
		for i, t := range times {
			pts[i] = AnalyticsSeriesPoint{
				T:     t.UTC().Format(time.RFC3339),
				Count: buckets[t],
			}
		}
		series[cat] = pts
	}

	byArea := make([]AnalyticsAreaCount, 0, len(areaTotals))
	for area, cnt := range areaTotals {
		byArea = append(byArea, AnalyticsAreaCount{
			AreaName: area,
			Count:    cnt,
		})
	}
	sort.Slice(byArea, func(i, j int) bool {
		if byArea[i].Count != byArea[j].Count {
			return byArea[i].Count > byArea[j].Count
		}
		return byArea[i].AreaName < byArea[j].AreaName
	})
	if len(byArea) > filter.TopAreas {
		byArea = byArea[:filter.TopAreas]
	}

	fromStr := ""
	if !filter.All && filter.From != nil {
		fromStr = filter.From.UTC().Format(time.RFC3339)
	}

	return AnalyticsSummary{
		From:             fromStr,
		To:               filter.To.UTC().Format(time.RFC3339),
		Bucket:           filter.Bucket,
		Total:            total,
		ByCategory:       byCategory,
		Series:           series,
		BySeverity:       bySeverity,
		ByArea:           byArea,
		GeneratedAt:      now.Format(time.RFC3339),
		Partial:          len(unavailable) > 0,
		UnavailableUnits: unavailable,
	}, nil
}

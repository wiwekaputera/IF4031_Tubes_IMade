package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/normalizer"
	"github.com/wiwekaputera/if4031-tubes-imade/aggregator/internal/store"
	"github.com/wiwekaputera/if4031-tubes-imade/contracts/hazard"
)

// ColumnMap configures source column names for catalog parsing.
type ColumnMap struct {
	ID         string
	Time       string
	Date       string // BMKG tgl when split from ot
	OriginTime string // BMKG ot when split from tgl
	Lat        string
	Lon        string
	Depth      string
	Mag        string
	Region     string
	TimeLayout string
}

// ParseStats tracks row counts and the first N invalid row errors.
type ParseStats struct {
	Read    int64
	Valid   int64
	Invalid int64
}

// RowError describes a single invalid row encountered during parsing.
type RowError struct {
	Row    int64
	Reason string
}

func detectFormat(filePath, explicit string) (string, error) {
	if explicit != "" {
		f := strings.ToLower(strings.TrimSpace(explicit))
		switch f {
		case "csv", "json", "ndjson":
			return f, nil
		case "jsonl":
			return "ndjson", nil
		default:
			return "", fmt.Errorf("unsupported format %q (expected csv, json, or ndjson)", explicit)
		}
	}
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".csv":
		return "csv", nil
	case ".json":
		return "json", nil
	case ".ndjson", ".jsonl":
		return "ndjson", nil
	default:
		return "", fmt.Errorf("cannot auto-detect format from extension %q; specify --format", ext)
	}
}

func resolveColumns(headers []string, cfg ColumnMap) ColumnMap {
	norm := make(map[string]string, len(headers))
	for _, h := range headers {
		trimmed := strings.TrimSpace(h)
		norm[strings.ToLower(trimmed)] = trimmed
	}

	pick := func(explicit string, candidates ...string) string {
		if explicit != "" {
			if actual, ok := norm[strings.ToLower(strings.TrimSpace(explicit))]; ok {
				return actual
			}
			return explicit
		}
		for _, c := range candidates {
			if actual, ok := norm[c]; ok {
				return actual
			}
		}
		return ""
	}

	resolved := ColumnMap{
		ID:         pick(cfg.ID, "id", "event_id", "source_ref_id", "eq_id", "kode"),
		Time:       pick(cfg.Time, "time", "occurred_at", "datetime", "timestamp", "origin_time", "date_time"),
		Lat:        pick(cfg.Lat, "latitude", "lat", "lintang"),
		Lon:        pick(cfg.Lon, "longitude", "lon", "lng", "long", "bujur"),
		Depth:      pick(cfg.Depth, "depth", "depth_km", "kedalaman"),
		Mag:        pick(cfg.Mag, "mag", "magnitude", "magnitudo", "mw", "ml", "mb"),
		Region:     pick(cfg.Region, "place", "remark", "region", "area_name", "area", "wilayah", "location"),
		TimeLayout: cfg.TimeLayout,
	}

	// BMKG-style split date/time columns (tgl + ot) when no single time column is set.
	if resolved.Time == "" {
		tgl := norm["tgl"]
		ot := norm["ot"]
		if tgl != "" && ot != "" {
			resolved.Date = tgl
			resolved.OriginTime = ot
		} else if tgl != "" {
			resolved.Time = tgl
		}
	}

	return resolved
}

var commonTimeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05.999999999 MST",
	"2006-01-02 15:04:05 MST",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006/01/02 15:04:05",
	"02-01-2006 15:04:05",
	"02/01/2006 15:04:05",
	"2006-01-02",
}

func parseTimeValue(raw, customLayout string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	if customLayout != "" {
		t, err := time.Parse(customLayout, s)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid timestamp %q for layout %q: %w", s, customLayout, err)
		}
		return t.UTC(), nil
	}
	for _, layout := range commonTimeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format %q", s)
}

func fallbackSourceRefID(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return "hist-" + hex.EncodeToString(sum[:])[:16]
}

func buildUpsertRequest(row map[string]string, rawRow string, cols ColumnMap) (store.UpsertRequest, error) {
	var timeStr string
	if cols.Date != "" && cols.OriginTime != "" {
		d := strings.TrimSpace(row[cols.Date])
		ot := strings.TrimSpace(row[cols.OriginTime])
		if d == "" || ot == "" {
			return store.UpsertRequest{}, fmt.Errorf("missing %s or %s", cols.Date, cols.OriginTime)
		}
		timeStr = d + " " + ot
	} else {
		if cols.Time == "" {
			return store.UpsertRequest{}, fmt.Errorf("no time column mapped")
		}
		timeStr = row[cols.Time]
	}

	occurredAt, err := parseTimeValue(timeStr, cols.TimeLayout)
	if err != nil {
		return store.UpsertRequest{}, err
	}

	if cols.Lat == "" || cols.Lon == "" {
		return store.UpsertRequest{}, fmt.Errorf("missing latitude or longitude column mapping")
	}
	lat, err := strconv.ParseFloat(strings.TrimSpace(row[cols.Lat]), 64)
	if err != nil || math.IsNaN(lat) || math.IsInf(lat, 0) || lat < -90 || lat > 90 {
		return store.UpsertRequest{}, fmt.Errorf("invalid latitude %q", row[cols.Lat])
	}
	lon, err := strconv.ParseFloat(strings.TrimSpace(row[cols.Lon]), 64)
	if err != nil || math.IsNaN(lon) || math.IsInf(lon, 0) || lon < -180 || lon > 180 {
		return store.UpsertRequest{}, fmt.Errorf("invalid longitude %q", row[cols.Lon])
	}

	if cols.Mag == "" {
		return store.UpsertRequest{}, fmt.Errorf("no magnitude column mapped")
	}
	mag, err := strconv.ParseFloat(strings.TrimSpace(row[cols.Mag]), 64)
	if err != nil || math.IsNaN(mag) || math.IsInf(mag, 0) || mag < 0 || mag > 12 {
		return store.UpsertRequest{}, fmt.Errorf("invalid magnitude %q", row[cols.Mag])
	}

	var depth float64
	if cols.Depth != "" {
		if dStr := strings.TrimSpace(row[cols.Depth]); dStr != "" {
			depth, err = strconv.ParseFloat(dStr, 64)
			if err != nil || math.IsNaN(depth) || math.IsInf(depth, 0) || depth < 0 {
				return store.UpsertRequest{}, fmt.Errorf("invalid depth %q", dStr)
			}
		}
	}

	areaName := "Unknown"
	if cols.Region != "" {
		if r := strings.TrimSpace(row[cols.Region]); r != "" {
			areaName = r
		}
	}

	sourceRefID := ""
	if cols.ID != "" {
		sourceRefID = strings.TrimSpace(row[cols.ID])
	}
	if sourceRefID == "" {
		sourceRefID = fallbackSourceRefID(rawRow)
	}

	mapped := map[string]bool{
		cols.ID:         cols.ID != "",
		cols.Time:       cols.Time != "",
		cols.Date:       cols.Date != "",
		cols.OriginTime: cols.OriginTime != "",
		cols.Lat:        cols.Lat != "",
		cols.Lon:        cols.Lon != "",
		cols.Depth:      cols.Depth != "",
		cols.Mag:        cols.Mag != "",
		cols.Region:     cols.Region != "",
	}

	attrs := map[string]interface{}{
		"magnitude": mag,
		"depth_km":  depth,
		"dataset":   "historical_catalog",
	}
	for k, v := range row {
		if mapped[k] {
			continue
		}
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			attrs[k] = trimmed
		}
	}

	return store.UpsertRequest{
		HazardID:    hazard.ID(hazard.SourceBMKG, sourceRefID),
		Source:      string(hazard.SourceBMKG),
		SourceRefID: sourceRefID,
		HazardType:  hazard.HazardTypeSeismic,
		Severity:    normalizer.SeismicSeverity(mag, false, ""),
		AreaName:    areaName,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  occurredAt,
		Sources: map[string]interface{}{
			"bmkg": map[string]interface{}{
				"dataset":       "historical_catalog",
				"source_ref_id": sourceRefID,
			},
		},
		Attributes: attrs,
	}, nil
}

// streamCatalog reads records from filePath in format (csv, json, ndjson) and sends
// valid UpsertRequest items to out. Invalid rows are sent to onError without aborting.
func streamCatalog(
	filePath string,
	format string,
	cfg ColumnMap,
	out chan<- store.UpsertRequest,
	onError func(RowError),
) (ParseStats, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return ParseStats{}, err
	}
	defer f.Close()

	switch format {
	case "csv":
		return streamCSV(f, cfg, out, onError)
	case "json":
		return streamJSON(f, cfg, out, onError)
	case "ndjson":
		return streamNDJSON(f, cfg, out, onError)
	default:
		return ParseStats{}, fmt.Errorf("unsupported format %q", format)
	}
}

func streamCSV(r io.Reader, cfg ColumnMap, out chan<- store.UpsertRequest, onError func(RowError)) (ParseStats, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1

	headers, err := cr.Read()
	if err != nil {
		return ParseStats{}, fmt.Errorf("read csv header: %w", err)
	}
	for i := range headers {
		headers[i] = strings.TrimSpace(strings.TrimPrefix(headers[i], "\ufeff"))
	}
	cols := resolveColumns(headers, cfg)

	var stats ParseStats
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		stats.Read++
		if err != nil {
			stats.Invalid++
			if onError != nil {
				onError(RowError{Row: stats.Read, Reason: fmt.Sprintf("csv read error: %v", err)})
			}
			continue
		}
		if len(rec) != len(headers) {
			stats.Invalid++
			if onError != nil {
				onError(RowError{Row: stats.Read, Reason: fmt.Sprintf("expected %d fields, got %d", len(headers), len(rec))})
			}
			continue
		}

		rowMap := make(map[string]string, len(headers))
		for i, h := range headers {
			rowMap[h] = rec[i]
		}
		req, err := buildUpsertRequest(rowMap, strings.Join(rec, ","), cols)
		if err != nil {
			stats.Invalid++
			if onError != nil {
				onError(RowError{Row: stats.Read, Reason: err.Error()})
			}
			continue
		}
		stats.Valid++
		out <- req
	}
	return stats, nil
}

func jsonObjectToStringMap(obj map[string]interface{}) (map[string]string, []string, string) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	row := make(map[string]string, len(obj))
	var rawBuilder strings.Builder
	for i, k := range keys {
		v := obj[k]
		var s string
		switch val := v.(type) {
		case nil:
			s = ""
		case string:
			s = val
		case float64:
			s = strconv.FormatFloat(val, 'f', -1, 64)
		case bool:
			s = strconv.FormatBool(val)
		default:
			b, _ := json.Marshal(val)
			s = string(b)
		}
		row[k] = s
		if i > 0 {
			rawBuilder.WriteByte('|')
		}
		rawBuilder.WriteString(k)
		rawBuilder.WriteByte('=')
		rawBuilder.WriteString(s)
	}
	return row, keys, rawBuilder.String()
}

func streamJSON(r io.Reader, cfg ColumnMap, out chan<- store.UpsertRequest, onError func(RowError)) (ParseStats, error) {
	dec := json.NewDecoder(r)
	tok, err := dec.Token()
	if err != nil {
		return ParseStats{}, fmt.Errorf("read json root: %w", err)
	}

	var stats ParseStats
	var cols *ColumnMap

	processObject := func(obj map[string]interface{}) {
		stats.Read++
		row, keys, raw := jsonObjectToStringMap(obj)
		if cols == nil {
			resolved := resolveColumns(keys, cfg)
			cols = &resolved
		}
		req, err := buildUpsertRequest(row, raw, *cols)
		if err != nil {
			stats.Invalid++
			if onError != nil {
				onError(RowError{Row: stats.Read, Reason: err.Error()})
			}
			return
		}
		stats.Valid++
		out <- req
	}

	delim, ok := tok.(json.Delim)
	if !ok {
		return stats, fmt.Errorf("expected JSON array or object, got %T", tok)
	}

	if delim == '[' {
		for dec.More() {
			var obj map[string]interface{}
			if err := dec.Decode(&obj); err != nil {
				stats.Read++
				stats.Invalid++
				if onError != nil {
					onError(RowError{Row: stats.Read, Reason: fmt.Sprintf("json decode error: %v", err)})
				}
				continue
			}
			processObject(obj)
		}
		return stats, nil
	}

	if delim == '{' {
		var wrapper map[string]json.RawMessage
		// Reconstruct full object since opening '{' was consumed.
		rest, err := io.ReadAll(dec.Buffered())
		if err != nil {
			return stats, err
		}
		tail, err := io.ReadAll(r)
		if err != nil {
			return stats, err
		}
		full := append([]byte{'{'}, rest...)
		full = append(full, tail...)
		if err := json.Unmarshal(full, &wrapper); err != nil {
			return stats, fmt.Errorf("decode json wrapper object: %w", err)
		}
		for _, rawArr := range wrapper {
			var items []map[string]interface{}
			if err := json.Unmarshal(rawArr, &items); err == nil {
				for _, obj := range items {
					processObject(obj)
				}
				return stats, nil
			}
		}
		return stats, fmt.Errorf("no array field found in JSON object")
	}

	return stats, fmt.Errorf("unexpected JSON delimiter %q", delim)
}

func streamNDJSON(r io.Reader, cfg ColumnMap, out chan<- store.UpsertRequest, onError func(RowError)) (ParseStats, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	var stats ParseStats
	var cols *ColumnMap

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		stats.Read++
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			stats.Invalid++
			if onError != nil {
				onError(RowError{Row: stats.Read, Reason: fmt.Sprintf("ndjson decode error: %v", err)})
			}
			continue
		}
		row, keys, _ := jsonObjectToStringMap(obj)
		if cols == nil {
			resolved := resolveColumns(keys, cfg)
			cols = &resolved
		}
		req, err := buildUpsertRequest(row, line, *cols)
		if err != nil {
			stats.Invalid++
			if onError != nil {
				onError(RowError{Row: stats.Read, Reason: err.Error()})
			}
			continue
		}
		stats.Valid++
		out <- req
	}
	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("scan ndjson: %w", err)
	}
	return stats, nil
}

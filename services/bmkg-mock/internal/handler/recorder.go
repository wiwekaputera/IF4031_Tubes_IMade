package handler

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

const defaultRecorderCapacity = 10000

// RequestRecord represents a single recorded data-endpoint request.
type RequestRecord struct {
	Time     time.Time `json:"time"`
	Instance string    `json:"instance"`
	Path     string    `json:"path"`
	Since    string    `json:"since"`
	Status   int       `json:"status"`
}

// DuplicateEntry represents a (path, since) pair requested by multiple distinct instances within a window.
type DuplicateEntry struct {
	Path      string   `json:"path"`
	Since     string   `json:"since"`
	Instances []string `json:"instances"`
	Count     int      `json:"count"`
}

// RequestsResponse is the JSON payload returned by GET /admin/requests.
type RequestsResponse struct {
	Total      int              `json:"total"`
	ByInstance map[string]int   `json:"by_instance"`
	Duplicates []DuplicateEntry `json:"duplicates"`
	Recent     []RequestRecord  `json:"recent"`
}

// RequestRecorder stores recent data-endpoint requests in a bounded ring buffer.
type RequestRecorder struct {
	mu         sync.Mutex
	capacity   int
	records    []RequestRecord
	head       int
	count      int
	total      int
	byInstance map[string]int
}

// NewRequestRecorder creates a RequestRecorder with the given ring buffer capacity.
func NewRequestRecorder(capacity int) *RequestRecorder {
	if capacity <= 0 {
		capacity = defaultRecorderCapacity
	}
	return &RequestRecorder{
		capacity:   capacity,
		records:    make([]RequestRecord, capacity),
		byInstance: make(map[string]int),
	}
}

// Record appends a request record to the ring buffer.
func (rr *RequestRecorder) Record(instance, path, since string, status int) {
	if instance == "" {
		instance = "unknown"
	}

	rr.mu.Lock()
	defer rr.mu.Unlock()

	rr.records[rr.head] = RequestRecord{
		Time:     time.Now().UTC(),
		Instance: instance,
		Path:     path,
		Since:    since,
		Status:   status,
	}
	rr.head = (rr.head + 1) % rr.capacity
	if rr.count < rr.capacity {
		rr.count++
	}
	rr.total++
	rr.byInstance[instance]++
}

// Reset clears all recorded requests and counters.
func (rr *RequestRecorder) Reset() {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	rr.head = 0
	rr.count = 0
	rr.total = 0
	rr.byInstance = make(map[string]int)
}

// Snapshot returns a summary of recorded requests and duplicates within window.
func (rr *RequestRecorder) Snapshot(window time.Duration) RequestsResponse {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	byInst := make(map[string]int, len(rr.byInstance))
	for k, v := range rr.byInstance {
		byInst[k] = v
	}

	ordered := rr.orderedRecordsLocked()
	recentCount := 50
	if len(ordered) < recentCount {
		recentCount = len(ordered)
	}
	recent := make([]RequestRecord, recentCount)
	copy(recent, ordered[len(ordered)-recentCount:])

	cutoff := time.Now().UTC().Add(-window)
	type dupKey struct {
		path  string
		since string
	}
	type dupAcc struct {
		instances map[string]struct{}
		count     int
	}
	grouped := make(map[dupKey]*dupAcc)
	for _, rec := range ordered {
		if rec.Time.Before(cutoff) {
			continue
		}
		k := dupKey{path: rec.Path, since: rec.Since}
		acc, ok := grouped[k]
		if !ok {
			acc = &dupAcc{instances: make(map[string]struct{})}
			grouped[k] = acc
		}
		acc.instances[rec.Instance] = struct{}{}
		acc.count++
	}

	duplicates := make([]DuplicateEntry, 0)
	for k, acc := range grouped {
		if len(acc.instances) <= 1 {
			continue
		}
		insts := make([]string, 0, len(acc.instances))
		for inst := range acc.instances {
			insts = append(insts, inst)
		}
		sort.Strings(insts)
		duplicates = append(duplicates, DuplicateEntry{
			Path:      k.path,
			Since:     k.since,
			Instances: insts,
			Count:     acc.count,
		})
	}
	sort.Slice(duplicates, func(i, j int) bool {
		if duplicates[i].Path != duplicates[j].Path {
			return duplicates[i].Path < duplicates[j].Path
		}
		return duplicates[i].Since < duplicates[j].Since
	})

	return RequestsResponse{
		Total:      rr.total,
		ByInstance: byInst,
		Duplicates: duplicates,
		Recent:     recent,
	}
}

func (rr *RequestRecorder) orderedRecordsLocked() []RequestRecord {
	if rr.count == 0 {
		return nil
	}
	out := make([]RequestRecord, rr.count)
	start := 0
	if rr.count == rr.capacity {
		start = rr.head
	}
	for i := 0; i < rr.count; i++ {
		out[i] = rr.records[(start+i)%rr.capacity]
	}
	return out
}

// RecordMiddleware records data-endpoint requests into the RequestRecorder.
func RecordMiddleware(rr *RequestRecorder) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(rec, r)

			instance := r.Header.Get("X-Instance-ID")
			if instance == "" {
				instance = "unknown"
			}
			rr.Record(instance, r.URL.Path, r.URL.Query().Get("since"), rec.statusCode)
		})
	}
}

// GetAdminRequests handles GET /admin/requests.
func (h *Handler) GetAdminRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"error":"Method not allowed"}`))
		return
	}

	windowS := 10
	if raw := r.URL.Query().Get("window_s"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"Invalid window_s parameter: must be a positive integer"}`))
			return
		}
		windowS = parsed
	}

	resp := h.recorder.Snapshot(time.Duration(windowS) * time.Second)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// ResetAdminRequests handles POST /admin/requests/reset.
func (h *Handler) ResetAdminRequests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"error":"Method not allowed"}`))
		return
	}

	h.recorder.Reset()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

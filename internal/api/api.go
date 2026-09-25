package api

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"takl/internal/engine/store"
	"takl/internal/metrics"
	"takl/internal/model"
)

type Server struct {
	store       *store.Store
	nodeID      string
	clock       *model.Clock
	adminToken  string
	enablePprof bool
}

type Options struct {
	AdminToken  string
	EnablePprof bool
}

func New(st *store.Store, nodeID string, clock *model.Clock, opts ...Options) *Server {
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	return &Server{store: st, nodeID: nodeID, clock: clock, adminToken: opt.AdminToken, enablePprof: opt.EnablePprof}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /api/v1/runners/best", s.handleBestRunner)
	mux.HandleFunc("GET /api/v1/runners", s.handleRunners)
	mux.HandleFunc("GET /api/v1/runners/{id}", s.handleRunner)
	mux.HandleFunc("GET /api/v1/runners/{id}/labels", s.handleGetLabels)
	mux.HandleFunc("PUT /api/v1/runners/{id}/labels", s.handleUpdateLabels)
	mux.HandleFunc("DELETE /api/v1/runners/{id}/labels", s.handleDeleteLabels)
	mux.HandleFunc("GET /api/v1/events", s.handleEvents)
	mux.HandleFunc("GET /api/v1/summary", s.handleSummary)
	mux.HandleFunc("GET /api/v1/cluster", s.handleCluster)
	if s.enablePprof {
		mux.Handle("GET /debug/pprof/", s.adminOnly(http.HandlerFunc(pprof.Index)))
		mux.Handle("GET /debug/pprof/cmdline", s.adminOnly(http.HandlerFunc(pprof.Cmdline)))
		mux.Handle("GET /debug/pprof/profile", s.adminOnly(http.HandlerFunc(pprof.Profile)))
		mux.Handle("GET /debug/pprof/symbol", s.adminOnly(http.HandlerFunc(pprof.Symbol)))
		mux.Handle("POST /debug/pprof/symbol", s.adminOnly(http.HandlerFunc(pprof.Symbol)))
		mux.Handle("GET /debug/pprof/trace", s.adminOnly(http.HandlerFunc(pprof.Trace)))
	}
	return s.observeHTTP(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node_id": s.nodeID})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP takl_http_requests_total Total HTTP requests by status code.")
	fmt.Fprintln(w, "# TYPE takl_http_requests_total counter")
	metrics.HTTPRequests.Write(w, "takl_http_requests_total")
	fmt.Fprintln(w, "# HELP takl_placement_requests_total Total placement query requests.")
	fmt.Fprintln(w, "# TYPE takl_placement_requests_total counter")
	fmt.Fprintf(w, "takl_placement_requests_total %d\n", metrics.PlacementRequests.Load())
	fmt.Fprintln(w, "# HELP takl_placement_errors_total Total placement query errors.")
	fmt.Fprintln(w, "# TYPE takl_placement_errors_total counter")
	fmt.Fprintf(w, "takl_placement_errors_total %d\n", metrics.PlacementErrors.Load())
	fmt.Fprintln(w, "# HELP takl_placement_latency_seconds Placement query latency.")
	fmt.Fprintln(w, "# TYPE takl_placement_latency_seconds summary")
	fmt.Fprintf(w, "takl_placement_latency_seconds_count %d\n", metrics.PlacementLatency.Count())
	fmt.Fprintf(w, "takl_placement_latency_seconds_sum %f\n", metrics.PlacementLatency.Seconds())
	fmt.Fprintln(w, "# HELP takl_sync_rounds_total Total sync rounds.")
	fmt.Fprintln(w, "# TYPE takl_sync_rounds_total counter")
	fmt.Fprintf(w, "takl_sync_rounds_total %d\n", metrics.SyncRounds.Load())
	fmt.Fprintln(w, "# HELP takl_sync_round_errors_total Total failed sync rounds.")
	fmt.Fprintln(w, "# TYPE takl_sync_round_errors_total counter")
	fmt.Fprintf(w, "takl_sync_round_errors_total %d\n", metrics.SyncRoundErrors.Load())
	fmt.Fprintln(w, "# HELP takl_sync_round_latency_seconds Sync round latency.")
	fmt.Fprintln(w, "# TYPE takl_sync_round_latency_seconds summary")
	fmt.Fprintf(w, "takl_sync_round_latency_seconds_count %d\n", metrics.SyncRoundLatency.Count())
	fmt.Fprintf(w, "takl_sync_round_latency_seconds_sum %f\n", metrics.SyncRoundLatency.Seconds())
	fmt.Fprintln(w, "# HELP takl_sync_rows_applied_total Total replicated rows applied by sync.")
	fmt.Fprintln(w, "# TYPE takl_sync_rows_applied_total counter")
	fmt.Fprintf(w, "takl_sync_rows_applied_total %d\n", metrics.SyncRowsApplied.Load())
	fmt.Fprintln(w, "# HELP takl_sync_events_applied_total Total replicated events applied by sync.")
	fmt.Fprintln(w, "# TYPE takl_sync_events_applied_total counter")
	fmt.Fprintf(w, "takl_sync_events_applied_total %d\n", metrics.SyncEventsApplied.Load())
	fmt.Fprintln(w, "# HELP takl_sync_checksum_mismatches_total Total row checksum mismatches requiring reconciliation.")
	fmt.Fprintln(w, "# TYPE takl_sync_checksum_mismatches_total counter")
	fmt.Fprintf(w, "takl_sync_checksum_mismatches_total %d\n", metrics.SyncChecksumMismatches.Load())
	fmt.Fprintln(w, "# HELP takl_sync_event_checksum_mismatches_total Total event checksum mismatches requiring reconciliation.")
	fmt.Fprintln(w, "# TYPE takl_sync_event_checksum_mismatches_total counter")
	fmt.Fprintf(w, "takl_sync_event_checksum_mismatches_total %d\n", metrics.SyncEventChecksumMismatches.Load())

	runners, err := listRunners(s.store, -1, 0)
	if err != nil {
		fmt.Fprintf(w, "takl_metrics_store_error 1\n")
		return
	}
	var active, capacity, available int
	for _, r := range runners {
		if r.Status == model.RunnerActive {
			active++
			capacity += r.WorkerCapacity
			available += r.AvailableWorkers
		}
	}
	fmt.Fprintln(w, "# HELP takl_runners_total Total known non-tombstoned runners.")
	fmt.Fprintln(w, "# TYPE takl_runners_total gauge")
	fmt.Fprintf(w, "takl_runners_total %d\n", len(runners))
	fmt.Fprintln(w, "# HELP takl_active_runners Total active runners.")
	fmt.Fprintln(w, "# TYPE takl_active_runners gauge")
	fmt.Fprintf(w, "takl_active_runners %d\n", active)
	fmt.Fprintln(w, "# HELP takl_worker_capacity Total active worker capacity.")
	fmt.Fprintln(w, "# TYPE takl_worker_capacity gauge")
	fmt.Fprintf(w, "takl_worker_capacity %d\n", capacity)
	fmt.Fprintln(w, "# HELP takl_available_workers Total active available workers.")
	fmt.Fprintln(w, "# TYPE takl_available_workers gauge")
	fmt.Fprintf(w, "takl_available_workers %d\n", available)
}

func parsePagination(r *http.Request) (int, int) {
	limit := -1
	offset := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, err := strconv.Atoi(v); err == nil && l > 0 {
			limit = l
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if o, err := strconv.Atoi(v); err == nil && o >= 0 {
			offset = o
		}
	}
	return limit, offset
}

func (s *Server) handleRunners(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, err := listRunners(s.store, limit, offset)
	if err != nil {
		writeError(w, err)
		return
	}
	s.mergeMetaLabels(items)
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleRunner(w http.ResponseWriter, r *http.Request) {
	row, ok, err := s.store.Get(store.KindRunner, r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok || row.Tombstone {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "runner not found"})
		return
	}
	item, err := model.Decode[model.Runner](row.Payload)
	if err != nil {
		writeError(w, err)
		return
	}
	s.mergeMetaLabelsOne(&item)
	writeJSON(w, http.StatusOK, item)
}

// --- Label CRUD ---

func (s *Server) handleGetLabels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	runnerRow, ok, err := s.store.Get(store.KindRunner, id)
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok || runnerRow.Tombstone {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "runner not found"})
		return
	}

	labels := make(map[string]string)
	runner, err := model.Decode[model.Runner](runnerRow.Payload)
	if err == nil {
		for k, v := range runner.Labels {
			labels[k] = v
		}
	}

	metaRow, ok, err := s.store.Get(store.KindMeta, id)
	if err != nil {
		writeError(w, err)
		return
	}
	if ok && !metaRow.Tombstone {
		var extra map[string]string
		if err := json.Unmarshal(metaRow.Payload, &extra); err == nil {
			for k, v := range extra {
				labels[k] = v
			}
		}
	}

	writeJSON(w, http.StatusOK, labels)
}

func (s *Server) handleUpdateLabels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Verify runner exists
	runnerRow, ok, err := s.store.Get(store.KindRunner, id)
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok || runnerRow.Tombstone {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "runner not found"})
		return
	}

	var incoming map[string]string
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json: expected {\"key\": \"value\", ...}"})
		return
	}

	// Merge with existing API labels
	existing := make(map[string]string)
	metaRow, ok, err := s.store.Get(store.KindMeta, id)
	if err != nil {
		writeError(w, err)
		return
	}
	if ok && !metaRow.Tombstone {
		_ = json.Unmarshal(metaRow.Payload, &existing)
	}
	for k, v := range incoming {
		existing[k] = v
	}

	payload, _ := json.Marshal(existing)
	hlc := s.clock.Tick()
	err = s.store.Put(store.Row{
		Kind:    store.KindMeta,
		Key:     id,
		Owner:   "api/" + s.nodeID,
		HLC:     hlc,
		Payload: payload,
	})
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, existing)
}

func (s *Server) handleDeleteLabels(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	hlc := s.clock.Tick()
	if err := s.store.Delete(store.KindMeta, id, "api/"+s.nodeID, hlc); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Events ---

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 1000 {
		limit = 1000
	}
	evs, err := s.store.Events(limit)
	if err != nil {
		writeError(w, err)
		return
	}
	if runner := r.URL.Query().Get("runner"); runner != "" {
		filtered := evs[:0]
		for _, e := range evs {
			if e.RunnerID == runner {
				filtered = append(filtered, e)
			}
		}
		evs = filtered
	}
	writeJSON(w, http.StatusOK, evs)
}

// --- Summary & Cluster ---

func (s *Server) handleSummary(w http.ResponseWriter, _ *http.Request) {
	items, err := listRunners(s.store, -1, 0)
	if err != nil {
		writeError(w, err)
		return
	}
	s.mergeMetaLabels(items)
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id": s.nodeID,
		"runners": items,
	})
}

func (s *Server) handleCluster(w http.ResponseWriter, _ *http.Request) {
	runners, err := listRunners(s.store, -1, 0)
	if err != nil {
		writeError(w, err)
		return
	}
	var (
		activeRunners int
		capacity      int
		avail         int
		cpuUtil       float64
		memUtil       float64
	)
	for _, r := range runners {
		if r.Status == model.RunnerActive {
			activeRunners++
			capacity += r.WorkerCapacity
			avail += r.AvailableWorkers
			cpuUtil += r.CPUUtil
			memUtil += r.MemUtil
		}
	}
	if activeRunners > 0 {
		cpuUtil /= float64(activeRunners)
		memUtil /= float64(activeRunners)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active_runners":    activeRunners,
		"worker_capacity":   capacity,
		"available_workers": avail,
		"avg_cpu_util":      cpuUtil,
		"avg_mem_util":      memUtil,
	})
}

// --- Placement / Scoring ---

type ScoredRunner struct {
	Runner model.Runner `json:"runner"`
	Score  float64      `json:"score"`
}

func scoreRunner(r model.Runner) float64 {
	base := float64(r.AvailableWorkers)*10 - r.CPUUtil*50 - r.MemUtil*30 + float64(r.FreeDiskMB)/1000
	jitter := (rand.Float64() * 10) - 5
	return base + jitter
}

func (s *Server) handleBestRunner(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.PlacementRequests.Inc()
	defer func() {
		metrics.PlacementLatency.Observe(time.Since(start))
	}()

	runners, err := listRunners(s.store, -1, 0)
	if err != nil {
		metrics.PlacementErrors.Inc()
		writeError(w, err)
		return
	}
	s.mergeMetaLabels(runners)

	region := r.URL.Query().Get("region")
	labels := r.URL.Query()["label"]

	var allEligible []model.Runner
	var regionEligible []model.Runner

	for _, rn := range runners {
		if rn.Status != model.RunnerActive {
			continue
		}
		if rn.AvailableWorkers == 0 {
			continue
		}

		matchLabels := true
		for _, lbl := range labels {
			parts := strings.SplitN(lbl, ":", 2)
			if len(parts) == 2 {
				if rn.Labels[parts[0]] != parts[1] {
					matchLabels = false
					break
				}
			}
		}
		if !matchLabels {
			continue
		}

		allEligible = append(allEligible, rn)
		if region == "" || rn.Region == region {
			regionEligible = append(regionEligible, rn)
		}
	}

	var finalEligible []model.Runner
	applyPenalty := false

	if len(regionEligible) > 0 {
		finalEligible = regionEligible
	} else if len(allEligible) > 0 {
		finalEligible = allEligible
		applyPenalty = true
	}

	if len(finalEligible) == 0 {
		metrics.PlacementErrors.Inc()
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "no available runners"})
		return
	}

	scored := make([]ScoredRunner, len(finalEligible))
	for i, rn := range finalEligible {
		sc := scoreRunner(rn)
		if applyPenalty {
			sc -= 1000
		}
		scored[i] = ScoredRunner{Runner: rn, Score: sc}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	writeJSON(w, http.StatusOK, scored)
}

// --- Helpers ---

func listRunners(st *store.Store, limit, offset int) ([]model.Runner, error) {
	var out []model.Runner
	err := st.List(store.KindRunner, limit, offset, nil, func(r store.Row) bool {
		item, err := model.Decode[model.Runner](r.Payload)
		if err != nil {
			return true
		}
		out = append(out, item)
		return true
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) mergeMetaLabels(runners []model.Runner) {
	for i := range runners {
		s.mergeMetaLabelsOne(&runners[i])
	}
}

func (s *Server) mergeMetaLabelsOne(runner *model.Runner) {
	row, ok, err := s.store.Get(store.KindMeta, runner.RunnerID)
	if err != nil || !ok || row.Tombstone {
		return
	}
	var extra map[string]string
	if err := json.Unmarshal(row.Payload, &extra); err != nil {
		return
	}
	if runner.Labels == nil {
		runner.Labels = make(map[string]string)
	}
	for k, v := range extra {
		runner.Labels[k] = v
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(b)
}

func (s *Server) observeHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		metrics.HTTPRequests.Inc(rec.status)
	})
}

func (s *Server) adminOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "admin token required"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authorized(r *http.Request) bool {
	if s.adminToken == "" {
		return false
	}
	got := r.Header.Get("X-Takl-Admin-Token")
	if got == "" {
		got = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.adminToken)) == 1
}

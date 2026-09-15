package api

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"takl/internal/engine/store"
	"takl/internal/model"
)

type Server struct {
	store  *store.Store
	nodeID string
	clock  *model.Clock
}

func New(st *store.Store, nodeID string, clock *model.Clock) *Server {
	return &Server{store: st, nodeID: nodeID, clock: clock}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /api/v1/runners/best", s.handleBestRunner)
	mux.HandleFunc("GET /api/v1/runners", s.handleRunners)
	mux.HandleFunc("GET /api/v1/runners/{id}", s.handleRunner)
	mux.HandleFunc("GET /api/v1/runners/{id}/labels", s.handleGetLabels)
	mux.HandleFunc("PUT /api/v1/runners/{id}/labels", s.handleUpdateLabels)
	mux.HandleFunc("DELETE /api/v1/runners/{id}/labels", s.handleDeleteLabels)
	mux.HandleFunc("GET /api/v1/events", s.handleEvents)
	mux.HandleFunc("GET /api/v1/summary", s.handleSummary)
	mux.HandleFunc("GET /api/v1/cluster", s.handleCluster)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "node_id": s.nodeID})
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
	runners, err := listRunners(s.store, -1, 0)
	if err != nil {
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

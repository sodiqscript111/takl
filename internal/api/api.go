package api

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"takl/internal/engine/store"
	"takl/internal/model"
)

type Server struct {
	store	*store.Store
	nodeID	string
}

func New(st *store.Store, nodeID string) *Server {
	return &Server{store: st, nodeID: nodeID}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /api/v1/runners/best", s.handleBestRunner)
	mux.HandleFunc("GET /api/v1/runners/for-project/{projectId}", s.handleRunnersForProject)
	mux.HandleFunc("GET /api/v1/runners", s.handleRunners)
	mux.HandleFunc("GET /api/v1/runners/{id}", s.handleRunner)
	mux.HandleFunc("GET /api/v1/capacity/check", s.handleCapacityCheck)
	mux.HandleFunc("GET /api/v1/builds", s.handleBuilds)
	mux.HandleFunc("GET /api/v1/queues", s.handleQueues)
	mux.HandleFunc("GET /api/v1/containers", s.handleContainers)
	mux.HandleFunc("GET /api/v1/mounts", s.handleMounts)
	mux.HandleFunc("GET /api/v1/images", s.handleImages)
	mux.HandleFunc("GET /api/v1/images/{digest}/runners", s.handleImageRunners)
	mux.HandleFunc("GET /api/v1/events", s.handleEvents)
	mux.HandleFunc("GET /api/v1/summary", s.handleSummary)
	mux.HandleFunc("GET /api/v1/cluster", s.handleCluster)
	mux.HandleFunc("GET /api/v1/analytics/hot-projects", s.handleHotProjects)
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
	items, err := list[model.Runner](s.store, store.KindRunner, limit, offset, nil)
	if err != nil {
		writeError(w, err)
		return
	}
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
	var item model.Runner
	item, err = model.Decode[model.Runner](row.Payload)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) handleCapacityCheck(w http.ResponseWriter, r *http.Request) {
	maxStr := r.URL.Query().Get("max")
	if maxStr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing max parameter"})
		return
	}
	maxBuilds, err := strconv.Atoi(maxStr)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid max parameter"})
		return
	}

	runners, err := list[model.Runner](s.store, store.KindRunner, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}

	var activeBuilds int
	for _, rn := range runners {
		if rn.Status == model.RunnerActive {
			activeBuilds += rn.ActiveBuilds
		}
	}

	allowed := activeBuilds < maxBuilds
	status := http.StatusOK
	if !allowed {
		status = http.StatusTooManyRequests
	}

	writeJSON(w, status, map[string]any{
		"allowed":	allowed,
		"current":	activeBuilds,
		"max":		maxBuilds,
	})
}

func (s *Server) handleBuilds(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	runner := r.URL.Query().Get("runner")
	status := r.URL.Query().Get("status")
	project := r.URL.Query().Get("project")

	filters := make(map[string]any)
	if runner != "" {
		filters["runner_id"] = runner
	}
	if status != "" {
		filters["status"] = status
	}
	if project != "" {
		filters["project_id"] = project
	}
	items, err := list[model.Build](s.store, store.KindBuild, limit, offset, filters)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, err := list[model.QueueStats](s.store, store.KindQueue, limit, offset, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleContainers(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	runner := r.URL.Query().Get("runner")
	status := r.URL.Query().Get("status")

	filters := make(map[string]any)
	if runner != "" {
		filters["runner_id"] = runner
	}
	if status != "" {
		filters["status"] = status
	}
	items, err := list[model.Container](s.store, store.KindContainer, limit, offset, filters)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleMounts(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, err := list[model.Mount](s.store, store.KindMount, limit, offset, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleImages(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, err := list[model.Image](s.store, store.KindImage, limit, offset, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) handleImageRunners(w http.ResponseWriter, r *http.Request) {
	digest := r.PathValue("digest")
	images, err := list[model.Image](s.store, store.KindImage, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}

	runners, err := list[model.Runner](s.store, store.KindRunner, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	runnerMap := make(map[string]model.Runner)
	for _, rn := range runners {
		if rn.Status == model.RunnerActive {
			runnerMap[rn.RunnerID] = rn
		}
	}

	var eligible []model.Runner
	seen := make(map[string]bool)
	for _, img := range images {
		if img.Digest == digest {
			if rn, ok := runnerMap[img.RunnerID]; ok {
				if !seen[rn.RunnerID] {
					seen[rn.RunnerID] = true
					eligible = append(eligible, rn)
				}
			}
		}
	}

	if len(eligible) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "no runners found for image"})
		return
	}

	scored := make([]ScoredRunner, len(eligible))
	for i, rn := range eligible {
		scored[i] = ScoredRunner{Runner: rn, Score: scoreRunner(rn)}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	writeJSON(w, http.StatusOK, scored)
}

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

func (s *Server) handleSummary(w http.ResponseWriter, _ *http.Request) {
	items, err := list[model.Runner](s.store, store.KindRunner, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":	s.nodeID,
		"runners":	items,
	})
}

func (s *Server) handleCluster(w http.ResponseWriter, _ *http.Request) {
	runners, err := list[model.Runner](s.store, store.KindRunner, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	var (
		activeRunners	int
		capacity	int
		avail		int
		activeBuilds	int
		cpuUtil		float64
		memUtil		float64
	)
	for _, r := range runners {
		if r.Status == model.RunnerActive {
			activeRunners++
			capacity += r.WorkerCapacity
			avail += r.AvailableWorkers
			activeBuilds += r.ActiveBuilds
			cpuUtil += r.CPUUtil
			memUtil += r.MemUtil
		}
	}
	if activeRunners > 0 {
		cpuUtil /= float64(activeRunners)
		memUtil /= float64(activeRunners)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active_runners":	activeRunners,
		"worker_capacity":	capacity,
		"available_workers":	avail,
		"active_builds":	activeBuilds,
		"avg_cpu_util":		cpuUtil,
		"avg_mem_util":		memUtil,
	})
}

func (s *Server) handleHotProjects(w http.ResponseWriter, r *http.Request) {
	builds, err := list[model.Build](s.store, store.KindBuild, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}

	cutoff := time.Now().Unix() - 86400
	counts := make(map[string]int)

	for _, b := range builds {
		if b.StartedAt >= cutoff {
			counts[b.ProjectID]++
		}
	}

	type projectFreq struct {
		ProjectID	string	`json:"project_id"`
		Count		int	`json:"count"`
	}

	var freqs []projectFreq
	for pid, count := range counts {
		freqs = append(freqs, projectFreq{ProjectID: pid, Count: count})
	}

	sort.Slice(freqs, func(i, j int) bool {
		return freqs[i].Count > freqs[j].Count
	})

	if len(freqs) > 10 {
		freqs = freqs[:10]
	}

	writeJSON(w, http.StatusOK, freqs)
}

func list[T any](st *store.Store, kind store.Kind, limit, offset int, filters map[string]any) ([]T, error) {
	var out []T
	err := st.List(kind, limit, offset, filters, func(r store.Row) bool {
		item, err := model.Decode[T](r.Payload)
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
}

type ScoredRunner struct {
	Runner	model.Runner	`json:"runner"`
	Score	float64		`json:"score"`
}

func scoreRunner(r model.Runner) float64 {
	base := float64(r.AvailableWorkers)*10 - r.CPUUtil*50 - r.MemUtil*30 + float64(r.FreeDiskMB)/1000
	jitter := (rand.Float64() * 10) - 5
	return base + jitter
}

func (s *Server) handleBestRunner(w http.ResponseWriter, r *http.Request) {
	runners, err := list[model.Runner](s.store, store.KindRunner, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}

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

	var cacheHitSet map[string]bool
	if project := r.URL.Query().Get("project"); project != "" {
		builds, err := list[model.Build](s.store, store.KindBuild, -1, 0, nil)
		if err != nil {
			writeError(w, err)
			return
		}
		cacheHitSet = make(map[string]bool)
		for _, b := range builds {
			if b.ProjectID == project && b.Status == model.BuildFinished {
				cacheHitSet[b.RunnerID] = true
			}
		}
	}

	scored := make([]ScoredRunner, len(finalEligible))
	for i, rn := range finalEligible {
		s := scoreRunner(rn)
		if cacheHitSet != nil && cacheHitSet[rn.RunnerID] {
			s += 20
		}
		if applyPenalty {
			s -= 1000
		}
		scored[i] = ScoredRunner{Runner: rn, Score: s}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	writeJSON(w, http.StatusOK, scored)
}

func (s *Server) handleRunnersForProject(w http.ResponseWriter, r *http.Request) {
	projectID := r.PathValue("projectId")
	labels := r.URL.Query()["label"]

	builds, err := list[model.Build](s.store, store.KindBuild, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	cacheRunnerIDs := make(map[string]bool)
	for _, b := range builds {
		if b.ProjectID == projectID && b.Status == model.BuildFinished {
			cacheRunnerIDs[b.RunnerID] = true
		}
	}

	runners, err := list[model.Runner](s.store, store.KindRunner, -1, 0, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	var eligible []model.Runner
	for _, rn := range runners {
		if !cacheRunnerIDs[rn.RunnerID] {
			continue
		}
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

		eligible = append(eligible, rn)
	}

	if len(eligible) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "no available runners"})
		return
	}

	scored := make([]ScoredRunner, len(eligible))
	for i, rn := range eligible {
		scored[i] = ScoredRunner{Runner: rn, Score: scoreRunner(rn)}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	writeJSON(w, http.StatusOK, scored)
}

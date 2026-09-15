package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"takl/internal/engine/store"
	"takl/internal/model"
)

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(":memory:", "test-node")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func putRunner(t *testing.T, st *store.Store, r model.Runner) {
	t.Helper()
	payload, err := model.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(store.Row{
		Kind:    store.KindRunner,
		Key:     r.RunnerID,
		Owner:   "test-node",
		HLC:     model.HLC{TS: 1, Seq: 0},
		Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBestRunnerRanking(t *testing.T) {
	st := openTestStore(t)

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-a",
		Status:           model.RunnerActive,
		CPUUtil:          0.9,
		MemUtil:          0.9,
		FreeDiskMB:       1000,
		WorkerCapacity:   4,
		AvailableWorkers: 0,
	})

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-b",
		Status:           model.RunnerActive,
		CPUUtil:          0.1,
		MemUtil:          0.2,
		FreeDiskMB:       5000,
		WorkerCapacity:   8,
		AvailableWorkers: 4,
	})

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-c",
		Status:           model.RunnerActive,
		CPUUtil:          0.5,
		MemUtil:          0.5,
		FreeDiskMB:       3000,
		WorkerCapacity:   4,
		AvailableWorkers: 2,
	})

	srv := New(st, "test-node", model.NewClock(nil))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runners/best", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result []ScoredRunner
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 runners, got %d", len(result))
	}

	if result[0].Runner.RunnerID != "runner-b" {
		t.Errorf("expected runner-b first, got %s", result[0].Runner.RunnerID)
	}
	if result[1].Runner.RunnerID != "runner-c" {
		t.Errorf("expected runner-c second, got %s", result[1].Runner.RunnerID)
	}

	if result[0].Score <= result[1].Score {
		t.Errorf("runner-b score (%.2f) should be > runner-c score (%.2f)", result[0].Score, result[1].Score)
	}
}

func TestLabelsCRUD(t *testing.T) {
	st := openTestStore(t)
	clock := model.NewClock(nil)

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-x",
		Status:           model.RunnerActive,
		WorkerCapacity:   4,
		AvailableWorkers: 4,
		Labels:           map[string]string{"role": "worker"},
	})

	srv := New(st, "test-node", clock)

	// 1. GET initial labels
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runners/runner-x/labels", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var labels map[string]string
	_ = json.NewDecoder(rec.Body).Decode(&labels)
	if labels["role"] != "worker" {
		t.Fatalf("expected role:worker, got %+v", labels)
	}

	// 2. PUT new labels
	body := strings.NewReader(`{"env":"prod","gpu":"true"}`)
	req = httptest.NewRequest(http.MethodPut, "/api/v1/runners/runner-x/labels", body)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT labels failed: %d %s", rec.Code, rec.Body.String())
	}

	// 3. GET merged labels
	req = httptest.NewRequest(http.MethodGet, "/api/v1/runners/runner-x/labels", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	labels = nil
	_ = json.NewDecoder(rec.Body).Decode(&labels)
	if labels["role"] != "worker" || labels["env"] != "prod" || labels["gpu"] != "true" {
		t.Fatalf("labels not merged: %+v", labels)
	}

	// 4. GET /runners/best with label filtering matching the new label
	req = httptest.NewRequest(http.MethodGet, "/api/v1/runners/best?label=gpu:true", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("best runner with label filter failed: %d", rec.Code)
	}
	var scored []ScoredRunner
	_ = json.NewDecoder(rec.Body).Decode(&scored)
	if len(scored) != 1 || scored[0].Runner.RunnerID != "runner-x" {
		t.Fatalf("expected runner-x matching gpu:true, got %+v", scored)
	}

	// 5. DELETE labels
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/runners/runner-x/labels", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}

	// 6. Verify extra labels removed, original remains
	req = httptest.NewRequest(http.MethodGet, "/api/v1/runners/runner-x/labels", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	labels = nil
	_ = json.NewDecoder(rec.Body).Decode(&labels)
	if labels["role"] != "worker" || labels["gpu"] != "" {
		t.Fatalf("expected only original labels after delete, got %+v", labels)
	}
}



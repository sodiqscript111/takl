package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func putBuild(t *testing.T, st *store.Store, b model.Build) {
	t.Helper()
	payload, err := model.Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(store.Row{
		Kind:    store.KindBuild,
		Key:     b.BuildID,
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

	srv := New(st, "test-node")
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

func TestCacheAwareLookup(t *testing.T) {
	st := openTestStore(t)

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-a",
		Status:           model.RunnerActive,
		CPUUtil:          0.3,
		MemUtil:          0.3,
		FreeDiskMB:       2000,
		WorkerCapacity:   4,
		AvailableWorkers: 2,
	})

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-b",
		Status:           model.RunnerActive,
		CPUUtil:          0.2,
		MemUtil:          0.2,
		FreeDiskMB:       3000,
		WorkerCapacity:   8,
		AvailableWorkers: 4,
	})

	putBuild(t, st, model.Build{
		BuildID:   "build-1",
		ProjectID: "web-app",
		RunnerID:  "runner-a",
		Status:    model.BuildFinished,
		StartedAt: 1000,
		UpdatedAt: 2000,
	})

	srv := New(st, "test-node")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runners/for-project/web-app", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result []ScoredRunner
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 runner, got %d", len(result))
	}
	if result[0].Runner.RunnerID != "runner-a" {
		t.Errorf("expected runner-a, got %s", result[0].Runner.RunnerID)
	}
}

func TestCombinedScoringWithCacheBonus(t *testing.T) {
	st := openTestStore(t)

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-a",
		Status:           model.RunnerActive,
		CPUUtil:          0.3,
		MemUtil:          0.3,
		FreeDiskMB:       2000,
		WorkerCapacity:   4,
		AvailableWorkers: 3,
	})

	putRunner(t, st, model.Runner{
		RunnerID:         "runner-b",
		Status:           model.RunnerActive,
		CPUUtil:          0.2,
		MemUtil:          0.2,
		FreeDiskMB:       3000,
		WorkerCapacity:   4,
		AvailableWorkers: 3,
	})

	putBuild(t, st, model.Build{
		BuildID:   "build-1",
		ProjectID: "web-app",
		RunnerID:  "runner-a",
		Status:    model.BuildFinished,
		StartedAt: 1000,
		UpdatedAt: 2000,
	})

	srv := New(st, "test-node")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runners/best?project=web-app", nil)
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

	if result[0].Runner.RunnerID != "runner-a" {
		t.Errorf("expected runner-a first (cache bonus), got %s (score=%.2f)",
			result[0].Runner.RunnerID, result[0].Score)
	}
	if result[1].Runner.RunnerID != "runner-b" {
		t.Errorf("expected runner-b second, got %s (score=%.2f)",
			result[1].Runner.RunnerID, result[1].Score)
	}

	if result[0].Score <= result[1].Score {
		t.Errorf("runner-a score (%.2f) should be > runner-b score (%.2f)",
			result[0].Score, result[1].Score)
	}
}

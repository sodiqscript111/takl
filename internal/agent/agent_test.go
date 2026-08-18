package agent

import (
	"reflect"
	"testing"

	"takl/internal/backend/stub"
	"takl/internal/engine/store"
	"takl/internal/model"
)

func newTestAgent(t *testing.T, seed int64) (*Agent, *store.Store, *stub.Stub) {
	t.Helper()
	st, err := store.Open(":memory:", "r1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	var now int64 = 1_700_000_000
	clock := model.NewClock(func() int64 {
		now++
		return now
	})
	s := stub.New(stub.Config{NodeID: "r1", Capacity: 4})
	return New("r1", st, s, clock, nil), st, s
}

func TestAgentWritesRunnerRow(t *testing.T) {
	a, st, s := newTestAgent(t, 42)
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := a.Step(); err != nil {
			t.Fatal(err)
		}
	}
	var rows []store.Row
	err := st.List(store.KindRunner, -1, 0, nil, func(r store.Row) bool {
		rows = append(rows, r)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want exactly 1 runner row, got %d", len(rows))
	}
	row := rows[0]
	if len(row.Owner) < 3 || row.Owner[:3] != "r1/" || row.Tombstone {
		t.Fatalf("bad runner row: %+v", row)
	}
	var got model.Runner
	got, err = model.Decode[model.Runner](row.Payload)
	if err != nil {
		t.Fatal(err)
	}
	want := s.Snapshot()
	want.Status = got.Status
	want.LastHeartbeat = got.LastHeartbeat
	want.CPUUtil = got.CPUUtil
	want.MemUtil = got.MemUtil
	want.FreeDiskMB = got.FreeDiskMB
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("store snapshot diverges from sim:\n%+v\n%+v", got, want)
	}
}

func TestAgentHLCMonotonic(t *testing.T) {
	a, st, _ := newTestAgent(t, 42)
	a.Start()
	prev := model.HLC{}
	for i := 0; i < 20; i++ {
		a.Step()
		row, _, err := st.Get(store.KindRunner, "r1")
		if err != nil {
			t.Fatal(err)
		}
		if !row.HLC.After(prev) {
			t.Fatalf("hlc not monotonic: %+v then %+v", prev, row.HLC)
		}
		prev = row.HLC
	}
}

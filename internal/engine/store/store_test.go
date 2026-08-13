package store

import (
	"encoding/json"
	"testing"

	"takl/internal/model"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	st, err := Open(":memory:", "test-node")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestPutGetRoundTrip(t *testing.T) {
	st := openTest(t)
	payload, err := json.Marshal(model.Runner{RunnerID: "r1", Hostname: "host-r1", CPUUtil: 0.5})
	if err != nil {
		t.Fatal(err)
	}
	row := Row{Kind: KindRunner, Key: "r1", Owner: "test-node", HLC: model.HLC{TS: 10, Seq: 0}, Payload: payload}
	if err := st.Put(row); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.Get(KindRunner, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("row not found")
	}
	if got.HLC.TS != 10 || got.Owner != "test-node" || got.Tombstone {
		t.Fatalf("bad row metadata: %+v", got)
	}
	var r model.Runner
	if err := json.Unmarshal(got.Payload, &r); err != nil {
		t.Fatal(err)
	}
	if r.RunnerID != "r1" || r.CPUUtil != 0.5 {
		t.Fatalf("payload mismatch: %+v", r)
	}
}

func TestGetMissing(t *testing.T) {
	st := openTest(t)
	if _, ok, err := st.Get(KindRunner, "nope"); err != nil || ok {
		t.Fatalf("expected miss, ok=%v err=%v", ok, err)
	}
}

func TestPutOverwrites(t *testing.T) {
	st := openTest(t)
	row := func(ts int64, cpu float64) Row {
		b, _ := json.Marshal(model.Runner{RunnerID: "r1", CPUUtil: cpu})
		return Row{Kind: KindRunner, Key: "r1", Owner: "n", HLC: model.HLC{TS: ts}, Payload: b}
	}
	if err := st.Put(row(10, 0.1)); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(row(5, 0.9)); err != nil {
		t.Fatal(err)
	}
	got, _, _ := st.Get(KindRunner, "r1")
	var r model.Runner
	json.Unmarshal(got.Payload, &r)
	if r.CPUUtil != 0.9 {
		t.Fatalf("Put should always overwrite locally, got %+v", r)
	}
}

func TestApplyLWW(t *testing.T) {
	st := openTest(t)
	apply := func(ts int64, owner string) {
		t.Helper()
		if err := st.Apply(Row{Kind: KindRunner, Key: "r1", Owner: owner, HLC: model.HLC{TS: ts}, Payload: []byte(owner)}); err != nil {
			t.Fatal(err)
		}
	}
	apply(5, "a")
	apply(3, "b")
	if got, _, _ := st.Get(KindRunner, "r1"); got.Owner != "a" {
		t.Fatalf("older write won: %+v", got)
	}
	apply(5, "b")
	if got, _, _ := st.Get(KindRunner, "r1"); got.Owner != "b" {
		t.Fatalf("equal hlc tiebreak failed: %+v", got)
	}
	apply(5, "a")
	if got, _, _ := st.Get(KindRunner, "r1"); got.Owner != "b" {
		t.Fatalf("lower owner must not win on equal hlc: %+v", got)
	}
	apply(9, "c")
	if got, _, _ := st.Get(KindRunner, "r1"); got.Owner != "c" {
		t.Fatalf("newer write lost: %+v", got)
	}
}

func TestListExcludesTombstones(t *testing.T) {
	st := openTest(t)
	put := func(key string, ts int64) {
		t.Helper()
		if err := st.Put(Row{Kind: KindBuild, Key: key, Owner: "n", HLC: model.HLC{TS: ts}, Payload: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}
	put("b1", 1)
	put("b2", 2)
	if err := st.Delete(KindBuild, "b1", "n", model.HLC{TS: 3}); err != nil {
		t.Fatal(err)
	}
	var rows []Row
	err := st.List(KindBuild, -1, 0, nil, func(r Row) bool {
		rows = append(rows, r)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Key != "b2" {
		t.Fatalf("tombstone leaked into list: %+v", rows)
	}
	if got, ok, _ := st.Get(KindBuild, "b1"); !ok || !got.Tombstone {
		t.Fatalf("tombstoned row should still be readable raw: %+v ok=%v", got, ok)
	}
}

func TestChangesSince(t *testing.T) {
	st := openTest(t)
	put := func(key string, ts, seq int64) {
		t.Helper()
		if err := st.Put(Row{Kind: KindBuild, Key: key, Owner: "n", HLC: model.HLC{TS: ts, Seq: seq}, Payload: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}
	put("b1", 1, 0)
	put("b2", 3, 0)
	put("b3", 3, 2)
	put("b4", 5, 0)
	rows, err := st.ChangesSince(KindBuild, model.HLC{TS: 3, Seq: 1})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.Key] = true
	}
	if len(rows) != 2 || !got["b3"] || !got["b4"] {
		t.Fatalf("ChangesSince wrong: %v", got)
	}
	if err := st.Delete(KindBuild, "b2", "n", model.HLC{TS: 4, Seq: 0}); err != nil {
		t.Fatal(err)
	}
	rows, err = st.ChangesSince(KindBuild, model.HLC{TS: 3, Seq: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("tombstones must be included in ChangesSince: %d", len(rows))
	}
}

func TestEvents(t *testing.T) {
	st := openTest(t)
	for i := 0; i < 3; i++ {
		if err := st.AppendEvent(model.Event{
			Type:		model.EventBuildStarted,
			RunnerID:	"r1",
			Payload:	map[string]any{"i": i},
			HLC:		model.HLC{TS: int64(i + 1)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := st.Events(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 {
		t.Fatalf("want 3 events, got %d", len(evs))
	}
	for i, e := range evs {
		if e.Seq != int64(i+1) || e.Type != model.EventBuildStarted {
			t.Fatalf("bad event %d: %+v", i, e)
		}
		if e.Payload["i"] != float64(i) {
			t.Fatalf("payload mismatch: %+v", e.Payload)
		}
	}
	evs, err = st.Events(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 || evs[0].Seq != 2 {
		t.Fatalf("limit broken: %+v", evs)
	}
}

func TestWatermarkMonotonic(t *testing.T) {
	st := openTest(t)
	if wm, err := st.Watermark("peer1", KindRunner); err != nil || (wm != model.HLC{}) {
		t.Fatalf("expected zero watermark, got %+v err=%v", wm, err)
	}
	if err := st.SetWatermark("peer1", KindRunner, model.HLC{TS: 5, Seq: 2}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetWatermark("peer1", KindRunner, model.HLC{TS: 3, Seq: 9}); err != nil {
		t.Fatal(err)
	}
	wm, err := st.Watermark("peer1", KindRunner)
	if err != nil {
		t.Fatal(err)
	}
	if wm.TS != 5 || wm.Seq != 2 {
		t.Fatalf("watermark went backwards: %+v", wm)
	}
	if err := st.SetWatermark("peer1", KindRunner, model.HLC{TS: 5, Seq: 5}); err != nil {
		t.Fatal(err)
	}
	wm, _ = st.Watermark("peer1", KindRunner)
	if wm.Seq != 5 {
		t.Fatalf("watermark did not advance: %+v", wm)
	}
	if err := st.SetWatermark("peer2", KindBuild, model.HLC{TS: 1}); err != nil {
		t.Fatal(err)
	}
	if wm, _ := st.Watermark("peer1", KindBuild); wm != (model.HLC{}) {
		t.Fatalf("watermark not scoped by kind: %+v", wm)
	}
}

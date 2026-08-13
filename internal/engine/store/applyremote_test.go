package store

import (
	"encoding/json"
	"testing"

	"takl/internal/model"
)

func TestApplyRemoteBatch(t *testing.T) {
	st := openTest(t)
	payloadA, _ := model.Encode(model.Runner{RunnerID: "r1", CPUUtil: 0.5})
	payloadB, _ := model.Encode(model.Runner{RunnerID: "r1", CPUUtil: 0.9})
	rows := []Row{
		{Kind: KindRunner, Key: "r1", Owner: "peer-x", HLC: model.HLC{TS: 10, Seq: 0}, Payload: payloadA},
		{Kind: KindBuild, Key: "b1", Owner: "peer-x", HLC: model.HLC{TS: 20, Seq: 0}, Payload: []byte("b1")},
		{Kind: KindRunner, Key: "r1", Owner: "peer-x", HLC: model.HLC{TS: 5, Seq: 0}, Payload: payloadB},
	}
	if err := st.ApplyRemote("peer-x", rows); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.Get(KindRunner, "r1")
	if err != nil || !ok {
		t.Fatalf("row missing: ok=%v err=%v", ok, err)
	}
	var r model.Runner
	json.Unmarshal(got.Payload, &r)
	if r.CPUUtil != 0.5 {
		t.Fatalf("older row in batch won LWW: %+v", r)
	}
	if got.Owner != "peer-x" {
		t.Fatalf("owner not preserved: %+v", got)
	}
	wm, err := st.Watermark("peer-x", KindRunner)
	if err != nil {
		t.Fatal(err)
	}
	if wm.TS != 10 {
		t.Fatalf("runner watermark should be max batch hlc (10), got %+v", wm)
	}
	wm, _ = st.Watermark("peer-x", KindBuild)
	if wm.TS != 20 {
		t.Fatalf("build watermark wrong: %+v", wm)
	}
	if wm, _ := st.Watermark("peer-x", KindQueue); wm != (model.HLC{}) {
		t.Fatalf("kind not in batch advanced watermark: %+v", wm)
	}
}

func TestApplyRemoteTombstonesAndWatermark(t *testing.T) {
	st := openTest(t)
	rows := []Row{
		{Kind: KindBuild, Key: "b1", Owner: "peer-x", HLC: model.HLC{TS: 1}, Payload: []byte("b1")},
	}
	if err := st.ApplyRemote("peer-x", rows); err != nil {
		t.Fatal(err)
	}
	rows = []Row{
		{Kind: KindBuild, Key: "b1", Owner: "peer-x", HLC: model.HLC{TS: 2}, Tombstone: true, Payload: []byte("b1")},
	}
	if err := st.ApplyRemote("peer-x", rows); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := st.Get(KindBuild, "b1")
	if !ok || !got.Tombstone {
		t.Fatalf("tombstone not applied: %+v ok=%v", got, ok)
	}
	if wm, _ := st.Watermark("peer-x", KindBuild); wm.TS != 2 {
		t.Fatalf("watermark did not advance to tombstone hlc: %+v", wm)
	}
}

func TestApplyRemoteWatermarkNeverRegresses(t *testing.T) {
	st := openTest(t)
	st.ApplyRemote("peer-x", []Row{{Kind: KindRunner, Key: "r1", Owner: "peer-x", HLC: model.HLC{TS: 50}, Payload: []byte("x")}})
	st.ApplyRemote("peer-x", []Row{
		{Kind: KindRunner, Key: "r1", Owner: "peer-x", HLC: model.HLC{TS: 10}, Payload: []byte("y")},
		{Kind: KindRunner, Key: "r9", Owner: "peer-x", HLC: model.HLC{TS: 20}, Payload: []byte("z")},
	})
	wm, _ := st.Watermark("peer-x", KindRunner)
	if wm.TS != 50 {
		t.Fatalf("watermark regressed: %+v", wm)
	}
	got, ok, _ := st.Get(KindRunner, "r1")
	if !ok || got.HLC.TS != 50 {
		t.Fatalf("old batch overwrote newer row: %+v", got)
	}
	if _, ok, _ := st.Get(KindRunner, "r9"); !ok {
		t.Fatal("new key in old batch not applied")
	}
}

func TestApplyRemoteEmpty(t *testing.T) {
	st := openTest(t)
	if err := st.ApplyRemote("peer-x", nil); err != nil {
		t.Fatal(err)
	}
}

func TestApplyRemoteIdempotent(t *testing.T) {
	st := openTest(t)
	batch := []Row{
		{Kind: KindQueue, Key: "r1/builds", Owner: "peer-x", HLC: model.HLC{TS: 7, Seq: 3}, Payload: []byte("q")},
	}
	for i := 0; i < 3; i++ {
		if err := st.ApplyRemote("peer-x", batch); err != nil {
			t.Fatal(err)
		}
	}
	var rows []Row
	err := st.List(KindQueue, -1, 0, nil, func(r Row) bool {
		rows = append(rows, r)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("replay created duplicates: %+v", rows)
	}
	if wm, _ := st.Watermark("peer-x", KindQueue); wm.Seq != 3 {
		t.Fatalf("watermark wrong after replay: %+v", wm)
	}
}

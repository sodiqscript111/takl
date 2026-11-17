package store

import (
	"testing"

	"takl/internal/model"
)

// A remote row is applied but the sync engine has not yet called clock.Update
// for it, and a local write lands in between. The local write must still get a
// newer HLC than that row.
func TestPutLocalSupersedesRowAppliedBeforeClockUpdate(t *testing.T) {
	st := openTest(t)
	clock := model.NewClock(func() int64 { return 100 })

	remote := model.HLC{TS: 100, Seq: 5}
	if _, err := st.ApplyRemote(Remote{StoreID: "peer", Rows: []Row{{Kind: KindMeta, Key: "r1", Owner: "api/peer", HLC: remote, Payload: []byte(`{"a":"1"}`)}}}); err != nil {
		t.Fatal(err)
	}
	// No clock.Update(remote) here: that is the race.

	var sawPayload string
	r, err := st.PutLocal(KindMeta, "r1", "api/me", clock, func(cur Row, ok bool) ([]byte, bool, error) {
		if !ok {
			t.Fatal("PutLocal did not see the applied row")
		}
		sawPayload = string(cur.Payload)
		return []byte(`{"a":"1","b":"2"}`), false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sawPayload != `{"a":"1"}` {
		t.Fatalf("build saw %q", sawPayload)
	}
	if !r.HLC.After(remote) {
		t.Fatalf("local write HLC %+v is not after the row it replaced %+v", r.HLC, remote)
	}
	got, _, _ := st.Get(KindMeta, "r1")
	if got.HLC != r.HLC || string(got.Payload) != `{"a":"1","b":"2"}` {
		t.Fatalf("stored row %+v", got)
	}
	// The clock itself has moved past the remote row too.
	if next := clock.Tick(); !next.After(r.HLC) {
		t.Fatalf("clock went backwards: %+v after %+v", next, r.HLC)
	}
}

func TestPutLocalTombstoneKeepsPayload(t *testing.T) {
	st := openTest(t)
	clock := model.NewClock(func() int64 { return 1 })
	if _, err := st.PutLocal(KindMeta, "r1", "api/me", clock, func(Row, bool) ([]byte, bool, error) {
		return []byte(`{"a":"1"}`), false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutLocal(KindMeta, "r1", "api/me", clock, func(cur Row, _ bool) ([]byte, bool, error) {
		return cur.Payload, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	got, ok, _ := st.Get(KindMeta, "r1")
	if !ok || !got.Tombstone || string(got.Payload) != `{"a":"1"}` {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}

func TestDeleteByNodeSupersedesNewerRows(t *testing.T) {
	st := openTest(t)
	clock := model.NewClock(func() int64 { return 100 })

	remote := model.HLC{TS: 100, Seq: 7}
	if _, err := st.ApplyRemote(Remote{StoreID: "peer", Rows: []Row{
		{Kind: KindRunner, Key: "dead", Owner: "dead/epoch1", HLC: remote, Payload: []byte("x")},
		{Kind: KindRunner, Key: "alive", Owner: "alive/epoch1", HLC: remote, Payload: []byte("y")},
	}}); err != nil {
		t.Fatal(err)
	}

	hlc, err := st.DeleteByNode("dead", clock)
	if err != nil {
		t.Fatal(err)
	}
	if !hlc.After(remote) {
		t.Fatalf("tombstone HLC %+v is not after the row it replaced %+v", hlc, remote)
	}
	got, _, _ := st.Get(KindRunner, "dead")
	if !got.Tombstone || got.HLC != hlc {
		t.Fatalf("dead row %+v", got)
	}
	if other, _, _ := st.Get(KindRunner, "alive"); other.Tombstone {
		t.Fatalf("other node's row was tombstoned: %+v", other)
	}
}

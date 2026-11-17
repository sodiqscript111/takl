package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"takl/internal/model"
)

func remote(storeID string, version uint64, rows ...Row) Remote {
	return Remote{Addr: "addr-" + storeID, StoreID: storeID, NodeID: "node-" + storeID, Version: version, Rows: rows, NowMs: 1000}
}

func TestApplyRemoteBatch(t *testing.T) {
	st := openTest(t)
	payloadA, _ := model.Encode(model.Runner{RunnerID: "r1", CPUUtil: 0.5})
	payloadB, _ := model.Encode(model.Runner{RunnerID: "r1", CPUUtil: 0.9})
	if _, err := st.ApplyRemote(remote("px", 42,
		Row{Kind: KindRunner, Key: "r1", Owner: "peer-x", HLC: model.HLC{TS: 10}, Payload: payloadA},
		Row{Kind: KindMeta, Key: "m1", Owner: "peer-x", HLC: model.HLC{TS: 20}, Payload: []byte("m1")},
		Row{Kind: KindRunner, Key: "r1", Owner: "peer-x", HLC: model.HLC{TS: 5}, Payload: payloadB},
	)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := st.Get(KindRunner, "r1")
	if err != nil || !ok {
		t.Fatalf("row missing: ok=%v err=%v", ok, err)
	}
	var r model.Runner
	_ = json.Unmarshal(got.Payload, &r)
	if r.CPUUtil != 0.5 || got.Owner != "peer-x" {
		t.Fatalf("older row in batch won LWW: %+v", got)
	}
	p, err := st.PeerByAddr("addr-px")
	if err != nil {
		t.Fatal(err)
	}
	if p.StoreID != "px" || p.NodeID != "node-px" || p.Watermark != 42 {
		t.Fatalf("peer bookkeeping wrong: %+v", p)
	}
}

func TestApplyRemoteWatermarkNeverRegresses(t *testing.T) {
	st := openTest(t)
	_, _ = st.ApplyRemote(remote("px", 50, Row{Kind: KindRunner, Key: "r1", Owner: "o", HLC: model.HLC{TS: 50}, Payload: []byte("x")}))
	_, _ = st.ApplyRemote(remote("px", 20,
		Row{Kind: KindRunner, Key: "r1", Owner: "o", HLC: model.HLC{TS: 10}, Payload: []byte("y")},
		Row{Kind: KindRunner, Key: "r9", Owner: "o", HLC: model.HLC{TS: 20}, Payload: []byte("z")},
	))
	if p, _ := st.PeerByAddr("addr-px"); p.Watermark != 50 {
		t.Fatalf("watermark regressed: %+v", p)
	}
	if got, _, _ := st.Get(KindRunner, "r1"); got.HLC.TS != 50 {
		t.Fatalf("old batch overwrote newer row: %+v", got)
	}
	if _, ok, _ := st.Get(KindRunner, "r9"); !ok {
		t.Fatal("new key in old batch not applied")
	}
}

func TestApplyRemoteIdempotent(t *testing.T) {
	st := openTest(t)
	row := Row{Kind: KindMeta, Key: "r1", Owner: "o", HLC: model.HLC{TS: 7, Seq: 3}, Payload: []byte("q")}
	_, _ = st.ApplyRemote(remote("px", 1, row))
	v1, _ := st.Version()
	for i := 0; i < 3; i++ {
		n, err := st.ApplyRemote(remote("px", 1, row))
		if err != nil || n != 0 {
			t.Fatalf("replay applied %d rows, err=%v", n, err)
		}
	}
	if v2, _ := st.Version(); v2 != v1 {
		t.Fatalf("replaying an already-applied row bumped the version %d -> %d", v1, v2)
	}
}

// The bug model checking found in the old HLC-watermark protocol: a peer that already pulled
// past HLC 100 never got a row with HLC 50 that arrived here later. With local
// versions the late row gets a fresh version and is sent.
func TestRelayedOldRowIsSentToPeersAlreadyPastItsHLC(t *testing.T) {
	st := openTest(t)
	_, _ = st.ApplyRemote(remote("p1", 1, Row{Kind: KindMeta, Key: "new", Owner: "o1", HLC: model.HLC{TS: 100}, Payload: []byte("n")}))
	res, err := st.Serve(ServeRequest{StoreID: "reader", NowMs: 1})
	if err != nil {
		t.Fatal(err)
	}
	readerWatermark := res.Version

	_, _ = st.ApplyRemote(remote("p2", 1, Row{Kind: KindMeta, Key: "late", Owner: "o2", HLC: model.HLC{TS: 50}, Payload: []byte("l")}))

	res, err = st.Serve(ServeRequest{StoreID: "reader", PeerStoreID: res.StoreID, Since: readerWatermark, NowMs: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0].Key != "late" {
		t.Fatalf("late-arriving row with an old HLC was not sent: %+v", res.Rows)
	}
}

func TestGCWaitsForEveryKnownPeer(t *testing.T) {
	st := openTest(t)
	if err := st.Delete(KindMeta, "m1", "o", model.HLC{TS: 10}); err != nil {
		t.Fatal(err)
	}
	tombVer, _ := st.Version()
	id, _ := st.StoreID()

	// Peer A has pulled past the tombstone, peer B has only pulled from us once, before it.
	_, _ = st.Serve(ServeRequest{StoreID: "A", NowMs: 1})
	_, _ = st.Serve(ServeRequest{StoreID: "A", PeerStoreID: id, Since: tombVer, NowMs: 1})
	_, _ = st.Serve(ServeRequest{StoreID: "B", PeerStoreID: id, Since: tombVer - 1, NowMs: 1})

	if n, _ := st.GCTombstones(1000); n != 0 {
		t.Fatal("tombstone dropped although peer B has not pulled it")
	}
	_, _ = st.Serve(ServeRequest{StoreID: "B", PeerStoreID: id, Since: tombVer, NowMs: 2})
	if n, _ := st.GCTombstones(5); n != 0 {
		t.Fatal("tombstone dropped before the grace period")
	}
	if n, _ := st.GCTombstones(1000); n != 1 {
		t.Fatal("tombstone not dropped once every peer had pulled it")
	}
}

func TestPeersWeOnlyPullFromBlockGCUntilTheyPullUs(t *testing.T) {
	st := openTest(t)
	_ = st.Delete(KindMeta, "m1", "o", model.HLC{TS: 10})
	// We pulled from C, but C has never pulled from us: it may hold the old row.
	_, _ = st.ApplyRemote(remote("C", 3))
	if n, _ := st.GCTombstones(1000); n != 0 {
		t.Fatal("tombstone dropped although peer C never pulled it")
	}
}

func TestForgottenPeerStopsBlockingGCAndMustWipe(t *testing.T) {
	st := openTest(t)
	_ = st.Delete(KindMeta, "m1", "o", model.HLC{TS: 10})
	_, _ = st.ApplyRemote(remote("C", 3))
	if _, err := st.Serve(ServeRequest{StoreID: "C", NowMs: 100}); err != nil {
		t.Fatal(err)
	}

	if n, _ := st.ForgetPeers(500); n != 0 {
		t.Fatal("forgot a peer seen after the cutoff")
	}
	if n, _ := st.ForgetPeers(2000); n != 1 {
		t.Fatal("did not forget a peer silent since before the cutoff")
	}
	if n, _ := st.GCTombstones(1000); n != 1 {
		t.Fatal("forgotten peer still blocks GC")
	}
	res, err := st.Serve(ServeRequest{StoreID: "C", NowMs: 300})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Forgotten || len(res.Rows) != 0 {
		t.Fatalf("forgotten peer was served: %+v", res)
	}
	if _, err := st.ApplyRemote(remote("C", 9, Row{Kind: KindMeta, Key: "m1", Owner: "o", HLC: model.HLC{TS: 1}, Payload: []byte("old")})); !errors.Is(err, ErrPeerForgotten) {
		t.Fatalf("rows from a forgotten store were accepted: err=%v", err)
	}
	if _, ok, _ := st.Get(KindMeta, "m1"); ok {
		t.Fatal("deleted row came back from a forgotten peer")
	}
}

func TestWipeKeepsOnlyOwnRowsNeverSent(t *testing.T) {
	st := openTest(t) // node id "test-node"
	put := func(key, owner string) {
		t.Helper()
		if err := st.Put(Row{Kind: KindMeta, Key: key, Owner: owner, HLC: model.HLC{TS: 1}, Payload: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}
	put("own-sent", "api/test-node")
	put("other", "api/other")
	if _, err := st.Serve(ServeRequest{StoreID: "peer", NowMs: 1}); err != nil {
		t.Fatal(err)
	}
	put("own-unsent", "api/test-node")
	put("own-runner-unsent", "test-node/epoch")
	put("lookalike", "test-nodeX/epoch")
	oldID, _ := st.StoreID()

	newID, kept, err := st.Wipe()
	if err != nil {
		t.Fatal(err)
	}
	if newID == oldID || kept != 2 {
		t.Fatalf("new id %q (old %q), kept %d", newID, oldID, kept)
	}
	for key, want := range map[string]bool{"own-sent": false, "other": false, "own-unsent": true, "own-runner-unsent": true, "lookalike": false} {
		if _, ok, _ := st.Get(KindMeta, key); ok != want {
			t.Errorf("%s kept=%v, want %v", key, ok, want)
		}
	}
	if p, _, _ := st.Peer("peer"); p.Ack != 0 {
		t.Fatal("peer bookkeeping survived the wipe")
	}
}

func TestDeleteByNodeTreatsWildcardsLiterally(t *testing.T) {
	st := openTest(t)
	clock := model.NewClock(func() int64 { return 1 })
	_ = st.Put(Row{Kind: KindRunner, Key: "a_b", Owner: "a_b/e1", HLC: model.HLC{TS: 1}, Payload: []byte("x")})
	_ = st.Put(Row{Kind: KindRunner, Key: "axb", Owner: "axb/e1", HLC: model.HLC{TS: 1}, Payload: []byte("y")})
	_ = st.Put(Row{Kind: KindRunner, Key: "a%", Owner: "a%/e1", HLC: model.HLC{TS: 1}, Payload: []byte("z")})
	if _, err := st.DeleteByNode("a_b", clock); err != nil {
		t.Fatal(err)
	}
	if r, _, _ := st.Get(KindRunner, "a_b"); !r.Tombstone {
		t.Fatal("a_b's own row was not tombstoned")
	}
	for _, key := range []string{"axb", "a%"} {
		if r, _, _ := st.Get(KindRunner, key); r.Tombstone {
			t.Fatalf("DeleteByNode(a_b) tombstoned %s", key)
		}
	}
}

func TestEventsFromARecreatedStoreDoNotCollide(t *testing.T) {
	old := openTest(t)
	fresh := openTest(t) // same node id, new database: seq starts over at 1
	for _, st := range []*Store{old, fresh} {
		if err := st.AppendEvent(model.Event{Type: model.EventRunnerJoined, RunnerID: "test-node", HLC: model.HLC{TS: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	sink := openTest(t)
	for i, st := range []*Store{old, fresh} {
		res, err := st.Serve(ServeRequest{StoreID: "sink", NowMs: 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sink.ApplyRemote(Remote{StoreID: res.StoreID, Version: res.Version, Events: res.Events, NowMs: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	evs, _ := sink.Events(10)
	if len(evs) != 2 {
		t.Fatalf("event from the recreated store was dropped as a duplicate: %+v", evs)
	}
}

func TestMigratesHLCWatermarkSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE rows (kind TEXT NOT NULL, key TEXT NOT NULL, owner TEXT NOT NULL, hlc_ts INTEGER NOT NULL, hlc_seq INTEGER NOT NULL, tombstone INTEGER NOT NULL DEFAULT 0, payload BLOB NOT NULL, PRIMARY KEY (kind, key))`,
		`CREATE TABLE events_v2 (runner_id TEXT NOT NULL, seq INTEGER NOT NULL, type TEXT NOT NULL, payload TEXT NOT NULL, hlc_ts INTEGER NOT NULL, hlc_seq INTEGER NOT NULL, PRIMARY KEY (runner_id, seq))`,
		`CREATE TABLE watermarks (peer TEXT NOT NULL, kind TEXT NOT NULL, hlc_ts INTEGER NOT NULL, hlc_seq INTEGER NOT NULL, PRIMARY KEY (peer, kind))`,
		`INSERT INTO rows VALUES ('meta', 'm1', 'api/n', 5, 0, 0, 'x')`,
		`INSERT INTO events_v2 VALUES ('n', 1, 'RunnerJoined', '{}', 5, 0)`,
		`INSERT INTO watermarks VALUES ('peer:1', 'meta', 99, 0)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()

	st, err := Open(path, "n")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.ChangesSince(KindMeta, 0)
	if err != nil || len(rows) != 1 || rows[0].Version == 0 {
		t.Fatalf("migrated rows: %+v err=%v", rows, err)
	}
	evs, err := st.Events(10)
	if err != nil || len(evs) != 1 {
		t.Fatalf("migrated events: %+v err=%v", evs, err)
	}
	if v, _ := st.Version(); v < rows[0].Version {
		t.Fatalf("version counter %d behind migrated rows", v)
	}
	if id, _ := st.StoreID(); id == "" {
		t.Fatal("no store id after migration")
	}
}

func TestApplyRemoteDiscardsResponsesFromBeforeAWipe(t *testing.T) {
	st := openTest(t)
	before, _ := st.StoreID()
	if _, _, err := st.Wipe(); err != nil {
		t.Fatal(err)
	}
	r := remote("px", 99, Row{Kind: KindMeta, Key: "m", Owner: "o", HLC: model.HLC{TS: 1}, Payload: []byte("x")})
	r.RequestedAs = before
	if n, err := st.ApplyRemote(r); err != nil || n != 0 {
		t.Fatalf("applied %d rows from a pre-wipe pull, err=%v", n, err)
	}
	if p, _ := st.PeerByAddr("addr-px"); p.Watermark != 0 {
		t.Fatalf("pre-wipe pull recorded a watermark: %+v", p)
	}
}

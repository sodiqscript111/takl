package transport

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"takl/internal/engine/store"
	"takl/internal/model"
	"takl/internal/transport/pb"
)

func startBufconn(t *testing.T, nodeID string, st *store.Store) pb.SyncServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(nodeID, st).RegisterWith(gs)
	go func() {
		if err := gs.Serve(lis); err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewSyncServiceClient(conn)
}

func seedRow(t *testing.T, st *store.Store, kind store.Kind, key, owner string, ts int64, tombstone bool) {
	t.Helper()
	payload, _ := model.Encode(map[string]any{"key": key})
	if err := st.Put(store.Row{Kind: kind, Key: key, Owner: owner, HLC: model.HLC{TS: ts}, Tombstone: tombstone, Payload: payload}); err != nil {
		t.Fatal(err)
	}
}

func TestPullReturnsAllRows(t *testing.T) {
	st, err := store.Open(":memory:", "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	seedRow(t, st, store.KindRunner, "r1", "a", 1, false)
	seedRow(t, st, store.KindRunner, "r2", "b", 2, false)
	seedRow(t, st, store.KindMeta, "m1", "a", 3, false)
	seedRow(t, st, store.KindMeta, "m9", "a", 4, true)

	client := startBufconn(t, "a", st)
	resp, err := client.Pull(context.Background(), &pb.PullRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, r := range resp.Rows {
		got[r.Kind+"\x00"+r.Key] = true
	}
	if len(resp.Rows) != 4 {
		t.Fatalf("want 4 rows, got %+v", resp.Rows)
	}
	if !got["runner\x00r1"] || !got["runner\x00r2"] || !got["meta\x00m1"] || !got["meta\x00m9"] {
		t.Fatalf("rows missing: %+v", got)
	}
}

func TestPullRespectsWatermark(t *testing.T) {
	st, err := store.Open(":memory:", "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	seedRow(t, st, store.KindMeta, "m1", "a", 10, false)
	seedRow(t, st, store.KindMeta, "m2", "a", 20, false)
	since, _ := st.Version()
	// m3 has an older HLC than m2 but changed later: it must still be sent.
	seedRow(t, st, store.KindMeta, "m3", "a", 5, true)
	id, _ := st.StoreID()

	client := startBufconn(t, "a", st)
	resp, err := client.Pull(context.Background(), &pb.PullRequest{StoreId: "req", PeerStoreId: id, SinceVersion: since})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) != 1 || resp.Rows[0].Key != "m3" {
		t.Fatalf("want only m3 after the watermark, got %+v", resp.Rows)
	}
	if resp.StoreId != id || resp.Version != since+1 {
		t.Fatalf("response store/version = %q/%d, want %q/%d", resp.StoreId, resp.Version, id, since+1)
	}

	// A watermark taken against another store id means nothing: send everything.
	resp, err = client.Pull(context.Background(), &pb.PullRequest{StoreId: "req", PeerStoreId: "old-id", SinceVersion: since})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) != 3 {
		t.Fatalf("stale store id should get every row, got %d", len(resp.Rows))
	}
}

func TestPullForgottenRequesterIsToldToWipe(t *testing.T) {
	st, err := store.Open(":memory:", "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	seedRow(t, st, store.KindMeta, "m1", "a", 10, false)
	client := startBufconn(t, "a", st)
	ctx := context.Background()

	if _, err := client.Pull(ctx, &pb.PullRequest{StoreId: "req"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ForgetPeers(1 << 62); err != nil {
		t.Fatal(err)
	}
	resp, err := client.Pull(ctx, &pb.PullRequest{StoreId: "req"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Forgotten || len(resp.Rows) != 0 {
		t.Fatalf("forgotten requester got forgotten=%v rows=%d", resp.Forgotten, len(resp.Rows))
	}
}

func TestPullFromForgettingPeerWipesServer(t *testing.T) {
	st, err := store.Open(":memory:", "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	seedRow(t, st, store.KindMeta, "m1", "b", 10, false) // someone else's row
	oldID, _ := st.StoreID()
	client := startBufconn(t, "a", st)

	resp, err := client.Pull(context.Background(), &pb.PullRequest{StoreId: "req", PeerStoreId: oldID, SinceVersion: 7, Forgotten: true})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StoreId == oldID {
		t.Fatal("server did not take a new store id after being told it was forgotten")
	}
	if len(resp.Rows) != 0 {
		t.Fatalf("wiped server still served other nodes' rows: %+v", resp.Rows)
	}
}

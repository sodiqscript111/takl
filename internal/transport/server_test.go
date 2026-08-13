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
	seedRow(t, st, store.KindBuild, "b1", "a", 3, false)
	seedRow(t, st, store.KindBuild, "b9", "a", 4, true)

	client := startBufconn(t, "a", st)
	resp, err := client.Pull(context.Background(), &pb.PullRequest{Watermarks: map[string]*pb.HLC{}})
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
	if !got["runner\x00r1"] || !got["runner\x00r2"] || !got["build\x00b1"] || !got["build\x00b9"] {
		t.Fatalf("rows missing: %+v", got)
	}
}

func TestPullRespectsWatermark(t *testing.T) {
	st, err := store.Open(":memory:", "a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	seedRow(t, st, store.KindBuild, "b1", "a", 10, false)
	seedRow(t, st, store.KindBuild, "b2", "a", 20, false)
	seedRow(t, st, store.KindBuild, "b3", "a", 30, true)

	client := startBufconn(t, "a", st)
	wm := map[string]*pb.HLC{"build": {Ts: 20}}
	resp, err := client.Pull(context.Background(), &pb.PullRequest{Watermarks: wm})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) != 1 {
		t.Fatalf("want only rows after watermark, got %d", len(resp.Rows))
	}
	if resp.Rows[0].Key != "b3" {
		t.Fatalf("wrong row: %+v", resp.Rows[0])
	}
}

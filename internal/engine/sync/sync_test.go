package sync

import (
	"context"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"

	"takl/internal/agent"
	"takl/internal/engine/store"
	"takl/internal/model"
	"takl/internal/backend/stub"
	"takl/internal/transport"
)

type node struct {
	id	string
	st	*store.Store
	ag	*agent.Agent
	clock	*model.Clock
	addr	string
	server	*grpc.Server
	client	*transport.Client
	stopper	chan struct{}
	closed	bool
}

func newNode(t *testing.T, nodeID string, seed int64, clock *model.Clock) *node {
	return newNodeAt(t, nodeID, seed, clock, filepath.Join(t.TempDir(), "db.sqlite"))
}

func newNodeAt(t *testing.T, nodeID string, seed int64, clock *model.Clock, dbPath string) *node {
	t.Helper()
	if clock == nil {
		clock = model.NewClock(nil)
	}
	st, err := store.Open(dbPath, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	s := stub.New(stub.Config{NodeID: nodeID, Capacity: 4})
	ag := agent.New(nodeID, st, s, clock, nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	transport.NewServer(nodeID, st).RegisterWith(gs)
	stopper := make(chan struct{})
	go func() {
		_ = gs.Serve(lis)
		close(stopper)
	}()

	n := &node{
		id:		nodeID,
		st:		st,
		ag:		ag,
		clock:		clock,
		addr:		lis.Addr().String(),
		server:		gs,
		client:		transport.NewClient(5 * time.Second),
		stopper:	stopper,
	}
	t.Cleanup(n.shutdown)
	return n
}

func (n *node) shutdown() {
	if n.closed {
		return
	}
	n.closed = true
	n.server.Stop()
	<-n.stopper
	_ = n.st.Close()
	_ = n.client.Close()
}

func (n *node) engine(peers []string) *Engine {
	client := transport.NewClient(time.Second)
	return New(n.id, n.st, n.clock, client, func() []string { return peers }, 10*time.Millisecond)
}

func storeSets(st *store.Store) map[store.Kind]map[string]string {
	out := map[store.Kind]map[string]string{}
	for _, k := range store.Kinds {
		rows, err := st.ChangesSince(k, model.HLC{})
		if err != nil {
			panic(err)
		}
		m := map[string]string{}
		for _, r := range rows {
			m[r.Key] = string(r.Payload)
		}
		out[k] = m
	}
	return out
}

func rowsIn(st *store.Store) int {
	total := 0
	for _, m := range storeSets(st) {
		total += len(m)
	}
	return total
}

func syncBoth(t *testing.T, ctx context.Context, ea, eb *Engine, pa, pb string, sa, sb *store.Store) {
	t.Helper()
	for round := 0; round < 6; round++ {
		if err := ea.RoundOnce(ctx, pa); err != nil {
			t.Fatal(err)
		}
		if err := eb.RoundOnce(ctx, pb); err != nil {
			t.Fatal(err)
		}
		if reflect.DeepEqual(storeSets(sa), storeSets(sb)) {
			return
		}
	}
	t.Fatal("stores did not converge within 6 rounds")
}

func TestTwoNodeConvergence(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, "a", 1, nil)
	b := newNode(t, "b", 2, nil)
	engA := a.engine([]string{b.addr})
	engB := b.engine([]string{a.addr})

	if err := a.ag.Start(); err != nil {
		t.Fatal(err)
	}
	if err := b.ag.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if err := a.ag.Step(); err != nil {
			t.Fatal(err)
		}
		if err := b.ag.Step(); err != nil {
			t.Fatal(err)
		}
		if i%5 == 0 {
			if err := engA.RoundOnce(ctx, b.addr); err != nil {
				t.Fatal(err)
			}
			if err := engB.RoundOnce(ctx, a.addr); err != nil {
				t.Fatal(err)
			}
		}
	}
	for round := 0; round < 6; round++ {
		engA.RoundOnce(ctx, b.addr)
		engB.RoundOnce(ctx, a.addr)
		if reflect.DeepEqual(storeSets(a.st), storeSets(b.st)) {
			break
		}
	}
	if !reflect.DeepEqual(storeSets(a.st), storeSets(b.st)) {
		t.Fatalf("stores diverged:\nA: %+v\nB: %+v", storeSets(a.st), storeSets(b.st))
	}
	evsB, err := b.st.Events(1000)
	if err != nil {
		t.Fatal(err)
	}

	hasForeignEvent := false
	for _, e := range evsB {
		if e.RunnerID != "b" {
			hasForeignEvent = true
			break
		}
	}
	if !hasForeignEvent {
		t.Fatalf("expected events to be replicated, but node b has no events from node a")
	}
}

func TestIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, "a", 1, nil)
	b := newNode(t, "b", 2, nil)
	engA := a.engine([]string{b.addr})

	a.ag.Start()
	b.ag.Start()
	for i := 0; i < 20; i++ {
		a.ag.Step()
		b.ag.Step()
	}

	before := rowsIn(a.st)
	if err := engA.RoundOnce(ctx, b.addr); err != nil {
		t.Fatal(err)
	}
	first := rowsIn(a.st) - before
	if first == 0 {
		t.Fatal("first round transferred nothing")
	}

	before = rowsIn(a.st)
	if err := engA.RoundOnce(ctx, b.addr); err != nil {
		t.Fatal(err)
	}
	if got := rowsIn(a.st) - before; got != 0 {
		t.Fatalf("watermark failed to block duplicate transfer: %d rows", got)
	}

	engB := b.engine([]string{a.addr})
	if err := engB.RoundOnce(ctx, a.addr); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(storeSets(a.st), storeSets(b.st)) {
		t.Fatalf("stores diverged after converge attempt")
	}
}

func TestRecoveryAfterPeerRestart(t *testing.T) {

	ctx := context.Background()
	shared := model.NewClock(nil)
	a := newNode(t, "a", 1, nil)

	dbPath := filepath.Join(t.TempDir(), "b.db")
	b1 := newNodeAt(t, "b", 2, shared, dbPath)

	engA := a.engine([]string{b1.addr})
	a.ag.Start()
	b1.ag.Start()
	for i := 0; i < 20; i++ {
		a.ag.Step()
		b1.ag.Step()
	}
	if err := engA.RoundOnce(ctx, b1.addr); err != nil {
		t.Fatal(err)
	}
	before := storeSets(a.st)

	b1.shutdown()
	if err := engA.RoundOnce(ctx, b1.addr); err == nil {
		t.Fatal("round against a dead peer should fail")
	}
	if !reflect.DeepEqual(before, storeSets(a.st)) {
		t.Fatal("failed round corrupted local state")
	}

	b2 := newNodeAt(t, "b", 7, shared, dbPath)
	b2.ag.Start()
	engA2 := a.engine([]string{b2.addr})
	engB2 := b2.engine([]string{a.addr})
	syncBoth(t, ctx, engA2, engB2, b2.addr, a.addr, a.st, b2.st)
}

func TestRandomizedConvergence(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, "a", 1, nil)
	b := newNode(t, "b", 2, nil)
	c := newNode(t, "c", 3, nil)

	engA := a.engine([]string{b.addr, c.addr})
	engB := b.engine([]string{a.addr, c.addr})
	engC := c.engine([]string{a.addr, b.addr})

	if err := a.ag.Start(); err != nil {
		t.Fatal(err)
	}
	if err := b.ag.Start(); err != nil {
		t.Fatal(err)
	}
	if err := c.ag.Start(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		_ = a.ag.Step()
		_ = b.ag.Step()
		_ = c.ag.Step()
		if i%2 == 0 {
			_ = engA.RoundOnce(ctx, b.addr)
		}
		if i%3 == 0 {
			_ = engB.RoundOnce(ctx, c.addr)
		}
		if i%5 == 0 {
			_ = engC.RoundOnce(ctx, a.addr)
		}
	}

	for round := 0; round < 10; round++ {
		_ = engA.RoundOnce(ctx, b.addr)
		_ = engA.RoundOnce(ctx, c.addr)
		_ = engB.RoundOnce(ctx, a.addr)
		_ = engB.RoundOnce(ctx, c.addr)
		_ = engC.RoundOnce(ctx, a.addr)
		_ = engC.RoundOnce(ctx, b.addr)
		if reflect.DeepEqual(storeSets(a.st), storeSets(b.st)) && reflect.DeepEqual(storeSets(b.st), storeSets(c.st)) {
			return
		}
	}
	t.Fatalf("failed to converge in random chaos")
}

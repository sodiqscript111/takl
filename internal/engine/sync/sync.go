package sync

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"takl/internal/engine/store"
	"takl/internal/metrics"
	"takl/internal/model"
	"takl/internal/transport"
	"takl/internal/transport/pb"
)

const (
	// DefaultTombstoneGrace is the minimum age of a tombstone before it can be
	// dropped. It is also only dropped once every known peer has pulled it.
	DefaultTombstoneGrace = time.Hour
	// DefaultForgetAfter is how long a peer can be unreachable before it stops
	// blocking tombstone GC. A forgotten peer must wipe before it syncs again.
	DefaultForgetAfter = 24 * time.Hour
)

type Engine struct {
	nodeID       string
	st           *store.Store
	clock        *model.Clock
	client       *transport.Client
	peerProvider func() []string
	interval     time.Duration

	grace       time.Duration
	forgetAfter time.Duration
	started     time.Time
	now         func() time.Time
}

func New(nodeID string, st *store.Store, clock *model.Clock, client *transport.Client, peerProvider func() []string, interval time.Duration) *Engine {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Engine{
		nodeID: nodeID, st: st, clock: clock, client: client, peerProvider: peerProvider, interval: interval,
		grace: DefaultTombstoneGrace, forgetAfter: DefaultForgetAfter,
		started: time.Now(), now: time.Now,
	}
}

// SetRetention overrides the tombstone grace period and the forget window.
func (e *Engine) SetRetention(grace, forgetAfter time.Duration) {
	e.grace = grace
	e.forgetAfter = forgetAfter
}

func (e *Engine) Run(ctx context.Context) error {
	if err := e.roundAll(ctx); err != nil {
		slog.Warn("initial sync round", "err", err)
	}
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := e.roundAll(ctx); err != nil {
				slog.Warn("sync round", "err", err)
			}
			if err := e.GC(); err != nil {
				slog.Warn("tombstone gc", "err", err)
			}
		}
	}
}

// GC forgets peers that have been silent for the forget window, then drops
// tombstones that are past the grace period and that every remaining peer has
// pulled. Peers are only forgotten once this node has itself been running for
// the whole window, so a node coming back from its own downtime never forgets
// healthy peers (and never makes them wipe).
func (e *Engine) GC() error {
	now := e.now()
	if now.Sub(e.started) >= e.forgetAfter {
		n, err := e.st.ForgetPeers(now.Add(-e.forgetAfter).UnixMilli())
		if err != nil {
			return err
		}
		if n > 0 {
			metrics.SyncPeersForgotten.Add(uint64(n))
			slog.Warn("forgot unreachable peers; they must wipe before syncing again", "count", n, "after", e.forgetAfter)
		}
	}
	n, err := e.st.GCTombstones(now.Add(-e.grace).UnixMilli())
	if err != nil {
		return err
	}
	metrics.SyncTombstonesCollected.Add(uint64(n))
	return nil
}

func (e *Engine) roundAll(ctx context.Context) error {
	start := time.Now()
	metrics.SyncRounds.Inc()
	defer func() {
		metrics.SyncRoundLatency.Observe(time.Since(start))
	}()

	peers := e.peerProvider()
	if len(peers) == 0 {
		return nil
	}

	errCh := make(chan error, len(peers))
	for _, p := range peers {
		peer := p
		if peer == "" {
			errCh <- nil
			continue
		}
		go func(p string) {
			tCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			errCh <- e.RoundOnce(tCtx, p)
		}(peer)
	}

	var firstErr error
	for i := 0; i < len(peers); i++ {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		metrics.SyncRoundErrors.Inc()
	}
	return firstErr
}

// RoundOnce pulls every change the peer at addr has made since our watermark
// for its store, and applies it.
func (e *Engine) RoundOnce(ctx context.Context, addr string) error {
	self, err := e.st.StoreID()
	if err != nil {
		return err
	}
	peer, err := e.st.PeerByAddr(addr)
	if err != nil {
		return err
	}
	resp, err := e.client.Pull(ctx, addr, &pb.PullRequest{
		NodeId:       e.nodeID,
		StoreId:      self,
		PeerStoreId:  peer.StoreID,
		SinceVersion: peer.Watermark,
		Forgotten:    peer.Forgotten,
	})
	if err != nil {
		return err
	}
	if resp.Forgotten {
		return transport.Wipe(e.st, "peer "+resp.NodeId+" has forgotten this store")
	}

	remote := store.Remote{
		RequestedAs: self,
		Addr:        addr,
		StoreID:     resp.StoreId,
		NodeID:      resp.NodeId,
		Version:     resp.Version,
		NowMs:       e.now().UnixMilli(),
	}
	var maxHLC model.HLC
	for _, r := range resp.Rows {
		h := model.HLC{TS: r.Hlc.GetTs(), Seq: r.Hlc.GetSeq()}
		remote.Rows = append(remote.Rows, store.Row{
			Kind:      store.Kind(r.Kind),
			Key:       r.Key,
			Owner:     r.Owner,
			HLC:       h,
			Tombstone: r.Tombstone,
			Payload:   r.Payload,
		})
		if h.After(maxHLC) {
			maxHLC = h
		}
	}
	for _, ev := range resp.Events {
		var pl map[string]any
		if ev.Payload != "" {
			_ = json.Unmarshal([]byte(ev.Payload), &pl)
		}
		h := model.HLC{TS: ev.Hlc.GetTs(), Seq: ev.Hlc.GetSeq()}
		remote.Events = append(remote.Events, model.Event{
			RunnerID: ev.RunnerId,
			Seq:      ev.Seq,
			Type:     model.EventType(ev.Type),
			Payload:  pl,
			HLC:      h,
			Origin:   ev.Origin,
		})
		if h.After(maxHLC) {
			maxHLC = h
		}
	}

	_, err = e.st.ApplyRemote(remote)
	if errors.Is(err, store.ErrPeerForgotten) {
		slog.Warn("peer we forgot is still serving its old store; skipping it", "peer", addr, "node", resp.NodeId)
		return nil
	}
	if err != nil {
		return err
	}
	metrics.SyncRowsApplied.Add(uint64(len(remote.Rows)))
	metrics.SyncEventsApplied.Add(uint64(len(remote.Events)))
	e.clock.Update(maxHLC)
	return nil
}

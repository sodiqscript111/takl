package sync

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"takl/internal/engine/store"
	"takl/internal/model"
	"takl/internal/transport"
)

type Engine struct {
	nodeID		string
	st		*store.Store
	clock		*model.Clock
	client		*transport.Client
	peerProvider	func() []string
	interval	time.Duration
}

func New(nodeID string, st *store.Store, clock *model.Clock, client *transport.Client, peerProvider func() []string, interval time.Duration) *Engine {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Engine{nodeID: nodeID, st: st, clock: clock, client: client, peerProvider: peerProvider, interval: interval}
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
			e.gcTombstones()
		}
	}
}

func (e *Engine) gcTombstones() {
	grace := int64(1 * 60 * 60 * 1000)
	nowTS := e.clock.Tick().TS

	var minTS int64
	if nowTS > grace {
		minTS = nowTS - grace
	}

	_, _ = e.st.GCTombstones(model.HLC{TS: minTS, Seq: 0})
}

func (e *Engine) roundAll(ctx context.Context) error {
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
	return firstErr
}

func (e *Engine) RoundOnce(ctx context.Context, peer string) error {
	watermarks := make(map[string]model.HLC, len(store.Kinds))
	checksums := make(map[string]uint64, len(store.Kinds))

	for _, kind := range store.Kinds {
		wm, err := e.st.Watermark(peer, kind)
		if err != nil {
			return err
		}
		watermarks[string(kind)] = wm

		chk, err := e.st.Checksum(kind)
		if err != nil {
			return err
		}
		checksums[string(kind)] = chk
	}

	evWm, err := e.st.Watermark(peer, store.KindEvent)
	if err != nil {
		return err
	}
	evChk, err := e.st.ChecksumEvents()
	if err != nil {
		return err
	}

	resp, err := e.client.Pull(ctx, peer, watermarks, checksums, evWm, evChk)
	if err != nil {
		return err
	}
	if len(resp.Rows) > 0 {
		rows := make([]store.Row, 0, len(resp.Rows))
		var maxHLC model.HLC
		for _, r := range resp.Rows {
			row := store.Row{
				Kind:		store.Kind(r.Kind),
				Key:		r.Key,
				Owner:		r.Owner,
				HLC:		model.HLC{TS: r.Hlc.GetTs(), Seq: r.Hlc.GetSeq()},
				Tombstone:	r.Tombstone,
				Payload:	r.Payload,
			}
			rows = append(rows, row)
			if row.HLC.After(maxHLC) {
				maxHLC = row.HLC
			}
		}
		if err := e.st.ApplyRemote(peer, rows); err != nil {
			return err
		}
		e.clock.Update(maxHLC)
	}

	if len(resp.Events) > 0 {
		var incoming []model.Event
		var maxEvHLC model.HLC
		for _, ev := range resp.Events {
			var pl map[string]any
			if ev.Payload != "" {
				_ = json.Unmarshal([]byte(ev.Payload), &pl)
			}
			h := model.HLC{TS: ev.Hlc.Ts, Seq: ev.Hlc.Seq}
			incoming = append(incoming, model.Event{
				RunnerID:	ev.RunnerId,
				Seq:		ev.Seq,
				Type:		model.EventType(ev.Type),
				Payload:	pl,
				HLC:		h,
			})
			if h.After(evWm) {
				evWm = h
			}
			if h.After(maxEvHLC) {
				maxEvHLC = h
			}
		}
		if err := e.st.ApplyEvents(incoming); err != nil {
			return err
		}
		if err := e.st.SetWatermark(peer, store.KindEvent, evWm); err != nil {
			return err
		}
		e.clock.Update(maxEvHLC)
	}
	return nil
}

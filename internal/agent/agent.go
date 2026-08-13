package agent

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"time"

	"takl/internal/engine/store"
	"takl/internal/membership"
	"takl/internal/model"
)

type Backend interface {
	Snapshot() model.Runner
	Step()
	DrainEvents() []model.Event
	Builds() []model.Build
	Queues() []model.QueueStats
	Containers() []model.Container
	Mounts() []model.Mount
	Images() []model.Image
}

type Agent struct {
	nodeID			string
	epoch			string
	store			*store.Store
	backend			Backend
	clock			*model.Clock
	cluster			*membership.Cluster
	seen			map[store.Kind]map[string]bool
	highMetricsCount	int
	normalMetricsCount	int
	isDraining		bool
}

func New(nodeID string, st *store.Store, backend Backend, clock *model.Clock, cluster *membership.Cluster) *Agent {
	b := make([]byte, 8)
	rand.Read(b)
	epoch := fmt.Sprintf("%x", b)

	return &Agent{
		nodeID:		nodeID,
		epoch:		epoch,
		store:		st,
		backend:	backend,
		clock:		clock,
		cluster:	cluster,
		seen:		map[store.Kind]map[string]bool{},
	}
}

func (a *Agent) owner() string {
	return a.nodeID + "/" + a.epoch
}

func (a *Agent) Start() error {
	hlc := a.clock.Tick()
	runner := a.backend.Snapshot()
	runner.Status = model.RunnerActive
	if err := a.putRow(store.KindRunner, runner.Key(), runner, hlc); err != nil {
		return err
	}
	return a.emit(model.EventRunnerJoined, nil, hlc)
}

func (a *Agent) Step() error {
	a.backend.Step()
	hlc := a.clock.Tick()
	runner := a.backend.Snapshot()

	events := a.backend.DrainEvents()

	isHot := runner.CPUUtil >= 0.90 || runner.MemUtil >= 0.90 || runner.FreeDiskMB < 2000
	if isHot {
		a.highMetricsCount++
		a.normalMetricsCount = 0
	} else {
		a.normalMetricsCount++
		a.highMetricsCount = 0
	}

	if a.highMetricsCount >= 5 {
		a.isDraining = true
	} else if a.normalMetricsCount >= 5 {
		a.isDraining = false
	}

	if a.isDraining {
		runner.Status = model.RunnerDraining
	} else {
		runner.Status = model.RunnerActive
	}

	if err := a.putRow(store.KindRunner, runner.Key(), runner, hlc); err != nil {
		return err
	}
	for _, q := range a.backend.Queues() {
		if err := a.putRow(store.KindQueue, q.Key(), q, hlc); err != nil {
			return err
		}
	}

	if err := syncAll(a, store.KindBuild, a.backend.Builds(), func(b model.Build) string { return b.Key() }, hlc); err != nil {
		return err
	}
	if err := syncAll(a, store.KindContainer, a.backend.Containers(), func(c model.Container) string { return c.Key() }, hlc); err != nil {
		return err
	}
	if err := syncAll(a, store.KindMount, a.backend.Mounts(), func(m model.Mount) string { return m.Key() }, hlc); err != nil {
		return err
	}
	if err := syncAll(a, store.KindImage, a.backend.Images(), func(i model.Image) string { return i.Key() }, hlc); err != nil {
		return err
	}
	for _, e := range events {
		if err := a.emit(e.Type, e.Payload, hlc); err != nil {
			return err
		}
	}
	return nil
}

func syncAll[T any](a *Agent, kind store.Kind, items []T, key func(T) string, hlc model.HLC) error {
	for _, it := range items {
		if err := a.putRow(kind, key(it), it, hlc); err != nil {
			return err
		}
	}
	return a.reconcile(kind, keySet(items, key), hlc)
}

func (a *Agent) Run(ctx context.Context, interval time.Duration) error {
	if err := a.Start(); err != nil {
		return err
	}
	if a.cluster != nil {
		go a.handleMembershipEvents(ctx)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := a.Step(); err != nil {
				slog.Error("agent step", "err", err)
			}
		}
	}
}

func (a *Agent) handleMembershipEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-a.cluster.Events():
			hlc := a.clock.Tick()
			var eventType model.EventType
			switch ev.Type {
			case membership.EventJoin:
				eventType = model.EventRunnerJoined
			case membership.EventLeave:
				eventType = model.EventRunnerLeft
				if err := a.store.DeleteByNode(ev.NodeID, hlc); err != nil {
					slog.Error("prune dead node", "node", ev.NodeID, "err", err)
				}
			case membership.EventUpdate:
				continue
			default:

				slog.Warn("unknown membership event", "type", ev.Type)
				continue
			}

			if err := a.emit(eventType, map[string]any{"node_id": ev.NodeID, "ip": ev.IP}, hlc); err != nil {
				slog.Error("membership event emit", "node", ev.NodeID, "err", err)
			}
		}
	}
}

func (a *Agent) reconcile(kind store.Kind, current map[string]bool, hlc model.HLC) error {
	seen := a.seen[kind]
	if seen == nil {
		seen = map[string]bool{}
		a.seen[kind] = seen
	}
	for key := range seen {
		if !current[key] {
			if err := a.store.Delete(kind, key, a.owner(), hlc); err != nil {
				return err
			}
			delete(seen, key)
		}
	}
	for key := range current {
		seen[key] = true
	}
	return nil
}

func keySet[T any](items []T, f func(T) string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, it := range items {
		out[f(it)] = true
	}
	return out
}

func (a *Agent) putRow(kind store.Kind, key string, v any, hlc model.HLC) error {
	payload, err := model.Encode(v)
	if err != nil {
		return err
	}
	return a.store.Put(store.Row{Kind: kind, Key: key, Owner: a.owner(), HLC: hlc, Payload: payload})
}

func (a *Agent) emit(t model.EventType, payload map[string]any, hlc model.HLC) error {
	return a.store.AppendEvent(model.Event{Type: t, RunnerID: a.nodeID, Payload: payload, HLC: hlc})
}

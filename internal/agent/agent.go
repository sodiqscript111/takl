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
}

type Agent struct {
	nodeID             string
	epoch              string
	store              *store.Store
	backend            Backend
	clock              *model.Clock
	cluster            *membership.Cluster
	highMetricsCount   int
	normalMetricsCount int
	isDraining         bool
}

func New(nodeID string, st *store.Store, backend Backend, clock *model.Clock, cluster *membership.Cluster) *Agent {
	b := make([]byte, 8)
	rand.Read(b)
	epoch := fmt.Sprintf("%x", b)

	return &Agent{
		nodeID:  nodeID,
		epoch:   epoch,
		store:   st,
		backend: backend,
		clock:   clock,
		cluster: cluster,
	}
}

func (a *Agent) owner() string {
	return a.nodeID + "/" + a.epoch
}

func (a *Agent) Start() error {
	runner := a.backend.Snapshot()
	runner.Status = model.RunnerActive
	hlc, err := a.putRow(store.KindRunner, runner.Key(), runner)
	if err != nil {
		return err
	}
	return a.emit(model.EventRunnerJoined, nil, hlc)
}

func (a *Agent) Step() error {
	a.backend.Step()
	runner := a.backend.Snapshot()

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

	_, err := a.putRow(store.KindRunner, runner.Key(), runner)
	return err
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
			var hlc model.HLC
			var eventType model.EventType
			switch ev.Type {
			case membership.EventJoin:
				eventType = model.EventRunnerJoined
				hlc = a.clock.Tick()
			case membership.EventLeave:
				eventType = model.EventRunnerLeft
				h, err := a.store.DeleteByNode(ev.NodeID, a.clock)
				if err != nil {
					slog.Error("prune dead node", "node", ev.NodeID, "err", err)
					h = a.clock.Tick()
				}
				hlc = h
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

func (a *Agent) putRow(kind store.Kind, key string, v any) (model.HLC, error) {
	payload, err := model.Encode(v)
	if err != nil {
		return model.HLC{}, err
	}
	r, err := a.store.PutLocal(kind, key, a.owner(), a.clock, func(store.Row, bool) ([]byte, bool, error) {
		return payload, false, nil
	})
	return r.HLC, err
}

func (a *Agent) emit(t model.EventType, payload map[string]any, hlc model.HLC) error {
	return a.store.AppendEvent(model.Event{Type: t, RunnerID: a.nodeID, Payload: payload, HLC: hlc})
}

package membership

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/hashicorp/memberlist"
)

type Event struct {
	Type	EventType
	NodeID	string
	IP	string
}

type EventType int

const (
	EventJoin	EventType	= iota
	EventLeave
	EventUpdate
)

type Member struct {
	NodeID		string
	SyncAddr	string
}

type Cluster struct {
	mlist	*memberlist.Memberlist
	events	chan Event
}

func NewCluster(nodeID string, bindPort int, syncAddr string, seeds []string) (*Cluster, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = nodeID
	config.BindPort = bindPort
	config.AdvertisePort = bindPort

	events := make(chan Event, 256)

	metaBytes, _ := json.Marshal(NodeMeta{SyncAddr: syncAddr})
	delegate := &delegate{events: events, meta: metaBytes}
	config.Events = delegate
	config.Delegate = delegate

	mlist, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	if len(seeds) > 0 {
		_, err := mlist.Join(seeds)
		if err != nil {
			slog.Warn("failed to join seeds", "err", err, "seeds", seeds)
		}
	}

	return &Cluster{
		mlist:	mlist,
		events:	events,
	}, nil
}

func (c *Cluster) Events() <-chan Event {
	return c.events
}

func (c *Cluster) Members() []Member {
	var nodes []Member
	for _, member := range c.mlist.Members() {
		if member.Name == c.mlist.LocalNode().Name {
			continue
		}
		var meta NodeMeta
		if err := json.Unmarshal(member.Meta, &meta); err == nil && meta.SyncAddr != "" {
			nodes = append(nodes, Member{
				NodeID:		member.Name,
				SyncAddr:	meta.SyncAddr,
			})
		} else {

			slog.Warn("member without usable sync metadata", "node", member.Name, "err", err)
		}
	}
	return nodes
}

func (c *Cluster) Shutdown() error {
	err := c.mlist.Leave(time.Second)
	if err != nil {
		return err
	}
	return c.mlist.Shutdown()
}

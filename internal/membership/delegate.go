package membership

import (
	"github.com/hashicorp/memberlist"
)

type NodeMeta struct {
	SyncAddr string `json:"sync_addr"`
}

type delegate struct {
	events	chan<- Event
	meta	[]byte
}

func (d *delegate) NodeMeta(limit int) []byte {
	return d.meta
}

func (d *delegate) NotifyMsg(b []byte)	{}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte	{ return nil }

func (d *delegate) LocalState(join bool) []byte	{ return nil }

func (d *delegate) MergeRemoteState(buf []byte, join bool)	{}

func (d *delegate) trySend(e Event) {
	select {
	case d.events <- e:
	default:
		// Drop event to prevent deadlocking memberlist's gossip goroutines.
	}
}

func (d *delegate) NotifyJoin(node *memberlist.Node) {
	d.trySend(Event{
		Type:	EventJoin,
		NodeID:	node.Name,
		IP:	node.Addr.String(),
	})
}

func (d *delegate) NotifyLeave(node *memberlist.Node) {
	d.trySend(Event{
		Type:	EventLeave,
		NodeID:	node.Name,
		IP:	node.Addr.String(),
	})
}

func (d *delegate) NotifyUpdate(node *memberlist.Node) {
	d.trySend(Event{
		Type:	EventUpdate,
		NodeID:	node.Name,
		IP:	node.Addr.String(),
	})
}

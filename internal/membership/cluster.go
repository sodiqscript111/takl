package membership

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
)

type Event struct {
	Type   EventType
	NodeID string
	IP     string
}

type EventType int

const (
	EventJoin EventType = iota
	EventLeave
	EventUpdate
)

type Member struct {
	NodeID   string
	SyncAddr string
}

type Cluster struct {
	mlist  *memberlist.Memberlist
	events chan Event
}

func NewCluster(nodeID string, bindPort int, advertiseAddr string, syncAddr string, profile string, gossipKey string, seeds []string) (*Cluster, error) {
	config := memberlist.DefaultLANConfig()
	if profile == "wan" {
		config = memberlist.DefaultWANConfig()
	}
	config.Name = nodeID
	config.BindPort = bindPort
	config.AdvertisePort = bindPort
	if advertiseAddr != "" {
		config.AdvertiseAddr = advertiseAddr
	}
	if gossipKey != "" {
		key, err := parseSecretKey(gossipKey)
		if err != nil {
			return nil, err
		}
		config.SecretKey = key
	}

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
		mlist:  mlist,
		events: events,
	}, nil
}

func parseSecretKey(raw string) ([]byte, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return nil, nil
	}
	if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
		if validSecretKeyLen(len(decoded)) {
			return decoded, nil
		}
	}
	plain := []byte(v)
	if validSecretKeyLen(len(plain)) {
		return plain, nil
	}
	return nil, fmt.Errorf("invalid gossip key length %d, expected 16, 24, or 32 bytes (raw or base64)", len(plain))
}

func validSecretKeyLen(n int) bool {
	return n == 16 || n == 24 || n == 32
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
				NodeID:   member.Name,
				SyncAddr: meta.SyncAddr,
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

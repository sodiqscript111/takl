package transport

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"takl/internal/model"
	"takl/internal/transport/pb"
)

type Client struct {
	mu	sync.Mutex
	conns	map[string]*grpc.ClientConn
	timeout	time.Duration
}

func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Client{conns: map[string]*grpc.ClientConn{}, timeout: timeout}
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc, ok := c.conns[addr]; ok {
		state := cc.GetState()
		if state != connectivity.Shutdown && state != connectivity.TransientFailure {
			return cc, nil
		}
		// Stale connection — close and reconnect.
		_ = cc.Close()
		delete(c.conns, addr)
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = cc
	return cc, nil
}

func (c *Client) Pull(ctx context.Context, addr string, watermarks map[string]model.HLC, checksums map[string]uint64, eventWm model.HLC, eventChk uint64) (*pb.PullResponse, error) {
	cc, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	req := &pb.PullRequest{
		Watermarks:	make(map[string]*pb.HLC, len(watermarks)),
		Checksums:	checksums,
		EventWatermark:	&pb.HLC{Ts: eventWm.TS, Seq: eventWm.Seq},
		EventChecksum:	eventChk,
	}
	for kind, h := range watermarks {
		req.Watermarks[kind] = &pb.HLC{Ts: h.TS, Seq: h.Seq}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return pb.NewSyncServiceClient(cc).Pull(ctx, req)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for addr, cc := range c.conns {
		if err := cc.Close(); err != nil && first == nil {
			first = err
		}
		delete(c.conns, addr)
	}
	return first
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"takl/internal/agent"
	"takl/internal/api"
	"takl/internal/backend/docker"
	"takl/internal/backend/stub"
	"takl/internal/engine/store"
	"takl/internal/engine/sync"
	"takl/internal/membership"
	"takl/internal/model"
	"takl/internal/transport"
	"takl/internal/version"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version.Info())
		return
	}
	var (
		nodeID		= flag.String("node-id", "runner-1", "unique node id")
		httpAddr	= flag.String("http-addr", "127.0.0.1:8090", "http query api listen address")
		syncAddr	= flag.String("sync-addr", "127.0.0.1:8100", "gRPC sync listen address")
		peers		= flag.String("peers", "", "comma-separated gRPC sync peer addresses (legacy)")
		bindPort	= flag.Int("bind", 7946, "SWIM gossip port")
		join		= flag.String("join", "", "comma-separated seeds to join (SWIM)")
		dbPath		= flag.String("db", "takl.db", "sqlite database path")
		capacity	= flag.Int("capacity", 4, "runner worker capacity")
		region		= flag.String("region", "us-east-1", "region label")
		tick		= flag.Duration("tick", time.Second, "agent sampling interval")
		syncInterval	= flag.Duration("sync-interval", 2*time.Second, "sync round interval")
		backendType	= flag.String("backend", "docker", "backend type (stub or docker)")
	)
	flag.Parse()

	hostname, _ := os.Hostname()

	st, err := store.Open(*dbPath, *nodeID)
	if err != nil {
		slog.Error("store open", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	clock := model.NewClock(nil)

	var backend agent.Backend
	if *backendType == "docker" {
		var err error
		backend, err = docker.New(docker.Config{
			NodeID:   *nodeID,
			Hostname: hostname,
			IP:       "127.0.0.1",
			Region:   *region,
			Version:  version.Version,
			Capacity: *capacity,
		})
		if err != nil {
			slog.Error("failed to init docker backend", "err", err)
			os.Exit(1)
		}
	} else {
		backend = stub.New(stub.Config{
			NodeID:   *nodeID,
			Hostname: hostname,
			IP:       "127.0.0.1",
			Region:   *region,
			Version:  version.Version,
			Capacity: *capacity,
		})
	}

	var cluster *membership.Cluster
	if *bindPort > 0 {
		var seeds []string
		if *join != "" {
			for _, s := range strings.Split(*join, ",") {
				if s = strings.TrimSpace(s); s != "" {
					seeds = append(seeds, s)
				}
			}
		}
		var err error
		cluster, err = membership.NewCluster(*nodeID, *bindPort, *syncAddr, seeds)
		if err != nil {
			slog.Error("membership cluster", "err", err)
			os.Exit(1)
		}
		defer cluster.Shutdown()
	}

	ag := agent.New(*nodeID, st, backend, clock, cluster)

	httpSrv := &http.Server{Addr: *httpAddr, Handler: api.New(st, *nodeID).Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("agent started", "node", *nodeID, "tick", *tick)
		if err := ag.Run(ctx, *tick); err != nil {
			slog.Error("agent", "err", err)
			stop()
		}
	}()

	var client *transport.Client
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()

	if *syncAddr != "" {
		lis, err := net.Listen("tcp", *syncAddr)
		if err != nil {
			slog.Error("sync listen", "err", err)
			os.Exit(1)
		}
		gs := grpc.NewServer()
		transport.NewServer(*nodeID, st).RegisterWith(gs)
		go func() {
			slog.Info("sync server", "addr", *syncAddr)
			if err := gs.Serve(lis); err != nil {
				slog.Error("sync server", "err", err)
				stop()
			}
		}()
		defer func() {
			stopped := make(chan struct{})
			go func() {
				gs.GracefulStop()
				close(stopped)
			}()
			t := time.NewTimer(5 * time.Second)
			defer t.Stop()
			select {
			case <-stopped:
			case <-t.C:
				gs.Stop()
			}
		}()
	}

	if *peers != "" || cluster != nil {
		var peerProvider func() []string
		if cluster != nil {
			peerProvider = func() []string {
				var addrs []string
				for _, m := range cluster.Members() {
					addrs = append(addrs, m.SyncAddr)
				}
				return addrs
			}
		} else {
			var peerList []string
			for _, p := range strings.Split(*peers, ",") {
				if p = strings.TrimSpace(p); p != "" {
					peerList = append(peerList, p)
				}
			}
			peerProvider = func() []string { return peerList }
		}

		client = transport.NewClient(5 * time.Second)
		eng := sync.New(*nodeID, st, clock, client, peerProvider, *syncInterval)
		go func() {
			slog.Info("sync engine started")
			if err := eng.Run(ctx); err != nil {
				slog.Error("sync engine", "err", err)
				stop()
			}
		}()
	}

	go func() {
		slog.Info("http api", "addr", *httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	slog.Info("shutdown complete")
}

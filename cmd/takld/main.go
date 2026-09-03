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
	"takl/internal/config"
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

	configFile := flag.String("config", "", "path to takl.yaml config file")
	nodeID := flag.String("node-id", "", "unique node id (overrides config)")
	httpAddr := flag.String("http-addr", "", "http query api listen address")
	syncAddr := flag.String("sync-addr", "", "gRPC sync listen address")
	peers := flag.String("peers", "", "comma-separated gRPC sync peer addresses (legacy)")
	bindPort := flag.Int("bind", 0, "SWIM gossip port")
	join := flag.String("join", "", "comma-separated seeds to join (SWIM)")
	dbPath := flag.String("db", "", "sqlite database path")
	capacity := flag.Int("capacity", 0, "runner worker capacity")
	region := flag.String("region", "", "region label")
	tick := flag.Duration("tick", 0, "agent sampling interval")
	syncInterval := flag.Duration("sync-interval", 0, "sync round interval")
	backendType := flag.String("backend", "", "backend type (stub or docker)")

	flag.Parse()

	cfg := config.Default()
	if *configFile != "" {
		var err error
		cfg, err = config.Load(*configFile, cfg)
		if err != nil {
			slog.Error("failed to load config file", "err", err)
			os.Exit(1)
		}
	}

	// Override from CLI flags if explicitly set
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "node-id":
			cfg.Cluster.NodeID = *nodeID
		case "http-addr":
			cfg.Network.HTTPAddr = *httpAddr
		case "sync-addr":
			cfg.Network.SyncAddr = *syncAddr
		case "peers":
			cfg.Cluster.Peers = strings.Split(*peers, ",")
		case "bind":
			cfg.Cluster.Bind = *bindPort
		case "join":
			cfg.Cluster.Join = strings.Split(*join, ",")
		case "db":
			cfg.Storage.DBPath = *dbPath
		case "capacity":
			cfg.Execution.Capacity = *capacity
		case "region":
			cfg.Execution.Region = *region
		case "tick":
			cfg.Agent.Tick = *tick
		case "sync-interval":
			cfg.Agent.SyncInterval = *syncInterval
		case "backend":
			cfg.Execution.Backend = *backendType
		}
	})

	hostname, _ := os.Hostname()

	st, err := store.Open(cfg.Storage.DBPath, cfg.Cluster.NodeID)
	if err != nil {
		slog.Error("store open", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	clock := model.NewClock(nil)

	var backend agent.Backend
	if cfg.Execution.Backend == "docker" {
		var err error
		backend, err = docker.New(docker.Config{
			NodeID:   cfg.Cluster.NodeID,
			Hostname: hostname,
			IP:       "127.0.0.1",
			Region:   cfg.Execution.Region,
			Version:  version.Version,
			Capacity: cfg.Execution.Capacity,
		})
		if err != nil {
			slog.Error("failed to init docker backend", "err", err)
			os.Exit(1)
		}
	} else {
		backend = stub.New(stub.Config{
			NodeID:   cfg.Cluster.NodeID,
			Hostname: hostname,
			IP:       "127.0.0.1",
			Region:   cfg.Execution.Region,
			Version:  version.Version,
			Capacity: cfg.Execution.Capacity,
		})
	}

	var cluster *membership.Cluster
	if cfg.Cluster.Bind > 0 {
		var seeds []string
		for _, s := range cfg.Cluster.Join {
			if s = strings.TrimSpace(s); s != "" {
				seeds = append(seeds, s)
			}
		}
		var err error
		cluster, err = membership.NewCluster(cfg.Cluster.NodeID, cfg.Cluster.Bind, cfg.Network.SyncAddr, seeds)
		if err != nil {
			slog.Error("membership cluster", "err", err)
			os.Exit(1)
		}
		defer cluster.Shutdown()
	}

	ag := agent.New(cfg.Cluster.NodeID, st, backend, clock, cluster)

	httpSrv := &http.Server{Addr: cfg.Network.HTTPAddr, Handler: api.New(st, cfg.Cluster.NodeID).Handler()}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("agent started", "node", cfg.Cluster.NodeID, "tick", cfg.Agent.Tick)
		if err := ag.Run(ctx, cfg.Agent.Tick); err != nil {
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

	if cfg.Network.SyncAddr != "" {
		lis, err := net.Listen("tcp", cfg.Network.SyncAddr)
		if err != nil {
			slog.Error("sync listen", "err", err)
			os.Exit(1)
		}
		gs := grpc.NewServer()
		transport.NewServer(cfg.Cluster.NodeID, st).RegisterWith(gs)
		go func() {
			slog.Info("sync server", "addr", cfg.Network.SyncAddr)
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

	if len(cfg.Cluster.Peers) > 0 || cluster != nil {
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
			for _, p := range cfg.Cluster.Peers {
				if p = strings.TrimSpace(p); p != "" {
					peerList = append(peerList, p)
				}
			}
			peerProvider = func() []string { return peerList }
		}

		client = transport.NewClient(5 * time.Second)
		eng := sync.New(cfg.Cluster.NodeID, st, clock, client, peerProvider, cfg.Agent.SyncInterval)
		go func() {
			slog.Info("sync engine started")
			if err := eng.Run(ctx); err != nil {
				slog.Error("sync engine", "err", err)
				stop()
			}
		}()
	}

	go func() {
		slog.Info("http api", "addr", cfg.Network.HTTPAddr)
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

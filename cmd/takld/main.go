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
	advertiseAddr := flag.String("advertise-addr", "", "advertise IP for peers and runner identity")
	peers := flag.String("peers", "", "comma-separated gRPC sync peer addresses (legacy)")
	bindPort := flag.Int("bind", 0, "SWIM gossip port")
	join := flag.String("join", "", "comma-separated seeds to join (SWIM)")
	clusterProfile := flag.String("cluster-profile", "", "memberlist profile: lan or wan")
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
		case "advertise-addr":
			cfg.Network.AdvertiseAddr = *advertiseAddr
		case "peers":
			cfg.Cluster.Peers = strings.Split(*peers, ",")
		case "bind":
			cfg.Cluster.Bind = *bindPort
		case "join":
			cfg.Cluster.Join = strings.Split(*join, ",")
		case "cluster-profile":
			cfg.Cluster.Profile = *clusterProfile
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

	runnerIP := resolveAdvertiseIP(cfg.Network.AdvertiseAddr)
	if runnerIP == "" {
		runnerIP = discoverAdvertiseIP(cfg.Network.SyncAddr, cfg.Network.HTTPAddr)
	}
	if runnerIP == "" {
		runnerIP = "127.0.0.1"
		slog.Warn("could not auto-detect advertise IP; using loopback", "ip", runnerIP)
	}

	advertisedSyncAddr, err := advertisedAddr(cfg.Network.SyncAddr, runnerIP)
	if err != nil {
		slog.Error("invalid sync address", "addr", cfg.Network.SyncAddr, "err", err)
		os.Exit(1)
	}

	profile := strings.ToLower(strings.TrimSpace(cfg.Cluster.Profile))
	if profile == "" {
		profile = "lan"
	}
	if profile != "lan" && profile != "wan" {
		slog.Warn("invalid cluster profile; defaulting to lan", "profile", cfg.Cluster.Profile)
		profile = "lan"
	}

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
		backend, err = docker.New(docker.Config{
			NodeID:   cfg.Cluster.NodeID,
			Hostname: hostname,
			IP:       runnerIP,
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
			IP:       runnerIP,
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
		cluster, err = membership.NewCluster(cfg.Cluster.NodeID, cfg.Cluster.Bind, runnerIP, advertisedSyncAddr, profile, seeds)
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
			slog.Info("sync server", "addr", cfg.Network.SyncAddr, "advertise", advertisedSyncAddr)
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

func advertisedAddr(listenAddr, advertiseIP string) (string, error) {
	if listenAddr == "" {
		return "", nil
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", err
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = advertiseIP
	}
	return net.JoinHostPort(host, port), nil
}

func discoverAdvertiseIP(addrs ...string) string {
	for _, addr := range addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			continue
		}
		if ip := resolveAdvertiseIP(host); ip != "" {
			return ip
		}
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP
			if ip == nil || ip.IsLoopback() || ip.IsUnspecified() {
				continue
			}
			if v4 := ip.To4(); v4 != nil {
				return v4.String()
			}
		}
	}
	return ""
}

func resolveAdvertiseIP(value string) string {
	host := strings.TrimSpace(value)
	if host == "" {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() || ip.IsUnspecified() {
			return ""
		}
		return ip.String()
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return ""
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsUnspecified() {
			continue
		}
		if v4 := ip.To4(); v4 != nil {
			return v4.String()
		}
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsUnspecified() {
			continue
		}
		return ip.String()
	}
	return ""
}

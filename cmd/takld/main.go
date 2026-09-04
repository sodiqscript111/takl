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
	"google.golang.org/grpc/credentials"

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
	gossipKey := flag.String("gossip-key", "", "memberlist gossip key (16/24/32 bytes raw or base64)")
	tlsCAFile := flag.String("sync-tls-ca-file", "", "sync mTLS CA PEM path")
	tlsCertFile := flag.String("sync-tls-cert-file", "", "sync mTLS cert PEM path")
	tlsKeyFile := flag.String("sync-tls-key-file", "", "sync mTLS key PEM path")
	allowInsecureSync := flag.Bool("allow-insecure-sync", false, "allow plaintext sync on non-loopback addresses")
	allowInsecureGossip := flag.Bool("allow-insecure-gossip", false, "allow wan profile without gossip key")
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
		case "gossip-key":
			cfg.Cluster.GossipKey = *gossipKey
		case "sync-tls-ca-file":
			cfg.Network.SyncTLS.CAFile = *tlsCAFile
		case "sync-tls-cert-file":
			cfg.Network.SyncTLS.CertFile = *tlsCertFile
		case "sync-tls-key-file":
			cfg.Network.SyncTLS.KeyFile = *tlsKeyFile
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

	profile := strings.ToLower(strings.TrimSpace(cfg.Cluster.Profile))
	if profile == "" {
		profile = "lan"
	}
	if profile != "lan" && profile != "wan" {
		slog.Warn("invalid cluster profile; defaulting to lan", "profile", cfg.Cluster.Profile)
		profile = "lan"
	}

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

	peerList := compactList(cfg.Cluster.Peers)
	seedList := compactList(cfg.Cluster.Join)
	tlsFiles := transport.TLSFiles{
		CAFile:   cfg.Network.SyncTLS.CAFile,
		CertFile: cfg.Network.SyncTLS.CertFile,
		KeyFile:  cfg.Network.SyncTLS.KeyFile,
	}
	if tlsFiles.Enabled() {
		if err := tlsFiles.Validate(); err != nil {
			slog.Error("invalid sync tls config", "err", err)
			os.Exit(1)
		}
	}
	if cfg.Network.SyncAddr != "" && addrExposed(cfg.Network.SyncAddr) && (len(peerList) > 0 || cfg.Cluster.Bind > 0) && !tlsFiles.Enabled() && !*allowInsecureSync {
		slog.Error("refusing insecure sync on non-loopback address", "sync_addr", cfg.Network.SyncAddr)
		os.Exit(1)
	}
	if cfg.Cluster.Bind > 0 && profile == "wan" && strings.TrimSpace(cfg.Cluster.GossipKey) == "" && !*allowInsecureGossip {
		slog.Error("refusing wan gossip without gossip key")
		os.Exit(1)
	}

	hostname, _ := os.Hostname()

	var clientCreds credentials.TransportCredentials
	var serverCreds credentials.TransportCredentials
	if tlsFiles.Enabled() {
		clientCreds, err = transport.LoadClientTLSCredentials(tlsFiles)
		if err != nil {
			slog.Error("load sync tls client credentials", "err", err)
			os.Exit(1)
		}
		serverCreds, err = transport.LoadServerTLSCredentials(tlsFiles)
		if err != nil {
			slog.Error("load sync tls server credentials", "err", err)
			os.Exit(1)
		}
	}

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
		cluster, err = membership.NewCluster(cfg.Cluster.NodeID, cfg.Cluster.Bind, runnerIP, advertisedSyncAddr, profile, cfg.Cluster.GossipKey, seedList)
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
		var serverOpts []grpc.ServerOption
		if serverCreds != nil {
			serverOpts = append(serverOpts, grpc.Creds(serverCreds))
		}
		gs := grpc.NewServer(serverOpts...)
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

	if len(peerList) > 0 || cluster != nil {
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
			peerProvider = func() []string { return peerList }
		}

		client = transport.NewClient(5*time.Second, clientCreds)
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

func compactList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func addrExposed(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "0.0.0.0" || host == "::" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		if host == "localhost" {
			return false
		}
		ips, err := net.LookupIP(host)
		if err != nil {
			return true
		}
		for _, ip = range ips {
			if !ip.IsLoopback() {
				return true
			}
		}
		return false
	}
	return !ip.IsLoopback()
}

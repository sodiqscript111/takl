package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync"
)

func binaryName() string {
	if runtime.GOOS == "windows" {
		return "takld.exe"
	}
	return "takld"
}

type Node struct {
	ID         string `json:"id"`
	HTTPPort   int    `json:"http_port"`
	SyncPort   int    `json:"sync_port"`
	GossipPort int    `json:"gossip_port"`
	Seed       int    `json:"seed"`
	Running    bool   `json:"running"`
	cmd        *exec.Cmd
}

var (
	mu    sync.Mutex
	nodes = map[string]*Node{
		"node-a": {ID: "node-a", HTTPPort: 8091, SyncPort: 8101, GossipPort: 7941, Seed: 1},
		"node-b": {ID: "node-b", HTTPPort: 8092, SyncPort: 8102, GossipPort: 7942, Seed: 2},
		"node-c": {ID: "node-c", HTTPPort: 8093, SyncPort: 8103, GossipPort: 7943, Seed: 3},
		"node-d": {ID: "node-d", HTTPPort: 8094, SyncPort: 8104, GossipPort: 7944, Seed: 4},
	}
)

func main() {
	bin := binaryName()
	if err := exec.Command("go", "build", "-o", bin, "./cmd/takld").Run(); err != nil {
		slog.Error("failed to build takld", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("cmd/taklui")))

	mux.HandleFunc("GET /api/nodes", handleListNodes)
	mux.HandleFunc("POST /api/nodes/{id}/start", handleStartNode)
	mux.HandleFunc("POST /api/nodes/{id}/stop", handleStopNode)
	mux.HandleFunc("GET /api/nodes/{id}/proxy/", handleProxy)

	slog.Info("taklui chaos controller listening on http://localhost:3000")
	if err := http.ListenAndServe(":3000", mux); err != nil {
		slog.Error("http serve", "err", err)
	}
}

func handleListNodes(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	list := make([]*Node, 0, len(nodes))
	for _, id := range []string{"node-a", "node-b", "node-c", "node-d"} {
		list = append(list, nodes[id])
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

func handleStartNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	defer mu.Unlock()

	node, ok := nodes[id]
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	if node.Running {
		w.WriteHeader(http.StatusOK)
		return
	}

	joinSeeds := "127.0.0.1:7941,127.0.0.1:7942,127.0.0.1:7943,127.0.0.1:7944"
	args := []string{
		"-node-id", node.ID,
		"-http-addr", fmt.Sprintf("127.0.0.1:%d", node.HTTPPort),
		"-sync-addr", fmt.Sprintf("127.0.0.1:%d", node.SyncPort),
		"-bind", fmt.Sprintf("%d", node.GossipPort),
		"-join", joinSeeds,
		"-db", fmt.Sprintf("%s.db", node.ID),
	}

	cmd := exec.Command("./"+binaryName(), args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	node.cmd = cmd
	node.Running = true

	go func() {
		_ = cmd.Wait()
		mu.Lock()
		node.Running = false
		node.cmd = nil
		mu.Unlock()
	}()

	w.WriteHeader(http.StatusOK)
}

func handleStopNode(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	defer mu.Unlock()

	node, ok := nodes[id]
	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	if !node.Running || node.cmd == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	_ = node.cmd.Process.Kill()
	node.Running = false
	node.cmd = nil
	w.WriteHeader(http.StatusOK)
}

func handleProxy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	node, ok := nodes[id]
	mu.Unlock()

	if !ok {
		http.Error(w, "node not found", http.StatusNotFound)
		return
	}
	if !node.Running {
		http.Error(w, "node is offline", http.StatusBadGateway)
		return
	}

	path := r.URL.Path[len("/api/nodes/"+id+"/proxy/"):]
	target := fmt.Sprintf("http://127.0.0.1:%d/%s", node.HTTPPort, path)

	resp, err := http.Get(target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		for _, vv := range v {
			w.Header().Add(k, vv)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

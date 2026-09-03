# Takl

Takl is a **masterless, predictive scheduling and metadata layer** for container and CI build fleets at the edge. Every node runs an identical `takld` daemon that keeps a local, decentralized view of the entire cluster — no centralized control plane, no single point of failure.

Nodes discover each other over **SWIM gossip**, continuously exchange their state (CPU pressure, memory, disk, cache layers, build queues) via **gRPC pull-replication**, and answer scheduling queries with a local scoring engine. The result: fast, cache-aware placement decisions that are resilient to thundering herds.

## Problems it solves

- **Thundering herds** — a spike in workloads can swamp a single "empty" node. Takl diffuses placements across the cluster with randomized top-K scoring.
- **Stale state** — centralized metrics lag real-time node pressure. Takl nodes gossip metrics directly, peer to peer.
- **Cache ignorance** — orchestrators guess where to place workloads, forcing nodes to re-download image layers over the network. Takl scores nodes on whether they already hold the layers for a project.
- **Control plane failure** — a dead orchestrator takes the whole fleet down. Takl has no master; any node can answer scheduling questions.

## Architecture

```
                        ┌──────────────────────────────────────────┐
                        │                 Cluster                   │
                        │  node-a ◄──── SWIM gossip ────► node-b    │
                        │    ▲                      ▲               │
                        │    │ gRPC pull            │ gRPC pull     │
                        │    ▼                      ▼               │
                        │  node-c ── SWIM ──────► node-d            │
                        └──────────────────────────────────────────┘

One takld process per node, composed of:

┌────────────────────────────── takld (one per node) ──────────────────────────────┐
│                                                                                  │
│  ┌──────────────┐   ┌───────────────┐   ┌───────────────────┐   ┌──────────────┐ │
│  │    agent     │──▶│   sqlite store │◀──│   sync engine    │──▶│ gRPC server  │ │
│  │  (backend    │   │ (CRDT-ish KV, │   │ (pull replication│   │  :8100       │ │
│  │   sampling)  │   │  HLC timestamps│   │  per peer)       │   │              │ │
│  └──────────────┘   └───────────────┘   └───────────────────┘   └──────────────┘ │
│                                                                                  │
│  ┌──────────────┐   ┌───────────────┐   ┌───────────────────┐                    │
│  │  membership  │   │  HTTP API     │   │  scoring engine   │                    │
│  │ (SWIM via    │   │  :8090        │   │ (best-runner,     │                    │
│  │  memberlist) │   │  query +      │   │  cache affinity,  │                    │
│  └──────────────┘   │  placement    │   │  jitter)          │                    │
│                     └───────────────┘   └───────────────────┘                    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Package | Role |
|---|---|---|
| `takld` | `cmd/takld` | The daemon; runs all subsystems on every node |
| `taklctl` | `cmd/taklctl` | CLI client for the HTTP API (`runners`, `builds`, `events`, ...) |
| `taklui` | `cmd/taklui` | Local chaos/test controller — launches a 4-node cluster and proxies their APIs on `:3000` |
| Agent | `internal/agent` | Samples the node backend each tick, classifies pressure, and writes state to the store |
| Backend | `internal/sim` | Simulated node: CPU/mem pressure, builds, containers, mounts, image cache. Implements `agent.Backend` so a real runtime can be swapped in |
| Store | `internal/engine/store` | SQLite-backed key-value store with per-kind rows, tombstones, checksums, and per-peer watermarks |
| Sync engine | `internal/engine/sync` | Periodic pull-replication rounds against every known peer |
| Transport | `internal/transport` | gRPC sync protocol (`proto/v1/sync.proto`) |
| Membership | `internal/membership` | SWIM gossip via HashiCorp `memberlist`; peers advertise their gRPC sync address in node metadata |
| API | `internal/api` | HTTP query + placement endpoints, including the scoring engine |
| Model | `internal/model` | Domain types: `Runner`, `Build`, `Container`, `Mount`, `Image`, `QueueStats` + hybrid logical clocks |

### Consistency model

- Every state row carries a **hybrid logical clock (HLC)** so events can be totally ordered across nodes without wall-clock coordination.
- Each node keeps a **watermark per peer** (per row kind), so sync rounds only fetch changes newer than the last exchange — replication is incremental.
- Deletes are **tombstones**; tombstones older than one hour are garbage-collected during sync rounds.
- **Checksums** per kind let peers detect divergence cheaply; incoming rows are applied with HLC-based conflict resolution (highest HLC wins).
- The store is a **single-writer per key** design (each row records its `owner`), which keeps last-writer-wins deterministic.

### Scheduling

The HTTP API exposes placement endpoints (`/api/v1/runners/best`, `/api/v1/runners/for-project/{projectId}`) driven by a local scoring engine:

- **Cache affinity** — runners that have finished a build for a project score +20, so repeat workloads land on nodes that already hold the layers.
- **Pressure weighting** — score = available workers × 10 − CPU% × 50 − mem% × 30 + free disk (MB) / 1000.
- **Jitter** — ±5 random jitter per candidate spreads concurrent placement requests across the top-K instead of collapsing onto one node (herd diffusion).
- **Region/label filtering** — candidates are filtered by `region` and `label` query params, with a penalty for placing outside the requested region.

Runners self-drain: after 5 consecutive ticks above 90% CPU/mem or below 2 GB free disk, the agent marks itself `draining` so the scheduler stops sending it work.

## Quick start

```bash
# Build
make build          # or: go build ./cmd/takld ./cmd/taklctl

# Run a single node
./bin/takld --node-id node-a

# Query it
./bin/taklctl runners
./bin/taklctl summary
curl http://127.0.0.1:8090/api/v1/runners/best?project=demo
```

### Multi-node cluster

```bash
# Node A (seeded first)
./bin/takld --node-id node-a --http-addr 127.0.0.1:8091 --sync-addr 127.0.0.1:8101 --bind 7941 --db takl-a.db

# Node B joins A over SWIM; state replicates over gRPC
./bin/takld --node-id node-b --http-addr 127.0.0.1:8092 --sync-addr 127.0.0.1:8102 --bind 7942 --join 127.0.0.1:7941 --db takl-b.db

# Node C joins as well
./bin/takld --node-id node-c --http-addr 127.0.0.1:8093 --sync-addr 127.0.0.1:8103 --bind 7943 --join 127.0.0.1:7941 --db takl-c.db

# VPS/multi-host note: set a reachable advertise IP per node
# ./bin/takld ... --http-addr 0.0.0.0:8090 --sync-addr 0.0.0.0:8100 --advertise-addr 10.0.1.12 --cluster-profile wan
```

Any node can now answer for the whole fleet:

```bash
./bin/taklctl -addr http://127.0.0.1:8092 cluster
```

### Local chaos UI

```bash
go run ./cmd/taklui    # http://localhost:3000 — start/stop 4 nodes, proxy their APIs
```

## API

| Endpoint | Description |
|---|---|
| `GET /healthz` | liveness probe |
| `GET /api/v1/runners` | all runners |
| `GET /api/v1/runners/{id}` | single runner |
| `GET /api/v1/runners/best?project=&region=&label=` | top-K scored placements |
| `GET /api/v1/runners/for-project/{projectId}` | runners with a warm cache for a project |
| `GET /api/v1/capacity/check` | capacity check |
| `GET /api/v1/builds` | build states |
| `GET /api/v1/queues` | per-runner queue stats |
| `GET /api/v1/containers` | containers |
| `GET /api/v1/mounts` | mounts |
| `GET /api/v1/images` | discovered images |
| `GET /api/v1/images/{digest}/runners` | runners holding an image digest |
| `GET /api/v1/events` | replicated event log |
| `GET /api/v1/summary` | cluster summary |
| `GET /api/v1/cluster` | cluster-wide capacity/load |
| `GET /api/v1/analytics/hot-projects` | hottest projects by activity |

## Commands

```text
make build     # build takld + taklctl into bin/
make test      # go test ./...
make lint      # go vet + gofmt check (+ golangci-lint if installed)
make gen       # regenerate gRPC stubs from proto/v1/sync.proto
```

## Project layout

```
cmd/takld     daemon
cmd/taklctl   CLI query tool
cmd/taklui    local multi-node chaos controller
internal/     agent, api, engine/store, engine/sync, membership, model, sim, transport, version
proto/v1      gRPC sync protocol definition
```

## License

MIT — see [LICENSE](LICENSE).

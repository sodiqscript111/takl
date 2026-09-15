# Takl

Takl is a **masterless, decentralized node metadata layer** for compute fleets. Every node runs an identical `takld` daemon that keeps a local, decentralized view of the entire cluster — no centralized control plane, no single point of failure.

Nodes discover each other over **SWIM gossip**, continuously exchange their state (CPU pressure, memory, disk, custom labels) via **gRPC pull-replication**, and answer placement queries with a local scoring engine. The result: fast, metadata-aware placement decisions that are resilient to failures.

## What it does

- **Node registry** — every node publishes its metrics (CPU, memory, disk) and user-defined labels to the cluster.
- **Placement queries** — ask any node "which node should I run this on?" and get a scored ranking based on load, capacity, region, and labels.
- **Custom metadata** — attach arbitrary key-value labels to any node at runtime via the API; labels replicate across the cluster automatically.
- **No single point of failure** — no master; any node can answer queries about the whole fleet.
- **Herd diffusion** — randomized top-K scoring spreads concurrent placement requests across nodes instead of collapsing onto one.

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
│  │  (metrics    │   │ (CRDT-ish KV, │   │ (pull replication│   │  :8100       │ │
│  │   sampling)  │   │  HLC timestamps│   │  per peer)       │   │              │ │
│  └──────────────┘   └───────────────┘   └───────────────────┘   └──────────────┘ │
│                                                                                  │
│  ┌──────────────┐   ┌───────────────┐   ┌───────────────────┐                    │
│  │  membership  │   │  HTTP API     │   │  scoring engine   │                    │
│  │ (SWIM via    │   │  :8090        │   │ (pressure weight, │                    │
│  │  memberlist) │   │  query +      │   │  region/label     │                    │
│  └──────────────┘   │  placement    │   │  filter, jitter)  │                    │
│                     └───────────────┘   └───────────────────┘                    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Package | Role |
|---|---|---|
| `takld` | `cmd/takld` | The daemon; runs all subsystems on every node |
| `taklctl` | `cmd/taklctl` | CLI client for the HTTP API (`runners`, `events`, ...) |
| Agent | `internal/agent` | Samples system metrics each tick, classifies pressure, writes state to the store |
| Store | `internal/engine/store` | SQLite-backed key-value store with per-kind rows, tombstones, checksums, and per-peer watermarks |
| Sync engine | `internal/engine/sync` | Periodic pull-replication rounds against every known peer |
| Transport | `internal/transport` | gRPC sync protocol (`proto/v1/sync.proto`) |
| Membership | `internal/membership` | SWIM gossip via HashiCorp `memberlist`; peers advertise their gRPC sync address in node metadata |
| API | `internal/api` | HTTP query + placement endpoints, including the scoring engine and label management |
| Model | `internal/model` | Domain types: `Runner`, hybrid logical clocks |

### Consistency model

- Every state row carries a **hybrid logical clock (HLC)** so events can be totally ordered across nodes without wall-clock coordination.
- Each node keeps a **watermark per peer** (per row kind), so sync rounds only fetch changes newer than the last exchange — replication is incremental.
- Deletes are **tombstones**; tombstones older than one hour are garbage-collected during sync rounds.
- **Checksums** per kind let peers detect divergence cheaply; incoming rows are applied with HLC-based conflict resolution (highest HLC wins).

### Scoring

The HTTP API exposes a placement endpoint (`/api/v1/runners/best`) driven by a local scoring engine:

- **Pressure weighting** — score = available workers × 10 − CPU% × 50 − mem% × 30 + free disk (MB) / 1000.
- **Jitter** — ±5 random jitter per candidate spreads concurrent placement requests across the top-K instead of collapsing onto one node (herd diffusion).
- **Region/label filtering** — candidates are filtered by `region` and `label` query params, with a penalty for placing outside the requested region.

Nodes self-drain: after 5 consecutive ticks above 90% CPU/mem or below 2 GB free disk, the agent marks itself `draining` so the scheduler stops sending it work.

## Quick start

```bash
# Build
make build          # or: go build ./cmd/takld ./cmd/taklctl

# Run a single node
./bin/takld --node-id node-a

# Query it
./bin/taklctl runners
./bin/taklctl summary
curl http://127.0.0.1:8090/api/v1/runners/best
```

### Multi-node cluster

```bash
# Node A (seeded first)
./bin/takld --node-id node-a --http-addr 127.0.0.1:8091 --sync-addr 127.0.0.1:8101 --bind 7941 --db takl-a.db

# Node B joins A over SWIM; state replicates over gRPC
./bin/takld --node-id node-b --http-addr 127.0.0.1:8092 --sync-addr 127.0.0.1:8102 --bind 7942 --join 127.0.0.1:7941 --db takl-b.db

# Node C joins as well
./bin/takld --node-id node-c --http-addr 127.0.0.1:8093 --sync-addr 127.0.0.1:8103 --bind 7943 --join 127.0.0.1:7941 --db takl-c.db
```

Any node can now answer for the whole fleet:

```bash
./bin/taklctl -addr http://127.0.0.1:8092 cluster
```

### Adding metadata to nodes

```bash
# Tag a node with custom labels (replicated across the cluster)
curl -X PUT http://127.0.0.1:8090/api/v1/runners/node-a/labels \
  -H "Content-Type: application/json" \
  -d '{"env": "production", "tier": "gpu", "team": "ml"}'

# Read labels
curl http://127.0.0.1:8090/api/v1/runners/node-a/labels

# Query by label
curl "http://127.0.0.1:8090/api/v1/runners/best?label=env:production&label=tier:gpu"

# Remove API-set labels
curl -X DELETE http://127.0.0.1:8090/api/v1/runners/node-a/labels
```

## API

| Endpoint | Method | Description |
|---|---|---|
| `/healthz` | GET | liveness probe |
| `/api/v1/runners` | GET | all registered nodes |
| `/api/v1/runners/{id}` | GET | single node |
| `/api/v1/runners/best?region=&label=` | GET | top-K scored placements |
| `/api/v1/runners/{id}/labels` | GET | node labels (config + API-set) |
| `/api/v1/runners/{id}/labels` | PUT | set/merge custom labels on a node |
| `/api/v1/runners/{id}/labels` | DELETE | remove API-set labels from a node |
| `/api/v1/events` | GET | replicated event log |
| `/api/v1/summary` | GET | cluster summary |
| `/api/v1/cluster` | GET | cluster-wide capacity/load |

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
internal/     agent, api, engine/store, engine/sync, membership, model, backend/stub, transport, version
proto/v1      gRPC sync protocol definition
```

## License

MIT — see [LICENSE](LICENSE).

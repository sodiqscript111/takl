# Takl Architecture (As-Built)

This document describes the architecture of Takl as a **decentralized node metadata and placement layer**.

## 1. System Overview

Takl is a **masterless observation, metadata registry, and placement system**. It runs as a decentralized daemon on edge or cloud compute nodes. Its primary purpose is to maintain a live, replicated view of node health, capacities, and custom metadata so that any node can answer placement queries for the entire cluster without central bottlenecks.

The core loop is:
1. **Observe**: Read host system metrics (CPU, memory, disk).
2. **Store**: Save these facts and custom labels to a local SQLite database.
3. **Sync**: Exchange these facts with peer nodes via gRPC pull replication.
4. **Score**: Calculate which node is best suited for a workload based on replicated global state and labels.

## 2. Membership & Discovery (SWIM Gossip)

Takl uses the **HashiCorp `memberlist`** library to implement a SWIM gossip protocol for node discovery and failure detection.

- **No central registry**: Nodes join the cluster by pointing to any existing "seed" node (`--join` flag).
- **Failure detection**: Nodes ping each other periodically. If a node stops responding, the cluster eventually agrees it is dead and removes it from the active membership list.
- **Metadata**: Nodes broadcast their gRPC Sync Address over the gossip network so peers know how to connect for data replication.

*Location: `internal/membership`*

## 3. Storage & Data Model (SQLite + CRDTs)

State is stored locally in SQLite with `PRAGMA journal_mode=WAL` for crash safety. Because any node can write data and network partitions happen, Takl uses **Conflict-Free Replicated Data Types (CRDTs)**.

### The `rows` Table
Everything in Takl (Node state, Custom metadata/labels) is stored as a generic row:
- `kind`: The type of data ("runner", "meta").
- `key`: The unique identifier (e.g., node ID).
- `owner`: Which node created this record (Format: `<nodeID>/<epoch>` or `api/<nodeID>`).
- `hlc_ts` / `hlc_seq`: The Hybrid Logical Clock timestamp.
- `tombstone`: A boolean flag indicating deletion.
- `payload`: A JSON blob of the actual data.

### Conflict Resolution (Last-Write-Wins)
When two nodes have different versions of the same row, Takl uses **LWW (Last-Write-Wins)** based on the Hybrid Logical Clock (HLC).
1. The row with the higher HLC timestamp wins.
2. If timestamps are exactly equal, the row with the lexicographically higher `owner` ID wins.

### Deletions (Tombstones)
Rows are never immediately deleted. Instead, their `tombstone` flag is set to `1`. This allows the deletion event to replicate to other nodes. A Garbage Collection (GC) routine permanently purges tombstones older than 1 hour.

*Location: `internal/engine/store`*

## 4. Synchronization (gRPC Pull Replication)

State replication is **pull-based** over gRPC. Every 2 seconds (configurable), a node asks a random subset of its peers for updates.

### Watermarks
To avoid pulling the entire database every 2 seconds, nodes track **Watermarks**. If Node A pulled from Node B up to HLC time `100`, Node A saves `100` as the watermark. Next time, it only asks Node B for changes *after* `100`.

### Checksum Divergence (Anti-Entropy)
If a node goes offline, misses a tombstone GC window, and comes back, watermarks alone might miss data. Takl uses a fast Merkle-like checksum:
1. Node A requests a pull and sends a 64-bit XOR checksum of all its keys.
2. Node B compares it to its own checksum.
3. If the checksums match, everything is perfectly in sync.
4. If they mismatch, Node B ignores the watermark and sends a full state reconciliation.

*Location: `internal/engine/sync` and `internal/transport`*

## 5. The Agent Loop

Every 1 second (`--tick` flag), the Agent:
1. **Snapshots metrics**: Uses `gopsutil` to read CPU, Memory, and Disk usage.
2. **Saves to Store**: Updates its own `<nodeID>` records in the local SQLite store.
3. **Self-Draining**: If CPU/Memory exceeds 90% or Disk drops below 2GB for 5 consecutive ticks, the node marks itself as `Draining` to stop receiving new workloads.

*Location: `internal/agent` and `internal/backend/stub`*

## 6. The Scoring Engine & Labels

When a client wants to run a workload, they ask Takl for the best node (`GET /api/v1/runners/best?region=&label=`). Any node can answer this query because all nodes have a fully replicated copy of the cluster state.

The base scoring formula is:
```text
Score = (Available Workers * 10) 
      - (CPU% * 50) 
      - (Mem% * 30) 
      + (Free Disk MB / 1000)
```

**Modifiers:**
- **Label Filtering**: Clients can filter by key-value pairs (e.g. `label=gpu:true&label=tier:prod`). Nodes lacking matching labels are excluded.
- **Region Penalty**: If the client requests a specific region and the node is outside that region, it receives a `-1000` penalty.
- **Jitter**: A random `±5` jitter is applied to prevent the "thundering herd" problem (where concurrent clients all collapse onto the exact same candidate).

*Location: `internal/api/api.go`*

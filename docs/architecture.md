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
- `ver`: This node's local version for the row's last change (replication bookkeeping, never replicated).

### Conflict Resolution (Last-Write-Wins)
When two nodes have different versions of the same row, Takl uses **LWW (Last-Write-Wins)** based on the Hybrid Logical Clock (HLC).
1. The row with the higher HLC timestamp wins.
2. If timestamps are exactly equal, the row with the lexicographically higher `owner` ID wins.

### Deletions (Tombstones)
Rows are never immediately deleted. Instead, their `tombstone` flag is set to `1`. This allows the deletion event to replicate to other nodes. A tombstone is only purged once it is older than 1 hour **and** every known peer has pulled it (see *Tombstone GC* below). Purging it earlier would let a peer that never saw the delete send the old row back.

*Location: `internal/engine/store`*

## 4. Synchronization (gRPC Pull Replication)

State replication is **pull-based** over gRPC. Every 2 seconds (configurable), a node asks a random subset of its peers for updates.

### Versions and watermarks
Every change to a node's store (a local write, or a row or event it accepts from a peer) gets the node's next **local version**. Each store also has a random **store id**.

If Node A has pulled Node B's store up to version `100`, A saves `100` as its watermark for B's store id. Next time it asks only for changes with version > `100`, and B answers with them plus its current version, which becomes A's new watermark.

Watermarks used to be HLC timestamps. That lost data: B can receive a row with an *old* HLC from a third node after A's watermark has already moved past it, so A never got it. With local versions, the late row gets a new version on B and is pulled like any other change.

A watermark only counts for the store id it was taken against. If B's database is wiped or recreated it gets a new store id, and A pulls it from version 0.

### Tombstone GC
When B serves a pull, it records the requester's watermark as an **ack**: that peer has everything of B's up to that version. B drops a tombstone only when it is past the grace period (1h) and every peer B knows (pulled from or been pulled by) has acked past it.

A peer that has been unreachable for 24h is **forgotten**: it stops blocking GC. To make sure a forgotten node cannot bring deleted rows back, peers that forgot it refuse to sync with it until it wipes:
- if it pulls from them, they answer "forgotten" and it wipes;
- if they pull from it, they tell it it was forgotten and it wipes before answering.

Wiping drops every row except the node's own writes that it never sent to anyone, then takes a new store id. Everything else re-replicates from peers.

A node only starts forgetting peers after it has itself been running for 24h, so restarting after its own downtime never makes it forget, and force-wipe, healthy peers.

### Model checking
The protocol was checked with TLA+/TLC: the old protocol had counterexamples for lost relayed rows (permanent divergence), tombstone resurrection and a clock race on local writes. For the current one, on bounded 3-node models: the convergence check passes a full search, and the resurrection check found no violation in 23.8M states (that search was not exhaustive).

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

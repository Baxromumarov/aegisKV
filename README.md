# AegisKV

A production-ready distributed in-memory key-value cache optimized for low latency, horizontal scalability, and predictable failure behavior.

## Features

- **Horizontally Scalable**: Add or remove nodes without downtime
- **Low Latency**: <1ms p99 latency in steady state
- **Fault Tolerant**: Automatic failover with lease-based elections
- **Simple API**: GET/SET/DEL with optional TTL
- **Consistent Hashing**: Minimal key redistribution on cluster changes
- **Async Replication**: Primary-follower replication with configurable factor
- **LRU Eviction**: Memory-bounded with segmented LRU eviction
- **Optional Persistence**: Write-ahead log for durability

## Architecture

AegisKV uses a peer-to-peer architecture where all nodes are identical. Key features:

- **Consistent Hashing** with virtual nodes for even distribution
- **Per-shard Leadership** with majority-acknowledged leases
- **Gossip-based Membership** (SWIM-like protocol)
- **Async Replication** with version-based conflict resolution

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                                │
└─────────────────────────┬───────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
     ┌─────────┐     ┌─────────┐     ┌─────────┐
     │  Node 1 │◄───►│  Node 2 │◄───►│  Node 3 │
     │ Primary │     │ Follower│     │ Follower│
     │  S1,S4  │     │  S2,S5  │     │  S3,S6  │
     └─────────┘     └─────────┘     └─────────┘
          ▲               ▲               ▲
          └───────────────┴───────────────┘
                    Gossip Protocol
```

## Quick Start

### Build

```bash
make build
```

### Run a Single Node

```bash
./bin/aegis --client-addr :7000 --gossip-addr :7002
```

### Run a Cluster

```bash
# Terminal 1: First node
./bin/aegis --id node1 --client-addr :7000 --gossip-addr :7002

# Terminal 2: Second node (joins via seed)
./bin/aegis --id node2 --client-addr :7010 --gossip-addr :7012 --seeds localhost:7002

# Terminal 3: Third node
./bin/aegis --id node3 --client-addr :7020 --gossip-addr :7022 --seeds localhost:7002
```

### Using the Client

```go
package main

import (
    "context"
    "github.com/baxromumarov/aegisKV/pkg/client"
)

func main() {
    cfg := client.DefaultConfig([]string{"localhost:7000"})
    c, _ := client.New(cfg)
    defer c.Close()

    ctx := context.Background()

    // Set a value
    c.Set(ctx, "key", []byte("value"))

    // Set with TTL (5 seconds)
    c.SetWithTTL(ctx, "temp-key", []byte("temp-value"), 5000)

    // Get a value
    value, _ := c.Get(ctx, "key")

    // Delete a value
    c.Delete(ctx, "key")
}
```

## Configuration

### Command Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--id` | auto | Node ID |
| `--client-addr` | :7000 | Client connection address |
| `--cluster-addr` | :7001 | Cluster communication address |
| `--gossip-addr` | :7002 | Gossip protocol address |
| `--seeds` | | Comma-separated seed node addresses |
| `--wal` | off | WAL mode: off, write, fsync |
| `--wal-dir` | ./data/wal | WAL directory |
| `--config` | | Path to config file |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `AEGIS_NODE_ID` | Node ID |
| `AEGIS_CLIENT_ADDR` | Client address |
| `AEGIS_CLUSTER_ADDR` | Cluster address |
| `AEGIS_GOSSIP_ADDR` | Gossip address |
| `AEGIS_SEED_NODES` | JSON array of seed nodes |
| `AEGIS_WAL_MODE` | WAL mode |
| `AEGIS_WAL_DIR` | WAL directory |

### Config File (JSON)

```json
{
  "nodeId": "node1",
  "clientAddr": ":7000",
  "clusterAddr": ":7001",
  "gossipAddr": ":7002",
  "seedNodes": ["host1:7002", "host2:7002"],
  "replicationFactor": 3,
  "virtualNodes": 100,
  "numShards": 256,
  "maxMemoryBytes": 1073741824,
  "walMode": 0,
  "walDir": "./data/wal"
}
```

## API

### Operations

| Operation | Description |
|-----------|-------------|
| `GET key` | Retrieve value for key |
| `SET key value [ttl]` | Store value with optional TTL (ms) |
| `DEL key` | Delete key |

### Consistency Model

- **Eventual Consistency** with session guarantees
- **Read-your-writes** guarantee per connection
- **No split-brain writes** (lease-based leadership)

## Design Philosophy

> **One writer per shard.
> Version everything.
> Fail predictably.
> Hide complexity from users.**

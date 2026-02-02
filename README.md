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
- **TLS Support**: Mutual TLS for secure communication
- **Authentication**: Token-based auth for clients and cluster

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
./bin/aegis --bind :7700 --gossip :7701
```

### Run a Cluster

```bash
# Terminal 1: First node
./bin/aegis --node-id node1 --bind :7700 --gossip :7701

# Terminal 2: Second node (joins via seed)
./bin/aegis --node-id node2 --bind :7710 --gossip :7711 --seeds localhost:7701

# Terminal 3: Third node
./bin/aegis --node-id node3 --bind :7720 --gossip :7721 --seeds localhost:7701
```

### Using the Client

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/baxromumarov/aegisKV/pkg/client"
)

func main() {
    // Create a new client
    c := client.New(client.Config{
        Seeds:        []string{"localhost:7700", "localhost:7710", "localhost:7720"},
        MaxConns:     10,
        ConnTimeout:  5 * time.Second,
        ReadTimeout:  5 * time.Second,
        WriteTimeout: 5 * time.Second,
        MaxRetries:   3,
        // AuthToken: "your-secret-token",  // optional
        // TLSConfig: &tls.Config{},        // optional
    })
    defer c.Close()

    // Set a value
    err := c.Set([]byte("user:1"), []byte(`{"name":"John"}`))
    if err != nil {
        log.Fatal(err)
    }

    // Set with TTL (expires in 1 hour)
    err = c.SetWithTTL([]byte("session:abc"), []byte("token123"), time.Hour)
    if err != nil {
        log.Fatal(err)
    }

    // Get a value
    value, err := c.Get([]byte("user:1"))
    if err != nil {
        if err == client.ErrNotFound {
            fmt.Println("Key not found")
        } else {
            log.Fatal(err)
        }
    }
    fmt.Printf("Value: %s\n", value)

    // Get value with remaining TTL
    value, ttl, err := c.GetWithTTL([]byte("session:abc"))
    if err == nil {
        fmt.Printf("Value: %s, TTL: %v\n", value, ttl)
    }

    // Delete a value
    err = c.Delete([]byte("user:1"))
    if err != nil {
        log.Fatal(err)
    }

    // Ping a specific node
    err = c.Ping("localhost:7700")
    if err != nil {
        log.Fatal(err)
    }
}
```

## Configuration

### Command Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--node-id` | hostname | Node ID |
| `--bind` | 0.0.0.0:7700 | Client connection address |
| `--gossip` | 0.0.0.0:7701 | Gossip protocol address |
| `--gossip-advertise` | | Gossip advertise address (for NAT/containers) |
| `--client-advertise` | | Client advertise address (for NAT/containers) |
| `--seeds` | | Comma-separated seed node gossip addresses |
| `--data-dir` | ./data | Data directory |
| `--replication-factor` | 3 | Replication factor |
| `--shards` | 256 | Number of shards |
| `--max-memory` | 1024 | Maximum memory in MB |
| `--wal` | off | WAL mode: off, write, fsync |
| `--auth-token` | | Shared authentication token |
| `--tls-cert` | | Path to TLS certificate |
| `--tls-key` | | Path to TLS private key |
| `--tls-ca` | | Path to TLS CA certificate (for mTLS) |
| `--gossip-secret` | | HMAC secret for gossip signing |
| `--health-addr` | | HTTP health endpoint address (e.g. :8080) |
| `--rate-limit` | 0 | Per-connection requests/sec (0=disabled) |
| `--rate-burst` | 10 | Rate limit burst size |
| `--log-level` | info | Log level: debug, info, warn, error |
| `--write-quorum` | 0 | Write quorum (0=async, -1=all) |
| `--read-quorum` | 0 | Read quorum (0=local only) |
| `--drain-timeout` | 30s | Graceful shutdown timeout |
| `--pid-file` | | Path to PID file |
| `--config` | | Path to config file |

### Config File (JSON)

```json
{
  "node_id": "node1",
  "bind_addr": "0.0.0.0:7700",
  "gossip_bind_addr": "0.0.0.0:7701",
  "seeds": ["host1:7701", "host2:7701"],
  "replication_factor": 3,
  "virtual_nodes": 100,
  "num_shards": 256,
  "max_memory_mb": 1024,
  "wal_mode": "off",
  "wal_dir": "./data/wal",
  "auth_token": "",
  "tls_enabled": false,
  "tls_cert_file": "",
  "tls_key_file": "",
  "health_addr": ":8080",
  "log_level": "info",
  "write_quorum": 0,
  "read_quorum": 0,
  "drain_timeout": "30s"
}
```

## Client API

| Method | Description |
|--------|-------------|
| `New(cfg Config)` | Create a new client |
| `Get(key []byte) ([]byte, error)` | Get value by key |
| `GetWithTTL(key []byte) ([]byte, time.Duration, error)` | Get value + remaining TTL |
| `Set(key, value []byte) error` | Set a key-value pair |
| `SetWithTTL(key, value []byte, ttl time.Duration) error` | Set with expiration |
| `Delete(key []byte) error` | Delete a key |
| `Ping(addr string) error` | Health check a node |
| `AddServer(addr string)` | Add a server to the pool |
| `RemoveServer(addr string)` | Remove a server from the pool |
| `Servers() []string` | List all known servers |
| `Close() error` | Close all connections |

### Error Types

| Error | Description |
|-------|-------------|
| `ErrNotFound` | Key does not exist |
| `ErrClosed` | Client has been closed |
| `ErrTimeout` | Request timed out |
| `ErrMaxRetries` | Maximum retry attempts exceeded |

## Health Endpoints

When `--health-addr` is configured, the following HTTP endpoints are available:

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness check |
| `GET /ready` | Readiness check |
| `GET /metrics` | Prometheus metrics |

## Consistency Model

- **Eventual Consistency** with session guarantees
- **Read-your-writes** guarantee per connection
- **No split-brain writes** (lease-based leadership)

## Production Deployment

### Recommended Settings

```bash
./bin/aegis \
  --node-id node1 \
  --bind 0.0.0.0:7700 \
  --gossip 0.0.0.0:7701 \
  --seeds node2:7701,node3:7701 \
  --replication-factor 3 \
  --shards 256 \
  --max-memory 4096 \
  --wal fsync \
  --auth-token "your-secure-token" \
  --tls-cert /path/to/cert.pem \
  --tls-key /path/to/key.pem \
  --health-addr :8080 \
  --log-level info \
  --drain-timeout 60s
```

### Docker

```bash
docker build -t aegiskv .
docker run -p 7700:7700 -p 7701:7701 aegiskv
```

### Docker Compose (3-node cluster)

```bash
docker-compose up -d
```



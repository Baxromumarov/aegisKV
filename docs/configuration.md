# AegisKV Configuration Reference

Complete reference for all AegisKV configuration options.

## Configuration File Format

AegisKV uses JSON for configuration. Create a file at `/etc/aegiskv/config.json`:

```json
{
  "node_id": "node1",
  "bind_addr": "0.0.0.0:7700",
  ...
}
```

## Environment Variables

All settings can be overridden with environment variables using the `AEGIS_` prefix:

```bash
AEGIS_NODE_ID=node1
AEGIS_BIND_ADDR=0.0.0.0:7700
AEGIS_MAX_MEMORY_MB=16384
```

---

## Node Configuration

### node_id

**Type:** `string`  
**Default:** hostname  
**Environment:** `AEGIS_NODE_ID`

Unique identifier for this node in the cluster. Must be unique across all nodes.

```json
{
  "node_id": "aegis-prod-us-west-1"
}
```

### bind_addr

**Type:** `string`  
**Default:** `0.0.0.0:7700`  
**Environment:** `AEGIS_BIND_ADDR`

Address and port for client connections.

```json
{
  "bind_addr": "0.0.0.0:7700"
}
```

### data_dir

**Type:** `string`  
**Default:** `./data`  
**Environment:** `AEGIS_DATA_DIR`

Directory for storing data files (WAL, snapshots).

```json
{
  "data_dir": "/var/lib/aegiskv"
}
```

### client_advertise_addr

**Type:** `string`  
**Default:** Same as `bind_addr`  
**Environment:** `AEGIS_CLIENT_ADVERTISE_ADDR`

Address advertised to clients for redirects. Useful when behind NAT or load balancer.

```json
{
  "client_advertise_addr": "aegis-node1.example.com:7700"
}
```

---

## Cluster Configuration

### seeds

**Type:** `[]string`  
**Default:** `[]` (empty)  
**Environment:** `AEGIS_SEEDS` (comma-separated)

List of seed node addresses for cluster discovery. At least one seed must be reachable.

```json
{
  "seeds": [
    "10.0.1.10:7701",
    "10.0.1.11:7701",
    "10.0.1.12:7701"
  ]
}
```

### replication_factor

**Type:** `int`  
**Default:** `3`  
**Environment:** `AEGIS_REPLICATION_FACTOR`

Number of replicas for each piece of data. Higher values increase durability but consume more resources.

| Value | Use Case |
|-------|----------|
| 1 | Development only |
| 2 | Non-critical data |
| 3 | Production (recommended) |
| 5 | High-durability requirements |

```json
{
  "replication_factor": 3
}
```

### num_shards

**Type:** `int`  
**Default:** `256`  
**Environment:** `AEGIS_NUM_SHARDS`

Number of shards for data partitioning. More shards = better distribution but more overhead.

| Cluster Size | Recommended Shards |
|--------------|-------------------|
| 3-5 nodes | 256 |
| 6-20 nodes | 512 |
| 21-50 nodes | 1024 |
| 50+ nodes | 2048 |

```json
{
  "num_shards": 256
}
```

**Warning:** Cannot be changed after cluster initialization.

### virtual_nodes

**Type:** `int`  
**Default:** `100`  
**Environment:** `AEGIS_VIRTUAL_NODES`

Number of virtual nodes per physical node in the consistent hash ring. More virtual nodes = better distribution.

```json
{
  "virtual_nodes": 100
}
```

---

## Memory Configuration

### max_memory_mb

**Type:** `int`  
**Default:** `1024` (1 GB)  
**Environment:** `AEGIS_MAX_MEMORY_MB`

Maximum memory to use for caching data. When exceeded, eviction policy kicks in.

```json
{
  "max_memory_mb": 16384
}
```

**Sizing Guide:**
- Leave at least 20% of system RAM for OS and overhead
- Account for replication factor in cluster sizing

### eviction_ratio

**Type:** `float`  
**Default:** `0.9`  
**Environment:** `AEGIS_EVICTION_RATIO`

Memory threshold that triggers eviction. Value of 0.9 means eviction starts at 90% of max_memory_mb.

```json
{
  "eviction_ratio": 0.85
}
```

### shard_max_bytes

**Type:** `int`  
**Default:** `67108864` (64 MB)  
**Environment:** `AEGIS_SHARD_MAX_BYTES`

Maximum size per shard. Used for internal memory management.

```json
{
  "shard_max_bytes": 67108864
}
```

---

## WAL Configuration

### wal_mode

**Type:** `string`  
**Default:** `off`  
**Environment:** `AEGIS_WAL_MODE`

Write-Ahead Log mode for durability.

| Mode | Description | Use Case |
|------|-------------|----------|
| `off` | No WAL | Cache-only, no persistence |
| `async` | Async flush | Best performance, some risk |
| `sync` | Sync after each write | Maximum durability |

```json
{
  "wal_mode": "async"
}
```

### wal_dir

**Type:** `string`  
**Default:** `<data_dir>/wal`  
**Environment:** `AEGIS_WAL_DIR`

Directory for WAL files. Use fast storage (NVMe SSD).

```json
{
  "wal_dir": "/var/lib/aegiskv/wal"
}
```

### wal_max_size_mb

**Type:** `int`  
**Default:** `64`  
**Environment:** `AEGIS_WAL_MAX_SIZE_MB`

Maximum size of each WAL segment before rotation.

```json
{
  "wal_max_size_mb": 128
}
```

---

## Gossip Configuration

### gossip_bind_addr

**Type:** `string`  
**Default:** `0.0.0.0:7701`  
**Environment:** `AEGIS_GOSSIP_BIND_ADDR`

Address for gossip protocol (inter-node communication).

```json
{
  "gossip_bind_addr": "0.0.0.0:7701"
}
```

### gossip_advertise_addr

**Type:** `string`  
**Default:** Same as `gossip_bind_addr`  
**Environment:** `AEGIS_GOSSIP_ADVERTISE_ADDR`

Address advertised to other nodes. Useful when behind NAT.

```json
{
  "gossip_advertise_addr": "10.0.1.10:7701"
}
```

### gossip_interval

**Type:** `duration`  
**Default:** `1s`  
**Environment:** `AEGIS_GOSSIP_INTERVAL`

Interval between gossip messages.

```json
{
  "gossip_interval": "1s"
}
```

### gossip_secret

**Type:** `string`  
**Default:** `` (empty, no encryption)  
**Environment:** `AEGIS_GOSSIP_SECRET`

HMAC key for signing gossip messages. All nodes must use the same key.

```json
{
  "gossip_secret": "your-32-byte-secret-key-here!!"
}
```

### suspect_timeout

**Type:** `duration`  
**Default:** `5s`  
**Environment:** `AEGIS_SUSPECT_TIMEOUT`

Time before a node is marked as suspect (may be failing).

```json
{
  "suspect_timeout": "5s"
}
```

### dead_timeout

**Type:** `duration`  
**Default:** `10s`  
**Environment:** `AEGIS_DEAD_TIMEOUT`

Time before a suspect node is marked as dead and removed.

```json
{
  "dead_timeout": "10s"
}
```

---

## Server Configuration

### max_conns

**Type:** `int`  
**Default:** `10000`  
**Environment:** `AEGIS_MAX_CONNS`

Maximum number of concurrent client connections.

```json
{
  "max_conns": 10000
}
```

### read_timeout

**Type:** `duration`  
**Default:** `30s`  
**Environment:** `AEGIS_READ_TIMEOUT`

Timeout for reading client requests.

```json
{
  "read_timeout": "30s"
}
```

### write_timeout

**Type:** `duration`  
**Default:** `30s`  
**Environment:** `AEGIS_WRITE_TIMEOUT`

Timeout for writing responses to clients.

```json
{
  "write_timeout": "30s"
}
```

---

## Replication Configuration

### replication_batch_size

**Type:** `int`  
**Default:** `100`  
**Environment:** `AEGIS_REPLICATION_BATCH_SIZE`

Number of operations to batch before sending to replicas.

```json
{
  "replication_batch_size": 100
}
```

### replication_batch_timeout

**Type:** `duration`  
**Default:** `10ms`  
**Environment:** `AEGIS_REPLICATION_BATCH_TIMEOUT`

Maximum time to wait for a full batch before sending partial batch.

```json
{
  "replication_batch_timeout": "10ms"
}
```

### replication_max_retries

**Type:** `int`  
**Default:** `3`  
**Environment:** `AEGIS_REPLICATION_MAX_RETRIES`

Maximum retries for failed replication attempts.

```json
{
  "replication_max_retries": 3
}
```

---

## Quorum Configuration

### write_quorum

**Type:** `int`  
**Default:** `0` (async)  
**Environment:** `AEGIS_WRITE_QUORUM`

Minimum number of successful writes before returning to client.

| Value | Behavior |
|-------|----------|
| 0 | Async (return immediately) |
| 1 | Wait for primary only |
| 2 | Wait for primary + 1 replica |
| -1 | Wait for all replicas |

```json
{
  "write_quorum": 2
}
```

### read_quorum

**Type:** `int`  
**Default:** `0` (local)  
**Environment:** `AEGIS_READ_QUORUM`

Number of nodes to read from for consistency.

| Value | Behavior |
|-------|----------|
| 0 | Local read only (fastest) |
| 1 | Read from 1 node |
| 2+ | Read repair enabled |

```json
{
  "read_quorum": 1
}
```

---

## Authentication

### auth_token

**Type:** `string`  
**Default:** `` (empty, no auth)  
**Environment:** `AEGIS_AUTH_TOKEN`

Shared secret for client authentication. Leave empty to disable auth.

```json
{
  "auth_token": "your-very-long-and-secure-token-here"
}
```

**Security Note:** Use at least 32 random characters.

---

## TLS Configuration

### tls_enabled

**Type:** `bool`  
**Default:** `false`  
**Environment:** `AEGIS_TLS_ENABLED`

Enable TLS for client connections.

```json
{
  "tls_enabled": true
}
```

### tls_cert_file

**Type:** `string`  
**Default:** `` (empty)  
**Environment:** `AEGIS_TLS_CERT_FILE`

Path to TLS certificate file.

```json
{
  "tls_cert_file": "/etc/aegiskv/ssl/server.crt"
}
```

### tls_key_file

**Type:** `string`  
**Default:** `` (empty)  
**Environment:** `AEGIS_TLS_KEY_FILE`

Path to TLS private key file.

```json
{
  "tls_key_file": "/etc/aegiskv/ssl/server.key"
}
```

### tls_ca_file

**Type:** `string`  
**Default:** `` (empty)  
**Environment:** `AEGIS_TLS_CA_FILE`

Path to CA certificate for client certificate validation (mTLS).

```json
{
  "tls_ca_file": "/etc/aegiskv/ssl/ca.crt"
}
```

---

## Health & Metrics

### health_addr

**Type:** `string`  
**Default:** `0.0.0.0:7702`  
**Environment:** `AEGIS_HEALTH_ADDR`

Address for health check and metrics endpoint.

```json
{
  "health_addr": "0.0.0.0:7702"
}
```

**Endpoints:**
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics
- `GET /cluster` - Cluster status

---

## Rate Limiting

### rate_limit

**Type:** `float`  
**Default:** `0` (unlimited)  
**Environment:** `AEGIS_RATE_LIMIT`

Maximum operations per second per connection. Set to 0 to disable.

```json
{
  "rate_limit": 10000
}
```

### rate_burst

**Type:** `int`  
**Default:** `100`  
**Environment:** `AEGIS_RATE_BURST`

Burst size for rate limiter (token bucket).

```json
{
  "rate_burst": 100
}
```

---

## Graceful Shutdown

### drain_timeout

**Type:** `duration`  
**Default:** `30s`  
**Environment:** `AEGIS_DRAIN_TIMEOUT`

Time to wait for in-flight requests during graceful shutdown.

```json
{
  "drain_timeout": "30s"
}
```

### pid_file

**Type:** `string`  
**Default:** `` (empty, disabled)  
**Environment:** `AEGIS_PID_FILE`

Path to write PID file for process management.

```json
{
  "pid_file": "/var/run/aegiskv.pid"
}
```

---

## Logging

### log_level

**Type:** `string`  
**Default:** `info`  
**Environment:** `AEGIS_LOG_LEVEL`

Logging verbosity level.

| Level | Description |
|-------|-------------|
| `debug` | Verbose debugging info |
| `info` | Normal operation |
| `warn` | Warnings only |
| `error` | Errors only |

```json
{
  "log_level": "info"
}
```

---

## Maintenance

### maintenance_interval

**Type:** `duration`  
**Default:** `10s`  
**Environment:** `AEGIS_MAINTENANCE_INTERVAL`

Interval for internal maintenance tasks (cleanup, compaction, etc.).

```json
{
  "maintenance_interval": "10s"
}
```

---

## Complete Example

Here's a complete production configuration:

```json
{
  "node_id": "aegis-prod-1",
  "bind_addr": "0.0.0.0:7700",
  "data_dir": "/var/lib/aegiskv",
  "client_advertise_addr": "aegis-prod-1.internal:7700",

  "seeds": [
    "aegis-prod-1.internal:7701",
    "aegis-prod-2.internal:7701",
    "aegis-prod-3.internal:7701"
  ],
  "replication_factor": 3,
  "num_shards": 256,
  "virtual_nodes": 100,

  "max_memory_mb": 32768,
  "eviction_ratio": 0.85,

  "wal_mode": "async",
  "wal_dir": "/var/lib/aegiskv/wal",
  "wal_max_size_mb": 256,

  "gossip_bind_addr": "0.0.0.0:7701",
  "gossip_advertise_addr": "aegis-prod-1.internal:7701",
  "gossip_interval": "1s",
  "gossip_secret": "your-32-byte-gossip-secret-key!!",
  "suspect_timeout": "5s",
  "dead_timeout": "10s",

  "max_conns": 10000,
  "read_timeout": "30s",
  "write_timeout": "30s",

  "replication_batch_size": 100,
  "replication_batch_timeout": "10ms",

  "write_quorum": 2,
  "read_quorum": 1,

  "auth_token": "your-very-long-and-secure-auth-token",

  "tls_enabled": true,
  "tls_cert_file": "/etc/aegiskv/ssl/server.crt",
  "tls_key_file": "/etc/aegiskv/ssl/server.key",
  "tls_ca_file": "/etc/aegiskv/ssl/ca.crt",

  "health_addr": "0.0.0.0:7702",

  "rate_limit": 100000,
  "rate_burst": 1000,

  "drain_timeout": "30s",
  "pid_file": "/var/run/aegiskv.pid",

  "log_level": "info",
  "maintenance_interval": "10s"
}
```

---

## Configuration Precedence

Configuration is loaded in this order (later overrides earlier):

1. Compiled defaults
2. Configuration file
3. Environment variables
4. Command-line flags

---

## Validation

AegisKV validates configuration on startup. Invalid configurations will prevent startup with clear error messages.

To validate a configuration file:

```bash
aegiskv -config /etc/aegiskv/config.json -validate
```

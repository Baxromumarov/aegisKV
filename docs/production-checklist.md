# AegisKV Production Checklist

This checklist ensures AegisKV is ready for production deployment.

## Pre-Deployment Checklist

### 1. Build & Test Verification ✅

- [ ] All unit tests pass: `go test ./pkg/... -count=1`
- [ ] Integration tests pass: `go test ./tests/integration/... -count=1`
- [ ] Process tests pass: `go test ./tests/process/... -count=1`
- [ ] Build successful: `go build -o bin/aegis ./cmd/aegis`
- [ ] No race conditions: `go test -race ./pkg/...`

### 2. Configuration Review ✅

- [ ] **Node ID**: Each node has a unique `--node-id`
- [ ] **Bind Address**: Correctly configured for client connections (`--bind`)
- [ ] **Gossip Address**: Properly set for cluster communication (`--gossip`)
- [ ] **Seeds**: At least one seed node configured for cluster discovery
- [ ] **Replication Factor**: Set to at least 3 for production (`--replication-factor=3`)
- [ ] **Shards**: Appropriate for data volume (`--shards`, default 64)
- [ ] **WAL Mode**: Set to `fsync` for durability (`--wal=fsync`)
- [ ] **Max Memory**: Configured per node (`--max-memory`)
- [ ] **Authentication**: Token set if required (`--auth-token`)
- [ ] **TLS**: Certificates configured for secure communication

### 3. Infrastructure Verification ✅

- [ ] **Ports Open**: 
  - Client port (default 7700)
  - Gossip port (default 7701)
  - Health port (default 7702)
- [ ] **Firewall Rules**: Allow traffic between cluster nodes
- [ ] **DNS/Service Discovery**: Node addresses are resolvable
- [ ] **Storage**: Sufficient disk space for WAL files
- [ ] **Memory**: At least 2x `--max-memory` available per node

### 4. Cluster Formation ✅

- [ ] Minimum 3 nodes for fault tolerance
- [ ] Nodes can discover each other via gossip
- [ ] Health endpoints return "healthy" status
- [ ] Shards are distributed across nodes

### 5. Monitoring & Observability ✅

- [ ] **Health Endpoints**: `/health`, `/ready`, `/live` accessible
- [ ] **Metrics**: Prometheus metrics exposed (if enabled)
- [ ] **Logging**: Log level appropriate (info/warn for production)
- [ ] **Alerting**: Set up for node failures, high latency, memory pressure

---

## Deployment Procedure

### Step 1: Build

```bash
go build -o bin/aegis ./cmd/aegis
```

### Step 2: Start First Node (Seed)

```bash
./bin/aegis \
  --node-id=node-1 \
  --bind=10.0.1.1:7700 \
  --gossip=10.0.1.1:7701 \
  --health=:7702 \
  --wal=fsync \
  --data-dir=/var/lib/aegis/data \
  --replication-factor=3 \
  --max-memory=4096
```

### Step 3: Start Additional Nodes

```bash
./bin/aegis \
  --node-id=node-2 \
  --bind=10.0.1.2:7700 \
  --gossip=10.0.1.2:7701 \
  --health=:7702 \
  --seeds=10.0.1.1:7701 \
  --wal=fsync \
  --data-dir=/var/lib/aegis/data \
  --replication-factor=3 \
  --max-memory=4096
```

### Step 4: Verify Cluster Health

```bash
# Check each node's health
curl http://10.0.1.1:7702/health
curl http://10.0.1.2:7702/health
curl http://10.0.1.3:7702/health
```

### Step 5: Test Operations

```bash
# Using nc/netcat for raw protocol
echo -e "SET testkey testvalue" | nc 10.0.1.1 7700
echo -e "GET testkey" | nc 10.0.1.1 7700
```

---

## Post-Deployment Verification

### Cluster Status Checks

| Check | Command | Expected |
|-------|---------|----------|
| Node health | `curl localhost:7702/health` | `{"status":"healthy"}` |
| Node ready | `curl localhost:7702/ready` | `{"ready":true}` |
| Cluster members | Check logs for "Node joined" | All nodes visible |

### Performance Baseline

Run a quick load test to establish baseline:

```bash
go test ./tests/integration/... -run TestHighThroughput -v
```

---

## Operational Procedures

### Rolling Restart

1. Stop one node at a time
2. Wait for cluster to detect node leave (check logs)
3. Start the node
4. Wait for node to rejoin and shards to rebalance
5. Verify health before proceeding to next node

### Adding Nodes

1. Start new node with existing seed addresses
2. Monitor shard rebalancing in logs
3. Verify data distribution with health endpoint

### Removing Nodes

1. Gracefully stop the node (SIGTERM)
2. Wait for gossip to detect leave
3. Verify shards rebalanced to remaining nodes

---

## Emergency Procedures

### Node Crash Recovery

1. Check WAL directory for intact files
2. Restart node with same configuration
3. Node will replay WAL on startup
4. Verify data integrity after recovery

### Full Cluster Recovery

1. Start seed node first
2. Start remaining nodes
3. All nodes will replay their WAL files
4. Wait for gossip convergence

### Data Corruption

1. Stop affected node
2. Remove corrupted WAL files (if identifiable)
3. Restart node (will sync from replicas)
4. Verify data consistency

---

## Test Categories

| Category | Command | Purpose |
|----------|---------|---------|
| Unit Tests | `go test ./pkg/...` | Core functionality |
| Integration | `go test ./tests/integration/...` | In-process cluster tests |
| Process | `go test ./tests/process/...` | Multi-process tests |
| Chaos | `go test ./tests/... -tags chaos` | Fault injection tests |
| Load | `go test ./tests/... -tags loadtest` | Performance tests |
| Docker | `go test ./tests/... -tags docker` | Docker cluster tests |

---

## Security Checklist

- [ ] TLS certificates valid and not expired
- [ ] Auth token is strong (32+ chars random)
- [ ] Gossip secret configured for cluster authentication
- [ ] Ports not exposed to public internet
- [ ] Data directory has appropriate permissions (700)
- [ ] Log files don't contain sensitive data

---

## Capacity Planning

| Metric | Recommendation |
|--------|----------------|
| CPU | 2+ cores per node |
| Memory | 2x max-memory setting |
| Disk | 10GB+ for WAL per node |
| Network | Low latency between nodes (<10ms) |
| Nodes | Minimum 3, recommend 5 for high availability |

---

## Monitoring Thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| Memory Usage | >80% | >95% |
| Disk Usage | >70% | >90% |
| Request Latency p99 | >50ms | >200ms |
| Error Rate | >1% | >5% |
| Dropped Replicas | >100/min | >1000/min |
| Circuit Breaker Opens | Any | Sustained |

---

## Version Information

- **Go Version**: 1.24.5+
- **AegisKV Version**: Check with `./bin/aegis --version`

---

## Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Developer | | | |
| Operations | | | |
| Security | | | |
| Manager | | | |

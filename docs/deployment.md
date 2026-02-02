# AegisKV Production Deployment Guide

This guide covers deploying AegisKV in production environments with high availability and optimal performance.

## Table of Contents

1. [Hardware Requirements](#hardware-requirements)
2. [Network Configuration](#network-configuration)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Cluster Setup](#cluster-setup)
6. [TLS/Security](#tlssecurity)
7. [Monitoring](#monitoring)
8. [Backup & Recovery](#backup--recovery)
9. [Troubleshooting](#troubleshooting)

---

## Hardware Requirements

### Minimum Requirements (per node)

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 8 GB | 32+ GB |
| Storage | 50 GB SSD | 200+ GB NVMe |
| Network | 1 Gbps | 10 Gbps |

### Memory Sizing

AegisKV is an in-memory cache. Size your nodes according to your dataset:

```
Total Data × Replication Factor = Cluster Memory Required
Cluster Memory / Number of Nodes = Per-Node Memory
```

**Example:** 100 GB dataset with RF=3 on 5 nodes:
- Cluster needs: 100 GB × 3 = 300 GB
- Per node: 300 GB / 5 = 60 GB RAM

**Recommendation:** Leave 20% headroom for operations and burst traffic.

### Disk Requirements

Even though AegisKV is in-memory, disk is used for:
- Write-Ahead Log (WAL) for durability
- Data snapshots
- Logs

Use fast SSDs (NVMe preferred) for WAL to avoid write bottlenecks.

---

## Network Configuration

### Required Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 7700 | TCP | Client connections |
| 7701 | TCP/UDP | Gossip (inter-node communication) |
| 7702 | TCP | Metrics/Health endpoint |

### Firewall Rules

```bash
# Allow client connections
sudo ufw allow 7700/tcp

# Allow gossip between cluster nodes
sudo ufw allow 7701/tcp
sudo ufw allow 7701/udp

# Allow health/metrics (restrict to monitoring)
sudo ufw allow from 10.0.0.0/8 to any port 7702
```

### DNS/Service Discovery

For production, use:
- **Static IPs** for seed nodes
- **DNS round-robin** for client load balancing
- **Internal DNS** for node discovery

---

## Installation

### From Binary

```bash
# Download latest release
curl -LO https://github.com/baxromumarov/aegisKV/releases/latest/download/aegiskv-linux-amd64.tar.gz
tar xzf aegiskv-linux-amd64.tar.gz
sudo mv aegiskv /usr/local/bin/

# Create user and directories
sudo useradd -r -s /bin/false aegiskv
sudo mkdir -p /etc/aegiskv /var/lib/aegiskv /var/log/aegiskv
sudo chown aegiskv:aegiskv /var/lib/aegiskv /var/log/aegiskv
```

### From Source

```bash
git clone https://github.com/baxromumarov/aegisKV.git
cd aegisKV
go build -o aegiskv ./cmd/aegiskv
sudo mv aegiskv /usr/local/bin/
```

### Docker

```bash
docker run -d \
  --name aegiskv \
  -p 7700:7700 \
  -p 7701:7701 \
  -v /data/aegiskv:/data \
  -e AEGIS_NODE_ID=node1 \
  -e AEGIS_SEEDS=node2:7701,node3:7701 \
  ghcr.io/baxromumarov/aegiskv:latest
```

---

## Configuration

### Configuration File

Create `/etc/aegiskv/config.json`:

```json
{
  "node_id": "node1",
  "bind_addr": "0.0.0.0:7700",
  "data_dir": "/var/lib/aegiskv",
  
  "addrs": [
    "10.0.1.10:7701",
    "10.0.1.11:7701",
    "10.0.1.12:7701"
  ],
  "replication_factor": 3,
  "num_shards": 256,
  
  "max_memory_mb": 16384,
  "eviction_ratio": 0.9,
  
  "wal_mode": "async",
  "wal_dir": "/var/lib/aegiskv/wal",
  "wal_max_size_mb": 256,
  
  "gossip_bind_addr": "0.0.0.0:7701",
  "gossip_interval": "1s",
  
  "max_conns": 10000,
  "read_timeout": "30s",
  "write_timeout": "30s",
  
  "health_addr": "0.0.0.0:7702",
  
  "auth_token": "your-secret-token",
  
  "write_quorum": 2,
  "read_quorum": 1,
  
  "drain_timeout": "30s",
  "log_level": "info"
}
```

### Systemd Service

Create `/etc/systemd/system/aegiskv.service`:

```ini
[Unit]
Description=AegisKV Distributed Cache
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=aegiskv
Group=aegiskv
ExecStart=/usr/local/bin/aegiskv -config /etc/aegiskv/config.json
ExecReload=/bin/kill -SIGHUP $MAINPID
Restart=on-failure
RestartSec=5
LimitNOFILE=65535
LimitNPROC=65535

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/aegiskv /var/log/aegiskv

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable aegiskv
sudo systemctl start aegiskv
```

---

## Cluster Setup

### Initial Bootstrap (3-Node Cluster)

**Node 1 (Seed):**
```json
{
  "node_id": "node1",
  "bind_addr": "10.0.1.10:7700",
  "gossip_bind_addr": "10.0.1.10:7701",
  "addrs": []
}
```

**Node 2:**
```json
{
  "node_id": "node2",
  "bind_addr": "10.0.1.11:7700",
  "gossip_bind_addr": "10.0.1.11:7701",
  "addrs": ["10.0.1.10:7701"]
}
```

**Node 3:**
```json
{
  "node_id": "node3",
  "bind_addr": "10.0.1.12:7700",
  "gossip_bind_addr": "10.0.1.12:7701",
  "addrs": ["10.0.1.10:7701", "10.0.1.11:7701"]
}
```

### Adding Nodes

1. Configure new node with existing cluster addrs
2. Start the node
3. Wait for gossip membership convergence
4. Shards will automatically rebalance

### Removing Nodes

1. Stop the node gracefully: `systemctl stop aegiskv`
2. The cluster will detect the node as dead after `dead_timeout`
3. Shards will be redistributed to remaining nodes

---

## TLS/Security

### Generate Certificates

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt \
  -subj "/CN=AegisKV CA"

# Generate node certificate
openssl genrsa -out node.key 2048
openssl req -new -key node.key -out node.csr \
  -subj "/CN=aegiskv-node1"
openssl x509 -req -days 365 -in node.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out node.crt
```

### Enable TLS

```json
{
  "tls_enabled": true,
  "tls_cert_file": "/etc/aegiskv/ssl/node.crt",
  "tls_key_file": "/etc/aegiskv/ssl/node.key",
  "tls_ca_file": "/etc/aegiskv/ssl/ca.crt"
}
```

### Authentication

Set a strong auth token (32+ random characters):

```bash
# Generate token
AUTH_TOKEN=$(openssl rand -base64 32)
echo "auth_token: $AUTH_TOKEN"
```

---

## Monitoring

### Prometheus Metrics

Scrape `/metrics` on the health port (7702):

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'aegiskv'
    static_configs:
      - targets:
        - 'aegiskv-node1:7702'
        - 'aegiskv-node2:7702'
        - 'aegiskv-node3:7702'
```

### Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `aegis_ops_total` | Total operations by type | - |
| `aegis_ops_errors_total` | Operation errors | > 1% of ops |
| `aegis_cache_size_bytes` | Cache memory usage | > 90% of max |
| `aegis_cache_items` | Number of items | - |
| `aegis_active_connections` | Client connections | > 80% of max |
| `aegis_cluster_members` | Healthy cluster nodes | < expected |
| `aegis_replica_success_total` | Successful replications | - |
| `aegis_replica_fail_total` | Failed replications | > 0 sustained |

### Grafana Dashboard

Import the provided dashboard from `docs/grafana-dashboard.json`.

### Health Checks

```bash
# Check node health
curl http://localhost:7702/health

# Check cluster status
curl http://localhost:7702/cluster
```

---

## Backup & Recovery

### WAL Recovery

If a node crashes, it will replay the WAL on restart:

```bash
# Check WAL files
ls -la /var/lib/aegiskv/wal/

# Force WAL replay (if needed)
aegiskv -config /etc/aegiskv/config.json -wal-replay
```

### Cross-Region Replication

For disaster recovery, run clusters in multiple regions with async replication:

```json
{
  "cross_dc_seeds": ["us-west-cluster:7701"],
  "cross_dc_replication": true
}
```

---

## Troubleshooting

### Node Won't Join Cluster

1. Check network connectivity:
   ```bash
   nc -zv seed-node 7701
   ```

2. Verify gossip secret matches all nodes

3. Check firewall rules

4. Review logs:
   ```bash
   journalctl -u aegiskv -f
   ```

### High Latency

1. Check memory pressure:
   ```bash
   curl localhost:7702/metrics | grep cache_size
   ```

2. Monitor eviction rate

3. Check network latency between nodes

4. Review client connection count

### Data Loss After Restart

1. Ensure WAL mode is not "off"
2. Check WAL directory permissions
3. Verify disk space availability
4. Review WAL corruption logs

### Memory Issues

1. Enable memory pressure detection:
   ```json
   {
     "max_memory_mb": 16384,
     "eviction_ratio": 0.85
   }
   ```

2. Monitor GC pauses in logs

3. Consider increasing `GOGC` environment variable

---

## Best Practices

1. **Always use odd number of nodes** (3, 5, 7) for quorum decisions
2. **Set replication_factor = 3** minimum for production
3. **Enable TLS and authentication** in production
4. **Use async WAL mode** for best performance, sync for durability
5. **Monitor memory usage** and set conservative eviction thresholds
6. **Use dedicated network** for gossip traffic
7. **Implement client-side retry logic** with exponential backoff
8. **Test failover scenarios** regularly
9. **Keep all nodes on the same version** during normal operation
10. **Use rolling restarts** for upgrades

---

## Support

- GitHub Issues: https://github.com/baxromumarov/aegisKV/issues
- Documentation: https://github.com/baxromumarov/aegisKV/docs

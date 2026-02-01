# AegisKV Operations Runbook

This runbook provides procedures for common operational tasks and incident response.

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Routine Maintenance](#routine-maintenance)
3. [Incident Response](#incident-response)
4. [Recovery Procedures](#recovery-procedures)
5. [Scaling Operations](#scaling-operations)
6. [Emergency Procedures](#emergency-procedures)

---

## Daily Operations

### Health Check Procedure

Run these checks daily:

```bash
#!/bin/bash
# daily-health-check.sh

NODES=("node1:7702" "node2:7702" "node3:7702")

for node in "${NODES[@]}"; do
  echo "=== Checking $node ==="
  
  # Health endpoint
  if curl -s "http://$node/health" | grep -q "ok"; then
    echo "✓ Health: OK"
  else
    echo "✗ Health: FAILED"
  fi
  
  # Memory usage
  mem=$(curl -s "http://$node/metrics" | grep aegis_cache_size_bytes | awk '{print $2}')
  echo "  Memory: $((mem / 1024 / 1024)) MB"
  
  # Active connections
  conns=$(curl -s "http://$node/metrics" | grep aegis_active_connections | awk '{print $2}')
  echo "  Connections: $conns"
  
  # Cluster members
  members=$(curl -s "http://$node/metrics" | grep aegis_cluster_members | awk '{print $2}')
  echo "  Cluster members: $members"
  
  echo ""
done
```

### Log Review

```bash
# Check for errors in the last 24 hours
journalctl -u aegiskv --since "24 hours ago" --priority err

# Check for warnings
journalctl -u aegiskv --since "24 hours ago" --priority warning | head -50

# Monitor real-time logs
journalctl -u aegiskv -f
```

### Metrics Dashboard Review

Check the following in Grafana:

1. **Request Rate** - Any unusual spikes or drops?
2. **Error Rate** - Should be < 0.1%
3. **P99 Latency** - Should be < 10ms for reads
4. **Memory Usage** - Should be < 85% of max
5. **Cluster Health** - All nodes should be visible

---

## Routine Maintenance

### Rolling Restart

Use this procedure to restart nodes without downtime:

```bash
#!/bin/bash
# rolling-restart.sh

NODES=("node1" "node2" "node3")
SLEEP_BETWEEN=60  # seconds

for node in "${NODES[@]}"; do
  echo "=== Restarting $node ==="
  
  # Graceful shutdown (waits for drain_timeout)
  ssh $node "sudo systemctl restart aegiskv"
  
  # Wait for node to rejoin cluster
  echo "Waiting for $node to rejoin..."
  sleep $SLEEP_BETWEEN
  
  # Verify health
  while ! curl -s "http://$node:7702/health" | grep -q "ok"; do
    echo "Waiting for $node to become healthy..."
    sleep 5
  done
  
  echo "✓ $node is healthy"
  echo ""
done

echo "Rolling restart complete"
```

### Configuration Reload (SIGHUP)

For configuration changes that don't require restart:

```bash
# Send SIGHUP to reload config
sudo systemctl reload aegiskv

# Or manually
sudo kill -SIGHUP $(cat /var/run/aegiskv.pid)

# Verify reload
journalctl -u aegiskv -n 10 | grep -i reload
```

**Reloadable settings:**
- Log level
- Rate limits
- Some timeout values

**Requires restart:**
- Node ID
- Bind addresses
- TLS configuration
- Replication factor

### WAL Cleanup

WAL segments are automatically cleaned, but you can force cleanup:

```bash
# Check WAL size
du -sh /var/lib/aegiskv/wal/

# Force WAL truncation (after ensuring all data is replicated)
curl -X POST http://localhost:7702/admin/wal/truncate
```

### Upgrading AegisKV

1. **Download new version:**
   ```bash
   curl -LO https://github.com/baxromumarov/aegisKV/releases/download/v2.0.0/aegiskv-linux-amd64.tar.gz
   tar xzf aegiskv-linux-amd64.tar.gz
   ```

2. **Stage the binary:**
   ```bash
   sudo cp aegiskv /usr/local/bin/aegiskv.new
   ```

3. **Rolling upgrade (one node at a time):**
   ```bash
   sudo systemctl stop aegiskv
   sudo mv /usr/local/bin/aegiskv /usr/local/bin/aegiskv.old
   sudo mv /usr/local/bin/aegiskv.new /usr/local/bin/aegiskv
   sudo systemctl start aegiskv
   ```

4. **Verify each node before proceeding to next**

5. **Rollback if issues:**
   ```bash
   sudo systemctl stop aegiskv
   sudo mv /usr/local/bin/aegiskv.old /usr/local/bin/aegiskv
   sudo systemctl start aegiskv
   ```

---

## Incident Response

### Severity Levels

| Level | Description | Response Time | Example |
|-------|-------------|---------------|---------|
| SEV1 | Complete outage | Immediate | All nodes down |
| SEV2 | Partial outage | 15 minutes | 1+ nodes down |
| SEV3 | Degraded performance | 1 hour | High latency |
| SEV4 | Minor issue | Next business day | Warning logs |

### SEV1: Complete Cluster Outage

**Symptoms:**
- All health checks failing
- No client connections
- Monitoring alerts firing

**Immediate Actions:**

1. **Verify scope:**
   ```bash
   for node in node1 node2 node3; do
     echo "$node: $(curl -s -o /dev/null -w '%{http_code}' http://$node:7702/health)"
   done
   ```

2. **Check network:**
   ```bash
   ping -c 3 node1
   nc -zv node1 7700
   nc -zv node1 7701
   ```

3. **Check process status:**
   ```bash
   ssh node1 "systemctl status aegiskv"
   ssh node1 "ps aux | grep aegiskv"
   ```

4. **Check logs:**
   ```bash
   ssh node1 "journalctl -u aegiskv -n 100 --no-pager"
   ```

5. **Check system resources:**
   ```bash
   ssh node1 "free -h; df -h; dmesg | tail -20"
   ```

**Recovery:**
- If OOM: Increase memory or reduce max_memory_mb
- If disk full: Clear old logs/WAL
- If network: Escalate to network team
- If unknown: Restart nodes one by one

### SEV2: Node Failure

**Symptoms:**
- One or more nodes unhealthy
- Increased latency on remaining nodes
- Replication errors

**Actions:**

1. **Identify failed node(s):**
   ```bash
   curl localhost:7702/cluster | jq '.members[] | select(.status != "alive")'
   ```

2. **Check node status:**
   ```bash
   ssh failed-node "systemctl status aegiskv"
   ```

3. **Attempt restart:**
   ```bash
   ssh failed-node "sudo systemctl restart aegiskv"
   ```

4. **Monitor recovery:**
   ```bash
   watch -n 2 'curl -s localhost:7702/cluster | jq ".members | length"'
   ```

5. **If node won't recover:**
   - Check disk space
   - Check for corrupt WAL files
   - Consider replacing the node

### SEV3: High Latency

**Symptoms:**
- P99 latency > 50ms
- Client timeouts
- Slow responses

**Diagnosis:**

1. **Check memory pressure:**
   ```bash
   curl localhost:7702/metrics | grep -E "cache_size|evict"
   ```

2. **Check connection count:**
   ```bash
   curl localhost:7702/metrics | grep active_connections
   ```

3. **Check CPU usage:**
   ```bash
   top -p $(pgrep aegiskv)
   ```

4. **Check network latency:**
   ```bash
   ping -c 10 other-node
   ```

**Resolution:**
- High memory: Lower max_memory_mb or add nodes
- Too many connections: Increase max_conns or add load balancer
- CPU bound: Add more nodes for sharding
- Network latency: Check network equipment

---

## Recovery Procedures

### Single Node Recovery

If a node has failed and won't restart:

1. **Stop the node:**
   ```bash
   sudo systemctl stop aegiskv
   ```

2. **Backup WAL (if accessible):**
   ```bash
   cp -r /var/lib/aegiskv/wal /backup/wal-$(date +%Y%m%d)
   ```

3. **Clear data directory (if corrupt):**
   ```bash
   rm -rf /var/lib/aegiskv/wal/*
   ```

4. **Restart node:**
   ```bash
   sudo systemctl start aegiskv
   ```

5. **The node will:**
   - Join the cluster
   - Receive data via replication
   - Resume normal operation

### Full Cluster Recovery

If all nodes have failed:

1. **Identify the node with most recent WAL:**
   ```bash
   for node in node1 node2 node3; do
     echo "$node: $(ssh $node 'ls -lt /var/lib/aegiskv/wal/ | head -2')"
   done
   ```

2. **Start the node with most recent data first (as seed):**
   ```bash
   # On node with best WAL
   sudo systemctl start aegiskv
   ```

3. **Start remaining nodes:**
   ```bash
   # On other nodes
   sudo systemctl start aegiskv
   ```

4. **Verify data integrity:**
   ```bash
   curl localhost:7702/stats
   ```

### Data Corruption Recovery

If data appears corrupted:

1. **Stop affected node:**
   ```bash
   sudo systemctl stop aegiskv
   ```

2. **Backup current state:**
   ```bash
   tar czf /backup/aegiskv-corrupt-$(date +%Y%m%d).tar.gz /var/lib/aegiskv
   ```

3. **Clear data:**
   ```bash
   rm -rf /var/lib/aegiskv/*
   ```

4. **Restart (will recover from replicas):**
   ```bash
   sudo systemctl start aegiskv
   ```

---

## Scaling Operations

### Adding Nodes

1. **Prepare new node:**
   ```bash
   # Install AegisKV
   # Configure with existing seeds
   ```

2. **Start the new node:**
   ```bash
   sudo systemctl start aegiskv
   ```

3. **Monitor shard rebalancing:**
   ```bash
   watch -n 5 'curl -s localhost:7702/metrics | grep shards_owned'
   ```

4. **Verify cluster membership:**
   ```bash
   curl localhost:7702/cluster
   ```

### Removing Nodes

1. **Gracefully shutdown the node:**
   ```bash
   sudo systemctl stop aegiskv
   ```

2. **Wait for data redistribution:**
   ```bash
   # Monitor other nodes' shard counts
   watch 'curl -s node1:7702/metrics | grep shards_owned'
   ```

3. **Remove from seed lists** (update config on other nodes)

4. **Reload config on other nodes:**
   ```bash
   sudo systemctl reload aegiskv
   ```

### Emergency Scaling

If you need to quickly add capacity:

1. **Deploy new nodes with minimal config:**
   ```json
   {
     "node_id": "emergency-node-1",
     "seeds": ["existing-node:7701"],
     "max_memory_mb": 32768
   }
   ```

2. **Start nodes:**
   ```bash
   sudo systemctl start aegiskv
   ```

3. **Data will automatically redistribute**

---

## Emergency Procedures

### Emergency Shutdown

If you need to stop all nodes immediately:

```bash
#!/bin/bash
# emergency-shutdown.sh

NODES=("node1" "node2" "node3")

for node in "${NODES[@]}"; do
  ssh $node "sudo systemctl stop aegiskv" &
done

wait
echo "All nodes stopped"
```

### Emergency Data Export

If you need to export all data:

```bash
# Connect to each node and dump data
aegiskv-cli --addr node1:7700 dump > node1-dump.json
aegiskv-cli --addr node2:7700 dump > node2-dump.json
aegiskv-cli --addr node3:7700 dump > node3-dump.json
```

### Circuit Breaker Trip

If a downstream service is causing cascade failures:

```bash
# Enable circuit breaker for specific service
curl -X POST localhost:7702/admin/circuit-breaker/trip?service=downstream

# Check circuit breaker status
curl localhost:7702/admin/circuit-breaker/status

# Reset circuit breaker
curl -X POST localhost:7702/admin/circuit-breaker/reset?service=downstream
```

### Rate Limit Emergency

If you're being overwhelmed by traffic:

```bash
# Reduce rate limit temporarily
curl -X POST localhost:7702/admin/rate-limit?ops_per_sec=1000

# Or via SIGHUP with updated config
echo '{"rate_limit": 1000}' > /tmp/emergency-config.json
kill -SIGHUP $(cat /var/run/aegiskv.pid)
```

---

## Contact & Escalation

### On-Call Rotation

- **Primary:** Check PagerDuty schedule
- **Secondary:** Backup on-call engineer
- **Escalation:** Team lead / Engineering manager

### External Resources

- GitHub Issues: https://github.com/baxromumarov/aegisKV/issues
- Documentation: https://github.com/baxromumarov/aegisKV/docs

---

## Appendix: Useful Commands

```bash
# Check cluster status
curl localhost:7702/cluster | jq

# Get node stats
curl localhost:7702/stats | jq

# Force GC (use sparingly)
curl -X POST localhost:7702/admin/gc

# Get goroutine dump (for debugging)
curl localhost:7702/debug/pprof/goroutine?debug=1

# Get heap profile
curl localhost:7702/debug/pprof/heap > heap.prof
go tool pprof heap.prof

# Check open file descriptors
ls -la /proc/$(pgrep aegiskv)/fd | wc -l

# Network connections
ss -tnp | grep aegiskv | wc -l
```

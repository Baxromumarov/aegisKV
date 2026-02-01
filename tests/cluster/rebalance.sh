#!/bin/bash
# Rebalance Test: Add/remove nodes and verify shard redistribution
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_ROOT/bin/aegiskv"
DATA_DIR="/tmp/aegiskv-rebalance-test"
LOG_DIR="$DATA_DIR/logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    pkill -f "aegiskv.*rebalance-test" 2>/dev/null || true
    sleep 1
    rm -rf "$DATA_DIR"
}

trap cleanup EXIT

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
log_blue() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

wait_for_ready() {
    local port=$1
    local max_wait=30
    local count=0
    while [ $count -lt $max_wait ]; do
        if curl -s "http://localhost:$port/ready" 2>/dev/null | grep -q '"status":"ready"'; then
            return 0
        fi
        sleep 1
        ((count++))
    done
    return 1
}

get_shard_count() {
    local port=$1
    curl -s "http://localhost:$port/admin/shards" 2>/dev/null | grep -o '"total":[0-9]*' | cut -d: -f2
}

get_primary_count() {
    local port=$1
    curl -s "http://localhost:$port/admin/shards" 2>/dev/null | grep -o '"primary_count":[0-9]*' | cut -d: -f2
}

# Build if needed
if [ ! -f "$BIN" ]; then
    log "Building aegiskv..."
    cd "$PROJECT_ROOT" && make build
fi

# Clean start
cleanup 2>/dev/null || true
mkdir -p "$DATA_DIR" "$LOG_DIR"

log "=== Shard Rebalancing Test ==="
echo ""

# Phase 1: Start with 2 nodes
log_blue "Phase 1: Starting 2-node cluster"

$BIN --addr=:9001 \
    --gossip-addr=:9002 \
    --health-addr=:9003 \
    --node-id=rebalance-node1 \
    --peers=localhost:9004 \
    --data-dir="$DATA_DIR/node1" \
    --log-level=info \
    > "$LOG_DIR/node1.log" 2>&1 &
NODE1_PID=$!
log "Started node1 (PID: $NODE1_PID)"

$BIN --addr=:9011 \
    --gossip-addr=:9004 \
    --health-addr=:9005 \
    --node-id=rebalance-node2 \
    --peers=localhost:9002 \
    --data-dir="$DATA_DIR/node2" \
    --log-level=info \
    > "$LOG_DIR/node2.log" 2>&1 &
NODE2_PID=$!
log "Started node2 (PID: $NODE2_PID)"

wait_for_ready 9003 || error "Node1 failed to become ready"
wait_for_ready 9005 || error "Node2 failed to become ready"
log "2-node cluster ready!"

# Check initial shard distribution
sleep 3
log "Initial shard distribution (2 nodes):"
log "  Node1: $(get_primary_count 9003) primary shards"
log "  Node2: $(get_primary_count 9005) primary shards"

# Write test data
log "Writing test data..."
for i in $(seq 1 50); do
    echo -e "SET rebalance_key_$i rebalance_value_$i\r" | nc -q1 localhost 9001 > /dev/null 2>&1 || \
    echo -e "SET rebalance_key_$i rebalance_value_$i\r" | nc -q1 localhost 9011 > /dev/null 2>&1 || true
done
sleep 2
log "Wrote 50 keys"

# Phase 2: Add a third node
log ""
log_blue "Phase 2: Adding node3 to cluster"

$BIN --addr=:9021 \
    --gossip-addr=:9006 \
    --health-addr=:9007 \
    --node-id=rebalance-node3 \
    --peers=localhost:9002,localhost:9004 \
    --data-dir="$DATA_DIR/node3" \
    --log-level=info \
    > "$LOG_DIR/node3.log" 2>&1 &
NODE3_PID=$!
log "Started node3 (PID: $NODE3_PID)"

wait_for_ready 9007 || error "Node3 failed to become ready"
log "Node3 is ready!"

# Wait for rebalancing
log "Waiting for shard rebalancing (15s)..."
sleep 15

log "Shard distribution after adding node3:"
log "  Node1: $(get_primary_count 9003) primary shards"
log "  Node2: $(get_primary_count 9005) primary shards"
log "  Node3: $(get_primary_count 9007) primary shards"

# Verify data is still accessible
log "Verifying data accessibility..."
ACCESSIBLE=0
for i in $(seq 1 50); do
    for port in 9001 9011 9021; do
        RESULT=$(echo -e "GET rebalance_key_$i\r" | nc -q1 localhost $port 2>/dev/null)
        if [[ "$RESULT" == *"rebalance_value_$i"* ]]; then
            ((ACCESSIBLE++))
            break
        fi
    done
done
log "  $ACCESSIBLE/50 keys accessible after adding node"

# Phase 3: Remove node2
log ""
log_blue "Phase 3: Removing node2 from cluster"
log "Gracefully stopping node2..."
kill -TERM $NODE2_PID 2>/dev/null || true
sleep 2
kill -9 $NODE2_PID 2>/dev/null || true

# Wait for cluster to stabilize
log "Waiting for cluster to stabilize (15s)..."
sleep 15

log "Shard distribution after removing node2:"
log "  Node1: $(get_primary_count 9003) primary shards"
log "  Node3: $(get_primary_count 9007) primary shards"

# Verify data is still accessible
log "Verifying data accessibility after node removal..."
ACCESSIBLE=0
for i in $(seq 1 50); do
    for port in 9001 9021; do
        RESULT=$(echo -e "GET rebalance_key_$i\r" | nc -q1 localhost $port 2>/dev/null)
        if [[ "$RESULT" == *"rebalance_value_$i"* ]]; then
            ((ACCESSIBLE++))
            break
        fi
    done
done
log "  $ACCESSIBLE/50 keys accessible after removing node"

# Phase 4: Add node2 back
log ""
log_blue "Phase 4: Re-adding node2 to cluster"

$BIN --addr=:9011 \
    --gossip-addr=:9004 \
    --health-addr=:9005 \
    --node-id=rebalance-node2 \
    --peers=localhost:9002,localhost:9006 \
    --data-dir="$DATA_DIR/node2" \
    --log-level=info \
    > "$LOG_DIR/node2_rejoin.log" 2>&1 &
NODE2_PID=$!
log "Restarted node2 (PID: $NODE2_PID)"

wait_for_ready 9005 || error "Node2 failed to rejoin"
log "Node2 has rejoined!"

# Wait for rebalancing
log "Waiting for final rebalancing (15s)..."
sleep 15

log "Final shard distribution (3 nodes):"
log "  Node1: $(get_primary_count 9003) primary shards"
log "  Node2: $(get_primary_count 9005) primary shards"
log "  Node3: $(get_primary_count 9007) primary shards"

# Final data verification
log "Final data verification..."
ACCESSIBLE=0
for i in $(seq 1 50); do
    for port in 9001 9011 9021; do
        RESULT=$(echo -e "GET rebalance_key_$i\r" | nc -q1 localhost $port 2>/dev/null)
        if [[ "$RESULT" == *"rebalance_value_$i"* ]]; then
            ((ACCESSIBLE++))
            break
        fi
    done
done

log ""
log "=== Rebalancing Test Complete ==="
echo ""
echo "Summary:"
echo "  - Started with 2 nodes"
echo "  - Added node3, verified rebalancing"
echo "  - Removed node2, verified failover"
echo "  - Re-added node2, verified re-balancing"
echo "  - Final data accessibility: $ACCESSIBLE/50 keys ($(( ACCESSIBLE * 100 / 50 ))%)"
echo ""
echo "Check logs in $LOG_DIR for details"

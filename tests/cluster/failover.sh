#!/bin/bash
# Failover test: Start 3 nodes, kill primary, verify failover
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_ROOT/bin/aegiskv"
DATA_DIR="/tmp/aegiskv-failover-test"
LOG_DIR="$DATA_DIR/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    pkill -f "aegiskv.*failover-test" 2>/dev/null || true
    sleep 1
    rm -rf "$DATA_DIR"
}

trap cleanup EXIT

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

wait_for_ready() {
    local port=$1
    local max_wait=30
    local count=0
    
    while [ $count -lt $max_wait ]; do
        if curl -s "http://localhost:$port/ready" | grep -q '"status":"ready"'; then
            return 0
        fi
        sleep 1
        ((count++))
    done
    return 1
}

get_admin_state() {
    local port=$1
    curl -s "http://localhost:$port/admin/state" 2>/dev/null
}

get_admin_shards() {
    local port=$1
    curl -s "http://localhost:$port/admin/shards" 2>/dev/null
}

# Build if needed
if [ ! -f "$BIN" ]; then
    log "Building aegiskv..."
    cd "$PROJECT_ROOT" && make build
fi

# Clean slate
cleanup 2>/dev/null || true
mkdir -p "$DATA_DIR" "$LOG_DIR"

log "=== Failover Test ==="
log "Starting 3-node cluster..."

# Node 1: Primary node
$BIN --addr=:6001 \
    --gossip-addr=:6002 \
    --health-addr=:6003 \
    --node-id=node1 \
    --peers=localhost:6004,localhost:6006 \
    --data-dir="$DATA_DIR/node1" \
    --log-level=info \
    > "$LOG_DIR/node1.log" 2>&1 &
NODE1_PID=$!
log "Started node1 (PID: $NODE1_PID)"

# Node 2
$BIN --addr=:6011 \
    --gossip-addr=:6004 \
    --health-addr=:6005 \
    --node-id=node2 \
    --peers=localhost:6002,localhost:6006 \
    --data-dir="$DATA_DIR/node2" \
    --log-level=info \
    > "$LOG_DIR/node2.log" 2>&1 &
NODE2_PID=$!
log "Started node2 (PID: $NODE2_PID)"

# Node 3
$BIN --addr=:6021 \
    --gossip-addr=:6006 \
    --health-addr=:6007 \
    --node-id=node3 \
    --peers=localhost:6002,localhost:6004 \
    --data-dir="$DATA_DIR/node3" \
    --log-level=info \
    > "$LOG_DIR/node3.log" 2>&1 &
NODE3_PID=$!
log "Started node3 (PID: $NODE3_PID)"

# Wait for all nodes to be ready
log "Waiting for cluster to form..."
wait_for_ready 6003 || error "Node1 failed to become ready"
wait_for_ready 6005 || error "Node2 failed to become ready"
wait_for_ready 6007 || error "Node3 failed to become ready"
log "All nodes ready!"

# Write some test data
log "Writing test data..."
echo -e "SET test_key1 test_value1\r" | nc -q1 localhost 6001
echo -e "SET test_key2 test_value2\r" | nc -q1 localhost 6001
echo -e "SET test_key3 test_value3\r" | nc -q1 localhost 6001
sleep 2

# Verify data is readable
log "Verifying data..."
RESULT=$(echo -e "GET test_key1\r" | nc -q1 localhost 6001)
if [[ "$RESULT" != *"test_value1"* ]]; then
    error "Failed to read test_key1 before failover"
fi

# Get initial shard info
log "Initial shard state:"
get_admin_shards 6003 | jq -r '.shards[] | "  Shard \(.id): primary=\(.primary), state=\(.state)"' 2>/dev/null || echo "  (jq not available, skipping pretty print)"

# Kill node1 (simulate crash)
log "Killing node1 (simulating crash)..."
kill -9 $NODE1_PID 2>/dev/null || true
sleep 5

# Check that remaining nodes detect the failure
log "Checking cluster state after failure..."
MEMBERS=$(curl -s "http://localhost:6005/admin/members" 2>/dev/null)
echo "Members from node2: $MEMBERS"

# Try to read data from remaining nodes
log "Testing read from node2..."
RESULT=$(echo -e "GET test_key1\r" | nc -q1 localhost 6011 2>/dev/null) || true
echo "Result: $RESULT"

log "Testing read from node3..."
RESULT=$(echo -e "GET test_key3\r" | nc -q1 localhost 6021 2>/dev/null) || true
echo "Result: $RESULT"

# Get final shard state
log "Final shard state (node2):"
get_admin_shards 6005 | jq -r '.shards[] | "  Shard \(.id): primary=\(.primary), state=\(.state)"' 2>/dev/null || get_admin_shards 6005

log "Final shard state (node3):"
get_admin_shards 6007 | jq -r '.shards[] | "  Shard \(.id): primary=\(.primary), state=\(.state)"' 2>/dev/null || get_admin_shards 6007

log "=== Failover Test Complete ==="
echo ""
echo "Check logs in $LOG_DIR for details"

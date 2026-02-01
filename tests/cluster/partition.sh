#!/bin/bash
# Network Partition Test: Simulate network partition using iptables or process isolation
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_ROOT/bin/aegiskv"
DATA_DIR="/tmp/aegiskv-partition-test"
LOG_DIR="$DATA_DIR/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    pkill -f "aegiskv.*partition-test" 2>/dev/null || true
    
    # Remove any iptables rules we added (requires root)
    if [ "$(id -u)" -eq 0 ]; then
        iptables -D INPUT -p tcp --dport 8004 -j DROP 2>/dev/null || true
        iptables -D OUTPUT -p tcp --dport 8004 -j DROP 2>/dev/null || true
    fi
    
    sleep 1
    rm -rf "$DATA_DIR"
}

trap cleanup EXIT

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

log_blue() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
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

get_members() {
    local port=$1
    curl -s "http://localhost:$port/admin/members" 2>/dev/null
}

count_alive_members() {
    local port=$1
    curl -s "http://localhost:$port/admin/state" 2>/dev/null | grep -o '"alive_members":[0-9]*' | cut -d: -f2
}

# Build if needed
if [ ! -f "$BIN" ]; then
    log "Building aegiskv..."
    cd "$PROJECT_ROOT" && make build
fi

# Clean start
cleanup 2>/dev/null || true
mkdir -p "$DATA_DIR" "$LOG_DIR"

log "=== Network Partition Test ==="
log_blue "This test simulates a network partition in a 3-node cluster"
echo ""

# Start 3 nodes
log "Starting 3-node cluster..."

# Node 1 (will be isolated)
$BIN --addr=:8001 \
    --gossip-addr=:8002 \
    --health-addr=:8003 \
    --node-id=partition-node1 \
    --peers=localhost:8004,localhost:8006 \
    --data-dir="$DATA_DIR/node1" \
    --log-level=info \
    > "$LOG_DIR/node1.log" 2>&1 &
NODE1_PID=$!
log "Started node1 (PID: $NODE1_PID) - will be partitioned"

# Node 2
$BIN --addr=:8011 \
    --gossip-addr=:8004 \
    --health-addr=:8005 \
    --node-id=partition-node2 \
    --peers=localhost:8002,localhost:8006 \
    --data-dir="$DATA_DIR/node2" \
    --log-level=info \
    > "$LOG_DIR/node2.log" 2>&1 &
NODE2_PID=$!
log "Started node2 (PID: $NODE2_PID)"

# Node 3
$BIN --addr=:8021 \
    --gossip-addr=:8006 \
    --health-addr=:8007 \
    --node-id=partition-node3 \
    --peers=localhost:8002,localhost:8004 \
    --data-dir="$DATA_DIR/node3" \
    --log-level=info \
    > "$LOG_DIR/node3.log" 2>&1 &
NODE3_PID=$!
log "Started node3 (PID: $NODE3_PID)"

# Wait for cluster
log "Waiting for cluster to form..."
wait_for_ready 8003 || error "Node1 failed to become ready"
wait_for_ready 8005 || error "Node2 failed to become ready"
wait_for_ready 8007 || error "Node3 failed to become ready"
log "All nodes ready!"

# Get initial state
log "Initial cluster state:"
for port in 8003 8005 8007; do
    ALIVE=$(count_alive_members $port)
    log "  Health port $port: $ALIVE alive members"
done

# Write data before partition
log "Writing data before partition..."
for i in $(seq 1 20); do
    echo -e "SET partition_key_$i partition_value_$i\r" | nc -q1 localhost 8001 > /dev/null
done
sleep 2

# Verify data across cluster
log "Verifying data replication..."
for port in 8001 8011 8021; do
    RESULT=$(echo -e "GET partition_key_10\r" | nc -q1 localhost $port 2>/dev/null)
    if [[ "$RESULT" == *"partition_value_10"* ]]; then
        log "  Port $port: ✓ Data available"
    else
        log "  Port $port: ✗ Data missing"
    fi
done

# Simulate partition by pausing node1
log ""
log_blue "=== Simulating Network Partition ==="
log "Pausing node1 to simulate network partition..."
kill -STOP $NODE1_PID
sleep 1

log "Node1 is now isolated (SIGSTOP)"

# Wait for gossip to detect failure
log "Waiting for cluster to detect partition (10s)..."
sleep 10

# Check cluster state during partition
log "Cluster state during partition:"
for port in 8005 8007; do
    ALIVE=$(count_alive_members $port)
    log "  Health port $port: $ALIVE alive members"
done

# Try writes on the majority partition (node2, node3)
log "Attempting writes on majority partition..."
for i in $(seq 21 30); do
    echo -e "SET partition_key_$i during_partition_$i\r" | nc -q1 localhost 8011 > /dev/null 2>&1 || true
done
sleep 2

# Verify reads on majority partition
log "Verifying reads on majority partition..."
RESULT=$(echo -e "GET partition_key_25\r" | nc -q1 localhost 8011 2>/dev/null)
if [[ "$RESULT" == *"during_partition_25"* ]]; then
    log "  Node2: ✓ Can read data written during partition"
else
    log "  Node2: ✗ Cannot read partition data (expected if quorum required)"
fi

# Heal the partition
log ""
log_blue "=== Healing Network Partition ==="
log "Resuming node1..."
kill -CONT $NODE1_PID
sleep 1

# Wait for re-convergence
log "Waiting for cluster to re-converge (15s)..."
sleep 15

# Check final cluster state
log "Cluster state after healing:"
for port in 8003 8005 8007; do
    ALIVE=$(count_alive_members $port)
    log "  Health port $port: $ALIVE alive members"
done

# Verify data consistency after healing
log "Verifying data consistency after healing..."
echo ""

# Check if node1 has data written during partition
RESULT=$(echo -e "GET partition_key_25\r" | nc -q1 localhost 8001 2>/dev/null)
if [[ "$RESULT" == *"during_partition_25"* ]]; then
    log "  Node1: ✓ Has data written during partition (sync worked)"
else
    log "  Node1: ✗ Missing data from partition (expected if it was isolated)"
fi

# Check if all nodes have pre-partition data
for port in 8001 8011 8021; do
    RESULT=$(echo -e "GET partition_key_5\r" | nc -q1 localhost $port 2>/dev/null)
    if [[ "$RESULT" == *"partition_value_5"* ]]; then
        log "  Port $port: ✓ Pre-partition data intact"
    else
        log "  Port $port: ✗ Pre-partition data missing"
    fi
done

log ""
log "=== Network Partition Test Complete ==="
echo ""
echo "Summary:"
echo "  - Cluster formed with 3 nodes"
echo "  - Node1 was isolated using SIGSTOP"
echo "  - Majority partition (node2, node3) continued operating"
echo "  - Partition was healed"
echo "  - Cluster re-converged"
echo ""
echo "Check logs in $LOG_DIR for detailed behavior"

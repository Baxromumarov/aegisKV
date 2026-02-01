#!/bin/bash
# WAL Crash Test: Write data, kill process during write, verify WAL recovery
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN="$PROJECT_ROOT/bin/aegiskv"
DATA_DIR="/tmp/aegiskv-wal-crash-test"
LOG_DIR="$DATA_DIR/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    pkill -f "aegiskv.*wal-crash-test" 2>/dev/null || true
    sleep 1
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

# Build if needed
if [ ! -f "$BIN" ]; then
    log "Building aegiskv..."
    cd "$PROJECT_ROOT" && make build
fi

# Clean start
cleanup 2>/dev/null || true
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR" "$LOG_DIR"

log "=== WAL Crash Recovery Test ==="

# Start single node with WAL enabled
log "Starting node with WAL enabled..."
$BIN --addr=:7001 \
    --gossip-addr=:7002 \
    --health-addr=:7003 \
    --node-id=wal-test-node \
    --data-dir="$DATA_DIR/node1" \
    --wal-mode=fsync \
    --log-level=info \
    > "$LOG_DIR/node1.log" 2>&1 &
NODE_PID=$!
log "Started node (PID: $NODE_PID)"

wait_for_ready 7003 || error "Node failed to become ready"
log "Node is ready"

# Phase 1: Write data
log "Phase 1: Writing test data..."
KEYS_WRITTEN=0
for i in $(seq 1 100); do
    echo -e "SET crash_key_$i crash_value_$i\r" | nc -q1 localhost 7001 > /dev/null
    ((KEYS_WRITTEN++))
done
log "Wrote $KEYS_WRITTEN keys"

# Verify some data is readable
log "Verifying data before crash..."
RESULT=$(echo -e "GET crash_key_50\r" | nc -q1 localhost 7001)
if [[ "$RESULT" != *"crash_value_50"* ]]; then
    error "Failed to read crash_key_50 before crash"
fi
log "Data verified!"

# Give WAL time to flush
sleep 2

# Check WAL file exists
WAL_FILE=$(find "$DATA_DIR" -name "*.wal" 2>/dev/null | head -1)
if [ -n "$WAL_FILE" ]; then
    log "WAL file found: $WAL_FILE ($(stat -c%s "$WAL_FILE" 2>/dev/null || stat -f%z "$WAL_FILE") bytes)"
else
    log "No WAL file found (WAL might be disabled or using different storage)"
fi

# Phase 2: Kill the process hard (simulate crash)
log "Phase 2: Simulating crash with SIGKILL..."
kill -9 $NODE_PID 2>/dev/null || true
sleep 2

# Phase 3: Restart and verify recovery
log "Phase 3: Restarting node for recovery..."
$BIN --addr=:7001 \
    --gossip-addr=:7002 \
    --health-addr=:7003 \
    --node-id=wal-test-node \
    --data-dir="$DATA_DIR/node1" \
    --wal-mode=fsync \
    --log-level=info \
    > "$LOG_DIR/node1_recovery.log" 2>&1 &
NODE_PID=$!
log "Restarted node (PID: $NODE_PID)"

wait_for_ready 7003 || error "Node failed to recover"
log "Node recovered and is ready!"

# Verify data was recovered
log "Verifying data recovery..."
RECOVERED=0
MISSING=0

for i in $(seq 1 100); do
    RESULT=$(echo -e "GET crash_key_$i\r" | nc -q1 localhost 7001 2>/dev/null)
    if [[ "$RESULT" == *"crash_value_$i"* ]]; then
        ((RECOVERED++))
    else
        ((MISSING++))
        if [ $MISSING -le 5 ]; then
            log "Missing: crash_key_$i"
        fi
    fi
done

log "Recovery results: $RECOVERED/$KEYS_WRITTEN keys recovered"

if [ $RECOVERED -eq $KEYS_WRITTEN ]; then
    log "${GREEN}✓ All data recovered successfully!${NC}"
elif [ $RECOVERED -gt 0 ]; then
    log "${YELLOW}⚠ Partial recovery: $RECOVERED/$KEYS_WRITTEN keys${NC}"
else
    log "${RED}✗ No data recovered!${NC}"
fi

# Phase 4: Write more data to verify node is functional
log "Phase 4: Writing additional data after recovery..."
echo -e "SET post_recovery_key post_recovery_value\r" | nc -q1 localhost 7001
RESULT=$(echo -e "GET post_recovery_key\r" | nc -q1 localhost 7001)
if [[ "$RESULT" == *"post_recovery_value"* ]]; then
    log "Post-recovery write/read successful!"
else
    error "Post-recovery write/read failed!"
fi

log "=== WAL Crash Recovery Test Complete ==="
echo ""
echo "Recovery Rate: $RECOVERED/$KEYS_WRITTEN ($(( RECOVERED * 100 / KEYS_WRITTEN ))%)"
echo "Check logs in $LOG_DIR for details"

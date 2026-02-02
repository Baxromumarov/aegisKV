#!/bin/bash

# Multi-node cluster test script for AegisKV
# This script starts multiple nodes and runs tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/bin/aegis"
NUM_NODES=${1:-5}
BASE_PORT=7700
BASE_GOSSIP_PORT=7800
PIDS=()

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    rm -rf /tmp/aegiskv-test-*
    log_info "Cleanup complete"
}

trap cleanup EXIT

# Build the binary if needed
if [ ! -f "$BINARY" ]; then
    log_info "Building AegisKV..."
    cd "$PROJECT_DIR" && make build
fi

# Create temp directories
for i in $(seq 0 $((NUM_NODES - 1))); do
    mkdir -p "/tmp/aegiskv-test-$i/wal"
done

log_info "Starting $NUM_NODES nodes..."

# Start the first node (seed node)
SEED_GOSSIP="127.0.0.1:$BASE_GOSSIP_PORT"
log_info "Starting node 0 (seed) on port $BASE_PORT, gossip port $BASE_GOSSIP_PORT..."
$BINARY \
    --node-id="node-0" \
    --bind="127.0.0.1:$BASE_PORT" \
    --gossip="127.0.0.1:$BASE_GOSSIP_PORT" \
    --data-dir="/tmp/aegiskv-test-0" \
    --shards=64 \
    --replication-factor=3 \
    --wal=off \
    > /tmp/aegiskv-test-0/node.log 2>&1 &
PIDS+=($!)

# Give the seed node time to start
sleep 2

# Start remaining nodes
for i in $(seq 1 $((NUM_NODES - 1))); do
    PORT=$((BASE_PORT + i))
    GOSSIP_PORT=$((BASE_GOSSIP_PORT + i))
    
    log_info "Starting node $i on port $PORT, gossip port $GOSSIP_PORT..."
    $BINARY \
        --node-id="node-$i" \
        --bind="127.0.0.1:$PORT" \
        --gossip="127.0.0.1:$GOSSIP_PORT" \
        --addrs="$SEED_GOSSIP" \
        --data-dir="/tmp/aegiskv-test-$i" \
        --shards=64 \
        --replication-factor=3 \
        --wal=off \
        > /tmp/aegiskv-test-$i/node.log 2>&1 &
    PIDS+=($!)
    
    sleep 0.5
done

log_info "Waiting for cluster to stabilize..."
sleep 5

log_info "Cluster started with $NUM_NODES nodes"
log_info "Node addresses:"
for i in $(seq 0 $((NUM_NODES - 1))); do
    PORT=$((BASE_PORT + i))
    echo "  - Node $i: 127.0.0.1:$PORT"
done

echo ""
log_info "Logs are available in /tmp/aegiskv-test-*/node.log"
echo ""
log_info "Press Ctrl+C to stop the cluster"

# Wait for all processes
wait

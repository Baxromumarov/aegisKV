#!/bin/bash

# Docker-based multi-node cluster test for AegisKV
# This script builds, runs, and tests a multi-node cluster using Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
COMPOSE_10_FILE="$PROJECT_DIR/docker-compose.10nodes.yml"
NUM_NODES=${1:-5}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

cleanup() {
    log_info "Cleaning up Docker containers..."
    cd "$PROJECT_DIR"
    if [ "$NUM_NODES" -eq 10 ]; then
        docker-compose -f docker-compose.10nodes.yml down -v 2>/dev/null || true
    else
        docker-compose down -v 2>/dev/null || true
    fi
}

trap cleanup EXIT

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    log_error "Docker is not running. Please start Docker first."
    exit 1
fi

cd "$PROJECT_DIR"

# Select compose file based on number of nodes
if [ "$NUM_NODES" -eq 10 ]; then
    COMPOSE_CMD="docker-compose -f docker-compose.10nodes.yml"
    log_info "Using 10-node cluster configuration"
else
    COMPOSE_CMD="docker-compose"
    log_info "Using 5-node cluster configuration"
fi

# Step 1: Build Docker images
log_step "Building Docker images..."
$COMPOSE_CMD build

# Step 2: Start the cluster
log_step "Starting $NUM_NODES-node cluster..."
$COMPOSE_CMD up -d

# Step 3: Wait for cluster to be ready
log_step "Waiting for cluster to be ready..."
sleep 10

# Check node health
log_step "Checking node health..."
for i in $(seq 1 $NUM_NODES); do
    PORT=$((7700 + (i-1) * 10))
    if nc -z localhost $PORT 2>/dev/null; then
        log_info "Node $i (port $PORT) is healthy"
    else
        log_warn "Node $i (port $PORT) is not responding"
    fi
done

# Step 4: Show cluster status
log_step "Cluster status:"
$COMPOSE_CMD ps

# Step 5: Show logs from seed node
log_step "Recent logs from seed node:"
docker logs aegis-node1 2>&1 | tail -20

echo ""
log_info "Cluster is running!"
echo ""
echo "Node ports:"
for i in $(seq 1 $NUM_NODES); do
    PORT=$((7700 + (i-1) * 10))
    echo "  - Node $i: localhost:$PORT"
done
echo ""
echo "To view logs:  $COMPOSE_CMD logs -f"
echo "To stop:       $COMPOSE_CMD down"
echo ""

# Keep running and show logs
log_info "Showing cluster logs (Ctrl+C to stop)..."
$COMPOSE_CMD logs -f

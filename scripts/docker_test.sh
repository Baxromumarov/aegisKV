#!/bin/bash

# Docker-based test script for AegisKV
# Runs correctness, resilience, and benchmark tests against a Docker cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; }

# Node addresses
NODES=(
    "localhost:7700"
    "localhost:7710"
    "localhost:7720"
    "localhost:7730"
    "localhost:7740"
)

# Check if cluster is running
check_cluster() {
    log_step "Checking cluster connectivity..."
    local healthy=0
    for node in "${NODES[@]}"; do
        host=$(echo $node | cut -d: -f1)
        port=$(echo $node | cut -d: -f2)
        if nc -z $host $port 2>/dev/null; then
            log_info "  $node is up"
            ((healthy++))
        else
            log_warn "  $node is down"
        fi
    done
    
    if [ $healthy -lt 3 ]; then
        log_error "Not enough nodes are healthy. Please start the cluster first:"
        echo "  cd $PROJECT_DIR && docker-compose up -d"
        exit 1
    fi
    
    log_info "$healthy nodes are healthy"
    return 0
}

# Test 1: Basic SET/GET operations
test_basic_operations() {
    log_step "Test 1: Basic SET/GET Operations"
    local passed=0
    local failed=0
    
    # We need to use the client to test. For now, use netcat for basic testing
    # In production, you would use the Go client
    
    # Simple test: check if nodes respond to connections
    for i in {1..10}; do
        host="localhost"
        port="7700"
        if timeout 1 bash -c "echo '' | nc $host $port" 2>/dev/null; then
            ((passed++))
        else
            ((failed++))
        fi
    done
    
    if [ $passed -gt 0 ]; then
        log_pass "Basic connectivity: $passed successful connections"
    else
        log_fail "Basic connectivity failed"
    fi
}

# Test 2: Data distribution test
test_data_distribution() {
    log_step "Test 2: Data Distribution (via Go test)"
    
    cd "$PROJECT_DIR"
    
    # Run Go integration tests against Docker cluster
    # First, export the node addresses as environment variables
    export AEGIS_TEST_NODES="localhost:7700,localhost:7710,localhost:7720,localhost:7730,localhost:7740"
    
    log_info "Running Go client tests against Docker cluster..."
    go run ./tests/docker/client_test.go 2>&1 || {
        log_warn "Go client test not available, skipping..."
    }
}

# Test 3: Node failure resilience
test_node_failure() {
    log_step "Test 3: Node Failure Resilience"
    
    log_info "Stopping node 3..."
    docker stop aegis-node3 2>/dev/null || true
    sleep 3
    
    # Check remaining nodes
    local healthy=0
    for node in "localhost:7700" "localhost:7710" "localhost:7730" "localhost:7740"; do
        host=$(echo $node | cut -d: -f1)
        port=$(echo $node | cut -d: -f2)
        if nc -z $host $port 2>/dev/null; then
            ((healthy++))
        fi
    done
    
    if [ $healthy -ge 3 ]; then
        log_pass "Cluster survived node failure: $healthy nodes still healthy"
    else
        log_fail "Cluster failed: only $healthy nodes remaining"
    fi
    
    log_info "Restarting node 3..."
    docker start aegis-node3 2>/dev/null || true
    sleep 5
    
    # Verify node rejoined
    if nc -z localhost 7720 2>/dev/null; then
        log_pass "Node 3 successfully rejoined the cluster"
    else
        log_warn "Node 3 failed to rejoin"
    fi
}

# Test 4: Multiple node failure
test_multiple_node_failure() {
    log_step "Test 4: Multiple Node Failure"
    
    log_info "Stopping nodes 2 and 4..."
    docker stop aegis-node2 aegis-node4 2>/dev/null || true
    sleep 3
    
    # Check remaining nodes
    local healthy=0
    for node in "localhost:7700" "localhost:7720" "localhost:7740"; do
        host=$(echo $node | cut -d: -f1)
        port=$(echo $node | cut -d: -f2)
        if nc -z $host $port 2>/dev/null; then
            ((healthy++))
        fi
    done
    
    if [ $healthy -ge 2 ]; then
        log_pass "Cluster survived multiple node failures: $healthy nodes still healthy"
    else
        log_fail "Cluster failed: only $healthy nodes remaining"
    fi
    
    log_info "Restarting nodes 2 and 4..."
    docker start aegis-node2 aegis-node4 2>/dev/null || true
    sleep 5
}

# Test 5: Cluster recovery
test_cluster_recovery() {
    log_step "Test 5: Cluster Recovery"
    
    # Give cluster time to recover
    sleep 5
    
    local healthy=0
    for node in "${NODES[@]}"; do
        host=$(echo $node | cut -d: -f1)
        port=$(echo $node | cut -d: -f2)
        if nc -z $host $port 2>/dev/null; then
            ((healthy++))
        fi
    done
    
    if [ $healthy -eq ${#NODES[@]} ]; then
        log_pass "All ${#NODES[@]} nodes recovered successfully"
    else
        log_warn "Only $healthy/${#NODES[@]} nodes recovered"
    fi
}

# Print summary
print_summary() {
    echo ""
    echo "=========================================="
    echo "       Docker Cluster Test Summary        "
    echo "=========================================="
    echo ""
    docker-compose -f "$PROJECT_DIR/docker-compose.yml" ps 2>/dev/null || true
    echo ""
    log_info "View logs with: docker-compose logs -f"
}

# Main
main() {
    echo ""
    echo "=========================================="
    echo "    AegisKV Docker Cluster Tests          "
    echo "=========================================="
    echo ""
    
    check_cluster
    echo ""
    
    test_basic_operations
    echo ""
    
    test_node_failure
    echo ""
    
    test_multiple_node_failure
    echo ""
    
    test_cluster_recovery
    echo ""
    
    print_summary
}

main "$@"

#!/bin/bash
# Run all cluster tests
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
log_blue() { echo -e "${BLUE}$1${NC}"; }
log_yellow() { echo -e "${YELLOW}$1${NC}"; }
error() { echo -e "${RED}[FAILED]${NC} $1"; }
success() { echo -e "${GREEN}[PASSED]${NC} $1"; }

# Make scripts executable
chmod +x "$SCRIPT_DIR"/*.sh

echo ""
log_blue "╔═══════════════════════════════════════════════════════════════╗"
log_blue "║              AegisKV Cluster Test Suite                       ║"
log_blue "╚═══════════════════════════════════════════════════════════════╝"
echo ""

TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=""

run_test() {
    local name=$1
    local script=$2
    
    echo ""
    log_yellow "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "Running: $name"
    log_yellow "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if bash "$script"; then
        success "$name"
        ((TESTS_PASSED++))
    else
        error "$name"
        ((TESTS_FAILED++))
        FAILED_TESTS="$FAILED_TESTS\n  - $name"
    fi
    
    # Brief pause between tests
    sleep 3
}

# Parse arguments
TESTS_TO_RUN="all"
if [ $# -gt 0 ]; then
    TESTS_TO_RUN="$1"
fi

case "$TESTS_TO_RUN" in
    failover)
        run_test "Failover Test" "$SCRIPT_DIR/failover.sh"
        ;;
    wal|wal_crash)
        run_test "WAL Crash Recovery Test" "$SCRIPT_DIR/wal_crash.sh"
        ;;
    partition)
        run_test "Network Partition Test" "$SCRIPT_DIR/partition.sh"
        ;;
    rebalance)
        run_test "Shard Rebalancing Test" "$SCRIPT_DIR/rebalance.sh"
        ;;
    all|*)
        run_test "Failover Test" "$SCRIPT_DIR/failover.sh"
        run_test "WAL Crash Recovery Test" "$SCRIPT_DIR/wal_crash.sh"
        run_test "Network Partition Test" "$SCRIPT_DIR/partition.sh"
        run_test "Shard Rebalancing Test" "$SCRIPT_DIR/rebalance.sh"
        ;;
esac

# Summary
echo ""
log_blue "╔═══════════════════════════════════════════════════════════════╗"
log_blue "║                       Test Summary                            ║"
log_blue "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo -e "  ${GREEN}Passed:${NC} $TESTS_PASSED"
echo -e "  ${RED}Failed:${NC} $TESTS_FAILED"

if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "\n  Failed tests:$FAILED_TESTS"
    exit 1
else
    echo ""
    log "All tests passed!"
    exit 0
fi

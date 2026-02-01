#!/bin/bash
# AegisKV Load Test Report Generator
# This script runs comprehensive load tests and generates a performance report.

set -e

cd "$(dirname "$0")/../.."

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║                     AegisKV Performance Benchmark Suite                      ║"
echo "║                         $(date '+%Y-%m-%d %H:%M:%S')                             ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Build first
echo "Building AegisKV..."
make build > /dev/null 2>&1
echo "✓ Build complete"
echo ""

# Results file
RESULTS_FILE="benchmark_results_$(date '+%Y%m%d_%H%M%S').txt"

run_test() {
    local name="$1"
    local test_name="$2"
    local timeout="${3:-300s}"
    
    echo "Running: $name..."
    go test -v -timeout "$timeout" ./tests/loadtest/... -run "$test_name" 2>&1 | tee -a "$RESULTS_FILE"
    echo ""
}

echo "═══════════════════════════════════════════════════════════════════════════════" | tee "$RESULTS_FILE"
echo "                          AegisKV Benchmark Results" | tee -a "$RESULTS_FILE"
echo "                          $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$RESULTS_FILE"
echo "═══════════════════════════════════════════════════════════════════════════════" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

# Single node test
run_test "Single Node Load Test" "TestAegisLoadSingle" "180s"

# Cluster test
run_test "3-Node Cluster Load Test" "TestAegisLoadCluster" "180s"

# Scalability test
run_test "Scalability Test (varying concurrency)" "TestAegisScalability" "300s"

# Value size test
run_test "Value Size Impact Test" "TestValueSizeImpact" "300s"

# Read/Write ratio test
run_test "Read/Write Ratio Test" "TestReadWriteRatioImpact" "300s"

echo ""
echo "═══════════════════════════════════════════════════════════════════════════════" | tee -a "$RESULTS_FILE"
echo "                              Benchmark Complete" | tee -a "$RESULTS_FILE"
echo "═══════════════════════════════════════════════════════════════════════════════" | tee -a "$RESULTS_FILE"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""

# Summary
echo "Quick Summary:"
echo "─────────────────────────────────────────────────────────────────────────────────"
grep -E "(Ops/sec:|P99 Latency:|Throughput:)" "$RESULTS_FILE" | head -20
echo ""

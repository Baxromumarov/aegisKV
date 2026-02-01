#!/bin/sh
# Test script to run inside Docker network

set -e

echo "=== AegisKV Docker Cluster Test ==="
echo ""

# Test 1: Basic connectivity test using netcat
echo "1. Testing TCP connectivity to all nodes..."
for port in 7700 7710 7720 7730 7740; do
    if nc -z localhost $port 2>/dev/null; then
        echo "   ✅ localhost:$port - connected"
    else
        echo "   ❌ localhost:$port - failed"
    fi
done
echo ""

# Test 2: Basic SET operation using raw protocol
echo "2. Testing SET operation on node1..."
{
    printf "*3\r\n"
    printf "\$3\r\nSET\r\n"
    printf "\$8\r\ntest-key\r\n"
    printf "\$10\r\ntest-value\r\n"
} | nc -w 2 localhost 7700 | head -1

echo ""

# Test 3: Basic GET operation
echo "3. Testing GET operation on node1..."
{
    printf "*2\r\n"
    printf "\$3\r\nGET\r\n"
    printf "\$8\r\ntest-key\r\n"
} | nc -w 2 localhost 7700 | head -1

echo ""

echo "=== Test Complete ==="

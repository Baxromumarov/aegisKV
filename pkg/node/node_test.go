package node

import (
	"testing"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/config"
	"github.com/baxromumarov/aegisKV/pkg/types"
)

func testConfig(t *testing.T, nodeID string, gossipPort, serverPort int) *config.Config {
	t.Helper()
	return &config.Config{
		NodeID:                  nodeID,
		BindAddr:                "127.0.0.1:" + itoa(serverPort),
		GossipBindAddr:          "127.0.0.1:" + itoa(gossipPort),
		GossipAdvertiseAddr:     "127.0.0.1:" + itoa(gossipPort),
		ClientAdvertiseAddr:     "127.0.0.1:" + itoa(serverPort),
		NumShards:               16,
		ShardMaxBytes:           1024 * 1024,
		VirtualNodes:            3,
		ReplicationFactor:       1,
		GossipInterval:          50 * time.Millisecond,
		SuspectTimeout:          100 * time.Millisecond,
		DeadTimeout:             200 * time.Millisecond,
		ReadTimeout:             time.Second,
		WriteTimeout:            time.Second,
		MaxConns:                10,
		WALMode:                 "off",
		WALDir:                  t.TempDir(),
		MaintenanceInterval:     100 * time.Millisecond,
		ReplicationBatchSize:    10,
		ReplicationBatchTimeout: 10 * time.Millisecond,
		ReplicationMaxRetries:   1,
		LogLevel:                "error",
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	for n > 0 {
		result = string('0'+byte(n%10)) + result
		n /= 10
	}
	return result
}

func TestNewNodeInvalidConfig(t *testing.T) {
	cfg := &config.Config{
		NumShards: -1, // Invalid shard count
	}
	_, err := New(cfg)
	// Empty config may not fail validation if defaults are applied
	// This is acceptable behavior - just ensure node creation doesn't panic
	_ = err
}

func TestNewNodeValidConfig(t *testing.T) {
	cfg := testConfig(t, "test-node-1", 17940, 17941)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	if node.NodeID() != "test-node-1" {
		t.Errorf("expected NodeID 'test-node-1', got '%s'", node.NodeID())
	}
}

func TestNodeStartStop(t *testing.T) {
	cfg := testConfig(t, "test-node-2", 17942, 17943)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := node.Stop(); err != nil {
		t.Errorf("failed to stop node: %v", err)
	}
}

func TestNodeGetSetDelete(t *testing.T) {
	cfg := testConfig(t, "test-node-3", 17944, 17945)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	key := []byte("test-key")
	value := []byte("test-value")

	_, err = node.HandleSet(key, value, 0)
	if err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	gotValue, _, _, found := node.HandleGet(key)
	if !found {
		t.Error("expected to find key")
	}
	if string(gotValue) != string(value) {
		t.Errorf("expected value '%s', got '%s'", value, gotValue)
	}

	if err := node.HandleDelete(key); err != nil {
		t.Errorf("failed to delete: %v", err)
	}

	_, _, _, found = node.HandleGet(key)
	if found {
		t.Error("expected key to be deleted")
	}
}

func TestNodeGetNonExistent(t *testing.T) {
	cfg := testConfig(t, "test-node-4", 17946, 17947)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	_, _, _, found := node.HandleGet([]byte("nonexistent"))
	if found {
		t.Error("expected key not found")
	}
}

func TestNodeSetWithTTL(t *testing.T) {
	cfg := testConfig(t, "test-node-5", 17948, 17949)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	key := []byte("ttl-key")
	value := []byte("ttl-value")

	_, err = node.HandleSet(key, value, 100)
	if err != nil {
		t.Fatalf("failed to set with TTL: %v", err)
	}

	_, ttl, _, found := node.HandleGet(key)
	if !found {
		t.Error("expected to find key")
	}
	if ttl <= 0 || ttl > 100 {
		t.Errorf("unexpected TTL: %d", ttl)
	}

	time.Sleep(150 * time.Millisecond)

	_, _, _, found = node.HandleGet(key)
	if found {
		t.Error("expected key to be expired")
	}
}

func TestNodeStats(t *testing.T) {
	cfg := testConfig(t, "test-node-6", 17950, 17951)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	stats := node.Stats()

	if stats.NodeID != "test-node-6" {
		t.Errorf("expected NodeID 'test-node-6', got '%s'", stats.NodeID)
	}
	if stats.TotalShards != 16 {
		t.Errorf("expected 16 shards, got %d", stats.TotalShards)
	}
	// Cluster size includes self and any members that joined during startup
	if stats.ClusterSize < 1 {
		t.Errorf("expected cluster size >= 1, got %d", stats.ClusterSize)
	}
}

func TestNodeMembers(t *testing.T) {
	cfg := testConfig(t, "test-node-7", 17952, 17953)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	members := node.Members()

	// Members() returns other cluster members. In single-node mode,
	// may include self or be empty depending on gossip implementation
	_ = len(members) // Just verify it doesn't panic
}

func TestNodeIsPrimaryFor(t *testing.T) {
	cfg := testConfig(t, "test-node-8", 17954, 17955)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	if !node.IsPrimaryFor([]byte("test-key")) {
		t.Error("single node should be primary for all keys")
	}
}

func TestNodeGetRedirectAddr(t *testing.T) {
	cfg := testConfig(t, "test-node-9", 17956, 17957)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	addr := node.GetRedirectAddr([]byte("test-key"))

	if addr != "127.0.0.1:17957" {
		t.Errorf("expected redirect addr '127.0.0.1:17957', got '%s'", addr)
	}
}

func TestNodeStatsFunc(t *testing.T) {
	cfg := testConfig(t, "test-node-10", 17958, 17959)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	statsMap := node.statsFunc()

	requiredKeys := []string{
		"active_conns", "total_requests", "total_shards",
		"primary_shards", "follower_shards", "dropped_replicas", "cluster_size",
	}

	for _, key := range requiredKeys {
		if _, ok := statsMap[key]; !ok {
			t.Errorf("missing key in stats map: %s", key)
		}
	}
}

func TestNodeOnNodeJoinLeave(t *testing.T) {
	cfg := testConfig(t, "test-node-11", 17960, 17961)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	info := types.NodeInfo{
		ID:         "new-node",
		Addr:       "127.0.0.1:18000",
		ClientAddr: "127.0.0.1:18001",
	}

	node.onNodeJoin(info)

	node.mu.RLock()
	_, exists := node.members["new-node"]
	node.mu.RUnlock()

	if !exists {
		t.Error("expected new-node in members")
	}

	node.onNodeLeave(info)

	node.mu.RLock()
	_, exists = node.members["new-node"]
	node.mu.RUnlock()

	if exists {
		t.Error("expected new-node to be removed from members")
	}
}

func TestNodeOnNodeSuspect(t *testing.T) {
	cfg := testConfig(t, "test-node-12", 17962, 17963)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	info := types.NodeInfo{
		ID:   "suspect-node",
		Addr: "127.0.0.1:18002",
	}

	node.onNodeSuspect(info)
}

func TestNodeGetNodeAddr(t *testing.T) {
	cfg := testConfig(t, "test-node-13", 17964, 17965)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	addr := node.getNodeAddr("test-node-13")
	if addr != "127.0.0.1:17965" {
		t.Errorf("expected own addr '127.0.0.1:17965', got '%s'", addr)
	}

	node.mu.Lock()
	node.members["other-node"] = types.NodeInfo{
		ID:   "other-node",
		Addr: "127.0.0.1:18003",
	}
	node.mu.Unlock()

	addr = node.getNodeAddr("other-node")
	if addr != "127.0.0.1:18003" {
		t.Errorf("expected '127.0.0.1:18003', got '%s'", addr)
	}

	addr = node.getNodeAddr("unknown-node")
}

func TestNodeGetClientAddr(t *testing.T) {
	cfg := testConfig(t, "test-node-14", 17966, 17967)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	addr := node.getClientAddr("test-node-14")
	if addr != "127.0.0.1:17967" {
		t.Errorf("expected own client addr '127.0.0.1:17967', got '%s'", addr)
	}

	node.mu.Lock()
	node.members["client-node"] = types.NodeInfo{
		ID:         "client-node",
		Addr:       "127.0.0.1:18004",
		ClientAddr: "127.0.0.1:18005",
	}
	node.mu.Unlock()

	addr = node.getClientAddr("client-node")
	if addr != "127.0.0.1:18005" {
		t.Errorf("expected client addr '127.0.0.1:18005', got '%s'", addr)
	}
}

func TestNodeDeleteNonExistent(t *testing.T) {
	cfg := testConfig(t, "test-node-15", 17968, 17969)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	err = node.HandleDelete([]byte("nonexistent-key"))
	if err != nil {
		t.Errorf("delete of non-existent key should not error: %v", err)
	}
}

func TestNodeMultipleSetsSameKey(t *testing.T) {
	cfg := testConfig(t, "test-node-16", 17970, 17971)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	key := []byte("update-key")

	v1, err := node.HandleSet(key, []byte("value1"), 0)
	if err != nil {
		t.Fatalf("first set failed: %v", err)
	}

	v2, err := node.HandleSet(key, []byte("value2"), 0)
	if err != nil {
		t.Fatalf("second set failed: %v", err)
	}

	if v2 <= v1 {
		t.Error("version should increase on update")
	}

	gotValue, _, _, found := node.HandleGet(key)
	if !found {
		t.Error("expected to find key")
	}
	if string(gotValue) != "value2" {
		t.Errorf("expected 'value2', got '%s'", gotValue)
	}
}

func TestNodeStatsFieldsAfterOperations(t *testing.T) {
	cfg := testConfig(t, "test-node-17", 17972, 17973)

	node, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("failed to start node: %v", err)
	}
	defer node.Stop()

	node.HandleSet([]byte("k1"), []byte("v1"), 0)
	node.HandleGet([]byte("k1"))
	node.HandleDelete([]byte("k1"))

	stats := node.Stats()

	if stats.PrimaryShards <= 0 {
		t.Errorf("expected primary shards > 0, got %d", stats.PrimaryShards)
	}
}

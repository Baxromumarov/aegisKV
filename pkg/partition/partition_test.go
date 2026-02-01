package partition

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New returned nil")
	}
	defer s.Close()

	if s.partitions == nil {
		t.Error("partitions map not initialized")
	}
	if s.delays == nil {
		t.Error("delays map not initialized")
	}
	if s.dropRate == nil {
		t.Error("dropRate map not initialized")
	}
}

func TestCreatePartition(t *testing.T) {
	s := New()
	defer s.Close()

	nodeA := "node1"
	nodeB := "node2"

	// Initially not partitioned
	if s.IsPartitioned(nodeA, nodeB) {
		t.Error("nodes should not be partitioned initially")
	}

	// Create partition
	s.CreatePartition(nodeA, nodeB)

	// Now should be partitioned (bidirectional)
	if !s.IsPartitioned(nodeA, nodeB) {
		t.Error("nodes should be partitioned A->B")
	}
	if !s.IsPartitioned(nodeB, nodeA) {
		t.Error("nodes should be partitioned B->A")
	}
}

func TestHealPartition(t *testing.T) {
	s := New()
	defer s.Close()

	nodeA := "node1"
	nodeB := "node2"

	s.CreatePartition(nodeA, nodeB)
	s.HealPartition(nodeA, nodeB)

	if s.IsPartitioned(nodeA, nodeB) {
		t.Error("nodes should not be partitioned after heal")
	}
	if s.IsPartitioned(nodeB, nodeA) {
		t.Error("nodes should not be partitioned after heal (reverse)")
	}
}

func TestHealAll(t *testing.T) {
	s := New()
	defer s.Close()

	// Create multiple partitions
	s.CreatePartition("node1", "node2")
	s.CreatePartition("node2", "node3")
	s.CreatePartition("node1", "node3")

	// Heal all
	s.HealAll()

	if s.IsPartitioned("node1", "node2") {
		t.Error("node1-node2 should not be partitioned")
	}
	if s.IsPartitioned("node2", "node3") {
		t.Error("node2-node3 should not be partitioned")
	}
	if s.IsPartitioned("node1", "node3") {
		t.Error("node1-node3 should not be partitioned")
	}
}

func TestSetDelay(t *testing.T) {
	s := New()
	defer s.Close()

	nodeA := "node1"
	nodeB := "node2"
	delay := 100 * time.Millisecond

	// Initially no delay
	if s.GetDelay(nodeA, nodeB) != 0 {
		t.Error("delay should be 0 initially")
	}

	// Set delay
	s.SetDelay(nodeA, nodeB, delay)

	if s.GetDelay(nodeA, nodeB) != delay {
		t.Errorf("expected delay %v, got %v", delay, s.GetDelay(nodeA, nodeB))
	}

	// Delay is directional, reverse should be 0
	if s.GetDelay(nodeB, nodeA) != 0 {
		t.Error("reverse delay should be 0")
	}
}

func TestClearDelay(t *testing.T) {
	s := New()
	defer s.Close()

	nodeA := "node1"
	nodeB := "node2"

	s.SetDelay(nodeA, nodeB, 100*time.Millisecond)
	s.ClearDelay(nodeA, nodeB)

	if s.GetDelay(nodeA, nodeB) != 0 {
		t.Error("delay should be 0 after clear")
	}
}

func TestSetDropRate(t *testing.T) {
	s := New()
	defer s.Close()

	nodeA := "node1"
	nodeB := "node2"
	rate := 0.25

	// Initially no drops
	if s.GetDropRate(nodeA, nodeB) != 0 {
		t.Error("drop rate should be 0 initially")
	}

	// Set drop rate
	s.SetDropRate(nodeA, nodeB, rate)

	if s.GetDropRate(nodeA, nodeB) != rate {
		t.Errorf("expected drop rate %v, got %v", rate, s.GetDropRate(nodeA, nodeB))
	}

	// Drop rate is directional
	if s.GetDropRate(nodeB, nodeA) != 0 {
		t.Error("reverse drop rate should be 0")
	}
}

func TestClearDropRate(t *testing.T) {
	s := New()
	defer s.Close()

	nodeA := "node1"
	nodeB := "node2"

	s.SetDropRate(nodeA, nodeB, 0.5)
	s.ClearDropRate(nodeA, nodeB)

	if s.GetDropRate(nodeA, nodeB) != 0 {
		t.Error("drop rate should be 0 after clear")
	}
}

func TestScenarioNone(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2", "node3"}

	// Create some partitions
	s.CreatePartition("node1", "node2")

	// Apply none scenario
	err := s.ApplyScenario(ScenarioNone, nodes)
	if err != nil {
		t.Fatalf("ApplyScenario failed: %v", err)
	}

	if s.IsPartitioned("node1", "node2") {
		t.Error("partitions should be cleared")
	}
}

func TestScenarioSplitBrain(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2", "node3", "node4"}

	err := s.ApplyScenario(ScenarioSplitBrain, nodes)
	if err != nil {
		t.Fatalf("ApplyScenario failed: %v", err)
	}

	// First half (node1, node2) should be partitioned from second half (node3, node4)
	if !s.IsPartitioned("node1", "node3") {
		t.Error("node1-node3 should be partitioned")
	}
	if !s.IsPartitioned("node1", "node4") {
		t.Error("node1-node4 should be partitioned")
	}
	if !s.IsPartitioned("node2", "node3") {
		t.Error("node2-node3 should be partitioned")
	}
	if !s.IsPartitioned("node2", "node4") {
		t.Error("node2-node4 should be partitioned")
	}

	// Within each group should not be partitioned
	if s.IsPartitioned("node1", "node2") {
		t.Error("node1-node2 should NOT be partitioned")
	}
	if s.IsPartitioned("node3", "node4") {
		t.Error("node3-node4 should NOT be partitioned")
	}
}

func TestScenarioSplitBrainInsufficientNodes(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1"}

	err := s.ApplyScenario(ScenarioSplitBrain, nodes)
	if err == nil {
		t.Error("expected error for insufficient nodes")
	}
}

func TestScenarioIsolateLeader(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"leader", "node2", "node3", "node4"}

	err := s.ApplyScenario(ScenarioIsolateLeader, nodes)
	if err != nil {
		t.Fatalf("ApplyScenario failed: %v", err)
	}

	// Leader should be partitioned from everyone
	if !s.IsPartitioned("leader", "node2") {
		t.Error("leader-node2 should be partitioned")
	}
	if !s.IsPartitioned("leader", "node3") {
		t.Error("leader-node3 should be partitioned")
	}
	if !s.IsPartitioned("leader", "node4") {
		t.Error("leader-node4 should be partitioned")
	}

	// Other nodes should communicate fine
	if s.IsPartitioned("node2", "node3") {
		t.Error("node2-node3 should NOT be partitioned")
	}
	if s.IsPartitioned("node2", "node4") {
		t.Error("node2-node4 should NOT be partitioned")
	}
}

func TestScenarioAsymmetric(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2"}

	err := s.ApplyScenario(ScenarioAsymmetric, nodes)
	if err != nil {
		t.Fatalf("ApplyScenario failed: %v", err)
	}

	// node2 -> node1 is blocked
	s.mu.RLock()
	blocked := s.partitions["node2"]["node1"]
	s.mu.RUnlock()

	if !blocked {
		t.Error("node2->node1 should be blocked")
	}

	// node1 -> node2 is NOT blocked
	s.mu.RLock()
	blocked2 := s.partitions["node1"]["node2"]
	s.mu.RUnlock()

	if blocked2 {
		t.Error("node1->node2 should NOT be blocked")
	}
}

func TestScenarioSlowNetwork(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2", "node3"}

	err := s.ApplyScenario(ScenarioSlowNetwork, nodes)
	if err != nil {
		t.Fatalf("ApplyScenario failed: %v", err)
	}

	expectedDelay := 100 * time.Millisecond

	if s.GetDelay("node1", "node2") != expectedDelay {
		t.Error("node1->node2 should have 100ms delay")
	}
	if s.GetDelay("node2", "node1") != expectedDelay {
		t.Error("node2->node1 should have 100ms delay")
	}
	if s.GetDelay("node1", "node3") != expectedDelay {
		t.Error("node1->node3 should have 100ms delay")
	}
}

func TestScenarioPacketLoss(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2", "node3"}

	err := s.ApplyScenario(ScenarioPacketLoss, nodes)
	if err != nil {
		t.Fatalf("ApplyScenario failed: %v", err)
	}

	expectedRate := 0.1

	if s.GetDropRate("node1", "node2") != expectedRate {
		t.Error("node1->node2 should have 10% drop rate")
	}
	if s.GetDropRate("node2", "node1") != expectedRate {
		t.Error("node2->node1 should have 10% drop rate")
	}
}

func TestScenarioUnknown(t *testing.T) {
	s := New()
	defer s.Close()

	err := s.ApplyScenario("unknown-scenario", []string{"node1"})
	if err == nil {
		t.Error("expected error for unknown scenario")
	}
}

func TestStats(t *testing.T) {
	s := New()
	defer s.Close()

	// Initially empty
	stats := s.Stats()
	if stats.PartitionCount != 0 {
		t.Errorf("expected 0 partitions, got %d", stats.PartitionCount)
	}

	// Add some partitions and delays
	s.CreatePartition("node1", "node2")
	s.CreatePartition("node2", "node3")
	s.SetDelay("node1", "node3", 50*time.Millisecond)
	s.SetDropRate("node2", "node3", 0.05)

	stats = s.Stats()
	if stats.PartitionCount != 2 {
		t.Errorf("expected 2 partitions, got %d", stats.PartitionCount)
	}
	if stats.DelayCount != 1 {
		t.Errorf("expected 1 delay, got %d", stats.DelayCount)
	}
	if stats.DropRateCount != 1 {
		t.Errorf("expected 1 drop rate, got %d", stats.DropRateCount)
	}
}

func TestDiagnose(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2", "node3"}

	s.CreatePartition("node1", "node2")
	s.SetDelay("node2", "node3", 100*time.Millisecond)

	diagnosis := s.Diagnose(nodes)

	// Should have 2 issues
	if len(diagnosis) != 2 {
		t.Errorf("expected 2 diagnoses, got %d", len(diagnosis))
	}

	// Check for partition
	found := false
	for _, d := range diagnosis {
		if (d.From == "node1" && d.To == "node2") || (d.From == "node2" && d.To == "node1") {
			if d.Partitioned {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected to find node1-node2 partition in diagnosis")
	}

	// Check for delay
	found = false
	for _, d := range diagnosis {
		if d.From == "node2" && d.To == "node3" {
			if d.Delay == 100*time.Millisecond {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected to find node2-node3 delay in diagnosis")
	}
}

func TestDiagnoseEmpty(t *testing.T) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2", "node3"}

	diagnosis := s.Diagnose(nodes)

	if len(diagnosis) != 0 {
		t.Errorf("expected 0 diagnoses for healthy network, got %d", len(diagnosis))
	}
}

func TestMultiplePartitions(t *testing.T) {
	s := New()
	defer s.Close()

	// Create mesh of 5 nodes
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}

	// Partition first 2 from last 3
	s.CreatePartition("node1", "node3")
	s.CreatePartition("node1", "node4")
	s.CreatePartition("node1", "node5")
	s.CreatePartition("node2", "node3")
	s.CreatePartition("node2", "node4")
	s.CreatePartition("node2", "node5")

	// Verify
	for _, a := range nodes[:2] {
		for _, b := range nodes[2:] {
			if !s.IsPartitioned(a, b) {
				t.Errorf("%s-%s should be partitioned", a, b)
			}
		}
	}

	// Internal connectivity should be fine
	if s.IsPartitioned("node1", "node2") {
		t.Error("node1-node2 should NOT be partitioned")
	}
	if s.IsPartitioned("node3", "node4") {
		t.Error("node3-node4 should NOT be partitioned")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	defer s.Close()

	done := make(chan bool)

	// Concurrent partitioning
	go func() {
		for i := 0; i < 100; i++ {
			s.CreatePartition("nodeA", "nodeB")
		}
		done <- true
	}()

	// Concurrent healing
	go func() {
		for i := 0; i < 100; i++ {
			s.HealPartition("nodeA", "nodeB")
		}
		done <- true
	}()

	// Concurrent checks
	go func() {
		for i := 0; i < 100; i++ {
			s.IsPartitioned("nodeA", "nodeB")
		}
		done <- true
	}()

	// Concurrent delays
	go func() {
		for i := 0; i < 100; i++ {
			s.SetDelay("nodeA", "nodeC", time.Millisecond)
			s.GetDelay("nodeA", "nodeC")
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 4; i++ {
		<-done
	}
}

func TestPartitionWithQuorum(t *testing.T) {
	s := New()
	defer s.Close()

	// 5-node cluster, need 3 for quorum
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}

	// Split: [node1, node2] vs [node3, node4, node5]
	for _, a := range nodes[:2] {
		for _, b := range nodes[2:] {
			s.CreatePartition(a, b)
		}
	}

	// Count reachable nodes from each perspective
	countReachable := func(from string, all []string) int {
		count := 1 // Self
		for _, to := range all {
			if from != to && !s.IsPartitioned(from, to) {
				count++
			}
		}
		return count
	}

	// node1 can reach 2 nodes (itself + node2) - no quorum
	if reach := countReachable("node1", nodes); reach != 2 {
		t.Errorf("node1 should reach 2 nodes, got %d", reach)
	}

	// node3 can reach 3 nodes (itself + node4 + node5) - has quorum
	if reach := countReachable("node3", nodes); reach != 3 {
		t.Errorf("node3 should reach 3 nodes, got %d", reach)
	}
}

func BenchmarkCreatePartition(b *testing.B) {
	s := New()
	defer s.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.CreatePartition("nodeA", "nodeB")
	}
}

func BenchmarkIsPartitioned(b *testing.B) {
	s := New()
	defer s.Close()

	s.CreatePartition("nodeA", "nodeB")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.IsPartitioned("nodeA", "nodeB")
	}
}

func BenchmarkApplyScenario(b *testing.B) {
	s := New()
	defer s.Close()

	nodes := []string{"node1", "node2", "node3", "node4", "node5"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ApplyScenario(ScenarioSplitBrain, nodes)
		s.HealAll()
	}
}

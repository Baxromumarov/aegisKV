// Package partition provides network partition simulation for testing.
// It can simulate various network failure scenarios to test cluster resilience.
package partition

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Simulator simulates network partitions between cluster nodes.
type Simulator struct {
	mu         sync.RWMutex
	partitions map[string]map[string]bool // from -> to -> blocked
	delays     map[string]map[string]time.Duration
	dropRate   map[string]map[string]float64
	listeners  []net.Listener
	proxies    map[string]*proxy
	stopped    bool
}

// proxy wraps a connection to add artificial delays and drops.
type proxy struct {
	listener net.Listener
	target   string
	sim      *Simulator
	from     string
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

// New creates a new partition simulator.
func New() *Simulator {
	return &Simulator{
		partitions: make(map[string]map[string]bool),
		delays:     make(map[string]map[string]time.Duration),
		dropRate:   make(map[string]map[string]float64),
		proxies:    make(map[string]*proxy),
	}
}

// Close stops all proxies and cleans up.
func (s *Simulator) Close() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()

	for _, p := range s.proxies {
		p.Close()
	}
	for _, l := range s.listeners {
		l.Close()
	}
}

// CreatePartition blocks traffic between two nodes.
func (s *Simulator) CreatePartition(nodeA, nodeB string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.partitions[nodeA] == nil {
		s.partitions[nodeA] = make(map[string]bool)
	}
	if s.partitions[nodeB] == nil {
		s.partitions[nodeB] = make(map[string]bool)
	}

	s.partitions[nodeA][nodeB] = true
	s.partitions[nodeB][nodeA] = true
}

// HealPartition allows traffic between two nodes again.
func (s *Simulator) HealPartition(nodeA, nodeB string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.partitions[nodeA] != nil {
		delete(s.partitions[nodeA], nodeB)
	}
	if s.partitions[nodeB] != nil {
		delete(s.partitions[nodeB], nodeA)
	}
}

// HealAll removes all partitions.
func (s *Simulator) HealAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.partitions = make(map[string]map[string]bool)
}

// IsPartitioned returns true if the two nodes are partitioned.
func (s *Simulator) IsPartitioned(nodeA, nodeB string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.partitions[nodeA] != nil {
		return s.partitions[nodeA][nodeB]
	}
	return false
}

// SetDelay adds artificial delay to traffic between two nodes.
func (s *Simulator) SetDelay(nodeA, nodeB string, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.delays[nodeA] == nil {
		s.delays[nodeA] = make(map[string]time.Duration)
	}
	s.delays[nodeA][nodeB] = delay
}

// ClearDelay removes artificial delay between two nodes.
func (s *Simulator) ClearDelay(nodeA, nodeB string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.delays[nodeA] != nil {
		delete(s.delays[nodeA], nodeB)
	}
}

// GetDelay returns the current delay between two nodes.
func (s *Simulator) GetDelay(from, to string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.delays[from] != nil {
		return s.delays[from][to]
	}
	return 0
}

// SetDropRate sets the packet drop rate between two nodes.
// rate should be between 0.0 (no drops) and 1.0 (all drops).
func (s *Simulator) SetDropRate(nodeA, nodeB string, rate float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dropRate[nodeA] == nil {
		s.dropRate[nodeA] = make(map[string]float64)
	}
	s.dropRate[nodeA][nodeB] = rate
}

// ClearDropRate removes the drop rate between two nodes.
func (s *Simulator) ClearDropRate(nodeA, nodeB string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dropRate[nodeA] != nil {
		delete(s.dropRate[nodeA], nodeB)
	}
}

// GetDropRate returns the current drop rate between two nodes.
func (s *Simulator) GetDropRate(from, to string) float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.dropRate[from] != nil {
		return s.dropRate[from][to]
	}
	return 0
}

// Close stops the proxy.
func (p *proxy) Close() {
	close(p.stopCh)
	p.listener.Close()
	p.wg.Wait()
}

// Scenario represents a predefined partition scenario.
type Scenario string

const (
	// ScenarioNone - no partition
	ScenarioNone Scenario = "none"

	// ScenarioSplitBrain - cluster split into two groups
	ScenarioSplitBrain Scenario = "split-brain"

	// ScenarioIsolateLeader - isolate a single node (e.g., leader)
	ScenarioIsolateLeader Scenario = "isolate-leader"

	// ScenarioAsymmetric - one-way partition
	ScenarioAsymmetric Scenario = "asymmetric"

	// ScenarioFlapping - intermittent connectivity
	ScenarioFlapping Scenario = "flapping"

	// ScenarioSlowNetwork - high latency
	ScenarioSlowNetwork Scenario = "slow-network"

	// ScenarioPacketLoss - lossy network
	ScenarioPacketLoss Scenario = "packet-loss"
)

// ApplyScenario applies a predefined partition scenario.
func (s *Simulator) ApplyScenario(scenario Scenario, nodes []string) error {
	switch scenario {
	case ScenarioNone:
		s.HealAll()
		return nil

	case ScenarioSplitBrain:
		if len(nodes) < 2 {
			return fmt.Errorf("split-brain requires at least 2 nodes")
		}
		// Split into two groups
		mid := len(nodes) / 2
		groupA := nodes[:mid]
		groupB := nodes[mid:]

		for _, a := range groupA {
			for _, b := range groupB {
				s.CreatePartition(a, b)
			}
		}
		return nil

	case ScenarioIsolateLeader:
		if len(nodes) < 2 {
			return fmt.Errorf("isolate-leader requires at least 2 nodes")
		}
		// Isolate first node from all others
		leader := nodes[0]
		for _, other := range nodes[1:] {
			s.CreatePartition(leader, other)
		}
		return nil

	case ScenarioAsymmetric:
		if len(nodes) < 2 {
			return fmt.Errorf("asymmetric requires at least 2 nodes")
		}
		// One-way partition: A can reach B, but B cannot reach A
		s.mu.Lock()
		if s.partitions[nodes[1]] == nil {
			s.partitions[nodes[1]] = make(map[string]bool)
		}
		s.partitions[nodes[1]][nodes[0]] = true
		s.mu.Unlock()
		return nil

	case ScenarioSlowNetwork:
		// Add 100ms delay between all nodes
		for _, a := range nodes {
			for _, b := range nodes {
				if a != b {
					s.SetDelay(a, b, 100*time.Millisecond)
				}
			}
		}
		return nil

	case ScenarioPacketLoss:
		// 10% packet loss between all nodes
		for _, a := range nodes {
			for _, b := range nodes {
				if a != b {
					s.SetDropRate(a, b, 0.1)
				}
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown scenario: %s", scenario)
	}
}

// Stats returns current partition statistics.
func (s *Simulator) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitionCount := 0
	for _, targets := range s.partitions {
		partitionCount += len(targets)
	}

	delayCount := 0
	for _, targets := range s.delays {
		delayCount += len(targets)
	}

	dropCount := 0
	for _, targets := range s.dropRate {
		dropCount += len(targets)
	}

	return Stats{
		PartitionCount: partitionCount / 2, // Bidirectional
		DelayCount:     delayCount,
		DropRateCount:  dropCount,
	}
}

// Stats contains simulator statistics.
type Stats struct {
	PartitionCount int
	DelayCount     int
	DropRateCount  int
}

// Diagnosis contains information about connectivity issues.
type Diagnosis struct {
	From        string
	To          string
	Partitioned bool
	Delay       time.Duration
	DropRate    float64
}

// Diagnose returns connectivity information between nodes.
func (s *Simulator) Diagnose(nodes []string) []Diagnosis {
	var result []Diagnosis

	for i, from := range nodes {
		for j, to := range nodes {
			if i >= j {
				continue
			}

			d := Diagnosis{
				From:        from,
				To:          to,
				Partitioned: s.IsPartitioned(from, to),
				Delay:       s.GetDelay(from, to),
				DropRate:    s.GetDropRate(from, to),
			}

			if d.Partitioned || d.Delay > 0 || d.DropRate > 0 {
				result = append(result, d)
			}
		}
	}

	return result
}

package process

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

// NetworkController manages network partitions between nodes.
// Note: This requires root privileges or CAP_NET_ADMIN capability.
// For testing without root, we provide a mock partition that just tracks state.
type NetworkController struct {
	mu          sync.Mutex
	partitions  map[string]map[string]bool // from -> to -> blocked
	useIPTables bool
}

// NewNetworkController creates a new network controller.
func NewNetworkController() *NetworkController {
	nc := &NetworkController{
		partitions: make(map[string]map[string]bool),
	}

	// Check if we can use iptables
	nc.useIPTables = nc.canUseIPTables()

	return nc
}

// canUseIPTables checks if iptables is available and usable.
func (nc *NetworkController) canUseIPTables() bool {
	cmd := exec.Command("iptables", "-L", "-n")
	err := cmd.Run()
	return err == nil
}

// Partition creates a network partition between two nodes.
// Traffic from nodeA to nodeB will be blocked (one-way).
func (nc *NetworkController) Partition(nodeAAddr, nodeBAddr string) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	if nc.partitions[nodeAAddr] == nil {
		nc.partitions[nodeAAddr] = make(map[string]bool)
	}
	nc.partitions[nodeAAddr][nodeBAddr] = true

	if nc.useIPTables {
		return nc.blockTraffic(nodeAAddr, nodeBAddr)
	}

	return nil
}

// PartitionBidirectional creates a bidirectional partition between two nodes.
func (nc *NetworkController) PartitionBidirectional(nodeAAddr, nodeBAddr string) error {
	if err := nc.Partition(nodeAAddr, nodeBAddr); err != nil {
		return err
	}
	return nc.Partition(nodeBAddr, nodeAAddr)
}

// Heal removes a network partition between two nodes.
func (nc *NetworkController) Heal(nodeAAddr, nodeBAddr string) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	if nc.partitions[nodeAAddr] != nil {
		delete(nc.partitions[nodeAAddr], nodeBAddr)
	}

	if nc.useIPTables {
		return nc.unblockTraffic(nodeAAddr, nodeBAddr)
	}

	return nil
}

// HealAll removes all network partitions.
func (nc *NetworkController) HealAll() error {
	nc.mu.Lock()
	partitionsCopy := make(map[string]map[string]bool)
	for from, toMap := range nc.partitions {
		partitionsCopy[from] = make(map[string]bool)
		for to := range toMap {
			partitionsCopy[from][to] = true
		}
	}
	nc.mu.Unlock()

	var lastErr error
	for from, toMap := range partitionsCopy {
		for to := range toMap {
			if err := nc.Heal(from, to); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

// IsPartitioned returns true if traffic from nodeA to nodeB is blocked.
func (nc *NetworkController) IsPartitioned(nodeAAddr, nodeBAddr string) bool {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	if nc.partitions[nodeAAddr] == nil {
		return false
	}
	return nc.partitions[nodeAAddr][nodeBAddr]
}

// blockTraffic uses iptables to block traffic.
func (nc *NetworkController) blockTraffic(fromAddr, toAddr string) error {
	// Extract IP and port
	fromIP := extractIP(fromAddr)
	toIP := extractIP(toAddr)
	toPort := extractPort(toAddr)

	// Block outgoing traffic from fromIP to toIP:toPort
	cmd := exec.Command("iptables", "-A", "OUTPUT",
		"-s", fromIP,
		"-d", toIP,
		"-p", "tcp",
		"--dport", toPort,
		"-j", "DROP")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iptables block failed: %s: %w", string(output), err)
	}

	// Also block UDP for gossip
	cmd = exec.Command("iptables", "-A", "OUTPUT",
		"-s", fromIP,
		"-d", toIP,
		"-p", "udp",
		"--dport", toPort,
		"-j", "DROP")

	output, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("iptables block (udp) failed: %s: %w", string(output), err)
	}

	return nil
}

// unblockTraffic removes iptables rules.
func (nc *NetworkController) unblockTraffic(fromAddr, toAddr string) error {
	fromIP := extractIP(fromAddr)
	toIP := extractIP(toAddr)
	toPort := extractPort(toAddr)

	// Remove TCP rule
	cmd := exec.Command("iptables", "-D", "OUTPUT",
		"-s", fromIP,
		"-d", toIP,
		"-p", "tcp",
		"--dport", toPort,
		"-j", "DROP")
	cmd.Run() // Ignore errors as rule might not exist

	// Remove UDP rule
	cmd = exec.Command("iptables", "-D", "OUTPUT",
		"-s", fromIP,
		"-d", toIP,
		"-p", "udp",
		"--dport", toPort,
		"-j", "DROP")
	cmd.Run() // Ignore errors

	return nil
}

func extractIP(addr string) string {
	parts := strings.Split(addr, ":")
	if len(parts) >= 1 {
		return parts[0]
	}
	return addr
}

func extractPort(addr string) string {
	parts := strings.Split(addr, ":")
	if len(parts) >= 2 {
		return parts[1]
	}
	return "7700"
}

// PartitionFromCluster partitions a node from the rest of the cluster.
func (nc *NetworkController) PartitionFromCluster(node *Node, otherNodes []*Node) error {
	for _, other := range otherNodes {
		if other.ID == node.ID {
			continue
		}
		// Bidirectional partition
		if err := nc.PartitionBidirectional(node.ClientAddr, other.ClientAddr); err != nil {
			return err
		}
		if err := nc.PartitionBidirectional(node.GossipAddr, other.GossipAddr); err != nil {
			return err
		}
	}
	return nil
}

// PartitionGroups creates a partition between two groups of nodes.
func (nc *NetworkController) PartitionGroups(groupA, groupB []*Node) error {
	for _, a := range groupA {
		for _, b := range groupB {
			if err := nc.PartitionBidirectional(a.ClientAddr, b.ClientAddr); err != nil {
				return err
			}
			if err := nc.PartitionBidirectional(a.GossipAddr, b.GossipAddr); err != nil {
				return err
			}
		}
	}
	return nil
}

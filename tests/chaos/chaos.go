// Package chaos provides chaos testing utilities for AegisKV.
// It randomly injects failures while running continuous workloads.
package chaos

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// FaultType represents a type of fault to inject.
type FaultType int

const (
	FaultKillNode FaultType = iota
	FaultRestartNode
	FaultNetworkLatency
	FaultPacketDrop
	FaultPacketLoss
	FaultSlowDisk
	FaultCPUStress
	FaultPauseNode
)

func (f FaultType) String() string {
	switch f {
	case FaultKillNode:
		return "kill-node"
	case FaultRestartNode:
		return "restart-node"
	case FaultNetworkLatency:
		return "network-latency"
	case FaultPacketDrop:
		return "packet-drop"
	case FaultPacketLoss:
		return "packet-loss"
	case FaultSlowDisk:
		return "slow-disk"
	case FaultCPUStress:
		return "cpu-stress"
	case FaultPauseNode:
		return "pause-node"
	default:
		return "unknown"
	}
}

// FaultEvent represents a fault injection event.
type FaultEvent struct {
	Type      FaultType
	NodeID    string
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Params    map[string]any
}

// ChaosController manages fault injection.
type ChaosController struct {
	mu            sync.Mutex
	nodes         []NodeController
	enabledFaults []FaultType
	minInterval   time.Duration
	maxInterval   time.Duration
	faultDuration time.Duration
	activeFaults  map[string]*FaultEvent
	faultHistory  []*FaultEvent
	stopCh        chan struct{}
	stopped       bool
	wg            sync.WaitGroup
	totalFaults   int64
	onFaultStart  func(*FaultEvent)
	onFaultEnd    func(*FaultEvent)
	hasIptables   bool
	hasTc         bool
	hasStressNg   bool
	minAliveNodes int
}

// NodeController interface for controlling nodes during chaos.
type NodeController interface {
	ID() string
	Kill() error
	Stop() error
	Restart() error
	Pause() error  // SIGSTOP
	Resume() error // SIGCONT
	IsRunning() bool
	PID() int
}

// ChaosConfig holds chaos testing configuration.
type ChaosConfig struct {
	Nodes         []NodeController
	EnabledFaults []FaultType
	MinInterval   time.Duration // Min time between faults (default: 5s)
	MaxInterval   time.Duration // Max time between faults (default: 20s)
	FaultDuration time.Duration // How long faults last (default: 5s)
	MinAliveNodes int           // Minimum nodes to keep alive (default: 1)
	OnFaultStart  func(*FaultEvent)
	OnFaultEnd    func(*FaultEvent)
}

// NewChaosController creates a new chaos controller.
func NewChaosController(cfg ChaosConfig) *ChaosController {
	if cfg.MinInterval == 0 {
		cfg.MinInterval = 5 * time.Second
	}
	if cfg.MaxInterval == 0 {
		cfg.MaxInterval = 20 * time.Second
	}
	if cfg.FaultDuration == 0 {
		cfg.FaultDuration = 5 * time.Second
	}
	if cfg.MinAliveNodes == 0 {
		cfg.MinAliveNodes = 1
	}
	if len(cfg.EnabledFaults) == 0 {
		cfg.EnabledFaults = []FaultType{FaultKillNode, FaultRestartNode}
	}

	c := &ChaosController{
		nodes:         cfg.Nodes,
		enabledFaults: cfg.EnabledFaults,
		minInterval:   cfg.MinInterval,
		maxInterval:   cfg.MaxInterval,
		faultDuration: cfg.FaultDuration,
		minAliveNodes: cfg.MinAliveNodes,
		activeFaults:  make(map[string]*FaultEvent),
		stopCh:        make(chan struct{}),
		onFaultStart:  cfg.OnFaultStart,
		onFaultEnd:    cfg.OnFaultEnd,
	}

	// Check available tools
	c.hasIptables = checkCommand("iptables")
	c.hasTc = checkCommand("tc")
	c.hasStressNg = checkCommand("stress-ng")

	return c
}

// checkCommand checks if a command is available.
func checkCommand(cmd string) bool {
	_, err := exec.LookPath(cmd)
	return err == nil
}

// Start starts the chaos controller.
func (c *ChaosController) Start() {
	c.wg.Add(1)
	go c.chaosLoop()
}

// Stop stops the chaos controller and heals all faults.
func (c *ChaosController) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	c.mu.Unlock()

	close(c.stopCh)
	c.wg.Wait()
	c.healAll()
}

// chaosLoop is the main chaos injection loop.
func (c *ChaosController) chaosLoop() {
	defer c.wg.Done()

	for {
		// Random interval between faults
		interval := c.minInterval + time.Duration(rand.Int63n(int64(c.maxInterval-c.minInterval)))

		select {
		case <-c.stopCh:
			return
		case <-time.After(interval):
			c.injectRandomFault()
		}
	}
}

// injectRandomFault injects a random fault.
func (c *ChaosController) injectRandomFault() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Count running nodes
	runningNodes := c.getRunningNodes()
	if len(runningNodes) <= c.minAliveNodes {
		log.Printf("[CHAOS] Skipping fault - only %d nodes running (min: %d)", len(runningNodes), c.minAliveNodes)
		return
	}

	// Pick random fault type
	faultType := c.enabledFaults[rand.Intn(len(c.enabledFaults))]

	// Pick random node (from running nodes)
	node := runningNodes[rand.Intn(len(runningNodes))]

	// Skip if node already has active fault
	if _, ok := c.activeFaults[node.ID()]; ok {
		return
	}

	event := &FaultEvent{
		Type:      faultType,
		NodeID:    node.ID(),
		StartTime: time.Now(),
		Duration:  c.faultDuration,
		Params:    make(map[string]any),
	}

	// Inject fault
	if err := c.injectFault(node, event); err != nil {
		log.Printf("[CHAOS] Failed to inject %s on %s: %v", faultType, node.ID(), err)
		return
	}

	c.activeFaults[node.ID()] = event
	c.faultHistory = append(c.faultHistory, event)
	atomic.AddInt64(&c.totalFaults, 1)

	log.Printf("[CHAOS] Injected %s on %s (duration: %v)", faultType, node.ID(), c.faultDuration)

	if c.onFaultStart != nil {
		c.onFaultStart(event)
	}

	// Schedule fault recovery
	go c.scheduleFaultRecovery(node, event)
}

// injectFault injects a specific fault on a node.
func (c *ChaosController) injectFault(node NodeController, event *FaultEvent) error {
	switch event.Type {
	case FaultKillNode:
		return node.Kill()

	case FaultRestartNode:
		if err := node.Stop(); err != nil {
			return err
		}
		// Restart happens in recovery
		return nil

	case FaultPauseNode:
		return node.Pause()

	case FaultNetworkLatency:
		if !c.hasTc {
			// Fallback to pause
			return node.Pause()
		}
		return c.addNetworkLatency(node, 100+rand.Intn(400)) // 100-500ms

	case FaultPacketDrop:
		if !c.hasIptables {
			return node.Pause()
		}
		return c.dropPackets(node, 50+rand.Intn(50)) // 50-100% drop

	case FaultPacketLoss:
		if !c.hasTc {
			return node.Pause()
		}
		return c.addPacketLoss(node, 10+rand.Intn(40)) // 10-50% loss

	case FaultSlowDisk:
		if !c.hasStressNg {
			return node.Pause()
		}
		return c.slowDisk(node)

	case FaultCPUStress:
		if !c.hasStressNg {
			return node.Pause()
		}
		return c.stressCPU(node)

	default:
		return fmt.Errorf("unknown fault type: %v", event.Type)
	}
}

// scheduleFaultRecovery schedules recovery from a fault.
func (c *ChaosController) scheduleFaultRecovery(node NodeController, event *FaultEvent) {
	select {
	case <-c.stopCh:
		c.recoverFault(node, event)
		return
	case <-time.After(event.Duration):
		c.recoverFault(node, event)
	}
}

// recoverFault recovers from a fault.
func (c *ChaosController) recoverFault(node NodeController, event *FaultEvent) {
	c.mu.Lock()
	defer c.mu.Unlock()

	event.EndTime = time.Now()

	switch event.Type {
	case FaultKillNode:
		// Retry restart a few times with backoff
		var err error
		for i := 0; i < 3; i++ {
			if i > 0 {
				time.Sleep(time.Duration(i) * 500 * time.Millisecond)
			}
			err = node.Restart()
			if err == nil {
				break
			}
		}
		if err != nil {
			log.Printf("[CHAOS] Failed to restart %s after retries: %v", node.ID(), err)
		}

	case FaultRestartNode:
		if err := node.Restart(); err != nil {
			log.Printf("[CHAOS] Failed to restart %s: %v", node.ID(), err)
		}

	case FaultPauseNode:
		if err := node.Resume(); err != nil {
			log.Printf("[CHAOS] Failed to resume %s: %v", node.ID(), err)
		}

	case FaultNetworkLatency:
		c.clearNetworkLatency(node)

	case FaultPacketDrop:
		c.clearPacketDrop(node)

	case FaultPacketLoss:
		c.clearPacketLoss(node)

	case FaultSlowDisk, FaultCPUStress:
		// stress-ng processes self-terminate
	}

	delete(c.activeFaults, node.ID())

	log.Printf("[CHAOS] Recovered %s on %s", event.Type, node.ID())

	if c.onFaultEnd != nil {
		c.onFaultEnd(event)
	}
}

// healAll heals all active faults.
func (c *ChaosController) healAll() {
	c.mu.Lock()
	faults := make(map[string]*FaultEvent)
	for k, v := range c.activeFaults {
		faults[k] = v
	}
	c.mu.Unlock()

	for _, event := range faults {
		node := c.getNode(event.NodeID)
		if node != nil {
			c.recoverFault(node, event)
		}
	}
}

// getNode returns a node by ID.
func (c *ChaosController) getNode(id string) NodeController {
	for _, n := range c.nodes {
		if n.ID() == id {
			return n
		}
	}
	return nil
}

// getRunningNodes returns currently running nodes.
func (c *ChaosController) getRunningNodes() []NodeController {
	var running []NodeController
	for _, n := range c.nodes {
		if n.IsRunning() {
			running = append(running, n)
		}
	}
	return running
}

// Network fault injection using tc/iptables

func (c *ChaosController) addNetworkLatency(node NodeController, latencyMs int) error {
	// Add latency using tc netem on localhost traffic to node's port
	// This is simplified - in production you'd need proper network namespace handling
	cmd := exec.Command(
		"tc",
		"qdisc",
		"add",
		"dev",
		"lo",
		"root",
		"netem",
		"delay",
		fmt.Sprintf("%dms", latencyMs),
		fmt.Sprintf("%dms", latencyMs/4),
	)
	return cmd.Run()
}

func (c *ChaosController) clearNetworkLatency(node NodeController) {
	exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
}

func (c *ChaosController) dropPackets(node NodeController, percent int) error {
	// Drop packets to node's port using iptables
	// Simplified - would need actual port from node
	return nil // iptables rules are complex, skip for now
}

func (c *ChaosController) clearPacketDrop(node NodeController) {
	// Clear iptables rules
}

func (c *ChaosController) addPacketLoss(node NodeController, percent int) error {
	cmd := exec.Command("tc", "qdisc", "add", "dev", "lo", "root", "netem", "loss",
		fmt.Sprintf("%d%%", percent))
	return cmd.Run()
}

func (c *ChaosController) clearPacketLoss(node NodeController) {
	exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
}

func (c *ChaosController) slowDisk(node NodeController) error {
	// Use stress-ng to stress disk I/O
	ctx, cancel := context.WithTimeout(context.Background(), c.faultDuration)
	defer cancel()
	cmd := exec.CommandContext(ctx, "stress-ng", "--hdd", "1", "--timeout",
		fmt.Sprintf("%ds", int(c.faultDuration.Seconds())))
	return cmd.Start()
}

func (c *ChaosController) stressCPU(node NodeController) error {
	// Use stress-ng to stress CPU
	ctx, cancel := context.WithTimeout(context.Background(), c.faultDuration)
	defer cancel()
	cmd := exec.CommandContext(ctx, "stress-ng", "--cpu", "2", "--timeout",
		fmt.Sprintf("%ds", int(c.faultDuration.Seconds())))
	return cmd.Start()
}

// Stats returns chaos statistics.
func (c *ChaosController) Stats() ChaosStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	faultCounts := make(map[FaultType]int)
	for _, e := range c.faultHistory {
		faultCounts[e.Type]++
	}

	return ChaosStats{
		TotalFaults:  atomic.LoadInt64(&c.totalFaults),
		ActiveFaults: len(c.activeFaults),
		FaultCounts:  faultCounts,
		History:      c.faultHistory,
	}
}

// ChaosStats contains chaos testing statistics.
type ChaosStats struct {
	TotalFaults  int64
	ActiveFaults int
	FaultCounts  map[FaultType]int
	History      []*FaultEvent
}

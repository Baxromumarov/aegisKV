// Package process provides a test harness for running AegisKV nodes as separate OS processes.
// This enables testing of real-world scenarios: crashes, restarts, disk operations, and timing.
package process

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/client"
)

// Node represents a running AegisKV server process.
type Node struct {
	ID            string
	ClientAddr    string
	GossipAddr    string
	DataDir       string
	Cmd           *exec.Cmd
	Stdout        io.ReadCloser
	Stderr        io.ReadCloser
	startTime     time.Time
	mu            sync.Mutex
	running       bool
	exitCh        chan struct{}
	outputLines   []string
	outputMu      sync.RWMutex
	walMode       string
	numShards     int
	replFactor    int
	addrs         []string
	binaryPath    string
	clientTimeout time.Duration
}

// NodeConfig holds configuration for a node.
type NodeConfig struct {
	ID            string
	ClientPort    int
	GossipPort    int
	DataDir       string
	Addrs         []string
	WALMode       string
	NumShards     int
	ReplFactor    int
	BinaryPath    string
	MaxMemoryMB   int64
	ClientTimeout time.Duration
}

// Cluster manages a group of AegisKV nodes running as OS processes.
type Cluster struct {
	mu          sync.RWMutex
	nodes       map[string]*Node
	baseDir     string
	binaryPath  string
	basePort    int
	walMode     string
	numShards   int
	replFactor  int
	maxMemoryMB int64
	partitions  map[string]map[string]bool // node -> blocked nodes
	partitionMu sync.RWMutex
}

// ClusterConfig holds configuration for the cluster.
type ClusterConfig struct {
	BaseDir     string
	BinaryPath  string
	BasePort    int
	WALMode     string
	NumShards   int
	ReplFactor  int
	MaxMemoryMB int64
}

// NewCluster creates a new cluster manager.
func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	if cfg.BinaryPath == "" {
		// Try to find the binary
		cfg.BinaryPath = findBinary()
	}

	if cfg.BaseDir == "" {
		tmpDir, err := os.MkdirTemp("", "aegis-test-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir: %w", err)
		}
		cfg.BaseDir = tmpDir
	}

	if cfg.BasePort == 0 {
		// Use random port base to avoid collisions with previous test runs
		// Range: 20000-50000
		cfg.BasePort = 20000 + rand.Intn(30000)
	}
	if cfg.WALMode == "" {
		cfg.WALMode = "off"
	}
	if cfg.NumShards == 0 {
		cfg.NumShards = 64
	}
	if cfg.ReplFactor == 0 {
		cfg.ReplFactor = 3
	}
	if cfg.MaxMemoryMB == 0 {
		cfg.MaxMemoryMB = 128
	}

	return &Cluster{
		nodes:       make(map[string]*Node),
		baseDir:     cfg.BaseDir,
		binaryPath:  cfg.BinaryPath,
		basePort:    cfg.BasePort,
		walMode:     cfg.WALMode,
		numShards:   cfg.NumShards,
		replFactor:  cfg.ReplFactor,
		maxMemoryMB: cfg.MaxMemoryMB,
		partitions:  make(map[string]map[string]bool),
	}, nil
}

// findBinary attempts to find the aegis binary.
func findBinary() string {
	// Try common locations
	candidates := []string{
		"./bin/aegis",
		"./aegis",
		"../../../bin/aegis",
		"../../bin/aegis",
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			absPath, _ := filepath.Abs(path)
			return absPath
		}
	}

	return "aegis" // Fall back to PATH
}

// StartNode starts a new node as an OS process.
func (c *Cluster) StartNode(id string, addrs []string) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.nodes[id]; exists {
		return nil, fmt.Errorf("node %s already exists", id)
	}

	// Assign ports
	nodeIndex := len(c.nodes)
	clientPort := c.basePort + nodeIndex*10
	gossipPort := clientPort + 1

	// Create data directory
	dataDir := filepath.Join(c.baseDir, id)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}

	node := &Node{
		ID:            id,
		ClientAddr:    fmt.Sprintf("127.0.0.1:%d", clientPort),
		GossipAddr:    fmt.Sprintf("127.0.0.1:%d", gossipPort),
		DataDir:       dataDir,
		binaryPath:    c.binaryPath,
		walMode:       c.walMode,
		numShards:     c.numShards,
		replFactor:    c.replFactor,
		addrs:         addrs,
		exitCh:        make(chan struct{}),
		clientTimeout: 5 * time.Second,
	}

	if err := node.Start(); err != nil {
		return nil, err
	}

	c.nodes[id] = node
	return node, nil
}

// Start starts the node process.
func (n *Node) Start() error {
	n.mu.Lock()

	if n.running {
		n.mu.Unlock()
		return fmt.Errorf("node %s is already running", n.ID)
	}

	args := []string{
		"--node-id=" + n.ID,
		"--bind=" + n.ClientAddr,
		"--gossip=" + n.GossipAddr,
		"--gossip-advertise=" + n.GossipAddr,
		"--client-advertise=" + n.ClientAddr,
		"--data-dir=" + n.DataDir,
		"--shards=" + fmt.Sprintf("%d", n.numShards),
		"--replication-factor=" + fmt.Sprintf("%d", n.replFactor),
		"--wal=" + n.walMode,
	}

	if len(n.addrs) > 0 {
		seedStr := ""
		for i, s := range n.addrs {
			if i > 0 {
				seedStr += ","
			}
			seedStr += s
		}
		args = append(args, "--addrs="+seedStr)
	}

	n.Cmd = exec.Command(n.binaryPath, args...)
	n.Cmd.Dir = n.DataDir

	// Set up process group so we can kill all children
	n.Cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	stdout, err := n.Cmd.StdoutPipe()
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("failed to get stdout: %w", err)
	}
	n.Stdout = stdout

	stderr, err := n.Cmd.StderrPipe()
	if err != nil {
		n.mu.Unlock()
		return fmt.Errorf("failed to get stderr: %w", err)
	}
	n.Stderr = stderr

	if err := n.Cmd.Start(); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("failed to start node %s: %w", n.ID, err)
	}

	n.running = true
	n.startTime = time.Now()
	n.exitCh = make(chan struct{})

	// Capture output in background
	go n.captureOutput(stdout)
	go n.captureOutput(stderr)

	// Monitor process exit
	go func() {
		n.Cmd.Wait()
		n.mu.Lock()
		n.running = false
		n.mu.Unlock()
		close(n.exitCh)
	}()

	// Unlock before waiting - waitReady() needs to check running status
	n.mu.Unlock()

	// Wait for the node to be ready
	return n.waitReady()
}

// waitReady waits for the node to accept connections.
func (n *Node) waitReady() error {
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", n.ClientAddr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			// Give it a bit more time to fully initialize
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		// Check if process died
		n.mu.Lock()
		running := n.running
		n.mu.Unlock()
		if !running {
			// Get last few lines of output for debugging
			output := n.Output()
			lastLines := ""
			if len(output) > 0 {
				start := len(output) - 10
				if start < 0 {
					start = 0
				}
				for _, line := range output[start:] {
					lastLines += "\n  " + line
				}
			}
			return fmt.Errorf("node %s exited unexpectedly, last output:%s", n.ID, lastLines)
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for node %s to become ready", n.ID)
}

// captureOutput captures output from a reader.
func (n *Node) captureOutput(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		n.outputMu.Lock()
		n.outputLines = append(n.outputLines, line)
		// Keep only last 1000 lines
		if len(n.outputLines) > 1000 {
			n.outputLines = n.outputLines[len(n.outputLines)-1000:]
		}
		n.outputMu.Unlock()
	}
}

// Stop stops the node gracefully.
func (n *Node) Stop() error {
	n.mu.Lock()

	if !n.running {
		n.mu.Unlock()
		return nil
	}

	// Send SIGTERM for graceful shutdown
	if err := n.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("failed to send SIGTERM: %w", err)
	}

	exitCh := n.exitCh
	pid := n.Cmd.Process.Pid
	n.mu.Unlock()

	// Wait for process to exit with timeout (without holding lock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	select {
	case <-exitCh:
		// Process exited gracefully
	case <-ctx.Done():
		// Force kill
		syscall.Kill(-pid, syscall.SIGKILL)
		<-exitCh
	}

	return nil
}

// Kill forcefully kills the node (simulates crash).
func (n *Node) Kill() error {
	n.mu.Lock()

	if !n.running {
		n.mu.Unlock()
		return nil
	}

	pid := n.Cmd.Process.Pid
	exitCh := n.exitCh
	n.mu.Unlock()

	// Kill the entire process group (without holding lock)
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("failed to kill node %s: %w", n.ID, err)
	}

	// Wait for process to actually exit
	select {
	case <-exitCh:
		// Process exited
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for node %s to exit after kill", n.ID)
	}

	// Wait longer for OS to clean up file descriptors and sockets
	time.Sleep(500 * time.Millisecond)

	return nil
}

// Restart restarts the node after it's been stopped.
func (n *Node) Restart() error {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		if err := n.Stop(); err != nil {
			return err
		}
	} else {
		n.mu.Unlock()
	}

	// Wait for OS to release ports - try to actually bind to check availability
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		clientFree := isPortAvailable(n.ClientAddr)
		gossipFree := isPortAvailable(n.GossipAddr)

		if clientFree && gossipFree {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Additional safety delay for OS cleanup
	time.Sleep(500 * time.Millisecond)

	return n.Start()
}

// isPortAvailable checks if a port is available by attempting to bind to it.
func isPortAvailable(addr string) bool {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

// IsRunning returns whether the node is running.
func (n *Node) IsRunning() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running
}

// Uptime returns how long the node has been running.
func (n *Node) Uptime() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return 0
	}
	return time.Since(n.startTime)
}

// Output returns the captured output lines.
func (n *Node) Output() []string {
	n.outputMu.RLock()
	defer n.outputMu.RUnlock()
	result := make([]string, len(n.outputLines))
	copy(result, n.outputLines)
	return result
}

// Client returns a client connected to this node.
func (n *Node) Client() *client.Client {
	return client.New(client.Config{
		Addrs:        []string{n.ClientAddr},
		MaxConns:     10,
		ConnTimeout:  n.clientTimeout,
		ReadTimeout:  n.clientTimeout,
		WriteTimeout: n.clientTimeout,
		MaxRetries:   3,
	})
}

// GetNode returns a node by ID.
func (c *Cluster) GetNode(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

// Nodes returns all nodes.
func (c *Cluster) Nodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// RunningNodes returns only running nodes.
func (c *Cluster) RunningNodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := make([]*Node, 0)
	for _, n := range c.nodes {
		if n.IsRunning() {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// StopNode stops a specific node.
func (c *Cluster) StopNode(id string) error {
	c.mu.RLock()
	node, exists := c.nodes[id]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node %s not found", id)
	}
	return node.Stop()
}

// KillNode forcefully kills a node (simulates crash).
func (c *Cluster) KillNode(id string) error {
	c.mu.RLock()
	node, exists := c.nodes[id]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node %s not found", id)
	}
	return node.Kill()
}

// RestartNode restarts a node.
func (c *Cluster) RestartNode(id string) error {
	c.mu.RLock()
	node, exists := c.nodes[id]
	c.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node %s not found", id)
	}
	return node.Restart()
}

// StopAll stops all nodes gracefully.
func (c *Cluster) StopAll() error {
	c.mu.RLock()
	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.RUnlock()

	var lastErr error
	for _, n := range nodes {
		if err := n.Stop(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Cleanup stops all nodes and removes data directories.
func (c *Cluster) Cleanup() error {
	if err := c.StopAll(); err != nil {
		// Continue cleanup even if stop fails
	}

	// Wait a bit for processes to fully exit
	time.Sleep(500 * time.Millisecond)

	// Remove base directory
	return os.RemoveAll(c.baseDir)
}

// Client returns a client connected to all running nodes.
func (c *Cluster) Client() *client.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addrs := make([]string, 0)
	for _, n := range c.nodes {
		if n.IsRunning() {
			addrs = append(addrs, n.ClientAddr)
		}
	}

	return client.New(client.Config{
		Addrs:        addrs,
		MaxConns:     20,
		ConnTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxRetries:   5,
	})
}

// WaitForCluster waits for all nodes to see each other.
func (c *Cluster) WaitForCluster(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	expectedNodes := len(c.RunningNodes())

	for time.Now().Before(deadline) {
		// Just verify all nodes are still running
		running := c.RunningNodes()
		if len(running) >= expectedNodes {
			// Give gossip time to propagate
			time.Sleep(2 * time.Second)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for cluster formation")
}

// StartCluster starts a cluster of N nodes.
func (c *Cluster) StartCluster(numNodes int) error {
	if numNodes < 1 {
		return fmt.Errorf("need at least 1 node")
	}

	// Start first node (seed node)
	seedNode, err := c.StartNode("node-0", nil)
	if err != nil {
		return fmt.Errorf("failed to start seed node: %w", err)
	}

	addrs := []string{seedNode.GossipAddr}

	// Start remaining nodes
	for i := 1; i < numNodes; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		_, err := c.StartNode(nodeID, addrs)
		if err != nil {
			return fmt.Errorf("failed to start node %s: %w", nodeID, err)
		}
	}

	return c.WaitForCluster(30 * time.Second)
}

// BaseDir returns the base directory for the cluster.
func (c *Cluster) BaseDir() string {
	return c.baseDir
}

// BinaryPath returns the path to the aegis binary.
func (c *Cluster) BinaryPath() string {
	return c.binaryPath
}

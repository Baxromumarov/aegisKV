package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/types"
)

// MessageType represents the type of gossip message.
type MessageType uint8

const (
	MessageTypePing MessageType = iota
	MessageTypeAck
	MessageTypePingReq
	MessageTypeSync
	MessageTypeJoin
	MessageTypeLeave
)

// Message represents a gossip protocol message.
type Message struct {
	Type       MessageType      `json:"type"`
	FromNodeID string           `json:"from_node_id"`
	ToNodeID   string           `json:"to_node_id,omitempty"`
	SeqNum     uint64           `json:"seq_num"`
	Members    []types.NodeInfo `json:"members,omitempty"`
	NodeInfo   *types.NodeInfo  `json:"node_info,omitempty"`
	Timestamp  int64            `json:"timestamp"`
}

// memberState tracks the state of a member.
type memberState struct {
	info        types.NodeInfo
	lastSeen    time.Time
	suspectTime time.Time
	incarnation uint64
}

// Gossip implements a SWIM-like gossip protocol for membership.
type Gossip struct {
	mu             sync.RWMutex
	nodeID         string
	bindAddr       string
	advertiseAddr  string
	clientAddr     string // Client-facing address for redirects
	members        map[string]*memberState
	seqNum         uint64
	incarnation    uint64
	conn           *net.UDPConn
	pingInterval   time.Duration
	suspectTimeout time.Duration
	deadTimeout    time.Duration
	onNodeJoin     func(types.NodeInfo)
	onNodeLeave    func(types.NodeInfo)
	onNodeSuspect  func(types.NodeInfo)
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

// Config holds configuration for the gossip protocol.
type Config struct {
	NodeID         string
	BindAddr       string
	AdvertiseAddr  string // Address to advertise to other nodes (for containers/NAT)
	ClientAddr     string // Client-facing address for redirects
	Seeds          []string
	PingInterval   time.Duration
	SuspectTimeout time.Duration
	DeadTimeout    time.Duration
	OnNodeJoin     func(types.NodeInfo)
	OnNodeLeave    func(types.NodeInfo)
	OnNodeSuspect  func(types.NodeInfo)
}

// New creates a new Gossip instance.
func New(cfg Config) (*Gossip, error) {
	if cfg.PingInterval == 0 {
		cfg.PingInterval = time.Second
	}
	if cfg.SuspectTimeout == 0 {
		cfg.SuspectTimeout = 5 * time.Second
	}
	if cfg.DeadTimeout == 0 {
		cfg.DeadTimeout = 10 * time.Second
	}

	// Use advertise address if specified, otherwise use bind address
	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = cfg.BindAddr
	}

	// Use client address if specified
	clientAddr := cfg.ClientAddr
	if clientAddr == "" {
		clientAddr = advertiseAddr // Fall back to gossip address if not specified
	}

	g := &Gossip{
		nodeID:         cfg.NodeID,
		bindAddr:       cfg.BindAddr,
		advertiseAddr:  advertiseAddr,
		clientAddr:     clientAddr,
		members:        make(map[string]*memberState),
		pingInterval:   cfg.PingInterval,
		suspectTimeout: cfg.SuspectTimeout,
		deadTimeout:    cfg.DeadTimeout,
		onNodeJoin:     cfg.OnNodeJoin,
		onNodeLeave:    cfg.OnNodeLeave,
		onNodeSuspect:  cfg.OnNodeSuspect,
		stopCh:         make(chan struct{}),
	}

	return g, nil
}

// Start starts the gossip protocol.
func (g *Gossip) Start() error {
	// Use ListenConfig with SO_REUSEADDR to allow quick restarts
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			if err := c.Control(func(fd uintptr) {
				opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				if opErr == nil {
					// SO_REUSEPORT (15 on Linux) allows binding to same port if previous process crashed
					const SO_REUSEPORT = 15
					syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, SO_REUSEPORT, 1) // Ignore error
				}
			}); err != nil {
				return err
			}
			return opErr
		},
	}

	pc, err := lc.ListenPacket(context.Background(), "udp", g.bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	g.conn = pc.(*net.UDPConn)

	// Add self to member list
	g.mu.Lock()
	g.members[g.nodeID] = &memberState{
		info: types.NodeInfo{
			ID:         g.nodeID,
			Addr:       g.advertiseAddr,
			ClientAddr: g.clientAddr,
			State:      types.NodeStateAlive,
			LastSeen:   time.Now(),
		},
		lastSeen: time.Now(),
	}
	g.mu.Unlock()

	g.wg.Add(2)
	go g.receiveLoop()
	go g.protocolLoop()

	return nil
}

// Stop stops the gossip protocol.
func (g *Gossip) Stop() error {
	close(g.stopCh)
	if g.conn != nil {
		g.conn.Close()
	}
	g.wg.Wait()
	return nil
}

// Join joins the cluster using seed nodes.
func (g *Gossip) Join(seeds []string) error {
	selfInfo := types.NodeInfo{
		ID:         g.nodeID,
		Addr:       g.advertiseAddr,
		ClientAddr: g.clientAddr,
		State:      types.NodeStateAlive,
		LastSeen:   time.Now(),
	}

	msg := &Message{
		Type:       MessageTypeJoin,
		FromNodeID: g.nodeID,
		NodeInfo:   &selfInfo,
		Timestamp:  time.Now().UnixNano(),
	}

	for _, seed := range seeds {
		if seed == g.bindAddr || seed == g.advertiseAddr {
			continue
		}
		if err := g.sendTo(seed, msg); err != nil {
			continue
		}
	}

	return nil
}

// Leave gracefully leaves the cluster.
func (g *Gossip) Leave() error {
	g.mu.Lock()
	g.incarnation++
	g.mu.Unlock()

	msg := &Message{
		Type:       MessageTypeLeave,
		FromNodeID: g.nodeID,
		Timestamp:  time.Now().UnixNano(),
	}

	g.broadcast(msg)
	return nil
}

// Members returns all known members.
func (g *Gossip) Members() []types.NodeInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	members := make([]types.NodeInfo, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, m.info)
	}
	return members
}

// AliveMembers returns all alive members.
func (g *Gossip) AliveMembers() []types.NodeInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	members := make([]types.NodeInfo, 0)
	for _, m := range g.members {
		if m.info.State == types.NodeStateAlive {
			members = append(members, m.info)
		}
	}
	return members
}

// receiveLoop handles incoming UDP messages.
func (g *Gossip) receiveLoop() {
	defer g.wg.Done()

	buf := make([]byte, 65535)
	for {
		select {
		case <-g.stopCh:
			return
		default:
		}

		g.conn.SetReadDeadline(time.Now().Add(time.Second))
		n, addr, err := g.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}

		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		g.handleMessage(&msg, addr)
	}
}

// handleMessage processes an incoming message.
func (g *Gossip) handleMessage(msg *Message, from *net.UDPAddr) {
	switch msg.Type {
	case MessageTypePing:
		g.handlePing(msg, from)
	case MessageTypeAck:
		g.handleAck(msg)
	case MessageTypePingReq:
		g.handlePingReq(msg, from)
	case MessageTypeSync:
		g.handleSync(msg)
	case MessageTypeJoin:
		g.handleJoin(msg, from)
	case MessageTypeLeave:
		g.handleLeave(msg)
	}
}

// handlePing handles PING messages.
func (g *Gossip) handlePing(msg *Message, from *net.UDPAddr) {
	g.mu.Lock()
	g.seqNum++
	seqNum := g.seqNum
	// Get the sender's stored address if known
	var replyAddr string
	if m, ok := g.members[msg.FromNodeID]; ok {
		replyAddr = m.info.Addr
		m.lastSeen = time.Now()
		m.info.State = types.NodeStateAlive
	} else {
		replyAddr = from.String()
	}
	g.mu.Unlock()

	ack := &Message{
		Type:       MessageTypeAck,
		FromNodeID: g.nodeID,
		SeqNum:     msg.SeqNum,
		Timestamp:  time.Now().UnixNano(),
		Members:    g.Members(),
	}

	_ = seqNum
	g.sendTo(replyAddr, ack)
}

// handleAck handles ACK messages.
func (g *Gossip) handleAck(msg *Message) {
	g.mu.Lock()
	if m, ok := g.members[msg.FromNodeID]; ok {
		m.lastSeen = time.Now()
		m.info.State = types.NodeStateAlive
	}
	g.mu.Unlock()

	for _, member := range msg.Members {
		g.mergeMember(member)
	}
}

// handlePingReq handles PING-REQ messages for indirect pings.
func (g *Gossip) handlePingReq(msg *Message, from *net.UDPAddr) {
	_ = from
	if msg.ToNodeID == "" {
		return
	}

	g.mu.RLock()
	target, ok := g.members[msg.ToNodeID]
	g.mu.RUnlock()

	if !ok {
		return
	}

	ping := &Message{
		Type:       MessageTypePing,
		FromNodeID: g.nodeID,
		SeqNum:     msg.SeqNum,
		Timestamp:  time.Now().UnixNano(),
	}

	g.sendTo(target.info.Addr, ping)
}

// handleSync handles SYNC messages for full state sync.
func (g *Gossip) handleSync(msg *Message) {
	for _, member := range msg.Members {
		g.mergeMember(member)
	}
}

// handleJoin handles JOIN messages.
func (g *Gossip) handleJoin(msg *Message, from *net.UDPAddr) {
	_ = from
	if msg.NodeInfo == nil {
		return
	}

	isNew := g.mergeMember(*msg.NodeInfo)

	if isNew {
		sync := &Message{
			Type:       MessageTypeSync,
			FromNodeID: g.nodeID,
			Members:    g.Members(),
			Timestamp:  time.Now().UnixNano(),
		}
		// Send sync to the node's advertised address
		g.sendTo(msg.NodeInfo.Addr, sync)
	}
}

// handleLeave handles LEAVE messages.
func (g *Gossip) handleLeave(msg *Message) {
	g.mu.Lock()
	if m, ok := g.members[msg.FromNodeID]; ok {
		m.info.State = types.NodeStateLeft
		m.info.LastSeen = time.Now()
		if g.onNodeLeave != nil {
			go g.onNodeLeave(m.info)
		}
	}
	g.mu.Unlock()
}

// mergeMember merges a member into the local state.
func (g *Gossip) mergeMember(info types.NodeInfo) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	existing, ok := g.members[info.ID]
	if !ok {
		g.members[info.ID] = &memberState{
			info:     info,
			lastSeen: time.Now(),
		}
		if g.onNodeJoin != nil && info.ID != g.nodeID {
			go g.onNodeJoin(info)
		}
		return true
	}

	if info.LastSeen.After(existing.info.LastSeen) {
		existing.info = info
		existing.lastSeen = time.Now()
	}

	return false
}

// protocolLoop runs the periodic protocol operations.
func (g *Gossip) protocolLoop() {
	defer g.wg.Done()

	ticker := time.NewTicker(g.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.pingRandomMember()
			g.checkTimeouts()
		}
	}
}

// pingRandomMember pings a random member.
func (g *Gossip) pingRandomMember() {
	members := g.Members()
	if len(members) == 0 {
		return
	}

	targets := make([]types.NodeInfo, 0)
	for _, m := range members {
		// Ping alive and suspected nodes (suspected nodes might recover)
		if m.ID != g.nodeID && (m.State == types.NodeStateAlive || m.State == types.NodeStateSuspect) {
			targets = append(targets, m)
		}
	}

	if len(targets) == 0 {
		return
	}

	target := targets[rand.Intn(len(targets))]

	g.mu.Lock()
	g.seqNum++
	seqNum := g.seqNum
	g.mu.Unlock()

	msg := &Message{
		Type:       MessageTypePing,
		FromNodeID: g.nodeID,
		SeqNum:     seqNum,
		Timestamp:  time.Now().UnixNano(),
	}

	g.sendTo(target.Addr, msg)
}

// checkTimeouts checks for suspect and dead nodes.
func (g *Gossip) checkTimeouts() {
	now := time.Now()

	g.mu.Lock()
	defer g.mu.Unlock()

	for id, m := range g.members {
		if id == g.nodeID {
			continue
		}

		timeSinceSeen := now.Sub(m.lastSeen)

		switch m.info.State {
		case types.NodeStateAlive:
			if timeSinceSeen > g.suspectTimeout {
				m.info.State = types.NodeStateSuspect
				m.suspectTime = now
				if g.onNodeSuspect != nil {
					go g.onNodeSuspect(m.info)
				}
			}
		case types.NodeStateSuspect:
			if now.Sub(m.suspectTime) > g.deadTimeout {
				m.info.State = types.NodeStateDead
				if g.onNodeLeave != nil {
					go g.onNodeLeave(m.info)
				}
			}
		}
	}
}

// sendTo sends a message to an address.
func (g *Gossip) sendTo(addr string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	_, err = g.conn.WriteToUDP(data, udpAddr)
	return err
}

// broadcast sends a message to all known members.
func (g *Gossip) broadcast(msg *Message) {
	members := g.Members()
	for _, m := range members {
		if m.ID != g.nodeID {
			g.sendTo(m.Addr, msg)
		}
	}
}

// AddMember manually adds a member.
func (g *Gossip) AddMember(info types.NodeInfo) {
	g.mergeMember(info)
}

// RemoveMember removes a member.
func (g *Gossip) RemoveMember(nodeID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.members, nodeID)
}

// GetMember returns info about a specific member.
func (g *Gossip) GetMember(nodeID string) (types.NodeInfo, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	m, ok := g.members[nodeID]
	if !ok {
		return types.NodeInfo{}, false
	}
	return m.info, true
}

// NodeID returns this node's ID.
func (g *Gossip) NodeID() string {
	return g.nodeID
}

// BindAddr returns this node's bind address.
func (g *Gossip) BindAddr() string {
	return g.bindAddr
}

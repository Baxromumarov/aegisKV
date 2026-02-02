package node

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/baxromumarov/aegisKV/pkg/config"
	"github.com/baxromumarov/aegisKV/pkg/consistent"
	"github.com/baxromumarov/aegisKV/pkg/gossip"
	"github.com/baxromumarov/aegisKV/pkg/health"
	"github.com/baxromumarov/aegisKV/pkg/logger"
	"github.com/baxromumarov/aegisKV/pkg/protocol"
	"github.com/baxromumarov/aegisKV/pkg/replication"
	"github.com/baxromumarov/aegisKV/pkg/server"
	"github.com/baxromumarov/aegisKV/pkg/shard"
	"github.com/baxromumarov/aegisKV/pkg/types"
	"github.com/baxromumarov/aegisKV/pkg/wal"
)

// Node represents a single AegisKV node.
type Node struct {
	mu         sync.RWMutex
	cfg        *config.Config
	nodeID     string
	ring       *consistent.HashRing
	shardMgr   *shard.Manager
	walLog     *wal.WAL
	gossiper   *gossip.Gossip
	replicator *replication.Replicator
	srv        *server.Server
	healthSrv  *health.Server
	members    map[string]types.NodeInfo
	stopCh     chan struct{}
	wg         sync.WaitGroup
	log        *logger.Logger
}

// New creates a new Node.
func New(cfg *config.Config) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	ring := consistent.NewHashRing(cfg.VirtualNodes)
	ring.AddNodeWithAddr(cfg.NodeID, cfg.BindAddr)

	shardMgr := shard.NewManager(
		cfg.NodeID,
		uint64(cfg.NumShards),
		cfg.ShardMaxBytes,
		ring,
		cfg.ReplicationFactor,
	)

	var walLog *wal.WAL
	var err error
	if cfg.GetWALMode() != types.WALModeOff {
		walLog, err = wal.New(cfg.WALDir, cfg.GetWALMode(), cfg.WALMaxSizeMB*1024*1024)
		if err != nil {
			return nil, fmt.Errorf("failed to create WAL: %w", err)
		}
	} else {
		walLog, _ = wal.New("", types.WALModeOff, 0)
	}

	n := &Node{
		cfg:      cfg,
		nodeID:   cfg.NodeID,
		ring:     ring,
		shardMgr: shardMgr,
		walLog:   walLog,
		members:  make(map[string]types.NodeInfo),
		stopCh:   make(chan struct{}),
		log:      logger.New("node", logger.ParseLevel(cfg.LogLevel), nil),
	}

	gossiper, err := gossip.New(gossip.Config{
		NodeID:         cfg.NodeID,
		BindAddr:       cfg.GossipBindAddr,
		AdvertiseAddr:  cfg.GossipAdvertiseAddr,
		ClientAddr:     cfg.ClientAdvertiseAddr,
		PingInterval:   cfg.GossipInterval,
		SuspectTimeout: cfg.SuspectTimeout,
		DeadTimeout:    cfg.DeadTimeout,
		OnNodeJoin:     n.onNodeJoin,
		OnNodeLeave:    n.onNodeLeave,
		OnNodeSuspect:  n.onNodeSuspect,
		ClusterSecret:  cfg.GossipSecret,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create gossip: %w", err)
	}
	n.gossiper = gossiper

	n.replicator = replication.New(replication.Config{
		NodeID:       cfg.NodeID,
		BatchSize:    cfg.ReplicationBatchSize,
		BatchTimeout: cfg.ReplicationBatchTimeout,
		MaxRetries:   cfg.ReplicationMaxRetries,
		GetNodeAddr:  n.getNodeAddr,
		AuthToken:    cfg.AuthToken,
		TLSConfig:    cfg.TLSConfig,
	})

	n.srv = server.New(server.Config{
		Addr:         cfg.BindAddr,
		Handler:      n,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		MaxConns:     cfg.MaxConns,
		AuthToken:    cfg.AuthToken,
		TLSConfig:    cfg.TLSConfig,
		RateLimit:    cfg.RateLimit,
		RateBurst:    cfg.RateBurst,
	})

	// Create health server
	if cfg.HealthAddr != "" {
		n.healthSrv = health.New(cfg.HealthAddr, cfg.NodeID, n.statsFunc)
	}

	return n, nil
}

// Start starts the node.
func (n *Node) Start() error {
	// Initialize shards first so WAL recovery can find them
	n.shardMgr.InitializeShards()

	// Recover from WAL
	if n.walLog != nil && n.walLog.Mode() != types.WALModeOff {
		if err := n.recoverFromWAL(); err != nil {
			return fmt.Errorf("failed to recover from WAL: %w", err)
		}
	}

	if err := n.gossiper.Start(); err != nil {
		return fmt.Errorf("failed to start gossip: %w", err)
	}

	if len(n.cfg.Addrs) > 0 {
		if err := n.gossiper.Join(n.cfg.Addrs); err != nil {
			n.log.Warn("failed to join cluster: %v", err)
		}
	}

	if err := n.replicator.Start(); err != nil {
		return fmt.Errorf("failed to start replicator: %w", err)
	}

	if err := n.srv.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	// Start health server
	if n.healthSrv != nil {
		if err := n.healthSrv.Start(); err != nil {
			n.log.Warn("failed to start health server: %v", err)
		} else {
			n.healthSrv.SetReady(true)
		}
	}

	n.wg.Add(1)
	go n.maintenanceLoop()

	n.log.Info("Node %s started, listening on %s", n.nodeID, n.cfg.BindAddr)
	return nil
}

// Stop stops the node.
func (n *Node) Stop() error {
	close(n.stopCh)
	n.wg.Wait()

	if n.healthSrv != nil {
		n.healthSrv.Stop()
	}

	n.gossiper.Leave()
	n.gossiper.Stop()
	n.replicator.Stop()
	n.srv.Stop()
	n.shardMgr.Close()

	if n.walLog != nil {
		n.walLog.Close()
	}

	n.log.Info("Node %s stopped", n.nodeID)
	return nil
}

// HandleGet handles GET requests.
func (n *Node) HandleGet(key []byte) (value []byte, ttl int64, version uint64, found bool) {
	shard, err := n.shardMgr.GetShard(key)
	if err != nil {
		return nil, 0, 0, false
	}

	entry, err := shard.Get(string(key))
	if err != nil || entry == nil {
		return nil, 0, 0, false
	}

	remainingTTL := int64(0)
	if entry.Expiry > 0 {
		remainingTTL = entry.Expiry - time.Now().UnixMilli()
		if remainingTTL <= 0 {
			return nil, 0, 0, false
		}
	}

	return entry.Value, remainingTTL, entry.Version.Seq, true
}

// HandleSet handles SET requests.
func (n *Node) HandleSet(key, value []byte, ttl int64) (version uint64, err error) {
	shard, err := n.shardMgr.GetShard(key)
	if err != nil {
		return 0, err
	}

	entry, err := shard.Set(string(key), value, ttl)
	if err != nil {
		return 0, err
	}

	if n.walLog != nil && n.walLog.Mode() != types.WALModeOff {
		n.walLog.AppendSet(shard.ID, key, value, ttl, entry.Version)
	}

	followers := shard.Followers
	if len(followers) > 0 {
		n.replicator.ReplicateSet(shard.ID, key, value, time.Duration(ttl)*time.Millisecond, entry.Version, followers)
	}

	return entry.Version.Seq, nil
}

// HandleDelete handles DELETE requests.
func (n *Node) HandleDelete(key []byte) error {
	shard, err := n.shardMgr.GetShard(key)
	if err != nil {
		return err
	}

	entry, err := shard.Delete(string(key))
	if err != nil {
		return err
	}

	ver := types.Version{}
	if entry != nil {
		ver = entry.Version
	}

	if n.walLog != nil && n.walLog.Mode() != types.WALModeOff {
		n.walLog.AppendDelete(shard.ID, key, ver)
	}

	followers := shard.Followers
	if len(followers) > 0 {
		n.replicator.ReplicateDelete(shard.ID, key, ver, followers)
	}

	return nil
}

// HandleReplicate handles replication requests from other nodes.
// Value format: [1B type][8B shardID][8B term][8B seq][8B ttlNanos][actual value]
func (n *Node) HandleReplicate(req *protocol.Request) error {
	// Minimum payload: 1 + 8 + 8 + 8 + 8 = 33 bytes
	if len(req.Value) < 33 {
		return fmt.Errorf("invalid replication payload: too short")
	}

	offset := 0

	// Decode metadata
	eventType := req.Value[offset]
	offset++

	shardID := binary.BigEndian.Uint64(req.Value[offset:])
	offset += 8

	term := binary.BigEndian.Uint64(req.Value[offset:])
	offset += 8

	seq := binary.BigEndian.Uint64(req.Value[offset:])
	offset += 8

	ttlNanos := binary.BigEndian.Uint64(req.Value[offset:])
	offset += 8

	actualValue := req.Value[offset:]

	// Get the shard
	s, err := n.shardMgr.GetShardByID(shardID)
	if err != nil {
		// Not owned by this node - ignore silently
		return nil
	}

	version := types.Version{Term: term, Seq: seq}

	switch eventType {
	case 0: // EventTypeSet
		var expiry int64
		if ttlNanos > 0 {
			expiry = time.Now().Add(time.Duration(ttlNanos)).UnixMilli()
		}

		entry := &types.Entry{
			Key:     req.Key,
			Value:   actualValue,
			Expiry:  expiry,
			Version: version,
			Created: time.Now(),
		}

		s.ApplyReplicated(entry)

		// WAL the replicated data
		if n.walLog != nil && n.walLog.Mode() != types.WALModeOff {
			n.walLog.AppendSet(shardID, req.Key, actualValue, time.Duration(ttlNanos).Milliseconds(), version)
		}

	case 1: // EventTypeDelete
		s.Delete(string(req.Key))

		// WAL the delete
		if n.walLog != nil && n.walLog.Mode() != types.WALModeOff {
			n.walLog.AppendDelete(shardID, req.Key, version)
		}
	}

	return nil
}

// GetRedirectAddr returns the address to redirect a request to.
func (n *Node) GetRedirectAddr(key []byte) string {
	primary := n.shardMgr.GetPrimaryForKey(key)
	return n.getClientAddr(primary)
}

// IsPrimaryFor returns true if this node is the primary for the given key.
func (n *Node) IsPrimaryFor(key []byte) bool {
	return n.shardMgr.IsPrimaryFor(key)
}

// getNodeAddr returns the gossip address for a node ID.
func (n *Node) getNodeAddr(nodeID string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if info, ok := n.members[nodeID]; ok {
		return info.Addr
	}

	if nodeID == n.nodeID {
		return n.cfg.BindAddr
	}

	return n.ring.GetNodeAddr(nodeID)
}

// getClientAddr returns the client-facing address for a node ID (for redirects).
func (n *Node) getClientAddr(nodeID string) string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if info, ok := n.members[nodeID]; ok {
		// Prefer ClientAddr if set, otherwise fall back to Addr
		if info.ClientAddr != "" {
			return info.ClientAddr
		}
		return info.Addr
	}

	// If this is our own node
	if nodeID == n.nodeID {
		if n.cfg.ClientAdvertiseAddr != "" {
			return n.cfg.ClientAdvertiseAddr
		}
		return n.cfg.BindAddr
	}

	return n.ring.GetNodeAddr(nodeID)
}

// onNodeJoin handles a node joining the cluster.
func (n *Node) onNodeJoin(info types.NodeInfo) {
	n.log.Info("Node joined: %s at %s", info.ID, info.Addr)

	n.mu.Lock()
	n.members[info.ID] = info
	n.mu.Unlock()

	n.ring.AddNodeWithAddr(info.ID, info.Addr)

	n.recomputeShards()
}

// onNodeLeave handles a node leaving the cluster.
func (n *Node) onNodeLeave(info types.NodeInfo) {
	n.log.Info("Node left: %s", info.ID)

	n.mu.Lock()
	delete(n.members, info.ID)
	n.mu.Unlock()

	n.ring.RemoveNode(info.ID)

	n.recomputeShards()
}

// onNodeSuspect handles a suspected node.
func (n *Node) onNodeSuspect(info types.NodeInfo) {
	n.log.Warn("Node suspected: %s", info.ID)
}

// recomputeShards recomputes shard ownership after cluster changes.
func (n *Node) recomputeShards() {
	n.shardMgr.UpdateRing(n.ring)
	acquire, release := n.shardMgr.RecomputeOwnership()

	if len(acquire) > 0 {
		n.log.Info("Acquiring shards: %v", acquire)
	}
	if len(release) > 0 {
		n.log.Info("Releasing shards: %v", release)
	}
}

// recoverFromWAL replays the WAL to recover state.
func (n *Node) recoverFromWAL() error {
	count := 0
	err := n.walLog.Replay(func(rec *wal.Record) error {
		shard, err := n.shardMgr.GetShardByID(rec.ShardID)
		if err != nil {
			return nil
		}

		switch rec.Op {
		case wal.OpSet:
			var expiry int64
			if rec.TTL > 0 {
				expiry = time.Now().UnixMilli() + rec.TTL
			}
			entry := &types.Entry{
				Key:     rec.Key,
				Value:   rec.Value,
				Expiry:  expiry,
				Version: rec.Version,
				Created: time.Unix(0, rec.Timestamp),
			}
			shard.ApplyReplicated(entry)
		case wal.OpDelete:
			shard.Delete(string(rec.Key))
		}
		count++
		return nil
	})

	if err != nil {
		return err
	}

	if count > 0 {
		n.log.Info("Recovered %d entries from WAL", count)
	}
	return nil
}

// maintenanceLoop runs periodic maintenance tasks.
func (n *Node) maintenanceLoop() {
	defer n.wg.Done()

	interval := n.cfg.MaintenanceInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.runMaintenance()
		}
	}
}

// runMaintenance runs maintenance tasks.
func (n *Node) runMaintenance() {
	for _, s := range n.shardMgr.OwnedShards() {
		s.CleanExpired()
	}
}

// Stats returns node statistics.
func (n *Node) Stats() NodeStats {
	serverStats := n.srv.Stats()
	shardStats := n.shardMgr.Stats()
	replStats := n.replicator.Stats()

	return NodeStats{
		NodeID:          n.nodeID,
		ActiveConns:     serverStats.ActiveConns,
		TotalRequests:   serverStats.TotalRequests,
		TotalShards:     shardStats.TotalShards,
		PrimaryShards:   shardStats.PrimaryShards,
		FollowerShards:  shardStats.FollowerShards,
		DroppedReplicas: replStats.DroppedEvents,
		ClusterSize:     len(n.gossiper.Members()) + 1,
	}
}

// NodeStats contains node statistics.
type NodeStats struct {
	NodeID          string
	ActiveConns     int64
	TotalRequests   uint64
	TotalShards     int
	PrimaryShards   int
	FollowerShards  int
	DroppedReplicas uint64
	ClusterSize     int
}

// NodeID returns the node's ID.
func (n *Node) NodeID() string {
	return n.nodeID
}

// Members returns the cluster members.
func (n *Node) Members() []types.NodeInfo {
	return n.gossiper.Members()
}

// statsFunc returns node stats for the health server.
func (n *Node) statsFunc() map[string]any {
	stats := n.Stats()
	return map[string]any{
		"active_conns":     stats.ActiveConns,
		"total_requests":   stats.TotalRequests,
		"total_shards":     stats.TotalShards,
		"primary_shards":   stats.PrimaryShards,
		"follower_shards":  stats.FollowerShards,
		"dropped_replicas": stats.DroppedReplicas,
		"cluster_size":     stats.ClusterSize,
	}
}

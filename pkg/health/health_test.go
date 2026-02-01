package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNew(t *testing.T) {
	s := New(":8080", "node1", nil)
	if s == nil {
		t.Fatal("New returned nil")
	}
	if s.addr != ":8080" {
		t.Errorf("expected addr ':8080', got '%s'", s.addr)
	}
	if s.nodeID != "node1" {
		t.Errorf("expected nodeID 'node1', got '%s'", s.nodeID)
	}
	if s.ready {
		t.Error("server should not be ready initially")
	}
}

func TestSetReady(t *testing.T) {
	s := New(":8080", "node1", nil)

	s.SetReady(true)
	s.mu.RLock()
	ready := s.ready
	s.mu.RUnlock()

	if !ready {
		t.Error("ready should be true")
	}

	s.SetReady(false)
	s.mu.RLock()
	ready = s.ready
	s.mu.RUnlock()

	if ready {
		t.Error("ready should be false")
	}
}

func TestSetDraining(t *testing.T) {
	s := New(":8080", "node1", nil)

	s.SetDraining(true)
	s.mu.RLock()
	draining := s.draining
	s.mu.RUnlock()

	if !draining {
		t.Error("draining should be true")
	}
}

func TestHandleHealth(t *testing.T) {
	s := New(":8080", "node1", nil)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	s.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got '%v'", resp["status"])
	}
	if resp["node_id"] != "node1" {
		t.Errorf("expected node_id 'node1', got '%v'", resp["node_id"])
	}
}

func TestHandleReadyNotReady(t *testing.T) {
	s := New(":8080", "node1", nil)

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	s.handleReady(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["status"] != "not_ready" {
		t.Errorf("expected status 'not_ready', got '%v'", resp["status"])
	}
}

func TestHandleReadyReady(t *testing.T) {
	s := New(":8080", "node1", nil)
	s.SetReady(true)

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	s.handleReady(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["status"] != "ready" {
		t.Errorf("expected status 'ready', got '%v'", resp["status"])
	}
}

func TestHandleLive(t *testing.T) {
	s := New(":8080", "node1", nil)

	req := httptest.NewRequest("GET", "/live", nil)
	w := httptest.NewRecorder()

	s.handleLive(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	if w.Body.String() != "OK" {
		t.Errorf("expected body 'OK', got '%s'", w.Body.String())
	}
}

func TestHandleStats(t *testing.T) {
	statsFunc := func() map[string]any {
		return map[string]any{
			"cache_size":  1000,
			"connections": 50,
		}
	}

	s := New(":8080", "node1", statsFunc)

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	s.handleStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["node_id"] != "node1" {
		t.Error("missing node_id")
	}
	if resp["cache_size"] != float64(1000) {
		t.Error("missing cache_size")
	}
	if resp["connections"] != float64(50) {
		t.Error("missing connections")
	}
}

func TestHandleStatsNoFunc(t *testing.T) {
	s := New(":8080", "node1", nil)

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()

	s.handleStats(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)

	if resp["node_id"] != "node1" {
		t.Error("missing node_id")
	}
}

func TestSetAdminFunc(t *testing.T) {
	s := New(":8080", "node1", nil)

	adminFunc := func() *AdminInfo {
		return &AdminInfo{
			Members: []MemberInfo{{ID: "node1", State: "alive"}},
		}
	}

	s.SetAdminFunc(adminFunc)

	s.mu.RLock()
	fn := s.adminFunc
	s.mu.RUnlock()

	if fn == nil {
		t.Error("adminFunc should be set")
	}
}

func TestHandleAdminMembers(t *testing.T) {
	s := New(":8080", "node1", nil)

	adminFunc := func() *AdminInfo {
		return &AdminInfo{
			Members: []MemberInfo{
				{ID: "node1", Addr: "localhost:7000", State: "alive"},
				{ID: "node2", Addr: "localhost:7001", State: "alive"},
			},
		}
	}
	s.SetAdminFunc(adminFunc)

	req := httptest.NewRequest("GET", "/admin/members", nil)
	w := httptest.NewRecorder()

	s.handleAdminMembers(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp struct {
		Members []MemberInfo `json:"members"`
	}
	json.NewDecoder(w.Body).Decode(&resp)

	if len(resp.Members) != 2 {
		t.Errorf("expected 2 members, got %d", len(resp.Members))
	}
}

func TestHandleAdminMembersNoFunc(t *testing.T) {
	s := New(":8080", "node1", nil)

	req := httptest.NewRequest("GET", "/admin/members", nil)
	w := httptest.NewRecorder()

	s.handleAdminMembers(w, req)

	// Without adminFunc, returns 503 Service Unavailable
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", w.Code)
	}
}

func TestStopNilServer(t *testing.T) {
	s := New(":8080", "node1", nil)

	err := s.Stop()
	if err != nil {
		t.Errorf("Stop on nil server should not error: %v", err)
	}
}

func TestAdminInfo(t *testing.T) {
	info := AdminInfo{
		Members: []MemberInfo{{ID: "node1"}},
		Shards:  []ShardInfo{{ID: 1, State: "active"}},
		State:   NodeState{NodeID: "node1", Ready: true},
	}

	if len(info.Members) != 1 {
		t.Error("expected 1 member")
	}
	if len(info.Shards) != 1 {
		t.Error("expected 1 shard")
	}
	if !info.State.Ready {
		t.Error("expected ready state")
	}
}

func TestMemberInfo(t *testing.T) {
	m := MemberInfo{
		ID:         "node1",
		Addr:       "localhost:7000",
		ClientAddr: "localhost:8000",
		State:      "alive",
		LastSeen:   "2024-01-01T00:00:00Z",
	}

	if m.ID != "node1" {
		t.Error("ID mismatch")
	}
	if m.State != "alive" {
		t.Error("State mismatch")
	}
}

func TestShardInfo(t *testing.T) {
	s := ShardInfo{
		ID:        1,
		State:     "active",
		Primary:   "node1",
		Followers: []string{"node2", "node3"},
		Term:      5,
		Seq:       100,
		IsPrimary: true,
	}

	if s.ID != 1 {
		t.Error("ID mismatch")
	}
	if !s.IsPrimary {
		t.Error("IsPrimary should be true")
	}
	if len(s.Followers) != 2 {
		t.Error("expected 2 followers")
	}
}

func TestNodeState(t *testing.T) {
	ns := NodeState{
		NodeID:         "node1",
		Ready:          true,
		Draining:       false,
		PrimaryShards:  3,
		FollowerShards: 2,
		TotalMembers:   5,
		AliveMembers:   4,
	}

	if !ns.Ready {
		t.Error("Ready should be true")
	}
	if ns.PrimaryShards != 3 {
		t.Errorf("expected 3 primary shards, got %d", ns.PrimaryShards)
	}
}

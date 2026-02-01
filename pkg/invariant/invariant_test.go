package invariant

import (
	"strings"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	c := New("node-1", false)
	if c == nil {
		t.Fatal("New returned nil")
	}
	if c.nodeID != "node-1" {
		t.Errorf("expected nodeID 'node-1', got '%s'", c.nodeID)
	}
	if c.failFast {
		t.Error("failFast should be false")
	}
}

func TestNewFailFast(t *testing.T) {
	c := New("node-1", true)
	if !c.failFast {
		t.Error("failFast should be true")
	}
}

func TestCheckPass(t *testing.T) {
	c := New("node-1", false)

	result := c.Check("test-invariant", true, "should not fail")
	if !result {
		t.Error("Check should return true for passing condition")
	}

	stats := c.Stats()
	if stats["total_checks"].(int64) != 1 {
		t.Errorf("expected 1 total check, got %v", stats["total_checks"])
	}
	if stats["failed_checks"].(int64) != 0 {
		t.Errorf("expected 0 failed checks, got %v", stats["failed_checks"])
	}
}

func TestCheckFail(t *testing.T) {
	c := New("node-1", false)

	result := c.Check("test-invariant", false, "condition failed: %d", 42)
	if result {
		t.Error("Check should return false for failing condition")
	}

	stats := c.Stats()
	if stats["failed_checks"].(int64) != 1 {
		t.Errorf("expected 1 failed check, got %v", stats["failed_checks"])
	}

	violations := c.Violations()
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation, got %d", len(violations))
	}
	if violations[0].Name != "test-invariant" {
		t.Errorf("expected name 'test-invariant', got '%s'", violations[0].Name)
	}
	if !strings.Contains(violations[0].Message, "42") {
		t.Error("message should contain formatted argument")
	}
}

func TestCheckFailFastPanics(t *testing.T) {
	c := New("node-1", true)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic in fail-fast mode")
		}
	}()

	c.Check("test-invariant", false, "this should panic")
}

func TestCheckNoPanic(t *testing.T) {
	c := New("node-1", true) // fail-fast enabled

	// CheckNoPanic should not panic even in fail-fast mode
	result := c.CheckNoPanic("soft-check", false, "soft failure")
	if result {
		t.Error("CheckNoPanic should return false for failing condition")
	}
}

func TestSetViolationCallback(t *testing.T) {
	c := New("node-1", false)

	var callbackCalled bool
	var receivedViolation Violation

	c.SetViolationCallback(func(v Violation) {
		callbackCalled = true
		receivedViolation = v
	})

	c.Check("callback-test", false, "test message")

	if !callbackCalled {
		t.Error("callback should have been called")
	}
	if receivedViolation.Name != "callback-test" {
		t.Error("callback should receive correct violation")
	}
}

func TestViolationHasStack(t *testing.T) {
	c := New("node-1", false)
	c.Check("stack-test", false, "test")

	violations := c.Violations()
	if len(violations) != 1 {
		t.Fatal("expected 1 violation")
	}
	if violations[0].Stack == "" {
		t.Error("violation should have stack trace")
	}
}

func TestClearViolations(t *testing.T) {
	c := New("node-1", false)
	c.Check("test", false, "fail")
	c.Check("test2", false, "fail2")

	if len(c.Violations()) != 2 {
		t.Error("expected 2 violations")
	}

	c.Clear()
	if len(c.Violations()) != 0 {
		t.Error("violations should be cleared")
	}
}

func TestConcurrentChecks(t *testing.T) {
	c := New("node-1", false)
	var wg sync.WaitGroup

	numGoroutines := 100
	checksPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < checksPerGoroutine; j++ {
				c.Check("concurrent", true, "pass")
				c.CheckNoPanic("concurrent-soft", id%2 == 0, "maybe fail")
			}
		}(i)
	}

	wg.Wait()

	stats := c.Stats()
	expectedTotal := int64(numGoroutines * checksPerGoroutine * 2)
	if stats["total_checks"].(int64) != expectedTotal {
		t.Errorf("expected %d total checks, got %v", expectedTotal, stats["total_checks"])
	}
}

func TestStatsAccuracy(t *testing.T) {
	c := New("node-1", false)

	for i := 0; i < 10; i++ {
		c.Check("pass", true, "ok")
	}
	for i := 0; i < 5; i++ {
		c.Check("fail", false, "not ok")
	}

	stats := c.Stats()
	if stats["total_checks"].(int64) != 15 {
		t.Errorf("expected 15 total, got %v", stats["total_checks"])
	}
	if stats["failed_checks"].(int64) != 5 {
		t.Errorf("expected 5 failed, got %v", stats["failed_checks"])
	}
	if stats["violation_count"].(int) != 5 {
		t.Errorf("expected 5 violations, got %v", stats["violation_count"])
	}
}

func BenchmarkCheckPass(b *testing.B) {
	c := New("bench-node", false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Check("benchmark", true, "pass")
	}
}

func BenchmarkCheckFail(b *testing.B) {
	c := New("bench-node", false)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Check("benchmark", false, "fail %d", i)
	}
}

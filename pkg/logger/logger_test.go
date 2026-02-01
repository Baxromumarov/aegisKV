package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "DEBUG"},
		{LevelInfo, "INFO"},
		{LevelWarn, "WARN"},
		{LevelError, "ERROR"},
		{Level(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if tt.level.String() != tt.expected {
			t.Errorf("Level(%d).String() = %s, want %s", tt.level, tt.level.String(), tt.expected)
		}
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"WARN", LevelWarn},
		{"warning", LevelWarn},
		{"WARNING", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"unknown", LevelInfo}, // Default
		{"", LevelInfo},        // Default
	}

	for _, tt := range tests {
		if got := ParseLevel(tt.input); got != tt.expected {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestNew(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("test", LevelInfo, buf)

	if l == nil {
		t.Fatal("New returned nil")
	}
	if l.prefix != "test" {
		t.Errorf("expected prefix 'test', got '%s'", l.prefix)
	}
	if l.level != LevelInfo {
		t.Errorf("expected level INFO, got %v", l.level)
	}
}

func TestNewNilOutput(t *testing.T) {
	l := New("test", LevelInfo, nil)
	// Should not panic
	l.Info("test message")
}

func TestLogLevels(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("test", LevelDebug, buf)

	l.Debug("debug message")
	if !strings.Contains(buf.String(), "DEBUG") {
		t.Error("expected DEBUG in output")
	}
	if !strings.Contains(buf.String(), "debug message") {
		t.Error("expected message in output")
	}

	buf.Reset()
	l.Info("info message")
	if !strings.Contains(buf.String(), "INFO") {
		t.Error("expected INFO in output")
	}

	buf.Reset()
	l.Warn("warn message")
	if !strings.Contains(buf.String(), "WARN") {
		t.Error("expected WARN in output")
	}

	buf.Reset()
	l.Error("error message")
	if !strings.Contains(buf.String(), "ERROR") {
		t.Error("expected ERROR in output")
	}
}

func TestLogFiltering(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("test", LevelWarn, buf)

	l.Debug("debug message")
	l.Info("info message")
	if buf.Len() > 0 {
		t.Error("debug and info should be filtered at WARN level")
	}

	l.Warn("warn message")
	if buf.Len() == 0 {
		t.Error("warn should be logged at WARN level")
	}

	buf.Reset()
	l.Error("error message")
	if buf.Len() == 0 {
		t.Error("error should be logged at WARN level")
	}
}

func TestLogFormatting(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("myprefix", LevelInfo, buf)

	l.Info("value is %d", 42)

	output := buf.String()
	if !strings.Contains(output, "[myprefix]") {
		t.Error("expected prefix in output")
	}
	if !strings.Contains(output, "value is 42") {
		t.Error("expected formatted message in output")
	}
}

func TestLogTimestamp(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("test", LevelInfo, buf)

	l.Info("test")

	output := buf.String()
	// Check for timestamp format: 2006-01-02T15:04:05.000
	if len(output) < 23 {
		t.Error("output should have timestamp")
	}
	// Should start with year
	if output[0] != '2' {
		t.Error("timestamp should start with year")
	}
}

func TestDefaultLogger(t *testing.T) {
	l := Default()
	if l == nil {
		t.Fatal("Default() returned nil")
	}
}

func TestSetLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	SetOutput(buf)
	SetLevel(LevelError)

	Info("should not appear")
	if buf.Len() > 0 {
		t.Error("info should be filtered at ERROR level")
	}

	Error("should appear")
	if buf.Len() == 0 {
		t.Error("error should be logged")
	}

	// Reset
	SetLevel(LevelInfo)
}

func TestPackageLevelFunctions(t *testing.T) {
	buf := &bytes.Buffer{}
	SetOutput(buf)
	SetLevel(LevelDebug)

	Debug("debug %d", 1)
	if !strings.Contains(buf.String(), "debug 1") {
		t.Error("Debug function should work")
	}

	buf.Reset()
	Info("info %d", 2)
	if !strings.Contains(buf.String(), "info 2") {
		t.Error("Info function should work")
	}

	buf.Reset()
	Warn("warn %d", 3)
	if !strings.Contains(buf.String(), "warn 3") {
		t.Error("Warn function should work")
	}

	buf.Reset()
	Error("error %d", 4)
	if !strings.Contains(buf.String(), "error 4") {
		t.Error("Error function should work")
	}

	// Reset
	SetLevel(LevelInfo)
}

func TestLoggerNoPrefix(t *testing.T) {
	buf := &bytes.Buffer{}
	l := New("", LevelInfo, buf)

	l.Info("no prefix")

	output := buf.String()
	if strings.Contains(output, "[]") {
		t.Error("should not have empty brackets")
	}
}

func BenchmarkLogInfo(b *testing.B) {
	buf := &bytes.Buffer{}
	l := New("bench", LevelInfo, buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Info("benchmark message %d", i)
		buf.Reset()
	}
}

func BenchmarkLogFiltered(b *testing.B) {
	buf := &bytes.Buffer{}
	l := New("bench", LevelError, buf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Debug("filtered message %d", i)
	}
}

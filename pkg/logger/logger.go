// Package logger provides a simple leveled logger with no external dependencies.
package logger

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents a log level.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// String returns the string representation of a log level.
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel parses a log level string.
func ParseLevel(s string) Level {
	switch s {
	case "debug", "DEBUG":
		return LevelDebug
	case "info", "INFO":
		return LevelInfo
	case "warn", "WARN", "warning", "WARNING":
		return LevelWarn
	case "error", "ERROR":
		return LevelError
	default:
		return LevelInfo
	}
}

// Logger is a leveled logger.
type Logger struct {
	mu     sync.Mutex
	level  Level
	out    io.Writer
	prefix string
}

var defaultLogger = &Logger{level: LevelInfo, out: os.Stderr}

// Default returns the default logger.
func Default() *Logger { return defaultLogger }

// SetLevel sets the default logger's level.
func SetLevel(l Level) {
	defaultLogger.mu.Lock()
	defaultLogger.level = l
	defaultLogger.mu.Unlock()
}

// SetOutput sets the default logger's output.
func SetOutput(w io.Writer) {
	defaultLogger.mu.Lock()
	defaultLogger.out = w
	defaultLogger.mu.Unlock()
}

// New creates a new logger with the given prefix, level, and output.
func New(prefix string, level Level, out io.Writer) *Logger {
	if out == nil {
		out = os.Stderr
	}
	return &Logger{prefix: prefix, level: level, out: out}
}

func (l *Logger) log(level Level, format string, args ...any) {
	if level < l.level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := time.Now().Format("2006-01-02T15:04:05.000")
	prefix := ""
	if l.prefix != "" {
		prefix = "[" + l.prefix + "] "
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.out, "%s %s %s%s\n", ts, level, prefix, msg)
}

// Debug logs a debug message.
func (l *Logger) Debug(format string, args ...any) { l.log(LevelDebug, format, args...) }

// Info logs an info message.
func (l *Logger) Info(format string, args ...any) { l.log(LevelInfo, format, args...) }

// Warn logs a warning message.
func (l *Logger) Warn(format string, args ...any) { l.log(LevelWarn, format, args...) }

// Error logs an error message.
func (l *Logger) Error(format string, args ...any) { l.log(LevelError, format, args...) }

// Package-level convenience functions using the default logger.

func Debug(format string, args ...any) { defaultLogger.Debug(format, args...) }
func Info(format string, args ...any)  { defaultLogger.Info(format, args...) }
func Warn(format string, args ...any)  { defaultLogger.Warn(format, args...) }
func Error(format string, args ...any) { defaultLogger.Error(format, args...) }

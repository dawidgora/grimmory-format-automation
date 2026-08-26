// Package logging provides the small structured logger used by the service.
package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

// Level controls which records are emitted.
type Level uint8

const (
	Debug Level = iota
	Info
	Warn
	Error
)

// Parse accepts the supported LOG_LEVEL values. Empty means the default info
// level.
func Parse(value string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return Info, nil
	case "debug":
		return Debug, nil
	case "warn", "warning":
		return Warn, nil
	case "error":
		return Error, nil
	default:
		return Info, fmt.Errorf("invalid log level %q", value)
	}
}

func (level Level) String() string {
	switch level {
	case Debug:
		return "debug"
	case Info:
		return "info"
	case Warn:
		return "warn"
	case Error:
		return "error"
	default:
		return "unknown"
	}
}

// Field is a key/value pair in a structured log record.
type Field struct {
	Key   string
	Value string
}

// Logger emits bounded, key/value records through the standard library logger.
type Logger struct {
	level Level
	std   *log.Logger
}

// New creates a logger. A nil writer uses stderr.
func New(level Level, output io.Writer) *Logger {
	if output == nil {
		output = os.Stderr
	}
	return &Logger{level: level, std: log.New(output, "", log.LstdFlags)}
}

// Level returns the configured minimum level.
func (logger *Logger) Level() Level {
	if logger == nil {
		return Info
	}
	return logger.level
}

// Log emits one structured record when level is enabled.
func (logger *Logger) Log(level Level, fields ...Field) {
	if logger == nil || level < logger.level {
		return
	}
	parts := make([]string, 0, len(fields)+1)
	parts = append(parts, "level="+level.String())
	for _, field := range fields {
		if field.Key == "" {
			continue
		}
		parts = append(parts, field.Key+"="+formatValue(field.Value))
	}
	logger.std.Print(strings.Join(parts, " "))
}

func formatValue(value string) string {
	if value != "" {
		valid := true
		for index := 0; index < len(value); index++ {
			char := value[index]
			if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || strings.ContainsRune("_-./:", rune(char))) {
				valid = false
				break
			}
		}
		if valid {
			return value
		}
	}
	return strconv.Quote(value)
}

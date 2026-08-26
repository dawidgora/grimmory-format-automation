// Package convert invokes Calibre for private reconciliation workspaces.
package convert

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

const (
	// MaxInputBytes is the safe default used when the configured limit is absent.
	MaxInputBytes      int64 = 100 << 20
	maxDiagnosticBytes       = 2048
)

var (
	ErrInvalidFormat  = errors.New("unsupported ebook format")
	ErrInputTooLarge  = errors.New("ebook input is too large")
	ErrOutputTooLarge = errors.New("ebook output is too large")
	ErrExecution      = errors.New("ebook conversion failed")
	ErrOutputMissing  = errors.New("converter did not create output")
)

// Executor abstracts process execution so conversion behavior can be tested
// without installing Calibre.
type Executor interface {
	Execute(ctx context.Context, executable string, args ...string) error
}

// CommandExecutor invokes a process directly; no shell is involved.
type CommandExecutor struct{}

func (CommandExecutor) Execute(ctx context.Context, executable string, args ...string) error {
	command := exec.CommandContext(ctx, executable, args...)
	command.Stdout = io.Discard
	stderr := &boundedOutput{}
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		return &ProcessError{cause: err, detail: stderr.String()}
	}
	return nil
}

// ProcessError preserves only a bounded amount of process diagnostics.
type ProcessError struct {
	cause  error
	detail string
}

func (err *ProcessError) Error() string {
	if err.detail == "" {
		return err.cause.Error()
	}
	return err.cause.Error() + ": " + err.detail
}

func (err *ProcessError) Unwrap() error { return err.cause }

type ExecutionError struct{ detail string }

func (err *ExecutionError) Error() string {
	if err.detail == "" {
		return ErrExecution.Error()
	}
	return ErrExecution.Error() + ": " + err.detail
}

func (err *ExecutionError) Unwrap() error { return ErrExecution }

type boundedOutput struct {
	data      []byte
	truncated bool
}

func (output *boundedOutput) Write(data []byte) (int, error) {
	remaining := maxDiagnosticBytes - len(output.data)
	if remaining > 0 {
		if len(data) > remaining {
			output.data = append(output.data, data[:remaining]...)
			output.truncated = true
		} else {
			output.data = append(output.data, data...)
		}
	} else if len(data) > 0 {
		output.truncated = true
	}
	return len(data), nil
}

func (output *boundedOutput) String() string {
	detail := strings.TrimSpace(string(output.data))
	if output.truncated {
		detail += " ...[truncated]"
	}
	return detail
}

func boundedDiagnostic(err error) string {
	var process *ProcessError
	if errors.As(err, &process) && process.detail != "" {
		return process.detail
	}
	detail := strings.TrimSpace(err.Error())
	if len(detail) > maxDiagnosticBytes {
		return detail[:maxDiagnosticBytes] + " ...[truncated]"
	}
	return detail
}

// FileConverter invokes Calibre on files already stored in a private
// reconciliation workspace. It keeps large books out of the Go heap and
// validates both sides of the process.
type FileConverter struct {
	executable string
	executor   Executor
	maxBytes   int64
}

func NewFileConverter(executable string, maxBytes int64, executor Executor) *FileConverter {
	if executor == nil {
		executor = CommandExecutor{}
	}
	if executable == "" {
		executable = "ebook-convert"
	}
	if maxBytes <= 0 {
		maxBytes = MaxInputBytes
	}
	return &FileConverter{executable: executable, executor: executor, maxBytes: maxBytes}
}

// Convert writes one Calibre result into workDir and returns its private path.
// The caller owns cleanup of workDir.
func (c *FileConverter) Convert(ctx context.Context, inputPath, source, target, workDir string) (string, error) {
	if c == nil || c.executor == nil || c.executable == "" {
		return "", errors.New("file converter is not initialized")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	target = strings.ToLower(strings.TrimSpace(target))
	if !safeFormat(source) || !safeFormat(target) || source == target {
		return "", ErrInvalidFormat
	}
	if err := verifyInputFile(inputPath, c.maxBytes); err != nil {
		return "", err
	}
	if workDir == "" {
		return "", errors.New("conversion work directory is empty")
	}
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return "", fmt.Errorf("create conversion workspace: %w", err)
	}
	output, err := os.CreateTemp(workDir, fmt.Sprintf(".calibre-output-*.%s", target))
	if err != nil {
		return "", fmt.Errorf("create conversion output: %w", err)
	}
	outputPath := output.Name()
	if err := output.Chmod(0o600); err != nil {
		_ = output.Close()
		_ = os.Remove(outputPath)
		return "", fmt.Errorf("restrict conversion output: %w", err)
	}
	if err := output.Close(); err != nil {
		_ = os.Remove(outputPath)
		return "", fmt.Errorf("close conversion output: %w", err)
	}
	// ebook-convert expects to create its destination; keeping a pre-created
	// path would make behavior differ between real Calibre and test executors.
	if err := os.Remove(outputPath); err != nil {
		return "", fmt.Errorf("prepare conversion output: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = os.Remove(outputPath)
		return "", err
	}
	if err := c.executor.Execute(ctx, c.executable, inputPath, outputPath); err != nil {
		_ = os.Remove(outputPath)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", &ExecutionError{detail: boundedDiagnostic(err)}
	}
	if err := ctx.Err(); err != nil {
		_ = os.Remove(outputPath)
		return "", err
	}
	if err := verifyRegularFile(outputPath, c.maxBytes); err != nil {
		_ = os.Remove(outputPath)
		if errors.Is(err, ErrOutputMissing) || errors.Is(err, ErrOutputTooLarge) {
			return "", err
		}
		return "", fmt.Errorf("inspect conversion output: %w", err)
	}
	if err := os.Chmod(outputPath, 0o600); err != nil {
		_ = os.Remove(outputPath)
		return "", fmt.Errorf("restrict conversion output: %w", err)
	}
	return outputPath, nil
}

// HashFile validates a file and calculates its SHA-256 without reading more
// than maxBytes. It is used immediately before every upload.
func HashFile(filePath string, maxBytes int64) (string, int64, error) {
	if err := verifyRegularFile(filePath, maxBytes); err != nil {
		return "", 0, err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return "", 0, fmt.Errorf("open file for hashing: %w", err)
	}
	defer file.Close()
	digest := sha256.New()
	count, err := io.Copy(digest, io.LimitReader(file, maxBytes+1))
	if err != nil {
		return "", 0, fmt.Errorf("hash file: %w", err)
	}
	if count > maxBytes {
		return "", 0, ErrOutputTooLarge
	}
	return fmt.Sprintf("%x", digest.Sum(nil)), count, nil
}

func verifyRegularFile(filePath string, maxBytes int64) error {
	if filePath == "" || maxBytes <= 0 {
		return ErrOutputMissing
	}
	info, err := os.Lstat(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrOutputMissing
		}
		return fmt.Errorf("inspect file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return ErrOutputMissing
	}
	if info.Size() > maxBytes {
		return ErrOutputTooLarge
	}
	return nil
}

func verifyInputFile(filePath string, maxBytes int64) error {
	if err := verifyRegularFile(filePath, maxBytes); errors.Is(err, ErrOutputTooLarge) {
		return ErrInputTooLarge
	} else if err != nil {
		return err
	}
	return nil
}

func safeFormat(value string) bool {
	if value == "" || len(value) > 32 {
		return false
	}
	for index, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '+' && char != '_' && char != '-' {
			return false
		}
		if index == 0 && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

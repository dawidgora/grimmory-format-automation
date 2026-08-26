package convert

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeExecutor struct {
	mu       sync.Mutex
	calls    [][]string
	write    bool
	err      error
	oversize bool
}

func (f *fakeExecutor) Execute(_ context.Context, executable string, args ...string) error {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string{executable}, args...))
	f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if f.write {
		data := []byte("converted")
		if f.oversize {
			data = []byte(strings.Repeat("x", 100))
		}
		return os.WriteFile(args[1], data, 0o600)
	}
	return nil
}

func TestFileConverterUsesDirectArgumentsAndRegularOutput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "book.epub")
	if err := os.WriteFile(input, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := &fakeExecutor{write: true}
	converter := NewFileConverter("ebook-convert", 1<<20, fake)
	output, err := converter.Convert(context.Background(), input, "EPUB", "MOBI", dir)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Ext(output) != ".mobi" {
		t.Fatalf("output extension = %q", filepath.Ext(output))
	}
	if info, err := os.Stat(output); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("output info = %+v err=%v", info, err)
	}
	fake.mu.Lock()
	calls := append([][]string(nil), fake.calls...)
	fake.mu.Unlock()
	if len(calls) != 1 || len(calls[0]) != 3 || calls[0][0] != "ebook-convert" || calls[0][1] != input || filepath.Ext(calls[0][2]) != ".mobi" {
		t.Fatalf("executor calls = %+v", calls)
	}
	if _, _, err := HashFile(output, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(output); err != nil {
		t.Fatal(err)
	}
}

func TestFileConverterRejectsInvalidInputMissingAndOversizedOutput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "book.epub")
	if err := os.WriteFile(input, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	converter := NewFileConverter("ebook-convert", 10, &fakeExecutor{write: true, oversize: true})
	if _, err := converter.Convert(context.Background(), input, "epub", "../mobi", dir); !errors.Is(err, ErrInvalidFormat) {
		t.Fatalf("invalid format error = %v", err)
	}
	if _, err := converter.Convert(context.Background(), filepath.Join(dir, "missing"), "epub", "mobi", dir); !errors.Is(err, ErrOutputMissing) {
		t.Fatalf("missing input error = %v", err)
	}
	if _, err := converter.Convert(context.Background(), input, "epub", "mobi", dir); !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("oversized output error = %v", err)
	}
}

func TestFileConverterFailureCleansOutput(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "book.epub")
	if err := os.WriteFile(input, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	converter := NewFileConverter("ebook-convert", 1<<20, &fakeExecutor{err: errors.New(strings.Repeat("diagnostic ", 500))})
	_, err := converter.Convert(context.Background(), input, "epub", "mobi", dir)
	if !errors.Is(err, ErrExecution) || len(err.Error()) > maxDiagnosticBytes+64 {
		t.Fatalf("execution error = %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 { // the input remains, but no conversion output does.
		t.Fatalf("workspace entries = %v", entries)
	}
}

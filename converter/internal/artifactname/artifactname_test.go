package artifactname

import (
	"strings"
	"testing"
)

func TestOutputFilename(t *testing.T) {
	tests := []struct {
		name   string
		source string
		format string
		want   string
	}{
		{name: "commas", source: "Author, Title.epub", format: "mobi", want: "Author_ Title.mobi"},
		{name: "path traversal and separators", source: `../nested\\Unsafe: Name.epub`, format: "azw3", want: "Unsafe_ Name.azw3"},
		{name: "unicode", source: "日本語 📚.epub", format: "mobi", want: "日本語 _.mobi"},
		{name: "empty name", source: "", format: "mobi", want: "book.mobi"},
		{name: "extension replacement", source: "book.epub", format: "MOBI", want: "book.mobi"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := OutputFilename(test.source, test.format); got != test.want {
				t.Fatalf("OutputFilename(%q, %q) = %q, want %q", test.source, test.format, got, test.want)
			}
		})
	}
}

func TestOutputFilenameLimitsSanitizedSourceTo180Runes(t *testing.T) {
	got := OutputFilename(strings.Repeat("界", 200)+".epub", "mobi")
	if runes := []rune(got); len(runes) != 185 {
		t.Fatalf("OutputFilename rune length = %d, want 185", len(runes))
	}
	if want := strings.Repeat("界", 180) + ".mobi"; got != want {
		t.Fatalf("OutputFilename long name = %q, want %q", got, want)
	}
}

// Package artifactname contains the shared remote artifact naming policy.
package artifactname

import (
	"path"
	"strings"
	"unicode"
)

// OutputFilename returns the safe filename used for a converted artifact.
func OutputFilename(sourceName, targetFormat string) string {
	base := path.Base(strings.ReplaceAll(strings.TrimSpace(sourceName), "\\", "/"))
	if base == "." || base == ".." || base == "/" {
		base = "book"
	}

	var builder strings.Builder
	for _, char := range base {
		switch {
		case unicode.IsLetter(char), unicode.IsDigit(char):
			builder.WriteRune(char)
		case strings.ContainsRune(" .-_()[]{}", char):
			builder.WriteRune(char)
		default:
			builder.WriteRune('_')
		}
	}
	base = strings.Trim(builder.String(), " .")
	if base == "" || base == "." || base == ".." {
		base = "book"
	}
	if runes := []rune(base); len(runes) > 180 {
		base = string(runes[:180])
	}

	stem := strings.TrimSuffix(base, path.Ext(base))
	if stem == "" {
		stem = "book"
	}
	targetFormat = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(targetFormat), "."))
	if targetFormat == "" {
		return stem
	}
	return stem + "." + targetFormat
}

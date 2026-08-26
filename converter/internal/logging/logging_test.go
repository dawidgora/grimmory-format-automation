package logging

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseAndThreshold(t *testing.T) {
	for _, test := range []struct {
		value string
		want  Level
	}{
		{"", Info}, {"debug", Debug}, {"INFO", Info}, {"warn", Warn}, {"error", Error},
	} {
		got, err := Parse(test.value)
		if err != nil || got != test.want {
			t.Fatalf("Parse(%q) = %v, %v; want %v", test.value, got, err, test.want)
		}
	}
	if _, err := Parse("trace"); err == nil {
		t.Fatal("expected invalid level")
	}

	var output bytes.Buffer
	logger := New(Info, &output)
	logger.Log(Debug, Field{Key: "ignored", Value: "true"})
	logger.Log(Info, Field{Key: "operation", Value: "convert"}, Field{Key: "message", Value: "hello world"})
	if strings.Contains(output.String(), "ignored") || !strings.Contains(output.String(), "operation=convert") || !strings.Contains(output.String(), `message="hello world"`) {
		t.Fatalf("unexpected log output: %s", output.String())
	}
}

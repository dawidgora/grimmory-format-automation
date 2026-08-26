package main

import "testing"

func TestParsePollFlagDefaultsAndOptIn(t *testing.T) {
	poll, err := parsePollFlag(nil)
	if err != nil || poll {
		t.Fatalf("default poll=%v err=%v", poll, err)
	}
	poll, err = parsePollFlag([]string{"--poll"})
	if err != nil || !poll {
		t.Fatalf("poll flag=%v err=%v", poll, err)
	}
	poll, err = parsePollFlag([]string{"--poll=false"})
	if err != nil || poll {
		t.Fatalf("explicit false poll=%v err=%v", poll, err)
	}
}

func TestParsePollFlagRejectsUnexpectedArguments(t *testing.T) {
	if _, err := parsePollFlag([]string{"--poll", "unexpected"}); err == nil {
		t.Fatal("expected positional argument error")
	}
}

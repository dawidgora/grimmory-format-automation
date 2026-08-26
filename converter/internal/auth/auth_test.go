package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAuthenticatorBearer(t *testing.T) {
	authenticator := New("correct horse battery staple")
	tests := map[string]bool{
		"Bearer correct horse battery staple": true,
		"bearer correct horse battery staple": true,
		"Bearer wrong":                        false,
		"Basic correct horse battery staple":  false,
		"Bearer":                              false,
		"Bearer ":                             false,
		"":                                    false,
	}
	for authorization, want := range tests {
		if got := authenticator.ValidBearer(authorization); got != want {
			t.Errorf("ValidBearer(%q) = %v, want %v", authorization, got, want)
		}
	}
}

func TestLoadOrCreateEnvironmentPrecedesPersistedKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api-key")
	if err := os.WriteFile(path, []byte("persisted"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("API_KEY", "from-environment")

	key, generated, err := LoadOrCreate(path)
	if err != nil {
		t.Fatal(err)
	}
	if key != "from-environment" || generated {
		t.Fatalf("got key %q generated=%v", key, generated)
	}
}

func TestLoadOrCreateGeneratesRestrictedKey(t *testing.T) {
	unsetEnv(t, "API_KEY")
	path := filepath.Join(t.TempDir(), "nested", "api-key")
	key, generated, err := LoadOrCreate(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) < 40 || !generated {
		t.Fatalf("unexpected generated key %q generated=%v", key, generated)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("key mode = %o, want 600", got)
	}
	loaded, generated, err := LoadOrCreate(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded != key || generated {
		t.Fatalf("reload got key %q generated=%v", loaded, generated)
	}
}

func TestLoadOrCreateRejectsSymlink(t *testing.T) {
	unsetEnv(t, "API_KEY")
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	path := filepath.Join(dir, "api-key")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, err := LoadOrCreate(path); err == nil {
		t.Fatal("expected symlink error")
	}
}

func unsetEnv(t *testing.T, name string) {
	t.Helper()
	value, present := os.LookupEnv(name)
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if present {
			_ = os.Setenv(name, value)
		} else {
			_ = os.Unsetenv(name)
		}
	})
}

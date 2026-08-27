package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var errEmptyAPIKey = errors.New("api key is empty")

// LoadOrCreate returns API_KEY when configured. Otherwise it loads the
// generated key from dataDir/api-key or creates and persists one with private
// file permissions. generated is true only when a new key was persisted.
func LoadOrCreate(dataDir string) (key string, generated bool, err error) {
	if value, present := os.LookupEnv("API_KEY"); present {
		value = strings.TrimSpace(value)
		if value == "" {
			return "", false, fmt.Errorf("API_KEY: %w", errEmptyAPIKey)
		}
		return value, false, nil
	}

	path := filepath.Join(dataDir, "api-key")
	if info, statErr := os.Lstat(path); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return "", false, errors.New("api key file must not be a symlink")
		}
		if !info.Mode().IsRegular() {
			return "", false, errors.New("api key path is not a regular file")
		}
		value, readErr := os.ReadFile(path)
		if readErr != nil {
			return "", false, fmt.Errorf("read api key: %w", readErr)
		}
		key = strings.TrimSpace(string(value))
		if key == "" {
			return "", false, errEmptyAPIKey
		}
		if chmodErr := os.Chmod(path, 0o600); chmodErr != nil {
			return "", false, fmt.Errorf("restrict api key file: %w", chmodErr)
		}
		return key, false, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return "", false, fmt.Errorf("inspect api key file: %w", statErr)
	}

	raw := make([]byte, 32)
	if _, err = io.ReadFull(rand.Reader, raw); err != nil {
		return "", false, fmt.Errorf("generate api key: %w", err)
	}
	key = base64.RawURLEncoding.EncodeToString(raw)

	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return "", false, fmt.Errorf("create api key directory: %w", err)
	}
	temp, err := os.CreateTemp(dir, ".api-key-*")
	if err != nil {
		return "", false, fmt.Errorf("create api key file: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err = temp.Chmod(0o600); err == nil {
		_, err = temp.WriteString(key)
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", false, fmt.Errorf("write api key file: %w", err)
	}
	if err = os.Rename(tempPath, path); err != nil {
		return "", false, fmt.Errorf("persist api key: %w", err)
	}
	return key, true, nil
}

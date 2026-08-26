package config

import (
	"testing"
	"time"

	"converter/internal/logging"
)

func TestLoadDefaults(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GRIMMORY_BASE_URL", "https://grimmory.example")
	t.Setenv("GRIMMORY_USERNAME", "user")
	t.Setenv("GRIMMORY_PASSWORD", "password")
	t.Setenv("LIBRARY_IDS", "1, 2")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || cfg.DataDir != "/data" || cfg.APIKeyPath != "/data/api-key" || cfg.CalibreBinary != "ebook-convert" || cfg.LogLevel != logging.Info {
		t.Fatalf("defaults = %+v", cfg)
	}
	if join(cfg.LibraryIDs) != "1,2" || join(cfg.OutputFormats) != "mobi,azw3" || join(cfg.SupportedInputFormats) != "epub,azw3,mobi" || cfg.MaxConcurrentBooks != 1 {
		t.Fatalf("format defaults = %+v", cfg)
	}
	if cfg.MaxFileBytes != 100<<20 || cfg.HTTPTimeout != 30*time.Second || cfg.PollInterval != 5*time.Minute || cfg.PollMaxAttempts != 5 || cfg.PollRetryBase != 30*time.Second || cfg.PollRetryMax != 15*time.Minute {
		t.Fatalf("limit defaults = %+v", cfg)
	}
}

func TestLibraryAllowlistAndConcurrencyAreValidated(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GRIMMORY_BASE_URL", "https://grimmory.example")
	t.Setenv("GRIMMORY_USERNAME", "user")
	t.Setenv("GRIMMORY_PASSWORD", "password")
	t.Setenv("LIBRARY_IDS", "001,2,2")
	t.Setenv("MAX_CONCURRENT_BOOKS", "16")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if join(cfg.LibraryIDs) != "1,2" || cfg.MaxConcurrentBooks != 16 {
		t.Fatalf("allowlist/concurrency = %+v", cfg)
	}
	for _, value := range []string{"0", "17", "not-an-integer"} {
		t.Setenv("MAX_CONCURRENT_BOOKS", value)
		if _, err := Load(); err == nil {
			t.Fatalf("accepted MAX_CONCURRENT_BOOKS=%q", value)
		}
	}
	t.Setenv("MAX_CONCURRENT_BOOKS", "1")
	for _, value := range []string{"", "1,two", "1,,2"} {
		t.Setenv("LIBRARY_IDS", value)
		if _, err := Load(); err == nil {
			t.Fatalf("accepted LIBRARY_IDS=%q", value)
		}
	}
}

func TestPollSettingsAreValidatedAndParsed(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GRIMMORY_BASE_URL", "https://grimmory.example")
	t.Setenv("GRIMMORY_USERNAME", "user")
	t.Setenv("GRIMMORY_PASSWORD", "password")
	t.Setenv("LIBRARY_IDS", "1")
	t.Setenv("POLL_INTERVAL", "2m")
	t.Setenv("POLL_MAX_ATTEMPTS", "7")
	t.Setenv("POLL_RETRY_BASE", "10s")
	t.Setenv("POLL_RETRY_MAX", "2m")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PollInterval != 2*time.Minute || cfg.PollMaxAttempts != 7 || cfg.PollRetryBase != 10*time.Second || cfg.PollRetryMax != 2*time.Minute {
		t.Fatalf("poll settings = %+v", cfg)
	}
	t.Setenv("POLL_RETRY_BASE", "3m")
	if _, err := Load(); err == nil {
		t.Fatal("expected retry base/max ordering error")
	}
}

func TestFormatsAreNormalizedDeduplicatedAndBounded(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GRIMMORY_BASE_URL", "https://grimmory.example")
	t.Setenv("GRIMMORY_USERNAME", "user")
	t.Setenv("GRIMMORY_PASSWORD", "password")
	t.Setenv("LIBRARY_IDS", "1")
	t.Setenv("OUTPUT_FORMATS", " MOBI, azw3, mobi ")
	t.Setenv("SUPPORTED_INPUT_FORMATS", "AZW3 epub azw3")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if join(cfg.OutputFormats) != "mobi,azw3" || join(cfg.SupportedInputFormats) != "azw3,epub" {
		t.Fatalf("normalized formats = %+v", cfg)
	}
}

func TestConfigurationRequiresLibraryIDsAndAllowsPerLibraryMainFormats(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GRIMMORY_BASE_URL", "https://grimmory.example")
	t.Setenv("GRIMMORY_USERNAME", "user")
	t.Setenv("GRIMMORY_PASSWORD", "password")
	if _, err := Load(); err == nil {
		t.Fatal("expected LIBRARY_IDS to be required")
	}
	t.Setenv("LIBRARY_IDS", "1")
	t.Setenv("OUTPUT_FORMATS", "mobi, EPUB, azw3")
	t.Setenv("SUPPORTED_INPUT_FORMATS", "mobi,azw3")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigurationRequiresGrimmoryCredentialsAndRejectsUnsafeFormats(t *testing.T) {
	clearConfigEnv(t)
	if _, err := Load(); err == nil {
		t.Fatal("expected required base URL error")
	}
	t.Setenv("GRIMMORY_BASE_URL", "https://grimmory.example")
	t.Setenv("GRIMMORY_USERNAME", "user")
	t.Setenv("GRIMMORY_PASSWORD", "password")
	t.Setenv("LIBRARY_IDS", "1")
	t.Setenv("OUTPUT_FORMATS", "../mobi")
	if _, err := Load(); err == nil {
		t.Fatal("expected unsafe format error")
	}
}

func TestBoundedSettingsRejectOutOfRangeValues(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("GRIMMORY_BASE_URL", "https://grimmory.example")
	t.Setenv("GRIMMORY_USERNAME", "user")
	t.Setenv("GRIMMORY_PASSWORD", "password")
	t.Setenv("LIBRARY_IDS", "1")
	t.Setenv("MAX_FILE_BYTES", "0")
	if _, err := Load(); err == nil {
		t.Fatal("expected max file limit error")
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"PORT", "CONVERTER_PORT", "ADDR", "CONVERTER_ADDR", "DATA_DIR", "CONVERTER_DATA_DIR",
		"API_KEY_FILE", "CALIBRE_BINARY", "CONVERTER_CALIBRE_BINARY",
		"LOG_LEVEL", "CONVERTER_LOG_LEVEL", "GRIMMORY_BASE_URL", "GRIMMORY_USERNAME", "GRIMMORY_PASSWORD",
		"LIBRARY_IDS", "OUTPUT_FORMATS", "SUPPORTED_INPUT_FORMATS", "IGNORE_PROCESSING_TAG", "FAILED_PROCESSING_TAG", "MAX_CONCURRENT_BOOKS", "MAX_FILE_BYTES", "MAX_RESPONSE_BYTES",
		"HTTP_TIMEOUT", "CONVERSION_TIMEOUT", "DATABASE_BUSY_TIMEOUT",
		"POLL_INTERVAL", "POLL_MAX_ATTEMPTS", "POLL_RETRY_BASE", "POLL_RETRY_MAX",
	} {
		t.Setenv(name, "")
	}
}

func join(values []string) string {
	result := ""
	for index, value := range values {
		if index > 0 {
			result += ","
		}
		result += value
	}
	return result
}

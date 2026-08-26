package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"converter/internal/logging"
)

const (
	defaultMaxFileBytes       int64 = 100 << 20
	defaultMaxResponseBytes   int64 = 8 << 20
	defaultHTTPTimeout              = 30 * time.Second
	defaultConversionTimeout        = 10 * time.Minute
	defaultBusyTimeout              = 5 * time.Second
	defaultPollInterval             = 5 * time.Minute
	defaultPollMaxAttempts          = 5
	defaultPollRetryBase            = 30 * time.Second
	defaultPollRetryMax             = 15 * time.Minute
	defaultMaxConcurrentBooks       = 1
	maxFormatLength                 = 32
)

var formatPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9+_-]{0,31}$`)

// Config contains runtime settings for the standalone reconciliation service.
type Config struct {
	Addr                  string
	DataDir               string
	APIKeyPath            string
	CalibreBinary         string
	LogLevel              logging.Level
	GrimmoryBaseURL       string
	GrimmoryUsername      string
	GrimmoryPassword      string
	LibraryIDs            []string
	OutputFormats         []string
	SupportedInputFormats []string
	IgnoreProcessingTag   string
	FailedProcessingTag   string
	MaxConcurrentBooks    int
	MaxFileBytes          int64
	MaxResponseBytes      int64
	HTTPTimeout           time.Duration
	ConversionTimeout     time.Duration
	DatabaseBusyTimeout   time.Duration
	PollInterval          time.Duration
	PollMaxAttempts       int
	PollRetryBase         time.Duration
	PollRetryMax          time.Duration
}

// Load reads the service configuration from the environment. The names in
// this function are deliberately not runtime switches: the service always
// uses the deployment-specific Grimmory /api/v1/books/{id}/files endpoint.
func Load() (Config, error) {
	port := firstNonEmpty("PORT", "CONVERTER_PORT")
	if port == "" {
		port = "8080"
	}
	if parsed, err := strconv.Atoi(port); err != nil || parsed < 0 || parsed > 65535 {
		return Config{}, fmt.Errorf("invalid port %q", port)
	}

	addr := firstNonEmpty("ADDR", "CONVERTER_ADDR")
	if addr == "" {
		addr = ":" + port
	}
	dataDir := firstNonEmpty("DATA_DIR", "CONVERTER_DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
	}
	keyPath := firstNonEmpty("API_KEY_FILE")
	if keyPath == "" {
		keyPath = filepath.Join(dataDir, "api-key")
	}
	calibre := firstNonEmpty("CALIBRE_BINARY", "CONVERTER_CALIBRE_BINARY")
	if calibre == "" {
		calibre = "ebook-convert"
	}
	logLevel, err := logging.Parse(firstNonEmpty("LOG_LEVEL", "CONVERTER_LOG_LEVEL"))
	if err != nil {
		return Config{}, err
	}

	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("GRIMMORY_BASE_URL")), "/")
	if baseURL == "" {
		return Config{}, fmt.Errorf("GRIMMORY_BASE_URL is required")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		return Config{}, fmt.Errorf("invalid GRIMMORY_BASE_URL %q", baseURL)
	}
	username, password := os.Getenv("GRIMMORY_USERNAME"), os.Getenv("GRIMMORY_PASSWORD")
	if strings.TrimSpace(username) == "" {
		return Config{}, fmt.Errorf("GRIMMORY_USERNAME is required")
	}
	if password == "" {
		return Config{}, fmt.Errorf("GRIMMORY_PASSWORD is required")
	}

	libraryIDs, err := parseLibraryIDs(os.Getenv("LIBRARY_IDS"))
	if err != nil {
		return Config{}, err
	}
	outputs, err := parseFormats("OUTPUT_FORMATS", firstNonEmpty("OUTPUT_FORMATS"), []string{"mobi", "azw3"})
	if err != nil {
		return Config{}, err
	}
	inputs, err := parseFormats("SUPPORTED_INPUT_FORMATS", firstNonEmpty("SUPPORTED_INPUT_FORMATS"), []string{"epub", "azw3", "mobi"})
	if err != nil {
		return Config{}, err
	}
	ignoreTag := strings.TrimSpace(os.Getenv("IGNORE_PROCESSING_TAG"))
	failedTag := strings.TrimSpace(os.Getenv("FAILED_PROCESSING_TAG"))
	maxConcurrentBooks, err := boundedInt("MAX_CONCURRENT_BOOKS", defaultMaxConcurrentBooks, 1, 16)
	if err != nil {
		return Config{}, err
	}

	maxFileBytes, err := boundedInt64("MAX_FILE_BYTES", defaultMaxFileBytes, 1, 2<<30)
	if err != nil {
		return Config{}, err
	}
	maxResponseBytes, err := boundedInt64("MAX_RESPONSE_BYTES", defaultMaxResponseBytes, 1, 64<<20)
	if err != nil {
		return Config{}, err
	}
	httpTimeout, err := boundedDuration("HTTP_TIMEOUT", defaultHTTPTimeout, time.Millisecond, 10*time.Minute)
	if err != nil {
		return Config{}, err
	}
	conversionTimeout, err := boundedDuration("CONVERSION_TIMEOUT", defaultConversionTimeout, time.Millisecond, 2*time.Hour)
	if err != nil {
		return Config{}, err
	}
	busyTimeout, err := boundedDuration("DATABASE_BUSY_TIMEOUT", defaultBusyTimeout, 0, time.Minute)
	if err != nil {
		return Config{}, err
	}
	pollInterval, err := boundedDuration("POLL_INTERVAL", defaultPollInterval, time.Millisecond, 24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	pollMaxAttempts, err := boundedInt("POLL_MAX_ATTEMPTS", defaultPollMaxAttempts, 1, 1000)
	if err != nil {
		return Config{}, err
	}
	pollRetryBase, err := boundedDuration("POLL_RETRY_BASE", defaultPollRetryBase, time.Millisecond, 24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	pollRetryMax, err := boundedDuration("POLL_RETRY_MAX", defaultPollRetryMax, time.Millisecond, 7*24*time.Hour)
	if err != nil {
		return Config{}, err
	}
	if pollRetryBase > pollRetryMax {
		return Config{}, fmt.Errorf("POLL_RETRY_BASE must not exceed POLL_RETRY_MAX")
	}

	return Config{
		Addr:                  addr,
		DataDir:               dataDir,
		APIKeyPath:            keyPath,
		CalibreBinary:         calibre,
		LogLevel:              logLevel,
		GrimmoryBaseURL:       baseURL,
		GrimmoryUsername:      username,
		GrimmoryPassword:      password,
		LibraryIDs:            append([]string(nil), libraryIDs...),
		OutputFormats:         append([]string(nil), outputs...),
		SupportedInputFormats: append([]string(nil), inputs...),
		IgnoreProcessingTag:   ignoreTag,
		FailedProcessingTag:   failedTag,
		MaxConcurrentBooks:    maxConcurrentBooks,
		MaxFileBytes:          maxFileBytes,
		MaxResponseBytes:      maxResponseBytes,
		HTTPTimeout:           httpTimeout,
		ConversionTimeout:     conversionTimeout,
		DatabaseBusyTimeout:   busyTimeout,
		PollInterval:          pollInterval,
		PollMaxAttempts:       pollMaxAttempts,
		PollRetryBase:         pollRetryBase,
		PollRetryMax:          pollRetryMax,
	}, nil
}

func parseLibraryIDs(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("LIBRARY_IDS is required")
	}
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			return nil, fmt.Errorf("LIBRARY_IDS must be a comma-separated integer allowlist")
		}
		parsed, err := strconv.ParseUint(entry, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid LIBRARY_IDS entry %q", entry)
		}
		id := strconv.FormatUint(parsed, 10)
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("LIBRARY_IDS must not be empty")
	}
	return result, nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func parseFormat(name, value, fallback string) (string, error) {
	if strings.TrimSpace(value) == "" {
		value = fallback
	}
	value = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(value, ".")))
	if !formatPattern.MatchString(value) || len(value) > maxFormatLength {
		return "", fmt.Errorf("invalid %s format %q", name, value)
	}
	return value, nil
}

func parseFormats(name, value string, fallback []string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		return append([]string(nil), fallback...), nil
	}
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, entry := range strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r' }) {
		format := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(entry, ".")))
		if !formatPattern.MatchString(format) || len(format) > maxFormatLength {
			return nil, fmt.Errorf("invalid %s format %q", name, entry)
		}
		if _, exists := seen[format]; !exists {
			seen[format] = struct{}{}
			result = append(result, format)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%s must not be empty", name)
	}
	return result, nil
}

func boundedInt64(name string, fallback, minimum, maximum int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("invalid %s %q", name, value)
	}
	return parsed, nil
}

func boundedInt(name string, fallback, minimum, maximum int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("invalid %s %q", name, value)
	}
	return parsed, nil
}

func boundedDuration(name string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("invalid %s %q", name, value)
	}
	return parsed, nil
}

func firstNonEmpty(names ...string) string {
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			return value
		}
	}
	return ""
}

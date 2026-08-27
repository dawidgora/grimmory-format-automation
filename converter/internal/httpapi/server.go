// Package httpapi exposes the standalone reconciliation API.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"converter/internal/auth"
	"converter/internal/grimmory"
	"converter/internal/logging"
	"converter/internal/reconcile"
)

type Server struct {
	auth    *auth.Authenticator
	service *reconcile.Service
	logger  *logging.Logger
}

var requestSequence uint64

func NewWithLogger(apiKey string, service *reconcile.Service, logger *logging.Logger) *Server {
	if logger == nil {
		logger = logging.New(logging.Info, nil)
	}
	return &Server{auth: auth.New(apiKey), service: service, logger: logger}
}

func (s *Server) Handler() http.Handler { return s }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := requestID(r.Header.Get("X-Request-ID"))
	w.Header().Set("X-Request-ID", id)
	response := &loggingResponseWriter{ResponseWriter: w}
	started := time.Now()
	syncLog := newSyncLog(r)
	s.serveHTTP(response, r, id, syncLog)
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	if s == nil || s.logger == nil || successfulHealth(r, status) {
		return
	}
	fields := []logging.Field{
		{Key: "request_id", Value: id},
		{Key: "method", Value: r.Method},
		{Key: "path", Value: r.URL.Path},
		{Key: "status", Value: strconv.Itoa(status)},
		{Key: "duration", Value: time.Since(started).String()},
	}
	if syncLog != nil && status >= http.StatusBadRequest {
		s.logger.Log(logging.Error, append(fields, syncLog.fields()...)...)
		return
	}
	s.logger.Log(logging.Info, fields...)
}

type syncLog struct {
	libraryID    string
	bookID       string
	dryRun       bool
	force        bool
	resultStatus string
	errorCode    string
	cause        string
	remoteStatus string
}

func newSyncLog(r *http.Request) *syncLog {
	if !strings.HasPrefix(r.URL.Path, "/sync/") {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/sync/"), "/")
	if len(parts) == 2 && reconcile.ValidLibraryID(parts[0]) && reconcile.ValidBookID(parts[1]) {
		return &syncLog{libraryID: parts[0], bookID: parts[1], dryRun: queryBool(r, "dryRun"), force: queryBool(r, "force"), resultStatus: "failed"}
	}
	return &syncLog{dryRun: queryBool(r, "dryRun"), force: queryBool(r, "force"), resultStatus: "failed"}
}

func successfulHealth(r *http.Request, status int) bool {
	return r.URL.Path == "/health" && r.Method == http.MethodGet && status >= http.StatusOK && status < http.StatusMultipleChoices
}

func (entry *syncLog) fail(errorCode, cause string) {
	entry.errorCode = errorCode
	entry.cause = cause
}

func (entry *syncLog) fields() []logging.Field {
	resultStatus := entry.resultStatus
	if resultStatus == "" {
		resultStatus = "failed"
	}
	errorCode := safeResultCode(entry.errorCode)
	if errorCode == "" {
		errorCode = "unknown"
	}
	cause := entry.cause
	if cause == "" {
		cause = "internal"
	}
	fields := []logging.Field{
		{Key: "library_id", Value: entry.libraryID},
		{Key: "book_id", Value: entry.bookID},
		{Key: "dry_run", Value: strconv.FormatBool(entry.dryRun)},
		{Key: "force", Value: strconv.FormatBool(entry.force)},
		{Key: "result_status", Value: safeResultStatus(resultStatus)},
		{Key: "error_code", Value: errorCode},
		{Key: "cause", Value: cause},
	}
	if entry.remoteStatus != "" {
		fields = append(fields, logging.Field{Key: "remote_status", Value: entry.remoteStatus})
	}
	return fields
}

func safeResultCode(value string) string {
	switch value {
	case "invalid_library_id", "library_not_allowed", "library_policy_failed", "invalid_book_id", "service_not_initialized", "get_book_failed", "state_read_failed", "no_source", "workspace_failed", "download_main_failed", "canonical_hash_mismatch", "state_write_failed", "derivative_failed", reconcile.SafeReplacementUnavailableCode, "download_source_failed", "source_hash_mismatch", "main_conversion_failed", "main_hash_failed", "main_upload_failed", "verification_failed", "failure_tag_failed", "invalid_dry_run", "invalid_force", "service_unavailable", "unauthorized", "method_not_allowed", "request_failed":
		return value
	default:
		return ""
	}
}

func safeResultStatus(value string) string {
	switch value {
	case "failed", "partial", "completed", "dry_run":
		return value
	default:
		return "unknown"
	}
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request, _ string, syncEntry *syncLog) {
	switch {
	case r.URL.Path == "/health":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.URL.Path == "/formats":
		if !s.authorized(w, r) {
			return
		}
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		if s == nil || s.service == nil {
			writeError(w, http.StatusInternalServerError, "service unavailable")
			return
		}
		_, inputs, outputs := s.service.Formats()
		writeJSON(w, http.StatusOK, struct {
			Inputs  []string `json:"inputs"`
			Outputs []string `json:"outputs"`
		}{Inputs: inputs, Outputs: outputs})
	case strings.HasPrefix(r.URL.Path, "/sync/"):
		if !s.authorized(w, r) {
			syncEntry.fail("unauthorized", "authorization")
			return
		}
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			syncEntry.fail("method_not_allowed", "validation")
			return
		}
		s.handleSync(w, r, syncEntry)
	default:
		http.NotFound(w, r)
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *loggingResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *loggingResponseWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func requestID(value string) string {
	value = strings.TrimSpace(value)
	if value != "" && len(value) <= 128 {
		valid := true
		for index := 0; index < len(value); index++ {
			if value[index] < 0x21 || value[index] > 0x7e {
				valid = false
				break
			}
		}
		if valid {
			return value
		}
	}
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err == nil {
		return hex.EncodeToString(bytes)
	}
	return fmt.Sprintf("%x-%s", atomic.AddUint64(&requestSequence, 1), strconv.FormatInt(time.Now().UnixNano(), 16))
}

func (s *Server) authorized(w http.ResponseWriter, r *http.Request) bool {
	if s == nil || s.auth == nil || !s.auth.ValidBearer(r.Header.Get("Authorization")) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="reconciliation"`)
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request, syncEntry *syncLog) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/sync/"), "/")
	if len(parts) != 2 || !reconcile.ValidLibraryID(parts[0]) || !reconcile.ValidBookID(parts[1]) {
		syncEntry.fail("invalid_library_id", "validation")
		writeError(w, http.StatusBadRequest, "invalid library or book id")
		return
	}
	libraryID, bookID := parts[0], parts[1]
	if !reconcile.ValidBookID(bookID) || strings.Contains(bookID, "/") {
		syncEntry.fail("invalid_book_id", "validation")
		writeError(w, http.StatusBadRequest, "invalid book id")
		return
	}
	syncEntry.libraryID = libraryID
	syncEntry.bookID = bookID
	dryRun := queryBool(r, "dryRun")
	force := queryBool(r, "force")
	syncEntry.dryRun, syncEntry.force = dryRun, force
	var err error
	if r.URL.Query().Get("dryRun") != "" {
		dryRun, err = parseBool(r.URL.Query().Get("dryRun"))
		if err != nil {
			syncEntry.fail("invalid_dry_run", "validation")
			writeError(w, http.StatusBadRequest, "invalid dryRun")
			return
		}
		syncEntry.dryRun = dryRun
	}
	if r.URL.Query().Get("force") != "" {
		force, err = parseBool(r.URL.Query().Get("force"))
		if err != nil {
			syncEntry.fail("invalid_force", "validation")
			writeError(w, http.StatusBadRequest, "invalid force")
			return
		}
		syncEntry.force = force
	}
	if s == nil || s.service == nil {
		syncEntry.fail("service_unavailable", "internal")
		writeError(w, http.StatusInternalServerError, "service unavailable")
		return
	}
	result, syncErr := s.service.Sync(r.Context(), libraryID, bookID, reconcile.SyncOptions{DryRun: dryRun, Force: force})
	syncEntry.resultStatus = result.Status
	syncEntry.errorCode = result.Error
	if syncErr != nil {
		syncEntry.cause = reconcile.ClassifyError(syncErr)
		syncEntry.remoteStatus = remoteStatus(syncErr)
		if syncEntry.cause == "reconciliation" && result.Error == "verification_failed" {
			syncEntry.cause = "invalid_response"
		}
		status := statusFor(syncErr)
		if result.Error == reconcile.SafeReplacementUnavailableCode {
			status = http.StatusConflict
		}
		writeJSON(w, status, result)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func remoteStatus(err error) string {
	var httpErr *grimmory.HTTPError
	if errors.As(err, &httpErr) && httpErr != nil && httpErr.Status >= 100 && httpErr.Status <= 599 {
		return strconv.Itoa(httpErr.Status)
	}
	var caused interface{ Cause() error }
	if errors.As(err, &caused) {
		if cause := caused.Cause(); cause != nil {
			return remoteStatus(cause)
		}
	}
	return ""
}

func queryBool(r *http.Request, key string) bool {
	return strings.EqualFold(r.URL.Query().Get(key), "true")
}

func parseBool(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, errors.New("boolean must be true or false")
	}
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, reconcile.ErrInvalidBookID):
		return http.StatusBadRequest
	case errors.Is(err, reconcile.ErrInvalidLibraryID):
		return http.StatusBadRequest
	case errors.Is(err, reconcile.ErrLibraryNotAllowed):
		return http.StatusForbidden
	case errors.Is(err, reconcile.ErrNoSource):
		return http.StatusUnprocessableEntity
	case errors.Is(err, reconcile.ErrSafeReplacementUnavailable):
		return http.StatusConflict
	case errors.Is(err, grimmory.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, reconcile.ErrState):
		return http.StatusInternalServerError
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

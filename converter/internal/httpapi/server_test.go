package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"converter/internal/grimmory"
	"converter/internal/logging"
	"converter/internal/reconcile"
	"converter/internal/state"
)

type endpointRemote struct{}

func (endpointRemote) GetLibrary(context.Context, string) (grimmory.Library, error) {
	return grimmory.Library{ID: "1", FormatPriority: []string{"epub", "mobi"}}, nil
}
func (endpointRemote) GetLibraryBook(context.Context, string, string) (grimmory.Book, error) {
	return grimmory.Book{ID: "book-1", LibraryID: "1", Files: []grimmory.File{{Format: "epub", Name: "book.epub"}, {Format: "mobi", Name: "book.mobi"}}}, nil
}
func (endpointRemote) DownloadContentScoped(context.Context, grimmory.BookReference, string, io.Writer) (int64, string, error) {
	return 0, "", nil
}
func (endpointRemote) UploadFileNamedScoped(context.Context, grimmory.BookReference, string, string, string) error {
	return nil
}
func (endpointRemote) UploadFileScoped(context.Context, grimmory.BookReference, string, string) error {
	return nil
}
func (endpointRemote) DeleteFileScoped(context.Context, grimmory.BookReference, string) error {
	return nil
}

type failingEndpointRemote struct{ err error }

func (remote failingEndpointRemote) GetLibrary(context.Context, string) (grimmory.Library, error) {
	return grimmory.Library{ID: "1", FormatPriority: []string{"epub"}}, nil
}
func (remote failingEndpointRemote) GetLibraryBook(context.Context, string, string) (grimmory.Book, error) {
	return grimmory.Book{}, remote.err
}
func (failingEndpointRemote) DownloadContentScoped(context.Context, grimmory.BookReference, string, io.Writer) (int64, string, error) {
	return 0, "", nil
}
func (failingEndpointRemote) UploadFileNamedScoped(context.Context, grimmory.BookReference, string, string, string) error {
	return nil
}
func (failingEndpointRemote) UploadFileScoped(context.Context, grimmory.BookReference, string, string) error {
	return nil
}
func (failingEndpointRemote) DeleteFileScoped(context.Context, grimmory.BookReference, string) error {
	return nil
}

type endpointStore struct{}

func (endpointStore) Get(context.Context, string, string) (state.BookState, map[string]state.DerivedState, error) {
	return state.BookState{}, map[string]state.DerivedState{}, nil
}
func (endpointStore) SetBook(context.Context, state.BookState) error       { return nil }
func (endpointStore) SetDerived(context.Context, state.DerivedState) error { return nil }

type endpointConverter struct{}

func (endpointConverter) Convert(context.Context, string, string, string, string) (string, error) {
	return "", nil
}

func endpointServer(t *testing.T) *Server {
	return endpointServerWithLogger(t, endpointRemote{}, logging.New(logging.Info, io.Discard))
}

func endpointServerWithLogger(t *testing.T, remote reconcile.Remote, logger *logging.Logger) *Server {
	t.Helper()
	service := reconcile.New(reconcile.Options{
		Client:             remote,
		Store:              endpointStore{},
		Converter:          endpointConverter{},
		LibraryIDs:         []string{"1"},
		OutputFormats:      []string{"mobi", "azw3"},
		SupportedInputs:    []string{"epub", "mobi", "azw3"},
		MaxConcurrentBooks: 1,
		MaxFileBytes:       1 << 20,
		ConversionTimeout:  10 * time.Minute,
	})
	return NewWithLogger("secret", service, logger)
}

func TestSuccessfulHealthIsNotAccessLogged(t *testing.T) {
	var output bytes.Buffer
	server := endpointServerWithLogger(t, endpointRemote{}, logging.New(logging.Info, &output))
	response := httptest.NewRecorder()
	server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("health status = %d", response.Code)
	}
	if output.Len() != 0 {
		t.Fatalf("successful health request was logged: %s", output.String())
	}
}

func TestFailedSyncAccessLogHasSafeOperationalContext(t *testing.T) {
	const (
		password  = "do-not-log-password"
		token     = "do-not-log-token"
		workspace = "/private/tmp/reconciliation-workspace"
	)
	remoteErr := fmt.Errorf("remote response password=%s token=%s path=%s: %w", password, token, workspace, &grimmory.HTTPError{Operation: "get book", Status: http.StatusBadGateway})
	var output bytes.Buffer
	server := endpointServerWithLogger(t, failingEndpointRemote{err: remoteErr}, logging.New(logging.Info, &output))
	request := httptest.NewRequest(http.MethodPost, "/sync/1/book-1?dryRun=true&force=false", nil)
	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("X-Request-ID", "request-123")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)

	if response.Code != http.StatusBadGateway {
		t.Fatalf("sync status = %d body=%s", response.Code, response.Body.String())
	}
	logOutput := output.String()
	for _, field := range []string{
		"level=error",
		"request_id=request-123",
		"library_id=1",
		"book_id=book-1",
		"dry_run=true",
		"force=false",
		"status=502",
		"result_status=failed",
		"error_code=get_book_failed",
		"cause=remote_http_status",
		"remote_status=502",
	} {
		if !strings.Contains(logOutput, field) {
			t.Errorf("log missing %q: %s", field, logOutput)
		}
	}
	for _, secret := range []string{password, token, workspace, "remote response"} {
		if strings.Contains(logOutput, secret) || strings.Contains(response.Body.String(), secret) {
			t.Errorf("secret or internal detail leaked for %q: log=%s body=%s", secret, logOutput, response.Body.String())
		}
	}
}

func TestHealthIsUnauthenticatedAndProtectedRoutesRequireBearer(t *testing.T) {
	server := endpointServer(t)
	health := httptest.NewRecorder()
	server.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK || health.Header().Get("X-Request-ID") == "" {
		t.Fatalf("health status = %d, headers = %v", health.Code, health.Header())
	}
	for _, path := range []string{"/formats", "/sync/1/book-1"} {
		response := httptest.NewRecorder()
		server.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s unauthenticated status = %d", path, response.Code)
		}
	}
}

func TestFormatsAndSyncDryRunAndRouteRemoval(t *testing.T) {
	server := endpointServer(t)
	formatsRequest := httptest.NewRequest(http.MethodGet, "/formats", nil)
	formatsRequest.Header.Set("Authorization", "Bearer secret")
	formats := httptest.NewRecorder()
	server.ServeHTTP(formats, formatsRequest)
	if formats.Code != http.StatusOK || strings.Contains(formats.Body.String(), `"main"`) || !strings.Contains(formats.Body.String(), `"azw3"`) {
		t.Fatalf("formats response = %d %s", formats.Code, formats.Body.String())
	}

	syncRequest := httptest.NewRequest(http.MethodPost, "/sync/1/book-1?dryRun=true&force=true", nil)
	syncRequest.Header.Set("Authorization", "Bearer secret")
	sync := httptest.NewRecorder()
	server.ServeHTTP(sync, syncRequest)
	if sync.Code != http.StatusOK || !strings.Contains(sync.Body.String(), `"status":"dry_run"`) || !strings.Contains(sync.Body.String(), `"force":true`) {
		t.Fatalf("dry run response = %d %s", sync.Code, sync.Body.String())
	}
	for _, path := range []string{"/convert", "/convert/", "/library/scan", "/grimmory/books"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", path, response.Code)
		}
	}
}

func TestSyncQueryAndBookIDValidation(t *testing.T) {
	server := endpointServer(t)
	for _, target := range []string{"/sync/1/book?dryRun=maybe", "/sync/a%2Fb/book", "/sync/"} {
		request := httptest.NewRequest(http.MethodPost, target, nil)
		request.Header.Set("Authorization", "Bearer secret")
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d body=%s", target, response.Code, response.Body.String())
		}
	}
	disallowed := httptest.NewRequest(http.MethodPost, "/sync/2/book", nil)
	disallowed.Header.Set("Authorization", "Bearer secret")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, disallowed)
	if response.Code != http.StatusForbidden {
		t.Fatalf("disallowed library status = %d body=%s", response.Code, response.Body.String())
	}
}

// Package grimmory is the narrow HTTP client used by reconciliation. It never
// accepts or constructs a local Grimmory library path.
package grimmory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

var (
	ErrInvalidBaseURL   = errors.New("invalid Grimmory base URL")
	ErrUnauthorized     = errors.New("Grimmory authentication failed")
	ErrNotFound         = errors.New("Grimmory resource not found")
	ErrResponseTooBig   = errors.New("Grimmory response is too large")
	ErrInvalidResponse  = errors.New("Grimmory response is invalid")
	ErrInvalidID        = errors.New("invalid Grimmory resource ID")
	ErrBookNotInLibrary = errors.New("Grimmory book does not belong to requested library")
	ErrFileNotFound     = errors.New("Grimmory file not found")
	ErrTagVerification  = errors.New("Grimmory tag verification failed")
	ErrEmptyTag         = errors.New("book tag is empty")
)

type HTTPError struct {
	Operation string
	Status    int
}

func (e *HTTPError) Error() string {
	if e == nil {
		return "Grimmory request failed"
	}
	return fmt.Sprintf("Grimmory %s failed with HTTP %d", e.Operation, e.Status)
}

func (e *HTTPError) Unwrap() error {
	if e != nil && e.Status == http.StatusUnauthorized {
		return ErrUnauthorized
	}
	if e != nil && e.Status == http.StatusNotFound {
		return ErrNotFound
	}
	return nil
}

// File is a file reported by GET /api/v1/books/{bookId}. MTime is trusted only
// when it came from one of the known timestamp fields and parsed successfully.
type File struct {
	ID                  string
	Name                string
	Format              string
	Type                string
	SizeKB              int64
	SHA256              string
	MetadataFingerprint string
	MTime               time.Time
	TrustedMTime        bool
}

type BookMetadata struct {
	Title           string
	Authors         []string
	Language        string
	Publisher       string
	PublicationDate string
	Identifiers     map[string]string
	Series          string
	SeriesIndex     string
	Tags            []string
	Description     string
	Comments        string
}

type Book struct {
	ID        string
	LibraryID string
	Files     []File
	Metadata  BookMetadata

	metadataSnapshot map[string]any
}

// BookReference identifies a book through its owning library. Mutating API
// calls use this reference to prove membership before writing to the global
// book endpoint.
type BookReference struct {
	LibraryID string
	BookID    string
}

func (reference BookReference) validate() error {
	if err := validateID(reference.LibraryID); err != nil {
		return err
	}
	return validateID(reference.BookID)
}

// Library is the normalized policy object returned by Grimmory. A nil
// AllowedFormats means that the API returned null (or omitted the field),
// while a non-nil empty slice means that it explicitly returned [].
type Library struct {
	ID             string
	Name           string
	FormatPriority []string
	AllowedFormats []string
}

type Client struct {
	baseURL      *url.URL
	username     string
	password     string
	httpClient   *http.Client
	maxResponse  int64
	maxFileBytes int64
	timeout      time.Duration

	mu           sync.Mutex
	accessToken  string
	refreshToken string
}

// New creates a client for the deployment-specific Grimmory API. The caller
// may inject an HTTP client for tests; its transport is never given credentials
// outside requests to baseURL.
func New(baseURL, username, password string, httpClient *http.Client, maxResponse, maxFileBytes int64, timeout time.Duration) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, ErrInvalidBaseURL
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	if httpClient.CheckRedirect == nil {
		copy := *httpClient
		copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		httpClient = &copy
	}
	if maxResponse <= 0 {
		maxResponse = 8 << 20
	}
	if maxFileBytes <= 0 {
		maxFileBytes = 100 << 20
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{baseURL: parsed, username: username, password: password, httpClient: httpClient, maxResponse: maxResponse, maxFileBytes: maxFileBytes, timeout: timeout}, nil
}

// GetBook retrieves full book metadata, including the description, and
// normalizes the response. The parser accepts the common direct, data, and
// book envelopes used by Grimmory releases.
func (c *Client) GetBook(ctx context.Context, bookID string) (Book, error) {
	if err := validateID(bookID); err != nil {
		return Book{}, err
	}
	response, err := c.do(ctx, http.MethodGet, c.bookPath(bookID)+"?withDescription=true", nil)
	if err != nil {
		return Book{}, err
	}
	defer response.Body.Close()
	body, err := readBounded(response.Body, c.maxResponse)
	if err != nil {
		return Book{}, err
	}
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return Book{}, fmt.Errorf("%w: decode Grimmory book response: %v", ErrInvalidResponse, err)
	}
	book, ok := parseBook(raw, bookID)
	if !ok {
		return Book{}, fmt.Errorf("%w: Grimmory book response has no book object", ErrInvalidResponse)
	}
	if book.ID != bookID {
		return Book{}, fmt.Errorf("%w: Grimmory book identity mismatch", ErrInvalidResponse)
	}
	return book, nil
}

// GetLibrary retrieves one normalized library policy object. The library
// endpoint is intentionally direct and unpaginated; unlike the global book
// endpoint, no envelope or page traversal is accepted here.
func (c *Client) GetLibrary(ctx context.Context, libraryID string) (Library, error) {
	if err := validateID(libraryID); err != nil {
		return Library{}, err
	}
	body, err := c.getBounded(ctx, libraryPath(libraryID))
	if err != nil {
		return Library{}, err
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return Library{}, fmt.Errorf("%w: decode Grimmory library response: %v", ErrInvalidResponse, err)
	}
	library, ok, err := parseLibrary(raw)
	if err != nil {
		return Library{}, err
	}
	if !ok || library.ID != libraryID {
		return Library{}, fmt.Errorf("%w: library identity mismatch", ErrInvalidResponse)
	}
	return library, nil
}

// ListLibraryBooks retrieves the direct, unpaginated Book[] for one library.
// IDs are validated and duplicate IDs are rejected before any result is
// returned so callers never reconcile an ambiguous membership set.
func (c *Client) ListLibraryBooks(ctx context.Context, libraryID string) ([]Book, error) {
	if err := validateID(libraryID); err != nil {
		return nil, err
	}
	body, err := c.getBounded(ctx, libraryPath(libraryID)+"/book")
	if err != nil {
		return nil, err
	}
	var values []json.RawMessage
	if len(bytes.TrimSpace(body)) == 0 || bytes.Equal(bytes.TrimSpace(body), []byte("null")) || json.Unmarshal(body, &values) != nil {
		return nil, fmt.Errorf("%w: library books response is not a direct array", ErrInvalidResponse)
	}
	books := make([]Book, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		var raw map[string]any
		if err := json.Unmarshal(value, &raw); err != nil {
			return nil, fmt.Errorf("%w: decode Grimmory library book: %v", ErrInvalidResponse, err)
		}
		book, ok := parseLibraryBook(raw)
		if !ok || book.ID == "" {
			return nil, fmt.Errorf("%w: library book has no valid ID", ErrInvalidResponse)
		}
		if err := validateID(book.ID); err != nil {
			return nil, err
		}
		if _, duplicate := seen[book.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate book ID %q", ErrInvalidResponse, book.ID)
		}
		seen[book.ID] = struct{}{}
		if book.LibraryID == "" {
			book.LibraryID = libraryID
		}
		if book.LibraryID != libraryID {
			return nil, fmt.Errorf("%w: book %q is outside library", ErrBookNotInLibrary, book.ID)
		}
		books = append(books, book)
	}
	return books, nil
}

func (c *Client) GetLibraryBooks(ctx context.Context, libraryID string) ([]Book, error) {
	return c.ListLibraryBooks(ctx, libraryID)
}

// GetLibraryBook retrieves a book through a library endpoint and verifies both
// requested identities.
func (c *Client) GetLibraryBook(ctx context.Context, libraryID, bookID string) (Book, error) {
	if err := validateID(libraryID); err != nil {
		return Book{}, err
	}
	if err := validateID(bookID); err != nil {
		return Book{}, err
	}
	body, err := c.getBounded(ctx, libraryPath(libraryID)+"/book/"+url.PathEscape(bookID))
	if err != nil {
		return Book{}, err
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return Book{}, fmt.Errorf("%w: decode Grimmory library book response: %v", ErrInvalidResponse, err)
	}
	book, ok := parseLibraryBook(raw)
	if !ok || book.ID != bookID {
		return Book{}, fmt.Errorf("%w: library book identity mismatch", ErrInvalidResponse)
	}
	if book.LibraryID != libraryID {
		return Book{}, fmt.Errorf("%w: book %q is outside library", ErrBookNotInLibrary, bookID)
	}
	return book, nil
}

// SetBookTagScoped safely adds or removes one exact tag. It first reads the
// complete metadata snapshot, avoids a PUT when the requested state already
// holds, proves library membership immediately before the metadata PUT, and
// verifies the resulting tag set with a fresh full-book read. The update only
// provides tags so concurrent changes to other metadata fields are preserved.
func (c *Client) SetBookTagScoped(ctx context.Context, reference BookReference, tag string, present bool) error {
	if err := reference.validate(); err != nil {
		return err
	}
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ErrEmptyTag
	}
	book, err := c.GetBook(ctx, reference.BookID)
	if err != nil {
		return err
	}
	if err := c.verifyBookMembership(ctx, reference); err != nil {
		return err
	}
	current := normalizedTagSet(book.Metadata.Tags)
	if tagAlreadyDesired(book, tag, present) {
		return nil
	}
	if present {
		current[tag] = struct{}{}
	} else {
		delete(current, tag)
	}
	tags := make([]string, 0, len(current))
	for value := range current {
		tags = append(tags, value)
	}
	sort.Strings(tags)
	clearTags := !present && len(tags) == 0
	encoded, err := json.Marshal(map[string]any{
		"metadata":   map[string]any{"tags": tags},
		"clearFlags": map[string]any{"tags": clearTags},
	})
	if err != nil {
		return fmt.Errorf("encode Grimmory metadata update: %w", err)
	}
	response, err := c.doWithPreflight(ctx, http.MethodPut, c.bookPath(reference.BookID)+"/metadata?replaceMode=REPLACE_WHEN_PROVIDED", func() (io.ReadCloser, string, error) {
		return io.NopCloser(bytes.NewReader(encoded)), "application/json", nil
	}, func(ctx context.Context) error {
		return c.verifyBookMembership(ctx, reference)
	})
	if err != nil {
		return err
	}
	responseBody, readErr := readBounded(response.Body, c.maxResponse)
	closeErr := response.Body.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	_ = responseBody
	verified, err := c.GetBook(ctx, reference.BookID)
	if err != nil {
		return err
	}
	if !sameTagSet(normalizedTagSet(verified.Metadata.Tags), current) || (present && tagCount(verified, tag) != 1) || (!present && tagCount(verified, tag) != 0) {
		return ErrTagVerification
	}
	return nil
}

func (c *Client) AddBookTagScoped(ctx context.Context, reference BookReference, tag string) error {
	return c.SetBookTagScoped(ctx, reference, tag, true)
}

func (c *Client) RemoveBookTagScoped(ctx context.Context, reference BookReference, tag string) error {
	return c.SetBookTagScoped(ctx, reference, tag, false)
}

func (c *Client) verifyBookMembership(ctx context.Context, reference BookReference) error {
	_, err := c.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	return err
}

func (c *Client) getBounded(ctx context.Context, requestPath string) ([]byte, error) {
	response, err := c.do(ctx, http.MethodGet, requestPath, nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	return readBounded(response.Body, c.maxResponse)
}

func libraryPath(libraryID string) string {
	return "/api/v1/libraries/" + url.PathEscape(libraryID)
}

func validateID(value string) error {
	if value == "" || value == "." || value == ".." || len(value) > 256 || strings.TrimSpace(value) != value {
		return ErrInvalidID
	}
	for _, char := range value {
		if char == '/' || char == '\\' || char < 0x20 || char == 0x7f {
			return ErrInvalidID
		}
	}
	return nil
}

func parseLibrary(object map[string]any) (Library, bool, error) {
	if object == nil {
		return Library{}, false, nil
	}
	id := stringValue(object, "id", "libraryId", "library_id")
	if id == "" {
		return Library{}, false, nil
	}
	if err := validateID(id); err != nil {
		return Library{}, false, err
	}
	priority, err := libraryFormats(object, "formatPriority", false)
	if err != nil {
		return Library{}, false, err
	}
	allowed, err := libraryFormats(object, "allowedFormats", true)
	if err != nil {
		return Library{}, false, err
	}
	return Library{ID: id, Name: strings.TrimSpace(stringValue(object, "name", "title")), FormatPriority: priority, AllowedFormats: allowed}, true, nil
}

func parseLibraryBook(object map[string]any) (Book, bool) {
	if book, ok := parseBook(object, ""); ok {
		return book, true
	}
	if !emptyBookInventory(object) {
		return Book{}, false
	}
	id := stringValue(object, "id", "bookId", "book_id")
	if id == "" {
		return Book{}, false
	}
	libraryID := stringValue(object, "libraryId", "library_id")
	if libraryID == "" {
		if library, ok := object["library"].(map[string]any); ok {
			libraryID = stringValue(library, "id", "libraryId", "library_id")
		}
	}
	return Book{ID: id, LibraryID: libraryID, Metadata: normalizeBookMetadata(object), metadataSnapshot: captureMetadataSnapshot(object)}, true
}

func emptyBookInventory(object map[string]any) bool {
	for _, key := range []string{"files", "bookFiles", "items", "results"} {
		if value, present := object[key]; present && !emptyArrayOrNull(value) {
			return false
		}
	}
	if value, present := object["primaryFile"]; present && value != nil {
		return false
	}
	if value, present := object["alternativeFormats"]; present && !emptyArrayOrNull(value) {
		return false
	}
	return true
}

func emptyArrayOrNull(value any) bool {
	if value == nil {
		return true
	}
	items, ok := value.([]any)
	return ok && len(items) == 0
}

func libraryFormats(object map[string]any, key string, nullable bool) ([]string, error) {
	value, exists := object[key]
	if !exists || value == nil {
		if nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: library %s is missing", ErrInvalidResponse, key)
	}
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: library %s is not an array", ErrInvalidResponse, key)
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		format, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%w: library %s contains a non-string format", ErrInvalidResponse, key)
		}
		format = normalizeLibraryFormat(format)
		if format == "" || !validLibraryFormat(format) {
			return nil, fmt.Errorf("%w: library %s contains an invalid format", ErrInvalidResponse, key)
		}
		if _, duplicate := seen[format]; duplicate {
			continue
		}
		seen[format] = struct{}{}
		result = append(result, format)
	}
	return result, nil
}

func normalizeLibraryFormat(value string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "."))
}

func validLibraryFormat(value string) bool {
	if value == "" || len(value) > 32 {
		return false
	}
	for index, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '+' && char != '_' && char != '-' {
			return false
		}
		if index == 0 && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

// ListBooks retrieves every page from the deployment-specific book endpoint.
// The deployment rejects sort parameters, so the client intentionally relies
// on the API's page order and validates the page metadata instead.
func (c *Client) ListBooks(ctx context.Context) ([]Book, error) {
	const pageSize = 100
	books := make([]Book, 0)
	seenIDs := make(map[string]struct{})
	firstPage, err := c.getBookPage(ctx, 0, pageSize)
	if err != nil {
		return nil, err
	}
	expectedTotalPages := firstPage.TotalPages
	pageCount := expectedTotalPages
	if pageCount == 0 {
		pageCount = 1
	}
	if firstPage.Terminal != (pageCount == 1) {
		return nil, fmt.Errorf("%w: Grimmory books page 0 has inconsistent terminal state", ErrInvalidResponse)
	}
	addPage := func(bookPage parsedBookPage, pageNumber int) error {
		for _, value := range bookPage.Content {
			book, ok := parseBook(value, "")
			if !ok || book.ID == "" {
				return fmt.Errorf("%w: Grimmory books page %d contains an invalid book", ErrInvalidResponse, pageNumber)
			}
			if err := validateID(book.ID); err != nil {
				return err
			}
			if _, duplicate := seenIDs[book.ID]; duplicate {
				return fmt.Errorf("%w: Grimmory books page contains duplicate book ID %q", ErrInvalidResponse, book.ID)
			}
			seenIDs[book.ID] = struct{}{}
			books = append(books, book)
		}
		return nil
	}
	if err := addPage(firstPage, 0); err != nil {
		return nil, err
	}
	for pageNumber := 1; pageNumber < pageCount; pageNumber++ {
		bookPage, err := c.getBookPage(ctx, pageNumber, pageSize)
		if err != nil {
			return nil, err
		}
		if bookPage.TotalPages != expectedTotalPages {
			return nil, fmt.Errorf("%w: Grimmory books page %d changed total pages", ErrInvalidResponse, pageNumber)
		}
		if bookPage.Terminal != (pageNumber == pageCount-1) {
			return nil, fmt.Errorf("%w: Grimmory books page %d has inconsistent terminal state", ErrInvalidResponse, pageNumber)
		}
		if err := addPage(bookPage, pageNumber); err != nil {
			return nil, err
		}
	}
	return books, nil
}

func (c *Client) getBookPage(ctx context.Context, pageNumber, pageSize int) (parsedBookPage, error) {
	requestPath := "/api/v1/books/page?page=" + strconv.Itoa(pageNumber) + "&size=" + strconv.Itoa(pageSize)
	response, err := c.do(ctx, http.MethodGet, requestPath, nil)
	if err != nil {
		return parsedBookPage{}, err
	}
	body, readErr := readBounded(response.Body, c.maxResponse)
	_ = response.Body.Close()
	if readErr != nil {
		return parsedBookPage{}, readErr
	}
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return parsedBookPage{}, fmt.Errorf("%w: decode Grimmory books page: %v", ErrInvalidResponse, err)
	}
	bookPage, ok := parseBookPage(raw, pageNumber)
	if !ok {
		return parsedBookPage{}, fmt.Errorf("%w: Grimmory books page %d is invalid", ErrInvalidResponse, pageNumber)
	}
	return bookPage, nil
}

const observationFingerprintVersion = "v1"

// ObservationFingerprint returns the versioned digest of the stable book
// observation. It deliberately excludes remote timestamps, checksums, the
// per-file metadata fingerprint, and fields not represented in the normalized
// model.
func ObservationFingerprint(book Book) string {
	projection := observationBookProjection{
		BookID: strings.TrimSpace(book.ID),
		Files:  make([]observationFileProjection, 0, len(book.Files)),
		Metadata: observationMetadataProjection{
			Authors:     make([]string, 0, len(book.Metadata.Authors)),
			Identifiers: make([]observationIdentifierProjection, 0, len(book.Metadata.Identifiers)),
			Tags:        make([]string, 0, len(book.Metadata.Tags)),
		},
	}
	for _, file := range book.Files {
		format := normalizeObservationToken(file.Format)
		typeName := normalizeObservationToken(file.Type)
		if format == "" {
			format = typeName
		}
		if typeName == "" {
			typeName = format
		}
		projection.Files = append(projection.Files, observationFileProjection{
			ID:     strings.TrimSpace(file.ID),
			Name:   strings.TrimSpace(file.Name),
			Format: format,
			Type:   typeName,
			SizeKB: file.SizeKB,
		})
	}
	for _, author := range book.Metadata.Authors {
		if author = strings.TrimSpace(author); author != "" {
			projection.Metadata.Authors = append(projection.Metadata.Authors, author)
		}
	}
	for key, value := range book.Metadata.Identifiers {
		key, value = strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(value)
		if key != "" && value != "" {
			projection.Metadata.Identifiers = append(projection.Metadata.Identifiers, observationIdentifierProjection{Key: key, Value: value})
		}
	}
	sort.SliceStable(projection.Metadata.Identifiers, func(i, j int) bool {
		if projection.Metadata.Identifiers[i].Key != projection.Metadata.Identifiers[j].Key {
			return projection.Metadata.Identifiers[i].Key < projection.Metadata.Identifiers[j].Key
		}
		return projection.Metadata.Identifiers[i].Value < projection.Metadata.Identifiers[j].Value
	})
	for _, tag := range book.Metadata.Tags {
		if tag = strings.TrimSpace(tag); tag != "" {
			projection.Metadata.Tags = append(projection.Metadata.Tags, tag)
		}
	}
	sort.Strings(projection.Metadata.Tags)
	uniqueTags := projection.Metadata.Tags[:0]
	for _, tag := range projection.Metadata.Tags {
		if len(uniqueTags) == 0 || uniqueTags[len(uniqueTags)-1] != tag {
			uniqueTags = append(uniqueTags, tag)
		}
	}
	projection.Metadata.Tags = uniqueTags
	projection.Metadata.Title = strings.TrimSpace(book.Metadata.Title)
	projection.Metadata.Language = strings.TrimSpace(book.Metadata.Language)
	projection.Metadata.Publisher = strings.TrimSpace(book.Metadata.Publisher)
	projection.Metadata.PublicationDate = strings.TrimSpace(book.Metadata.PublicationDate)
	projection.Metadata.Series = strings.TrimSpace(book.Metadata.Series)
	projection.Metadata.SeriesIndex = strings.TrimSpace(book.Metadata.SeriesIndex)
	projection.Metadata.Description = strings.TrimSpace(book.Metadata.Description)
	projection.Metadata.Comments = strings.TrimSpace(book.Metadata.Comments)

	encoded, _ := json.Marshal(projection)
	digest := sha256.Sum256(encoded)
	return observationFingerprintVersion + ":" + hex.EncodeToString(digest[:])
}

// ObservationFingerprintIgnoringTags excludes service-managed tags from the
// polling checkpoint. Adding/removing an operational tag must not create a
// new observation or consume a reconciliation retry attempt.
func ObservationFingerprintIgnoringTags(book Book, ignored ...string) string {
	if len(ignored) == 0 {
		return ObservationFingerprint(book)
	}
	ignoredSet := make(map[string]struct{}, len(ignored))
	for _, tag := range ignored {
		if tag = strings.TrimSpace(tag); tag != "" {
			ignoredSet[tag] = struct{}{}
		}
	}
	if len(ignoredSet) == 0 {
		return ObservationFingerprint(book)
	}
	copyBook := book
	copyBook.Metadata.Tags = make([]string, 0, len(book.Metadata.Tags))
	for _, tag := range book.Metadata.Tags {
		if _, skip := ignoredSet[tag]; !skip {
			copyBook.Metadata.Tags = append(copyBook.Metadata.Tags, tag)
		}
	}
	return ObservationFingerprint(copyBook)
}

type observationBookProjection struct {
	BookID   string                        `json:"bookId"`
	Files    []observationFileProjection   `json:"files"`
	Metadata observationMetadataProjection `json:"metadata"`
}

type observationFileProjection struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Format string `json:"format"`
	Type   string `json:"type"`
	SizeKB int64  `json:"sizeKb"`
}

type observationMetadataProjection struct {
	Title           string                            `json:"title"`
	Authors         []string                          `json:"authors"`
	Language        string                            `json:"language"`
	Publisher       string                            `json:"publisher"`
	PublicationDate string                            `json:"publicationDate"`
	Identifiers     []observationIdentifierProjection `json:"identifiers"`
	Series          string                            `json:"series"`
	SeriesIndex     string                            `json:"seriesIndex"`
	Tags            []string                          `json:"tags"`
	Description     string                            `json:"description"`
	Comments        string                            `json:"comments"`
}

type observationIdentifierProjection struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func normalizeObservationToken(value string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "."))
}

// DownloadContent streams a book file into dst with a strict size bound. It
// returns bytes and a hash calculated while copying, so callers need not keep
// an ebook in memory.
func (c *Client) DownloadContent(ctx context.Context, bookID, format string, dst io.Writer) (int64, string, error) {
	return c.downloadContent(ctx, bookID, format, dst)
}

// DownloadContentScoped is the reconciliation download path. The reference
// is validated even though Grimmory's content endpoint is book-global.
func (c *Client) DownloadContentScoped(ctx context.Context, reference BookReference, format string, dst io.Writer) (int64, string, error) {
	if err := reference.validate(); err != nil {
		return 0, "", err
	}
	if err := c.verifyBookMembership(ctx, reference); err != nil {
		return 0, "", err
	}
	return c.downloadContent(ctx, reference.BookID, format, dst)
}

func (c *Client) downloadContent(ctx context.Context, bookID, format string, dst io.Writer) (int64, string, error) {
	if dst == nil {
		return 0, "", errors.New("download destination is nil")
	}
	if err := validateID(bookID); err != nil {
		return 0, "", err
	}
	endpoint := c.bookPath(bookID) + "/content?bookType=" + url.QueryEscape(strings.ToLower(strings.TrimSpace(format)))
	response, err := c.do(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	if response.ContentLength > c.maxFileBytes {
		return 0, "", ErrResponseTooBig
	}
	digest := sha256.New()
	count, err := io.Copy(io.MultiWriter(dst, digest), io.LimitReader(response.Body, c.maxFileBytes+1))
	if err != nil {
		return 0, "", fmt.Errorf("download Grimmory content: %w", err)
	}
	if count > c.maxFileBytes {
		return 0, "", ErrResponseTooBig
	}
	return count, fmt.Sprintf("%x", digest.Sum(nil)), nil
}

// UploadFileScoped posts the deployment-specific multipart shape after
// proving that the referenced book belongs to the referenced library. A
// successful return means only that POST succeeded; callers must call GetBook
// afterwards and verify the target file before recording state.
func (c *Client) UploadFileScoped(ctx context.Context, reference BookReference, format, filePath string) error {
	if err := reference.validate(); err != nil {
		return err
	}
	format = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(format), "."))
	return c.uploadFileScoped(ctx, reference, format, filePath, "book."+format)
}

// UploadFileNamedScoped preserves the source filename during upload.
func (c *Client) UploadFileNamedScoped(ctx context.Context, reference BookReference, format, filePath, filename string) error {
	if err := reference.validate(); err != nil {
		return err
	}
	format = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(format), "."))
	return c.uploadFileScoped(ctx, reference, format, filePath, safeUploadFilename(filename, format))
}

// DeleteFileScoped deletes one exact file after proving that the referenced
// book still belongs to the referenced library. A 404 from the delete itself
// is distinguished from a membership failure so reconciliation can safely
// continue when another actor removed the planned replacement first.
func (c *Client) DeleteFileScoped(ctx context.Context, reference BookReference, fileID string) error {
	if err := reference.validate(); err != nil {
		return err
	}
	if err := validateID(fileID); err != nil {
		return err
	}
	response, err := c.doWithPreflight(ctx, http.MethodDelete, c.bookPath(reference.BookID)+"/files/"+url.PathEscape(fileID), nil, func(ctx context.Context) error {
		return c.verifyBookMembership(ctx, reference)
	})
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == http.StatusNotFound && httpErr.Operation == strings.ToLower(http.MethodDelete) {
			return fmt.Errorf("%w: %w", ErrFileNotFound, err)
		}
		return err
	}
	defer response.Body.Close()
	_, readErr := readBounded(response.Body, c.maxResponse)
	if readErr != nil {
		return readErr
	}
	return nil
}

func (c *Client) uploadFileScoped(ctx context.Context, reference BookReference, format, filePath, filename string) error {
	if c == nil {
		return errors.New("Grimmory client is not initialized")
	}
	info, err := os.Lstat(filePath)
	if err != nil {
		return fmt.Errorf("inspect upload file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("upload file is not regular")
	}
	if info.Size() > c.maxFileBytes {
		return ErrResponseTooBig
	}
	response, err := c.doWithPreflight(ctx, http.MethodPost, c.bookPath(reference.BookID)+"/files", func() (io.ReadCloser, string, error) {
		return multipartFileBodyNamed(filePath, format, filename, c.maxFileBytes)
	}, func(ctx context.Context) error {
		return c.verifyBookMembership(ctx, reference)
	})
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = readBounded(response.Body, c.maxResponse)
		return &HTTPError{Operation: "file upload", Status: response.StatusCode}
	}
	return nil
}

func (c *Client) bookPath(bookID string) string {
	// Book IDs are validated by the service. PathEscape is still used here so
	// this client cannot accidentally turn an ID into another URL path.
	return "/api/v1/books/" + url.PathEscape(bookID)
}

type bodyFactory func() (io.ReadCloser, string, error)

func (c *Client) do(parent context.Context, method, requestPath string, body bodyFactory) (*http.Response, error) {
	return c.doWithPreflight(parent, method, requestPath, body, nil)
}

func (c *Client) doWithPreflight(parent context.Context, method, requestPath string, body bodyFactory, preflight func(context.Context) error) (*http.Response, error) {
	if c == nil || c.baseURL == nil {
		return nil, errors.New("Grimmory client is not initialized")
	}
	token, err := c.ensureToken(parent)
	if err != nil {
		return nil, err
	}
	if preflight != nil {
		if err := preflight(parent); err != nil {
			return nil, err
		}
		c.mu.Lock()
		token = c.accessToken
		c.mu.Unlock()
	}
	response, err := c.send(parent, method, requestPath, token, body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusUnauthorized {
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			defer response.Body.Close()
			_, _ = readBounded(response.Body, c.maxResponse)
			return nil, &HTTPError{Operation: strings.ToLower(method), Status: response.StatusCode}
		}
		return response, nil
	}
	_ = response.Body.Close()
	// Exactly one application request retry is made. Refresh/login may use
	// their own authentication requests, but a second failed resource request
	// is never attempted.
	if err := c.refreshOrLogin(parent); err != nil {
		return nil, err
	}
	c.mu.Lock()
	token = c.accessToken
	c.mu.Unlock()
	if preflight != nil {
		if err := preflight(parent); err != nil {
			return nil, err
		}
		c.mu.Lock()
		token = c.accessToken
		c.mu.Unlock()
	}
	response, err = c.send(parent, method, requestPath, token, body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		_, _ = readBounded(response.Body, c.maxResponse)
		return nil, &HTTPError{Operation: strings.ToLower(method), Status: response.StatusCode}
	}
	return response, nil
}

func (c *Client) send(parent context.Context, method, requestPath, token string, body bodyFactory) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	var requestBody io.ReadCloser
	var contentType string
	var err error
	if body != nil {
		requestBody, contentType, err = body()
		if err != nil {
			cancel()
			return nil, err
		}
	}
	endpoint := c.endpoint(requestPath)
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), requestBody)
	if err != nil {
		if requestBody != nil {
			_ = requestBody.Close()
		}
		cancel()
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		cancel()
		if requestBody != nil {
			_ = requestBody.Close()
		}
		return nil, err
	}
	response.Body = &cancelReadCloser{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

func (c *Client) ensureToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.accessToken != "" {
		return c.accessToken, nil
	}
	return c.loginLocked(ctx)
}

func (c *Client) refreshOrLogin(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.refreshToken != "" {
		if err := c.refreshLocked(ctx); err == nil {
			return nil
		}
	}
	_, err := c.loginLocked(ctx)
	return err
}

func (c *Client) loginLocked(parent context.Context) (string, error) {
	payload := map[string]string{"username": c.username, "password": c.password}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	response, err := c.authRequest(parent, http.MethodPost, "/api/v1/auth/login", body, "")
	if err != nil {
		return "", err
	}
	access, refresh, err := parseTokens(response)
	if err != nil {
		return "", err
	}
	c.accessToken, c.refreshToken = access, refresh
	return access, nil
}

func (c *Client) refreshLocked(parent context.Context) error {
	payload, err := json.Marshal(map[string]string{"refreshToken": c.refreshToken})
	if err != nil {
		return err
	}
	response, err := c.authRequest(parent, http.MethodPost, "/api/v1/auth/refresh", payload, c.refreshToken)
	if err != nil {
		return err
	}
	access, refresh, err := parseTokens(response)
	if err != nil {
		return err
	}
	c.accessToken = access
	if refresh != "" {
		c.refreshToken = refresh
	}
	return nil
}

func (c *Client) authRequest(parent context.Context, method, requestPath string, payload []byte, token string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	endpoint := c.endpoint(requestPath)
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), strings.NewReader(string(payload)))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, readErr := readBounded(response.Body, c.maxResponse)
	if readErr != nil {
		return nil, readErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if response.StatusCode == http.StatusUnauthorized {
			return nil, ErrUnauthorized
		}
		return nil, &HTTPError{Operation: "authentication", Status: response.StatusCode}
	}
	return body, nil
}

func (c *Client) endpoint(requestPath string) url.URL {
	endpoint := *c.baseURL
	parsed, err := url.Parse(requestPath)
	if err != nil {
		parsed = &url.URL{Path: requestPath}
	}
	endpoint.Path = path.Join(strings.TrimRight(endpoint.Path, "/"), parsed.Path)
	endpoint.RawPath = ""
	endpoint.RawQuery = parsed.RawQuery
	return endpoint
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *cancelReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.cancel()
	return err
}

func parseTokens(body []byte) (string, string, error) {
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return "", "", fmt.Errorf("%w: decode Grimmory authentication response: %v", ErrInvalidResponse, err)
	}
	objects := []map[string]any{}
	collectObjects(raw, &objects, []string{"data", "body", "result", "tokens"}, true)
	var access, refresh string
	for _, object := range objects {
		if access == "" {
			access = stringValue(object, "accessToken", "access_token", "token")
		}
		if refresh == "" {
			refresh = stringValue(object, "refreshToken", "refresh_token")
		}
	}
	if access == "" {
		return "", "", ErrUnauthorized
	}
	return access, refresh, nil
}

func collectObjects(value any, result *[]map[string]any, envelopeKeys []string, descendArrays bool) {
	switch object := value.(type) {
	case map[string]any:
		*result = append(*result, object)
		for _, key := range envelopeKeys {
			if nested, ok := object[key]; ok {
				collectObjects(nested, result, envelopeKeys, descendArrays)
			}
		}
	case []any:
		if !descendArrays {
			return
		}
		for _, nested := range object {
			collectObjects(nested, result, envelopeKeys, descendArrays)
		}
	}
}

func parseBook(value any, fallbackID string) (Book, bool) {
	objects := []map[string]any{}
	collectObjects(value, &objects, []string{"book", "data", "body", "result"}, false)
	for _, object := range objects {
		files, ok := parseBookFiles(object)
		if !ok {
			continue
		}
		bookID := stringValue(object, "id", "bookId", "book_id")
		if bookID == "" {
			bookID = fallbackID
		}
		libraryID := stringValue(object, "libraryId", "library_id")
		if libraryID == "" {
			if library, ok := object["library"].(map[string]any); ok {
				libraryID = stringValue(library, "id", "libraryId", "library_id")
			}
		}
		return Book{ID: bookID, LibraryID: libraryID, Files: files, Metadata: normalizeBookMetadata(object), metadataSnapshot: captureMetadataSnapshot(object)}, true
	}
	return Book{}, false
}

func normalizeBookMetadata(object map[string]any) BookMetadata {
	sources := []map[string]any{object}
	if nested, ok := object["metadata"].(map[string]any); ok {
		sources = append(sources, nested)
	}
	metadata := BookMetadata{}
	metadata.Title = metadataString(sources, "title")
	authorsValue, _ := metadataValue(sources, "authors")
	metadata.Authors = normalizeAuthors(authorsValue)
	metadata.Language = metadataString(sources, "language")
	metadata.Publisher = metadataString(sources, "publisher")
	metadata.PublicationDate = metadataString(sources, "publicationDate", "publishedDate", "datePublished")
	identifiersValue, _ := metadataValue(sources, "identifiers")
	metadata.Identifiers = normalizeIdentifiers(identifiersValue)
	seriesValue, _ := metadataValue(sources, "series")
	series, seriesIndex := normalizeSeries(seriesValue)
	metadata.Series = series
	metadata.SeriesIndex = seriesIndex
	if value, ok := metadataValue(sources, "seriesIndex", "seriesNumber"); ok {
		metadata.SeriesIndex = scalarString(value)
	}
	tagsValue, _ := metadataValue(sources, "tags")
	metadata.Tags = normalizeTags(tagsValue)
	metadata.Description = metadataString(sources, "description")
	metadata.Comments = metadataString(sources, "comments")
	return metadata
}

func captureMetadataSnapshot(object map[string]any) map[string]any {
	result := make(map[string]any)
	nested, ok := object["metadata"].(map[string]any)
	if !ok {
		return nil
	}
	for key, value := range nested {
		result[key] = cloneJSONValue(value)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func cloneJSONValue(value any) any {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var clone any
	if err := json.Unmarshal(encoded, &clone); err != nil {
		return nil
	}
	return clone
}

func metadataPayload(book Book) (map[string]any, bool) {
	if len(book.metadataSnapshot) == 0 {
		return nil, false
	}
	payload := make(map[string]any)
	for key, value := range book.metadataSnapshot {
		payload[key] = cloneJSONValue(value)
	}
	return payload, true
}

func normalizedTagSet(tags []string) map[string]struct{} {
	result := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if tag = strings.TrimSpace(tag); tag != "" {
			result[tag] = struct{}{}
		}
	}
	return result
}

func tagAlreadyDesired(book Book, tag string, present bool) bool {
	count := tagCount(book, tag)
	if present {
		return count == 1
	}
	return count == 0
}

func tagCount(book Book, target string) int {
	values := book.Metadata.Tags
	if snapshot, ok := book.metadataSnapshot["tags"]; ok {
		values = rawTagValues(snapshot)
	}
	count := 0
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			count++
		}
	}
	return count
}

func rawTagValues(value any) []string {
	var values []any
	switch typed := value.(type) {
	case []any:
		values = typed
	case []string:
		return append([]string(nil), typed...)
	default:
		return nil
	}
	result := make([]string, 0, len(values))
	for _, item := range values {
		if object, ok := item.(map[string]any); ok {
			item = firstScalar(object, "name", "label", "tag")
		}
		result = append(result, scalarString(item))
	}
	return result
}

func sameTagSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for tag := range left {
		if _, exists := right[tag]; !exists {
			return false
		}
	}
	return true
}

func metadataValue(sources []map[string]any, keys ...string) (any, bool) {
	for _, source := range sources {
		if value, ok := firstValue(source, keys...); ok {
			return value, true
		}
	}
	return nil, false
}

func metadataString(sources []map[string]any, keys ...string) string {
	value, _ := metadataValue(sources, keys...)
	return scalarString(value)
}

func normalizeAuthors(value any) []string {
	if value == nil {
		return nil
	}
	var values []any
	switch value := value.(type) {
	case []any:
		values = value
	default:
		values = []any{value}
	}
	authors := make([]string, 0, len(values))
	for _, value := range values {
		var author string
		if object, ok := value.(map[string]any); ok {
			author = stringValue(object, "name", "displayName", "fullName", "author")
			if author == "" {
				first := stringValue(object, "firstName")
				last := stringValue(object, "lastName")
				author = strings.TrimSpace(strings.TrimSpace(first) + " " + strings.TrimSpace(last))
			}
		} else {
			author = scalarString(value)
		}
		if author != "" {
			authors = append(authors, author)
		}
	}
	return authors
}

func normalizeIdentifiers(value any) map[string]string {
	if value == nil {
		return nil
	}
	identifiers := make(map[string]string)
	switch value := value.(type) {
	case map[string]any:
		for key, value := range value {
			if nested, ok := value.(map[string]any); ok {
				value = firstScalar(nested, "value", "identifier", "id")
			}
			key = strings.ToLower(strings.TrimSpace(key))
			normalizedValue := scalarString(value)
			if key != "" && normalizedValue != "" {
				identifiers[key] = normalizedValue
			}
		}
	case []any:
		for _, item := range value {
			object, ok := item.(map[string]any)
			if !ok {
				continue
			}
			key := strings.ToLower(stringValue(object, "type", "scheme", "name", "kind"))
			identifier := firstScalar(object, "value", "identifier", "id")
			if key == "" && len(object) == 1 {
				for objectKey, objectValue := range object {
					key = strings.ToLower(strings.TrimSpace(objectKey))
					identifier = scalarString(objectValue)
				}
			}
			if key != "" && identifier != "" {
				identifiers[key] = identifier
			}
		}
	}
	if len(identifiers) == 0 {
		return nil
	}
	return identifiers
}

func firstScalar(object map[string]any, keys ...string) string {
	value, _ := firstValue(object, keys...)
	return scalarString(value)
}

func normalizeSeries(value any) (string, string) {
	if object, ok := value.(map[string]any); ok {
		return stringValue(object, "name", "title", "series"), firstScalar(object, "index", "sequence", "number", "position")
	}
	if values, ok := value.([]any); ok && len(values) > 0 {
		return normalizeSeries(values[0])
	}
	return scalarString(value), ""
}

func normalizeTags(value any) []string {
	if value == nil {
		return nil
	}
	var values []any
	switch value := value.(type) {
	case []any:
		values = value
	default:
		values = []any{value}
	}
	tags := make([]string, 0, len(values))
	for _, value := range values {
		if object, ok := value.(map[string]any); ok {
			value = firstScalar(object, "name", "label", "tag")
		}
		if tag := scalarString(value); tag != "" {
			tags = append(tags, tag)
		}
	}
	sort.Strings(tags)
	result := tags[:0]
	for _, tag := range tags {
		if len(result) == 0 || result[len(result)-1] != tag {
			result = append(result, tag)
		}
	}
	return result
}

func scalarString(value any) string {
	switch value := value.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return strings.TrimSpace(string(value))
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(value), 'f', -1, 32)
	case int:
		return strconv.Itoa(value)
	case int8:
		return strconv.FormatInt(int64(value), 10)
	case int16:
		return strconv.FormatInt(int64(value), 10)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	case uint:
		return strconv.FormatUint(uint64(value), 10)
	case uint8:
		return strconv.FormatUint(uint64(value), 10)
	case uint16:
		return strconv.FormatUint(uint64(value), 10)
	case uint32:
		return strconv.FormatUint(uint64(value), 10)
	case uint64:
		return strconv.FormatUint(value, 10)
	default:
		return ""
	}
}

func integerField(object map[string]any, key string) (int, bool) {
	value, exists := object[key]
	if !exists {
		return 0, false
	}
	number, ok := integerValue(value)
	maxInt := int64(^uint(0) >> 1)
	if !ok || number < int64(-maxInt-1) || number > maxInt {
		return 0, false
	}
	return int(number), true
}

func integerValue(value any) (int64, bool) {
	switch value := value.(type) {
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value < -9.223372036854776e18 || value >= 9.223372036854776e18 {
			return 0, false
		}
		return int64(value), true
	case json.Number:
		if number, err := strconv.ParseInt(string(value), 10, 64); err == nil {
			return number, true
		}
		parsed, err := strconv.ParseFloat(string(value), 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || math.Trunc(parsed) != parsed || parsed < -9.223372036854776e18 || parsed >= 9.223372036854776e18 {
			return 0, false
		}
		return int64(parsed), true
	case string:
		number, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return number, err == nil
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func booleanField(object map[string]any, key string) (bool, bool) {
	value, exists := object[key]
	if !exists {
		return false, false
	}
	switch value := value.(type) {
	case bool:
		return value, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		return parsed, err == nil
	default:
		return false, false
	}
}

type parsedBookPage struct {
	Content    []any
	TotalPages int
	Terminal   bool
}

func parseBookPage(value any, requestedPage int) (parsedBookPage, bool) {
	object, ok := value.(map[string]any)
	if !ok {
		return parsedBookPage{}, false
	}
	contentValue, ok := object["content"]
	if !ok {
		return parsedBookPage{}, false
	}
	content, ok := contentValue.([]any)
	if !ok {
		return parsedBookPage{}, false
	}
	pageValue, ok := object["page"]
	if !ok {
		return parsedBookPage{}, false
	}
	pageObject, ok := pageValue.(map[string]any)
	if !ok {
		return parsedBookPage{}, false
	}
	pageNumber, ok := integerField(pageObject, "number")
	if !ok || pageNumber < 0 || pageNumber != requestedPage {
		return parsedBookPage{}, false
	}
	if _, exists := pageObject["size"]; exists {
		size, ok := integerField(pageObject, "size")
		if !ok || size <= 0 {
			return parsedBookPage{}, false
		}
	}

	totalPages, exists := integerField(pageObject, "totalPages")
	if !exists || totalPages < 0 || (totalPages == 0 && pageNumber != 0) || (totalPages > 0 && pageNumber >= totalPages) {
		return parsedBookPage{}, false
	}
	if totalPages == 0 && len(content) != 0 {
		return parsedBookPage{}, false
	}
	terminal, terminalKnown := totalPages == 0 || pageNumber == totalPages-1, true
	if last, exists := booleanField(pageObject, "last"); exists {
		if terminalKnown && terminal != last {
			return parsedBookPage{}, false
		}
		terminal, terminalKnown = last, true
	} else if _, exists := pageObject["last"]; exists {
		return parsedBookPage{}, false
	}
	linksValue, exists := object["links"]
	if !exists {
		return parsedBookPage{}, false
	}
	hasNext, linksKnown := nextLink(linksValue)
	if !linksKnown {
		return parsedBookPage{}, false
	}
	if terminalKnown && terminal == hasNext {
		return parsedBookPage{}, false
	}
	terminal, terminalKnown = !hasNext, true
	if !terminalKnown || (!terminal && len(content) == 0) {
		return parsedBookPage{}, false
	}
	return parsedBookPage{Content: content, TotalPages: totalPages, Terminal: terminal}, true
}

func nextLink(value any) (bool, bool) {
	switch links := value.(type) {
	case map[string]any:
		next, exists := links["next"]
		if !exists || next == nil {
			return false, true
		}
		return nonEmptyLink(next)
	case []any:
		for _, value := range links {
			link, ok := value.(map[string]any)
			if !ok {
				return false, false
			}
			if strings.EqualFold(strings.TrimSpace(stringValue(link, "rel", "relation")), "next") {
				href, exists := firstValue(link, "href", "url", "uri")
				if !exists || href == nil {
					return false, true
				}
				return nonEmptyLink(href)
			}
		}
		return false, true
	default:
		return false, false
	}
}

func nonEmptyLink(value any) (bool, bool) {
	switch link := value.(type) {
	case string:
		return strings.TrimSpace(link) != "", true
	case map[string]any:
		href, exists := firstValue(link, "href", "url", "uri")
		if !exists || href == nil {
			return false, false
		}
		return nonEmptyLink(href)
	default:
		return false, false
	}
}

func parseBookFiles(object map[string]any) ([]File, bool) {
	if filesValue, ok := firstValue(object, "files", "bookFiles", "items", "results"); ok {
		return parseFiles(filesValue)
	}
	return parseLiveFiles(object)
}

func parseLiveFiles(object map[string]any) ([]File, bool) {
	primaryValue, ok := object["primaryFile"]
	if !ok || primaryValue == nil {
		return nil, false
	}
	primary, ok := primaryValue.(map[string]any)
	if !ok {
		return nil, false
	}

	files := []File{normalizeFile(primary)}
	if alternativesValue, exists := object["alternativeFormats"]; exists {
		alternatives, ok := alternativesValue.([]any)
		if !ok {
			return nil, false
		}
		for _, item := range alternatives {
			alternative, ok := item.(map[string]any)
			if ok {
				files = append(files, normalizeFile(alternative))
			}
		}
	}
	sortFiles(files)
	return files, true
}

func parseFiles(value any) ([]File, bool) {
	items, ok := value.([]any)
	if !ok {
		if object, objectOK := value.(map[string]any); objectOK {
			for _, key := range []string{"files", "bookFiles", "items", "data", "results"} {
				if nested, exists := object[key]; exists {
					if parsed, parsedOK := parseFiles(nested); parsedOK {
						return parsed, true
					}
				}
			}
		}
		return nil, false
	}
	files := make([]File, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		files = append(files, normalizeFile(object))
	}
	sortFiles(files)
	return files, true
}

func normalizeFile(object map[string]any) File {
	format := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(stringValue(object, "bookType", "format", "extension", "fileType")), "."))
	name := stringValue(object, "fileName", "filename", "name", "file_name")
	if format == "" {
		if extension := path.Ext(name); extension != "" {
			format = strings.ToLower(strings.TrimPrefix(extension, "."))
		}
	}
	typeName := normalizeObservationToken(stringValue(object, "bookType", "fileType", "type"))
	if typeName == "" {
		typeName = format
	}
	mtime, trusted := timestamp(object, "updatedAt", "modifiedAt", "mtime", "lastModified")
	return File{ID: stringValue(object, "id", "fileId", "file_id"), Name: name, Format: format, Type: typeName, SizeKB: fileSizeKB(object), SHA256: sha256Value(object), MetadataFingerprint: metadataFingerprint(object), MTime: mtime, TrustedMTime: trusted}
}

func fileSizeKB(object map[string]any) int64 {
	for _, key := range []string{"fileSizeKb", "fileSizeKB", "sizeKb", "sizeKB"} {
		if value, exists := object[key]; exists {
			size, ok := integerValue(value)
			if ok && size >= 0 {
				return size
			}
		}
	}
	return 0
}

func sortFiles(files []File) {
	sort.SliceStable(files, func(i, j int) bool {
		if files[i].Format != files[j].Format {
			return files[i].Format < files[j].Format
		}
		if files[i].Name != files[j].Name {
			return files[i].Name < files[j].Name
		}
		return files[i].ID < files[j].ID
	})
}

func sha256Value(object map[string]any) string {
	value := strings.ToLower(stringValue(object, "sha256", "checksum", "contentHash", "hash"))
	if len(value) != sha256.Size*2 {
		return ""
	}
	if _, err := hex.DecodeString(value); err != nil {
		return ""
	}
	return value
}

func metadataFingerprint(object map[string]any) string {
	metadata := make(map[string]any)
	for _, key := range []string{"id", "fileId", "file_id", "fileName", "filename", "name", "file_name", "bookType", "format", "extension", "fileType", "updatedAt", "modifiedAt", "mtime", "lastModified", "size", "fileSize", "fileSizeKb"} {
		if value, ok := object[key]; ok {
			metadata[key] = value
		}
	}
	if len(metadata) == 0 {
		return ""
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest[:])
}

func timestamp(object map[string]any, fields ...string) (time.Time, bool) {
	for _, field := range fields {
		value, ok := object[field]
		if !ok || value == nil {
			continue
		}
		switch value := value.(type) {
		case float64:
			return numericTime(value)
		case json.Number:
			parsed, err := strconv.ParseFloat(string(value), 64)
			if err == nil {
				return numericTime(parsed)
			}
		case string:
			if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
				return parsed.UTC(), true
			}
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
				return numericTime(parsed)
			}
		}
	}
	return time.Time{}, false
}

func numericTime(value float64) (time.Time, bool) {
	if value <= 0 || value > 1e15 {
		return time.Time{}, false
	}
	if value < 1e11 {
		value *= 1000
	}
	return time.UnixMilli(int64(value)).UTC(), true
}

func firstValue(object map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		if value, ok := object[key]; ok && value != nil {
			return value, true
		}
	}
	return nil, false
}

func stringValue(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := object[key]; ok && value != nil {
			if value := scalarString(value); value != "" {
				return value
			}
		}
	}
	return ""
}

func readBounded(reader io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, ErrResponseTooBig
	}
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, ErrResponseTooBig
	}
	return body, nil
}

func multipartFileBody(filePath, format string, maxBytes int64) (io.ReadCloser, string, error) {
	return multipartFileBodyNamed(filePath, format, "book."+format, maxBytes)
}

func multipartFileBodyNamed(filePath, format, filename string, maxBytes int64) (io.ReadCloser, string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, "", fmt.Errorf("open upload file: %w", err)
	}
	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	go func() {
		defer file.Close()
		part, err := multipartWriter.CreateFormFile("file", filename)
		if err == nil {
			var count int64
			count, err = io.Copy(part, io.LimitReader(file, maxBytes+1))
			if err == nil && count > maxBytes {
				err = ErrResponseTooBig
			}
		}
		if err == nil {
			err = multipartWriter.WriteField("isBook", "true")
		}
		if err == nil {
			err = multipartWriter.WriteField("bookType", strings.ToUpper(format))
		}
		if err == nil {
			err = multipartWriter.Close()
		}
		if err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		_ = writer.Close()
	}()
	return reader, multipartWriter.FormDataContentType(), nil
}

func safeUploadFilename(filename, format string) string {
	base := path.Base(strings.ReplaceAll(strings.TrimSpace(filename), "\\", "/"))
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
	if format == "" {
		return stem
	}
	return stem + "." + format
}

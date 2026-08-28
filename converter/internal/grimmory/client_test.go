package grimmory

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClientParsesLiveBookResponse(t *testing.T) {
	fixture, err := os.ReadFile("testdata/live-book-response.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture)
	}))
	defer server.Close()

	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	book, err := client.GetBook(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if book.ID != "1" || len(book.Files) != 3 {
		t.Fatalf("parsed live book = %+v", book)
	}
	if book.Metadata.Description != "A complete description." || book.Metadata.Comments != "Complete comments." || book.Metadata.Identifiers["isbn"] != "9780000000001" {
		t.Fatalf("parsed live metadata = %+v", book.Metadata)
	}
	byFormat := make(map[string]File, len(book.Files))
	for _, file := range book.Files {
		byFormat[file.Format] = file
	}
	if len(byFormat) != 3 {
		t.Fatalf("normalised formats = %#v", byFormat)
	}
	if primary := byFormat["epub"]; primary.ID != "101" || primary.Name != "A Book.epub" || !primary.TrustedMTime || primary.MetadataFingerprint == "" {
		t.Fatalf("normalised primary file = %+v", primary)
	}
	if alternative := byFormat["mobi"]; alternative.ID != "102" || alternative.Name != "A Book.mobi" {
		t.Fatalf("normalised alternative file = %+v", alternative)
	}
	if _, supplementaryIncluded := byFormat["jpg"]; supplementaryIncluded {
		t.Fatalf("supplementary file included in ebook files: %#v", byFormat["jpg"])
	}
}

func TestClientLibraryMethodsUseStrictPathsAndNormalizePolicy(t *testing.T) {
	policyNames := []string{"library-policy-null.json", "library-policy-empty.json", "library-policy-values.json"}
	policyBodies := make(map[string][]byte, len(policyNames))
	for _, name := range policyNames {
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		policyBodies[name] = body
	}
	books, err := os.ReadFile("testdata/library-books.json")
	if err != nil {
		t.Fatal(err)
	}
	detail, err := os.ReadFile("testdata/library-book-detail.json")
	if err != nil {
		t.Fatal(err)
	}
	var policyBody []byte
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeJSON(w, map[string]string{"accessToken": "token"})
		case "/api/v1/libraries/library-1":
			_, _ = w.Write(policyBody)
		case "/api/v1/libraries/library-1/book":
			_, _ = w.Write(books)
		case "/api/v1/libraries/library-1/book/book-1":
			_, _ = w.Write(detail)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for index, name := range policyNames {
		policyBody = policyBodies[name]
		library, err := client.GetLibrary(context.Background(), "library-1")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(library.FormatPriority, []string{"epub", "mobi"}) {
			t.Fatalf("format priority = %#v", library.FormatPriority)
		}
		switch index {
		case 0:
			if library.AllowedFormats != nil {
				t.Fatalf("null allowed formats = %#v", library.AllowedFormats)
			}
		case 1:
			if library.AllowedFormats == nil || len(library.AllowedFormats) != 0 {
				t.Fatalf("empty allowed formats = %#v", library.AllowedFormats)
			}
		case 2:
			if !reflect.DeepEqual(library.AllowedFormats, []string{"azw3", "mobi"}) {
				t.Fatalf("configured allowed formats = %#v", library.AllowedFormats)
			}
		}
	}
	libraryBooks, err := client.ListLibraryBooks(context.Background(), "library-1")
	if err != nil || len(libraryBooks) != 2 || libraryBooks[0].LibraryID != "library-1" {
		t.Fatalf("library books = %#v err=%v", libraryBooks, err)
	}
	libraryBook, err := client.GetLibraryBook(context.Background(), "library-1", "book-1")
	if err != nil || libraryBook.ID != "book-1" || libraryBook.LibraryID != "library-1" {
		t.Fatalf("library book = %+v err=%v", libraryBook, err)
	}
	for _, want := range []string{"/api/v1/libraries/library-1", "/api/v1/libraries/library-1/book", "/api/v1/libraries/library-1/book/book-1"} {
		found := false
		for _, got := range paths {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("request path %q missing from %v", want, paths)
		}
	}
}

func TestClientLibraryBooksRejectDuplicateAndForeignMembership(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		switch r.URL.Path {
		case "/api/v1/libraries/library-1/book":
			_, _ = io.WriteString(w, `[{"id":"book-1","libraryId":"library-1","files":[]},{"id":"book-1","libraryId":"library-1","files":[]}]`)
		case "/api/v1/libraries/library-1/book/book-1":
			_, _ = io.WriteString(w, `{"id":"book-1","libraryId":"library-2","files":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListLibraryBooks(context.Background(), "library-1"); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("duplicate library books error = %v", err)
	}
	if _, err := client.GetLibraryBook(context.Background(), "library-1", "book-1"); !errors.Is(err, ErrBookNotInLibrary) {
		t.Fatalf("foreign library book error = %v", err)
	}
	if _, err := client.GetLibrary(context.Background(), "../unsafe"); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("invalid library ID error = %v", err)
	}
}

func TestClientAllowsLibraryBooksWithNoPrimaryOrAlternative(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		switch r.URL.Path {
		case "/api/v1/libraries/library-1/book":
			writeJSON(w, []any{
				map[string]any{"id": "empty-1", "libraryId": "library-1", "metadata": map[string]any{"title": "Empty"}, "primaryFile": nil, "alternativeFormats": []any{}},
			})
		case "/api/v1/libraries/library-1/book/empty-1":
			writeJSON(w, map[string]any{"id": "empty-1", "libraryId": "library-1", "metadata": map[string]any{"title": "Empty"}, "primaryFile": nil, "alternativeFormats": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	books, err := client.ListLibraryBooks(context.Background(), "library-1")
	if err != nil || len(books) != 1 || len(books[0].Files) != 0 {
		t.Fatalf("empty library book list = %#v err=%v", books, err)
	}
	detail, err := client.GetLibraryBook(context.Background(), "library-1", "empty-1")
	if err != nil || detail.ID != "empty-1" || len(detail.Files) != 0 {
		t.Fatalf("empty library book detail = %#v err=%v", detail, err)
	}
}

func TestClientGetBookRejectsResponseIDMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		writeJSON(w, map[string]any{"id": "book-2", "files": []any{}})
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetBook(context.Background(), "book-1"); !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("mismatched book response error = %v", err)
	}
}

func TestClientLibraryResponseIsBounded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		_, _ = io.WriteString(w, strings.Repeat("x", 100))
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 64, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetLibrary(context.Background(), "library-1"); !errors.Is(err, ErrResponseTooBig) {
		t.Fatalf("bounded library response error = %v", err)
	}
}

func TestClientBookTagMutationUsesTagPatchAndIsIdempotent(t *testing.T) {
	currentTags := []string{"keep", "keep"}
	description := "Description"
	var putCount, getCount, membershipCount int
	var updatedPayload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeJSON(w, map[string]string{"accessToken": "token"})
		case "/api/v1/books/book-1":
			if r.Method == http.MethodPut {
				t.Errorf("tag mutation used the book endpoint instead of metadata endpoint")
				return
			}
			withDescription := r.URL.Query().Get("withDescription") == "true"
			if !withDescription {
				t.Errorf("full metadata GET query = %q", r.URL.RawQuery)
			}
			getCount++
			metadata := map[string]any{
				"title": "Book", "authors": []string{"Author"}, "language": "en", "publisher": "Press",
				"publicationDate": "2025-01-02", "identifiers": map[string]string{"isbn": "123"},
				"series": "Series", "seriesIndex": 1,
				"customField": "preserve", "tags": currentTags,
			}
			if withDescription {
				metadata["description"] = description
			}
			writeJSON(w, map[string]any{
				"id": "book-1", "libraryId": "library-1",
				"metadata": metadata,
				"files":    []any{},
			})
		case "/api/v1/books/book-1/metadata":
			if r.Method != http.MethodPut {
				t.Errorf("metadata endpoint method = %s", r.Method)
				return
			}
			if r.URL.Query().Get("replaceMode") != "REPLACE_WHEN_PROVIDED" || len(r.URL.Query()) != 1 {
				t.Errorf("metadata update query = %q", r.URL.RawQuery)
			}
			putCount++
			if err := json.NewDecoder(r.Body).Decode(&updatedPayload); err != nil {
				t.Errorf("metadata body: %v", err)
				return
			}
			metadata, ok := updatedPayload["metadata"].(map[string]any)
			if !ok {
				t.Errorf("nested metadata = %#v", updatedPayload["metadata"])
			} else {
				values, valuesOK := metadata["tags"].([]any)
				if !valuesOK {
					t.Errorf("updated tags = %#v", metadata["tags"])
				} else {
					currentTags = make([]string, 0, len(values))
					for _, value := range values {
						currentTags = append(currentTags, value.(string))
					}
				}
				if value, exists := metadata["description"]; exists {
					description = value.(string)
				}
			}
			writeJSON(w, map[string]any{"ok": true})
			return
		case "/api/v1/libraries/library-1/book/book-1":
			membershipCount++
			if membershipCount == 2 {
				description = "Concurrent description"
			}
			writeJSON(w, map[string]any{"id": "book-1", "libraryId": "library-1", "metadata": map[string]any{"title": "Book"}, "files": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reference := BookReference{LibraryID: "library-1", BookID: "book-1"}
	if err := client.AddBookTagScoped(context.Background(), reference, "new"); err != nil {
		t.Fatal(err)
	}
	if putCount != 1 || !reflect.DeepEqual(currentTags, []string{"keep", "new"}) {
		t.Fatalf("add tags=%v puts=%d", currentTags, putCount)
	}
	if description != "Concurrent description" {
		t.Fatalf("concurrent metadata change was lost: %q", description)
	}
	metadata, ok := updatedPayload["metadata"].(map[string]any)
	if !ok || len(metadata) != 1 || !reflect.DeepEqual(metadata["tags"], []any{"keep", "new"}) {
		t.Fatalf("metadata tag patch = %#v", updatedPayload)
	}
	if _, exists := metadata["description"]; exists {
		t.Fatalf("concurrent metadata was included in tag patch: %#v", metadata)
	}
	clearFlags, ok := updatedPayload["clearFlags"].(map[string]any)
	if !ok || clearFlags["tags"] != false {
		t.Fatalf("clear flags = %#v", updatedPayload["clearFlags"])
	}
	if len(updatedPayload) != 2 || len(clearFlags) != 1 {
		t.Fatalf("wire body was not exact: %#v", updatedPayload)
	}
	if err := client.AddBookTagScoped(context.Background(), reference, "new"); err != nil {
		t.Fatal(err)
	}
	if putCount != 1 {
		t.Fatalf("idempotent add issued %d PUTs", putCount)
	}
	if err := client.RemoveBookTagScoped(context.Background(), reference, "new"); err != nil {
		t.Fatal(err)
	}
	if putCount != 2 || !reflect.DeepEqual(currentTags, []string{"keep"}) {
		t.Fatalf("remove tags=%v puts=%d", currentTags, putCount)
	}
	if getCount != 5 || membershipCount != 5 {
		t.Fatalf("GET count=%d membership=%d, expected scoped checks for each operation", getCount, membershipCount)
	}
	if err := client.AddBookTagScoped(context.Background(), reference, " "); !errors.Is(err, ErrEmptyTag) {
		t.Fatalf("empty tag error = %v", err)
	}
}

func TestClientBookTagMutationClearsFinalTag(t *testing.T) {
	currentTags := []string{"only"}
	var updatedPayload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeJSON(w, map[string]string{"accessToken": "token"})
		case "/api/v1/books/book-1":
			writeJSON(w, map[string]any{
				"id": "book-1", "libraryId": "library-1",
				"metadata": map[string]any{"title": "Book", "tags": currentTags},
				"files":    []any{},
			})
		case "/api/v1/libraries/library-1/book/book-1":
			writeJSON(w, map[string]any{"id": "book-1", "libraryId": "library-1", "metadata": map[string]any{"title": "Book"}, "files": []any{}})
		case "/api/v1/books/book-1/metadata":
			if r.Method != http.MethodPut || r.URL.Query().Get("replaceMode") != "REPLACE_WHEN_PROVIDED" || len(r.URL.Query()) != 1 {
				t.Errorf("metadata update request = %s %q", r.Method, r.URL.RawQuery)
			}
			if err := json.NewDecoder(r.Body).Decode(&updatedPayload); err != nil {
				t.Errorf("metadata body: %v", err)
				return
			}
			metadata, metadataOK := updatedPayload["metadata"].(map[string]any)
			clearFlags, clearFlagsOK := updatedPayload["clearFlags"].(map[string]any)
			if len(updatedPayload) != 2 || !metadataOK || len(metadata) != 1 || !reflect.DeepEqual(metadata["tags"], []any{}) {
				t.Errorf("final tag metadata patch = %#v", updatedPayload)
			}
			if !clearFlagsOK || len(clearFlags) != 1 || clearFlags["tags"] != true {
				t.Errorf("final tag clear flags = %#v", updatedPayload["clearFlags"])
			}
			currentTags = []string{}
			writeJSON(w, map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveBookTagScoped(context.Background(), BookReference{LibraryID: "library-1", BookID: "book-1"}, "only"); err != nil {
		t.Fatal(err)
	}
	if len(currentTags) != 0 {
		t.Fatalf("final tag was not cleared: %v", currentTags)
	}
}

func TestClientBookTagMutationVerifiesResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		if r.URL.Path == "/api/v1/books/book-1/metadata" && r.Method == http.MethodPut {
			writeJSON(w, map[string]bool{"ok": true})
			return
		}
		if r.URL.Path == "/api/v1/libraries/library-1/book/book-1" {
			writeJSON(w, map[string]any{"id": "book-1", "libraryId": "library-1", "metadata": map[string]any{"title": "Book"}, "files": []any{}})
			return
		}
		writeJSON(w, map[string]any{"id": "book-1", "metadata": map[string]any{"title": "Book", "description": "Description", "tags": []string{}}, "files": []any{}})
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.AddBookTagScoped(context.Background(), BookReference{LibraryID: "library-1", BookID: "book-1"}, "missing"); !errors.Is(err, ErrTagVerification) {
		t.Fatalf("verification error = %v", err)
	}
}

func TestClientListsBooksAcrossLivePages(t *testing.T) {
	client, server := newBookPageFixtureClient(t, map[string]string{
		"0": "testdata/books-page-0.json",
		"1": "testdata/books-page-1.json",
	})
	defer server.Close()

	books, err := client.ListBooks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{books[0].ID, books[1].ID, books[2].ID}; !reflect.DeepEqual(got, []string{"10", "20", "30"}) {
		t.Fatalf("book IDs = %v", got)
	}
	first := books[0]
	if first.Metadata.Title != "First Book" || !reflect.DeepEqual(first.Metadata.Authors, []string{"Second Author", "First Author"}) || first.Metadata.Language != "en" || first.Metadata.Publisher != "Example Press" || first.Metadata.PublicationDate != "2024-01-02" || first.Metadata.Series != "Example Series" || first.Metadata.SeriesIndex != "1" || !reflect.DeepEqual(first.Metadata.Tags, []string{"alpha", "zeta"}) || first.Metadata.Description != "A first book." || first.Metadata.Comments != "Keep this comment." {
		t.Fatalf("first book metadata = %+v", first.Metadata)
	}
	if first.Metadata.Identifiers["isbn"] != "9780000000001" || first.Metadata.Identifiers["goodreads"] != "g-10" {
		t.Fatalf("first book identifiers = %#v", first.Metadata.Identifiers)
	}
	if len(first.Files) != 2 || first.Files[0].Format != "epub" || first.Files[0].Type != "epub" || first.Files[0].ID != "1001" || first.Files[0].SizeKB != 1200 || first.Files[1].Format != "mobi" || first.Files[1].Type != "mobi" || first.Files[1].SizeKB != 900 {
		t.Fatalf("first book files = %+v", first.Files)
	}
	if books[2].Metadata.Series != "Another Series" || books[2].Metadata.SeriesIndex != "2" || books[2].Metadata.Identifiers["isbn"] != "9780000000003" {
		t.Fatalf("third book metadata = %+v", books[2].Metadata)
	}
}

func TestClientRejectsMalformedOrNonProgressingBookPages(t *testing.T) {
	tests := []struct {
		name  string
		pages map[string]string
	}{
		{
			name:  "malformed page",
			pages: map[string]string{"0": "testdata/books-page-malformed.json"},
		},
		{
			name: "page number does not progress",
			pages: map[string]string{
				"0": "testdata/books-page-0.json",
				"1": "testdata/books-page-non-progressing.json",
			},
		},
		{
			name: "duplicate book ID",
			pages: map[string]string{
				"0": "testdata/books-page-0.json",
				"1": "testdata/books-page-duplicate.json",
			},
		},
		{
			name: "inconsistent total pages",
			pages: map[string]string{
				"0": "testdata/books-page-0.json",
				"1": "testdata/books-page-inconsistent-total.json",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, server := newBookPageFixtureClient(t, test.pages)
			defer server.Close()
			if _, err := client.ListBooks(context.Background()); !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("ListBooks error = %v", err)
			}
		})
	}
}

func newBookPageFixtureClient(t *testing.T, pages map[string]string) (*Client, *httptest.Server) {
	t.Helper()
	fixtures := make(map[string][]byte, len(pages))
	for page, fixturePath := range pages {
		fixture, err := os.ReadFile(fixturePath)
		if err != nil {
			t.Fatal(err)
		}
		fixtures[page] = fixture
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		if r.URL.Path != "/api/v1/books/page" || r.URL.Query().Get("size") != "100" {
			t.Errorf("list request = %s", r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if _, hasSort := r.URL.Query()["sort"]; hasSort {
			t.Errorf("list request unexpectedly has sort: %s", r.URL.String())
			http.Error(w, "sort is not supported", http.StatusBadRequest)
			return
		}
		fixture, ok := fixtures[r.URL.Query().Get("page")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture)
	}))
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return client, server
}

func TestObservationFingerprintUsesStableBookProjection(t *testing.T) {
	base := Book{
		ID: "book-1",
		Files: []File{
			{ID: "file-1", Name: "book.epub", Format: "EPUB", Type: "EPUB", SizeKB: 100, SHA256: "before", MetadataFingerprint: "legacy", MTime: time.Unix(1, 0), TrustedMTime: true},
			{ID: "file-2", Name: "book.mobi", Format: "MOBI", Type: "MOBI", SizeKB: 80},
		},
		Metadata: BookMetadata{
			Title:           "Book",
			Authors:         []string{"Second Author", "First Author"},
			Language:        "en",
			Publisher:       "Publisher",
			PublicationDate: "2025-01-02",
			Identifiers:     map[string]string{"isbn": "9780000000001", "asin": "B0001"},
			Series:          "Series",
			SeriesIndex:     "1",
			Tags:            []string{"zeta", "alpha"},
			Description:     "Description",
			Comments:        "Comments",
		},
	}
	baseFingerprint := ObservationFingerprint(base)

	volatile := base
	volatile.Files = append([]File(nil), base.Files...)
	volatile.Files[0].SHA256 = "after"
	volatile.Files[0].MetadataFingerprint = "changed"
	volatile.Files[0].MTime = time.Unix(2, 0)
	if got := ObservationFingerprint(volatile); got != baseFingerprint {
		t.Fatalf("volatile fields changed fingerprint: before=%q after=%q", baseFingerprint, got)
	}

	filenameChanged := base
	filenameChanged.Files = append([]File(nil), base.Files...)
	filenameChanged.Files[0].Name = "renamed.epub"
	if got := ObservationFingerprint(filenameChanged); got == baseFingerprint {
		t.Fatalf("filename change did not change fingerprint: %q", got)
	}

	metadataChanged := base
	metadataChanged.Metadata.Title = "Changed Book"
	if got := ObservationFingerprint(metadataChanged); got == baseFingerprint {
		t.Fatalf("material metadata change did not change fingerprint: %q", got)
	}

	unordered := base
	unordered.Metadata.Tags = []string{"alpha", "zeta"}
	if got := ObservationFingerprint(unordered); got != baseFingerprint {
		t.Fatalf("tag order changed fingerprint: before=%q after=%q", baseFingerprint, got)
	}

	managedTag := base
	managedTag.Metadata.Tags = append([]string(nil), base.Metadata.Tags...)
	managedTag.Metadata.Tags = append(managedTag.Metadata.Tags, "failed")
	if got := ObservationFingerprintIgnoringTags(managedTag, "failed"); got != ObservationFingerprintIgnoringTags(base, "failed") {
		t.Fatalf("managed tag changed fingerprint: before=%q after=%q", ObservationFingerprintIgnoringTags(base, "failed"), got)
	}
	spaceTag := base
	spaceTag.Metadata.Tags = append([]string(nil), base.Metadata.Tags...)
	spaceTag.Metadata.Tags = append(spaceTag.Metadata.Tags, " failed ")
	if got := ObservationFingerprintIgnoringTags(spaceTag, "failed"); got == ObservationFingerprintIgnoringTags(base, "failed") {
		t.Fatalf("non-exact tag changed no fingerprint: %q", got)
	}

	authorOrderChanged := base
	authorOrderChanged.Metadata.Authors = []string{"First Author", "Second Author"}
	if got := ObservationFingerprint(authorOrderChanged); got == baseFingerprint {
		t.Fatalf("author order did not change fingerprint: %q", got)
	}
}

func TestObservationFingerprintNormalizesEmptyCollections(t *testing.T) {
	nilCollections := Book{ID: "book-1"}
	emptyCollections := Book{
		ID:    "book-1",
		Files: []File{},
		Metadata: BookMetadata{
			Authors:     []string{},
			Identifiers: map[string]string{},
			Tags:        []string{},
		},
	}
	if nilFingerprint, emptyFingerprint := ObservationFingerprint(nilCollections), ObservationFingerprint(emptyCollections); nilFingerprint != emptyFingerprint {
		t.Fatalf("nil and empty collections differ: nil=%q empty=%q", nilFingerprint, emptyFingerprint)
	}
}

func TestClientParsesBookEnvelopesAndTimestampFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			var credentials map[string]string
			if err := json.NewDecoder(r.Body).Decode(&credentials); err != nil || credentials["username"] != "user" || credentials["password"] != "password" {
				t.Errorf("login credentials = %#v err=%v", credentials, err)
			}
			writeJSON(w, map[string]any{"data": map[string]string{"accessToken": "token", "refreshToken": "refresh"}})
			return
		}
		writeJSON(w, map[string]any{"data": map[string]any{"book": map[string]any{
			"id": "book-1", "files": []any{
				map[string]any{"id": "2", "fileName": "z.mobi", "bookType": "MOBI", "updatedAt": "2025-01-02T03:04:05Z"},
				map[string]any{"id": "1", "fileName": "a.epub", "extension": ".epub", "mtime": 1735787045},
			},
		}}})
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	book, err := client.GetBook(context.Background(), "book-1")
	if err != nil {
		t.Fatal(err)
	}
	if book.ID != "book-1" || len(book.Files) != 2 || book.Files[0].Format != "epub" || !book.Files[0].TrustedMTime || book.Files[0].MetadataFingerprint == "" || book.Files[1].Format != "mobi" {
		t.Fatalf("parsed book = %+v", book)
	}
}

func TestClientRefreshesAndRetriesExactlyOnce(t *testing.T) {
	var mu sync.Mutex
	var resourceCalls, refreshCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeJSON(w, map[string]string{"accessToken": "old", "refreshToken": "refresh"})
		case "/api/v1/auth/refresh":
			refreshCalls++
			writeJSON(w, map[string]string{"accessToken": "new", "refreshToken": "refresh-2"})
		case "/api/v1/books/book":
			resourceCalls++
			if r.Header.Get("Authorization") != "Bearer new" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			writeJSON(w, map[string]any{"id": "book", "files": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "u", "p", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetBook(context.Background(), "book"); err != nil {
		t.Fatal(err)
	}
	if resourceCalls != 2 || refreshCalls != 1 {
		t.Fatalf("calls resource=%d refresh=%d", resourceCalls, refreshCalls)
	}

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeJSON(w, map[string]string{"accessToken": "old", "refreshToken": "refresh"})
		case "/api/v1/auth/refresh":
			writeJSON(w, map[string]string{"accessToken": "new"})
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer server2.Close()
	second, _ := New(server2.URL, "u", "p", server2.Client(), 1<<20, 1<<20, time.Second)
	if _, err := second.GetBook(context.Background(), "book"); !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("second unauthorized request error = %v", err)
	}
}

func TestClientDownloadsBoundedContentAndUploadsExpectedMultipart(t *testing.T) {
	dir := t.TempDir()
	uploadPath := filepath.Join(dir, "input.epub")
	if err := os.WriteFile(uploadPath, []byte("ebook"), 0o600); err != nil {
		t.Fatal(err)
	}
	var uploadValues url.Values
	var filename string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeJSON(w, map[string]string{"accessToken": "token"})
		case "/api/v1/books/book/content":
			if r.URL.Query().Get("bookType") != "epub" {
				t.Errorf("download format query = %q", r.URL.RawQuery)
			}
			_, _ = io.WriteString(w, "content")
		case "/api/v1/books/book/files":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Errorf("multipart parse: %v", err)
				return
			}
			uploadValues = r.MultipartForm.Value
			filename = r.MultipartForm.File["file"][0].Filename
			writeJSON(w, map[string]bool{"ok": true})
		case "/api/v1/libraries/library-1/book/book":
			writeJSON(w, map[string]any{"id": "book", "libraryId": "library-1", "files": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "u", "p", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "download.epub")
	file, _ := os.OpenFile(output, os.O_CREATE|os.O_WRONLY, 0o600)
	count, hash, err := client.DownloadContent(context.Background(), "book", "EPUB", file)
	_ = file.Close()
	if err != nil || count != 7 || hash == "" {
		t.Fatalf("download count=%d hash=%q err=%v", count, hash, err)
	}
	reference := BookReference{LibraryID: "library-1", BookID: "book"}
	if err := client.UploadFileScoped(context.Background(), reference, "azw3", uploadPath); err != nil {
		t.Fatal(err)
	}
	if uploadValues.Get("isBook") != "true" || uploadValues.Get("bookType") != "AZW3" || filename != "book.azw3" {
		t.Fatalf("multipart values=%v filename=%q", uploadValues, filename)
	}
	if err := client.UploadFileNamedScoped(context.Background(), reference, "azw3", uploadPath, "../Unsafe: Name.epub"); err != nil {
		t.Fatal(err)
	}
	if filename != "Unsafe_ Name.azw3" {
		t.Fatalf("named multipart filename=%q", filename)
	}
}

func TestClientDeletesExactFileBeforeReplacementAvoidingConflict(t *testing.T) {
	dir := t.TempDir()
	uploadPath := filepath.Join(dir, "replacement.mobi")
	if err := os.WriteFile(uploadPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldFilePresent := true
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch r.URL.Path {
		case "/api/v1/auth/login":
			writeJSON(w, map[string]string{"accessToken": "token"})
		case "/api/v1/libraries/library-1/book/book-1":
			writeJSON(w, map[string]any{"id": "book-1", "libraryId": "library-1", "files": []any{}})
		case "/api/v1/books/book-1/files/file-42":
			if r.Method != http.MethodDelete {
				t.Errorf("file mutation method = %s", r.Method)
			}
			oldFilePresent = false
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/books/book-1/files":
			if oldFilePresent {
				w.WriteHeader(http.StatusConflict)
				return
			}
			writeJSON(w, map[string]bool{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reference := BookReference{LibraryID: "library-1", BookID: "book-1"}
	if err := client.DeleteFileScoped(context.Background(), reference, "file-42"); err != nil {
		t.Fatal(err)
	}
	if err := client.UploadFileScoped(context.Background(), reference, "mobi", uploadPath); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"POST /api/v1/auth/login",
		"GET /api/v1/libraries/library-1/book/book-1",
		"DELETE /api/v1/books/book-1/files/file-42",
		"GET /api/v1/libraries/library-1/book/book-1",
		"POST /api/v1/books/book-1/files",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("replacement calls=%v, want %v", calls, want)
	}
}

func TestClientDeleteReportsMissingFileWithoutLeakingResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		if r.URL.Path == "/api/v1/libraries/library-1/book/book-1" {
			writeJSON(w, map[string]any{"id": "book-1", "libraryId": "library-1", "files": []any{}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "sensitive server detail")
	}))
	defer server.Close()
	client, err := New(server.URL, "user", "password", server.Client(), 1<<20, 1<<20, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteFileScoped(context.Background(), BookReference{LibraryID: "library-1", BookID: "book-1"}, "missing")
	if !errors.Is(err, ErrFileNotFound) || !errors.Is(err, ErrNotFound) || strings.Contains(err.Error(), "sensitive server detail") {
		t.Fatalf("missing delete error = %v", err)
	}
}

func TestClientRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			writeJSON(w, map[string]string{"accessToken": "token"})
			return
		}
		_, _ = io.WriteString(w, strings.Repeat("x", 100))
	}))
	defer server.Close()
	client, _ := New(server.URL, "u", "p", server.Client(), 64, 1<<20, time.Second)
	if _, err := client.GetBook(context.Background(), "book"); !errors.Is(err, ErrResponseTooBig) {
		t.Fatalf("oversized response error = %v", err)
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

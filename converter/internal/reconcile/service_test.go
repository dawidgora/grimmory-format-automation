package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"converter/internal/grimmory"
	"converter/internal/state"
)

func TestClassifyErrorUsesBoundedSecretSafeCategories(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "authentication", err: grimmory.ErrUnauthorized, want: "grimmory_authentication_failed"},
		{name: "remote status", err: &grimmory.HTTPError{Operation: "get book", Status: 502}, want: "remote_http_status"},
		{name: "partial remote status", err: newPartialError(&grimmory.HTTPError{Operation: "upload", Status: 503}), want: "remote_http_status"},
		{name: "invalid response", err: fmt.Errorf("body contains secret: %w", grimmory.ErrInvalidResponse), want: "invalid_response"},
		{name: "state", err: fmt.Errorf("database path /private/tmp/db: %w", ErrState), want: "state"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyError(test.err); got != test.want {
				t.Fatalf("ClassifyError() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestResolveLibraryPolicyUsesPriorityAndAllowedIntersection(t *testing.T) {
	policy, err := ResolveLibraryPolicy(grimmory.Library{
		ID: "1", FormatPriority: []string{"epub", "pdf", "mobi", "azw3"},
		AllowedFormats: []string{"epub", "mobi"},
	}, []string{"epub", "mobi", "azw3"}, []string{"epub", "azw3", "mobi"})
	if err != nil {
		t.Fatal(err)
	}
	if policy.MainFormat != "epub" || len(policy.FallbackFormats) != 2 || policy.FallbackFormats[0] != "mobi" || policy.FallbackFormats[1] != "azw3" || len(policy.OutputFormats) != 1 || policy.OutputFormats[0] != "mobi" {
		t.Fatalf("resolved policy = %+v", policy)
	}
	if _, err := ResolveLibraryPolicy(grimmory.Library{ID: "1", FormatPriority: []string{"pdf"}}, []string{"mobi"}, []string{"epub"}); err == nil {
		t.Fatal("unsupported library main format was accepted")
	}
	if _, err := ResolveLibraryPolicy(grimmory.Library{ID: "1", FormatPriority: []string{"epub"}, AllowedFormats: []string{"mobi"}}, nil, []string{"epub"}); err == nil {
		t.Fatal("disallowed library main format was accepted")
	}
}

type fakeRemote struct {
	mu          sync.Mutex
	book        grimmory.Book
	content     map[string][]byte
	uploads     []string
	deletes     []string
	getCount    int
	uploadError error
	uploadNoop  bool
	rejectOld   bool
	deleteError error
	deleteGone  bool
	tagError    error
	addedTags   []string
	removedTags []string
}

func (f *fakeRemote) GetLibrary(context.Context, string) (grimmory.Library, error) {
	return grimmory.Library{ID: "1", FormatPriority: []string{"epub", "azw3", "mobi"}}, nil
}
func (f *fakeRemote) GetLibraryBook(context.Context, string, string) (grimmory.Book, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCount++
	book := f.book
	if book.LibraryID == "" {
		book.LibraryID = "1"
	}
	book.Files = append([]grimmory.File(nil), book.Files...)
	return book, nil
}
func (f *fakeRemote) DownloadContentScoped(_ context.Context, _ grimmory.BookReference, format string, dst io.Writer) (int64, string, error) {
	f.mu.Lock()
	data := append([]byte(nil), f.content[format]...)
	f.mu.Unlock()
	if _, err := dst.Write(data); err != nil {
		return 0, "", err
	}
	digest := sha256.Sum256(data)
	return int64(len(data)), fmtHash(digest[:]), nil
}
func (f *fakeRemote) UploadFileNamedScoped(_ context.Context, _ grimmory.BookReference, format, filePath, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.uploadError != nil {
		return f.uploadError
	}
	if f.rejectOld {
		for _, file := range f.book.Files {
			if file.Format == format {
				return &grimmory.HTTPError{Operation: "file upload", Status: 409}
			}
		}
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	f.content[format] = data
	f.uploads = append(f.uploads, format)
	if f.uploadNoop {
		return nil
	}
	f.book.Files = append(f.book.Files, grimmory.File{ID: format + "-id", Format: format, Name: "book." + format, MTime: time.Now().UTC(), TrustedMTime: true})
	return nil
}
func (f *fakeRemote) UploadFileScoped(ctx context.Context, reference grimmory.BookReference, format, filePath string) error {
	return f.UploadFileNamedScoped(ctx, reference, format, filePath, "")
}
func (f *fakeRemote) DeleteFileScoped(_ context.Context, _ grimmory.BookReference, fileID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteError != nil {
		return f.deleteError
	}
	for index, file := range f.book.Files {
		if file.ID != fileID {
			continue
		}
		f.book.Files = append(f.book.Files[:index], f.book.Files[index+1:]...)
		f.deletes = append(f.deletes, fileID)
		if f.deleteGone {
			return grimmory.ErrNotFound
		}
		return nil
	}
	return grimmory.ErrNotFound
}
func (f *fakeRemote) AddBookTagScoped(_ context.Context, reference grimmory.BookReference, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tagError != nil {
		return f.tagError
	}
	f.addedTags = append(f.addedTags, reference.LibraryID+":"+tag)
	return nil
}
func (f *fakeRemote) RemoveBookTagScoped(_ context.Context, reference grimmory.BookReference, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tagError != nil {
		return f.tagError
	}
	f.removedTags = append(f.removedTags, reference.LibraryID+":"+tag)
	return nil
}

type memoryStore struct {
	mu          sync.Mutex
	book        state.BookState
	derived     map[string]state.DerivedState
	setBookErr  error
	setDerError error
}

func (s *memoryStore) Get(context.Context, string, string) (state.BookState, map[string]state.DerivedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyDerived := make(map[string]state.DerivedState, len(s.derived))
	for key, value := range s.derived {
		copyDerived[key] = value
	}
	return s.book, copyDerived, nil
}
func (s *memoryStore) SetBook(_ context.Context, value state.BookState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setBookErr != nil {
		return s.setBookErr
	}
	s.book = value
	return nil
}
func (s *memoryStore) SetDerived(_ context.Context, value state.DerivedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setDerError != nil {
		return s.setDerError
	}
	if s.derived == nil {
		s.derived = make(map[string]state.DerivedState)
	}
	s.derived[value.Format] = value
	return nil
}

type fakeConverter struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeConverter) Convert(_ context.Context, input, source, target, dir string) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, source+">"+target)
	f.mu.Unlock()
	data, err := os.ReadFile(input)
	if err != nil {
		return "", err
	}
	output, err := os.CreateTemp(dir, "converted-*."+target)
	if err != nil {
		return "", err
	}
	if _, err := output.Write(append(data, []byte("-"+target)...)); err != nil {
		_ = output.Close()
		_ = os.Remove(output.Name())
		return "", err
	}
	if err := output.Close(); err != nil {
		_ = os.Remove(output.Name())
		return "", err
	}
	return output.Name(), nil
}

func TestSelectSourceUsesConfiguredOrderAndNeverSelectsMain(t *testing.T) {
	files := []grimmory.File{{Format: "mobi", Name: "z"}, {Format: "azw3", Name: "a"}, {Format: "epub", Name: "main"}}
	file, ok := SelectSource(files, "epub", []string{"azw3", "mobi"})
	if !ok || file.Format != "azw3" {
		t.Fatalf("source = %+v ok=%v", file, ok)
	}
	if _, ok := SelectSource(files, "epub", []string{"epub"}); ok {
		t.Fatal("selected main as a derivative source")
	}
}

func TestPlanDerivativesHandlesMissingHashStaleTimestampAndForce(t *testing.T) {
	canonicalTime := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
	files := []grimmory.File{
		{Format: "mobi", Name: "mobi", MTime: canonicalTime.Add(-time.Hour), TrustedMTime: true},
		{Format: "azw3", Name: "azw3", MTime: canonicalTime, TrustedMTime: true},
	}
	generated := canonicalTime.Add(time.Minute)
	complete := func(format, source, output string) state.DerivedState {
		return state.DerivedState{BookID: "book", Format: format, GrimmoryFileID: "file", SourceSHA256: source, OutputSHA256: output, GeneratedAt: generated}
	}
	plans := PlanDerivatives(files, []string{"mobi", "azw3", "epub"}, "epub", "new", map[string]state.DerivedState{
		"mobi": complete("mobi", "old", "mobi-output"), "azw3": complete("azw3", "new", "azw3-output"),
	}, canonicalTime, true, false, false, true, nil)
	if plans[0].Action != "rebuild" || plans[0].Reason != "canonical_hash_changed" {
		t.Fatalf("hash plan = %+v", plans[0])
	}
	if plans[1].Action != "unchanged" {
		t.Fatalf("current plan = %+v", plans[1])
	}
	fromState := PlanDerivatives([]grimmory.File{{Format: "azw3", Name: "azw3"}}, []string{"azw3"}, "epub", "new", map[string]state.DerivedState{
		"azw3": {BookID: "book", Format: "azw3", GrimmoryFileID: "file", SourceSHA256: "new", OutputSHA256: "output", TrustedMTime: canonicalTime.Add(-time.Hour), HasMTime: true, GeneratedAt: generated},
	}, canonicalTime, true, false, false, true, nil)
	if fromState[0].Reason != "trusted_timestamp_stale" {
		t.Fatalf("stored timestamp plan = %+v", fromState[0])
	}
	forced := PlanDerivatives(files, []string{"mobi", "azw3"}, "epub", "new", nil, canonicalTime, true, false, true, true, nil)
	if forced[0].Reason != "forced" || forced[1].Reason != "forced" {
		t.Fatalf("force plans = %+v", forced)
	}
}

func TestPlanDerivativesRebuildsMissingOrIncompletePersistentState(t *testing.T) {
	files := []grimmory.File{{Format: "mobi", Name: "book.mobi"}}
	missing := PlanDerivatives(files, []string{"mobi"}, "epub", "sha", nil, time.Time{}, false, false, false, true, nil)
	if missing[0].Action != "rebuild" || missing[0].Reason != "state_missing" {
		t.Fatalf("missing state plan = %+v", missing)
	}
	incomplete := PlanDerivatives(files, []string{"mobi"}, "epub", "sha", map[string]state.DerivedState{
		"mobi": {BookID: "book", Format: "mobi", SourceSHA256: "sha"},
	}, time.Time{}, false, false, false, true, nil)
	if incomplete[0].Action != "rebuild" || incomplete[0].Reason != "state_incomplete" {
		t.Fatalf("incomplete state plan = %+v", incomplete)
	}
}

func TestPlanDerivativesUsesTargetAwareGenerationFingerprints(t *testing.T) {
	book := grimmory.Book{Metadata: grimmory.BookMetadata{
		Title: "Book", Authors: []string{"Author"}, Language: "en", Publisher: "Publisher",
		PublicationDate: "2025-01-02", Identifiers: map[string]string{"isbn": "9780000000001"},
		Series: "Series", SeriesIndex: "1", Tags: []string{"tag"}, Description: "Description", Comments: "Comments",
	}}
	outputs := []string{"epub", "azw3", "mobi", "pdf"}
	files := []grimmory.File{{ID: "epub-id", Format: "epub"}, {ID: "azw3-id", Format: "azw3"}, {ID: "mobi-id", Format: "mobi"}, {ID: "pdf-id", Format: "pdf"}}
	stateFor := func(fingerprints map[string]string) map[string]state.DerivedState {
		result := make(map[string]state.DerivedState, len(outputs))
		for _, format := range outputs {
			result[format] = state.DerivedState{
				BookID: "book", Format: format, GrimmoryFileID: format + "-id", SourceSHA256: "source", OutputSHA256: format + "-output",
				GenerationFingerprint: fingerprints[format], GeneratedAt: time.Unix(1, 0),
			}
		}
		return result
	}
	plan := func(value grimmory.Book, sourceName string) []DerivativePlan {
		fingerprints := DesiredGenerationFingerprints(value, "source", sourceName, outputs)
		return PlanDerivatives(files, outputs, "source", "source", stateFor(DesiredGenerationFingerprints(book, "source", "Book.epub", outputs)), time.Time{}, false, false, false, true, fingerprints)
	}

	current := plan(book, "Book.epub")
	for _, item := range current {
		if item.Action != "unchanged" {
			t.Fatalf("current plan = %+v", current)
		}
	}

	filenameChanged := plan(book, "Renamed.epub")
	for _, item := range filenameChanged {
		if item.Action != "rebuild" || item.Reason != "generation_fingerprint_changed" {
			t.Fatalf("filename change plan = %+v", filenameChanged)
		}
	}

	seriesChangedBook := book
	seriesChangedBook.Metadata.Series = "New Series"
	seriesChanged := plan(seriesChangedBook, "Book.epub")
	for _, item := range seriesChanged {
		switch item.Format {
		case "epub", "azw3":
			if item.Action != "rebuild" {
				t.Fatalf("series change did not rebuild %s: %+v", item.Format, seriesChanged)
			}
		case "mobi", "pdf":
			if item.Action != "unchanged" {
				t.Fatalf("series change rebuilt %s: %+v", item.Format, seriesChanged)
			}
		}
	}

	titleChangedBook := book
	titleChangedBook.Metadata.Title = "New Title"
	titleChanged := plan(titleChangedBook, "Book.epub")
	for _, item := range titleChanged {
		if item.Format == "pdf" {
			if item.Action != "unchanged" {
				t.Fatalf("title change rebuilt unsupported metadata target: %+v", titleChanged)
			}
			continue
		}
		if item.Action != "rebuild" {
			t.Fatalf("title change did not rebuild %s: %+v", item.Format, titleChanged)
		}
	}
}

func TestSyncCreatesMissingMainThenCanonicalDerivativesAndCleansWorkspace(t *testing.T) {
	tempRoot := t.TempDir()
	remote := &fakeRemote{book: grimmory.Book{ID: "book", Files: []grimmory.File{{ID: "source-id", Format: "mobi", Name: "source.mobi"}}}, content: map[string][]byte{"mobi": []byte("source")}}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi", "azw3"}, SupportedInputs: []string{"epub", "azw3", "mobi"}, MaxConcurrentBooks: 1, FailedProcessingTag: "failed", MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: tempRoot})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || result.Main.Status != "created" {
		t.Fatalf("result = %+v", result)
	}
	remote.mu.Lock()
	uploads := append([]string(nil), remote.uploads...)
	remote.mu.Unlock()
	if len(uploads) != 3 || uploads[0] != "epub" || uploads[1] != "mobi" || uploads[2] != "azw3" {
		t.Fatalf("uploads = %v", uploads)
	}
	if len(remote.removedTags) != 1 || remote.removedTags[0] != "1:failed" {
		t.Fatalf("failure tag removals = %v", remote.removedTags)
	}
	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("workspace leftovers = %v", entries)
	}
	store.mu.Lock()
	lastSuccessfulSync := store.book.LastSuccessfulSync
	store.mu.Unlock()
	if lastSuccessfulSync.IsZero() {
		t.Fatal("completed reconciliation did not record last successful sync")
	}
	if store.book.CanonicalFileID != "epub-id" || store.derived["mobi"].GrimmoryFileID != "mobi-id" || store.derived["mobi"].GeneratedAt.IsZero() || store.derived["mobi"].GenerationFingerprint == "" {
		t.Fatalf("verification state = book=%+v derived=%+v", store.book, store.derived)
	}
}

func TestSyncDryRunDoesNotDownloadConvertOrUpload(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", Files: []grimmory.File{{Format: "epub", Name: "book.epub"}, {Format: "mobi", Name: "book.mobi"}}}, content: map[string][]byte{"epub": []byte("main")}}
	service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{DryRun: true, Force: true})
	if err != nil || result.Status != "dry_run" || result.Derivatives[0].Reason != "forced" {
		t.Fatalf("dry run result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if len(remote.uploads) != 0 {
		t.Fatal("dry run uploaded a file")
	}
}

func TestSyncRebuildDeletesExactDerivativeBeforeConflictUpload(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content:   map[string][]byte{"epub": []byte("main")},
		rejectOld: true,
	}
	store := &memoryStore{derived: map[string]state.DerivedState{}}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if err != nil || result.Status != "completed" {
		t.Fatalf("replacement result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes := append([]string(nil), remote.deletes...)
	uploads := append([]string(nil), remote.uploads...)
	remote.mu.Unlock()
	if !reflect.DeepEqual(deletes, []string{"old-mobi-id"}) || !reflect.DeepEqual(uploads, []string{"mobi"}) {
		t.Fatalf("replacement calls deletes=%v uploads=%v", deletes, uploads)
	}
	store.mu.Lock()
	derived := store.derived["mobi"]
	store.mu.Unlock()
	if derived.GrimmoryFileID == "old-mobi-id" || derived.GrimmoryFileID == "" {
		t.Fatalf("replacement state=%+v", derived)
	}
}

func TestSyncProtectsPrimaryAndAmbiguousDerivativeInventories(t *testing.T) {
	tests := []struct {
		name  string
		files []grimmory.File
		cause error
	}{
		{name: "primary identity", files: []grimmory.File{{ID: "same-id", Format: "epub", Name: "book.epub"}, {ID: "same-id", Format: "mobi", Name: "book.mobi"}}, cause: ErrPrimaryFile},
		{name: "duplicate target", files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}, {ID: "one", Format: "mobi", Name: "book.mobi"}, {ID: "two", Format: "mobi", Name: "other.mobi"}}, cause: ErrAmbiguousFile},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			remote := &fakeRemote{book: grimmory.Book{ID: "book", Files: test.files}, content: map[string][]byte{"epub": []byte("main")}}
			service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
			result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
			if !errors.Is(err, ErrPartial) || result.Status != "partial" {
				t.Fatalf("unsafe replacement result=%+v err=%v", result, err)
			}
			var partial *partialError
			if !errors.As(err, &partial) || !errors.Is(partial.Cause(), test.cause) {
				t.Fatalf("unsafe replacement cause=%v, want %v", partial.Cause(), test.cause)
			}
			remote.mu.Lock()
			deletes, uploads := len(remote.deletes), len(remote.uploads)
			remote.mu.Unlock()
			if deletes != 0 || uploads != 0 {
				t.Fatalf("unsafe replacement mutated remote deletes=%d uploads=%d", deletes, uploads)
			}
		})
	}
}

func TestSyncStopsAfterDerivativeDeleteFailure(t *testing.T) {
	remote := &fakeRemote{
		book:        grimmory.Book{ID: "book", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}, {ID: "old-id", Format: "mobi", Name: "book.mobi"}}},
		content:     map[string][]byte{"epub": []byte("main")},
		deleteError: errors.New("delete failed"),
	}
	service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if !errors.Is(err, ErrPartial) || result.Status != "partial" {
		t.Fatalf("delete failure result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	remote.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("delete failure uploaded %d files", uploads)
	}
}

func TestSyncDoesNotPersistAfterDerivativeUploadOrVerificationFailure(t *testing.T) {
	for _, test := range []struct {
		name        string
		uploadError error
		uploadNoop  bool
	}{
		{name: "upload", uploadError: errors.New("upload failed")},
		{name: "verification", uploadNoop: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			remote := &fakeRemote{
				book:        grimmory.Book{ID: "book", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}, {ID: "old-id", Format: "mobi", Name: "book.mobi"}}},
				content:     map[string][]byte{"epub": []byte("main")},
				uploadError: test.uploadError,
				uploadNoop:  test.uploadNoop,
			}
			store := &memoryStore{derived: map[string]state.DerivedState{}}
			service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
			result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
			if !errors.Is(err, ErrPartial) || result.Status != "partial" {
				t.Fatalf("failure result=%+v err=%v", result, err)
			}
			remote.mu.Lock()
			deletes, uploads := len(remote.deletes), len(remote.uploads)
			remote.mu.Unlock()
			wantUploads := 1
			if test.uploadError != nil {
				wantUploads = 0
			}
			if deletes != 1 || uploads != wantUploads {
				t.Fatalf("failure calls deletes=%d uploads=%d, want uploads=%d", deletes, uploads, wantUploads)
			}
			store.mu.Lock()
			_, persisted := store.derived["mobi"]
			store.mu.Unlock()
			if persisted {
				t.Fatal("failed replacement persisted derived state")
			}
		})
	}
}

func TestSyncContinuesWhenPlannedDerivativeDisappearsBeforeDelete(t *testing.T) {
	remote := &fakeRemote{
		book:       grimmory.Book{ID: "book", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}, {ID: "old-id", Format: "mobi", Name: "book.mobi"}}},
		content:    map[string][]byte{"epub": []byte("main")},
		deleteGone: true,
	}
	service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if err != nil || result.Status != "completed" {
		t.Fatalf("disappeared derivative result=%+v err=%v", result, err)
	}
}

func TestSyncReportsFailureTagMutationFailure(t *testing.T) {
	remote := &fakeRemote{
		book:     grimmory.Book{ID: "book", Files: []grimmory.File{{ID: "epub-id", Name: "book.epub", Format: "epub"}}},
		content:  map[string][]byte{"epub": []byte("main")},
		tagError: errors.New("tag endpoint unavailable"),
	}
	service := New(Options{Client: remote, Store: &memoryStore{derived: make(map[string]state.DerivedState)}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, FailedProcessingTag: "failed"})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrFailureTagMutation) || result.Status != "partial" || result.Error != "failure_tag_failed" {
		t.Fatalf("failure tag result=%+v err=%v", result, err)
	}
}

func TestSyncPreservesDerivativeStateWhenStateWriteFails(t *testing.T) {
	old := state.DerivedState{BookID: "book", Format: "mobi", SourceSHA256: "old", OutputSHA256: "old-output"}
	lastSuccess := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	remote := &fakeRemote{book: grimmory.Book{ID: "book", Files: []grimmory.File{{Format: "epub", Name: "book.epub"}, {Format: "mobi", Name: "book.mobi"}}}, content: map[string][]byte{"epub": []byte("new-main")}}
	store := &memoryStore{book: state.BookState{BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalFileID: "old-main", CanonicalFileName: "book.epub", CanonicalSHA256: "old", LastSuccessfulSync: lastSuccess}, derived: map[string]state.DerivedState{"mobi": old}, setDerError: errors.New("database unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrPartial) || result.Status != "partial" {
		t.Fatalf("state failure result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	got := store.derived["mobi"]
	store.mu.Unlock()
	if got.SourceSHA256 != old.SourceSHA256 || got.OutputSHA256 != old.OutputSHA256 {
		t.Fatalf("failed state was overwritten: %+v", got)
	}
	if !store.book.LastSuccessfulSync.Equal(lastSuccess) {
		t.Fatalf("partial reconciliation changed last successful sync: %v", store.book.LastSuccessfulSync)
	}
}

func TestSyncSerializesSameBook(t *testing.T) {
	remote := &lockingRemote{book: grimmory.Book{ID: "book", Files: []grimmory.File{{Format: "epub", Name: "book.epub"}}}, started: make(chan struct{}), secondStarted: make(chan struct{}), release: make(chan struct{})}
	service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute})
	results := make(chan error, 2)
	go func() {
		_, err := service.Sync(context.Background(), "1", "book", SyncOptions{DryRun: true})
		results <- err
	}()
	<-remote.started
	go func() {
		_, err := service.Sync(context.Background(), "1", "book", SyncOptions{DryRun: true})
		results <- err
	}()
	select {
	case <-remote.secondStarted:
		t.Fatal("same-book requests overlapped")
	case <-time.After(50 * time.Millisecond):
	}
	close(remote.release)
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
}

type lockingRemote struct {
	mu            sync.Mutex
	book          grimmory.Book
	started       chan struct{}
	secondStarted chan struct{}
	release       chan struct{}
	active        int
	getCount      int
}

func (*lockingRemote) GetLibrary(context.Context, string) (grimmory.Library, error) {
	return grimmory.Library{ID: "1", FormatPriority: []string{"epub", "mobi"}}, nil
}
func (r *lockingRemote) GetLibraryBook(context.Context, string, string) (grimmory.Book, error) {
	r.mu.Lock()
	if r.started == nil {
		r.started, r.secondStarted, r.release = make(chan struct{}), make(chan struct{}), make(chan struct{})
	}
	r.active++
	r.getCount++
	if r.getCount == 1 {
		close(r.started)
	} else if r.active == 2 {
		close(r.secondStarted)
	}
	r.mu.Unlock()
	<-r.release
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	book := r.book
	book.LibraryID = "1"
	return book, nil
}
func (*lockingRemote) DownloadContentScoped(context.Context, grimmory.BookReference, string, io.Writer) (int64, string, error) {
	return 0, "", nil
}
func (*lockingRemote) UploadFileNamedScoped(context.Context, grimmory.BookReference, string, string, string) error {
	return nil
}
func (*lockingRemote) UploadFileScoped(context.Context, grimmory.BookReference, string, string) error {
	return nil
}
func (*lockingRemote) DeleteFileScoped(context.Context, grimmory.BookReference, string) error {
	return nil
}

func fmtHash(value []byte) string {
	result := ""
	for _, b := range value {
		result += "0123456789abcdef"[b>>4:b>>4+1] + "0123456789abcdef"[b&15:b&15+1]
	}
	return result
}

package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
	downloads   []string
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
	// downloadHook runs outside the fake's mutex and can model an inventory
	// change after candidate bytes were downloaded.
	downloadHook func(string)
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
	f.downloads = append(f.downloads, format)
	hook := f.downloadHook
	f.mu.Unlock()
	if hook != nil {
		hook(format)
	}
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
	digest := sha256.Sum256(data)
	f.book.Files = append(f.book.Files, grimmory.File{ID: format + "-id", Format: format, Name: "book." + format, SHA256: fmtHash(digest[:]), MTime: time.Now().UTC(), TrustedMTime: true})
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
	mu           sync.Mutex
	book         state.BookState
	derived      map[string]state.DerivedState
	intents      map[string]state.DerivedUploadIntent
	setBookErr   error
	setIntentErr error
	setDerError  error
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
func (s *memoryStore) GetDerivedUploadIntents(_ context.Context, _, _ string) (map[string]state.DerivedUploadIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyIntents := make(map[string]state.DerivedUploadIntent, len(s.intents))
	for key, value := range s.intents {
		copyIntents[key] = value
	}
	return copyIntents, nil
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
func (s *memoryStore) SetDerivedUploadIntent(_ context.Context, value state.DerivedUploadIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setIntentErr != nil {
		return s.setIntentErr
	}
	if s.intents == nil {
		s.intents = make(map[string]state.DerivedUploadIntent)
	}
	s.intents[value.Format] = value
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
	delete(s.intents, value.Format)
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

func TestPlanDerivativesUsesConversionInputGenerationFingerprints(t *testing.T) {
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
		if item.Action != "rebuild" || item.Reason != "generation_fingerprint_changed" || !item.Blocked {
			t.Fatalf("filename change plan = %+v", filenameChanged)
		}
	}

	seriesChangedBook := book
	seriesChangedBook.Metadata.Series = "New Series"
	seriesChanged := plan(seriesChangedBook, "Book.epub")
	for _, item := range seriesChanged {
		if item.Action != "unchanged" || item.Blocked {
			t.Fatalf("series metadata change changed %s: %+v", item.Format, seriesChanged)
		}
	}

	titleChangedBook := book
	titleChangedBook.Metadata.Title = "New Title"
	titleChanged := plan(titleChangedBook, "Book.epub")
	for _, item := range titleChanged {
		if item.Action != "unchanged" || item.Blocked {
			t.Fatalf("title metadata change changed %s: %+v", item.Format, titleChanged)
		}
	}
	if first := GenerationFingerprint(book, "source", "Book.epub", "mobi"); first != GenerationFingerprint(seriesChangedBook, "source", "Book.epub", "mobi") {
		t.Fatal("metadata-only change changed generation fingerprint")
	}
}

func TestSyncCreatesMissingMainThenCanonicalDerivativesAndCleansWorkspace(t *testing.T) {
	tempRoot := t.TempDir()
	remote := &fakeRemote{book: grimmory.Book{ID: "book", Files: []grimmory.File{{ID: "source-id", Format: "mobi", Name: "source.mobi"}}}, content: map[string][]byte{"mobi": []byte("source")}}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi", "azw3"}, SupportedInputs: []string{"epub", "azw3", "mobi"}, MaxConcurrentBooks: 1, FailedProcessingTag: "failed", MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: tempRoot})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Main.Status != "created" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("result = %+v", result)
	}
	remote.mu.Lock()
	uploads := append([]string(nil), remote.uploads...)
	deletes := append([]string(nil), remote.deletes...)
	removedTags := append([]string(nil), remote.removedTags...)
	remote.mu.Unlock()
	if len(uploads) != 2 || uploads[0] != "epub" || uploads[1] != "azw3" {
		t.Fatalf("uploads = %v", uploads)
	}
	if len(deletes) != 0 || len(removedTags) != 0 {
		t.Fatalf("existing derivative was mutated: deletes=%v removals=%v", deletes, removedTags)
	}
	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("workspace leftovers = %v", entries)
	}
	store.mu.Lock()
	bookState := store.book
	derivedState := make(map[string]state.DerivedState, len(store.derived))
	for format, value := range store.derived {
		derivedState[format] = value
	}
	store.mu.Unlock()
	lastSuccessfulSync := bookState.LastSuccessfulSync
	_, missingDerivativeTracked := derivedState["azw3"]
	_, existingDerivativeTracked := derivedState["mobi"]
	if !lastSuccessfulSync.IsZero() || !missingDerivativeTracked || existingDerivativeTracked {
		t.Fatalf("partial reconciliation state: lastSuccess=%v derived=%+v", lastSuccessfulSync, derivedState)
	}
	if bookState.CanonicalFileID != "epub-id" || derivedState["azw3"].GeneratedAt.IsZero() || derivedState["azw3"].GenerationFingerprint == "" {
		t.Fatalf("verification state = book=%+v derived=%+v", bookState, derivedState)
	}
}

func TestSyncDryRunDoesNotDownloadConvertOrUpload(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", Files: []grimmory.File{{Format: "epub", Name: "book.epub"}, {Format: "mobi", Name: "book.mobi"}}}, content: map[string][]byte{"epub": []byte("main")}}
	service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{DryRun: true, Force: true})
	if err != nil || result.Status != "dry_run" || result.Error != SafeReplacementUnavailableCode || result.Derivatives[0].Reason != "forced" || result.Derivatives[0].Status != "blocked" || result.Derivatives[0].Error != SafeReplacementUnavailableCode {
		t.Fatalf("dry run result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if len(remote.uploads) != 0 {
		t.Fatal("dry run uploaded a file")
	}
}

func TestSyncBlocksRebuildWithoutDeletingOrOverwritingDerivative(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if !errors.Is(err, ErrPartial) || !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("blocked result=%+v err=%v", result, err)
	}
	if len(result.Derivatives) != 1 || result.Derivatives[0].Action != "rebuild" || result.Derivatives[0].Status != "blocked" || result.Derivatives[0].Error != SafeReplacementUnavailableCode {
		t.Fatalf("blocked derivative result=%+v", result.Derivatives)
	}
	remote.mu.Lock()
	deletes := len(remote.deletes)
	uploads := len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 0 || uploads != 0 {
		t.Fatalf("blocked rebuild mutated remote deletes=%d uploads=%d", deletes, uploads)
	}
	converter.mu.Lock()
	conversions := len(converter.calls)
	converter.mu.Unlock()
	if conversions != 0 {
		t.Fatalf("blocked rebuild converted %d times", conversions)
	}
}

func TestSyncRecoversPartialSiblingWithoutBookSuccessGate(t *testing.T) {
	mainSHA := hashBytes([]byte("main"))
	mobiSHA := hashBytes([]byte("tracked mobi"))
	azw3SHA := hashBytes([]byte("uploaded azw3"))
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "mobi-id", Format: "mobi", Name: "book.mobi", SHA256: mobiSHA},
			{ID: "azw3-id", Format: "azw3", Name: "book.azw3", SHA256: azw3SHA},
		}},
		content: map[string][]byte{"epub": []byte("main"), "azw3": []byte("uploaded azw3")},
	}
	fingerprints := DesiredGenerationFingerprints(remote.book, mainSHA, "book.epub", []string{"mobi", "azw3"})
	store := &memoryStore{
		book: state.BookState{LibraryID: "1", BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalFileID: "main-id", CanonicalFileName: "book.epub", CanonicalSHA256: mainSHA, UpdatedAt: now},
		derived: map[string]state.DerivedState{
			"mobi": {LibraryID: "1", BookID: "book", Format: "mobi", GrimmoryFileID: "mobi-id", SourceSHA256: mainSHA, OutputSHA256: mobiSHA, GenerationFingerprint: fingerprints["mobi"], GeneratedAt: now, UpdatedAt: now},
		},
		intents: map[string]state.DerivedUploadIntent{
			"azw3": {LibraryID: "1", BookID: "book", Format: "azw3", OutputName: "book.azw3", OutputSHA256: azw3SHA, SourceSHA256: mainSHA, GenerationFingerprint: fingerprints["azw3"], UpdatedAt: now},
		},
	}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi", "azw3"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" {
		t.Fatalf("partial sibling recovery result=%+v err=%v", result, err)
	}
	if len(result.Derivatives) != 2 || result.Derivatives[0].Status != "unchanged" || result.Derivatives[1].Status != "adopted" {
		t.Fatalf("partial sibling recovery derivatives=%+v", result.Derivatives)
	}
	remote.mu.Lock()
	uploads, deletes := len(remote.uploads), len(remote.deletes)
	remote.mu.Unlock()
	if uploads != 0 || deletes != 0 {
		t.Fatalf("partial sibling recovery mutated remote uploads=%d deletes=%d", uploads, deletes)
	}
	store.mu.Lock()
	_, mobiTracked := store.derived["mobi"]
	azw3, azw3Tracked := store.derived["azw3"]
	intents := len(store.intents)
	store.mu.Unlock()
	if !mobiTracked || !azw3Tracked || azw3.GrimmoryFileID != "azw3-id" || intents != 0 {
		t.Fatalf("partial sibling recovery state mobi=%v azw3=%+v intents=%d", mobiTracked, azw3, intents)
	}
}

func TestSyncRetriesStateWriteFailureByAdoptingUploadIntent(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, setDerError: errors.New("state unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	first, firstErr := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(firstErr, ErrPartial) || first.Status != "partial" || first.Derivatives[0].Error != "state_write_failed" {
		t.Fatalf("state write failure result=%+v err=%v", first, firstErr)
	}
	store.mu.Lock()
	intent, intentExists := store.intents["mobi"]
	store.mu.Unlock()
	if !intentExists {
		t.Fatal("state write failure did not retain upload intent")
	}
	remote.mu.Lock()
	firstUploads := len(remote.uploads)
	remote.mu.Unlock()

	store.mu.Lock()
	store.setDerError = nil
	store.mu.Unlock()
	second, secondErr := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if secondErr != nil || second.Status != "completed" || len(second.Derivatives) != 1 || second.Derivatives[0].Status != "adopted" {
		t.Fatalf("state write retry result=%+v err=%v", second, secondErr)
	}
	remote.mu.Lock()
	secondUploads := len(remote.uploads)
	remote.mu.Unlock()
	if secondUploads != firstUploads {
		t.Fatalf("state write retry uploaded again: first=%d second=%d", firstUploads, secondUploads)
	}
	store.mu.Lock()
	_, intentRemains := store.intents[intent.Format]
	_, derived := store.derived["mobi"]
	store.mu.Unlock()
	if intentRemains || !derived {
		t.Fatalf("state write retry did not commit recovery intent=%v derived=%v", intentRemains, derived)
	}
}

func TestSyncAdoptsUsingDownloadedCandidateBytesNotInventoryHash(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, setDerError: errors.New("state unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrPartial) {
		t.Fatalf("initial state write failure = %v", err)
	}
	store.mu.Lock()
	store.setDerError = nil
	store.mu.Unlock()
	remote.mu.Lock()
	for index := range remote.book.Files {
		if remote.book.Files[index].Format == "mobi" {
			remote.book.Files[index].SHA256 = "inventory-hash-is-not-authoritative"
		}
	}
	remote.mu.Unlock()

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" || len(result.Derivatives) != 1 || result.Derivatives[0].Status != "adopted" {
		t.Fatalf("downloaded-byte adoption result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	downloads := append([]string(nil), remote.downloads...)
	remote.mu.Unlock()
	for _, format := range downloads {
		if format == "mobi" {
			return
		}
	}
	t.Fatalf("adoption did not download candidate: downloads=%v", downloads)
}

func TestSyncRetainsIntentWhenCandidateChangesBeforeAdoptionCommit(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, setDerError: errors.New("state unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrPartial) {
		t.Fatalf("initial state write failure = %v", err)
	}
	store.mu.Lock()
	store.setDerError = nil
	store.mu.Unlock()
	remote.mu.Lock()
	remote.downloadHook = func(format string) {
		if format != "mobi" {
			return
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		for index := range remote.book.Files {
			if remote.book.Files[index].Format == "mobi" {
				remote.book.Files[index].ID = "changed-before-commit"
				return
			}
		}
	}
	remote.mu.Unlock()

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Derivatives[0].Status != "blocked" {
		t.Fatalf("changed candidate result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	_, intentRemains := store.intents["mobi"]
	_, derived := store.derived["mobi"]
	store.mu.Unlock()
	if !intentRemains || derived {
		t.Fatalf("candidate change cleared or committed intent=%v derived=%v", intentRemains, derived)
	}
}

func TestSyncRetainsIntentWhenCanonicalSourceChangesBeforeAdoptionCommit(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, setDerError: errors.New("state unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrPartial) {
		t.Fatalf("initial state write failure = %v", err)
	}
	store.mu.Lock()
	store.setDerError = nil
	store.mu.Unlock()
	remote.mu.Lock()
	remote.downloadHook = func(format string) {
		if format != "mobi" {
			return
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		for index := range remote.book.Files {
			if remote.book.Files[index].Format == "epub" {
				remote.book.Files[index].ID = "changed-main-id"
				remote.book.Files[index].Name = "renamed.epub"
				return
			}
		}
	}
	remote.mu.Unlock()

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Derivatives[0].Status != "blocked" {
		t.Fatalf("changed canonical source result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	_, intentRemains := store.intents["mobi"]
	_, derived := store.derived["mobi"]
	store.mu.Unlock()
	if !intentRemains || derived {
		t.Fatalf("canonical source change cleared or committed intent=%v derived=%v", intentRemains, derived)
	}
}

func TestSyncRetainsIntentWhenFreshInventoryHasDuplicateCanonicalSources(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, setDerError: errors.New("state unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrPartial) {
		t.Fatalf("initial state write failure = %v", err)
	}
	store.mu.Lock()
	store.setDerError = nil
	store.mu.Unlock()
	remote.mu.Lock()
	remote.downloadHook = func(format string) {
		if format != "mobi" {
			return
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		remote.book.Files = append(remote.book.Files, grimmory.File{ID: "duplicate-main-id", Format: "epub", Name: "book.epub"})
	}
	remote.mu.Unlock()

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Derivatives[0].Status != "blocked" {
		t.Fatalf("duplicate canonical source result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	_, intentRemains := store.intents["mobi"]
	_, derived := store.derived["mobi"]
	store.mu.Unlock()
	if !intentRemains || derived {
		t.Fatalf("duplicate canonical source cleared or committed intent=%v derived=%v", intentRemains, derived)
	}
}

func TestSyncAdoptsWithOmittedCanonicalFilenameUsingEffectiveFallback(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub"}}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, setDerError: errors.New("state unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrPartial) {
		t.Fatalf("initial state write failure = %v", err)
	}
	store.mu.Lock()
	store.setDerError = nil
	store.mu.Unlock()

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" || result.Derivatives[0].Status != "adopted" {
		t.Fatalf("omitted canonical filename result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	_, intentRemains := store.intents["mobi"]
	_, derived := store.derived["mobi"]
	store.mu.Unlock()
	if intentRemains || !derived {
		t.Fatalf("omitted canonical filename state intent=%v derived=%v", intentRemains, derived)
	}
}

func TestSyncDoesNotAdoptPendingIntentForForcedRebuild(t *testing.T) {
	mainSHA := hashBytes([]byte("main"))
	outputSHA := hashBytes([]byte("pending output"))
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "pending-id", Format: "mobi", Name: "book.mobi", SHA256: outputSHA},
	}}, content: map[string][]byte{"epub": []byte("main"), "mobi": []byte("pending output")}}
	fingerprints := DesiredGenerationFingerprints(remote.book, mainSHA, "book.epub", []string{"mobi"})
	store := &memoryStore{
		book:    state.BookState{LibraryID: "1", BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalFileID: "main-id", CanonicalFileName: "book.epub", CanonicalSHA256: mainSHA},
		derived: map[string]state.DerivedState{},
		intents: map[string]state.DerivedUploadIntent{
			"mobi": {LibraryID: "1", BookID: "book", Format: "mobi", OutputName: "book.mobi", OutputSHA256: outputSHA, SourceSHA256: mainSHA, GenerationFingerprint: fingerprints["mobi"]},
		},
	}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Derivatives[0].Status != "blocked" {
		t.Fatalf("forced pending intent result=%+v err=%v", result, err)
	}
	converter.mu.Lock()
	conversions := len(converter.calls)
	converter.mu.Unlock()
	if conversions != 0 {
		t.Fatalf("forced pending intent was converted %d times", conversions)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	downloads := append([]string(nil), remote.downloads...)
	remote.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("forced pending intent uploaded %d files", uploads)
	}
	for _, format := range downloads {
		if format == "mobi" {
			t.Fatalf("forced pending intent downloaded candidate: %v", downloads)
		}
	}
	store.mu.Lock()
	_, intentRemains := store.intents["mobi"]
	_, derived := store.derived["mobi"]
	store.mu.Unlock()
	if !intentRemains || derived {
		t.Fatalf("forced pending intent changed state intent=%v derived=%v", intentRemains, derived)
	}
}

func TestGenerationFingerprintRetainsV1CheckpointVersion(t *testing.T) {
	if fingerprint := GenerationFingerprint(grimmory.Book{}, "source", "book.epub", "mobi"); !strings.HasPrefix(fingerprint, "v1:") {
		t.Fatalf("generation fingerprint version = %q", fingerprint)
	}
}

func TestPlanDerivativesAcceptsLegacyV1MetadataFingerprintWithCoreChecks(t *testing.T) {
	canonicalSHA := "source"
	canonicalName := "Book.epub"
	legacy := "v1:4e7bf234c5311dd0feedbb7a8de7102126e0e14115c2c12171a14f290af98773"
	desired := GenerationFingerprint(grimmory.Book{}, canonicalSHA, canonicalName, "mobi")
	if legacy == desired {
		t.Fatalf("legacy and metadata-free fingerprints unexpectedly match: %q", desired)
	}
	files := []grimmory.File{{ID: "mobi-id", Format: "mobi", Name: "Book.mobi"}}
	saved := map[string]state.DerivedState{"mobi": {
		BookID: "book", Format: "mobi", GrimmoryFileID: "mobi-id",
		SourceSHA256: canonicalSHA, OutputSHA256: "output", GenerationFingerprint: legacy,
		GeneratedAt: time.Unix(1, 0),
	}}
	desiredFingerprints := map[string]string{"mobi": desired}
	plans := PlanDerivatives(files, []string{"mobi"}, "epub", canonicalSHA, saved, time.Time{}, false, false, false, false, desiredFingerprints, canonicalName)
	if len(plans) != 1 || plans[0].Action != "unchanged" {
		t.Fatalf("legacy fingerprint plan = %+v", plans)
	}

	nameChanged := PlanDerivatives(files, []string{"mobi"}, "epub", canonicalSHA, saved, time.Time{}, false, false, false, false, desiredFingerprints, "Renamed.epub")
	if nameChanged[0].Reason != "output_name_changed" || !nameChanged[0].Blocked {
		t.Fatalf("legacy name check = %+v", nameChanged)
	}
	sourceChanged := PlanDerivatives(files, []string{"mobi"}, "epub", "changed-source", saved, time.Time{}, false, false, false, false, map[string]string{"mobi": GenerationFingerprint(grimmory.Book{}, "changed-source", canonicalName, "mobi")}, canonicalName)
	if sourceChanged[0].Reason != "canonical_hash_changed" || !sourceChanged[0].Blocked {
		t.Fatalf("legacy source check = %+v", sourceChanged)
	}
}

func TestSyncRetriesVisibilityFailureByAdoptingMatchingIntent(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main")}, uploadNoop: true}
	store := &memoryStore{derived: map[string]state.DerivedState{}}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	first, firstErr := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(firstErr, ErrPartial) || first.Status != "partial" || first.Derivatives[0].Error != "verification_failed" {
		t.Fatalf("visibility failure result=%+v err=%v", first, firstErr)
	}
	store.mu.Lock()
	intent, intentExists := store.intents["mobi"]
	store.mu.Unlock()
	if !intentExists {
		t.Fatal("visibility failure did not retain upload intent")
	}
	remote.mu.Lock()
	remote.uploadNoop = false
	remote.book.Files = append(remote.book.Files, grimmory.File{ID: "mobi-id", Format: "mobi", Name: intent.OutputName, SHA256: intent.OutputSHA256})
	firstUploads := len(remote.uploads)
	remote.mu.Unlock()

	second, secondErr := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if secondErr != nil || second.Status != "completed" || len(second.Derivatives) != 1 || second.Derivatives[0].Status != "adopted" {
		t.Fatalf("visibility retry result=%+v err=%v", second, secondErr)
	}
	remote.mu.Lock()
	secondUploads := len(remote.uploads)
	remote.mu.Unlock()
	if secondUploads != firstUploads {
		t.Fatalf("visibility retry uploaded again: first=%d second=%d", firstUploads, secondUploads)
	}
	store.mu.Lock()
	_, intentRemains := store.intents[intent.Format]
	store.mu.Unlock()
	if intentRemains {
		t.Fatal("visibility retry did not clear recovered intent")
	}
}

func TestSyncRejectsNonMatchingIntentCandidate(t *testing.T) {
	mainSHA := hashBytes([]byte("main"))
	actualSHA := hashBytes([]byte("actual remote bytes"))
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "mobi-id", Format: "mobi", Name: "book.mobi", SHA256: actualSHA},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, intents: map[string]state.DerivedUploadIntent{
		"mobi": {LibraryID: "1", BookID: "book", Format: "mobi", OutputName: "book.mobi", OutputSHA256: hashBytes([]byte("intended bytes")), SourceSHA256: mainSHA},
	}}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("non-matching candidate result=%+v err=%v", result, err)
	}
	if len(result.Derivatives) != 1 || result.Derivatives[0].Status != "blocked" || result.Derivatives[0].Error != SafeReplacementUnavailableCode {
		t.Fatalf("non-matching candidate derivative=%+v", result.Derivatives)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	remote.mu.Unlock()
	converter.mu.Lock()
	conversions := len(converter.calls)
	converter.mu.Unlock()
	if uploads != 0 || conversions != 0 {
		t.Fatalf("non-matching candidate was replaced uploads=%d conversions=%d", uploads, conversions)
	}
}

func TestSyncDoesNotUploadWhenIntentPersistenceFails(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{}, setIntentErr: errors.New("intent state unavailable")}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrPartial) || result.Status != "partial" || result.Derivatives[0].Error != "state_write_failed" {
		t.Fatalf("intent persistence failure result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	remote.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("intent persistence failure uploaded %d files", uploads)
	}
	converter.mu.Lock()
	conversions := len(converter.calls)
	converter.mu.Unlock()
	if conversions != 1 {
		t.Fatalf("intent persistence failure did not produce artifact before refusing upload: conversions=%d", conversions)
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
				book:        grimmory.Book{ID: "book", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}},
				content:     map[string][]byte{"epub": []byte("main")},
				uploadError: test.uploadError,
				uploadNoop:  test.uploadNoop,
			}
			store := &memoryStore{derived: map[string]state.DerivedState{}}
			service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
			result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
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
			if deletes != 0 || uploads != wantUploads {
				t.Fatalf("failure calls deletes=%d uploads=%d, want uploads=%d", deletes, uploads, wantUploads)
			}
			store.mu.Lock()
			_, persisted := store.derived["mobi"]
			store.mu.Unlock()
			if persisted {
				t.Fatal("failed creation persisted derived state")
			}
		})
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

func hashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return fmtHash(digest[:])
}

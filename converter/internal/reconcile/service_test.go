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
	mu                      sync.Mutex
	book                    grimmory.Book
	content                 map[string][]byte
	uploads                 []string
	downloads               []string
	deletes                 []string
	getCount                int
	uploadError             error
	uploadAfterMutationErr  error
	uploadNoop              bool
	rejectOld               bool
	deleteError             error
	deleteGone              bool
	omitChecksum            bool
	reappearAfterDelete     bool
	tagError                error
	removeTagError          error
	removeTagAfterMutation  bool
	getAfterTagRemovalError error
	addedTags               []string
	removedTags             []string
	deleteHook              func(context.Context)
	afterDeleteHook         func()
	// downloadHook runs outside the fake's mutex and can model an inventory
	// change after candidate bytes were downloaded.
	downloadHook func(string)
	// uploadHook runs after a successful remote upload and models a concurrent
	// canonical-source change before upload verification completes.
	uploadHook func(string)
}

func (f *fakeRemote) GetLibrary(context.Context, string) (grimmory.Library, error) {
	return grimmory.Library{ID: "1", FormatPriority: []string{"epub", "azw3", "mobi"}}, nil
}
func (f *fakeRemote) GetLibraryBook(context.Context, string, string) (grimmory.Book, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getAfterTagRemovalError != nil && len(f.removedTags) > 0 {
		return grimmory.Book{}, f.getAfterTagRemovalError
	}
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
	if f.uploadError != nil {
		f.mu.Unlock()
		return f.uploadError
	}
	if f.rejectOld {
		for _, file := range f.book.Files {
			if file.Format == format {
				f.mu.Unlock()
				return &grimmory.HTTPError{Operation: "file upload", Status: 409}
			}
		}
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	f.content[format] = data
	f.uploads = append(f.uploads, format)
	if f.uploadNoop {
		hook := f.uploadHook
		f.mu.Unlock()
		if hook != nil {
			hook(format)
		}
		return nil
	}
	checksum := ""
	if !f.omitChecksum {
		digest := sha256.Sum256(data)
		checksum = fmtHash(digest[:])
	}
	f.book.Files = append(f.book.Files, grimmory.File{ID: format + "-id", Format: format, Name: "book." + format, SHA256: checksum, MTime: time.Now().UTC(), TrustedMTime: true})
	hook := f.uploadHook
	afterMutationErr := f.uploadAfterMutationErr
	f.mu.Unlock()
	if hook != nil {
		hook(format)
	}
	if afterMutationErr != nil {
		return afterMutationErr
	}
	return nil
}
func (f *fakeRemote) UploadFileScoped(ctx context.Context, reference grimmory.BookReference, format, filePath string) error {
	return f.UploadFileNamedScoped(ctx, reference, format, filePath, "")
}
func (f *fakeRemote) DeleteFileScoped(ctx context.Context, _ grimmory.BookReference, fileID string) error {
	f.mu.Lock()
	hook := f.deleteHook
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	if f.deleteError != nil {
		f.mu.Unlock()
		return f.deleteError
	}
	var afterDeleteHook func()
	deleteGone := false
	found := false
	for index, file := range f.book.Files {
		if file.ID != fileID {
			continue
		}
		found = true
		f.book.Files = append(f.book.Files[:index], f.book.Files[index+1:]...)
		f.deletes = append(f.deletes, fileID)
		if f.reappearAfterDelete {
			f.book.Files = append(f.book.Files, grimmory.File{ID: "reappeared-mobi-id", Format: file.Format, Name: file.Name})
		}
		deleteGone = f.deleteGone
		afterDeleteHook = f.afterDeleteHook
		break
	}
	f.mu.Unlock()
	if afterDeleteHook != nil {
		afterDeleteHook()
	}
	if !found {
		return grimmory.ErrNotFound
	}
	if deleteGone {
		return grimmory.ErrNotFound
	}
	return nil
}
func (f *fakeRemote) AddBookTagScoped(_ context.Context, reference grimmory.BookReference, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tagError != nil {
		return f.tagError
	}
	f.addedTags = append(f.addedTags, reference.LibraryID+":"+tag)
	for _, existing := range f.book.Metadata.Tags {
		if existing == tag {
			return nil
		}
	}
	f.book.Metadata.Tags = append(f.book.Metadata.Tags, tag)
	return nil
}
func (f *fakeRemote) RemoveBookTagScoped(_ context.Context, reference grimmory.BookReference, tag string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	removeErr := f.removeTagError
	if removeErr == nil {
		removeErr = f.tagError
	}
	if removeErr != nil && !f.removeTagAfterMutation {
		return removeErr
	}
	f.removedTags = append(f.removedTags, reference.LibraryID+":"+tag)
	filtered := f.book.Metadata.Tags[:0]
	for _, existing := range f.book.Metadata.Tags {
		if existing != tag {
			filtered = append(filtered, existing)
		}
	}
	f.book.Metadata.Tags = filtered
	if removeErr != nil {
		return removeErr
	}
	return nil
}

type memoryStore struct {
	mu          sync.Mutex
	book        state.BookState
	derived     map[string]state.DerivedState
	intents     map[string]state.DerivedUploadIntent
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

func (s *memoryStore) GetDerivedUploadIntents(_ context.Context, _, _ string) (map[string]state.DerivedUploadIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]state.DerivedUploadIntent, len(s.intents))
	for format, value := range s.intents {
		result[format] = value
	}
	return result, nil
}

func (s *memoryStore) SetDerivedUploadIntent(_ context.Context, value state.DerivedUploadIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.intents == nil {
		s.intents = make(map[string]state.DerivedUploadIntent)
	}
	s.intents[value.Format] = value
	return nil
}

func (s *memoryStore) PrepareReplacement(_ context.Context, value state.DerivedUploadIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.intents == nil {
		s.intents = make(map[string]state.DerivedUploadIntent)
	}
	s.intents[value.Format] = value
	s.book.ReplacementInProgress = true
	if value.ReplacementTag != "" {
		s.book.ReplacementInProgressTag = value.ReplacementTag
	}
	return nil
}

func (s *memoryStore) CommitDerived(_ context.Context, value state.DerivedState, pendingTag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setDerError != nil {
		return s.setDerError
	}
	if s.derived == nil {
		s.derived = make(map[string]state.DerivedState)
	}
	s.derived[value.Format] = value
	s.book.ReplacementInProgress = false
	if pendingTag != "" {
		s.book.ReplacementInProgressTag = pendingTag
	}
	if s.intents != nil {
		delete(s.intents, value.Format)
	}
	return nil
}

func (s *memoryStore) MarkPendingReplacementCleanup(_ context.Context, _, _, tag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.book.PendingReplacementTag != tag && s.book.ReplacementInProgressTag != tag {
		return errors.New("replacement authorization is not in progress")
	}
	s.book.PendingReplacementTag = tag
	s.book.ReplacementInProgressTag = ""
	s.book.ReplacementInProgress = false
	return nil
}

func (s *memoryStore) ClearPendingReplacementTag(_ context.Context, _, _, tag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.book.PendingReplacementTag == tag {
		s.book.PendingReplacementTag = ""
	}
	return nil
}

type fakeConverter struct {
	mu          sync.Mutex
	calls       []string
	failTargets map[string]error
}

func (f *fakeConverter) Convert(_ context.Context, input, source, target, dir string) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, source+">"+target)
	fail := f.failTargets[target]
	f.mu.Unlock()
	if fail != nil {
		return "", fail
	}
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
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{DryRun: true})
	if err != nil || result.Status != "dry_run" || result.Error != SafeReplacementUnavailableCode || result.Derivatives[0].Reason != "state_missing" || result.Derivatives[0].Status != "blocked" || result.Derivatives[0].Error != SafeReplacementUnavailableCode {
		t.Fatalf("dry run result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if len(remote.uploads) != 0 {
		t.Fatal("dry run uploaded a file")
	}
}

func TestSyncDryRunDoesNotPerformPendingReplacementCleanup(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{
		ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}},
		Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}, {ID: "mobi-id", Format: "mobi", Name: "book.mobi"}},
	}}
	store := &memoryStore{book: state.BookState{LibraryID: "1", BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: "main", PendingReplacementTag: "replace-me"}}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{DryRun: true})
	if err != nil || result.Status != "dry_run" {
		t.Fatalf("dry-run cleanup result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	removed := len(remote.removedTags)
	remote.mu.Unlock()
	store.mu.Lock()
	pending, lastSuccessful := store.book.PendingReplacementTag, store.book.LastSuccessfulSync
	store.mu.Unlock()
	if removed != 0 || pending != "replace-me" || !lastSuccessful.IsZero() {
		t.Fatalf("dry-run cleanup mutated remote/state removed=%d pending=%q last=%v", removed, pending, lastSuccessful)
	}
}

func TestSyncRetainsInProgressReplacementAuthorizationAcrossPartialDerivatives(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		{ID: "old-azw3-id", Format: "azw3", Name: "book.azw3"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	converter := &fakeConverter{failTargets: map[string]error{"azw3": errors.New("azw3 conversion failed")}}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi", "azw3"}, SupportedInputs: []string{"epub"}, DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	first, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrPartial) || first.Status != "partial" {
		t.Fatalf("partial replacement result=%+v err=%v", first, err)
	}
	store.mu.Lock()
	firstBook := store.book
	_, mobiCommitted := store.derived["mobi"]
	_, azw3Committed := store.derived["azw3"]
	store.mu.Unlock()
	if firstBook.ReplacementInProgressTag != "replace-me" || firstBook.PendingReplacementTag != "" || !mobiCommitted || azw3Committed {
		t.Fatalf("partial replacement state=%+v mobi=%v azw3=%v", firstBook, mobiCommitted, azw3Committed)
	}

	converter.mu.Lock()
	firstConversions := len(converter.calls)
	converter.failTargets = nil
	converter.mu.Unlock()
	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" {
		t.Fatalf("resumed replacement result=%+v err=%v", second, err)
	}
	converter.mu.Lock()
	totalConversions := len(converter.calls)
	converter.mu.Unlock()
	remote.mu.Lock()
	deletes, uploads, removals := len(remote.deletes), len(remote.uploads), len(remote.removedTags)
	remote.mu.Unlock()
	store.mu.Lock()
	finalBook := store.book
	store.mu.Unlock()
	if totalConversions != firstConversions+1 || deletes != 2 || uploads != 2 || removals != 1 || !finalBook.LastSuccessfulSync.After(firstBook.LastSuccessfulSync) || finalBook.ReplacementInProgressTag != "" || finalBook.PendingReplacementTag != "" {
		t.Fatalf("resumed replacement repeated work conversions=%d first=%d deletes=%d uploads=%d removals=%d state=%+v", totalConversions, firstConversions, deletes, uploads, removals, finalBook)
	}
}

func TestSyncRecoversLaterOutputUsingEvolvingInventory(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	remote.uploadHook = func(format string) {
		if format != "azw3" {
			return
		}
		remote.mu.Lock()
		remote.content["epub"] = []byte("changed-main")
		remote.mu.Unlock()
	}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi", "azw3"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	first, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || first.Status != "partial" {
		t.Fatalf("initial evolving-inventory result=%+v err=%v", first, err)
	}
	store.mu.Lock()
	_, firstCommitted := store.derived["mobi"]
	_, secondIntent := store.intents["azw3"]
	store.mu.Unlock()
	if !firstCommitted || !secondIntent {
		t.Fatalf("initial evolving-inventory state first=%v secondIntent=%v", firstCommitted, secondIntent)
	}
	converter.mu.Lock()
	firstConversions := len(converter.calls)
	converter.mu.Unlock()
	remote.mu.Lock()
	remote.content["epub"] = []byte("main")
	remote.uploadHook = nil
	remote.mu.Unlock()

	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" || len(second.Derivatives) != 2 || second.Derivatives[1].Status != "adopted" {
		t.Fatalf("evolving-inventory recovery result=%+v err=%v", second, err)
	}
	converter.mu.Lock()
	totalConversions := len(converter.calls)
	converter.mu.Unlock()
	remote.mu.Lock()
	uploads, deletes := len(remote.uploads), len(remote.deletes)
	remote.mu.Unlock()
	store.mu.Lock()
	_, mobiCommitted := store.derived["mobi"]
	_, azw3Committed := store.derived["azw3"]
	_, intentRemaining := store.intents["azw3"]
	store.mu.Unlock()
	if totalConversions != firstConversions || uploads != 2 || deletes != 0 || !mobiCommitted || !azw3Committed || intentRemaining {
		t.Fatalf("evolving-inventory repeated work conversions=%d/%d uploads=%d deletes=%d mobi=%v azw3=%v intent=%v", totalConversions, firstConversions, uploads, deletes, mobiCommitted, azw3Committed, intentRemaining)
	}
}

func TestSyncClearsRecoveredPhaseBeforeIndependentLaterOutput(t *testing.T) {
	mainSHA := hashBytes([]byte("main"))
	initialFiles := []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		{ID: "old-azw3-id", Format: "azw3", Name: "book.azw3"},
	}
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "new-mobi-id", Format: "mobi", Name: "book.mobi"},
		{ID: "old-azw3-id", Format: "azw3", Name: "book.azw3"},
	}}, content: map[string][]byte{"epub": []byte("main"), "mobi": []byte("mobi-output")}}
	plan := DerivativePlan{Format: "mobi", GenerationFingerprint: GenerationFingerprint(grimmory.Book{}, mainSHA, "book.epub", "mobi")}
	intent := uploadIntentFor("1", "book", plan, "book.epub", mainSHA, initialFiles[0], stableInventoryFingerprint(initialFiles, "mobi"), false, "")
	intent.OutputSHA256 = hashBytes([]byte("mobi-output"))
	intent.ReplacementTargetID = "old-mobi-id"
	intent.ReplacementTargetName = "book.mobi"
	intent.ReplacementTargetFormat = "mobi"
	creationPlan := DerivativePlan{Format: "azw3", GenerationFingerprint: GenerationFingerprint(grimmory.Book{}, mainSHA, "book.epub", "azw3")}
	creationIntent := uploadIntentFor("1", "book", creationPlan, "book.epub", mainSHA, initialFiles[0], stableInventoryFingerprint(initialFiles, "azw3"), false, "")
	creationIntent.OutputSHA256 = hashBytes([]byte("unrelated-output"))
	store := &memoryStore{
		book:    state.BookState{LibraryID: "1", BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalFileID: "main-id", CanonicalFileName: "book.epub", CanonicalSHA256: mainSHA, ReplacementInProgress: true},
		intents: map[string]state.DerivedUploadIntent{"mobi": intent, "azw3": creationIntent}, derived: make(map[string]state.DerivedState),
	}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"azw3", "mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "preserve", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrPartial) || result.Status != "partial" || len(result.Derivatives) != 2 || result.Derivatives[0].Status != "adopted" || result.Derivatives[1].Status != "blocked" {
		t.Fatalf("independent later output result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	store.mu.Lock()
	phase := store.book.ReplacementInProgress
	_, recovered := store.derived["mobi"]
	store.mu.Unlock()
	if deletes != 0 || uploads != 0 || phase || !recovered {
		t.Fatalf("later output reused recovered destructive phase deletes=%d uploads=%d phase=%v recovered=%v", deletes, uploads, phase, recovered)
	}
}

func TestSyncStopsLaterOutputMutationWhenEarlierIntentNeedsRecovery(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	remote.uploadHook = func(format string) {
		if format != "mobi" {
			return
		}
		remote.mu.Lock()
		remote.content["epub"] = []byte("changed-main")
		remote.mu.Unlock()
	}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	converter := &fakeConverter{}
	service := New(Options{Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi", "azw3"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	first, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || first.Status != "partial" {
		t.Fatalf("earlier intent failure result=%+v err=%v", first, err)
	}
	remote.mu.Lock()
	firstUploads := len(remote.uploads)
	remote.uploadHook = nil
	remote.content["epub"] = []byte("main")
	remote.mu.Unlock()
	store.mu.Lock()
	_, firstIntent := store.intents["mobi"]
	_, laterIntent := store.intents["azw3"]
	store.mu.Unlock()
	if firstUploads != 1 || !firstIntent || laterIntent {
		t.Fatalf("later output mutated despite earlier intent uploads=%d firstIntent=%v laterIntent=%v", firstUploads, firstIntent, laterIntent)
	}
	converter.mu.Lock()
	firstConversions := len(converter.calls)
	converter.mu.Unlock()

	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" || second.Derivatives[0].Status != "adopted" {
		t.Fatalf("earlier intent recovery result=%+v err=%v", second, err)
	}
	converter.mu.Lock()
	totalConversions := len(converter.calls)
	converter.mu.Unlock()
	remote.mu.Lock()
	totalUploads := len(remote.uploads)
	remote.mu.Unlock()
	store.mu.Lock()
	_, mobiCommitted := store.derived["mobi"]
	_, azw3Committed := store.derived["azw3"]
	_, intentRemaining := store.intents["mobi"]
	store.mu.Unlock()
	if totalConversions != firstConversions+1 || totalUploads != 2 || !mobiCommitted || !azw3Committed || intentRemaining {
		t.Fatalf("earlier intent recovery wedged conversions=%d/%d uploads=%d mobi=%v azw3=%v intent=%v", totalConversions, firstConversions, totalUploads, mobiCommitted, azw3Committed, intentRemaining)
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
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
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

func TestSyncForceAuthorizesReplacementUnderPreserve(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		ExistingDerivativePolicy: "preserve", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if err != nil || result.Status != "completed" || result.Derivatives[0].Status != "uploaded" {
		t.Fatalf("force replacement result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes := append([]string(nil), remote.deletes...)
	remote.mu.Unlock()
	if len(deletes) != 1 || deletes[0] != "old-mobi-id" {
		t.Fatalf("force replacement deletes=%v", deletes)
	}
}

func TestSyncDoesNotPersistDerivativeWhenCanonicalChangesAfterUpload(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	remote.uploadHook = func(format string) {
		if format != "mobi" {
			return
		}
		remote.mu.Lock()
		remote.content["epub"] = []byte("changed-main")
		remote.mu.Unlock()
	}
	store := &memoryStore{derived: map[string]state.DerivedState{}}
	service := New(Options{
		Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"},
		OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace",
		MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("canonical change after upload result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	_, persisted := store.derived["mobi"]
	store.mu.Unlock()
	if persisted {
		t.Fatal("canonical change after upload persisted derivative state")
	}
}

func TestSyncDoesNotPersistDerivativeWhenTargetSwapsDuringCanonicalRevalidation(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	uploaded := false
	remote.uploadHook = func(format string) {
		if format == "mobi" {
			uploaded = true
		}
	}
	remote.downloadHook = func(format string) {
		if !uploaded || format != "epub" {
			return
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		for index := range remote.book.Files {
			if remote.book.Files[index].Format == "mobi" {
				remote.book.Files[index].ID = "swapped-mobi-id"
			}
		}
	}
	store := &memoryStore{derived: map[string]state.DerivedState{}}
	service := New(Options{
		Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"},
		OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace",
		MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("target swap during canonical revalidation result=%+v err=%v", result, err)
	}
	store.mu.Lock()
	_, persisted := store.derived["mobi"]
	store.mu.Unlock()
	if persisted {
		t.Fatal("target swap during canonical revalidation persisted derivative state")
	}
}

func TestSyncGlobalReplaceSafelyReplacesExistingDerivative(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content:      map[string][]byte{"epub": []byte("main")},
		deleteGone:   true,
		omitChecksum: true,
	}
	store := &memoryStore{derived: map[string]state.DerivedState{}}
	converter := &fakeConverter{}
	service := New(Options{
		Client: remote, Store: store, Converter: converter, LibraryIDs: []string{"1"},
		OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1,
		MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" || result.Derivatives[0].Status != "uploaded" {
		t.Fatalf("global replace result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes := append([]string(nil), remote.deletes...)
	uploads := append([]string(nil), remote.uploads...)
	files := append([]grimmory.File(nil), remote.book.Files...)
	remote.mu.Unlock()
	if len(deletes) != 1 || deletes[0] != "old-mobi-id" || len(uploads) != 1 || uploads[0] != "mobi" {
		t.Fatalf("global replace mutations deletes=%v uploads=%v", deletes, uploads)
	}
	if len(files) != 2 || files[1].ID == "old-mobi-id" {
		t.Fatalf("global replace files=%v", files)
	}
	remote.mu.Lock()
	downloads := append([]string(nil), remote.downloads...)
	remote.mu.Unlock()
	verified := false
	for _, format := range downloads {
		if format == "mobi" {
			verified = true
			break
		}
	}
	if !verified {
		t.Fatalf("missing-checksum derivative was not downloaded for verification: %v", downloads)
	}
}

func TestSyncReplacementContinuesAfterCallerCancellationOnceDeleteStarts(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	ctx, cancel := context.WithCancel(context.Background())
	remote.deleteHook = func(context.Context) { cancel() }
	service := New(Options{Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(ctx, "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" {
		t.Fatalf("replacement cancellation result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 1 || uploads != 1 {
		t.Fatalf("replacement cancellation mutations deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestSyncLatchesReplacementAuthorizationAfterDelete(t *testing.T) {
	for _, test := range []struct {
		name       string
		changeBook func(*fakeRemote)
		options    Options
	}{
		{
			name: "replacement tag removed",
			changeBook: func(remote *fakeRemote) {
				filtered := remote.book.Metadata.Tags[:0]
				for _, tag := range remote.book.Metadata.Tags {
					if tag != "replace-me" {
						filtered = append(filtered, tag)
					}
				}
				remote.book.Metadata.Tags = filtered
			},
			options: Options{DerivativeReplacementTag: "replace-me"},
		},
		{
			name: "ignore tag added",
			changeBook: func(remote *fakeRemote) {
				remote.book.Metadata.Tags = append(remote.book.Metadata.Tags, "ignore-me")
			},
			options: Options{DerivativeReplacementTag: "replace-me", IgnoreProcessingTag: "ignore-me"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
				{ID: "main-id", Format: "epub", Name: "book.epub"},
				{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
			}}, content: map[string][]byte{"epub": []byte("main")}}
			remote.afterDeleteHook = func() {
				remote.mu.Lock()
				test.changeBook(remote)
				remote.mu.Unlock()
			}
			remote.uploadHook = func(format string) {
				if format != "mobi" {
					return
				}
				remote.mu.Lock()
				remote.content["epub"] = []byte("changed-main")
				remote.mu.Unlock()
			}
			store := &memoryStore{derived: map[string]state.DerivedState{}}
			options := test.options
			options.Client, options.Store, options.Converter = remote, store, &fakeConverter{}
			options.LibraryIDs = []string{"1"}
			options.OutputFormats = []string{"mobi"}
			options.SupportedInputs = []string{"epub"}
			options.MaxConcurrentBooks = 1
			options.MaxFileBytes = 1 << 20
			options.ConversionTimeout = time.Minute
			options.TempRoot = t.TempDir()
			result, err := New(options).Sync(context.Background(), "1", "book", SyncOptions{})
			if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" {
				t.Fatalf("latched authorization result=%+v err=%v", result, err)
			}
			remote.mu.Lock()
			deletes, uploads := len(remote.deletes), len(remote.uploads)
			remote.mu.Unlock()
			store.mu.Lock()
			_, committed := store.derived["mobi"]
			_, intentRetained := store.intents["mobi"]
			store.mu.Unlock()
			if deletes != 1 || uploads != 1 || committed || !intentRetained {
				t.Fatalf("latched authorization state deletes=%d uploads=%d committed=%v intent=%v", deletes, uploads, committed, intentRetained)
			}
		})
	}
}

func TestSyncRefreshesIgnoreAfterFreshDestructiveReplacement(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		{ID: "old-azw3-id", Format: "azw3", Name: "book.azw3"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	remote.afterDeleteHook = func() {
		remote.mu.Lock()
		remote.book.Metadata.Tags = append(remote.book.Metadata.Tags, "ignore-me")
		remote.mu.Unlock()
	}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi", "azw3"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", IgnoreProcessingTag: "ignore-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "ignored" || len(result.Derivatives) != 1 || result.Derivatives[0].Status != "uploaded" {
		t.Fatalf("fresh destructive ignore result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	store.mu.Lock()
	_, recovered := store.derived["mobi"]
	store.mu.Unlock()
	if deletes != 1 || uploads != 1 || !recovered {
		t.Fatalf("later output mutated after ignore appeared deletes=%d uploads=%d recovered=%v", deletes, uploads, recovered)
	}
}

func TestSyncResumesDetachedReplacementBeforeIgnoreHandling(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}, uploadError: errors.New("detached upload failed")}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", IgnoreProcessingTag: "ignore-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	first, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrPartial) || first.Status != "partial" {
		t.Fatalf("detached replacement result=%+v err=%v", first, err)
	}
	store.mu.Lock()
	phase := store.book.ReplacementInProgress
	_, intentRetained := store.intents["mobi"]
	store.mu.Unlock()
	if !phase || !intentRetained {
		t.Fatalf("detached replacement phase=%v intent=%v", phase, intentRetained)
	}
	remote.mu.Lock()
	remote.uploadError = nil
	remote.book.Metadata.Tags = []string{"ignore-me"}
	remote.mu.Unlock()

	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" {
		t.Fatalf("detached replacement resume result=%+v err=%v", second, err)
	}
	third, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || third.Status != "ignored" {
		t.Fatalf("completed replacement was rebuilt under ignore result=%+v err=%v", third, err)
	}
	store.mu.Lock()
	finalPhase := store.book.ReplacementInProgress
	_, finalIntent := store.intents["mobi"]
	_, finalDerived := store.derived["mobi"]
	store.mu.Unlock()
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if finalPhase || finalIntent || !finalDerived || deletes != 1 || uploads != 1 {
		t.Fatalf("detached replacement final state phase=%v intent=%v derived=%v deletes=%d uploads=%d", finalPhase, finalIntent, finalDerived, deletes, uploads)
	}
}

func TestSyncResumesReplacementAfterDeleteFailureBeforeRemoval(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}, deleteError: errors.New("delete status unknown")}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	first, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || first.Status != "partial" {
		t.Fatalf("failed delete result=%+v err=%v", first, err)
	}
	store.mu.Lock()
	prepared := store.book.ReplacementInProgress
	intent := store.intents["mobi"]
	store.mu.Unlock()
	if !prepared || intent.ReplacementTargetID != "old-mobi-id" || intent.ReplacementTargetName != "book.mobi" {
		t.Fatalf("failed delete durable state phase=%v intent=%+v", prepared, intent)
	}

	remote.mu.Lock()
	remote.deleteError = nil
	remote.mu.Unlock()
	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" {
		t.Fatalf("failed delete retry result=%+v err=%v", second, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	store.mu.Lock()
	phase := store.book.ReplacementInProgress
	_, committed := store.derived["mobi"]
	store.mu.Unlock()
	if deletes != 1 || uploads != 1 || phase || !committed {
		t.Fatalf("failed delete retry state deletes=%d uploads=%d phase=%v committed=%v", deletes, uploads, phase, committed)
	}
}

func TestSyncRetainsPreparedEvidenceAcrossRepeatedMissingTargetUploadFailures(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}, uploadError: errors.New("restore failed")}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "preserve", DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrPartial) {
		t.Fatal("initial failed restoration did not remain partial")
	}
	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrPartial) {
		t.Fatal("repeated failed restoration did not remain partial")
	}
	store.mu.Lock()
	intent := store.intents["mobi"]
	phase := store.book.ReplacementInProgress
	store.mu.Unlock()
	if !phase || intent.ReplacementTargetID != "old-mobi-id" || intent.ReplacementTargetName != "book.mobi" || intent.ReplacementTag != "replace-me" {
		t.Fatalf("missing-target recovery evidence was lost phase=%v intent=%+v", phase, intent)
	}

	remote.mu.Lock()
	remote.uploadError = nil
	remote.mu.Unlock()
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" {
		t.Fatalf("later restoration retry result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	store.mu.Lock()
	_, committed := store.derived["mobi"]
	_, intentRemaining := store.intents["mobi"]
	store.mu.Unlock()
	if deletes != 1 || uploads != 1 || !committed || intentRemaining {
		t.Fatalf("later restoration retry state deletes=%d uploads=%d committed=%v intent=%v", deletes, uploads, committed, intentRemaining)
	}
}

func TestSyncBlocksReplacementWhenPersistedTargetIdentityChanges(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}, deleteError: errors.New("delete failed before removal")}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})
	if _, err := service.Sync(context.Background(), "1", "book", SyncOptions{}); !errors.Is(err, ErrSafeReplacementUnavailable) {
		t.Fatal("initial failed delete did not leave a durable safe-recovery error")
	}
	remote.mu.Lock()
	remote.deleteError = nil
	remote.book.Files[1].ID = "different-mobi-id"
	remote.mu.Unlock()
	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || second.Status != "partial" {
		t.Fatalf("changed target result=%+v err=%v", second, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 0 || uploads != 0 {
		t.Fatalf("changed target was mutated deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestSyncRecoversAmbiguousUploadAfterMutation(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}, uploadAfterMutationErr: errors.New("upload response unknown")}
	store := &memoryStore{derived: make(map[string]state.DerivedState)}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})

	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" || result.Derivatives[0].Status != "uploaded" {
		t.Fatalf("ambiguous upload result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	store.mu.Lock()
	_, committed := store.derived["mobi"]
	_, intentRemaining := store.intents["mobi"]
	store.mu.Unlock()
	if deletes != 1 || uploads != 1 || !committed || intentRemaining {
		t.Fatalf("ambiguous upload state deletes=%d uploads=%d committed=%v intent=%v", deletes, uploads, committed, intentRemaining)
	}
}

func TestStrictIntentRecoveryRequiresExactOutputName(t *testing.T) {
	main := grimmory.File{ID: "main-id", Format: "epub", Name: "book.epub"}
	book := grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{main, {ID: "mobi-id", Format: "mobi", Name: "book.mobi"}}}
	remote := &fakeRemote{book: book, content: map[string][]byte{"epub": []byte("main"), "mobi": []byte("derivative")}}
	service := New(Options{Client: remote, Store: &memoryStore{}, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, SupportedInputs: []string{"epub"}, MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, TempRoot: t.TempDir()})
	plan := DerivativePlan{Format: "mobi", GenerationFingerprint: "generation"}
	intent := uploadIntentFor("1", "book", plan, main.Name, hashBytes([]byte("main")), main, stableInventoryFingerprint(book.Files, "mobi"), false, "")
	intent.OutputSHA256 = hashBytes([]byte("derivative"))
	intent.OutputName = "Book.mobi"

	_, _, recoverable, err := service.recoverableIntentCandidate(context.Background(), grimmory.BookReference{LibraryID: "1", BookID: "book"}, t.TempDir(), book, main, plan, intent, hashBytes([]byte("main")), main.Name, "epub")
	if err != nil {
		t.Fatal(err)
	}
	if recoverable {
		t.Fatal("strict recovery accepted a non-exact output name")
	}
}

func TestSyncAbortsIfDerivativeReappearsAfterDeletion(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content:             map[string][]byte{"epub": []byte("main")},
		reappearAfterDelete: true,
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("reappeared derivative result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	deletes := len(remote.deletes)
	remote.mu.Unlock()
	if uploads != 0 || deletes != 1 {
		t.Fatalf("reappeared derivative mutations uploads=%d deletes=%d", uploads, deletes)
	}
}

func TestSyncAbortsWhenCanonicalChangesDuringReplacementPreflight(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	remote.downloadHook = func(format string) {
		if format != "epub" {
			return
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		if len(remote.downloads) != 2 {
			return
		}
		for index := range remote.book.Files {
			if remote.book.Files[index].Format == "epub" {
				remote.book.Files[index].ID = "changed-main-id"
			}
		}
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("changed canonical result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	deletes := len(remote.deletes)
	remote.mu.Unlock()
	if uploads != 0 || deletes != 0 {
		t.Fatalf("changed canonical mutated uploads=%d deletes=%d", uploads, deletes)
	}
}

func TestSyncAbortsWhenOverrideAuthorizationDisappearsDuringPreflight(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	remote.downloadHook = func(format string) {
		if format != "epub" {
			return
		}
		remote.mu.Lock()
		defer remote.mu.Unlock()
		if len(remote.downloads) != 2 {
			return
		}
		filtered := remote.book.Metadata.Tags[:0]
		for _, tag := range remote.book.Metadata.Tags {
			if tag != "replace-me" {
				filtered = append(filtered, tag)
			}
		}
		remote.book.Metadata.Tags = filtered
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" || result.Error != SafeReplacementUnavailableCode {
		t.Fatalf("changed authorization result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	deletes := len(remote.deletes)
	remote.mu.Unlock()
	if uploads != 0 || deletes != 0 {
		t.Fatalf("changed authorization mutated uploads=%d deletes=%d", uploads, deletes)
	}
}

func TestSyncPerBookReplacementTagAuthorizesOneReplacementAndIsConsumed(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"keep", "replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		ExistingDerivativePolicy: "preserve", DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1,
		MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" {
		t.Fatalf("tag override result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removedTags...)
	remainingTags := append([]string(nil), remote.book.Metadata.Tags...)
	remote.mu.Unlock()
	if len(removed) != 1 || removed[0] != "1:replace-me" {
		t.Fatalf("replacement tag cleanup=%v", removed)
	}
	if strings.Contains(strings.Join(remainingTags, ","), "replace-me") || !strings.Contains(strings.Join(remainingTags, ","), "keep") {
		t.Fatalf("replacement tag cleanup changed tags=%v", remainingTags)
	}
	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" {
		t.Fatalf("one-shot follow-up result=%+v err=%v", second, err)
	}
	remote.mu.Lock()
	deletes, uploads, removals := len(remote.deletes), len(remote.uploads), len(remote.removedTags)
	remote.mu.Unlock()
	if deletes != 1 || uploads != 1 || removals != 1 {
		t.Fatalf("one-shot follow-up mutated again deletes=%d uploads=%d removals=%d", deletes, uploads, removals)
	}
}

func TestSyncReplacementTagDoesNotForceCurrentDerivative(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	fingerprint := GenerationFingerprint(grimmory.Book{}, hashBytes([]byte("main")), "book.epub", "mobi")
	store := &memoryStore{derived: map[string]state.DerivedState{"mobi": {
		BookID: "book", Format: "mobi", GrimmoryFileID: "mobi-id", SourceSHA256: hashBytes([]byte("main")), OutputSHA256: "output", GenerationFingerprint: fingerprint, GeneratedAt: time.Now(),
	}}}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" || result.Derivatives[0].Action != "unchanged" {
		t.Fatalf("current tag result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads, removals := len(remote.deletes), len(remote.uploads), len(remote.removedTags)
	remote.mu.Unlock()
	if deletes != 0 || uploads != 0 || removals != 0 {
		t.Fatalf("current tag caused mutation deletes=%d uploads=%d removals=%d", deletes, uploads, removals)
	}
}

func TestSyncKeepsReplacementTagWhenBookOnlyNeedsCreation(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" {
		t.Fatalf("tag creation result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removedTags...)
	tags := append([]string(nil), remote.book.Metadata.Tags...)
	remote.mu.Unlock()
	if len(removed) != 0 || len(tags) != 1 || tags[0] != "replace-me" {
		t.Fatalf("creation consumed replacement tag removed=%v tags=%v", removed, tags)
	}
}

func TestSyncRetainsReplacementTagWhenCleanupFails(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")}, tagError: errors.New("tag endpoint unavailable"),
	}
	store := &memoryStore{derived: map[string]state.DerivedState{}}
	service := New(Options{
		Client: remote, Store: store, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrReplacementTagMutation) || result.Status != "partial" || result.Error != "replacement_tag_failed" {
		t.Fatalf("tag cleanup failure result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	remainingTags := append([]string(nil), remote.book.Metadata.Tags...)
	remote.mu.Unlock()
	if len(remainingTags) != 1 || remainingTags[0] != "replace-me" {
		t.Fatalf("replacement tag was not retained: %v", remainingTags)
	}
	remote.mu.Lock()
	remote.tagError = nil
	remote.mu.Unlock()
	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" {
		t.Fatalf("cleanup-only retry result=%+v err=%v", second, err)
	}
	remote.mu.Lock()
	deletes, uploads, removals := len(remote.deletes), len(remote.uploads), len(remote.removedTags)
	remote.mu.Unlock()
	store.mu.Lock()
	lastSuccessful := store.book.LastSuccessfulSync
	store.mu.Unlock()
	if deletes != 1 || uploads != 1 || removals != 1 {
		t.Fatalf("cleanup-only retry repeated derivative work deletes=%d uploads=%d removals=%d", deletes, uploads, removals)
	}
	if lastSuccessful.IsZero() {
		t.Fatal("cleanup-only retry did not update LastSuccessfulSync")
	}
}

func TestSyncRestoresReplacementTagAfterAmbiguousRemovalError(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"keep", "replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content:                map[string][]byte{"epub": []byte("main")},
		removeTagError:         errors.New("tag update status unknown"),
		removeTagAfterMutation: true,
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrReplacementTagMutation) || result.Status != "partial" || result.Error != "replacement_tag_failed" {
		t.Fatalf("ambiguous tag removal result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removedTags...)
	added := append([]string(nil), remote.addedTags...)
	tags := append([]string(nil), remote.book.Metadata.Tags...)
	remote.mu.Unlock()
	if len(removed) != 1 || len(added) != 1 || len(tags) != 2 || tags[0] != "keep" || tags[1] != "replace-me" {
		t.Fatalf("ambiguous tag removal did not restore exact tags removed=%v added=%v tags=%v", removed, added, tags)
	}
}

func TestSyncRestoresReplacementTagWhenCleanupVerificationReadFails(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"keep", "replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content:                 map[string][]byte{"epub": []byte("main")},
		getAfterTagRemovalError: errors.New("verification read unavailable"),
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrReplacementTagMutation) || result.Status != "partial" || result.Error != "replacement_tag_failed" {
		t.Fatalf("cleanup verification read result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removedTags...)
	added := append([]string(nil), remote.addedTags...)
	tags := append([]string(nil), remote.book.Metadata.Tags...)
	remote.mu.Unlock()
	if len(removed) != 1 || len(added) != 1 || len(tags) != 2 || tags[0] != "keep" || tags[1] != "replace-me" {
		t.Fatalf("cleanup verification read did not restore exact tags removed=%v added=%v tags=%v", removed, added, tags)
	}
}

func TestSyncRetainsReplacementTagWhenTakeoverFails(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content:     map[string][]byte{"epub": []byte("main")},
		uploadError: errors.New("upload failed"),
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20,
		ConversionTimeout: time.Minute, TempRoot: t.TempDir(),
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrPartial) || result.Status != "partial" {
		t.Fatalf("takeover failure result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removedTags...)
	tags := append([]string(nil), remote.book.Metadata.Tags...)
	deletes := append([]string(nil), remote.deletes...)
	remote.mu.Unlock()
	if len(removed) != 0 || len(tags) != 1 || tags[0] != "replace-me" || len(deletes) != 1 {
		t.Fatalf("takeover failure cleanup removed=%v tags=%v deletes=%v", removed, tags, deletes)
	}
}

func TestSyncIgnoreTagWinsOverReplacementAuthorization(t *testing.T) {
	remote := &fakeRemote{
		book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"ignore-me", "replace-me"}}, Files: []grimmory.File{
			{ID: "main-id", Format: "epub", Name: "book.epub"},
			{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
		}},
		content: map[string][]byte{"epub": []byte("main")},
	}
	service := New(Options{
		Client: remote, Store: &memoryStore{derived: map[string]state.DerivedState{}}, Converter: &fakeConverter{},
		LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		ExistingDerivativePolicy: "replace", IgnoreProcessingTag: "ignore-me", DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1,
		MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute,
	})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if err != nil || result.Status != "ignored" {
		t.Fatalf("ignored result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 0 || uploads != 0 {
		t.Fatalf("ignored book was mutated deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestSyncTagAuthorizationStateDoesNotBypassIgnore(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Metadata: grimmory.BookMetadata{Tags: []string{"ignore-me"}}, Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{book: state.BookState{ReplacementInProgressTag: "replace-me"}, derived: map[string]state.DerivedState{}}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", IgnoreProcessingTag: "ignore-me", DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "ignored" {
		t.Fatalf("tag-only authorization result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 0 || uploads != 0 {
		t.Fatalf("tag-only authorization mutated ignored book deletes=%d uploads=%d", deletes, uploads)
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
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}, {ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"}}}, content: map[string][]byte{"epub": []byte("new-main")}}
	store := &memoryStore{book: state.BookState{BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalFileID: "old-main", CanonicalFileName: "book.epub", CanonicalSHA256: "old", LastSuccessfulSync: lastSuccess}, derived: map[string]state.DerivedState{"mobi": old}, setDerError: errors.New("database unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: 10 * time.Minute, TempRoot: t.TempDir()})
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
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 1 || uploads != 1 {
		t.Fatalf("state failure did not occur after remote mutation: deletes=%d uploads=%d", deletes, uploads)
	}
	store.mu.Lock()
	_, intentRetained := store.intents["mobi"]
	store.mu.Unlock()
	if !intentRetained {
		t.Fatal("upload intent was cleared after failed state commit")
	}
}

func TestSyncRecoversVerifiedUploadIntentWithoutConvertingOrReplacingAgain(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}}, content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{derived: map[string]state.DerivedState{"mobi": {BookID: "book", Format: "mobi", SourceSHA256: "old", OutputSHA256: "old-output"}}, setDerError: errors.New("commit unavailable")}
	service := New(Options{Client: remote, Store: store, Converter: &fakeConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: t.TempDir()})
	first, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrPartial) || first.Status != "partial" {
		t.Fatalf("initial failed commit result=%+v err=%v", first, err)
	}
	store.mu.Lock()
	_, intentRetained := store.intents["mobi"]
	store.setDerError = nil
	store.mu.Unlock()
	if !intentRetained {
		t.Fatal("initial failed commit did not retain intent")
	}
	converter := service.converter.(*fakeConverter)
	converter.mu.Lock()
	firstConversions := len(converter.calls)
	converter.mu.Unlock()
	second, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" || second.Derivatives[0].Status != "adopted" {
		t.Fatalf("intent recovery result=%+v err=%v", second, err)
	}
	converter.mu.Lock()
	secondConversions := len(converter.calls)
	converter.mu.Unlock()
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	store.mu.Lock()
	_, intentRemaining := store.intents["mobi"]
	_, derived := store.derived["mobi"]
	store.mu.Unlock()
	if secondConversions != firstConversions || deletes != 1 || uploads != 1 || intentRemaining || !derived {
		t.Fatalf("recovery repeated work conversions=%d/%d deletes=%d uploads=%d intent=%v derived=%v", firstConversions, secondConversions, deletes, uploads, intentRemaining, derived)
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

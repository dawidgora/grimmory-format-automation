package reconcile

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"converter/internal/grimmory"
	"converter/internal/state"
)

func hashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("%x", digest[:])
}

type fakeRemote struct {
	mu                     sync.Mutex
	book                   grimmory.Book
	content                map[string][]byte
	uploads                []string
	deletes                []string
	uploadAfterMutationErr error
	afterUploadHook        func()
	beforeDeleteHook       func()
	afterDeleteHook        func()
}

func (r *fakeRemote) GetLibrary(context.Context, string) (grimmory.Library, error) {
	return grimmory.Library{ID: "1", FormatPriority: []string{"epub", "mobi", "azw3"}}, nil
}

func (r *fakeRemote) GetLibraryBook(ctx context.Context, _, _ string) (grimmory.Book, error) {
	if err := ctx.Err(); err != nil {
		return grimmory.Book{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	book := r.book
	book.LibraryID = "1"
	book.Files = append([]grimmory.File(nil), r.book.Files...)
	book.Metadata.Tags = append([]string(nil), r.book.Metadata.Tags...)
	return book, nil
}

func (r *fakeRemote) DownloadContentScoped(ctx context.Context, _ grimmory.BookReference, format string, dst io.Writer) (int64, string, error) {
	if err := ctx.Err(); err != nil {
		return 0, "", err
	}
	r.mu.Lock()
	data := append([]byte(nil), r.content[normalizeFormat(format)]...)
	r.mu.Unlock()
	if _, err := dst.Write(data); err != nil {
		return 0, "", err
	}
	return int64(len(data)), hashBytes(data), nil
}

func (r *fakeRemote) UploadFileScoped(ctx context.Context, ref grimmory.BookReference, format, path string) error {
	return r.UploadFileNamedScoped(ctx, ref, format, path, desiredOutputName("book.epub", format))
}

func (r *fakeRemote) UploadFileNamedScoped(ctx context.Context, _ grimmory.BookReference, format, path, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	r.mu.Lock()
	format = normalizeFormat(format)
	r.content[format] = append([]byte(nil), data...)
	r.uploads = append(r.uploads, format)
	r.book.Files = append(r.book.Files, grimmory.File{ID: fmt.Sprintf("%s-%d", format, len(r.uploads)), Format: format, Name: name, SHA256: hashBytes(data), TrustedMTime: true, MTime: time.Now().UTC()})
	err = r.uploadAfterMutationErr
	hook := r.afterUploadHook
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return err
}

func (r *fakeRemote) DeleteFileScoped(ctx context.Context, _ grimmory.BookReference, fileID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.beforeDeleteHook != nil {
		r.beforeDeleteHook()
	}
	r.mu.Lock()
	for index, file := range r.book.Files {
		if file.ID == fileID {
			r.book.Files = append(r.book.Files[:index], r.book.Files[index+1:]...)
			r.deletes = append(r.deletes, fileID)
			r.mu.Unlock()
			if r.afterDeleteHook != nil {
				r.afterDeleteHook()
			}
			return nil
		}
	}
	r.mu.Unlock()
	return grimmory.ErrNotFound
}

type memoryStore struct {
	mu          sync.Mutex
	book        state.BookState
	derived     map[string]state.DerivedState
	receipts    map[string]state.DerivedUploadReceipt
	setDerError error
}

func (s *memoryStore) Get(context.Context, string, string) (state.BookState, map[string]state.DerivedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	derived := make(map[string]state.DerivedState, len(s.derived))
	for key, value := range s.derived {
		derived[key] = value
	}
	return s.book, derived, nil
}

func (s *memoryStore) SetBook(_ context.Context, value state.BookState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *memoryStore) GetDerivedUploadReceipts(_ context.Context, _, _ string) (map[string]state.DerivedUploadReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	receipts := make(map[string]state.DerivedUploadReceipt, len(s.receipts))
	for key, value := range s.receipts {
		receipts[key] = value
	}
	return receipts, nil
}

func (s *memoryStore) SetDerivedUploadReceipt(_ context.Context, value state.DerivedUploadReceipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipts == nil {
		s.receipts = make(map[string]state.DerivedUploadReceipt)
	}
	s.receipts[value.Format] = value
	return nil
}

func (s *memoryStore) CommitDerived(_ context.Context, value state.DerivedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setDerError != nil {
		return s.setDerError
	}
	if s.derived == nil {
		s.derived = make(map[string]state.DerivedState)
	}
	s.derived[value.Format] = value
	delete(s.receipts, value.Format)
	return nil
}

func newService(remote *fakeRemote, store *memoryStore, options SyncOptions) *Service {
	return New(Options{
		Client: remote, Store: store, Converter: &testConverter{}, LibraryIDs: []string{"1"},
		OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"},
		ExistingDerivativePolicy: "preserve", MaxConcurrentBooks: 1,
		MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: os.TempDir(),
		Logger: nil, DerivativeReplacementTag: "replace-me",
	})
}

type testConverter struct{}

func (*testConverter) Convert(_ context.Context, input, _, target, dir string) (string, error) {
	data, err := os.ReadFile(input)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp(dir, "converted-*")
	if err != nil {
		return "", err
	}
	if _, err := file.Write(append(data, []byte("-"+target)...)); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return file.Name(), nil
}

func replacementBook() grimmory.Book {
	return grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{
		{ID: "main-id", Format: "epub", Name: "book.epub"},
		{ID: "old-mobi-id", Format: "mobi", Name: "book.mobi"},
	}, Metadata: grimmory.BookMetadata{Title: "Book"}}
}

func configuredService(remote *fakeRemote, store Store, policy string, outputs ...string) *Service {
	if len(outputs) == 0 {
		outputs = []string{"mobi"}
	}
	service := New(Options{Client: remote, Store: store, Converter: &testConverter{}, LibraryIDs: []string{"1"}, OutputFormats: outputs, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: policy, DerivativeReplacementTag: "replace-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: os.TempDir()})
	return service
}

func replacementService(remote *fakeRemote, store Store, policy string) *Service {
	return configuredService(remote, store, policy, "mobi")
}

func TestClassifyErrorUsesBoundedCategories(t *testing.T) {
	if got := ClassifyError(fmt.Errorf("secret: %w", ErrState)); got != "state" {
		t.Fatalf("ClassifyError() = %q", got)
	}
}

func TestResolveLibraryPolicyUsesPriority(t *testing.T) {
	policy, err := ResolveLibraryPolicy(grimmory.Library{ID: "1", FormatPriority: []string{"epub", "mobi"}, AllowedFormats: []string{"epub", "mobi"}}, []string{"mobi", "azw3"}, []string{"epub", "mobi", "azw3"})
	if err != nil || policy.MainFormat != "epub" || len(policy.OutputFormats) != 1 || policy.OutputFormats[0] != "mobi" {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}
}

func TestWithBookLockSerializesSameBook(t *testing.T) {
	service := replacementService(&fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}, &memoryStore{}, "preserve")
	started := make(chan struct{})
	release := make(chan struct{})
	secondStarted := make(chan struct{})
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- service.WithBookLock(context.Background(), "1", "book", func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	go func() {
		secondDone <- service.WithBookLock(context.Background(), "1", "book", func(context.Context) error {
			close(secondStarted)
			return nil
		})
	}()
	select {
	case <-secondStarted:
		t.Fatal("same-book lock admitted concurrent work")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}

func TestPlanDerivativesBlocksDuplicateTargetEvenWithMatchingState(t *testing.T) {
	canonicalSHA := hashBytes([]byte("main"))
	fingerprint := GenerationFingerprint(grimmory.Book{}, canonicalSHA, "book.epub", "mobi")
	plans := PlanDerivatives([]grimmory.File{
		{ID: "main", Format: "epub", Name: "book.epub"},
		{ID: "mobi-a", Format: "mobi", Name: "book.mobi"},
		{ID: "mobi-b", Format: "mobi", Name: "book.mobi"},
	}, []string{"mobi"}, "epub", canonicalSHA, map[string]state.DerivedState{"mobi": {Format: "mobi", GrimmoryFileID: "mobi-a", SourceSHA256: canonicalSHA, OutputSHA256: "output", GenerationFingerprint: fingerprint, GeneratedAt: time.Now()}}, time.Time{}, false, false, true, false, map[string]string{"mobi": fingerprint}, "book.epub")
	if len(plans) != 1 || !plans[0].Unsafe || plans[0].Reason != "ambiguous_output" {
		t.Fatalf("duplicate target plan = %+v", plans)
	}
}

func TestSelectSourceUsesConfiguredOrderAndUniqueCandidates(t *testing.T) {
	files := []grimmory.File{
		{ID: "azw3", Format: "azw3", Name: "book.azw3"},
		{ID: "mobi", Format: "mobi", Name: "book.mobi"},
	}
	selected, ok := SelectSource(files, "epub", []string{"mobi", "azw3"})
	if !ok || selected.ID != "mobi" {
		t.Fatalf("selected source=%+v ok=%v", selected, ok)
	}
	files = append(files, grimmory.File{ID: "duplicate-mobi", Format: "mobi", Name: "other.mobi"})
	selected, ok = SelectSource(files, "epub", []string{"mobi", "azw3"})
	if !ok || selected.ID != "azw3" {
		t.Fatalf("duplicate source was selected: %+v ok=%v", selected, ok)
	}
}

func TestPreserveBlocksStaleDerivativeAndForceAuthorizesReplacement(t *testing.T) {
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main"), "mobi": []byte("old")}}
	store := &memoryStore{derived: map[string]state.DerivedState{"mobi": {LibraryID: "1", BookID: "book", Format: "mobi", GrimmoryFileID: "old-mobi-id", SourceSHA256: hashBytes([]byte("main")), OutputSHA256: "old-output", GenerationFingerprint: "old-generation", GeneratedAt: time.Now()}}}
	blocked, err := replacementService(remote, store, "preserve").Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || blocked.Status != "partial" {
		t.Fatalf("preserve result=%+v err=%v", blocked, err)
	}
	remote.mu.Lock()
	if len(remote.deletes) != 0 || len(remote.uploads) != 0 {
		t.Fatalf("preserve mutated stale derivative deletes=%v uploads=%v", remote.deletes, remote.uploads)
	}
	remote.mu.Unlock()
	forced, err := replacementService(remote, store, "preserve").Sync(context.Background(), "1", "book", SyncOptions{Force: true})
	if err != nil || forced.Status != "completed" {
		t.Fatalf("force result=%+v err=%v", forced, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 1 || uploads != 1 {
		t.Fatalf("force mutations deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestSyncCreatesMissingMainAndDerivativesWithFreshChecks(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "source-id", Format: "mobi", Name: "book.mobi"}}}, content: map[string][]byte{"mobi": []byte("source")}}
	service := New(Options{Client: remote, Store: &memoryStore{}, Converter: &testConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"azw3"}, SupportedInputs: []string{"mobi", "epub"}, ExistingDerivativePolicy: "preserve", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: os.TempDir()})
	result, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" || result.Main.Status != "created" {
		t.Fatalf("missing main result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := append([]string(nil), remote.uploads...)
	remote.mu.Unlock()
	if len(uploads) != 2 || uploads[0] != "epub" || uploads[1] != "azw3" {
		t.Fatalf("missing main uploads=%v", uploads)
	}
}

func TestReceiptIsWrittenBeforeDelete(t *testing.T) {
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{}
	remote.beforeDeleteHook = func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		if store.receipts["mobi"].OutputSHA256 == "" || store.receipts["mobi"].OutputSHA256 == "pending" {
			t.Error("receipt was not durable before DELETE")
		}
	}
	result, err := replacementService(remote, store, "replace").Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestInterruptedAfterDeleteIsReconciledFresh(t *testing.T) {
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{}
	ctx, cancel := context.WithCancel(context.Background())
	remote.afterDeleteHook = cancel
	first, err := replacementService(remote, store, "replace").Sync(ctx, "1", "book", SyncOptions{})
	if err == nil || first.Status != "partial" {
		t.Fatalf("interrupted result=%+v err=%v", first, err)
	}
	store.mu.Lock()
	_, receipt := store.receipts["mobi"]
	store.mu.Unlock()
	if !receipt {
		t.Fatal("interrupted replacement lost receipt")
	}
	remote.afterDeleteHook = nil
	second, err := replacementService(remote, store, "replace").Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" {
		t.Fatalf("fresh retry result=%+v err=%v", second, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 1 || uploads != 1 {
		t.Fatalf("fresh retry mutations deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestAmbiguousPostIsAdoptedFromReceipt(t *testing.T) {
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}, uploadAfterMutationErr: errors.New("response lost")}
	store := &memoryStore{}
	result, err := replacementService(remote, store, "replace").Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" || result.Derivatives[0].Status != "adopted" {
		t.Fatalf("ambiguous upload result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	remote.mu.Unlock()
	if uploads != 1 {
		t.Fatalf("ambiguous upload was repeated: %d", uploads)
	}
}

func TestFailedStateCommitIsAdoptedFromReceipt(t *testing.T) {
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{setDerError: errors.New("state unavailable")}
	first, err := replacementService(remote, store, "replace").Sync(context.Background(), "1", "book", SyncOptions{})
	if err == nil || first.Status != "partial" {
		t.Fatalf("failed commit result=%+v err=%v", first, err)
	}
	store.mu.Lock()
	_, receipt := store.receipts["mobi"]
	store.setDerError = nil
	store.mu.Unlock()
	if !receipt {
		t.Fatal("failed commit removed receipt")
	}
	second, err := replacementService(remote, store, "replace").Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || second.Status != "completed" || second.Derivatives[0].Status != "adopted" {
		t.Fatalf("adoption result=%+v err=%v", second, err)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	remote.mu.Unlock()
	if uploads != 1 {
		t.Fatalf("failed commit repeated upload: %d", uploads)
	}
}

func TestDuplicateCandidateIsRefused(t *testing.T) {
	book := replacementBook()
	book.Files = append(book.Files, grimmory.File{ID: "duplicate-mobi-id", Format: "mobi", Name: "book.mobi"})
	remote := &fakeRemote{book: book, content: map[string][]byte{"epub": []byte("main"), "mobi": []byte("output")}}
	store := &memoryStore{derived: map[string]state.DerivedState{"mobi": {LibraryID: "1", BookID: "book", Format: "mobi", GrimmoryFileID: "old-mobi-id", OutputSHA256: hashBytes([]byte("output")), SourceSHA256: hashBytes([]byte("main")), GenerationFingerprint: GenerationFingerprint(grimmory.Book{}, hashBytes([]byte("main")), "book.epub", "mobi"), GeneratedAt: time.Now()}}}
	result, err := replacementService(remote, store, "preserve").Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" {
		t.Fatalf("duplicate candidate result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 0 || uploads != 0 {
		t.Fatalf("duplicate candidate was mutated deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestTargetAppearingAfterInitialPlanningIsReclassified(t *testing.T) {
	remote := &fakeRemote{book: grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "main-id", Format: "epub", Name: "book.epub"}}}, content: map[string][]byte{"epub": []byte("main"), "azw3": []byte("unexpected")}}
	appeared := false
	remote.afterUploadHook = func() {
		remote.mu.Lock()
		defer remote.mu.Unlock()
		if appeared {
			return
		}
		appeared = true
		remote.book.Files = append(remote.book.Files, grimmory.File{ID: "appeared-azw3", Format: "azw3", Name: "book.azw3"})
	}
	result, err := configuredService(remote, &memoryStore{}, "preserve", "mobi", "azw3").Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" {
		t.Fatalf("target appearance result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	uploads := append([]string(nil), remote.uploads...)
	remote.mu.Unlock()
	if len(uploads) != 1 || uploads[0] != "mobi" {
		t.Fatalf("stale create plan uploaded unexpected targets: %v", uploads)
	}
}

func TestReceiptRecoverySurvivesSQLiteRestartAfterAcceptedPost(t *testing.T) {
	dir := t.TempDir()
	store, err := state.Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}
	ctx, cancel := context.WithCancel(context.Background())
	remote.afterUploadHook = cancel
	first, firstErr := replacementService(remote, store, "replace").Sync(ctx, "1", "book", SyncOptions{})
	if firstErr == nil || first.Status != "partial" {
		t.Fatalf("accepted post result=%+v err=%v", first, firstErr)
	}
	receipts, err := store.GetDerivedUploadReceipts(context.Background(), "1", "book")
	if err != nil || len(receipts) != 1 {
		t.Fatalf("accepted post receipt=%+v err=%v", receipts, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := state.Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	remote.afterUploadHook = nil
	second, secondErr := replacementService(remote, reopened, "preserve").Sync(context.Background(), "1", "book", SyncOptions{})
	if secondErr != nil || second.Status != "completed" || second.Derivatives[0].Status != "adopted" {
		t.Fatalf("restarted adoption result=%+v err=%v", second, secondErr)
	}
	remote.mu.Lock()
	uploads := len(remote.uploads)
	remote.mu.Unlock()
	if uploads != 1 {
		t.Fatalf("restarted adoption repeated POST: %d", uploads)
	}
}

func TestFreshRetryAfterSQLiteRestartAndDeleteInterruptionUploadsAbsentTarget(t *testing.T) {
	dir := t.TempDir()
	store, err := state.Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}
	ctx, cancel := context.WithCancel(context.Background())
	remote.afterDeleteHook = cancel
	first, firstErr := replacementService(remote, store, "replace").Sync(ctx, "1", "book", SyncOptions{})
	if firstErr == nil || first.Status != "partial" {
		t.Fatalf("delete interruption result=%+v err=%v", first, firstErr)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := state.Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	remote.afterDeleteHook = nil
	second, secondErr := replacementService(remote, reopened, "preserve").Sync(context.Background(), "1", "book", SyncOptions{})
	if secondErr != nil || second.Status != "completed" {
		t.Fatalf("fresh absent-target retry result=%+v err=%v", second, secondErr)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 1 || uploads != 1 {
		t.Fatalf("fresh retry mutations deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestReceiptIsBlockedWhenCanonicalChangesBetweenRuns(t *testing.T) {
	dir := t.TempDir()
	store, err := state.Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}
	ctx, cancel := context.WithCancel(context.Background())
	remote.afterUploadHook = cancel
	if _, err := replacementService(remote, store, "replace").Sync(ctx, "1", "book", SyncOptions{}); err == nil {
		t.Fatal("accepted POST unexpectedly completed")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	remote.content["epub"] = []byte("changed")
	remote.mu.Unlock()
	reopened, err := state.Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	remote.afterUploadHook = nil
	result, err := replacementService(remote, reopened, "preserve").Sync(context.Background(), "1", "book", SyncOptions{})
	if !errors.Is(err, ErrSafeReplacementUnavailable) || result.Status != "partial" {
		t.Fatalf("changed canonical result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	deletes, uploads := len(remote.deletes), len(remote.uploads)
	remote.mu.Unlock()
	if deletes != 1 || uploads != 1 {
		t.Fatalf("changed canonical was mutated deletes=%d uploads=%d", deletes, uploads)
	}
}

func TestReplacementTagIsPersistentAuthorization(t *testing.T) {
	remote := &fakeRemote{book: func() grimmory.Book {
		book := replacementBook()
		book.Metadata.Tags = []string{"replace-me"}
		return book
	}(), content: map[string][]byte{"epub": []byte("main")}}
	result, err := replacementService(remote, &memoryStore{}, "preserve").Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || result.Status != "completed" {
		t.Fatalf("tag replacement result=%+v err=%v", result, err)
	}
	remote.mu.Lock()
	tags := append([]string(nil), remote.book.Metadata.Tags...)
	remote.mu.Unlock()
	if len(tags) != 1 || tags[0] != "replace-me" {
		t.Fatalf("replacement tag was consumed: %v", tags)
	}
}

func TestIgnoreDelaysRepairAfterInterruptedReplacement(t *testing.T) {
	remote := &fakeRemote{book: replacementBook(), content: map[string][]byte{"epub": []byte("main")}}
	store := &memoryStore{}
	service := New(Options{Client: remote, Store: store, Converter: &testConverter{}, LibraryIDs: []string{"1"}, OutputFormats: []string{"mobi"}, SupportedInputs: []string{"epub"}, ExistingDerivativePolicy: "replace", IgnoreProcessingTag: "ignore-me", MaxConcurrentBooks: 1, MaxFileBytes: 1 << 20, ConversionTimeout: time.Minute, TempRoot: os.TempDir()})
	ctx, cancel := context.WithCancel(context.Background())
	remote.afterDeleteHook = func() {
		remote.mu.Lock()
		remote.book.Metadata.Tags = []string{"ignore-me"}
		remote.mu.Unlock()
		cancel()
	}
	if _, err := service.Sync(ctx, "1", "book", SyncOptions{}); err == nil {
		t.Fatal("interrupted replacement unexpectedly completed")
	}
	remote.afterDeleteHook = nil
	ignored, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || ignored.Status != "ignored" {
		t.Fatalf("ignore did not delay repair result=%+v err=%v", ignored, err)
	}
	remote.mu.Lock()
	remote.book.Metadata.Tags = nil
	remote.mu.Unlock()
	completed, err := service.Sync(context.Background(), "1", "book", SyncOptions{})
	if err != nil || completed.Status != "completed" {
		t.Fatalf("repair after ignore result=%+v err=%v", completed, err)
	}
}

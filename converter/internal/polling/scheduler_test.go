package polling

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"converter/internal/grimmory"
	"converter/internal/logging"
	"converter/internal/reconcile"
	"converter/internal/state"
)

type fakeRemote struct {
	mu         sync.Mutex
	books      []grimmory.Book
	detail     []grimmory.Book
	gets       int
	listErr    error
	listErrors map[string]error
	tagError   error
	added      []string
	removed    []string
}

type blockingPollRemote struct {
	mu       sync.Mutex
	started  chan struct{}
	release  chan struct{}
	listCall int
}

func (r *blockingPollRemote) ListLibraryBooks(_ context.Context, _ string) ([]grimmory.Book, error) {
	r.mu.Lock()
	r.listCall++
	first := r.listCall == 1
	if first {
		close(r.started)
	}
	r.mu.Unlock()
	if first {
		<-r.release
	}
	return nil, nil
}

func (*blockingPollRemote) GetLibraryBook(context.Context, string, string) (grimmory.Book, error) {
	return grimmory.Book{}, nil
}

func (r *fakeRemote) ListLibraryBooks(_ context.Context, libraryID string) ([]grimmory.Book, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.listErrors[libraryID]; err != nil {
		return nil, err
	}
	if r.listErr != nil {
		return nil, r.listErr
	}
	return append([]grimmory.Book(nil), r.books...), nil
}

func (r *fakeRemote) GetLibraryBook(context.Context, string, string) (grimmory.Book, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gets++
	if len(r.detail) == 0 {
		return grimmory.Book{}, errors.New("missing fake detail")
	}
	book := r.detail[0]
	if len(r.detail) > 1 {
		r.detail = r.detail[1:]
	}
	return book, nil
}

func (r *fakeRemote) AddBookTagScoped(_ context.Context, reference grimmory.BookReference, tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tagError != nil {
		return r.tagError
	}
	r.added = append(r.added, reference.LibraryID+"/"+reference.BookID+":"+tag)
	return nil
}
func (r *fakeRemote) RemoveBookTagScoped(_ context.Context, reference grimmory.BookReference, tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tagError != nil {
		return r.tagError
	}
	r.removed = append(r.removed, reference.LibraryID+"/"+reference.BookID+":"+tag)
	return nil
}

type fakeReconciler struct {
	mu        sync.Mutex
	calls     []string
	err       error
	started   chan struct{}
	finished  chan struct{}
	remote    *fakeRemote
	errByBook map[string]error
}

func (r *fakeReconciler) Sync(ctx context.Context, libraryID, bookID string, _ reconcile.SyncOptions) (reconcile.Result, error) {
	r.mu.Lock()
	r.calls = append(r.calls, libraryID+"/"+bookID)
	if r.started != nil {
		close(r.started)
		r.started = nil
	}
	r.mu.Unlock()
	if r.finished != nil {
		select {
		case <-r.finished:
		case <-ctx.Done():
			return reconcile.Result{}, ctx.Err()
		}
	}
	if err := r.errByBook[bookID]; err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, r.err
}

func (r *fakeReconciler) SetFailureTag(ctx context.Context, libraryID, bookID string, present bool) error {
	if r.remote == nil {
		return reconcile.ErrFailureTagMutation
	}
	reference := grimmory.BookReference{LibraryID: libraryID, BookID: bookID}
	if present {
		return r.remote.AddBookTagScoped(ctx, reference, "failed")
	}
	return r.remote.RemoveBookTagScoped(ctx, reference, "failed")
}

type memoryStore struct {
	mu       sync.Mutex
	states   map[string]state.PollState
	failures int
}

func (s *memoryStore) UpsertPollObservation(_ context.Context, libraryID, bookID, fingerprint string, seenAt time.Time) (state.PollState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = make(map[string]state.PollState)
	}
	value, exists := s.states[bookID]
	if !exists {
		value = state.PollState{LibraryID: libraryID, BookID: bookID, Status: state.PollStatusPending}
	} else if value.ObservationFingerprint != fingerprint {
		value.Status = state.PollStatusPending
		value.AttemptCount = 0
		value.NextAttemptAt = time.Time{}
		value.ErrorCode = ""
		if value.AppliedFingerprint == fingerprint {
			value.Status = state.PollStatusCurrent
		}
	}
	value.BookID, value.ObservationFingerprint, value.LastSeenAt, value.UpdatedAt = bookID, fingerprint, seenAt, seenAt
	s.states[bookID] = value
	return value, nil
}

func (s *memoryStore) ListDuePollStates(_ context.Context, libraryID string, now time.Time, _ int) ([]state.PollState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]state.PollState, 0)
	for _, value := range s.states {
		if value.LibraryID == libraryID && (value.Status == state.PollStatusPending || value.Status == state.PollStatusRetry) && (value.NextAttemptAt.IsZero() || !value.NextAttemptAt.After(now)) {
			result = append(result, value)
		}
	}
	return result, nil
}

func (s *memoryStore) MarkPollSuccess(_ context.Context, _, bookID, fingerprint string, updatedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, exists := s.states[bookID]
	if !exists || value.ObservationFingerprint != fingerprint {
		return errors.New("observation changed")
	}
	value.AppliedFingerprint, value.Status, value.AttemptCount = fingerprint, state.PollStatusCurrent, 0
	value.NextAttemptAt, value.ErrorCode, value.UpdatedAt = time.Time{}, "", updatedAt
	s.states[bookID] = value
	return nil
}

func (s *memoryStore) RecordPollFailure(_ context.Context, _, bookID, fingerprint, code string, next time.Time, maxAttempts int, updatedAt time.Time) (state.PollState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures++
	value := s.states[bookID]
	if value.ObservationFingerprint != fingerprint || (value.Status != state.PollStatusPending && value.Status != state.PollStatusRetry) {
		return state.PollState{}, errors.New("failure target changed")
	}
	value.AttemptCount++
	value.ErrorCode, value.UpdatedAt = code, updatedAt
	if value.AttemptCount >= maxAttempts {
		value.Status, value.NextAttemptAt = state.PollStatusFailed, time.Time{}
	} else {
		value.Status, value.NextAttemptAt = state.PollStatusRetry, next
	}
	s.states[bookID] = value
	return value, nil
}

func (s *memoryStore) RequeuePollObservation(_ context.Context, _, bookID, fingerprint, code string, next, updatedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, exists := s.states[bookID]
	if !exists || value.ObservationFingerprint != fingerprint {
		return errors.New("observation changed")
	}
	value.Status, value.NextAttemptAt, value.ErrorCode, value.UpdatedAt = state.PollStatusRetry, next, code, updatedAt
	s.states[bookID] = value
	return nil
}

func testBook() grimmory.Book {
	return grimmory.Book{ID: "book", LibraryID: "1", Files: []grimmory.File{{ID: "epub-id", Name: "book.epub", Format: "epub"}}, Metadata: grimmory.BookMetadata{Title: "Book"}}
}

func newTestScheduler(t *testing.T, remote *fakeRemote, store *memoryStore, reconciler *fakeReconciler, now *time.Time) *Scheduler {
	t.Helper()
	reconciler.remote = remote
	scheduler, err := New(Options{
		Remote: remote, Store: store, Reconciler: reconciler,
		LibraryIDs: []string{"1"}, MaxConcurrentBooks: 1,
		Interval: time.Hour, MaxAttempts: 2, RetryBase: time.Second, RetryMax: time.Minute,
		Now: func() time.Time { return *now }, Random: func() float64 { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	return scheduler
}

func TestSchedulerLogsScanSummaryAndSecretSafeBookFailures(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	unchanged := testBook()
	unchanged.ID = "unchanged"
	ignored := testBook()
	ignored.ID, ignored.Metadata.Tags = "ignored", []string{"ignore"}
	good := testBook()
	good.ID = "good"
	retried := testBook()
	retried.ID = "retried"
	exhausted := testBook()
	exhausted.ID = "exhausted"
	remote := &fakeRemote{
		books:  []grimmory.Book{unchanged, ignored, good, retried, exhausted},
		detail: []grimmory.Book{unchanged, ignored, good, retried, exhausted, good},
	}
	store := &memoryStore{}
	currentFingerprint := grimmory.ObservationFingerprint(unchanged)
	if _, err := store.UpsertPollObservation(context.Background(), "1", unchanged.ID, currentFingerprint, now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPollSuccess(context.Background(), "1", unchanged.ID, currentFingerprint, now); err != nil {
		t.Fatal(err)
	}
	reconciler := &fakeReconciler{errByBook: map[string]error{
		"retried":   &grimmory.HTTPError{Operation: "sync", Status: 503},
		"exhausted": errors.New("password=secret token=secret /private/secret metadata=secret"),
	}}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	scheduler.ignoreTag = "ignore"
	var output bytes.Buffer
	scheduler.logger = logging.New(logging.Info, &output)

	if err := scheduler.Scan(context.Background()); err == nil {
		t.Fatal("expected aggregated book failure")
	}
	logs := output.String()
	for _, field := range []string{
		"poll scan started",
		"poll scan completed",
		"libraries_attempted=1",
		"libraries_failed=0",
		"books_seen=5",
		"books_ignored=1",
		"books_queued=3",
		"books_succeeded=1",
		"books_retried=1",
		"books_retry_exhausted=1",
		"library_id=1 book_id=retried error_code=remote_http_status",
		"remote_status=503",
		"library_id=1 book_id=exhausted error_code=internal",
	} {
		if !strings.Contains(logs, field) {
			t.Errorf("logs missing %q: %s", field, logs)
		}
	}
	if strings.Contains(logs, "book_id=unchanged") {
		t.Fatalf("unchanged book was logged: %s", logs)
	}
	for _, secret := range []string{"password=secret", "token=secret", "/private/secret", "metadata=secret"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("secret leaked in logs for %q: %s", secret, logs)
		}
	}
}

func TestSchedulerRunLogsStartConfigurationAndSkippedTicks(t *testing.T) {
	remote := &blockingPollRemote{started: make(chan struct{}), release: make(chan struct{})}
	store := &memoryStore{}
	reconciler := &fakeReconciler{}
	var output bytes.Buffer
	scheduler, err := New(Options{
		Remote: remote, Store: store, Reconciler: reconciler, LibraryIDs: []string{"1"},
		Interval: time.Millisecond, MaxAttempts: 2, RetryBase: time.Second, RetryMax: time.Minute,
		MaxConcurrentBooks: 1, Logger: logging.New(logging.Info, &output),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	<-remote.started
	time.Sleep(20 * time.Millisecond)
	close(remote.release)
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	logs := output.String()
	for _, field := range []string{
		"poller started",
		"interval=1ms",
		"worker_limit=1",
		"poll scan started",
		"poll scan completed",
		"poll scan skipped overlapping tick",
	} {
		if !strings.Contains(logs, field) {
			t.Errorf("logs missing %q: %s", field, logs)
		}
	}
}

func TestSchedulerSkipsCurrentObservation(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	fingerprint := grimmory.ObservationFingerprint(book)
	if _, err := store.UpsertPollObservation(context.Background(), "1", book.ID, fingerprint, now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPollSuccess(context.Background(), "1", book.ID, fingerprint, now); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(reconciler.calls) != 0 {
		t.Fatalf("current observation was reconciled: %v", reconciler.calls)
	}
}

func TestSchedulerIgnoredBookClearsFailureTagWithoutProcessing(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	book.Metadata.Tags = []string{"ignore", "failed"}
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	scheduler.ignoreTag, scheduler.failedTag = "ignore", "failed"
	fingerprint := grimmory.ObservationFingerprintIgnoringTags(book, scheduler.ignoreTag, scheduler.failedTag)
	if _, err := store.UpsertPollObservation(context.Background(), "1", book.ID, fingerprint, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailure(context.Background(), "1", book.ID, fingerprint, "retry", now, 5, now); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removed...)
	remote.mu.Unlock()
	store.mu.Lock()
	got := store.states[book.ID]
	store.mu.Unlock()
	if len(reconciler.calls) != 0 || len(removed) != 1 || removed[0] != "1/book:failed" {
		t.Fatalf("ignored book calls=%v removed=%v", reconciler.calls, removed)
	}
	if got.AttemptCount != 1 || got.Status != state.PollStatusRetry {
		t.Fatalf("ignored book consumed retry attempt: %+v", got)
	}
}

func TestSchedulerIgnoredBookWithIdenticalTagsIsNoOp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	book.Metadata.Tags = []string{"managed"}
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	scheduler.ignoreTag, scheduler.failedTag = "managed", "managed"
	if err := scheduler.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removed...)
	remote.mu.Unlock()
	if len(reconciler.calls) != 0 || len(removed) != 0 {
		t.Fatalf("identical tag configuration calls=%v removed=%v", reconciler.calls, removed)
	}
}

func TestSchedulerIgnoredBookWithEmptyFailureTagIsNoOp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	book.Metadata.Tags = []string{"ignore"}
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	scheduler.ignoreTag = "ignore"
	if err := scheduler.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	remote.mu.Lock()
	removed := append([]string(nil), remote.removed...)
	remote.mu.Unlock()
	if len(reconciler.calls) != 0 || len(removed) != 0 {
		t.Fatalf("empty failure tag configuration calls=%v removed=%v", reconciler.calls, removed)
	}
}

func TestSchedulerContinuesHealthyLibraryAfterListFailure(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	book.ID, book.LibraryID = "healthy", "2"
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}, listErrors: map[string]error{"1": errors.New("library unavailable")}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	scheduler.libraryIDs = []string{"1", "2"}
	if err := scheduler.Scan(context.Background()); err == nil {
		t.Fatal("expected aggregated library list failure")
	}
	if len(reconciler.calls) != 1 || reconciler.calls[0] != "2/healthy" {
		t.Fatalf("healthy library was not processed: %v", reconciler.calls)
	}
}

func TestSchedulerContinuesHealthyBooksAfterOneSyncFailure(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	bad, good := testBook(), testBook()
	bad.ID, good.ID = "bad", "good"
	remote := &fakeRemote{books: []grimmory.Book{bad, good}, detail: []grimmory.Book{bad, good, good}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{errByBook: map[string]error{"bad": errors.New("library policy unavailable")}}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	if err := scheduler.Scan(context.Background()); err == nil {
		t.Fatal("expected aggregated book sync failure")
	}
	if len(reconciler.calls) != 2 {
		t.Fatalf("healthy book was not processed: %v", reconciler.calls)
	}
	store.mu.Lock()
	goodState := store.states[good.ID]
	store.mu.Unlock()
	if goodState.Status != state.PollStatusCurrent {
		t.Fatalf("healthy book state = %+v", goodState)
	}
}

func TestSchedulerRetriesTransientThenExhaustsRetries(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{err: &grimmory.HTTPError{Operation: "sync", Status: 503}}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	if err := scheduler.Scan(context.Background()); err == nil {
		t.Fatal("expected aggregated transient failure")
	}
	now = now.Add(time.Second)
	if err := scheduler.Scan(context.Background()); err == nil {
		t.Fatal("expected aggregated retry exhaustion")
	}
	store.mu.Lock()
	value := store.states[book.ID]
	store.mu.Unlock()
	if value.Status != state.PollStatusFailed || value.AttemptCount != 2 || store.failures != 2 {
		t.Fatalf("retry state = %+v failures=%d", value, store.failures)
	}
}

func TestSchedulerMarksPostUploadObservationCurrent(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	before, after := testBook(), testBook()
	after.Files = append(after.Files, grimmory.File{ID: "mobi-id", Name: "book.mobi", Format: "mobi"})
	remote := &fakeRemote{books: []grimmory.Book{before}, detail: []grimmory.Book{before, after}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	if err := scheduler.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	value := store.states[before.ID]
	store.mu.Unlock()
	want := grimmory.ObservationFingerprint(after)
	if value.Status != state.PollStatusCurrent || value.AppliedFingerprint != want || len(reconciler.calls) != 1 {
		t.Fatalf("post-upload state = %+v calls=%v", value, reconciler.calls)
	}
}

func TestSchedulerCancellationConsumesNoAttempt(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	reconciler := &fakeReconciler{started: started, finished: make(chan struct{})}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	result := make(chan error, 1)
	go func() { result <- scheduler.Scan(ctx) }()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("scan cancellation error = %v", err)
	}
	store.mu.Lock()
	value := store.states[book.ID]
	store.mu.Unlock()
	if value.AttemptCount != 0 || store.failures != 0 {
		t.Fatalf("cancellation consumed attempt: %+v failures=%d", value, store.failures)
	}
}

func TestSchedulerFailureTagsDoNotChangeRetryObservation(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{err: errors.New("permanent conversion failure")}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	scheduler.failedTag = "failed"
	fingerprint := grimmory.ObservationFingerprintIgnoringTags(book, "failed")
	stateValue, err := store.UpsertPollObservation(context.Background(), "1", book.ID, fingerprint, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.process(context.Background(), stateValue); err == nil {
		t.Fatal("expected conversion failure")
	}
	store.mu.Lock()
	got := store.states[book.ID]
	store.mu.Unlock()
	remote.mu.Lock()
	added := append([]string(nil), remote.added...)
	remote.mu.Unlock()
	if got.Status != state.PollStatusFailed || got.AttemptCount != 1 || len(added) != 1 {
		t.Fatalf("retry-exhausted tag state=%+v added=%v", got, added)
	}
}

func TestSchedulerRequeuesWhenFailureTagMutationFails(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	book := testBook()
	remote := &fakeRemote{books: []grimmory.Book{book}, detail: []grimmory.Book{book}}
	store := &memoryStore{}
	reconciler := &fakeReconciler{err: errors.New("permanent conversion failure")}
	scheduler := newTestScheduler(t, remote, store, reconciler, &now)
	scheduler.failedTag = "failed"
	fingerprint := grimmory.ObservationFingerprintIgnoringTags(book, "failed")
	stateValue, err := store.UpsertPollObservation(context.Background(), "1", book.ID, fingerprint, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.process(context.Background(), stateValue); err == nil {
		t.Fatal("expected conversion failure with tag mutation failure")
	}
	remote.mu.Lock()
	remote.added = nil
	remote.mu.Unlock()
	// A tag mutation error must requeue the same observation without consuming
	// another conversion attempt.
	remote.tagError = errors.New("tag endpoint unavailable")
	if err := scheduler.process(context.Background(), stateValue); err == nil {
		t.Fatal("expected conversion failure after tag mutation failure")
	}
	remote.mu.Lock()
	added := append([]string(nil), remote.added...)
	remote.mu.Unlock()
	store.mu.Lock()
	got := store.states[book.ID]
	store.mu.Unlock()
	if len(added) != 0 || got.Status != state.PollStatusRetry || got.AttemptCount != 1 || got.NextAttemptAt.After(now) {
		t.Fatalf("tag mutation retry state=%+v added=%v", got, added)
	}
}

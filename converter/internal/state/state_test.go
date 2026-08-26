package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStorePersistsBookAndDerivedState(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	canonical := now.Add(-time.Minute)
	if err := store.SetBook(context.Background(), BookState{LibraryID: "library", BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: "main-sha", CanonicalMTime: canonical, TrustedMTime: true, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDerived(context.Background(), DerivedState{LibraryID: "library", BookID: "book", Format: "mobi", GrimmoryFileID: "file", SourceSHA256: "main-sha", OutputSHA256: "output-sha", GenerationFingerprint: "generation-sha", TrustedMTime: now, HasMTime: true, GeneratedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	book, derived, err := store.Get(context.Background(), "library", "book")
	if err != nil {
		t.Fatal(err)
	}
	if book.CanonicalSHA256 != "main-sha" || !book.TrustedMTime || book.CanonicalMTime.UnixNano() != canonical.UnixNano() {
		t.Fatalf("book state = %+v", book)
	}
	if got := derived["mobi"]; got.GrimmoryFileID != "file" || got.SourceSHA256 != "main-sha" || got.OutputSHA256 != "output-sha" || got.GenerationFingerprint != "generation-sha" || !got.HasMTime || got.GeneratedAt.IsZero() {
		t.Fatalf("derived state = %+v", derived)
	}
	info, err := os.Stat(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state database mode=%v", info.Mode().Perm())
	}
}

func TestStoreLeavesExistingDerivedStateOnBookUpdate(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SetBook(ctx, BookState{LibraryID: "library", BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: "old", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDerived(ctx, DerivedState{LibraryID: "library", BookID: "book", Format: "mobi", SourceSHA256: "old", OutputSHA256: "old-output", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetBook(ctx, BookState{LibraryID: "library", BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: "new", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	_, derived, err := store.Get(ctx, "library", "book")
	if err != nil {
		t.Fatal(err)
	}
	if derived["mobi"].SourceSHA256 != "old" {
		t.Fatal("book update deleted old derivative evidence")
	}
}

func TestStorePreservesUnixEpochCheckpoints(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	epoch := time.Unix(0, 0).UTC()
	if err := store.SetBook(context.Background(), BookState{
		LibraryID:       "library",
		BookID:          "book",
		MainFormat:      "epub",
		CanonicalFormat: "epub",
		CanonicalSHA256: "sha",
		CanonicalMTime:  epoch,
		TrustedMTime:    true,
		UpdatedAt:       epoch,
	}); err != nil {
		t.Fatal(err)
	}
	book, _, err := store.Get(context.Background(), "library", "book")
	if err != nil {
		t.Fatal(err)
	}
	if !book.TrustedMTime || !book.CanonicalMTime.Equal(epoch) || !book.UpdatedAt.Equal(epoch) {
		t.Fatalf("epoch checkpoint = %+v", book)
	}
}

func TestStoreIsolatesEqualBookIDsAcrossLibraries(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, time.August, 26, 15, 0, 0, 0, time.UTC)
	for library, sha := range map[string]string{"library-a": "sha-a", "library-b": "sha-b"} {
		if err := store.SetBook(ctx, BookState{LibraryID: library, BookID: "same-book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: sha, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if err := store.SetDerived(ctx, DerivedState{LibraryID: library, BookID: "same-book", Format: "mobi", SourceSHA256: sha, OutputSHA256: "output-" + library, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		poll, err := store.UpsertPollObservation(ctx, library, "same-book", "observation-"+library, now)
		if err != nil {
			t.Fatal(err)
		}
		if poll.LibraryID != library || poll.BookID != "same-book" {
			t.Fatalf("poll scope = %+v", poll)
		}
	}

	bookA, derivedA, err := store.Get(ctx, "library-a", "same-book")
	if err != nil {
		t.Fatal(err)
	}
	bookB, derivedB, err := store.Get(ctx, "library-b", "same-book")
	if err != nil {
		t.Fatal(err)
	}
	if bookA.LibraryID != "library-a" || bookA.CanonicalSHA256 != "sha-a" || derivedA["mobi"].OutputSHA256 != "output-library-a" {
		t.Fatalf("library-a state = book=%+v derived=%+v", bookA, derivedA)
	}
	if bookB.LibraryID != "library-b" || bookB.CanonicalSHA256 != "sha-b" || derivedB["mobi"].OutputSHA256 != "output-library-b" {
		t.Fatalf("library-b state = book=%+v derived=%+v", bookB, derivedB)
	}

	if err := store.MarkPollSuccess(ctx, "library-a", "same-book", "observation-library-a", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	pollB, err := store.UpsertPollObservation(ctx, "library-b", "same-book", "observation-library-b", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if pollB.Status != PollStatusPending || pollB.AppliedFingerprint != "" {
		t.Fatalf("library-b poll changed by library-a success = %+v", pollB)
	}
}

func TestStorePollStaleGuardIncludesLibrary(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, time.August, 26, 16, 0, 0, 0, time.UTC)
	for _, library := range []string{"library-a", "library-b"} {
		if _, err := store.UpsertPollObservation(ctx, library, "same-book", "old", now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.UpsertPollObservation(ctx, "library-a", "same-book", "new", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPollSuccess(ctx, "library-a", "same-book", "old", now.Add(2*time.Second)); !errors.Is(err, ErrPollObservationChanged) {
		t.Fatalf("stale success error = %v", err)
	}
	if err := store.MarkPollSuccess(ctx, "library-b", "same-book", "old", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	dueA, err := store.ListDuePollStates(ctx, "library-a", now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	dueB, err := store.ListDuePollStates(ctx, "library-b", now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dueA) != 1 || dueA[0].ObservationFingerprint != "new" || len(dueB) != 0 {
		t.Fatalf("stale guard leaked: due-a=%+v due-b=%+v", dueA, dueB)
	}
}

func TestStoreConfiguresSQLitePragmas(t *testing.T) {
	store, err := Open(t.TempDir(), 1500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var journal, foreign, busy string
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreign); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("PRAGMA busy_timeout").Scan(&busy); err != nil {
		t.Fatal(err)
	}
	if journal != "wal" || foreign != "1" || busy != "1500" {
		t.Fatalf("pragmas journal=%q foreign=%q busy=%q", journal, foreign, busy)
	}
}

func TestStorePersistsPollStateAndResetsChangedObservations(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	seen := time.Date(2026, time.August, 26, 10, 0, 0, 0, time.UTC)

	poll, err := store.UpsertPollObservation(ctx, "library", "book", "one", seen)
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusPending || poll.AttemptCount != 0 || poll.ObservationFingerprint != "one" || poll.AppliedFingerprint != "" || !poll.NextAttemptAt.Equal(seen) || !poll.LastSeenAt.Equal(seen) || !poll.UpdatedAt.Equal(seen) {
		t.Fatalf("initial poll state = %+v", poll)
	}
	due, err := store.ListDuePollStates(ctx, "library", seen, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].BookID != "book" {
		t.Fatalf("initial due poll states = %+v", due)
	}
	if err := store.MarkPollSuccess(ctx, "library", "book", "one", seen.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	retryAt := seen.Add(5 * time.Minute)
	poll, err = store.UpsertPollObservation(ctx, "library", "book", "one", seen.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.AttemptCount != 0 || poll.AppliedFingerprint != "one" || !poll.LastSeenAt.Equal(seen.Add(2*time.Second)) {
		t.Fatalf("unchanged poll state = %+v", poll)
	}

	poll, err = store.UpsertPollObservation(ctx, "library", "book", "two", seen.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusPending || poll.AttemptCount != 0 || poll.AppliedFingerprint != "one" || poll.ErrorCode != "" || !poll.NextAttemptAt.Equal(seen.Add(3*time.Second)) {
		t.Fatalf("changed poll state = %+v", poll)
	}
	if _, err := store.RecordPollFailure(ctx, "library", "book", "two", "network_timeout", retryAt, 2, seen.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err = store.UpsertPollObservation(ctx, "library", "book", "two", seen.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusRetry || poll.AttemptCount != 1 || poll.ErrorCode != "network_timeout" || !poll.NextAttemptAt.Equal(retryAt) || !poll.LastSeenAt.Equal(seen.Add(6*time.Second)) {
		t.Fatalf("repeated retry poll state = %+v", poll)
	}
	due, err = store.ListDuePollStates(ctx, "library", seen.Add(4*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("early due poll states = %+v", due)
	}
	due, err = store.ListDuePollStates(ctx, "library", retryAt, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].AttemptCount != 1 {
		t.Fatalf("retry due poll states = %+v", due)
	}

	if err := store.MarkPollSuccess(ctx, "library", "book", "one", seen.Add(7*time.Second)); !errors.Is(err, ErrPollObservationChanged) {
		t.Fatalf("stale success error = %v", err)
	}
	if err := store.MarkPollSuccess(ctx, "library", "book", "two", seen.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err = store.UpsertPollObservation(ctx, "library", "book", "two", seen.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.AttemptCount != 0 || poll.AppliedFingerprint != "two" || !poll.NextAttemptAt.IsZero() || poll.ErrorCode != "" {
		t.Fatalf("successful poll state = %+v", poll)
	}
	due, err = store.ListDuePollStates(ctx, "library", seen.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("successful due poll states = %+v", due)
	}
}

func TestStoreMarksRetryExhaustedPollFailureAndBoundsErrorCode(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, time.August, 26, 11, 0, 0, 0, time.UTC)
	if _, err := store.UpsertPollObservation(ctx, "library", "book", "fingerprint", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailure(ctx, "library", "book", "fingerprint", strings.Repeat("x", MaxPollErrorCodeLength+1), now, 2, now); err == nil {
		t.Fatal("accepted an oversized poll error code")
	}
	if _, err := store.RecordPollFailure(ctx, "library", "book", "fingerprint", "unsafe code", now, 2, now); err == nil {
		t.Fatal("accepted an unsafe poll error code")
	}
	if err := store.MarkPollFailure(ctx, "library", "book", "fingerprint", "terminal_failure", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err := store.UpsertPollObservation(ctx, "library", "book", "fingerprint", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusFailed || poll.AttemptCount != 1 || poll.ErrorCode != "terminal_failure" || !poll.NextAttemptAt.IsZero() {
		t.Fatalf("retry-exhausted poll state = %+v", poll)
	}
	if err := store.MarkPollSuccess(ctx, "library", "book", "fingerprint", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err = store.UpsertPollObservation(ctx, "library", "book", "fingerprint", now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.AttemptCount != 0 || poll.AppliedFingerprint != "fingerprint" || poll.ErrorCode != "" {
		t.Fatalf("recovered poll state = %+v", poll)
	}
	due, err := store.ListDuePollStates(ctx, "library", now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("retry-exhausted failure was due = %+v", due)
	}
}

func TestStoreRequeuesPollObservationWithoutConsumingAttempt(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	if _, err := store.UpsertPollObservation(ctx, "library", "book", "fingerprint", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailure(ctx, "library", "book", "fingerprint", "conversion", now, 2, now); err != nil {
		t.Fatal(err)
	}
	if err := store.RequeuePollObservation(ctx, "library", "book", "fingerprint", "failure_tag_mutation", time.Time{}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	due, err := store.ListDuePollStates(ctx, "library", now.Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Status != PollStatusRetry || due[0].AttemptCount != 1 || !due[0].NextAttemptAt.IsZero() || due[0].ErrorCode != "failure_tag_mutation" {
		t.Fatalf("requeued poll states = %+v", due)
	}
}

func TestStoreRecordPollFailureEnforcesMaxAttempts(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, time.August, 26, 13, 0, 0, 0, time.UTC)
	if _, err := store.UpsertPollObservation(ctx, "library", "book", "fingerprint", now); err != nil {
		t.Fatal(err)
	}
	retryAt := now.Add(time.Minute)
	poll, err := store.RecordPollFailure(ctx, "library", "book", "fingerprint", "temporary", retryAt, 2, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusRetry || poll.AttemptCount != 1 || !poll.NextAttemptAt.Equal(retryAt) || poll.ErrorCode != "temporary" {
		t.Fatalf("retry boundary poll state = %+v", poll)
	}
	poll, err = store.RecordPollFailure(ctx, "library", "book", "fingerprint", "terminal", now.Add(2*time.Minute), 2, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusFailed || poll.AttemptCount != 2 || !poll.NextAttemptAt.IsZero() || poll.ErrorCode != "terminal" {
		t.Fatalf("failure boundary poll state = %+v", poll)
	}
	if due, err := store.ListDuePollStates(ctx, "library", now.Add(time.Hour), 10); err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("retry-exhausted poll state was due = %+v", due)
	}

	if _, err := store.UpsertPollObservation(ctx, "library", "other-book", "other-fingerprint", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailure(ctx, "library", "other-book", "other-fingerprint", "bad", now.Add(time.Minute), 0, now.Add(time.Second)); err == nil {
		t.Fatal("accepted zero max attempts")
	}
	poll, err = store.UpsertPollObservation(ctx, "library", "other-book", "other-fingerprint", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusPending || poll.AttemptCount != 0 || poll.ErrorCode != "" {
		t.Fatalf("state changed after invalid max attempts = %+v", poll)
	}
}

func TestStoreObservationReturningToAppliedIsCurrent(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, time.August, 26, 14, 0, 0, 0, time.UTC)
	if _, err := store.UpsertPollObservation(ctx, "library", "book", "applied", now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPollSuccess(ctx, "library", "book", "applied", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertPollObservation(ctx, "library", "book", "changed", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailure(ctx, "library", "book", "changed", "temporary", now.Add(time.Hour), 2, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err := store.UpsertPollObservation(ctx, "library", "book", "applied", now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.ObservationFingerprint != "applied" || poll.AppliedFingerprint != "applied" || poll.AttemptCount != 0 || !poll.NextAttemptAt.IsZero() || poll.ErrorCode != "" {
		t.Fatalf("returned observation poll state = %+v", poll)
	}
	if due, err := store.ListDuePollStates(ctx, "library", now.Add(2*time.Hour), 10); err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("returned applied observation was due = %+v", due)
	}
}

func TestStorePollStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	store, err := Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertPollObservation(context.Background(), "library", "book", "fingerprint", now); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	due, err := store.ListDuePollStates(context.Background(), "library", now, 1)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].LibraryID != "library" || due[0].ObservationFingerprint != "fingerprint" {
		_ = store.Close()
		t.Fatalf("reopened due poll states = %+v", due)
	}
	if err := store.MarkPollSuccess(context.Background(), "library", "book", "fingerprint", now.Add(time.Second)); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	poll, err := store.UpsertPollObservation(context.Background(), "library", "book", "fingerprint", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.LibraryID != "library" || poll.Status != PollStatusCurrent || poll.AppliedFingerprint != "fingerprint" {
		t.Fatalf("reopened poll state = %+v", poll)
	}
}

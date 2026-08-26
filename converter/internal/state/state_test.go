package state

import (
	"context"
	"database/sql"
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
	if err := store.SetBookScoped(context.Background(), "library", BookState{BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: "main-sha", CanonicalMTime: canonical, TrustedMTime: true, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDerivedScoped(context.Background(), "library", DerivedState{BookID: "book", Format: "mobi", GrimmoryFileID: "file", SourceSHA256: "main-sha", OutputSHA256: "output-sha", GenerationFingerprint: "generation-sha", TrustedMTime: now, HasMTime: true, GeneratedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	book, derived, err := store.GetScoped(context.Background(), "library", "book")
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
	if err := store.SetBookScoped(ctx, "library", BookState{BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: "old", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDerivedScoped(ctx, "library", DerivedState{BookID: "book", Format: "mobi", SourceSHA256: "old", OutputSHA256: "old-output", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetBookScoped(ctx, "library", BookState{BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: "new", UpdatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	_, derived, err := store.GetScoped(ctx, "library", "book")
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
	if err := store.SetBookScoped(context.Background(), "library", BookState{
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
	book, _, err := store.GetScoped(context.Background(), "library", "book")
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
		if err := store.SetBookScoped(ctx, library, BookState{BookID: "same-book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalSHA256: sha, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		if err := store.SetDerivedScoped(ctx, library, DerivedState{BookID: "same-book", Format: "mobi", SourceSHA256: sha, OutputSHA256: "output-" + library, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
		poll, err := store.UpsertPollObservationScoped(ctx, library, "same-book", "observation-"+library, now)
		if err != nil {
			t.Fatal(err)
		}
		if poll.LibraryID != library || poll.BookID != "same-book" {
			t.Fatalf("poll scope = %+v", poll)
		}
	}

	bookA, derivedA, err := store.GetScoped(ctx, "library-a", "same-book")
	if err != nil {
		t.Fatal(err)
	}
	bookB, derivedB, err := store.GetScoped(ctx, "library-b", "same-book")
	if err != nil {
		t.Fatal(err)
	}
	if bookA.LibraryID != "library-a" || bookA.CanonicalSHA256 != "sha-a" || derivedA["mobi"].OutputSHA256 != "output-library-a" {
		t.Fatalf("library-a state = book=%+v derived=%+v", bookA, derivedA)
	}
	if bookB.LibraryID != "library-b" || bookB.CanonicalSHA256 != "sha-b" || derivedB["mobi"].OutputSHA256 != "output-library-b" {
		t.Fatalf("library-b state = book=%+v derived=%+v", bookB, derivedB)
	}

	if err := store.MarkPollSuccessScoped(ctx, "library-a", "same-book", "observation-library-a", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	pollB, err := store.UpsertPollObservationScoped(ctx, "library-b", "same-book", "observation-library-b", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if pollB.Status != PollStatusPending || pollB.AppliedFingerprint != "" {
		t.Fatalf("library-b poll changed by library-a success = %+v", pollB)
	}
}

func TestStoreScopedPollStaleGuardIncludesLibrary(t *testing.T) {
	store, err := Open(t.TempDir(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, time.August, 26, 16, 0, 0, 0, time.UTC)
	for _, library := range []string{"library-a", "library-b"} {
		if _, err := store.UpsertPollObservationScoped(ctx, library, "same-book", "old", now); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.UpsertPollObservationScoped(ctx, "library-a", "same-book", "new", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPollSuccessScoped(ctx, "library-a", "same-book", "old", now.Add(2*time.Second)); !errors.Is(err, ErrPollObservationChanged) {
		t.Fatalf("stale scoped success error = %v", err)
	}
	if err := store.MarkPollSuccessScoped(ctx, "library-b", "same-book", "old", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	dueA, err := store.ListDuePollStatesScoped(ctx, "library-a", now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	dueB, err := store.ListDuePollStatesScoped(ctx, "library-b", now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dueA) != 1 || dueA[0].ObservationFingerprint != "new" || len(dueB) != 0 {
		t.Fatalf("scoped stale guard leaked: due-a=%+v due-b=%+v", dueA, dueB)
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

func TestStoreResetsUnscopedStateSchema(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE book_state (book_id TEXT PRIMARY KEY, main_format TEXT NOT NULL, canonical_format TEXT NOT NULL, canonical_sha256 TEXT NOT NULL, canonical_mtime_ns INTEGER, updated_at_ns INTEGER NOT NULL);
CREATE TABLE derived (book_id TEXT NOT NULL, format TEXT NOT NULL, source_sha256 TEXT NOT NULL, output_sha256 TEXT NOT NULL, trusted_mtime_ns INTEGER, updated_at_ns INTEGER NOT NULL, PRIMARY KEY(book_id, format), FOREIGN KEY(book_id) REFERENCES book_state(book_id));
CREATE TABLE poll_state (book_id TEXT PRIMARY KEY, observation_fingerprint TEXT NOT NULL, applied_fingerprint TEXT, status TEXT NOT NULL, attempt_count INTEGER NOT NULL, next_attempt_at_ns INTEGER, error_code TEXT, last_seen_at_ns INTEGER NOT NULL, updated_at_ns INTEGER NOT NULL);
CREATE TABLE book_state_v2 (book_id TEXT PRIMARY KEY);
CREATE TABLE derived_v2 (book_id TEXT NOT NULL, format TEXT NOT NULL, PRIMARY KEY(book_id, format));
CREATE TABLE poll_state_v2 (book_id TEXT PRIMARY KEY);
INSERT INTO book_state (book_id, main_format, canonical_format, canonical_sha256, canonical_mtime_ns, updated_at_ns) VALUES ('legacy-book', 'epub', 'epub', 'legacy-sha', NULL, 1);
INSERT INTO derived (book_id, format, source_sha256, output_sha256, trusted_mtime_ns, updated_at_ns) VALUES ('legacy-book', 'mobi', 'legacy-sha', 'legacy-output', NULL, 1);
INSERT INTO poll_state (book_id, observation_fingerprint, applied_fingerprint, status, attempt_count, next_attempt_at_ns, error_code, last_seen_at_ns, updated_at_ns) VALUES ('legacy-book', 'legacy-observation', NULL, 'pending', 0, NULL, NULL, 1, 1);
INSERT INTO book_state_v2 (book_id) VALUES ('alternate-book');
INSERT INTO derived_v2 (book_id, format) VALUES ('alternate-book', 'mobi');
INSERT INTO poll_state_v2 (book_id) VALUES ('alternate-book');`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, table := range []string{"book_state", "derived", "poll_state"} {
		var rows int
		if err := store.db.QueryRow("SELECT count(*) FROM " + table).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("migration retained unscoped rows in %s: %d", table, rows)
		}
	}
	var alternateRows int
	if err := store.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name LIKE '%\\_v2' ESCAPE '\\'").Scan(&alternateRows); err != nil {
		t.Fatal(err)
	}
	if alternateRows != 0 {
		t.Fatalf("migration retained alternate tables: %d", alternateRows)
	}
	var libraryColumn, bookPrimaryKey int
	if err := store.db.QueryRow("SELECT \"notnull\" FROM pragma_table_info('book_state') WHERE name = 'library_id'").Scan(&libraryColumn); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow("SELECT pk FROM pragma_table_info('book_state') WHERE name = 'book_id'").Scan(&bookPrimaryKey); err != nil {
		t.Fatal(err)
	}
	if libraryColumn != 1 || bookPrimaryKey != 2 {
		t.Fatalf("rebuilt book_state key metadata: library notnull=%d book pk=%d", libraryColumn, bookPrimaryKey)
	}
	lastSuccess := time.Now().UTC()
	if err := store.SetBookScoped(context.Background(), "library", BookState{BookID: "book", MainFormat: "epub", CanonicalFormat: "epub", CanonicalFileID: "file", CanonicalFileName: "book.epub", CanonicalSHA256: "sha", LastSuccessfulSync: lastSuccess, UpdatedAt: lastSuccess}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDerivedScoped(context.Background(), "library", DerivedState{BookID: "book", Format: "mobi", GrimmoryFileID: "derived-file", SourceSHA256: "sha", OutputSHA256: "output", GenerationFingerprint: "generation-sha", GeneratedAt: lastSuccess, UpdatedAt: lastSuccess}); err != nil {
		t.Fatal(err)
	}
	book, derived, err := store.GetScoped(context.Background(), "library", "book")
	if err != nil {
		t.Fatal(err)
	}
	if book.CanonicalFileID != "file" || book.CanonicalFileName != "book.epub" || book.LastSuccessfulSync.IsZero() || derived["mobi"].GrimmoryFileID != "derived-file" || derived["mobi"].GenerationFingerprint != "generation-sha" || derived["mobi"].GeneratedAt.IsZero() {
		t.Fatalf("migrated state book=%+v derived=%+v", book, derived)
	}
	columns := make(map[string]bool)
	rows, err := store.db.Query("PRAGMA table_info(poll_state)")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"library_id", "book_id", "observation_fingerprint", "applied_fingerprint", "status", "attempt_count", "next_attempt_at_ns", "error_code", "last_seen_at_ns", "updated_at_ns"} {
		if !columns[column] {
			t.Errorf("poll_state is missing column %q", column)
		}
	}
}

func TestStoreRebuildsIncompatibleScopedTable(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE poll_state (
library_id TEXT NOT NULL,
book_id TEXT NOT NULL,
observation_fingerprint TEXT NOT NULL,
applied_fingerprint TEXT,
status TEXT NOT NULL,
attempt_count INTEGER NOT NULL,
next_attempt_at_ns INTEGER,
error_code TEXT,
last_seen_at_ns INTEGER NOT NULL,
updated_at_ns INTEGER NOT NULL,
PRIMARY KEY (library_id, book_id)
);
INSERT INTO poll_state (library_id, book_id, observation_fingerprint, status, attempt_count, last_seen_at_ns, updated_at_ns) VALUES ('library', 'book', 'old', 'invalid', -1, 1, 1);`)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			t.Log(closeErr)
		}
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(dir, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var rows int
	if err := store.db.QueryRow("SELECT count(*) FROM poll_state").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("incompatible poll state was retained: %d rows", rows)
	}
	if _, err := store.db.Exec(`INSERT INTO poll_state (library_id, book_id, observation_fingerprint, status, attempt_count, last_seen_at_ns, updated_at_ns) VALUES ('library', 'book', 'new', 'invalid', -1, 1, 1)`); err == nil {
		t.Fatal("rebuilt poll state accepted invalid values")
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

	poll, err := store.UpsertPollObservationScoped(ctx, "library", "book", "one", seen)
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusPending || poll.AttemptCount != 0 || poll.ObservationFingerprint != "one" || poll.AppliedFingerprint != "" || !poll.NextAttemptAt.Equal(seen) || !poll.LastSeenAt.Equal(seen) || !poll.UpdatedAt.Equal(seen) {
		t.Fatalf("initial poll state = %+v", poll)
	}
	due, err := store.ListDuePollStatesScoped(ctx, "library", seen, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].BookID != "book" {
		t.Fatalf("initial due poll states = %+v", due)
	}
	if err := store.MarkPollSuccessScoped(ctx, "library", "book", "one", seen.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	retryAt := seen.Add(5 * time.Minute)
	poll, err = store.UpsertPollObservationScoped(ctx, "library", "book", "one", seen.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.AttemptCount != 0 || poll.AppliedFingerprint != "one" || !poll.LastSeenAt.Equal(seen.Add(2*time.Second)) {
		t.Fatalf("unchanged poll state = %+v", poll)
	}

	poll, err = store.UpsertPollObservationScoped(ctx, "library", "book", "two", seen.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusPending || poll.AttemptCount != 0 || poll.AppliedFingerprint != "one" || poll.ErrorCode != "" || !poll.NextAttemptAt.Equal(seen.Add(3*time.Second)) {
		t.Fatalf("changed poll state = %+v", poll)
	}
	if _, err := store.RecordPollFailureScoped(ctx, "library", "book", "two", "network_timeout", retryAt, 2, seen.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err = store.UpsertPollObservationScoped(ctx, "library", "book", "two", seen.Add(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusRetry || poll.AttemptCount != 1 || poll.ErrorCode != "network_timeout" || !poll.NextAttemptAt.Equal(retryAt) || !poll.LastSeenAt.Equal(seen.Add(6*time.Second)) {
		t.Fatalf("repeated retry poll state = %+v", poll)
	}
	due, err = store.ListDuePollStatesScoped(ctx, "library", seen.Add(4*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("early due poll states = %+v", due)
	}
	due, err = store.ListDuePollStatesScoped(ctx, "library", retryAt, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].AttemptCount != 1 {
		t.Fatalf("retry due poll states = %+v", due)
	}

	if err := store.MarkPollSuccessScoped(ctx, "library", "book", "one", seen.Add(7*time.Second)); !errors.Is(err, ErrPollObservationChanged) {
		t.Fatalf("stale success error = %v", err)
	}
	if err := store.MarkPollSuccessScoped(ctx, "library", "book", "two", seen.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err = store.UpsertPollObservationScoped(ctx, "library", "book", "two", seen.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.AttemptCount != 0 || poll.AppliedFingerprint != "two" || !poll.NextAttemptAt.IsZero() || poll.ErrorCode != "" {
		t.Fatalf("successful poll state = %+v", poll)
	}
	due, err = store.ListDuePollStatesScoped(ctx, "library", seen.Add(time.Hour), 10)
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
	if _, err := store.UpsertPollObservationScoped(ctx, "library", "book", "fingerprint", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailureScoped(ctx, "library", "book", "fingerprint", strings.Repeat("x", MaxPollErrorCodeLength+1), now, 2, now); err == nil {
		t.Fatal("accepted an oversized poll error code")
	}
	if _, err := store.RecordPollFailureScoped(ctx, "library", "book", "fingerprint", "unsafe code", now, 2, now); err == nil {
		t.Fatal("accepted an unsafe poll error code")
	}
	if err := store.MarkPollFailureScoped(ctx, "library", "book", "fingerprint", "terminal_failure", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err := store.UpsertPollObservationScoped(ctx, "library", "book", "fingerprint", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusFailed || poll.AttemptCount != 1 || poll.ErrorCode != "terminal_failure" || !poll.NextAttemptAt.IsZero() {
		t.Fatalf("retry-exhausted poll state = %+v", poll)
	}
	if err := store.MarkPollSuccessScoped(ctx, "library", "book", "fingerprint", now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err = store.UpsertPollObservationScoped(ctx, "library", "book", "fingerprint", now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.AttemptCount != 0 || poll.AppliedFingerprint != "fingerprint" || poll.ErrorCode != "" {
		t.Fatalf("recovered poll state = %+v", poll)
	}
	due, err := store.ListDuePollStatesScoped(ctx, "library", now.Add(time.Hour), 10)
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
	if _, err := store.UpsertPollObservationScoped(ctx, "library", "book", "fingerprint", now); err != nil {
		t.Fatal(err)
	}
	retryAt := now.Add(time.Minute)
	poll, err := store.RecordPollFailureScoped(ctx, "library", "book", "fingerprint", "temporary", retryAt, 2, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusRetry || poll.AttemptCount != 1 || !poll.NextAttemptAt.Equal(retryAt) || poll.ErrorCode != "temporary" {
		t.Fatalf("retry boundary poll state = %+v", poll)
	}
	poll, err = store.RecordPollFailureScoped(ctx, "library", "book", "fingerprint", "terminal", now.Add(2*time.Minute), 2, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusFailed || poll.AttemptCount != 2 || !poll.NextAttemptAt.IsZero() || poll.ErrorCode != "terminal" {
		t.Fatalf("failure boundary poll state = %+v", poll)
	}
	if due, err := store.ListDuePollStatesScoped(ctx, "library", now.Add(time.Hour), 10); err != nil {
		t.Fatal(err)
	} else if len(due) != 0 {
		t.Fatalf("retry-exhausted poll state was due = %+v", due)
	}

	if _, err := store.UpsertPollObservationScoped(ctx, "library", "other-book", "other-fingerprint", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailureScoped(ctx, "library", "other-book", "other-fingerprint", "bad", now.Add(time.Minute), 0, now.Add(time.Second)); err == nil {
		t.Fatal("accepted zero max attempts")
	}
	poll, err = store.UpsertPollObservationScoped(ctx, "library", "other-book", "other-fingerprint", now.Add(2*time.Second))
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
	if _, err := store.UpsertPollObservationScoped(ctx, "library", "book", "applied", now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPollSuccessScoped(ctx, "library", "book", "applied", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertPollObservationScoped(ctx, "library", "book", "changed", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPollFailureScoped(ctx, "library", "book", "changed", "temporary", now.Add(time.Hour), 2, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	poll, err := store.UpsertPollObservationScoped(ctx, "library", "book", "applied", now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.Status != PollStatusCurrent || poll.ObservationFingerprint != "applied" || poll.AppliedFingerprint != "applied" || poll.AttemptCount != 0 || !poll.NextAttemptAt.IsZero() || poll.ErrorCode != "" {
		t.Fatalf("returned observation poll state = %+v", poll)
	}
	if due, err := store.ListDuePollStatesScoped(ctx, "library", now.Add(2*time.Hour), 10); err != nil {
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
	if _, err := store.UpsertPollObservationScoped(context.Background(), "library", "book", "fingerprint", now); err != nil {
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
	due, err := store.ListDuePollStatesScoped(context.Background(), "library", now, 1)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].LibraryID != "library" || due[0].ObservationFingerprint != "fingerprint" {
		_ = store.Close()
		t.Fatalf("reopened due poll states = %+v", due)
	}
	if err := store.MarkPollSuccessScoped(context.Background(), "library", "book", "fingerprint", now.Add(time.Second)); err != nil {
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
	poll, err := store.UpsertPollObservationScoped(context.Background(), "library", "book", "fingerprint", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if poll.LibraryID != "library" || poll.Status != PollStatusCurrent || poll.AppliedFingerprint != "fingerprint" {
		t.Fatalf("reopened poll state = %+v", poll)
	}
}

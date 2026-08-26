// Package state stores the service's reconciliation checkpoints. It contains
// no Grimmory files; only hashes and metadata needed to make the next decision.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS book_state (
    library_id TEXT NOT NULL,
    book_id TEXT NOT NULL,
    main_format TEXT NOT NULL,
    canonical_format TEXT NOT NULL,
    canonical_file_id TEXT,
    canonical_file_name TEXT,
    canonical_sha256 TEXT NOT NULL,
    metadata_fingerprint TEXT,
    canonical_mtime_ns INTEGER,
    last_successful_sync_ns INTEGER,
    updated_at_ns INTEGER NOT NULL,
    PRIMARY KEY (library_id, book_id)
);
CREATE TABLE IF NOT EXISTS derived (
    library_id TEXT NOT NULL,
    book_id TEXT NOT NULL,
    format TEXT NOT NULL,
    grimmory_file_id TEXT,
    source_sha256 TEXT NOT NULL,
    output_sha256 TEXT NOT NULL,
    generation_fingerprint TEXT,
    trusted_mtime_ns INTEGER,
    generated_at_ns INTEGER,
    updated_at_ns INTEGER NOT NULL,
    PRIMARY KEY (library_id, book_id, format),
    FOREIGN KEY (library_id, book_id) REFERENCES book_state(library_id, book_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS poll_state (
    library_id TEXT NOT NULL,
    book_id TEXT NOT NULL,
    observation_fingerprint TEXT NOT NULL,
    applied_fingerprint TEXT,
    status TEXT NOT NULL CHECK (status IN ('current', 'pending', 'retry', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at_ns INTEGER,
    error_code TEXT CHECK (error_code IS NULL OR length(error_code) BETWEEN 1 AND 64),
    last_seen_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    PRIMARY KEY (library_id, book_id)
);`

// The initial library-aware release reuses the original table names. There is
// no safe way to assign an old unscoped row to a library, so an incompatible
// existing state schema is reset transactionally and its checkpoints are
// intentionally discarded. The reset also removes the *_v2 tables written by
// an intermediate development build; those tables are never created here.
var stateTableSpecs = map[string]tableSpec{
	"book_state": {
		columns: []columnSpec{
			{name: "library_id", typ: "TEXT", notNull: true, primaryKey: 1},
			{name: "book_id", typ: "TEXT", notNull: true, primaryKey: 2},
			{name: "main_format", typ: "TEXT", notNull: true},
			{name: "canonical_format", typ: "TEXT", notNull: true},
			{name: "canonical_file_id", typ: "TEXT"},
			{name: "canonical_file_name", typ: "TEXT"},
			{name: "canonical_sha256", typ: "TEXT", notNull: true},
			{name: "metadata_fingerprint", typ: "TEXT"},
			{name: "canonical_mtime_ns", typ: "INTEGER"},
			{name: "last_successful_sync_ns", typ: "INTEGER"},
			{name: "updated_at_ns", typ: "INTEGER", notNull: true},
		},
	},
	"derived": {
		columns: []columnSpec{
			{name: "library_id", typ: "TEXT", notNull: true, primaryKey: 1},
			{name: "book_id", typ: "TEXT", notNull: true, primaryKey: 2},
			{name: "format", typ: "TEXT", notNull: true, primaryKey: 3},
			{name: "grimmory_file_id", typ: "TEXT"},
			{name: "source_sha256", typ: "TEXT", notNull: true},
			{name: "output_sha256", typ: "TEXT", notNull: true},
			{name: "generation_fingerprint", typ: "TEXT"},
			{name: "trusted_mtime_ns", typ: "INTEGER"},
			{name: "generated_at_ns", typ: "INTEGER"},
			{name: "updated_at_ns", typ: "INTEGER", notNull: true},
		},
		foreignKey: true,
	},
	"poll_state": {
		columns: []columnSpec{
			{name: "library_id", typ: "TEXT", notNull: true, primaryKey: 1},
			{name: "book_id", typ: "TEXT", notNull: true, primaryKey: 2},
			{name: "observation_fingerprint", typ: "TEXT", notNull: true},
			{name: "applied_fingerprint", typ: "TEXT"},
			{name: "status", typ: "TEXT", notNull: true},
			{name: "attempt_count", typ: "INTEGER", notNull: true, defaultValue: "0"},
			{name: "next_attempt_at_ns", typ: "INTEGER"},
			{name: "error_code", typ: "TEXT"},
			{name: "last_seen_at_ns", typ: "INTEGER", notNull: true},
			{name: "updated_at_ns", typ: "INTEGER", notNull: true},
		},
		requiredSQLParts: []string{
			"check (status in ('current', 'pending', 'retry', 'failed'))",
			"check (attempt_count >= 0)",
			"check (error_code is null or length(error_code) between 1 and 64)",
		},
	},
}

type columnSpec struct {
	name         string
	typ          string
	notNull      bool
	primaryKey   int
	defaultValue string
}

type tableSpec struct {
	columns          []columnSpec
	foreignKey       bool
	requiredSQLParts []string
}

const (
	PollStatusCurrent = "current"
	PollStatusPending = "pending"
	PollStatusRetry   = "retry"
	PollStatusFailed  = "failed"

	// MaxPollErrorCodeLength prevents implementation details or unbounded error
	// text from becoming durable poll-state data.
	MaxPollErrorCodeLength = 64
)

// ErrPollObservationChanged indicates that a scheduler attempted to update a
// poll row after a newer observation had replaced the one it read.
var ErrPollObservationChanged = errors.New("poll observation changed")

// BookState is the canonical source checkpoint and the last completed sync.
type BookState struct {
	LibraryID           string
	BookID              string
	MainFormat          string
	CanonicalFormat     string
	CanonicalFileID     string
	CanonicalFileName   string
	CanonicalSHA256     string
	MetadataFingerprint string
	CanonicalMTime      time.Time
	TrustedMTime        bool
	LastSuccessfulSync  time.Time
	UpdatedAt           time.Time
}

// DerivedState records a derivative only after Grimmory confirmed its upload.
type DerivedState struct {
	LibraryID             string
	BookID                string
	Format                string
	GrimmoryFileID        string
	SourceSHA256          string
	OutputSHA256          string
	GenerationFingerprint string
	TrustedMTime          time.Time
	HasMTime              bool
	GeneratedAt           time.Time
	UpdatedAt             time.Time
}

// PollState is the durable scheduler checkpoint for one book. A zero
// AppliedFingerprint means that the latest observation has not completed, and
// a zero NextAttemptAt means work is immediately due when the state is retry.
type PollState struct {
	LibraryID              string
	BookID                 string
	ObservationFingerprint string
	AppliedFingerprint     string
	Status                 string
	AttemptCount           int
	NextAttemptAt          time.Time
	ErrorCode              string
	LastSeenAt             time.Time
	UpdatedAt              time.Time
}

// Store is a SQLite-backed checkpoint store. A single connection makes the
// write ordering deterministic while WAL still permits readers during writes.
type Store struct {
	db *sql.DB
}

// Open creates or opens state.db below dataDir and applies the required SQLite
// safety pragmas and schema.
func Open(dataDir string, busyTimeout time.Duration) (*Store, error) {
	if dataDir == "" {
		return nil, errors.New("state data directory is empty")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create state data directory: %w", err)
	}
	dbPath := filepath.Join(dataDir, "state.db")
	if info, err := os.Lstat(dbPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state database must not be a symlink")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect state database: %w", err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.configure(busyTimeout); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := os.Chmod(dbPath, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("restrict state database: %w", err)
	}
	return store, nil
}

// OpenDB is useful for tests that already have a SQLite database. Production
// code should use Open so state.db placement is consistent.
func OpenDB(db *sql.DB, busyTimeout time.Duration) (*Store, error) {
	if db == nil {
		return nil, errors.New("state database is nil")
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.configure(busyTimeout); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) configure(busyTimeout time.Duration) error {
	if busyTimeout < 0 {
		busyTimeout = 0
	}
	busyMillis := busyTimeout.Milliseconds()
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		fmt.Sprintf("PRAGMA busy_timeout=%d", busyMillis),
	} {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("configure state database: %w", err)
		}
	}
	if err := s.migrate(); err != nil {
		return err
	}
	return nil
}

func (s *Store) migrate() error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin state schema migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	incompatible, err := hasIncompatibleStateTables(tx)
	if err != nil {
		return fmt.Errorf("inspect state schema: %w", err)
	}
	if incompatible {
		// The old tables have no library owner. Dropping them in the same
		// transaction as the rebuild means a failed migration leaves the old
		// database untouched rather than leaving a half-reset state store.
		for _, table := range []string{"derived", "poll_state", "book_state"} {
			if err := dropStateObject(tx, table); err != nil {
				return fmt.Errorf("reset state table %s: %w", table, err)
			}
		}
	}
	// *_v2 tables were produced by an intermediate build. They are not part
	// of this release and must not remain as alternate sources of state.
	for _, table := range []string{"derived_v2", "poll_state_v2", "book_state_v2"} {
		if err := dropStateObject(tx, table); err != nil {
			return fmt.Errorf("remove alternate state table %s: %w", table, err)
		}
	}
	if _, err := tx.Exec(schema); err != nil {
		return fmt.Errorf("create state schema: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state schema migration: %w", err)
	}
	return nil
}

func hasIncompatibleStateTables(tx *sql.Tx) (bool, error) {
	for _, table := range []string{"book_state", "derived", "poll_state"} {
		spec := stateTableSpecs[table]
		exists, err := stateObjectExists(tx, table)
		if err != nil {
			return false, err
		}
		if !exists {
			continue
		}
		compatible, err := compatibleStateTable(tx, table, spec)
		if err != nil {
			return false, err
		}
		if !compatible {
			return true, nil
		}
	}
	return false, nil
}

func stateObjectExists(tx *sql.Tx, name string) (bool, error) {
	var objectType string
	err := tx.QueryRow(`SELECT type FROM sqlite_master WHERE lower(name) = lower(?)`, name).Scan(&objectType)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return objectType != "", nil
}

func compatibleStateTable(tx *sql.Tx, name string, spec tableSpec) (bool, error) {
	var objectType string
	if err := tx.QueryRow(`SELECT type FROM sqlite_master WHERE lower(name) = lower(?)`, name).Scan(&objectType); err != nil {
		return false, err
	}
	if objectType != "table" {
		return false, nil
	}

	rows, err := tx.Query(`PRAGMA table_info("` + name + `")`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var cid, notNull, primaryKey int
		var columnName, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if index >= len(spec.columns) {
			return false, nil
		}
		expected := spec.columns[index]
		if columnName != expected.name || !strings.EqualFold(columnType, expected.typ) || (notNull != 0) != expected.notNull || primaryKey != expected.primaryKey {
			return false, nil
		}
		if expected.defaultValue != "" && (defaultValue == nil || strings.TrimSpace(fmt.Sprint(defaultValue)) != expected.defaultValue) {
			return false, nil
		}
		index++
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if index != len(spec.columns) {
		return false, nil
	}
	if len(spec.requiredSQLParts) > 0 {
		var createSQL sql.NullString
		if err := tx.QueryRow(`SELECT sql FROM sqlite_master WHERE lower(name) = lower(?) AND type = 'table'`, name).Scan(&createSQL); err != nil {
			return false, err
		}
		if !createSQL.Valid {
			return false, nil
		}
		sqlText := strings.ToLower(strings.Join(strings.Fields(createSQL.String), " "))
		for _, part := range spec.requiredSQLParts {
			if !strings.Contains(sqlText, part) {
				return false, nil
			}
		}
	}
	if spec.foreignKey {
		return hasDerivedForeignKey(tx)
	}
	return true, nil
}

func hasDerivedForeignKey(tx *sql.Tx) (bool, error) {
	rows, err := tx.Query(`PRAGMA foreign_key_list("derived")`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	refs := make(map[string]string, 2)
	foreignKeyID := -1
	sequences := map[string]int{"library_id": 0, "book_id": 1}
	rowCount := 0
	for rows.Next() {
		var id, sequence int
		var tableName, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &sequence, &tableName, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return false, err
		}
		rowCount++
		if rowCount > 2 || tableName != "book_state" || onDelete != "CASCADE" || from == "" || to == "" {
			return false, nil
		}
		if foreignKeyID >= 0 && id != foreignKeyID {
			return false, nil
		}
		foreignKeyID = id
		if expectedSequence, ok := sequences[from]; !ok || sequence != expectedSequence {
			return false, nil
		}
		if _, exists := refs[from]; exists {
			return false, nil
		}
		refs[from] = to
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return rowCount == 2 && len(refs) == 2 && refs["library_id"] == "library_id" && refs["book_id"] == "book_id", nil
}

func dropStateObject(tx *sql.Tx, name string) error {
	var objectType string
	err := tx.QueryRow(`SELECT type FROM sqlite_master WHERE lower(name) = lower(?)`, name).Scan(&objectType)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	statement := "DROP TABLE "
	switch objectType {
	case "view":
		statement = "DROP VIEW "
	case "index":
		statement = "DROP INDEX "
	case "trigger":
		statement = "DROP TRIGGER "
	case "table":
	default:
		return fmt.Errorf("unsupported SQLite object type %q", objectType)
	}
	_, err = tx.Exec(statement + `"` + name + `"`)
	return err
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Get returns the checkpoint for one book in one library.
func (s *Store) Get(ctx context.Context, libraryID, bookID string) (BookState, map[string]DerivedState, error) {
	if s == nil || s.db == nil {
		return BookState{}, nil, errors.New("state store is not initialized")
	}
	if err := validateStateScope(libraryID, bookID); err != nil {
		return BookState{}, nil, err
	}
	var result BookState
	var canonicalMTime, lastSuccessfulSync, updatedAt sql.NullInt64
	var canonicalFileID, canonicalFileName, metadataFingerprint sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT library_id, book_id, main_format, canonical_format, canonical_file_id, canonical_file_name, canonical_sha256, metadata_fingerprint, canonical_mtime_ns, last_successful_sync_ns, updated_at_ns FROM book_state WHERE library_id = ? AND book_id = ?`, libraryID, bookID).Scan(
		&result.LibraryID, &result.BookID, &result.MainFormat, &result.CanonicalFormat, &canonicalFileID, &canonicalFileName, &result.CanonicalSHA256, &metadataFingerprint, &canonicalMTime, &lastSuccessfulSync, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return BookState{}, make(map[string]DerivedState), nil
	}
	if err != nil {
		return BookState{}, nil, fmt.Errorf("read book state: %w", err)
	}
	result.CanonicalFileID, result.CanonicalFileName, result.MetadataFingerprint = canonicalFileID.String, canonicalFileName.String, metadataFingerprint.String
	result.CanonicalMTime, result.TrustedMTime = fromNS(canonicalMTime)
	result.LastSuccessfulSync, _ = fromNS(lastSuccessfulSync)
	result.UpdatedAt, _ = fromNS(updatedAt)

	rows, err := s.db.QueryContext(ctx, `SELECT library_id, book_id, format, grimmory_file_id, source_sha256, output_sha256, generation_fingerprint, trusted_mtime_ns, generated_at_ns, updated_at_ns FROM derived WHERE library_id = ? AND book_id = ?`, libraryID, bookID)
	if err != nil {
		return BookState{}, nil, fmt.Errorf("read derived state: %w", err)
	}
	defer rows.Close()
	derived := make(map[string]DerivedState)
	for rows.Next() {
		var value DerivedState
		var mtime, derivedUpdated sql.NullInt64
		var generatedAt sql.NullInt64
		var grimmoryFileID, generationFingerprint sql.NullString
		if err := rows.Scan(&value.LibraryID, &value.BookID, &value.Format, &grimmoryFileID, &value.SourceSHA256, &value.OutputSHA256, &generationFingerprint, &mtime, &generatedAt, &derivedUpdated); err != nil {
			return BookState{}, nil, fmt.Errorf("scan derived state: %w", err)
		}
		value.GrimmoryFileID = grimmoryFileID.String
		value.GenerationFingerprint = generationFingerprint.String
		value.TrustedMTime, value.HasMTime = fromNS(mtime)
		value.GeneratedAt, _ = fromNS(generatedAt)
		value.UpdatedAt, _ = fromNS(derivedUpdated)
		derived[value.Format] = value
	}
	if err := rows.Err(); err != nil {
		return BookState{}, nil, fmt.Errorf("read derived state: %w", err)
	}
	return result, derived, nil
}

// The explicit Scoped spellings are kept as harmless source aliases for
// consumers migrating to the library-aware signatures. They still require a
// library ID and never infer one.
func (s *Store) GetScoped(ctx context.Context, libraryID, bookID string) (BookState, map[string]DerivedState, error) {
	return s.Get(ctx, libraryID, bookID)
}

// SetBook records the canonical source. It intentionally does not delete
// derived rows: a failed or partial reconciliation must preserve old evidence.
func (s *Store) SetBook(ctx context.Context, value BookState) error {
	if value.LibraryID == "" {
		return errors.New("library ID is empty")
	}
	if s == nil || s.db == nil {
		return errors.New("state store is not initialized")
	}
	if err := bindBookScope(value.LibraryID, &value); err != nil {
		return err
	}
	if value.UpdatedAt.IsZero() {
		value.UpdatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO book_state (library_id, book_id, main_format, canonical_format, canonical_file_id, canonical_file_name, canonical_sha256, metadata_fingerprint, canonical_mtime_ns, last_successful_sync_ns, updated_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(library_id, book_id) DO UPDATE SET
 main_format=excluded.main_format,
 canonical_format=excluded.canonical_format,
 canonical_file_id=excluded.canonical_file_id,
 canonical_file_name=excluded.canonical_file_name,
 canonical_sha256=excluded.canonical_sha256,
 metadata_fingerprint=excluded.metadata_fingerprint,
 canonical_mtime_ns=excluded.canonical_mtime_ns,
 last_successful_sync_ns=excluded.last_successful_sync_ns,
 updated_at_ns=excluded.updated_at_ns`,
		value.LibraryID, value.BookID, value.MainFormat, value.CanonicalFormat, value.CanonicalFileID, value.CanonicalFileName, value.CanonicalSHA256, nullableString(value.MetadataFingerprint), toNS(value.CanonicalMTime, value.TrustedMTime), toNS(value.LastSuccessfulSync, !value.LastSuccessfulSync.IsZero()), value.UpdatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("write book state: %w", err)
	}
	return nil
}

func (s *Store) SetBookScoped(ctx context.Context, libraryID string, value BookState) error {
	if value.LibraryID != "" && value.LibraryID != libraryID {
		return errors.New("book state library ID does not match target")
	}
	value.LibraryID = libraryID
	return s.SetBook(ctx, value)
}

// SetDerived updates one derivative checkpoint in a transaction. No caller
// should invoke it until the post-upload GET has found the requested format.
func (s *Store) SetDerived(ctx context.Context, value DerivedState) error {
	if value.LibraryID == "" {
		return errors.New("library ID is empty")
	}
	if s == nil || s.db == nil {
		return errors.New("state store is not initialized")
	}
	if err := bindDerivedScope(value.LibraryID, &value); err != nil {
		return err
	}
	if value.UpdatedAt.IsZero() {
		value.UpdatedAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin derived state transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO derived (library_id, book_id, format, grimmory_file_id, source_sha256, output_sha256, generation_fingerprint, trusted_mtime_ns, generated_at_ns, updated_at_ns)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(library_id, book_id, format) DO UPDATE SET
 grimmory_file_id=excluded.grimmory_file_id,
 source_sha256=excluded.source_sha256,
 output_sha256=excluded.output_sha256,
 generation_fingerprint=excluded.generation_fingerprint,
 trusted_mtime_ns=excluded.trusted_mtime_ns,
 generated_at_ns=excluded.generated_at_ns,
 updated_at_ns=excluded.updated_at_ns`,
		value.LibraryID, value.BookID, value.Format, nullableString(value.GrimmoryFileID), value.SourceSHA256, value.OutputSHA256, nullableString(value.GenerationFingerprint), toNS(value.TrustedMTime, value.HasMTime), toNS(value.GeneratedAt, !value.GeneratedAt.IsZero()), value.UpdatedAt.UnixNano()); err != nil {
		return fmt.Errorf("write derived state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit derived state: %w", err)
	}
	return nil
}

func (s *Store) SetDerivedScoped(ctx context.Context, libraryID string, value DerivedState) error {
	if value.LibraryID != "" && value.LibraryID != libraryID {
		return errors.New("derived state library ID does not match target")
	}
	value.LibraryID = libraryID
	return s.SetDerived(ctx, value)
}

// UpsertPollObservation records the latest observation for an explicit
// library/book pair. Repeated observations retain their status, retry schedule,
// and attempt count. A new fingerprint starts pending work immediately and
// resets those fields, while preserving the last successfully applied
// fingerprint.
func (s *Store) UpsertPollObservation(ctx context.Context, libraryID, bookID, fingerprint string, seenAt time.Time) (PollState, error) {
	if s == nil || s.db == nil {
		return PollState{}, errors.New("state store is not initialized")
	}
	if err := validateStateScope(libraryID, bookID); err != nil {
		return PollState{}, err
	}
	if fingerprint == "" {
		return PollState{}, errors.New("poll observation fingerprint is empty")
	}
	seenAt = stateTime(seenAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PollState{}, fmt.Errorf("begin poll state transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO poll_state (library_id, book_id, observation_fingerprint, applied_fingerprint, status, attempt_count, next_attempt_at_ns, error_code, last_seen_at_ns, updated_at_ns)
VALUES (?, ?, ?, NULL, ?, 0, ?, NULL, ?, ?)
ON CONFLICT(library_id, book_id) DO UPDATE SET
 observation_fingerprint=excluded.observation_fingerprint,
 status=CASE
   WHEN poll_state.observation_fingerprint = excluded.observation_fingerprint THEN poll_state.status
   WHEN poll_state.applied_fingerprint = excluded.observation_fingerprint THEN ?
   ELSE ?
 END,
 attempt_count=CASE WHEN poll_state.observation_fingerprint = excluded.observation_fingerprint THEN poll_state.attempt_count ELSE 0 END,
 next_attempt_at_ns=CASE
   WHEN poll_state.observation_fingerprint = excluded.observation_fingerprint THEN poll_state.next_attempt_at_ns
   WHEN poll_state.applied_fingerprint = excluded.observation_fingerprint THEN NULL
   ELSE excluded.next_attempt_at_ns
 END,
 error_code=CASE WHEN poll_state.observation_fingerprint = excluded.observation_fingerprint THEN poll_state.error_code ELSE NULL END,
 last_seen_at_ns=excluded.last_seen_at_ns,
 updated_at_ns=excluded.updated_at_ns`,
		libraryID, bookID, fingerprint, PollStatusPending, seenAt.UnixNano(), seenAt.UnixNano(), seenAt.UnixNano(), PollStatusCurrent, PollStatusPending); err != nil {
		return PollState{}, fmt.Errorf("write poll observation: %w", err)
	}
	value, err := scanPollState(tx.QueryRowContext(ctx, `SELECT library_id, book_id, observation_fingerprint, applied_fingerprint, status, attempt_count, next_attempt_at_ns, error_code, last_seen_at_ns, updated_at_ns FROM poll_state WHERE library_id = ? AND book_id = ?`, libraryID, bookID))
	if err != nil {
		return PollState{}, fmt.Errorf("read poll observation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PollState{}, fmt.Errorf("commit poll observation: %w", err)
	}
	return value, nil
}

func (s *Store) UpsertPollObservationScoped(ctx context.Context, libraryID, bookID, fingerprint string, seenAt time.Time) (PollState, error) {
	return s.UpsertPollObservation(ctx, libraryID, bookID, fingerprint, seenAt)
}

// ListDuePollStates returns pending and retry states for one library
// whose next attempt is ready. A zero limit means no limit; a negative limit is
// rejected. Results are ordered by due time and then book ID for deterministic
// scheduler batches.
func (s *Store) ListDuePollStates(ctx context.Context, libraryID string, now time.Time, limit int) ([]PollState, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("state store is not initialized")
	}
	if libraryID == "" {
		return nil, errors.New("library ID is empty")
	}
	if limit < 0 {
		return nil, errors.New("poll state limit is negative")
	}
	now = stateTime(now)
	query := `SELECT library_id, book_id, observation_fingerprint, applied_fingerprint, status, attempt_count, next_attempt_at_ns, error_code, last_seen_at_ns, updated_at_ns
FROM poll_state
WHERE library_id = ? AND status IN (?, ?) AND (next_attempt_at_ns IS NULL OR next_attempt_at_ns <= ?)
ORDER BY next_attempt_at_ns IS NOT NULL, next_attempt_at_ns, book_id`
	args := []any{libraryID, PollStatusPending, PollStatusRetry, now.UnixNano()}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read due poll state: %w", err)
	}
	defer rows.Close()
	result := make([]PollState, 0)
	for rows.Next() {
		value, err := scanPollState(rows)
		if err != nil {
			return nil, fmt.Errorf("scan due poll state: %w", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read due poll state: %w", err)
	}
	return result, nil
}

func (s *Store) ListDuePollStatesScoped(ctx context.Context, libraryID string, now time.Time, limit int) ([]PollState, error) {
	return s.ListDuePollStates(ctx, libraryID, now, limit)
}

// MarkPollSuccess applies the observation identified by fingerprint for
// one library/book pair. The fingerprint guard prevents a stale scheduler
// worker from marking a newer observation current.
func (s *Store) MarkPollSuccess(ctx context.Context, libraryID, bookID, fingerprint string, updatedAt time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("state store is not initialized")
	}
	if err := validateScopedPollTarget(libraryID, bookID, fingerprint); err != nil {
		return err
	}
	updatedAt = stateTime(updatedAt)
	result, err := s.db.ExecContext(ctx, `
UPDATE poll_state
SET applied_fingerprint = observation_fingerprint,
    status = ?,
    attempt_count = 0,
    next_attempt_at_ns = NULL,
    error_code = NULL,
    updated_at_ns = ?
WHERE library_id = ? AND book_id = ? AND observation_fingerprint = ? AND status IN (?, ?, ?, ?)`,
		PollStatusCurrent, updatedAt.UnixNano(), libraryID, bookID, fingerprint, PollStatusPending, PollStatusRetry, PollStatusFailed, PollStatusCurrent)
	if err != nil {
		return fmt.Errorf("mark poll success: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("check poll success: %w", err)
	} else if changed == 0 {
		return fmt.Errorf("mark poll success: %w", ErrPollObservationChanged)
	}
	return nil
}

func (s *Store) MarkPollSuccessScoped(ctx context.Context, libraryID, bookID, fingerprint string, updatedAt time.Time) error {
	return s.MarkPollSuccess(ctx, libraryID, bookID, fingerprint, updatedAt)
}

// RequeuePollObservation leaves an observation immediately due without
// consuming a conversion attempt. It is used when an operational mutation,
// such as failure-tag maintenance, could not be committed.
func (s *Store) RequeuePollObservation(ctx context.Context, libraryID, bookID, fingerprint, errorCode string, nextAttemptAt, updatedAt time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("state store is not initialized")
	}
	if err := validateScopedPollTarget(libraryID, bookID, fingerprint); err != nil {
		return err
	}
	if err := validatePollErrorCode(errorCode); err != nil {
		return err
	}
	updatedAt = stateTime(updatedAt)
	result, err := s.db.ExecContext(ctx, `
UPDATE poll_state
SET status = ?, next_attempt_at_ns = ?, error_code = ?, updated_at_ns = ?
WHERE library_id = ? AND book_id = ? AND observation_fingerprint = ? AND status IN (?, ?, ?, ?)`,
		PollStatusRetry, toNS(nextAttemptAt, !nextAttemptAt.IsZero()), nullableString(errorCode), updatedAt.UnixNano(), libraryID, bookID, fingerprint, PollStatusPending, PollStatusRetry, PollStatusFailed, PollStatusCurrent)
	if err != nil {
		return fmt.Errorf("requeue poll observation: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("check requeue poll observation: %w", err)
	} else if changed == 0 {
		return fmt.Errorf("requeue poll observation: %w", ErrPollObservationChanged)
	}
	return nil
}

func (s *Store) RequeuePollObservationScoped(ctx context.Context, libraryID, bookID, fingerprint, errorCode string, nextAttemptAt, updatedAt time.Time) error {
	return s.RequeuePollObservation(ctx, libraryID, bookID, fingerprint, errorCode, nextAttemptAt, updatedAt)
}

// RecordPollFailure atomically records one failed attempt. It increments the
// attempt count and selects retry until maxAttempts is reached, then selects
// failed. nextAttemptAt is stored only when the resulting state is retry; a
// zero nextAttemptAt makes that retry immediately due.
// RecordPollFailure atomically records one failed attempt for an
// explicit library/book observation. It increments the attempt count and
// selects retry until maxAttempts is reached, then selects failed.
func (s *Store) RecordPollFailure(ctx context.Context, libraryID, bookID, fingerprint, errorCode string, nextAttemptAt time.Time, maxAttempts int, updatedAt time.Time) (PollState, error) {
	if s == nil || s.db == nil {
		return PollState{}, errors.New("state store is not initialized")
	}
	if err := validateScopedPollTarget(libraryID, bookID, fingerprint); err != nil {
		return PollState{}, err
	}
	if err := validatePollErrorCode(errorCode); err != nil {
		return PollState{}, err
	}
	if maxAttempts <= 0 {
		return PollState{}, errors.New("poll max attempts must be positive")
	}
	updatedAt = stateTime(updatedAt)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PollState{}, fmt.Errorf("begin poll failure transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if result, err := tx.ExecContext(ctx, `
UPDATE poll_state
SET status = CASE WHEN attempt_count + 1 >= ? THEN ? ELSE ? END,
    attempt_count = attempt_count + 1,
    next_attempt_at_ns = CASE WHEN attempt_count + 1 >= ? THEN NULL ELSE ? END,
    error_code = ?,
    updated_at_ns = ?
WHERE library_id = ? AND book_id = ? AND observation_fingerprint = ? AND status IN (?, ?)`,
		maxAttempts, PollStatusFailed, PollStatusRetry, maxAttempts, toNS(nextAttemptAt, !nextAttemptAt.IsZero()), nullableString(errorCode), updatedAt.UnixNano(), libraryID, bookID, fingerprint, PollStatusPending, PollStatusRetry); err != nil {
		return PollState{}, fmt.Errorf("record poll failure: %w", err)
	} else if changed, err := result.RowsAffected(); err != nil {
		return PollState{}, fmt.Errorf("check poll failure: %w", err)
	} else if changed == 0 {
		return PollState{}, fmt.Errorf("record poll failure: %w", ErrPollObservationChanged)
	}
	value, err := scanPollState(tx.QueryRowContext(ctx, `SELECT library_id, book_id, observation_fingerprint, applied_fingerprint, status, attempt_count, next_attempt_at_ns, error_code, last_seen_at_ns, updated_at_ns FROM poll_state WHERE library_id = ? AND book_id = ?`, libraryID, bookID))
	if err != nil {
		return PollState{}, fmt.Errorf("read poll failure state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PollState{}, fmt.Errorf("commit poll failure: %w", err)
	}
	return value, nil
}

func (s *Store) RecordPollFailureScoped(ctx context.Context, libraryID, bookID, fingerprint, errorCode string, nextAttemptAt time.Time, maxAttempts int, updatedAt time.Time) (PollState, error) {
	return s.RecordPollFailure(ctx, libraryID, bookID, fingerprint, errorCode, nextAttemptAt, maxAttempts, updatedAt)
}

// MarkPollFailure is the retry-exhausted convenience form of RecordPollFailure.
func (s *Store) MarkPollFailure(ctx context.Context, libraryID, bookID, fingerprint, errorCode string, updatedAt time.Time) error {
	_, err := s.RecordPollFailure(ctx, libraryID, bookID, fingerprint, errorCode, time.Time{}, 1, updatedAt)
	return err
}

func (s *Store) MarkPollFailureScoped(ctx context.Context, libraryID, bookID, fingerprint, errorCode string, updatedAt time.Time) error {
	return s.MarkPollFailure(ctx, libraryID, bookID, fingerprint, errorCode, updatedAt)
}

func validatePollTarget(bookID, fingerprint string) error {
	if fingerprint == "" {
		return errors.New("poll observation fingerprint is empty")
	}
	if bookID == "" {
		return errors.New("poll book ID is empty")
	}
	return nil
}

func validateStateScope(libraryID, bookID string) error {
	if libraryID == "" {
		return errors.New("library ID is empty")
	}
	if bookID == "" {
		return errors.New("book ID is empty")
	}
	return nil
}

func bindBookScope(libraryID string, value *BookState) error {
	if err := validateStateScope(libraryID, value.BookID); err != nil {
		return err
	}
	if value.LibraryID != "" && value.LibraryID != libraryID {
		return errors.New("book state library ID does not match target")
	}
	value.LibraryID = libraryID
	return nil
}

func bindDerivedScope(libraryID string, value *DerivedState) error {
	if err := validateStateScope(libraryID, value.BookID); err != nil {
		return err
	}
	if value.Format == "" {
		return errors.New("derived format is empty")
	}
	if value.LibraryID != "" && value.LibraryID != libraryID {
		return errors.New("derived state library ID does not match target")
	}
	value.LibraryID = libraryID
	return nil
}

func validateScopedPollTarget(libraryID, bookID, fingerprint string) error {
	if err := validateStateScope(libraryID, bookID); err != nil {
		return err
	}
	if fingerprint == "" {
		return errors.New("poll observation fingerprint is empty")
	}
	return nil
}

func validatePollErrorCode(value string) error {
	if value == "" {
		return nil
	}
	if len(value) > MaxPollErrorCodeLength {
		return fmt.Errorf("poll error code exceeds %d bytes", MaxPollErrorCodeLength)
	}
	for _, char := range []byte(value) {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' {
			continue
		}
		return errors.New("poll error code contains unsafe characters")
	}
	return nil
}

func stateTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

type stateScanner interface {
	Scan(dest ...any) error
}

func scanPollState(scanner stateScanner) (PollState, error) {
	var value PollState
	var appliedFingerprint, errorCode sql.NullString
	var nextAttemptAt, lastSeenAt, updatedAt sql.NullInt64
	if err := scanner.Scan(&value.LibraryID, &value.BookID, &value.ObservationFingerprint, &appliedFingerprint, &value.Status, &value.AttemptCount, &nextAttemptAt, &errorCode, &lastSeenAt, &updatedAt); err != nil {
		return PollState{}, err
	}
	value.AppliedFingerprint = appliedFingerprint.String
	value.ErrorCode = errorCode.String
	value.NextAttemptAt, _ = fromNS(nextAttemptAt)
	value.LastSeenAt, _ = fromNS(lastSeenAt)
	value.UpdatedAt, _ = fromNS(updatedAt)
	return value, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func toNS(value time.Time, trusted bool) any {
	if !trusted || value.IsZero() {
		return nil
	}
	return value.UnixNano()
}

func fromNS(value sql.NullInt64) (time.Time, bool) {
	if !value.Valid {
		return time.Time{}, false
	}
	return time.Unix(0, value.Int64).UTC(), true
}

// Package reconcile contains the stateful, one-book reconciliation operation.
package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"converter/internal/convert"
	"converter/internal/grimmory"
	"converter/internal/logging"
	"converter/internal/state"
)

var (
	ErrInvalidBookID      = errors.New("invalid book ID")
	ErrInvalidLibraryID   = errors.New("invalid library ID")
	ErrNoSource           = errors.New("no configured source format is available")
	ErrVerification       = errors.New("Grimmory upload verification failed")
	ErrPartial            = errors.New("reconciliation completed with failures")
	ErrState              = errors.New("reconciliation state operation failed")
	ErrLibraryNotAllowed  = errors.New("library is not allowed")
	ErrFailureTagMutation = errors.New("failure tag mutation failed")
	ErrAmbiguousFile      = errors.New("Grimmory file inventory is ambiguous")
	ErrPrimaryFile        = errors.New("primary Grimmory file cannot be deleted")
)

// ClassifyError returns a bounded, secret-safe category for operational logs.
// It intentionally does not include the underlying error text.
func ClassifyError(err error) string {
	if err == nil {
		return ""
	}
	var partial *partialError
	if errors.As(err, &partial) && partial != nil && partial.cause != nil {
		return ClassifyError(partial.cause)
	}
	var httpErr *grimmory.HTTPError
	switch {
	case errors.Is(err, grimmory.ErrUnauthorized):
		return "grimmory_authentication_failed"
	case errors.As(err, &httpErr), errors.Is(err, grimmory.ErrNotFound):
		return "remote_http_status"
	case errors.Is(err, grimmory.ErrInvalidResponse), errors.Is(err, grimmory.ErrResponseTooBig), errors.Is(err, ErrVerification):
		return "invalid_response"
	case errors.Is(err, ErrState):
		return "state"
	case errors.Is(err, ErrFailureTagMutation):
		return "failure_tag_mutation"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrInvalidBookID):
		return "validation"
	case errors.Is(err, ErrInvalidLibraryID), errors.Is(err, ErrLibraryNotAllowed):
		return "validation"
	case errors.Is(err, ErrNoSource):
		return "no_source"
	case errors.Is(err, ErrPartial):
		return "reconciliation"
	default:
		return "internal"
	}
}

type partialError struct{ cause error }

func (e *partialError) Error() string { return ErrPartial.Error() }

func (e *partialError) Unwrap() error { return ErrPartial }

func (e *partialError) Cause() error { return e.cause }

func newPartialError(cause error) error {
	if cause == nil {
		return ErrPartial
	}
	return &partialError{cause: cause}
}

// Remote is deliberately small so reconciliation policy can be tested with a
// fake without a Grimmory server.
type Remote interface {
	GetLibrary(context.Context, string) (grimmory.Library, error)
	GetLibraryBook(context.Context, string, string) (grimmory.Book, error)
	DownloadContentScoped(context.Context, grimmory.BookReference, string, io.Writer) (int64, string, error)
	UploadFileScoped(context.Context, grimmory.BookReference, string, string) error
	UploadFileNamedScoped(context.Context, grimmory.BookReference, string, string, string) error
	DeleteFileScoped(context.Context, grimmory.BookReference, string) error
}

type Converter interface {
	Convert(context.Context, string, string, string, string) (string, error)
}

type FailureTagger interface {
	AddBookTagScoped(context.Context, grimmory.BookReference, string) error
	RemoveBookTagScoped(context.Context, grimmory.BookReference, string) error
}

// BookLocker lets polling perform its poll-state transition and operational
// tag mutation under the same keyed lock as a manual reconciliation.
type BookLocker interface {
	WithBookLock(context.Context, string, string, func(context.Context) error) error
}

type FailureTagSetter interface {
	SetFailureTag(context.Context, string, string, bool) error
}

type Store interface {
	Get(context.Context, string, string) (state.BookState, map[string]state.DerivedState, error)
	SetBook(context.Context, state.BookState) error
	SetDerived(context.Context, state.DerivedState) error
}

type Options struct {
	Client              Remote
	Store               Store
	Converter           Converter
	OutputFormats       []string
	SupportedInputs     []string
	LibraryIDs          []string
	MaxConcurrentBooks  int
	FailedProcessingTag string
	MaxFileBytes        int64
	ConversionTimeout   time.Duration
	TempRoot            string
	Logger              *logging.Logger
}

type Service struct {
	client            Remote
	store             Store
	converter         Converter
	outputs           []string
	inputs            []string
	limiter           chan struct{}
	allowedLibraries  map[string]struct{}
	maxFileBytes      int64
	conversionTimeout time.Duration
	tempRoot          string
	logger            *logging.Logger
	failedTag         string
	locksMu           sync.Mutex
	locks             map[string]*bookLock
}

// LibraryPolicy is resolved from Grimmory for every sync. It is immutable for
// the lifetime of that operation, so a policy change cannot mix formats within
// one reconciliation.
type LibraryPolicy struct {
	LibraryID       string
	MainFormat      string
	FallbackFormats []string
	OutputFormats   []string
}

func ResolveLibraryPolicy(library grimmory.Library, outputs, supported []string) (LibraryPolicy, error) {
	priority := normalizeFormats(library.FormatPriority, "", false)
	if library.ID == "" || len(priority) == 0 {
		return LibraryPolicy{}, errors.New("library format priority is empty")
	}
	main := priority[0]
	supported = normalizeFormats(supported, "", false)
	if !containsFormat(supported, main) {
		return LibraryPolicy{}, fmt.Errorf("library main format %q is not supported", main)
	}
	if len(library.AllowedFormats) > 0 && !containsFormat(normalizeFormats(library.AllowedFormats, "", false), main) {
		return LibraryPolicy{}, fmt.Errorf("library main format %q is not allowed", main)
	}
	fallback := make([]string, 0, len(priority))
	for _, format := range priority[1:] {
		if containsFormat(supported, format) {
			fallback = append(fallback, format)
		}
	}
	configuredOutputs := normalizeFormats(outputs, "", false)
	allowed := normalizeFormats(library.AllowedFormats, "", false)
	resultOutputs := make([]string, 0, len(configuredOutputs))
	for _, format := range configuredOutputs {
		if format == main {
			continue
		}
		if len(allowed) == 0 || containsFormat(allowed, format) {
			resultOutputs = append(resultOutputs, format)
		}
	}
	return LibraryPolicy{LibraryID: library.ID, MainFormat: main, FallbackFormats: fallback, OutputFormats: resultOutputs}, nil
}

func containsFormat(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

type bookLock struct {
	mu   sync.Mutex
	refs int
}

type bookLockContextKey struct{}

type bookLockContext struct {
	service   *Service
	libraryID string
	bookID    string
}

func New(options Options) *Service {
	allowed := make(map[string]struct{}, len(options.LibraryIDs))
	for _, libraryID := range options.LibraryIDs {
		if ValidLibraryID(libraryID) {
			allowed[libraryID] = struct{}{}
		}
	}
	return &Service{
		client: options.Client, store: options.Store, converter: options.Converter,
		outputs: normalizeFormats(options.OutputFormats, "", false),
		inputs:  normalizeFormats(options.SupportedInputs, "", false), maxFileBytes: options.MaxFileBytes,
		conversionTimeout: options.ConversionTimeout, tempRoot: options.TempRoot, logger: options.Logger, failedTag: strings.TrimSpace(options.FailedProcessingTag),
		locks: make(map[string]*bookLock), limiter: make(chan struct{}, options.MaxConcurrentBooks), allowedLibraries: allowed,
	}
}

// WithBookLock runs fn while holding the service-wide concurrency slot and the
// keyed library/book lock. The context passed to fn carries the lock scope, so
// a nested Sync call reuses both locks instead of deadlocking.
func (s *Service) WithBookLock(ctx context.Context, libraryID, bookID string, fn func(context.Context) error) error {
	if s == nil || s.client == nil || s.store == nil || s.limiter == nil || s.locks == nil {
		return errors.New("reconciliation service is not initialized")
	}
	if !ValidLibraryID(libraryID) {
		return ErrInvalidLibraryID
	}
	if !ValidBookID(bookID) {
		return ErrInvalidBookID
	}
	if _, allowed := s.allowedLibraries[libraryID]; !allowed {
		return ErrLibraryNotAllowed
	}
	if fn == nil {
		return errors.New("book lock callback is nil")
	}
	if s.bookLockHeld(ctx, libraryID, bookID) {
		return fn(ctx)
	}
	select {
	case s.limiter <- struct{}{}:
		defer func() { <-s.limiter }()
	case <-ctx.Done():
		return ctx.Err()
	}
	lock, release := s.bookLock(libraryID, bookID)
	lock.Lock()
	defer func() {
		lock.Unlock()
		release()
	}()
	lockedContext := context.WithValue(ctx, bookLockContextKey{}, bookLockContext{service: s, libraryID: libraryID, bookID: bookID})
	return fn(lockedContext)
}

func (s *Service) bookLockHeld(ctx context.Context, libraryID, bookID string) bool {
	value, ok := ctx.Value(bookLockContextKey{}).(bookLockContext)
	return ok && value.service == s && value.libraryID == libraryID && value.bookID == bookID
}

func (s *Service) Formats() (string, []string, []string) {
	return "", copyStrings(s.inputs), copyStrings(s.outputs)
}

func (s *Service) resolvePolicy(ctx context.Context, libraryID string) (LibraryPolicy, error) {
	library, err := s.client.GetLibrary(ctx, libraryID)
	if err != nil {
		return LibraryPolicy{}, err
	}
	if library.ID != libraryID {
		return LibraryPolicy{}, errors.New("library policy identity mismatch")
	}
	return ResolveLibraryPolicy(library, s.outputs, s.inputs)
}

// Result is intentionally JSON-ready. Error contains a stable code, never a
// Grimmory response body, credential, command line, or temporary path.
type Result struct {
	Status      string       `json:"status"`
	LibraryID   string       `json:"libraryId"`
	BookID      string       `json:"bookId"`
	DryRun      bool         `json:"dryRun"`
	Force       bool         `json:"force"`
	Main        ItemResult   `json:"main"`
	Derivatives []ItemResult `json:"derivatives"`
	Error       string       `json:"error,omitempty"`
}

type ItemResult struct {
	Format       string `json:"format"`
	SourceFormat string `json:"sourceFormat,omitempty"`
	Action       string `json:"action"`
	Reason       string `json:"reason,omitempty"`
	Status       string `json:"status"`
	Error        string `json:"error,omitempty"`
}

type SyncOptions struct {
	DryRun bool
	Force  bool
}

func (s *Service) Sync(ctx context.Context, libraryID, bookID string, options SyncOptions) (Result, error) {
	result := Result{Status: "failed", LibraryID: libraryID, BookID: bookID, DryRun: options.DryRun, Force: options.Force}
	if !ValidLibraryID(libraryID) {
		result.Error = "invalid_library_id"
		return result, ErrInvalidLibraryID
	}
	if !ValidBookID(bookID) {
		result.Error = "invalid_book_id"
		return result, ErrInvalidBookID
	}
	if s == nil || s.client == nil || s.store == nil || s.converter == nil || len(s.inputs) == 0 {
		result.Error = "service_not_initialized"
		return result, errors.New("reconciliation service is not initialized")
	}
	if _, allowed := s.allowedLibraries[libraryID]; !allowed {
		result.Error = "library_not_allowed"
		return result, ErrLibraryNotAllowed
	}
	locked := s.bookLockHeld(ctx, libraryID, bookID)
	if !locked {
		select {
		case s.limiter <- struct{}{}:
			defer func() { <-s.limiter }()
		case <-ctx.Done():
			result.Error = "canceled"
			return result, ctx.Err()
		}
	}
	policy, err := s.resolvePolicy(ctx, libraryID)
	if err != nil {
		result.Error = "library_policy_failed"
		return result, err
	}
	if !locked {
		lock, release := s.bookLock(libraryID, bookID)
		lock.Lock()
		defer func() {
			lock.Unlock()
			release()
		}()
		ctx = context.WithValue(ctx, bookLockContextKey{}, bookLockContext{service: s, libraryID: libraryID, bookID: bookID})
	}

	book, err := s.client.GetLibraryBook(ctx, libraryID, bookID)
	if err != nil {
		result.Error = codeForError(err, "get_book_failed")
		return result, err
	}
	if book.ID != bookID {
		result.Error = "get_book_failed"
		return result, fmt.Errorf("%w: book identity mismatch", grimmory.ErrInvalidResponse)
	}
	if book.LibraryID != libraryID {
		result.Error = "get_book_failed"
		return result, grimmory.ErrBookNotInLibrary
	}
	savedBook, savedDerived, err := s.store.Get(ctx, libraryID, bookID)
	if err != nil {
		result.Error = "state_read_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	mainFile, hasMain := FindFile(book.Files, policy.MainFormat)
	if !hasMain {
		source, ok := SelectSource(book.Files, policy.MainFormat, policy.FallbackFormats)
		if !ok {
			result.Main = ItemResult{Format: policy.MainFormat, Action: "create", Status: "blocked", Error: "no_source"}
			result.Error = "no_source"
			for _, format := range policy.OutputFormats {
				result.Derivatives = append(result.Derivatives, ItemResult{Format: format, Action: "create", Status: "blocked", Error: "main_unavailable"})
			}
			return result, ErrNoSource
		}
		result.Main = ItemResult{Format: policy.MainFormat, SourceFormat: source.Format, Action: "create", Status: "planned", Reason: "main_missing"}
		if options.DryRun {
			for _, format := range policy.OutputFormats {
				action := "create"
				if _, exists := FindFile(book.Files, format); exists {
					action = "rebuild"
				}
				result.Derivatives = append(result.Derivatives, ItemResult{Format: format, SourceFormat: policy.MainFormat, Action: action, Status: "planned", Reason: "main_would_be_created"})
			}
			result.Status = "dry_run"
			return result, nil
		}
		return s.createMissingMain(ctx, libraryID, bookID, policy, source, savedBook, options, result)
	}

	result.Main = ItemResult{Format: policy.MainFormat, Action: "unchanged", Status: "existing", Reason: "main_present"}
	canonicalSHA := savedBook.CanonicalSHA256
	// A deployment-supplied canonical hash is authoritative even for dry runs.
	if options.DryRun && mainFile.SHA256 != "" {
		canonicalSHA = mainFile.SHA256
	}
	canonicalPath := ""
	canonicalName := mainFile.Name
	if canonicalName == "" {
		canonicalName = desiredOutputName("", policy.MainFormat)
	}
	canonicalMTime := mainFile.MTime
	canonicalTrustedMTime := mainFile.TrustedMTime
	var workspace string
	var canonicalState state.BookState
	if !options.DryRun {
		workspace, err = s.newWorkspace()
		if err != nil {
			result.Error = "workspace_failed"
			return result, err
		}
		defer os.RemoveAll(workspace)
		canonicalPath, canonicalSHA, err = s.download(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, policy.MainFormat, workspace, "canonical")
		if err != nil {
			result.Error = "download_main_failed"
			return result, err
		}
		if mainFile.SHA256 != "" && !strings.EqualFold(mainFile.SHA256, canonicalSHA) {
			result.Error = "canonical_hash_mismatch"
			return result, ErrVerification
		}
	}
	if !canonicalTrustedMTime && savedBook.TrustedMTime {
		canonicalMTime, canonicalTrustedMTime = savedBook.CanonicalMTime, true
	}
	if !options.DryRun {
		canonicalState = state.BookState{LibraryID: libraryID, BookID: bookID, MainFormat: policy.MainFormat, CanonicalFormat: policy.MainFormat, CanonicalFileID: mainFile.ID, CanonicalFileName: canonicalName, CanonicalSHA256: canonicalSHA, MetadataFingerprint: mainFile.MetadataFingerprint, CanonicalMTime: canonicalMTime, TrustedMTime: canonicalTrustedMTime, LastSuccessfulSync: savedBook.LastSuccessfulSync, UpdatedAt: time.Now().UTC()}
		if err := s.store.SetBook(ctx, canonicalState); err != nil {
			result.Error = "state_write_failed"
			return result, fmt.Errorf("%w: %v", ErrState, err)
		}
	}
	generationFingerprints := DesiredGenerationFingerprints(book, canonicalSHA, canonicalName, policy.OutputFormats)
	plans := PlanDerivatives(book.Files, policy.OutputFormats, policy.MainFormat, canonicalSHA, savedDerived, canonicalMTime, canonicalTrustedMTime, false, options.Force, completeBookState(savedBook), generationFingerprints)
	if options.DryRun {
		for _, plan := range plans {
			result.Derivatives = append(result.Derivatives, ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Status: "planned", Reason: plan.Reason})
		}
		result.Status = "dry_run"
		return result, nil
	}
	failed := false
	var firstFailure error
	for _, plan := range plans {
		item := ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Reason: plan.Reason}
		if plan.Action == "unchanged" {
			item.Status = "unchanged"
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		item.Status = "failed"
		outputPath, conversionErr := s.convert(ctx, canonicalPath, policy.MainFormat, plan.Format, workspace)
		var outputSHA string
		if conversionErr == nil {
			if !withinWorkspace(workspace, outputPath) {
				conversionErr = errors.New("converter output escaped workspace")
			} else {
				outputSHA, _, conversionErr = convert.HashFile(outputPath, s.maxFileBytes)
			}
		}
		before, hadBefore := FindFile(book.Files, plan.Format)
		if conversionErr == nil && plan.Action == "rebuild" {
			before, hadBefore, conversionErr = s.prepareDerivativeReplacement(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, plan.Format, policy.MainFormat, mainFile.ID)
		}
		if conversionErr == nil {
			conversionErr = s.upload(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, plan.Format, outputPath, canonicalName)
		}
		if conversionErr == nil {
			verifiedBook, verifyErr := s.client.GetLibraryBook(ctx, libraryID, bookID)
			if verifyErr != nil {
				conversionErr = verifyErr
			} else if verifiedFile, ok := findUploadedFile(verifiedBook.Files, plan.Format, desiredOutputName(canonicalName, plan.Format), outputSHA); !ok {
				conversionErr = ErrVerification
			} else if conversionErr = verifyUploadedFile(before, hadBefore, verifiedFile, outputSHA); conversionErr != nil {
			} else {
				if err := s.store.SetDerived(ctx, state.DerivedState{LibraryID: libraryID, BookID: bookID, Format: plan.Format, GrimmoryFileID: verifiedFile.ID, SourceSHA256: canonicalSHA, OutputSHA256: outputSHA, GenerationFingerprint: plan.GenerationFingerprint, TrustedMTime: verifiedFile.MTime, HasMTime: verifiedFile.TrustedMTime, GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
					conversionErr = fmt.Errorf("%w: %v", ErrState, err)
				} else {
					item.Status = "uploaded"
				}
			}
		}
		if withinWorkspace(workspace, outputPath) {
			_ = os.Remove(outputPath)
		}
		if conversionErr != nil {
			failed = true
			if firstFailure == nil {
				firstFailure = conversionErr
			}
			item.Error = codeForError(conversionErr, "derivative_failed")
		}
		result.Derivatives = append(result.Derivatives, item)
	}
	if failed {
		result.Status = "partial"
		result.Error = "derivative_failed"
		return result, newPartialError(firstFailure)
	}
	canonicalState.LastSuccessfulSync = time.Now().UTC()
	canonicalState.UpdatedAt = time.Now().UTC()
	if err := s.store.SetBook(ctx, canonicalState); err != nil {
		result.Status, result.Error = "partial", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	result.Status = "completed"
	if err := s.SetFailureTag(ctx, libraryID, bookID, false); err != nil {
		result.Status, result.Error = "partial", "failure_tag_failed"
		return result, fmt.Errorf("%w: %v", ErrFailureTagMutation, err)
	}
	return result, nil
}

func (s *Service) createMissingMain(ctx context.Context, libraryID, bookID string, policy LibraryPolicy, source grimmory.File, savedBook state.BookState, options SyncOptions, result Result) (Result, error) {
	workspace, err := s.newWorkspace()
	if err != nil {
		result.Error = "workspace_failed"
		return result, err
	}
	defer os.RemoveAll(workspace)
	sourcePath, sourceSHA, err := s.download(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, source.Format, workspace, "source")
	if err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "download_source_failed", "download_source_failed"
		return result, err
	}
	if source.SHA256 != "" && !strings.EqualFold(source.SHA256, sourceSHA) {
		result.Main.Status, result.Main.Error, result.Error = "failed", "source_hash_mismatch", "source_hash_mismatch"
		return result, ErrVerification
	}
	mainPath, err := s.convert(ctx, sourcePath, source.Format, policy.MainFormat, workspace)
	if err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "main_conversion_failed", "main_conversion_failed"
		return result, err
	}
	mainSHA, _, err := convert.HashFile(mainPath, s.maxFileBytes)
	if err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "main_hash_failed", "main_hash_failed"
		return result, err
	}
	if err := s.upload(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, policy.MainFormat, mainPath, source.Name); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "main_upload_failed", "main_upload_failed"
		return result, err
	}
	verifiedBook, err := s.client.GetLibraryBook(ctx, libraryID, bookID)
	if err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "verification_failed", "verification_failed"
		return result, err
	}
	verifiedMain, ok := FindFile(verifiedBook.Files, policy.MainFormat)
	if !ok {
		result.Main.Status, result.Main.Error, result.Error = "failed", "verification_failed", "verification_failed"
		return result, ErrVerification
	}
	verifiedMainSHA, err := verifiedHash(verifiedMain, mainSHA)
	if err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "verification_failed", "verification_failed"
		return result, err
	}
	canonicalName := verifiedMain.Name
	if canonicalName == "" {
		canonicalName = desiredOutputName(source.Name, policy.MainFormat)
	}
	canonicalState := state.BookState{LibraryID: libraryID, BookID: bookID, MainFormat: policy.MainFormat, CanonicalFormat: policy.MainFormat, CanonicalFileID: verifiedMain.ID, CanonicalFileName: canonicalName, CanonicalSHA256: verifiedMainSHA, MetadataFingerprint: verifiedMain.MetadataFingerprint, CanonicalMTime: verifiedMain.MTime, TrustedMTime: verifiedMain.TrustedMTime, LastSuccessfulSync: savedBook.LastSuccessfulSync, UpdatedAt: time.Now().UTC()}
	if err := s.store.SetBook(ctx, canonicalState); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "state_write_failed", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	result.Main.Status, result.Main.Action, result.Main.Reason = "created", "created", "main_missing"
	generationFingerprints := DesiredGenerationFingerprints(verifiedBook, verifiedMainSHA, canonicalName, policy.OutputFormats)
	plans := PlanDerivatives(verifiedBook.Files, policy.OutputFormats, policy.MainFormat, verifiedMainSHA, nil, verifiedMain.MTime, verifiedMain.TrustedMTime, true, options.Force, completeBookState(savedBook), generationFingerprints)
	failed := false
	var firstFailure error
	for _, plan := range plans {
		item := ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Reason: plan.Reason, Status: "failed"}
		outputPath, conversionErr := s.convert(ctx, mainPath, policy.MainFormat, plan.Format, workspace)
		var outputSHA string
		before, hadBefore := FindFile(verifiedBook.Files, plan.Format)
		if conversionErr == nil {
			if !withinWorkspace(workspace, outputPath) {
				conversionErr = errors.New("converter output escaped workspace")
			} else {
				outputSHA, _, conversionErr = convert.HashFile(outputPath, s.maxFileBytes)
			}
		}
		if conversionErr == nil && plan.Action == "rebuild" {
			before, hadBefore, conversionErr = s.prepareDerivativeReplacement(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, plan.Format, policy.MainFormat, verifiedMain.ID)
		}
		if conversionErr == nil {
			conversionErr = s.upload(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, plan.Format, outputPath, canonicalName)
		}
		if conversionErr == nil {
			verified, verifyErr := s.client.GetLibraryBook(ctx, libraryID, bookID)
			if verifyErr != nil {
				conversionErr = verifyErr
			} else if verifiedFile, exists := findUploadedFile(verified.Files, plan.Format, desiredOutputName(canonicalName, plan.Format), outputSHA); !exists {
				conversionErr = ErrVerification
			} else if conversionErr = verifyUploadedFile(before, hadBefore, verifiedFile, outputSHA); conversionErr != nil {
			} else {
				if stateErr := s.store.SetDerived(ctx, state.DerivedState{LibraryID: libraryID, BookID: bookID, Format: plan.Format, GrimmoryFileID: verifiedFile.ID, SourceSHA256: verifiedMainSHA, OutputSHA256: outputSHA, GenerationFingerprint: plan.GenerationFingerprint, TrustedMTime: verifiedFile.MTime, HasMTime: verifiedFile.TrustedMTime, GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); stateErr != nil {
					conversionErr = fmt.Errorf("%w: %v", ErrState, stateErr)
				} else {
					item.Status = "uploaded"
				}
			}
		}
		if withinWorkspace(workspace, outputPath) {
			_ = os.Remove(outputPath)
		}
		if conversionErr != nil {
			failed = true
			if firstFailure == nil {
				firstFailure = conversionErr
			}
			item.Error = codeForError(conversionErr, "derivative_failed")
		}
		result.Derivatives = append(result.Derivatives, item)
	}
	if failed {
		result.Status, result.Error = "partial", "derivative_failed"
		return result, newPartialError(firstFailure)
	}
	canonicalState.LastSuccessfulSync = time.Now().UTC()
	canonicalState.UpdatedAt = time.Now().UTC()
	if err := s.store.SetBook(ctx, canonicalState); err != nil {
		result.Status, result.Error = "partial", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	result.Status = "completed"
	if err := s.SetFailureTag(ctx, libraryID, bookID, false); err != nil {
		result.Status, result.Error = "partial", "failure_tag_failed"
		return result, fmt.Errorf("%w: %v", ErrFailureTagMutation, err)
	}
	return result, nil
}

// SetFailureTag is the sole service entry point for the operational failure
// tag. Calls made during a locked poll operation reuse that lock.
func (s *Service) SetFailureTag(ctx context.Context, libraryID, bookID string, present bool) error {
	if s.failedTag == "" {
		return nil
	}
	if s.bookLockHeld(ctx, libraryID, bookID) {
		return s.setFailureTagLocked(ctx, libraryID, bookID, present)
	}
	return s.WithBookLock(ctx, libraryID, bookID, func(lockedContext context.Context) error {
		return s.setFailureTagLocked(lockedContext, libraryID, bookID, present)
	})
}

func (s *Service) setFailureTagLocked(ctx context.Context, libraryID, bookID string, present bool) error {
	tagger, ok := s.client.(FailureTagger)
	if !ok {
		return errors.New("failure tag mutation is unsupported")
	}
	reference := grimmory.BookReference{LibraryID: libraryID, BookID: bookID}
	var err error
	if present {
		err = tagger.AddBookTagScoped(ctx, reference, s.failedTag)
	} else {
		err = tagger.RemoveBookTagScoped(ctx, reference, s.failedTag)
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrFailureTagMutation, err)
	}
	return nil
}

func (s *Service) log(level logging.Level, message string, values ...string) {
	if s.logger == nil {
		return
	}
	fields := []logging.Field{{Key: "message", Value: message}}
	for index := 0; index+1 < len(values); index += 2 {
		fields = append(fields, logging.Field{Key: values[index], Value: values[index+1]})
	}
	s.logger.Log(level, fields...)
}

type DerivativePlan struct {
	Format                string
	Action                string
	Reason                string
	GenerationFingerprint string
}

// PlanDerivatives returns actions for configured outputs. Existing derivatives
// are preserved only when their complete checkpoint establishes ownership.
func PlanDerivatives(files []grimmory.File, outputs []string, mainFormat, canonicalSHA string, saved map[string]state.DerivedState, canonicalMTime time.Time, canonicalTrusted, canonicalRecreated, force, bookCheckpointComplete bool, desiredFingerprints map[string]string) []DerivativePlan {
	result := make([]DerivativePlan, 0, len(outputs))
	for _, format := range outputs {
		format = normalizeFormat(format)
		if format == "" || format == normalizeFormat(mainFormat) {
			continue
		}
		existing, exists := FindFile(files, format)
		if !exists {
			result = append(result, derivativePlan(format, "create", "missing_output", desiredFingerprints))
			continue
		}
		if force {
			result = append(result, derivativePlan(format, "rebuild", "forced", desiredFingerprints))
			continue
		}
		if canonicalRecreated {
			result = append(result, derivativePlan(format, "rebuild", "canonical_recreated", desiredFingerprints))
			continue
		}
		previous, tracked := saved[format]
		if !bookCheckpointComplete {
			result = append(result, derivativePlan(format, "rebuild", "state_incomplete", desiredFingerprints))
			continue
		}
		if !tracked {
			result = append(result, derivativePlan(format, "rebuild", "state_missing", desiredFingerprints))
			continue
		}
		if !completeDerivedState(previous) {
			result = append(result, derivativePlan(format, "rebuild", "state_incomplete", desiredFingerprints))
			continue
		}
		if existing.ID != "" && previous.GrimmoryFileID != existing.ID {
			result = append(result, derivativePlan(format, "rebuild", "output_identity_changed", desiredFingerprints))
			continue
		}
		if canonicalSHA != "" && tracked && previous.SourceSHA256 != "" && previous.SourceSHA256 != canonicalSHA {
			result = append(result, derivativePlan(format, "rebuild", "canonical_hash_changed", desiredFingerprints))
			continue
		}
		if tracked && existing.SHA256 != "" && previous.OutputSHA256 != "" && !strings.EqualFold(existing.SHA256, previous.OutputSHA256) {
			result = append(result, derivativePlan(format, "rebuild", "output_hash_changed", desiredFingerprints))
			continue
		}
		if desired, fingerprinted := desiredFingerprints[format]; fingerprinted {
			if previous.GenerationFingerprint == "" {
				result = append(result, derivativePlan(format, "rebuild", "generation_fingerprint_missing", desiredFingerprints))
				continue
			}
			if previous.GenerationFingerprint != desired {
				result = append(result, derivativePlan(format, "rebuild", "generation_fingerprint_changed", desiredFingerprints))
				continue
			}
		}
		derivativeMTime, derivativeTrusted := existing.MTime, existing.TrustedMTime
		if !derivativeTrusted && tracked && previous.HasMTime {
			derivativeMTime, derivativeTrusted = previous.TrustedMTime, true
		}
		if canonicalTrusted && derivativeTrusted && derivativeMTime.Before(canonicalMTime) {
			result = append(result, derivativePlan(format, "rebuild", "trusted_timestamp_stale", desiredFingerprints))
			continue
		}
		reason := "state_current"
		if canonicalSHA == "" {
			reason = "canonical_hash_unknown_preserved"
		}
		result = append(result, derivativePlan(format, "unchanged", reason, desiredFingerprints))
	}
	return result
}

func derivativePlan(format, action, reason string, desiredFingerprints map[string]string) DerivativePlan {
	return DerivativePlan{Format: format, Action: action, Reason: reason, GenerationFingerprint: desiredFingerprints[format]}
}

const generationFingerprintVersion = "v1"

// DesiredGenerationFingerprints returns a deterministic checkpoint for each
// configured output, including target-specific material metadata.
func DesiredGenerationFingerprints(book grimmory.Book, canonicalSHA, sourceName string, outputs []string) map[string]string {
	result := make(map[string]string, len(outputs))
	for _, format := range outputs {
		format = normalizeFormat(format)
		if format == "" {
			continue
		}
		result[format] = GenerationFingerprint(book, canonicalSHA, sourceName, format)
	}
	return result
}

// GenerationFingerprint returns the versioned identity of a desired output
// from its content, filename, target format, and material target metadata.
func GenerationFingerprint(book grimmory.Book, canonicalSHA, sourceName, targetFormat string) string {
	targetFormat = normalizeFormat(targetFormat)
	projection := generationFingerprintProjection{
		CanonicalContentIdentity: strings.ToLower(strings.TrimSpace(canonicalSHA)),
		DesiredOutputName:        desiredOutputName(sourceName, targetFormat),
		TargetFormat:             targetFormat,
		MaterialMetadata:         generationMetadataProjection(book.Metadata, targetFormat),
	}
	encoded, _ := json.Marshal(projection)
	digest := sha256.Sum256(encoded)
	return generationFingerprintVersion + ":" + hex.EncodeToString(digest[:])
}

type generationFingerprintProjection struct {
	CanonicalContentIdentity string `json:"canonicalContentIdentity"`
	DesiredOutputName        string `json:"desiredOutputName"`
	TargetFormat             string `json:"targetFormat"`
	MaterialMetadata         any    `json:"materialMetadata"`
}

type fullGenerationMetadata struct {
	Title           string                 `json:"title"`
	Authors         []string               `json:"authors"`
	Language        string                 `json:"language"`
	Publisher       string                 `json:"publisher"`
	PublicationDate string                 `json:"publicationDate"`
	Identifiers     []generationIdentifier `json:"identifiers"`
	Series          string                 `json:"series"`
	SeriesIndex     string                 `json:"seriesIndex"`
	Tags            []string               `json:"tags"`
	Description     string                 `json:"description"`
	Comments        string                 `json:"comments"`
}

type mobiGenerationMetadata struct {
	Title           string                 `json:"title"`
	Authors         []string               `json:"authors"`
	Language        string                 `json:"language"`
	Publisher       string                 `json:"publisher"`
	PublicationDate string                 `json:"publicationDate"`
	Identifiers     []generationIdentifier `json:"identifiers"`
	Description     string                 `json:"description"`
}

type generationIdentifier struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func generationMetadataProjection(metadata grimmory.BookMetadata, targetFormat string) any {
	authors := normalizedGenerationAuthors(metadata.Authors)
	identifiers := normalizedGenerationIdentifiers(metadata.Identifiers)
	switch targetFormat {
	case "epub", "azw3":
		return fullGenerationMetadata{
			Title:           strings.TrimSpace(metadata.Title),
			Authors:         authors,
			Language:        strings.TrimSpace(metadata.Language),
			Publisher:       strings.TrimSpace(metadata.Publisher),
			PublicationDate: strings.TrimSpace(metadata.PublicationDate),
			Identifiers:     identifiers,
			Series:          strings.TrimSpace(metadata.Series),
			SeriesIndex:     strings.TrimSpace(metadata.SeriesIndex),
			Tags:            normalizedGenerationTags(metadata.Tags),
			Description:     strings.TrimSpace(metadata.Description),
			Comments:        strings.TrimSpace(metadata.Comments),
		}
	case "mobi":
		return mobiGenerationMetadata{
			Title:           strings.TrimSpace(metadata.Title),
			Authors:         authors,
			Language:        strings.TrimSpace(metadata.Language),
			Publisher:       strings.TrimSpace(metadata.Publisher),
			PublicationDate: strings.TrimSpace(metadata.PublicationDate),
			Identifiers:     identifiers,
			Description:     strings.TrimSpace(metadata.Description),
		}
	default:
		return nil
	}
}

func normalizedGenerationAuthors(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func normalizedGenerationIdentifiers(values map[string]string) []generationIdentifier {
	byKey := make(map[string]string, len(values))
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		normalizedKey, normalizedValue := strings.ToLower(strings.TrimSpace(key)), strings.TrimSpace(values[key])
		if normalizedKey != "" && normalizedValue != "" {
			byKey[normalizedKey] = normalizedValue
		}
	}
	normalizedKeys := make([]string, 0, len(byKey))
	for key := range byKey {
		normalizedKeys = append(normalizedKeys, key)
	}
	sort.Strings(normalizedKeys)
	result := make([]generationIdentifier, 0, len(normalizedKeys))
	for _, key := range normalizedKeys {
		result = append(result, generationIdentifier{Key: key, Value: byKey[key]})
	}
	return result
}

func normalizedGenerationTags(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	unique := result[:0]
	for _, value := range result {
		if len(unique) == 0 || unique[len(unique)-1] != value {
			unique = append(unique, value)
		}
	}
	return unique
}

func desiredOutputName(sourceName, targetFormat string) string {
	base := sanitizedSourceBasename(sourceName)
	stem := strings.TrimSuffix(base, path.Ext(base))
	if stem == "" {
		stem = "book"
	}
	targetFormat = normalizeFormat(targetFormat)
	if targetFormat == "" {
		return stem
	}
	return stem + "." + targetFormat
}

func sanitizedSourceBasename(sourceName string) string {
	// Grimmory names are remote input. Treat both slash styles as path
	// separators before applying a conservative filename character policy.
	base := path.Base(strings.ReplaceAll(strings.TrimSpace(sourceName), "\\", "/"))
	if base == "." || base == ".." || base == "/" {
		return "book"
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
		return "book"
	}
	if runes := []rune(base); len(runes) > 180 {
		base = string(runes[:180])
	}
	return base
}

func (s *Service) upload(ctx context.Context, reference grimmory.BookReference, format, filePath, sourceName string) error {
	return s.client.UploadFileNamedScoped(ctx, reference, format, filePath, desiredOutputName(sourceName, format))
}

// prepareDerivativeReplacement refreshes inventory before deleting a rebuild
// target. A target missing at delete time is safe to replace; creates never
// delete.
func (s *Service) prepareDerivativeReplacement(ctx context.Context, reference grimmory.BookReference, format, mainFormat, canonicalFileID string) (grimmory.File, bool, error) {
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.File{}, false, err
	}
	if current.ID != reference.BookID {
		return grimmory.File{}, false, grimmory.ErrInvalidResponse
	}
	if current.LibraryID != reference.LibraryID {
		return grimmory.File{}, false, grimmory.ErrBookNotInLibrary
	}
	format = normalizeFormat(format)
	if format == "" {
		return grimmory.File{}, false, ErrAmbiguousFile
	}
	existingFiles := filesForFormat(current.Files, format)
	switch len(existingFiles) {
	case 0:
		return grimmory.File{}, false, nil
	case 1:
		if existingFiles[0].ID == "" {
			return grimmory.File{}, false, ErrAmbiguousFile
		}
	default:
		return grimmory.File{}, false, ErrAmbiguousFile
	}
	existing := existingFiles[0]
	if format == normalizeFormat(mainFormat) {
		return grimmory.File{}, false, ErrPrimaryFile
	}
	primaryFiles := filesForFormat(current.Files, mainFormat)
	switch len(primaryFiles) {
	case 0:
		return grimmory.File{}, false, ErrPrimaryFile
	case 1:
	default:
		return grimmory.File{}, false, ErrAmbiguousFile
	}
	if primaryFiles[0].ID != "" && primaryFiles[0].ID == existing.ID {
		return grimmory.File{}, false, ErrPrimaryFile
	}
	if canonicalFileID != "" && canonicalFileID == existing.ID {
		return grimmory.File{}, false, ErrPrimaryFile
	}
	identityMatches := 0
	for _, file := range current.Files {
		if file.ID == existing.ID {
			identityMatches++
		}
	}
	if identityMatches != 1 {
		return grimmory.File{}, false, ErrAmbiguousFile
	}
	if err := s.client.DeleteFileScoped(ctx, reference, existing.ID); err != nil {
		// Another writer can remove the planned target after this inventory read
		// but before DELETE. In that case the replacement upload is still safe.
		if missingDerivativeFile(err) {
			return existing, true, nil
		}
		return grimmory.File{}, false, err
	}
	return existing, true, nil
}

func missingDerivativeFile(err error) bool {
	if errors.Is(err, grimmory.ErrFileNotFound) {
		return true
	}
	if !errors.Is(err, grimmory.ErrNotFound) {
		return false
	}
	var httpErr *grimmory.HTTPError
	if !errors.As(err, &httpErr) {
		return true
	}
	return httpErr.Operation == "delete" || httpErr.Operation == "file delete"
}

func filesForFormat(files []grimmory.File, format string) []grimmory.File {
	format = normalizeFormat(format)
	result := make([]grimmory.File, 0, 1)
	for _, file := range files {
		if normalizeFormat(file.Format) == format {
			result = append(result, file)
		}
	}
	return result
}

func completeBookState(value state.BookState) bool {
	return value.BookID != "" && value.MainFormat != "" && value.CanonicalFormat != "" && value.CanonicalFileID != "" && value.CanonicalFileName != "" && value.CanonicalSHA256 != "" && !value.LastSuccessfulSync.IsZero()
}

func completeDerivedState(value state.DerivedState) bool {
	return value.BookID != "" && value.Format != "" && value.GrimmoryFileID != "" && value.SourceSHA256 != "" && value.OutputSHA256 != "" && !value.GeneratedAt.IsZero()
}

// verifyUploadedFile requires evidence that the post-upload inventory is the
// file just produced. A format merely still existing may be an ignored upload.
func verifyUploadedFile(before grimmory.File, hadBefore bool, after grimmory.File, outputSHA string) error {
	if after.SHA256 != "" {
		if strings.EqualFold(after.SHA256, outputSHA) {
			return nil
		}
		return ErrVerification
	}
	if after.ID == "" || (hadBefore && after.ID == before.ID) {
		return ErrVerification
	}
	return nil
}

// SelectSource uses configured format order rather than API order.
func SelectSource(files []grimmory.File, mainFormat string, allowed []string) (grimmory.File, bool) {
	for _, format := range allowed {
		format = normalizeFormat(format)
		if format == "" || format == normalizeFormat(mainFormat) {
			continue
		}
		if file, ok := FindFile(files, format); ok {
			return file, true
		}
	}
	return grimmory.File{}, false
}

func FindFile(files []grimmory.File, format string) (grimmory.File, bool) {
	format = normalizeFormat(format)
	var selected grimmory.File
	found := false
	for _, file := range files {
		if normalizeFormat(file.Format) != format {
			continue
		}
		if !found || file.Name < selected.Name || (file.Name == selected.Name && file.ID < selected.ID) {
			selected, found = file, true
		}
	}
	return selected, found
}

func findUploadedFile(files []grimmory.File, format, desiredName, outputSHA string) (grimmory.File, bool) {
	format = normalizeFormat(format)
	for _, file := range files {
		if normalizeFormat(file.Format) == format && file.Name == desiredName && (file.SHA256 == "" || strings.EqualFold(file.SHA256, outputSHA)) {
			return file, true
		}
	}
	if outputSHA != "" {
		for _, file := range files {
			if normalizeFormat(file.Format) == format && file.SHA256 != "" && strings.EqualFold(file.SHA256, outputSHA) {
				return file, true
			}
		}
	}
	return FindFile(files, format)
}

func verifiedHash(file grimmory.File, fallback string) (string, error) {
	if file.SHA256 == "" {
		return fallback, nil
	}
	if fallback != "" && !strings.EqualFold(file.SHA256, fallback) {
		return "", ErrVerification
	}
	return strings.ToLower(file.SHA256), nil
}

func (s *Service) download(ctx context.Context, reference grimmory.BookReference, format, workspace, name string) (string, string, error) {
	filePath := filepath.Join(workspace, name+"."+normalizeFormat(format))
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", "", fmt.Errorf("create download file: %w", err)
	}
	_, remoteHash, downloadErr := s.client.DownloadContentScoped(ctx, reference, format, file)
	closeErr := file.Close()
	if downloadErr != nil {
		return "", "", downloadErr
	}
	if closeErr != nil {
		return "", "", closeErr
	}
	hash, _, err := convert.HashFile(filePath, s.maxFileBytes)
	if err != nil {
		return "", "", err
	}
	if remoteHash != "" && !strings.EqualFold(remoteHash, hash) {
		return "", "", errors.New("download hash mismatch")
	}
	return filePath, hash, nil
}

func (s *Service) convert(ctx context.Context, sourcePath, sourceFormat, targetFormat, workspace string) (string, error) {
	conversionCtx, cancel := context.WithTimeout(ctx, s.conversionTimeout)
	defer cancel()
	return s.converter.Convert(conversionCtx, sourcePath, sourceFormat, targetFormat, workspace)
}

func (s *Service) newWorkspace() (string, error) {
	return os.MkdirTemp(s.tempRoot, ".grimmory-reconcile-")
}

func withinWorkspace(workspace, filePath string) bool {
	if workspace == "" || filePath == "" {
		return false
	}
	relative, err := filepath.Rel(workspace, filePath)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}

func (s *Service) bookLock(libraryID, bookID string) (*sync.Mutex, func()) {
	key := libraryID + "\x00" + bookID
	s.locksMu.Lock()
	lock := s.locks[key]
	if lock == nil {
		lock = &bookLock{}
		s.locks[key] = lock
	}
	lock.refs++
	s.locksMu.Unlock()
	return &lock.mu, func() {
		s.locksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.locks, key)
		}
		s.locksMu.Unlock()
	}
}

func ValidBookID(bookID string) bool {
	if bookID == "" || bookID == "." || bookID == ".." || len(bookID) > 256 || strings.TrimSpace(bookID) != bookID {
		return false
	}
	for _, char := range bookID {
		if char == '/' || char == '\\' || char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func ValidLibraryID(libraryID string) bool {
	if libraryID == "" || strings.TrimSpace(libraryID) != libraryID {
		return false
	}
	for _, char := range libraryID {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func normalizeFormat(format string) string {
	return strings.ToLower(strings.TrimSpace(strings.TrimPrefix(format, ".")))
}

func copyStrings(values []string) []string { return append([]string(nil), values...) }

func normalizeFormats(values []string, excluded string, exclude bool) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = normalizeFormat(value)
		if value == "" || !validFormat(value) || (exclude && value == excluded) {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func validFormat(value string) bool {
	if len(value) == 0 || len(value) > 32 {
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

func codeForError(err error, fallback string) string {
	switch {
	case errors.Is(err, grimmory.ErrNotFound):
		return "book_not_found"
	case errors.Is(err, ErrVerification):
		return "verification_failed"
	case errors.Is(err, ErrState):
		return "state_write_failed"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return fallback
	}
}

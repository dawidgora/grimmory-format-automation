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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"converter/internal/artifactname"
	"converter/internal/convert"
	"converter/internal/grimmory"
	"converter/internal/logging"
	"converter/internal/state"
)

var (
	ErrInvalidBookID              = errors.New("invalid book ID")
	ErrInvalidLibraryID           = errors.New("invalid library ID")
	ErrNoSource                   = errors.New("no configured source format is available")
	ErrVerification               = errors.New("Grimmory upload verification failed")
	ErrPartial                    = errors.New("reconciliation completed with failures")
	ErrState                      = errors.New("reconciliation state operation failed")
	ErrLibraryNotAllowed          = errors.New("library is not allowed")
	ErrFailureTagMutation         = errors.New("failure tag mutation failed")
	ErrReplacementTagMutation     = errors.New("derivative replacement tag mutation failed")
	ErrSafeReplacementUnavailable = errors.New("safe replacement unavailable")
)

const SafeReplacementUnavailableCode = "safe_replacement_unavailable"

const defaultDerivativeReplacementTag = "derivative-replacement"

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
	case errors.Is(err, ErrReplacementTagMutation):
		return "replacement_tag_mutation"
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
	case errors.Is(err, ErrSafeReplacementUnavailable):
		return SafeReplacementUnavailableCode
	case errors.Is(err, ErrPartial):
		return "reconciliation"
	default:
		return "internal"
	}
}

type partialError struct{ cause error }

func (e *partialError) Error() string { return ErrPartial.Error() }

func (e *partialError) Unwrap() error { return ErrPartial }

func (e *partialError) Is(target error) bool {
	return target == ErrPartial || errors.Is(e.cause, target)
}

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

// UploadIntentStore is implemented by the SQLite store. It is optional at the
// interface boundary so small integrations that only need the original Store
// methods remain source compatible, while the production store still gets
// durable recovery semantics.
type UploadIntentStore interface {
	GetDerivedUploadIntents(context.Context, string, string) (map[string]state.DerivedUploadIntent, error)
	SetDerivedUploadIntent(context.Context, state.DerivedUploadIntent) error
}

type DerivedCommitStore interface {
	CommitDerived(context.Context, state.DerivedState, string) error
}

type PendingReplacementStore interface {
	ClearPendingReplacementTag(context.Context, string, string, string) error
}

type PendingReplacementMarkerStore interface {
	MarkPendingReplacementCleanup(context.Context, string, string, string) error
}

type ReplacementProgressStore interface {
	MarkReplacementInProgress(context.Context, string, string, string) error
}

type ReplacementPreparationStore interface {
	PrepareReplacement(context.Context, state.DerivedUploadIntent) error
}

type Options struct {
	Client                   Remote
	Store                    Store
	Converter                Converter
	OutputFormats            []string
	SupportedInputs          []string
	LibraryIDs               []string
	MaxConcurrentBooks       int
	FailedProcessingTag      string
	IgnoreProcessingTag      string
	ExistingDerivativePolicy string
	DerivativeReplacementTag string
	MaxFileBytes             int64
	ConversionTimeout        time.Duration
	TempRoot                 string
	Logger                   *logging.Logger
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
	ignoreTag         string
	existingPolicy    string
	replacementTag    string
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
		conversionTimeout: options.ConversionTimeout, tempRoot: options.TempRoot, logger: options.Logger,
		failedTag: strings.TrimSpace(options.FailedProcessingTag), ignoreTag: strings.TrimSpace(options.IgnoreProcessingTag),
		existingPolicy: normalizeExistingDerivativePolicy(options.ExistingDerivativePolicy),
		replacementTag: strings.TrimSpace(options.DerivativeReplacementTag),
		locks:          make(map[string]*bookLock), limiter: make(chan struct{}, options.MaxConcurrentBooks), allowedLibraries: allowed,
	}
}

func normalizeExistingDerivativePolicy(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "replace") {
		return "replace"
	}
	return "preserve"
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
	if savedBook.PendingReplacementTag != "" {
		if options.DryRun {
			result.Status = "dry_run"
			result.Main = ItemResult{Action: "cleanup", Status: "planned", Reason: "pending_replacement_cleanup"}
			return result, nil
		}
		return s.finishPendingReplacementCleanup(ctx, libraryID, bookID, book, savedBook, result)
	}
	if hasTag(book, s.ignoreTag) && !savedBook.ReplacementInProgress {
		result.Status = "ignored"
		return result, nil
	}
	policy, err := s.resolvePolicy(ctx, libraryID)
	if err != nil {
		result.Error = "library_policy_failed"
		return result, err
	}
	replacementAllowed := s.replacementAllowedForState(book, savedBook, options)
	replacementTagRequested := (s.replacementTag != "" && hasTag(book, s.replacementTag)) || savedBook.ReplacementInProgressTag != ""
	replacementTag := s.replacementTag
	if savedBook.ReplacementInProgressTag != "" {
		replacementTag = savedBook.ReplacementInProgressTag
	}
	uploadIntents, err := s.getUploadIntents(ctx, libraryID, bookID)
	if err != nil {
		result.Error = "state_read_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	if savedBook.ReplacementInProgress && len(uploadIntents) == 0 {
		result.Status, result.Error = "partial", SafeReplacementUnavailableCode
		return result, newPartialError(ErrSafeReplacementUnavailable)
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
			replacementOwner, _ := replacementOwnerFormat(savedBook, uploadIntents)
			for _, format := range policy.OutputFormats {
				action := "create"
				if _, exists := FindFile(book.Files, format); exists {
					action = "rebuild"
				}
				item := ItemResult{Format: format, SourceFormat: policy.MainFormat, Action: action, Status: "planned", Reason: "main_would_be_created"}
				allowedForFormat := s.replacementAllowedForFormat(book, savedBook, replacementOwner, format, options)
				if action == "rebuild" && !allowedForFormat {
					item.Status = "blocked"
					item.Error = SafeReplacementUnavailableCode
					result.Error = SafeReplacementUnavailableCode
				}
				result.Derivatives = append(result.Derivatives, item)
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
		canonicalState = state.BookState{LibraryID: libraryID, BookID: bookID, MainFormat: policy.MainFormat, CanonicalFormat: policy.MainFormat, CanonicalFileID: mainFile.ID, CanonicalFileName: canonicalName, CanonicalSHA256: canonicalSHA, MetadataFingerprint: mainFile.MetadataFingerprint, CanonicalMTime: canonicalMTime, TrustedMTime: canonicalTrustedMTime, LastSuccessfulSync: savedBook.LastSuccessfulSync, PendingReplacementTag: savedBook.PendingReplacementTag, ReplacementInProgressTag: savedBook.ReplacementInProgressTag, ReplacementInProgress: savedBook.ReplacementInProgress, UpdatedAt: time.Now().UTC()}
		if err := s.store.SetBook(ctx, canonicalState); err != nil {
			result.Error = "state_write_failed"
			return result, fmt.Errorf("%w: %v", ErrState, err)
		}
	}
	generationFingerprints := DesiredGenerationFingerprints(book, canonicalSHA, canonicalName, policy.OutputFormats)
	plans := PlanDerivatives(book.Files, policy.OutputFormats, policy.MainFormat, canonicalSHA, savedDerived, canonicalMTime, canonicalTrustedMTime, false, options.Force, false, generationFingerprints, canonicalName)
	plans, replacementOwner, ownerReady := prioritizeReplacementOwner(savedBook, uploadIntents, plans)
	if savedBook.ReplacementInProgress && !ownerReady {
		result.Status, result.Error = "partial", SafeReplacementUnavailableCode
		return result, newPartialError(ErrSafeReplacementUnavailable)
	}
	if options.DryRun {
		for _, plan := range plans {
			item := ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Status: "planned", Reason: plan.Reason}
			allowedForFormat := s.replacementAllowedForFormat(book, savedBook, replacementOwner, plan.Format, options)
			if plan.Blocked && !allowedForFormat {
				item.Status = "blocked"
				item.Error = SafeReplacementUnavailableCode
				result.Error = SafeReplacementUnavailableCode
			}
			result.Derivatives = append(result.Derivatives, item)
		}
		result.Status = "dry_run"
		return result, nil
	}
	failed := false
	blocked := false
	stopFurther := false
	var firstFailure error
	pendingCleanupTag := savedBook.ReplacementInProgressTag
	replacementAuthorizedState := savedBook.ReplacementInProgress || pendingCleanupTag != ""
	phaseCleared := false
	completionCtx := ctx
	var completionCancel context.CancelFunc
	defer func() {
		if completionCancel != nil {
			completionCancel()
		}
	}()
	expectedBook := book
	for _, plan := range plans {
		if stopFurther {
			break
		}
		if phaseCleared {
			if s.ignoreTag != "" {
				refreshedBook, ignored, refreshErr := s.refreshBookBeforeNextDerivative(ctx, libraryID, bookID)
				if refreshErr != nil {
					result.Status, result.Error = "partial", codeForError(refreshErr, "verification_failed")
					return result, newPartialError(refreshErr)
				}
				expectedBook = refreshedBook
				if ignored {
					result.Status = "ignored"
					return result, nil
				}
			}
			phaseCleared = false
		}
		replacementAllowed = s.replacementAllowedForState(expectedBook, savedBook, options)
		replacementTagRequested = (s.replacementTag != "" && hasTag(expectedBook, s.replacementTag)) || savedBook.ReplacementInProgressTag != ""
		replacementTag = s.replacementTag
		if savedBook.ReplacementInProgressTag != "" {
			replacementTag = savedBook.ReplacementInProgressTag
		}
		isReplacementOwner := replacementOwner != "" && normalizeFormat(plan.Format) == replacementOwner
		if savedBook.ReplacementInProgress && replacementOwner != "" && !isReplacementOwner {
			stopFurther = true
			break
		}
		item := ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Reason: plan.Reason}
		intent, hasIntent := uploadIntents[plan.Format]
		if isReplacementOwner && savedBook.ReplacementInProgress && hasIntent && plan.Action == "unchanged" {
			plan.Action, plan.Reason, plan.Blocked = "rebuild", "replacement_in_progress", true
			item.Action, item.Reason = plan.Action, plan.Reason
		}
		if plan.Action == "unchanged" {
			item.Status = "unchanged"
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		before, hadBefore := FindFile(expectedBook.Files, plan.Format)
		resumeDestructive := isReplacementOwner && savedBook.ReplacementInProgress && hasIntent && replacementTargetMatches(expectedBook.Files, plan.Format, intent)
		safeMissingRecovery := isReplacementOwner && savedBook.ReplacementInProgress && hasIntent && replacementTargetMissing(expectedBook.Files, plan.Format, intent)
		if isReplacementOwner && savedBook.ReplacementInProgress && !hasIntent {
			failed = true
			blocked = true
			stopFurther = true
			firstFailure = ErrSafeReplacementUnavailable
			item.Status, item.Error = "blocked", SafeReplacementUnavailableCode
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		needsIntentRecovery := hasIntent && !resumeDestructive && !safeMissingRecovery && ((isReplacementOwner && savedBook.ReplacementInProgress) || (plan.Blocked && !options.Force) || plan.Action == "create")
		if needsIntentRecovery {
			if hasIntent {
				candidate, recoveredBook, recoverable, recoveryErr := s.recoverableIntentCandidate(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, workspace, expectedBook, mainFile, plan, intent, canonicalSHA, canonicalName, policy.MainFormat)
				if recoveryErr != nil {
					failed = true
					stopFurther = true
					if firstFailure == nil {
						firstFailure = recoveryErr
					}
					item.Status = "failed"
					item.Error = codeForError(recoveryErr, "derivative_failed")
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
				if recoverable {
					adopted := adoptedDerivedState(libraryID, bookID, plan, candidate, intent)
					if err := s.commitDerived(ctx, adopted, intent.ReplacementTag); err != nil {
						stateErr := fmt.Errorf("%w: %v", ErrState, err)
						failed = true
						stopFurther = true
						if firstFailure == nil {
							firstFailure = stateErr
						}
						item.Status = "failed"
						item.Error = codeForError(stateErr, "state_write_failed")
						result.Derivatives = append(result.Derivatives, item)
						continue
					}
					item.Status, item.Reason = "adopted", "upload_intent_recovered"
					if intent.ReplacementTag != "" {
						pendingCleanupTag = intent.ReplacementTag
						savedBook.ReplacementInProgressTag = intent.ReplacementTag
						replacementAuthorizedState = true
					}
					if isReplacementOwner && savedBook.ReplacementInProgress {
						savedBook.ReplacementInProgress = false
						replacementAuthorizedState = pendingCleanupTag != ""
						phaseCleared = true
					}
					expectedBook = recoveredBook
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
				if isReplacementOwner && savedBook.ReplacementInProgress {
					failed = true
					blocked = true
					stopFurther = true
					firstFailure = ErrSafeReplacementUnavailable
					item.Status, item.Error = "blocked", SafeReplacementUnavailableCode
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
				if plan.Blocked && !options.Force && len(filesForFormat(expectedBook.Files, plan.Format)) > 0 {
					failed = true
					blocked = true
					stopFurther = true
					firstFailure = ErrSafeReplacementUnavailable
					item.Status, item.Error = "blocked", SafeReplacementUnavailableCode
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
			}
		}
		if plan.Blocked && !replacementAllowed {
			item.Status = "blocked"
			item.Error = SafeReplacementUnavailableCode
			failed = true
			blocked = true
			if firstFailure == nil {
				firstFailure = ErrSafeReplacementUnavailable
			}
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		item.Status = "failed"
		uploadAttempted := false
		tagAuthorized := replacementTagRequested && !options.Force && s.existingPolicy != "replace" && plan.Blocked && hadBefore
		intent = uploadIntentFor(libraryID, bookID, plan, canonicalName, canonicalSHA, mainFile, stableInventoryFingerprint(expectedBook.Files, plan.Format), tagAuthorized, replacementTag)
		if safeMissingRecovery {
			intent = preservePreparedReplacementEvidence(uploadIntents[plan.Format], intent)
		}
		outputPath, conversionErr := s.convert(ctx, canonicalPath, policy.MainFormat, plan.Format, workspace)
		var outputSHA string
		if conversionErr == nil {
			if !withinWorkspace(workspace, outputPath) {
				conversionErr = errors.New("converter output escaped workspace")
			} else {
				outputSHA, _, conversionErr = convert.HashFile(outputPath, s.maxFileBytes)
			}
		}
		if conversionErr == nil {
			intent.OutputSHA256 = outputSHA
		}
		operationCtx := ctx
		var operationCancel context.CancelFunc
		destructiveOperation := false
		if conversionErr == nil {
			uploadAttempted = true
			destructiveOperation, operationCtx, operationCancel, conversionErr = s.uploadDerivative(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, plan, before, hadBefore, expectedBook, policy.MainFormat, mainFile, canonicalSHA, workspace, outputPath, canonicalName, options, intent, replacementAuthorizedState)
			if destructiveOperation {
				savedBook.ReplacementInProgress = true
			}
		}
		if conversionErr == nil {
			verifiedBook, verifyErr := s.client.GetLibraryBook(operationCtx, libraryID, bookID)
			if verifyErr != nil {
				conversionErr = verifyErr
			} else if verifiedFile, ok := findUploadedFile(verifiedBook.Files, plan.Format, desiredOutputName(canonicalName, plan.Format)); !ok {
				conversionErr = ErrVerification
			} else if conversionErr = s.verifyUploadedFile(operationCtx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, before, hadBefore, verifiedFile, outputSHA, workspace, verifiedBook.Files, mainFile.ID); conversionErr != nil {
			} else if conversionErr = s.revalidateCanonicalSource(operationCtx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, policy.MainFormat, mainFile, canonicalSHA, canonicalPath, workspace, plan.Format, verifiedBook); conversionErr != nil {
			} else {
				if err := s.commitDerived(operationCtx, state.DerivedState{LibraryID: libraryID, BookID: bookID, Format: plan.Format, GrimmoryFileID: verifiedFile.ID, SourceSHA256: canonicalSHA, OutputSHA256: outputSHA, GenerationFingerprint: plan.GenerationFingerprint, TrustedMTime: verifiedFile.MTime, HasMTime: verifiedFile.TrustedMTime, GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}, intent.ReplacementTag); err != nil {
					conversionErr = fmt.Errorf("%w: %v", ErrState, err)
				} else {
					item.Status = "uploaded"
					if intent.ReplacementTag != "" {
						pendingCleanupTag = intent.ReplacementTag
						savedBook.ReplacementInProgressTag = intent.ReplacementTag
						replacementAuthorizedState = true
					}
					if destructiveOperation || (isReplacementOwner && savedBook.ReplacementInProgress) {
						savedBook.ReplacementInProgress = false
						replacementAuthorizedState = pendingCleanupTag != ""
						phaseCleared = true
					}
					expectedBook = verifiedBook
				}
			}
		}
		if operationCancel != nil {
			if completionCancel != nil {
				completionCancel()
			}
			completionCtx, completionCancel = operationCtx, operationCancel
		}
		if withinWorkspace(workspace, outputPath) {
			_ = os.Remove(outputPath)
		}
		if conversionErr != nil {
			failed = true
			if uploadAttempted {
				stopFurther = true
			}
			if errors.Is(conversionErr, ErrSafeReplacementUnavailable) {
				blocked = true
			}
			if firstFailure == nil {
				firstFailure = conversionErr
			}
			item.Error = codeForError(conversionErr, "derivative_failed")
		}
		result.Derivatives = append(result.Derivatives, item)
	}
	if failed {
		result.Status = "partial"
		if blocked {
			result.Error = SafeReplacementUnavailableCode
		} else {
			result.Error = "derivative_failed"
		}
		if blocked {
			firstFailure = ErrSafeReplacementUnavailable
		}
		return result, newPartialError(firstFailure)
	}
	finalCtx := ctx
	if completionCancel != nil {
		finalCtx = completionCtx
	}
	completionBook, completionErr := s.client.GetLibraryBook(finalCtx, libraryID, bookID)
	if completionErr != nil {
		result.Status, result.Error = "partial", codeForError(completionErr, "verification_failed")
		return result, newPartialError(completionErr)
	}
	if !sameFileInventory(completionBook, expectedBook) {
		result.Status, result.Error = "partial", SafeReplacementUnavailableCode
		return result, newPartialError(ErrSafeReplacementUnavailable)
	}
	if pendingCleanupTag != "" {
		if err := s.markPendingReplacementCleanup(finalCtx, libraryID, bookID, pendingCleanupTag); err != nil {
			result.Status, result.Error = "partial", "state_write_failed"
			return result, fmt.Errorf("%w: %v", ErrState, err)
		}
		if err := s.clearReplacementTag(finalCtx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, completionBook, pendingCleanupTag); err != nil {
			result.Status, result.Error = "partial", "replacement_tag_failed"
			return result, fmt.Errorf("%w: %v", ErrReplacementTagMutation, err)
		}
		canonicalState.PendingReplacementTag = pendingCleanupTag
		canonicalState.ReplacementInProgressTag = ""
	}
	canonicalState.ReplacementInProgress = false
	canonicalState.LastSuccessfulSync = time.Now().UTC()
	canonicalState.UpdatedAt = time.Now().UTC()
	if err := s.store.SetBook(finalCtx, canonicalState); err != nil {
		result.Status, result.Error = "partial", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	if pendingCleanupTag != "" {
		if err := s.clearPendingReplacementTag(finalCtx, libraryID, bookID, pendingCleanupTag); err != nil {
			result.Status, result.Error = "partial", "state_write_failed"
			return result, fmt.Errorf("%w: %v", ErrState, err)
		}
	}
	result.Status = "completed"
	if err := s.SetFailureTag(finalCtx, libraryID, bookID, false); err != nil {
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
	if !ok || len(filesForFormat(verifiedBook.Files, policy.MainFormat)) != 1 || !uniqueFileIDs(verifiedBook.Files) {
		result.Main.Status, result.Main.Error, result.Error = "failed", "verification_failed", "verification_failed"
		return result, ErrVerification
	}
	if err := s.verifyUploadedFile(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, grimmory.File{}, false, verifiedMain, mainSHA, workspace, verifiedBook.Files, ""); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "verification_failed", "verification_failed"
		return result, err
	}
	verifiedMainSHA := mainSHA
	canonicalName := verifiedMain.Name
	if canonicalName == "" {
		canonicalName = desiredOutputName(source.Name, policy.MainFormat)
	}
	canonicalState := state.BookState{LibraryID: libraryID, BookID: bookID, MainFormat: policy.MainFormat, CanonicalFormat: policy.MainFormat, CanonicalFileID: verifiedMain.ID, CanonicalFileName: canonicalName, CanonicalSHA256: verifiedMainSHA, MetadataFingerprint: verifiedMain.MetadataFingerprint, CanonicalMTime: verifiedMain.MTime, TrustedMTime: verifiedMain.TrustedMTime, LastSuccessfulSync: savedBook.LastSuccessfulSync, PendingReplacementTag: savedBook.PendingReplacementTag, ReplacementInProgressTag: savedBook.ReplacementInProgressTag, ReplacementInProgress: savedBook.ReplacementInProgress, UpdatedAt: time.Now().UTC()}
	if err := s.store.SetBook(ctx, canonicalState); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "state_write_failed", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	result.Main.Status, result.Main.Action, result.Main.Reason = "created", "created", "main_missing"
	replacementAllowed := s.replacementAllowedForState(verifiedBook, savedBook, options)
	replacementTagRequested := (s.replacementTag != "" && hasTag(verifiedBook, s.replacementTag)) || savedBook.ReplacementInProgressTag != ""
	replacementTag := s.replacementTag
	if savedBook.ReplacementInProgressTag != "" {
		replacementTag = savedBook.ReplacementInProgressTag
	}
	uploadIntents, err := s.getUploadIntents(ctx, libraryID, bookID)
	if err != nil {
		result.Status, result.Error = "partial", "state_read_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	if savedBook.ReplacementInProgress && len(uploadIntents) == 0 {
		result.Status, result.Error = "partial", SafeReplacementUnavailableCode
		return result, newPartialError(ErrSafeReplacementUnavailable)
	}
	generationFingerprints := DesiredGenerationFingerprints(verifiedBook, verifiedMainSHA, canonicalName, policy.OutputFormats)
	plans := PlanDerivatives(verifiedBook.Files, policy.OutputFormats, policy.MainFormat, verifiedMainSHA, nil, verifiedMain.MTime, verifiedMain.TrustedMTime, true, options.Force, false, generationFingerprints, canonicalName)
	plans, replacementOwner, ownerReady := prioritizeReplacementOwner(savedBook, uploadIntents, plans)
	if savedBook.ReplacementInProgress && !ownerReady {
		result.Status, result.Error = "partial", SafeReplacementUnavailableCode
		return result, newPartialError(ErrSafeReplacementUnavailable)
	}
	failed := false
	blocked := false
	stopFurther := false
	var firstFailure error
	pendingCleanupTag := savedBook.ReplacementInProgressTag
	replacementAuthorizedState := savedBook.ReplacementInProgress || pendingCleanupTag != ""
	phaseCleared := false
	completionCtx := ctx
	var completionCancel context.CancelFunc
	defer func() {
		if completionCancel != nil {
			completionCancel()
		}
	}()
	expectedBook := verifiedBook
	for _, plan := range plans {
		if stopFurther {
			break
		}
		if phaseCleared {
			if s.ignoreTag != "" {
				refreshedBook, ignored, refreshErr := s.refreshBookBeforeNextDerivative(ctx, libraryID, bookID)
				if refreshErr != nil {
					result.Status, result.Error = "partial", codeForError(refreshErr, "verification_failed")
					return result, newPartialError(refreshErr)
				}
				expectedBook = refreshedBook
				if ignored {
					result.Status = "ignored"
					return result, nil
				}
			}
			phaseCleared = false
		}
		replacementAllowed = s.replacementAllowedForState(expectedBook, savedBook, options)
		replacementTagRequested = (s.replacementTag != "" && hasTag(expectedBook, s.replacementTag)) || savedBook.ReplacementInProgressTag != ""
		replacementTag = s.replacementTag
		if savedBook.ReplacementInProgressTag != "" {
			replacementTag = savedBook.ReplacementInProgressTag
		}
		isReplacementOwner := replacementOwner != "" && normalizeFormat(plan.Format) == replacementOwner
		if savedBook.ReplacementInProgress && replacementOwner != "" && !isReplacementOwner {
			stopFurther = true
			break
		}
		item := ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Reason: plan.Reason, Status: "failed"}
		before, hadBefore := FindFile(expectedBook.Files, plan.Format)
		intent, hasIntent := uploadIntents[plan.Format]
		if isReplacementOwner && savedBook.ReplacementInProgress && hasIntent && plan.Action == "unchanged" {
			plan.Action, plan.Reason, plan.Blocked = "rebuild", "replacement_in_progress", true
			item.Action, item.Reason = plan.Action, plan.Reason
		}
		resumeDestructive := isReplacementOwner && savedBook.ReplacementInProgress && hasIntent && replacementTargetMatches(expectedBook.Files, plan.Format, intent)
		safeMissingRecovery := isReplacementOwner && savedBook.ReplacementInProgress && hasIntent && replacementTargetMissing(expectedBook.Files, plan.Format, intent)
		if isReplacementOwner && savedBook.ReplacementInProgress && !hasIntent {
			failed = true
			blocked = true
			stopFurther = true
			firstFailure = ErrSafeReplacementUnavailable
			item.Status, item.Error = "blocked", SafeReplacementUnavailableCode
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		needsIntentRecovery := hasIntent && !resumeDestructive && !safeMissingRecovery && ((isReplacementOwner && savedBook.ReplacementInProgress) || (plan.Blocked && !options.Force) || plan.Action == "create")
		if needsIntentRecovery {
			if hasIntent {
				candidate, recoveredBook, recoverable, recoveryErr := s.recoverableIntentCandidate(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, workspace, expectedBook, verifiedMain, plan, intent, verifiedMainSHA, canonicalName, policy.MainFormat)
				if recoveryErr != nil {
					failed = true
					stopFurther = true
					if firstFailure == nil {
						firstFailure = recoveryErr
					}
					item.Status, item.Error = "failed", codeForError(recoveryErr, "derivative_failed")
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
				if recoverable {
					if err := s.commitDerived(ctx, adoptedDerivedState(libraryID, bookID, plan, candidate, intent), intent.ReplacementTag); err != nil {
						stateErr := fmt.Errorf("%w: %v", ErrState, err)
						failed = true
						stopFurther = true
						if firstFailure == nil {
							firstFailure = stateErr
						}
						item.Status, item.Error = "failed", codeForError(stateErr, "state_write_failed")
						result.Derivatives = append(result.Derivatives, item)
						continue
					}
					item.Status, item.Reason = "adopted", "upload_intent_recovered"
					if intent.ReplacementTag != "" {
						pendingCleanupTag = intent.ReplacementTag
						savedBook.ReplacementInProgressTag = intent.ReplacementTag
					}
					if isReplacementOwner && savedBook.ReplacementInProgress {
						savedBook.ReplacementInProgress = false
						replacementAuthorizedState = pendingCleanupTag != ""
						phaseCleared = true
					}
					expectedBook = recoveredBook
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
				if isReplacementOwner && savedBook.ReplacementInProgress {
					failed = true
					blocked = true
					stopFurther = true
					firstFailure = ErrSafeReplacementUnavailable
					item.Status, item.Error = "blocked", SafeReplacementUnavailableCode
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
				if plan.Blocked && !options.Force && len(filesForFormat(expectedBook.Files, plan.Format)) > 0 {
					failed = true
					blocked = true
					stopFurther = true
					firstFailure = ErrSafeReplacementUnavailable
					item.Status, item.Error = "blocked", SafeReplacementUnavailableCode
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
			}
		}
		if plan.Blocked && !replacementAllowed {
			item.Status = "blocked"
			item.Error = SafeReplacementUnavailableCode
			failed = true
			blocked = true
			if firstFailure == nil {
				firstFailure = ErrSafeReplacementUnavailable
			}
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		tagAuthorized := replacementTagRequested && !options.Force && s.existingPolicy != "replace" && plan.Blocked && hadBefore
		intent = uploadIntentFor(libraryID, bookID, plan, canonicalName, verifiedMainSHA, verifiedMain, stableInventoryFingerprint(expectedBook.Files, plan.Format), tagAuthorized, replacementTag)
		if safeMissingRecovery {
			intent = preservePreparedReplacementEvidence(uploadIntents[plan.Format], intent)
		}
		outputPath, conversionErr := s.convert(ctx, mainPath, policy.MainFormat, plan.Format, workspace)
		var outputSHA string
		if conversionErr == nil {
			if !withinWorkspace(workspace, outputPath) {
				conversionErr = errors.New("converter output escaped workspace")
			} else {
				outputSHA, _, conversionErr = convert.HashFile(outputPath, s.maxFileBytes)
			}
		}
		if conversionErr == nil {
			intent.OutputSHA256 = outputSHA
		}
		operationCtx := ctx
		var operationCancel context.CancelFunc
		uploadAttempted := false
		destructiveOperation := false
		if conversionErr == nil {
			uploadAttempted = true
			destructiveOperation, operationCtx, operationCancel, conversionErr = s.uploadDerivative(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, plan, before, hadBefore, expectedBook, policy.MainFormat, verifiedMain, verifiedMainSHA, workspace, outputPath, canonicalName, options, intent, replacementAuthorizedState)
			if destructiveOperation {
				savedBook.ReplacementInProgress = true
			}
		}
		if conversionErr == nil {
			verified, verifyErr := s.client.GetLibraryBook(operationCtx, libraryID, bookID)
			if verifyErr != nil {
				conversionErr = verifyErr
			} else if verifiedFile, exists := findUploadedFile(verified.Files, plan.Format, desiredOutputName(canonicalName, plan.Format)); !exists {
				conversionErr = ErrVerification
			} else if conversionErr = s.verifyUploadedFile(operationCtx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, before, hadBefore, verifiedFile, outputSHA, workspace, verified.Files, verifiedMain.ID); conversionErr != nil {
			} else if conversionErr = s.revalidateCanonicalSource(operationCtx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, policy.MainFormat, verifiedMain, verifiedMainSHA, mainPath, workspace, plan.Format, verified); conversionErr != nil {
			} else {
				if stateErr := s.commitDerived(operationCtx, state.DerivedState{LibraryID: libraryID, BookID: bookID, Format: plan.Format, GrimmoryFileID: verifiedFile.ID, SourceSHA256: verifiedMainSHA, OutputSHA256: outputSHA, GenerationFingerprint: plan.GenerationFingerprint, TrustedMTime: verifiedFile.MTime, HasMTime: verifiedFile.TrustedMTime, GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}, intent.ReplacementTag); stateErr != nil {
					conversionErr = fmt.Errorf("%w: %v", ErrState, stateErr)
				} else {
					item.Status = "uploaded"
					if intent.ReplacementTag != "" {
						pendingCleanupTag = intent.ReplacementTag
						savedBook.ReplacementInProgressTag = intent.ReplacementTag
						replacementAuthorizedState = true
					}
					if destructiveOperation || (isReplacementOwner && savedBook.ReplacementInProgress) {
						savedBook.ReplacementInProgress = false
						replacementAuthorizedState = pendingCleanupTag != ""
						phaseCleared = true
					}
					expectedBook = verified
				}
			}
		}
		if operationCancel != nil {
			if completionCancel != nil {
				completionCancel()
			}
			completionCtx, completionCancel = operationCtx, operationCancel
		}
		if withinWorkspace(workspace, outputPath) {
			_ = os.Remove(outputPath)
		}
		if conversionErr != nil {
			failed = true
			if uploadAttempted {
				stopFurther = true
			}
			if errors.Is(conversionErr, ErrSafeReplacementUnavailable) {
				blocked = true
			}
			if firstFailure == nil {
				firstFailure = conversionErr
			}
			item.Error = codeForError(conversionErr, "derivative_failed")
		}
		result.Derivatives = append(result.Derivatives, item)
	}
	if failed {
		result.Status = "partial"
		if blocked {
			result.Error = SafeReplacementUnavailableCode
			firstFailure = ErrSafeReplacementUnavailable
		} else {
			result.Error = "derivative_failed"
		}
		return result, newPartialError(firstFailure)
	}
	finalCtx := ctx
	if completionCancel != nil {
		finalCtx = completionCtx
	}
	completionBook, completionErr := s.client.GetLibraryBook(finalCtx, libraryID, bookID)
	if completionErr != nil {
		result.Status, result.Error = "partial", codeForError(completionErr, "verification_failed")
		return result, newPartialError(completionErr)
	}
	if !sameFileInventory(completionBook, expectedBook) {
		result.Status, result.Error = "partial", SafeReplacementUnavailableCode
		return result, newPartialError(ErrSafeReplacementUnavailable)
	}
	if pendingCleanupTag != "" {
		if err := s.markPendingReplacementCleanup(finalCtx, libraryID, bookID, pendingCleanupTag); err != nil {
			result.Status, result.Error = "partial", "state_write_failed"
			return result, fmt.Errorf("%w: %v", ErrState, err)
		}
		if err := s.clearReplacementTag(finalCtx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, completionBook, pendingCleanupTag); err != nil {
			result.Status, result.Error = "partial", "replacement_tag_failed"
			return result, fmt.Errorf("%w: %v", ErrReplacementTagMutation, err)
		}
		canonicalState.PendingReplacementTag = pendingCleanupTag
		canonicalState.ReplacementInProgressTag = ""
	}
	canonicalState.ReplacementInProgress = false
	canonicalState.LastSuccessfulSync = time.Now().UTC()
	canonicalState.UpdatedAt = time.Now().UTC()
	if err := s.store.SetBook(finalCtx, canonicalState); err != nil {
		result.Status, result.Error = "partial", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	if pendingCleanupTag != "" {
		if err := s.clearPendingReplacementTag(finalCtx, libraryID, bookID, pendingCleanupTag); err != nil {
			result.Status, result.Error = "partial", "state_write_failed"
			return result, fmt.Errorf("%w: %v", ErrState, err)
		}
	}
	result.Status = "completed"
	if err := s.SetFailureTag(finalCtx, libraryID, bookID, false); err != nil {
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

func (s *Service) replacementAllowed(book grimmory.Book, options SyncOptions) bool {
	if hasTag(book, s.ignoreTag) {
		return false
	}
	return s.existingPolicy == "replace" || (s.replacementTag != "" && hasTag(book, s.replacementTag)) || options.Force
}

func (s *Service) replacementAllowedForState(book grimmory.Book, savedBook state.BookState, options SyncOptions) bool {
	if savedBook.ReplacementInProgress || savedBook.ReplacementInProgressTag != "" {
		return true
	}
	return s.replacementAllowed(book, options)
}

func (s *Service) replacementAllowedForFormat(book grimmory.Book, savedBook state.BookState, ownerFormat, format string, options SyncOptions) bool {
	if savedBook.ReplacementInProgress && ownerFormat != "" && normalizeFormat(format) != ownerFormat {
		savedBook.ReplacementInProgress = false
	}
	return s.replacementAllowedForState(book, savedBook, options)
}

func replacementTargetMatches(files []grimmory.File, format string, intent state.DerivedUploadIntent) bool {
	if intent.ReplacementTargetID == "" || normalizeFormat(intent.ReplacementTargetFormat) != normalizeFormat(format) {
		return false
	}
	targets := filesForFormat(files, format)
	if len(targets) != 1 || !uniqueFileIDs(files) {
		return false
	}
	return sameFileIdentity(targets[0], grimmory.File{
		ID: intent.ReplacementTargetID, Name: intent.ReplacementTargetName,
		Format: intent.ReplacementTargetFormat, Type: intent.ReplacementTargetType,
	})
}

func replacementTargetMissing(files []grimmory.File, format string, intent state.DerivedUploadIntent) bool {
	return intent.ReplacementTargetID != "" && normalizeFormat(intent.ReplacementTargetFormat) == normalizeFormat(format) && uniqueFileIDs(files) && len(filesForFormat(files, format)) == 0
}

func replacementOwnerFormat(savedBook state.BookState, intents map[string]state.DerivedUploadIntent) (string, bool) {
	if !savedBook.ReplacementInProgress {
		return "", true
	}
	formats := make([]string, 0, len(intents))
	for format := range intents {
		formats = append(formats, format)
	}
	sort.Strings(formats)
	prepared := make([]string, 0, len(formats))
	for _, format := range formats {
		intent := intents[format]
		candidate := normalizeFormat(intent.ReplacementTargetFormat)
		if intent.ReplacementTargetID == "" || candidate == "" || candidate != normalizeFormat(format) {
			continue
		}
		prepared = append(prepared, candidate)
	}
	if len(prepared) == 1 {
		return prepared[0], true
	}
	if len(prepared) > 1 {
		return "", false
	}
	if len(formats) == 1 {
		return normalizeFormat(formats[0]), true
	}
	return "", false
}

func prioritizeReplacementOwner(savedBook state.BookState, intents map[string]state.DerivedUploadIntent, plans []DerivativePlan) ([]DerivativePlan, string, bool) {
	owner, ok := replacementOwnerFormat(savedBook, intents)
	if !ok {
		return plans, "", false
	}
	if !savedBook.ReplacementInProgress {
		return plans, "", true
	}
	ownerIndex := -1
	for index, plan := range plans {
		if normalizeFormat(plan.Format) == owner {
			ownerIndex = index
			break
		}
	}
	if ownerIndex < 0 {
		return plans, owner, false
	}
	if ownerIndex == 0 {
		return plans, owner, true
	}
	ordered := make([]DerivativePlan, 0, len(plans))
	ordered = append(ordered, plans[ownerIndex])
	ordered = append(ordered, plans[:ownerIndex]...)
	ordered = append(ordered, plans[ownerIndex+1:]...)
	return ordered, owner, true
}

func hasTag(book grimmory.Book, wanted string) bool {
	if wanted == "" {
		return false
	}
	for _, tag := range book.Metadata.Tags {
		if tag == wanted {
			return true
		}
	}
	return false
}

func (s *Service) refreshBookBeforeNextDerivative(ctx context.Context, libraryID, bookID string) (grimmory.Book, bool, error) {
	book, err := s.client.GetLibraryBook(ctx, libraryID, bookID)
	if err != nil {
		return grimmory.Book{}, false, err
	}
	if book.ID != bookID || book.LibraryID != libraryID {
		return grimmory.Book{}, false, fmt.Errorf("%w: book identity changed before next derivative", ErrSafeReplacementUnavailable)
	}
	return book, hasTag(book, s.ignoreTag), nil
}

func (s *Service) getUploadIntents(ctx context.Context, libraryID, bookID string) (map[string]state.DerivedUploadIntent, error) {
	store, ok := s.store.(UploadIntentStore)
	if !ok {
		return map[string]state.DerivedUploadIntent{}, nil
	}
	return store.GetDerivedUploadIntents(ctx, libraryID, bookID)
}

func (s *Service) setUploadIntent(ctx context.Context, value state.DerivedUploadIntent) error {
	store, ok := s.store.(UploadIntentStore)
	if !ok {
		return nil
	}
	return store.SetDerivedUploadIntent(ctx, value)
}

func (s *Service) commitDerived(ctx context.Context, value state.DerivedState, pendingTag string) error {
	if store, ok := s.store.(DerivedCommitStore); ok {
		return store.CommitDerived(ctx, value, pendingTag)
	}
	if err := s.store.SetDerived(ctx, value); err != nil {
		return err
	}
	book, _, err := s.store.Get(ctx, value.LibraryID, value.BookID)
	if err != nil {
		return err
	}
	if pendingTag != "" {
		book.ReplacementInProgressTag = pendingTag
	}
	book.ReplacementInProgress = false
	book.UpdatedAt = time.Now().UTC()
	return s.store.SetBook(ctx, book)
}

func (s *Service) markReplacementInProgress(ctx context.Context, libraryID, bookID, tag string) error {
	if store, ok := s.store.(ReplacementProgressStore); ok {
		return store.MarkReplacementInProgress(ctx, libraryID, bookID, tag)
	}
	book, _, err := s.store.Get(ctx, libraryID, bookID)
	if err != nil {
		return err
	}
	book.ReplacementInProgress = true
	if tag != "" {
		book.ReplacementInProgressTag = tag
	}
	book.UpdatedAt = time.Now().UTC()
	return s.store.SetBook(ctx, book)
}

func (s *Service) prepareReplacement(ctx context.Context, intent state.DerivedUploadIntent) error {
	if store, ok := s.store.(ReplacementPreparationStore); ok {
		return store.PrepareReplacement(ctx, intent)
	}
	if _, ok := s.store.(UploadIntentStore); !ok {
		return errors.New("durable replacement intent is unsupported")
	}
	if err := s.setUploadIntent(ctx, intent); err != nil {
		return err
	}
	return s.markReplacementInProgress(ctx, intent.LibraryID, intent.BookID, intent.ReplacementTag)
}

func (s *Service) markPendingReplacementCleanup(ctx context.Context, libraryID, bookID, tag string) error {
	if store, ok := s.store.(PendingReplacementMarkerStore); ok {
		return store.MarkPendingReplacementCleanup(ctx, libraryID, bookID, tag)
	}
	book, _, err := s.store.Get(ctx, libraryID, bookID)
	if err != nil {
		return err
	}
	if book.PendingReplacementTag != tag && book.ReplacementInProgressTag != tag {
		return errors.New("replacement authorization is not in progress")
	}
	book.PendingReplacementTag = tag
	book.ReplacementInProgressTag = ""
	book.ReplacementInProgress = false
	book.UpdatedAt = time.Now().UTC()
	return s.store.SetBook(ctx, book)
}

func (s *Service) clearPendingReplacementTag(ctx context.Context, libraryID, bookID, tag string) error {
	if store, ok := s.store.(PendingReplacementStore); ok {
		return store.ClearPendingReplacementTag(ctx, libraryID, bookID, tag)
	}
	book, _, err := s.store.Get(ctx, libraryID, bookID)
	if err != nil {
		return err
	}
	if book.PendingReplacementTag == "" || book.PendingReplacementTag != tag {
		return nil
	}
	book.PendingReplacementTag = ""
	book.UpdatedAt = time.Now().UTC()
	return s.store.SetBook(ctx, book)
}

func (s *Service) finishPendingReplacementCleanup(ctx context.Context, libraryID, bookID string, book grimmory.Book, savedBook state.BookState, result Result) (Result, error) {
	reference := grimmory.BookReference{LibraryID: libraryID, BookID: bookID}
	if err := s.clearReplacementTag(ctx, reference, book, savedBook.PendingReplacementTag); err != nil {
		result.Status, result.Error = "partial", "replacement_tag_failed"
		return result, fmt.Errorf("%w: %v", ErrReplacementTagMutation, err)
	}
	savedBook.ReplacementInProgressTag = ""
	savedBook.ReplacementInProgress = false
	savedBook.LastSuccessfulSync = time.Now().UTC()
	savedBook.UpdatedAt = time.Now().UTC()
	if err := s.store.SetBook(ctx, savedBook); err != nil {
		result.Status, result.Error = "partial", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	if err := s.clearPendingReplacementTag(ctx, libraryID, bookID, savedBook.PendingReplacementTag); err != nil {
		result.Status, result.Error = "partial", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	if err := s.SetFailureTag(ctx, libraryID, bookID, false); err != nil {
		result.Status, result.Error = "partial", "failure_tag_failed"
		return result, fmt.Errorf("%w: %v", ErrFailureTagMutation, err)
	}
	result.Status = "completed"
	return result, nil
}

func (s *Service) replacementCompletionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := 2 * time.Minute
	if s.conversionTimeout > 0 && s.conversionTimeout < timeout {
		timeout = s.conversionTimeout
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func verifyLocalOutputUnchanged(filePath, expectedSHA string, maxBytes int64) error {
	if filePath == "" || expectedSHA == "" {
		return fmt.Errorf("%w: local output content is unavailable", ErrSafeReplacementUnavailable)
	}
	actual, _, err := convert.HashFile(filePath, maxBytes)
	if err != nil {
		return err
	}
	if !strings.EqualFold(actual, expectedSHA) {
		return fmt.Errorf("%w: local output content changed", ErrSafeReplacementUnavailable)
	}
	return nil
}

func uploadIntentFor(libraryID, bookID string, plan DerivativePlan, canonicalName, canonicalSHA string, canonical grimmory.File, stableInventory string, tagAuthorized bool, replacementTag string) state.DerivedUploadIntent {
	intent := state.DerivedUploadIntent{
		LibraryID: libraryID, BookID: bookID, Format: plan.Format,
		OutputName: desiredOutputName(canonicalName, plan.Format), OutputSHA256: "pending",
		SourceSHA256: canonicalSHA, GenerationFingerprint: plan.GenerationFingerprint,
		SourceFileID: canonical.ID, SourceFileName: canonical.Name, SourceFormat: canonical.Format,
		StableInventoryFingerprint: stableInventory, UpdatedAt: time.Now().UTC(),
	}
	if tagAuthorized {
		intent.ReplacementTag = replacementTag
	}
	return intent
}

func preservePreparedReplacementEvidence(prepared, regenerated state.DerivedUploadIntent) state.DerivedUploadIntent {
	regenerated.ReplacementTag = prepared.ReplacementTag
	regenerated.ReplacementTargetID = prepared.ReplacementTargetID
	regenerated.ReplacementTargetName = prepared.ReplacementTargetName
	regenerated.ReplacementTargetFormat = prepared.ReplacementTargetFormat
	regenerated.ReplacementTargetType = prepared.ReplacementTargetType
	return regenerated
}

func adoptedDerivedState(libraryID, bookID string, plan DerivativePlan, candidate grimmory.File, intent state.DerivedUploadIntent) state.DerivedState {
	return state.DerivedState{
		LibraryID: libraryID, BookID: bookID, Format: plan.Format,
		GrimmoryFileID: candidate.ID, SourceSHA256: intent.SourceSHA256,
		OutputSHA256: intent.OutputSHA256, GenerationFingerprint: intent.GenerationFingerprint,
		TrustedMTime: candidate.MTime, HasMTime: candidate.TrustedMTime,
		GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
}

// recoverableIntentCandidate adopts only a unique candidate whose complete
// scoped evidence still agrees with the intent. It intentionally performs no
// remote mutation; an absent, ambiguous, or mismatched candidate is simply
// left for the normal planner to handle.
func (s *Service) recoverableIntentCandidate(ctx context.Context, reference grimmory.BookReference, workspace string, initialBook grimmory.Book, canonical grimmory.File, plan DerivativePlan, intent state.DerivedUploadIntent, canonicalSHA, canonicalName, canonicalFormat string) (grimmory.File, grimmory.Book, bool, error) {
	expectedName := desiredOutputName(canonicalName, plan.Format)
	if normalizeFormat(intent.Format) != normalizeFormat(plan.Format) || !sameNormalizedName(intent.OutputName, expectedName) || intent.OutputSHA256 == "" || intent.SourceSHA256 == "" || intent.GenerationFingerprint == "" {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	if canonicalSHA == "" || !strings.EqualFold(intent.SourceSHA256, canonicalSHA) || intent.GenerationFingerprint != plan.GenerationFingerprint {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	if intent.SourceFileID != "" && intent.SourceFileID != canonical.ID {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	if intent.SourceFileName != "" && !sameNormalizedName(intent.SourceFileName, canonical.Name) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	if intent.SourceFormat != "" && normalizeFormat(intent.SourceFormat) != normalizeFormat(canonicalFormat) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	expectedStableInventory := intent.StableInventoryFingerprint
	if expectedStableInventory == "" {
		// Legacy rows did not persist this evidence. Derive it from the scoped
		// observation, while still requiring every candidate check below.
		expectedStableInventory = stableInventoryFingerprint(initialBook.Files, plan.Format)
	}
	if expectedStableInventory == "" || stableInventoryFingerprint(initialBook.Files, plan.Format) != expectedStableInventory {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.File{}, grimmory.Book{}, false, err
	}
	if current.ID != reference.BookID || current.LibraryID != reference.LibraryID || !uniqueFileIDs(current.Files) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	currentCanonical := filesForFormat(current.Files, canonicalFormat)
	if len(currentCanonical) != 1 || !sameFileIdentity(currentCanonical[0], canonical) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	if stableInventoryFingerprint(current.Files, plan.Format) != expectedStableInventory {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	candidate, ok := uniqueIntentCandidate(current.Files, plan.Format, expectedName, currentCanonical[0].ID)
	if !ok {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	outputPath, outputSHA, err := s.download(ctx, reference, plan.Format, workspace, "adoption-output-"+normalizeFormat(plan.Format))
	if outputPath != "" {
		defer os.Remove(outputPath)
	}
	if err != nil {
		return grimmory.File{}, grimmory.Book{}, false, err
	}
	if !strings.EqualFold(outputSHA, intent.OutputSHA256) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	sourcePath, downloadedSourceSHA, err := s.download(ctx, reference, canonicalFormat, workspace, "adoption-source-"+normalizeFormat(plan.Format))
	if sourcePath != "" {
		defer os.Remove(sourcePath)
	}
	if err != nil {
		return grimmory.File{}, grimmory.Book{}, false, err
	}
	if !strings.EqualFold(downloadedSourceSHA, intent.SourceSHA256) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	latest, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.File{}, grimmory.Book{}, false, err
	}
	if latest.ID != reference.BookID || latest.LibraryID != reference.LibraryID || !uniqueFileIDs(latest.Files) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	latestCanonical := filesForFormat(latest.Files, canonicalFormat)
	if len(latestCanonical) != 1 || !sameFileIdentity(latestCanonical[0], canonical) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	if !sameFileInventory(latest, current) || stableInventoryFingerprint(latest.Files, plan.Format) != expectedStableInventory {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	latestCandidate, ok := uniqueIntentCandidate(latest.Files, plan.Format, expectedName, latestCanonical[0].ID)
	if !ok || latestCandidate.ID != candidate.ID {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	return candidate, latest, true, nil
}

func uniqueIntentCandidate(files []grimmory.File, format, expectedName, canonicalID string) (grimmory.File, bool) {
	candidates := filesForFormat(files, format)
	if len(candidates) != 1 || !uniqueFileIDs(files) {
		return grimmory.File{}, false
	}
	candidate := candidates[0]
	if candidate.ID == "" || candidate.ID == canonicalID || !sameNormalizedName(candidate.Name, expectedName) {
		return grimmory.File{}, false
	}
	return candidate, true
}

func sameNormalizedName(left, right string) bool {
	return left == right
}

func stableInventoryFingerprint(files []grimmory.File, excludedFormat string) string {
	values := make([]inventoryFileKey, 0, len(files))
	for _, file := range files {
		if normalizeFormat(file.Format) == normalizeFormat(excludedFormat) {
			continue
		}
		values = append(values, inventoryFileKey{ID: file.ID, Name: file.Name, Format: normalizeFormat(file.Format), Type: normalizeFormat(file.Type), SizeKB: file.SizeKB, SHA256: strings.ToLower(strings.TrimSpace(file.SHA256))})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].ID != values[j].ID {
			return values[i].ID < values[j].ID
		}
		if values[i].Name != values[j].Name {
			return values[i].Name < values[j].Name
		}
		return values[i].Format < values[j].Format
	})
	encoded, _ := json.Marshal(values)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// uploadDerivative performs the only supported existing-file replacement
// sequence: revalidate the complete book, delete the exact target, probe the
// scoped inventory, and immediately upload the newly converted bytes. Once the
// delete starts, the completion context outlives caller cancellation.
func (s *Service) uploadDerivative(ctx context.Context, reference grimmory.BookReference, plan DerivativePlan, before grimmory.File, hadBefore bool, initialBook grimmory.Book, canonicalFormat string, canonical grimmory.File, canonicalSHA, workspace, outputPath, canonicalName string, options SyncOptions, intent state.DerivedUploadIntent, replacementAuthorized bool) (bool, context.Context, context.CancelFunc, error) {
	if !plan.Blocked || !hadBefore {
		if err := s.setUploadIntent(ctx, intent); err != nil {
			return false, ctx, nil, fmt.Errorf("%w: %v", ErrState, err)
		}
		return false, ctx, nil, s.upload(ctx, reference, plan.Format, outputPath, canonicalName)
	}
	if before.ID == "" || canonical.ID == "" || canonicalSHA == "" {
		return false, ctx, nil, ErrSafeReplacementUnavailable
	}
	if len(filesForFormat(initialBook.Files, plan.Format)) != 1 || len(filesForFormat(initialBook.Files, canonicalFormat)) != 1 || !uniqueFileIDs(initialBook.Files) {
		return false, ctx, nil, fmt.Errorf("%w: initial replacement inventory is ambiguous", ErrSafeReplacementUnavailable)
	}
	if err := verifyLocalOutputUnchanged(outputPath, intent.OutputSHA256, s.maxFileBytes); err != nil {
		return false, ctx, nil, err
	}
	current, err := s.revalidateReplacementInventory(ctx, reference, plan.Format, before, canonicalFormat, canonical, canonicalSHA, workspace, options, true, true, replacementAuthorized)
	if err != nil {
		return false, ctx, nil, err
	}
	target := currentTarget(current.Files, plan.Format)
	intent.ReplacementTargetID = target.ID
	intent.ReplacementTargetName = target.Name
	intent.ReplacementTargetFormat = target.Format
	intent.ReplacementTargetType = target.Type
	if err := s.prepareReplacement(ctx, intent); err != nil {
		return false, ctx, nil, fmt.Errorf("%w: %v", ErrState, err)
	}
	if err := ctx.Err(); err != nil {
		return false, ctx, nil, err
	}
	// Preflight has authorized this exact replacement. Keep that decision for
	// the completion phase so a tag/ignore metadata change cannot strand the
	// book after the old derivative has been deleted.
	replacementAuthorized = true
	completionCtx, cancel := s.replacementCompletionContext(ctx)
	deleteErr := s.client.DeleteFileScoped(completionCtx, reference, currentTarget(current.Files, plan.Format).ID)
	if deleteErr != nil {
		if _, _, recoverable, recoveryErr := s.recoverableIntentCandidate(completionCtx, reference, workspace, initialBook, canonical, plan, intent, canonicalSHA, canonicalName, canonicalFormat); recoveryErr != nil {
			cancel()
			return false, ctx, nil, recoveryErr
		} else if recoverable {
			return true, completionCtx, cancel, nil
		}
	}
	if _, err := s.probeReplacementAfterDelete(completionCtx, reference, plan.Format, before, canonicalFormat, canonical, options, replacementAuthorized); err != nil {
		cancel()
		return false, ctx, nil, err
	}
	if err := verifyLocalOutputUnchanged(outputPath, intent.OutputSHA256, s.maxFileBytes); err != nil {
		cancel()
		return false, ctx, nil, err
	}
	if err := s.upload(completionCtx, reference, plan.Format, outputPath, canonicalName); err != nil {
		if _, _, recoverable, recoveryErr := s.recoverableIntentCandidate(completionCtx, reference, workspace, initialBook, canonical, plan, intent, canonicalSHA, canonicalName, canonicalFormat); recoveryErr != nil {
			cancel()
			return true, ctx, nil, recoveryErr
		} else if recoverable {
			return true, completionCtx, cancel, nil
		}
		cancel()
		return true, ctx, nil, err
	}
	return true, completionCtx, cancel, nil
}

func (s *Service) probeReplacementAfterDelete(ctx context.Context, reference grimmory.BookReference, targetFormat string, before grimmory.File, canonicalFormat string, canonical grimmory.File, options SyncOptions, replacementAuthorized bool) (grimmory.Book, error) {
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.Book{}, err
	}
	if err := s.validateReplacementBook(current, reference, targetFormat, before, canonicalFormat, canonical, options, false, true, replacementAuthorized); err != nil {
		return grimmory.Book{}, err
	}
	return current, nil
}

func (s *Service) revalidateReplacementInventory(ctx context.Context, reference grimmory.BookReference, targetFormat string, before grimmory.File, canonicalFormat string, canonical grimmory.File, canonicalSHA, workspace string, options SyncOptions, targetPresent, checkAuthorization, replacementAuthorized bool) (grimmory.Book, error) {
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.Book{}, err
	}
	if err := s.validateReplacementBook(current, reference, targetFormat, before, canonicalFormat, canonical, options, targetPresent, checkAuthorization, replacementAuthorized); err != nil {
		return grimmory.Book{}, err
	}
	if err := s.verifyCanonicalUnchanged(ctx, reference, canonicalFormat, canonicalSHA, workspace, targetFormat); err != nil {
		return grimmory.Book{}, err
	}
	latest, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.Book{}, err
	}
	if !sameFileInventory(latest, current) {
		return grimmory.Book{}, fmt.Errorf("%w: replacement inventory changed during source verification", ErrSafeReplacementUnavailable)
	}
	if err := s.validateReplacementBook(latest, reference, targetFormat, before, canonicalFormat, canonical, options, targetPresent, checkAuthorization, replacementAuthorized); err != nil {
		return grimmory.Book{}, err
	}
	if err := s.verifyCanonicalUnchanged(ctx, reference, canonicalFormat, canonicalSHA, workspace, targetFormat); err != nil {
		return grimmory.Book{}, err
	}
	return latest, nil
}

func (s *Service) validateReplacementBook(current grimmory.Book, reference grimmory.BookReference, targetFormat string, before grimmory.File, canonicalFormat string, canonical grimmory.File, options SyncOptions, targetPresent, checkAuthorization, replacementAuthorized bool) error {
	if current.ID != reference.BookID || current.LibraryID != reference.LibraryID {
		return fmt.Errorf("%w: replacement inventory identity changed", ErrSafeReplacementUnavailable)
	}
	if !uniqueFileIDs(current.Files) {
		return fmt.Errorf("%w: replacement inventory has non-unique file IDs", ErrSafeReplacementUnavailable)
	}
	canonicalFiles := filesForFormat(current.Files, canonicalFormat)
	if len(canonicalFiles) != 1 || !sameFileIdentity(canonicalFiles[0], canonical) || canonicalFiles[0].ID == "" {
		return fmt.Errorf("%w: canonical source changed or is ambiguous", ErrSafeReplacementUnavailable)
	}
	if checkAuthorization && !replacementAuthorized && !s.replacementAllowed(current, options) {
		return fmt.Errorf("%w: replacement authorization changed", ErrSafeReplacementUnavailable)
	}
	targets := filesForFormat(current.Files, targetFormat)
	if targetPresent {
		if len(targets) != 1 || !sameFileIdentity(targets[0], before) || targets[0].ID == canonical.ID {
			return fmt.Errorf("%w: replacement target changed or is ambiguous", ErrSafeReplacementUnavailable)
		}
	} else if len(targets) != 0 {
		return fmt.Errorf("%w: replacement target remains after deletion", ErrSafeReplacementUnavailable)
	}
	return nil
}

func currentTarget(files []grimmory.File, format string) grimmory.File {
	targets := filesForFormat(files, format)
	if len(targets) == 1 {
		return targets[0]
	}
	return grimmory.File{}
}

func uniqueFileIDs(files []grimmory.File) bool {
	seen := make(map[string]struct{}, len(files))
	for _, file := range files {
		if file.ID == "" {
			return false
		}
		if _, exists := seen[file.ID]; exists {
			return false
		}
		seen[file.ID] = struct{}{}
	}
	return true
}

func sameFileIdentity(left, right grimmory.File) bool {
	return left.ID != "" && left.ID == right.ID && left.Name == right.Name && normalizeFormat(left.Format) == normalizeFormat(right.Format) && normalizeFormat(left.Type) == normalizeFormat(right.Type)
}

func (s *Service) revalidateCanonicalSource(ctx context.Context, reference grimmory.BookReference, canonicalFormat string, canonical grimmory.File, canonicalSHA, localCanonicalPath, workspace, targetFormat string, expectedBook grimmory.Book) error {
	if err := verifyLocalCanonicalUnchanged(localCanonicalPath, canonicalSHA, s.maxFileBytes); err != nil {
		return err
	}
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return err
	}
	if !sameFileInventory(current, expectedBook) {
		return fmt.Errorf("%w: derivative inventory changed during canonical validation", ErrSafeReplacementUnavailable)
	}
	if err := validateCanonicalBook(current, reference, canonicalFormat, canonical); err != nil {
		return err
	}
	if err := s.verifyCanonicalUnchanged(ctx, reference, canonicalFormat, canonicalSHA, workspace, targetFormat); err != nil {
		return err
	}
	latest, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return err
	}
	if !sameFileInventory(latest, current) {
		return fmt.Errorf("%w: canonical inventory changed during upload verification", ErrSafeReplacementUnavailable)
	}
	if err := validateCanonicalBook(latest, reference, canonicalFormat, canonical); err != nil {
		return err
	}
	if err := s.verifyCanonicalUnchanged(ctx, reference, canonicalFormat, canonicalSHA, workspace, targetFormat); err != nil {
		return err
	}
	if err := verifyLocalCanonicalUnchanged(localCanonicalPath, canonicalSHA, s.maxFileBytes); err != nil {
		return err
	}
	return nil
}

func validateCanonicalBook(current grimmory.Book, reference grimmory.BookReference, canonicalFormat string, canonical grimmory.File) error {
	if current.ID != reference.BookID || current.LibraryID != reference.LibraryID {
		return fmt.Errorf("%w: canonical book identity changed", ErrSafeReplacementUnavailable)
	}
	if !uniqueFileIDs(current.Files) {
		return fmt.Errorf("%w: canonical inventory has non-unique file IDs", ErrSafeReplacementUnavailable)
	}
	canonicalFiles := filesForFormat(current.Files, canonicalFormat)
	if len(canonicalFiles) != 1 || !sameFileIdentity(canonicalFiles[0], canonical) {
		return fmt.Errorf("%w: canonical source changed or is ambiguous", ErrSafeReplacementUnavailable)
	}
	return nil
}

func (s *Service) verifyCanonicalUnchanged(ctx context.Context, reference grimmory.BookReference, format, canonicalSHA, workspace, targetFormat string) error {
	filePath, currentSHA, err := s.download(ctx, reference, format, workspace, "revalidate-canonical-"+normalizeFormat(targetFormat))
	if filePath != "" {
		defer os.Remove(filePath)
	}
	if err != nil {
		return err
	}
	if !strings.EqualFold(currentSHA, canonicalSHA) {
		return fmt.Errorf("%w: canonical source content changed", ErrSafeReplacementUnavailable)
	}
	return nil
}

func verifyLocalCanonicalUnchanged(filePath, expectedSHA string, maxBytes int64) error {
	if filePath == "" || expectedSHA == "" {
		return fmt.Errorf("%w: canonical local content is unavailable", ErrSafeReplacementUnavailable)
	}
	actualSHA, _, err := convert.HashFile(filePath, maxBytes)
	if err != nil {
		return err
	}
	if !strings.EqualFold(actualSHA, expectedSHA) {
		return fmt.Errorf("%w: local canonical content changed", ErrSafeReplacementUnavailable)
	}
	return nil
}

type inventoryFileKey struct {
	ID     string
	Name   string
	Format string
	Type   string
	SizeKB int64
	SHA256 string
}

func sameFileInventory(left, right grimmory.Book) bool {
	if len(left.Files) != len(right.Files) {
		return false
	}
	counts := make(map[inventoryFileKey]int, len(left.Files))
	for _, file := range left.Files {
		counts[inventoryFileKey{ID: file.ID, Name: file.Name, Format: normalizeFormat(file.Format), Type: normalizeFormat(file.Type), SizeKB: file.SizeKB, SHA256: strings.ToLower(file.SHA256)}]++
	}
	for _, file := range right.Files {
		key := inventoryFileKey{ID: file.ID, Name: file.Name, Format: normalizeFormat(file.Format), Type: normalizeFormat(file.Type), SizeKB: file.SizeKB, SHA256: strings.ToLower(file.SHA256)}
		if counts[key] == 0 {
			return false
		}
		counts[key]--
	}
	return true
}

func (s *Service) clearReplacementTag(ctx context.Context, reference grimmory.BookReference, expected grimmory.Book, tag string) error {
	if tag == "" {
		return errors.New("replacement tag is empty")
	}
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return err
	}
	if current.ID != reference.BookID || current.LibraryID != reference.LibraryID {
		return fmt.Errorf("%w: replacement book identity changed before tag cleanup", ErrSafeReplacementUnavailable)
	}
	if !sameFileInventory(current, expected) {
		return fmt.Errorf("%w: replacement inventory changed before tag cleanup", ErrSafeReplacementUnavailable)
	}
	if !hasTag(current, tag) {
		return nil
	}
	tagger, ok := s.client.(FailureTagger)
	if !ok {
		return errors.New("replacement tag mutation is unsupported")
	}
	if err := tagger.RemoveBookTagScoped(ctx, reference, tag); err != nil {
		// A remote error may have been returned after the tag was removed. Restore
		// the authorization idempotently before reporting cleanup failure.
		if restoreErr := tagger.AddBookTagScoped(context.WithoutCancel(ctx), reference, tag); restoreErr != nil {
			return errors.Join(err, fmt.Errorf("restore replacement tag after ambiguous removal: %w", restoreErr))
		}
		return err
	}
	// SetBookTagScoped verifies the metadata mutation, while this final scoped
	// read ensures the one-shot cleanup did not race a file inventory change.
	after, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		if restoreErr := tagger.AddBookTagScoped(context.WithoutCancel(ctx), reference, tag); restoreErr != nil {
			return errors.Join(err, fmt.Errorf("restore replacement tag after cleanup verification: %w", restoreErr))
		}
		return err
	}
	if sameFileInventory(after, expected) {
		return nil
	}
	// Retain the authorization if the inventory changed during the mutation.
	// The lock-aware tag client changes only this tag and restores tagsLocked.
	if restoreErr := tagger.AddBookTagScoped(context.WithoutCancel(ctx), reference, tag); restoreErr != nil {
		return errors.Join(ErrSafeReplacementUnavailable, restoreErr)
	}
	return fmt.Errorf("%w: replacement inventory changed during tag cleanup", ErrSafeReplacementUnavailable)
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
	Blocked               bool
}

// PlanDerivatives returns actions for configured outputs. Existing derivatives
// are marked blocked when they need rebuilding unless the caller explicitly
// authorizes replacement through force or a replacement policy. The book
// checkpoint argument is retained for call-site compatibility, but derivative
// ownership is decided from each derivative's own evidence. An optional
// canonical name enables the independent output-name check needed for legacy v1
// fingerprint compatibility.
func PlanDerivatives(files []grimmory.File, outputs []string, mainFormat, canonicalSHA string, saved map[string]state.DerivedState, canonicalMTime time.Time, canonicalTrusted, canonicalRecreated, force, _ bool, desiredFingerprints map[string]string, canonicalNames ...string) []DerivativePlan {
	canonicalName := ""
	if len(canonicalNames) > 0 {
		canonicalName = canonicalNames[0]
	}
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
		if !tracked {
			result = append(result, derivativePlan(format, "rebuild", "state_missing", desiredFingerprints))
			continue
		}
		if !completeDerivedState(previous) {
			result = append(result, derivativePlan(format, "rebuild", "state_incomplete", desiredFingerprints))
			continue
		}
		if previous.Format != format {
			result = append(result, derivativePlan(format, "rebuild", "output_format_changed", desiredFingerprints))
			continue
		}
		if existing.ID != "" && previous.GrimmoryFileID != existing.ID {
			result = append(result, derivativePlan(format, "rebuild", "output_identity_changed", desiredFingerprints))
			continue
		}
		if canonicalName != "" && existing.Name != desiredOutputName(canonicalName, format) {
			result = append(result, derivativePlan(format, "rebuild", "output_name_changed", desiredFingerprints))
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
			legacyNameMatches := canonicalName != "" && existing.Name == desiredOutputName(canonicalName, format)
			if previous.GenerationFingerprint != desired && !legacyV1FingerprintCompatible(previous.GenerationFingerprint, legacyNameMatches) {
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
	return DerivativePlan{
		Format: format, Action: action, Reason: reason,
		GenerationFingerprint: desiredFingerprints[format],
		Blocked:               action == "rebuild",
	}
}

const generationFingerprintVersion = "v1"

// DesiredGenerationFingerprints returns a deterministic checkpoint for each
// configured output. The book argument is retained for call-site compatibility;
// only values passed to conversion/upload participate in the checkpoint.
func DesiredGenerationFingerprints(_ grimmory.Book, canonicalSHA, sourceName string, outputs []string) map[string]string {
	result := make(map[string]string, len(outputs))
	for _, format := range outputs {
		format = normalizeFormat(format)
		if format == "" {
			continue
		}
		result[format] = GenerationFingerprint(grimmory.Book{}, canonicalSHA, sourceName, format)
	}
	return result
}

// GenerationFingerprint returns the versioned identity of a desired output
// from the canonical content hash, desired output name, and target format.
func GenerationFingerprint(_ grimmory.Book, canonicalSHA, sourceName, targetFormat string) string {
	targetFormat = normalizeFormat(targetFormat)
	projection := generationFingerprintProjection{
		CanonicalContentIdentity: strings.ToLower(strings.TrimSpace(canonicalSHA)),
		DesiredOutputName:        desiredOutputName(sourceName, targetFormat),
		TargetFormat:             targetFormat,
	}
	encoded, _ := json.Marshal(projection)
	digest := sha256.Sum256(encoded)
	return generationFingerprintVersion + ":" + hex.EncodeToString(digest[:])
}

func legacyV1FingerprintCompatible(value string, coreIdentityMatches bool) bool {
	if !coreIdentityMatches || !strings.HasPrefix(value, "v1:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "v1:"))
	return err == nil && len(strings.TrimPrefix(value, "v1:")) == sha256.Size*2
}

type generationFingerprintProjection struct {
	CanonicalContentIdentity string `json:"canonicalContentIdentity"`
	DesiredOutputName        string `json:"desiredOutputName"`
	TargetFormat             string `json:"targetFormat"`
}

func desiredOutputName(sourceName, targetFormat string) string {
	return artifactname.OutputFilename(sourceName, targetFormat)
}

func (s *Service) upload(ctx context.Context, reference grimmory.BookReference, format, filePath, sourceName string) error {
	return s.client.UploadFileNamedScoped(ctx, reference, format, filePath, desiredOutputName(sourceName, format))
}

func completeDerivedState(value state.DerivedState) bool {
	return value.BookID != "" && value.Format != "" && value.GrimmoryFileID != "" && value.SourceSHA256 != "" && value.OutputSHA256 != "" && !value.GeneratedAt.IsZero()
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

// verifyUploadedFile requires locally hashed content from the exact post-upload
// file before its state can become authoritative. Inventory checksums are
// advisory and are never sufficient on their own.
func (s *Service) verifyUploadedFile(ctx context.Context, reference grimmory.BookReference, before grimmory.File, hadBefore bool, after grimmory.File, outputSHA, workspace string, inventory []grimmory.File, canonicalID string) error {
	if after.ID == "" || (hadBefore && after.ID == before.ID) || after.ID == canonicalID || countFileID(inventory, after.ID) != 1 {
		return ErrVerification
	}
	filePath, downloadedSHA, err := s.download(ctx, reference, after.Format, workspace, "verify-"+normalizeFormat(after.Format))
	if filePath != "" {
		defer os.Remove(filePath)
	}
	if err != nil || !strings.EqualFold(downloadedSHA, outputSHA) {
		return ErrVerification
	}
	return nil
}

func countFileID(files []grimmory.File, wanted string) int {
	if wanted == "" {
		return 0
	}
	count := 0
	for _, file := range files {
		if file.ID == wanted {
			count++
		}
	}
	return count
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

func findUploadedFile(files []grimmory.File, format, desiredName string) (grimmory.File, bool) {
	candidates := filesForFormat(files, format)
	if len(candidates) != 1 {
		return grimmory.File{}, false
	}
	candidate := candidates[0]
	if candidate.Name != desiredName {
		return grimmory.File{}, false
	}
	return candidate, true
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
	case errors.Is(err, ErrSafeReplacementUnavailable):
		return SafeReplacementUnavailableCode
	case errors.Is(err, ErrReplacementTagMutation):
		return "replacement_tag_failed"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return fallback
	}
}

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
	GetDerivedUploadReceipts(context.Context, string, string) (map[string]state.DerivedUploadReceipt, error)
	SetDerivedUploadReceipt(context.Context, state.DerivedUploadReceipt) error
	CommitDerived(context.Context, state.DerivedState) error
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
	if hasTag(book, s.ignoreTag) {
		result.Status = "ignored"
		return result, nil
	}
	policy, err := s.resolvePolicy(ctx, libraryID)
	if err != nil {
		result.Error = "library_policy_failed"
		return result, err
	}
	replacementAllowed := s.replacementAllowed(book, options)
	uploadReceipts, err := s.store.GetDerivedUploadReceipts(ctx, libraryID, bookID)
	if err != nil {
		result.Error = "state_read_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	mainCandidates := filesForFormat(book.Files, policy.MainFormat)
	if len(mainCandidates) > 1 {
		result.Main = ItemResult{Format: policy.MainFormat, Action: "blocked", Status: "blocked", Error: SafeReplacementUnavailableCode}
		result.Error = SafeReplacementUnavailableCode
		return result, ErrSafeReplacementUnavailable
	}
	var mainFile grimmory.File
	hasMain := len(mainCandidates) == 1
	if hasMain {
		mainFile = mainCandidates[0]
	}
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
				candidates := filesForFormat(book.Files, format)
				if len(candidates) > 1 {
					result.Derivatives = append(result.Derivatives, ItemResult{Format: format, SourceFormat: policy.MainFormat, Action: "blocked", Status: "blocked", Reason: "ambiguous_output", Error: SafeReplacementUnavailableCode})
					result.Error = SafeReplacementUnavailableCode
					continue
				}
				if len(candidates) == 1 {
					action = "rebuild"
				}
				item := ItemResult{Format: format, SourceFormat: policy.MainFormat, Action: action, Status: "planned", Reason: "main_would_be_created"}
				if action == "rebuild" && !replacementAllowed {
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
		canonicalState = state.BookState{LibraryID: libraryID, BookID: bookID, MainFormat: policy.MainFormat, CanonicalFormat: policy.MainFormat, CanonicalFileID: mainFile.ID, CanonicalFileName: canonicalName, CanonicalSHA256: canonicalSHA, MetadataFingerprint: mainFile.MetadataFingerprint, CanonicalMTime: canonicalMTime, TrustedMTime: canonicalTrustedMTime, LastSuccessfulSync: savedBook.LastSuccessfulSync, UpdatedAt: time.Now().UTC()}
		if err := s.store.SetBook(ctx, canonicalState); err != nil {
			result.Error = "state_write_failed"
			return result, fmt.Errorf("%w: %v", ErrState, err)
		}
	}
	generationFingerprints := DesiredGenerationFingerprints(book, canonicalSHA, canonicalName, policy.OutputFormats)
	plans := PlanDerivatives(book.Files, policy.OutputFormats, policy.MainFormat, canonicalSHA, savedDerived, canonicalMTime, canonicalTrustedMTime, false, options.Force, false, generationFingerprints, canonicalName)
	if options.DryRun {
		for _, plan := range plans {
			item := ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Status: "planned", Reason: plan.Reason}
			if plan.Unsafe || (plan.Blocked && !replacementAllowed) {
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
	expectedBook := book
	for _, initialPlan := range plans {
		if stopFurther {
			break
		}
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
		if err := validateCanonicalBook(refreshedBook, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, policy.MainFormat, mainFile); err != nil {
			item := ItemResult{Format: initialPlan.Format, SourceFormat: policy.MainFormat, Action: initialPlan.Action, Status: "blocked", Reason: "canonical_changed", Error: SafeReplacementUnavailableCode}
			result.Derivatives = append(result.Derivatives, item)
			failed, blocked, stopFurther = true, true, true
			if firstFailure == nil {
				firstFailure = err
			}
			continue
		}
		if err := verifyLocalCanonicalUnchanged(canonicalPath, canonicalSHA, s.maxFileBytes); err != nil {
			item := ItemResult{Format: initialPlan.Format, SourceFormat: policy.MainFormat, Action: initialPlan.Action, Status: "blocked", Reason: "canonical_changed", Error: codeForError(err, SafeReplacementUnavailableCode)}
			result.Derivatives = append(result.Derivatives, item)
			failed, stopFurther = true, true
			if errors.Is(err, ErrSafeReplacementUnavailable) {
				blocked = true
			}
			if firstFailure == nil {
				firstFailure = err
			}
			continue
		}
		if err := s.verifyCanonicalUnchanged(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, policy.MainFormat, canonicalSHA, workspace, initialPlan.Format); err != nil {
			item := ItemResult{Format: initialPlan.Format, SourceFormat: policy.MainFormat, Action: initialPlan.Action, Status: "blocked", Reason: "canonical_changed", Error: codeForError(err, SafeReplacementUnavailableCode)}
			result.Derivatives = append(result.Derivatives, item)
			failed, stopFurther = true, true
			if errors.Is(err, ErrSafeReplacementUnavailable) {
				blocked = true
			}
			if firstFailure == nil {
				firstFailure = err
			}
			continue
		}
		currentPlans := PlanDerivatives(refreshedBook.Files, []string{initialPlan.Format}, policy.MainFormat, canonicalSHA, savedDerived, canonicalMTime, canonicalTrustedMTime, false, options.Force, false, generationFingerprints, canonicalName)
		if len(currentPlans) != 1 {
			err := fmt.Errorf("%w: no current derivative plan", ErrSafeReplacementUnavailable)
			item := ItemResult{Format: initialPlan.Format, SourceFormat: policy.MainFormat, Action: initialPlan.Action, Status: "blocked", Reason: "current_inventory_unavailable", Error: SafeReplacementUnavailableCode}
			result.Derivatives = append(result.Derivatives, item)
			failed, blocked = true, true
			if firstFailure == nil {
				firstFailure = err
			}
			continue
		}
		plan := currentPlans[0]
		replacementAllowed = s.replacementAllowed(refreshedBook, options)
		item := ItemResult{Format: plan.Format, SourceFormat: policy.MainFormat, Action: plan.Action, Reason: plan.Reason}
		if plan.Unsafe {
			item.Status = "blocked"
			item.Error = SafeReplacementUnavailableCode
			failed, blocked = true, true
			if firstFailure == nil {
				firstFailure = ErrSafeReplacementUnavailable
			}
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		if plan.Action == "unchanged" {
			item.Status = "unchanged"
			result.Derivatives = append(result.Derivatives, item)
			continue
		}
		targetCandidates := filesForFormat(refreshedBook.Files, plan.Format)
		var before grimmory.File
		hadBefore := len(targetCandidates) == 1
		if hadBefore {
			before = targetCandidates[0]
		}
		if receipt, hasReceipt := uploadReceipts[plan.Format]; hasReceipt {
			candidate, recoveredBook, recoverable, recoveryErr := s.recoverableReceiptCandidate(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, workspace, mainFile, plan, receipt, canonicalSHA, canonicalName, policy.MainFormat)
			if recoveryErr != nil {
				failed, stopFurther = true, true
				if firstFailure == nil {
					firstFailure = recoveryErr
				}
				item.Status, item.Error = "failed", codeForError(recoveryErr, "derivative_failed")
				result.Derivatives = append(result.Derivatives, item)
				continue
			}
			if recoverable {
				if err := s.commitDerived(ctx, adoptedDerivedState(libraryID, bookID, plan, candidate, receipt)); err != nil {
					stateErr := fmt.Errorf("%w: %v", ErrState, err)
					failed, stopFurther = true, true
					if firstFailure == nil {
						firstFailure = stateErr
					}
					item.Status, item.Error = "failed", codeForError(stateErr, "state_write_failed")
					result.Derivatives = append(result.Derivatives, item)
					continue
				}
				item.Status, item.Reason = "adopted", "upload_receipt_recovered"
				expectedBook = recoveredBook
				savedDerived[plan.Format] = adoptedDerivedState(libraryID, bookID, plan, candidate, receipt)
				result.Derivatives = append(result.Derivatives, item)
				continue
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
		receipt := uploadReceiptFor(libraryID, bookID, plan, canonicalName, canonicalSHA)
		outputPath, conversionErr := s.convert(ctx, canonicalPath, policy.MainFormat, plan.Format, workspace)
		var outputSHA string
		mutationAttempted := false
		adoptedFromReceipt := false
		if conversionErr == nil {
			if !withinWorkspace(workspace, outputPath) {
				conversionErr = errors.New("converter output escaped workspace")
			} else {
				outputSHA, _, conversionErr = convert.HashFile(outputPath, s.maxFileBytes)
			}
		}
		if conversionErr == nil {
			receipt.OutputSHA256 = outputSHA
		}
		if conversionErr == nil {
			mutationAttempted, conversionErr = s.uploadDerivative(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, plan, before, hadBefore, refreshedBook, policy.MainFormat, mainFile, canonicalSHA, workspace, outputPath, canonicalName, options, receipt)
			if conversionErr != nil && ctx.Err() == nil {
				if candidate, recoveredBook, recoverable, recoveryErr := s.recoverableReceiptCandidate(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, workspace, mainFile, plan, receipt, canonicalSHA, canonicalName, policy.MainFormat); recoveryErr == nil && recoverable {
					adopted := adoptedDerivedState(libraryID, bookID, plan, candidate, receipt)
					if commitErr := s.commitDerived(ctx, adopted); commitErr == nil {
						conversionErr = nil
						item.Status, item.Reason = "adopted", "upload_receipt_recovered"
						expectedBook = recoveredBook
						savedDerived[plan.Format] = adopted
						mutationAttempted = false
						adoptedFromReceipt = true
					}
				}
			}
		}
		if conversionErr == nil && !adoptedFromReceipt {
			verifiedBook, verifyErr := s.client.GetLibraryBook(ctx, libraryID, bookID)
			if verifyErr != nil {
				conversionErr = verifyErr
			} else if verifiedFile, ok := findUploadedFile(verifiedBook.Files, plan.Format, desiredOutputName(canonicalName, plan.Format)); !ok {
				conversionErr = ErrVerification
			} else if conversionErr = s.verifyUploadedFile(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, before, hadBefore, verifiedFile, outputSHA, workspace, verifiedBook.Files, mainFile.ID); conversionErr != nil {
			} else if conversionErr = s.revalidateCanonicalSource(ctx, grimmory.BookReference{LibraryID: libraryID, BookID: bookID}, policy.MainFormat, mainFile, canonicalSHA, canonicalPath, workspace, plan.Format, verifiedBook); conversionErr != nil {
			} else {
				if err := s.commitDerived(ctx, state.DerivedState{LibraryID: libraryID, BookID: bookID, Format: plan.Format, GrimmoryFileID: verifiedFile.ID, SourceSHA256: canonicalSHA, OutputSHA256: outputSHA, GenerationFingerprint: plan.GenerationFingerprint, TrustedMTime: verifiedFile.MTime, HasMTime: verifiedFile.TrustedMTime, GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}); err != nil {
					conversionErr = fmt.Errorf("%w: %v", ErrState, err)
				} else {
					if item.Status != "adopted" {
						item.Status = "uploaded"
					}
					expectedBook = verifiedBook
					savedDerived[plan.Format] = state.DerivedState{LibraryID: libraryID, BookID: bookID, Format: plan.Format, GrimmoryFileID: verifiedFile.ID, SourceSHA256: canonicalSHA, OutputSHA256: outputSHA, GenerationFingerprint: plan.GenerationFingerprint, TrustedMTime: verifiedFile.MTime, HasMTime: verifiedFile.TrustedMTime, GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
				}
			}
		}
		if withinWorkspace(workspace, outputPath) {
			_ = os.Remove(outputPath)
		}
		if conversionErr != nil {
			failed = true
			if mutationAttempted {
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
	completionBook, completionErr := s.client.GetLibraryBook(ctx, libraryID, bookID)
	if completionErr != nil {
		result.Status, result.Error = "partial", codeForError(completionErr, "verification_failed")
		return result, newPartialError(completionErr)
	}
	if !sameFileInventory(completionBook, expectedBook) {
		result.Status, result.Error = "partial", SafeReplacementUnavailableCode
		return result, newPartialError(ErrSafeReplacementUnavailable)
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
	reference := grimmory.BookReference{LibraryID: libraryID, BookID: bookID}
	sourcePath, sourceSHA, err := s.download(ctx, reference, source.Format, workspace, "source")
	if err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "download_source_failed", "download_source_failed"
		return result, err
	}
	if source.SHA256 != "" && !strings.EqualFold(source.SHA256, sourceSHA) {
		result.Main.Status, result.Main.Error, result.Error = "failed", "source_hash_mismatch", "source_hash_mismatch"
		return result, ErrVerification
	}
	if err := s.revalidateMissingMainInventory(ctx, reference, policy.MainFormat, source, sourceSHA, sourcePath, workspace); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "blocked", SafeReplacementUnavailableCode, SafeReplacementUnavailableCode
		return result, err
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
	if err := s.revalidateMissingMainInventory(ctx, reference, policy.MainFormat, source, sourceSHA, sourcePath, workspace); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "blocked", SafeReplacementUnavailableCode, SafeReplacementUnavailableCode
		return result, err
	}
	if err := s.upload(ctx, reference, policy.MainFormat, mainPath, source.Name); err != nil {
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
	if err := s.verifyUploadedFile(ctx, reference, grimmory.File{}, false, verifiedMain, mainSHA, workspace, verifiedBook.Files, ""); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "verification_failed", "verification_failed"
		return result, err
	}
	canonicalName := verifiedMain.Name
	if canonicalName == "" {
		canonicalName = desiredOutputName(source.Name, policy.MainFormat)
	}
	canonicalState := state.BookState{LibraryID: libraryID, BookID: bookID, MainFormat: policy.MainFormat, CanonicalFormat: policy.MainFormat, CanonicalFileID: verifiedMain.ID, CanonicalFileName: canonicalName, CanonicalSHA256: mainSHA, MetadataFingerprint: verifiedMain.MetadataFingerprint, CanonicalMTime: verifiedMain.MTime, TrustedMTime: verifiedMain.TrustedMTime, LastSuccessfulSync: savedBook.LastSuccessfulSync, UpdatedAt: time.Now().UTC()}
	if err := s.store.SetBook(ctx, canonicalState); err != nil {
		result.Main.Status, result.Main.Error, result.Error = "failed", "state_write_failed", "state_write_failed"
		return result, fmt.Errorf("%w: %v", ErrState, err)
	}
	result.Main.Status, result.Main.Action, result.Main.Reason = "created", "created", "main_missing"
	derivativeResult, syncErr := s.Sync(ctx, libraryID, bookID, options)
	result.Derivatives = derivativeResult.Derivatives
	result.Status, result.Error = derivativeResult.Status, derivativeResult.Error
	return result, syncErr
}

func (s *Service) revalidateMissingMainInventory(ctx context.Context, reference grimmory.BookReference, mainFormat string, source grimmory.File, sourceSHA, sourcePath, workspace string) error {
	if err := verifyLocalCanonicalUnchanged(sourcePath, sourceSHA, s.maxFileBytes); err != nil {
		return err
	}
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return err
	}
	if hasTag(current, s.ignoreTag) {
		return fmt.Errorf("%w: ignore authorization appeared before main creation", ErrSafeReplacementUnavailable)
	}
	if current.ID != reference.BookID || current.LibraryID != reference.LibraryID || !uniqueFileIDs(current.Files) {
		return fmt.Errorf("%w: main creation inventory identity changed", ErrSafeReplacementUnavailable)
	}
	if len(filesForFormat(current.Files, mainFormat)) != 0 {
		return fmt.Errorf("%w: main target appeared before creation", ErrSafeReplacementUnavailable)
	}
	sources := filesForFormat(current.Files, source.Format)
	if len(sources) != 1 || !sameFileIdentity(sources[0], source) {
		return fmt.Errorf("%w: main source changed before creation", ErrSafeReplacementUnavailable)
	}
	checkPath, checkSHA, err := s.download(ctx, reference, source.Format, workspace, "revalidate-main-source")
	if checkPath != "" {
		defer os.Remove(checkPath)
	}
	if err != nil {
		return err
	}
	if !strings.EqualFold(checkSHA, sourceSHA) {
		return fmt.Errorf("%w: main source content changed before creation", ErrSafeReplacementUnavailable)
	}
	latest, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return err
	}
	if hasTag(latest, s.ignoreTag) || !sameFileInventory(latest, current) {
		return fmt.Errorf("%w: main creation inventory changed during source verification", ErrSafeReplacementUnavailable)
	}
	if len(filesForFormat(latest.Files, mainFormat)) != 0 {
		return fmt.Errorf("%w: main target appeared before creation", ErrSafeReplacementUnavailable)
	}
	latestSources := filesForFormat(latest.Files, source.Format)
	if len(latestSources) != 1 || !sameFileIdentity(latestSources[0], source) {
		return fmt.Errorf("%w: main source changed before creation", ErrSafeReplacementUnavailable)
	}
	return nil
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

func (s *Service) getUploadReceipts(ctx context.Context, libraryID, bookID string) (map[string]state.DerivedUploadReceipt, error) {
	return s.store.GetDerivedUploadReceipts(ctx, libraryID, bookID)
}

func (s *Service) setUploadReceipt(ctx context.Context, value state.DerivedUploadReceipt) error {
	return s.store.SetDerivedUploadReceipt(ctx, value)
}

func (s *Service) commitDerived(ctx context.Context, value state.DerivedState) error {
	return s.store.CommitDerived(ctx, value)
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

func uploadReceiptFor(libraryID, bookID string, plan DerivativePlan, canonicalName, canonicalSHA string) state.DerivedUploadReceipt {
	return state.DerivedUploadReceipt{
		LibraryID: libraryID, BookID: bookID, Format: plan.Format,
		OutputName: desiredOutputName(canonicalName, plan.Format), OutputSHA256: "pending",
		SourceSHA256: canonicalSHA, GenerationFingerprint: plan.GenerationFingerprint,
		UpdatedAt: time.Now().UTC(),
	}
}

func adoptedDerivedState(libraryID, bookID string, plan DerivativePlan, candidate grimmory.File, receipt state.DerivedUploadReceipt) state.DerivedState {
	return state.DerivedState{
		LibraryID: libraryID, BookID: bookID, Format: plan.Format,
		GrimmoryFileID: candidate.ID, SourceSHA256: receipt.SourceSHA256,
		OutputSHA256: receipt.OutputSHA256, GenerationFingerprint: receipt.GenerationFingerprint,
		TrustedMTime: candidate.MTime, HasMTime: candidate.TrustedMTime,
		GeneratedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
}

// recoverableReceiptCandidate adopts only one uniquely named candidate whose
// downloaded bytes match the receipt. It performs no remote mutation.
func (s *Service) recoverableReceiptCandidate(ctx context.Context, reference grimmory.BookReference, workspace string, canonical grimmory.File, plan DerivativePlan, receipt state.DerivedUploadReceipt, canonicalSHA, canonicalName, canonicalFormat string) (grimmory.File, grimmory.Book, bool, error) {
	expectedName := desiredOutputName(canonicalName, plan.Format)
	if normalizeFormat(receipt.Format) != normalizeFormat(plan.Format) || !sameNormalizedName(receipt.OutputName, expectedName) || receipt.OutputSHA256 == "" || receipt.SourceSHA256 == "" || receipt.GenerationFingerprint == "" {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	if canonicalSHA == "" || !strings.EqualFold(receipt.SourceSHA256, canonicalSHA) || receipt.GenerationFingerprint != plan.GenerationFingerprint {
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
	if !strings.EqualFold(outputSHA, receipt.OutputSHA256) {
		return grimmory.File{}, grimmory.Book{}, false, nil
	}
	sourcePath, downloadedSourceSHA, err := s.download(ctx, reference, canonicalFormat, workspace, "adoption-source-"+normalizeFormat(plan.Format))
	if sourcePath != "" {
		defer os.Remove(sourcePath)
	}
	if err != nil {
		return grimmory.File{}, grimmory.Book{}, false, err
	}
	if !strings.EqualFold(downloadedSourceSHA, receipt.SourceSHA256) {
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
	if !sameFileInventory(latest, current) {
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

// uploadDerivative performs a fresh, exact-target replacement. The receipt is
// written before the first remote derivative mutation and the caller context
// is used for every operation, including DELETE and the follow-up upload.
func (s *Service) uploadDerivative(ctx context.Context, reference grimmory.BookReference, plan DerivativePlan, before grimmory.File, hadBefore bool, initialBook grimmory.Book, canonicalFormat string, canonical grimmory.File, canonicalSHA, workspace, outputPath, canonicalName string, options SyncOptions, receipt state.DerivedUploadReceipt) (bool, error) {
	if !hadBefore {
		if err := s.revalidateCreateInventory(ctx, reference, plan.Format, canonicalFormat, canonical, canonicalSHA, workspace); err != nil {
			return false, err
		}
		if err := verifyLocalOutputUnchanged(outputPath, receipt.OutputSHA256, s.maxFileBytes); err != nil {
			return false, err
		}
		if err := s.setUploadReceipt(ctx, receipt); err != nil {
			return false, fmt.Errorf("%w: %v", ErrState, err)
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, s.upload(ctx, reference, plan.Format, outputPath, canonicalName)
	}
	if before.ID == "" || canonical.ID == "" || canonicalSHA == "" {
		return false, ErrSafeReplacementUnavailable
	}
	if len(filesForFormat(initialBook.Files, plan.Format)) != 1 || len(filesForFormat(initialBook.Files, canonicalFormat)) != 1 || !uniqueFileIDs(initialBook.Files) {
		return false, fmt.Errorf("%w: initial replacement inventory is ambiguous", ErrSafeReplacementUnavailable)
	}
	if err := verifyLocalOutputUnchanged(outputPath, receipt.OutputSHA256, s.maxFileBytes); err != nil {
		return false, err
	}
	current, err := s.revalidateReplacementInventory(ctx, reference, plan.Format, before, canonicalFormat, canonical, canonicalSHA, workspace, options)
	if err != nil {
		return false, err
	}
	target := currentTarget(current.Files, plan.Format)
	if err := s.setUploadReceipt(ctx, receipt); err != nil {
		return false, fmt.Errorf("%w: %v", ErrState, err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	deleteErr := s.client.DeleteFileScoped(ctx, reference, target.ID)
	if deleteErr != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		currentAfter, readErr := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
		if readErr != nil {
			return true, deleteErr
		}
		if err := s.validateReplacementBook(currentAfter, reference, plan.Format, before, canonicalFormat, canonical, options, false, false); err != nil {
			return true, deleteErr
		}
	}
	if _, err := s.probeReplacementAfterDelete(ctx, reference, plan.Format, before, canonicalFormat, canonical, options); err != nil {
		return true, err
	}
	if err := verifyLocalOutputUnchanged(outputPath, receipt.OutputSHA256, s.maxFileBytes); err != nil {
		return true, err
	}
	return true, s.upload(ctx, reference, plan.Format, outputPath, canonicalName)
}

func (s *Service) revalidateCreateInventory(ctx context.Context, reference grimmory.BookReference, targetFormat, canonicalFormat string, canonical grimmory.File, canonicalSHA, workspace string) error {
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return err
	}
	if hasTag(current, s.ignoreTag) {
		return fmt.Errorf("%w: ignore authorization appeared before derivative creation", ErrSafeReplacementUnavailable)
	}
	if err := validateCanonicalBook(current, reference, canonicalFormat, canonical); err != nil {
		return err
	}
	if len(filesForFormat(current.Files, targetFormat)) != 0 {
		return fmt.Errorf("%w: derivative target appeared before creation", ErrSafeReplacementUnavailable)
	}
	if err := s.verifyCanonicalUnchanged(ctx, reference, canonicalFormat, canonicalSHA, workspace, targetFormat); err != nil {
		return err
	}
	latest, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return err
	}
	if hasTag(latest, s.ignoreTag) || !sameFileInventory(latest, current) {
		return fmt.Errorf("%w: create inventory changed during source verification", ErrSafeReplacementUnavailable)
	}
	if err := validateCanonicalBook(latest, reference, canonicalFormat, canonical); err != nil {
		return err
	}
	if len(filesForFormat(latest.Files, targetFormat)) != 0 {
		return fmt.Errorf("%w: derivative target appeared before creation", ErrSafeReplacementUnavailable)
	}
	return s.verifyCanonicalUnchanged(ctx, reference, canonicalFormat, canonicalSHA, workspace, targetFormat)
}

func (s *Service) probeReplacementAfterDelete(ctx context.Context, reference grimmory.BookReference, targetFormat string, before grimmory.File, canonicalFormat string, canonical grimmory.File, options SyncOptions) (grimmory.Book, error) {
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.Book{}, err
	}
	if err := s.validateReplacementBook(current, reference, targetFormat, before, canonicalFormat, canonical, options, false, false); err != nil {
		return grimmory.Book{}, err
	}
	return current, nil
}

func (s *Service) revalidateReplacementInventory(ctx context.Context, reference grimmory.BookReference, targetFormat string, before grimmory.File, canonicalFormat string, canonical grimmory.File, canonicalSHA, workspace string, options SyncOptions) (grimmory.Book, error) {
	current, err := s.client.GetLibraryBook(ctx, reference.LibraryID, reference.BookID)
	if err != nil {
		return grimmory.Book{}, err
	}
	if err := s.validateReplacementBook(current, reference, targetFormat, before, canonicalFormat, canonical, options, true, true); err != nil {
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
	if err := s.validateReplacementBook(latest, reference, targetFormat, before, canonicalFormat, canonical, options, true, true); err != nil {
		return grimmory.Book{}, err
	}
	if err := s.verifyCanonicalUnchanged(ctx, reference, canonicalFormat, canonicalSHA, workspace, targetFormat); err != nil {
		return grimmory.Book{}, err
	}
	return latest, nil
}

func (s *Service) validateReplacementBook(current grimmory.Book, reference grimmory.BookReference, targetFormat string, before grimmory.File, canonicalFormat string, canonical grimmory.File, options SyncOptions, targetPresent, checkAuthorization bool) error {
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
	if checkAuthorization && !s.replacementAllowed(current, options) {
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
	Unsafe                bool
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
		candidates := filesForFormat(files, format)
		if len(candidates) > 1 {
			result = append(result, DerivativePlan{Format: format, Action: "blocked", Reason: "ambiguous_output", GenerationFingerprint: desiredFingerprints[format], Blocked: true, Unsafe: true})
			continue
		}
		if len(candidates) == 0 {
			result = append(result, derivativePlan(format, "create", "missing_output", desiredFingerprints))
			continue
		}
		existing := candidates[0]
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
		candidates := filesForFormat(files, format)
		if len(candidates) == 1 {
			return candidates[0], true
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
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return fallback
	}
}

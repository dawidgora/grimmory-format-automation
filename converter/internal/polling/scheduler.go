// Package polling contains the opt-in library reconciliation loop.
package polling

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"converter/internal/grimmory"
	"converter/internal/logging"
	"converter/internal/reconcile"
	"converter/internal/state"
)

// Remote is the library-scoped inventory portion of the Grimmory client. Policy
// resolution and all book mutations remain in reconciliation.
type Remote interface {
	ListLibraryBooks(context.Context, string) ([]grimmory.Book, error)
	GetLibraryBook(context.Context, string, string) (grimmory.Book, error)
}

type Reconciler interface {
	Sync(context.Context, string, string, reconcile.SyncOptions) (reconcile.Result, error)
}

type Store interface {
	UpsertPollObservation(context.Context, string, string, string, time.Time) (state.PollState, error)
	ListDuePollStates(context.Context, string, time.Time, int) ([]state.PollState, error)
	MarkPollSuccess(context.Context, string, string, string, time.Time) error
	RecordPollFailure(context.Context, string, string, string, string, time.Time, int, time.Time) (state.PollState, error)
	RequeuePollObservation(context.Context, string, string, string, string, time.Time, time.Time) error
}

type Options struct {
	Remote              Remote
	Store               Store
	Reconciler          Reconciler
	LibraryIDs          []string
	Interval            time.Duration
	MaxAttempts         int
	RetryBase           time.Duration
	RetryMax            time.Duration
	MaxConcurrentBooks  int
	IgnoreProcessingTag string
	FailedProcessingTag string
	Logger              *logging.Logger
	Now                 func() time.Time
	Random              func() float64
}

type Scheduler struct {
	remote             Remote
	store              Store
	reconciler         Reconciler
	libraryIDs         []string
	interval           time.Duration
	maxAttempts        int
	retryBase          time.Duration
	retryMax           time.Duration
	maxConcurrentBooks int
	ignoreTag          string
	failedTag          string
	logger             *logging.Logger
	now                func() time.Time
	random             func() float64
	randomMu           sync.Mutex
}

func New(options Options) (*Scheduler, error) {
	if options.Remote == nil || options.Store == nil || options.Reconciler == nil {
		return nil, errors.New("polling scheduler dependencies are not initialized")
	}
	if len(options.LibraryIDs) == 0 {
		return nil, errors.New("poll library allowlist is empty")
	}
	if options.Interval <= 0 {
		return nil, errors.New("poll interval must be positive")
	}
	if options.MaxAttempts <= 0 {
		return nil, errors.New("poll max attempts must be positive")
	}
	if options.RetryBase <= 0 || options.RetryMax <= 0 || options.RetryBase > options.RetryMax {
		return nil, errors.New("poll retry bounds are invalid")
	}
	concurrency := options.MaxConcurrentBooks
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > 16 {
		concurrency = 16
	}
	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	random := options.Random
	if random == nil {
		source := rand.NewSource(time.Now().UnixNano())
		generator := rand.New(source)
		random = generator.Float64
	}
	ids := make([]string, 0, len(options.LibraryIDs))
	seen := make(map[string]struct{}, len(options.LibraryIDs))
	for _, id := range options.LibraryIDs {
		id = strings.TrimSpace(id)
		if !reconcile.ValidLibraryID(id) {
			return nil, errors.New("poll library ID is invalid")
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return &Scheduler{
		remote: options.Remote, store: options.Store, reconciler: options.Reconciler,
		libraryIDs: ids, interval: options.Interval, maxAttempts: options.MaxAttempts,
		retryBase: options.RetryBase, retryMax: options.RetryMax,
		maxConcurrentBooks: concurrency, ignoreTag: strings.TrimSpace(options.IgnoreProcessingTag),
		failedTag: strings.TrimSpace(options.FailedProcessingTag), logger: options.Logger,
		now: now, random: random,
	}, nil
}

// Run starts an immediate scan and then uses a start-to-start ticker. Ticks
// received while a scan is running are discarded, so scans never overlap and a
// slow scan cannot create a backlog of work.
func (s *Scheduler) Run(ctx context.Context) error {
	if s == nil {
		return errors.New("polling scheduler is nil")
	}
	s.log(logging.Info, "poller started", "interval", s.interval.String(), "worker_limit", strconv.Itoa(s.maxConcurrentBooks))
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.Scan(ctx); err != nil && ctx.Err() == nil {
			s.log(logging.Warn, "poll scan failed", "error_class", reconcile.ClassifyError(err))
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		for {
			select {
			case <-ticker.C:
				s.log(logging.Warn, "poll scan skipped overlapping tick", "reason", "scan_in_progress")
				continue
			default:
				goto waitForTick
			}
		}
	waitForTick:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Scan performs one complete inventory pass over every configured library.
func (s *Scheduler) Scan(ctx context.Context) (scanErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	summary := newScanSummary()
	s.log(logging.Info, "poll scan started")
	defer func() {
		fields := summary.fields()
		level := logging.Info
		if scanErr != nil {
			level = logging.Warn
			fields = append(fields, "error_class", reconcile.ClassifyError(scanErr))
		}
		s.log(level, "poll scan completed", fields...)
	}()
	var scanErrors []error
	appendError := func(err error) {
		if err != nil && ctx.Err() == nil {
			scanErrors = append(scanErrors, err)
		}
	}
	ready := make(map[string]struct{})
	for _, libraryID := range s.libraryIDs {
		summary.libraryAttempted()
		books, err := s.remote.ListLibraryBooks(ctx, libraryID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			summary.libraryFailed(libraryID)
			appendError(fmt.Errorf("list library %s: %w", libraryID, err))
			continue
		}
		seen := make(map[string]struct{}, len(books))
		for _, listed := range books {
			summary.bookSeen()
			if err := ctx.Err(); err != nil {
				return err
			}
			bookID := strings.TrimSpace(listed.ID)
			if !reconcile.ValidBookID(bookID) {
				s.logBookFailure(libraryID, safeLogBookID(bookID), "poll book listing failed", reconcile.ErrInvalidBookID)
				appendError(fmt.Errorf("invalid book ID in library %s listing", libraryID))
				continue
			}
			if _, duplicate := seen[bookID]; duplicate {
				continue
			}
			seen[bookID] = struct{}{}
			book, err := s.remote.GetLibraryBook(ctx, libraryID, bookID)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				s.logBookFailure(libraryID, bookID, "poll book inventory failed", err)
				appendError(fmt.Errorf("get library %s book %s: %w", libraryID, bookID, err))
				continue
			}
			if book.ID != bookID || book.LibraryID != libraryID {
				s.logBookFailure(libraryID, bookID, "poll book membership failed", grimmory.ErrInvalidResponse)
				appendError(fmt.Errorf("library %s book %s failed membership validation", libraryID, bookID))
				continue
			}
			if hasTag(book, s.ignoreTag) {
				summary.bookIgnored()
				if err := s.clearFailureTagForIgnored(ctx, libraryID, bookID, book); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					s.logBookFailure(libraryID, bookID, "poll ignored book tag cleanup failed", err)
					appendError(fmt.Errorf("clear failure tag for ignored book %s/%s: %w", libraryID, bookID, err))
				}
				continue
			}
			fingerprint := grimmory.ObservationFingerprintIgnoringTags(book, s.ignoreTag, s.failedTag)
			if _, err := s.store.UpsertPollObservation(ctx, libraryID, bookID, fingerprint, s.now()); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				s.logBookFailure(libraryID, bookID, "poll observation failed", err)
				appendError(fmt.Errorf("upsert poll observation %s/%s: %w", libraryID, bookID, err))
				continue
			}
			ready[pollKey(libraryID, bookID)] = struct{}{}
		}
	}

	jobs := make(chan state.PollState)
	var workers sync.WaitGroup
	var workerErrorsMu sync.Mutex
	var workerErrors []error
	for index := 0; index < s.maxConcurrentBooks; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case pollState, ok := <-jobs:
					if !ok {
						return
					}
					err := s.process(ctx, pollState)
					if err == nil {
						summary.bookSucceeded()
						continue
					}
					if ctx.Err() == nil {
						summary.bookFailed(s.failureWasRetried(pollState, err))
						workerErrorsMu.Lock()
						workerErrors = append(workerErrors, fmt.Errorf("poll book %s/%s: %w", pollState.LibraryID, pollState.BookID, err))
						workerErrorsMu.Unlock()
					}
				}
			}
		}()
	}
	produceDone := false
	for _, libraryID := range s.libraryIDs {
		due, err := s.store.ListDuePollStates(ctx, libraryID, s.now(), 0)
		if err != nil {
			if ctx.Err() != nil {
				close(jobs)
				workers.Wait()
				return ctx.Err()
			}
			summary.libraryFailed(libraryID)
			appendError(fmt.Errorf("list due poll states for library %s: %w", libraryID, err))
			continue
		}
		for _, pollState := range due {
			if _, current := ready[pollKey(pollState.LibraryID, pollState.BookID)]; !current {
				continue
			}
			select {
			case <-ctx.Done():
				produceDone = true
			case jobs <- pollState:
				summary.bookQueued()
			}
			if produceDone {
				break
			}
		}
		if produceDone {
			break
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	workerErrorsMu.Lock()
	scanErrors = append(scanErrors, workerErrors...)
	workerErrorsMu.Unlock()
	if len(scanErrors) > 0 {
		return errors.Join(scanErrors...)
	}
	return nil
}

func pollKey(libraryID, bookID string) string { return libraryID + "\x00" + bookID }

type scanSummary struct {
	mu                  sync.Mutex
	librariesAttempted  int
	librariesFailed     int
	failedLibraries     map[string]struct{}
	booksSeen           int
	booksIgnored        int
	booksQueued         int
	booksSucceeded      int
	booksRetried        int
	booksRetryExhausted int
}

func newScanSummary() *scanSummary {
	return &scanSummary{failedLibraries: make(map[string]struct{})}
}

func (summary *scanSummary) libraryAttempted() {
	summary.mu.Lock()
	summary.librariesAttempted++
	summary.mu.Unlock()
}

func (summary *scanSummary) libraryFailed(libraryID string) {
	summary.mu.Lock()
	if _, exists := summary.failedLibraries[libraryID]; !exists {
		summary.failedLibraries[libraryID] = struct{}{}
		summary.librariesFailed++
	}
	summary.mu.Unlock()
}

func (summary *scanSummary) bookSeen() {
	summary.mu.Lock()
	summary.booksSeen++
	summary.mu.Unlock()
}

func (summary *scanSummary) bookIgnored() {
	summary.mu.Lock()
	summary.booksIgnored++
	summary.mu.Unlock()
}

func (summary *scanSummary) bookQueued() {
	summary.mu.Lock()
	summary.booksQueued++
	summary.mu.Unlock()
}

func (summary *scanSummary) bookSucceeded() {
	summary.mu.Lock()
	summary.booksSucceeded++
	summary.mu.Unlock()
}

func (summary *scanSummary) bookFailed(retried bool) {
	summary.mu.Lock()
	if retried {
		summary.booksRetried++
	} else {
		summary.booksRetryExhausted++
	}
	summary.mu.Unlock()
}

func (summary *scanSummary) fields() []string {
	summary.mu.Lock()
	defer summary.mu.Unlock()
	return []string{
		"libraries_attempted", strconv.Itoa(summary.librariesAttempted),
		"libraries_failed", strconv.Itoa(summary.librariesFailed),
		"books_seen", strconv.Itoa(summary.booksSeen),
		"books_ignored", strconv.Itoa(summary.booksIgnored),
		"books_queued", strconv.Itoa(summary.booksQueued),
		"books_succeeded", strconv.Itoa(summary.booksSucceeded),
		"books_retried", strconv.Itoa(summary.booksRetried),
		"books_retry_exhausted", strconv.Itoa(summary.booksRetryExhausted),
	}
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

func (s *Scheduler) clearFailureTagForIgnored(ctx context.Context, libraryID, bookID string, book grimmory.Book) error {
	if s.failedTag == "" || s.failedTag == s.ignoreTag || !hasTag(book, s.failedTag) {
		return nil
	}
	// The reconciliation service owns the scoped metadata-preserving mutation
	// and acquires the keyed book lock before changing Grimmory.
	tagger, ok := s.reconciler.(reconcile.FailureTagSetter)
	if !ok {
		return reconcile.ErrFailureTagMutation
	}
	return tagger.SetFailureTag(ctx, libraryID, bookID, false)
}

func (s *Scheduler) process(ctx context.Context, pollState state.PollState) error {
	if locker, ok := s.reconciler.(reconcile.BookLocker); ok {
		return locker.WithBookLock(ctx, pollState.LibraryID, pollState.BookID, func(lockedContext context.Context) error {
			return s.processLocked(lockedContext, pollState)
		})
	}
	return s.processLocked(ctx, pollState)
}

func (s *Scheduler) processLocked(ctx context.Context, pollState state.PollState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, syncErr := s.reconciler.Sync(ctx, pollState.LibraryID, pollState.BookID, reconcile.SyncOptions{})
	if syncErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.recordFailure(ctx, pollState, syncErr)
		return syncErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	postBook, err := s.remote.GetLibraryBook(ctx, pollState.LibraryID, pollState.BookID)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.recordFailure(ctx, pollState, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	postFingerprint := grimmory.ObservationFingerprintIgnoringTags(postBook, s.ignoreTag, s.failedTag)
	// Complete only the observation this worker reconciled. Recording the
	// post-sync observation afterward keeps a distinct concurrent observation
	// pending for a later pass.
	if err := s.store.MarkPollSuccess(ctx, pollState.LibraryID, pollState.BookID, pollState.ObservationFingerprint, s.now()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.recordFailure(ctx, pollState, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := s.store.UpsertPollObservation(ctx, pollState.LibraryID, pollState.BookID, postFingerprint, s.now()); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.recordFailure(ctx, pollState, err)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (s *Scheduler) recordFailure(ctx context.Context, pollState state.PollState, cause error) {
	if ctx.Err() != nil {
		return
	}
	if errors.Is(cause, reconcile.ErrFailureTagMutation) {
		s.recordTagMutationFailure(ctx, pollState, cause)
		return
	}
	transient := IsTransient(cause)
	maxAttempts := 1
	nextAttempt := time.Time{}
	if transient {
		maxAttempts = s.maxAttempts
		nextAttempt = s.now().Add(s.backoff(pollState.AttemptCount))
	}
	retryExhausted := !transient || pollState.AttemptCount+1 >= maxAttempts
	if retryExhausted && s.failedTag != "" {
		if err := s.setFailureTag(ctx, pollState, true); err != nil {
			s.recordTagMutationFailure(ctx, pollState, err)
			return
		}
	}
	code := failureCode(cause, transient)
	_, err := s.store.RecordPollFailure(ctx, pollState.LibraryID, pollState.BookID, pollState.ObservationFingerprint, code, nextAttempt, maxAttempts, s.now())
	if err != nil {
		if ctx.Err() == nil {
			s.logBookFailure(pollState.LibraryID, pollState.BookID, "poll failure state failed", err)
		}
		return
	}
	if transient {
		s.logBookFailure(pollState.LibraryID, pollState.BookID, "poll retry scheduled", cause)
	} else {
		s.logBookFailure(pollState.LibraryID, pollState.BookID, "poll retries exhausted", cause)
	}
}

func (s *Scheduler) setFailureTag(ctx context.Context, pollState state.PollState, present bool) error {
	tagger, ok := s.reconciler.(reconcile.FailureTagSetter)
	if !ok {
		return reconcile.ErrFailureTagMutation
	}
	return tagger.SetFailureTag(ctx, pollState.LibraryID, pollState.BookID, present)
}

func (s *Scheduler) recordTagMutationFailure(ctx context.Context, pollState state.PollState, cause error) {
	if ctx.Err() != nil {
		return
	}
	if err := s.store.RequeuePollObservation(ctx, pollState.LibraryID, pollState.BookID, pollState.ObservationFingerprint, "failure_tag_mutation", time.Time{}, s.now()); err != nil {
		if ctx.Err() == nil {
			s.logBookFailure(pollState.LibraryID, pollState.BookID, "poll failure tag state failed", err)
		}
		return
	}
	s.logBookFailure(pollState.LibraryID, pollState.BookID, "poll failure tag retry scheduled", cause)
}

func (s *Scheduler) failureWasRetried(pollState state.PollState, err error) bool {
	if errors.Is(err, reconcile.ErrFailureTagMutation) {
		return true
	}
	return IsTransient(err) && pollState.AttemptCount+1 < s.maxAttempts
}

func (s *Scheduler) backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := s.retryBase
	for index := 0; index < attempt && delay < s.retryMax; index++ {
		if delay > s.retryMax/2 {
			delay = s.retryMax
			break
		}
		delay *= 2
		if delay > s.retryMax {
			delay = s.retryMax
		}
	}
	minimum := delay / 2
	s.randomMu.Lock()
	value := s.random()
	s.randomMu.Unlock()
	if value < 0 {
		value = 0
	}
	if value > 1 {
		value = 1
	}
	return minimum + time.Duration(float64(delay-minimum)*value)
}

func IsTransient(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if cause := unwrapCause(err); cause != nil && cause != err {
		return IsTransient(cause)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ETIMEDOUT) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var httpError *grimmory.HTTPError
	if errors.As(err, &httpError) {
		return httpError.Status == 408 || httpError.Status == 425 || httpError.Status == 429 || httpError.Status >= 500
	}
	if errors.Is(err, reconcile.ErrVerification) {
		return true
	}
	return isSQLiteBusy(err)
}

func unwrapCause(err error) error {
	var causer interface{ Cause() error }
	if errors.As(err, &causer) && causer != nil {
		return causer.Cause()
	}
	return nil
}

func isSQLiteBusy(err error) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		message := strings.ToLower(current.Error())
		if (strings.Contains(message, "database") && (strings.Contains(message, "locked") || strings.Contains(message, "busy"))) || strings.Contains(message, "sqlite_busy") || strings.Contains(message, "sqlite_locked") {
			return true
		}
	}
	return errors.Is(err, syscall.EBUSY)
}

func failureCode(err error, transient bool) string {
	class := reconcile.ClassifyError(err)
	if class == "" {
		class = "internal"
	}
	if transient {
		return "retry_" + class
	}
	return "permanent_" + class
}

func (s *Scheduler) logBookFailure(libraryID, bookID, message string, err error) {
	class := reconcile.ClassifyError(err)
	if class == "" {
		class = "internal"
	}
	values := []string{"library_id", libraryID, "book_id", safeLogBookID(bookID), "error_code", class, "error_class", class}
	if status := remoteStatus(err); status != "" {
		values = append(values, "remote_status", status)
	}
	s.log(logging.Warn, message, values...)
}

func safeLogBookID(bookID string) string {
	if reconcile.ValidBookID(bookID) {
		return bookID
	}
	return "invalid"
}

func remoteStatus(err error) string {
	var httpError *grimmory.HTTPError
	if errors.As(err, &httpError) && httpError != nil && httpError.Status >= 100 && httpError.Status <= 599 {
		return strconv.Itoa(httpError.Status)
	}
	return ""
}

func (s *Scheduler) log(level logging.Level, message string, values ...string) {
	if s.logger == nil {
		return
	}
	fields := []logging.Field{{Key: "message", Value: message}}
	for index := 0; index+1 < len(values); index += 2 {
		fields = append(fields, logging.Field{Key: values[index], Value: values[index+1]})
	}
	s.logger.Log(level, fields...)
}

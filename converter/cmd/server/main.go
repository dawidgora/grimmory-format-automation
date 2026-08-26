package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"converter/internal/auth"
	"converter/internal/config"
	"converter/internal/convert"
	"converter/internal/grimmory"
	"converter/internal/httpapi"
	"converter/internal/logging"
	"converter/internal/polling"
	"converter/internal/reconcile"
	"converter/internal/state"
)

func main() {
	poll, err := parsePollFlag(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Fatal(err)
	}
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	logger := logging.New(cfg.LogLevel, os.Stderr)
	apiKey, generated, err := auth.LoadOrCreate(cfg.APIKeyPath)
	if err != nil {
		log.Fatal(err)
	}
	if generated {
		// Deliberately log only the storage path, never the credential.
		logger.Log(logging.Info, logging.Field{Key: "message", Value: "generated API key"}, logging.Field{Key: "path", Value: cfg.APIKeyPath})
	}

	store, err := state.Open(cfg.DataDir, cfg.DatabaseBusyTimeout)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	client, err := grimmory.New(cfg.GrimmoryBaseURL, cfg.GrimmoryUsername, cfg.GrimmoryPassword, &http.Client{Timeout: cfg.HTTPTimeout}, cfg.MaxResponseBytes, cfg.MaxFileBytes, cfg.HTTPTimeout)
	if err != nil {
		log.Fatal(err)
	}
	converter := convert.NewFileConverter(cfg.CalibreBinary, cfg.MaxFileBytes, nil)
	service := reconcile.New(reconcile.Options{
		Client:              client,
		Store:               store,
		Converter:           converter,
		LibraryIDs:          cfg.LibraryIDs,
		OutputFormats:       cfg.OutputFormats,
		SupportedInputs:     cfg.SupportedInputFormats,
		MaxConcurrentBooks:  cfg.MaxConcurrentBooks,
		FailedProcessingTag: cfg.FailedProcessingTag,
		MaxFileBytes:        cfg.MaxFileBytes,
		ConversionTimeout:   cfg.ConversionTimeout,
		Logger:              logger,
	})
	var poller *polling.Scheduler
	if poll {
		poller, err = polling.New(polling.Options{
			Remote:              client,
			Store:               store,
			Reconciler:          service,
			LibraryIDs:          cfg.LibraryIDs,
			Interval:            cfg.PollInterval,
			MaxAttempts:         cfg.PollMaxAttempts,
			RetryBase:           cfg.PollRetryBase,
			RetryMax:            cfg.PollRetryMax,
			MaxConcurrentBooks:  cfg.MaxConcurrentBooks,
			IgnoreProcessingTag: cfg.IgnoreProcessingTag,
			FailedProcessingTag: cfg.FailedProcessingTag,
			Logger:              logger,
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	api := httpapi.NewWithLogger(apiKey, service, logger)
	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.HTTPTimeout,
		// A reconciliation can run several conversions sequentially. Do not impose
		// a server-wide write deadline shorter than a valid one-book sync.
		WriteTimeout: 0,
		IdleTimeout:  60 * time.Second,
		BaseContext:  func(net.Listener) context.Context { return ctx },
	}

	var pollWG sync.WaitGroup
	if poller != nil {
		pollWG.Add(1)
		go func() {
			defer pollWG.Done()
			if err := poller.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				logger.Log(logging.Error, logging.Field{Key: "message", Value: "poll loop stopped"}, logging.Field{Key: "error_class", Value: reconcile.ClassifyError(err)})
			}
		}()
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		// One request may create the main plus every configured derivative.
		shutdownWindow := cfg.ConversionTimeout*time.Duration(len(cfg.OutputFormats)+1) + cfg.HTTPTimeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownWindow)
		defer cancel()
		if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Log(logging.Error, logging.Field{Key: "message", Value: "server shutdown"}, logging.Field{Key: "error", Value: shutdownErr.Error()})
		}
	}()

	logger.Log(logging.Info, logging.Field{Key: "message", Value: "reconciliation service listening"}, logging.Field{Key: "addr", Value: cfg.Addr})
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	if ctx.Err() != nil {
		// Shutdown waits for active handlers; do not close state while a sync runs.
		<-shutdownDone
		pollWG.Wait()
	}
}

func parsePollFlag(args []string) (bool, error) {
	flags := flag.NewFlagSet("converter", flag.ContinueOnError)
	poll := flags.Bool("poll", false, "run the sequential Grimmory library poller")
	if err := flags.Parse(args); err != nil {
		return false, err
	}
	if flags.NArg() != 0 {
		return false, fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	return *poll, nil
}

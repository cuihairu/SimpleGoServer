// Package main is the reference answer to interview question 2
// ("write a simple Go service", see README.md): a small HTTP service
// with an in-memory key/value API, a health endpoint, timeouts and a
// graceful shutdown path. Standard library only — every dependency it
// refuses is a talking point.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// osExit is a variable so tests can run main() without terminating the
// test binary (same shape as cmd/cli).
var osExit = os.Exit

func main() { osExit(run(os.Args[1:])) }

// run is the testable entry point: flags in, exit code out. main only
// translates the code into os.Exit, so the whole lifecycle below —
// listen, serve, trap a signal, drain — runs under go test (same shape
// as cmd/cli).
func run(args []string) int {
	fs := flag.NewFlagSet("http-service", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	maxValue := fs.Int64("max-bytes", defaultMaxValueBytes, "max value size in bytes")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second,
		"grace period for in-flight requests after SIGINT/SIGTERM")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	svc := NewService(NewMemoryStore(), *maxValue)
	srv := &http.Server{
		Addr:    *addr,
		Handler: newHandler(svc, log),
		// net/http defaults to zero timeouts: a client that opens a
		// socket and sends nothing parks a goroutine forever
		// (slowloris). All four are set, in increasing span.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// NotifyContext turns SIGINT/SIGTERM into ctx cancellation; the
	// signal is consumed here so nothing else in the process sees it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Buffered: ListenAndServe's error must be deliverable even if the
	// main flow already moved on to the shutdown branch.
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	log.Info("listening", "addr", *addr)
	select {
	case err := <-errCh:
		// ListenAndServe always returns non-nil. ErrServerClosed only
		// ever comes after Shutdown, and the only Shutdown call lives
		// in the ctx branch below — so reaching this case at all means
		// the bind failed (or the listener died under us): report, exit.
		log.Error("server failed", "err", err)
		return 1
	case <-ctx.Done():
		// Shutdown stops accepting, then waits for in-flight handlers
		// and closes keep-alive connections. The timeout turns a stuck
		// handler into a deadline instead of an unkillable process.
		sctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil {
			log.Error("graceful shutdown timed out", "err", err)
			return 1
		}
		log.Info("shutdown complete")
		return 0
	}
}

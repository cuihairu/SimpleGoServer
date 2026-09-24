package executor

import (
	"context"
	"testing"
	"time"
)

// TestAsyncFutureBooleanViews covers the fresh-future branches: before any
// resolution or cancellation both predicates must report false.
func TestAsyncFutureBooleanViews(t *testing.T) {
	e := NewAsyncExecutor[string](1)
	defer e.Stop()

	f := e.Submit(func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	if f.IsDone() {
		t.Fatal("a pending future must not report done")
	}
	if f.IsCancelled() {
		t.Fatal("a pending future must not report cancelled")
	}
	// cancelling flips IsCancelled but doneCh only closes once the task
	// observes the cancellation and resolves the future
	f.Cancel()
	if !f.IsCancelled() {
		t.Fatal("a cancelled future must report cancelled")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !f.IsDone() {
		time.Sleep(10 * time.Millisecond)
	}
	if !f.IsDone() {
		t.Fatal("a cancelled future must still resolve (doneCh closed)")
	}
}

// TestAsyncExecutorNumWorker covers the introspection accessor. There is no
// Size to test: the task channel is unbuffered, so len(tasks) would always
// read zero — the method was removed as a fiction rather than fixed.
func TestAsyncExecutorNumWorker(t *testing.T) {
	e := NewAsyncExecutor[string](2)
	defer e.Stop()
	if got := e.NumWorker(); got != 2 {
		t.Fatalf("NumWorker() = %d, want 2", got)
	}

	zero := NewAsyncExecutor[int](0)
	defer zero.Stop()
	if zero.NumWorker() < 1 {
		t.Fatalf("NumWorker() = %d for a count of 0, want normalization to >= 1", zero.NumWorker())
	}
}

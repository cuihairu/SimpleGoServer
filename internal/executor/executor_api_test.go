package executor

import (
	"testing"
)

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

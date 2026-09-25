package executor

import (
	"context"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg"
)

// stoppedExecutor builds an executor with a cancelled context, buffered
// channels and no workers running, so Submit/Execute sends land in the
// buffer deterministically.
func stoppedExecutor[T any](buffer int) *AsyncExecutor[T] {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return &AsyncExecutor[T]{
		tasks:     make(chan pkg.Action, buffer),
		futureCh:  make(chan pkg.Future[T], buffer),
		numWorker: 1,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// TestSubmitAfterStopHandsBackCancelledFuture: a send that lands after the
// executor was stopped must not return a future nobody will ever resolve —
// the re-check catches the cancelled ctx and cancels the future instead of
// leaving Get blocked forever. Both arms of Submit's outer select are ready
// here (buffered channel, cancelled ctx) and Go resolves that randomly, so
// the loop must not stop at the first cancelled future: running the full
// batch gives the send arm its turn with overwhelming certainty while also
// asserting that every single submit resolves cancelled, whichever arm won.
func TestSubmitAfterStopHandsBackCancelledFuture(t *testing.T) {
	e := stoppedExecutor[int](128)
	for i := 0; i < 64; i++ {
		future := e.Submit(func(context.Context) (int, error) { return i, nil })
		if !future.IsCancelled() {
			t.Fatalf("submit %d on a stopped executor was not cancelled", i)
		}
		if v, err := future.Get(); err == nil || v != 0 {
			t.Fatalf("Get() = (%d, %v), want (0, cancelled)", v, err)
		}
	}
}

// TestExecuteAfterStopDropsQuietly: same contract for the void path — the
// task is accepted into the queue, the re-check sees the stop and drops it
// instead of leaving it queued forever.
func TestExecuteAfterStopDropsQuietly(t *testing.T) {
	e := stoppedExecutor[int](128)
	ran := make(chan struct{}, 1)
	task := func(context.Context) { ran <- struct{}{} }

	e.Execute(task) // nothing consumes it: no workers exist
	select {
	case <-ran:
		t.Fatal("a queued task ran with no workers present")
	default:
	}

	for i := 0; i < 64; i++ {
		e.Execute(task) // must not block or panic
	}
}

// TestWorkerIgnoresNilHandoffs pins the worker's zero-value guards: a nil
// task or a nil future handed over a channel must be skipped, not called —
// calling either would be a process-killing nil-func panic.
func TestWorkerIgnoresNilHandoffs(t *testing.T) {
	e := NewAsyncExecutor[int](1)

	e.Execute(nil)
	go func() { e.futureCh <- pkg.Future[int](nil) }()

	// give the worker a moment to chew through both nils, then shut down
	time.Sleep(50 * time.Millisecond)
	e.Stop()
	e.wg.Wait()
}

// TestFutureDoIsIdempotent: the second Do on the same future is a no-op —
// the action runs exactly once.
func TestFutureDoIsIdempotent(t *testing.T) {
	runs := 0
	f := NewFuture[int](context.Background(), func(context.Context) (int, error) {
		runs++
		return 7, nil
	})

	f.Do()
	f.Do()

	got, err := f.Get()
	if err != nil || got != 7 {
		t.Fatalf("Get() = %d, %v; want 7, nil", got, err)
	}
	if runs != 1 {
		t.Fatalf("action ran %d times, want exactly 1", runs)
	}
}

// TestCancelAfterCompletionIsHarmless: cancelling a future whose action
// already finished must not clobber the result — the late Cancel loses
// against the completed resolution.
func TestCancelAfterCompletionIsHarmless(t *testing.T) {
	f := NewFuture[int](context.Background(), func(context.Context) (int, error) {
		return 7, nil
	})
	f.Do()
	f.Cancel()

	got, err := f.Get()
	if err != nil || got != 7 {
		t.Fatalf("Get() = %d, %v; want the untouched 7, nil", got, err)
	}
}

package executor

import (
	"context"
	"github.com/cuihairu/simplegoserver/pkg"
	"runtime"
	"sync"
)

type AsyncExecutor[T any] struct {
	tasks     chan pkg.Action
	futureCh  chan pkg.Future[T]
	numWorker int
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func (e *AsyncExecutor[T]) Submit(action pkg.ActionWithReturn[T]) pkg.Future[T] {
	future := NewFuture[T](e.ctx, action)
	select {
	case e.futureCh <- future:
		// re-check after the send: a worker may already be gone, in which
		// case the future would never be resolved and Get would block
		// forever — hand back an explicitly cancelled one instead
		select {
		case <-e.ctx.Done():
			future.Cancel()
		default:
		}
		return future
	case <-e.ctx.Done():
		future.Cancel()
		return future
	}
}

func NewAsyncExecutor[T any](numWorker int) *AsyncExecutor[T] {
	if numWorker < 1 {
		numWorker = runtime.NumCPU()
	}
	ctx, cancel := context.WithCancel(context.Background())
	executor := &AsyncExecutor[T]{
		tasks:     make(chan pkg.Action),
		futureCh:  make(chan pkg.Future[T]),
		numWorker: numWorker,
		ctx:       ctx,
		cancel:    cancel,
	}
	executor.Start()
	go executor.Wait()
	return executor
}

func (e *AsyncExecutor[T]) Start() {
	for i := 0; i < e.numWorker; i++ {
		e.wg.Add(1)
		go e.worker()
	}
}

// Stop cancels the executor: workers drain out through ctx.Done. The task
// channels are deliberately not closed — a close would turn any concurrent
// Submit/Execute send into a "send on closed channel" panic, and would
// hand the workers zero values to call. Tasks submitted after Stop are
// dropped without feedback (the Executor interface has no error return).
func (e *AsyncExecutor[T]) Stop() {
	e.cancel()
}

func (e *AsyncExecutor[T]) Execute(task pkg.Action) {
	select {
	case e.tasks <- task:
		select {
		case <-e.ctx.Done():
			// executor stopped before a worker picked it up; dropped
		default:
		}
		return
	case <-e.ctx.Done():
		// dropped: executor stopped
	}
}

func (e *AsyncExecutor[T]) worker() {
	defer e.wg.Done()
	for {
		select {
		case t := <-e.tasks:
			// Stop closes both channels; a closed channel makes this
			// receive immediately ready with the zero value, and with
			// ctx.Done also ready the select may pick either side — a
			// nil action here would be a call of a nil func, i.e. a
			// process-killing panic in a worker goroutine.
			if t == nil {
				continue
			}
			t(e.ctx)
		case p := <-e.futureCh:
			if p == nil {
				continue
			}
			p.Do()
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *AsyncExecutor[T]) Wait() {
	e.wg.Wait()
}
func (e *AsyncExecutor[T]) Shutdown() {
	e.Stop()
}

func (e *AsyncExecutor[T]) Size() int {
	return len(e.tasks)
}

func (e *AsyncExecutor[T]) NumWorker() int {
	return e.numWorker
}

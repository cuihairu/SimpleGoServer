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
	e.futureCh <- future
	return future
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

func (e *AsyncExecutor[T]) Stop() {
	e.cancel()
	close(e.tasks)
	close(e.futureCh)
}

func (e *AsyncExecutor[T]) Execute(task pkg.Action) {
	e.tasks <- task
}

func (e *AsyncExecutor[T]) worker() {
	defer e.wg.Done()
	for {
		select {
		case t := <-e.tasks:
			t(e.ctx)
		case p := <-e.futureCh:
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

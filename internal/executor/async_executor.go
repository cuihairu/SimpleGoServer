package executor

import (
	"context"
	"github.com/cuihairu/simplegoserver/pkg"
	"runtime"
	"sync"
)

var _ pkg.Executor = (*AsyncExecutor)(nil)

type AsyncExecutor struct {
	tasks     chan func()
	numWorker int
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewAsyncExecutor(numWorker int) *AsyncExecutor {
	if numWorker < 1 {
		numWorker = runtime.NumCPU()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncExecutor{
		tasks:     make(chan func()),
		numWorker: numWorker,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (e *AsyncExecutor) Start() {
	for i := 0; i < e.numWorker; i++ {
		e.wg.Add(1)
		go e.worker()
	}
}

func (e *AsyncExecutor) Stop() {
	e.cancel()
}

func (e *AsyncExecutor) Exec(task pkg.Action) {
	e.tasks <- task
}

func (e *AsyncExecutor) worker() {
	defer e.wg.Done()
	for {
		select {
		case t := <-e.tasks:
			t()
		case <-e.ctx.Done():
			return
		}
	}
}

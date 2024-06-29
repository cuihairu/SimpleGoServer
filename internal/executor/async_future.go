package executor

import (
	"context"
	"fmt"
	"github.com/cuihairu/simplegoserver/pkg"
	"sync"
	"sync/atomic"
	"time"
)

type AsyncFuture[T any] struct {
	result     T
	err        error
	f          func(ctx context.Context) (T, error)
	ctx        context.Context
	cancel     context.CancelFunc
	running    atomic.Bool
	rwMutex    sync.RWMutex
	doneCh     chan struct{}
	isCanceled atomic.Bool
}

func NewFuture[T any](ctx context.Context, f pkg.ActionWithReturn[T]) *AsyncFuture[T] {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &AsyncFuture[T]{
		ctx:    cancelCtx,
		cancel: cancel,
		f:      f,
		doneCh: make(chan struct{}),
	}
}

func (f *AsyncFuture[T]) Do() {
	if f.running.Swap(true) {
		return
	}
	result, err := f.f(f.ctx)
	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()
	if f.IsDone() {
		return
	}
	f.result = result
	f.err = err
	close(f.doneCh)
}

func (f *AsyncFuture[T]) Get() (T, error) {
	<-f.doneCh
	return f.result, f.err
}

func (f *AsyncFuture[T]) GetWithTimeout(timeout time.Duration) (T, error) {
	select {
	case <-f.doneCh:
		return f.result, f.err
	case <-time.After(timeout):
		err := fmt.Errorf("timeout")
		f.cancelWithErr(err)
		return f.result, err
	}
}

func (f *AsyncFuture[T]) Cancel() {
	f.cancelWithErr(fmt.Errorf("cancelled"))
}

func (f *AsyncFuture[T]) cancelWithErr(err error) {
	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()
	if f.IsDone() {
		return
	}
	f.cancel()
	var zero T
	f.result = zero
	f.err = err
	close(f.doneCh)
}

func (f *AsyncFuture[T]) IsCancelled() bool {
	if f.isCanceled.Load() {
		return true
	}
	select {
	case <-f.ctx.Done():
		return true
	default:
		return false
	}
}

func (f *AsyncFuture[T]) IsDone() bool {
	if f.IsCancelled() {
		return true
	}
	select {
	case <-f.doneCh:
		return true
	case <-f.ctx.Done():
		return true
	default:
		return false
	}
}

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
	result  T
	err     error
	f       func(ctx context.Context) (T, error)
	ctx     context.Context
	cancel  context.CancelFunc
	running atomic.Bool
	rwMutex sync.RWMutex
	doneCh  chan struct{}
}

func NewFuture[T any](ctx context.Context, f pkg.ActionWithReturn[T]) *AsyncFuture[T] {
	cancelCtx, cancel := context.WithCancel(ctx) // #nosec G118 -- cancel is stored and invoked via cancelWithErr (Do failure, Cancel, rejected Submit)
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
	// a Cancel racing the run already resolved the future — keep that
	// result instead of overwriting it
	select {
	case <-f.doneCh:
		return
	default:
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
		// result stays untouched here on purpose: Do() may be writing it
		// right now (its write is lock-guarded, this read would not be)
		var zero T
		return zero, err
	}
}

func (f *AsyncFuture[T]) Cancel() {
	f.cancelWithErr(fmt.Errorf("cancelled"))
}

func (f *AsyncFuture[T]) cancelWithErr(err error) {
	// cancelling the ctx is safe to repeat and lets a still-running task
	// observe the cancellation
	f.cancel()
	f.rwMutex.Lock()
	defer f.rwMutex.Unlock()
	select {
	case <-f.doneCh:
		return // already resolved by Do or an earlier cancel
	default:
	}
	var zero T
	f.result = zero
	f.err = err
	close(f.doneCh)
}

func (f *AsyncFuture[T]) IsCancelled() bool {
	select {
	case <-f.ctx.Done():
		return true
	default:
		return false
	}
}

// IsDone reports whether the future has resolved — Get would return
// immediately. Done here always means "doneCh is closed": done and
// cancelled used to be conflated, with cancelled futures reporting done
// while Get still blocked forever on an unclosed doneCh.
func (f *AsyncFuture[T]) IsDone() bool {
	select {
	case <-f.doneCh:
		return true
	default:
		return false
	}
}

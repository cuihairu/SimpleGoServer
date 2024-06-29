package pkg

import (
	"context"
	"time"
)

type Action func(ctx context.Context)
type ActionWithReturn[T any] func(ctx context.Context) (T, error)

type Future[T any] interface {
	Cancel()
	IsCancelled() bool
	IsDone() bool
	Get() (T, error)
	Do()
	GetWithTimeout(timeout time.Duration) (T, error)
}

type Executor[T any] interface {
	Execute(action Action)
	Submit(action ActionWithReturn[T]) Future[T]
	Shutdown()
}

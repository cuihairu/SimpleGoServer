package transport

import (
	"context"
	"io"
)

type BackpressureReader[T any] struct {
	ctx    context.Context
	queue  chan T
	cancel context.CancelFunc
	reader io.Reader
}

func NewBackpressureReader[T any](ctx context.Context, reader io.Reader, readBuffSize int) *BackpressureReader[T] {
	if readBuffSize <= 0 {
		readBuffSize = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	return &BackpressureReader[T]{
		ctx:    ctx,
		queue:  make(chan T, readBuffSize),
		cancel: cancel,
		reader: reader,
	}
}

func (r *BackpressureReader[T]) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(p)
	}
}

func (r *BackpressureReader[T]) Close() error {
	r.cancel()
	return nil
}
func (r *BackpressureReader[T]) loop() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			// pass
		}

	}
}

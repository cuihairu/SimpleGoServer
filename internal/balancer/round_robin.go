package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"sync"
)

type RoundRobinBalancer[T pkg.Backend] struct {
	backends []T
	current  int
	rwMutex  sync.RWMutex
}

var _ pkg.Balancer[pkg.Backend] = (*RoundRobinBalancer[pkg.Backend])(nil)

func NewRoundRobinBalancer[T pkg.Backend]() *RoundRobinBalancer[T] {
	return &RoundRobinBalancer[T]{
		backends: make([]T, 0),
		current:  0,
		rwMutex:  sync.RWMutex{},
	}
}

func (b *RoundRobinBalancer[T]) Next(key string) (T, error) {
	// a write lock, not RLock: Next advances b.current, and RLock would
	// let concurrent Next calls race on that counter
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	size := len(b.backends)
	var backend T
	if size == 0 {
		return backend, errors.New("no backends registered")
	}
	// Unregister may have shrunk the list below the stored position
	b.current %= size
	backend = b.backends[b.current]
	b.current = (b.current + 1) % size
	return backend, nil
}

func (b *RoundRobinBalancer[T]) Register(backend T) error {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for _, back := range b.backends {
		if back.Id() == backend.Id() {
			return nil
		}
	}
	b.backends = append(b.backends, backend)
	return nil
}

func (b *RoundRobinBalancer[T]) Unregister(unregisterBackend T) error {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for i, back := range b.backends {
		if back.Id() == unregisterBackend.Id() {
			b.backends = append(b.backends[:i], b.backends[i+1:]...)
			return nil
		}
	}
	return nil
}

func (b *RoundRobinBalancer[T]) Size() int {
	b.rwMutex.RLock()
	defer b.rwMutex.RUnlock()
	return len(b.backends)
}

func (b *RoundRobinBalancer[T]) Iterate(f func(b T) bool) {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for _, backend := range b.backends {
		if !f(backend) {
			break
		}
	}
}

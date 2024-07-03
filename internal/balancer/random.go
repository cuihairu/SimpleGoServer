package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"math/rand"
	"sync"
)

type RandomBalancer[T pkg.Backend] struct {
	backends []T
	rwMutex  sync.RWMutex
}

func NewRandomBalancer[T pkg.Backend]() *RandomBalancer[T] {
	return &RandomBalancer[T]{
		backends: make([]T, 0),
		rwMutex:  sync.RWMutex{},
	}
}

func (r *RandomBalancer[T]) Next(key string) (T, error) {
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	var b T
	if len(r.backends) == 0 {
		return b, errors.New("no backends")
	}
	if len(r.backends) == 1 {
		return r.backends[0], nil
	}

	return r.backends[rand.Intn(len(r.backends))], nil
}

func (r *RandomBalancer[T]) Register(registerBackend T) error {
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	for _, back := range r.backends {
		if back.Id() == registerBackend.Id() {
			return nil
		}
	}
	r.backends = append(r.backends, registerBackend)
	return nil
}

func (r *RandomBalancer[T]) Unregister(unregisterBackend T) error {
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	for i, back := range r.backends {
		if back.Id() == unregisterBackend.Id() {
			r.backends = append(r.backends[:i], r.backends[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *RandomBalancer[T]) Size() int {
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	return len(r.backends)
}

func (r *RandomBalancer[T]) Iterate(f func(b T) bool) {
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	for _, back := range r.backends {
		if !f(back) {
			break
		}
	}
}

var _ pkg.Balancer[pkg.CountBackend] = (*RandomBalancer[pkg.CountBackend])(nil)

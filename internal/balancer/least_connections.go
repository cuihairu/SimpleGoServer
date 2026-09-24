package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"sync"
)

type LeastConnectionsBalancer[T pkg.CountBackend] struct {
	backends []T
	// updateCount=true makes the balancer do the bookkeeping: every pick
	// bumps the winner's count so repeated Next calls rotate across peers.
	// There is no decrement hook — backends whose count must track live
	// connections should maintain it themselves and pass false, or the
	// count only ever grows.
	updateCount bool
	rwMutex     sync.RWMutex
}

func NewLeastConnectionsBalancer[T pkg.CountBackend](updateCount bool) *LeastConnectionsBalancer[T] {
	return &LeastConnectionsBalancer[T]{
		backends:    make([]T, 0),
		rwMutex:     sync.RWMutex{},
		updateCount: updateCount,
	}
}

func (l *LeastConnectionsBalancer[T]) Next(key string) (T, error) {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	size := len(l.backends)
	var next T
	if size == 0 {
		return next, errors.New("no backends registered")
	}
	next = l.backends[0]
	defer func() {
		if l.updateCount {
			next.SetCount(next.Count() + 1)
		}
	}()
	if size == 1 {
		return next, nil
	}
	leastConn := l.backends[0].Count()
	idx := 0
	for i := 1; i < len(l.backends); i++ {
		if leastConn > l.backends[i].Count() {
			leastConn = l.backends[i].Count()
			idx = i
		}
	}
	next = l.backends[idx]
	return next, nil
}

func (l *LeastConnectionsBalancer[T]) Register(registerBackend T) error {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	for _, back := range l.backends {
		if back.Id() == registerBackend.Id() {
			if l.updateCount {
				back.SetCount(registerBackend.Count())
			}
			return nil
		}
	}
	l.backends = append(l.backends, registerBackend)
	return nil
}

func (l *LeastConnectionsBalancer[T]) Unregister(b T) error {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	for i, back := range l.backends {
		if back.Id() == b.Id() {
			l.backends = append(l.backends[:i], l.backends[i+1:]...)
			return nil
		}
	}
	return nil
}

func (l *LeastConnectionsBalancer[T]) Size() int {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	return len(l.backends)
}

func (l *LeastConnectionsBalancer[T]) Iterate(f func(b T) bool) {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	for _, back := range l.backends {
		if !f(back) {
			break
		}
	}
}

var _ pkg.Balancer[pkg.CountBackend] = (*LeastConnectionsBalancer[pkg.CountBackend])(nil)

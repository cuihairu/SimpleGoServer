package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"net"
	"sync"
)

type RoundRobinBalancer struct {
	backends []pkg.Backend
	current  int
	rwMutex  sync.RWMutex
}

var _ pkg.Balancer = (*RoundRobinBalancer)(nil)

func NewRoundRobinBalancer() *RoundRobinBalancer {
	return &RoundRobinBalancer{
		backends: make([]pkg.Backend, 0),
		current:  0,
		rwMutex:  sync.RWMutex{},
	}
}

func (b *RoundRobinBalancer) Next(ch net.Conn) (pkg.Backend, error) {
	b.rwMutex.RLock()
	defer b.rwMutex.RUnlock()
	size := len(b.backends)
	if size == 0 {
		return nil, errors.New("no backends registered")
	}
	backend := b.backends[b.current]
	b.current = (b.current + 1) % size
	return backend, nil
}

func (b *RoundRobinBalancer) Register(backend pkg.Backend) error {
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

func (b *RoundRobinBalancer) Unregister(unregisterBackend pkg.Backend) error {
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

func (b *RoundRobinBalancer) Size() int {
	b.rwMutex.RLock()
	defer b.rwMutex.RUnlock()
	return len(b.backends)
}

func (b *RoundRobinBalancer) Iterate(f func(b pkg.Backend) bool) {
	b.rwMutex.Lock()
	defer b.rwMutex.Unlock()
	for _, backend := range b.backends {
		if !f(backend) {
			break
		}
	}
}

package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"math/rand"
	"net"
	"sync"
)

type RandomBalancer struct {
	backends []pkg.Backend
	rwMutex  sync.RWMutex
}

func NewRandomBalancer() *RandomBalancer {
	return &RandomBalancer{
		backends: make([]pkg.Backend, 0),
		rwMutex:  sync.RWMutex{},
	}
}

func (r *RandomBalancer) Next(ch net.Conn) (pkg.Backend, error) {
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	if len(r.backends) == 0 {
		return nil, errors.New("no backends")
	}
	if len(r.backends) == 1 {
		return r.backends[0], nil
	}

	return r.backends[rand.Intn(len(r.backends))], nil
}

func (r *RandomBalancer) Register(registerBackend pkg.Backend) error {
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

func (r *RandomBalancer) Unregister(unregisterBackend pkg.Backend) error {
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

func (r *RandomBalancer) Size() int {
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	return len(r.backends)
}

func (r *RandomBalancer) Iterate(f func(b pkg.Backend) bool) {
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	for _, back := range r.backends {
		if !f(back) {
			break
		}
	}
}

var _ pkg.Balancer = (*RandomBalancer)(nil)

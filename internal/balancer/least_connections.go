package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"sync"
)

type LeastConnectionsBalancer struct {
	backends []pkg.WeightBackend
	rwMutex  sync.RWMutex
}

func NewLeastConnectionsBalancer() *LeastConnectionsBalancer {
	return &LeastConnectionsBalancer{
		backends: make([]pkg.WeightBackend, 0),
		rwMutex:  sync.RWMutex{},
	}
}

func (l *LeastConnectionsBalancer) Next(key string) (pkg.Backend, error) {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	size := len(l.backends)
	if size == 0 {
		return nil, errors.New("no backends registered")
	}
	if size == 1 {
		l.backends[0].SetWeight(l.backends[0].Weight() + 1)
		return l.backends[0], nil
	}
	leastConn := l.backends[0].Weight()
	idx := 0
	for i := 1; i < len(l.backends); i++ {
		if leastConn > l.backends[i].Weight() {
			leastConn = l.backends[i].Weight()
			idx = i
		}
	}
	l.backends[idx].SetWeight(l.backends[idx].Weight() + 1)
	return l.backends[idx], nil
}

func (l *LeastConnectionsBalancer) Register(registerBackend pkg.Backend) error {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	weightedBackend, ok := registerBackend.(pkg.WeightBackend)
	if !ok {
		return errors.New("register backend is not a WeightBackend")
	}
	for _, back := range l.backends {
		if back.Id() != registerBackend.Id() {
			continue
		}
		if back.Weight() != weightedBackend.Weight() {
			back.SetWeight(weightedBackend.Weight())
			return nil
		}
	}
	l.backends = append(l.backends, weightedBackend)
	return nil
}

func (l *LeastConnectionsBalancer) Unregister(b pkg.Backend) error {
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

func (l *LeastConnectionsBalancer) Size() int {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	return len(l.backends)
}

func (l *LeastConnectionsBalancer) Iterate(f func(b pkg.Backend) bool) {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	for _, back := range l.backends {
		if !f(back) {
			break
		}
	}
}

var _ pkg.Balancer = (*LeastConnectionsBalancer)(nil)

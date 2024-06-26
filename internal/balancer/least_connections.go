package balancer

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg"
	"net"
	"sync"
)

type LeastConnectionsBalancer struct {
	backends    []pkg.Backend
	indexToConn []int
	rwMutex     sync.RWMutex
}

func NewLeastConnectionsBalancer() *LeastConnectionsBalancer {
	return &LeastConnectionsBalancer{
		backends:    make([]pkg.Backend, 0),
		indexToConn: make([]int, 0),
		rwMutex:     sync.RWMutex{},
	}
}

func (l *LeastConnectionsBalancer) Next(ch net.Conn) (pkg.Backend, error) {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	size := len(l.indexToConn)
	if size == 0 {
		return nil, errors.New("no backends registered")
	}
	if size == 1 {
		l.indexToConn[0]++
		return l.backends[0], nil
	}
	leastConn := l.indexToConn[0]
	idx := 0
	for i := 1; i < len(l.indexToConn); i++ {
		if leastConn > l.indexToConn[i] {
			leastConn = l.indexToConn[i]
			idx = i
		}
	}
	l.indexToConn[idx]++
	return l.backends[idx], nil
}

func (l *LeastConnectionsBalancer) Register(registerBackend pkg.Backend) error {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	for _, back := range l.backends {
		if back.Id() == registerBackend.Id() {
			return nil
		}
	}
	l.backends = append(l.backends, registerBackend)
	l.indexToConn = append(l.indexToConn, 0)
	return nil
}

func (l *LeastConnectionsBalancer) Unregister(b pkg.Backend) error {
	l.rwMutex.Lock()
	defer l.rwMutex.Unlock()
	for i, back := range l.backends {
		if back.Id() == b.Id() {
			l.backends = append(l.backends[:i], l.backends[i+1:]...)
			l.indexToConn = append(l.indexToConn[:i], l.indexToConn[i+1:]...)
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

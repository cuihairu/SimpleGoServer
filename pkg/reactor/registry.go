package reactor

import (
	"net"
	"sync"
)

// ConnectionRegistry tracks the connections currently owned by the server.
// It is what makes graceful shutdown possible: the reactor can enumerate
// and force-close whatever is still open instead of leaking goroutines
// blocked in conn.Read.
type ConnectionRegistry struct {
	mu    sync.RWMutex
	conns map[net.Conn]struct{}
}

func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{conns: make(map[net.Conn]struct{})}
}

func (r *ConnectionRegistry) Add(conn net.Conn) {
	r.mu.Lock()
	r.conns[conn] = struct{}{}
	r.mu.Unlock()
}

func (r *ConnectionRegistry) Remove(conn net.Conn) {
	r.mu.Lock()
	delete(r.conns, conn)
	r.mu.Unlock()
}

// Len reports how many connections are currently tracked.
func (r *ConnectionRegistry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// CloseAll closes every tracked connection and returns how many were
// closed. Reads blocked on those connections return immediately, which is
// what unblocks the per-connection goroutines.
func (r *ConnectionRegistry) CloseAll() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	closed := 0
	for conn := range r.conns {
		if err := conn.Close(); err == nil {
			closed++
		}
	}
	r.conns = make(map[net.Conn]struct{})
	return closed
}

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
	mu     sync.RWMutex
	conns  map[net.Conn]struct{}
	closed bool
}

func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{conns: make(map[net.Conn]struct{})}
}

// Add tracks a connection and reports whether the registry accepted it.
// It returns false once CloseAll has swept: shutdown can no longer see —
// and will never close — such a connection, so the caller must close it
// itself and must not start a handler for it. Serializing this gate with
// CloseAll under the same mutex closes the registration race: whichever
// order the two run in, every accepted connection is guaranteed to be
// force-closed, and every rejected one is guaranteed to be closed by the
// rejecting worker. Without the gate, a connection handed to a worker just
// before shutdown could be registered after the sweep — invisible to the
// drain count and to CloseAll alike — and its handler would park in
// conn.Read forever.
func (r *ConnectionRegistry) Add(conn net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.conns[conn] = struct{}{}
	return true
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
// what unblocks the per-connection goroutines. The registry stays closed
// afterwards: shutdown is terminal (the reactor runs it under a sync.Once),
// so later Adds are rejected rather than tracked into a sweep that will
// never happen.
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
	r.closed = true
	return closed
}

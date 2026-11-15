package proto

import (
	"bytes"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gateConn is a scriptable net.Conn whose Write calls park on content-keyed
// gates before completing, and whose completion order is the recorded wire
// order. It exists to pin one scheduling question deterministically: can a
// single-frame request complete on the wire between another caller's
// fragments?
type gateConn struct {
	mu   sync.Mutex
	log  [][]byte      // completed writes, in wire order
	read chan struct{} // closed to unblock Read

	// gate hooks, consulted at Write entry
	tinyEntered atomic.Int32 // Write seen carrying the "tiny" action
	lastEntered atomic.Int32 // Write seen carrying the last-fragment marker

	letTiny chan struct{}
	letLast chan struct{}
}

func newGateConn() *gateConn {
	return &gateConn{read: make(chan struct{}), letTiny: make(chan struct{}), letLast: make(chan struct{})}
}

func (g *gateConn) record(p []byte) {
	g.mu.Lock()
	g.log = append(g.log, append([]byte(nil), p...))
	g.mu.Unlock()
}

func (g *gateConn) wireOrder() [][]byte {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([][]byte(nil), g.log...)
}

func (g *gateConn) Write(p []byte) (int, error) {
	switch {
	case bytes.Contains(p, []byte(`"action":"tiny"`)):
		g.tinyEntered.Add(1)
		<-g.letTiny
	case bytes.Contains(p, []byte("ENDMARK")):
		g.lastEntered.Add(1)
		<-g.letLast
	}
	g.record(p)
	return len(p), nil
}

func (g *gateConn) Read([]byte) (int, error) {
	<-g.read
	return 0, net.ErrClosed
}

// the remaining net.Conn surface is irrelevant to the write-order question
func (g *gateConn) Close() error {
	select {
	case <-g.read:
	default:
		close(g.read)
	}
	return nil
}
func (g *gateConn) LocalAddr() net.Addr              { return nil }
func (g *gateConn) RemoteAddr() net.Addr             { return nil }
func (g *gateConn) SetDeadline(time.Time) error      { return nil }
func (g *gateConn) SetReadDeadline(time.Time) error  { return nil }
func (g *gateConn) SetWriteDeadline(time.Time) error { return nil }

// TestCallWriteOrderNoInterleavedFragments pins the outbound write-order
// guarantee: fragments of one fragmented call must reach the wire back to
// back, with no concurrent single-frame call slipping in between. The
// scheduler parks the tiny single-frame write at the gate and parks the big
// call's last fragment at another gate, then releases tiny while big is
// mid-message — the exact interleaving window the old single-frame path
// (write after unlocking) allowed. With the shared write lock, tiny cannot
// complete between big's fragments no matter how the gates are released.
func TestCallWriteOrderNoInterleavedFragments(t *testing.T) {
	conn := newGateConn()
	c := NewClient(conn, nil)
	defer c.Close()

	big, err := EncodeJSON(REQUEST, 0, "big", strings.Repeat("A", 64)+"ENDMARK")
	if err != nil {
		t.Fatalf("EncodeJSON(big): %v", err)
	}
	// exactly two fragments: the first carries the envelope head with the
	// action, the second carries a 12-byte tail that still contains the
	// full ENDMARK marker (the split point would otherwise shave its
	// first byte off)
	c.streamThreshold = len(big.Payload) - 12
	tiny, err := EncodeJSON(REQUEST, 0, "tiny", "t")
	if err != nil {
		t.Fatalf("EncodeJSON(tiny): %v", err)
	}
	if len(tiny.Payload) >= c.streamThreshold {
		t.Fatalf("tiny payload (%d bytes) must stay a single frame below the threshold (%d)", len(tiny.Payload), c.streamThreshold)
	}

	tinyDone := make(chan struct{})
	go func() {
		defer close(tinyDone)
		_, _ = c.Call("tiny", "t", 2*time.Second)
	}()

	// Wait until tiny has reached its write (old code: parked at the gate
	// past the pending lock; new code: parked on the write lock itself).
	// The bounded fallback keeps the schedule moving under both codes.
	deadline := time.Now().Add(300 * time.Millisecond)
	for conn.tinyEntered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	bigDone := make(chan struct{})
	go func() {
		defer close(bigDone)
		_, _ = c.Call("big", strings.Repeat("A", 64)+"ENDMARK", 2*time.Second)
	}()

	// Can big reach its parked last fragment while tiny is still parked?
	// Only code that lets tiny hold no lock across its write can — the
	// interleaving window. Code that serializes writes parks tiny INSIDE
	// the write lock, so big cannot even start: there the window is
	// structurally gone, and the schedule falls back to a plain finish.
	reachedMid := false
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(conn.wireOrder()) >= 1 && conn.lastEntered.Load() == 1 {
			reachedMid = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if reachedMid {
		// tiny holds no lock: release it mid-message — the exact window
		// an unlocked single-frame write slips through — then finish big.
		close(conn.letTiny)
		time.Sleep(50 * time.Millisecond)
		close(conn.letLast)
	} else {
		// writes are serialized: tiny is parked holding the write lock,
		// so finish tiny first; big then writes its whole message and
		// parks on the last fragment.
		close(conn.letTiny)
		deadline = time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if len(conn.wireOrder()) >= 2 && conn.lastEntered.Load() == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("big call never reached its last fragment; wire so far: %d write(s)", len(conn.wireOrder()))
			}
			time.Sleep(2 * time.Millisecond)
		}
		close(conn.letLast)
	}

	select {
	case <-tinyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("tiny call did not finish")
	}
	select {
	case <-bigDone:
	case <-time.After(2 * time.Second):
		t.Fatal("big call did not finish")
	}

	// Assert wire order: tiny must not sit between big's two fragments.
	wire := conn.wireOrder()
	var frag1, frag2, tinyAt = -1, -1, -1
	for i, w := range wire {
		switch {
		case bytes.Contains(w, []byte(`"action":"big"`)):
			if frag1 == -1 {
				frag1 = i
			}
		case bytes.Contains(w, []byte("ENDMARK")):
			frag2 = i
		case bytes.Contains(w, []byte(`"action":"tiny"`)):
			tinyAt = i
		}
	}
	if frag1 == -1 || frag2 == -1 || tinyAt == -1 {
		t.Fatalf("wire order incomplete: frag1=%d frag2=%d tiny=%d (log has %d writes)", frag1, frag2, tinyAt, len(wire))
	}
	if frag1 < tinyAt && tinyAt < frag2 {
		t.Fatalf("single-frame call interleaved between fragments: frag1=%d tiny=%d frag2=%d", frag1, tinyAt, frag2)
	}
}

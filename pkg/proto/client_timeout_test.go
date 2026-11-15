package proto

import (
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// stallConn is a net.Conn whose writes park until released or until the
// armed write deadline passes — the shape of a peer that stopped reading.
// It models the kernel's deadline semantics (a blocked Write returns
// os.ErrDeadlineExceeded once the deadline fires) so the test exercises the
// same timeout path a real socket would.
type stallConn struct {
	mu sync.Mutex
	wd time.Time
	// release unblocks a parked write; closed parks Read so the read loop
	// stays alive (a silent peer sends nothing and closes nothing).
	release chan struct{}
	read    chan struct{}
}

func newStallConn() *stallConn {
	return &stallConn{release: make(chan struct{}), read: make(chan struct{})}
}

func (s *stallConn) Write(p []byte) (int, error) {
	s.mu.Lock()
	dl := s.wd
	s.mu.Unlock()
	if dl.IsZero() {
		<-s.release
		return len(p), nil
	}
	delay := time.Until(dl)
	if delay <= 0 {
		return 0, os.ErrDeadlineExceeded
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.release:
		return len(p), nil
	case <-timer.C:
		return 0, os.ErrDeadlineExceeded
	}
}

func (s *stallConn) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.wd = t
	s.mu.Unlock()
	return nil
}

func (s *stallConn) Read([]byte) (int, error) {
	<-s.read
	return 0, net.ErrClosed
}

func (s *stallConn) Close() error {
	select {
	case <-s.read:
	default:
		close(s.read)
	}
	return nil
}
func (s *stallConn) LocalAddr() net.Addr             { return nil }
func (s *stallConn) RemoteAddr() net.Addr            { return nil }
func (s *stallConn) SetDeadline(time.Time) error     { return nil }
func (s *stallConn) SetReadDeadline(time.Time) error { return nil }

// TestCallTimeoutBoundsWedgedWrite pins the send-side timeout: a peer that
// stops reading parks the socket write, and Call's timeout must turn that
// into a deadline error instead of hanging the caller (and, via writeMu,
// every other caller on the connection) forever.
func TestCallTimeoutBoundsWedgedWrite(t *testing.T) {
	conn := newStallConn()
	c := NewClient(conn, nil)
	defer c.Close()

	done := make(chan error, 1)
	go func() {
		_, err := c.Call("stuck", "x", 150*time.Millisecond)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Call on a wedged write returned %v; want the write deadline error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Call with a timeout hung past 2s on a wedged write; the send must respect the deadline")
	}
}

// TestDialErrorSurfaces pins that Dial reports dial failures. The resilient
// path used to be its only error-route exerciser; it dials through
// DialTimeout now, so this keeps Dial's own error branch honest.
func TestDialErrorSurfaces(t *testing.T) {
	if _, err := Dial("127.0.0.1:1", nil); err == nil {
		t.Fatal("Dial to a dead address must report an error")
	}
}

// TestDialTimeoutReachesResilientDial pins that the resilient reconnect
// passes its handshake budget down to the dialer: the reconnect bound must
// cover the dial itself, not only the HELLO round trip.
func TestDialTimeoutReachesResilientDial(t *testing.T) {
	realDial := dialTimeout
	defer func() { dialTimeout = realDial }()

	type captured struct {
		network string
		addr    string
		timeout time.Duration
	}
	captures := make(chan captured, 8)
	dialTimeout = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		captures <- captured{network, addr, timeout}
		return nil, net.ErrClosed // fail fast; only the bound is under test
	}

	rc := NewResilientClient("127.0.0.1:65535", nil, &ResilientOptions{HandshakeTimeout: 1234 * time.Millisecond})
	defer rc.Close()

	if _, _, err := rc.dialAndHandshake(rc.opts.handshakeTimeout()); err == nil {
		t.Fatal("dialAndHandshake on a failing dialer must report an error")
	}
	select {
	case got := <-captures:
		if got.timeout != 1234*time.Millisecond {
			t.Fatalf("dialer received timeout %s, want the handshake budget 1234ms", got.timeout)
		}
		if got.network != "tcp" || got.addr != "127.0.0.1:65535" {
			t.Fatalf("dialer received %s %s, want tcp 127.0.0.1:65535", got.network, got.addr)
		}
	default:
		t.Fatal("dialAndHandshake never reached the dialer")
	}
}

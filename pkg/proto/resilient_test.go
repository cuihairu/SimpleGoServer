package proto

import (
	"errors"
	"net"
	"testing"
	"time"
)

// killCurrent closes the connection the resilient client currently uses,
// simulating a network drop from the outside.
func killCurrent(t *testing.T, rc *ResilientClient) *Client {
	t.Helper()
	rc.mu.Lock()
	c := rc.client
	rc.mu.Unlock()
	if c == nil {
		t.Fatal("no current client to kill")
	}
	_ = c.Close()
	return c
}

// TestResilientClientRecoversFromDrop covers the happy path: a subscribed
// client whose connection drops is reconnected and the server restores the
// subscription via the session token — pushes keep flowing.
func TestResilientClientRecoversFromDrop(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes := make(chan *Frame, 1)
	rc := NewResilientClient(addr, func(frame *Frame) { pushes <- frame }, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	dead := killCurrent(t, rc)

	// the drop must not close the resilient client, and the subscription
	// must come back on the reconnected transport
	deadline := time.After(3 * time.Second)
	for {
		if _, err := ph.Publish("ticks", "still there?"); err != nil {
			t.Fatalf("Publish(): %v", err)
		}
		select {
		case <-deadline:
			t.Fatal("no push within 3s — reconnection or subscription restore failed")
		case frame := <-pushes:
			msg, err := DecodeJSONMessage(frame)
			if err != nil || msg.Action != "ticks" {
				t.Fatalf("unexpected push %v (err=%v)", frame, err)
			}
			// a resumed push can be delivered by the new client's readLoop
			// before swap() installs it — Dial starts reading immediately,
			// and the server restores the subscription during the handshake.
			// Give the install a moment; if the transport really stayed
			// dead, rc.client never flips and this fails below.
			installDeadline := time.Now().Add(2 * time.Second)
			for {
				rc.mu.Lock()
				alive := rc.client != nil && rc.client != dead
				rc.mu.Unlock()
				if alive {
					return
				}
				if time.Now().After(installDeadline) {
					t.Fatal("push arrived on the dead connection")
				}
				time.Sleep(5 * time.Millisecond)
			}
		case <-time.After(100 * time.Millisecond):
			// retry the publish; early attempts may race the reconnect
		}
	}
}

// TestResilientClientResubscribesWhenSessionExpired forces the server to
// forget sessions (TTL below zero), so the reconnect cannot resume: the
// client must notice Resumed == false and re-send its subscriptions.
func TestResilientClientResubscribesWhenSessionExpired(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.sessionTTL = -time.Second
	ph.mu.Unlock()

	pushes := make(chan *Frame, 1)
	rc := NewResilientClient(addr, func(frame *Frame) { pushes <- frame }, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	killCurrent(t, rc)

	deadline := time.After(3 * time.Second)
	for {
		if _, err := ph.Publish("ticks", "resubscribed?"); err != nil {
			t.Fatalf("Publish(): %v", err)
		}
		select {
		case <-deadline:
			t.Fatal("no push within 3s — re-subscribe after expired session failed")
		case frame := <-pushes:
			msg, _ := DecodeJSONMessage(frame)
			if msg.Action != "ticks" {
				t.Fatalf("unexpected push %v", frame)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestResilientClientSurvivesDeadServer checks the no-recovery-yet path:
// with the server gone the reconnect loop keeps backing off, the client
// stays open (drops are recoverable), and Close ends everything cleanly.
func TestResilientClientSurvivesDeadServer(t *testing.T) {
	// the test owns the listener because it must kill the whole server,
	// not just the current connection — startTCPServer's cleanup would be
	// too late for that
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ph := NewProtocolHandler(nil)
	t.Cleanup(func() {
		ph.Close()
		_ = listener.Close()
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveConn(conn, ph)
		}
	}()

	rc := NewResilientClient(listener.Addr().String(), nil, &ResilientOptions{BackoffStart: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond})
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}

	// close the listener first: with no server to accept, the reconnect
	// loop deterministically never succeeds. The reverse order raced — the
	// loop could re-dial before the server died and "Call must fail" would
	// flake (CI caught exactly that under load).
	_ = listener.Close()
	killCurrent(t, rc)

	// drops do not close the client; calls fail while the server is gone
	if _, err := rc.Call("echo", "x", 2*time.Second); err == nil {
		t.Fatal("Call on a dead connection unexpectedly succeeded")
	}
	select {
	case <-rc.Done():
		t.Fatal("Done closed by a recoverable drop")
	case <-time.After(100 * time.Millisecond):
	}

	if err := rc.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	select {
	case <-rc.Done():
		// the reconnect loop stopped with the client
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not close after Close")
	}
}

// TestResilientClientRequiresConnect pins the API contract before Connect.
func TestResilientClientRequiresConnect(t *testing.T) {
	rc := NewResilientClient("127.0.0.1:1", nil, nil)
	if _, err := rc.Call("echo", nil, time.Second); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Call before Connect = %v, want ErrNotConnected", err)
	}
	if err := rc.Subscribe("t", time.Second); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Subscribe before Connect = %v, want ErrNotConnected", err)
	}
}

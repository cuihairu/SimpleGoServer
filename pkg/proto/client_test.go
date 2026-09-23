package proto

import (
	"net"
	"testing"
	"time"
)

// TestClientKeepAlivePingsRegularly checks that the keepalive loop keeps
// issuing pings while the connection is healthy, and can be stopped.
func TestClientKeepAlivePingsRegularly(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	stop := client.KeepAlive(20*time.Millisecond, time.Second)
	defer stop()

	// the loop needs a few ticks to fire; the server counts the pings
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if pings, _ := ph.Stats(); pings >= 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pings, _ := ph.Stats(); pings < 3 {
		t.Fatalf("server saw %d pings during 3s of 20ms keepalive, want >= 3", pings)
	}
}

// TestClientKeepAliveDetectsDeadServer checks the failure path: a server
// that accepts but never answers makes every ping time out, and the
// keepalive loop must close the client so watchers of Done are released.
func TestClientKeepAliveDetectsDeadServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	blackhole := make(chan net.Conn, 1) // hold the accepted conn open
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			blackhole <- conn // never read from it: pings get no reply
		}
	}()

	client, err := Dial(listener.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}

	stop := client.KeepAlive(20*time.Millisecond, 200*time.Millisecond)
	defer stop()

	select {
	case <-client.Done():
		// ping timeouts were detected and the client closed itself
	case <-time.After(2 * time.Second):
		t.Fatal("keepalive did not close the client after repeated ping timeouts")
	}
}

package proto

import (
	"encoding/json"
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

// TestClientHandshake checks the happy path end to end: the negotiated
// version is reported, remembered on the client, and the connection keeps
// working for regular calls afterwards.
func TestClientHandshake(t *testing.T) {
	echo := func(action string, data []byte) (any, error) {
		return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
	}
	addr, _ := startTCPServer(t, echo)

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	if got := client.NegotiatedVersion(); got != 0 {
		t.Fatalf("NegotiatedVersion() before handshake = %d, want 0", got)
	}
	res, err := client.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if res.Version != ProtocolVersion {
		t.Fatalf("negotiated version = %d, want %d", res.Version, ProtocolVersion)
	}
	if client.NegotiatedVersion() != ProtocolVersion {
		t.Fatalf("NegotiatedVersion() = %d, want %d", client.NegotiatedVersion(), ProtocolVersion)
	}

	resp, err := client.Call("echo", "still alive", 2*time.Second)
	if err != nil || !resp.OK() {
		t.Fatalf("Call after handshake: resp=%v err=%v", resp, err)
	}
}

// TestClientHandshakeRejectedClosesConnection uses a fake server that
// always answers HELLO with an error, and checks that the client closes
// itself: there is no point in talking to a peer that cannot understand
// our frames.
func TestClientHandshakeRejectedClosesConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		// read the HELLO and reject whatever it offers
		if _, err := Decode(conn); err == nil {
			if frame, err := EncodeJSON(RESPONSE, 1, "hello", &ErrorMessage{Error: "version too new"}); err == nil {
				if buf, err := Encode(frame); err == nil {
					_, _ = conn.Write(buf)
				}
			}
		}
	}()

	client, err := Dial(listener.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}

	if _, err := client.Handshake(2 * time.Second); err == nil {
		t.Fatal("Handshake() unexpectedly succeeded against a rejecting server")
	}
	select {
	case <-client.Done():
		// the client closed itself after the rejection
	case <-time.After(2 * time.Second):
		t.Fatal("client stayed open after a rejected handshake")
	}
}

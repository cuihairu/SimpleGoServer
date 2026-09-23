package reactor

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// echoInitializer wires the frame codec and an echo protocol handler into
// every accepted connection.
func echoInitializer(p handler.Pipeline) error {
	_ = p.AddLast(
		proto.NewFrameCodec(),
		proto.NewProtocolHandler(func(action string, data []byte) (any, error) {
			return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
		}),
	)
	return nil
}

// newTestOptions binds the reactor to an ephemeral loopback port.
func newTestOptions(t *testing.T) pkg.Options {
	t.Helper()
	opts := NewServerOptions()
	opts.Listener = "tcp://127.0.0.1:0"
	return opts
}

func startTestReactor(t *testing.T) *Reactor {
	t.Helper()
	reactor, err := NewReactor(newTestOptions(t), nil, nil, echoInitializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	go reactor.Run()
	t.Cleanup(func() { _ = reactor.ShutdownWithTimeout(time.Second) })
	return reactor
}

func waitListening(t *testing.T, reactor *Reactor) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", reactor.Addr().String(), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("reactor never started listening on %s", reactor.Addr())
}

func TestReactorServesProtocolEcho(t *testing.T) {
	reactor := startTestReactor(t)
	waitListening(t, reactor)

	client, err := proto.Dial(reactor.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	resp, err := client.Call("echo", "hello reactor", 5*time.Second)
	if err != nil {
		t.Fatalf("Call(): %v", err)
	}
	if !resp.OK() {
		t.Fatalf("response error: %s", resp.Err.Error)
	}
	if want := `"data":"hello reactor"`; !strings.Contains(string(resp.Data), want) {
		t.Fatalf("data = %s, want it to contain %s", resp.Data, want)
	}
}

func TestReactorGracefulShutdownClosesLiveConnections(t *testing.T) {
	reactor := startTestReactor(t)
	waitListening(t, reactor)

	conn, err := net.Dial("tcp", reactor.Addr().String())
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer conn.Close()

	start := time.Now()
	if err := reactor.ShutdownWithTimeout(500 * time.Millisecond); err != nil {
		t.Fatalf("ShutdownWithTimeout(): %v", err)
	}
	// the live connection never drains on its own, so shutdown must take
	// about the drain timeout — not hang, not return instantly
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("shutdown took %s, expected ~drain timeout", elapsed)
	}

	// the live connection must have been force-closed
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 16)); err == nil {
		t.Fatal("expected the live connection to be closed by shutdown")
	}
}

func TestReactorDrainsClosedConnectionsQuickly(t *testing.T) {
	reactor := startTestReactor(t)
	waitListening(t, reactor)

	// open and immediately close a client: the server-side connection
	// should observe EOF and release without the force-close stage
	conn, err := net.Dial("tcp", reactor.Addr().String())
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	_ = conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reactor.workers.TotalCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("connection count never dropped to 0 (still %d)", reactor.workers.TotalCount())
}

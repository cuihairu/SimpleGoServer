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
func newTestOptions(tb testing.TB) pkg.Options {
	tb.Helper()
	opts := NewServerOptions()
	opts.Listener = "tcp://127.0.0.1:0"
	return opts
}

func startTestReactor(tb testing.TB) *Reactor {
	tb.Helper()
	reactor, err := NewReactor(newTestOptions(tb), nil, nil, echoInitializer, nil)
	if err != nil {
		tb.Fatalf("NewReactor(): %v", err)
	}
	go reactor.Run()
	tb.Cleanup(func() { _ = reactor.ShutdownWithTimeout(time.Second) })
	return reactor
}

// startTestReactorWithIdle is startTestReactor with dead-link reaping armed.
func startTestReactorWithIdle(tb testing.TB, idle time.Duration) *Reactor {
	tb.Helper()
	opts := newTestOptions(tb)
	opts.(*ServerOptions).IdleTimeout = idle
	reactor, err := NewReactor(opts, nil, nil, echoInitializer, nil)
	if err != nil {
		tb.Fatalf("NewReactor(): %v", err)
	}
	go reactor.Run()
	tb.Cleanup(func() { _ = reactor.ShutdownWithTimeout(time.Second) })
	return reactor
}

func waitListening(tb testing.TB, reactor *Reactor) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", reactor.Addr().String(), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("reactor never started listening on %s", reactor.Addr())
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

func TestReactorReapsIdleConnections(t *testing.T) {
	reactor := startTestReactorWithIdle(t, 300*time.Millisecond)
	waitListening(t, reactor)

	// connect and never send anything: the server must close the dead
	// connection instead of keeping it forever
	conn, err := net.Dial("tcp", reactor.Addr().String())
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 16)); err == nil {
		t.Fatal("expected the idle connection to be closed by the server")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if reactor.workers.TotalCount() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("idle connection was closed but its handler never exited (count=%d)", reactor.workers.TotalCount())
}

func TestReactorKeepsActiveConnectionsAlive(t *testing.T) {
	reactor := startTestReactorWithIdle(t, 300*time.Millisecond)
	waitListening(t, reactor)

	client, err := proto.Dial(reactor.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	// 10 requests spaced 100ms apart: every request renews the idle
	// allowance, so the connection must survive far past one idle timeout
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		if _, err := client.Call("echo", "keepalive", 2*time.Second); err != nil {
			t.Fatalf("call %d failed on a connection that should stay alive: %v", i, err)
		}
	}
	if reactor.workers.TotalCount() != 1 {
		t.Fatalf("connection count = %d after continuous traffic, want 1", reactor.workers.TotalCount())
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

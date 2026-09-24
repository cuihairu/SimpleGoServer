package reactor

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	balancerImpl "github.com/cuihairu/simplegoserver/internal/balancer"
	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// echoHandler echoes the raw JSON payload back untouched.
func echoHandler(action string, data []byte) (any, error) {
	return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
}

// echoProtocol is shared by every connection of every test reactor. The
// handler owns a session janitor goroutine, so it must be constructed once
// per process — a per-connection construction would leak one janitor per
// accepted connection, none of which ever gets closed.
var echoProtocol = proto.NewProtocolHandler(echoHandler)

// echoInitializer wires the frame codec and the shared protocol handler
// into every accepted connection.
func echoInitializer(p handler.Pipeline) error {
	_ = p.AddLast(
		proto.NewFrameCodec(),
		echoProtocol,
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

// TestWorkerAddConnAfterStopRejectsConnection pins the shutdown race where
// a connection is dispatched to a worker whose loop already returned: the
// blind channel send would park it in a buffer nobody drains, leaking the
// fd for the rest of the process lifetime.
func TestWorkerAddConnAfterStopRejectsConnection(t *testing.T) {
	worker := NewWorker(context.Background(), "worker:test", handlerImpl.NewErrorHandler(), echoInitializer, false, 0, NewConnectionRegistry(), &WorkerGroup{})
	go worker.Run()
	worker.Stop()

	conn, peer := net.Pipe()
	defer peer.Close()
	if err := worker.AddConn(conn); err == nil {
		t.Fatal("AddConn on a stopped worker must fail, not queue silently")
	}
	// the refused connection must be closed by AddConn itself
	_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the refused connection to be closed by AddConn")
	}
}

func TestDispatchClosesConnectionWhenNoBackend(t *testing.T) {
	group := &WorkerGroup{balancer: balancerImpl.NewAdaptiveBalancer[*Worker]()}
	conn, peer := net.Pipe()
	defer peer.Close()
	if err := group.Dispatch(conn); err == nil {
		t.Fatal("Dispatch with no backends must report an error")
	}
	_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the rejected connection to be closed by Dispatch")
	}
}

// TestReactorRunExitsWhenListenerClosedExternally covers the accept loop's
// closed-listener exit: closing the listener outside the shutdown path used
// to leave Run spinning on accept errors forever.
func TestReactorRunExitsWhenListenerClosedExternally(t *testing.T) {
	reactor, err := NewReactor(newTestOptions(t), nil, nil, echoInitializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	done := make(chan struct{})
	go func() {
		reactor.Run()
		close(done)
	}()
	waitListening(t, reactor)

	_ = reactor.listener.Close() // not via shutdown: stopping stays false
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after the listener was closed externally")
	}

	// finish the lifecycle so worker loops and handlers are released
	if err := reactor.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatalf("ShutdownWithTimeout(): %v", err)
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

// TestReactorEventGroupAPI covers the programmatic event.Group surface:
// handing a connection in without going through Run, closing it again,
// and asking the balancer for an event loop.
func TestReactorEventGroupAPI(t *testing.T) {
	reactor := startTestReactor(t)
	waitListening(t, reactor)

	loop := reactor.Next()
	if loop == nil {
		t.Fatal("Next() returned no event loop")
	}
	worker, ok := loop.(*Worker)
	if !ok {
		t.Fatalf("Next() returned %T, want *Worker", loop)
	}
	if worker.Id() == "" {
		t.Fatal("event loop has an empty id")
	}

	// Register hands the connection to a worker even though Run's accept
	// loop is what normally feeds it; Unregister closes it, and the
	// handler must release without the force-close stage.
	conn, peer := net.Pipe()
	defer peer.Close()
	reactor.Register(conn)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && reactor.workers.TotalCount() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if reactor.workers.TotalCount() != 1 {
		t.Fatalf("connection count = %d after Register, want 1", reactor.workers.TotalCount())
	}
	reactor.Unregister(conn)
	for time.Now().Before(deadline) && reactor.workers.TotalCount() != 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if reactor.workers.TotalCount() != 0 {
		t.Fatalf("connection count = %d after Unregister, want 0", reactor.workers.TotalCount())
	}
}

package reactor

import (
	"context"
	"encoding/json"
	"io"
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
			// The probe connection is accepted and its handler reaped
			// asynchronously; on a loaded runner the reap can lag well
			// past this return. Tests that assert exact worker counts
			// must not inherit the phantom entry, so wait for the table
			// to drain before declaring the server ready.
			for time.Now().Before(deadline) && reactor.workers.TotalCount() > 0 {
				time.Sleep(10 * time.Millisecond)
			}
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
	// allowance, so the connection must survive far past one idle
	// timeout. The Call loop itself is the assertion — a failed call
	// means the server killed a connection it should have kept, and a
	// successful one proves the connection is alive end to end.
	//
	// Deliberately no explicit worker-count assert here: on slow 2-core
	// runners the scheduler delay between the last call and the count
	// read competes with the 300ms idle deadline, and once the reaper
	// wins the count reads 0 forever (CI 1.22.x, run 36064425070).
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		if _, err := client.Call("echo", "keepalive", 2*time.Second); err != nil {
			t.Fatalf("call %d failed on a connection that should stay alive: %v", i, err)
		}
	}
	t.Logf("all calls survived; worker count at teardown = %d", reactor.workers.TotalCount())
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
	//
	// Registration is proven by the connection's own protocol traffic,
	// not by the worker count. The count only moves once the worker loop
	// gets scheduled, and an earlier polling form of this test
	// (waitCount against TotalCount, fd78d23 CI failure) spun in 20ms
	// sleeps competing with that very scheduling — red on a loaded
	// 2-core runner even though nothing in the registration path blocks.
	// Here the test goroutine blocks in Write/Read instead, yielding the
	// P to the worker, with net.Pipe deadlines keeping every wait
	// bounded. The echo round-trip is also a strictly stronger fact than
	// a counter bump: the whole pipeline is alive end to end.
	conn, peer := net.Pipe()
	defer peer.Close()
	reactor.Register(conn)

	req, err := proto.EncodeJSON(proto.REQUEST, 1, "echo", "group-api")
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	wire, err := proto.Encode(req)
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	if err := peer.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline(): %v", err)
	}
	// net.Pipe hands Write bytes straight to the reader: the Write
	// completing at all means the frame codec is consuming this
	// connection, which is only possible once Register took effect.
	if n, werr := peer.Write(wire); werr != nil || n != len(wire) {
		t.Fatalf("worker never consumed the registered connection (wrote %d/%d bytes): %v", n, len(wire), werr)
	}

	if err := peer.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline(): %v", err)
	}
	resp, err := proto.Decode(peer)
	if err != nil {
		t.Fatalf("no response on the registered connection: %v", err)
	}
	if resp.Header.FrameType != proto.RESPONSE || resp.Header.StreamId != 1 {
		t.Fatalf("got %v stream %d, want RESPONSE stream 1", resp.Header.FrameType, resp.Header.StreamId)
	}
	msg, err := proto.DecodeJSONMessage(resp)
	if err != nil {
		t.Fatalf("DecodeJSONMessage(): %v", err)
	}
	if msg.Action != "echo" {
		t.Fatalf("response action = %q, want echo", msg.Action)
	}

	// Unregister must close the connection: EOF on the peer is the
	// release-side mirror of the round-trip — the handler tore its
	// pipeline down rather than merely dropping a counter. The deadline
	// is refreshed before the close: net.Pipe's Close invalidates both
	// ends, so setting it afterwards would fail outright.
	if err := peer.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline(): %v", err)
	}
	reactor.Unregister(conn)
	if _, rerr := proto.Decode(peer); rerr != io.EOF {
		t.Fatalf("read after Unregister = %v, want io.EOF (handler must close the connection)", rerr)
	}
}

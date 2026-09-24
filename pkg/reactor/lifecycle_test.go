package reactor

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
)

// recordingEvents satisfies event.Listener while logging every callback
// for assertions. The embedded ErrorHandler only completes the interface;
// every method is overridden.
type recordingEvents struct {
	handlerImpl.ErrorHandler
	mu     sync.Mutex
	events []string
}

func (r *recordingEvents) record(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, name)
}

func (r *recordingEvents) OnStartup()         { r.record("startup") }
func (r *recordingEvents) OnReload()          { r.record("reload") }
func (r *recordingEvents) OnShutdown()        { r.record("shutdown") }
func (r *recordingEvents) OnError(err any)    { r.record("error:" + fmt.Sprint(err)) }
func (r *recordingEvents) OnConnect(net.Conn) { r.record("connect") }

func (r *recordingEvents) has(prefix string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.events {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

// TestReactorReloadAndStop covers the small lifecycle surface: Reload
// forwards to the listener without a running loop, Stop ends Run, and the
// startup callback fired on the way.
func TestReactorReloadAndStop(t *testing.T) {
	rec := &recordingEvents{}
	reactor, err := NewReactor(newTestOptions(t), nil, rec, echoInitializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}

	// Reload is just a forwarded event; it works before Run
	reactor.Reload()
	if !rec.has("reload") {
		t.Fatal("Reload() did not reach the event listener")
	}

	done := make(chan struct{})
	go func() {
		reactor.Run()
		close(done)
	}()
	waitListening(t, reactor)
	if !rec.has("startup") {
		t.Fatal("OnStartup never fired")
	}

	if err := reactor.Stop(); err != nil {
		t.Fatalf("Stop(): %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit after Stop")
	}
}

// TestReactorShutdownGracefullyWhenIdle pins the fast path of the no-arg
// graceful shutdown: with no connections to drain it must not sit through
// the full 10s drain timeout.
func TestReactorShutdownGracefullyWhenIdle(t *testing.T) {
	rec := &recordingEvents{}
	reactor, err := NewReactor(newTestOptions(t), nil, rec, echoInitializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	done := make(chan struct{})
	go func() {
		reactor.Run()
		close(done)
	}()
	waitListening(t, reactor)

	start := time.Now()
	reactor.ShutdownGracefully()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("idle ShutdownGracefully took %s, want an immediate return", elapsed)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after ShutdownGracefully")
	}
	if !rec.has("shutdown") {
		t.Fatal("OnShutdown never fired")
	}
}

// TestReactorRegisterAfterShutdownReportsError: handing a connection over
// once the workers are gone must surface the rejection through OnError,
// not vanish.
func TestReactorRegisterAfterShutdownReportsError(t *testing.T) {
	rec := &recordingEvents{}
	reactor, err := NewReactor(newTestOptions(t), nil, rec, echoInitializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	go reactor.Run()
	waitListening(t, reactor)
	if err := reactor.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatalf("ShutdownWithTimeout(): %v", err)
	}

	conn, peer := net.Pipe()
	defer peer.Close()
	reactor.Register(conn)
	if !rec.has("error:") {
		t.Fatal("Register() after shutdown did not report an error")
	}
}

// TestNewReactorRejectsInvalidConfiguration walks the constructor's error
// branches: wrong options type, unparseable listener URL, unknown network
// scheme, and a port that is already taken.
func TestNewReactorRejectsInvalidConfiguration(t *testing.T) {
	rec := &recordingEvents{}

	if _, err := NewReactor(struct{}{}, nil, rec, nil, nil); err == nil {
		t.Fatal("NewReactor with non-ServerOptions must fail")
	}

	badURL := NewServerOptions()
	badURL.Listener = "://bad"
	if _, err := NewReactor(badURL, nil, rec, nil, nil); err == nil {
		t.Fatal("NewReactor with an unparseable listener must fail")
	}

	badScheme := NewServerOptions()
	badScheme.Listener = "badscheme://127.0.0.1:0"
	if _, err := NewReactor(badScheme, nil, rec, nil, nil); err == nil {
		t.Fatal("NewReactor with an unknown network scheme must fail")
	}

	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer taken.Close()
	badPort := NewServerOptions()
	badPort.Listener = "tcp://" + taken.Addr().String()
	if _, err := NewReactor(badPort, nil, rec, nil, nil); err == nil {
		t.Fatal("NewReactor on a taken port must fail")
	}
}

// TestConnectionRegistryLenAndCloseAll covers the registry counter and the
// bulk close: CloseAll reports what it closed and empties the registry.
func TestConnectionRegistryLenAndCloseAll(t *testing.T) {
	reg := NewConnectionRegistry()
	if reg.Len() != 0 {
		t.Fatalf("Len() = %d on a fresh registry", reg.Len())
	}

	var conns []net.Conn
	for i := 0; i < 3; i++ {
		conn, _ := net.Pipe()
		defer conn.Close()
		conns = append(conns, conn)
		reg.Add(conn)
	}
	if reg.Len() != 3 {
		t.Fatalf("Len() = %d after 3 Adds, want 3", reg.Len())
	}

	reg.Remove(conns[0])
	if reg.Len() != 2 {
		t.Fatalf("Len() = %d after a Remove, want 2", reg.Len())
	}

	if closed := reg.CloseAll(); closed != 2 {
		t.Fatalf("CloseAll() = %d, want 2", closed)
	}
	if reg.Len() != 0 {
		t.Fatalf("Len() = %d after CloseAll, want 0", reg.Len())
	}
	// idempotent: nothing left to close
	if closed := reg.CloseAll(); closed != 0 {
		t.Fatalf("second CloseAll() = %d, want 0", closed)
	}
}

// stuckAddr satisfies net.Addr for the stuck connection below.
type stuckAddr struct{}

func (stuckAddr) Network() string { return "tcp" }
func (stuckAddr) String() string  { return "127.0.0.1:9999" }

// stuckConn simulates a connection whose handler no amount of force-closing
// can release: Close reports success but Read stays parked until the test
// opens the gate. This is the worst case graceful shutdown must survive.
type stuckConn struct {
	readGate   chan struct{}
	parkOnce   sync.Once
	parked     chan struct{}
	closeCalls atomic.Int32
}

func (c *stuckConn) Read(b []byte) (int, error) {
	// signal the first entry so the test only starts a shutdown once the
	// handler is really parked here — a handler that has not run yet would
	// exit cleanly on the cancelled ctx and never hit the leak path
	c.parkOnce.Do(func() { close(c.parked) })
	<-c.readGate
	return 0, io.EOF
}

func (c *stuckConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *stuckConn) Close() error                     { c.closeCalls.Add(1); return nil }
func (c *stuckConn) LocalAddr() net.Addr              { return stuckAddr{} }
func (c *stuckConn) RemoteAddr() net.Addr             { return stuckAddr{} }
func (c *stuckConn) SetDeadline(time.Time) error      { return nil }
func (c *stuckConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stuckConn) SetWriteDeadline(time.Time) error { return nil }

// TestReactorShutdownReportsStuckHandler pins the leak-reporting contract:
// a handler that outlives the force-close stage makes ShutdownWithTimeout
// return an error instead of an unconditional nil, and the force-close
// really was attempted on the stuck connection.
func TestReactorShutdownReportsStuckHandler(t *testing.T) {
	rec := &recordingEvents{}
	quietLogger := log.New(io.Discard, "", 0)
	reactor, err := NewReactor(newTestOptions(t), quietLogger, rec, echoInitializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	go reactor.Run()
	waitListening(t, reactor)

	conn := &stuckConn{readGate: make(chan struct{}), parked: make(chan struct{})}
	defer close(conn.readGate) // release the handler so nothing leaks
	// idempotent cleanup so an aborted run still shuts the reactor down and
	// leaves no goroutines for goleak; on the happy path stopOnce no-ops
	defer func() { _ = reactor.ShutdownWithTimeout(50 * time.Millisecond) }()
	reactor.Register(conn)
	// parked is the exact sync point: it fires from inside the connection's
	// own Read, so it proves a worker dequeued the conn and its handler is
	// now parked where force-close cannot reach it. TotalCount() must not
	// be asserted here — waitListening's probe connection is still being
	// reaped asynchronously at this point, so the count transiently reads 2.
	select {
	case <-conn.parked:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never parked in Read")
	}

	start := time.Now()
	err = reactor.ShutdownWithTimeout(200 * time.Millisecond)
	if err == nil {
		t.Fatal("a handler that outlives shutdown must be reported as an error")
	}
	if !strings.Contains(err.Error(), "did not exit") {
		t.Fatalf("shutdown error = %v, want the stuck-handler message", err)
	}
	if conn.closeCalls.Load() == 0 {
		t.Fatal("force-close never attempted the stuck connection")
	}
	// the wait for handlers is bounded by handlersExitTimeout, not skipped
	if elapsed := time.Since(start); elapsed < 4*time.Second {
		t.Fatalf("shutdown returned after %s, want it to wait for handlers", elapsed)
	}
}

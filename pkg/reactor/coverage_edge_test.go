package reactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	balancerImpl "github.com/cuihairu/simplegoserver/internal/balancer"
	"github.com/cuihairu/simplegoserver/pkg/handler"
)

// ---- stubs --------------------------------------------------------------

// nopEventListener accepts every event and does nothing.
type nopEventListener struct{}

func (nopEventListener) OnStartup()         {}
func (nopEventListener) OnReload()          {}
func (nopEventListener) OnShutdown()        {}
func (nopEventListener) OnError(any)        {}
func (nopEventListener) OnConnect(net.Conn) {}

// errRecorder counts errors so tests can assert the accept loop reported
// what it survived.
type errRecorder struct {
	mu   sync.Mutex
	errs []string
}

func (r *errRecorder) OnStartup()         {}
func (r *errRecorder) OnReload()          {}
func (r *errRecorder) OnShutdown()        {}
func (r *errRecorder) OnConnect(net.Conn) {}
func (r *errRecorder) OnError(err any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprint(err))
}

func (r *errRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.errs)
}

// rejectBalancer refuses every operation; Next's error text doubles as the
// dispatch-failure marker.
type rejectBalancer struct{}

func (rejectBalancer) Next(string) (*Worker, error) { return nil, errors.New("route refused") }
func (rejectBalancer) Register(*Worker) error       { return errors.New("register refused") }
func (rejectBalancer) Unregister(*Worker) error     { return nil }
func (rejectBalancer) Size() int                    { return 0 }
func (rejectBalancer) Iterate(func(*Worker) bool)   {}

// scriptedListener serves pre-programmed Accept failures first, then hands
// out queued connections, and reports net.ErrClosed once closed.
type scriptedListener struct {
	mu    sync.Mutex
	errs  []error
	conns chan net.Conn
	dead  chan struct{}
	once  sync.Once
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if len(l.errs) > 0 {
		err := l.errs[0]
		l.errs = l.errs[1:]
		l.mu.Unlock()
		return nil, err
	}
	l.mu.Unlock()
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.dead:
		return nil, net.ErrClosed
	}
}

func (l *scriptedListener) Close() error {
	l.once.Do(func() { close(l.dead) })
	return nil
}

func (l *scriptedListener) Addr() net.Addr { return &net.TCPAddr{} }

// panicOnActive blows up inside the pipeline's active event so the
// handler goroutine's recover path fires.
type panicOnActive struct{}

func (panicOnActive) HandleActive(handler.ActiveContext) { panic("boom on active") }

// exceptRecorder catches exceptions flowing towards the head of the
// pipeline.
type exceptRecorder struct {
	ch chan any
}

func (e *exceptRecorder) HandleException(ctx handler.ExceptionContext, ex handler.Exception) {
	e.ch <- ex
}

// noopInbound swallows reads, so the handler loop spins without ever
// blocking on the connection.
type noopInbound struct{}

func (noopInbound) HandleRead(handler.InboundContext, handler.Message) {}

// ---- worker group -------------------------------------------------------

func TestWorkerSetCountIsNoOp(t *testing.T) {
	w := NewWorker(context.Background(), "w", nopEventListener{}, nil, false, 0, NewConnectionRegistry(), &WorkerGroup{eventListener: nopEventListener{}})
	w.count.Add(3)
	w.SetCount(99) // balancers must not overwrite the live count
	if got := w.Count(); got != 3 {
		t.Fatalf("Count() = %d after SetCount(99), want the live 3", got)
	}
}

func TestNewWorkerGroupPanicsWithoutEventListener(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewWorkerGroup without an event listener must panic")
		}
	}()
	_, _ = NewWorkerGroup(NewServerOptions(), context.Background(), nil, nil, nil)
}

// TestNewWorkerGroupDefaultsServeConnections builds a group with both the
// initializer and the balancer left nil, so the defaults are exercised on a
// real (pipe) connection.
func TestNewWorkerGroupDefaultsServeConnections(t *testing.T) {
	opts := NewServerOptions()
	opts.NumWorkers = 1
	ctx, cancel := context.WithCancel(context.Background())
	group, err := NewWorkerGroup(opts, ctx, nopEventListener{}, nil, nil)
	if err != nil {
		t.Fatalf("NewWorkerGroup(): %v", err)
	}
	group.Start()

	c1, c2 := net.Pipe()
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()
	if err := group.Dispatch(c1); err != nil {
		t.Fatalf("Dispatch(): %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for group.TotalCount() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("connection was never taken over by the default pipeline")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	_ = c2.Close() // unblock the codec's read so the handler can finish
	if !group.AwaitDone(2 * time.Second) {
		t.Fatal("handler did not exit after cancellation")
	}
}

func TestNewWorkerGroupRejectsFailingBalancer(t *testing.T) {
	opts := NewServerOptions()
	opts.NumWorkers = 1
	_, err := NewWorkerGroup(opts, context.Background(), nopEventListener{}, nil, rejectBalancer{})
	if err == nil || !strings.Contains(err.Error(), "register refused") {
		t.Fatalf("NewWorkerGroup() = %v, want a registration failure", err)
	}
}

func TestWorkerRunHonorsLockThread(t *testing.T) {
	group := &WorkerGroup{eventListener: nopEventListener{}}
	w := NewWorker(context.Background(), "locked", nopEventListener{}, nil, true, 0, NewConnectionRegistry(), group)
	go w.Run()
	time.Sleep(20 * time.Millisecond)
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop(): %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the loop exit before goleak
}

// lazyCancelCtx reports "not cancelled" on the first Done() call and
// "cancelled" from the second on, so AddConn's queue send wins its select
// deterministically while the re-check still sees the cancellation.
type lazyCancelCtx struct {
	context.Context
	calls atomic.Int32
}

var (
	openChan   = make(chan struct{})
	closedChan = func() chan struct{} {
		c := make(chan struct{})
		close(c)
		return c
	}()
)

func (c *lazyCancelCtx) Done() <-chan struct{} {
	if c.calls.Add(1) >= 2 {
		return closedChan
	}
	return openChan
}

// TestAddConnSeesCancellationAfterQueuedSend pins the re-check after a
// successful queue send: a worker that stopped between "queued" and
// "handled" must close the connection instead of parking it forever.
func TestAddConnSeesCancellationAfterQueuedSend(t *testing.T) {
	group := &WorkerGroup{eventListener: nopEventListener{}}
	w := &Worker{
		ctx:      &lazyCancelCtx{Context: context.Background()},
		newCh:    make(chan net.Conn, 100),
		id:       "w",
		registry: NewConnectionRegistry(),
		group:    group,
	}
	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()
	if err := w.AddConn(c1); err == nil {
		t.Fatal("the re-check must catch the cancellation and reject the conn")
	}
	if !lcClosed(t, c2) {
		t.Fatal("the rejected connection must be closed, not parked")
	}
}

// TestWorkerClosePendingDropsQueuedConnections: connections still queued
// when the worker stops must be closed, never leaked.
func TestWorkerClosePendingDropsQueuedConnections(t *testing.T) {
	group := &WorkerGroup{eventListener: nopEventListener{}, registry: NewConnectionRegistry()}
	w := NewWorker(context.Background(), "w", nopEventListener{}, nil, false, 0, group.registry, group)
	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()
	if err := w.AddConn(c1); err != nil {
		t.Fatalf("AddConn(): %v", err)
	}
	w.closePending()
	if !lcClosed(t, c2) {
		t.Fatal("a queued connection must be closed on worker stop")
	}
}

// lcClosed reports whether the peer end of a pipe sees its counterpart
// closed.
func lcClosed(t *testing.T, peer net.Conn) bool {
	t.Helper()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	_, err := peer.Read(make([]byte, 1))
	return err != nil
}

// ---- connection handler -------------------------------------------------

func TestHandleConnectionInitializerErrorPanics(t *testing.T) {
	group := &WorkerGroup{eventListener: &errRecorder{}, registry: NewConnectionRegistry()}
	w := NewWorker(context.Background(), "w", group.eventListener,
		func(handler.Pipeline) error { return errors.New("broken initializer") },
		false, 0, group.registry, group)
	group.handlerStarted()
	w.count.Add(1)

	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()
	lc := &lifecycleConn{Conn: c1}

	panicked := make(chan bool, 1)
	go func() {
		defer func() { panicked <- recover() != nil }()
		w.handleConnection(lc)
	}()
	if !(<-panicked) {
		t.Fatal("a failing initializer must crash the handler goroutine")
	}
	// the registration the worker loop already performed is undone
	if !lc.closed.Load() {
		t.Fatal("the connection must be closed on initializer failure")
	}
	if got := group.registry.Len(); got != 0 {
		t.Fatalf("registry holds %d connections, want 0", got)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("count = %d, want 0 after the undo", got)
	}
	if !group.AwaitDone(time.Second) {
		t.Fatal("the handler slot was never released")
	}
}

func TestHandleConnectionRecoversHandlerPanic(t *testing.T) {
	exceptions := make(chan any, 1)
	group := &WorkerGroup{eventListener: nopEventListener{}, registry: NewConnectionRegistry()}
	w := NewWorker(context.Background(), "w", group.eventListener,
		func(p handler.Pipeline) error {
			_ = p.AddLast(&exceptRecorder{ch: exceptions}, panicOnActive{})
			return nil
		},
		false, 0, group.registry, group)
	group.handlerStarted()
	w.count.Add(1)

	c1, c2 := net.Pipe()
	defer func() {
		_ = c1.Close()
		_ = c2.Close()
	}()
	lc := &lifecycleConn{Conn: c1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.handleConnection(lc)
	}()
	select {
	case <-exceptions:
		// the panic surfaced as a pipeline exception
	case <-time.After(2 * time.Second):
		t.Fatal("the handler panic never reached the exception handler")
	}
	<-done
	if !lc.closed.Load() {
		t.Fatal("the connection must be closed after the handler returns")
	}
	if !group.AwaitDone(time.Second) {
		t.Fatal("the handler slot was never released")
	}
}

// TestHandleConnectionExitsOnCancel drives the mid-loop cancellation check
// with a pipeline that never blocks: the loop spins until the ctx fires.
func TestHandleConnectionExitsOnCancel(t *testing.T) {
	group := &WorkerGroup{eventListener: nopEventListener{}, registry: NewConnectionRegistry()}
	ctx, cancel := context.WithCancel(context.Background())
	w := NewWorker(ctx, "w", group.eventListener,
		func(p handler.Pipeline) error {
			_ = p.AddLast(noopInbound{})
			return nil
		},
		false, 0, group.registry, group)
	group.handlerStarted()
	w.count.Add(1)

	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()
	lc := &lifecycleConn{Conn: c1}

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.handleConnection(lc)
	}()
	time.Sleep(50 * time.Millisecond) // the loop is spinning on the ctx check
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the handler ignored the cancelled context")
	}
	if !lc.closed.Load() {
		t.Fatal("cancellation must close the connection")
	}
}

// ---- reactor ------------------------------------------------------------

func TestNewReactorRejectsWhenWorkerRegistrationFails(t *testing.T) {
	opts := NewServerOptions()
	opts.NumWorkers = 1
	opts.Listener = "tcp://127.0.0.1:0"
	_, err := NewReactor(opts, nil, nopEventListener{}, nil, rejectBalancer{})
	if err == nil || !strings.Contains(err.Error(), "register refused") {
		t.Fatalf("NewReactor() = %v, want a registration failure", err)
	}
	// the listener bound before the failure must have been released; the
	// worker loops never started, so there is nothing to leak
}

// TestReactorReportsDispatchFailure wires a reactor whose balancer never
// routes: an accepted connection must be closed and the failure reported.
func TestReactorReportsDispatchFailure(t *testing.T) {
	rec := &errRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener := &scriptedListener{conns: make(chan net.Conn, 1), dead: make(chan struct{})}
	r := &Reactor{
		listener:      listener,
		workers:       &WorkerGroup{balancer: rejectBalancer{}},
		ctx:           ctx,
		cancelFunc:    cancel,
		eventListener: rec,
		logger:        log.New(io.Discard, "", 0),
	}
	go r.Run()

	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()
	listener.conns <- c1
	deadline := time.Now().Add(2 * time.Second)
	for rec.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("dispatch failure was never reported")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Dispatch closed the connection on routing failure; the peer sees it
	if err := waitClosed(c2, 2*time.Second); err != nil {
		t.Fatalf("the routed-off connection was not closed: %v", err)
	}
	_ = listener.Close() // the accept loop exits on net.ErrClosed
	time.Sleep(50 * time.Millisecond)
}

// TestReactorAcceptBackoffLadder survives a run of transient accept
// failures — long enough to hit the 1s backoff cap — then serves one
// connection and shuts down cleanly.
func TestReactorAcceptBackoffLadder(t *testing.T) {
	rec := &errRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make([]error, 10)
	for i := range errs {
		errs[i] = fmt.Errorf("transient accept failure %d", i)
	}
	listener := &scriptedListener{errs: errs, conns: make(chan net.Conn, 1), dead: make(chan struct{})}
	group, err := NewWorkerGroup(func() *ServerOptions {
		o := NewServerOptions()
		o.NumWorkers = 1
		return o
	}(), ctx, nopEventListener{}, nil, nil)
	if err != nil {
		t.Fatalf("NewWorkerGroup(): %v", err)
	}
	r := &Reactor{
		listener:      listener,
		workers:       group,
		ctx:           ctx,
		cancelFunc:    cancel,
		eventListener: rec,
		logger:        log.New(io.Discard, "", 0),
	}
	go r.Run()

	c1, c2 := net.Pipe()
	defer func() { _ = c2.Close() }()
	listener.conns <- c1 // served after the backoff ladder drains

	deadline := time.Now().Add(10 * time.Second)
	for rec.count() < len(errs) {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d accept failures reported", rec.count(), len(errs))
		}
		time.Sleep(10 * time.Millisecond)
	}
	// the ladder must have reached the 1s cap: 5ms doubling needs nine
	// doublings to exceed it, so ten failures guarantee the cap branch
	if err := r.ShutdownWithTimeout(2 * time.Second); err != nil {
		t.Fatalf("ShutdownWithTimeout(): %v", err)
	}
}

// TestReactorAcceptErrorWithCancelledContext parks the accept loop in
// Accept, cancels the context, then closes the listener: the error path
// must notice the cancellation and return quietly.
func TestReactorAcceptErrorWithCancelledContext(t *testing.T) {
	reactor, err := NewReactor(newTestOptions(t), nil, nil, echoInitializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	go reactor.Run()
	time.Sleep(50 * time.Millisecond) // the accept loop is parked in Accept

	reactor.cancelFunc()
	_ = reactor.listener.Close() // unblocks Accept with an error
	time.Sleep(100 * time.Millisecond)
	// worker loops are children of the cancelled context; nothing to stop
}

func TestReactorNextReturnsNilWhenBalancerEmpty(t *testing.T) {
	r := &Reactor{workers: &WorkerGroup{balancer: balancerImpl.NewRoundRobinBalancer[*Worker]()}}
	if got := r.Next(); got != nil {
		t.Fatalf("Next() on an empty balancer = %v, want nil", got)
	}
}

// waitClosed blocks until reading the pipe fails.
func waitClosed(peer net.Conn, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_ = peer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := peer.Read(make([]byte, 1)); err != nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("connection stayed open")
		}
	}
}

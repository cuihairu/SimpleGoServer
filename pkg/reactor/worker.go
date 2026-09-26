package reactor

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	balancerImpl "github.com/cuihairu/simplegoserver/internal/balancer"
	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg"
	"github.com/cuihairu/simplegoserver/pkg/event"
	"github.com/cuihairu/simplegoserver/pkg/handler"
)

type Worker struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	newCh               chan net.Conn
	count               atomic.Int32
	id                  string
	lockThread          bool
	idleTimeout         time.Duration
	pipelineInitializer handler.PipelineInitializer
	eventListener       event.Listener
	registry            *ConnectionRegistry
	group               *WorkerGroup
}

func (w *Worker) Id() string {
	return w.id
}

// SetCount exists to satisfy pkg.CountBackend; the live count is maintained
// by handleConnection and must not be overwritten by balancers.
func (w *Worker) SetCount(int) {}

type WorkerGroup struct {
	balancer      pkg.Balancer[*Worker]
	eventListener event.Listener
	registry      *ConnectionRegistry

	// active counts in-flight connection handlers. A shared sync.WaitGroup
	// would be wrong here: handlers start dynamically as connections
	// arrive, and Add racing a Wait is undefined behavior for WaitGroup.
	// An atomic counter read by polling (see AwaitDone) has no such rule.
	active atomic.Int32
}

func NewWorkerGroup(opts pkg.Options, ctx context.Context, eventListener event.Listener, pipelineInitializer handler.PipelineInitializer, balancer pkg.Balancer[*Worker]) (*WorkerGroup, error) {
	if eventListener == nil {
		panic("eventListener must not be nil")
	}
	serverOptions := opts.(*ServerOptions)
	if serverOptions.NumWorkers <= 0 || serverOptions.NumWorkers > runtime.NumCPU() || serverOptions.Multicore {
		serverOptions.NumWorkers = runtime.NumCPU()
	}
	if pipelineInitializer == nil {
		pipelineInitializer = handlerImpl.WithDefaultPipeline
	}
	if balancer == nil {
		// adaptive default: route new connections to the least loaded
		// worker instead of blind rotation
		balancer = balancerImpl.NewAdaptiveBalancer[*Worker]()
	}
	registry := NewConnectionRegistry()
	group := &WorkerGroup{eventListener: eventListener, registry: registry}

	for i := 0; i < serverOptions.NumWorkers; i++ {
		worker := NewWorker(ctx, fmt.Sprintf("worker:%d", i), eventListener, pipelineInitializer, serverOptions.GetLockThread(), serverOptions.GetIdleTimeout(), registry, group)
		err := balancer.Register(worker)
		if err != nil {
			eventListener.OnError(err)
			return nil, err
		}
	}
	group.balancer = balancer
	return group, nil
}

func (g *WorkerGroup) Start() {
	g.balancer.Iterate(func(bc *Worker) bool {
		go bc.Run()
		return true
	})
}

func (g *WorkerGroup) Dispatch(conn net.Conn) error {
	worker, err := g.balancer.Next(conn.RemoteAddr().String())
	if err != nil {
		// the connection was never handed to a worker, so nobody owns its
		// lifecycle: close it here or the fd leaks
		_ = conn.Close()
		return err
	}
	return worker.AddConn(conn)
}

// TotalCount reports the number of connections currently being handled.
func (g *WorkerGroup) TotalCount() int {
	total := 0
	g.balancer.Iterate(func(w *Worker) bool {
		total += w.Count()
		return true
	})
	return total
}

func (g *WorkerGroup) Registry() *ConnectionRegistry {
	return g.registry
}

func (g *WorkerGroup) Stop() {
	g.balancer.Iterate(func(w *Worker) bool {
		_ = w.Stop()
		return true
	})
}

// handlerStarted registers a handler that is about to run. It is called
// before the handler goroutine starts so AwaitDone can never observe an
// unregistered in-flight handler.
func (g *WorkerGroup) handlerStarted() { g.active.Add(1) }

// handlerFinished unregisters a handler once it has returned.
func (g *WorkerGroup) handlerFinished() { g.active.Add(-1) }

// AwaitDone waits for all in-flight connection handlers to finish, or until
// the timeout elapses, and reports whether they finished. Call it only after
// the worker loops have stopped, so no new handler can start. It polls the
// atomic count: handlers finishing wake nobody, but this runs once per
// shutdown, so a 10ms tick costs nothing.
func (g *WorkerGroup) AwaitDone(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for g.active.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return g.active.Load() == 0
}

// NewWorker builds a worker. Construction cannot fail — every fallible
// step happens later on the loop — so there is deliberately no error to
// propagate.
func NewWorker(parent context.Context, id string, eventListener event.Listener, pipelineInitializer handler.PipelineInitializer, lockThread bool, idleTimeout time.Duration, registry *ConnectionRegistry, group *WorkerGroup) *Worker {
	ctx, cancel := context.WithCancel(parent)
	return &Worker{
		ctx:                 ctx,
		cancel:              cancel,
		newCh:               make(chan net.Conn, 100),
		id:                  id,
		lockThread:          lockThread,
		idleTimeout:         idleTimeout,
		pipelineInitializer: pipelineInitializer,
		eventListener:       eventListener,
		registry:            registry,
		group:               group,
	}
}

// AddConn queues a connection for the worker loop. A full queue blocks the
// caller on purpose: that is backpressure flowing back to the accept loop.
// Once the worker has stopped, though, nothing will ever drain newCh, so
// queueing blindly would leak the connection's fd (or block forever once
// the buffer fills) — the connection is closed and an error returned then.
func (w *Worker) AddConn(conn net.Conn) error {
	select {
	case w.newCh <- conn:
		// Re-check after the send: ctx may have been cancelled while the
		// send raced the loop's exit. If ctx is not done here, the loop is
		// still alive (it only exits via ctx.Done), so its closePending
		// will reap this entry on shutdown either way.
		select {
		case <-w.ctx.Done():
			_ = conn.Close()
			return fmt.Errorf("reactor: worker %s already stopped", w.id)
		default:
			return nil
		}
	case <-w.ctx.Done():
		_ = conn.Close()
		return fmt.Errorf("reactor: worker %s already stopped", w.id)
	}
}

// Stop stops the worker loop. Connections already handed to handlers are
// closed by themselves once they observe the cancelled context.
func (w *Worker) Stop() error {
	w.cancel()
	return nil
}

func (w *Worker) Run() {
	if w.lockThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	for {
		select {
		case c := <-w.newCh:
			// registration happens here, on the worker loop, not inside the
			// handler goroutine: by the time the connection is visible to a
			// concurrent AwaitDone (active) it is equally visible to the
			// drain count and to Registry().CloseAll, so shutdown never
			// observes three different stories about one connection
			lc := &lifecycleConn{Conn: c}
			if !w.registry.Add(lc) {
				// shutdown's CloseAll already swept the registry, so it
				// will never see this connection: nobody else would ever
				// close it, and a handler spawned here would park in Read
				// on it forever (AwaitDone then times out and the fd
				// leaks). Closing it is this worker's job — along with
				// anything still queued behind it. The select above can
				// pick this arm even with ctx already cancelled, so the
				// rejection is the only thing standing between a conn
				// accepted mid-shutdown and a leaked handler.
				_ = c.Close()
				w.closePending()
				return
			}
			w.group.handlerStarted()
			w.count.Add(1)
			go w.handleConnection(lc)
		case <-w.ctx.Done():
			w.closePending()
			return
		}
	}
}

// closePending closes connections still queued in newCh so they are not
// leaked when the worker stops before dispatching them.
func (w *Worker) closePending() {
	for {
		select {
		case c := <-w.newCh:
			_ = c.Close()
		default:
			return
		}
	}
}

func (w *Worker) Count() int {
	return int(w.count.Load())
}

// Load is the live load figure the adaptive balancer consumes: the number
// of connections this worker is currently handling.
func (w *Worker) Load() float64 {
	return float64(w.count.Load())
}

// lifecycleConn watches its own closure so the read loop can tell "handler
// closed the connection" apart from "keep reading".
type lifecycleConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *lifecycleConn) Close() error {
	wasClosed := c.closed.Swap(true)
	if wasClosed {
		return nil // idempotent: repeated Close stays a success
	}
	return c.Conn.Close()
}

func (w *Worker) handleConnection(lc *lifecycleConn) {
	pipeline := handlerImpl.NewPipeline(lc)
	err := w.pipelineInitializer(pipeline)
	if err != nil {
		// a broken initializer is a programming error; undo the
		// registration the worker loop already performed, then crash
		w.eventListener.OnError(err)
		_ = lc.Close()
		w.registry.Remove(lc)
		w.count.Add(-1)
		w.group.handlerFinished()
		panic(err)
	}
	defer func() {
		_ = lc.Close()
		w.registry.Remove(lc)
		pipeline.FireInactive(nil)
		w.count.Add(-1)
		w.group.handlerFinished()
	}()
	defer func() {
		if e := recover(); e != nil {
			pipeline.FireException(e)
		}
	}()
	pipeline.FireActive()
	for {
		select {
		case <-w.ctx.Done():
			_ = lc.Close()
			return
		default:
		}
		if w.idleTimeout > 0 {
			// dead-link reaping: a silent connection's blocked read returns
			// ErrDeadlineExceeded once the deadline passes and the codec
			// closes it; every received frame renews the allowance here
			_ = lc.SetReadDeadline(time.Now().Add(w.idleTimeout))
		}
		pipeline.FireRead(lc)
		if lc.closed.Load() {
			// a handler closed the connection (peer EOF, protocol error,
			// server shutdown); stop reading
			return
		}
	}
}

var _ pkg.CountBackend = (*Worker)(nil)
var _ event.Loop = (*Worker)(nil)

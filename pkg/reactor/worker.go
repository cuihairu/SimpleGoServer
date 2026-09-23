package reactor

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
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
	pipelineInitializer handler.PipelineInitializer
	eventListener       event.Listener
	registry            *ConnectionRegistry
	wg                  *sync.WaitGroup
}

func (w *Worker) Id() string {
	return w.id
}

// SetCount exists to satisfy pkg.CountBackend; the live count is maintained
// by handleConnection and must not be overwritten by balancers.
func (w *Worker) SetCount(int) {}

type WorkerGroup struct {
	balancer      pkg.Balancer[*Worker]
	opts          pkg.Options
	eventListener event.Listener
	registry      *ConnectionRegistry
	wg            *sync.WaitGroup
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
	wg := &sync.WaitGroup{}

	for i := 0; i < serverOptions.NumWorkers; i++ {
		worker, err := NewWorker(ctx, fmt.Sprintf("worker:%d", i), eventListener, pipelineInitializer, serverOptions.GetLockThread(), registry, wg)
		if err != nil {
			eventListener.OnError(err)
			return nil, err
		}
		err = balancer.Register(worker)
		if err != nil {
			eventListener.OnError(err)
			return nil, err
		}
	}
	return &WorkerGroup{balancer: balancer, eventListener: eventListener, registry: registry, wg: wg}, nil
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
		return err
	}
	worker.AddConn(conn)
	return nil
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

// AwaitDone waits for all in-flight connection goroutines to finish, or
// until the timeout elapses, and reports whether they finished. Call it only
// after every worker has stopped, so no new waiters can join the WaitGroup.
func (g *WorkerGroup) AwaitDone(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false // the Wait goroutine ends once the last handler returns
	}
}

func NewWorker(parent context.Context, id string, eventListener event.Listener, pipelineInitializer handler.PipelineInitializer, lockThread bool, registry *ConnectionRegistry, wg *sync.WaitGroup) (*Worker, error) {
	ctx, cancel := context.WithCancel(parent)
	return &Worker{
		ctx:                 ctx,
		cancel:              cancel,
		newCh:               make(chan net.Conn, 100),
		id:                  id,
		lockThread:          lockThread,
		pipelineInitializer: pipelineInitializer,
		eventListener:       eventListener,
		registry:            registry,
		wg:                  wg,
	}, nil
}

func (w *Worker) AddConn(conn net.Conn) {
	w.newCh <- conn
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
			// counting happens before the goroutine starts so a concurrent
			// AwaitDone can never observe an unregistered in-flight handler
			w.wg.Add(1)
			go w.handleConnection(c)
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

func (w *Worker) handleConnection(conn net.Conn) {
	lc := &lifecycleConn{Conn: conn}
	w.registry.Add(lc)
	defer w.wg.Done()

	pipeline := handlerImpl.NewPipeline(lc)
	err := w.pipelineInitializer(pipeline)
	if err != nil {
		w.eventListener.OnError(err)
		panic(err)
	}
	w.count.Add(1)
	defer func() {
		_ = lc.Close()
		w.registry.Remove(lc)
		pipeline.FireInactive(nil)
		w.count.Add(-1)
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

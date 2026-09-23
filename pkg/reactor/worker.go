package reactor

import (
	"context"
	"fmt"
	balancerImpl "github.com/cuihairu/simplegoserver/internal/balancer"
	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg"
	"github.com/cuihairu/simplegoserver/pkg/event"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"net"
	"runtime"
	"sync/atomic"
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
}

func (w *Worker) Id() string {
	return w.id
}

func (w *Worker) SetCount(count int) {

}

type WorkerGroup struct {
	balancer      pkg.Balancer[*Worker]
	opts          pkg.Options
	eventListener event.Listener
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
		balancer = balancerImpl.NewLeastConnectionsBalancer[*Worker](false)
	}

	for i := 0; i < serverOptions.NumWorkers; i++ {
		worker, err := NewWorker(ctx, fmt.Sprintf("worker:%d", i), eventListener, pipelineInitializer, serverOptions.GetLockThread())
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
	return &WorkerGroup{balancer: balancer, eventListener: eventListener}, nil
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

func NewWorker(parent context.Context, id string, eventListener event.Listener, pipelineInitializer handler.PipelineInitializer, lockThread bool) (*Worker, error) {
	ctx, cancel := context.WithCancel(parent)
	return &Worker{
		ctx:                 ctx,
		cancel:              cancel,
		newCh:               make(chan net.Conn, 100),
		id:                  id,
		lockThread:          lockThread,
		pipelineInitializer: pipelineInitializer,
		eventListener:       eventListener,
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
			go w.handleConnection(c)
		case <-w.ctx.Done():
			w.closePending()
			return
		}
	}
}

// closePending closes connections still queued in newCh so they are not leaked
// when the worker stops before dispatching them.
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

func (w *Worker) handleConnection(conn net.Conn) {
	pipeline := handlerImpl.NewPipeline(conn)
	err := w.pipelineInitializer(pipeline)
	if err != nil {
		w.eventListener.OnError(err)
		panic(err)
	}
	w.count.Add(1)
	defer func() {
		pipeline.FireInactive(conn.Close())
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
			return
		default:
			pipeline.FireRead(conn)
		}
	}
}

var _ pkg.CountBackend = (*Worker)(nil)
var _ event.Loop = (*Worker)(nil)

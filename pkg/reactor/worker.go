package reactor

import (
	"context"
	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg"
	"github.com/cuihairu/simplegoserver/pkg/event"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
)

type Worker struct {
	mutex               sync.Mutex
	ctx                 context.Context
	newCh               chan net.Conn
	count               atomic.Int32
	id                  int
	opts                Options
	pipelineInitializer handler.PipeInitializer
	eventListener       event.EventListener
}

type WorkerGroup struct {
	balancer      pkg.Balancer
	opts          Options
	eventListener event.EventListener
}

func NewWorkerGroup(opts Options, ctx context.Context, eventListener event.EventListener, pipelineInitializer handler.PipeInitializer, balancer pkg.Balancer) (*WorkerGroup, error) {
	if eventListener == nil {
		panic("eventListener must not be nil")
	}
	if opts.NumWorkers <= 0 || opts.NumWorkers > runtime.NumCPU() || opts.Multicore {
		opts.NumWorkers = runtime.NumCPU()
	}
	if pipelineInitializer == nil {
		pipelineInitializer = handlerImpl.WithDefaultPipeline
	}
	if balancer == nil {
		balancer = pkg.NewLeastBalancer()
	}

	for i := 0; i < opts.NumWorkers; i++ {
		worker, err := NewWorker(ctx, i, eventListener, pipelineInitializer)
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
	g.balancer.Iterate(func(wk *Worker) bool {
		go wk.Run()
		return true
	})
}

func (g *WorkerGroup) Dispatch(conn net.Conn) error {
	worker, err := g.balancer.Dispatch(conn)
	if err != nil {
		return err
	}
	worker.AddConn(conn)
	return nil
}

func NewWorker(ctx context.Context, id int, eventListener event.EventListener, pipelineInitializer handler.PipeInitializer) (*Worker, error) {
	return &Worker{
		ctx:                 ctx,
		newCh:               make(chan net.Conn, 100),
		id:                  id,
		pipelineInitializer: pipelineInitializer,
		eventListener:       eventListener,
	}, nil
}

func (w *Worker) AddConn(conn net.Conn) {
	w.newCh <- conn
}
func (w *Worker) Run() {
	if w.opts.LockThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	for {
		select {
		case c := <-w.newCh:
			go w.handleConnection(c)
		case <-w.ctx.Done():
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

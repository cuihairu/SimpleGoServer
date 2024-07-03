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
	"sync"
	"sync/atomic"
)

type Worker struct {
	mutex               sync.Mutex
	ctx                 context.Context
	newCh               chan net.Conn
	count               atomic.Int32
	id                  string
	opts                *ServerOptions
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
		worker, err := NewWorker(ctx, fmt.Sprintf("worker:%d", i), eventListener, pipelineInitializer)
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

func NewWorker(ctx context.Context, id string, eventListener event.Listener, pipelineInitializer handler.PipelineInitializer) (*Worker, error) {
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

var _ pkg.CountBackend = (*Worker)(nil)

package reactor

import (
	"context"
	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"net"
	"net/url"
	"os"
)

type SlaveRector struct {
}

type Reactor struct {
	opts                Options
	listener            net.Listener
	workers             *WorkerGroup
	cancelFunc          context.CancelFunc
	ctx                 context.Context
	graceful            *utils.Graceful
	eventListener       handler.EventListener
	pipelineInitializer handler.PipeInitializer
}

func NewReactor(opts Options, eventListener handler.EventListener, pipelineInitializer handler.PipeInitializer, balancer Balancer) (*Reactor, error) {
	if eventListener == nil {
		eventListener = handlerImpl.NewErrorHandler()
	}
	parse, err := url.Parse(opts.Listener)
	if err != nil {
		eventListener.OnError(err)
		return nil, err
	}
	listener, err := net.Listen(parse.Scheme, parse.Host)
	if err != nil {
		eventListener.OnError(err)
		return nil, err
	}
	ctx, ctxCancel := context.WithCancel(context.Background())

	group, err := NewWorkerGroup(opts, ctx, eventListener, pipelineInitializer, balancer)
	if err != nil {
		eventListener.OnError(err)
		return nil, err
	}
	reactor := &Reactor{
		opts:          opts,
		listener:      listener,
		workers:       group,
		ctx:           ctx,
		cancelFunc:    ctxCancel,
		eventListener: eventListener,
	}
	reactor.graceful = utils.NewGraceful(func(signal os.Signal) {
		reactor.Stop()
	}, func() {
		reactor.Reload()
	})

	return reactor, err
}

func (r *Reactor) Reload() {
	r.eventListener.OnReload()
}

func (r *Reactor) Run() {
	r.eventListener.OnStartup()
	r.workers.Start()
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			// pass
		}
		conn, err := r.listener.Accept()
		if err != nil {
			r.eventListener.OnError(err)
			continue
		}
		r.eventListener.OnConnect(conn)
		err = r.workers.Dispatch(conn)
		if err != nil {
			r.eventListener.OnError(err)
		}
	}
}

func (r *Reactor) Stop() {
	r.eventListener.OnShutdown()
	r.cancelFunc()
	r.listener.Close()
}

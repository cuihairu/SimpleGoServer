package reactor

import (
	"context"
	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg"
	"github.com/cuihairu/simplegoserver/pkg/event"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"log"
	"net"
	"net/url"
	"os"
)

type SlaveRector struct {
}

type Reactor struct {
	opts                pkg.Options
	listener            net.Listener
	workers             *WorkerGroup
	cancelFunc          context.CancelFunc
	ctx                 context.Context
	graceful            *utils.Graceful
	eventListener       event.Listener
	pipelineInitializer handler.PipelineInitializer
	logger              *log.Logger
}

var _ event.Group = (*Reactor)(nil)

func NewReactor(opts pkg.Options, logger *log.Logger, eventListener event.Listener, pipelineInitializer handler.PipelineInitializer, balancer pkg.Balancer[*Worker]) (*Reactor, error) {
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	}
	if eventListener == nil {
		eventListener = handlerImpl.NewErrorHandler()
	}
	serverOptions := opts.(*ServerOptions)
	parse, err := url.Parse(serverOptions.Listener)
	if err != nil {
		eventListener.OnError(err)
		return nil, err
	}
	listener, err := net.Listen(parse.Scheme, parse.Host)
	if err != nil {
		eventListener.OnError(err)
		return nil, err
	}
	logger.Printf("listening on %s:%s", parse.Scheme, parse.Host)
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
		reactor.ShutdownGracefully()
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

func (r *Reactor) ShutdownGracefully() {
	r.Stop()
}

func (r *Reactor) Stop() error {
	r.eventListener.OnShutdown()
	r.cancelFunc()
	return r.listener.Close()
}

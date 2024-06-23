package reactor

import (
	"context"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"net"
	"net/url"
	"os"
)

type SlaveRector struct {
}

type Reactor struct {
	opts       Options
	listener   net.Listener
	workers    *WorkerGroup
	cancelFunc context.CancelFunc
	ctx        context.Context
	graceful   *utils.Graceful
}

func NewReactor(opts Options) (*Reactor, error) {
	if opts.eventListener == nil {
		opts.eventListener = handler.NewErrorHandler()
	}
	if opts.initializer == nil {
		opts.initializer = handler.WithDefaultPipeline
	}
	parse, err := url.Parse(opts.Listener)
	if err != nil {
		opts.eventListener.OnError(err)
		return nil, err
	}
	listener, err := net.Listen(parse.Scheme, parse.Host)
	if err != nil {
		opts.eventListener.OnError(err)
		return nil, err
	}
	ctx, ctxCancel := context.WithCancel(context.Background())
	balancer := NewLeastBalancer()
	group, err := NewWorkerGroup(opts, ctx, balancer)
	if err != nil {
		opts.eventListener.OnError(err)
		return nil, err
	}
	reactor := &Reactor{
		opts:       opts,
		listener:   listener,
		workers:    group,
		ctx:        ctx,
		cancelFunc: ctxCancel,
	}
	reactor.graceful = utils.NewGraceful(func(signal os.Signal) {
		reactor.Stop()
	}, func() {
		reactor.Reload()
	})

	return reactor, err
}

func (r *Reactor) Reload() {
	r.opts.eventListener.OnReload()
}

func (r *Reactor) Run() {
	r.opts.eventListener.OnStartup()
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
			r.opts.eventListener.OnError(err)
			continue
		}
		r.opts.eventListener.OnConnect(conn)
		err = r.workers.Dispatch(conn)
		if err != nil {
			r.opts.eventListener.OnError(err)
		}
	}
}

func (r *Reactor) Stop() {
	r.opts.eventListener.OnShutdown()
	r.cancelFunc()
	r.listener.Close()
}

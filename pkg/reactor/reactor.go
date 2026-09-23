package reactor

import (
	"context"
	"fmt"
	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg"
	"github.com/cuihairu/simplegoserver/pkg/event"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/utils"
	"log"
	"net"
	"net/url"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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
	stopOnce            sync.Once
	// stopping is set just before the listener closes so the accept loop
	// can tell a shutdown close from a real accept failure and exit
	// quietly instead of spinning on error logs for the whole drain
	stopping atomic.Bool
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
		ctxCancel()
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
		logger:        logger,
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
			if r.stopping.Load() {
				// listener closed by shutdown; expected, not an error
				return
			}
			select {
			case <-r.ctx.Done():
				// listener closed while shutting down; expected error
				return
			default:
			}
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

// Addr reports the address the reactor is listening on. For a listener
// bound to port 0 this is the only way to learn the actual port.
func (r *Reactor) Addr() net.Addr {
	return r.listener.Addr()
}

// ShutdownGracefully stops accepting new connections, waits for in-flight
// connections to finish on their own and force-closes whatever is still
// open after the drain timeout.
func (r *Reactor) ShutdownGracefully() {
	r.ShutdownWithTimeout(defaultDrainTimeout)
}

// defaultDrainTimeout bounds how long a graceful shutdown waits for
// existing connections to close themselves.
const defaultDrainTimeout = 10 * time.Second

// ShutdownWithTimeout runs the graceful shutdown in stages:
//
//  1. close the listener — no new connections are accepted;
//  2. drain — wait up to timeout for existing connections to close
//     themselves (peer hangup, finished sessions);
//  3. force close — connections still open are closed, unblocking every
//     goroutine parked in conn.Read;
//  4. stop the worker loops and wait briefly for handlers to return;
//  5. fire the shutdown event.
func (r *Reactor) ShutdownWithTimeout(timeout time.Duration) error {
	r.stopOnce.Do(func() {
		// stage 1: stop accepting
		r.stopping.Store(true)
		_ = r.listener.Close()
		// stage 2: give existing connections a chance to drain
		deadline := time.Now().Add(timeout)
		for r.workers.TotalCount() > 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		// stage 3: force close what is left; this unblocks reads
		if closed := r.workers.Registry().CloseAll(); closed > 0 {
			r.eventListener.OnError(fmt.Sprintf("shutdown: force-closed %d connections after drain timeout", closed))
		}
		// stage 4: stop worker loops and wait for handlers to return
		r.cancelFunc()
		r.workers.Stop()
		if !r.workers.AwaitDone(handlersExitTimeout) {
			r.eventListener.OnError("shutdown: handlers did not exit in time")
			// a handler that outlives shutdown is a leak; dump every
			// goroutine stack so the stuck handler is identifiable
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			r.logger.Printf("goroutine dump on stuck shutdown:\n%s", buf[:n])
		}
		r.eventListener.OnShutdown()
	})
	return nil
}

const handlersExitTimeout = 5 * time.Second

// Register hands an accepted connection over to the worker group. It lets the
// reactor be used programmatically instead of through Run.
func (r *Reactor) Register(conn net.Conn) {
	if err := r.workers.Dispatch(conn); err != nil {
		r.eventListener.OnError(err)
	}
}

// Unregister closes the connection; handlers observe it as an inactive event.
func (r *Reactor) Unregister(conn net.Conn) {
	_ = conn.Close()
}

// Next returns a worker as an event loop, chosen by the load balancer.
func (r *Reactor) Next() event.Loop {
	worker, err := r.workers.balancer.Next("")
	if err != nil {
		return nil
	}
	return worker
}

func (r *Reactor) Stop() error {
	// cancel wakes the accept loop, closing the listener unblocks a pending
	// Accept so Run can observe the cancelled context and return
	r.stopping.Store(true)
	r.cancelFunc()
	return r.listener.Close()
}

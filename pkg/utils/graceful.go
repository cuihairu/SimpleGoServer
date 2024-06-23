package utils

import (
	"os"
	"os/signal"
	"syscall"
)

type ShutdownCallback func(os.Signal)
type ReloadCallback func()

type Graceful struct {
	shutdownCh       chan os.Signal
	reloadCh         chan os.Signal
	doneCh           chan bool
	shutdownSignals  []os.Signal
	reloadSignals    []os.Signal
	shutdownCallback ShutdownCallback
	reloadCallback   ReloadCallback
}

func NewGraceful(shutdownCallback ShutdownCallback, reloadCallback ReloadCallback) *Graceful {
	return &Graceful{
		shutdownCh: make(chan os.Signal, 1),
		reloadCh:   make(chan os.Signal, 1),
		doneCh:     make(chan bool, 1),

		shutdownSignals:  []os.Signal{syscall.SIGINT, syscall.SIGTERM, os.Interrupt, syscall.SIGQUIT},
		reloadSignals:    []os.Signal{syscall.SIGHUP},
		shutdownCallback: shutdownCallback,
		reloadCallback:   reloadCallback,
	}
}

func (g *Graceful) listen() {
	if len(g.shutdownSignals) == 0 {
		panic("shutdown signals not set")
	}
	signal.Notify(g.shutdownCh, g.shutdownSignals...)
	if len(g.reloadSignals) > 0 {
		signal.Notify(g.reloadCh, g.reloadSignals...)
	}
}

func (g *Graceful) Wait() {
	g.listen()
	for {
		select {
		case s := <-g.shutdownCh:
			g.stopBySignal(s)
			return
		case <-g.reloadCh:
			g.reload()
		case <-g.doneCh:
			return
		}
	}
}

func (g *Graceful) Stop() {
	g.doneCh <- true
}

func (g *Graceful) stopBySignal(sig os.Signal) {
	if g.shutdownCallback != nil {
		g.shutdownCallback(sig)
	}
}

func (g *Graceful) reload() {
	if g.reloadCallback != nil {
		g.reloadCallback()
	}
}
func (g *Graceful) Reload() {
	g.reload()
}

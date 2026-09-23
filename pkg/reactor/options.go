package reactor

import (
	"runtime"
	"time"
)

type ServerOptions struct {
	Multicore  bool // 是否使用多核
	NumWorkers int
	Listener   string
	LockThread bool
	// IdleTimeout closes connections that stay silent for this long, reaping
	// peers that died without a TCP close. Any received frame — heartbeat
	// included — renews the allowance, so clients can keep a connection
	// alive with proto.Client.Ping. It must exceed the worst-case time to
	// transfer one frame, or slow but healthy transfers get cut mid-frame.
	// Zero (the default) disables idle reaping.
	IdleTimeout time.Duration
}

func NewServerOptions() *ServerOptions {
	return &ServerOptions{
		Multicore:  false,
		NumWorkers: 0,
		Listener:   "127.0.0.1:8080",
		LockThread: false,
	}
}

func (o *ServerOptions) GetNumWorkers() int {
	if o.NumWorkers <= 0 || o.NumWorkers > runtime.NumCPU() || o.Multicore {
		o.NumWorkers = runtime.NumCPU()
	}
	return o.NumWorkers
}

func (o *ServerOptions) GetListener() string {
	if o.Listener == "" {
		o.Listener = "127.0.0.1:8080"
	}
	return o.Listener
}

func (o *ServerOptions) GetLockThread() bool {
	return o.LockThread
}

func (o *ServerOptions) GetMulticore() bool {
	return o.Multicore
}

func (o *ServerOptions) GetIdleTimeout() time.Duration {
	return o.IdleTimeout
}

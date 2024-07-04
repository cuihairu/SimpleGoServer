package reactor

import "runtime"

type ServerOptions struct {
	Multicore  bool // 是否使用多核
	NumWorkers int
	Listener   string
	LockThread bool
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

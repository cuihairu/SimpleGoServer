package reactor

import "github.com/cuihairu/simplegoserver/pkg/handler"

type Options struct {
	Multicore     bool // 是否使用多核
	NumWorkers    int
	Listener      string
	LockThread    bool
	initializer   handler.Pipeinitializer
	eventListener handler.EventListener
}

package channel

import "github.com/cuihairu/simplegoserver/pkg/event"

type Channel interface {
	ID() string
	EventLoop() event.Loop
	Config()
	IsOpen() bool
	IsRegistered() bool
	IsActive() bool
	Write([]byte) (int, error)
	Writev([][]byte) (int, error)

	Close(err error)
}

package handler

import (
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"net"
	"sync"
)

var (
	once     sync.Once
	instance *Holder
)

type Holder struct {
	channels map[net.Conn]struct{}
	mutex    sync.RWMutex
}

func (h *Holder) HandleActive(ctx handler.ActiveContext) {
	h.Add(ctx.Conn())
}

func (h *Holder) HandleInactive(ctx handler.InactiveContext, ex handler.Exception) {
	h.Remove(ctx.Conn())
}

func NewHolder() *Holder {
	once.Do(func() {
		instance = &Holder{
			channels: make(map[net.Conn]struct{}, 4096),
		}
	})
	return instance
}

func (h *Holder) Add(conn net.Conn) {
	h.mutex.Lock()
	h.channels[conn] = struct{}{}
	h.mutex.Unlock()
}

func (h *Holder) Remove(conn net.Conn) {
	h.mutex.Lock()
	delete(h.channels, conn)
	h.mutex.Unlock()
}

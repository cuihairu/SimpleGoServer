package handler

import (
	"net"
	"sync"
)

type Holder struct {
	channels map[net.Conn]struct{}
	mutex    sync.RWMutex
}

func NewHolder(initCapacity int) *Holder {
	return &Holder{
		channels: make(map[net.Conn]struct{}, initCapacity),
	}
}

func (h *Holder) Add(conn net.Conn) {
	h.mutex.Lock()
	h.channels[conn] = struct{}{}
	h.mutex.Unlock()
}

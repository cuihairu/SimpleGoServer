package handler

import (
	"errors"
	"net"
	"sync"
)

type Pipeline struct {
	head *HandlerNodeContext
	tail *HandlerNodeContext
	conn net.Conn
	size int
	sync.RWMutex
}

type Pipeinitializer func(pipeline *Pipeline) error

func WithDefaultPipeline(pipeline *Pipeline) error {
	if pipeline == nil {
		return errors.New("pipeline is nil")
	}
	pipeline.AddLast(NewErrorHandler)
	return nil
}

func NewPipeline(conn net.Conn) *Pipeline {
	p := &Pipeline{
		size: 2,
		conn: conn,
	}
	p.head = NewHandlerNodeContext(p, &HeadHandler{}, nil, nil)
	p.tail = NewHandlerNodeContext(p, &TailHeader{}, nil, nil)
	p.head.next = p.tail
	p.tail.prev = p.head
	return p
}

func (p *Pipeline) Size() int {
	return p.size
}

func (p *Pipeline) Conn() net.Conn {
	return p.conn
}

func (p *Pipeline) addFirst(handler Handler) *Pipeline {
	next := p.head.next
	p.head.prev = NewHandlerNodeContext(p, handler, p.head, next)
	next.prev.next = p.head.next
	p.size++
	return p
}
func (p *Pipeline) AddFirst(handlers ...Handler) *Pipeline {
	p.RWMutex.Lock()
	defer p.RWMutex.Unlock()
	if !IsValidHandlers(handlers) {
		panic("invalid handler")
	}
	for _, handler := range handlers {
		p.addFirst(handler)
	}
	return p
}

func (p *Pipeline) addLast(handler Handler) *Pipeline {
	next := p.tail.next
	p.tail.prev = NewHandlerNodeContext(p, handler, next, p.tail)
	p.tail.next = p.tail.prev
	p.size++
	return p
}

func (p *Pipeline) AddLast(handlers ...Handler) *Pipeline {
	p.RWMutex.Lock()
	defer p.RWMutex.Unlock()
	if !IsValidHandlers(handlers) {
		panic("invalid handler")
	}
	for _, handler := range handlers {
		p.addLast(handler)
	}
	return p
}

func (p *Pipeline) AddHandler(position int, handlers ...Handler) *Pipeline {
	if !IsValidHandlers(handlers) {
		panic("invalid handler")
	}
	if (position < 0) || (position > p.size) {
		panic("invalid position")
	}
	if position == 0 {
		return p.AddFirst(handlers)
	}
	if position == p.size {
		return p.AddLast(handlers...)
	}
	p.RWMutex.Lock()
	defer p.RWMutex.Unlock()
	curNode := p.head
	for i := 0; i < position; i++ {
		curNode = curNode.next
	}

	for _, h := range handlers {
		oldNext := curNode.next
		curNode.next = NewHandlerNodeContext(p, h, curNode, oldNext)

		oldNext.prev = curNode.next
		curNode = curNode.next
		p.size++
	}

	return p
}

func (p *Pipeline) FireActive() {
	p.head.HandleActive()
}

func (p *Pipeline) FireRead(message Message) {
	p.head.HandleRead(message)
}

func (p *Pipeline) FireWrite(message Message) {
	p.tail.HandleWrite(message)
}

func (p *Pipeline) FireException(ex Exception) {
	p.head.HandleException(ex)
}

func (p *Pipeline) FireInactive(ex error) {
	p.head.HandleInactive(ex)
}

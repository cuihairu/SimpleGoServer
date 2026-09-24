package handler

import (
	"errors"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"net"
)

type LinkedPipeline struct {
	head *NodeContext
	tail *NodeContext
	conn net.Conn
	size int
}

func (p *LinkedPipeline) IndexOf(f func(handler.Handler) bool) int {
	cur := p.head
	for i := 0; ; i++ {
		if f(cur.handler) {
			return i
		}
		if cur = cur.next; cur == nil {
			break
		}
	}
	return -1
}

// LastIndexOf scans from the tail back towards the head, returning the
// node's index in the same index space IndexOf uses (head = 0).
func (p *LinkedPipeline) LastIndexOf(f func(handler.Handler) bool) int {
	cur := p.tail
	for i := p.size - 1; ; i-- {
		if f(cur.handler) {
			return i
		}
		if cur = cur.prev; cur == nil {
			break
		}
	}
	return -1
}

func (p *LinkedPipeline) ContextAt(position int) handler.Context {
	if position < 0 || position >= p.size {
		return nil
	}
	node := p.head
	for ; position > 0; position-- {
		if node.next == nil {
			return nil
		}
		node = node.next
	}
	return node
}

var _ handler.Pipeline = (*LinkedPipeline)(nil)

func WithDefaultPipeline(p handler.Pipeline) error {
	if p == nil {
		return errors.New("pipeline is nil")
	}
	p.AddLast(NewErrorHandler())
	return nil
}

func NewPipeline(conn net.Conn) *LinkedPipeline {
	p := &LinkedPipeline{
		size: 2,
		conn: conn,
	}
	p.head = NewNodeContext(p, NewHeadHandler(), nil, nil)
	p.tail = NewNodeContext(p, NewTailHeader(), nil, nil)
	p.head.next = p.tail
	p.tail.prev = p.head
	return p
}

func (p *LinkedPipeline) Size() int {
	return p.size
}

func (p *LinkedPipeline) Conn() net.Conn {
	return p.conn
}

func (p *LinkedPipeline) addFirst(handler handler.Handler) *LinkedPipeline {
	oldNext := p.head.next
	p.head.next = NewNodeContext(p, handler, p.head, oldNext)
	oldNext.prev = p.head.next
	p.size++
	return p
}
func (p *LinkedPipeline) AddFirst(handlers ...handler.Handler) handler.Pipeline {
	if !handler.IsValidHandlers(handlers...) {
		panic("invalid handler")
	}
	for _, h := range handlers {
		p.addFirst(h)
	}
	return p
}

func (p *LinkedPipeline) addLast(h handler.Handler) *LinkedPipeline {
	oldPrev := p.tail.prev
	p.tail.prev = NewNodeContext(p, h, oldPrev, p.tail)
	oldPrev.next = p.tail.prev
	p.size++
	return p
}

func (p *LinkedPipeline) AddLast(handlers ...handler.Handler) handler.Pipeline {
	if !handler.IsValidHandlers(handlers...) {
		panic("invalid handler")
	}
	for _, h := range handlers {
		p.addLast(h)
	}
	return p
}

func (p *LinkedPipeline) AddHandler(position int, handlers ...handler.Handler) handler.Pipeline {
	if !handler.IsValidHandlers(handlers...) {
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
	curNode := p.head
	for i := 0; i < position; i++ {
		curNode = curNode.next
	}

	for _, h := range handlers {
		oldNext := curNode.next
		curNode.next = NewNodeContext(p, h, curNode, oldNext)

		oldNext.prev = curNode.next
		curNode = curNode.next
		p.size++
	}

	return p
}

func (p *LinkedPipeline) FireActive() {
	p.head.HandleActive()
}

func (p *LinkedPipeline) FireRead(message handler.Message) {
	p.head.HandleRead(message)
}

func (p *LinkedPipeline) FireWrite(message handler.Message) {
	p.tail.HandleWrite(message)
}

func (p *LinkedPipeline) FireException(ex handler.Exception) {
	p.head.HandleException(ex)
}

func (p *LinkedPipeline) FireInactive(ex handler.Exception) {
	p.head.HandleInactive(ex)
}

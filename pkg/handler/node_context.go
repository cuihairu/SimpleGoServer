package handler

import (
	"net"
	"sync"
)

type NodeContext struct {
	pipeline         Pipeline
	handler          Handler
	prev             *NodeContext
	next             *NodeContext
	activeHandler    ActiveHandler
	inboundHandler   InboundHandler
	outboundHandler  OutboundHandler
	exceptionHandler ExceptionHandler
	inactiveHandler  InactiveHandler

	// per-connection attachment (e.g. a codec's partial-frame buffer);
	// handlers may set it from pipeline callbacks on different goroutines,
	// so access is guarded
	attachMu   sync.RWMutex
	attachment Attachment
}

var _ Context = (*NodeContext)(nil)

func NewNodeContext(pipeline Pipeline, h Handler, prev *NodeContext, next *NodeContext) *NodeContext {
	n := &NodeContext{
		pipeline: pipeline,
		handler:  h,
		prev:     prev,
		next:     next,
	}
	n.activeHandler, _ = h.(ActiveHandler)
	n.inboundHandler, _ = h.(InboundHandler)
	n.outboundHandler, _ = h.(OutboundHandler)
	n.exceptionHandler, _ = h.(ExceptionHandler)
	n.inactiveHandler, _ = h.(InactiveHandler)
	return n
}

func (n *NodeContext) Conn() net.Conn {
	return n.pipeline.Conn()
}

func (n *NodeContext) Handler() Handler {
	return n.handler
}

func (n *NodeContext) Write(message Message) {
	defer func() {
		if err := recover(); err != nil {
			n.pipeline.FireException(err)
		}
	}()
	// start at the previous node: writing from a context propagates towards
	// the head, skipping the handler that owns this context — matching the
	// outbound semantics of Netty and avoiding self re-entry
	for cur := n.prev; cur != nil; cur = cur.prev {
		if cur.outboundHandler != nil {
			cur.outboundHandler.HandleWrite(cur, message)
			break
		}
	}
}

func (n *NodeContext) Close(exception error) {
	_ = n.pipeline.Conn().Close()
}

func (n *NodeContext) Attachment() Attachment {
	n.attachMu.RLock()
	defer n.attachMu.RUnlock()
	return n.attachment
}

func (n *NodeContext) SetAttachment(attachment Attachment) {
	n.attachMu.Lock()
	n.attachment = attachment
	n.attachMu.Unlock()
}

var _ ActiveContext = (*NodeContext)(nil)

func (n *NodeContext) HandleActive() {
	// inbound propagation starts at the next node, skipping the handler
	// that owns this context (Netty semantics)
	for cur := n.next; cur != nil; cur = cur.next {
		if cur.activeHandler != nil {
			cur.activeHandler.HandleActive(cur)
			break
		}
	}
}

var _ InboundContext = (*NodeContext)(nil)

func (n *NodeContext) HandleRead(message Message) {
	// see HandleActive: skip self when propagating inbound
	for cur := n.next; cur != nil; cur = cur.next {
		if cur.inboundHandler != nil {
			cur.inboundHandler.HandleRead(cur, message)
			break
		}
	}
}

var _ OutboundContext = (*NodeContext)(nil)

func (n *NodeContext) HandleWrite(message Message) {
	// see Write: outbound propagation starts at the previous node
	for cur := n.prev; cur != nil; cur = cur.prev {
		if cur.outboundHandler != nil {
			cur.outboundHandler.HandleWrite(cur, message)
			break
		}
	}
}

var _ ExceptionContext = (*NodeContext)(nil)

func (n *NodeContext) HandleException(ex Exception) {
	// see HandleActive: skip self when propagating inbound
	for cur := n.next; cur != nil; cur = cur.next {
		if cur.exceptionHandler != nil {
			cur.exceptionHandler.HandleException(cur, ex)
			break
		}
	}
}

var _ InactiveContext = (*NodeContext)(nil)

func (n *NodeContext) HandleInactive(ex Exception) {
	// see HandleActive: skip self when propagating inbound
	for cur := n.next; cur != nil; cur = cur.next {
		if cur.inactiveHandler != nil {
			cur.inactiveHandler.HandleInactive(cur, ex)
			break
		}
	}
}

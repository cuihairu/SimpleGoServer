package handler

import (
	"net"
	"sync"

	"github.com/cuihairu/simplegoserver/pkg/handler"
)

type NodeContext struct {
	pipeline         handler.Pipeline
	handler          handler.Handler
	prev             *NodeContext
	next             *NodeContext
	activeHandler    handler.ActiveHandler
	inboundHandler   handler.InboundHandler
	outboundHandler  handler.OutboundHandler
	exceptionHandler handler.ExceptionHandler
	inactiveHandler  handler.InactiveHandler
	executorHandler  handler.ExecutorHandler

	// per-connection attachment (e.g. a codec's partial-frame buffer);
	// handlers may set it from pipeline callbacks on different goroutines,
	// so access is guarded
	attachMu   sync.RWMutex
	attachment handler.Attachment
}

var _ handler.Context = (*NodeContext)(nil)

func NewNodeContext(pipeline handler.Pipeline, h handler.Handler, prev *NodeContext, next *NodeContext) *NodeContext {
	n := &NodeContext{
		pipeline: pipeline,
		handler:  h,
		prev:     prev,
		next:     next,
	}
	n.activeHandler, _ = h.(handler.ActiveHandler)
	n.inboundHandler, _ = h.(handler.InboundHandler)
	n.outboundHandler, _ = h.(handler.OutboundHandler)
	n.exceptionHandler, _ = h.(handler.ExceptionHandler)
	n.inactiveHandler, _ = h.(handler.InactiveHandler)
	n.executorHandler, _ = h.(handler.ExecutorHandler)
	return n
}

func (n *NodeContext) Conn() net.Conn {
	return n.pipeline.Conn()
}

func (n *NodeContext) Handler() handler.Handler {
	return n.handler
}

func (n *NodeContext) Write(message handler.Message) {
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
	n.pipeline.Conn().Close()
}

func (n *NodeContext) Attachment() handler.Attachment {
	n.attachMu.RLock()
	defer n.attachMu.RUnlock()
	return n.attachment
}

func (n *NodeContext) SetAttachment(attachment handler.Attachment) {
	n.attachMu.Lock()
	n.attachment = attachment
	n.attachMu.Unlock()
}

var _ handler.ActiveContext = (*NodeContext)(nil)

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

var _ handler.InboundContext = (*NodeContext)(nil)

func (n *NodeContext) HandleRead(message handler.Message) {
	// see HandleActive: skip self when propagating inbound
	for cur := n.next; cur != nil; cur = cur.next {
		if cur.inboundHandler != nil {
			cur.inboundHandler.HandleRead(cur, message)
			break
		}
	}
}

var _ handler.OutboundContext = (*NodeContext)(nil)

func (n *NodeContext) HandleWrite(message handler.Message) {
	// see Write: outbound propagation starts at the previous node
	for cur := n.prev; cur != nil; cur = cur.prev {
		if cur.outboundHandler != nil {
			cur.outboundHandler.HandleWrite(cur, message)
			break
		}
	}
}

var _ handler.ExceptionContext = (*NodeContext)(nil)

func (n *NodeContext) HandleException(ex handler.Exception) {
	// see HandleActive: skip self when propagating inbound
	for cur := n.next; cur != nil; cur = cur.next {
		if cur.exceptionHandler != nil {
			cur.exceptionHandler.HandleException(cur, ex)
			break
		}
	}
}

var _ handler.InactiveContext = (*NodeContext)(nil)

func (n *NodeContext) HandleInactive(ex handler.Exception) {
	// see HandleActive: skip self when propagating inbound
	for cur := n.next; cur != nil; cur = cur.next {
		if cur.inactiveHandler != nil {
			cur.inactiveHandler.HandleInactive(cur, ex)
			break
		}
	}
}

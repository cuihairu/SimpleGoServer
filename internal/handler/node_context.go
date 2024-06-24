package handler

import (
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"net"
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
}

var _ handler.HandlerContext = (*NodeContext)(nil)

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
	for cur := n; cur != nil; cur = cur.prev {
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
	//TODO implement me
	panic("implement me")
}

func (n *NodeContext) SetAttachment(attachment handler.Attachment) {
	//TODO implement me
	panic("implement me")
}

var _ handler.ActiveContext = (*NodeContext)(nil)

func (n *NodeContext) HandleActive() {
	for cur := n; cur != nil; cur = cur.next {
		if cur.activeHandler != nil {
			cur.activeHandler.HandleActive(cur)
			break
		}
	}
}

var _ handler.InboundContext = (*NodeContext)(nil)

func (n *NodeContext) HandleRead(message handler.Message) {
	for cur := n; cur != nil; cur = cur.next {
		if cur.inboundHandler != nil {
			cur.inboundHandler.HandleRead(cur, message)
			break
		}
	}
}

var _ handler.OutboundContext = (*NodeContext)(nil)

func (n *NodeContext) HandleWrite(message handler.Message) {
	for cur := n; cur != nil; cur = cur.prev {
		if cur.outboundHandler != nil {
			cur.outboundHandler.HandleWrite(cur, message)
			break
		}
	}
}

var _ handler.ExceptionContext = (*NodeContext)(nil)

func (n *NodeContext) HandleException(ex handler.Exception) {
	for cur := n; cur != nil; cur = cur.next {
		if cur.exceptionHandler != nil {
			cur.exceptionHandler.HandleException(cur, ex)
			break
		}
	}
}

var _ handler.InactiveContext = (*NodeContext)(nil)

func (n *NodeContext) HandleInactive(ex handler.Exception) {
	for cur := n; cur != nil; cur = cur.next {
		if cur.inactiveHandler != nil {
			cur.inactiveHandler.HandleInactive(cur, ex)
			break
		}
	}
}

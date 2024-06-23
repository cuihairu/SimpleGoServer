package handler

import "net"

type (
	Message interface {
	}
	Attachment interface {
	}
	HandlerContext interface {
		Conn() net.Conn
		Handler() Handler
		Write(Message)
		Close(error)
		Attachment() Attachment
		SetAttachment(Attachment)
	}
	ActiveContext interface {
		HandlerContext
		HandleActive()
	}

	// InboundContext defines an inbound handler
	InboundContext interface {
		HandlerContext
		HandleRead(message Message)
	}

	// OutboundContext defines an outbound handler
	OutboundContext interface {
		HandlerContext
		HandleWrite(message Message)
	}

	// ExceptionContext defines an exception handler
	ExceptionContext interface {
		HandlerContext
		HandleException(ex Exception)
	}

	// InactiveContext defines an inactive handler
	InactiveContext interface {
		HandlerContext
		HandleInactive(error)
	}
)

type HandlerNodeContext struct {
	pipeline         *Pipeline
	handler          Handler
	prev             *HandlerNodeContext
	next             *HandlerNodeContext
	activeHandler    ActiveHandler
	inboundHandler   InboundHandler
	outboundHandler  OutboundHandler
	exceptionHandler ExceptionHandler
	inactiveHandler  InactiveHandler
}

var _ HandlerContext = (*HandlerNodeContext)(nil)

func NewHandlerNodeContext(pipeline *Pipeline, handler Handler, prev *HandlerNodeContext, next *HandlerNodeContext) *HandlerNodeContext {
	n := &HandlerNodeContext{
		pipeline: pipeline,
		handler:  handler,
		prev:     prev,
		next:     next,
	}
	n.activeHandler, _ = handler.(ActiveHandler)
	n.inboundHandler, _ = handler.(InboundHandler)
	n.outboundHandler, _ = handler.(OutboundHandler)
	n.exceptionHandler, _ = handler.(ExceptionHandler)
	n.inactiveHandler, _ = handler.(InactiveHandler)
	return n
}

func (n *HandlerNodeContext) Conn() net.Conn {
	return n.pipeline.Conn()
}

func (n *HandlerNodeContext) Handler() Handler {
	return n.handler
}

func (n *HandlerNodeContext) Write(message Message) {
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

func (n *HandlerNodeContext) Close(exception error) {
	n.pipeline.Conn().Close()
}

func (n *HandlerNodeContext) Attachment() Attachment {
	//TODO implement me
	panic("implement me")
}

func (n *HandlerNodeContext) SetAttachment(attachment Attachment) {
	//TODO implement me
	panic("implement me")
}

var _ ActiveContext = (*HandlerNodeContext)(nil)

func (n *HandlerNodeContext) HandleActive() {
	for cur := n; cur != nil; cur = cur.next {
		if cur.activeHandler != nil {
			cur.activeHandler.HandleActive(cur)
			break
		}
	}
}

var _ InboundContext = (*HandlerNodeContext)(nil)

func (n *HandlerNodeContext) HandleRead(message Message) {
	for cur := n; cur != nil; cur = cur.next {
		if cur.inboundHandler != nil {
			cur.inboundHandler.HandleRead(cur, message)
			break
		}
	}
}

var _ OutboundContext = (*HandlerNodeContext)(nil)

func (n *HandlerNodeContext) HandleWrite(message Message) {
	for cur := n; cur != nil; cur = cur.prev {
		if cur.outboundHandler != nil {
			cur.outboundHandler.HandleWrite(cur, message)
			break
		}
	}
}

var _ ExceptionContext = (*HandlerNodeContext)(nil)

func (n *HandlerNodeContext) HandleException(ex Exception) {
	for cur := n; cur != nil; cur = cur.next {
		if cur.exceptionHandler != nil {
			cur.exceptionHandler.HandleException(cur, ex)
			break
		}
	}
}

var _ InactiveContext = (*HandlerNodeContext)(nil)

func (n *HandlerNodeContext) HandleInactive(ex error) {
	for cur := n; cur != nil; cur = cur.next {
		if cur.inactiveHandler != nil {
			cur.inactiveHandler.HandleInactive(cur, ex)
			break
		}
	}
}

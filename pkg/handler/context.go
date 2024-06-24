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
		HandleInactive(ex Exception)
	}
)

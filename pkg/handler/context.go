package handler

import "net"

type (
	Message interface {
	}
	Attachment interface {
	}
	Context interface {
		Conn() net.Conn
		Handler() Handler
		Write(Message)
		Close(error)
		Attachment() Attachment
		SetAttachment(Attachment)
	}
	ActiveContext interface {
		Context
		HandleActive()
	}

	// InboundContext defines an inbound handler
	InboundContext interface {
		Context
		HandleRead(message Message)
	}

	// OutboundContext defines an outbound handler
	OutboundContext interface {
		Context
		HandleWrite(message Message)
	}

	// ExceptionContext defines an exception handler
	ExceptionContext interface {
		Context
		HandleException(ex Exception)
	}

	// InactiveContext defines an inactive handler
	InactiveContext interface {
		Context
		HandleInactive(ex Exception)
	}
)

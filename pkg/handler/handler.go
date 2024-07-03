package handler

import (
	"github.com/cuihairu/simplegoserver/pkg"
)

type (
	Handler interface {
	}
	ActiveHandler interface {
		HandleActive(ctx ActiveContext)
	}
	Exception interface {
	}
	// InboundHandler defines an Inbound handler
	InboundHandler interface {
		HandleRead(ctx InboundContext, message Message)
	}

	// OutboundHandler defines an outbound handler
	OutboundHandler interface {
		HandleWrite(ctx OutboundContext, message Message)
	}

	// ExceptionHandler defines an exception handler
	ExceptionHandler interface {
		HandleException(ctx ExceptionContext, ex Exception)
	}

	// InactiveHandler defines an inactive handler
	InactiveHandler interface {
		HandleInactive(ctx InactiveContext, ex Exception)
	}
	ExecutorHandler interface {
		Executor() pkg.Executor[any]
	}
)

type CodecHandler interface {
	CodecName() string
	InboundHandler
	OutboundHandler
}

// ChannelHandler defines a channels handler
type ChannelHandler interface {
	ActiveHandler
	InboundHandler
	OutboundHandler
	ExceptionHandler
	InactiveHandler
}

// ChannelInboundHandler defines a channels inbound handler
type ChannelInboundHandler interface {
	ActiveHandler
	InboundHandler
	InactiveHandler
}

// ChannelOutboundHandler defines a channels outbound handler
type ChannelOutboundHandler interface {
	ActiveHandler
	OutboundHandler
	InactiveHandler
}

func IsValidHandlers(handlers ...Handler) bool {
	for _, h := range handlers {
		if h == nil {
			return false
		}
		switch h.(type) {
		case ActiveHandler:
		case InboundHandler:
		case OutboundHandler:
		case InactiveHandler:
		case ExecutorHandler:
		case ExceptionHandler:
		default:
			return false
		}
	}
	return true
}

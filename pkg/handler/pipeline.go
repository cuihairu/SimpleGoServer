package handler

import "net"

type Pipeline interface {

	// AddFirst add a handler to the first.
	AddFirst(handlers ...Handler) Pipeline

	// AddLast add a handler to the last.
	AddLast(handlers ...Handler) Pipeline

	// AddHandler add handlers in position.
	AddHandler(position int, handlers ...Handler) Pipeline

	// IndexOf find fist index of handler.
	IndexOf(func(Handler) bool) int

	// LastIndexOf find last index of handler.
	LastIndexOf(func(Handler) bool) int

	// ContextAt get context by position.
	ContextAt(position int) Context

	// Size of handler
	Size() int

	Conn() net.Conn

	FireActive()
	FireRead(message Message)
	FireWrite(message Message)
	FireException(ex Exception)
	FireInactive(ex Exception)
}

type PipeInitializer func(pipeline Pipeline) error

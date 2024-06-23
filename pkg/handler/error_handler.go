package handler

import (
	"log"
	"net"
	"os"
)

type ErrorHandler struct {
	logger *log.Logger
}

func (e ErrorHandler) OnStartup() {
	e.logger.Printf("server is starting up")
}

func (e ErrorHandler) OnReload() {
	e.logger.Printf("server is reloading")
}

func (e ErrorHandler) OnShutdown() {
	e.logger.Printf("server is shutting down")
}

func (e ErrorHandler) OnError(err any) {
	e.logger.Println(err)
}

func (e ErrorHandler) OnConnect(conn net.Conn) {
	e.logger.Printf("new connect :%s", conn.RemoteAddr())
}

func (e ErrorHandler) HandleException(ctx ExceptionContext, ex Exception) {
	e.logger.Printf("handle exception :%s", ex)
}

func NewErrorHandler() *ErrorHandler {
	return &ErrorHandler{
		logger: log.New(os.Stderr, "--", log.LstdFlags),
	}
}

var _ ExceptionHandler = (*ErrorHandler)(nil)
var _ EventListener = (*ErrorHandler)(nil)

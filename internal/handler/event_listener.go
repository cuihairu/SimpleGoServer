package handler

import (
	"github.com/cuihairu/simplegoserver/pkg/event"
	"github.com/cuihairu/simplegoserver/pkg/handler"
	"log"
	"net"
	"os"
	"sync"
)

var (
	onceErrorHandler     sync.Once
	instanceErrorHandler *ErrorHandler
)

type ErrorHandler struct {
	logger    *log.Logger
	errLogger *log.Logger
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
	e.errLogger.Println(err)
}

func (e ErrorHandler) OnConnect(conn net.Conn) {
	e.logger.Printf("new connect :%s", conn.RemoteAddr())
}

func (e ErrorHandler) HandleException(ctx handler.ExceptionContext, ex handler.Exception) {
	e.errLogger.Printf("handle exception :%s", ex)
}

func NewErrorHandler() *ErrorHandler {
	onceErrorHandler.Do(func() {
		instanceErrorHandler = &ErrorHandler{
			errLogger: log.New(os.Stderr, "[Error]", log.LstdFlags),
			logger:    log.New(os.Stdout, "[Info]", log.LstdFlags),
		}
	})
	return instanceErrorHandler
}

var _ handler.ExceptionHandler = (*ErrorHandler)(nil)
var _ event.Listener = (*ErrorHandler)(nil)

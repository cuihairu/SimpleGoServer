package handler

import "net"

type EventListener interface {
	OnStartup()
	OnReload()
	OnShutdown()
	OnError(err any)
	OnConnect(conn net.Conn)
}

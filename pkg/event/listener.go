package event

import "net"

type Event interface {
}

type Listener interface {
	OnStartup()
	OnReload()
	OnShutdown()
	OnError(err any)
	OnConnect(conn net.Conn)
}

package event

import "net"

type Group interface {
	Register(conn net.Conn)
	ShutdownGracefully()
	Next() Loop
}

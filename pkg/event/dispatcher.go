package event

import "net"

type Dispatcher interface {
	RegisterEvent()
	Dispatch(conn net.Conn)
}

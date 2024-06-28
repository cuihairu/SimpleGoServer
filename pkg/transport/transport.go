package transport

import "net"

type Transport interface {
	net.Conn
}

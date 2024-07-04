package reactor

import "net"

type Acceptor interface {
	Accept() (net.Conn, error)
	Close() error
}

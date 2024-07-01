package tcp

import "net"

type tcpTransport struct {
	net.Conn
}

func newTcpTransport(conn *net.TCPConn, tcpOptions *Options) (*tcpTransport, error) {
	return &tcpTransport{
		Conn: conn,
	}, nil
}

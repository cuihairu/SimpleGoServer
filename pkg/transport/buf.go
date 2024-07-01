package transport

import (
	"bufio"
	"io"
	"net"
)

type BufTransport struct {
	net.Conn
	reader io.Reader
	write bufio.Writer
}

func NewBufTransport(conn net.Conn, readBufSize int, writeBufSize int) *BufTransport {
	r := bufio.NewReader(conn)
	r.
	return &BufTransport{
		Conn:         conn,
		ReadWriter:   bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		readBufSize:  readBufSize,
		writeBufSize: writeBufSize,
	}
}

package transport

import (
	"io"
	"net"
)

type BuffersWriter interface {
	Writev(buffers net.Buffers) (int, error)
}

type FlushWriter interface {
	Write(p []byte) (n int, err error)
	Flush() error
	Close() error
}

type WrapTransport[T any] interface {
	RawTransport() T
}

type Transport interface {
	net.Conn
	BuffersWriter
	Flush() error
}

type transport struct {
	net.Conn
	reader io.Reader
	writer io.Writer
}

func (t transport) Writev(buffers net.Buffers) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (t transport) Flush() error {
	//TODO implement me
	panic("implement me")
}

func (t transport) RawTransport() interface{} {
	//TODO implement me
	panic("implement me")
}

func NewTransport(conn net.Conn, reader io.Reader, writer io.Writer) Transport {
	return &transport{
		Conn:   conn,
		reader: reader,
		writer: writer,
	}
}

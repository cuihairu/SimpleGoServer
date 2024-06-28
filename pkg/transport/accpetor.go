package transport

type Acceptor interface {
	Accept()
	Close() error
}

package channels

type Acceptor interface {
	Accept() (Channel, error)
}

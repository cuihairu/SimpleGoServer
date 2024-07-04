package channels

type Listen func(network string, address string) (Acceptor, error)

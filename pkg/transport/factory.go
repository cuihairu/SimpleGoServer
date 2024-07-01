package transport

import (
	"github.com/cuihairu/simplegoserver/pkg"
)

type Factory interface {
	Schemes() Schemes
	Connect(opts pkg.Options) (Transport, error)
	Listen(opts pkg.Options) (Acceptor, error)
}

package balancer

import (
	"github.com/cuihairu/simplegoserver/pkg"
	"net"
)

type IPHashingBalancer struct {
}

func (I IPHashingBalancer) Next(ch net.Conn) (pkg.Backend, error) {
	//TODO implement me
	panic("implement me")
}

func (I IPHashingBalancer) Register(b pkg.Backend) error {
	//TODO implement me
	panic("implement me")
}

func (I IPHashingBalancer) Unregister(b pkg.Backend) error {
	//TODO implement me
	panic("implement me")
}

func (I IPHashingBalancer) Size() int {
	//TODO implement me
	panic("implement me")
}

func (I IPHashingBalancer) Iterate(f func(b pkg.Backend) bool) {
	//TODO implement me
	panic("implement me")
}

var _ pkg.Balancer = (*IPHashingBalancer)(nil)

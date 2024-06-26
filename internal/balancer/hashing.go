package balancer

import (
	"github.com/cuihairu/simplegoserver/pkg"
	"net"
)

type HashingBalancer struct {
}

func (h HashingBalancer) Next(ch net.Conn) (pkg.Backend, error) {
	//TODO implement me
	panic("implement me")
}

func (h HashingBalancer) Register(b pkg.Backend) error {
	//TODO implement me
	panic("implement me")
}

func (h HashingBalancer) Unregister(b pkg.Backend) error {
	//TODO implement me
	panic("implement me")
}

func (h HashingBalancer) Size() int {
	//TODO implement me
	panic("implement me")
}

func (h HashingBalancer) Iterate(f func(b pkg.Backend) bool) {
	//TODO implement me
	panic("implement me")
}

var _ pkg.Balancer = (*HashingBalancer)(nil)

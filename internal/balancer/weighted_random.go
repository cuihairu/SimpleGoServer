package balancer

import (
	"github.com/cuihairu/simplegoserver/pkg"
	"net"
)

type WeightedRandomBalancer struct {
}

func (w WeightedRandomBalancer) Next(ch net.Conn) (pkg.Backend, error) {
	//TODO implement me
	panic("implement me")
}

func (w WeightedRandomBalancer) Register(b pkg.Backend) error {
	//TODO implement me
	panic("implement me")
}

func (w WeightedRandomBalancer) Unregister(b pkg.Backend) error {
	//TODO implement me
	panic("implement me")
}

func (w WeightedRandomBalancer) Size() int {
	//TODO implement me
	panic("implement me")
}

func (w WeightedRandomBalancer) Iterate(f func(b pkg.Backend) bool) {
	//TODO implement me
	panic("implement me")
}

var _ pkg.Balancer = (*WeightedRandomBalancer)(nil)

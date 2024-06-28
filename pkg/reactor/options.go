package reactor

import "github.com/cuihairu/simplegoserver/pkg"

type ServerOptions struct {
	Multicore  bool // 是否使用多核
	NumWorkers int
	Listener   string
	LockThread bool
}

func (s ServerOptions) Apply(opts ...pkg.Option) error {
	return pkg.Apply(s, opts...)
}

var _ pkg.Options = (*ServerOptions)(nil)

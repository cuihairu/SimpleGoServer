package pkg

type Backend interface {
	Id() string
}

type WeightBackend interface {
	Backend
	Weight() int
	SetWeight(weight int)
}

type CountBackend interface {
	Backend
	Count() int
	SetCount(count int)
}

type Balancer interface {
	Next(key string) (Backend, error)
	Register(b Backend) error
	Unregister(b Backend) error
	Size() int
	Iterate(f func(b Backend) bool)
}

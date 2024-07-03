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

type Balancer[T Backend] interface {
	Next(key string) (T, error)
	Register(b T) error
	Unregister(b T) error
	Size() int
	Iterate(f func(b T) bool)
}

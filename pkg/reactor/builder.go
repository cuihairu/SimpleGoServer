package reactor

type Builder struct {
}

func (b Builder) Build() *Reactor {
	return &Reactor{}
}

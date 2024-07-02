package event

type Event interface {
	Type() uint32
	Source() any
}

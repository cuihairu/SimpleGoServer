package event

type Group interface {
	Dispatcher
	Loop
}

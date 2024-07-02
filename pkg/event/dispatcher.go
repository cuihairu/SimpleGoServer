package event

type Dispatcher interface {
	RegisterEvent(event Event, handler Handler)
	UnregisterEvent(event Event)
	Dispatch(event Event)
}

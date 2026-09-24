package event

import (
	"net"
	"testing"
)

// The package is pure interface surface; these compile-time assertions pin
// the method sets that implementers across the repo rely on.
var (
	_ Dispatcher = dispatcherStub{}
	_ Event      = eventStub{}
	_ Handler    = handlerStub{}
	_ Listener   = listenerStub{}
	_ Loop       = loopStub{}
	_ Group      = groupStub{}
)

type dispatcherStub struct{}

func (dispatcherStub) RegisterEvent(Event, Handler) {}
func (dispatcherStub) UnregisterEvent(Event)        {}
func (dispatcherStub) Dispatch(Event)               {}

type eventStub struct{}

func (eventStub) Type() uint32 { return 0 }
func (eventStub) Source() any  { return nil }

type handlerStub struct{}

type listenerStub struct{}

func (listenerStub) OnStartup()         {}
func (listenerStub) OnReload()          {}
func (listenerStub) OnShutdown()        {}
func (listenerStub) OnError(any)        {}
func (listenerStub) OnConnect(net.Conn) {}

type loopStub struct{}

func (loopStub) Run()        {}
func (loopStub) Stop() error { return nil }

type groupStub struct{}

func (groupStub) Register(net.Conn)   {}
func (groupStub) Unregister(net.Conn) {}
func (groupStub) ShutdownGracefully() {}
func (groupStub) Next() Loop          { return loopStub{} }

func TestInterfaceStubsSatisfy(t *testing.T) {
	var e Event = eventStub{}
	if e.Type() != 0 || e.Source() != nil {
		t.Fatal("event stub should report zero values")
	}
	if loop := (groupStub{}).Next(); loop == nil {
		t.Fatal("group stub should produce a loop")
	}
}

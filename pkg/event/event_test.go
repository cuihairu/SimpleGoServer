package event

import (
	"net"
	"testing"
)

// The package is pure interface surface; these compile-time assertions pin
// the method sets that implementers across the repo rely on.
var (
	_ Listener = listenerStub{}
	_ Loop     = loopStub{}
	_ Group    = groupStub{}
)

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
	if loop := (groupStub{}).Next(); loop == nil {
		t.Fatal("group stub should produce a loop")
	}
}

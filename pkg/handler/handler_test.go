package handler

import "testing"

// minimal single-capability stubs: each satisfies exactly one of the
// handler interfaces.
type inboundOnly struct{}

func (inboundOnly) HandleRead(ctx InboundContext, message Message) {}

type outboundOnly struct{}

func (outboundOnly) HandleWrite(ctx OutboundContext, message Message) {}

type fullChannel struct{}

func (fullChannel) HandleActive(ctx ActiveContext)                     {}
func (fullChannel) HandleRead(ctx InboundContext, message Message)     {}
func (fullChannel) HandleWrite(ctx OutboundContext, message Message)   {}
func (fullChannel) HandleException(ctx ExceptionContext, ex Exception) {}
func (fullChannel) HandleInactive(ctx InactiveContext, ex Exception)   {}

// TestIsValidHandlers pins the gate that AddLast/AddFirst/AddHandler run
// before touching the pipeline: any nil, or any value that implements none
// of the handler interfaces, is rejected — everything else passes.
func TestIsValidHandlers(t *testing.T) {
	if IsValidHandlers() != true {
		t.Fatal("an empty handler list is valid")
	}
	if !IsValidHandlers(inboundOnly{}, outboundOnly{}, fullChannel{}) {
		t.Fatal("handlers implementing known interfaces must pass")
	}
	// plain values that implement nothing
	if IsValidHandlers("not a handler") {
		t.Fatal("a value implementing no interface must be rejected")
	}
	if IsValidHandlers(42) {
		t.Fatal("an int must be rejected")
	}
	if IsValidHandlers(nil) {
		t.Fatal("a nil handler must be rejected")
	}
	if IsValidHandlers(inboundOnly{}, nil) {
		t.Fatal("nil anywhere in the list must be rejected")
	}
}

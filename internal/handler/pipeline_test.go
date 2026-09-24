package handler

import (
	"net"
	"sync"
	"testing"

	"github.com/cuihairu/simplegoserver/pkg/handler"
)

// readRecorder is an inbound handler that logs its name and keeps the
// inbound event propagating towards the tail.
type readRecorder struct {
	name   string
	events *[]string
}

func (r *readRecorder) HandleRead(ctx handler.InboundContext, message handler.Message) {
	*r.events = append(*r.events, r.name)
	ctx.HandleRead(message)
}

// writeRecorder is an outbound handler that logs its name and keeps the
// outbound event propagating towards the head.
type writeRecorder struct {
	name   string
	events *[]string
}

func (r *writeRecorder) HandleWrite(ctx handler.OutboundContext, message handler.Message) {
	*r.events = append(*r.events, r.name)
	ctx.HandleWrite(message)
}

// newTestPipeline wires the pipeline to an in-memory pipe whose peer is
// drained in the background: a FireWrite test that reaches the head
// sentinel writes through to it, and the pipe's synchronous semantics
// would otherwise block that write forever.
func newTestPipeline(t *testing.T) *LinkedPipeline {
	t.Helper()
	p, _ := newTestPipelineWithPeer(t)
	return p
}

// newTestPipelineWithPeer also hands back the far end of the pipe so tests
// can assert on what actually reached the connection. A drain goroutine
// consumes the peer: a FireWrite test that reaches the head sentinel
// writes through to it, and the pipe's synchronous semantics would
// otherwise block that write forever. Tests that read the peer themselves
// should use rawTestPipeline instead.
func newTestPipelineWithPeer(t *testing.T) (*LinkedPipeline, net.Conn) {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() {
		_ = conn.Close()
		_ = peer.Close()
	})
	go func() {
		for {
			buf := make([]byte, 512)
			if _, err := peer.Read(buf); err != nil {
				return
			}
		}
	}()
	return NewPipeline(conn), peer
}

// rawTestPipeline wires a pipeline to a pipe without a drain goroutine:
// the returned peer is the test's to read, including detecting a closed
// connection as EOF.
func rawTestPipeline(t *testing.T) (*LinkedPipeline, net.Conn) {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() {
		_ = conn.Close()
		_ = peer.Close()
	})
	return NewPipeline(conn), peer
}

func TestPipelineIndexLookups(t *testing.T) {
	p := newTestPipeline(t)
	a := &readRecorder{name: "a"}
	b := &readRecorder{name: "b"}
	p.AddLast(a, b)

	// index space: head sentinel = 0, then a, b, tail sentinel
	isB := func(h handler.Handler) bool { return h == handler.Handler(b) }
	if got := p.IndexOf(isB); got != 2 {
		t.Fatalf("IndexOf(b) = %d, want 2", got)
	}
	if got := p.LastIndexOf(isB); got != 2 {
		t.Fatalf("LastIndexOf(b) = %d, want 2", got)
	}
	// regression: LastIndexOf used to walk tail.next (nil) and report -1
	// for every node except the tail sentinel
	isA := func(h handler.Handler) bool { return h == handler.Handler(a) }
	if got := p.LastIndexOf(isA); got != 1 {
		t.Fatalf("LastIndexOf(a) = %d, want 1", got)
	}
	ctxB := p.ContextAt(2)
	if ctxB == nil || ctxB.Handler() != handler.Handler(b) {
		t.Fatalf("ContextAt(2) = %v, want b's context", ctxB)
	}
	if p.ContextAt(p.Size()) != nil || p.ContextAt(-1) != nil {
		t.Fatal("ContextAt out of range must return nil")
	}
}

func TestPipelinePropagationDirections(t *testing.T) {
	p := newTestPipeline(t)
	reads := &[]string{}
	writes := &[]string{}
	in1 := &readRecorder{name: "in1", events: reads}
	in2 := &readRecorder{name: "in2", events: reads}
	out1 := &writeRecorder{name: "out1", events: writes}
	out2 := &writeRecorder{name: "out2", events: writes}
	p.AddLast(in1, out1, in2, out2)

	p.FireRead([]byte("payload"))
	// inbound flows head → tail
	wantReads := []string{"in1", "in2"}
	if len(*reads) != 2 || (*reads)[0] != wantReads[0] || (*reads)[1] != wantReads[1] {
		t.Fatalf("inbound order = %v, want %v", *reads, wantReads)
	}

	p.FireWrite([]byte("payload"))
	// outbound flows tail → head
	wantWrites := []string{"out2", "out1"}
	if len(*writes) != 2 || (*writes)[0] != wantWrites[0] || (*writes)[1] != wantWrites[1] {
		t.Fatalf("outbound order = %v, want %v", *writes, wantWrites)
	}
}

func TestPipelineAddHandlerInsertsAfterPosition(t *testing.T) {
	p := newTestPipeline(t)
	a := &readRecorder{name: "a"}
	b := &readRecorder{name: "b"}
	p.AddLast(a, b)
	x := &readRecorder{name: "x"}
	p.AddHandler(1, x) // inserts after the node at position 1 (a), i.e. between a and b
	isX := func(h handler.Handler) bool { return h == handler.Handler(x) }
	if got := p.IndexOf(isX); got != 2 {
		t.Fatalf("IndexOf(x) = %d, want 2", got)
	}
	if p.Size() != 5 { // head + a + x + b + tail
		t.Fatalf("Size() = %d, want 5", p.Size())
	}
}

func TestNodeContextAttachment(t *testing.T) {
	p := newTestPipeline(t)
	h := &readRecorder{name: "a"}
	p.AddLast(h)
	ctx, ok := p.ContextAt(1).(*NodeContext)
	if !ok {
		t.Fatal("ContextAt(1) is not a *NodeContext")
	}

	ctx.SetAttachment("first")
	if got := ctx.Attachment(); got != "first" {
		t.Fatalf("Attachment() = %v, want first", got)
	}

	// handlers on different goroutines may touch the attachment concurrently
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ctx.SetAttachment(j)
				_ = ctx.Attachment()
			}
		}()
	}
	wg.Wait()
}

// TestPipelineIndexLookupMisses covers the not-found tails of IndexOf and
// LastIndexOf: a predicate nothing matches must fall off the scan and
// report -1 from both directions.
func TestPipelineIndexLookupMisses(t *testing.T) {
	p := newTestPipeline(t)
	p.AddLast(&readRecorder{}, &writeRecorder{})

	if got := p.IndexOf(func(h handler.Handler) bool { return false }); got != -1 {
		t.Fatalf("IndexOf miss = %d, want -1", got)
	}
	if got := p.LastIndexOf(func(h handler.Handler) bool { return false }); got != -1 {
		t.Fatalf("LastIndexOf miss = %d, want -1", got)
	}
}

// TestPipelineContextAtDefensiveBounds drives the mid-scan guard with a
// pipeline whose size disagrees with its chain — a state reachable only
// through misuse, but the guard exists precisely for it.
func TestPipelineContextAtDefensiveBounds(t *testing.T) {
	p := NewPipeline(nil) // sentinels only, no handlers between them
	p.size = 5            // lie about the chain length
	if got := p.ContextAt(3); got != nil {
		t.Fatalf("ContextAt past the real chain = %v, want nil", got)
	}
}

// TestPipelineAddPanicsOnInvalidInput pins the panic contracts: nil
// handlers and out-of-range positions must panic rather than corrupt the
// chain.
func TestPipelineAddPanicsOnInvalidInput(t *testing.T) {
	p := newTestPipeline(t)

	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s did not panic", name)
			}
		}()
		f()
	}
	mustPanic("AddFirst(nil)", func() { p.AddFirst(nil) })
	mustPanic("AddLast(nil)", func() { p.AddLast(nil) })
	mustPanic("AddHandler(nil)", func() { p.AddHandler(0, nil) })
	mustPanic("AddHandler(-1)", func() { p.AddHandler(-1, &readRecorder{}) })
	mustPanic("AddHandler(past end)", func() { p.AddHandler(p.Size()+1, &readRecorder{}) })
}

// TestWithDefaultPipeline covers the package's default initializer: a nil
// pipeline is rejected, a real one gains the error handler as its last
// handler.
func TestWithDefaultPipeline(t *testing.T) {
	if err := WithDefaultPipeline(nil); err == nil {
		t.Fatal("WithDefaultPipeline(nil) must fail")
	}
	p := newTestPipeline(t)
	if err := WithDefaultPipeline(p); err != nil {
		t.Fatalf("WithDefaultPipeline(): %v", err)
	}
	last := p.ContextAt(p.Size() - 2) // tail sentinel sits at Size()-1
	if _, ok := last.Handler().(*ErrorHandler); !ok {
		t.Fatalf("last handler = %T, want *ErrorHandler", last.Handler())
	}
}

// TestNewErrorHandlerSingleton pins the lazy singleton: repeated calls
// return the same instance.
func TestNewErrorHandlerSingleton(t *testing.T) {
	if NewErrorHandler() != NewErrorHandler() {
		t.Fatal("NewErrorHandler() must return the same instance")
	}
}

// panickingWriter blows up inside HandleWrite so NodeContext.Write's
// recover path fires and forwards the panic down the pipeline as an
// exception.
type panickingWriter struct{}

func (panickingWriter) HandleWrite(handler.OutboundContext, handler.Message) {
	panic("boom from outbound")
}

// TestNodeContextWriteRecoversHandlerPanic: a panic inside an outbound
// handler must not escape Write — it becomes a pipeline exception picked
// up by the first inbound handler.
func TestNodeContextWriteRecoversHandlerPanic(t *testing.T) {
	p := newTestPipeline(t)
	var events []string
	guard := &exceptionRecorder{name: "guard", events: &events}
	p.AddLast(guard)                   // [head, guard, tail]
	p.AddHandler(1, panickingWriter{}) // [head, guard, panicW, tail]
	p.AddHandler(2, &readRecorder{})   // [head, guard, panicW, reader, tail]

	reader := p.ContextAt(3)
	reader.Write("hello")

	if len(events) != 1 || events[0] != "guard" {
		t.Fatalf("exception events = %v, want [guard] — the panic must reach the first inbound handler", events)
	}
}

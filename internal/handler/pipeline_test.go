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
	return NewPipeline(conn)
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

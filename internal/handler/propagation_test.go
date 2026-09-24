package handler

import (
	"bytes"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
)

// exceptionRecorder stops the exception event at itself (the single-cast
// propagation contract: the first ExceptionHandler downstream of the head
// gets the event and the walk ends there).
type exceptionRecorder struct {
	name   string
	events *[]string
}

func (e *exceptionRecorder) HandleException(ctx handler.ExceptionContext, ex handler.Exception) {
	*e.events = append(*e.events, e.name)
}

// lifecycleRecorder implements the active/inactive half of the handler
// surface.
type lifecycleRecorder struct {
	name   string
	events *[]string
}

func (l *lifecycleRecorder) HandleActive(ctx handler.ActiveContext) {
	*l.events = append(*l.events, "active:"+l.name)
}

func (l *lifecycleRecorder) HandleInactive(ctx handler.InactiveContext, ex handler.Exception) {
	*l.events = append(*l.events, "inactive:"+l.name)
}

func TestPipelineAddHandlerAtPositionZero(t *testing.T) {
	p := newTestPipeline(t)
	a := &readRecorder{name: "a"}
	p.AddLast(a)
	x := &readRecorder{name: "x"}

	// used to box the []Handler slice into one empty-interface value,
	// which IsValidHandlers rejected — AddHandler(0, ...) always panicked
	p.AddHandler(0, x)

	isX := func(h handler.Handler) bool { return h == handler.Handler(x) }
	if got := p.IndexOf(isX); got != 1 {
		t.Fatalf("IndexOf(x) = %d, want 1 (right after the head sentinel)", got)
	}
	// position 0 is between head and x's old spot; Size counts sentinels
	if p.Size() != 4 { // head + x + a + tail
		t.Fatalf("Size() = %d, want 4", p.Size())
	}
}

func TestPipelineAddHandlerAtTailPosition(t *testing.T) {
	p := newTestPipeline(t)
	a := &readRecorder{name: "a"}
	p.AddLast(a)
	x := &readRecorder{name: "x"}
	p.AddHandler(p.Size(), x) // == AddLast
	isX := func(h handler.Handler) bool { return h == handler.Handler(x) }
	if got := p.LastIndexOf(isX); got != p.Size()-2 {
		t.Fatalf("LastIndexOf(x) = %d, want %d (before the tail sentinel)", got, p.Size()-2)
	}
}

func TestPipelineLifecyclePropagation(t *testing.T) {
	p := newTestPipeline(t)
	events := &[]string{}
	life1 := &lifecycleRecorder{name: "life1", events: events}
	reader := &readRecorder{name: "reader"}
	life2 := &lifecycleRecorder{name: "life2", events: events}
	p.AddLast(life1, reader, life2)

	p.FireActive()
	p.FireInactive(nil)

	// single-cast: the first Active/InactiveHandler after the head wins,
	// handlers that don't implement the interface are skipped, and the
	// walk does not continue past the winner
	want := []string{"active:life1", "inactive:life1"}
	if len(*events) != 2 || (*events)[0] != want[0] || (*events)[1] != want[1] {
		t.Fatalf("lifecycle events = %v, want %v", *events, want)
	}
}

func TestPipelineExceptionStopsAtFirstHandler(t *testing.T) {
	p, peer := rawTestPipeline(t)
	events := &[]string{}
	rec := &exceptionRecorder{name: "guard", events: events}
	p.AddLast(rec)

	p.FireException("boom")

	if len(*events) != 1 || (*events)[0] != "guard" {
		t.Fatalf("exception events = %v, want [guard]", *events)
	}
	// the guard absorbed the event, so the tail sentinel never ran its
	// close-on-unhandled-exception path: the connection must still carry
	// writes end to end. net.Pipe is synchronous — the reader must be
	// running before the write or FireWrite blocks forever.
	got := make(chan string, 1)
	go func() {
		buf := make([]byte, 5)
		_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadFull(peer, buf); err != nil {
			got <- "read-error:" + err.Error()
			return
		}
		got <- string(buf)
	}()
	p.FireWrite([]byte("alive"))
	select {
	case s := <-got:
		if s != "alive" {
			t.Fatalf("peer read %q, want alive", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("write never reached the peer — the connection looks closed")
	}
}

func TestPipelineUnhandledExceptionClosesConnection(t *testing.T) {
	p, peer := rawTestPipeline(t)
	// no ExceptionHandler in the pipeline: the event falls through to the
	// tail sentinel, which logs to stderr and closes the connection
	stderr := captureStderr(t, func() {
		p.FireException("unhandled")
	})
	if !strings.Contains(stderr, "unhandled") {
		t.Fatalf("stderr = %q, want the exception logged", stderr)
	}

	_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the connection to be closed by the tail sentinel")
	}
}

// TestNodeContextWriteFromMiddle pins the outbound starting point: Write
// on a context walks towards the head starting at its previous node, so
// handlers tail-ward of the writer never see the message.
func TestNodeContextWriteFromMiddle(t *testing.T) {
	p := newTestPipeline(t)
	writes := &[]string{}
	out1 := &writeRecorder{name: "out1", events: writes}
	out2 := &writeRecorder{name: "out2", events: writes}
	p.AddLast(out1, out2)

	// context of out2 (position 2: head, out1, out2)
	ctx, ok := p.ContextAt(2).(*NodeContext)
	if !ok {
		t.Fatal("ContextAt(2) is not a *NodeContext")
	}
	ctx.Write([]byte("payload"))

	if len(*writes) != 1 || (*writes)[0] != "out1" {
		t.Fatalf("outbound visits = %v, want [out1] — writing from out2's context must not re-enter out2", *writes)
	}
}

func TestNodeContextCloseClosesConnection(t *testing.T) {
	p, peer := rawTestPipeline(t)
	ctx, ok := p.ContextAt(0).(*NodeContext)
	if !ok {
		t.Fatal("ContextAt(0) is not a *NodeContext")
	}
	ctx.Close(nil)
	_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the connection to be closed by ctx.Close")
	}
}

// TestHeadHandlerWriteMessageTypes covers every branch of the head
// sentinel's write fan-out; a peer read-back verifies the bytes really
// reached the connection.
type writerToMessage struct{ body []byte }

func (w writerToMessage) WriteTo(dst io.Writer) (int64, error) {
	n, err := dst.Write(w.body)
	return int64(n), err
}

type plainReader struct{ io.Reader }

func TestHeadHandlerWriteMessageTypes(t *testing.T) {
	conn, peer := net.Pipe()
	t.Cleanup(func() {
		_ = conn.Close()
		_ = peer.Close()
	})
	p := NewPipeline(conn)

	// startRead arms the peer reader before the write: net.Pipe writes
	// block until the far end consumes them, so reading after FireWrite
	// would deadlock
	startRead := func(n int) <-chan string {
		ch := make(chan string, 1)
		go func() {
			buf := make([]byte, n)
			_ = peer.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := io.ReadFull(peer, buf); err != nil {
				ch <- "read-error:" + err.Error()
				return
			}
			ch <- string(buf)
		}()
		return ch
	}
	writeAndExpect := func(t *testing.T, msg handler.Message, n int, want string) {
		t.Helper()
		ch := startRead(n)
		p.FireWrite(msg)
		select {
		case got := <-ch:
			if got != want {
				t.Fatalf("peer read %q, want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("write %q never reached the peer", want)
		}
	}

	// []byte
	writeAndExpect(t, []byte("raw"), 3, "raw")

	// [][]byte: coalesced in order
	writeAndExpect(t, [][]byte{[]byte("a"), []byte("b"), []byte("c")}, 3, "abc")

	// Buffer (bytes.Buffer has WriteTo too, but the Buffer case matches first)
	writeAndExpect(t, bytes.NewBufferString("buf"), 3, "buf")

	// io.WriterTo (without Bytes(), so the Buffer case can't shadow it)
	writeAndExpect(t, writerToMessage{body: []byte("wto")}, 3, "wto")

	// io.Reader without WriteTo: plainReader strips the interface
	writeAndExpect(t, plainReader{Reader: strings.NewReader("rdr")}, 3, "rdr")

	// unsupported type must panic out of the head sentinel
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("writing an unsupported message type must panic")
		}
	}()
	p.FireWrite(42)
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	fn()
	os.Stderr = old
	_ = w.Close()
	out, _ := io.ReadAll(r)
	_ = r.Close()
	return string(out)
}

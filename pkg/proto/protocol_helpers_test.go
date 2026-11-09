package proto

import (
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg/handler"
)

type testError string

func (e testError) Error() string { return string(e) }

func echoHandler(action string, data []byte) (any, error) {
	if action == "fail" {
		return nil, testError("intentional failure")
	}
	// echo the raw JSON payload back untouched
	return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
}

// pipeServer wires a ProtocolHandler into a real pipeline over one half of
// a net.Pipe. Frames written to the returned conn are decoded and served;
// responses can be read back from the same conn.
func pipeServer(t *testing.T, requestHandler RequestHandler) (ph *ProtocolHandler, conn net.Conn) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	ph = NewProtocolHandler(requestHandler)
	pipeline := handlerImpl.NewPipeline(serverSide)
	_ = pipeline.AddLast(NewFrameCodec(), ph)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			pipeline.FireRead(serverSide)
			if _, err := serverSide.Read(make([]byte, 0)); err != nil {
				return // pipe closed, stop serving
			}
		}
	}()
	t.Cleanup(func() {
		ph.Close() // stop the session janitor
		_ = serverSide.Close()
		_ = clientSide.Close()
		<-done
	})
	return ph, clientSide
}

func writeFrame(t *testing.T, conn net.Conn, frame *Frame) {
	t.Helper()
	buf, err := Encode(frame)
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(buf); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readFrame(t *testing.T, conn net.Conn) *Frame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := Decode(conn)
	if err != nil {
		t.Fatalf("Decode(): %v", err)
	}
	return frame
}

type inboundContextStub struct {
	conn net.Conn
}

func (s *inboundContextStub) Conn() net.Conn                   { return s.conn }
func (s *inboundContextStub) Handler() handler.Handler         { return nil }
func (s *inboundContextStub) Write(m handler.Message)          {}
func (s *inboundContextStub) Close(error)                      { _ = s.conn.Close() }
func (s *inboundContextStub) Attachment() handler.Attachment   { return nil }
func (s *inboundContextStub) SetAttachment(handler.Attachment) {}
func (s *inboundContextStub) HandleRead(handler.Message)       {}

// startTCPServer runs a real TCP server speaking the protocol, mirroring
// what the reactor does per connection.
func startTCPServer(t *testing.T, requestHandler RequestHandler) (addr string, ph *ProtocolHandler) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ph = NewProtocolHandler(requestHandler)
	t.Cleanup(func() {
		ph.Close() // stop the session janitor
		_ = listener.Close()
	})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveConn(conn, ph)
		}
	}()
	return listener.Addr().String(), ph
}

func serveConn(conn net.Conn, ph *ProtocolHandler) {
	tracked := &trackingConn{Conn: conn}
	pipeline := handlerImpl.NewPipeline(tracked)
	_ = pipeline.AddLast(NewFrameCodec(), ph)
	for {
		pipeline.FireRead(tracked)
		if tracked.closed.Load() {
			return
		}
	}
}

type trackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// collectPublishes drains pushed publish frames (action, sequence) until
// the deadline, for replay assertions.
func collectPublishes(pushes <-chan *Frame, n int, timeout time.Duration) ([]uint32, bool) {
	var seqs []uint32
	deadline := time.After(timeout)
	for len(seqs) < n {
		select {
		case frame := <-pushes:
			seqs = append(seqs, frame.Header.StreamId)
		case <-deadline:
			return seqs, false
		}
	}
	return seqs, true
}

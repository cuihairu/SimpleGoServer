package proto

import (
	"encoding/json"
	"net"
	"sync"
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

func TestProtocolHandlerRequestResponse(t *testing.T) {
	_, conn := pipeServer(t, echoHandler)

	frame, err := EncodeJSON(REQUEST, 42, "echo", "hello")
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	writeFrame(t, conn, frame)

	resp := readFrame(t, conn)
	if resp.Header.FrameType != RESPONSE {
		t.Fatalf("frame type = %s, want RESPONSE", resp.Header.FrameType)
	}
	if resp.Header.StreamId != 42 {
		t.Fatalf("stream id = %d, want 42 (request correlation)", resp.Header.StreamId)
	}
	// responses carry the JSONMessage envelope
	env, err := DecodeJSONMessage(resp)
	if err != nil {
		t.Fatalf("DecodeJSONMessage(): %v", err)
	}
	if env.Action != "echo" {
		t.Fatalf("envelope action = %q, want echo", env.Action)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(env.Data, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	var echoed string
	if err := json.Unmarshal(body["data"], &echoed); err != nil {
		t.Fatalf("echoed data: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("echo = %q, want hello", echoed)
	}
}

func TestProtocolHandlerErrorResponse(t *testing.T) {
	_, conn := pipeServer(t, echoHandler)

	frame, _ := EncodeJSON(REQUEST, 7, "fail", nil)
	writeFrame(t, conn, frame)

	resp := readFrame(t, conn)
	msg, err := DecodeJSONMessage(resp)
	if err != nil {
		t.Fatalf("DecodeJSONMessage(): %v", err)
	}
	var errMsg ErrorMessage
	if err := json.Unmarshal(msg.Data, &errMsg); err != nil {
		t.Fatalf("error payload: %v", err)
	}
	if errMsg.Error != "intentional failure" {
		t.Fatalf("error = %q", errMsg.Error)
	}
	// the connection must survive a failed request
	frame2, _ := EncodeJSON(REQUEST, 8, "echo", "still-alive")
	writeFrame(t, conn, frame2)
	resp2 := readFrame(t, conn)
	if resp2.Header.StreamId != 8 {
		t.Fatalf("stream id after error = %d, connection should stay open", resp2.Header.StreamId)
	}
}

func TestProtocolHandlerPingPong(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)

	frame, _ := EncodeJSON(PING, 9, "ping", nil)
	writeFrame(t, conn, frame)

	resp := readFrame(t, conn)
	if resp.Header.FrameType != PONG || resp.Header.StreamId != 9 {
		t.Fatalf("got %s stream %d, want PONG stream 9", resp.Header.FrameType, resp.Header.StreamId)
	}
	pings, _ := ph.Stats()
	if pings != 1 {
		t.Fatalf("stats pings=%d, want 1", pings)
	}
}

func TestProtocolHandlerSubscribeAndPublish(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)

	sub, _ := EncodeJSON(SUBSCRIBE, 10, "news", nil)
	writeFrame(t, conn, sub)
	ack := readFrame(t, conn)
	if ack.Header.FrameType != RESPONSE {
		t.Fatalf("subscribe ack type = %s", ack.Header.FrameType)
	}
	if ph.Subscribers("news") != 1 {
		t.Fatalf("subscribers = %d, want 1", ph.Subscribers("news"))
	}

	// reads must run concurrently with Publish: a synchronous pipe write
	// would otherwise block until a reader shows up
	type pushResult struct {
		frame *Frame
		err   error
	}
	pushCh := make(chan pushResult, 1)
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		frame, err := Decode(conn)
		pushCh <- pushResult{frame, err}
	}()
	delivered, err := ph.Publish("news", map[string]string{"headline": "go"})
	if err != nil || delivered != 1 {
		t.Fatalf("Publish() = %d, %v; want 1, nil", delivered, err)
	}
	select {
	case got := <-pushCh:
		if got.err != nil {
			t.Fatalf("Decode push: %v", got.err)
		}
		if got.frame.Header.FrameType != PUBLISH {
			t.Fatalf("push type = %s, want PUBLISH", got.frame.Header.FrameType)
		}
		msg, _ := DecodeJSONMessage(got.frame)
		if msg.Action != "news" {
			t.Fatalf("push topic = %q, want news", msg.Action)
		}
		var body map[string]string
		if err := json.Unmarshal(msg.Data, &body); err != nil || body["headline"] != "go" {
			t.Fatalf("push body = %s (%v)", msg.Data, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no push received")
	}
	_ = conn.SetReadDeadline(time.Time{})

	// unsubscribe stops delivery
	unsub, _ := EncodeJSON(UNSUBSCRIBE, 11, "news", nil)
	writeFrame(t, conn, unsub)
	readFrame(t, conn) // drain ack
	if ph.Subscribers("news") != 0 {
		t.Fatalf("subscribers after unsubscribe = %d", ph.Subscribers("news"))
	}
	if delivered, err := ph.Publish("news", "nobody listens"); err != nil || delivered != 0 {
		t.Fatalf("Publish without subscribers = %d, %v", delivered, err)
	}
}

func TestProtocolHandlerCloseFrame(t *testing.T) {
	_, conn := pipeServer(t, echoHandler)

	frame, _ := EncodeJSON(CLOSE, 12, "bye", nil)
	writeFrame(t, conn, frame)

	ack := readFrame(t, conn)
	if ack.Header.FrameType != RESPONSE {
		t.Fatalf("close ack type = %s, want RESPONSE", ack.Header.FrameType)
	}
	// server closes its side; a read must eventually fail
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 16)); err == nil {
		t.Fatal("expected read error after CLOSE handshake")
	}
}

func TestProtocolHandlerRejectsNonFrameMessage(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)
	// hand the protocol handler a message that is not a *Frame; it must
	// close the connection instead of panicking
	ph.HandleRead(&inboundContextStub{conn: conn}, "not a frame")
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 8)); err == nil {
		t.Fatal("expected connection to close after protocol violation")
	}
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

func TestProtocolHandlerPublishDropsBrokenSubscriber(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)
	sub, _ := EncodeJSON(SUBSCRIBE, 1, "t2", nil)
	writeFrame(t, conn, sub)
	readFrame(t, conn)

	// kill the subscriber; the next Publish must prune it, not wedge
	_ = conn.Close()
	time.Sleep(50 * time.Millisecond)

	delivered, err := ph.Publish("t2", "hello?")
	if err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	if delivered != 0 {
		t.Fatalf("delivered = %d on dead subscriber, want 0", delivered)
	}
	if got := ph.Subscribers("t2"); got != 0 {
		t.Fatalf("subscribers = %d after failed publish, want 0", got)
	}
}

// startTCPServer runs a real TCP server speaking the protocol, mirroring
// what the reactor does per connection.
func startTCPServer(t *testing.T, requestHandler RequestHandler) (addr string, ph *ProtocolHandler) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ph = NewProtocolHandler(requestHandler)
	t.Cleanup(func() { _ = listener.Close() })

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

func TestClientEndToEndConcurrentCalls(t *testing.T) {
	addr, _ := startTCPServer(t, echoHandler)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	const goroutines = 8
	const calls = 25
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < calls; i++ {
				want := "hello"
				resp, err := client.Call("echo", want, 5*time.Second)
				if err != nil {
					errs <- err
					return
				}
				var body struct {
					Action string          `json:"action"`
					Data   json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(resp.Data, &body); err != nil {
					errs <- err
					return
				}
				var echoed string
				if err := json.Unmarshal(body.Data, &echoed); err != nil {
					errs <- err
					return
				}
				if echoed != want {
					errs <- testError("cross-wired response: " + echoed)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestClientSubscribeReceivesPublish(t *testing.T) {
	addr, ph := startTCPServer(t, echoHandler)

	pushes := make(chan *Frame, 8)
	client, err := Dial(addr, func(frame *Frame) { pushes <- frame })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	if err := client.Subscribe("alerts", 5*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	if _, err := ph.Publish("alerts", map[string]string{"n": "1"}); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	select {
	case frame := <-pushes:
		if frame.Header.FrameType != PUBLISH {
			t.Fatalf("event type = %s, want PUBLISH", frame.Header.FrameType)
		}
		msg, _ := DecodeJSONMessage(frame)
		if msg.Action != "alerts" {
			t.Fatalf("event topic = %q, want alerts", msg.Action)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no publish received")
	}

	if err := client.Unsubscribe("alerts", 5*time.Second); err != nil {
		t.Fatalf("Unsubscribe(): %v", err)
	}
	_, _ = ph.Publish("alerts", "gone")
	select {
	case frame := <-pushes:
		t.Fatalf("unexpected push after unsubscribe: %s", frame)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestClientCallTimeout(t *testing.T) {
	slow := func(action string, data []byte) (any, error) {
		time.Sleep(300 * time.Millisecond)
		return "done", nil
	}
	addr, _ := startTCPServer(t, slow)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	if _, err := client.Call("echo", nil, 50*time.Millisecond); err == nil {
		t.Fatal("expected timeout error")
	}
	// a late response must not corrupt the next call's correlation
	resp, err := client.Call("echo", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("call after timeout: %v", err)
	}
	if string(resp.Data) != `"done"` {
		t.Fatalf("data = %s", resp.Data)
	}
}

func TestClientPendingFailOnDisconnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	client, err := Dial(listener.Addr().String(), nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	serverSide := <-accepted
	// hold a call open, then kill the server side
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Call("echo", nil, 10*time.Second)
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	_ = serverSide.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after disconnect")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call did not fail after disconnect")
	}
}

func TestClientPingAndGracefulClose(t *testing.T) {
	addr, _ := startTCPServer(t, echoHandler)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()
	if err := client.Ping(5 * time.Second); err != nil {
		t.Fatalf("Ping(): %v", err)
	}
	if err := client.CloseGracefully(5 * time.Second); err != nil {
		t.Fatalf("CloseGracefully(): %v", err)
	}
	// idempotent
	if err := client.CloseGracefully(time.Second); err != nil {
		t.Fatalf("second CloseGracefully(): %v", err)
	}
}

func TestClientCallAfterClose(t *testing.T) {
	addr, _ := startTCPServer(t, echoHandler)
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if _, err := client.Call("echo", nil, time.Second); err != ErrClosed {
		t.Fatalf("Call after close = %v, want ErrClosed", err)
	}
}

package proto

import (
	"encoding/json"
	"net"
	"strings"
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

func TestNegotiate(t *testing.T) {
	cases := []struct {
		name           string
		clientVersions []int
		want           int
	}{
		{"exact match", []int{ProtocolVersion}, ProtocolVersion},
		{"highest common wins", []int{1, 3, 7}, 1}, // server below speaks {1}
		{"no overlap", []int{2, 3}, 0},
		{"empty offer", nil, 0},
		{"duplicates", []int{1, 1}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := negotiate(tc.clientVersions); got != tc.want {
				t.Fatalf("negotiate(%v) = %d, want %d", tc.clientVersions, got, tc.want)
			}
		})
	}
}

// TestServerHelloPaths drives the server's HELLO handling over a real TCP
// connection: a supported version is confirmed with the chosen version and
// feature list, while an unsupported or empty offer gets an error response
// the client is expected to react to by disconnecting.
func TestServerHelloPaths(t *testing.T) {
	addr, _ := startTCPServer(t, nil)

	// supported versions -> ack with the common one. Each path below uses
	// its own connection: a connection handshakes exactly once.
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	resp, err := client.roundTrip(HELLO, RESPONSE, "hello", &HelloRequest{Versions: []int{3, ProtocolVersion, 2}}, 2*time.Second)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	if !resp.OK() {
		t.Fatalf("HELLO with supported version rejected: %s", resp.Err.Error)
	}
	var agreed HelloResponse
	if err := json.Unmarshal(resp.Data, &agreed); err != nil {
		t.Fatalf("decode HELLO ack: %v", err)
	}
	if agreed.Version != ProtocolVersion {
		t.Fatalf("negotiated version = %d, want %d", agreed.Version, ProtocolVersion)
	}
	if len(agreed.Features) == 0 {
		t.Fatal("HELLO ack carried no features")
	}
	_ = client.Close()

	// unsupported offer -> structured error, connection still open
	reject, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer reject.Close()
	resp, err = reject.roundTrip(HELLO, RESPONSE, "hello", &HelloRequest{Versions: []int{42}}, 2*time.Second)
	if err != nil {
		t.Fatalf("HELLO (unsupported): %v", err)
	}
	if resp.OK() {
		t.Fatal("HELLO with unsupported version unexpectedly accepted")
	}

	// empty offer -> same error path, again on a fresh connection
	empty, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer empty.Close()
	resp, err = empty.roundTrip(HELLO, RESPONSE, "hello", &HelloRequest{}, 2*time.Second)
	if err != nil {
		t.Fatalf("HELLO (empty): %v", err)
	}
	if resp.OK() {
		t.Fatal("HELLO with empty version list unexpectedly accepted")
	}
}

// TestClientResumeRestoresSubscriptions covers the reconnect flow end to
// end: a session drops after subscribing, the client reconnects presenting
// the token, and the subscription is live again — pushes reach the new
// connection without an explicit re-subscribe.
func TestClientResumeRestoresSubscriptions(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	c1, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if res1.Resumed {
		t.Fatal("first handshake unexpectedly resumed a session")
	}
	if res1.Token == "" {
		t.Fatal("handshake returned no session token")
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	_ = c1.Close() // the drop

	pushes := make(chan *Frame, 1)
	c2, err := Dial(addr, func(frame *Frame) { pushes <- frame })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume with a live session token was not recognized")
	}
	if res2.Token != res1.Token {
		t.Fatalf("resume changed the token: got %q, want %q", res2.Token, res1.Token)
	}
	if got := ph.Subscribers("ticks"); got != 1 {
		t.Fatalf("subscribers after resume = %d, want 1 (the dead transport must be gone)", got)
	}

	if _, err := ph.Publish("ticks", map[string]string{"hello": "again"}); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	select {
	case frame := <-pushes:
		msg, err := DecodeJSONMessage(frame)
		if err != nil || msg.Action != "ticks" {
			t.Fatalf("unexpected push %v (err=%v)", frame, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no push after resume — subscription was not restored")
	}
}

// TestClientResumeUnknownTokenStartsFresh checks that a bogus token is not
// an error: the client just gets a new session and subscribes normally.
func TestClientResumeUnknownTokenStartsFresh(t *testing.T) {
	addr, _ := startTCPServer(t, nil)

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()
	res, err := client.HandshakeWith("no-such-token", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("HandshakeWith(): %v", err)
	}
	if res.Resumed {
		t.Fatal("unknown token was reported as resumed")
	}
	if res.Token == "" || res.Token == "no-such-token" {
		t.Fatalf("fresh session token = %q, want a new one", res.Token)
	}
}

// TestClientResumeExpiredSessionIsFresh shrinks the session TTL so the
// reconnect arrives after expiry: the token is not honored, mirroring an
// abandoned session.
func TestClientResumeExpiredSessionIsFresh(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	c1, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	_ = c1.Close()

	ph.mu.Lock()
	ph.sessionTTL = -time.Second // every resume attempt is now "too late"
	ph.mu.Unlock()

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer client.Close()
	res2, err := client.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if res2.Resumed {
		t.Fatal("expired session was resumed")
	}
}

// TestActiveSessionOutlivesTTL pins what keeps a session alive: inbound
// frames refresh its timestamp, so a connection that only ever heartbeats
// (KeepAlive) stays resumable no matter how long it has been connected —
// while a silent session still ages out at the TTL. Without the refresh,
// the janitor would expire the session of a live connection and its later
// resume would silently start from scratch, dropping the subscriptions.
func TestActiveSessionOutlivesTTL(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.sessionTTL = 200 * time.Millisecond
	ph.mu.Unlock()

	// active group: keep pinging well past the TTL, then resume
	c1, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	for i := 0; i < 8; i++ {
		time.Sleep(50 * time.Millisecond)
		if err := c1.Ping(2 * time.Second); err != nil {
			t.Fatalf("Ping %d: %v", i, err)
		}
	}
	_ = c1.Close()

	c2, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("a heartbeat-alive session was expired by the server")
	}

	// silent group: same TTL passes with no traffic, resume must fail
	c3, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res3, err := c3.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	_ = c3.Close()
	time.Sleep(300 * time.Millisecond)

	c4, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c4.Close()
	res4, err := c4.HandshakeWith(res3.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if res4.Resumed {
		t.Fatal("a silent session outlived the TTL")
	}
}

// TestDroppedSubscriberRevivesOnResume verifies the accounting behind
// resume: a write failure drops the transport from the subscription table
// (TCP semantics force repeated publishes until the server notices), but
// the session keeps the subscription intent, so reconnecting restores it
// — the client wanted the topic and never said otherwise.
func TestDroppedSubscriberRevivesOnResume(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	c1, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	_ = c1.Close() // dead transport
	// TCP semantics: the first write to a closed peer still succeeds (the
	// RST has not come back yet), so keep publishing until the server has
	// observed the write failure and dropped the subscriber
	deadline := time.Now().Add(2 * time.Second)
	for ph.Subscribers("ticks") > 0 && time.Now().Before(deadline) {
		_, _ = ph.Publish("ticks", "x")
		time.Sleep(10 * time.Millisecond)
	}
	if got := ph.Subscribers("ticks"); got != 0 {
		t.Fatalf("subscribers = %d after the transport died, want 0", got)
	}

	c2, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume not recognized")
	}
	if got := ph.Subscribers("ticks"); got != 1 {
		t.Fatalf("subscribers after resume = %d, want 1 — the session kept the intent", got)
	}
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

// TestResumeReplaysMissedPublishes covers the offline catch-up: publishes
// that happened while the client was disconnected are replayed in order
// right after the resume handshake, ahead of live traffic.
func TestResumeReplaysMissedPublishes(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes1 := make(chan *Frame, 8)
	c1, err := Dial(addr, func(f *Frame) { pushes1 <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	// publish #1 while connected: the client sees it live
	if _, err := ph.Publish("ticks", "first"); err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	live := <-pushes1
	cursor := uint64(live.Header.StreamId)

	// the drop; publishes #2 and #3 happen while nobody is listening
	_ = c1.Close()
	if _, err := ph.Publish("ticks", "second"); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if _, err := ph.Publish("ticks", "third"); err != nil {
		t.Fatalf("Publish 3: %v", err)
	}

	// reconnect presenting the token and the cursor of publish #1
	pushes2 := make(chan *Frame, 8)
	c2, err := Dial(addr, func(f *Frame) { pushes2 <- f })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, map[string]uint64{"ticks": cursor}, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume not recognized")
	}

	// the two missed publishes arrive as replay...
	seqs, ok := collectPublishes(pushes2, 2, 2*time.Second)
	if !ok {
		t.Fatalf("expected 2 replayed publishes, got %d (seqs=%v)", len(seqs), seqs)
	}
	if seqs[0] <= uint32(cursor) || seqs[1] <= seqs[0] {
		t.Fatalf("replay out of order or behind cursor: cursor=%d seqs=%v", cursor, seqs)
	}
	// ...and a live publish afterwards keeps the sequence going
	if _, err := ph.Publish("ticks", "fourth"); err != nil {
		t.Fatalf("Publish 4: %v", err)
	}
	later, ok := collectPublishes(pushes2, 1, 2*time.Second)
	if !ok {
		t.Fatal("no live publish after replay")
	}
	if later[0] <= seqs[1] {
		t.Fatalf("live publish seq %d not after replay %d", later[0], seqs[1])
	}
}

// TestResumeWithFreshCursorReplaysNothing checks that a client reporting
// it has seen everything is not spammed with duplicates.
func TestResumeWithFreshCursorReplaysNothing(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes1 := make(chan *Frame, 8)
	c1, err := Dial(addr, func(f *Frame) { pushes1 <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	if _, err := ph.Publish("ticks", "seen"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	seen := <-pushes1
	_ = c1.Close()

	// cursor at the last seen sequence: nothing to replay
	pushes2 := make(chan *Frame, 8)
	c2, err := Dial(addr, func(f *Frame) { pushes2 <- f })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	if _, err := c2.HandshakeWith(res1.Token, map[string]uint64{"ticks": uint64(seen.Header.StreamId)}, 2*time.Second); err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	select {
	case frame := <-pushes2:
		t.Fatalf("unexpected replay of seq %d the client already saw", frame.Header.StreamId)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestPublishCachesWhileNoSubscriberConnected pins the offline-cache
// invariant: once the dead transport is reaped and the subscriber set is
// empty, publishes keep being cached while a session still records the
// topic — otherwise everything after the drop would be silently lost
// instead of replayed on resume.
func TestPublishCachesWhileNoSubscriberConnected(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes1 := make(chan *Frame, 8)
	c1, err := Dial(addr, func(f *Frame) { pushes1 <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	if _, err := ph.Publish("ticks", "live"); err != nil {
		t.Fatalf("Publish live: %v", err)
	}
	cursor := uint64((<-pushes1).Header.StreamId)

	// drop; keep publishing until the server has observed the write
	// failure and reaped the dead subscriber (see the TCP note above)
	_ = c1.Close()
	deadline := time.Now().Add(2 * time.Second)
	for ph.Subscribers("ticks") > 0 && time.Now().Before(deadline) {
		_, _ = ph.Publish("ticks", "x")
		time.Sleep(10 * time.Millisecond)
	}
	if got := ph.Subscribers("ticks"); got != 0 {
		t.Fatalf("subscribers = %d after the transport died, want 0", got)
	}

	// nobody connected from here on, yet both publishes must be cached
	if _, err := ph.Publish("ticks", "offline 1"); err != nil {
		t.Fatalf("Publish offline 1: %v", err)
	}
	if _, err := ph.Publish("ticks", "offline 2"); err != nil {
		t.Fatalf("Publish offline 2: %v", err)
	}

	pushes2 := make(chan *Frame, 8)
	c2, err := Dial(addr, func(f *Frame) { pushes2 <- f })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, map[string]uint64{"ticks": cursor}, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume not recognized")
	}
	seqs, ok := collectPublishes(pushes2, 2, 2*time.Second)
	if !ok {
		t.Fatalf("expected 2 replayed publishes with no subscriber connected, got %d (seqs=%v)", len(seqs), seqs)
	}
}

// TestTopicCacheEviction pins the cache bound: publishing past the cache
// size keeps only the most recent entries.
func TestTopicCacheEviction(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	pushes := make(chan *Frame, 1)
	c, err := Dial(addr, func(f *Frame) { pushes <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	if err := c.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	// drain live pushes so the channel never blocks Publish
	go func() {
		for range pushes {
		}
	}()

	total := topicCacheSize + 10
	for i := 0; i < total; i++ {
		if _, err := ph.Publish("ticks", i); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	ph.mu.RLock()
	cache := ph.topicCache["ticks"]
	ph.mu.RUnlock()
	if len(cache) != topicCacheSize {
		t.Fatalf("cache holds %d entries, want %d", len(cache), topicCacheSize)
	}
	// the retained entries are the most recent ones, in order
	for i := 1; i < len(cache); i++ {
		if cache[i].seq <= cache[i-1].seq {
			t.Fatalf("cache not monotonic at %d: %v", i, cache)
		}
	}
	if cache[len(cache)-1].seq != uint32(total) {
		t.Fatalf("newest cached seq = %d, want %d", cache[len(cache)-1].seq, total)
	}
}

// TestTopicCacheGlobalBudget pins the memory bound of the replay cache:
// the per-topic entry cap does not bound memory (topics are unbounded, a
// cached frame can be up to MaxFrameSize), so a global byte budget evicts
// whole topics largest-first under pressure. Bookkeeping must stay exact —
// cacheBytes has to equal the sum of what survives.
func TestTopicCacheGlobalBudget(t *testing.T) {
	const budget = 4 * 1024
	addr, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.cacheBudget = budget
	ph.mu.Unlock()

	// sessions must remember the topics, or Publish would skip the cache
	// with nobody to ever ask for the replay
	for _, topic := range []string{"a", "b"} {
		c, err := Dial(addr, nil)
		if err != nil {
			t.Fatalf("Dial(%s): %v", topic, err)
		}
		defer c.Close()
		if _, err := c.Handshake(2 * time.Second); err != nil {
			t.Fatalf("Handshake(): %v", err)
		}
		if err := c.Subscribe(topic, 2*time.Second); err != nil {
			t.Fatalf("Subscribe(%s): %v", topic, err)
		}
	}

	payload := strings.Repeat("x", 900) // ~1KiB per encoded frame
	for i := 0; i < 10; i++ {
		if _, err := ph.Publish("a", payload); err != nil {
			t.Fatalf("Publish a/%d: %v", i, err)
		}
		if _, err := ph.Publish("b", payload); err != nil {
			t.Fatalf("Publish b/%d: %v", i, err)
		}
	}

	ph.mu.RLock()
	held := ph.cacheBytes
	kept := make(map[string]int, len(ph.topicCache))
	for topic, cache := range ph.topicCache {
		kept[topic] = frameBytes(cache)
	}
	ph.mu.RUnlock()

	if held > budget {
		t.Fatalf("cache holds %d bytes, budget is %d", held, budget)
	}
	total := 0
	for _, size := range kept {
		total += size
	}
	if total != held {
		t.Fatalf("bookkeeping drift: cacheBytes=%d, surviving caches sum to %d (%v)", held, total, kept)
	}
	// 20 publishes at ~1KiB each amount to ~20KiB — roughly 5x the budget.
	// Landing at or under the budget is itself the proof that evictions
	// fired; without enforcement `held` would sit near 20KiB.
	if held < budget/2 {
		t.Fatalf("cache at %d bytes looks over-evicted for budget %d — bookkeeping likely drifts low", held, budget)
	}
}

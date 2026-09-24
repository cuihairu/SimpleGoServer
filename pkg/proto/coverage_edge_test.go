package proto

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
)

// ---- wire helpers -------------------------------------------------------

// wireEnvelope encodes a JSONMessage envelope frame to its wire bytes.
func wireEnvelope(t *testing.T, ft FrameType, id uint32, action string, data any) []byte {
	t.Helper()
	frame, err := EncodeJSON(ft, id, action, data)
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	buf, err := Encode(frame)
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	return buf
}

// wireRaw encodes a frame with an arbitrary payload to its wire bytes.
func wireRaw(t *testing.T, ft FrameType, id uint32, payload []byte) []byte {
	t.Helper()
	buf, err := Encode(newFrame(ft, id, payload))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	return buf
}

// rawScriptServer accepts connections and feeds every decoded inbound frame
// to respond; the returned bytes (a complete encoded frame) go back on the
// wire verbatim. Returning nil drops the frame without a reply. The kill
// function closes every connection the server accepted.
func rawScriptServer(t *testing.T, respond func(*Frame) []byte) (addr string, kill func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go func(conn net.Conn) {
				for {
					frame, err := DecodeStreamed(conn)
					if err != nil {
						_ = conn.Close()
						return
					}
					if out := respond(frame); out != nil {
						if _, err := conn.Write(out); err != nil {
							_ = conn.Close()
							return
						}
					}
				}
			}(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	}
}

// silentServer accepts connections but never reads or writes them, so
// client writes land in the socket buffer and every reply wait runs into
// its timeout.
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// sendRawExpectClosed dials, writes pre-encoded frames and waits for the
// server to close the connection on us.
func sendRawExpectClosed(t *testing.T, addr string, frames ...[]byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	for _, f := range frames {
		if _, err := conn.Write(f); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	for {
		if _, err := conn.Read(buf); err != nil {
			return // any read error means the server closed on us
		}
	}
}

// ---- codec / json / stream ----------------------------------------------

func TestDecodeHeaderRejectsShortInput(t *testing.T) {
	if _, err := DecodeHeader(make([]byte, HeaderSize-1)); err == nil {
		t.Fatal("DecodeHeader must reject input shorter than a header")
	}
}

func TestDecodeRejectsImpossibleLength(t *testing.T) {
	buf := make([]byte, HeaderSize)
	buf[0] = byte(REQUEST)
	buf[6], buf[7], buf[8], buf[9] = 0xFF, 0xFF, 0xFF, 0xFF // length = -1
	if _, err := DecodeHeader(buf); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("DecodeHeader(negative length) = %v, want ErrFrameTooLarge", err)
	}
	if _, err := Decode(bytes.NewReader(buf)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Decode(negative length) = %v, want ErrFrameTooLarge", err)
	}
}

func TestDecodeEOFContracts(t *testing.T) {
	full := wireRaw(t, PING, 1, []byte("abcd"))

	// a stream ending mid-header is a clean end between frames: io.EOF
	if _, err := Decode(bytes.NewReader(full[:5])); !errors.Is(err, io.EOF) {
		t.Fatalf("mid-header end = %v, want io.EOF", err)
	}
	// header complete but zero payload bytes before EOF: truncated, not clean
	if _, err := Decode(bytes.NewReader(full[:HeaderSize])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("zero payload bytes = %v, want ErrUnexpectedEOF", err)
	}
	// payload cut in half: truncated as well
	if _, err := Decode(bytes.NewReader(full[:HeaderSize+2])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("partial payload = %v, want ErrUnexpectedEOF", err)
	}
}

func TestJSONGuards(t *testing.T) {
	if _, err := DecodeJSONMessage(nil); err == nil {
		t.Fatal("DecodeJSONMessage(nil) must fail")
	}
	if err := DecodeJSONData(nil, new(string)); err == nil {
		t.Fatal("DecodeJSONData(nil) must fail")
	}
	frame, err := EncodeJSON(REQUEST, 1, "x", map[string]any{"k": 1})
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	var out string
	if err := DecodeJSONData(frame, &out); err == nil {
		t.Fatal("decoding a JSON object into a string must fail")
	}
}

func TestDecodeStreamedRejectsBrokenFirstFrame(t *testing.T) {
	if _, err := DecodeStreamed(bytes.NewReader([]byte{0x01})); !errors.Is(err, io.EOF) {
		t.Fatalf("broken first frame = %v, want io.EOF", err)
	}
}

func TestDecodeStreamedRejectsFragmentStreamMismatch(t *testing.T) {
	first := wireRaw(t, REQUEST, 1, []byte("a"))
	first[1] |= FlagMore
	second := wireRaw(t, REQUEST, 2, []byte("b")) // wrong stream id
	_, err := DecodeStreamed(bytes.NewReader(append(first, second...)))
	if err == nil || !strings.Contains(err.Error(), "stream id") {
		t.Fatalf("mismatched fragment stream = %v, want a stream violation", err)
	}

	third := wireRaw(t, RESPONSE, 1, []byte("b")) // wrong frame type
	_, err = DecodeStreamed(bytes.NewReader(append(first, third...)))
	if err == nil || !strings.Contains(err.Error(), "fragment type") {
		t.Fatalf("mismatched fragment type = %v, want a stream violation", err)
	}

	// a stream that dies mid-assembly surfaces the read error
	_, err = DecodeStreamed(bytes.NewReader(append(first, 0x01)))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("stream cut mid-assembly = %v, want io.EOF", err)
	}
}

func TestEncodeStreamFramesRejectsOversizeOutput(t *testing.T) {
	// a threshold above MaxFrameSize lets an oversize payload slip past the
	// split, so Encode itself must refuse the single frame
	if _, err := encodeStreamFrames(REQUEST, 1, make([]byte, MaxFrameSize+1), MaxFrameSize+10); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversize single frame = %v, want ErrFrameTooLarge", err)
	}
	// and the fragment path: the first chunk is over the limit as well
	if _, err := encodeStreamFrames(REQUEST, 1, make([]byte, 2*(MaxFrameSize+10)), MaxFrameSize+10); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversize fragment = %v, want ErrFrameTooLarge", err)
	}
}

func TestEncodeStreamFramesClampsFinalFragment(t *testing.T) {
	buffers, err := encodeStreamFrames(REQUEST, 1, make([]byte, 25), 10)
	if err != nil {
		t.Fatalf("encodeStreamFrames(): %v", err)
	}
	if len(buffers) != 3 {
		t.Fatalf("got %d fragments, want 3", len(buffers))
	}
	last, err := Decode(bytes.NewReader(buffers[2]))
	if err != nil {
		t.Fatalf("Decode(last fragment): %v", err)
	}
	if len(last.Payload) != 5 {
		t.Fatalf("last fragment carries %d bytes, want the 5-byte remainder", len(last.Payload))
	}
	if last.Header.Flags&FlagMore != 0 {
		t.Fatal("the final fragment must not carry FlagMore")
	}
}

// ---- client -------------------------------------------------------------

func TestClosedClientContracts(t *testing.T) {
	addr, _ := startTCPServer(t, nil)
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if _, err := c.Handshake(time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Handshake = %v, want ErrClosed", err)
	}
	if err := c.Subscribe("t", time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe = %v, want ErrClosed", err)
	}
	if err := c.Unsubscribe("t", time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Unsubscribe = %v, want ErrClosed", err)
	}
}

func TestHandshakeWithRejectsGarbageResponse(t *testing.T) {
	addr, _ := rawScriptServer(t, func(frame *Frame) []byte {
		// a well-formed envelope whose Data is not a HelloResponse
		return wireRaw(t, RESPONSE, frame.Header.StreamId, []byte(`{"action":"hello","data":123}`))
	})
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	if _, err := c.Handshake(time.Second); err == nil || !strings.Contains(err.Error(), "malformed handshake") {
		t.Fatalf("Handshake() = %v, want a malformed-response error", err)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("a rejected handshake must close the client")
	}
}

// TestClientSurfacesRejections drives the !resp.OK() paths of every
// convenience call against a server that denies everything but HELLO.
func TestClientSurfacesRejections(t *testing.T) {
	addr, _ := rawScriptServer(t, func(frame *Frame) []byte {
		if frame.Header.FrameType == HELLO {
			return wireEnvelope(t, RESPONSE, frame.Header.StreamId, "hello", &HelloResponse{Version: ProtocolVersion})
		}
		return wireEnvelope(t, RESPONSE, frame.Header.StreamId, "denied", &ErrorMessage{Error: "denied"})
	})
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	if _, err := c.Handshake(time.Second); err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c.Subscribe("t", time.Second); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("Subscribe() = %v, want a rejection", err)
	}
	if err := c.Unsubscribe("t", time.Second); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("Unsubscribe() = %v, want a rejection", err)
	}
	if err := c.Ping(time.Second); err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("Ping() = %v, want a rejection", err)
	}
}

func TestClientSurfacesGarbageResponsesAsErrors(t *testing.T) {
	addr, _ := rawScriptServer(t, func(frame *Frame) []byte {
		return wireRaw(t, RESPONSE, frame.Header.StreamId, []byte("garbage"))
	})
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	resp, err := c.Call("echo", nil, time.Second)
	if err != nil {
		t.Fatalf("Call(): %v", err)
	}
	if resp.OK() {
		t.Fatal("a garbage payload must surface as a response error, not success")
	}
}

func TestClientRejectsUnmarshalablePayload(t *testing.T) {
	addr, _ := startTCPServer(t, nil)
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	if _, err := c.Call("x", make(chan int), time.Second); err == nil {
		t.Fatal("a payload json cannot encode must fail before hitting the wire")
	}
}

func TestKeepAliveStops(t *testing.T) {
	addr, _ := startTCPServer(t, nil)
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	// the ticker never fires within the test, so the stop function is the
	// only way out of the loop
	stop := c.KeepAlive(time.Hour, time.Second)
	stop()
	time.Sleep(50 * time.Millisecond) // let the loop observe the stop

	// closing the client ends the loop just as well
	c2, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c2.Close()
	_ = c2.KeepAlive(time.Hour, time.Second)
	time.Sleep(20 * time.Millisecond) // loop is parked in its select
	_ = c2.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestCloseGracefullyFallsBackAfterTimeout(t *testing.T) {
	addr := silentServer(t)
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	// the goodbye is written but never answered: the fallback close must
	// still return nil and end the client
	if err := c.CloseGracefully(80 * time.Millisecond); err != nil {
		t.Fatalf("CloseGracefully(): %v", err)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("CloseGracefully must close the client")
	}
}

// scriptedConn is a net.Conn whose Read parks until Close and whose Write
// fails on the failAt-th call (every call when failAt is 0).
type scriptedConn struct {
	mu        sync.Mutex
	writes    int
	failAt    int
	dead      chan struct{}
	closeOnce sync.Once
}

func newScriptedConn(failAt int) *scriptedConn {
	return &scriptedConn{failAt: failAt, dead: make(chan struct{})}
}

func (c *scriptedConn) Read(b []byte) (int, error) {
	<-c.dead
	return 0, io.EOF
}

func (c *scriptedConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	n := c.writes
	c.mu.Unlock()
	if c.failAt == 0 || n == c.failAt {
		return 0, errors.New("write refused")
	}
	return len(b), nil
}

func (c *scriptedConn) Close() error {
	c.closeOnce.Do(func() { close(c.dead) })
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

func TestClientRoundTripWriteFailure(t *testing.T) {
	c := NewClient(newScriptedConn(0), nil)
	defer c.Close()
	if _, err := c.Call("x", "y", time.Second); err == nil {
		t.Fatal("a refused write must fail the call")
	}
}

func TestClientWriteAllStopsAtFirstFailure(t *testing.T) {
	// a fragmented call: the second fragment's write fails and the rest
	// must not even be attempted
	c := NewClient(newScriptedConn(2), nil)
	defer c.Close()
	big := make([]byte, defaultStreamThreshold*3/2)
	if _, err := c.Call("x", big, time.Second); err == nil {
		t.Fatal("a mid-fragment write failure must fail the call")
	}
}

func TestClientZeroTimeoutContracts(t *testing.T) {
	addr, kill := rawScriptServer(t, func(frame *Frame) []byte {
		msg, err := DecodeJSONMessage(frame)
		if err != nil || msg.Action == "hang" {
			return nil // the hang call never gets an answer
		}
		return wireEnvelope(t, RESPONSE, frame.Header.StreamId, msg.Action, "pong")
	})
	// with no timeout the response is awaited on a bare channel receive
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	if resp, err := c.Call("echo", "x", 0); err != nil || resp.Action != "echo" {
		t.Fatalf("zero-timeout Call = %+v, %v; want an echo response", resp, err)
	}

	// and a dropped connection fails the bare receive as ErrPendingClosed
	c2, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c2.Close()
	done := make(chan error, 1)
	go func() {
		_, err := c2.Call("hang", "x", 0)
		done <- err
	}()
	time.Sleep(250 * time.Millisecond) // let the call register its pending
	kill()
	if err := <-done; !errors.Is(err, ErrPendingClosed) {
		t.Fatalf("zero-timeout call on a dropped conn = %v, want ErrPendingClosed", err)
	}
}

// ---- server-side protocol -----------------------------------------------

func TestRequestHandlerMarshalFailureClosesConnection(t *testing.T) {
	addr, _ := startTCPServer(t, func(action string, data []byte) (any, error) {
		return make(chan int), nil // json cannot encode this
	})
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	_, err = c.Call("boom", nil, 2*time.Second)
	if !errors.Is(err, ErrPendingClosed) {
		t.Fatalf("Call = %v, want ErrPendingClosed (connection reaped)", err)
	}
}

func TestDefaultHandlerRejectsUnknownAction(t *testing.T) {
	addr, _ := startTCPServer(t, nil)
	c, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	resp, err := c.Call("never-registered", "x", 2*time.Second)
	if err != nil {
		t.Fatalf("Call(): %v", err)
	}
	if resp.OK() || !strings.Contains(resp.Err.Error, "no request handler registered") {
		t.Fatalf("resp = %+v, want the default-handler error", resp)
	}
}

func TestServerCountsPong(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	before := ph.pongs.Load()
	if _, err := conn.Write(wireRaw(t, PONG, 9, []byte("x"))); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for ph.pongs.Load() == before {
		if time.Now().After(deadline) {
			t.Fatal("server did not count the unsolicited PONG")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestServerClosesOnProtocolViolations(t *testing.T) {
	addr, _ := startTCPServer(t, nil)
	cases := []struct {
		name   string
		frames [][]byte
	}{
		{"unknown frame type", [][]byte{wireRaw(t, FrameType(0x7F), 1, []byte("x"))}},
		{"malformed HELLO payload", [][]byte{wireRaw(t, HELLO, 1, []byte("{bogus"))}},
		{"HELLO data of wrong shape", [][]byte{wireRaw(t, HELLO, 1, []byte(`{"action":"hello","data":123}`))}},
		{"malformed REQUEST payload", [][]byte{wireRaw(t, REQUEST, 1, []byte("bogus"))}},
		{"malformed SUBSCRIBE payload", [][]byte{wireRaw(t, SUBSCRIBE, 1, []byte("bogus"))}},
		{"SUBSCRIBE without topic", [][]byte{wireRaw(t, SUBSCRIBE, 1, []byte(`{"action":""}`))}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sendRawExpectClosed(t, addr, tc.frames...)
		})
	}
}

func TestPublishRejectsUnmarshalablePayload(t *testing.T) {
	_, ph := startTCPServer(t, nil)
	if _, err := ph.Publish("t", make(chan int)); err == nil {
		t.Fatal("Publish must reject a payload json cannot encode")
	}
}

func TestPublishRejectsOversizePayload(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	// a subscriber must exist so the no-members early return does not
	// pre-empt the encode
	if _, err := conn.Write(wireEnvelope(t, SUBSCRIBE, 1, "big", nil)); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		ph.mu.RLock()
		n := len(ph.subs["big"])
		ph.mu.RUnlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscription never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := ph.Publish("big", strings.Repeat("x", MaxFrameSize+64)); err == nil {
		t.Fatal("an unencodable publish must fail")
	}
}

func TestCacheBudgetResetsOnBookkeepingDrift(t *testing.T) {
	_, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.cacheBytes = ph.cacheBudget + 1 // nothing cached, yet over budget
	ph.enforceCacheBudgetLocked()
	reset := ph.cacheBytes
	ph.mu.Unlock()
	if reset != 0 {
		t.Fatalf("cacheBytes = %d, want the drift reset to 0", reset)
	}
}

func TestReplayAbandonsOnWriteFailure(t *testing.T) {
	_, ph := startTCPServer(t, nil)
	conn, peer := net.Pipe()
	_ = peer.Close() // the far end is gone: writes must fail
	ph.replayTo(conn, [][]byte{[]byte("missed")})
	// no panic, no hang: a failing replay write is abandoned
	_ = conn.Close()
}

func TestSessionJanitorSweepsOnTick(t *testing.T) {
	oldInterval := janitorTickInterval
	janitorTickInterval = 10 * time.Millisecond
	defer func() { janitorTickInterval = oldInterval }()

	_, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	stale := &session{topics: map[string]struct{}{}}
	stale.lastSeen.Store(time.Now().Add(-time.Hour).UnixNano())
	ph.sessions["stale"] = stale
	ph.mu.Unlock()

	// a second janitor picks up the shrunken interval; both exit on Close.
	// The interval is read here, on the test goroutine — the janitor itself
	// only sees the captured value, so the deferred restore below cannot
	// race with it.
	go ph.sessionJanitor(janitorTickInterval)

	deadline := time.Now().Add(2 * time.Second)
	for {
		ph.mu.RLock()
		left := len(ph.sessions)
		ph.mu.RUnlock()
		if left == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("janitor did not reap the stale session on its tick")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNewSessionTokenFallsBackWithoutEntropy(t *testing.T) {
	oldRead := readRandom
	readRandom = func(b []byte) (int, error) { return 0, errors.New("no entropy") }
	defer func() { readRandom = oldRead }()

	token := newSessionToken()
	if !strings.HasPrefix(token, "s-") {
		t.Fatalf("token = %q, want the time-based fallback", token)
	}
}

// codecCtx is a minimal pipeline context for driving the FrameCodec
// handlers directly.
type codecCtx struct {
	conn   net.Conn
	closed error
	wrote  []handler.Message
}

func (c *codecCtx) Conn() net.Conn                   { return c.conn }
func (c *codecCtx) Handler() handler.Handler         { return nil }
func (c *codecCtx) Write(m handler.Message)          { c.wrote = append(c.wrote, m) }
func (c *codecCtx) Close(err error)                  { c.closed = err }
func (c *codecCtx) Attachment() handler.Attachment   { return nil }
func (c *codecCtx) SetAttachment(handler.Attachment) {}
func (c *codecCtx) HandleRead(handler.Message)       {}
func (c *codecCtx) HandleWrite(handler.Message)      {}

func TestFrameCodecReapsIdleConnection(t *testing.T) {
	conn, peer := net.Pipe()
	defer func() {
		_ = peer.Close()
		_ = conn.Close()
	}()
	_ = conn.SetReadDeadline(time.Now().Add(-time.Second)) // already expired
	codec := NewFrameCodec()
	ctx := &codecCtx{conn: conn}
	codec.HandleRead(ctx, nil)
	// the reaper closes the connection itself instead of going through
	// ctx.Close, so verify from the peer end that it is really shut
	done := make(chan error, 1)
	go func() {
		_, err := peer.Read(make([]byte, 1))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("peer still readable: the connection was not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("an expired read deadline must reap the connection")
	}
}

func TestFrameCodecHandleWriteClosesOnUnencodableFrame(t *testing.T) {
	codec := &FrameCodec{logger: log.New(io.Discard, "", 0), streamThreshold: 1 << 30}
	ctx := &codecCtx{}
	codec.HandleWrite(ctx, newFrame(REQUEST, 1, make([]byte, MaxFrameSize+1)))
	if ctx.closed == nil {
		t.Fatal("an unencodable frame must close the pipeline")
	}
}

// ---- resilient client ---------------------------------------------------

func TestResilientConnectContracts(t *testing.T) {
	// dial failures surface directly
	rc := NewResilientClient("127.0.0.1:1", nil, nil)
	if err := rc.Connect(time.Second); err == nil {
		t.Fatal("Connect to a dead address must fail")
	}
	// after Close, Connect is refused
	if err := rc.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := rc.Connect(time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Connect after Close = %v, want ErrClosed", err)
	}
	// a second Connect on a connected client is a no-op
	addr, _ := startTCPServer(t, nil)
	rc2 := NewResilientClient(addr, nil, nil)
	defer rc2.Close()
	if err := rc2.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc2.Connect(2 * time.Second); err != nil {
		t.Fatalf("idempotent Connect() = %v, want nil", err)
	}
}

// TestResilientTransportErrorsSurface pins that API calls report the dead
// transport instead of silently succeeding. The listener dies first, so the
// background reconnect cannot heal the client before the calls run.
func TestResilientTransportErrorsSurface(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ph := NewProtocolHandler(nil)
	t.Cleanup(func() {
		ph.Close()
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

	rc := NewResilientClient(listener.Addr().String(), nil, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("t", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	_ = listener.Close()
	killCurrent(t, rc)

	if err := rc.Subscribe("t", time.Second); err == nil {
		t.Fatal("Subscribe on a dead transport must fail")
	}
	if err := rc.Unsubscribe("t", time.Second); err == nil {
		t.Fatal("Unsubscribe on a dead transport must fail")
	}
}

func TestResilientConnectRejectsBadHandshake(t *testing.T) {
	addr, _ := rawScriptServer(t, func(frame *Frame) []byte {
		return wireEnvelope(t, RESPONSE, frame.Header.StreamId, "hello", &ErrorMessage{Error: "version unsupported"})
	})
	rc := NewResilientClient(addr, nil, nil)
	defer rc.Close()
	if err := rc.Connect(time.Second); err == nil || !strings.Contains(err.Error(), "handshake rejected") {
		t.Fatalf("Connect() = %v, want a rejection", err)
	}
}

func TestResilientCloseWinsOverReconnectBackoff(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ph := NewProtocolHandler(nil)
	t.Cleanup(func() {
		ph.Close()
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

	rc := NewResilientClient(listener.Addr().String(), nil, &ResilientOptions{BackoffStart: 10 * time.Millisecond, BackoffMax: 30 * time.Millisecond})
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}

	// kill the server, then the connection: the reconnect loop now spins
	// in its backoff waits, and Close must win over the next dial
	_ = listener.Close()
	killCurrent(t, rc)
	time.Sleep(200 * time.Millisecond)
	if err := rc.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	select {
	case <-rc.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not close")
	}
	time.Sleep(100 * time.Millisecond) // let the loop observe the close
}

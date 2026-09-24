package proto

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
)

// TestResilientClientUnsubscribeStopsPushes pins Unsubscribe on the
// resilient wrapper: the server drops the member, the client forgets the
// topic (so a later reconnect must not silently restore it), and pushes
// stop arriving.
func TestResilientClientUnsubscribeStopsPushes(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes := make(chan *Frame, 8)
	rc := NewResilientClient(addr, func(frame *Frame) { pushes <- frame }, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("t1", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	if _, err := ph.Publish("t1", "while subscribed"); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	select {
	case <-pushes:
	case <-time.After(2 * time.Second):
		t.Fatal("no push while subscribed")
	}

	if err := rc.Unsubscribe("t1", 2*time.Second); err != nil {
		t.Fatalf("Unsubscribe(): %v", err)
	}
	if got := ph.Subscribers("t1"); got != 0 {
		t.Fatalf("server-side subscribers = %d after Unsubscribe, want 0", got)
	}
	rc.mu.Lock()
	_, remembered := rc.subscribed["t1"]
	rc.mu.Unlock()
	if remembered {
		t.Fatal("client still remembers the topic after Unsubscribe")
	}

	if _, err := ph.Publish("t1", "after unsubscribe"); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	select {
	case frame := <-pushes:
		t.Fatalf("push arrived after Unsubscribe: %v", frame)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestResilientClientPingAndGracefulClose covers the remaining thin
// wrappers: Ping round-trips on the current connection, CloseGracefully
// says goodbye and stops the client for good, and every later call sees
// ErrClosed.
func TestResilientClientPingAndGracefulClose(t *testing.T) {
	addr, _ := startTCPServer(t, nil)
	rc := NewResilientClient(addr, nil, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}

	if err := rc.Ping(2 * time.Second); err != nil {
		t.Fatalf("Ping(): %v", err)
	}

	if err := rc.CloseGracefully(2 * time.Second); err != nil {
		t.Fatalf("CloseGracefully(): %v", err)
	}
	select {
	case <-rc.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not close after CloseGracefully")
	}
	if _, err := rc.Call("echo", nil, time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Call after close = %v, want ErrClosed", err)
	}
	if err := rc.Ping(time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ping after close = %v, want ErrClosed", err)
	}
	if err := rc.Unsubscribe("t", time.Second); !errors.Is(err, ErrClosed) {
		t.Fatalf("Unsubscribe after close = %v, want ErrClosed", err)
	}
	// closing twice must stay quiet
	if err := rc.CloseGracefully(time.Second); err != nil {
		t.Fatalf("second CloseGracefully(): %v", err)
	}
}

// TestRequireHelloViaPublicAPI exercises the public toggle rather than the
// internal field the existing strict-handshake tests set directly.
func TestRequireHelloViaPublicAPI(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	ph.RequireHello(true)

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	if _, err := client.Call("echo", "too early", 2*time.Second); err == nil {
		t.Fatal("Call before HELLO unexpectedly succeeded in strict mode")
	}
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("connection stayed open after a pre-handshake frame")
	}

	// the toggle is dynamic: switching it off lets a plain handshake through
	ph.RequireHello(false)
	ok, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer ok.Close()
	if _, err := ok.Handshake(2 * time.Second); err != nil {
		t.Fatalf("Handshake() after toggling strict mode off: %v", err)
	}
}

func TestFrameStringers(t *testing.T) {
	want := map[FrameType]string{
		REQUEST:     "REQUEST",
		RESPONSE:    "RESPONSE",
		PUBLISH:     "PUBLISH",
		SUBSCRIBE:   "SUBSCRIBE",
		UNSUBSCRIBE: "UNSUBSCRIBE",
		PING:        "PING",
		PONG:        "PONG",
		CLOSE:       "CLOSE",
		HELLO:       "HELLO",
	}
	for ft, name := range want {
		if got := ft.String(); got != name {
			t.Fatalf("FrameType(%d).String() = %q, want %q", uint8(ft), got, name)
		}
	}
	if got := FrameType(200).String(); got != "FrameType(200)" {
		t.Fatalf("unknown FrameType String = %q", got)
	}

	h := FrameHeader{FrameType: REQUEST, Flags: 1, StreamId: 2, Length: 3}
	if got := h.String(); !strings.Contains(got, "REQUEST") || !strings.Contains(got, "streamId:2") {
		t.Fatalf("FrameHeader.String() = %q", got)
	}
	f := &Frame{Header: h, Payload: []byte("xy")}
	if got := f.String(); !strings.Contains(got, "2 bytes") {
		t.Fatalf("Frame.String() = %q, want the payload size", got)
	}
}

func TestQuoteEscapes(t *testing.T) {
	if got := quote(`a"b\c`); got != `"a\"b\\c"` {
		t.Fatalf("quote() = %s, want escaped JSON string", got)
	}
}

// captureOutbound records whatever the codec hands towards the head.
// Only HandleWrite is ever called on it, so the embedded nil interface
// never panics.
type captureOutbound struct {
	handler.OutboundContext
	written []byte
}

func (c *captureOutbound) HandleWrite(message handler.Message) {
	if b, ok := message.([]byte); ok {
		c.written = append(c.written, b...)
		return
	}
	c.written = append(c.written, []byte(fmt.Sprint(message))...)
}

// TestFrameCodecStreamThresholdFragmentsOutbound drives the custom-threshold
// codec: a payload over the threshold must leave as several frames — all
// but the last flagged FlagMore — that DecodeStreamed reassembles into the
// original message, while small payloads and non-frame messages pass
// through untouched.
func TestFrameCodecStreamThresholdFragmentsOutbound(t *testing.T) {
	codec := NewFrameCodecWithStreamThreshold(8)
	payload := bytes.Repeat([]byte("x"), 20)

	out := &captureOutbound{}
	codec.HandleWrite(out, &Frame{
		Header:  FrameHeader{FrameType: REQUEST, StreamId: 7},
		Payload: payload,
	})

	// walk the emitted bytes frame by frame
	var fragments []*Frame
	r := bytes.NewReader(out.written)
	for r.Len() > 0 {
		f, err := Decode(r)
		if err != nil {
			t.Fatalf("Decode() fragment %d: %v", len(fragments), err)
		}
		fragments = append(fragments, f)
	}
	if len(fragments) < 3 {
		t.Fatalf("20 bytes over a threshold of 8 produced %d frames, want at least 3", len(fragments))
	}
	for i, f := range fragments {
		if f.Header.StreamId != 7 || f.Header.FrameType != REQUEST {
			t.Fatalf("fragment %d header = %+v, want streamId 7 / REQUEST", i, f.Header)
		}
		if f.Header.Length > int32(8) {
			t.Fatalf("fragment %d length = %d, exceeds the threshold", i, f.Header.Length)
		}
		wantFlag := i < len(fragments)-1
		if has := f.Header.Flags&FlagMore != 0; has != wantFlag {
			t.Fatalf("fragment %d FlagMore = %v, want %v", i, has, wantFlag)
		}
	}
	// inbound assembly must reproduce the original message
	assembled, err := DecodeStreamed(bytes.NewReader(out.written))
	if err != nil {
		t.Fatalf("DecodeStreamed(): %v", err)
	}
	if !bytes.Equal(assembled.Payload, payload) {
		t.Fatalf("assembled %d bytes, want the original %d", len(assembled.Payload), len(payload))
	}
	if assembled.Header.Length != int32(len(payload)) {
		t.Fatalf("assembled header length = %d, want %d", assembled.Header.Length, len(payload))
	}

	// a payload under the threshold stays a single unflagged frame
	out2 := &captureOutbound{}
	codec.HandleWrite(out2, &Frame{
		Header:  FrameHeader{FrameType: PING, StreamId: 1},
		Payload: []byte("tiny"),
	})
	single, err := DecodeStreamed(bytes.NewReader(out2.written))
	if err != nil {
		t.Fatalf("DecodeStreamed() small payload: %v", err)
	}
	if string(single.Payload) != "tiny" || single.Header.Flags&FlagMore != 0 {
		t.Fatalf("small payload decoded as %+v, want a single unflagged frame", single.Header)
	}

	// non-frame messages propagate unchanged
	out3 := &captureOutbound{}
	codec.HandleWrite(out3, []byte("passthrough"))
	if string(out3.written) != "passthrough" {
		t.Fatalf("non-frame message became %q", out3.written)
	}
}

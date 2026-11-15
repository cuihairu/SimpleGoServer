package demo

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// servePump wires Initializer onto a fresh pipeline over conn and serves it
// until the connection dies, exactly one message per FireRead call the way
// the reactor worker drives a connection.
func servePump(t *testing.T, conn net.Conn) {
	t.Helper()
	p := handlerImpl.NewPipeline(conn)
	if err := Initializer(p); err != nil {
		t.Errorf("Initializer returned an error: %v", err)
	}
	go func() {
		for {
			p.FireRead(conn)
			if _, err := conn.Read(make([]byte, 0)); err != nil {
				return // pipe closed, stop serving
			}
		}
	}()
	t.Cleanup(func() { _ = conn.Close() })
}

func writeFrame(t *testing.T, conn net.Conn, frame *proto.Frame) {
	t.Helper()
	buf, err := proto.Encode(frame)
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(buf); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func readFrame(t *testing.T, conn net.Conn) *proto.Frame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	frame, err := proto.Decode(conn)
	if err != nil {
		t.Fatalf("Decode(): %v", err)
	}
	return frame
}

// TestInitializerWiresCodecAndProtocol drives the demo pipeline assembly end
// to end on a real pipeline: a request frame written as raw wire bytes to the
// client end of a pipe must come back as a RESPONSE whose envelope action
// matches and whose data still carries the original payload — which is only
// possible when Initializer attached both the frame codec (raw bytes in,
// frames out) and the shared protocol handler (REQUEST -> RESPONSE) in order.
func TestInitializerWiresCodecAndProtocol(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	servePump(t, server)

	frame, err := proto.EncodeJSON(proto.REQUEST, 42, "echo", "hello")
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	writeFrame(t, client, frame)

	resp := readFrame(t, client)
	if resp.Header.FrameType != proto.RESPONSE {
		t.Fatalf("frame type = %s, want RESPONSE", resp.Header.FrameType)
	}
	if resp.Header.StreamId != 42 {
		t.Fatalf("stream id = %d, want 42", resp.Header.StreamId)
	}
	env, err := proto.DecodeJSONMessage(resp)
	if err != nil {
		t.Fatalf("DecodeJSONMessage(): %v", err)
	}
	if env.Action != "echo" {
		t.Fatalf("envelope action = %q, want echo", env.Action)
	}
	var body struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(env.Data, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	var echoed string
	if err := json.Unmarshal(body.Data, &echoed); err != nil {
		t.Fatalf("echoed data: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("echo = %q, want hello (raw payload must survive the envelope)", echoed)
	}
}

// TestEchoActionEnvelopesPayload pins the demo business semantics: the action
// name comes back with the raw payload wrapped as a JSON envelope. The
// payload keys are deliberately out of alphabetical order — a re-marshal
// through a map would sort them, so finding the original order on the wire
// proves the bytes pass through untouched.
func TestEchoActionEnvelopesPayload(t *testing.T) {
	const payload = `{"b":1,"a":1}`
	got, err := echoAction("echo", []byte(payload))
	if err != nil {
		t.Fatalf("echoAction returned an error: %v", err)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if !strings.Contains(string(encoded), `"action":"echo"`) {
		t.Fatalf("envelope = %s, want action echo", encoded)
	}
	if !strings.Contains(string(encoded), payload) {
		t.Fatalf("envelope = %s, want the exact original payload %s (raw passthrough, not a re-marshal that would sort keys)", encoded, payload)
	}
}

// TestRequireHelloRejectsNonHelloFirstFrame pins strict handshake behavior
// end to end: with RequireHello(true), a REQUEST written as the first frame
// must get the connection closed instead of a response, and a proper HELLO
// handshake first keeps the same request working.
func TestRequireHelloRejectsNonHelloFirstFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	servePump(t, server)

	RequireHello(true)
	defer RequireHello(false)

	req, _ := proto.EncodeJSON(proto.REQUEST, 7, "echo", "early")
	writeFrame(t, client, req)

	// strict mode: the server must close, not answer. On a synchronous pipe
	// the close is observable as a read error (EOF), never a RESPONSE frame.
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := proto.Decode(client); err == nil {
		t.Fatal("a pre-HELLO REQUEST in strict mode got a response frame; want the connection closed")
	}

	// control: with a HELLO first, the same request must succeed
	client2, server2 := net.Pipe()
	defer client2.Close()
	servePump(t, server2)
	hello, _ := proto.EncodeJSON(proto.HELLO, 1, "hello", &proto.HelloRequest{Versions: []int{proto.ProtocolVersion}})
	writeFrame(t, client2, hello)
	ack := readFrame(t, client2)
	if ack.Header.FrameType != proto.RESPONSE {
		t.Fatalf("HELLO ack frame type = %s, want RESPONSE", ack.Header.FrameType)
	}
	ok, _ := proto.EncodeJSON(proto.REQUEST, 8, "echo", "after-hello")
	writeFrame(t, client2, ok)
	resp := readFrame(t, client2)
	if resp.Header.StreamId != 8 {
		t.Fatalf("post-HELLO REQUEST stream id = %d, want 8 (connection must work)", resp.Header.StreamId)
	}
}

// TestRequireHelloAndClose covers the config application and the once-guarded
// shutdown: the second Close must be a no-op.
func TestRequireHelloAndClose(t *testing.T) {
	RequireHello(true)
	RequireHello(false)
	Close()
	Close() // the once guard must make the second call a no-op
}

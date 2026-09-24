package proto

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// TestEncodeStreamFramesSmallPayload checks the pass-through case: small
// payloads stay a single plain frame, so nothing changes for peers that
// never stream.
func TestEncodeStreamFramesSmallPayload(t *testing.T) {
	buffers, err := encodeStreamFrames(REQUEST, 7, []byte("hello"), 1024)
	if err != nil {
		t.Fatalf("encodeStreamFrames(): %v", err)
	}
	if len(buffers) != 1 {
		t.Fatalf("got %d buffers, want 1 for a small payload", len(buffers))
	}
	frame, err := Decode(bytes.NewReader(buffers[0]))
	if err != nil {
		t.Fatalf("Decode(): %v", err)
	}
	if frame.Header.Flags&FlagMore != 0 {
		t.Fatal("single frame unexpectedly flagged FlagMore")
	}
	if string(frame.Payload) != "hello" {
		t.Fatalf("payload = %q", frame.Payload)
	}
}

// TestEncodeStreamFramesFragments checks the fragmentation layout: every
// fragment but the last carries FlagMore, all share the stream id, and
// concatenating the payloads restores the original.
func TestEncodeStreamFramesFragments(t *testing.T) {
	const threshold = 16
	payload := bytes.Repeat([]byte("x"), 50) // 4 fragments: 16+16+16+2

	buffers, err := encodeStreamFrames(REQUEST, 9, payload, threshold)
	if err != nil {
		t.Fatalf("encodeStreamFrames(): %v", err)
	}
	if len(buffers) != 4 {
		t.Fatalf("got %d fragments, want 4", len(buffers))
	}
	var assembled []byte
	for i, buf := range buffers {
		frame, err := Decode(bytes.NewReader(buf))
		if err != nil {
			t.Fatalf("Decode fragment %d: %v", i, err)
		}
		if frame.Header.StreamId != 9 {
			t.Fatalf("fragment %d stream id = %d, want 9", i, frame.Header.StreamId)
		}
		flagged := frame.Header.Flags&FlagMore != 0
		if want := i < len(buffers)-1; flagged != want {
			t.Fatalf("fragment %d FlagMore = %v, want %v", i, flagged, want)
		}
		// every fragment must respect the per-frame wire limit
		if len(frame.Payload) > MaxFrameSize {
			t.Fatalf("fragment %d larger than MaxFrameSize", i)
		}
		assembled = append(assembled, frame.Payload...)
	}
	if !bytes.Equal(assembled, payload) {
		t.Fatalf("assembled %d bytes, want the original %d", len(assembled), len(payload))
	}
}

// TestDecodeStreamedRoundTrip drives the reader through a fragmented
// message: fragments written back to back assemble into one logical frame.
func TestDecodeStreamedRoundTrip(t *testing.T) {
	const threshold = 8
	payload := []byte("streamed payload, larger than the threshold")

	buffers, err := encodeStreamFrames(RESPONSE, 3, payload, threshold)
	if err != nil {
		t.Fatalf("encodeStreamFrames(): %v", err)
	}
	var wire []byte
	for _, buf := range buffers {
		wire = append(wire, buf...)
	}

	frame, err := DecodeStreamed(bytes.NewReader(wire))
	if err != nil {
		t.Fatalf("DecodeStreamed(): %v", err)
	}
	if frame.Header.FrameType != RESPONSE || frame.Header.StreamId != 3 {
		t.Fatalf("header = %s, want RESPONSE stream 3", frame.Header)
	}
	if frame.Header.Flags&FlagMore != 0 {
		t.Fatal("assembled frame still flagged FlagMore")
	}
	if int(frame.Header.Length) != len(payload) {
		t.Fatalf("assembled length = %d, want %d", frame.Header.Length, len(payload))
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatal("assembled payload mismatch")
	}
}

// TestDecodeStreamedRejectsInterleaved checks that a fragment sequence
// interrupted by a foreign stream id is a protocol error — after
// interleaving, the byte stream cannot be trusted.
func TestDecodeStreamedRejectsInterleaved(t *testing.T) {
	first, _ := encodeStreamFrames(REQUEST, 1, bytes.Repeat([]byte("a"), 20), 8)
	stray, _ := Encode(newFrame(RESPONSE, 99, []byte("stray")))

	var wire []byte
	wire = append(wire, first[0]...) // first fragment of stream 1
	wire = append(wire, stray...)    // an unrelated frame sneaks in

	if _, err := DecodeStreamed(bytes.NewReader(wire)); err == nil {
		t.Fatal("interleaved fragment sequence accepted")
	}
}

// TestDecodeStreamedRejectsOversize checks the assembly bound: fragments
// adding up beyond MaxStreamSize are rejected instead of exhausting memory.
func TestDecodeStreamedRejectsOversize(t *testing.T) {
	chunk := bytes.Repeat([]byte{0}, MaxFrameSize) // one full-size fragment
	var wire []byte
	for i := 0; i <= MaxStreamSize/MaxFrameSize; i++ {
		f := newFrame(REQUEST, 5, chunk)
		f.Header.Flags |= FlagMore // every fragment claims a successor
		buf, err := Encode(f)
		if err != nil {
			t.Fatalf("Encode(): %v", err)
		}
		wire = append(wire, buf...)
	}
	if _, err := DecodeStreamed(bytes.NewReader(wire)); !errors.Is(err, ErrStreamTooLarge) {
		t.Fatalf("oversize assembly = %v, want ErrStreamTooLarge", err)
	}
}

// TestCallStreamsLargePayload is the end-to-end proof: a request payload
// beyond the single-frame limit travels as fragments, the server's codec
// assembles it, and the equally fragmented response comes back intact.
func TestCallStreamsLargePayload(t *testing.T) {
	echo := func(action string, data []byte) (any, error) {
		return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
	}
	addr, _ := startTCPServer(t, echo)

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()

	// 2MB of recognizable data: far above MaxFrameSize, so both the
	// request and the echoed response must travel as fragments. The
	// timeout is generous on purpose: this test also runs under -race in
	// full-suite sweeps on loaded machines, where a round trip that takes
	// ~2s unloaded can stretch several times that.
	payload := bytes.Repeat([]byte("0123456789abcdef"), 128*1024)
	resp, err := client.Call("echo", json.RawMessage(`"`+string(payload)+`"`), 30*time.Second)
	if err != nil {
		t.Fatalf("Call(2MB): %v", err)
	}
	if !resp.OK() {
		t.Fatalf("server rejected the streamed call: %s", resp.Err.Error)
	}
	var envelope struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(resp.Data, &envelope); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if envelope.Data != string(payload) {
		t.Fatalf("streamed round trip lost data: got %d bytes, want %d", len(envelope.Data), len(payload))
	}
}

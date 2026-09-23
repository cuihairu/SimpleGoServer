package proto

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		frame   *Frame
		wantAct string
	}{
		{
			name: "request with json payload",
			frame: &Frame{
				Header: FrameHeader{FrameType: REQUEST, StreamId: 42, Length: 0},
				Payload: []byte(`{"action":"echo","data":"hi"}`),
			},
			wantAct: "echo",
		},
		{
			name: "ping without payload content",
			frame: &Frame{
				Header:  FrameHeader{FrameType: PING, StreamId: 7},
				Payload: []byte(`{}`),
			},
			wantAct: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.frame.Header.Length = int32(len(tt.frame.Payload))
			buf, err := Encode(tt.frame)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			if len(buf) != HeaderSize+len(tt.frame.Payload) {
				t.Fatalf("encoded size = %d, want %d", len(buf), HeaderSize+len(tt.frame.Payload))
			}
			got, err := Decode(bytes.NewReader(buf))
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if got.Header.FrameType != tt.frame.Header.FrameType ||
				got.Header.StreamId != tt.frame.Header.StreamId ||
				got.Header.Flags != tt.frame.Header.Flags {
				t.Fatalf("header mismatch: got %s want %s", got.Header, tt.frame.Header)
			}
			if !bytes.Equal(got.Payload, tt.frame.Payload) {
				t.Fatalf("payload mismatch: got %q want %q", got.Payload, tt.frame.Payload)
			}
			msg, err := DecodeJSONMessage(got)
			if err != nil {
				t.Fatalf("DecodeJSONMessage() error = %v", err)
			}
			if msg.Action != tt.wantAct {
				t.Fatalf("action = %q, want %q", msg.Action, tt.wantAct)
			}
		})
	}
}

func TestDecodeSplitAcrossWrites(t *testing.T) {
	// a frame delivered one byte at a time must still decode
	frame, err := EncodeJSON(RESPONSE, 99, "reply", map[string]int{"code": 1})
	if err != nil {
		t.Fatalf("EncodeJSON() error = %v", err)
	}
	buf, err := Encode(frame)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	oneByteAtATime := &slowReader{data: buf}
	got, err := Decode(oneByteAtATime)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	var payload map[string]int
	if err := DecodeJSONData(got, &payload); err != nil {
		t.Fatalf("DecodeJSONData() error = %v", err)
	}
	if payload["code"] != 1 {
		t.Fatalf("payload = %v, want code=1", payload)
	}
}

type slowReader struct {
	data []byte
	pos  int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	p[0] = r.data[r.pos]
	r.pos++
	return 1, nil
}

func TestDecodeCleanEOFBetweenFrames(t *testing.T) {
	got, err := Decode(strings.NewReader(""))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Decode(empty) error = %v, want io.EOF", err)
	}
	if got != nil {
		t.Fatalf("Decode(empty) frame = %v, want nil", got)
	}
}

func TestDecodeTruncatedPayload(t *testing.T) {
	frame, err := EncodeJSON(REQUEST, 1, "echo", "hello")
	if err != nil {
		t.Fatalf("EncodeJSON() error = %v", err)
	}
	buf, err := Encode(frame)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	// drop bytes from the tail so the announced length can't be satisfied
	_, err = Decode(bytes.NewReader(buf[:len(buf)-3]))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Decode(truncated) error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestDecodeHeaderRejectsOversizeLength(t *testing.T) {
	header := make([]byte, HeaderSize)
	header[0] = byte(REQUEST)
	// length field = MaxFrameSize + 1
	announced := uint32(MaxFrameSize) + 1
	header[6] = byte(announced >> 24)
	header[7] = byte(announced >> 16)
	header[8] = byte(announced >> 8)
	header[9] = byte(announced)
	_, err := DecodeHeader(header)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("DecodeHeader(oversize) error = %v, want ErrFrameTooLarge", err)
	}
}

func TestEncodeRejectsOversizeAndEmpty(t *testing.T) {
	if _, err := Encode(nil); err == nil {
		t.Fatal("Encode(nil) should fail")
	}
	if _, err := Encode(&Frame{Header: FrameHeader{FrameType: PING}}); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("Encode(empty payload) error = %v, want ErrEmptyFrame", err)
	}
	big := &Frame{Header: FrameHeader{FrameType: REQUEST}, Payload: make([]byte, MaxFrameSize+1)}
	if _, err := Encode(big); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Encode(oversize) error = %v, want ErrFrameTooLarge", err)
	}
}

func TestHeaderIdPacksTypeAndStream(t *testing.T) {
	h := FrameHeader{FrameType: RESPONSE, StreamId: 0x00ABCDEF}
	if got := h.Id(); got != uint32(RESPONSE)<<24|0x00ABCDEF {
		t.Fatalf("Id() = %#x", got)
	}
}

func TestJSONRoundTripOmitsEmptyData(t *testing.T) {
	frame, err := EncodeJSON(PING, 5, "heartbeat", nil)
	if err != nil {
		t.Fatalf("EncodeJSON() error = %v", err)
	}
	if bytes.Contains(frame.Payload, []byte("data")) {
		t.Fatalf("payload %q should omit empty data", frame.Payload)
	}
	msg, err := DecodeJSONMessage(frame)
	if err != nil {
		t.Fatalf("DecodeJSONMessage() error = %v", err)
	}
	if msg.Action != "heartbeat" {
		t.Fatalf("action = %q", msg.Action)
	}
	var out map[string]any
	if err := DecodeJSONData(frame, &out); err != nil {
		t.Fatalf("DecodeJSONData with no data should not error, got %v", err)
	}
}

func TestEncodeJSONInvalidData(t *testing.T) {
	if _, err := EncodeJSON(REQUEST, 1, "bad", make(chan int)); err == nil {
		t.Fatal("EncodeJSON with unmarshalable data should fail")
	}
}

func TestFrameTypeString(t *testing.T) {
	if FrameType(REQUEST).String() != "REQUEST" {
		t.Fatalf("REQUEST.String() = %q", FrameType(REQUEST).String())
	}
	unknown := FrameType(200)
	if !strings.HasPrefix(unknown.String(), "FrameType(") {
		t.Fatalf("unknown type string = %q", unknown.String())
	}
}

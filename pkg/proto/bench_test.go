package proto

import (
	"bytes"
	"testing"
)

func BenchmarkEncodeSmallFrame(b *testing.B) {
	frame, err := EncodeJSON(REQUEST, 1, "echo", map[string]string{"msg": "hello"})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeDecodeRoundTrip(b *testing.B) {
	frame, err := EncodeJSON(REQUEST, 1, "echo", map[string]string{"msg": "hello"})
	if err != nil {
		b.Fatal(err)
	}
	wire, err := Encode(frame)
	if err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(wire)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(wire)
		if _, err := Decode(reader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeLargeFrame(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 64*1024)
	frame := &Frame{
		Header:  FrameHeader{FrameType: REQUEST, StreamId: 1, Length: int32(len(payload))},
		Payload: payload,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Encode(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONEnvelope(b *testing.B) {
	data := map[string]any{"user": "bench", "count": 42, "ok": true}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame, err := EncodeJSON(REQUEST, uint32(i), "action.name", data)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := DecodeJSONMessage(frame); err != nil {
			b.Fatal(err)
		}
	}
}

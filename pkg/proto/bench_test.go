package proto

import (
	"bytes"
	"strings"
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

// BenchmarkPooledDecodeRoundTrip is BenchmarkEncodeDecodeRoundTrip's pooled
// twin: the same wire bytes through the same decode core, but with a pool
// backend — the path the server codec actually runs. The plain pair above is
// the public Decode that hands payload aliases to application code and
// therefore cannot recycle; this one releases after every frame exactly as
// FrameCodec does, since its dispatch is synchronous.
//
// The backend is private to the benchmark so the numbers describe a warm
// pool and are not perturbed by other packages' traffic.
func BenchmarkPooledDecodeRoundTrip(b *testing.B) {
	frame, err := EncodeJSON(REQUEST, 1, "echo", map[string]string{"msg": "hello"})
	if err != nil {
		b.Fatal(err)
	}
	wire, err := Encode(frame)
	if err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(wire)
	backend := &syncPoolBackend{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Reset(wire)
		f, err := decodeFrame(reader, backend)
		if err != nil {
			b.Fatal(err)
		}
		if len(f.Payload) == 0 {
			b.Fatal("decoded an empty payload")
		}
		f.release(backend)
	}
}

// BenchmarkEncodeSmallFramePooled is BenchmarkEncodeSmallFrame's pooled twin:
// the same frame encoded into a size-class buffer that is recycled right after
// the write consumed it. Buffers/cells are handed to putPayloadArray through
// one preallocated pair of slices, because the real callers release through
// releaseBuffers on slices they built once per message — allocating those
// inside the loop would charge the harness, not the code under test, for them.
func BenchmarkEncodeSmallFramePooled(b *testing.B) {
	frame, err := EncodeJSON(REQUEST, 1, "echo", map[string]string{"msg": "hello"})
	if err != nil {
		b.Fatal(err)
	}
	backend := &syncPoolBackend{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, cell, err := encodeFrameBuffered(backend, REQUEST, 1, frame.Payload, 0)
		if err != nil {
			b.Fatal(err)
		}
		if len(buf) <= HeaderSize {
			b.Fatal("encoded frame has no payload")
		}
		putPayloadArray(backend, buf, cell)
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

// BenchmarkJSONEnvelopeString1MiB is the large-payload shape the streaming
// benchmarks expose: a plain-string request/response body of 1MiB. The
// string path is the envelope's hot case — EncodeJSON splices plain-ASCII
// strings straight into the payload and DecodeJSONMessage aliases the data
// instead of copying it, so this benchmark pins both optimizations.
func BenchmarkJSONEnvelopeString1MiB(b *testing.B) {
	data := strings.Repeat("payload-0123456789", 1<<20/19)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frame, err := EncodeJSON(REQUEST, uint32(i), "echo", data)
		if err != nil {
			b.Fatal(err)
		}
		msg, err := DecodeJSONMessage(frame)
		if err != nil {
			b.Fatal(err)
		}
		if len(msg.Data) == 0 {
			b.Fatal("empty data")
		}
	}
}

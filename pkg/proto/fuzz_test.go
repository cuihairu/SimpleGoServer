package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// The fuzz targets below cost nothing in a normal `go test`: only their seed
// corpus is replayed. CI additionally runs scripts/fuzz-smoke.sh, which gives
// every target it discovers a fixed mutation budget per push -- point -fuzz at
// one of them to dig deeper locally.

// FuzzDecodeHeader throws arbitrary bytes at the header parser: it may
// reject anything, but it must never panic, never return a header
// alongside an error, and never produce a header that violates the
// length contract.
func FuzzDecodeHeader(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, HeaderSize))
	f.Add([]byte{byte(REQUEST), 0, 0, 0, 0, 1, 0, 0, 0, 4, 'x', 'x', 'x', 'x'})
	huge := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(huge[6:10], uint32(MaxFrameSize)+1)
	f.Add(huge)
	negative := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(negative[6:10], 0xffffffff) // -1 as int32
	f.Add(negative)
	f.Add([]byte{byte(PUBLISH), FlagMore, 0, 0, 0, 9, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		header, err := DecodeHeader(data)
		if err != nil {
			if header != nil {
				t.Fatalf("DecodeHeader returned a header alongside an error: %+v", header)
			}
			return
		}
		if len(data) < HeaderSize {
			t.Fatalf("DecodeHeader accepted a %d-byte header, want >= %d", len(data), HeaderSize)
		}
		if header.Length < 0 || header.Length > MaxFrameSize {
			t.Fatalf("DecodeHeader accepted out-of-range length %d", header.Length)
		}
	})
}

// FuzzDecodeStream feeds arbitrary bytes to the frame decoder: hostile
// input must surface as an error (never a panic), and the error surface
// stays closed — only the documented outcomes may come out.
func FuzzDecodeStream(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{byte(REQUEST), 0})
	f.Add([]byte{byte(REQUEST), 0, 0, 0, 0, 1, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{byte(REQUEST), 0, 0, 0, 0, 1, 0, 0, 0, 99, 'x'})

	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := Decode(bytes.NewReader(data))
		switch {
		case err == nil:
			if frame == nil {
				t.Fatal("Decode returned neither error nor frame")
			}
			if int(frame.Header.Length) != len(frame.Payload) {
				t.Fatalf("announced length %d != payload %d", frame.Header.Length, len(frame.Payload))
			}
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF),
			errors.Is(err, ErrFrameTooLarge):
			// the documented outcomes for truncated or oversized input
		default:
			t.Fatalf("error outside the documented surface: %v", err)
		}
	})
}

// FuzzFrameRoundTrip pins the codec from the other side: any header and
// payload that Encode accepts must decode back field-identical.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add(uint8(REQUEST), uint8(0), uint32(7), []byte("hello"))
	f.Add(uint8(HELLO), uint8(FlagMore), uint32(0xffffffff), []byte("x"))
	f.Add(uint8(PUBLISH), uint8(0xff), uint32(0), []byte("payload"))

	f.Fuzz(func(t *testing.T, ft, flags uint8, streamId uint32, payload []byte) {
		if len(payload) == 0 || len(payload) > MaxFrameSize {
			t.Skip("outside the encoder's contract")
		}
		original := &Frame{
			Header: FrameHeader{
				FrameType: FrameType(ft),
				Flags:     flags,
				StreamId:  streamId,
				Length:    int32(len(payload)),
			},
			Payload: payload,
		}
		encoded, err := Encode(original)
		if err != nil {
			t.Fatalf("Encode(): %v", err)
		}
		decoded, err := Decode(bytes.NewReader(encoded))
		if err != nil {
			t.Fatalf("Decode(): %v", err)
		}
		if decoded.Header != original.Header {
			t.Fatalf("header changed across the round trip: %+v -> %+v", original.Header, decoded.Header)
		}
		if !bytes.Equal(decoded.Payload, payload) {
			t.Fatalf("payload corrupted across the round trip")
		}
	})
}

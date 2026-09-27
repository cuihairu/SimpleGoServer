package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrFrameTooLarge means the peer announced a frame exceeding MaxFrameSize.
	ErrFrameTooLarge = errors.New("proto: frame exceeds maximum size")
	// ErrEmptyFrame means a frame was written without any payload.
	ErrEmptyFrame = errors.New("proto: frame payload is empty")
)

// Encode serializes a frame into a freshly allocated buffer:
// the fixed header followed by the payload.
func Encode(frame *Frame) ([]byte, error) {
	if frame == nil {
		return nil, errors.New("proto: frame is nil")
	}
	if len(frame.Payload) == 0 {
		return nil, ErrEmptyFrame
	}
	if int64(len(frame.Payload)) > int64(MaxFrameSize) {
		return nil, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, len(frame.Payload), MaxFrameSize)
	}
	buf := make([]byte, HeaderSize+len(frame.Payload))
	EncodeHeaderTo(buf, &frame.Header)
	copy(buf[HeaderSize:], frame.Payload)
	return buf, nil
}

// EncodeHeaderTo writes the 10-byte big-endian header into dst,
// which must be at least HeaderSize long.
func EncodeHeaderTo(dst []byte, header *FrameHeader) {
	dst[0] = byte(header.FrameType)
	dst[1] = header.Flags
	binary.BigEndian.PutUint32(dst[2:6], header.StreamId)
	binary.BigEndian.PutUint32(dst[6:10], uint32(header.Length)) // #nosec G115 -- Length was validated against MaxFrameSize by Encode
}

// DecodeHeader parses a header from src, which must be at least HeaderSize long.
func DecodeHeader(src []byte) (*FrameHeader, error) {
	var h FrameHeader
	if err := parseHeaderInto(src, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// parseHeaderInto is the allocation-free core of DecodeHeader: the pooled
// decode path parses straight into the frame's own header, skipping both
// the scratch buffer and the intermediate FrameHeader a fresh decode used
// to pay for.
func parseHeaderInto(src []byte, h *FrameHeader) error {
	if len(src) < HeaderSize {
		return fmt.Errorf("proto: short header %d < %d", len(src), HeaderSize)
	}
	length := int32(binary.BigEndian.Uint32(src[6:10])) // #nosec G115 -- deliberate wrap: high bits become a negative length, rejected by the < 0 check below
	if length < 0 || length > MaxFrameSize {
		return fmt.Errorf("%w: announced %d, limit %d", ErrFrameTooLarge, length, MaxFrameSize)
	}
	h.FrameType = FrameType(src[0])
	h.Flags = src[1]
	h.StreamId = binary.BigEndian.Uint32(src[2:6])
	h.Length = length
	return nil
}

// Decode reads exactly one frame from r. On a clean stream end between frames
// it returns io.EOF; a stream ending mid-frame yields io.ErrUnexpectedEOF so
// callers can tell a graceful close from a truncated message.
//
// The returned frame is freshly allocated. The server pipeline decodes
// through the same core with a pool backend (see pool.go); this public path
// stays pool-free because its callers — the client read loop above all — hand
// payload aliases to application code.
func Decode(r io.Reader) (*Frame, error) {
	return decodeFrame(r, nil)
}

// decodeFrame is the one frame reader both paths share. With a backend it
// takes the frame object and its payload buffer from that pool and releases
// them again on every error exit; with a nil backend the behavior is the
// historical exact-size allocation. Errors are identical on both paths.
func decodeFrame(r io.Reader, b poolBackend) (*Frame, error) {
	// the plain path allocates its own frame; the pooled path must not, or
	// every decode would pay an allocation it immediately discards
	var f *Frame
	if b != nil {
		f = acquireFrame(b)
	} else {
		f = &Frame{}
	}
	if _, err := io.ReadFull(r, f.scratch[:]); err != nil {
		f.release(b)
		if err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	if err := parseHeaderInto(f.scratch[:], &f.Header); err != nil {
		f.release(b)
		return nil, err
	}
	switch {
	case b != nil && f.Header.Length > 0:
		acquirePayload(b, f, int(f.Header.Length)) // #nosec G115 -- parseHeaderInto bounds Length to [0, MaxFrameSize]
	case b == nil:
		f.Payload = make([]byte, f.Header.Length)
	}
	if n, err := io.ReadFull(r, f.Payload); err != nil {
		readErr := err
		if readErr == io.EOF {
			readErr = io.ErrUnexpectedEOF
		}
		truncated := fmt.Errorf("proto: truncated payload (%d/%d): %w", n, f.Header.Length, readErr)
		f.release(b)
		return nil, truncated
	}
	return f, nil
}

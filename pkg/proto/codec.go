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
	binary.BigEndian.PutUint32(dst[6:10], uint32(header.Length))
}

// DecodeHeader parses a header from src, which must be at least HeaderSize long.
func DecodeHeader(src []byte) (*FrameHeader, error) {
	if len(src) < HeaderSize {
		return nil, fmt.Errorf("proto: short header %d < %d", len(src), HeaderSize)
	}
	length := int32(binary.BigEndian.Uint32(src[6:10]))
	if length < 0 || length > MaxFrameSize {
		return nil, fmt.Errorf("%w: announced %d, limit %d", ErrFrameTooLarge, length, MaxFrameSize)
	}
	return &FrameHeader{
		FrameType: FrameType(src[0]),
		Flags:     src[1],
		StreamId:  binary.BigEndian.Uint32(src[2:6]),
		Length:    length,
	}, nil
}

// Decode reads exactly one frame from r. On a clean stream end between frames
// it returns io.EOF; a stream ending mid-frame yields io.ErrUnexpectedEOF so
// callers can tell a graceful close from a truncated message.
func Decode(r io.Reader) (*Frame, error) {
	headerBuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	header, err := DecodeHeader(headerBuf)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, header.Length)
	if n, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("proto: truncated payload (%d/%d): %w", n, header.Length, err)
	}
	return &Frame{Header: *header, Payload: payload}, nil
}

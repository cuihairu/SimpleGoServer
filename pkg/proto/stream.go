package proto

import (
	"errors"
	"fmt"
	"io"
)

// FlagMore marks a frame as a fragment that is followed by more fragments
// of the same logical message (same stream id). The last fragment — like
// every ordinary single-frame message — carries no flag, so streaming is
// fully backwards compatible: peers that never fragment produce byte
// streams that a streaming-unaware reader decodes unchanged.
const FlagMore uint8 = 0x01

// MaxStreamSize bounds the assembled size of a streamed message. Single
// frames stay bounded by MaxFrameSize; this is the limit for what many of
// them may add up to.
const MaxStreamSize = 8 << 20 // 8MB

// ErrStreamTooLarge means a streamed message exceeded MaxStreamSize.
var ErrStreamTooLarge = errors.New("proto: streamed message exceeds maximum size")

// streamViolationError marks an illegal fragment sequence. It wraps the
// concrete reason; callers only need to know that the peer broke the
// framing contract and the connection cannot be trusted anymore.
func streamViolation(reason string) error {
	return fmt.Errorf("proto: stream violation: %s", reason)
}

// DecodeStreamed reads one logical message from r. A frame carrying
// FlagMore is a fragment: further frames with the same stream id and type
// follow, and their payloads are assembled into the returned frame (whose
// header reports the total length and no flags).
//
// The check for fragment continuity is strict — a mismatched stream id or
// type mid-stream is an error, because after interleaved fragments the
// byte stream cannot be trusted. The assembled size is bounded by
// MaxStreamSize.
func DecodeStreamed(r io.Reader) (*Frame, error) {
	frame, err := Decode(r)
	if err != nil {
		return nil, err
	}
	if frame.Header.Flags&FlagMore == 0 {
		return frame, nil
	}
	streamId := frame.Header.StreamId
	frameType := frame.Header.FrameType
	// alias the first fragment instead of copying it: its buffer came
	// freshly allocated from Decode and has no other owner, so the
	// appends below can grow it without anyone observing the old slice
	buf := frame.Payload
	for {
		next, err := Decode(r)
		if err != nil {
			return nil, err
		}
		if next.Header.StreamId != streamId {
			return nil, streamViolation(fmt.Sprintf("fragment stream id %d, want %d", next.Header.StreamId, streamId))
		}
		if next.Header.FrameType != frameType {
			return nil, streamViolation(fmt.Sprintf("fragment type %s mid-stream, want %s", next.Header.FrameType, frameType))
		}
		buf = append(buf, next.Payload...)
		if len(buf) > MaxStreamSize {
			return nil, ErrStreamTooLarge
		}
		if next.Header.Flags&FlagMore == 0 {
			return &Frame{
				Header: FrameHeader{
					FrameType: frameType,
					StreamId:  streamId,
					Length:    int32(len(buf)),
				},
				Payload: buf,
			}, nil
		}
	}
}

// defaultStreamThreshold is the payload size above which outbound frames
// are split into fragments: staying under MaxFrameSize with room to spare.
const defaultStreamThreshold = MaxFrameSize - 64*1024

// encodeStreamFrames turns one logical payload into the encoded bytes of
// one or more wire frames. Payloads up to the threshold produce a single
// plain frame; larger ones produce fragments — every frame but the last
// flagged with FlagMore, all sharing the stream id. Each returned buffer
// is written as one unit, in order.
func encodeStreamFrames(t FrameType, streamId uint32, payload []byte, threshold int) ([][]byte, error) {
	if len(payload) <= threshold {
		buf, err := Encode(newFrame(t, streamId, payload))
		if err != nil {
			return nil, err
		}
		return [][]byte{buf}, nil
	}
	var frames [][]byte
	for offset := 0; offset < len(payload); {
		end := offset + threshold
		if end > len(payload) {
			end = len(payload)
		}
		f := newFrame(t, streamId, payload[offset:end])
		if end < len(payload) {
			f.Header.Flags |= FlagMore
		}
		buf, err := Encode(f)
		if err != nil {
			return nil, err
		}
		frames = append(frames, buf)
		offset = end
	}
	return frames, nil
}

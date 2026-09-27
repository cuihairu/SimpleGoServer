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
//
// The returned frame is freshly allocated; the server pipeline passes a pool
// backend instead of nil, which recycles fragment buffers as soon as their
// bytes are appended to the aggregate. Both share this body.
func DecodeStreamed(r io.Reader) (*Frame, error) {
	return decodeStreamed(r, nil)
}

// decodeStreamed is the shared body of the pooled and plain readers. With a
// backend, frames and payload buffers come from that pool and fragment frames
// are released as soon as their bytes have been appended — a streamed message
// no longer leaves one dead buffer per fragment behind it. The caller owns
// releasing the returned frame when it is done (FrameCodec does so right
// after dispatch returns).
func decodeStreamed(r io.Reader, b poolBackend) (*Frame, error) {
	frame, err := decodeFrame(r, b)
	if err != nil {
		return nil, err
	}
	if frame.Header.Flags&FlagMore == 0 {
		return frame, nil
	}
	streamId := frame.Header.StreamId
	frameType := frame.Header.FrameType
	// alias the first fragment instead of copying it: its buffer came
	// freshly allocated (or from the pool, exclusively owned either way)
	// from decodeFrame and has no other owner, so the appends below can
	// grow it without anyone observing the old slice
	buf := frame.Payload
	for {
		next, err := decodeFrame(r, b)
		if err != nil {
			// the stream is broken: the aggregate, the first fragment's
			// buffer and the frame object all die here — release them
			frame.release(b)
			return nil, err
		}
		if next.Header.StreamId != streamId {
			violation := streamViolation(fmt.Sprintf("fragment stream id %d, want %d", next.Header.StreamId, streamId))
			next.release(b)
			frame.release(b)
			return nil, violation
		}
		if next.Header.FrameType != frameType {
			violation := streamViolation(fmt.Sprintf("fragment type %s mid-stream, want %s", next.Header.FrameType, frameType))
			next.release(b)
			frame.release(b)
			return nil, violation
		}
		// read the flag before releasing: release zeroes the header
		last := next.Header.Flags&FlagMore == 0
		buf = append(buf, next.Payload...)
		// the fragment's bytes now live in the aggregate; its frame and
		// buffer are dead, so recycle them immediately instead of waiting
		// for the GC to notice
		next.release(b)
		if len(buf) > MaxStreamSize {
			frame.release(b)
			return nil, ErrStreamTooLarge
		}
		if last {
			// the aggregate reuses the first fragment's frame object; if
			// append moved to a grown array, release's backing check recycles
			// both the original and the grown buffer
			frame.Payload = buf
			frame.Header = FrameHeader{
				FrameType: frameType,
				StreamId:  streamId,
				// buf is the aggregate, not one fragment: it fits int32
				// because the ErrStreamTooLarge check above caps it at
				// MaxStreamSize (8 MiB), far below math.MaxInt32.
				Length: int32(len(buf)), // #nosec G115
			}
			return frame, nil
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
//
// Buffers are exact-size allocations; the pipeline's hot paths pass a pool
// backend instead, which produces identical bytes (see pool.go).
func encodeStreamFrames(t FrameType, streamId uint32, payload []byte, threshold int) ([][]byte, error) {
	msg, err := encodeMessage(t, streamId, payload, threshold, nil)
	if err != nil {
		return nil, err
	}
	return msg.buffers(), nil
}

// encodedMessage is one logical message's wire form: the encoded buffers plus
// the pool cells that own them. A payload up to the stream threshold — which
// is nearly all traffic — is carried in buf/cell with no slice header at all,
// so the pooled encode path allocates nothing for bookkeeping (the earlier
// shape returned two parallel slices, which cost more than the encode it
// described). Only the fragmented case fills bufs/cells, sized exactly once.
type encodedMessage struct {
	buf   []byte   // single-buffer case
	cell  *[]byte  // the cell buf came from; nil for an ordinary allocation
	bufs  [][]byte // fragmented case, parallel to cells
	cells []*[]byte
}

// buffers returns every encoded buffer as one slice, for callers that want a
// uniform view. It allocates in the single-buffer case, which is why the
// encode hot paths read buf directly instead.
func (m *encodedMessage) buffers() [][]byte {
	if m.buf != nil {
		return [][]byte{m.buf}
	}
	return m.bufs
}

// release returns the message's buffers to the pool; a nil cell means the
// buffer was an ordinary allocation and its array goes to the GC. It is
// idempotent, like Frame.release: the fields are cleared so a second call
// cannot hand the same array to two future acquires.
func (m *encodedMessage) release(b poolBackend) {
	if m.buf != nil {
		putPayloadArray(b, m.buf, m.cell)
		m.buf, m.cell = nil, nil
		return
	}
	for i, cell := range m.cells {
		if cell == nil {
			continue
		}
		putPayloadArray(b, m.bufs[i], cell)
	}
	m.bufs, m.cells = nil, nil
}

// encodeMessage encodes one logical message, pooling through b when it is
// non-nil. The result is owned by the caller, which must release it once the
// bytes have been written (see pool.go for who may release when).
func encodeMessage(t FrameType, streamId uint32, payload []byte, threshold int, b poolBackend) (encodedMessage, error) {
	// a fragment may never exceed MaxFrameSize, so the threshold is clamped
	// into [1, MaxFrameSize]: a caller asking for a bigger or non-positive
	// fragment size gets the frame ceiling rather than a stream no decoder
	// could accept
	if threshold <= 0 || threshold > MaxFrameSize {
		threshold = MaxFrameSize
	}
	if len(payload) <= threshold {
		if b == nil {
			buf, err := Encode(newFrame(t, streamId, payload))
			if err != nil {
				return encodedMessage{}, err
			}
			return encodedMessage{buf: buf}, nil
		}
		buf, cell, err := encodeFrameBuffered(b, t, streamId, payload, 0)
		if err != nil {
			return encodedMessage{}, err
		}
		return encodedMessage{buf: buf, cell: cell}, nil
	}
	// exact fragment count up front: two backing arrays for the whole message
	// instead of an append-growth step per fragment
	fragments := (len(payload) + threshold - 1) / threshold
	msg := encodedMessage{
		bufs:  make([][]byte, 0, fragments),
		cells: make([]*[]byte, 0, fragments),
	}
	for offset := 0; offset < len(payload); {
		end := offset + threshold
		if end > len(payload) {
			end = len(payload)
		}
		flags := uint8(0)
		if end < len(payload) {
			flags = FlagMore
		}
		// no error to handle: with the clamped threshold every fragment is
		// non-empty and at most MaxFrameSize, which is exactly the range
		// encodeFrameInto accepts without complaint
		buf, cell := encodeFrameInto(b, t, streamId, payload[offset:end], flags)
		msg.bufs = append(msg.bufs, buf)
		msg.cells = append(msg.cells, cell)
		offset = end
	}
	return msg, nil
}

// encodeFrameBuffered encodes one wire frame into a pooled buffer, the
// pooled twin of Encode (same validation, same bytes). A cell of nil means
// the buffer is an ordinary allocation — total sizes above the largest
// class fall back to that instead of refusing to encode.
func encodeFrameBuffered(b poolBackend, t FrameType, streamId uint32, payload []byte, flags uint8) ([]byte, *[]byte, error) {
	if len(payload) == 0 {
		return nil, nil, ErrEmptyFrame
	}
	if int64(len(payload)) > int64(MaxFrameSize) {
		return nil, nil, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, len(payload), MaxFrameSize)
	}
	buf, cell := encodeFrameInto(b, t, streamId, payload, flags)
	return buf, cell, nil
}

// encodeFrameInto is the encoding half of encodeFrameBuffered, with a nil
// backend meaning "no pool". The fragment loop uses it directly: the clamped
// fragment size puts every fragment inside the accepted range, so there is
// nothing left to reject, and a dead error branch there would be code no test
// could ever reach.
func encodeFrameInto(b poolBackend, t FrameType, streamId uint32, payload []byte, flags uint8) ([]byte, *[]byte) {
	total := HeaderSize + len(payload)
	header := FrameHeader{
		FrameType: t,
		Flags:     flags,
		StreamId:  streamId,
		Length:    int32(len(payload)), // #nosec G115 -- callers pass at most MaxFrameSize bytes
	}
	if b != nil {
		if idx := classIndexForSize(total); idx >= 0 {
			cell := b.getPayload(idx)
			buf := (*cell)[:total]
			EncodeHeaderTo(buf, &header)
			copy(buf[HeaderSize:], payload)
			return buf, cell
		}
	}
	// no pool, or a total above the largest class: exact-size allocation
	buf := make([]byte, total)
	EncodeHeaderTo(buf, &header)
	copy(buf[HeaderSize:], payload)
	return buf, nil
}

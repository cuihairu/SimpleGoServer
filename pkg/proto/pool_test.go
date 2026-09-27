package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
)

// ---- test seam -------------------------------------------------------------
//
// sync.Pool makes no promise to hand back what was put in, so "did the
// release route to the right class" cannot be asserted against the real
// backend. The fake records routing decisions; sync.Pool's own recycle
// semantics are the standard library's problem.

type fakePools struct {
	getFrames   int
	putFrames   int
	getPayloads []int // class indexes acquired, in order
	putPayloads []int // class indexes released, in order
}

func (f *fakePools) getFrame() *Frame {
	f.getFrames++
	return &Frame{}
}
func (f *fakePools) putFrame(*Frame) { f.putFrames++ }

func (f *fakePools) getPayload(idx int) *[]byte {
	f.getPayloads = append(f.getPayloads, idx)
	cell := new([]byte)
	*cell = make([]byte, payloadClasses[idx])
	return cell
}

func (f *fakePools) putPayload(idx int, cell *[]byte) {
	f.putPayloads = append(f.putPayloads, idx)
}

// poolBackend interface implementation
var _ poolBackend = (*fakePools)(nil)

func withFakePools(t *testing.T) *fakePools {
	t.Helper()
	return &fakePools{}
}

func intsEqual(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// ---- class routing ---------------------------------------------------------

func TestClassIndexForSize(t *testing.T) {
	cases := []struct {
		size int
		want int
	}{
		{0, -1}, {-4, -1},
		{1, 0}, {255, 0}, {256, 0},
		{257, 1}, {1024, 1},
		{1025, 2}, {4096, 2},
		{4097, 3}, {16384, 3},
		{16385, 4}, {65536, 4},
		{65537, 5}, {262144, 5},
		{262145, 6}, {1 << 20, 6},
		{1<<20 + 1, -1}, {8 << 20, -1},
	}
	for _, tc := range cases {
		if got := classIndexForSize(tc.size); got != tc.want {
			t.Errorf("classIndexForSize(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

func TestClassIndexForCap(t *testing.T) {
	cases := []struct {
		cap  int
		want int
	}{
		{0, -1}, {100, -1}, // below the smallest class
		{255, -1}, {256, 0}, // boundary into the smallest class
		{300, 0}, {1024, 1}, // a 300B buffer still serves the 256B class
		{1025, 1}, {4096, 2}, // capacities between classes serve the one below
		{8192, 2}, {16384, 3}, // grown-to-8192 serves 4096
		{1 << 20, 6},                   // the largest class
		{1<<20 + 1, -1}, {2 << 20, -1}, // above the ceiling: drop, do not park
	}
	for _, tc := range cases {
		if got := classIndexForCap(tc.cap); got != tc.want {
			t.Errorf("classIndexForCap(%d) = %d, want %d", tc.cap, got, tc.want)
		}
	}
}

// ---- frame acquire/release semantics ---------------------------------------

func TestPooledDecodeRoundTripAndIdempotentRelease(t *testing.T) {
	fake := withFakePools(t)

	payload := bytes.Repeat([]byte("ab"), 150) // 300 bytes -> class 1KiB (idx 1)
	wire, err := Encode(newFrame(REQUEST, 9, payload))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	frame, err := decodeStreamed(bytes.NewReader(wire), fake)
	if err != nil {
		t.Fatalf("decodeStreamed(pooled): %v", err)
	}
	if !frame.fromPool {
		t.Fatal("decodeStreamed(pooled) returned a frame not marked as pooled")
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatalf("payload mismatch: %d bytes, want %d", len(frame.Payload), len(payload))
	}
	if !intsEqual(fake.getPayloads, []int{1}) {
		t.Fatalf("payload acquired from classes %v, want [1]", fake.getPayloads)
	}

	frame.release(fake)
	if !intsEqual(fake.putPayloads, []int{1}) {
		t.Fatalf("release routed to classes %v, want [1]", fake.putPayloads)
	}
	if fake.putFrames != 1 {
		t.Fatalf("release returned %d frames, want 1", fake.putFrames)
	}
	if frame.Payload != nil || frame.Header != (FrameHeader{}) {
		t.Fatal("release must zero the frame so use-after-release fails loudly")
	}

	// a second release must be a no-op: double-putting would alias one
	// object to two future acquires
	frame.release(fake)
	if fake.putFrames != 1 || len(fake.putPayloads) != 1 {
		t.Fatalf("double release recycled again: putFrames=%d putPayloads=%v", fake.putFrames, fake.putPayloads)
	}
}

func TestReleaseIsNoOpOnPlainFrames(t *testing.T) {
	fake := withFakePools(t)
	frame := &Frame{Header: FrameHeader{FrameType: PING, StreamId: 3}, Payload: []byte("keep")}
	frame.release(fake)
	if frame.Payload == nil || frame.Header.StreamId != 3 {
		t.Fatal("release must not touch frames that never came from the pool")
	}
	var nilFrame *Frame
	nilFrame.release(fake) // nil must not panic either
}

// ---- aggregation recycling -------------------------------------------------

// encodeFragment builds one wire frame by hand; Encode refuses empty
// payloads, but aggregation tests need explicit control over flags.
func encodeFragment(t *testing.T, id uint32, more bool, payload []byte) []byte {
	t.Helper()
	header := FrameHeader{FrameType: REQUEST, StreamId: id, Length: int32(len(payload))}
	if more {
		header.Flags = FlagMore
	}
	buf := make([]byte, HeaderSize+len(payload))
	EncodeHeaderTo(buf, &header)
	copy(buf[HeaderSize:], payload)
	return buf
}

func TestAggregationRecyclesFragmentsDuringAssembly(t *testing.T) {
	fake := withFakePools(t)

	// three fragments with distinct size classes: 100B (idx 0, becomes the
	// aggregate), 300B (idx 1), 6000B (idx 3, since 6000 > 4096)
	var wire []byte
	wire = append(wire, encodeFragment(t, 5, true, bytes.Repeat([]byte("a"), 100))...)
	wire = append(wire, encodeFragment(t, 5, true, bytes.Repeat([]byte("b"), 300))...)
	wire = append(wire, encodeFragment(t, 5, false, bytes.Repeat([]byte("c"), 6000))...)

	frame, err := decodeStreamed(bytes.NewReader(wire), fake)
	if err != nil {
		t.Fatalf("decodeStreamed(pooled): %v", err)
	}
	want := append(append(bytes.Repeat([]byte("a"), 100), bytes.Repeat([]byte("b"), 300)...), bytes.Repeat([]byte("c"), 6000)...)
	if !bytes.Equal(frame.Payload, want) {
		t.Fatalf("aggregate mismatch: %d bytes, want %d", len(frame.Payload), len(want))
	}
	// fragments 2 and 3 were recycled during the loop, before the caller
	// even saw the assembled frame
	if !intsEqual(fake.putPayloads, []int{1, 3}) || fake.putFrames != 2 {
		t.Fatalf("mid-assembly recycling: putPayloads=%v putFrames=%d, want [1 3]/2", fake.putPayloads, fake.putFrames)
	}

	frame.release(fake)
	// the aggregate outgrew the first fragment's 256B buffer: release must
	// recycle the original array (class 0) and the grown array (cap 6400
	// serves class 2 because 4096 is the largest class <= 6400), in that order
	if !intsEqual(fake.putPayloads, []int{1, 3, 0, 2}) {
		t.Fatalf("aggregate release routing: putPayloads=%v, want [1 3 0 2]", fake.putPayloads)
	}
	if fake.putFrames != 3 {
		t.Fatalf("putFrames = %d, want 3 (two fragments + aggregate)", fake.putFrames)
	}
}

func TestStreamDecodeErrorRecyclesFirstFragment(t *testing.T) {
	fake := withFakePools(t)
	wire := encodeFragment(t, 7, true, []byte("orphan")) // promised more, then EOF
	_, err := decodeStreamed(bytes.NewReader(wire), fake)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	// Two frames released, one payload: the frame decodeFrame had just acquired
	// when the *header* read hit EOF (codec.go:98 — it never reached
	// acquirePayload, so it contributes no payload put), and the first fragment
	// this call was holding for aggregation (stream.go:74).
	if !intsEqual(fake.putPayloads, []int{0}) || fake.putFrames != 2 {
		t.Fatalf("error path recycling: putPayloads=%v putFrames=%d, want [0]/2", fake.putPayloads, fake.putFrames)
	}
}

func TestStreamViolationRecyclesBothFrames(t *testing.T) {
	fake := withFakePools(t)
	var wire []byte
	wire = append(wire, encodeFragment(t, 1, true, []byte("first"))...)
	wire = append(wire, encodeFragment(t, 2, false, []byte("wrong-id"))...)
	if _, err := decodeStreamed(bytes.NewReader(wire), fake); err == nil {
		t.Fatal("mismatched fragment stream id must fail")
	}
	if fake.putFrames != 2 || !intsEqual(fake.putPayloads, []int{0, 0}) {
		t.Fatalf("violation recycling: putFrames=%d putPayloads=%v, want 2/[0 0]", fake.putFrames, fake.putPayloads)
	}

	// type mismatch mid-stream recycles the same way
	fake.putFrames, fake.putPayloads = 0, nil
	wire = wire[:0]
	wire = append(wire, encodeFragment(t, 1, true, []byte("first"))...)
	bad := encodeFragment(t, 1, false, []byte("wrong-type"))
	bad[0] = byte(PING) // same id, different frame type
	wire = append(wire, bad...)
	if _, err := decodeStreamed(bytes.NewReader(wire), fake); err == nil {
		t.Fatal("mismatched fragment type must fail")
	}
	if fake.putFrames != 2 {
		t.Fatalf("type-violation putFrames = %d, want 2", fake.putFrames)
	}
}

func TestOversizeAggregateIsNotParked(t *testing.T) {
	fake := withFakePools(t)
	// nine 1MiB fragments: the ninth append crosses MaxStreamSize and the
	// grown aggregate (far past the largest class) must go to the GC, not
	// the pools. Fragments 2..9 are recycled during the loop (8 puts); the
	// first fragment's original buffer is recycled when the frame is
	// released on the error path (1 put) — all class 6.
	var wire []byte
	mib := bytes.Repeat([]byte("z"), 1<<20)
	for i := 0; i < 9; i++ {
		wire = append(wire, encodeFragment(t, 3, i < 8, mib)...)
	}
	_, err := decodeStreamed(bytes.NewReader(wire), fake)
	if !errors.Is(err, ErrStreamTooLarge) {
		t.Fatalf("err = %v, want ErrStreamTooLarge", err)
	}
	for _, idx := range fake.putPayloads {
		if idx != len(payloadClasses)-1 {
			t.Fatalf("oversize aggregate was parked in class %d; only 1MiB recycles may appear (puts=%v)", idx, fake.putPayloads)
		}
	}
	if len(fake.putPayloads) != 9 {
		t.Fatalf("putPayloads = %v, want nine 1MiB recycles (8 fragments + the original first-fragment buffer)", fake.putPayloads)
	}
}

// ---- decode edge: zero-length payloads skip the payload pool ---------------

func TestPooledDecodeZeroLengthPayload(t *testing.T) {
	fake := withFakePools(t)
	header := FrameHeader{FrameType: PING, StreamId: 4}
	buf := make([]byte, HeaderSize)
	EncodeHeaderTo(buf, &header)
	frame, err := decodeFrame(bytes.NewReader(buf), fake)
	if err != nil {
		t.Fatalf("decodeFrame(pooled): %v", err)
	}
	if frame.Payload != nil {
		t.Fatalf("zero-length payload = %v, want nil", frame.Payload)
	}
	if len(fake.getPayloads) != 0 {
		t.Fatalf("zero-length payload acquired from classes %v, want none", fake.getPayloads)
	}
	frame.release(fake)
	if len(fake.putPayloads) != 0 {
		t.Fatalf("release put payload classes %v, want none", fake.putPayloads)
	}

	// the plain path keeps its historical make() behavior for empty payloads
	frame2, err := Decode(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("Decode(): %v", err)
	}
	if frame2.Payload == nil || len(frame2.Payload) != 0 {
		t.Fatalf("plain zero-length payload = %v, want non-nil empty", frame2.Payload)
	}
}

// ---- parity: pooled and plain encoders emit identical bytes -----------------

func TestPooledAndPlainEncodersAgree(t *testing.T) {
	fake := withFakePools(t)
	// the sizes include both sides of the threshold: that boundary is where the
	// single-buffer and fragmented shapes meet, so parity has to hold there too
	const threshold = 64 * 1024
	for _, size := range []int{1, 10, 1024, threshold, threshold + 1, 70 * 1024, MaxFrameSize} {
		payload := bytes.Repeat([]byte("q"), size)
		plain, err := encodeStreamFrames(REQUEST, 77, payload, threshold)
		if err != nil {
			t.Fatalf("encodeStreamFrames(%d): %v", size, err)
		}
		msg, err := encodeMessage(REQUEST, 77, payload, threshold, fake)
		if err != nil {
			t.Fatalf("encodeMessage(%d): %v", size, err)
		}
		pooled := msg.buffers()
		if len(plain) != len(pooled) {
			t.Fatalf("size %d: %d plain buffers, %d pooled", size, len(plain), len(pooled))
		}
		for i := range plain {
			if !bytes.Equal(plain[i], pooled[i]) {
				t.Fatalf("size %d: pooled buffer %d differs from the plain encoder", size, i)
			}
		}
	}
}

// ---- codec release points --------------------------------------------------

// inboundStub is a minimal handler.InboundContext that captures the frame
// the codec propagates and snapshots its payload while dispatch is still on
// the stack.
type inboundStub struct {
	conn     net.Conn
	seen     *Frame
	snapshot []byte
}

func (s *inboundStub) Conn() net.Conn                   { return s.conn }
func (s *inboundStub) Handler() handler.Handler         { return nil }
func (s *inboundStub) Write(m handler.Message)          {}
func (s *inboundStub) Close(error)                      {}
func (s *inboundStub) Attachment() handler.Attachment   { return nil }
func (s *inboundStub) SetAttachment(handler.Attachment) {}
func (s *inboundStub) HandleRead(message handler.Message) {
	f, ok := message.(*Frame)
	if !ok {
		return
	}
	s.seen = f
	// copy now: by the time HandleRead returns to the codec the payload
	// has been released, so the snapshot proves what dispatch saw while it
	// was still valid
	s.snapshot = append([]byte(nil), f.Payload...)
}

func TestFrameCodecReleasesFrameAfterDispatch(t *testing.T) {
	fake := withFakePools(t)

	conn, peer := net.Pipe()
	defer func() { _ = conn.Close(); _ = peer.Close() }()

	payload := bytes.Repeat([]byte("rq"), 128) // 256B -> class 0
	wire, err := Encode(newFrame(REQUEST, 11, payload))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	go func() {
		_, _ = peer.Write(wire)
		_, _ = peer.Read(make([]byte, 1)) // hold the pipe open until close
	}()

	stub := &inboundStub{conn: conn}
	// Directly call the internal decode + dispatch to test the release point
	frame, err := decodeStreamed(conn, fake)
	if err != nil {
		t.Fatalf("decodeStreamed: %v", err)
	}
	stub.HandleRead(frame)
	frame.release(fake)

	if stub.seen == nil {
		t.Fatal("codec did not propagate the frame downstream")
	}
	if !bytes.Equal(stub.snapshot, payload) {
		t.Fatal("downstream saw wrong payload bytes during dispatch")
	}
	if stub.seen.Payload != nil || stub.seen.Header != (FrameHeader{}) {
		t.Fatal("codec must release (and zero) the frame once dispatch returns")
	}
	if fake.putFrames != 1 || !intsEqual(fake.putPayloads, []int{0}) {
		t.Fatalf("codec release routing: putFrames=%d putPayloads=%v, want 1/[0]", fake.putFrames, fake.putPayloads)
	}
}

// writeProbe is an OutboundContext that also records the pool's state at the
// exact moment the bytes are handed downstream. HandleWrite's two branches
// release in *opposite* orders — the single buffer after the write, the
// fragments before it — and that ordering is the difference between a recycled
// array still being written and one that is safely dead. A test that copies the
// encode/join/release sequence into itself cannot observe it, because nothing in
// that copy goes through the production branch.
type writeProbe struct {
	handler.OutboundContext
	fake        *fakePools
	written     []byte
	putsAtWrite int
}

func (p *writeProbe) HandleWrite(message handler.Message) {
	p.putsAtWrite = len(p.fake.putPayloads)
	if b, ok := message.([]byte); ok {
		p.written = append(p.written, b...)
	}
}

func TestFrameCodecHandleWriteReleasesSingleEncodedBuffer(t *testing.T) {
	fake := withFakePools(t)
	codec := NewFrameCodec()
	codec.pools = fake
	out := &writeProbe{fake: fake}
	payload := bytes.Repeat([]byte("p"), 40) // 50B total -> class 0
	frame := newFrame(RESPONSE, 2, payload)

	codec.HandleWrite(out, frame)

	if !bytes.Contains(out.written, payload) {
		t.Fatal("downstream write missed the payload")
	}
	if !intsEqual(fake.getPayloads, []int{0}) {
		t.Fatalf("encoded buffer acquire: getPayloads=%v, want [0]", fake.getPayloads)
	}
	if !intsEqual(fake.putPayloads, []int{0}) {
		t.Fatalf("encoded buffer routing: putPayloads=%v, want [0]", fake.putPayloads)
	}
	// The frame is the caller's, not the pool's: the outbound path must neither
	// borrow another frame nor recycle the one it was handed.
	if fake.getFrames != 0 || fake.putFrames != 0 {
		t.Fatalf("frame pool traffic on the write path: get=%d put=%d, want 0/0",
			fake.getFrames, fake.putFrames)
	}
	// Downstream still needs the bytes, so nothing may have been recycled yet.
	if out.putsAtWrite != 0 {
		t.Fatalf("%d buffer(s) were already recycled when the write happened; "+
			"the single-buffer path must release after it", out.putsAtWrite)
	}
}

func TestFrameCodecHandleWriteReleasesFragmentsAfterJoin(t *testing.T) {
	fake := withFakePools(t)
	codec := NewFrameCodecWithStreamThreshold(8)
	codec.pools = fake
	out := &writeProbe{fake: fake}
	payload := bytes.Repeat([]byte("f"), 20) // 3 fragments of <= 8B

	codec.HandleWrite(out, newFrame(REQUEST, 1, payload))

	if len(fake.putPayloads) != 3 {
		t.Fatalf("fragment puts = %v, want three recycles", fake.putPayloads)
	}
	// Every acquire comes back, in its own class: three fragments out of three
	// acquires, none recycled to a class it was not taken from.
	if !intsEqual(fake.getPayloads, fake.putPayloads) {
		t.Fatalf("fragment classes: get=%v put=%v, want each acquire returned "+
			"to the class it came from", fake.getPayloads, fake.putPayloads)
	}
	assembled, err := DecodeStreamed(bytes.NewReader(out.written))
	if err != nil {
		t.Fatalf("DecodeStreamed(): %v", err)
	}
	if !bytes.Equal(assembled.Payload, payload) {
		t.Fatal("joined bytes do not reassemble to the original payload")
	}
	// The opposite ordering from the single-buffer branch, and the reason both
	// need to run through HandleWrite rather than a copy in this file: the
	// fragments must already be back in the pool by the time the joined bytes
	// go downstream, because the join copied them.
	if out.putsAtWrite != 3 {
		t.Fatalf("%d fragment(s) recycled at write time, want all 3 already "+
			"returned before the write", out.putsAtWrite)
	}
	if fake.getFrames != 0 || fake.putFrames != 0 {
		t.Fatalf("frame pool traffic on the write path: get=%d put=%d, want 0/0",
			fake.getFrames, fake.putFrames)
	}
}

// TestEncodeMessageFragmentsPooled exercises the fragmented path through
// encodeMessage with a pool backend, ensuring the fragment loop is covered.
func TestEncodeMessageFragmentsPooled(t *testing.T) {
	fake := withFakePools(t)
	payload := bytes.Repeat([]byte("f"), 20) // 3 fragments with threshold=8
	msg, err := encodeMessage(REQUEST, 1, payload, 8, fake)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	if len(msg.buffers()) != 3 {
		t.Fatalf("expected 3 fragment buffers, got %d", len(msg.buffers()))
	}
	// All fragments should have come from the pool
	if len(fake.getPayloads) != 3 {
		t.Fatalf("expected 3 pool gets, got %v", fake.getPayloads)
	}
	msg.release(fake)
	if len(fake.putPayloads) != 3 {
		t.Fatalf("expected 3 pool puts, got %v", fake.putPayloads)
	}
}

// TestEncodeMessageAtThresholdIsOneFrameNotAStream pins the inclusive boundary
// in "payloads up to the threshold produce a single frame". The wire bytes are
// identical on both sides of that choice, so nothing but the shape can be wrong
// here -- and the shape is what ownership hangs on: buf/cell is the
// single-buffer case, whose buffer is released *after* the write, while
// bufs/cells is the fragmented case, recycled *before* it (see
// FrameCodec.HandleWrite). A payload of exactly the threshold drifting into the
// stream shape would therefore also move it to the other release order.
func TestEncodeMessageAtThresholdIsOneFrameNotAStream(t *testing.T) {
	const threshold = 8
	for _, tc := range []struct {
		name   string
		size   int
		single bool
		frags  int
	}{
		{"one below the threshold", threshold - 1, true, 1},
		{"exactly the threshold", threshold, true, 1},
		{"one over the threshold", threshold + 1, false, 2},
	} {
		fake := withFakePools(t)
		payload := bytes.Repeat([]byte("t"), tc.size)
		msg, err := encodeMessage(REQUEST, 3, payload, threshold, fake)
		if err != nil {
			t.Fatalf("%s: encodeMessage(): %v", tc.name, err)
		}
		if tc.single {
			// no bookkeeping slices at all: the shape the encodedMessage
			// comment promises for "nearly all traffic"
			if msg.buf == nil || len(msg.bufs) != 0 || cap(msg.bufs) != 0 || len(msg.cells) != 0 {
				t.Fatalf("%s: single-buffer shape violated: buf set = %v, bufs %d/%d, cells %d",
					tc.name, msg.buf != nil, len(msg.bufs), cap(msg.bufs), len(msg.cells))
			}
		} else if msg.buf != nil || len(msg.bufs) != tc.frags {
			t.Fatalf("%s: got %d fragments with single buffer set = %v, want %d fragments and no buf",
				tc.name, len(msg.bufs), msg.buf != nil, tc.frags)
		}
		buffers := msg.buffers()
		if len(buffers) != tc.frags {
			t.Fatalf("%s: %d buffers, want %d", tc.name, len(buffers), tc.frags)
		}
		if len(fake.getPayloads) != tc.frags {
			t.Fatalf("%s: acquired %v, want %d buffer(s)", tc.name, fake.getPayloads, tc.frags)
		}
		// the shape choice must not touch the wire form: every fragment but
		// the last carries FlagMore, and the payload reassembles
		for i, buf := range buffers {
			if more := buf[1]&FlagMore != 0; more != (i < len(buffers)-1) {
				t.Fatalf("%s: buffer %d FlagMore = %v, want %v", tc.name, i, more, i < len(buffers)-1)
			}
		}
		assembled, err := DecodeStreamed(bytes.NewReader(bytes.Join(buffers, nil)))
		if err != nil {
			t.Fatalf("%s: DecodeStreamed(): %v", tc.name, err)
		}
		if !bytes.Equal(assembled.Payload, payload) {
			t.Fatalf("%s: reassembled %q, want %q", tc.name, assembled.Payload, payload)
		}
		// and it must not move the bytes away from the plain encoder either
		plain, err := encodeStreamFrames(REQUEST, 3, payload, threshold)
		if err != nil {
			t.Fatalf("%s: encodeStreamFrames(): %v", tc.name, err)
		}
		if len(plain) != len(buffers) {
			t.Fatalf("%s: plain encoder made %d buffers, pooled made %d", tc.name, len(plain), len(buffers))
		}
		for i := range plain {
			if !bytes.Equal(plain[i], buffers[i]) {
				t.Fatalf("%s: buffer %d differs from the plain encoder", tc.name, i)
			}
		}
		msg.release(fake)
		if !intsEqual(fake.getPayloads, fake.putPayloads) {
			t.Fatalf("%s: released %v, want each acquire back in its own class %v",
				tc.name, fake.putPayloads, fake.getPayloads)
		}
	}
}

// TestEncodeMessageSizesFragmentArraysExactlyOnce pins encodeMessage's "exact
// fragment count up front" claim: both bookkeeping slices get their final size
// in one make(), so no append ever reallocates. Acquire counts, buffer counts
// and bytes all stay correct under a wrong precomputed count -- a truncated
// count is silently repaired by append's growth -- so capacity is the only
// thing that can see this, which is why it has its own test rather than being
// folded into the parity checks. The divisible cases guard the other direction:
// a count that overshoots would leave unused slots.
func TestEncodeMessageSizesFragmentArraysExactlyOnce(t *testing.T) {
	for _, tc := range []struct{ size, threshold, frags int }{
		{9, 8, 2},       // 1.125 fragments -> 2
		{16, 8, 2},      // exactly divisible
		{20, 8, 3},      // undercounting to 2 would grow to cap 4
		{1000, 100, 10}, // exactly divisible at a realistic size
		{1001, 100, 11},
	} {
		fake := withFakePools(t)
		msg, err := encodeMessage(REQUEST, 4, bytes.Repeat([]byte("z"), tc.size), tc.threshold, fake)
		if err != nil {
			t.Fatalf("size %d threshold %d: encodeMessage(): %v", tc.size, tc.threshold, err)
		}
		if len(msg.bufs) != tc.frags || len(msg.cells) != tc.frags {
			t.Fatalf("size %d threshold %d: %d bufs / %d cells, want %d of each",
				tc.size, tc.threshold, len(msg.bufs), len(msg.cells), tc.frags)
		}
		if cap(msg.bufs) != tc.frags || cap(msg.cells) != tc.frags {
			t.Fatalf("size %d threshold %d: cap(bufs)=%d cap(cells)=%d, want exactly %d -- a smaller "+
				"precomputed count is hidden by append growth, a larger one leaves dead slots",
				tc.size, tc.threshold, cap(msg.bufs), cap(msg.cells), tc.frags)
		}
		if len(fake.getPayloads) != tc.frags {
			t.Fatalf("size %d threshold %d: acquired %v, want %d buffers", tc.size, tc.threshold, fake.getPayloads, tc.frags)
		}
		msg.release(fake)
		if !intsEqual(fake.getPayloads, fake.putPayloads) {
			t.Fatalf("size %d threshold %d: released %v, want the acquired classes %v",
				tc.size, tc.threshold, fake.putPayloads, fake.getPayloads)
		}
	}
}

// TestEncodeMessageThresholdClamping verifies the threshold is clamped to
// [1, MaxFrameSize] as documented.
func TestEncodeMessageThresholdClamping(t *testing.T) {
	fake := withFakePools(t)
	payload := bytes.Repeat([]byte("x"), 100)

	// negative threshold -> clamped to MaxFrameSize (single buffer)
	msg, err := encodeMessage(REQUEST, 1, payload, -1, fake)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	if len(msg.buffers()) != 1 {
		t.Fatalf("negative threshold: expected 1 buffer, got %d", len(msg.buffers()))
	}
	msg.release(fake)

	// huge threshold -> clamped to MaxFrameSize (single buffer)
	msg, err = encodeMessage(REQUEST, 1, payload, MaxFrameSize*2, fake)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	if len(msg.buffers()) != 1 {
		t.Fatalf("huge threshold: expected 1 buffer, got %d", len(msg.buffers()))
	}
	msg.release(fake)

	// zero threshold -> clamped to MaxFrameSize
	msg, err = encodeMessage(REQUEST, 1, payload, 0, fake)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	if len(msg.buffers()) != 1 {
		t.Fatalf("zero threshold: expected 1 buffer, got %d", len(msg.buffers()))
	}
	msg.release(fake)
}

// TestEncodeMessageEncodeFrameIntoFallback exercises the fallback path in
// encodeFrameInto when the total frame size exceeds the largest pool class.
func TestEncodeMessageEncodeFrameIntoFallback(t *testing.T) {
	fake := withFakePools(t)
	// A single fragment larger than the largest class (1MiB payload + header > 1MiB)
	// This can't happen through encodeMessage because it clamps threshold to MaxFrameSize,
	// but encodeFrameInto is called directly by the fragment loop with clamped sizes.
	// To test the fallback, we call encodeFrameInto directly with a too-large payload.
	payload := make([]byte, MaxFrameSize+1)
	_, cell := encodeFrameInto(fake, REQUEST, 1, payload, 0)
	if cell != nil {
		t.Fatal("encodeFrameInto with over-class payload must return nil cell")
	}
	if len(fake.getPayloads) != 0 {
		t.Fatalf("expected no pool gets for over-class payload, got %v", fake.getPayloads)
	}
}

// TestEncodeMessageErrorPaths covers the error return from encodeFrameBuffered
// inside encodeMessage for empty payload. The oversized payload path is
// unreachable through encodeMessage because threshold is clamped to MaxFrameSize
// before the single-buffer path is taken, so payload <= MaxFrameSize is guaranteed.
func TestEncodeMessageErrorPaths(t *testing.T) {
	fake := withFakePools(t)

	// b == nil, empty payload -> Encode returns ErrEmptyFrame
	_, err := encodeMessage(REQUEST, 1, nil, 64*1024, nil)
	if !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("empty payload with nil backend: err = %v, want ErrEmptyFrame", err)
	}

	// b != nil, empty payload -> encodeFrameBuffered returns ErrEmptyFrame
	_, err = encodeMessage(REQUEST, 1, nil, 64*1024, fake)
	if !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("empty payload with pooled backend: err = %v, want ErrEmptyFrame", err)
	}
}

// ---- outbound encode edge cases --------------------------------------------

func TestEncodeFrameBufferedFallsBackAboveLargestClass(t *testing.T) {
	fake := withFakePools(t)
	payload := bytes.Repeat([]byte("x"), 1<<20) // header + 1MiB > the 1MiB class
	buf, cell, err := encodeFrameBuffered(fake, REQUEST, 8, payload, 0)
	if err != nil {
		t.Fatalf("encodeFrameBuffered(): %v", err)
	}
	if cell != nil {
		t.Fatal("an over-class buffer must not carry a pool cell")
	}
	if len(fake.getPayloads) != 0 {
		t.Fatalf("acquired from classes %v, want none", fake.getPayloads)
	}
	frame, err := Decode(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("Decode(): %v", err)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatal("fallback bytes differ from Encode output")
	}
}

func TestEncodeFrameBufferedValidatesLikeEncode(t *testing.T) {
	fake := withFakePools(t)
	if _, _, err := encodeFrameBuffered(fake, REQUEST, 1, nil, 0); !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("empty payload err = %v, want ErrEmptyFrame", err)
	}
	if _, _, err := encodeFrameBuffered(fake, REQUEST, 1, make([]byte, MaxFrameSize+1), 0); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversize payload err = %v, want ErrFrameTooLarge", err)
	}
	// flags must land in the encoded header (the fragment path depends on it)
	buf, _, err := encodeFrameBuffered(fake, REQUEST, 6, []byte("frag"), FlagMore)
	if err != nil {
		t.Fatalf("encodeFrameBuffered(): %v", err)
	}
	if buf[1]&FlagMore == 0 {
		t.Fatal("FlagMore missing from the encoded header")
	}
	if got := binary.BigEndian.Uint32(buf[2:6]); got != 6 {
		t.Fatalf("streamId = %d, want 6", got)
	}
}

func TestReleaseBuffersSkipsNilCells(t *testing.T) {
	fake := withFakePools(t)
	// A message over the threshold uses the fragmented path with explicit cells.
	// But we want to test the nil cell path: a message that falls back to
	// exact-size allocation because it's above the largest class.
	// Since encodeMessage clamps threshold to MaxFrameSize, we can't trigger
	// the fallback through the threshold. Instead, test the single-buffer
	// path with a nil backend to get a nil cell, then release through the
	// fake (which will be a no-op since cell is nil).
	msg, err := encodeMessage(REQUEST, 1, []byte("data"), 64*1024, nil)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	// cell is nil because backend was nil
	msg.release(fake) // nil cell path: putPayloadArray does nothing for nil cell
	if len(fake.putPayloads) != 0 {
		t.Fatal("a nil cell must not produce a pool put")
	}

	// Also test the fragmented case with nil cells (not really possible
	// through encodeMessage, but release handles it)
	msg2 := encodedMessage{
		bufs:  [][]byte{make([]byte, 32)},
		cells: []*[]byte{nil},
	}
	msg2.release(fake)
	if len(fake.putPayloads) != 0 {
		t.Fatal("a nil cell in fragmented path must not produce a pool put")
	}
}

// ---- end to end: premature release would corrupt the wire ------------------

// TestClientRoundTripReleasesBuffers drives a real Call through the pooled
// outbound path against a full protocol handler over net.Pipe. If roundTrip
// released its encoded buffers before the server consumed them, the echoed
// bytes would come back corrupted — correctness and recycling are asserted
// in the same run.
func TestClientRoundTripReleasesBuffers(t *testing.T) {
	fake := withFakePools(t)

	ph, clientConn := pipeServer(t, echoHandler)
	defer ph.Close()

	client := NewClient(clientConn, nil)
	// Override the client's pool backend with our fake
	client.pools = fake
	defer func() { _ = client.Close() }()

	resp, err := client.Call("echo", json.RawMessage(`"roundtrip"`), 2*time.Second)
	if err != nil {
		t.Fatalf("Call(): %v", err)
	}
	if got := string(resp.Data); got != `{"action":"echo","data":"roundtrip"}` {
		t.Fatalf("response data = %s, want the echoed envelope", got)
	}
	if len(fake.putPayloads) == 0 {
		t.Fatal("roundTrip never recycled its encoded buffers")
	}
}

// ---- pool reuse across frames ----------------------------------------------

// TestPooledDecodeReusesRecycledBuffers decodes two different frames back to
// back, releasing between them: the second decode's payload must be exactly
// its own bytes. A recycled buffer handed out still aliased to the first
// frame (or poisoned by stale contents beyond the new length) fails here.
func TestPooledDecodeReusesRecycledBuffers(t *testing.T) {
	fake := withFakePools(t)

	first := bytes.Repeat([]byte("1"), 200)
	second := bytes.Repeat([]byte("2"), 100)

	wire1, err := Encode(newFrame(REQUEST, 1, first))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	frame1, err := decodeStreamed(bytes.NewReader(wire1), fake)
	if err != nil {
		t.Fatalf("decode #1: %v", err)
	}
	if !bytes.Equal(frame1.Payload, first) {
		t.Fatalf("decode #1 saw %d bytes, want %d", len(frame1.Payload), len(first))
	}
	frame1.release(fake)

	wire2, err := Encode(newFrame(REQUEST, 2, second))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	frame2, err := decodeStreamed(bytes.NewReader(wire2), fake)
	if err != nil {
		t.Fatalf("decode #2: %v", err)
	}
	if !bytes.Equal(frame2.Payload, second) {
		t.Fatal("decode #2 observed stale bytes from the recycled buffer")
	}
	frame2.release(fake)
}

// FuzzPooledDecodeReuse: arbitrary payloads through the pooled decode path
// must (a) reproduce their bytes exactly and (b) not leak them into the
// next decode after release. This is the regression net for the entire
// release-point design: any premature or missing release shows up as wrong
// bytes here.
func FuzzPooledDecodeReuse(f *testing.F) {
	f.Add([]byte("hello"), []byte("world"))
	f.Add(bytes.Repeat([]byte("x"), 100*1024), []byte("y")) // multi-fragment
	f.Fuzz(func(t *testing.T, a, b []byte) {
		if len(a) == 0 || len(b) == 0 || len(a) > MaxStreamSize || len(b) > MaxStreamSize {
			return
		}
		fake := &fakePools{}

		for _, round := range [][]byte{a, b, a} {
			msg, err := encodeMessage(REQUEST, 42, round, 64*1024, fake)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var stream bytes.Buffer
			for _, buf := range msg.buffers() {
				stream.Write(buf)
			}
			frame, err := decodeStreamed(bytes.NewReader(stream.Bytes()), fake)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !bytes.Equal(frame.Payload, round) {
				t.Fatalf("pooled decode returned %d bytes that differ from the %d encoded", len(frame.Payload), len(round))
			}
			frame.release(fake)
		}
	})
}

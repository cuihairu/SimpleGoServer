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

// One fake per test, handed to the code under test as its poolBackend: the
// counters below are then a private ledger, so exact assertions neither race
// nor depend on which other tests happen to be running.

type fakePools struct {
	getPayloads []int // class indexes acquired, in order
	putPayloads []int // class indexes released, in order
	putCaps     []int // capacity of each released array, parallel to putPayloads
	putFrames   int
}

func newFakePools() *fakePools { return &fakePools{} }

func (f *fakePools) getFrame() *Frame { return &Frame{} }
func (f *fakePools) putFrame(*Frame)  { f.putFrames++ }
func (f *fakePools) getPayload(idx int) *[]byte {
	f.getPayloads = append(f.getPayloads, idx)
	cell := new([]byte)
	*cell = make([]byte, payloadClasses[idx])
	return cell
}

func (f *fakePools) putPayload(idx int, cell *[]byte) {
	f.putPayloads = append(f.putPayloads, idx)
	f.putCaps = append(f.putCaps, cap(*cell))
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
	fake := newFakePools()

	payload := bytes.Repeat([]byte("ab"), 150) // 300 bytes -> class 1KiB (idx 1)
	wire, err := Encode(newFrame(REQUEST, 9, payload))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	frame, err := decodeStreamed(bytes.NewReader(wire), fake)
	if err != nil {
		t.Fatalf("decodeStreamed(): %v", err)
	}
	if !frame.fromPool {
		t.Fatal("the pooled decode returned a frame not marked as pooled")
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
	fake := newFakePools()
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
	fake := newFakePools()

	// three fragments landing in distinct size classes once the 10B header is
	// added: 110B (idx 0, becomes the aggregate), 310B (idx 1), 6010B (idx 3)
	var wire []byte
	wire = append(wire, encodeFragment(t, 5, true, bytes.Repeat([]byte("a"), 100))...)
	wire = append(wire, encodeFragment(t, 5, true, bytes.Repeat([]byte("b"), 300))...)
	wire = append(wire, encodeFragment(t, 5, false, bytes.Repeat([]byte("c"), 6000))...)

	frame, err := decodeStreamed(bytes.NewReader(wire), fake)
	if err != nil {
		t.Fatalf("decodeStreamed(): %v", err)
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
	// the aggregate outgrew the first fragment's 256B buffer, so release must
	// recycle both arrays: the original (cap 256 -> class 0) and the grown
	// one. The grown array's class follows from its capacity, which append's
	// growth factor does not fix, so the expectation is derived rather than
	// hardcoded — what is asserted is the routing rule, not the allocator.
	if len(fake.putPayloads) != 4 || fake.putPayloads[2] != 0 {
		t.Fatalf("aggregate release routing: putPayloads=%v, want [1 3 0 ...]", fake.putPayloads)
	}
	if got := fake.putPayloads[3]; got != classIndexForCap(fake.putCaps[3]) {
		t.Fatalf("grown array of cap %d released to class %d, want %d", fake.putCaps[3], got, classIndexForCap(fake.putCaps[3]))
	}
	if fake.putCaps[3] < len(want) {
		t.Fatalf("grown array cap %d cannot hold the %d-byte aggregate", fake.putCaps[3], len(want))
	}
	if fake.putFrames != 3 {
		t.Fatalf("putFrames = %d, want 3 (two fragments + aggregate)", fake.putFrames)
	}
}

func TestStreamDecodeErrorRecyclesFirstFragment(t *testing.T) {
	fake := newFakePools()
	wire := encodeFragment(t, 7, true, []byte("orphan")) // promised more, then EOF
	_, err := decodeStreamed(bytes.NewReader(wire), fake)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	// two frame objects die here: the half-read second fragment (its header
	// arrived, so a pooled frame was taken but no payload was ever acquired)
	// and the orphan first fragment holding the only payload buffer
	if !intsEqual(fake.putPayloads, []int{0}) || fake.putFrames != 2 {
		t.Fatalf("error path recycling: putPayloads=%v putFrames=%d, want [0]/2", fake.putPayloads, fake.putFrames)
	}
}

func TestStreamViolationRecyclesBothFrames(t *testing.T) {
	fake := newFakePools()
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
	fake := newFakePools()
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
	fake := newFakePools()
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
	fake := newFakePools()
	for _, size := range []int{1, 10, 1024, 70 * 1024, MaxFrameSize} {
		payload := bytes.Repeat([]byte("q"), size)
		plain, err := encodeStreamFrames(REQUEST, 77, payload, 64*1024)
		if err != nil {
			t.Fatalf("encodeStreamFrames(%d): %v", size, err)
		}
		msg, err := encodeMessage(REQUEST, 77, payload, 64*1024, fake)
		if err != nil {
			t.Fatalf("encodeMessage(%d): %v", size, err)
		}
		pooled := msg.buffers()
		msg.release(fake)
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

// TestEncodeMessagePropagatesSingleFrameError: below the threshold the pooled
// branch has to surface the same validation error the plain branch gets from
// Encode, and must not hand back a partial result.
func TestEncodeMessagePropagatesSingleFrameError(t *testing.T) {
	fake := newFakePools()
	msg, err := encodeMessage(REQUEST, 1, nil, 1024, fake)
	if !errors.Is(err, ErrEmptyFrame) {
		t.Fatalf("err = %v, want ErrEmptyFrame", err)
	}
	if msg.buf != nil || msg.bufs != nil {
		t.Fatalf("error path returned a message: %+v", msg)
	}
	if len(fake.getPayloads) != 0 {
		t.Fatalf("a rejected frame still took a buffer: classes %v", fake.getPayloads)
	}
}

// TestEncodeMessageClampsFragmentSize: a threshold outside [1, MaxFrameSize]
// is clamped to the frame ceiling, so a caller with a nonsensical fragment
// size still gets a decodable stream instead of an infinite loop (a zero
// threshold) or a frame no decoder would accept (an oversized one).
func TestEncodeMessageClampsFragmentSize(t *testing.T) {
	fake := newFakePools()
	payload := bytes.Repeat([]byte("y"), 3*MaxFrameSize+17)

	for _, threshold := range []int{0, -1, 2 * MaxFrameSize} {
		msg, err := encodeMessage(REQUEST, 4, payload, threshold, fake)
		if err != nil {
			t.Fatalf("encodeMessage(threshold=%d): %v", threshold, err)
		}
		if want := 4; len(msg.bufs) != want {
			t.Fatalf("threshold %d: %d fragments, want %d", threshold, len(msg.bufs), want)
		}
		for i, buf := range msg.bufs {
			length := binary.BigEndian.Uint32(buf[6:10])
			if length > MaxFrameSize {
				t.Fatalf("threshold %d fragment %d announces %d bytes, over the limit", threshold, i, length)
			}
		}
		msg.release(fake)
	}
}

// TestEncodedMessageReleaseIsIdempotent: releasing twice must not hand the
// same array to two future acquires, and nothing may go back before the
// caller has actually written the bytes.
func TestEncodedMessageReleaseIsIdempotent(t *testing.T) {
	fake := newFakePools()
	msg, err := encodeMessage(REQUEST, 1, bytes.Repeat([]byte("z"), 30), 8, fake)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	if len(msg.bufs) != 4 {
		t.Fatalf("%d fragments, want 4", len(msg.bufs))
	}
	if len(fake.putPayloads) != 0 {
		t.Fatalf("buffers were released before the write: %v", fake.putPayloads)
	}
	msg.release(fake)
	if len(fake.putPayloads) != 4 {
		t.Fatalf("release put %v, want four fragments back", fake.putPayloads)
	}
	msg.release(fake)
	if len(fake.putPayloads) != 4 {
		t.Fatalf("double release recycled again: %v", fake.putPayloads)
	}
	// the single-buffer case recycles through the same method
	single, err := encodeMessage(REQUEST, 1, []byte("one buffer"), 1024, fake)
	if err != nil {
		t.Fatalf("encodeMessage: %v", err)
	}
	single.release(fake)
	single.release(fake)
	if len(fake.putPayloads) != 5 {
		t.Fatalf("single-buffer release put %d buffers, want 5 in total", len(fake.putPayloads))
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
func (s *inboundStub) Write(handler.Message)            {}
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
func (s *inboundStub) HandleWrite(handler.Message) {}

func TestFrameCodecReleasesFrameAfterDispatch(t *testing.T) {
	fake := newFakePools()

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
	codec := NewFrameCodec()
	codec.pools = fake
	codec.HandleRead(stub, nil)

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

func TestFrameCodecHandleWriteReleasesSingleEncodedBuffer(t *testing.T) {
	fake := newFakePools()
	codec := NewFrameCodec()
	codec.pools = fake
	out := &captureOutbound{}
	payload := bytes.Repeat([]byte("p"), 40) // 50B total -> class 0
	frame := newFrame(RESPONSE, 2, payload)

	codec.HandleWrite(out, frame)

	if !bytes.Contains(out.written, payload) {
		t.Fatal("downstream write missed the payload")
	}
	if !intsEqual(fake.putPayloads, []int{0}) {
		t.Fatalf("encoded buffer routing: putPayloads=%v, want [0]", fake.putPayloads)
	}
}

func TestFrameCodecHandleWriteReleasesFragmentsAfterJoin(t *testing.T) {
	fake := newFakePools()
	codec := NewFrameCodecWithStreamThreshold(8)
	codec.pools = fake
	out := &captureOutbound{}
	payload := bytes.Repeat([]byte("f"), 20) // 3 fragments of <= 8B

	codec.HandleWrite(out, newFrame(REQUEST, 1, payload))

	if len(fake.putPayloads) != 3 {
		t.Fatalf("fragment puts = %v, want three recycles", fake.putPayloads)
	}
	assembled, err := DecodeStreamed(bytes.NewReader(out.written))
	if err != nil {
		t.Fatalf("DecodeStreamed(): %v", err)
	}
	if !bytes.Equal(assembled.Payload, payload) {
		t.Fatal("joined bytes do not reassemble to the original payload")
	}
}

// ---- outbound encode edge cases --------------------------------------------

func TestEncodeFrameBufferedFallsBackAboveLargestClass(t *testing.T) {
	fake := newFakePools()
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
	fake := newFakePools()
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

// TestEncodedMessageReleaseSkipsNilCells: a buffer that fell back to an
// ordinary allocation (a total above the largest class) carries no cell, and
// release must not invent a pool put for it.
func TestEncodedMessageReleaseSkipsNilCells(t *testing.T) {
	fake := newFakePools()
	msg := encodedMessage{
		bufs:  [][]byte{make([]byte, 32)},
		cells: []*[]byte{nil},
	}
	msg.release(fake)
	if len(fake.putPayloads) != 0 {
		t.Fatal("a nil cell must not produce a pool put")
	}
}

// ---- end to end: premature release would corrupt the wire ------------------

// TestClientRoundTripReleasesBuffers drives a real Call through the pooled
// outbound path against a full protocol handler over net.Pipe. If roundTrip
// released its encoded buffers before the server consumed them, the echoed
// bytes would come back corrupted — correctness and recycling are asserted
// in the same run.
func TestClientRoundTripReleasesBuffers(t *testing.T) {
	fake := newFakePools()

	ph, conn := pipeServer(t, echoHandler)
	defer ph.Close()
	_ = conn

	clientConn, clientPeer := net.Pipe()
	// bridge: bytes the client writes reach the pipe server, and its
	// responses come back — a tiny relay with two goroutines
	go func() {
		_, _ = io.Copy(clientPeer, conn)
	}()
	go func() {
		_, _ = io.Copy(conn, clientPeer)
	}()

	client := NewClient(clientConn, nil)
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

// TestSyncPoolBackendReusesCells pins down the one thing the recording fake
// cannot: that the real backend is a pool rather than a fresh allocator.
//
// Identity of one particular Put/Get pair cannot be asserted: sync.Pool may
// drop anything at any GC, and a Get may run on a different P than the Put,
// where the object is invisible (per-P local caches). So the assertion is
// directional — over many rounds, at least one hands back the very array
// that was just returned. That is stable in practice and cannot flake; the
// load-independent proof that recycling really happens is the benchmark's
// 0 allocs/op (see Benchmark.md「帧对象池化 A/B」).
func TestSyncPoolBackendReusesCells(t *testing.T) {
	b := &syncPoolBackend{}

	reusedCell := false
	for i := 0; i < 64 && !reusedCell; i++ {
		cell := b.getPayload(1)
		if cap(*cell) < payloadClasses[1] {
			t.Fatalf("class 1 cell has cap %d, want >= %d", cap(*cell), payloadClasses[1])
		}
		b.putPayload(1, cell)
		if again := b.getPayload(1); &(*again)[0] == &(*cell)[0] {
			reusedCell = true
		}
	}
	if !reusedCell {
		t.Error("64 put/get rounds never handed back a resident cell: the pool is not recycling")
	}

	reusedFrame := false
	for i := 0; i < 64 && !reusedFrame; i++ {
		frame := b.getFrame()
		b.putFrame(frame)
		if again := b.getFrame(); again == frame {
			reusedFrame = true
		}
	}
	if !reusedFrame {
		t.Error("64 put/get rounds never handed back a resident frame")
	}
}

// TestPooledDecodeReusesRecycledBuffers decodes two different frames back to
// back through the real backend, releasing between them: the second decode's
// payload must be exactly its own bytes. A recycled buffer handed out still
// aliased to the first frame (or poisoned by stale contents beyond the new
// length) fails here.
func TestPooledDecodeReusesRecycledBuffers(t *testing.T) {
	realPools := &syncPoolBackend{}

	first := bytes.Repeat([]byte("1"), 200)
	second := bytes.Repeat([]byte("2"), 100)

	wire1, err := Encode(newFrame(REQUEST, 1, first))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	frame1, err := decodeStreamed(bytes.NewReader(wire1), realPools)
	if err != nil {
		t.Fatalf("decode #1: %v", err)
	}
	if !bytes.Equal(frame1.Payload, first) {
		t.Fatalf("decode #1 saw %d bytes, want %d", len(frame1.Payload), len(first))
	}
	frame1.release(realPools)

	wire2, err := Encode(newFrame(REQUEST, 2, second))
	if err != nil {
		t.Fatalf("Encode(): %v", err)
	}
	frame2, err := decodeStreamed(bytes.NewReader(wire2), realPools)
	if err != nil {
		t.Fatalf("decode #2: %v", err)
	}
	if !bytes.Equal(frame2.Payload, second) {
		t.Fatal("decode #2 observed stale bytes from the recycled buffer")
	}
	frame2.release(realPools)
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
		// a real backend, private to this case: encode and decode then
		// contend for the same cells, which is exactly the aliasing the
		// release points have to get right
		realPools := &syncPoolBackend{}

		for _, round := range [][]byte{a, b, a} {
			msg, err := encodeMessage(REQUEST, 42, round, 64*1024, realPools)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var stream bytes.Buffer
			for _, buf := range msg.bufs {
				stream.Write(buf)
			}
			if msg.buf != nil {
				stream.Write(msg.buf)
			}
			// the encoded bytes now live in a private copy: the pooled
			// buffers are dead and must go back
			msg.release(realPools)
			frame, err := decodeStreamed(bytes.NewReader(stream.Bytes()), realPools)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !bytes.Equal(frame.Payload, round) {
				t.Fatalf("pooled decode returned %d bytes that differ from the %d encoded", len(frame.Payload), len(round))
			}
			frame.release(realPools)
		}
	})
}

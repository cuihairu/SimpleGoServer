package proto

import "sync"

// This file owns the recycle side of the framing hot path. Pooling a frame
// sounds trivial until the zero-copy decoder is remembered: DecodeJSONMessage
// aliases frame.Payload, so a pooled buffer is only safe to recycle once
// every alias holder is done with it. The ownership analysis that makes the
// release points below sound:
//
//   - Inbound (server): the pipeline propagates events as plain synchronous
//     method calls (internal/handler walks a linked list; the ExecutorHandler
//     interface is asserted but never dispatched), so when FrameCodec's
//     ctx.HandleRead(frame) returns, every downstream consumer — dispatch,
//     the business handler, the response marshal, the outbound encode — has
//     already finished. Nothing retains the payload past that point
//     (ProtocolHandler tables store freshly encoded outbound bytes, never
//     inbound payloads), so the codec releases right after dispatch returns.
//   - Inbound (client): resolvePending sets Response.Data = msg.Data and
//     hands it to application code through a channel — the alias escapes.
//     The client therefore decodes through the non-pooled path; a pooled
//     frame it never released is simply never recycled.
//   - Outbound: encoded wire buffers die at HeadHandler's synchronous
//     conn.Write, so FrameCodec and Client roundTrip release them after the
//     write returns. Publish is the deliberate exception: it caches its own
//     Encode output for later replay and never goes through the pipeline,
//     so it keeps exact-size allocations.
//
// Buffers are pooled in size classes (256B..1MiB, 4x steps): an exact-fit
// freelist per length would need unbounded bookkeeping, and classes trade at
// most 4x slack per buffer (2x on average) for seven pools. Aggregates that
// outgrow the largest class are dropped back to the GC rather than parked,
// so no pool ever holds more than MaxFrameSize per entry.

// payloadClasses are the buffer sizes the pools hand out, smallest first.
// The largest equals MaxFrameSize: a legal frame payload always fits a class.
var payloadClasses = [...]int{256, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20}

// classIndexForSize returns the index of the smallest class >= size, or -1
// when size is non-positive or above the largest class.
func classIndexForSize(size int) int {
	if size <= 0 {
		return -1
	}
	for i, c := range payloadClasses {
		if size <= c {
			return i
		}
	}
	return -1
}

// classIndexForCap returns the index of the largest class <= cap, the class
// a buffer of that capacity can still serve; -1 when the capacity is below
// the smallest class (foreign tiny buffer) or above the largest (grown
// aggregate — deliberately not parked, see the file comment).
func classIndexForCap(cap int) int {
	if cap < payloadClasses[0] || cap > payloadClasses[len(payloadClasses)-1] {
		return -1
	}
	idx := -1
	for i, c := range payloadClasses {
		if c > cap {
			break
		}
		idx = i
	}
	return idx
}

// poolBackend is the seam between the routing logic and the actual pools.
// Production uses syncPoolBackend; tests substitute a recording fake so
// recycle decisions (which class, how many puts, in what order) are asserted
// deterministically instead of hoping sync.Pool hands back the same object.
//
// The backend is threaded through the call chain rather than reached for
// through a package variable, so a test's fake is private to that test: a
// swappable global would be reconfigured underneath codecs still running on
// other connections, which both races and makes the fake's counters depend
// on unrelated tests' goroutines. A nil backend means "no pooling" and
// selects the exact-allocation path.
type poolBackend interface {
	getFrame() *Frame
	putFrame(f *Frame)
	// getPayload returns a cell whose slice has capacity >= the class at
	// idx; putPayload takes a cell back for that class.
	getPayload(idx int) *[]byte
	putPayload(idx int, cell *[]byte)
}

// The pool stores *[]byte "cells" rather than []byte values: a slice stored
// in an interface would box its 24-byte header on every Put, while a pointer
// already fits in the interface word. Each cell is allocated once, when its
// buffer is first created, and travels with the buffer for the rest of its
// life — steady-state Get/Put cycles allocate nothing.
type syncPoolBackend struct {
	frames   sync.Pool // *Frame
	payloads [len(payloadClasses)]sync.Pool
}

// defaultPools is the production backend. It is assigned once at init and
// never reassigned — see the poolBackend comment for why the backend is a
// parameter everywhere else.
var defaultPools poolBackend = &syncPoolBackend{}

func (b *syncPoolBackend) getFrame() *Frame {
	if f, ok := b.frames.Get().(*Frame); ok {
		return f
	}
	return &Frame{}
}

func (b *syncPoolBackend) putFrame(f *Frame) {
	b.frames.Put(f)
}

func (b *syncPoolBackend) getPayload(idx int) *[]byte {
	if cell, ok := b.payloads[idx].Get().(*[]byte); ok && cap(*cell) >= payloadClasses[idx] {
		return cell
	}
	cell := new([]byte)
	*cell = make([]byte, payloadClasses[idx])
	return cell
}

func (b *syncPoolBackend) putPayload(idx int, cell *[]byte) {
	b.payloads[idx].Put(cell)
}

// acquireFrame hands out a clean frame from the pool. Everything a caller
// can observe (Header, Payload) was zeroed at release, so no stale data
// leaks from one frame's tenant to the next.
func acquireFrame(b poolBackend) *Frame {
	f := b.getFrame()
	f.fromPool = true
	f.released = false
	return f
}

// acquirePayload attaches a pooled buffer of at least size bytes to f,
// shrinking the cell's slice to exactly the requested length.
//
// Precondition: 0 < size <= MaxFrameSize, which is the largest class, so
// classIndexForSize cannot answer -1. The one call site establishes it —
// decodeFrame only calls after parseHeaderInto accepted the announced length
// (bounded to [0, MaxFrameSize]) and the length is positive. There is no
// fallible branch here on purpose: an index of -1 would panic inside the
// pool index, and a defensive check that no caller can reach is only a lie
// waiting to be untested.
func acquirePayload(b poolBackend, f *Frame, size int) {
	idx := classIndexForSize(size)
	cell := b.getPayload(idx)
	f.payloadCell = cell
	f.Payload = (*cell)[:size]
}

// release returns a pooled frame and everything it owns to the pool it came
// from. It is idempotent and a no-op on frames that never came from a pool,
// so callers on mixed paths (pooled and plain decode share one code body) can
// call it unconditionally. After release the frame is zeroed: a caller still
// holding the pointer observes empty fields instead of silently recycled
// bytes, which turns use-after-release into a loud failure.
//
// b must be the backend the frame was acquired from; pooling exists only to
// feed a specific pool, so a mismatched one would quietly recycle into the
// wrong place (the failure is observable as missing puts, not as corruption).
func (f *Frame) release(b poolBackend) {
	if f == nil || !f.fromPool || f.released {
		return
	}
	f.released = true
	payload, cell := f.Payload, f.payloadCell
	f.fromPool = false
	f.Header = FrameHeader{}
	f.Payload = nil
	f.payloadCell = nil
	releasePayload(b, payload, cell)
	b.putFrame(f)
}

// releasePayload recycles the array the frame last used. When an aggregate
// outgrew its first fragment's buffer, append moved to a fresh array: both
// arrays are still owned (the old one is dead but allocated), so both are
// returned — the old one through its original cell, the grown one boxed on
// its first release.
func releasePayload(b poolBackend, payload []byte, cell *[]byte) {
	if cell == nil {
		putPayloadArray(b, payload, nil)
		return
	}
	if sameBacking(payload, *cell) {
		putPayloadArray(b, payload, cell)
		return
	}
	putPayloadArray(b, *cell, cell)
	putPayloadArray(b, payload, nil)
}

// putPayloadArray routes an array to the class its capacity can serve; a
// nil cell (an array that grew on the heap, not out of a pool cell) gets
// boxed here once and becomes an ordinary pool citizen.
func putPayloadArray(b poolBackend, arr []byte, cell *[]byte) {
	idx := classIndexForCap(cap(arr))
	if idx < 0 {
		return // below the smallest or above the largest class: let the GC take it
	}
	if cell == nil {
		cell = new([]byte)
		*cell = arr[:cap(arr)]
	}
	b.putPayload(idx, cell)
}

// sameBacking reports whether two slices view the same array; empty slices
// have no addressable first element and are reported as different.
func sameBacking(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

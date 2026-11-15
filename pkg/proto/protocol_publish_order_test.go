package proto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
)

// gatedConn is the server end of a protocol connection driven through a
// pipeline: it serves the queued frames to the decoder, parks its reads
// afterwards, and parks writes carrying the marker until the gate is
// released — letting the test freeze one publish mid-fan-out. Writes are
// recorded at completion, so the log is the wire order.
type gatedConn struct {
	queue   []byte
	marker  []byte
	parked  chan struct{}
	gate    chan struct{}
	entered atomic.Int32 // Write calls seen carrying the marker
	logMu   sync.Mutex
	log     [][]byte
}

func newGatedConn(queue []byte, marker string) *gatedConn {
	return &gatedConn{
		queue:  queue,
		marker: []byte(marker),
		parked: make(chan struct{}),
		gate:   make(chan struct{}),
	}
}

func (g *gatedConn) Read(p []byte) (int, error) {
	if len(g.queue) > 0 {
		n := copy(p, g.queue)
		g.queue = g.queue[n:]
		return n, nil
	}
	<-g.parked
	return 0, net.ErrClosed
}

func (g *gatedConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, g.marker) {
		g.entered.Add(1)
		<-g.gate
	}
	g.logMu.Lock()
	g.log = append(g.log, append([]byte(nil), p...))
	g.logMu.Unlock()
	return len(p), nil
}

func (g *gatedConn) wireLog() [][]byte {
	g.logMu.Lock()
	defer g.logMu.Unlock()
	return append([][]byte(nil), g.log...)
}

func (g *gatedConn) Close() error {
	select {
	case <-g.parked:
	default:
		close(g.parked)
	}
	return nil
}
func (g *gatedConn) LocalAddr() net.Addr              { return nil }
func (g *gatedConn) RemoteAddr() net.Addr             { return nil }
func (g *gatedConn) SetDeadline(time.Time) error      { return nil }
func (g *gatedConn) SetReadDeadline(time.Time) error  { return nil }
func (g *gatedConn) SetWriteDeadline(time.Time) error { return nil }

// pumpGatedConn drives a pipeline over the gated conn like the reactor
// pump does; closing the conn ends it.
func pumpGatedConn(t *testing.T, ph *ProtocolHandler, conn *gatedConn) {
	t.Helper()
	pipeline := handlerImpl.NewPipeline(conn)
	_ = pipeline.AddLast(NewFrameCodec(), ph)
	pumped := make(chan struct{})
	go func() {
		defer close(pumped)
		for {
			pipeline.FireRead(conn)
			if _, err := conn.Read(make([]byte, 0)); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = conn.Close()
		<-pumped
	})
}

// publishSeqs extracts the stream ids of the recorded PUBLISH frames in
// wire order.
func publishSeqs(t *testing.T, writes [][]byte) []uint32 {
	t.Helper()
	var seqs []uint32
	for _, w := range writes {
		if len(w) < 10 || FrameType(w[0]) != PUBLISH {
			continue
		}
		seqs = append(seqs, binary.BigEndian.Uint32(w[2:6]))
	}
	return seqs
}

// waitForGatedWrite parks the test until conn has seen a Write carrying
// the gate marker (the frozen publish is mid-fan-out).
func waitForGatedWrite(t *testing.T, conn *gatedConn, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for conn.entered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if conn.entered.Load() == 0 {
		t.Fatalf("%s never reached its gated write", what)
	}
}

// TestPublishFanOutPreservesSequenceOrder pins the per-topic publish gate:
// sequence commit, cache append, and subscriber fan-out are one critical
// section. The schedule freezes the seq-1 publisher inside its fan-out
// write and only then fires the seq-2 publisher — ungated code mints and
// lands seq 2 while seq 1 is still parked, so the wire carries [2 1]; a
// confirm-mode client's cursor would jump past the unseen seq 1 and the
// reconnect replay could never repair it. Gated code cannot even mint
// seq 2 until the frozen fan-out completes, so the wire stays [1 2].
func TestPublishFanOutPreservesSequenceOrder(t *testing.T) {
	ph := NewProtocolHandler(echoHandler)
	t.Cleanup(ph.Close)
	subWire, _ := Encode(mustFrame(EncodeJSON(SUBSCRIBE, 1, "order-topic", nil)))
	conn := newGatedConn(subWire, "first")
	pumpGatedConn(t, ph, conn)

	// subscribed once the ack write has been recorded
	deadline := time.Now().Add(2 * time.Second)
	for len(conn.wireLog()) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if len(conn.wireLog()) == 0 {
		t.Fatal("subscribe ack never reached the wire")
	}

	firstDone := make(chan struct{})
	returnedEarly := false
	go func() {
		defer close(firstDone)
		if _, err := ph.Publish("order-topic", "first"); err != nil {
			t.Errorf("first publish: %v", err)
		}
	}()
	waitForGatedWrite(t, conn, "first publish")

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		if _, err := ph.Publish("order-topic", "second"); err != nil {
			t.Errorf("second publish: %v", err)
		}
	}()

	// give the second publish its window while the first stays parked:
	// ungated code finishes it now and its seq-2 frame lands first; gated
	// code cannot even mint seq 2 (the frozen fan-out holds the publish
	// gate) so the wait just times out
	deadline = time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-secondDone:
			returnedEarly = true
		default:
		}
		if returnedEarly {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// release the frozen fan-out; the second publish follows in both codes
	close(conn.gate)
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first publish did not finish")
	}
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second publish did not finish")
	}

	seqs := publishSeqs(t, conn.wireLog())
	if len(seqs) != 2 {
		t.Fatalf("expected 2 publish frames on the wire, got %v", seqs)
	}
	if seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("wire order was %v; seq 1 must reach the wire before seq 2", seqs)
	}
}

// TestResumeExpiringBetweenPhasesReturnsGates pins the give-back path: a
// session that phase 1 finds fresh but phase 2 finds expired (the janitor
// or TTL raced the resume) must release the gates it pinned and fall back
// to a fresh session — leaving them locked would wedge every publisher of
// those topics forever. The schedule freezes the resume between the phases
// by holding the topic's gate and waiting for the resume to park on it
// (a failed TryLock proves the resume holds the gate).
func TestResumeExpiringBetweenPhasesReturnsGates(t *testing.T) {
	ph := NewProtocolHandler(echoHandler)
	t.Cleanup(ph.Close)
	client1, server1 := pipeHandler(t, ph)

	hello, _ := EncodeJSON(HELLO, 1, "hello", &HelloRequest{Versions: []int{ProtocolVersion}})
	writeFrame(t, client1, hello)
	ack := readFrame(t, client1)
	env, err := DecodeJSONMessage(ack)
	if err != nil {
		t.Fatalf("handshake ack: %v", err)
	}
	var res HelloResponse
	if err := json.Unmarshal(env.Data, &res); err != nil {
		t.Fatalf("handshake ack payload: %v", err)
	}
	writeFrame(t, client1, mustFrame(EncodeJSON(SUBSCRIBE, 2, "expire-topic", nil)))
	if ack := readFrame(t, client1); ack.Header.FrameType != RESPONSE {
		t.Fatalf("subscribe ack type = %s, want RESPONSE", ack.Header.FrameType)
	}
	ph.HandleInactive(&inactiveContextStub{conn: server1}, nil)

	// A helper goroutine holds the topic's gate like a slow publisher, so
	// the resume parks between its phases; a channel tells the helper when
	// to expire the session and release. The test's park signal is the
	// gate's refcount hitting 2: the helper pinned it first, so a second
	// pin can only be the resume's own acquire — the resume is then past
	// phase 1 (its topics loop ran) and parked for phase 2.
	gate := ph.acquirePublishGate("expire-topic")
	givenBack := false
	giveBack := func() {
		if givenBack {
			return
		}
		givenBack = true
		ph.releasePublishGate(gate)
	}
	defer giveBack()

	ph.mu.RLock()
	s := ph.sessions[res.Token]
	ph.mu.RUnlock()
	if s == nil {
		t.Fatal("session vanished before the resume froze")
	}

	held := make(chan struct{})
	flipNow := make(chan struct{})
	helperDone := make(chan struct{})
	go func() {
		defer close(helperDone)
		gate.mu.Lock()
		defer gate.mu.Unlock()
		close(held)
		<-flipNow
		// the session expires while the resume is frozen between phases
		s.lastSeen.Store(time.Now().Add(-time.Hour).UnixNano())
	}()
	// the helper provably holds the gate before the resume starts: its
	// phase-2 re-validation can then only run after the flip, so the
	// expired fallback is the only reachable outcome
	<-held

	type attachResult struct {
		token   string
		resumed bool
		gates   int
	}
	done := make(chan attachResult, 1)
	client2, _ := net.Pipe()
	defer client2.Close()
	go func() {
		got := ph.attachSession(client2, res.Token, nil)
		done <- attachResult{token: got.token, resumed: got.resumed, gates: len(got.gates)}
	}()

	// wait until the resume has pinned the gate (refs 1 -> 2)
	parked := false
	deadline := time.Now().Add(2 * time.Second)
	for !parked && time.Now().Before(deadline) {
		ph.mu.RLock()
		refs := gate.refs
		ph.mu.RUnlock()
		if refs == 2 {
			parked = true
			continue
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !parked {
		t.Fatal("resume never pinned the topic's publish gate")
	}

	// expire the session; the helper's unlock (happens-before the resume's
	// phase 2 via gate.mu) makes the staleness land deterministically
	close(flipNow)
	<-helperDone

	got := <-done
	giveBack()

	if got.resumed {
		t.Fatal("resume succeeded on a session that expired between the phases")
	}
	if got.token == res.Token || got.token == "" {
		t.Fatalf("fresh session not minted after the expired resume (token %q)", got.token)
	}
	if got.gates != 0 {
		t.Fatalf("expired resume returned %d locked gates; they must be given back", got.gates)
	}
	ph.mu.RLock()
	_, gateLeft := ph.publishGates["expire-topic"]
	_, newSessionLeft := ph.sessions[got.token]
	ph.mu.RUnlock()
	if gateLeft {
		t.Fatal("publish gate still pinned after the expired resume gave it back")
	}
	if !newSessionLeft {
		t.Fatal("fresh session not recorded after the expired resume fell back")
	}
	// the expired session itself stays recorded — reaping it is the
	// janitor's job, not the resume fallback's
}

// TestSubscribeResponseWaitsForInFlightPublish pins the subscribe-side
// half of the publish gate: the response's LastSeq is the replay boundary
// a resilient client baselines on — everything at or below it was never
// fanned out to this conn, everything above it will be. The schedule
// freezes the seq-1 publish mid-fan-out and fires the subscribe while it
// is parked: gated code blocks the subscribe on the topic's gate (no
// response inside the window) and reads the sequence only after the
// frozen fan-out completes, so the conn misses seq 1 and the response
// says exactly 1. Ungated code joins the member between the publisher's
// member snapshot and its sequence commit, responds while the publish is
// still frozen, and the conn silently misses a publish its cursor claims
// it will see.
func TestSubscribeResponseWaitsForInFlightPublish(t *testing.T) {
	ph := NewProtocolHandler(echoHandler)
	t.Cleanup(ph.Close)
	subWire, _ := Encode(mustFrame(EncodeJSON(SUBSCRIBE, 1, "gate-topic", nil)))
	conn1 := newGatedConn(subWire, "first")
	pumpGatedConn(t, ph, conn1)

	// subscribed once the ack write has been recorded
	deadline := time.Now().Add(2 * time.Second)
	for len(conn1.wireLog()) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if len(conn1.wireLog()) == 0 {
		t.Fatal("subscribe ack never reached the wire")
	}

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		if _, err := ph.Publish("gate-topic", "first"); err != nil {
			t.Errorf("first publish: %v", err)
		}
	}()
	waitForGatedWrite(t, conn1, "first publish")

	// a second transport whose subscribe response must wait for the
	// frozen fan-out
	client2, _ := pipeHandler(t, ph)
	pushed := make(chan *Frame, 4)
	c2 := NewClient(client2, func(frame *Frame) { pushed <- frame })
	defer c2.Close()

	subRes := make(chan *SubscribeResponse, 1)
	subErr := make(chan error, 1)
	go func() {
		sub, err := c2.SubscribeWithResult("gate-topic", 5*time.Second)
		if err != nil {
			subErr <- err
			return
		}
		subRes <- sub
	}()

	// while the seq-1 fan-out stays frozen, the subscribe must stay parked
	// on the topic's gate — no response inside the window
	returned := false
	deadline = time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-subRes:
			returned = true
		case err := <-subErr:
			t.Fatalf("subscribe failed while the fan-out was frozen: %v", err)
		default:
		}
		if returned {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if returned {
		t.Fatal("subscribe response arrived while the seq-1 fan-out was still frozen")
	}

	close(conn1.gate)
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first publish did not finish")
	}
	select {
	case sub := <-subRes:
		if sub.LastSeq != 1 {
			t.Fatalf("subscribe LastSeq = %d, want 1 (the frozen publish's sequence)", sub.LastSeq)
		}
	case err := <-subErr:
		t.Fatalf("subscribe failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("subscribe did not finish after the fan-out was released")
	}

	// the boundary holds: the conn missed the frozen seq 1 and the next
	// publish arrives as seq 2 — nothing below LastSeq ever reaches it
	if _, err := ph.Publish("gate-topic", "second"); err != nil {
		t.Fatalf("second publish: %v", err)
	}
	select {
	case frame := <-pushed:
		if frame.Header.FrameType != PUBLISH || frame.Header.StreamId != 2 {
			t.Fatalf("first push after subscribe = %s/%d, want PUBLISH/2", frame.Header.FrameType, frame.Header.StreamId)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no push after the second publish")
	}
}

// TestResumeReplayReachesWireBeforeLivePublishes pins the resume ordering:
// the session's topics' publish gates are held from the member join until
// the replay has been written, so a publisher concurrent with the resume
// can only fan out after the history catch-up. Without the pins a live
// publish could join the fresh transport mid-resume, land its higher
// sequence first, push the client's cursor past the replayed frames, and
// the client would drop them as stale.
func TestResumeReplayReachesWireBeforeLivePublishes(t *testing.T) {
	ph := NewProtocolHandler(echoHandler)
	t.Cleanup(ph.Close)
	client1, server1 := pipeHandler(t, ph)

	hello, _ := EncodeJSON(HELLO, 1, "hello", &HelloRequest{Versions: []int{ProtocolVersion}})
	writeFrame(t, client1, hello)
	ack := readFrame(t, client1)
	env, err := DecodeJSONMessage(ack)
	if err != nil {
		t.Fatalf("handshake ack: %v", err)
	}
	var res HelloResponse
	if err := json.Unmarshal(env.Data, &res); err != nil {
		t.Fatalf("handshake ack payload: %v", err)
	}
	if res.Token == "" {
		t.Fatal("handshake returned no session token")
	}
	writeFrame(t, client1, mustFrame(EncodeJSON(SUBSCRIBE, 2, "order-topic", nil)))
	if ack := readFrame(t, client1); ack.Header.FrameType != RESPONSE {
		t.Fatalf("subscribe ack type = %s, want RESPONSE", ack.Header.FrameType)
	}
	publishAsync(t, ph, "order-topic", "first")
	if pushed := readFrame(t, client1); pushed.Header.FrameType != PUBLISH {
		t.Fatalf("pushed frame type = %s, want PUBLISH", pushed.Header.FrameType)
	}

	// transport dies; the session keeps the topic intent. The next publish
	// is cached for the resume (offline delivery reaches nobody).
	ph.HandleInactive(&inactiveContextStub{conn: server1}, nil)
	if n, err := ph.Publish("order-topic", "replay-me"); err != nil || n != 0 {
		t.Fatalf("offline publish: delivered %d err %v, want 0 nil", n, err)
	}

	// resume over a gated transport: the replay write parks until released.
	// The cursor reports seq 1 as already seen, so the replay must carry
	// exactly the cached seq 2 — anything later is live.
	resumeWire, _ := Encode(mustFrame(EncodeJSON(HELLO, 3, "hello", &HelloRequest{
		Versions: []int{ProtocolVersion},
		Resume:   res.Token,
		Cursors:  map[string]uint64{"order-topic": 1},
	})))
	conn2 := newGatedConn(resumeWire, "replay-me")
	pipeline := handlerImpl.NewPipeline(conn2)
	_ = pipeline.AddLast(NewFrameCodec(), ph)
	pumped := make(chan struct{})
	go func() {
		defer close(pumped)
		pipeline.FireRead(conn2)
	}()
	t.Cleanup(func() {
		_ = conn2.Close()
		<-pumped
	})

	// the resume is mid-flight once the parked replay write has been seen
	waitForGatedWrite(t, conn2, "resume replay")

	// a publish concurrent with the mid-flight resume: gated code waits at
	// the topic's publish gate (held by the resume) and cannot even mint a
	// sequence; ungated code fans out immediately and overtakes the replay
	liveDone := make(chan struct{})
	returnedEarly := false
	go func() {
		defer close(liveDone)
		if _, err := ph.Publish("order-topic", "live-me"); err != nil {
			t.Errorf("live publish: %v", err)
		}
	}()

	// give the live publish its window while the replay stays parked:
	// ungated ordering (gates dropped before the replay) lets it finish
	// now and its seq-3 frame lands first; gated code holds the publish
	// gate until the replay is through, so the wait just times out
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-liveDone:
			returnedEarly = true
		default:
		}
		if returnedEarly {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// let the replay through; gated code then lets the live publish follow
	close(conn2.gate)
	select {
	case <-liveDone:
	case <-time.After(10 * time.Second):
		t.Fatal("live publish did not finish")
	}
	select {
	case <-pumped:
	case <-time.After(10 * time.Second):
		t.Fatal("resume handling did not finish")
	}

	// the replayed seq (2) must be on the wire before the live seq (3):
	// ungated code records the overtaking live frame first
	seqs := publishSeqs(t, conn2.wireLog())
	if len(seqs) != 2 {
		t.Fatalf("expected 2 publish frames on the wire (replay + live), got %v", seqs)
	}
	if seqs[0] != 2 || seqs[1] != 3 {
		t.Fatalf("wire order was %v; replayed seq 2 must precede live seq 3", seqs)
	}
}

package proto

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// killCurrent closes the connection the resilient client currently uses,
// simulating a network drop from the outside.
func killCurrent(t *testing.T, rc *ResilientClient) *Client {
	t.Helper()
	rc.mu.Lock()
	c := rc.client
	rc.mu.Unlock()
	if c == nil {
		t.Fatal("no current client to kill")
	}
	_ = c.Close()
	return c
}

// TestResilientClientRecoversFromDrop covers the happy path: a subscribed
// client whose connection drops is reconnected and the server restores the
// subscription via the session token — pushes keep flowing.
func TestResilientClientRecoversFromDrop(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes := make(chan *Frame, 1)
	rc := NewResilientClient(addr, func(frame *Frame) { pushes <- frame }, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	dead := killCurrent(t, rc)

	// the drop must not close the resilient client, and the subscription
	// must come back on the reconnected transport
	deadline := time.After(3 * time.Second)
	for {
		if _, err := ph.Publish("ticks", "still there?"); err != nil {
			t.Fatalf("Publish(): %v", err)
		}
		select {
		case <-deadline:
			t.Fatal("no push within 3s — reconnection or subscription restore failed")
		case frame := <-pushes:
			msg, err := DecodeJSONMessage(frame)
			if err != nil || msg.Action != "ticks" {
				t.Fatalf("unexpected push %v (err=%v)", frame, err)
			}
			// a resumed push can be delivered by the new client's readLoop
			// before swap() installs it — Dial starts reading immediately,
			// and the server restores the subscription during the handshake.
			// Give the install a moment; if the transport really stayed
			// dead, rc.client never flips and this fails below.
			installDeadline := time.Now().Add(2 * time.Second)
			for {
				rc.mu.Lock()
				alive := rc.client != nil && rc.client != dead
				rc.mu.Unlock()
				if alive {
					return
				}
				if time.Now().After(installDeadline) {
					t.Fatal("push arrived on the dead connection")
				}
				time.Sleep(5 * time.Millisecond)
			}
		case <-time.After(100 * time.Millisecond):
			// retry the publish; early attempts may race the reconnect
		}
	}
}

// TestResilientClientResubscribesWhenSessionExpired forces the server to
// forget sessions (TTL below zero), so the reconnect cannot resume: the
// client must notice Resumed == false and re-send its subscriptions.
func TestResilientClientResubscribesWhenSessionExpired(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.sessionTTL = -time.Second
	ph.mu.Unlock()

	pushes := make(chan *Frame, 1)
	rc := NewResilientClient(addr, func(frame *Frame) { pushes <- frame }, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	killCurrent(t, rc)

	deadline := time.After(3 * time.Second)
	for {
		if _, err := ph.Publish("ticks", "resubscribed?"); err != nil {
			t.Fatalf("Publish(): %v", err)
		}
		select {
		case <-deadline:
			t.Fatal("no push within 3s — re-subscribe after expired session failed")
		case frame := <-pushes:
			msg, _ := DecodeJSONMessage(frame)
			if msg.Action != "ticks" {
				t.Fatalf("unexpected push %v", frame)
			}
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestAcknowledgedResilientClientKeepsDroppedPublishReplayable pins the
// cursor rule behind the acknowledged constructor: a full bounded buffer
// rejects the publish, the reconnect cursor stays on the previous sequence,
// and the server replays the rejected frame after the transport returns.
func TestAcknowledgedResilientClientKeepsDroppedPublishReplayable(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	received := make(chan *Frame, 2)
	var accepted atomic.Uint64
	rc := NewAcknowledgedResilientClient(addr, func(frame *Frame) bool {
		select {
		case received <- frame:
			accepted.Add(1)
			return true
		default:
			return false
		}
	}, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	// fill the two-slot bounded buffer. Both accepted publishes advance the
	// cursor; the third is rejected and must remain older than the cursor.
	if _, err := ph.Publish("ticks", "accepted-1"); err != nil {
		t.Fatalf("Publish(accepted-1): %v", err)
	}
	if _, err := ph.Publish("ticks", "accepted-2"); err != nil {
		t.Fatalf("Publish(accepted-2): %v", err)
	}
	if _, err := ph.Publish("ticks", "rejected"); err != nil {
		t.Fatalf("Publish(rejected): %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for accepted.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if accepted.Load() != 2 {
		t.Fatal("the two bounded-buffer publishes never arrived before the rejected one")
	}
	rc.mu.Lock()
	cursors := make(map[string]uint64, len(rc.cursors))
	for topic, seq := range rc.cursors {
		cursors[topic] = seq
	}
	rc.mu.Unlock()
	if cursors["ticks"] != 2 {
		t.Fatalf("cursor = %v, want sequence 2 after two accepted publishes", cursors["ticks"])
	}

	// Empty the bounded buffer before dropping the transport. Otherwise the
	// replay can arrive before this test drains accepted-1/2, be rejected by
	// the still-full consumer, and make the final assertion depend on timing.
	for len(received) > 0 {
		<-received
	}

	dead := killCurrent(t, rc)

	// accept everything after the reconnect: the rejected sequence must come
	// back from replay, while the already accepted one must not.
	deadline = time.Now().Add(3 * time.Second)
	for {
		if _, err := ph.Publish("ticks", "reconnect-live"); err != nil {
			t.Fatalf("Publish after reconnect: %v", err)
		}
		rc.mu.Lock()
		alive := rc.client != nil && rc.client != dead
		rc.mu.Unlock()
		if alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("client never reconnected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var replayed bool
	for time.Now().Before(deadline) {
		select {
		case frame := <-received:
			msg, err := DecodeJSONMessage(frame)
			if err != nil || msg.Action != "ticks" {
				t.Fatalf("unexpected replayed frame %v (err=%v)", frame, err)
			}
			if string(msg.Data) == `"rejected"` {
				replayed = true
			}
			if replayed {
				return
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("the rejected publish was never replayed")
}

func TestDecodePublishRejectsNonPublishAndMalformedPayload(t *testing.T) {
	if _, _, ok := decodePublish(nil); ok {
		t.Fatal("decodePublish(nil) accepted a nil frame")
	}
	if _, _, ok := decodePublish(&Frame{Header: FrameHeader{FrameType: RESPONSE}}); ok {
		t.Fatal("decodePublish accepted a non-PUBLISH frame")
	}
	malformed := &Frame{Header: FrameHeader{FrameType: PUBLISH}, Payload: []byte("not json")}
	if _, _, ok := decodePublish(malformed); ok {
		t.Fatal("decodePublish accepted a malformed publish payload")
	}
}

// TestResilientClientSurvivesDeadServer checks the no-recovery-yet path:
// with the server gone the reconnect loop keeps backing off, the client
// stays open (drops are recoverable), and Close ends everything cleanly.
func TestResilientClientSurvivesDeadServer(t *testing.T) {
	// the test owns the listener because it must kill the whole server,
	// not just the current connection — startTCPServer's cleanup would be
	// too late for that
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ph := NewProtocolHandler(nil)
	t.Cleanup(func() {
		ph.Close()
		_ = listener.Close()
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go serveConn(conn, ph)
		}
	}()

	rc := NewResilientClient(listener.Addr().String(), nil, &ResilientOptions{BackoffStart: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond})
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}

	// close the listener first: with no server to accept, the reconnect
	// loop deterministically never succeeds. The reverse order raced — the
	// loop could re-dial before the server died and "Call must fail" would
	// flake (CI caught exactly that under load).
	_ = listener.Close()
	killCurrent(t, rc)

	// drops do not close the client; calls fail while the server is gone
	if _, err := rc.Call("echo", "x", 2*time.Second); err == nil {
		t.Fatal("Call on a dead connection unexpectedly succeeded")
	}
	select {
	case <-rc.Done():
		t.Fatal("Done closed by a recoverable drop")
	case <-time.After(100 * time.Millisecond):
	}

	if err := rc.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	select {
	case <-rc.Done():
		// the reconnect loop stopped with the client
	case <-time.After(2 * time.Second):
		t.Fatal("Done did not close after Close")
	}
}

// TestResilientClientRequiresConnect pins the API contract before Connect.
func TestResilientClientRequiresConnect(t *testing.T) {
	rc := NewResilientClient("127.0.0.1:1", nil, nil)
	if _, err := rc.Call("echo", nil, time.Second); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Call before Connect = %v, want ErrNotConnected", err)
	}
	if err := rc.Subscribe("t", time.Second); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("Subscribe before Connect = %v, want ErrNotConnected", err)
	}
}

// TestResilientClientReconnectLoopExits pins both done-driven exits of
// reconnectLoop deterministically. Reached through watch(), which exit runs
// depends on where Close lands relative to the dial and the backoff sleep —
// a race that made statement coverage flap between runs under load.
func TestResilientClientReconnectLoopExits(t *testing.T) {
	// exit at the loop top: done already closed, so the select fires before
	// any dial is attempted
	rc := &ResilientClient{
		addr:       "127.0.0.1:1",
		opts:       &ResilientOptions{HandshakeTimeout: time.Millisecond},
		subscribed: map[string]struct{}{},
		cursors:    map[string]uint64{},
		done:       make(chan struct{}),
	}
	close(rc.done)
	if rc.reconnectLoop() {
		t.Fatal("reconnectLoop on a closed client must return false")
	}

	// exit from the backoff sleep: the first dial fails fast against the
	// closed port, and the 10s backoff guarantees Close lands inside the
	// sleep — the done branch beats the timer deterministically
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close() // connection refused from the very first dial on

	rc2 := NewResilientClient(addr, nil, &ResilientOptions{
		BackoffStart: 10 * time.Second,
		BackoffMax:   10 * time.Second,
	})
	go func() {
		time.Sleep(50 * time.Millisecond)
		rc2.Close()
	}()
	if rc2.reconnectLoop() {
		t.Fatal("reconnectLoop after Close must return false")
	}
}

// TestAcknowledgedCursorKeepsContiguousPrefix pins the sparse-set cursor rule
// behind the acknowledged constructor: a single monotonic cursor per topic
// cannot express "accepted 1,2 and 4 but rejected 3", so the cursor must stay
// on the contiguous accepted prefix (2) until the gap (3) is accepted, and
// then leap over it. Buffered accepts collapse once the older gaps are filled.
func TestAcknowledgedCursorKeepsContiguousPrefix(t *testing.T) {
	rc := &ResilientClient{}

	rc.acknowledgePublish("t", 1)
	rc.acknowledgePublish("t", 2)
	rc.acknowledgePublish("t", 4)
	if rc.shouldDeliverPublish("t", 4) {
		t.Fatal("an already accepted gap must be filtered on replay")
	}
	rc.acknowledgePublish("t", 3)
	rc.acknowledgePublish("t", 6)

	if got := rc.cursors["t"]; got != 4 {
		t.Fatalf("cursor = %d, want 4 after accepting 1,2,4 then the missing 3", got)
	}
	if len(rc.acceptedPublishes["t"]) != 1 {
		t.Fatalf("acceptedPublishes = %v, want the unfilled sequence 6", rc.acceptedPublishes["t"])
	}

	rc.acknowledgePublish("t", 4)
	rc.acknowledgePublish("t", 5)
	rc.acknowledgePublish("t", 6)
	rc.acknowledgePublish("t", 7)
	rc.acknowledgePublish("t", 9)
	rc.acknowledgePublish("t", 11)

	if got := rc.cursors["t"]; got != 7 {
		t.Fatalf("cursor = %d, want 7 after filling the remaining gap", got)
	}
	if len(rc.acceptedPublishes["t"]) != 2 {
		t.Fatalf("acceptedPublishes = %v, want unfilled sequences 9 and 11", rc.acceptedPublishes["t"])
	}
	rc.acknowledgePublish("t", 8)
	rc.acknowledgePublish("t", 10)
	if got := rc.cursors["t"]; got != 11 {
		t.Fatalf("cursor = %d, want 11 after filling the final gaps", got)
	}
	if rc.acceptedPublishes["t"] != nil {
		t.Fatalf("acceptedPublishes = %v, want empty after every gap was filled", rc.acceptedPublishes["t"])
	}
	rc.acknowledgePublish("t", 12)
	rc.acknowledgePublish("t", 13)
	if got := rc.cursors["t"]; got != 13 {
		t.Fatalf("cursor after contiguous replay = %d, want 13", got)
	}
}

// TestAcknowledgedClientDoesNotRedeliverAcceptedGapAfterReplay covers the
// sparse acceptance case end to end: sequence 3 is rejected while 4 is
// accepted, so the cursor stays at 2. On reconnect the server replays 3;
// sequence 4 must be withheld until accepting 3 completes the prefix.
func TestAcknowledgedClientDoesNotRedeliverAcceptedGapAfterReplay(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	received := make(chan string, 4)
	var rejectedCalls atomic.Uint64
	rc := NewAcknowledgedResilientClient(addr, func(frame *Frame) bool {
		msg, err := DecodeJSONMessage(frame)
		if err != nil {
			return false
		}
		payload := string(msg.Data)
		if payload == `"rejected"` && rejectedCalls.Add(1) == 1 {
			return false
		}
		select {
		case received <- payload:
			return true
		default:
			return false
		}
	}, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	for _, payload := range []string{"accepted-1", "accepted-2", "rejected", "accepted-gap"} {
		if _, err := ph.Publish("ticks", payload); err != nil {
			t.Fatalf("Publish(%s): %v", payload, err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(received) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(received) != 3 {
		t.Fatalf("received %d initial publishes, want 3", len(received))
	}

	dead := killCurrent(t, rc)
	reconnected := make(chan struct{})
	go func() {
		defer close(reconnected)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			rc.mu.Lock()
			alive := rc.client != nil && rc.client != dead
			rc.mu.Unlock()
			if alive {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("client never reconnected")
	}

	replayDeadline := time.Now().Add(3 * time.Second)
	for rejectedCalls.Load() < 2 {
		if time.Now().After(replayDeadline) {
			t.Fatal("the rejected publish was never replayed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	want := []string{`"accepted-1"`, `"accepted-2"`, `"accepted-gap"`, `"rejected"`}
	for _, expected := range want {
		select {
		case got := <-received:
			if got != expected {
				t.Fatalf("delivery after replay = %s, want %s", got, expected)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("publish %s was not delivered in replay order", expected)
		}
	}
	select {
	case got := <-received:
		t.Fatalf("unexpected extra delivery after replay: %s", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestPublishSequenceIsPerTopic pins the replay-contract invariant: PUBLISH
// StreamIds are contiguous within each topic, independent of interleaved
// publishes on other topics.
func TestPublishSequenceIsPerTopic(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes := make(chan *Frame, 6)
	rc := NewResilientClient(addr, func(frame *Frame) { pushes <- frame }, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	for _, topic := range []string{"one", "two"} {
		if err := rc.Subscribe(topic, 2*time.Second); err != nil {
			t.Fatalf("Subscribe(%s): %v", topic, err)
		}
	}
	for i := 0; i < 3; i++ {
		for _, topic := range []string{"one", "two"} {
			if _, err := ph.Publish(topic, i); err != nil {
				t.Fatalf("Publish(%s): %v", topic, err)
			}
		}
	}

	sequences := map[string][]uint64{}
	deadline := time.After(2 * time.Second)
	for len(sequences["one"]) < 3 || len(sequences["two"]) < 3 {
		select {
		case frame := <-pushes:
			msg, err := DecodeJSONMessage(frame)
			if err != nil {
				t.Fatal(err)
			}
			sequences[msg.Action] = append(sequences[msg.Action], uint64(frame.Header.StreamId))
		case <-deadline:
			t.Fatalf("timed out collecting publishes: %v", sequences)
		}
	}
	for topic, got := range sequences {
		want := []uint64{1, 2, 3}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s sequences = %v, want %v", topic, got, want)
		}
	}

	for _, topic := range []string{"one", "two"} {
		if err := rc.Unsubscribe(topic, 2*time.Second); err != nil {
			t.Fatalf("Unsubscribe(%s): %v", topic, err)
		}
	}
	rc.Close()
	ph.Close()
	ph.mu.Lock()
	_, seqExists := ph.topicSeqs["one"]
	ph.mu.Unlock()
	if seqExists {
		t.Fatal("topic sequence state leaked after all subscriptions and sessions released it")
	}
}

// TestAcknowledgedClientClearsReplayStateOnSessionExpiry pins that a
// reconnect that cannot resume the session (Resumed == false, e.g. after the
// server janitor reaped the expired session) also clears the client's
// per-topic replay bookkeeping. The server drops the topic's publish
// sequence when the last subscriber/session releases it, so a stale cursor
// would silently drop the fresh publishes of the re-subscribed topic.
func TestAcknowledgedClientClearsReplayStateOnSessionExpiry(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	received := make(chan string, 8)
	rc := NewAcknowledgedResilientClient(addr, func(frame *Frame) bool {
		msg, err := DecodeJSONMessage(frame)
		if err != nil {
			return false
		}
		select {
		case received <- string(msg.Data):
			return true
		default:
			return false
		}
	}, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("t", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	for _, payload := range []string{"one", "two"} {
		if _, err := ph.Publish("t", payload); err != nil {
			t.Fatalf("Publish(%s): %v", payload, err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(received) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(received) != 2 {
		t.Fatalf("received %d initial publishes, want 2", len(received))
	}
	rc.mu.Lock()
	if rc.cursors["t"] != 2 {
		rc.mu.Unlock()
		t.Fatalf("cursor = %d before expiry, want 2", rc.cursors["t"])
	}
	rc.mu.Unlock()
	// Drain so the final assertion observes only the fresh publish.
	for len(received) > 0 {
		<-received
	}

	// Age every session past the TTL and sweep, exactly as the janitor
	// would — the session expires, the server drops the topic's sequence
	// and replay cache. The client transport is still open, so the drop is
	// only discovered on the next reconnect.
	ph.mu.RLock()
	for _, s := range ph.sessions {
		s.lastSeen.Store(time.Now().Add(-ph.sessionTTL - time.Second).UnixNano())
	}
	ph.mu.RUnlock()
	ph.reapExpiredSessions()

	dead := killCurrent(t, rc)
	reconnected := make(chan struct{})
	go func() {
		defer close(reconnected)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			rc.mu.Lock()
			alive := rc.client != nil && rc.client != dead
			rc.mu.Unlock()
			if alive {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("client never reconnected after session expiry")
	}

	if _, err := ph.Publish("t", "fresh"); err != nil {
		t.Fatalf("Publish() after session-expired reconnect: %v", err)
	}
	select {
	case got := <-received:
		if got != `"fresh"` {
			t.Fatalf("push after session-expired reconnect = %s, want \"fresh\"", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fresh publish dropped after session-expired reconnect — stale cursor survived")
	}
}

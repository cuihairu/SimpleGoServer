package proto

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// TestFirstSubscribeBaselinesReplayCursor pins the first-contact baseline:
// the subscribe response's LastSeq is the server-confirmed replay boundary
// — the conn was not a member when sequences at or below it were fanned
// out, so the contiguous accepted prefix starts there, not at zero. On a
// topic already published to 5, the next publish must be acknowledged
// contiguously; without the baseline it would park in acceptedPublishes
// forever and the set would grow without bound.
func TestFirstSubscribeBaselinesReplayCursor(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	// the topic is already published: five sequences exist before the
	// resilient client ever subscribes
	bystander, err := DialTimeout(addr, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("bystander dial: %v", err)
	}
	defer bystander.Close()
	if err := bystander.Subscribe("t", 2*time.Second); err != nil {
		t.Fatalf("bystander subscribe: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := ph.Publish("t", fmt.Sprintf("m%d", i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	received := make(chan uint64, 8)
	rc := NewAcknowledgedResilientClient(addr, func(frame *Frame) bool {
		_, seq, ok := decodePublish(frame)
		if !ok {
			return false
		}
		received <- seq
		return true
	}, nil)
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("t", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	if _, err := ph.Publish("t", "m5"); err != nil {
		t.Fatalf("publish 6: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		rc.mu.Lock()
		cursor := rc.cursors["t"]
		parked := len(rc.acceptedPublishes["t"])
		rc.mu.Unlock()
		if cursor == 6 && parked == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor = %d parked = %d after the next publish; want cursor 6 parked 0 — the subscribe baseline must make seq 6 contiguous", cursor, parked)
		}
		time.Sleep(2 * time.Millisecond)
	}
	select {
	case seq := <-received:
		if seq != 6 {
			t.Fatalf("delivered seq %d, want 6", seq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("seq 6 was never delivered")
	}
}

// TestAlignRacingAcceptDrainsAcrossBaseline pins the response-vs-push race
// on the client side: a publish delivered while the subscribe response was
// still in flight is acknowledged but parked (nothing is contiguous at
// cursor 0); when the response baselines the cursor at the reported
// boundary, the parked acceptance sits above it and must drain across.
func TestAlignRacingAcceptDrainsAcrossBaseline(t *testing.T) {
	rc := NewAcknowledgedResilientClient("127.0.0.1:1", func(frame *Frame) bool { return true }, nil)
	defer rc.Close()

	rc.acknowledgePublish("t", 101)

	rc.mu.Lock()
	rc.alignReplayState("t", 100)
	cursor := rc.cursors["t"]
	parked := len(rc.acceptedPublishes["t"])
	rc.mu.Unlock()
	if cursor != 101 || parked != 0 {
		t.Fatalf("cursor = %d parked = %d after the baseline; want 101/0 — the raced acceptance must drain across the boundary", cursor, parked)
	}
}

// TestAlignFirstContactDoesNotRewindRacedCursor pins the plain client's
// first-contact guard: a publish that raced the subscribe response already
// moved the cursor to 101; the response's older report (100) must not
// rewind it, or the resume handshake would re-request and re-deliver a
// frame the handler has already seen.
func TestAlignFirstContactDoesNotRewindRacedCursor(t *testing.T) {
	rc := NewResilientClient("127.0.0.1:1", nil, nil)
	defer rc.Close()

	rc.setPublishCursor("t", 101)

	rc.mu.Lock()
	rc.alignReplayState("t", 100)
	cursor := rc.cursors["t"]
	rc.mu.Unlock()
	if cursor != 101 {
		t.Fatalf("cursor rewound to %d; the raced publish must keep the cursor at 101", cursor)
	}
}

// TestReplayFloorClosesStaleAcceptances pins the sweep inside the floor
// jump: acceptances parked below the new cursor belong to the closed
// prefix — the frames were evicted, the handler's live acceptance is
// already counted by the jump — and must be dropped, or they would linger
// in the set forever.
func TestReplayFloorClosesStaleAcceptances(t *testing.T) {
	rc := NewAcknowledgedResilientClient("127.0.0.1:1", func(frame *Frame) bool { return true }, nil)
	defer rc.Close()

	// accepted live while the cursor was stuck at 0
	rc.acknowledgePublish("t", 3)

	rc.mu.Lock()
	rc.applyReplayFloorsLocked(map[string]uint64{"t": 5})
	cursor := rc.cursors["t"]
	parked := len(rc.acceptedPublishes["t"])
	rc.mu.Unlock()
	if cursor != 4 || parked != 0 {
		t.Fatalf("cursor = %d parked = %d after the floor jump; want 4/0 — the stale acceptance below the floor must be closed with the prefix", cursor, parked)
	}
}

// TestResumeFloorJumpsCursorPastEvictedFrames pins the replay floor: when
// the session resumes onto a topic whose cache head has been evicted past
// the client's cursor, the handshake's floor says where the obtainable
// prefix starts and the cursor must jump there — frames below it can never
// be replayed, and without the jump every later publish parks in
// acceptedPublishes forever.
func TestResumeFloorJumpsCursorPastEvictedFrames(t *testing.T) {
	ph := NewProtocolHandler(nil)
	t.Cleanup(ph.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	accept := func(ln net.Listener) {
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go serveConn(conn, ph)
			}
		}()
	}
	accept(listener)
	t.Cleanup(func() { _ = listener.Close() })

	received := make(chan uint64, 128)
	rc := NewAcknowledgedResilientClient(addr, func(frame *Frame) bool {
		_, seq, ok := decodePublish(frame)
		if !ok {
			return false
		}
		received <- seq
		return true
	}, &ResilientOptions{BackoffStart: 50 * time.Millisecond, BackoffMax: 200 * time.Millisecond})
	defer rc.Close()
	if err := rc.Connect(2 * time.Second); err != nil {
		t.Fatalf("Connect(): %v", err)
	}
	if err := rc.Subscribe("t", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	// two live publishes acknowledged; the cursor rides at 2
	for i := 1; i <= 2; i++ {
		if _, err := ph.Publish("t", fmt.Sprintf("m%d", i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		select {
		case seq := <-received:
			if seq != uint64(i) {
				t.Fatalf("live delivery %d, want %d", seq, i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("live publish %d was not delivered", i)
		}
	}

	// drop the transport and make every reconnect dial fail while the
	// cache churns: 68 more publishes push seq 70 through the 64-frame
	// per-topic cache, so the replay floor climbs to 7 — frames 3..6 are
	// evicted and no resume can ever supply them. The listener closes
	// before the kill so the reconnect's immediate dial is guaranteed to
	// hit a closed port.
	_ = listener.Close()
	killCurrent(t, rc)
	for i := 3; i <= 70; i++ {
		if _, err := ph.Publish("t", fmt.Sprintf("m%d", i)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// reopen the listener on the same address: the next reconnect dial
	// succeeds, resumes the session, and the floor-jumped cursor turns the
	// replayed 7..70 into a contiguous catch-up
	reopened, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen %s: %v", addr, err)
	}
	accept(reopened)
	t.Cleanup(func() { _ = reopened.Close() })

	deadline := time.Now().Add(5 * time.Second)
	for {
		rc.mu.Lock()
		cursor := rc.cursors["t"]
		parked := len(rc.acceptedPublishes["t"])
		rc.mu.Unlock()
		if cursor == 70 && parked == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor = %d parked = %d after the resume; want 70/0 — the reported floor must jump the cursor past the evicted head", cursor, parked)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// exactly the obtainable prefix was delivered, once: 1 and 2 were
	// consumed and verified in the live loop above, so the replay must
	// supply exactly 7..70 — the evicted 3..6 are gone and must not stall it
	want := make([]uint64, 0, 64)
	for i := uint64(7); i <= 70; i++ {
		want = append(want, i)
	}
	got := make([]uint64, 0, len(want))
	for len(got) < len(want) {
		select {
		case seq := <-received:
			got = append(got, seq)
		case <-time.After(2 * time.Second):
			t.Fatalf("delivered %d frame(s) %v; want %d", len(got), got, len(want))
		}
	}
	select {
	case seq := <-received:
		t.Fatalf("unexpected extra delivery %d after %v", seq, got)
	default:
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivery order %v; want %v", got, want)
		}
	}
}

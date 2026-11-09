package proto

import (
	"testing"
	"time"
)

// TestResumeReplaysMissedPublishes covers the offline catch-up: publishes
// that happened while the client was disconnected are replayed in order
// right after the resume handshake, ahead of live traffic.
func TestResumeReplaysMissedPublishes(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes1 := make(chan *Frame, 8)
	c1, err := Dial(addr, func(f *Frame) { pushes1 <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	// publish #1 while connected: the client sees it live
	if _, err := ph.Publish("ticks", "first"); err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	live := <-pushes1
	cursor := uint64(live.Header.StreamId)

	// the drop; publishes #2 and #3 happen while nobody is listening
	_ = c1.Close()
	if _, err := ph.Publish("ticks", "second"); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	if _, err := ph.Publish("ticks", "third"); err != nil {
		t.Fatalf("Publish 3: %v", err)
	}

	// reconnect presenting the token and the cursor of publish #1
	pushes2 := make(chan *Frame, 8)
	c2, err := Dial(addr, func(f *Frame) { pushes2 <- f })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, map[string]uint64{"ticks": cursor}, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume not recognized")
	}

	// the two missed publishes arrive as replay...
	seqs, ok := collectPublishes(pushes2, 2, 2*time.Second)
	if !ok {
		t.Fatalf("expected 2 replayed publishes, got %d (seqs=%v)", len(seqs), seqs)
	}
	if seqs[0] <= uint32(cursor) || seqs[1] <= seqs[0] {
		t.Fatalf("replay out of order or behind cursor: cursor=%d seqs=%v", cursor, seqs)
	}
	// ...and a live publish afterwards keeps the sequence going
	if _, err := ph.Publish("ticks", "fourth"); err != nil {
		t.Fatalf("Publish 4: %v", err)
	}
	later, ok := collectPublishes(pushes2, 1, 2*time.Second)
	if !ok {
		t.Fatal("no live publish after replay")
	}
	if later[0] <= seqs[1] {
		t.Fatalf("live publish seq %d not after replay %d", later[0], seqs[1])
	}
}

// TestResumeWithFreshCursorReplaysNothing checks that a client reporting
// it has seen everything is not spammed with duplicates.
func TestResumeWithFreshCursorReplaysNothing(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes1 := make(chan *Frame, 8)
	c1, err := Dial(addr, func(f *Frame) { pushes1 <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	if _, err := ph.Publish("ticks", "seen"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	seen := <-pushes1
	_ = c1.Close()

	// cursor at the last seen sequence: nothing to replay
	pushes2 := make(chan *Frame, 8)
	c2, err := Dial(addr, func(f *Frame) { pushes2 <- f })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	if _, err := c2.HandshakeWith(res1.Token, map[string]uint64{"ticks": uint64(seen.Header.StreamId)}, 2*time.Second); err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	select {
	case frame := <-pushes2:
		t.Fatalf("unexpected replay of seq %d the client already saw", frame.Header.StreamId)
	case <-time.After(300 * time.Millisecond):
	}
}

package proto

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestProtocolHandlerSubscribeAndPublish(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)

	sub, _ := EncodeJSON(SUBSCRIBE, 10, "news", nil)
	writeFrame(t, conn, sub)
	ack := readFrame(t, conn)
	if ack.Header.FrameType != RESPONSE {
		t.Fatalf("subscribe ack type = %s", ack.Header.FrameType)
	}
	if ph.Subscribers("news") != 1 {
		t.Fatalf("subscribers = %d, want 1", ph.Subscribers("news"))
	}

	// reads must run concurrently with Publish: a synchronous pipe write
	// would otherwise block until a reader shows up
	type pushResult struct {
		frame *Frame
		err   error
	}
	pushCh := make(chan pushResult, 1)
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		frame, err := Decode(conn)
		pushCh <- pushResult{frame, err}
	}()
	delivered, err := ph.Publish("news", map[string]string{"headline": "go"})
	if err != nil || delivered != 1 {
		t.Fatalf("Publish() = %d, %v; want 1, nil", delivered, err)
	}
	select {
	case got := <-pushCh:
		if got.err != nil {
			t.Fatalf("Decode push: %v", got.err)
		}
		if got.frame.Header.FrameType != PUBLISH {
			t.Fatalf("push type = %s, want PUBLISH", got.frame.Header.FrameType)
		}
		msg, _ := DecodeJSONMessage(got.frame)
		if msg.Action != "news" {
			t.Fatalf("push topic = %q, want news", msg.Action)
		}
		var body map[string]string
		if err := json.Unmarshal(msg.Data, &body); err != nil || body["headline"] != "go" {
			t.Fatalf("push body = %s (%v)", msg.Data, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no push received")
	}
	_ = conn.SetReadDeadline(time.Time{})

	// unsubscribe stops delivery
	unsub, _ := EncodeJSON(UNSUBSCRIBE, 11, "news", nil)
	writeFrame(t, conn, unsub)
	readFrame(t, conn) // drain ack
	if ph.Subscribers("news") != 0 {
		t.Fatalf("subscribers after unsubscribe = %d", ph.Subscribers("news"))
	}
	if delivered, err := ph.Publish("news", "nobody listens"); err != nil || delivered != 0 {
		t.Fatalf("Publish without subscribers = %d, %v", delivered, err)
	}
}

func TestProtocolHandlerPublishDropsBrokenSubscriber(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)
	sub, _ := EncodeJSON(SUBSCRIBE, 1, "t2", nil)
	writeFrame(t, conn, sub)
	readFrame(t, conn)

	// kill the subscriber; the next Publish must prune it, not wedge
	_ = conn.Close()
	time.Sleep(50 * time.Millisecond)

	delivered, err := ph.Publish("t2", "hello?")
	if err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	if delivered != 0 {
		t.Fatalf("delivered = %d on dead subscriber, want 0", delivered)
	}
	if got := ph.Subscribers("t2"); got != 0 {
		t.Fatalf("subscribers = %d after failed publish, want 0", got)
	}
}

// TestPublishOversizeDoesNotConsumeSequence pins the sequence-hole fix:
// Encode rejects oversize payloads, and the rejection must happen BEFORE
// the topic sequence is committed — otherwise the failed publish leaves a
// permanent hole, confirm-mode clients wait forever for the missing seq,
// and every later publish widens the gap.
func TestPublishOversizeDoesNotConsumeSequence(t *testing.T) {
	ph, conn := pipeServer(t, echoHandler)
	sub, _ := EncodeJSON(SUBSCRIBE, 1, "hole-topic", nil)
	writeFrame(t, conn, sub)
	readFrame(t, conn)

	// payload big enough that the encoded envelope exceeds MaxFrameSize
	oversize := json.RawMessage(`[` + strings.Repeat("1,", MaxFrameSize/2) + `1]`)
	delivered, err := ph.Publish("hole-topic", oversize)
	if err == nil {
		t.Fatalf("oversize publish = (%d, nil), want an error", delivered)
	}

	// the rejected publish must not have consumed a sequence number
	if got := ph.topicSequenceLocked("hole-topic"); got != 0 {
		t.Fatalf("topic sequence after rejected publish = %d, want 0 (no hole)", got)
	}
	ph.mu.RLock()
	cache := len(ph.topicCache["hole-topic"])
	ph.mu.RUnlock()
	if cache != 0 {
		t.Fatalf("replay cache holds %d entries after a rejected publish, want 0", cache)
	}

	// the next publish must be sequence 1, not 2 — no gap for a client
	// to wait on. net.Pipe is synchronous, so drain the pushed frame
	// concurrently with the publish.
	type pubResult struct {
		n   int
		err error
	}
	done := make(chan pubResult, 1)
	go func() {
		n, err := ph.Publish("hole-topic", "ok")
		done <- pubResult{n, err}
	}()
	pushed := readFrame(t, conn)
	if pushed.Header.StreamId != 1 {
		t.Fatalf("first good publish sequence = %d, want 1 (no hole consumed the seq)", pushed.Header.StreamId)
	}
	res := <-done
	if res.err != nil {
		t.Fatalf("publish after rejection: %v", res.err)
	}
	if res.n != 1 {
		t.Fatalf("delivered = %d, want 1", res.n)
	}
	if got := ph.topicSequenceLocked("hole-topic"); got != 1 {
		t.Fatalf("topic sequence after first good publish = %d, want 1 (continuous)", got)
	}
}

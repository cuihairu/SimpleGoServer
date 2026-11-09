package proto

import (
	"encoding/json"
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

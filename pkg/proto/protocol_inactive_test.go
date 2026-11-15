package proto

import (
	"encoding/json"
	"net"
	"testing"

	handlerImpl "github.com/cuihairu/simplegoserver/internal/handler"
	"github.com/cuihairu/simplegoserver/pkg/handler"
)

// pipeHandler wires a pipeline (codec + ph) over one end of a pipe and
// pumps it like the reactor does; it returns the client end to write/read
// frames and the server end whose identity is what subscriptions record.
func pipeHandler(t *testing.T, ph *ProtocolHandler) (client, server net.Conn) {
	t.Helper()
	client, server = net.Pipe()
	pipeline := handlerImpl.NewPipeline(server)
	_ = pipeline.AddLast(NewFrameCodec(), ph)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			pipeline.FireRead(server)
			if _, err := server.Read(make([]byte, 0)); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		<-done
	})
	return client, server
}

// publishAsync runs Publish off the test goroutine: net.Pipe is
// synchronous, so the subscriber write would block until the test reads
// the pushed frame.
func publishAsync(t *testing.T, ph *ProtocolHandler, topic string, payload any) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := ph.Publish(topic, payload); err != nil {
			t.Errorf("Publish(%s): %v", topic, err)
		}
	}()
	t.Cleanup(func() { <-done })
}

// mustFrame encodes a frame the test knows cannot fail; a failure is fatal.
func mustFrame(frame *Frame, err error) *Frame {
	if err != nil {
		panic(err) // unreachable for the wire shapes these tests build
	}
	return frame
}

// inactiveContextStub is the minimal InactiveContext HandleInactive needs:
// only the dead connection is consulted, nothing is written back.
type inactiveContextStub struct {
	conn net.Conn
}

func (s *inactiveContextStub) Conn() net.Conn                   { return s.conn }
func (s *inactiveContextStub) Handler() handler.Handler         { return nil }
func (s *inactiveContextStub) Write(handler.Message)            {}
func (s *inactiveContextStub) Close(error)                      { _ = s.conn.Close() }
func (s *inactiveContextStub) Attachment() handler.Attachment   { return nil }
func (s *inactiveContextStub) SetAttachment(handler.Attachment) {}
func (s *inactiveContextStub) HandleInactive(handler.Exception) {}

// TestHandleInactiveReclaimsUnhandshakedSubscriber pins the leak fix: a
// connection that subscribes without ever handshaking has no session, so
// neither the janitor nor a resume would ever clean it up — the inactive
// signal is the only reclaim path. After the transport dies, the topic must
// have no members left and its replay state (cache and sequence) must be
// released: nothing holds it.
func TestHandleInactiveReclaimsUnhandshakedSubscriber(t *testing.T) {
	ph := NewProtocolHandler(echoHandler)
	t.Cleanup(ph.Close)
	client, server := pipeHandler(t, ph)

	writeFrame(t, client, mustFrame(EncodeJSON(SUBSCRIBE, 5, "quiet-topic", nil)))
	ack := readFrame(t, client)
	if ack.Header.FrameType != RESPONSE {
		t.Fatalf("subscribe ack type = %s, want RESPONSE", ack.Header.FrameType)
	}
	if ph.Subscribers("quiet-topic") != 1 {
		t.Fatalf("Subscribers = %d, want 1 before the transport dies", ph.Subscribers("quiet-topic"))
	}
	// a publish before the death caches the frame for replay (read the
	// pushed frame so the pipe write completes)
	publishAsync(t, ph, "quiet-topic", "payload")
	pushed := readFrame(t, client)
	if pushed.Header.FrameType != PUBLISH {
		t.Fatalf("pushed frame type = %s, want PUBLISH", pushed.Header.FrameType)
	}

	// a second subscriber on another topic must not be touched by the
	// first connection's reclaim
	clientB, _ := pipeHandler(t, ph)
	writeFrame(t, clientB, mustFrame(EncodeJSON(SUBSCRIBE, 6, "other-topic", nil)))
	if ack := readFrame(t, clientB); ack.Header.FrameType != RESPONSE {
		t.Fatalf("subscribe ack type = %s, want RESPONSE", ack.Header.FrameType)
	}

	// the transport dies without any session to reap it
	ph.HandleInactive(&inactiveContextStub{conn: server}, nil)

	if got := ph.Subscribers("other-topic"); got != 1 {
		t.Fatalf("Subscribers(other-topic) = %d, want 1 (another connection's membership must survive)", got)
	}
	if got := ph.Subscribers("quiet-topic"); got != 0 {
		t.Fatalf("Subscribers after inactive = %d, want 0 (dead subscriber must be reclaimed)", got)
	}
	ph.mu.RLock()
	_, topicAlive := ph.subs["quiet-topic"]
	cache := len(ph.topicCache["quiet-topic"])
	_, seqAlive := ph.topicSeqs["quiet-topic"]
	ph.mu.RUnlock()
	if topicAlive {
		t.Fatal("topic member set survived the inactive reclaim of an unhandshaked subscriber")
	}
	if cache != 0 {
		t.Fatalf("replay cache holds %d entries after the last subscriber died; want released", cache)
	}
	if seqAlive {
		t.Fatal("topic sequence survived the inactive reclaim; want released with the topic")
	}
}

// TestHandleInactiveKeepsSessionIntent pins the resume contract: a
// handshaked subscriber's transport dying removes the live membership but
// keeps the session's recorded topics, so a reconnect with the token
// restores the subscription — and the replay cache is not dropped while
// the session still records the topic.
func TestHandleInactiveKeepsSessionIntent(t *testing.T) {
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

	writeFrame(t, client1, mustFrame(EncodeJSON(SUBSCRIBE, 2, "resume-topic", nil)))
	readFrame(t, client1) // subscribed ack
	if ph.Subscribers("resume-topic") != 1 {
		t.Fatalf("Subscribers = %d, want 1", ph.Subscribers("resume-topic"))
	}
	publishAsync(t, ph, "resume-topic", "cached")
	if pushed := readFrame(t, client1); pushed.Header.FrameType != PUBLISH {
		t.Fatalf("pushed frame type = %s, want PUBLISH", pushed.Header.FrameType)
	}

	// transport dies; the session must keep the intent, the live set must go
	ph.HandleInactive(&inactiveContextStub{conn: server1}, nil)
	if got := ph.Subscribers("resume-topic"); got != 0 {
		t.Fatalf("Subscribers after inactive = %d, want 0 (dead transport must leave the live set)", got)
	}
	ph.mu.RLock()
	s := ph.sessions[res.Token]
	_, intent := s.topics["resume-topic"]
	cache := len(ph.topicCache["resume-topic"])
	ph.mu.RUnlock()
	if !intent {
		t.Fatal("session lost the recorded topic on transport death; resume would restore nothing")
	}
	if cache == 0 {
		t.Fatal("replay cache dropped while a session still records the topic; resume could not catch up")
	}

	// reconnect with the token: the subscription is restored on the new
	// transport and the cached publish is replayed
	client2, _ := pipeHandler(t, ph)
	hello2, _ := EncodeJSON(HELLO, 3, "hello", &HelloRequest{Versions: []int{ProtocolVersion}, Resume: res.Token})
	writeFrame(t, client2, hello2)
	readFrame(t, client2) // replayed publish
	resumedAck := readFrame(t, client2)
	env2, err := DecodeJSONMessage(resumedAck)
	if err != nil {
		t.Fatalf("resume ack: %v", err)
	}
	var res2 HelloResponse
	if err := json.Unmarshal(env2.Data, &res2); err != nil {
		t.Fatalf("resume ack payload: %v", err)
	}
	if !res2.Resumed {
		t.Fatalf("resume = %v, want true (token %q)", res2.Resumed, res.Token)
	}
	if ph.Subscribers("resume-topic") != 1 {
		t.Fatalf("Subscribers after resume = %d, want 1 (intent must move onto the new transport)", ph.Subscribers("resume-topic"))
	}
}

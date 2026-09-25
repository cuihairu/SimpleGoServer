package proto

import (
	"net"
	"testing"
	"time"
)

// TestSessionJanitorReapsExpiredSessions drives the janitor's sweep
// directly (the production ticker fires once a minute): a session whose
// lastSeen aged past the TTL is dropped together with its connection
// token, while a session that keeps talking survives.
func TestSessionJanitorReapsExpiredSessions(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.sessionTTL = 50 * time.Millisecond
	ph.mu.Unlock()

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()
	if _, err := client.Handshake(2 * time.Second); err != nil {
		t.Fatalf("Handshake(): %v", err)
	}

	// wait for the server to record the session
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ph.mu.RLock()
		n := len(ph.sessions)
		ph.mu.RUnlock()
		if n == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ph.mu.RLock()
	if len(ph.sessions) != 1 {
		t.Fatalf("sessions = %d after handshake, want 1", len(ph.sessions))
	}
	ph.mu.RUnlock()

	// a connected session's lastSeen was just touched, so an immediate
	// sweep must keep it — connected sessions never expire
	ph.reapExpiredSessions()
	ph.mu.RLock()
	if len(ph.sessions) != 1 {
		ph.mu.RUnlock()
		t.Fatal("an active session was reaped before its TTL")
	}
	ph.mu.RUnlock()

	// the client goes away; the sweep drops the session and its
	// connection token once the TTL passes. Reap is a pure function of
	// lastSeen, so age the timestamp directly instead of sleeping past
	// the TTL — a real sleep would bet the CI scheduler on a 30ms
	// margin for a wait the test does not actually need.
	_ = client.Close()
	ph.mu.RLock()
	for _, s := range ph.sessions {
		s.lastSeen.Store(time.Now().Add(-ph.sessionTTL - time.Second).UnixNano())
	}
	ph.mu.RUnlock()
	ph.reapExpiredSessions()
	ph.mu.RLock()
	sessions := len(ph.sessions)
	tokens := len(ph.connTokens)
	ph.mu.RUnlock()
	if sessions != 0 || tokens != 0 {
		t.Fatalf("after TTL: sessions=%d connTokens=%d, want both 0", sessions, tokens)
	}
}

// TestSessionJanitorDetachesExpiredSessionFromTopics pins the sweep's
// member-set reclamation: a session that expires must leave not only the
// session tables but also every subscription member set its transport
// still occupies — and stop pinning the topic's replay cache. Without
// this, a client that subscribed and vanished lingers as a phantom
// subscriber until a publish fails on its dead write; on a quiet topic
// the phantom would hold the replay cache forever.
func TestSessionJanitorDetachesExpiredSessionFromTopics(t *testing.T) {
	ph := NewProtocolHandler(echoHandler)
	t.Cleanup(ph.Close) // stop the session janitor

	srv, client := net.Pipe()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = client.Close()
	})
	// the stub swallows ack writes: no reader needed for setup
	stub := &inboundContextStub{conn: srv}

	hello, err := EncodeJSON(HELLO, 1, "hello", &HelloRequest{Versions: []int{ProtocolVersion}})
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	ph.HandleRead(stub, hello)
	sub, err := EncodeJSON(SUBSCRIBE, 2, "topic", nil)
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	ph.HandleRead(stub, sub)

	ph.mu.RLock()
	subscribed := len(ph.subs["topic"]) == 1 && len(ph.sessions) == 1 && len(ph.connTokens) == 1
	ph.mu.RUnlock()
	if !subscribed {
		t.Fatal("setup failed: session or subscription not recorded")
	}
	// a publish before the drop seeds the replay cache the phantom would
	// otherwise pin (rememberPublish is bookkeeping only, no I/O)
	ph.rememberPublish("topic", 1, []byte("seed-frame"))

	// expire the session and sweep — same direct-drive pattern as
	// TestSessionJanitorReapsExpiredSessions: age lastSeen instead of
	// sleeping past the TTL
	ph.mu.RLock()
	for _, s := range ph.sessions {
		s.lastSeen.Store(time.Now().Add(-ph.sessionTTL - time.Second).UnixNano())
	}
	ph.mu.RUnlock()
	ph.reapExpiredSessions()

	ph.mu.RLock()
	defer ph.mu.RUnlock()
	if len(ph.sessions) != 0 || len(ph.connTokens) != 0 {
		t.Fatalf("sessions=%d connTokens=%d, want both 0", len(ph.sessions), len(ph.connTokens))
	}
	if len(ph.subs) != 0 {
		t.Fatalf("subscription member sets = %d, want the phantom subscriber gone", len(ph.subs))
	}
	if _, cached := ph.topicCache["topic"]; cached {
		t.Fatal("replay cache still pinned by the expired session's transport")
	}
	if ph.cacheBytes != 0 {
		t.Fatalf("cacheBytes = %d after the topic cache was dropped, want 0", ph.cacheBytes)
	}
}

package proto

import (
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

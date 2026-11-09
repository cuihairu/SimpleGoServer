package proto

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNegotiate(t *testing.T) {
	cases := []struct {
		name           string
		clientVersions []int
		want           int
	}{
		{"exact match", []int{ProtocolVersion}, ProtocolVersion},
		{"highest common wins", []int{1, 3, 7}, 1}, // server below speaks {1}
		{"no overlap", []int{2, 3}, 0},
		{"empty offer", nil, 0},
		{"duplicates", []int{1, 1}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := negotiate(tc.clientVersions); got != tc.want {
				t.Fatalf("negotiate(%v) = %d, want %d", tc.clientVersions, got, tc.want)
			}
		})
	}
}

// TestServerHelloPaths drives the server's HELLO handling over a real TCP
// connection: a supported version is confirmed with the chosen version and
// feature list, while an unsupported or empty offer gets an error response
// the client is expected to react to by disconnecting.
func TestServerHelloPaths(t *testing.T) {
	addr, _ := startTCPServer(t, nil)

	// supported versions -> ack with the common one. Each path below uses
	// its own connection: a connection handshakes exactly once.
	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	resp, err := client.roundTrip(HELLO, RESPONSE, "hello", &HelloRequest{Versions: []int{3, ProtocolVersion, 2}}, 2*time.Second)
	if err != nil {
		t.Fatalf("HELLO: %v", err)
	}
	if !resp.OK() {
		t.Fatalf("HELLO with supported version rejected: %s", resp.Err.Error)
	}
	var agreed HelloResponse
	if err := json.Unmarshal(resp.Data, &agreed); err != nil {
		t.Fatalf("decode HELLO ack: %v", err)
	}
	if agreed.Version != ProtocolVersion {
		t.Fatalf("negotiated version = %d, want %d", agreed.Version, ProtocolVersion)
	}
	if len(agreed.Features) == 0 {
		t.Fatal("HELLO ack carried no features")
	}
	_ = client.Close()

	// unsupported offer -> structured error, connection still open
	reject, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer reject.Close()
	resp, err = reject.roundTrip(HELLO, RESPONSE, "hello", &HelloRequest{Versions: []int{42}}, 2*time.Second)
	if err != nil {
		t.Fatalf("HELLO (unsupported): %v", err)
	}
	if resp.OK() {
		t.Fatal("HELLO with unsupported version unexpectedly accepted")
	}

	// empty offer -> same error path, again on a fresh connection
	empty, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer empty.Close()
	resp, err = empty.roundTrip(HELLO, RESPONSE, "hello", &HelloRequest{}, 2*time.Second)
	if err != nil {
		t.Fatalf("HELLO (empty): %v", err)
	}
	if resp.OK() {
		t.Fatal("HELLO with empty version list unexpectedly accepted")
	}
}

// TestClientResumeRestoresSubscriptions covers the reconnect flow end to
// end: a session drops after subscribing, the client reconnects presenting
// the token, and the subscription is live again — pushes reach the new
// connection without an explicit re-subscribe.
func TestClientResumeRestoresSubscriptions(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	c1, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if res1.Resumed {
		t.Fatal("first handshake unexpectedly resumed a session")
	}
	if res1.Token == "" {
		t.Fatal("handshake returned no session token")
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	_ = c1.Close() // the drop

	pushes := make(chan *Frame, 1)
	c2, err := Dial(addr, func(frame *Frame) { pushes <- frame })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume with a live session token was not recognized")
	}
	if res2.Token != res1.Token {
		t.Fatalf("resume changed the token: got %q, want %q", res2.Token, res1.Token)
	}
	if got := ph.Subscribers("ticks"); got != 1 {
		t.Fatalf("subscribers after resume = %d, want 1 (the dead transport must be gone)", got)
	}

	if _, err := ph.Publish("ticks", map[string]string{"hello": "again"}); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	select {
	case frame := <-pushes:
		msg, err := DecodeJSONMessage(frame)
		if err != nil || msg.Action != "ticks" {
			t.Fatalf("unexpected push %v (err=%v)", frame, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no push after resume — subscription was not restored")
	}
}

// TestClientResumeUnknownTokenStartsFresh checks that a bogus token is not
// an error: the client just gets a new session and subscribes normally.
func TestClientResumeUnknownTokenStartsFresh(t *testing.T) {
	addr, _ := startTCPServer(t, nil)

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()
	res, err := client.HandshakeWith("no-such-token", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("HandshakeWith(): %v", err)
	}
	if res.Resumed {
		t.Fatal("unknown token was reported as resumed")
	}
	if res.Token == "" || res.Token == "no-such-token" {
		t.Fatalf("fresh session token = %q, want a new one", res.Token)
	}
}

// TestClientResumeExpiredSessionIsFresh shrinks the session TTL so the
// reconnect arrives after expiry: the token is not honored, mirroring an
// abandoned session.
func TestClientResumeExpiredSessionIsFresh(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	c1, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	_ = c1.Close()

	ph.mu.Lock()
	ph.sessionTTL = -time.Second // every resume attempt is now "too late"
	ph.mu.Unlock()

	client, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer client.Close()
	res2, err := client.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if res2.Resumed {
		t.Fatal("expired session was resumed")
	}
}

// TestActiveSessionOutlivesTTL pins what keeps a session alive: inbound
// frames refresh its timestamp, so a connection that only ever heartbeats
// (KeepAlive) stays resumable no matter how long it has been connected —
// while a silent session still ages out at the TTL. Without the refresh,
// the janitor would expire the session of a live connection and its later
// resume would silently start from scratch, dropping the subscriptions.
func TestActiveSessionOutlivesTTL(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.sessionTTL = 200 * time.Millisecond
	ph.mu.Unlock()

	// active group: keep pinging well past the TTL, then resume
	c1, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	for i := 0; i < 8; i++ {
		time.Sleep(50 * time.Millisecond)
		if err := c1.Ping(2 * time.Second); err != nil {
			t.Fatalf("Ping %d: %v", i, err)
		}
	}
	_ = c1.Close()

	c2, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("a heartbeat-alive session was expired by the server")
	}

	// silent group: same TTL passes with no traffic, resume must fail
	c3, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res3, err := c3.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	_ = c3.Close()
	time.Sleep(300 * time.Millisecond)

	c4, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c4.Close()
	res4, err := c4.HandshakeWith(res3.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if res4.Resumed {
		t.Fatal("a silent session outlived the TTL")
	}
}

// TestDroppedSubscriberRevivesOnResume verifies the accounting behind
// resume: a write failure drops the transport from the subscription table
// (TCP semantics force repeated publishes until the server notices), but
// the session keeps the subscription intent, so reconnecting restores it
// — the client wanted the topic and never said otherwise.
func TestDroppedSubscriberRevivesOnResume(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	c1, err := Dial(addr, nil)
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

	_ = c1.Close() // dead transport
	// TCP semantics: the first write to a closed peer still succeeds (the
	// RST has not come back yet), so keep publishing until the server has
	// observed the write failure and dropped the subscriber
	deadline := time.Now().Add(2 * time.Second)
	for ph.Subscribers("ticks") > 0 && time.Now().Before(deadline) {
		_, _ = ph.Publish("ticks", "x")
		time.Sleep(10 * time.Millisecond)
	}
	if got := ph.Subscribers("ticks"); got != 0 {
		t.Fatalf("subscribers = %d after the transport died, want 0", got)
	}

	c2, err := Dial(addr, nil)
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, nil, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume not recognized")
	}
	if got := ph.Subscribers("ticks"); got != 1 {
		t.Fatalf("subscribers after resume = %d, want 1 — the session kept the intent", got)
	}
}

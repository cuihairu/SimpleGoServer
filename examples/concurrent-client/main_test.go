package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/cuihairu/simplegoserver/pkg/reactor"
)

// echoHandler mirrors the echo-server example's business logic.
func echoHandler(action string, data []byte) (any, error) {
	if action != "echo" {
		return nil, fmt.Errorf("unknown action %q", action)
	}
	// json.RawMessage keeps the payload as-is; a plain []byte would be
	// base64-encoded by json.Marshal and break the round trip
	return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
}

// startServer spins up a real reactor with the given business handler and
// returns its address along with the shared protocol handler, so tests can
// publish to subscribers.
func startServer(t *testing.T, biz func(action string, data []byte) (any, error)) (string, *proto.ProtocolHandler) {
	t.Helper()
	protocol := proto.NewProtocolHandler(biz)
	initializer := func(p handler.Pipeline) error {
		_ = p.AddLast(proto.NewFrameCodec(), protocol)
		return nil
	}
	server, err := reactor.NewReactor(&reactor.ServerOptions{Listener: "tcp://127.0.0.1:0"}, nil, nil, initializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	go server.Run()
	t.Cleanup(server.ShutdownGracefully)
	return server.Addr().String(), protocol
}

// startHangingServer accepts connections and never answers, so client calls
// run into their timeouts.
func startHangingServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				time.Sleep(10 * time.Second)
			}(c)
		}
	}()
	return ln.Addr().String()
}

func TestRunAllAndSummarize(t *testing.T) {
	addr, _ := startServer(t, echoHandler)
	results := runAll(addr, 3, 2, 2*time.Second, 0)
	if len(results) != 3 {
		t.Fatalf("runAll returned %d results, want 3", len(results))
	}
	for _, r := range results {
		if r.err != nil {
			t.Fatalf("client %d: %v", r.id, r.err)
		}
		if r.requests != 2 || r.ok != 2 {
			t.Fatalf("client %d: %d/%d ok, want 2/2", r.id, r.ok, r.requests)
		}
	}
	summarize(results)  // scoreboard only; failures print per client
	summarize([]result{ // the per-client failure report
		{id: 9, err: errors.New("boom")},
		{id: 1, requests: 2, ok: 1, latency: 3 * time.Millisecond},
	})
}

// TestMainDrivesTheWholeFlow runs the real main() against a live server.
func TestMainDrivesTheWholeFlow(t *testing.T) {
	addr, _ := startServer(t, echoHandler)
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{
		"concurrent-client", "-addr", addr,
		"-clients", "2", "-requests", "1", "-timeout", "2s", "-subscribe", "100ms",
	}
	main()
}

func TestRunClientSubscribeFailsOnStrictServer(t *testing.T) {
	addr, protocol := startServer(t, echoHandler)
	protocol.RequireHello(true) // subscribing before HELLO gets the conn closed
	res := runClient(addr, 0, 1, 2*time.Second, 200*time.Millisecond)
	if res.err == nil || !strings.Contains(res.err.Error(), "subscribe:") {
		t.Fatalf("a strict server must fail the subscribe: %v", res.err)
	}
}

func TestRunClientEchoRoundTrip(t *testing.T) {
	addr, _ := startServer(t, echoHandler)
	res := runClient(addr, 7, 4, 2*time.Second, 0)
	if res.err != nil {
		t.Fatalf("runClient(): %v", res.err)
	}
	if res.ok != 4 {
		t.Fatalf("ok = %d, want 4", res.ok)
	}
}

func TestRunClientDialFailure(t *testing.T) {
	res := runClient("127.0.0.1:1", 3, 1, 300*time.Millisecond, 0)
	if res.err == nil {
		t.Fatal("dialing a refused port must fail the client")
	}
}

func TestRunClientCallErrorOnSilentServer(t *testing.T) {
	addr := startHangingServer(t)
	res := runClient(addr, 5, 1, 250*time.Millisecond, 0)
	if res.err == nil {
		t.Fatal("a server that never answers must fail the call")
	}
}

func TestRunClientSurfacesServerError(t *testing.T) {
	addr, _ := startServer(t, func(action string, data []byte) (any, error) {
		return nil, errors.New("denied")
	})
	res := runClient(addr, 2, 1, 2*time.Second, 0)
	if res.err == nil || !strings.Contains(res.err.Error(), "server error: denied") {
		t.Fatalf("a server-side error must surface: %v", res.err)
	}
}

func TestRunClientDetectsUndecodableResponse(t *testing.T) {
	addr, _ := startServer(t, func(action string, data []byte) (any, error) {
		return 42, nil // not an object: the envelope decode must fail
	})
	res := runClient(addr, 2, 1, 2*time.Second, 0)
	if res.err == nil || !strings.Contains(res.err.Error(), "decode response") {
		t.Fatalf("a non-object response must fail to decode: %v", res.err)
	}
}

func TestRunClientDetectsEchoMismatch(t *testing.T) {
	addr, _ := startServer(t, func(action string, data []byte) (any, error) {
		return map[string]any{"action": action, "data": "different"}, nil
	})
	res := runClient(addr, 2, 1, 2*time.Second, 0)
	if res.err == nil || !strings.Contains(res.err.Error(), "echo mismatch") {
		t.Fatalf("a corrupted echo must be detected: %v", res.err)
	}
}

func TestRunClientSubscriberReceivesPushes(t *testing.T) {
	addr, protocol := startServer(t, func(action string, data []byte) (any, error) {
		return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
	})
	// the subscriber's connection only lives while runClient is working,
	// so give it 300 requests (~tens of ms) and watch the topic until a
	// tick is delivered; the burst that follows outruns collectPushes'
	// printing, which drives the read loop's onEvent into its full-channel
	// default branch
	resCh := make(chan result, 1)
	go func() { resCh <- runClient(addr, 0, 300, 2*time.Second, 10*time.Millisecond) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		delivered, _ := protocol.Publish("ticks", map[string]any{"note": "warm"})
		if delivered > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the subscription never delivered a tick")
		}
		time.Sleep(2 * time.Millisecond)
	}
	for i := 0; i < 300; i++ {
		_, _ = protocol.Publish("ticks", map[string]any{"note": "burst"})
	}

	res := <-resCh
	if res.err != nil {
		t.Fatalf("runClient(): %v", res.err)
	}
	if res.ok != 300 {
		t.Fatalf("ok = %d, want 300", res.ok)
	}
}

func TestCollectPushesPrintsAndIgnoresGarbage(t *testing.T) {
	good, err := proto.EncodeJSON(proto.PUBLISH, 1, "ticks", map[string]any{"note": "x"})
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	garbage := &proto.Frame{
		Header:  proto.FrameHeader{FrameType: proto.PUBLISH, StreamId: 2},
		Payload: []byte("not json"),
	}
	pushes := make(chan *proto.Frame, 2)
	pushes <- good
	pushes <- garbage

	done := make(chan struct{})
	go func() {
		defer close(done)
		collectPushes(pushes, 100*time.Millisecond)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("collectPushes never returned after its window")
	}
}

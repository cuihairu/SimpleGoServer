package main

import (
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/cuihairu/simplegoserver/pkg/reactor"
)

// freePort reserves an ephemeral port and hands the address back, so tests
// can dial a server that picks its own listener internally.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestHandleDemoEchoesReportsTimeAndRejects(t *testing.T) {
	echo, err := handleDemo("echo", []byte(`"hi"`))
	if err != nil {
		t.Fatalf("handleDemo(echo): %v", err)
	}
	m, ok := echo.(map[string]any)
	if !ok {
		t.Fatalf("echo response is %T, want a map", echo)
	}
	if m["action"] != "echo" {
		t.Fatalf("echo action = %v", m["action"])
	}
	if data, ok := m["data"].(json.RawMessage); !ok || string(data) != `"hi"` {
		t.Fatalf("echo data = %v, want the payload passed through", m["data"])
	}

	now, err := handleDemo("time", nil)
	if err != nil {
		t.Fatalf("handleDemo(time): %v", err)
	}
	m, ok = now.(map[string]any)
	if !ok {
		t.Fatalf("time response is %T, want a map", now)
	}
	if ts, ok := m["now"].(string); !ok || ts == "" {
		t.Fatalf("time response carries no timestamp: %v", m)
	}

	if _, err := handleDemo("nope", nil); err == nil || !strings.Contains(err.Error(), `unknown action "nope"`) {
		t.Fatalf("handleDemo(nope) = %v, want an unknown-action error", err)
	}
}

func TestRunServerServesUntilSignalled(t *testing.T) {
	addr := freePort(t)
	sigCh := make(chan os.Signal, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- runServer(addr, 20*time.Millisecond, 0, sigCh) }()

	time.Sleep(300 * time.Millisecond) // let it bind and park on the signal
	// one real request so the pipeline initializer runs end to end; the
	// broadcast interval above is also live now, ticking with no subscriber
	client, err := proto.Dial(addr, func(*proto.Frame) {})
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	if _, err := client.Call("echo", "hi", 2*time.Second); err != nil {
		t.Fatalf("Call(echo): %v", err)
	}
	_ = client.CloseGracefully(time.Second)

	close(sigCh) // a closed channel unblocks <-sigCh at once

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runServer(): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runServer did not return after the signal")
	}
}

func TestRunServerRejectsUnusableListener(t *testing.T) {
	if err := runServer("127.0.0.1:99999", 0, 0, make(chan os.Signal, 1)); err == nil {
		t.Fatal("an unbindable listener must fail runServer")
	}
}

func TestMainStartsAndExitsOnListenerFailure(t *testing.T) {
	oldArgs := os.Args
	oldExit := osExit
	defer func() { os.Args = oldArgs; osExit = oldExit }()
	os.Args = []string{"echo-server", "-addr", "127.0.0.1:99999"}
	code := 0
	osExit = func(c int) { code = c }

	main()

	if code != 1 {
		t.Fatalf("exit code = %d, want 1 for a failed startup", code)
	}
}

// TestBroadcastLoopReachesSubscribersAndStops drives broadcastLoop against a
// real reactor so a subscribed client proves ticks actually get delivered.
func TestBroadcastLoopReachesSubscribersAndStops(t *testing.T) {
	protocol := proto.NewProtocolHandler(handleDemo)
	initializer := func(p handler.Pipeline) error {
		_ = p.AddLast(proto.NewFrameCodec(), protocol)
		return nil
	}
	server, err := reactor.NewReactor(&reactor.ServerOptions{Listener: "tcp://127.0.0.1:0"}, nil, nil, initializer, nil)
	if err != nil {
		t.Fatalf("NewReactor(): %v", err)
	}
	go server.Run()

	pushes := make(chan *proto.Frame, 16)
	client, err := proto.Dial(server.Addr().String(), func(f *proto.Frame) {
		select {
		case pushes <- f:
		default:
		}
	})
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer client.Close()
	if err := client.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		broadcastLoop(protocol, 10*time.Millisecond, stop)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for len(pushes) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no tick ever reached the subscriber")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcastLoop ignored the stop channel")
	}

	_ = client.CloseGracefully(2 * time.Second)
	server.ShutdownGracefully()
}

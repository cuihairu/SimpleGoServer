package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/cuihairu/simplegoserver/pkg/reactor"
)

// echoHandler is the demo business logic the cli is aimed at.
func echoHandler(action string, data []byte) (any, error) {
	if action != "echo" {
		return nil, fmt.Errorf("unknown action %q", action)
	}
	return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
}

// testServer is a live reactor the tests can dial and shut down.
type testServer struct {
	addr     string
	protocol *proto.ProtocolHandler
	reactor  *reactor.Reactor
}

// startServer spins up a real reactor with the given business handler.
func startServer(t *testing.T, biz func(action string, data []byte) (any, error)) *testServer {
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
	return &testServer{addr: server.Addr().String(), protocol: protocol, reactor: server}
}

// startHangingServer accepts connections and never answers, so calls run
// into their timeouts.
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

// startGarbageHandshakeServer accepts connections and answers every HELLO
// with a response whose payload is not a HelloResponse, so the client's
// handshake fails to decode.
func startGarbageHandshakeServer(t *testing.T) string {
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
				for {
					frame, err := proto.DecodeStreamed(c)
					if err != nil {
						return
					}
					if frame.Header.FrameType == proto.HELLO {
						reply, _ := proto.EncodeJSON(proto.RESPONSE, frame.Header.StreamId, "hello", 123)
						wire, err := proto.Encode(reply)
						if err != nil {
							return
						}
						if _, err := c.Write(wire); err != nil {
							return
						}
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

// runAsync runs the cli against args in the background and returns the exit
// code once it finishes.
func runAsync(args ...string) <-chan int {
	ch := make(chan int, 1)
	go func() { ch <- run(args) }()
	return ch
}

func waitForCode(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case code := <-ch:
		return code
	case <-time.After(10 * time.Second):
		t.Fatal("the cli never finished")
		return -1
	}
}

func TestPrintEventHandlesValidAndGarbageFrames(t *testing.T) {
	good, err := proto.EncodeJSON(proto.PUBLISH, 1, "ticks", map[string]any{"note": "x"})
	if err != nil {
		t.Fatalf("EncodeJSON(): %v", err)
	}
	printEvent(good)
	printEvent(&proto.Frame{
		Header:  proto.FrameHeader{FrameType: proto.PUBLISH, StreamId: 2},
		Payload: []byte("not json"),
	})
}

func TestRunRejectsUnknownFlag(t *testing.T) {
	if code := run([]string{"--definitely-not-a-flag"}); code != 2 {
		t.Fatalf("run() = %d, want 2 for a parse failure", code)
	}
}

func TestMainDelegatesExitCodeToRun(t *testing.T) {
	oldArgs := os.Args
	oldExit := osExit
	defer func() { os.Args = oldArgs; osExit = oldExit }()
	os.Args = []string{"client", "--definitely-not-a-flag"}
	code := 0
	osExit = func(c int) { code = c }

	main()

	if code != 2 {
		t.Fatalf("main exit code = %d, want 2 for a parse failure", code)
	}
}

func TestRunPlainDialFailure(t *testing.T) {
	if code := run([]string{"-addr", "127.0.0.1:1", "-timeout", "300ms"}); code != 1 {
		t.Fatalf("run() = %d, want 1 for a failed dial", code)
	}
}

func TestRunResilientConnectFailure(t *testing.T) {
	if code := run([]string{"-resilient", "-addr", "127.0.0.1:1", "-timeout", "300ms"}); code != 1 {
		t.Fatalf("run() = %d, want 1 for a failed connect", code)
	}
}

func TestRunEchoRoundTripWithKeepalive(t *testing.T) {
	server := startServer(t, echoHandler)
	// the default keepalive interval is live here, covering the KeepAlive
	// branch; the deferred stop runs before the goodbye
	code := run([]string{"-addr", server.addr, "-action", "echo", "-data", `"hi"`, "-timeout", "2s"})
	if code != 0 {
		t.Fatalf("run() = %d, want 0 for a successful round trip", code)
	}
}

func TestRunHandshakeOk(t *testing.T) {
	server := startServer(t, echoHandler)
	code := run([]string{"-addr", server.addr, "-hello", "-action", "echo", "-data", `"hi"`, "-timeout", "2s"})
	if code != 0 {
		t.Fatalf("run() = %d, want 0 for a successful handshake", code)
	}
}

func TestRunHandshakeRejectsGarbage(t *testing.T) {
	addr := startGarbageHandshakeServer(t)
	if code := run([]string{"-addr", addr, "-hello", "-timeout", "2s"}); code != 1 {
		t.Fatalf("run() = %d, want 1 for a malformed handshake", code)
	}
}

func TestRunSubscribeFailsOnStrictServer(t *testing.T) {
	server := startServer(t, echoHandler)
	server.protocol.RequireHello(true) // subscribing before HELLO gets the conn closed
	code := run([]string{"-addr", server.addr, "-subscribe", "ticks", "-timeout", "2s"})
	if code != 1 {
		t.Fatalf("run() = %d, want 1 for a denied subscribe", code)
	}
}

func TestRunCallErrorOnSilentServer(t *testing.T) {
	addr := startHangingServer(t)
	code := run([]string{"-addr", addr, "-action", "echo", "-timeout", "250ms", "-keepalive", "0"})
	if code != 1 {
		t.Fatalf("run() = %d, want 1 for a timed-out call", code)
	}
}

func TestRunSurfacesServerSideError(t *testing.T) {
	server := startServer(t, func(action string, data []byte) (any, error) {
		return nil, fmt.Errorf("denied")
	})
	code := run([]string{"-addr", server.addr, "-action", "echo", "-timeout", "2s", "-keepalive", "0"})
	if code != 0 {
		t.Fatalf("run() = %d, want 0: a structured error response is not a cli failure", code)
	}
}

// TestRunSubscribeWatchReceivesPushesUntilTimer covers the watch loop's
// event and timer branches in one run.
func TestRunSubscribeWatchReceivesPushesUntilTimer(t *testing.T) {
	server := startServer(t, echoHandler)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _ = server.protocol.Publish("ticks", map[string]any{"note": "tick"})
			case <-stop:
				return
			}
		}
	}()
	code := waitForCode(t, runAsync("-addr", server.addr, "-subscribe", "ticks", "-watch", "300ms", "-keepalive", "0", "-timeout", "2s"))
	if code != 0 {
		t.Fatalf("run() = %d, want 0 after the watch window", code)
	}
}

// TestRunSubscribeWatchesUntilSignal covers the implicit one-hour watch: a
// trapped SIGTERM is the way out.
func TestRunSubscribeWatchesUntilSignal(t *testing.T) {
	server := startServer(t, echoHandler)
	ch := runAsync("-addr", server.addr, "-subscribe", "ticks", "-timeout", "2s")
	time.Sleep(400 * time.Millisecond) // dial + subscribe + signal handler setup
	_ = syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	if code := waitForCode(t, ch); code != 0 {
		t.Fatalf("run() = %d, want 0 after the signal", code)
	}
}

// TestRunWatchReportsClosedConnection kills the server mid-watch so the
// loop exits through client.Done().
func TestRunWatchReportsClosedConnection(t *testing.T) {
	server := startServer(t, echoHandler)
	ch := runAsync("-addr", server.addr, "-subscribe", "ticks", "-watch", "30s", "-keepalive", "0", "-timeout", "2s")
	time.Sleep(400 * time.Millisecond)
	server.reactor.ShutdownGracefully()
	if code := waitForCode(t, ch); code != 0 {
		t.Fatalf("run() = %d, want 0 once the connection closed", code)
	}
}

// TestCoerceJSONPayload pins the -data ergonomics contract: raw JSON goes
// through untouched, bare text is wrapped into a JSON string.
func TestCoerceJSONPayload(t *testing.T) {
	got := coerceJSONPayload(`{"a":1}`)
	if _, ok := got.(json.RawMessage); !ok {
		t.Fatalf("coerceJSONPayload(valid json) = %T, want json.RawMessage", got)
	}
	if got := coerceJSONPayload("hello"); got != "hello" {
		t.Fatalf("coerceJSONPayload(bare text) = %v, want the raw string", got)
	}
}

// TestRunCallSendsBareTextAsJSONString drives a full round trip with the
// most natural -data input, the one that used to die on a marshal error.
func TestRunCallSendsBareTextAsJSONString(t *testing.T) {
	server := startServer(t, echoHandler)
	code := run([]string{"-addr", server.addr, "-action", "echo", "-data", "hello-from-cli", "-timeout", "2s", "-keepalive", "0"})
	if code != 0 {
		t.Fatalf("run() = %d, want 0 for a bare-text payload", code)
	}
}

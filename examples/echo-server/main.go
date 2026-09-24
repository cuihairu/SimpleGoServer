// Command echo-server is a complete, runnable example of the SimpleGoServer
// framework. It demonstrates:
//
//   - wiring a custom protocol (frame codec + business handler) into the
//     reactor pipeline;
//   - request/response handling with plain functions returning (value, error);
//   - server-initiated publish/subscribe broadcasts;
//   - dead-link handling: server-side idle timeout + client-side keepalive;
//   - graceful shutdown on SIGINT/SIGTERM.
//
// Start it and point the concurrent-client example at it:
//
//	go run ./examples/echo-server
//	go run ./examples/concurrent-client
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/cuihairu/simplegoserver/pkg/reactor"
)

// osExit is a variable so tests can observe the failure exit without
// terminating the test binary.
var osExit = os.Exit

// handleDemo is the entire business logic of the server. It receives the
// action name and the raw JSON payload of a request and returns the value to
// send back — or an error, which becomes a structured error response on the
// same stream id without tearing the connection down.
func handleDemo(action string, data []byte) (any, error) {
	switch action {
	case "echo":
		// pass the payload straight through; json.RawMessage keeps it as-is
		return map[string]any{"action": action, "data": json.RawMessage(data)}, nil
	case "time":
		return map[string]any{"action": action, "now": time.Now().Format(time.RFC3339Nano)}, nil
	default:
		return nil, fmt.Errorf("unknown action %q", action)
	}
}

// Flags are registered exactly once per process (init), so main stays a
// thin, repeatable shell that tests can drive more than once.
var (
	addr           = flag.String("addr", "127.0.0.1:8080", "listen address")
	broadcastEvery = flag.Duration("broadcast", 2*time.Second, "interval of the ticks topic broadcast (0 disables)")
	idle           = flag.Duration("idle", 60*time.Second, "close connections silent for this long (0 disables)")
)

func main() {
	flag.Parse()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	if err := runServer(*addr, *broadcastEvery, *idle, sigCh); err != nil {
		fmt.Fprintf(os.Stderr, "start server: %v\n", err)
		osExit(1)
	}
}

// runServer wires the protocol and pipeline, serves until a value arrives on
// sigCh (a SIGINT/SIGTERM in production), then shuts down gracefully. It is
// split out of main so tests can drive the whole lifecycle with a synthetic
// signal channel and an ephemeral port.
func runServer(addr string, broadcastEvery, idle time.Duration, sigCh <-chan os.Signal) error {
	// One shared protocol handler for every connection: it is safe for
	// concurrent use (the subscription table is internally locked), and
	// sharing it is what lets the broadcast goroutine below reach all
	// subscribers regardless of which connection they arrived on.
	protocol := proto.NewProtocolHandler(handleDemo)

	// pipelineInitializer runs once per accepted connection: frame codec
	// first (bytes <-> frames), business handler second (frames <-> logic).
	initializer := func(p handler.Pipeline) error {
		_ = p.AddLast(proto.NewFrameCodec(), protocol)
		return nil
	}

	options := &reactor.ServerOptions{
		Listener: "tcp://" + addr,
		// reap connections that stay silent for this long; any received
		// frame — heartbeat or business traffic — resets the timer, so
		// keepalive clients (see concurrent-client) survive it while a
		// dead peer is cleaned up instead of leaking a socket
		IdleTimeout: idle,
	}
	server, err := reactor.NewReactor(options, nil, nil, initializer, nil)
	if err != nil {
		return err
	}

	go func() {
		server.Run()
	}()

	stopBroadcast := make(chan struct{})
	if broadcastEvery > 0 {
		go broadcastLoop(protocol, broadcastEvery, stopBroadcast)
	}

	fmt.Printf("echo-server listening on %s (ctrl-c to shut down)\n", addr)

	<-sigCh

	fmt.Println("shutting down…")
	close(stopBroadcast)
	server.ShutdownGracefully()
	fmt.Println("bye")
	return nil
}

// broadcastLoop publishes a tick to every subscriber of the "ticks" topic —
// the server pushing without being asked, the second protocol mode — until
// stop is closed.
func broadcastLoop(protocol *proto.ProtocolHandler, every time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			// the payload is our own fixed map of strings, so Publish cannot
			// fail to encode it and there is deliberately no error path
			delivered, _ := protocol.Publish("ticks", map[string]any{
				"now":  now.Format(time.RFC3339),
				"note": "server push",
			})
			if delivered > 0 {
				fmt.Printf("tick delivered to %d subscriber(s)\n", delivered)
			}
		case <-stop:
			return
		}
	}
}

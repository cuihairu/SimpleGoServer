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
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/handler"
	"github.com/cuihairu/simplegoserver/pkg/proto"
	"github.com/cuihairu/simplegoserver/pkg/reactor"
)

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

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	broadcastEvery := flag.Duration("broadcast", 2*time.Second, "interval of the ticks topic broadcast (0 disables)")
	idle := flag.Duration("idle", 60*time.Second, "close connections silent for this long (0 disables)")
	flag.Parse()

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
		Listener: "tcp://" + *addr,
		// reap connections that stay silent for this long; any received
		// frame — heartbeat or business traffic — resets the timer, so
		// keepalive clients (see concurrent-client) survive it while a
		// dead peer is cleaned up instead of leaking a socket
		IdleTimeout: *idle,
	}
	server, err := reactor.NewReactor(options, nil, nil, initializer, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start server: %v\n", err)
		os.Exit(1)
	}

	go func() {
		server.Run()
	}()

	if *broadcastEvery > 0 {
		go broadcastLoop(protocol, *broadcastEvery)
	}

	fmt.Printf("echo-server listening on %s (ctrl-c to shut down)\n", *addr)

	// SIGINT/SIGTERM trigger the staged shutdown: stop accepting, let live
	// connections drain, force-close the rest, stop the worker loops.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("shutting down…")
	server.ShutdownGracefully()
	fmt.Println("bye")
}

// broadcastLoop publishes a tick to every subscriber of the "ticks" topic —
// the server pushing without being asked, the second protocol mode.
func broadcastLoop(protocol *proto.ProtocolHandler, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for now := range ticker.C {
		delivered, err := protocol.Publish("ticks", map[string]any{
			"now":  now.Format(time.RFC3339),
			"note": "server push",
		})
		if err != nil {
			log.Printf("broadcast: %v", err)
		}
		if delivered > 0 {
			fmt.Printf("tick delivered to %d subscriber(s)\n", delivered)
		}
	}
}

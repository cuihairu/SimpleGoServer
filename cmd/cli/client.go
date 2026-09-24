// Command client is a protocol client for the SimpleGoServer demo server.
// It can send one request, subscribe to a topic and keep receiving pushes,
// or both. Run with -h for usage.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// protocolClient is the surface cli needs; both the plain client and the
// auto-reconnecting one satisfy it.
type protocolClient interface {
	Call(action string, data any, timeout time.Duration) (*proto.Response, error)
	Subscribe(topic string, timeout time.Duration) error
	Done() <-chan struct{}
	Close() error
	CloseGracefully(timeout time.Duration) error
}

// osExit is a variable so tests can run main() without terminating the
// test binary.
var osExit = os.Exit

func main() {
	osExit(run(os.Args[1:]))
}

// run parses args, performs the requested operations and returns the
// process exit code. Failures are reported on stderr and yield 1 — defers
// below still release the connection, which a bare os.Exit would skip.
func run(args []string) int {
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "server address")
	action := fs.String("action", "", "request action (e.g. echo)")
	data := fs.String("data", "", "request payload; raw JSON, anything else is sent as a JSON string")
	topic := fs.String("subscribe", "", "topic to subscribe; with -watch keeps receiving pushes")
	watch := fs.Duration("watch", 0, "keep running for this long after the request (e.g. 30s)")
	timeout := fs.Duration("timeout", 5*time.Second, "per-request timeout")
	keepalive := fs.Duration("keepalive", 30*time.Second, "ping interval, keeps the connection alive through server idle timeouts (0 disables)")
	hello := fs.Bool("hello", false, "negotiate the protocol version before anything else")
	resilient := fs.Bool("resilient", false, "auto-reconnect when the connection drops, restoring the session and subscriptions (implies a handshake)")
	if err := fs.Parse(args); err != nil {
		// flag.ExitOnError (the previous default) exited with code 2 on a
		// parse failure; keep that contract now that we own the exit
		return 2
	}

	eventCh := make(chan *proto.Frame, 16)
	onEvent := func(frame *proto.Frame) { eventCh <- frame }

	var client protocolClient
	if *resilient {
		rc := proto.NewResilientClient(*addr, onEvent, nil)
		client = rc
		// Connect handshakes and will keep the session alive across
		// server restarts; -hello and -keepalive are subsumed by it
		if err := rc.Connect(*timeout); err != nil {
			fmt.Fprintf(os.Stderr, "connect %s: %v\n", *addr, err)
			return 1
		}
	} else {
		plain, err := proto.Dial(*addr, onEvent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect %s: %v\n", *addr, err)
			return 1
		}
		client = plain
	}
	defer client.Close()

	// graceful goodbye so the server releases the connection right away
	defer func() {
		_ = client.CloseGracefully(*timeout)
	}()

	if plain, isPlain := client.(*proto.Client); isPlain {
		// a watching subscriber only receives, so the server's idle
		// timeout would eventually reap the connection; ping periodically
		// to prevent it. The resilient client recovers instead of keeping
		// the link alive, so it needs no keepalive.
		if *keepalive > 0 {
			stop := plain.KeepAlive(*keepalive, *timeout)
			defer stop()
		}
		if *hello {
			res, err := plain.Handshake(*timeout)
			if err != nil {
				fmt.Fprintf(os.Stderr, "handshake: %v\n", err)
				return 1
			}
			fmt.Printf("handshake ok: protocol v%d, features %v\n", res.Version, res.Features)
		}
	}

	if *topic != "" {
		if err := client.Subscribe(*topic, *timeout); err != nil {
			fmt.Fprintf(os.Stderr, "subscribe %s: %v\n", *topic, err)
			return 1
		}
		fmt.Printf("subscribed to %q\n", *topic)
	}

	if *action != "" {
		var payload any
		if *data != "" {
			payload = coerceJSONPayload(*data)
		}
		resp, err := client.Call(*action, payload, *timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "call %s: %v\n", *action, err)
			return 1
		}
		if !resp.OK() {
			fmt.Printf("response: error: %s\n", resp.Err.Error)
		} else {
			fmt.Printf("response: %s\n", string(resp.Data))
		}
	}

	watchFor := *watch
	if *topic != "" && watchFor == 0 {
		watchFor = time.Hour // stay subscribed until interrupted
	}
	if watchFor > 0 {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		timer := time.NewTimer(watchFor)
		defer timer.Stop()
		for {
			select {
			case frame := <-eventCh:
				printEvent(frame)
			case <-timer.C:
				return 0
			case <-sigCh:
				return 0
			case <-client.Done():
				fmt.Println("connection closed")
				return 0
			}
		}
	}
	return 0
}

// coerceJSONPayload passes well-formed JSON through untouched and wraps any
// other input into a JSON string, so `-data hello` sends "hello" instead of
// dying on a RawMessage marshal error.
func coerceJSONPayload(data string) any {
	if json.Valid([]byte(data)) {
		return json.RawMessage(data)
	}
	return data
}

func printEvent(frame *proto.Frame) {
	msg, err := proto.DecodeJSONMessage(frame)
	if err != nil {
		fmt.Printf("push: %s (undecodable)\n", frame)
		return
	}
	fmt.Printf("push [%s]: %s\n", msg.Action, string(msg.Data))
}

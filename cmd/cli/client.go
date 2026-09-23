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

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "server address")
	action := flag.String("action", "", "request action (e.g. echo)")
	data := flag.String("data", "", "request payload; raw JSON string")
	topic := flag.String("subscribe", "", "topic to subscribe; with -watch keeps receiving pushes")
	watch := flag.Duration("watch", 0, "keep running for this long after the request (e.g. 30s)")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout")
	keepalive := flag.Duration("keepalive", 30*time.Second, "ping interval, keeps the connection alive through server idle timeouts (0 disables)")
	hello := flag.Bool("hello", false, "negotiate the protocol version before anything else")
	resilient := flag.Bool("resilient", false, "auto-reconnect when the connection drops, restoring the session and subscriptions (implies a handshake)")
	flag.Parse()

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
			os.Exit(1)
		}
	} else {
		plain, err := proto.Dial(*addr, onEvent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect %s: %v\n", *addr, err)
			os.Exit(1)
		}
		client = plain
	}
	defer client.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

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
				os.Exit(1)
			}
			fmt.Printf("handshake ok: protocol v%d, features %v\n", res.Version, res.Features)
		}
	}

	if *topic != "" {
		if err := client.Subscribe(*topic, *timeout); err != nil {
			fmt.Fprintf(os.Stderr, "subscribe %s: %v\n", *topic, err)
			os.Exit(1)
		}
		fmt.Printf("subscribed to %q\n", *topic)
	}

	if *action != "" {
		var payload any
		if *data != "" {
			payload = json.RawMessage(*data)
		}
		resp, err := client.Call(*action, payload, *timeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "call %s: %v\n", *action, err)
			os.Exit(1)
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
		timer := time.NewTimer(watchFor)
		defer timer.Stop()
		for {
			select {
			case frame := <-eventCh:
				printEvent(frame)
			case <-timer.C:
				return
			case <-sigCh:
				return
			case <-client.Done():
				fmt.Println("connection closed")
				return
			}
		}
	}
}

func printEvent(frame *proto.Frame) {
	msg, err := proto.DecodeJSONMessage(frame)
	if err != nil {
		fmt.Printf("push: %s (undecodable)\n", frame)
		return
	}
	fmt.Printf("push [%s]: %s\n", msg.Action, string(msg.Data))
}

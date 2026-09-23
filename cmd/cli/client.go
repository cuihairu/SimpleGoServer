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

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "server address")
	action := flag.String("action", "", "request action (e.g. echo)")
	data := flag.String("data", "", "request payload; raw JSON string")
	topic := flag.String("subscribe", "", "topic to subscribe; with -watch keeps receiving pushes")
	watch := flag.Duration("watch", 0, "keep running for this long after the request (e.g. 30s)")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout")
	flag.Parse()

	eventCh := make(chan *proto.Frame, 16)
	client, err := proto.Dial(*addr, func(frame *proto.Frame) {
		eventCh <- frame
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer client.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// graceful goodbye so the server releases the connection right away
	defer func() {
		_ = client.CloseGracefully(*timeout)
	}()

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
				fmt.Println("connection closed by server")
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

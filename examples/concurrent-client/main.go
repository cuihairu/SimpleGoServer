// Command concurrent-client drives the echo-server example the way a real
// workload would. It demonstrates:
//
//   - many concurrent connections, each multiplexing its own requests;
//   - the client-side request/response API (Call) with per-call timeouts;
//   - subscribing to a server-pushed topic and receiving PUBLISH frames;
//   - a graceful goodbye (CLOSE frame) instead of a silent disconnect.
//
// Start the server first, then:
//
//	go run ./examples/concurrent-client
//	go run ./examples/concurrent-client -clients 32 -requests 100
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/proto"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "server address")
	clients := flag.Int("clients", 8, "number of concurrent connections")
	requests := flag.Int("requests", 20, "requests per connection")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout")
	subscribe := flag.Duration("subscribe", 3*time.Second, "how long to listen to the ticks topic (0 disables)")
	flag.Parse()

	var wg sync.WaitGroup
	results := make(chan result, *clients)

	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results <- runClient(*addr, id, *requests, *timeout, *subscribe)
		}(i)
	}
	wg.Wait()
	close(results)

	total, ok, latency := 0, 0, time.Duration(0)
	for r := range results {
		total += r.requests
		ok += r.ok
		latency += r.latency
		if r.err != nil {
			fmt.Printf("client %d: %v\n", r.id, r.err)
		}
	}
	fmt.Printf("done: %d/%d requests ok, avg latency %s\n", ok, total, latency/time.Duration(max(total, 1)))
}

type result struct {
	id       int
	requests int
	ok       int
	latency  time.Duration
	err      error
}

// runClient opens one connection, fires requests over it concurrently with
// the other clients, optionally listens to server pushes, and says goodbye.
func runClient(addr string, id, requests int, timeout, subscribeFor time.Duration) result {
	pushes := make(chan *proto.Frame, 16)
	client, err := proto.Dial(addr, func(frame *proto.Frame) {
		select {
		case pushes <- frame:
		default: // never let a slow consumer block the read loop
		}
	})
	if err != nil {
		return result{id: id, err: fmt.Errorf("dial: %w", err)}
	}
	// CloseGracefully sends a CLOSE frame and waits for the ack, so the
	// server releases its per-connection state immediately. It falls back
	// to a plain close if the server does not answer.
	defer func() { _ = client.CloseGracefully(timeout) }()

	res := result{id: id}

	if subscribeFor > 0 && id == 0 { // one client doubles as a subscriber
		if err := client.Subscribe("ticks", timeout); err != nil {
			return result{id: id, err: fmt.Errorf("subscribe: %w", err)}
		}
		fmt.Printf("client %d: subscribed to \"ticks\"\n", id)
		go collectPushes(pushes, subscribeFor)
	}

	for i := 0; i < requests; i++ {
		payload := fmt.Sprintf("hello from client %d, request %d", id, i)
		start := time.Now()
		resp, err := client.Call("echo", payload, timeout)
		res.latency += time.Since(start)
		res.requests++
		if err != nil {
			res.err = fmt.Errorf("call: %w", err)
			return res
		}
		if !resp.OK() {
			res.err = fmt.Errorf("server error: %s", resp.Err.Error)
			return res
		}
		// the response is the JSONMessage envelope {"action","data"};
		// verify the payload survived the round trip intact
		var envelope struct {
			Data string `json:"data"`
		}
		if err := json.Unmarshal(resp.Data, &envelope); err != nil {
			res.err = fmt.Errorf("decode response: %w", err)
			return res
		}
		if envelope.Data != payload {
			res.err = fmt.Errorf("echo mismatch: got %q", envelope.Data)
			return res
		}
		res.ok++
	}
	return res
}

// collectPushes drains the push channel while the subscription is live.
func collectPushes(pushes <-chan *proto.Frame, forDuration time.Duration) {
	deadline := time.After(forDuration)
	for {
		select {
		case frame := <-pushes:
			msg, err := proto.DecodeJSONMessage(frame)
			if err != nil {
				continue
			}
			fmt.Printf("push [%s]: %s\n", msg.Action, string(msg.Data))
		case <-deadline:
			return
		}
	}
}

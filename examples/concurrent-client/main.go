// Command concurrent-client drives the echo-server example the way a real
// workload would. It demonstrates:
//
//   - many concurrent connections, each multiplexing its own requests;
//   - the client-side request/response API (Call) with per-call timeouts;
//   - subscribing to a server-pushed topic and receiving PUBLISH frames;
//   - client-side keepalive so a receive-only subscriber survives the
//     server's idle timeout;
//   - streamed large payloads: -payload above the fragmentation threshold
//     rides the FlagMore path end to end, transparently to the caller;
//   - a graceful goodbye (CLOSE frame) instead of a silent disconnect.
//
// Start the server first, then:
//
//	go run ./examples/concurrent-client
//	go run ./examples/concurrent-client -clients 32 -requests 100
//	go run ./examples/concurrent-client -payload 2097152
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// Flags are registered exactly once per process (init), so main stays a
// thin, repeatable shell that tests can drive more than once.
var (
	addr      = flag.String("addr", "127.0.0.1:8080", "server address")
	clients   = flag.Int("clients", 8, "number of concurrent connections")
	requests  = flag.Int("requests", 20, "requests per connection")
	timeout   = flag.Duration("timeout", 5*time.Second, "per-request timeout")
	subscribe = flag.Duration("subscribe", 3*time.Second, "how long to listen to the ticks topic (0 disables)")
	payload   = flag.Int("payload", 0, "request payload size in bytes (0 = short greeting; sizes past ~960KiB exercise streaming fragmentation)")
)

func main() {
	flag.Parse()

	summarize(runAll(*addr, *clients, *requests, *timeout, *subscribe, *payload))
}

// bigPayload builds exactly size bytes: sentinel head/tail around a
// repeating pattern, so the echo comparison proves the fragments were
// reassembled in order, not just that the lengths match.
func bigPayload(size int) string {
	if size < 16 {
		return strings.Repeat("x", size)
	}
	const head, tail = "HEAD:", ":TAIL"
	body := strings.Repeat("0123456789abcdef", size/16)
	return (head + body + tail)[:size-len(tail)] + tail
}

// runAll drives numClients concurrent connections and collects one result
// per client; payloadSize > 0 swaps the greeting for a payload of that
// many bytes.
func runAll(addr string, numClients, requests int, timeout, subscribeFor time.Duration, payloadSize int) []result {
	var wg sync.WaitGroup
	results := make(chan result, numClients)

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results <- runClient(addr, id, requests, timeout, subscribeFor, payloadSize)
		}(i)
	}
	wg.Wait()
	close(results)

	out := make([]result, 0, numClients)
	for r := range results {
		out = append(out, r)
	}
	return out
}

// summarize prints the per-client failures and the aggregate scoreboard.
func summarize(results []result) {
	total, ok, latency := 0, 0, time.Duration(0)
	for _, r := range results {
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
// payloadSize > 0 sends that many bytes per request instead of a greeting;
// past the fragmentation threshold the round trip rides streamed frames.
func runClient(addr string, id, requests int, timeout, subscribeFor time.Duration, payloadSize int) result {
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
		// a subscriber only receives frames, so the server's idle timeout
		// would eventually reap this otherwise silent connection — keep
		// it alive with periodic pings. The interval must stay well under
		// the server's IdleTimeout (60s in the echo-server example).
		stopKeepAlive := client.KeepAlive(15*time.Second, timeout)
		defer stopKeepAlive()
		go collectPushes(pushes, subscribeFor)
	}

	for i := 0; i < requests; i++ {
		payload := fmt.Sprintf("hello from client %d, request %d", id, i)
		if payloadSize > 0 {
			payload = bigPayload(payloadSize)
		}
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

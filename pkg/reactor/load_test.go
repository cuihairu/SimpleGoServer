package reactor

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// TestManyConcurrentClients simulates a burst of production load: many
// clients connect at once, each runs a series of requests over its own
// connection and then one bulk transfer, and every response must arrive
// complete and paired with the right request.
func TestManyConcurrentClients(t *testing.T) {
	reactor := startTestReactor(t)
	waitListening(t, reactor)

	const (
		clients           = 32
		requestsPerClient = 10
		bulkSize          = 64 * 1024
	)
	bulk := strings.Repeat("a", bulkSize) // echoed back inside a JSON string

	var wg sync.WaitGroup
	errs := make(chan error, clients)
	start := make(chan struct{})
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := proto.Dial(reactor.Addr().String(), nil)
			if err != nil {
				errs <- fmt.Errorf("Dial(): %w", err)
				return
			}
			defer client.Close()
			<-start // release all clients at once for a real thundering herd

			for i := 0; i < requestsPerClient; i++ {
				resp, err := client.Call("load", "ping", 10*time.Second)
				if err != nil {
					errs <- fmt.Errorf("Call(): %w", err)
					return
				}
				if !resp.OK() {
					errs <- fmt.Errorf("server error: %s", resp.Err.Error)
					return
				}
			}

			// bulk transfer: the payload must survive the round trip intact
			resp, err := client.Call("load", bulk, 30*time.Second)
			if err != nil {
				errs <- fmt.Errorf("bulk Call(): %w", err)
				return
			}
			var envelope struct {
				Data string `json:"data"`
			}
			if err := json.Unmarshal(resp.Data, &envelope); err != nil {
				errs <- fmt.Errorf("unmarshal bulk response: %w", err)
				return
			}
			if envelope.Data != bulk {
				errs <- fmt.Errorf("bulk payload mismatch: got %d bytes, want %d", len(envelope.Data), len(bulk))
				return
			}
			errs <- nil
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}

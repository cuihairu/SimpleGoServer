package reactor

import (
	"fmt"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// oneEchoCall performs one request/response round trip and fails the
// benchmark if it does not complete cleanly.
func oneEchoCall(b *testing.B, client *proto.Client) {
	resp, err := client.Call("bench", "ping", 10*time.Second)
	if err != nil {
		b.Errorf("Call(): %v", err)
		return
	}
	if !resp.OK() {
		b.Errorf("server error: %s", resp.Err.Error)
	}
}

// BenchmarkReactorEchoThroughput measures end-to-end request/response
// throughput over real TCP. Clients fan out with b.RunParallel, so the
// concurrency is controlled by -parallel (GOMAXPROCS by default):
//
//	go test ./pkg/reactor -bench EchoThroughput -benchmem
func BenchmarkReactorEchoThroughput(b *testing.B) {
	reactor := startTestReactor(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client, err := proto.Dial(reactor.Addr().String(), nil)
		if err != nil {
			b.Errorf("Dial(): %v", err)
			return
		}
		defer client.Close()
		for pb.Next() {
			oneEchoCall(b, client)
		}
	})
}

// BenchmarkReactorEchoClients reports throughput at fixed client counts so
// the effect of concurrency on the reactor is comparable across runs.
func BenchmarkReactorEchoClients(b *testing.B) {
	reactor := startTestReactor(b)
	for _, clients := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("clients=%d", clients), func(b *testing.B) {
			per := b.N / clients
			if per == 0 {
				per = 1
			}
			results := make(chan error, clients)
			b.ResetTimer()
			for c := 0; c < clients; c++ {
				go func() {
					client, err := proto.Dial(reactor.Addr().String(), nil)
					if err != nil {
						results <- err
						return
					}
					defer client.Close()
					for i := 0; i < per; i++ {
						resp, err := client.Call("bench", "ping", 10*time.Second)
						if err != nil {
							results <- err
							return
						}
						if !resp.OK() {
							results <- fmt.Errorf("server error: %s", resp.Err.Error)
							return
						}
					}
					results <- nil
				}()
			}
			for c := 0; c < clients; c++ {
				if err := <-results; err != nil {
					b.Error(err)
				}
			}
		})
	}
}

// BenchmarkReactorConnChurn measures the accept + pipeline setup + teardown
// path: every iteration opens a fresh connection, completes one request and
// closes it. This is the cost profile of clients that do not keep
// connections alive.
func BenchmarkReactorConnChurn(b *testing.B) {
	reactor := startTestReactor(b)
	addr := reactor.Addr().String()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client, err := proto.Dial(addr, nil)
		if err != nil {
			b.Fatalf("Dial(): %v", err)
		}
		resp, err := client.Call("bench", "ping", 10*time.Second)
		if err != nil {
			_ = client.Close()
			b.Fatalf("Call(): %v", err)
		}
		if !resp.OK() {
			_ = client.Close()
			b.Fatalf("server error: %s", resp.Err.Error)
		}
		_ = client.Close()
	}
}

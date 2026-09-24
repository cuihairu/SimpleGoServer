package reactor

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cuihairu/simplegoserver/pkg/proto"
)

// TestSoakMemoryStability is the long-run soak that docs/Benchmark.md
// calls out as out of scope for the regular benchmark suite. It drives
// the reactor through both load shapes — connection churn (accept /
// serveConn / close recycling) and sustained long-lived traffic — and
// samples goroutines and heap along the way. The assertions are
// deliberately about monotonic growth and final reclamation, not
// absolute numbers: GC timing and machine load make absolutes flaky.
//
// It is skipped by default so CI stays fast:
//
//	SOAK=1 go test ./pkg/reactor -run TestSoak -timeout 15m
//	SOAK=1 SOAK_SECONDS=600 go test ./pkg/reactor -run TestSoak  # longer
func TestSoakMemoryStability(t *testing.T) {
	if os.Getenv("SOAK") == "" {
		t.Skip("set SOAK=1 (optionally SOAK_SECONDS=<n>) to run the multi-minute soak")
	}
	seconds := 180
	if v := os.Getenv("SOAK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			seconds = n
		}
	}

	reactor := startTestReactor(t)
	waitListening(t, reactor)
	addr := reactor.Addr().String()
	baseGoroutines := runtime.NumGoroutine()
	start := time.Now()

	sample := func(stage string, conns int) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		t.Logf("%-11s t=%5.0fs conns=%7d goroutines=%3d (base %d) heapInuse=%4dMiB sys=%4dMiB numGC=%3d",
			stage, time.Since(start).Seconds(), conns, runtime.NumGoroutine(), baseGoroutines,
			m.HeapInuse>>20, m.Sys>>20, m.NumGC)
	}

	// --- stage 1: connection churn — every iteration builds a fresh
	// batch of clients, works them, and tears them down, so any accept,
	// serveConn or close path that leaks grows the footprint linearly.
	const batches, perBatch = 8, 8
	churnUntil := time.Now().Add(time.Duration(seconds/2) * time.Second)
	n := 0
	for time.Now().Before(churnUntil) {
		var wg sync.WaitGroup
		for i := 0; i < batches; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client, err := proto.Dial(addr, nil)
				if err != nil {
					t.Errorf("churn dial: %v", err)
					return
				}
				for j := 0; j < perBatch; j++ {
					if _, err := client.Call("echo", "churn", 5*time.Second); err != nil {
						t.Errorf("churn call: %v", err)
						return
					}
				}
				client.CloseGracefully(time.Second)
			}()
		}
		wg.Wait()
		n += batches
		sample("churn", n)
	}

	// The quiescent watermark: churn just ended — every batch goroutine
	// reaped, every connection closed, and the reactor's own loops
	// (workers, accept) are fully expanded by now, unlike right after
	// waitListening. This is the floor the teardown asserts against.
	quiescent := runtime.NumGoroutine()

	// --- stage 2: sustained traffic on long-lived connections — the
	// steady state the subscription / cache paths live in. Goroutine
	// count must hold flat: clients are fixed, so any growth is a leak.
	clients := make([]*proto.Client, batches)
	for i := range clients {
		c, err := proto.Dial(addr, nil)
		if err != nil {
			t.Fatalf("steady dial: %v", err)
		}
		clients[i] = c
	}
	steadyUntil := time.Now().Add(time.Duration(seconds/2) * time.Second)
	var steadyMu sync.Mutex
	steadyFirst, steadyLast := 0, 0
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c *proto.Client) {
			defer wg.Done()
			for time.Now().Before(steadyUntil) {
				if _, err := c.Call("echo", fmt.Sprintf("steady-%d", i), 5*time.Second); err != nil {
					t.Errorf("steady call: %v", err)
					return
				}
			}
		}(i, c)
	}
	for tick := 0; time.Now().Before(steadyUntil); tick++ {
		time.Sleep(15 * time.Second)
		g := runtime.NumGoroutine()
		steadyMu.Lock()
		if steadyFirst == 0 {
			steadyFirst = g
		}
		steadyLast = g
		steadyMu.Unlock()
		sample("steady", batches)
	}
	wg.Wait()
	sample("steady-end", batches+n)

	// Goroutines must hold flat across the steady stage: the client set
	// is fixed, so creeping growth is a leak, while a bounded drift is
	// scheduler noise.
	if steadyFirst > 0 && steadyLast > steadyFirst+steadyFirst/10+5 {
		t.Errorf("goroutines grew %d -> %d during the steady stage", steadyFirst, steadyLast)
	}

	// Tear the clients down explicitly — a deferred close would run
	// after this teardown assert and hold its goroutines open — then
	// require the footprint to return to the quiescent neighborhood.
	for _, c := range clients {
		_ = c.CloseGracefully(time.Second)
	}
	sample("closed", batches+n)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= quiescent+5 {
			sample("reclaimed", batches+n)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("goroutines did not return to the quiescent watermark: quiescent=%d now=%d",
		quiescent, runtime.NumGoroutine())
}

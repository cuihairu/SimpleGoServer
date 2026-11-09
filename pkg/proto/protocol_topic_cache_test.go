package proto

import (
	"strings"
	"testing"
	"time"
)

// TestPublishCachesWhileNoSubscriberConnected pins the offline-cache
// invariant: once the dead transport is reaped and the subscriber set is
// empty, publishes keep being cached while a session still records the
// topic — otherwise everything after the drop would be silently lost
// instead of replayed on resume.
func TestPublishCachesWhileNoSubscriberConnected(t *testing.T) {
	addr, ph := startTCPServer(t, nil)

	pushes1 := make(chan *Frame, 8)
	c1, err := Dial(addr, func(f *Frame) { pushes1 <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	res1, err := c1.Handshake(2 * time.Second)
	if err != nil {
		t.Fatalf("Handshake(): %v", err)
	}
	if err := c1.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	if _, err := ph.Publish("ticks", "live"); err != nil {
		t.Fatalf("Publish live: %v", err)
	}
	cursor := uint64((<-pushes1).Header.StreamId)

	// drop; keep publishing until the server has observed the write
	// failure and reaped the dead subscriber (see the TCP note above)
	_ = c1.Close()
	deadline := time.Now().Add(2 * time.Second)
	for ph.Subscribers("ticks") > 0 && time.Now().Before(deadline) {
		_, _ = ph.Publish("ticks", "x")
		time.Sleep(10 * time.Millisecond)
	}
	if got := ph.Subscribers("ticks"); got != 0 {
		t.Fatalf("subscribers = %d after the transport died, want 0", got)
	}

	// nobody connected from here on, yet both publishes must be cached
	if _, err := ph.Publish("ticks", "offline 1"); err != nil {
		t.Fatalf("Publish offline 1: %v", err)
	}
	if _, err := ph.Publish("ticks", "offline 2"); err != nil {
		t.Fatalf("Publish offline 2: %v", err)
	}

	pushes2 := make(chan *Frame, 8)
	c2, err := Dial(addr, func(f *Frame) { pushes2 <- f })
	if err != nil {
		t.Fatalf("re-Dial(): %v", err)
	}
	defer c2.Close()
	res2, err := c2.HandshakeWith(res1.Token, map[string]uint64{"ticks": cursor}, 2*time.Second)
	if err != nil {
		t.Fatalf("resume Handshake(): %v", err)
	}
	if !res2.Resumed {
		t.Fatal("resume not recognized")
	}
	seqs, ok := collectPublishes(pushes2, 2, 2*time.Second)
	if !ok {
		t.Fatalf("expected 2 replayed publishes with no subscriber connected, got %d (seqs=%v)", len(seqs), seqs)
	}
}

// TestTopicCacheEviction pins the cache bound: publishing past the cache
// size keeps only the most recent entries.
func TestTopicCacheEviction(t *testing.T) {
	addr, ph := startTCPServer(t, nil)
	pushes := make(chan *Frame, 1)
	c, err := Dial(addr, func(f *Frame) { pushes <- f })
	if err != nil {
		t.Fatalf("Dial(): %v", err)
	}
	defer c.Close()
	if err := c.Subscribe("ticks", 2*time.Second); err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
	// drain live pushes so the channel never blocks Publish. Shutdown is
	// ordered: cleanup closes the client first (its read loop exits, so no
	// more `pushes <- f`), then releases the drain goroutine and waits for
	// it. Closing the channel here instead raced that in-flight send — the
	// CI race detector caught close(pushes) vs `pushes <- f`.
	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-stop:
				return
			case <-pushes:
			}
		}
	}()
	t.Cleanup(func() {
		_ = c.Close()
		close(stop)
		<-drained
	})

	total := topicCacheSize + 10
	for i := 0; i < total; i++ {
		if _, err := ph.Publish("ticks", i); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	ph.mu.RLock()
	cache := ph.topicCache["ticks"]
	ph.mu.RUnlock()
	if len(cache) != topicCacheSize {
		t.Fatalf("cache holds %d entries, want %d", len(cache), topicCacheSize)
	}
	// the retained entries are the most recent ones, in order
	for i := 1; i < len(cache); i++ {
		if cache[i].seq <= cache[i-1].seq {
			t.Fatalf("cache not monotonic at %d: %v", i, cache)
		}
	}
	if cache[len(cache)-1].seq != uint32(total) {
		t.Fatalf("newest cached seq = %d, want %d", cache[len(cache)-1].seq, total)
	}
}

// TestTopicCacheGlobalBudget pins the memory bound of the replay cache:
// the per-topic entry cap does not bound memory (topics are unbounded, a
// cached frame can be up to MaxFrameSize), so a global byte budget evicts
// whole topics largest-first under pressure. Bookkeeping must stay exact —
// cacheBytes has to equal the sum of what survives.
func TestTopicCacheGlobalBudget(t *testing.T) {
	const budget = 4 * 1024
	addr, ph := startTCPServer(t, nil)
	ph.mu.Lock()
	ph.cacheBudget = budget
	ph.mu.Unlock()

	// sessions must remember the topics, or Publish would skip the cache
	// with nobody to ever ask for the replay
	for _, topic := range []string{"a", "b"} {
		c, err := Dial(addr, nil)
		if err != nil {
			t.Fatalf("Dial(%s): %v", topic, err)
		}
		defer c.Close()
		if _, err := c.Handshake(2 * time.Second); err != nil {
			t.Fatalf("Handshake(): %v", err)
		}
		if err := c.Subscribe(topic, 2*time.Second); err != nil {
			t.Fatalf("Subscribe(%s): %v", topic, err)
		}
	}

	payload := strings.Repeat("x", 900) // ~1KiB per encoded frame
	for i := 0; i < 10; i++ {
		if _, err := ph.Publish("a", payload); err != nil {
			t.Fatalf("Publish a/%d: %v", i, err)
		}
		if _, err := ph.Publish("b", payload); err != nil {
			t.Fatalf("Publish b/%d: %v", i, err)
		}
	}

	ph.mu.RLock()
	held := ph.cacheBytes
	kept := make(map[string]int, len(ph.topicCache))
	for topic, cache := range ph.topicCache {
		kept[topic] = frameBytes(cache)
	}
	ph.mu.RUnlock()

	if held > budget {
		t.Fatalf("cache holds %d bytes, budget is %d", held, budget)
	}
	total := 0
	for _, size := range kept {
		total += size
	}
	if total != held {
		t.Fatalf("bookkeeping drift: cacheBytes=%d, surviving caches sum to %d (%v)", held, total, kept)
	}
	// 20 publishes at ~1KiB each amount to ~20KiB — roughly 5x the budget.
	// Landing at or under the budget is itself the proof that evictions
	// fired; without enforcement `held` would sit near 20KiB.
	if held < budget/2 {
		t.Fatalf("cache at %d bytes looks over-evicted for budget %d — bookkeeping likely drifts low", held, budget)
	}
}

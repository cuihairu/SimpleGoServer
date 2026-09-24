package balancer

import (
	"fmt"
	"sync"
	"testing"
)

func TestRoundRobinBalancer_Next(t *testing.T) {
	bulk := NewFakeBackendBulk(10)
	balancer := NewRoundRobinBalancer[*FakeBackend]()
	for _, backend := range bulk {
		err := balancer.Register(backend)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 100; i++ {
		next, err := balancer.Next(fmt.Sprintf("%d", i))
		if err != nil {
			t.Error(err)
		}
		t.Logf("select backend %s", next.Id())
	}
}

// TestRoundRobinBalancer_NextAfterShrink pins the out-of-range fix: the
// stored cursor must wrap when Unregister shrinks the backend list, or the
// next Next indexes past the slice and panics.
func TestRoundRobinBalancer_NextAfterShrink(t *testing.T) {
	bulk := NewFakeBackendBulk(3)
	balancer := NewRoundRobinBalancer[*FakeBackend]()
	for _, backend := range bulk {
		if err := balancer.Register(backend); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := balancer.Next("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := balancer.Next("b"); err != nil {
		t.Fatal(err)
	}
	if err := balancer.Unregister(bulk[0]); err != nil {
		t.Fatal(err)
	}
	// cursor stood at index 2, list is now length 2 — these must not panic
	for i := 0; i < 5; i++ {
		if _, err := balancer.Next("c"); err != nil {
			t.Fatalf("Next after shrink: %v", err)
		}
	}
}

// TestRoundRobinBalancer_ConcurrentNext drives Next from several goroutines:
// the cursor is a written counter, so a read-locked Next is a data race the
// race detector flags (and lost updates break the rotation).
func TestRoundRobinBalancer_ConcurrentNext(t *testing.T) {
	bulk := NewFakeBackendBulk(4)
	balancer := NewRoundRobinBalancer[*FakeBackend]()
	for _, backend := range bulk {
		if err := balancer.Register(backend); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, err := balancer.Next("k"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

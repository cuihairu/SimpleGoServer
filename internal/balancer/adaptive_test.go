package balancer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cuihairu/simplegoserver/pkg"
)

type loadBackend struct {
	id   string
	load float64
	mu   sync.Mutex
}

func (b *loadBackend) Id() string { return b.id }

func (b *loadBackend) Load() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.load
}

func (b *loadBackend) setLoad(load float64) {
	b.mu.Lock()
	b.load = load
	b.mu.Unlock()
}

func newLoadBackends(loads ...float64) []*loadBackend {
	backends := make([]*loadBackend, len(loads))
	for i, load := range loads {
		backends[i] = &loadBackend{id: fmt.Sprintf("backend-%d", i), load: load}
	}
	return backends
}

func TestAdaptiveBalancerPicksLeastLoaded(t *testing.T) {
	balancer := NewAdaptiveBalancer[*loadBackend]()
	backends := newLoadBackends(10, 1, 10)
	for _, b := range backends {
		if err := balancer.Register(b); err != nil {
			t.Fatalf("Register(): %v", err)
		}
	}

	for i := 0; i < 20; i++ {
		next, err := balancer.Next("")
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
		if next.Id() != "backend-1" {
			t.Fatalf("picked %s, want backend-1 (lowest load)", next.Id())
		}
	}
}

func TestAdaptiveBalancerFollowsLoadChanges(t *testing.T) {
	balancer := NewAdaptiveBalancer[*loadBackend]()
	backends := newLoadBackends(1, 1)
	for _, b := range backends {
		_ = balancer.Register(b)
	}

	// equal loads: both backends must receive traffic (random tie-break)
	seen := map[string]int{}
	for i := 0; i < 200; i++ {
		next, err := balancer.Next("")
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
		seen[next.Id()]++
	}
	if len(seen) != 2 {
		t.Fatalf("tie-break spread = %v, want both backends picked", seen)
	}

	// one backend becomes loaded: all traffic shifts to the other
	backends[0].setLoad(100)
	for i := 0; i < 20; i++ {
		next, _ := balancer.Next("")
		if next.Id() != "backend-1" {
			t.Fatalf("picked %s after load shift, want backend-1", next.Id())
		}
	}
}

func TestAdaptiveBalancerEmptyAndSingle(t *testing.T) {
	balancer := NewAdaptiveBalancer[*loadBackend]()
	if _, err := balancer.Next(""); err == nil {
		t.Fatal("Next() on empty balancer should fail")
	}
	only := newLoadBackends(5)
	_ = balancer.Register(only[0])
	next, err := balancer.Next("")
	if err != nil || next.Id() != only[0].Id() {
		t.Fatalf("Next() = %v, %v; want the single backend", next, err)
	}
}

func TestAdaptiveBalancerRegisterDedupAndUnregister(t *testing.T) {
	balancer := NewAdaptiveBalancer[*loadBackend]()
	backends := newLoadBackends(1, 2)
	for _, b := range backends {
		_ = balancer.Register(b)
	}
	_ = balancer.Register(backends[0]) // duplicate id must be ignored
	if balancer.Size() != 2 {
		t.Fatalf("size = %d after duplicate register, want 2", balancer.Size())
	}
	_ = balancer.Unregister(backends[0])
	if balancer.Size() != 1 {
		t.Fatalf("size = %d after unregister, want 1", balancer.Size())
	}
}

func TestAdaptiveBalancerConcurrentNext(t *testing.T) {
	balancer := NewAdaptiveBalancer[*loadBackend]()
	backends := newLoadBackends(1, 2, 3)
	for _, b := range backends {
		_ = balancer.Register(b)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if _, err := balancer.Next(""); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

var _ pkg.Backend = (*loadBackend)(nil)

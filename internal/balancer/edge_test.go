package balancer

import (
	"testing"
)

// balancerFacade smooths the generic differences between strategies
// (adaptive needs Load, weighted ones need Weight) behind closures, so
// cross-cutting contracts — empty-table errors, Iterate early exit — run
// against every one of them.
type balancerFacade struct {
	register func(id string) error
	next     func(key string) error
	iterate  func(visit func() bool) // visit returning false stops the walk
}

// everyBalancer lists a fresh instance of each strategy behind one entry
// point.
func everyBalancer(t *testing.T) map[string]balancerFacade {
	t.Helper()
	fake := func(id string) *FakeBackend { return NewFakeBackend(id, 1) }
	return map[string]balancerFacade{
		"adaptive": func() balancerFacade {
			b := NewAdaptiveBalancer[*loadableBackend]()
			return balancerFacade{
				register: func(id string) error { return b.Register(&loadableBackend{FakeBackend: NewFakeBackend(id, 1)}) },
				next:     func(key string) error { _, err := b.Next(key); return err },
				iterate:  func(visit func() bool) { b.Iterate(func(*loadableBackend) bool { return visit() }) },
			}
		}(),
		"random": func() balancerFacade {
			b := NewRandomBalancer[*FakeBackend]()
			return balancerFacade{
				register: func(id string) error { return b.Register(fake(id)) },
				next:     func(key string) error { _, err := b.Next(key); return err },
				iterate:  func(visit func() bool) { b.Iterate(func(*FakeBackend) bool { return visit() }) },
			}
		}(),
		"round_robin": func() balancerFacade {
			b := NewRoundRobinBalancer[*FakeBackend]()
			return balancerFacade{
				register: func(id string) error { return b.Register(fake(id)) },
				next:     func(key string) error { _, err := b.Next(key); return err },
				iterate:  func(visit func() bool) { b.Iterate(func(*FakeBackend) bool { return visit() }) },
			}
		}(),
		"least_connections": func() balancerFacade {
			b := NewLeastConnectionsBalancer[*FakeBackend](false)
			return balancerFacade{
				register: func(id string) error { return b.Register(fake(id)) },
				next:     func(key string) error { _, err := b.Next(key); return err },
				iterate:  func(visit func() bool) { b.Iterate(func(*FakeBackend) bool { return visit() }) },
			}
		}(),
		"weighted_round_robin": func() balancerFacade {
			b := NewWeightedRoundRobinBalancer[*FakeBackend]()
			return balancerFacade{
				register: func(id string) error { return b.Register(fake(id)) },
				next:     func(key string) error { _, err := b.Next(key); return err },
				iterate:  func(visit func() bool) { b.Iterate(func(*FakeBackend) bool { return visit() }) },
			}
		}(),
		"weighted_random": func() balancerFacade {
			b := NewWeightedRandomBalancer[*FakeBackend]()
			return balancerFacade{
				register: func(id string) error { return b.Register(fake(id)) },
				next:     func(key string) error { _, err := b.Next(key); return err },
				iterate:  func(visit func() bool) { b.Iterate(func(*FakeBackend) bool { return visit() }) },
			}
		}(),
		"ip_hashing": func() balancerFacade {
			b := NewIPHashingBalancer[*FakeBackend](4)
			return balancerFacade{
				register: func(id string) error { return b.Register(fake(id)) },
				next:     func(key string) error { _, err := b.Next(key); return err },
				iterate:  func(visit func() bool) { b.Iterate(func(*FakeBackend) bool { return visit() }) },
			}
		}(),
	}
}

// TestNextOnEmptyBalancerErrors pins that every strategy reports an error
// instead of returning a zero backend when nothing is registered.
func TestNextOnEmptyBalancerErrors(t *testing.T) {
	for name, b := range everyBalancer(t) {
		if b.next("10.0.0.1:1234") == nil {
			t.Fatalf("%s: Next on an empty table must fail", name)
		}
	}
}

// TestIterateStopsWhenCallbackSaysSo pins the early-exit contract: a
// callback returning false ends the walk, so exactly one backend is
// visited no matter how many are registered.
func TestIterateStopsWhenCallbackSaysSo(t *testing.T) {
	for name, b := range everyBalancer(t) {
		for _, id := range []string{"a", "b", "c"} {
			if err := b.register(id); err != nil {
				t.Fatalf("%s: Register(): %v", name, err)
			}
		}
		visited := 0
		b.iterate(func() bool {
			visited++
			return false
		})
		if visited != 1 {
			t.Fatalf("%s: Iterate visited %d backends after the first false, want 1", name, visited)
		}
	}
}

// TestWeightedRoundRobinRegisterValidation covers the weight rules: a
// non-positive weight is rejected, and re-registering an existing backend
// with a new weight updates it in place and adjusts the total.
func TestWeightedRoundRobinRegisterValidation(t *testing.T) {
	b := NewWeightedRoundRobinBalancer[*FakeBackend]()
	if err := b.Register(NewFakeBackend("zero", 0)); err == nil {
		t.Fatal("a zero weight must be rejected")
	}
	if err := b.Register(NewFakeBackend("neg", -3)); err == nil {
		t.Fatal("a negative weight must be rejected")
	}

	first := NewFakeBackend("a", 2)
	if err := b.Register(first); err != nil {
		t.Fatalf("Register(): %v", err)
	}
	if b.totalWeight != 2 {
		t.Fatalf("totalWeight = %d after the first register, want 2", b.totalWeight)
	}

	// same id, new weight: the entry is updated in place, not duplicated
	replacement := NewFakeBackend("a", 5)
	if err := b.Register(replacement); err != nil {
		t.Fatalf("re-Register(): %v", err)
	}
	if len(b.backends) != 1 {
		t.Fatalf("backends = %d after a duplicate register, want 1", len(b.backends))
	}
	if b.totalWeight != 5 {
		t.Fatalf("totalWeight = %d after the weight update, want 5", b.totalWeight)
	}
}

// TestLeastConnectionsRegisterRefreshesCount covers the updateCount mode's
// duplicate-register branch: re-registering an existing backend refreshes
// its live count instead of adding a second entry.
func TestLeastConnectionsRegisterRefreshesCount(t *testing.T) {
	b := NewLeastConnectionsBalancer[*FakeBackend](true)
	fresh := NewFakeBackend("a", 1)
	if err := b.Register(fresh); err != nil {
		t.Fatalf("Register(): %v", err)
	}

	busy := NewFakeBackend("a", 1)
	busy.SetCount(9)
	if err := b.Register(busy); err != nil {
		t.Fatalf("re-Register(): %v", err)
	}
	if len(b.backends) != 1 {
		t.Fatalf("backends = %d after the duplicate register, want 1", len(b.backends))
	}
	if got := b.backends[0].Count(); got != 9 {
		t.Fatalf("count = %d after the refresh, want 9", got)
	}
}

// TestWeightedRoundRobinFallsBackOnDecayedWeights covers Next's trailing
// fallback: Weight() is a live figure a backend may change behind the
// balancer's back, and a backend decayed to zero shifts the running sums,
// so the walk can fall through and must still return a usable backend.
func TestWeightedRoundRobinFallsBackOnDecayedWeights(t *testing.T) {
	b := NewWeightedRoundRobinBalancer[*FakeBackend]()
	if err := b.Register(NewFakeBackend("a", 1)); err != nil {
		t.Fatalf("Register(): %v", err)
	}
	if err := b.Register(NewFakeBackend("b", 2)); err != nil {
		t.Fatalf("Register(): %v", err)
	}

	// "b" decays to zero without telling the balancer; totalWeight still
	// says 3, so currentWeight can step past the real sums
	b.backends[1].SetWeight(0)
	for i := 0; i < 6; i++ {
		got, err := b.Next("k")
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
		if got.Id() == "" {
			t.Fatal("Next returned a zero backend")
		}
	}
}
